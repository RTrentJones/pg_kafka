//! Handing parsed requests to the DB thread.

use crossbeam_channel::Sender;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::{debug, warn};

use super::long_poll::LongPoll;
use super::response_budget::ReplyCharge;
use crate::kafka::api::{FetchRequestData, RequestBody};
use crate::kafka::constants::MAX_LONG_POLLS_PER_CONNECTION;
use crate::kafka::messages::{KafkaRequest, RequestLanes};
use crate::kafka::pending_fetches::PendingFetchRegistry;
use crate::kafka::protocol::DecodedCharge;

/// How long a full request channel is waited on before the next try.
const FULL_CHANNEL_BACKOFF: Duration = Duration::from_millis(1);

/// Where a connection's requests go: the DB thread's lanes, and the long-poll machinery.
#[derive(Clone)]
pub(super) struct Dispatcher {
    pub(super) request_lanes: RequestLanes,
    pub(super) registry: Arc<PendingFetchRegistry>,
    /// The safety-net re-fetch interval of a long poll (`pg_kafka.fetch_poll_interval_ms`),
    /// or `None` when long polling is disabled.
    pub(super) long_poll_interval: Option<Duration>,
    /// The configuration snapshot: `pg_kafka.fetch_max_bytes` is read from it per Fetch, so a
    /// reload applies to the requests read after it.
    pub(super) runtime_context: Arc<crate::kafka::RuntimeContext>,
}

/// Kafka's broker-side limits on a consumer Fetch (`KafkaApis.handleFetchRequest`): the
/// response holds at most `fetch_max_bytes` whatever the request's `max_bytes`, and
/// `min_bytes` is lowered to that, so a long poll never waits for more than it may return.
/// Each partition still reads up to its `partition_max_bytes` within what is left, and the
/// first record of the first partition with data is returned even when larger (KIP-74).
pub(super) fn cap_fetch(fetch: &mut FetchRequestData, fetch_max_bytes: i32) {
    fetch.max_bytes = fetch.max_bytes.min(fetch_max_bytes);
    fetch.min_bytes = fetch.min_bytes.min(fetch.max_bytes);
}

/// Where [`Dispatcher::route`] sends a request.
pub(super) enum Route {
    /// A Fetch that may wait: run it as a long poll ([`Dispatcher::spawn_long_poll`]).
    LongPoll(LongPollJob),
    /// Anything else: straight to its lane ([`Dispatcher::send`]).
    Lane(KafkaRequest),
}

/// A Fetch admitted as a long poll, with its per-connection long-poll slot.
pub(super) struct LongPollJob {
    long_poll: LongPoll,
    permit: OwnedSemaphorePermit,
    decode_charge: DecodedCharge,
    poll_interval: Duration,
}

impl Dispatcher {
    /// Hand one parsed request to the DB thread ([`Self::route`], then the long poll or the
    /// lane), reserving nothing of the connection's response budget beyond what the long poll
    /// reserves itself. Returns false when the worker channel is gone.
    #[cfg(test)]
    pub(super) async fn dispatch(
        &self,
        request: KafkaRequest,
        long_poll_permits: &Arc<Semaphore>,
        reply: ReplyCharge,
    ) -> bool {
        match self.route(request, long_poll_permits) {
            Route::LongPoll(job) => {
                self.spawn_long_poll(job, reply);
                true
            }
            Route::Lane(request) => self.send(request).await,
        }
    }

    /// Cap a Fetch ([`cap_fetch`]) and decide where the request goes: a Fetch with
    /// `max_wait_ms > 0` to a long-poll task while the connection's `long_poll_permits`
    /// allow, everything else straight to its lane.
    pub(super) fn route(
        &self,
        mut request: KafkaRequest,
        long_poll_permits: &Arc<Semaphore>,
    ) -> Route {
        if let RequestBody::Fetch(fetch) = &mut request.body {
            cap_fetch(fetch, self.fetch_max_bytes());
        }
        match (self.long_poll_slot(&request, long_poll_permits), request) {
            (
                Some((permit, poll_interval)),
                KafkaRequest {
                    header,
                    body: RequestBody::Fetch(fetch),
                    response_tx,
                    decode_charge,
                },
            ) => Route::LongPoll(LongPollJob {
                long_poll: LongPoll::new(
                    header,
                    fetch,
                    self.request_lanes.main.clone(),
                    response_tx,
                ),
                permit,
                decode_charge,
                poll_interval,
            }),
            (_, request) => Route::Lane(request),
        }
    }

    /// Run a long poll in a detached task; `reply` is what its response may hold of the
    /// connection's response budget.
    pub(super) fn spawn_long_poll(&self, job: LongPollJob, reply: ReplyCharge) {
        let registry = Arc::clone(&self.registry);
        let LongPollJob {
            long_poll,
            permit,
            decode_charge,
            poll_interval,
        } = job;
        tokio::spawn(async move {
            // Hold the per-connection slot, and the decoded request's share of the
            // decoded-request budget (the task holds the request), for the task's
            // lifetime.
            let _slot = permit;
            let _held = decode_charge;
            if let Err(e) = long_poll.run(&registry, poll_interval, &reply).await {
                warn!("Long poll handler error: {e}");
            }
        });
    }

    /// Hand a request to its lane: Heartbeat to the liveness lane, everything else to the
    /// main lane, so group liveness never queues behind heavy work. Returns false when the
    /// worker channel is gone.
    pub(super) async fn send(&self, request: KafkaRequest) -> bool {
        let lane = self.request_lanes.route(&request);
        send_with_backpressure(lane, request).await
    }

    /// The current `pg_kafka.fetch_max_bytes`.
    pub(super) fn fetch_max_bytes(&self) -> i32 {
        self.runtime_context.config().fetch_max_bytes
    }

    /// A long-poll slot and the safety-net interval, when `request` is a Fetch that may
    /// wait. A connection holds at most [`MAX_LONG_POLLS_PER_CONNECTION`] long polls; a
    /// Fetch over the cap is served by the immediate path, a valid (possibly empty)
    /// response, so the degradation is latency, never an error.
    fn long_poll_slot(
        &self,
        request: &KafkaRequest,
        long_poll_permits: &Arc<Semaphore>,
    ) -> Option<(OwnedSemaphorePermit, Duration)> {
        let poll_interval = self.long_poll_interval?;
        let RequestBody::Fetch(fetch) = &request.body else {
            return None;
        };
        if fetch.max_wait_ms <= 0 {
            return None;
        }
        let Ok(permit) = Arc::clone(long_poll_permits).try_acquire_owned() else {
            debug!(
                "Per-connection long-poll cap ({MAX_LONG_POLLS_PER_CONNECTION}) reached; \
                 serving fetch immediately"
            );
            return None;
        };
        Some((permit, poll_interval))
    }
}

/// Hand a request to the DB thread without blocking the async runtime.
///
/// The request channel is a bounded crossbeam channel; a plain `Sender::send` *blocks the calling
/// thread* when the channel is full (DB thread behind), which on the tokio runtime parks a whole
/// worker thread and starves every other connection scheduled on it. Instead we `try_send` and, on a
/// full channel, `await` a short sleep and retry — yielding the worker to other tasks. This preserves
/// the bounded channel's backpressure (we never drop or reorder the request, we wait for room) while
/// keeping the runtime responsive. The normal, not-full case takes the first `try_send` with no sleep.
///
/// Returns `true` once the item is enqueued, or `false` if the channel is disconnected (DB thread
/// gone) — the caller tears down the connection in that case.
pub(super) async fn send_with_backpressure<T>(tx: &Sender<T>, mut item: T) -> bool {
    loop {
        match tx.try_send(item) {
            Ok(()) => return true,
            Err(crossbeam_channel::TrySendError::Full(returned)) => {
                item = returned;
                // Yield the worker thread instead of blocking it, then retry.
                tokio::time::sleep(FULL_CHANNEL_BACKOFF).await;
            }
            Err(crossbeam_channel::TrySendError::Disconnected(_)) => return false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fetch(max_bytes: i32, min_bytes: i32) -> FetchRequestData {
        FetchRequestData {
            max_wait_ms: 500,
            min_bytes,
            max_bytes,
            isolation_level: 0,
            topic_data: Vec::new(),
            whole_first_record: true,
        }
    }

    #[test]
    fn test_cap_fetch_applies_kafkas_fetch_max_bytes() {
        let cap = crate::config::DEFAULT_FETCH_MAX_BYTES;
        // A request that asks for everything gets at most the cap, and waits for no more.
        let mut f = fetch(i32::MAX, i32::MAX);
        cap_fetch(&mut f, cap);
        assert_eq!((f.max_bytes, f.min_bytes), (cap, cap));
        // Below the cap the request is unchanged.
        let mut f = fetch(1 << 20, 1);
        cap_fetch(&mut f, cap);
        assert_eq!((f.max_bytes, f.min_bytes), (1 << 20, 1));
        // min_bytes above max_bytes is lowered to it, as Kafka does.
        let mut f = fetch(1000, 5000);
        cap_fetch(&mut f, cap);
        assert_eq!((f.max_bytes, f.min_bytes), (1000, 1000));
    }

    #[tokio::test]
    async fn test_dispatch_caps_every_fetch() {
        // Both the immediate path and a long poll get the capped request.
        let (main, main_rx) = crossbeam_channel::bounded(4);
        let (liveness, _liveness_rx) = crossbeam_channel::bounded(4);
        let mut config = crate::config::Config::load();
        config.fetch_max_bytes = 4096;
        let dispatcher = Dispatcher {
            request_lanes: RequestLanes { main, liveness },
            registry: Arc::new(PendingFetchRegistry::new()),
            long_poll_interval: None,
            runtime_context: Arc::new(crate::kafka::RuntimeContext::new(config)),
        };
        let (response_tx, _responses) = tokio::sync::mpsc::unbounded_channel();
        let request = KafkaRequest {
            header: crate::kafka::api::RequestHeader {
                api_key: crate::kafka::constants::API_KEY_FETCH,
                api_version: 4,
                correlation_id: 1,
                client_id: None,
            },
            body: RequestBody::Fetch(fetch(i32::MAX, 1)),
            response_tx,
            decode_charge: DecodedCharge::default(),
        };
        let permits = Arc::new(Semaphore::new(MAX_LONG_POLLS_PER_CONNECTION));
        let reply = ReplyCharge::new(
            super::super::response_budget::ResponseBudget::new(1 << 20),
            0,
        );
        assert!(dispatcher.dispatch(request, &permits, reply).await);
        let RequestBody::Fetch(sent) = main_rx.try_recv().unwrap().body else {
            panic!("a Fetch");
        };
        assert_eq!(sent.max_bytes, 4096);
    }

    #[tokio::test]
    async fn test_send_with_backpressure_succeeds_once_room_frees() {
        // A full bounded channel must not make the send hang forever or drop the item — it
        // waits (yielding the runtime) until a slot frees, then enqueues the item in order.
        let (tx, rx) = crossbeam_channel::bounded::<i32>(1);
        tx.try_send(1).unwrap(); // fill to capacity

        let tx2 = tx.clone();
        let send = tokio::spawn(async move { send_with_backpressure(&tx2, 2).await });

        // Let the sender spin a few backoff iterations, then free a slot.
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(rx.recv().unwrap(), 1, "first item drains");

        let join = tokio::time::timeout(Duration::from_millis(500), send).await;
        assert!(join.is_ok(), "send task should finish, not hang");
        let sent = join.unwrap().expect("send task should not panic");
        assert!(sent, "send should report success once room frees");
        assert_eq!(
            rx.recv().unwrap(),
            2,
            "the backpressured item is enqueued in order"
        );
    }

    #[tokio::test]
    async fn test_send_with_backpressure_reports_disconnect() {
        // If the DB thread is gone (receiver dropped), the send reports failure so the caller
        // tears down the connection instead of looping forever.
        let (tx, rx) = crossbeam_channel::bounded::<i32>(1);
        drop(rx);
        assert!(
            !send_with_backpressure(&tx, 1).await,
            "a disconnected channel must report failure"
        );
    }
}
