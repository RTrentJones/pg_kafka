//! Handing parsed requests to the DB thread.

use crossbeam_channel::Sender;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::{debug, warn};

use super::long_poll::LongPoll;
use crate::kafka::api::RequestBody;
use crate::kafka::constants::MAX_LONG_POLLS_PER_CONNECTION;
use crate::kafka::messages::{KafkaRequest, RequestLanes};
use crate::kafka::pending_fetches::PendingFetchRegistry;

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
}

impl Dispatcher {
    /// Hand one parsed request to the DB thread: a Fetch with `max_wait_ms > 0` goes to a
    /// long-poll task while the connection's `long_poll_permits` allow, everything else
    /// straight to its lane. Returns false when the worker channel is gone.
    pub(super) async fn dispatch(
        &self,
        request: KafkaRequest,
        long_poll_permits: &Arc<Semaphore>,
    ) -> bool {
        let request = match (self.long_poll_slot(&request, long_poll_permits), request) {
            (
                Some((permit, poll_interval)),
                KafkaRequest {
                    header,
                    body: RequestBody::Fetch(fetch),
                    response_tx,
                },
            ) => {
                let long_poll =
                    LongPoll::new(header, fetch, self.request_lanes.main.clone(), response_tx);
                let registry = Arc::clone(&self.registry);
                tokio::spawn(async move {
                    // Hold the per-connection slot for the task's lifetime.
                    let _slot = permit;
                    if let Err(e) = long_poll.run(&registry, poll_interval).await {
                        warn!("Long poll handler error: {e}");
                    }
                });
                return true;
            }
            (_, request) => request,
        };
        // Heartbeat goes to the liveness lane, everything else to the main lane, so
        // group liveness never queues behind heavy work.
        let lane = self.request_lanes.route(&request);
        send_with_backpressure(lane, request).await
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
