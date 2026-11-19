//! Fetch long polling: re-run a Fetch until it has `min_bytes`, a partition error, or
//! its `max_wait_ms` deadline passes.
//!
//! The fetch is re-run only when there is reason to expect new data:
//! 1. Every requested topic-partition is registered in the [`PendingFetchRegistry`] (one
//!    shared `Notify`) BEFORE the first fetch attempt, and stays registered for the
//!    whole long poll. A notification that lands while an attempt is in flight leaves a
//!    stored permit on the `Notify`, so there is no missed wake-up window between "fetch
//!    returned too little" and "start waiting".
//! 2. The fetch goes to the worker; its response is returned if it meets `min_bytes`,
//!    carries a partition error, or the deadline has passed.
//! 3. Otherwise the long poll waits for a new-data notification (Produce for this
//!    partition, or EndTxn for a `read_committed` fetch), the deadline, or the slow
//!    safety-net interval (`pg_kafka.fetch_poll_interval_ms`, for a dropped notification
//!    or data that became visible without one), then goes to 2.
//!
//! An idle long poll therefore costs one fetch at the start, one per safety-net interval
//! and one at the deadline.

use bytes::Bytes;
use crossbeam_channel::Sender;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::Notify;
use tracing::{debug, warn};

use super::dispatch::send_with_backpressure;
use crate::kafka::api::{FetchRequestData, RequestBody, RequestHeader, ResponseBody};
use crate::kafka::constants::API_KEY_FETCH;
use crate::kafka::messages::{KafkaRequest, KafkaResponse};
use crate::kafka::pending_fetches::PendingFetchRegistry;
use crate::kafka::storage::IsolationLevel;

/// The safety-net interval for a `pg_kafka.fetch_poll_interval_ms` value (at least 1 ms).
pub(super) fn poll_interval(fetch_poll_interval_ms: i32) -> Duration {
    Duration::from_millis(u64::try_from(fetch_poll_interval_ms).unwrap_or(0).max(1))
}

/// Why a long poll ended without answering its client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum LongPollError {
    /// The worker's request channel is closed.
    WorkerGone,
    /// The worker dropped a fetch attempt without answering it.
    NoResponse,
}

impl std::fmt::Display for LongPollError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            LongPollError::WorkerGone => "Worker channel closed",
            LongPollError::NoResponse => "Response channel closed",
        })
    }
}

impl std::error::Error for LongPollError {}

/// One Fetch request that waits for data.
pub(super) struct LongPoll {
    header: RequestHeader,
    fetch: FetchRequestData,
    /// The worker's main lane.
    request_tx: Sender<KafkaRequest>,
    /// The client's response slot.
    response_tx: UnboundedSender<KafkaResponse>,
}

impl LongPoll {
    /// The long poll of one Fetch request; `request_tx` is the worker's main lane.
    pub(super) fn new(
        header: RequestHeader,
        fetch: FetchRequestData,
        request_tx: Sender<KafkaRequest>,
        response_tx: UnboundedSender<KafkaResponse>,
    ) -> Self {
        debug!(
            "Fetch request with long polling: max_wait_ms={}, min_bytes={}, isolation_level={}",
            fetch.max_wait_ms, fetch.min_bytes, fetch.isolation_level
        );
        LongPoll {
            header,
            fetch,
            request_tx,
            response_tx,
        }
    }

    /// Answer the client: register for wake-ups, fetch until done, unregister.
    pub(super) async fn run(
        self,
        registry: &PendingFetchRegistry,
        poll_interval: Duration,
    ) -> Result<(), LongPollError> {
        // The client is gone (its connection's writer, which owns the response slot,
        // has been torn down): do nothing rather than queue work for the DB thread.
        if self.response_tx.is_closed() {
            debug!("Long poll: client disconnected, abandoning fetch");
            return Ok(());
        }
        let max_wait = u64::try_from(self.fetch.max_wait_ms).unwrap_or(0);
        let deadline = Instant::now() + Duration::from_millis(max_wait);
        let read_committed =
            IsolationLevel::from_i8(self.fetch.isolation_level) == IsolationLevel::ReadCommitted;

        let notify = std::sync::Arc::new(Notify::new());
        for (topic, partition) in self.partitions() {
            registry
                .register(
                    topic,
                    partition.partition_index,
                    partition.fetch_offset,
                    notify.clone(),
                    read_committed,
                )
                .await;
        }

        let result = self
            .fetch_until_done(&notify, deadline, poll_interval)
            .await;

        // Always drop the registrations, whatever the outcome.
        for (topic, partition) in self.partitions() {
            registry
                .unregister(topic, partition.partition_index, &notify)
                .await;
        }
        result
    }

    /// Every requested `(topic name, partition)`.
    fn partitions(
        &self,
    ) -> impl Iterator<Item = (&str, &crate::kafka::messages::PartitionFetchData)> {
        self.fetch
            .topic_data
            .iter()
            .flat_map(|t| t.partitions.iter().map(move |p| (t.name.as_str(), p)))
    }

    /// The fetch / wait loop; [`run`](Self::run) owns the registration around it.
    async fn fetch_until_done(
        &self,
        notify: &Notify,
        deadline: Instant,
        poll_interval: Duration,
    ) -> Result<(), LongPollError> {
        loop {
            // Stop instead of re-fetching for a client that has disconnected.
            if self.response_tx.is_closed() {
                debug!("Long poll: client disconnected, abandoning fetch");
                return Ok(());
            }
            let response = self.fetch_once().await?;

            let total_bytes = calculate_response_bytes(&response);
            let min_bytes = self.fetch.min_bytes;
            debug!("Long poll fetch returned {total_bytes} bytes (min_bytes={min_bytes})");
            // A partition error (unknown partition, offset out of range, ...) is returned
            // straight away, as Kafka does, so the consumer can react instead of waiting
            // out max_wait_ms for data that will not arrive. A non-positive min_bytes is
            // met by any response.
            let enough =
                min_bytes <= 0 || usize::try_from(min_bytes).is_ok_and(|min| total_bytes >= min);
            if enough || response_has_partition_error(&response) {
                debug!("Long poll: min_bytes threshold met, returning response");
                let _ = self.response_tx.send(response);
                return Ok(());
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                debug!("Long poll: timeout reached, returning response");
                let _ = self.response_tx.send(response);
                return Ok(());
            }

            // Not enough data and not timed out: wait for a wake-up. The deadline wait is
            // exact (the final re-fetch happens at max_wait_ms); the safety-net interval
            // only bounds how long a missed notification can delay delivery.
            let wait_time = remaining.min(poll_interval);
            debug!(
                "Long poll: waiting up to {wait_time:?} for new-data notification (remaining: {remaining:?})"
            );
            tokio::select! {
                () = notify.notified() => {
                    debug!("Long poll: woken by new-data notification");
                }
                () = tokio::time::sleep(wait_time) => {
                    debug!("Long poll: deadline or safety-net interval elapsed, re-fetching");
                }
                () = self.response_tx.closed() => {
                    debug!("Long poll: client disconnected while waiting");
                }
            }
        }
    }

    /// One fetch attempt on the worker, without waiting (`max_wait_ms = 0`) and accepting
    /// any amount of data (`min_bytes = 0`).
    async fn fetch_once(&self) -> Result<KafkaResponse, LongPollError> {
        let (attempt_tx, mut attempt_rx) = tokio::sync::mpsc::unbounded_channel();
        let request = KafkaRequest {
            header: RequestHeader {
                api_key: API_KEY_FETCH,
                ..self.header.clone()
            },
            body: RequestBody::Fetch(FetchRequestData {
                max_wait_ms: 0,
                min_bytes: 0,
                ..self.fetch.clone()
            }),
            response_tx: attempt_tx,
        };
        if !send_with_backpressure(&self.request_tx, request).await {
            warn!("Worker channel closed during long poll");
            return Err(LongPollError::WorkerGone);
        }
        attempt_rx.recv().await.ok_or_else(|| {
            warn!("Response channel closed during long poll");
            LongPollError::NoResponse
        })
    }
}

/// Calculate the total bytes in a FetchResponse
fn calculate_response_bytes(response: &KafkaResponse) -> usize {
    match &response.body {
        ResponseBody::Fetch(response) => response
            .responses
            .iter()
            .flat_map(|t| t.partitions.iter())
            .filter_map(|p| p.records.as_ref())
            .map(Bytes::len)
            .sum(),
        _ => 0,
    }
}

/// Whether any partition in a Fetch response carries a non-zero error code.
fn response_has_partition_error(response: &KafkaResponse) -> bool {
    match &response.body {
        ResponseBody::Fetch(response) => response
            .responses
            .iter()
            .flat_map(|t| t.partitions.iter())
            .any(|p| p.error_code != 0),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::messages::{PartitionFetchData, TopicFetchData};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    /// A Fetch v4 long poll (correlation id 1, `max_bytes` 1024) of `topics`.
    fn long_poll(
        max_wait_ms: i32,
        min_bytes: i32,
        isolation_level: i8,
        topics: Vec<TopicFetchData>,
        request_tx: Sender<KafkaRequest>,
        response_tx: UnboundedSender<KafkaResponse>,
    ) -> LongPoll {
        LongPoll {
            header: RequestHeader {
                api_key: API_KEY_FETCH,
                api_version: 4,
                correlation_id: 1,
                client_id: None,
            },
            fetch: FetchRequestData {
                max_wait_ms,
                min_bytes,
                max_bytes: 1024,
                isolation_level,
                topic_data: topics,
            },
            request_tx,
            response_tx,
        }
    }

    #[test]
    fn test_long_poll_stops_when_client_is_gone() {
        // A long poll whose client has disconnected (response slot closed) must return
        // straight away instead of re-fetching on the DB thread until max_wait_ms.
        let (request_tx, request_rx) = crossbeam_channel::bounded::<KafkaRequest>(16);
        let (final_tx, final_rx) = tokio::sync::mpsc::unbounded_channel::<KafkaResponse>();
        drop(final_rx);
        let topic = TopicFetchData {
            name: "t".to_string(),
            partitions: vec![PartitionFetchData {
                partition_index: 0,
                fetch_offset: 0,
                partition_max_bytes: 1024,
            }],
        };
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        let started = Instant::now();
        rt.block_on(
            long_poll(i32::MAX, i32::MAX, 0, vec![topic], request_tx, final_tx)
                .run(&PendingFetchRegistry::new(), Duration::from_millis(100)),
        )
        .unwrap();
        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(
            request_rx.try_recv().is_err(),
            "no fetch may reach the worker for a disconnected client"
        );
    }

    fn one_partition_topic(name: &str) -> TopicFetchData {
        TopicFetchData {
            name: name.to_string(),
            partitions: vec![PartitionFetchData {
                partition_index: 0,
                fetch_offset: 0,
                partition_max_bytes: 1024,
            }],
        }
    }

    fn fetch_response(with_records: bool) -> KafkaResponse {
        use kafka_protocol::messages::fetch_response::{
            FetchResponse, FetchableTopicResponse, PartitionData,
        };
        let mut response = FetchResponse::default();
        if with_records {
            let mut partition = PartitionData::default();
            partition.records = Some(Bytes::from_static(b"some-record-bytes"));
            let mut topic = FetchableTopicResponse::default();
            topic.partitions = vec![partition];
            response.responses = vec![topic];
        }
        KafkaResponse::new(1, 4, ResponseBody::Fetch(response))
    }

    /// Fake DB thread: answers every Fetch it receives. `before_answer(n)` runs before
    /// answering the n-th fetch (1-based) and returns whether that answer carries records.
    /// Returns a counter of fetches served.
    fn spawn_fake_fetch_worker<F, Fut>(
        rx: crossbeam_channel::Receiver<KafkaRequest>,
        before_answer: F,
    ) -> Arc<AtomicUsize>
    where
        F: Fn(usize) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = bool> + Send,
    {
        let served = Arc::new(AtomicUsize::new(0));
        let served_out = served.clone();
        tokio::spawn(async move {
            loop {
                match rx.try_recv() {
                    Ok(KafkaRequest {
                        body: RequestBody::Fetch(_),
                        response_tx,
                        ..
                    }) => {
                        let n = served.fetch_add(1, Ordering::SeqCst) + 1;
                        let with_records = before_answer(n).await;
                        let _ = response_tx.send(fetch_response(with_records));
                    }
                    Ok(_) => {}
                    Err(crossbeam_channel::TryRecvError::Empty) => {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                    Err(crossbeam_channel::TryRecvError::Disconnected) => return,
                }
            }
        });
        served_out
    }

    #[tokio::test]
    async fn test_idle_long_poll_does_not_refetch_until_deadline() {
        // With no notification and a safety-net interval longer than max_wait, an idle long
        // poll runs exactly two fetches: the initial attempt and the one at the deadline. The
        // old loop re-fetched every fetch_poll_interval_ms (100 ms → ~7 fetches here).
        let (request_tx, request_rx) = crossbeam_channel::bounded::<KafkaRequest>(16);
        let served = spawn_fake_fetch_worker(request_rx, |_| async { false });
        let (final_tx, mut final_rx) = tokio::sync::mpsc::unbounded_channel();
        let registry = Arc::new(PendingFetchRegistry::new());

        let started = Instant::now();
        long_poll(
            600,
            1,
            0,
            vec![one_partition_topic("t")],
            request_tx,
            final_tx,
        )
        .run(&registry, Duration::from_millis(10_000))
        .await
        .unwrap();

        assert!(started.elapsed() >= Duration::from_millis(600));
        assert!(final_rx.try_recv().is_ok(), "the deadline response is sent");
        assert_eq!(served.load(Ordering::SeqCst), 2);
        assert!(registry.is_empty().await, "registrations are removed");
    }

    #[tokio::test]
    async fn test_long_poll_safety_net_interval_still_refetches() {
        // Data that becomes visible without a notification is picked up at the safety-net
        // interval, well before max_wait_ms.
        let (request_tx, request_rx) = crossbeam_channel::bounded::<KafkaRequest>(16);
        let served = spawn_fake_fetch_worker(request_rx, |n| async move { n >= 2 });
        let (final_tx, mut final_rx) = tokio::sync::mpsc::unbounded_channel();

        let started = Instant::now();
        long_poll(
            10_000,
            1,
            0,
            vec![one_partition_topic("t")],
            request_tx,
            final_tx,
        )
        .run(&PendingFetchRegistry::new(), Duration::from_millis(200))
        .await
        .unwrap();

        let elapsed = started.elapsed();
        assert!(elapsed >= Duration::from_millis(200), "waited {elapsed:?}");
        assert!(elapsed < Duration::from_secs(3), "waited {elapsed:?}");
        assert_eq!(served.load(Ordering::SeqCst), 2);
        assert!(calculate_response_bytes(&final_rx.try_recv().unwrap()) > 0);
    }

    #[tokio::test]
    async fn test_notification_during_fetch_attempt_is_not_missed() {
        // Missed-wakeup window: the produce notification lands while the first fetch attempt is
        // still on the DB thread (it returned "no data"). The long poll registered before that
        // attempt, so the wake-up is kept and the record is returned at once — not after the
        // safety-net interval (10 s here) or max_wait_ms (10 s).
        let (request_tx, request_rx) = crossbeam_channel::bounded::<KafkaRequest>(16);
        let registry = Arc::new(PendingFetchRegistry::new());
        let reg = registry.clone();
        let served = spawn_fake_fetch_worker(request_rx, move |n| {
            let reg = reg.clone();
            async move {
                if n == 1 {
                    reg.notify_new_data("t", 0, 1).await;
                    false
                } else {
                    true
                }
            }
        });
        let (final_tx, mut final_rx) = tokio::sync::mpsc::unbounded_channel();

        let started = Instant::now();
        long_poll(
            10_000,
            1,
            0,
            vec![one_partition_topic("t")],
            request_tx,
            final_tx,
        )
        .run(&registry, Duration::from_millis(10_000))
        .await
        .unwrap();

        assert!(started.elapsed() < Duration::from_secs(2));
        assert_eq!(served.load(Ordering::SeqCst), 2);
        assert!(calculate_response_bytes(&final_rx.try_recv().unwrap()) > 0);
        assert!(registry.is_empty().await);
    }

    #[tokio::test]
    async fn test_read_committed_long_poll_wakes_on_transaction_end() {
        // EndTxn → TransactionEnded wakes a read_committed long poll promptly.
        let (request_tx, request_rx) = crossbeam_channel::bounded::<KafkaRequest>(16);
        let registry = Arc::new(PendingFetchRegistry::new());
        let reg = registry.clone();
        let served = spawn_fake_fetch_worker(request_rx, move |n| {
            let reg = reg.clone();
            async move {
                if n == 1 {
                    // Simulate the commit landing (after the first attempt saw nothing).
                    let reg = reg.clone();
                    tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_millis(150)).await;
                        reg.notify_transaction_ended().await;
                    });
                    false
                } else {
                    true
                }
            }
        });
        let (final_tx, mut final_rx) = tokio::sync::mpsc::unbounded_channel();

        let started = Instant::now();
        long_poll(
            10_000,
            1,
            1,
            vec![one_partition_topic("t")],
            request_tx,
            final_tx,
        )
        .run(&registry, Duration::from_millis(10_000))
        .await
        .unwrap();

        assert!(started.elapsed() < Duration::from_secs(2));
        assert_eq!(served.load(Ordering::SeqCst), 2);
        assert!(calculate_response_bytes(&final_rx.try_recv().unwrap()) > 0);
    }
}
