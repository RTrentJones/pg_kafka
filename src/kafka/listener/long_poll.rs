//! Fetch long polling: re-run a Fetch until it has `min_bytes`, an error, or its
//! `max_wait_ms` deadline passes.

use crossbeam_channel::Sender;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::UnboundedSender;
use tracing::{debug, warn};

use super::dispatch::send_with_backpressure;
use crate::kafka::api::{FetchRequestData, RequestBody, RequestHeader, ResponseBody};
use crate::kafka::messages::{KafkaRequest, KafkaResponse, TopicFetchData};
use crate::kafka::pending_fetches::PendingFetchRegistry;

/// Handle a FetchRequest with long polling support
///
/// The fetch is re-run only when there is reason to expect new data:
/// 1. Register every requested topic-partition in the registry (one shared
///    `Notify`) BEFORE the first fetch attempt, and keep the registration for
///    the whole long poll. A notification that lands while an attempt is in
///    flight leaves a stored permit on the `Notify`, so there is no missed
///    wake-up window between "fetch returned too little" and "start waiting".
/// 2. Send the fetch to the worker; return it if it meets `min_bytes`, carries a
///    partition error, or the deadline has passed.
/// 3. Otherwise wait for a new-data notification (Produce for this partition,
///    or EndTxn for a `read_committed` fetch), the deadline, or the slow
///    safety-net interval `poll_interval_ms` (for a dropped notification or data
///    that became visible without one), then go to 2.
///
/// An idle long poll therefore costs one fetch at the start, one per safety-net
/// interval (default 1 s), and one at the deadline, instead of one per 100 ms.
#[allow(clippy::too_many_arguments)]
pub(super) async fn handle_fetch_long_poll(
    correlation_id: i32,
    client_id: Option<String>,
    api_version: i16,
    max_wait_ms: i32,
    min_bytes: i32,
    max_bytes: i32,
    isolation_level: i8,
    topic_data: Vec<TopicFetchData>,
    request_tx: Sender<KafkaRequest>,
    final_response_tx: UnboundedSender<KafkaResponse>,
    registry: Arc<PendingFetchRegistry>,
    poll_interval_ms: i32,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let deadline = Instant::now() + Duration::from_millis(max_wait_ms.max(0) as u64);
    let poll_interval = Duration::from_millis(poll_interval_ms.max(1) as u64);
    let read_committed = isolation_level == 1;

    // The client is gone (its connection's writer, which owns the response slot,
    // has been torn down): do nothing rather than queue work for the DB thread.
    if final_response_tx.is_closed() {
        debug!("Long poll: client disconnected, abandoning fetch");
        return Ok(());
    }

    let notify = Arc::new(tokio::sync::Notify::new());
    for topic in &topic_data {
        for partition in &topic.partitions {
            registry
                .register(
                    &topic.name,
                    partition.partition_index,
                    partition.fetch_offset,
                    notify.clone(),
                    read_committed,
                )
                .await;
        }
    }

    let result = long_poll_loop(
        correlation_id,
        client_id,
        api_version,
        max_bytes,
        min_bytes,
        isolation_level,
        &topic_data,
        &request_tx,
        &final_response_tx,
        &notify,
        deadline,
        poll_interval,
    )
    .await;

    // Always drop our registrations, whatever the outcome.
    for topic in &topic_data {
        for partition in &topic.partitions {
            registry
                .unregister(&topic.name, partition.partition_index, &notify)
                .await;
        }
    }

    result
}

/// The fetch / wait loop of [`handle_fetch_long_poll`]; the caller owns the
/// registry registration around it.
#[allow(clippy::too_many_arguments)]
async fn long_poll_loop(
    correlation_id: i32,
    client_id: Option<String>,
    api_version: i16,
    max_bytes: i32,
    min_bytes: i32,
    isolation_level: i8,
    topic_data: &[TopicFetchData],
    request_tx: &Sender<KafkaRequest>,
    final_response_tx: &UnboundedSender<KafkaResponse>,
    notify: &tokio::sync::Notify,
    deadline: Instant,
    poll_interval: Duration,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    loop {
        // Stop instead of re-fetching for a client that has disconnected.
        if final_response_tx.is_closed() {
            debug!("Long poll: client disconnected, abandoning fetch");
            return Ok(());
        }

        // Create a response channel for this fetch attempt
        let (inner_tx, mut inner_rx) = tokio::sync::mpsc::unbounded_channel();

        // Reconstruct the Fetch request with our response channel
        let fetch_request = KafkaRequest {
            header: RequestHeader {
                api_key: crate::kafka::constants::API_KEY_FETCH,
                api_version,
                correlation_id,
                client_id: client_id.clone(),
            },
            body: RequestBody::Fetch(FetchRequestData {
                max_wait_ms: 0, // Don't long poll again in worker
                min_bytes: 0,   // Accept any amount of data
                max_bytes,
                isolation_level,
                topic_data: topic_data.to_vec(),
            }),
            response_tx: inner_tx,
        };

        // Send to worker (SEC-4: non-blocking handoff)
        if !send_with_backpressure(request_tx, fetch_request).await {
            warn!("Worker channel closed during long poll");
            return Err("Worker channel closed".into());
        }

        // Wait for response
        let response = match inner_rx.recv().await {
            Some(resp) => resp,
            None => {
                warn!("Response channel closed during long poll");
                return Err("Response channel closed".into());
            }
        };

        // Check if we have enough data
        let total_bytes = calculate_response_bytes(&response);
        debug!(
            "Long poll fetch returned {} bytes (min_bytes={})",
            total_bytes, min_bytes
        );

        // A partition error (unknown partition, offset out of range, ...) is returned
        // straight away, as Kafka does, so the consumer can react instead of waiting
        // out max_wait_ms for data that will not arrive.
        if total_bytes >= min_bytes as usize
            || min_bytes <= 0
            || response_has_partition_error(&response)
        {
            debug!("Long poll: min_bytes threshold met, returning response");
            let _ = final_response_tx.send(response);
            return Ok(());
        }

        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            debug!("Long poll: timeout reached, returning response");
            let _ = final_response_tx.send(response);
            return Ok(());
        }

        // Not enough data and not timed out: wait for a wake-up. The deadline
        // wait is exact (the final re-fetch happens at max_wait_ms); the
        // safety-net interval only bounds how long a missed notification can
        // delay delivery.
        let wait_time = remaining.min(poll_interval);
        debug!(
            "Long poll: waiting up to {:?} for new-data notification (remaining: {:?})",
            wait_time, remaining
        );

        tokio::select! {
            _ = notify.notified() => {
                debug!("Long poll: woken by new-data notification");
            }
            _ = tokio::time::sleep(wait_time) => {
                debug!("Long poll: deadline or safety-net interval elapsed, re-fetching");
            }
            _ = final_response_tx.closed() => {
                debug!("Long poll: client disconnected while waiting");
            }
        }
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
            .map(|r| r.len())
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
    use bytes::Bytes;
    use std::sync::atomic::{AtomicUsize, Ordering};
    #[test]
    fn test_long_poll_stops_when_client_is_gone() {
        // A long poll whose client has disconnected (response slot closed) must return
        // straight away instead of re-fetching on the DB thread until max_wait_ms.
        let (request_tx, request_rx) = crossbeam_channel::bounded::<KafkaRequest>(16);
        let (final_tx, final_rx) = tokio::sync::mpsc::unbounded_channel::<KafkaResponse>();
        drop(final_rx);
        let topic = TopicFetchData {
            name: "t".to_string(),
            partitions: vec![crate::kafka::messages::PartitionFetchData {
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
        rt.block_on(handle_fetch_long_poll(
            1,
            None,
            4,
            i32::MAX,
            i32::MAX,
            1024,
            0,
            vec![topic],
            request_tx,
            final_tx,
            Arc::new(PendingFetchRegistry::new()),
            100,
        ))
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
            partitions: vec![crate::kafka::messages::PartitionFetchData {
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
                        tokio::time::sleep(Duration::from_millis(1)).await
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
        handle_fetch_long_poll(
            1,
            None,
            4,
            600,
            1,
            1024,
            0,
            vec![one_partition_topic("t")],
            request_tx,
            final_tx,
            registry.clone(),
            10_000,
        )
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
        handle_fetch_long_poll(
            1,
            None,
            4,
            10_000,
            1,
            1024,
            0,
            vec![one_partition_topic("t")],
            request_tx,
            final_tx,
            Arc::new(PendingFetchRegistry::new()),
            200,
        )
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
        handle_fetch_long_poll(
            1,
            None,
            4,
            10_000,
            1,
            1024,
            0,
            vec![one_partition_topic("t")],
            request_tx,
            final_tx,
            registry.clone(),
            10_000,
        )
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
        handle_fetch_long_poll(
            1,
            None,
            4,
            10_000,
            1,
            1024,
            1, // read_committed
            vec![one_partition_topic("t")],
            request_tx,
            final_tx,
            registry,
            10_000,
        )
        .await
        .unwrap();

        assert!(started.elapsed() < Duration::from_secs(2));
        assert_eq!(served.load(Ordering::SeqCst), 2);
        assert!(calculate_response_bytes(&final_rx.try_recv().unwrap()) > 0);
    }
}
