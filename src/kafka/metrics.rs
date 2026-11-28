//! Broker request metrics (per-API counters, latency, connections, queue depth).
//!
//! `kafka.metrics` (bootstrap.sql) is a snapshot of row counts: it cannot show request rates,
//! latencies, error counts, open connections or the request-queue depth, because those live in
//! the background worker's memory and SQL backends are separate processes. This module keeps
//! them in lock-free atomics and publishes them to two UNLOGGED tables
//! (`kafka.broker_api_stats`, `kafka.broker_gauges`) from the worker's main loop, read through
//! the views `kafka.api_metrics`, `kafka.broker_metrics` and `kafka.consumer_lag`.
//!
//! ## Who records what
//!
//! - The **DB thread** records one observation per executed request ([`RequestTimer`]): count,
//!   whether the response carried any non-zero error code (or the request failed outright),
//!   and the service time on the DB thread (handler + commit; queue wait is not included). A
//!   long-poll Fetch that is re-submitted counts each execution.
//! - The **network thread** tracks connections ([`ConnectionGuard`], [`connection_rejected`])
//!   and records the APIs it answers itself (SaslHandshake/SaslAuthenticate, `listener::sasl`)
//!   with the same [`RequestTimer`].
//! - The flush samples the request-lane depths at flush time.
//!
//! Everything here except [`write_batch`] is pure Rust (atomics, no pgrx), so it is safe on
//! both threads and unit-testable without Postgres.
//!
//! ## Publishing
//!
//! [`MetricsFlusher::poll`] runs on the DB thread every loop iteration. At most once per
//! [`FLUSH_INTERVAL`] it snapshots the counters and, only if something changed since the last
//! successful flush, returns a [`FlushBatch`] that [`write_batch`] writes with ONE statement
//! (a writable CTE upserting both tables from `unnest()`ed arrays). The first flush after
//! worker start writes every known API (zeros included) so a restart resets the published
//! values left by the previous worker, and so does the first one after the worker finds the
//! schema again after DROP/CREATE EXTENSION ([`MetricsFlusher::forget_published`]: the new
//! tables start empty); later flushes write only the APIs whose counters moved.
//! Counters are cumulative since worker start.

use crate::kafka::messages::KafkaResponse;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Number of API-key slots. Every API pg_kafka implements has a key below this.
pub const API_SLOTS: usize = 64;

/// How often the worker publishes metrics (when they changed).
pub const FLUSH_INTERVAL: Duration = Duration::from_secs(1);

/// Display name for an API key (`"Unknown"` for a key pg_kafka does not implement). The
/// APIs and their names come from the API registry (`crate::kafka::api::APIS`); the first
/// flush after a worker start writes a row for each, so stale values from a previous worker
/// are reset.
fn api_name(api_key: i16) -> &'static str {
    crate::kafka::api::api_name(api_key)
}

fn slot(api_key: i16) -> Option<usize> {
    usize::try_from(api_key).ok().filter(|i| *i < API_SLOTS)
}

/// Per-API atomic counters.
struct ApiCounters {
    requests: AtomicU64,
    errors: AtomicU64,
    latency_us_sum: AtomicU64,
    latency_us_max: AtomicU64,
}

impl ApiCounters {
    #[expect(
        clippy::declare_interior_mutable_const,
        reason = "only used as an array-repeat initializer, so each slot gets its own atomics"
    )]
    const NEW: ApiCounters = ApiCounters {
        requests: AtomicU64::new(0),
        errors: AtomicU64::new(0),
        latency_us_sum: AtomicU64::new(0),
        latency_us_max: AtomicU64::new(0),
    };
}

/// The broker's in-memory metrics. One global instance ([`global`]) is shared by the network
/// and DB threads; tests construct their own.
pub struct BrokerMetrics {
    apis: [ApiCounters; API_SLOTS],
    connections_open: AtomicU64,
    connections_accepted: AtomicU64,
    connections_rejected: AtomicU64,
}

impl Default for BrokerMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl BrokerMetrics {
    /// A zeroed metrics registry.
    pub const fn new() -> Self {
        BrokerMetrics {
            apis: [ApiCounters::NEW; API_SLOTS],
            connections_open: AtomicU64::new(0),
            connections_accepted: AtomicU64::new(0),
            connections_rejected: AtomicU64::new(0),
        }
    }

    /// Record one executed request. Keys outside `0..API_SLOTS` are ignored.
    pub fn record_request(&self, api_key: i16, latency: Duration, error: bool) {
        let Some(i) = slot(api_key) else { return };
        let c = &self.apis[i];
        let us = u64::try_from(latency.as_micros()).unwrap_or(u64::MAX);
        c.requests.fetch_add(1, Ordering::Relaxed);
        if error {
            c.errors.fetch_add(1, Ordering::Relaxed);
        }
        c.latency_us_sum.fetch_add(us, Ordering::Relaxed);
        c.latency_us_max.fetch_max(us, Ordering::Relaxed);
    }

    /// A connection was accepted; returns a guard that marks it closed when dropped.
    pub fn connection_opened(&self) -> ConnectionGuard<'_> {
        self.connections_accepted.fetch_add(1, Ordering::Relaxed);
        self.connections_open.fetch_add(1, Ordering::Relaxed);
        ConnectionGuard { metrics: self }
    }

    /// A connection was refused at the concurrent-connection cap.
    pub fn connection_rejected(&self) {
        self.connections_rejected.fetch_add(1, Ordering::Relaxed);
    }

    /// Read every counter. Each value is read atomically; the set is not one atomic snapshot,
    /// which is fine for monitoring (a concurrent request shows up in the next flush).
    pub fn snapshot(&self, request_queue_depth: usize, liveness_queue_depth: usize) -> Snapshot {
        let mut apis = [ApiStats::default(); API_SLOTS];
        for (out, c) in apis.iter_mut().zip(self.apis.iter()) {
            *out = ApiStats {
                requests: c.requests.load(Ordering::Relaxed),
                errors: c.errors.load(Ordering::Relaxed),
                latency_us_sum: c.latency_us_sum.load(Ordering::Relaxed),
                latency_us_max: c.latency_us_max.load(Ordering::Relaxed),
            };
        }
        Snapshot {
            apis,
            gauges: Gauges {
                connections_open: self.connections_open.load(Ordering::Relaxed),
                connections_accepted: self.connections_accepted.load(Ordering::Relaxed),
                connections_rejected: self.connections_rejected.load(Ordering::Relaxed),
                request_queue_depth: request_queue_depth as u64,
                liveness_queue_depth: liveness_queue_depth as u64,
            },
        }
    }
}

static GLOBAL: BrokerMetrics = BrokerMetrics::new();

/// The process-wide metrics registry.
pub fn global() -> &'static BrokerMetrics {
    &GLOBAL
}

/// Held by a connection task for its lifetime; decrements the open-connection gauge on drop.
pub struct ConnectionGuard<'a> {
    metrics: &'a BrokerMetrics,
}

impl ConnectionGuard<'static> {
    /// Hook for the listener: call once per accepted connection and hold the guard.
    pub fn open() -> Self {
        global().connection_opened()
    }
}

impl Drop for ConnectionGuard<'_> {
    fn drop(&mut self) {
        self.metrics
            .connections_open
            .fetch_sub(1, Ordering::Relaxed);
    }
}

/// Hook for the listener: a connection was refused at the connection cap.
pub fn connection_rejected() {
    global().connection_rejected();
}

/// Times one request on the DB thread and records it into the global registry when dropped
/// (including when dropped by an unwind, so a failed commit is still counted).
pub struct RequestTimer<'a> {
    metrics: &'a BrokerMetrics,
    api_key: i16,
    start: Instant,
    error: bool,
}

impl RequestTimer<'static> {
    /// Hook for the worker: start timing a request for `api_key`.
    pub fn start(api_key: i16) -> Self {
        RequestTimer::start_in(global(), api_key)
    }
}

impl<'a> RequestTimer<'a> {
    /// Start timing against a specific registry (tests).
    fn start_in(metrics: &'a BrokerMetrics, api_key: i16) -> Self {
        RequestTimer {
            metrics,
            api_key,
            start: Instant::now(),
            error: false,
        }
    }

    /// Inspect a response about to be released to the client; any non-zero error code in it
    /// marks the request as an error.
    pub fn observe(&mut self, response: &KafkaResponse) {
        if response_has_error(response) {
            self.error = true;
        }
    }

    /// Mark the request failed (e.g. the transaction failed to commit).
    pub fn mark_error(&mut self) {
        self.error = true;
    }
}

impl Drop for RequestTimer<'_> {
    fn drop(&mut self) {
        self.metrics
            .record_request(self.api_key, self.start.elapsed(), self.error);
    }
}

/// Whether a response carries a non-zero error code anywhere (top level, or on any topic,
/// partition, group, member or resource entry); see `KafkaApi::response_has_error`.
fn response_has_error(response: &KafkaResponse) -> bool {
    response.body.has_error()
}

/// Counters for one API at one instant.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ApiStats {
    pub requests: u64,
    pub errors: u64,
    pub latency_us_sum: u64,
    pub latency_us_max: u64,
}

/// Broker-wide gauges and counters at one instant.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Gauges {
    pub connections_open: u64,
    pub connections_accepted: u64,
    pub connections_rejected: u64,
    pub request_queue_depth: u64,
    pub liveness_queue_depth: u64,
}

impl Gauges {
    /// `(name, value)` rows as published to `kafka.broker_gauges`.
    pub fn rows(&self) -> [(&'static str, u64); 5] {
        [
            ("connections_open", self.connections_open),
            ("connections_accepted_total", self.connections_accepted),
            ("connections_rejected_total", self.connections_rejected),
            ("request_queue_depth", self.request_queue_depth),
            ("liveness_queue_depth", self.liveness_queue_depth),
        ]
    }
}

/// Every counter at one instant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    pub apis: [ApiStats; API_SLOTS],
    pub gauges: Gauges,
}

/// One API row to upsert into `kafka.broker_api_stats`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiRow {
    pub api_key: i16,
    pub api_name: &'static str,
    pub stats: ApiStats,
}

/// What one flush writes: the changed API rows and all gauges, plus the snapshot to remember
/// once the write committed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlushBatch {
    pub api_rows: Vec<ApiRow>,
    pub snapshot: Snapshot,
}

fn to_i64(v: u64) -> i64 {
    i64::try_from(v).unwrap_or(i64::MAX)
}

impl FlushBatch {
    /// Column arrays for the `unnest()` in [`FLUSH_SQL`], in parameter order
    /// ($1 api_key, $2 api_name, $3 requests, $4 errors, $5 latency sum, $6 latency max).
    #[expect(
        clippy::type_complexity,
        reason = "one Vec per SQL parameter, in parameter order"
    )]
    pub fn api_columns(
        &self,
    ) -> (
        Vec<i16>,
        Vec<&'static str>,
        Vec<i64>,
        Vec<i64>,
        Vec<i64>,
        Vec<i64>,
    ) {
        let rows = &self.api_rows;
        (
            rows.iter().map(|r| r.api_key).collect(),
            rows.iter().map(|r| r.api_name).collect(),
            rows.iter().map(|r| to_i64(r.stats.requests)).collect(),
            rows.iter().map(|r| to_i64(r.stats.errors)).collect(),
            rows.iter()
                .map(|r| to_i64(r.stats.latency_us_sum))
                .collect(),
            rows.iter()
                .map(|r| to_i64(r.stats.latency_us_max))
                .collect(),
        )
    }

    /// Gauge arrays for the `unnest()` in [`FLUSH_SQL`] ($7 name, $8 value).
    pub fn gauge_columns(&self) -> (Vec<&'static str>, Vec<i64>) {
        self.snapshot
            .gauges
            .rows()
            .iter()
            .map(|(n, v)| (*n, to_i64(*v)))
            .unzip()
    }
}

/// The single statement each flush runs: upsert the API rows and the gauges together.
/// A data-modifying CTE runs whether or not the outer query references it.
pub const FLUSH_SQL: &str = "WITH api AS (
    INSERT INTO kafka.broker_api_stats
        (api_key, api_name, requests, errors, latency_us_sum, latency_us_max, updated_at)
    SELECT k, n, r, e, s, m, now()
    FROM unnest($1::int2[], $2::text[], $3::int8[], $4::int8[], $5::int8[], $6::int8[])
         AS a(k, n, r, e, s, m)
    ON CONFLICT (api_key) DO UPDATE SET
        api_name = EXCLUDED.api_name,
        requests = EXCLUDED.requests,
        errors = EXCLUDED.errors,
        latency_us_sum = EXCLUDED.latency_us_sum,
        latency_us_max = EXCLUDED.latency_us_max,
        updated_at = EXCLUDED.updated_at
)
INSERT INTO kafka.broker_gauges (name, value, updated_at)
SELECT n, v, now() FROM unnest($7::text[], $8::int8[]) AS g(n, v)
ON CONFLICT (name) DO UPDATE SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at";

/// Decides when to publish and what. Lives on the DB thread.
pub struct MetricsFlusher<'a> {
    metrics: &'a BrokerMetrics,
    interval: Duration,
    last_poll: Option<Instant>,
    /// The last snapshot that was committed to the tables; `None` before the first flush.
    published: Option<Snapshot>,
    /// Whether the previous write failed (log only the first failure of a run).
    failing: bool,
}

impl MetricsFlusher<'static> {
    /// Flusher over the global registry with the default interval.
    pub fn new() -> Self {
        MetricsFlusher::with_registry(global(), FLUSH_INTERVAL)
    }
}

impl Default for MetricsFlusher<'static> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> MetricsFlusher<'a> {
    fn with_registry(metrics: &'a BrokerMetrics, interval: Duration) -> Self {
        MetricsFlusher {
            metrics,
            interval,
            last_poll: None,
            published: None,
            failing: false,
        }
    }

    /// Hook for the worker loop: returns a batch to write when the interval has elapsed AND
    /// something changed since the last committed flush; `None` otherwise (no SQL runs).
    pub fn poll(
        &mut self,
        request_queue_depth: usize,
        liveness_queue_depth: usize,
    ) -> Option<FlushBatch> {
        self.poll_at(Instant::now(), request_queue_depth, liveness_queue_depth)
    }

    /// [`poll`](Self::poll) with an explicit clock (tests).
    fn poll_at(
        &mut self,
        now: Instant,
        request_queue_depth: usize,
        liveness_queue_depth: usize,
    ) -> Option<FlushBatch> {
        if let Some(last) = self.last_poll {
            if now.duration_since(last) < self.interval {
                return None;
            }
        }
        self.last_poll = Some(now);
        let snapshot = self
            .metrics
            .snapshot(request_queue_depth, liveness_queue_depth);
        build_batch(self.published.as_ref(), snapshot)
    }

    /// Forget the published baseline: the next flush writes every known API, as the first
    /// one after worker start does. For a schema found again after DROP/CREATE EXTENSION,
    /// whose broker tables start empty: a flush of only the changed slots would leave out
    /// every API whose counters did not move since the drop.
    pub fn forget_published(&mut self) {
        self.published = None;
    }

    /// Hook for the worker loop: report the outcome of writing `batch`. On success the batch's
    /// snapshot becomes the published baseline; on failure it is not, so the next poll retries.
    /// Returns a message to log for the first failure in a run of failures.
    pub fn finish(&mut self, batch: FlushBatch, result: Result<(), String>) -> Option<String> {
        match result {
            Ok(()) => {
                self.published = Some(batch.snapshot);
                self.failing = false;
                None
            }
            Err(e) => {
                let first = !self.failing;
                self.failing = true;
                first.then(|| {
                    format!(
                        "Failed to publish broker metrics (kafka.broker_api_stats / \
                         kafka.broker_gauges); retrying each interval, further failures not logged: {e}"
                    )
                })
            }
        }
    }
}

/// Diff `snapshot` against what is published. First flush: every known API plus any other
/// non-zero slot. Later: only slots whose counters moved; `None` if nothing at all changed.
fn build_batch(published: Option<&Snapshot>, snapshot: Snapshot) -> Option<FlushBatch> {
    // Slot `i` holds API key `i` (API_SLOTS is far below i16::MAX).
    let api_rows: Vec<ApiRow> = (0_i16..)
        .zip(snapshot.apis.iter().enumerate())
        .filter(|&(key, (i, stats))| match published {
            None => *stats != ApiStats::default() || crate::kafka::api::api_info(key).is_some(),
            Some(prev) => prev.apis[i] != *stats,
        })
        .map(|(key, (_, stats))| ApiRow {
            api_key: key,
            api_name: api_name(key),
            stats: *stats,
        })
        .collect();

    if let Some(prev) = published {
        if api_rows.is_empty() && prev.gauges == snapshot.gauges {
            return None;
        }
    }
    Some(FlushBatch { api_rows, snapshot })
}

/// Write one batch with a single statement. Must run on the DB thread inside a transaction
/// (the worker wraps it in its subtransaction guard so a Postgres ERROR rolls back cleanly).
pub fn write_batch(batch: &FlushBatch) -> Result<(), String> {
    let (keys, names, requests, errors, sums, maxes) = batch.api_columns();
    let (gauge_names, gauge_values) = batch.gauge_columns();
    pgrx::Spi::connect_mut(|client| {
        client
            .update(
                FLUSH_SQL,
                None,
                &[
                    keys.into(),
                    names.into(),
                    requests.into(),
                    errors.into(),
                    sums.into(),
                    maxes.into(),
                    gauge_names.into(),
                    gauge_values.into(),
                ],
            )
            .map(|_| ())
    })
    .map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_protocol::messages::fetch_response::{
        FetchResponse, FetchableTopicResponse, PartitionData,
    };
    use kafka_protocol::messages::produce_response::{
        PartitionProduceResponse, ProduceResponse, TopicProduceResponse,
    };

    fn produce_response(code: i16) -> KafkaResponse {
        KafkaResponse::new(
            1,
            9,
            crate::kafka::api::ResponseBody::Produce(ProduceResponse::default().with_responses(
                vec![
                    TopicProduceResponse::default().with_partition_responses(vec![
                        PartitionProduceResponse::default(),
                        PartitionProduceResponse::default().with_error_code(code),
                    ]),
                ],
            )),
        )
    }

    #[test]
    fn record_request_accumulates_count_errors_sum_and_max() {
        let m = BrokerMetrics::new();
        m.record_request(0, Duration::from_micros(10), false);
        m.record_request(0, Duration::from_micros(30), true);
        m.record_request(1, Duration::from_micros(5), false);
        let s = m.snapshot(0, 0);
        assert_eq!(
            s.apis[0],
            ApiStats {
                requests: 2,
                errors: 1,
                latency_us_sum: 40,
                latency_us_max: 30
            }
        );
        assert_eq!(s.apis[1].requests, 1);
        assert_eq!(s.apis[2], ApiStats::default());
    }

    #[test]
    fn out_of_range_api_keys_are_ignored() {
        let m = BrokerMetrics::new();
        m.record_request(-1, Duration::from_micros(1), true);
        m.record_request(
            i16::try_from(API_SLOTS).unwrap(),
            Duration::from_micros(1),
            true,
        );
        m.record_request(i16::MAX, Duration::from_micros(1), true);
        let s = m.snapshot(0, 0);
        assert!(s.apis.iter().all(|a| *a == ApiStats::default()));
    }

    #[test]
    fn counters_are_safe_across_threads() {
        let m = std::sync::Arc::new(BrokerMetrics::new());
        let handles: Vec<_> = (0..4)
            .map(|_| {
                let m = m.clone();
                std::thread::spawn(move || {
                    for _ in 0..1000 {
                        m.record_request(3, Duration::from_micros(2), false);
                        let _g = m.connection_opened();
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        let s = m.snapshot(0, 0);
        assert_eq!(s.apis[3].requests, 4000);
        assert_eq!(s.apis[3].latency_us_sum, 8000);
        assert_eq!(s.gauges.connections_accepted, 4000);
        assert_eq!(s.gauges.connections_open, 0);
    }

    #[test]
    fn connection_guard_tracks_open_and_total() {
        let m = BrokerMetrics::new();
        let a = m.connection_opened();
        let b = m.connection_opened();
        m.connection_rejected();
        let s = m.snapshot(7, 2);
        assert_eq!(s.gauges.connections_open, 2);
        assert_eq!(s.gauges.connections_accepted, 2);
        assert_eq!(s.gauges.connections_rejected, 1);
        assert_eq!(s.gauges.request_queue_depth, 7);
        assert_eq!(s.gauges.liveness_queue_depth, 2);
        drop(a);
        drop(b);
        let s = m.snapshot(0, 0);
        assert_eq!(s.gauges.connections_open, 0);
        assert_eq!(s.gauges.connections_accepted, 2);
    }

    #[test]
    fn request_timer_records_on_drop_and_flags_errors() {
        let m = BrokerMetrics::new();
        {
            let mut t = RequestTimer::start_in(&m, 0);
            t.observe(&produce_response(0));
        }
        {
            let mut t = RequestTimer::start_in(&m, 0);
            t.observe(&produce_response(6)); // NOT_LEADER_OR_FOLLOWER on one partition
        }
        {
            let mut t = RequestTimer::start_in(&m, 1);
            t.mark_error();
        }
        let s = m.snapshot(0, 0);
        assert_eq!(s.apis[0].requests, 2);
        assert_eq!(s.apis[0].errors, 1);
        assert_eq!(s.apis[1].requests, 1);
        assert_eq!(s.apis[1].errors, 1);
    }

    #[test]
    fn request_timer_records_when_dropped_by_unwind() {
        let m = BrokerMetrics::new();
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut t = RequestTimer::start_in(&m, 8);
            t.mark_error();
            panic!("commit failed");
        }));
        assert!(r.is_err());
        let s = m.snapshot(0, 0);
        assert_eq!(s.apis[8].requests, 1);
        assert_eq!(s.apis[8].errors, 1);
    }

    #[test]
    fn response_has_error_inspects_nested_codes() {
        assert!(!response_has_error(&produce_response(0)));
        assert!(response_has_error(&produce_response(3)));

        let fetch = |top: i16, part: i16| {
            KafkaResponse::new(
                1,
                12,
                crate::kafka::api::ResponseBody::Fetch(
                    FetchResponse::default()
                        .with_error_code(top)
                        .with_responses(vec![FetchableTopicResponse::default().with_partitions(
                            vec![PartitionData::default().with_error_code(part)],
                        )]),
                ),
            )
        };
        assert!(!response_has_error(&fetch(0, 0)));
        assert!(response_has_error(&fetch(0, 1)));
        assert!(response_has_error(&fetch(15, 0)));

        assert!(response_has_error(&KafkaResponse::new(
            1,
            0,
            crate::kafka::api::ResponseBody::UnknownApi { error_code: 35 }
        )));
        assert!(!response_has_error(&KafkaResponse::new(
            1,
            4,
            crate::kafka::api::ResponseBody::Heartbeat(
                kafka_protocol::messages::heartbeat_response::HeartbeatResponse::default()
            )
        )));
        assert!(response_has_error(&KafkaResponse::new(
            1,
            4,
            crate::kafka::api::ResponseBody::Heartbeat(
                kafka_protocol::messages::heartbeat_response::HeartbeatResponse::default()
                    .with_error_code(27)
            )
        )));
    }

    #[test]
    fn response_has_error_add_partitions_to_txn_both_layouts() {
        use kafka_protocol::messages::add_partitions_to_txn_response::*;
        let topic = |code| {
            AddPartitionsToTxnTopicResult::default().with_results_by_partition(vec![
                AddPartitionsToTxnPartitionResult::default().with_partition_error_code(code),
            ])
        };
        let v3 = |code| {
            KafkaResponse::new(
                1,
                3,
                crate::kafka::api::ResponseBody::AddPartitionsToTxn(
                    AddPartitionsToTxnResponse::default()
                        .with_results_by_topic_v3_and_below(vec![topic(code)]),
                ),
            )
        };
        let v4 = |code| {
            KafkaResponse::new(
                1,
                4,
                crate::kafka::api::ResponseBody::AddPartitionsToTxn(
                    AddPartitionsToTxnResponse::default()
                        .with_results_by_transaction(vec![AddPartitionsToTxnResult::default()
                            .with_topic_results(vec![topic(code)])]),
                ),
            )
        };
        assert!(!response_has_error(&v3(0)));
        assert!(response_has_error(&v3(48)));
        assert!(!response_has_error(&v4(0)));
        assert!(response_has_error(&v4(48)));
    }

    #[test]
    fn api_names_cover_known_keys() {
        assert_eq!(api_name(0), "Produce");
        assert_eq!(api_name(1), "Fetch");
        assert_eq!(api_name(44), "IncrementalAlterConfigs");
        assert_eq!(api_name(7), "Unknown");
        // Every registered API has a counter slot.
        assert!(crate::kafka::api::APIS
            .iter()
            .all(|info| usize::try_from(info.key).unwrap() < API_SLOTS));
    }

    #[test]
    fn first_flush_writes_every_known_api_even_when_idle() {
        let m = BrokerMetrics::new();
        let mut f = MetricsFlusher::with_registry(&m, Duration::from_secs(1));
        let batch = f
            .poll_at(Instant::now(), 0, 0)
            .expect("first flush resets the tables");
        assert_eq!(batch.api_rows.len(), crate::kafka::api::APIS.len());
        assert!(batch
            .api_rows
            .iter()
            .all(|r| r.stats == ApiStats::default()));
    }

    #[test]
    fn flush_is_skipped_when_nothing_changed_and_retried_after_failure() {
        let m = BrokerMetrics::new();
        let mut f = MetricsFlusher::with_registry(&m, Duration::from_secs(1));
        let t0 = Instant::now();
        let b = f.poll_at(t0, 0, 0).unwrap();
        assert_eq!(f.finish(b, Ok(())), None);

        // Interval elapsed but nothing changed: no batch, no SQL.
        assert!(f.poll_at(t0 + Duration::from_secs(2), 0, 0).is_none());

        // One Produce: only its row is written, gauges always ride along.
        m.record_request(0, Duration::from_micros(4), false);
        let b = f.poll_at(t0 + Duration::from_secs(3), 0, 0).unwrap();
        assert_eq!(b.api_rows.len(), 1);
        assert_eq!(b.api_rows[0].api_key, 0);
        assert_eq!(b.api_rows[0].api_name, "Produce");
        assert_eq!(b.api_rows[0].stats.requests, 1);

        // The write failed: the first failure is reported, the next is not, and the change is
        // offered again until a write succeeds.
        assert!(f.finish(b, Err("boom".into())).is_some());
        let b = f.poll_at(t0 + Duration::from_secs(4), 0, 0).unwrap();
        assert_eq!(b.api_rows.len(), 1);
        assert!(f.finish(b, Err("boom".into())).is_none());
        let b = f.poll_at(t0 + Duration::from_secs(5), 0, 0).unwrap();
        assert_eq!(f.finish(b, Ok(())), None);
        assert!(f.poll_at(t0 + Duration::from_secs(6), 0, 0).is_none());

        // A gauge-only change (queue depth) also flushes, with no API rows.
        let b = f.poll_at(t0 + Duration::from_secs(7), 3, 0).unwrap();
        assert_eq!(b.api_rows, Vec::new());
        assert_eq!(b.snapshot.gauges.request_queue_depth, 3);
    }

    #[test]
    fn a_forgotten_baseline_writes_every_known_api_again() {
        let m = BrokerMetrics::new();
        let mut f = MetricsFlusher::with_registry(&m, Duration::from_secs(1));
        let t0 = Instant::now();
        m.record_request(0, Duration::from_micros(4), false);
        let b = f.poll_at(t0, 0, 0).unwrap();
        assert_eq!(f.finish(b, Ok(())), None);
        m.record_request(3, Duration::from_micros(4), false);
        f.forget_published();
        let b = f.poll_at(t0 + Duration::from_secs(2), 0, 0).unwrap();
        assert_eq!(b.api_rows.len(), crate::kafka::api::APIS.len());
        let produce = b.api_rows.iter().find(|r| r.api_key == 0).unwrap();
        assert_eq!(produce.stats.requests, 1);
        assert_eq!(f.finish(b, Ok(())), None);
        assert!(f.poll_at(t0 + Duration::from_secs(4), 0, 0).is_none());
    }

    #[test]
    fn flush_respects_interval() {
        let m = BrokerMetrics::new();
        let mut f = MetricsFlusher::with_registry(&m, Duration::from_secs(1));
        let t0 = Instant::now();
        let b = f.poll_at(t0, 0, 0).unwrap();
        f.finish(b, Ok(()));
        m.record_request(1, Duration::from_micros(1), false);
        assert!(f.poll_at(t0 + Duration::from_millis(500), 0, 0).is_none());
        assert!(f.poll_at(t0 + Duration::from_millis(1000), 0, 0).is_some());
    }

    #[test]
    fn batch_columns_line_up_with_flush_sql_parameters() {
        let m = BrokerMetrics::new();
        m.record_request(1, Duration::from_micros(9), true);
        let _c = m.connection_opened();
        let batch =
            build_batch(Some(&BrokerMetrics::new().snapshot(0, 0)), m.snapshot(2, 1)).unwrap();
        let (keys, names, req, err, sum, max) = batch.api_columns();
        assert_eq!(keys, vec![1]);
        assert_eq!(names, vec!["Fetch"]);
        assert_eq!((req[0], err[0], sum[0], max[0]), (1, 1, 9, 9));
        let (gn, gv) = batch.gauge_columns();
        assert_eq!(gn.len(), gv.len());
        let get = |n: &str| gv[gn.iter().position(|x| *x == n).unwrap()];
        assert_eq!(get("connections_open"), 1);
        assert_eq!(get("request_queue_depth"), 2);
        assert_eq!(get("liveness_queue_depth"), 1);
        for p in 1..=8 {
            assert!(FLUSH_SQL.contains(&format!("${p}")));
        }
        assert!(!FLUSH_SQL.contains("$9"));
        assert_eq!(to_i64(u64::MAX), i64::MAX);
    }
}
