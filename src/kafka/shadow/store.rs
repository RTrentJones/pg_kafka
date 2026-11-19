// Copyright (c) 2026 Robert Trent Jones.
// This file is part of the "Shadow Mode" feature of pg_kafka.
//
// Use of this source code for production purposes is governed by the
// Commercial License found in the LICENSE file in this directory.
// Development and evaluation use is permitted.
//
// GitHub Sponsors: https://github.com/sponsors/RTrentJones

//! ShadowStore - KafkaStore wrapper with shadow forwarding
//!
//! This module provides a KafkaStore implementation that wraps another store
//! (typically PostgresStore) and adds shadow forwarding functionality for
//! write operations.
//!
//! ## Design
//!
//! ShadowStore implements the decorator pattern:
//! 1. All reads delegate directly to the inner store
//! 2. Writes: behavior depends on write_mode:
//!    - DualWrite: Write locally first, then forward to external Kafka (best-effort)
//!    - ExternalOnly: Try external first, fallback to local on failure
//!
//! ## Runtime Configuration
//!
//! Shadow mode can be enabled/disabled at runtime via the `pg_kafka.shadow_mode_enabled`
//! GUC. The producer is lazily initialized on first use when shadow mode is enabled.
//! This allows enabling shadow mode via `ALTER SYSTEM` + `pg_reload_conf()` without
//! requiring a PostgreSQL restart.
//!
//! ## Durable forwarding outbox (SH-9)
//!
//! A `DualWrite` produce no longer forwards inline. Instead, in the SAME
//! transaction as the local write, it inserts a pending pointer row into
//! `kafka.shadow_tracking` for each record the percentage router selects. The
//! DB-thread periodic poll claims due rows, joins `kafka.messages` for the
//! payload, and hands them to the network thread (which owns the idempotent
//! rdkafka producer). The network thread forwards and returns a `ForwardAck`;
//! the poll finalizes the row (`external_offset` set on success, `retry_count`
//! bumped on failure). This is at-least-once — a crash before the ack just
//! re-forwards, and the idempotent producer absorbs the duplicate.
//!
//! Per-topic `sync_mode` only changes when the forward happens, never whether
//! it is durable:
//!
//! - **Async**: the poll forwards the row after commit (non-blocking produce).
//! - **Sync (bounded)**: after the request's transaction commits, and before
//!   its response is released, the worker calls `forward_sync_after_commit`,
//!   which forwards the request's rows and waits up to a short cap for their
//!   acks. On timeout the response goes out anyway — the row stays durable and
//!   the poll retries it (SH-14). Forwarding only after commit means a request
//!   that rolls back never leaves a copy on the external broker.
//!
//! ## Database access
//!
//! Every SQL statement and GUC read goes through the [`ShadowDb`] trait
//! (`super::outbox`): `SpiShadowDb` in production, an in-memory fake in unit
//! tests, so the outbox logic in this file is unit-tested.
//!
//! ## Thread Safety
//!
//! The inner store and producer are wrapped appropriately for safe access.
//! The forwarder maintains internal mutable state for primary detection caching.

use super::config::{SyncMode, TopicConfigCache, TopicShadowConfig, WriteMode};
use super::forwarder::ForwardDecision;
use super::license::LicenseValidator;
use super::outbox::{
    OutboxClaim, PendingForwardRecord, ShadowDb, SpiShadowDb, OUTBOX_MAX_BACKOFF_MS,
};
use super::primary::PrimaryStatus;
use super::routing::make_forward_decision;
use super::{
    ForwardAck, ForwardOutcome, ForwardRecord, ForwardRequest, HeldRelease, RowKey, SyncHold,
};
use crate::kafka::error::{KafkaError, Result};
use crate::kafka::messages::Record;
use crate::kafka::storage::{
    delegate_offset_store, delegate_producer_store, delegate_topic_store, FetchedMessage,
    IsolationLevel, KafkaStore, LogStore, MaintenanceStore, OffsetStore, ProducerStore, TopicStore,
    TransactionState, TransactionStore,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

// A committed transactional record's outbox pointer (`PendingForwardRecord`,
// defined in `super::outbox`) is buffered during `insert_transactional_records()`
// and enqueued into the durable `kafka.shadow_tracking` outbox when
// `commit_transaction()` is called (RA-1/RA-2). Only the row key + record key are
// needed — the payload is re-read from `kafka.messages` by the forwarder.

/// Key for pending transaction messages: (producer_id, producer_epoch)
type TxnKey = (i64, i16);

/// Buffered transactional forward records plus when the buffer was first
/// created, so the timeout sweeper can evict an abandoned buffer.
///
/// `abort_timed_out_transactions` (the storage-layer sweeper) force-aborts a
/// timed-out transaction directly in `kafka.transactions` and never routes
/// through `abort_transaction()`, so without an age-based eviction the in-RAM
/// buffer for a producer that opened a transaction and vanished would leak
/// forever. `first_buffered` lets the override drop buffers older than the
/// transaction timeout, matching the sweeper's own threshold.
struct PendingTxnBuffer {
    records: Vec<PendingForwardRecord>,
    first_buffered: Instant,
}

impl Default for PendingTxnBuffer {
    fn default() -> Self {
        Self {
            records: Vec::new(),
            first_buffered: Instant::now(),
        }
    }
}

impl From<Vec<PendingForwardRecord>> for PendingTxnBuffer {
    fn from(records: Vec<PendingForwardRecord>) -> Self {
        Self {
            records,
            first_buffered: Instant::now(),
        }
    }
}

/// Cache of pending transactional records awaiting commit
type PendingTxnMessages = Arc<RwLock<HashMap<TxnKey, PendingTxnBuffer>>>;

/// Metrics for shadow forwarding operations
#[derive(Debug, Default)]
pub struct ShadowMetrics {
    /// Messages successfully forwarded to external Kafka
    pub forwarded: AtomicU64,
    /// Messages skipped due to percentage routing
    pub skipped: AtomicU64,
    /// Forward attempts that failed
    pub failed: AtomicU64,
    /// Messages written to local due to external failure (external_only mode)
    pub fallback_local: AtomicU64,
}

impl ShadowMetrics {
    /// Create new metrics with all counters at zero
    pub fn new() -> Self {
        Self::default()
    }

    /// Get a snapshot of current metric values
    pub fn snapshot(&self) -> (u64, u64, u64, u64) {
        (
            self.forwarded.load(Ordering::Relaxed),
            self.skipped.load(Ordering::Relaxed),
            self.failed.load(Ordering::Relaxed),
            self.fallback_local.load(Ordering::Relaxed),
        )
    }
}

/// How often the worker's outbox tick runs when nothing asked for an earlier one.
pub const OUTBOX_POLL_INTERVAL: Duration = Duration::from_millis(250);

/// Max forwards outstanding at once (dispatched, ack not yet applied). The poll
/// claims no more than the remaining room, so the forward channel (capacity
/// 10 000, see worker.rs) never fills with records and a sync hold always finds
/// room; it also bounds the in-flight key arrays the claim passes to SQL.
const MAX_INFLIGHT: usize = 8_192;

/// Max acks applied per outbox tick (the rest wait for the next tick, which is
/// requested at once).
const MAX_ACKS_PER_TICK: usize = 20_000;

/// How long an in-flight forward blocks re-dispatch of its outbox row before
/// the entry is presumed lost and evicted (issue #93). Sized above rdkafka's
/// `message.timeout.ms` (120s — see shadow::producer), after which every send
/// is guaranteed to have produced a delivery report; an entry older than this
/// means the ack itself was lost (forwarder crash) and the row is claimed again,
/// counting the lost attempt.
const INFLIGHT_EVICT_AFTER: Duration = Duration::from_secs(150);

/// Cap on sync-mode responses waiting for their batch's commit. Entries beyond
/// it are not held (their records are forwarded by the poll as usual).
const SYNC_PENDING_CAP: usize = 10_000;

/// Longest pause of the outbox tick after repeated failures (for example the
/// schema was dropped): the tick backs off exponentially instead of failing,
/// and logging, every 250 ms.
const OUTBOX_FAILURE_BACKOFF_MAX: Duration = Duration::from_secs(30);

/// A KafkaStore wrapper that adds shadow mode forwarding
///
/// Wraps an inner store (typically PostgresStore) and writes the durable
/// forwarding outbox for produce operations based on per-topic configuration.
/// The external producer lives on the network thread (`forwarder_task`).
pub struct ShadowStore<S: KafkaStore, D: ShadowDb = SpiShadowDb> {
    /// The wrapped inner store
    inner: S,
    /// Shadow mode's own SQL and GUC reads (outbox, config, dead letters)
    db: D,
    /// Topic configuration cache
    topic_cache: Arc<TopicConfigCache>,
    /// Primary status checker
    primary_status: Mutex<PrimaryStatus>,
    /// Shadow forwarding metrics
    metrics: ShadowMetrics,
    /// Channel to the network-thread forwarder (records and sync holds)
    forward_tx: RwLock<Option<crossbeam_channel::Sender<ForwardRequest>>>,
    /// Channel on which the network thread returns forward results (acks) for
    /// the durable outbox. Drained on the DB thread by the outbox tick to
    /// finalize `kafka.shadow_tracking` rows. (SH-9)
    forward_ack_rx: RwLock<Option<crossbeam_channel::Receiver<ForwardAck>>>,
    /// Pending transactional records awaiting commit for shadow forwarding
    pending_txn_messages: PendingTxnMessages,
    /// License validator for shadow mode (Commercial License)
    license: RwLock<Option<LicenseValidator>>,
    /// Outbox rows dispatched to the network thread whose ack has not yet
    /// been applied; the claim excludes them, so a slow ack never produces a
    /// duplicate delivery (issue #93).
    inflight: super::inflight::InflightTracker,
    /// The request the worker is running (see `begin_request`).
    current_slot: AtomicU64,
    /// Sync-mode outbox rows written by requests of the current batch, each with
    /// its request slot and the xid of the subtransaction that wrote it.
    sync_pending: Mutex<Vec<SyncPending>>,
    /// Set when the outbox tick should run at the next loop iteration instead of
    /// waiting `OUTBOX_POLL_INTERVAL` (sync rows committed, a full claim).
    poll_requested: AtomicBool,
    /// Consecutive failed outbox ticks and the pause they imposed.
    outbox_failures: Mutex<(u32, Option<Instant>)>,
}

/// One sync-mode outbox row waiting for its request's commit.
struct SyncPending {
    slot: u64,
    xid: u32,
    key: RowKey,
}

impl<S: KafkaStore> ShadowStore<S> {
    /// Create a new ShadowStore wrapping the given inner store
    ///
    /// # Arguments
    /// * `inner` - The store to wrap (typically PostgresStore)
    pub fn new(inner: S) -> Self {
        Self::with_db(inner, SpiShadowDb)
    }
}

impl<S: KafkaStore, D: ShadowDb> ShadowStore<S, D> {
    /// Create a ShadowStore over `inner` whose own SQL goes through `db`.
    pub fn with_db(inner: S, db: D) -> Self {
        Self {
            inner,
            db,
            topic_cache: Arc::new(TopicConfigCache::new()),
            primary_status: Mutex::new(PrimaryStatus::new()),
            metrics: ShadowMetrics::new(),
            forward_tx: RwLock::new(None),
            forward_ack_rx: RwLock::new(None),
            pending_txn_messages: Arc::new(RwLock::new(HashMap::new())),
            license: RwLock::new(None),
            inflight: super::inflight::InflightTracker::new(INFLIGHT_EVICT_AFTER),
            current_slot: AtomicU64::new(0),
            sync_pending: Mutex::new(Vec::new()),
            poll_requested: AtomicBool::new(false),
            outbox_failures: Mutex::new((0, None)),
        }
    }

    /// The database backend (tests inspect the fake through this).
    #[cfg(test)]
    pub(crate) fn db(&self) -> &D {
        &self.db
    }

    /// Set the forward channel to the network-thread forwarder.
    pub fn set_forward_channel(&self, tx: crossbeam_channel::Sender<ForwardRequest>) {
        let mut guard = self.forward_tx.write().unwrap_or_else(|poisoned| {
            tracing::warn!("forward_tx write lock was poisoned, recovering");
            poisoned.into_inner()
        });
        *guard = Some(tx);
    }

    /// Install the channel on which the network thread returns forward acks.
    ///
    /// Must be called after construction to enable the durable outbox; the DB
    /// thread drains this on its outbox tick to finalize
    /// `kafka.shadow_tracking` rows. (SH-9)
    pub fn set_ack_channel(&self, rx: crossbeam_channel::Receiver<ForwardAck>) {
        let mut guard = self.forward_ack_rx.write().unwrap_or_else(|poisoned| {
            tracing::warn!("forward_ack_rx write lock was poisoned, recovering");
            poisoned.into_inner()
        });
        *guard = Some(rx);
    }

    /// Set license key and initialize validator (Commercial License)
    pub fn set_license_key(&self, license_key: &str) {
        let validator = LicenseValidator::new(license_key);
        let mut guard = self.license.write().unwrap_or_else(|poisoned| {
            tracing::warn!("license write lock was poisoned, recovering");
            poisoned.into_inner()
        });
        *guard = Some(validator);
    }

    /// Check license and emit rate-limited warnings if shadow mode is active
    fn check_license(&self) {
        let guard = self.license.read().unwrap_or_else(|poisoned| {
            tracing::warn!("license read lock was poisoned, recovering");
            poisoned.into_inner()
        });
        if let Some(ref validator) = *guard {
            validator.check_and_warn();
        }
    }

    /// Get the topic configuration cache
    pub fn topic_cache(&self) -> &TopicConfigCache {
        &self.topic_cache
    }

    /// Get the inner store
    pub fn inner(&self) -> &S {
        &self.inner
    }

    /// Get shadow forwarding metrics
    pub fn metrics(&self) -> &ShadowMetrics {
        &self.metrics
    }

    /// Check if we're running on the primary (not a standby)
    fn is_primary(&self) -> bool {
        let mut status = self.primary_status.lock().unwrap_or_else(|poisoned| {
            tracing::warn!("primary_status lock was poisoned, recovering");
            poisoned.into_inner()
        });
        status.check()
    }

    /// Whether `topic_id` is an *external-primary* topic — `ExternalOnly`
    /// write_mode with forwarding active (SH-6). Such a topic is written and
    /// forwarded exactly like DualWrite, but its local reads are suppressed so
    /// consumers must read from the external broker. The config loader forces
    /// `forward_percentage = 100` for these topics, so every record is
    /// guaranteed to be forwarded (a sampled-out record would be readable
    /// nowhere).
    ///
    /// Read suppression depends only on the global shadow GUC + the per-topic
    /// config, not on primary/standby (RA-6): a standby must suppress these reads
    /// too, since the external broker — not the local copy — is the source of
    /// truth for an external-primary topic. (Forwarding itself is still
    /// primary-only; that gate lives on the write path.)
    fn is_external_primary(&self, topic_id: i32) -> bool {
        if !self.is_enabled() {
            return false;
        }
        self.topic_cache
            .get(topic_id)
            .map(|c| c.should_forward() && c.write_mode == WriteMode::ExternalOnly)
            .unwrap_or(false)
    }

    /// RV-6: whether to suppress local reads for a topic.
    ///
    /// A healthy external-primary topic suppresses local reads so consumers cut over
    /// to the external broker. But a forward that exhausts the retry cap is
    /// dead-lettered — present locally, never external. To avoid silent non-delivery
    /// ("readable nowhere"), once a topic has any dead-lettered forward we STOP
    /// suppressing and serve the record locally. `pg_kafka.shadow_external_primary_strict`
    /// keeps full suppression (external-only-strict, the record is accepted as lost).
    fn suppress_external_primary_reads(&self, topic_id: i32) -> bool {
        if !self.is_external_primary(topic_id) {
            return false;
        }
        if self.db.external_primary_strict() {
            return true;
        }
        !self.topic_has_dead_letters(topic_id)
    }

    /// True if the topic has a forward that exhausted the retry cap and will never be
    /// delivered externally (`external_offset` still NULL, `retry_count >= cap`).
    fn topic_has_dead_letters(&self, topic_id: i32) -> bool {
        self.db
            .topic_has_dead_letters(topic_id, self.db.settings().max_retries)
            .unwrap_or(false)
    }

    /// Force a re-check of primary status (SH-2).
    ///
    /// `is_primary()` caches its result for the life of the process, which is
    /// correct only while the recovery state never changes. After a failover a
    /// promoted standby would otherwise stay cached as a non-primary and never
    /// start forwarding. The worker calls this on its periodic config-reload
    /// cycle so a promotion is picked up within one reload interval.
    pub fn refresh_primary_status(&self) -> bool {
        let mut status = self.primary_status.lock().unwrap_or_else(|poisoned| {
            tracing::warn!("primary_status lock was poisoned, recovering");
            poisoned.into_inner()
        });
        status.refresh()
    }

    /// Check if shadow mode is enabled globally by reading the GUC directly
    fn is_enabled(&self) -> bool {
        self.db.shadow_mode_enabled()
    }

    /// Decide whether to forward a message based on percentage routing.
    ///
    /// Delegates to the shared `routing` module so the live produce path and the
    /// outbox poll make identical decisions.
    fn decide_forward(
        &self,
        key: Option<&[u8]>,
        offset: i64,
        forward_percentage: u8,
    ) -> ForwardDecision {
        make_forward_decision(key, offset, forward_percentage)
    }

    // ===== Durable forwarding outbox (SH-9, SH-7, SH-8, SH-14, SH-15) =====

    /// Try to hand a message to the network thread. Never blocks; gives the
    /// message back if no channel is wired up or it is full.
    fn try_send_forward(
        &self,
        msg: ForwardRequest,
    ) -> std::result::Result<(), Box<ForwardRequest>> {
        let guard = self.forward_tx.read().unwrap_or_else(|poisoned| {
            tracing::warn!("forward_tx read lock was poisoned, recovering");
            poisoned.into_inner()
        });
        match guard.as_ref() {
            Some(tx) => tx.try_send(msg).map_err(|e| Box::new(e.into_inner())),
            None => Err(Box::new(msg)),
        }
    }

    /// Insert a batch of pending `(topic_id, partition_id, local_offset)` rows
    /// into the `kafka.shadow_tracking` outbox in the current transaction. The
    /// shared write path for both the live produce and the txn-commit enqueue.
    fn insert_outbox_rows(&self, rows: &[(i32, i32, i64)]) -> Result<()> {
        self.db.insert_outbox_rows(rows)
    }

    /// Count sampled-out records in `kafka.shadow_metrics.messages_skipped`
    /// (when `pg_kafka.shadow_metrics_enabled`) and in the in-RAM counter.
    fn count_skipped(&self, skipped: &HashMap<(i32, i32), i64>, metrics_enabled: bool) {
        for (&(topic_id, partition_id), &n) in skipped {
            if n == 0 {
                continue;
            }
            self.metrics.skipped.fetch_add(n as u64, Ordering::Relaxed);
            if metrics_enabled {
                if let Err(e) = self.db.record_skipped(topic_id, partition_id, n) {
                    tracing::warn!("Shadow: failed to count skipped records: {:?}", e);
                }
            }
        }
    }

    /// Enqueue the records of a just-committed transaction into the durable
    /// outbox (RA-1/RA-2).
    ///
    /// Each percentage-selected record (looked up against the current per-topic
    /// config) gets a pending `shadow_tracking` row written inside the commit
    /// transaction; the outbox poll then forwards it via the idempotent
    /// producer — non-blocking, at-least-once, re-partitioned by key, and
    /// (crucially for external-primary topics, whose local reads are suppressed)
    /// durably retried instead of best-effort-and-dropped.
    fn enqueue_committed_txn_outbox(&self, records: &[PendingForwardRecord]) {
        if records.is_empty() {
            return;
        }
        self.check_license();

        let mut rows: Vec<(i32, i32, i64)> = Vec::with_capacity(records.len());
        let mut skipped: HashMap<(i32, i32), i64> = HashMap::new();
        for r in records {
            // The topic may have stopped forwarding since the records were
            // buffered (config reload); re-check at commit time.
            let percentage = match self.topic_cache.get(r.topic_id) {
                Some(c) if c.should_forward() => c.forward_percentage,
                _ => continue,
            };
            match self.decide_forward(r.key.as_deref(), r.offset, percentage) {
                ForwardDecision::Skip => {
                    *skipped.entry((r.topic_id, r.partition_id)).or_default() += 1;
                }
                ForwardDecision::Forward => rows.push((r.topic_id, r.partition_id, r.offset)),
            }
        }
        self.count_skipped(&skipped, self.db.settings().metrics_enabled);

        if rows.is_empty() {
            return;
        }
        if let Err(e) = self.insert_outbox_rows(&rows) {
            tracing::warn!(
                "Shadow: failed to enqueue {} committed txn outbox row(s): {:?}",
                rows.len(),
                e
            );
        }
    }

    /// Write durable outbox rows for a freshly-produced batch (SH-9).
    ///
    /// For each record the percentage router selects (SH-8), insert a *pending*
    /// `kafka.shadow_tracking` row in the CURRENT transaction (this runs inside
    /// the produce request's subtransaction), so the outbox is exactly
    /// consistent with what was persisted locally. No payload is copied — it
    /// already lives in `kafka.messages`. Sync topics also note the rows so the
    /// worker can hold the request's response until they are delivered.
    fn enqueue_outbox(
        &self,
        topic_id: i32,
        partition_id: i32,
        records: &[Record],
        base_offset: i64,
        config: &TopicShadowConfig,
    ) {
        let forward_percentage = config.forward_percentage;
        let settings = self.db.settings();

        // License nag (rate-limited).
        self.check_license();

        // SH-8: select records by percentage first (decide_forward is pure).
        let mut selected: Vec<i64> = Vec::with_capacity(records.len());
        let mut skipped = 0i64;
        for (i, record) in records.iter().enumerate() {
            let local_offset = base_offset + i as i64;
            match self.decide_forward(record.key.as_deref(), local_offset, forward_percentage) {
                ForwardDecision::Skip => skipped += 1,
                ForwardDecision::Forward => selected.push(local_offset),
            }
        }
        if skipped > 0 {
            let mut by_partition = HashMap::new();
            by_partition.insert((topic_id, partition_id), skipped);
            self.count_skipped(&by_partition, settings.metrics_enabled);
        }

        if selected.is_empty() {
            return;
        }

        // SH-9: persist a pending pointer row per selected record, same txn.
        let rows: Vec<(i32, i32, i64)> = selected
            .iter()
            .map(|&local_offset| (topic_id, partition_id, local_offset))
            .collect();
        if let Err(e) = self.insert_outbox_rows(&rows) {
            tracing::warn!(
                "Shadow: failed to write {} outbox row(s) for topic_id={} partition={}: {:?}",
                selected.len(),
                topic_id,
                partition_id,
                e
            );
            return;
        }

        // Sync topics: the response waits for these rows (see
        // `take_sync_holds`). Nothing leaves for the external broker while the
        // request can still roll back: the poll claims the rows only after the
        // commit.
        if config.sync_mode == SyncMode::Sync && settings.sync_timeout_ms > 0 {
            let xid = self.db.current_transaction_id();
            let slot = self.current_slot.load(Ordering::Relaxed);
            let mut queue = self.sync_pending.lock().unwrap_or_else(|p| p.into_inner());
            for key in rows {
                if queue.len() >= SYNC_PENDING_CAP {
                    tracing::warn!(
                        "Shadow: {} sync forwards awaiting commit; not holding the rest",
                        queue.len()
                    );
                    break;
                }
                queue.push(SyncPending { slot, xid, key });
            }
        }
    }

    /// Called by the worker before each request of a batch; returns the
    /// request's slot, which `take_sync_holds` reports its sync rows under.
    pub fn begin_request(&self) -> u64 {
        self.current_slot.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// After the batch committed: the sync-mode rows of every request whose
    /// writes committed, by request slot (a row from a rolled-back request is
    /// dropped: its outbox row is gone too). Asks for an immediate outbox poll
    /// so the rows go out at once.
    pub fn take_sync_holds(&self) -> HashMap<u64, Vec<RowKey>> {
        let queued =
            std::mem::take(&mut *self.sync_pending.lock().unwrap_or_else(|p| p.into_inner()));
        let mut holds: HashMap<u64, Vec<RowKey>> = HashMap::new();
        let mut committed: HashMap<u32, bool> = HashMap::new();
        for SyncPending { slot, xid, key } in queued {
            let ok = *committed
                .entry(xid)
                .or_insert_with(|| self.db.transaction_committed(xid));
            if ok {
                holds.entry(slot).or_default().push(key);
            }
        }
        if !holds.is_empty() {
            self.poll_requested.store(true, Ordering::Relaxed);
        }
        holds
    }

    /// Hold a committed sync-mode response until `keys` are delivered or
    /// `pg_kafka.shadow_sync_timeout_ms` passes (SH-14, wave 3). The worker is
    /// not blocked: the forwarder releases the response. If the hold cannot be
    /// handed over (no forwarder, full channel) the response is released now.
    pub fn hold_response(&self, keys: Vec<RowKey>, release: HeldRelease) {
        let timeout = Duration::from_millis(self.db.settings().sync_timeout_ms);
        let hold = ForwardRequest::Hold(SyncHold {
            keys,
            timeout,
            release,
        });
        if let Err(refused) = self.try_send_forward(hold) {
            if let ForwardRequest::Hold(hold) = *refused {
                hold.release.release();
            }
        }
    }

    /// Whether the worker's outbox tick should run now, `since_last` after the
    /// previous one: every `OUTBOX_POLL_INTERVAL`, at once when requested, and
    /// never during a failure backoff.
    pub fn outbox_tick_due(&self, since_last: Duration) -> bool {
        let failures = self
            .outbox_failures
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if let Some(until) = failures.1 {
            if Instant::now() < until {
                return false;
            }
        }
        self.poll_requested.load(Ordering::Relaxed) || since_last >= OUTBOX_POLL_INTERVAL
    }

    /// Record how an outbox tick ended. A failure pauses the tick exponentially
    /// (up to `OUTBOX_FAILURE_BACKOFF_MAX`); returns whether this failure should
    /// be logged (the first, then at the 2nd, 4th, 8th... in a row).
    pub fn note_outbox_tick(&self, ok: bool) -> bool {
        let mut failures = self
            .outbox_failures
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if ok {
            *failures = (0, None);
            return false;
        }
        failures.0 = failures.0.saturating_add(1);
        let n = failures.0;
        let pause = OUTBOX_POLL_INTERVAL
            .saturating_mul(1u32 << (n - 1).min(10))
            .min(OUTBOX_FAILURE_BACKOFF_MAX);
        failures.1 = Some(Instant::now() + pause);
        self.poll_requested.store(false, Ordering::Relaxed);
        n.is_power_of_two()
    }

    /// Claim a batch of due outbox rows and hand them to the network thread
    /// (SH-9; ordering and retry rules in `ShadowDb::claim_due_outbox_rows`).
    /// Claims at most `pg_kafka.shadow_batch_size` rows and no more than the
    /// in-flight room left. Returns the number of rows dispatched.
    pub fn poll_and_forward_outbox(&self) -> super::error::ShadowResult<usize> {
        self.poll_requested.store(false, Ordering::Relaxed);
        // Nothing to do if no forward channel is wired up yet.
        {
            let guard = self.forward_tx.read().unwrap_or_else(|poisoned| {
                tracing::warn!("forward_tx read lock was poisoned, recovering");
                poisoned.into_inner()
            });
            if guard.is_none() {
                return Ok(0);
            }
        }

        let settings = self.db.settings();
        let room = MAX_INFLIGHT.saturating_sub(self.inflight.len());
        let limit = (settings.batch_size.max(1) as usize).min(room);
        if limit == 0 {
            return Ok(0);
        }
        let (inflight_topics, inflight_partitions, inflight_offsets) = self.inflight.live_keys();

        let records: Vec<ForwardRecord> = self
            .db
            .claim_due_outbox_rows(&OutboxClaim {
                batch_limit: limit as i64,
                max_retries: settings.max_retries,
                backoff_base_ms: settings.retry_backoff_ms,
                backoff_max_ms: OUTBOX_MAX_BACKOFF_MS,
                inflight_topics,
                inflight_partitions,
                inflight_offsets,
            })
            .map_err(|e: KafkaError| {
                super::error::ShadowError::DatabaseError(format!("outbox poll failed: {}", e))
            })?;
        let claimed = records.len();

        let mut dispatched = 0usize;
        let mut not_sent: Vec<RowKey> = Vec::new();
        for record in records {
            let key = record.row_key();
            // The claim excluded in-flight rows, so this registers a fresh entry.
            self.inflight.try_begin(key);
            match self.try_send_forward(ForwardRequest::Record(record)) {
                Ok(()) => dispatched += 1,
                Err(_) => {
                    self.inflight.complete(&key);
                    not_sent.push(key);
                }
            }
        }
        if !not_sent.is_empty() {
            // Never reached the forwarder: close the attempt without counting it.
            tracing::debug!(
                "Shadow: forward channel unavailable, {} row(s) left pending",
                not_sent.len()
            );
            self.db
                .apply_not_attempted(&not_sent)
                .map_err(|e| super::error::ShadowError::DatabaseError(e.to_string()))?;
        }
        // A full claim means more rows are probably due: poll again at once.
        if claimed >= limit {
            self.poll_requested.store(true, Ordering::Relaxed);
        }
        Ok(dispatched)
    }

    /// Drain the currently-available forward acks and finalize their outbox
    /// rows (SH-9), a few statements per tick whatever the number of acks.
    /// Non-blocking. Returns the number of acks applied.
    pub fn drain_forward_acks(&self) -> usize {
        let mut acks: Vec<ForwardAck> = Vec::new();
        {
            let guard = self.forward_ack_rx.read().unwrap_or_else(|poisoned| {
                tracing::warn!("forward_ack_rx read lock was poisoned, recovering");
                poisoned.into_inner()
            });
            if let Some(rx) = guard.as_ref() {
                while acks.len() < MAX_ACKS_PER_TICK {
                    match rx.try_recv() {
                        Ok(ack) => acks.push(ack),
                        Err(_) => break,
                    }
                }
            }
        }
        if acks.len() >= MAX_ACKS_PER_TICK {
            self.poll_requested.store(true, Ordering::Relaxed);
        }
        self.apply_acks(&acks);
        acks.len()
    }

    /// Finalize outbox rows from forward acks (SH-9, SH-7).
    ///
    /// Delivered → external offset recorded; the row is done. Failed → the row
    /// stays pending with `retry_count + 1` and the error, and is re-attempted
    /// after its backoff; at `pg_kafka.shadow_max_retries` it is a dead letter.
    /// NotAttempted → the attempt is closed without counting. The forward metric
    /// is counted exactly once, on the transition (SH-7).
    fn apply_acks(&self, acks: &[ForwardAck]) {
        if acks.is_empty() {
            return;
        }
        let settings = self.db.settings();
        let mut delivered: Vec<(RowKey, i64)> = Vec::new();
        let mut failed: Vec<(RowKey, String)> = Vec::new();
        let mut not_attempted: Vec<RowKey> = Vec::new();
        for ack in acks {
            // Issue #93: the forward has resolved, so the row may be claimed again.
            self.inflight.complete(&ack.row_key());
            match &ack.outcome {
                ForwardOutcome::Delivered(x) => delivered.push((ack.row_key(), *x)),
                ForwardOutcome::Failed(e) => failed.push((ack.row_key(), e.clone())),
                ForwardOutcome::NotAttempted(_) => not_attempted.push(ack.row_key()),
            }
        }

        match self
            .db
            .apply_delivered(&delivered, settings.metrics_enabled)
        {
            Ok(n) => {
                self.metrics
                    .forwarded
                    .fetch_add(n as u64, Ordering::Relaxed);
            }
            Err(e) => tracing::warn!("Shadow: failed to apply delivery acks: {:?}", e),
        }
        match self
            .db
            .apply_failed(&failed, settings.max_retries, settings.metrics_enabled)
        {
            Ok(applied) => {
                self.metrics
                    .failed
                    .fetch_add(applied.transitioned as u64, Ordering::Relaxed);
                for (key, msg) in applied.dead_lettered {
                    // RV-6: at the retry cap the poll stops retrying this row (it
                    // is dead-lettered). For an external-primary topic the record
                    // then falls back to local reads unless strict — log loudly.
                    crate::pg_warning!(
                        "shadow forward dead-lettered after {} failed attempts (topic_id={}, partition_id={}, local_offset={}): {} — re-queue it with kafka.replay_shadow_messages(); external-primary reads for this topic fall back to local unless pg_kafka.shadow_external_primary_strict is set",
                        settings.max_retries,
                        key.0,
                        key.1,
                        key.2,
                        msg
                    );
                }
            }
            Err(e) => tracing::warn!("Shadow: failed to apply failure acks: {:?}", e),
        }
        if let Err(e) = self.db.apply_not_attempted(&not_attempted) {
            tracing::warn!("Shadow: failed to apply not-attempted acks: {:?}", e);
        }
    }

    /// Load topic shadow configurations from database
    ///
    /// Queries kafka.shadow_config and populates the topic_cache.
    /// Call this on startup and periodically to refresh config.
    pub fn load_topic_config_from_db(&self) -> super::error::ShadowResult<usize> {
        // Collect into a local vec first and only swap the cache once the query
        // has succeeded (SH-3). The cache must be *replaced*, not merely
        // updated: a topic whose row was deleted from kafka.shadow_config would
        // otherwise linger in the cache (and keep forwarding) forever. But a
        // transient query failure must NOT wipe a working cache, so we clear
        // only on the success path.
        let Some(mut new_configs) = self.db.load_topic_configs() else {
            // Only replace the cache when the query actually ran. On failure, keep
            // whatever was loaded previously rather than serving an empty config.
            return Err(super::error::ShadowError::DatabaseError(
                "failed to query kafka.shadow_config".to_string(),
            ));
        };

        for config in &mut new_configs {
            // SH-6 guard: an external-primary topic suppresses local
            // reads, so a record sampled out by forward_percentage would
            // be readable nowhere — neither locally (suppressed) nor
            // externally (not forwarded). Force 100% so every record is
            // forwarded; the percentage dial is meaningless here.
            if config.write_mode == WriteMode::ExternalOnly && config.forward_percentage != 100 {
                tracing::warn!(
                    "Shadow: topic '{}' is external-primary (ExternalOnly) but \
                     forward_percentage={}; forcing 100% to avoid unreadable records",
                    config.topic_name,
                    config.forward_percentage
                );
                config.forward_percentage = 100;
            }
        }

        let count = new_configs.len();
        self.topic_cache.clear();
        for config in new_configs {
            self.topic_cache.update(config);
        }

        Ok(count)
    }

    /// Flush accumulated metrics to logs
    ///
    /// Logs metrics summary and resets counters. Call periodically from the worker loop.
    pub fn flush_metrics_to_db(&self) -> super::error::ShadowResult<()> {
        let (forwarded, skipped, failed, fallback_local) = self.metrics.snapshot();

        // Only log if there's something to report
        if forwarded == 0 && skipped == 0 && failed == 0 && fallback_local == 0 {
            return Ok(());
        }

        tracing::info!(
            "Shadow metrics: forwarded={}, skipped={}, failed={}, fallback_local={}",
            forwarded,
            skipped,
            failed,
            fallback_local
        );

        // Reset counters after logging
        self.metrics.forwarded.store(0, Ordering::Relaxed);
        self.metrics.skipped.store(0, Ordering::Relaxed);
        self.metrics.failed.store(0, Ordering::Relaxed);
        self.metrics.fallback_local.store(0, Ordering::Relaxed);

        Ok(())
    }
}

// Roles shadow mode does not change are forwarded to the wrapped store wholesale.
impl<S: KafkaStore, D: ShadowDb> TopicStore for ShadowStore<S, D> {
    delegate_topic_store!(TopicStore, inner);
}

impl<S: KafkaStore, D: ShadowDb> OffsetStore for ShadowStore<S, D> {
    delegate_offset_store!(OffsetStore, inner);
}

impl<S: KafkaStore, D: ShadowDb> ProducerStore for ShadowStore<S, D> {
    delegate_producer_store!(ProducerStore, inner);
}

// The log: writes feed the outbox (forwarding); reads are suppressed for
// external-primary topics (SH-6).
impl<S: KafkaStore, D: ShadowDb> LogStore for ShadowStore<S, D> {
    fn insert_records(&self, topic_id: i32, partition_id: i32, records: &[Record]) -> Result<i64> {
        // Skip shadow forwarding if:
        // 1. Shadow mode is not enabled globally
        // 2. We're running on a standby (only primary forwards)
        let is_enabled = self.is_enabled();
        let is_primary = self.is_primary();

        if !is_enabled || !is_primary {
            // Wave 3: no per-produce LOG lines (this used to log twice per
            // Produce even with shadow mode off and filled server logs).
            return self.inner.insert_records(topic_id, partition_id, records);
        }

        // SH-6/SH-13: external-primary (ExternalOnly) and DualWrite WRITE
        // identically — the record is always persisted locally (real offset)
        // and, when configured, forwarded via the durable outbox. write_mode now
        // governs only READS: an external-primary topic suppresses local fetches
        // (see fetch_records / get_high_watermark), so consumers cut over to the
        // external broker and delivery comes from exactly one source. That makes
        // forward_percentage a real cutover dial instead of a split between local
        // reads and the external broker. The old synthetic-offset / skip-local /
        // partial-failure double-write paths are gone.
        let topic_config = self.topic_cache.get(topic_id);
        let base_offset = self.inner.insert_records(topic_id, partition_id, records)?;

        if let Some(config) = &topic_config {
            if config.should_forward() {
                self.enqueue_outbox(topic_id, partition_id, records, base_offset, config);
            }
        }

        Ok(base_offset)
    }

    fn fetch_records(
        &self,
        topic_id: i32,
        partition_id: i32,
        fetch_offset: i64,
        max_bytes: i32,
    ) -> Result<Vec<FetchedMessage>> {
        // SH-6: external-primary topics suppress local reads so consumers cut
        // over to the external broker. The data is still stored locally (for
        // durability / replay) — it is just not served from here.
        if self.suppress_external_primary_reads(topic_id) {
            return Ok(Vec::new());
        }
        self.inner
            .fetch_records(topic_id, partition_id, fetch_offset, max_bytes)
    }

    fn fetch_records_with_isolation(
        &self,
        topic_id: i32,
        partition_id: i32,
        fetch_offset: i64,
        max_bytes: i32,
        isolation_level: IsolationLevel,
    ) -> Result<Vec<FetchedMessage>> {
        // SH-6: external-primary read suppression (see fetch_records).
        if self.suppress_external_primary_reads(topic_id) {
            return Ok(Vec::new());
        }
        self.inner.fetch_records_with_isolation(
            topic_id,
            partition_id,
            fetch_offset,
            max_bytes,
            isolation_level,
        )
    }

    fn get_high_watermark(&self, topic_id: i32, partition_id: i32) -> Result<i64> {
        // SH-6: report an empty partition for external-primary topics so a
        // consumer pointed at pg_kafka sees nothing to read and uses the
        // external broker instead.
        if self.suppress_external_primary_reads(topic_id) {
            return Ok(0);
        }
        self.inner.get_high_watermark(topic_id, partition_id)
    }

    fn get_earliest_offset(&self, topic_id: i32, partition_id: i32) -> Result<i64> {
        if self.suppress_external_primary_reads(topic_id) {
            return Ok(0);
        }
        self.inner.get_earliest_offset(topic_id, partition_id)
    }

    fn get_log_start_offset(&self, topic_id: i32, partition_id: i32) -> Result<i64> {
        if self.suppress_external_primary_reads(topic_id) {
            return Ok(0);
        }
        self.inner.get_log_start_offset(topic_id, partition_id)
    }

    fn get_last_stable_offset(&self, topic_id: i32, partition_id: i32) -> Result<i64> {
        if self.suppress_external_primary_reads(topic_id) {
            return Ok(0);
        }
        self.inner.get_last_stable_offset(topic_id, partition_id)
    }

    fn get_partition_bounds(
        &self,
        topic_id: i32,
        partition_id: i32,
        isolation_level: crate::kafka::storage::IsolationLevel,
    ) -> Result<crate::kafka::storage::PartitionBounds> {
        // SH-6: same suppression as the single-value bound methods above.
        if self.suppress_external_primary_reads(topic_id) {
            return Ok(crate::kafka::storage::PartitionBounds::default());
        }
        self.inner
            .get_partition_bounds(topic_id, partition_id, isolation_level)
    }

    fn get_offset_for_timestamp(
        &self,
        topic_id: i32,
        partition_id: i32,
        timestamp_ms: i64,
    ) -> Result<Option<(i64, i64)>> {
        if self.suppress_external_primary_reads(topic_id) {
            return Ok(None);
        }
        self.inner
            .get_offset_for_timestamp(topic_id, partition_id, timestamp_ms)
    }

    fn delete_records_before(
        &self,
        topic_id: i32,
        partition_id: i32,
        before_offset: i64,
    ) -> Result<i64> {
        self.inner
            .delete_records_before(topic_id, partition_id, before_offset)
    }
}

// Transactions: transactional writes are buffered and forwarded on commit.
impl<S: KafkaStore, D: ShadowDb> TransactionStore for ShadowStore<S, D> {
    fn get_or_create_transactional_producer(
        &self,
        transactional_id: &str,
        transaction_timeout_ms: i32,
        client_id: Option<&str>,
    ) -> Result<(i64, i16)> {
        let (producer_id, new_epoch) = self.inner.get_or_create_transactional_producer(
            transactional_id,
            transaction_timeout_ms,
            client_id,
        )?;
        // RV-2 residual: a re-init bumps the producer epoch and fences the
        // old-epoch DB rows, but the in-RAM forward buffer keyed
        // (producer_id, old_epoch) would otherwise linger until the age sweep
        // because commit/abort look it up by the NEW epoch. Drop any stale-epoch
        // buffer for this producer now (no-op when the epoch didn't change).
        {
            let mut pending = self
                .pending_txn_messages
                .write()
                .unwrap_or_else(|poisoned| {
                    tracing::warn!("pending_txn_messages write lock was poisoned, recovering");
                    poisoned.into_inner()
                });
            pending.retain(|&(pid, ep), _| pid != producer_id || ep == new_epoch);
        }
        Ok((producer_id, new_epoch))
    }

    fn begin_transaction(
        &self,
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
    ) -> Result<()> {
        self.inner
            .begin_transaction(transactional_id, producer_id, producer_epoch)
    }

    fn begin_or_continue_transaction(
        &self,
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
    ) -> Result<()> {
        self.inner
            .begin_or_continue_transaction(transactional_id, producer_id, producer_epoch)
    }

    fn validate_transaction(
        &self,
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
    ) -> Result<()> {
        self.inner
            .validate_transaction(transactional_id, producer_id, producer_epoch)
    }

    fn insert_transactional_records(
        &self,
        topic_id: i32,
        partition_id: i32,
        records: &[Record],
        producer_id: i64,
        producer_epoch: i16,
    ) -> Result<i64> {
        // Check if shadow forwarding should happen
        let is_enabled = self.is_enabled();
        let is_primary = self.is_primary();

        if !is_enabled || !is_primary {
            // Shadow mode not enabled - just delegate to inner
            return self.inner.insert_transactional_records(
                topic_id,
                partition_id,
                records,
                producer_id,
                producer_epoch,
            );
        }

        // Get topic configuration
        let topic_config = self.topic_cache.get(topic_id);

        // SH-6/SH-13: external-primary (ExternalOnly) and DualWrite WRITE
        // identically — always persist locally with a real offset and buffer for
        // forward-on-commit. write_mode now affects only reads (external-primary
        // suppresses local fetches). So we no longer branch on it here.
        let should_forward = match &topic_config {
            Some(config) => config.should_forward(),
            None => false,
        };

        // Always write locally first (real offset).
        let base_offset = self.inner.insert_transactional_records(
            topic_id,
            partition_id,
            records,
            producer_id,
            producer_epoch,
        )?;

        // Buffer the committed offsets so commit_transaction forwards them.
        if should_forward {
            let pending_records: Vec<PendingForwardRecord> = records
                .iter()
                .enumerate()
                .map(|(i, r)| PendingForwardRecord {
                    topic_id,
                    partition_id,
                    offset: base_offset + i as i64,
                    key: r.key.clone(),
                })
                .collect();

            let key = (producer_id, producer_epoch);
            let mut pending = self
                .pending_txn_messages
                .write()
                .unwrap_or_else(|poisoned| {
                    tracing::warn!("pending_txn_messages write lock was poisoned, recovering");
                    poisoned.into_inner()
                });
            let buf = pending.entry(key).or_default();
            buf.records.extend(pending_records);

            tracing::trace!(
                "ShadowStore: buffered {} txn records for topic {} partition {} (forward on commit)",
                records.len(),
                topic_id,
                partition_id
            );
        }

        Ok(base_offset)
    }

    fn store_txn_pending_offset(
        &self,
        transactional_id: &str,
        group_id: &str,
        topic_id: i32,
        partition_id: i32,
        offset: i64,
        metadata: Option<&str>,
    ) -> Result<()> {
        self.inner.store_txn_pending_offset(
            transactional_id,
            group_id,
            topic_id,
            partition_id,
            offset,
            metadata,
        )
    }

    fn commit_transaction(
        &self,
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
    ) -> Result<()> {
        // 1. Get pending records BEFORE commit
        let key = (producer_id, producer_epoch);
        let pending_records = {
            let mut pending = self
                .pending_txn_messages
                .write()
                .unwrap_or_else(|poisoned| {
                    tracing::warn!("pending_txn_messages write lock was poisoned, recovering");
                    poisoned.into_inner()
                });
            pending.remove(&key)
        };

        // 2. The records about to be committed. Read them from kafka.messages, in
        // this transaction, rather than trusting the in-RAM buffer: the buffer is
        // empty after a worker restart, and it is removed above even if the commit
        // below then fails and the client retries, either of which committed the
        // transaction without ever forwarding it.
        //
        // Only while shadow mode is on and this node is primary, the same gate
        // insert_transactional_records applies when buffering: records produced
        // while forwarding was off must not be queued (and later forwarded) here.
        let forwarding = self.is_enabled() && self.is_primary();
        let records = if forwarding {
            self.db
                .pending_txn_records(producer_id, producer_epoch)
                .unwrap_or_else(|| pending_records.map(|buf| buf.records).unwrap_or_default())
        } else {
            Vec::new()
        };

        // 3. Commit the inner transaction unconditionally. Even with zero
        // forwarded records, the kafka.transactions state row must transition to
        // committed and any pending consumer offsets (TxnOffsetCommit) must be
        // applied — otherwise the transaction lingers as Ongoing until the
        // timeout sweeper force-aborts it.
        self.inner
            .commit_transaction(transactional_id, producer_id, producer_epoch)?;

        // 4. RA-1/RA-2: enqueue the committed records into the durable outbox in
        // this same commit transaction. The poll forwards them — no DB-thread
        // block_on (RA-1), at-least-once with re-partition-by-key (RA-2), and
        // durably retried for external-primary topics (whose local reads are
        // suppressed) instead of best-effort-and-dropped.
        self.enqueue_committed_txn_outbox(&records);

        Ok(())
    }

    fn abort_transaction(
        &self,
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
    ) -> Result<()> {
        // 1. Get pending records to check if any were written locally
        let key = (producer_id, producer_epoch);
        let pending_records = {
            let mut pending = self
                .pending_txn_messages
                .write()
                .unwrap_or_else(|poisoned| {
                    tracing::warn!("pending_txn_messages write lock was poisoned, recovering");
                    poisoned.into_inner()
                });
            pending.remove(&key)
        };

        // 2. Abort the inner transaction unconditionally. Even when no record
        // was written locally (ExternalOnly), the transaction's state row in
        // kafka.transactions must transition to aborted and pending offsets
        // must be discarded — otherwise the transaction lingers as Ongoing
        // until the timeout sweeper force-aborts it.
        self.inner
            .abort_transaction(transactional_id, producer_id, producer_epoch)?;

        // Buffered forward records are never forwarded on abort, so just
        // discard the buffer (already removed from pending_txn_messages above).
        if let Some(buf) = &pending_records {
            if !buf.records.is_empty() {
                tracing::debug!(
                    "Shadow txn abort: discarded {} buffered forward record(s)",
                    buf.records.len()
                );
            }
        }

        Ok(())
    }

    fn get_transaction_state(&self, transactional_id: &str) -> Result<Option<TransactionState>> {
        self.inner.get_transaction_state(transactional_id)
    }
}

// Maintenance: the timeout sweep also evicts stale forward buffers (SH-4).
impl<S: KafkaStore, D: ShadowDb> MaintenanceStore for ShadowStore<S, D> {
    fn abort_timed_out_transactions(&self, timeout: Duration) -> Result<Vec<String>> {
        let aborted = self.inner.abort_timed_out_transactions(timeout)?;

        // SH-4: the storage-layer sweeper force-aborts timed-out transactions
        // directly and never routes through abort_transaction(), so the in-RAM
        // forward buffer for an abandoned producer would leak forever. Evict any
        // buffer older than the same timeout the sweeper just applied.
        {
            let mut pending = self
                .pending_txn_messages
                .write()
                .unwrap_or_else(|poisoned| {
                    tracing::warn!("pending_txn_messages write lock was poisoned, recovering");
                    poisoned.into_inner()
                });
            let before = pending.len();
            pending.retain(|_, buf| buf.first_buffered.elapsed() < timeout);
            let evicted = before - pending.len();
            if evicted > 0 {
                tracing::debug!(
                    "Shadow: evicted {} stale pending-txn forward buffer(s) on timeout sweep",
                    evicted
                );
            }
        }

        Ok(aborted)
    }

    fn cleanup_aborted_messages(&self, older_than: Duration) -> Result<u64> {
        self.inner.cleanup_aborted_messages(older_than)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::shadow::config::{ShadowMode, WriteMode};

    #[test]
    fn test_shadow_store_topic_cache() {
        // Test that topic cache is accessible
        let cache = TopicConfigCache::new();
        let config = super::super::config::TopicShadowConfig {
            topic_id: 1,
            topic_name: "test".to_string(),
            mode: ShadowMode::Shadow,
            forward_percentage: 100,
            external_topic_name: None,
            sync_mode: super::super::config::SyncMode::Async,
            write_mode: WriteMode::DualWrite,
        };
        cache.update(config);

        let retrieved = cache.get(1);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().topic_name, "test");
    }

    #[test]
    fn test_forward_decision_percentage() {
        // Test that forward decisions work correctly
        let cache = TopicConfigCache::new();
        let config = super::super::config::TopicShadowConfig {
            topic_id: 1,
            topic_name: "test".to_string(),
            mode: ShadowMode::Shadow,
            forward_percentage: 50,
            external_topic_name: None,
            sync_mode: super::super::config::SyncMode::Async,
            write_mode: WriteMode::DualWrite,
        };
        cache.update(config);

        // The decision logic is tested in forwarder module
        // Here we just verify cache integration works
        let retrieved = cache.get(1).unwrap();
        assert!(retrieved.should_forward());
    }

    #[test]
    fn test_txn_key_type() {
        // TxnKey is (producer_id, producer_epoch)
        let key1: TxnKey = (1000, 0);
        let key2: TxnKey = (1000, 1);
        let key3: TxnKey = (1001, 0);

        // Keys with same producer_id but different epoch should differ
        assert_ne!(key1, key2);
        // Keys with different producer_id should differ
        assert_ne!(key1, key3);

        // Test HashMap key behavior
        let mut map: HashMap<TxnKey, Vec<PendingForwardRecord>> = HashMap::new();
        map.insert(key1, vec![]);
        map.insert(key2, vec![]);

        assert_eq!(map.len(), 2);
        assert!(map.contains_key(&(1000, 0)));
        assert!(map.contains_key(&(1000, 1)));
        assert!(!map.contains_key(&(1000, 2)));
    }

    #[test]
    fn test_pending_txn_clear_on_commit() {
        let pending: PendingTxnMessages = Arc::new(RwLock::new(HashMap::new()));
        let key: TxnKey = (1000, 0);

        // Add records
        {
            let mut guard = pending.write().unwrap();
            guard.insert(
                key,
                vec![PendingForwardRecord {
                    topic_id: 1,
                    partition_id: 0,
                    offset: 0,
                    key: None,
                }]
                .into(),
            );
        }

        // Simulate commit by removing
        {
            let mut guard = pending.write().unwrap();
            let removed = guard.remove(&key);
            assert!(removed.is_some());
            assert_eq!(removed.unwrap().records.len(), 1);
        }

        // Verify empty
        let guard = pending.read().unwrap();
        assert!(guard.get(&key).is_none());
    }

    #[test]
    fn test_pending_txn_abort_clears_records() {
        let pending: PendingTxnMessages = Arc::new(RwLock::new(HashMap::new()));
        let key: TxnKey = (1000, 0);

        // Add records
        {
            let mut guard = pending.write().unwrap();
            guard.insert(
                key,
                vec![PendingForwardRecord {
                    topic_id: 1,
                    partition_id: 0,
                    offset: 0,
                    key: None,
                }]
                .into(),
            );
        }

        // Simulate abort by removing without forwarding
        {
            let mut guard = pending.write().unwrap();
            guard.remove(&key); // Discard records on abort
        }

        // Verify empty
        assert!(pending.read().unwrap().is_empty());
    }

    #[test]
    fn test_shadow_metrics_initialization() {
        let metrics = ShadowMetrics::default();

        assert_eq!(
            metrics.forwarded.load(std::sync::atomic::Ordering::Relaxed),
            0
        );
        assert_eq!(
            metrics.skipped.load(std::sync::atomic::Ordering::Relaxed),
            0
        );
        assert_eq!(metrics.failed.load(std::sync::atomic::Ordering::Relaxed), 0);
        assert_eq!(
            metrics
                .fallback_local
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
    }

    #[test]
    fn test_shadow_metrics_atomic_increment() {
        let metrics = ShadowMetrics::default();

        // Increment counters
        metrics
            .forwarded
            .fetch_add(10, std::sync::atomic::Ordering::Relaxed);
        metrics
            .skipped
            .fetch_add(5, std::sync::atomic::Ordering::Relaxed);
        metrics
            .failed
            .fetch_add(2, std::sync::atomic::Ordering::Relaxed);
        metrics
            .fallback_local
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        assert_eq!(
            metrics.forwarded.load(std::sync::atomic::Ordering::Relaxed),
            10
        );
        assert_eq!(
            metrics.skipped.load(std::sync::atomic::Ordering::Relaxed),
            5
        );
        assert_eq!(metrics.failed.load(std::sync::atomic::Ordering::Relaxed), 2);
        assert_eq!(
            metrics
                .fallback_local
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
    }

    #[test]
    fn test_shadow_metrics_thread_safe() {
        use std::sync::atomic::Ordering;
        use std::thread;

        let metrics = Arc::new(ShadowMetrics::default());

        // Spawn multiple threads incrementing counters
        let handles: Vec<_> = (0..10)
            .map(|_| {
                let m = Arc::clone(&metrics);
                thread::spawn(move || {
                    for _ in 0..100 {
                        m.forwarded.fetch_add(1, Ordering::Relaxed);
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(metrics.forwarded.load(Ordering::Relaxed), 1000);
    }

    // ===== Transaction finalization and ExternalOnly fallback tests =====

    use crate::kafka::shadow::outbox::fake::InMemoryShadowDb;
    use crate::testing::mocks::MockKafkaStore;

    fn external_only_record(topic_id: i32) -> PendingForwardRecord {
        PendingForwardRecord {
            topic_id,
            partition_id: 0,
            offset: -1,
            key: None,
        }
    }

    #[test]
    fn test_abort_finalizes_inner_txn_with_only_external_only_records() {
        // Regression: aborting a transaction with no locally-written records
        // used to skip the inner abort, leaving the transaction Ongoing
        let mut mock = MockKafkaStore::new();
        mock.expect_abort_transaction()
            .times(1)
            .returning(|_, _, _| Ok(()));

        let store = ShadowStore::with_db(mock, InMemoryShadowDb::default());
        {
            let mut pending = store.pending_txn_messages.write().unwrap();
            pending
                .entry((100, 0))
                .or_default()
                .records
                .push(external_only_record(1));
        }

        store.abort_transaction("txn-1", 100, 0).unwrap();
    }

    #[test]
    fn test_commit_finalizes_inner_txn() {
        // The inner commit must run unconditionally (state row → committed,
        // pending offsets applied) even though forwarding is now deferred to the
        // durable outbox (RA-1/RA-2) rather than done inline. In test builds the
        // outbox enqueue is a no-op stub, so this asserts the inner commit fires.
        let mut mock = MockKafkaStore::new();
        mock.expect_commit_transaction()
            .times(1)
            .returning(|_, _, _| Ok(()));

        let store = ShadowStore::with_db(mock, InMemoryShadowDb::default());
        {
            let mut pending = store.pending_txn_messages.write().unwrap();
            pending
                .entry((100, 0))
                .or_default()
                .records
                .push(external_only_record(1));
        }

        store.commit_transaction("txn-1", 100, 0).unwrap();
    }

    #[test]
    fn test_unchanged_roles_are_delegated_to_the_inner_store() {
        // TopicStore, OffsetStore and ProducerStore are forwarded wholesale
        // (delegate_*_store!): each call reaches the wrapped store with its arguments
        // and returns the inner result unchanged.
        let mut mock = MockKafkaStore::new();
        mock.expect_get_topic_id()
            .withf(|name| name == "orders")
            .times(1)
            .returning(|_| Ok(Some(7)));
        mock.expect_set_topic_retention_ms()
            .withf(|id, ms| *id == 7 && *ms == Some(60_000))
            .times(1)
            .returning(|_, _| Ok(()));
        mock.expect_commit_offset()
            .withf(|g, t, p, o, m| g == "g" && *t == 7 && *p == 1 && *o == 42 && m.is_none())
            .times(1)
            .returning(|_, _, _, _, _| Ok(()));
        mock.expect_delete_consumer_group_offsets()
            .times(1)
            .returning(|_| Ok(()));
        mock.expect_allocate_producer_id()
            .times(1)
            .returning(|_, _| Ok((5, 0)));
        mock.expect_record_producer_sequence()
            .withf(|pid, t, p, seq| (*pid, *t, *p, *seq) == (5, 7, 1, 9))
            .times(1)
            .returning(|_, _, _, _| Ok(()));

        let store = ShadowStore::with_db(mock, InMemoryShadowDb::default());
        assert_eq!(store.get_topic_id("orders").unwrap(), Some(7));
        store.set_topic_retention_ms(7, Some(60_000)).unwrap();
        store.commit_offset("g", 7, 1, 42, None).unwrap();
        store.delete_consumer_group_offsets("g").unwrap();
        assert_eq!(store.allocate_producer_id(None, None).unwrap(), (5, 0));
        store.record_producer_sequence(5, 7, 1, 9).unwrap();
    }

    #[test]
    fn test_abort_timed_out_evicts_stale_pending_buffers() {
        // SH-4: the storage sweeper force-aborts timed-out transactions without
        // routing through abort_transaction(), so a buffer for an abandoned
        // producer must be evicted on the same timeout or it leaks forever.
        let mut mock = MockKafkaStore::new();
        mock.expect_abort_timed_out_transactions()
            .times(1)
            .returning(|_| Ok(vec![]));

        let store = ShadowStore::with_db(mock, InMemoryShadowDb::default());
        {
            let mut pending = store.pending_txn_messages.write().unwrap();
            // Stale: first_buffered older than the timeout we will sweep with.
            let mut stale = PendingTxnBuffer::from(vec![external_only_record(1)]);
            stale.first_buffered = Instant::now()
                .checked_sub(Duration::from_secs(120))
                .unwrap_or_else(Instant::now);
            pending.insert((1, 0), stale);
            // Fresh: just buffered, well within the timeout.
            pending.insert(
                (2, 0),
                PendingTxnBuffer::from(vec![external_only_record(1)]),
            );
        }

        store
            .abort_timed_out_transactions(Duration::from_secs(60))
            .unwrap();

        let pending = store.pending_txn_messages.read().unwrap();
        assert!(
            !pending.contains_key(&(1, 0)),
            "stale buffer should be evicted by the timeout sweep"
        );
        assert!(
            pending.contains_key(&(2, 0)),
            "fresh buffer must be retained"
        );
    }

    #[test]
    fn test_reinit_drops_stale_epoch_pending_buffer() {
        // RV-2 residual: a producer re-init bumps the epoch; the old-epoch in-RAM
        // forward buffer must be dropped on re-init (commit/abort look it up by the
        // NEW epoch and would otherwise never free it). A different producer's
        // buffer must be left untouched.
        let mut mock = MockKafkaStore::new();
        mock.expect_get_or_create_transactional_producer()
            .times(1)
            .returning(|_, _, _| Ok((100, 1)));

        let store = ShadowStore::with_db(mock, InMemoryShadowDb::default());
        {
            let mut pending = store.pending_txn_messages.write().unwrap();
            // Stale: producer 100 at the OLD epoch 0.
            pending.insert(
                (100, 0),
                PendingTxnBuffer::from(vec![external_only_record(1)]),
            );
            // Unrelated producer 200 — must be kept.
            pending.insert(
                (200, 0),
                PendingTxnBuffer::from(vec![external_only_record(1)]),
            );
        }

        let (pid, epoch) = store
            .get_or_create_transactional_producer("txn-1", 60_000, None)
            .unwrap();
        assert_eq!((pid, epoch), (100, 1));

        let pending = store.pending_txn_messages.read().unwrap();
        assert!(
            !pending.contains_key(&(100, 0)),
            "stale old-epoch buffer should be dropped on re-init"
        );
        assert!(
            pending.contains_key(&(200, 0)),
            "a different producer's buffer must be retained"
        );
    }

    #[test]
    fn test_shadow_metrics_new() {
        let metrics = ShadowMetrics::new();
        assert_eq!(
            metrics.forwarded.load(std::sync::atomic::Ordering::Relaxed),
            0
        );
        assert_eq!(
            metrics.skipped.load(std::sync::atomic::Ordering::Relaxed),
            0
        );
        assert_eq!(metrics.failed.load(std::sync::atomic::Ordering::Relaxed), 0);
        assert_eq!(
            metrics
                .fallback_local
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
    }

    #[test]
    fn test_shadow_metrics_snapshot() {
        let metrics = ShadowMetrics::default();

        // Set some values
        metrics
            .forwarded
            .store(100, std::sync::atomic::Ordering::Relaxed);
        metrics
            .skipped
            .store(50, std::sync::atomic::Ordering::Relaxed);
        metrics
            .failed
            .store(5, std::sync::atomic::Ordering::Relaxed);
        metrics
            .fallback_local
            .store(3, std::sync::atomic::Ordering::Relaxed);

        // Get snapshot
        let (forwarded, skipped, failed, fallback) = metrics.snapshot();

        assert_eq!(forwarded, 100);
        assert_eq!(skipped, 50);
        assert_eq!(failed, 5);
        assert_eq!(fallback, 3);
    }

    #[test]
    fn test_shadow_metrics_concurrent_updates() {
        use std::sync::atomic::Ordering;
        use std::thread;

        let metrics = Arc::new(ShadowMetrics::default());

        // Spawn threads for each counter
        let handles: Vec<_> = vec![
            {
                let m = Arc::clone(&metrics);
                thread::spawn(move || {
                    for _ in 0..50 {
                        m.forwarded.fetch_add(1, Ordering::Relaxed);
                    }
                })
            },
            {
                let m = Arc::clone(&metrics);
                thread::spawn(move || {
                    for _ in 0..30 {
                        m.skipped.fetch_add(1, Ordering::Relaxed);
                    }
                })
            },
            {
                let m = Arc::clone(&metrics);
                thread::spawn(move || {
                    for _ in 0..10 {
                        m.failed.fetch_add(1, Ordering::Relaxed);
                    }
                })
            },
            {
                let m = Arc::clone(&metrics);
                thread::spawn(move || {
                    for _ in 0..5 {
                        m.fallback_local.fetch_add(1, Ordering::Relaxed);
                    }
                })
            },
        ];

        for handle in handles {
            handle.join().unwrap();
        }

        let (forwarded, skipped, failed, fallback) = metrics.snapshot();
        assert_eq!(forwarded, 50);
        assert_eq!(skipped, 30);
        assert_eq!(failed, 10);
        assert_eq!(fallback, 5);
    }

    #[test]
    fn test_topic_config_cache_get_by_name() {
        let cache = TopicConfigCache::new();

        // Insert config
        let config = super::super::config::TopicShadowConfig {
            topic_id: 42,
            topic_name: "my-special-topic".to_string(),
            mode: ShadowMode::Shadow,
            forward_percentage: 100,
            external_topic_name: Some("external-topic".to_string()),
            sync_mode: super::super::config::SyncMode::Sync,
            write_mode: WriteMode::DualWrite,
        };
        cache.update(config);

        // Get by name
        let retrieved = cache.get_by_name("my-special-topic");
        assert!(retrieved.is_some());
        let cfg = retrieved.unwrap();
        assert_eq!(cfg.topic_id, 42);
        assert_eq!(cfg.external_topic_name, Some("external-topic".to_string()));

        // Non-existent name
        assert!(cache.get_by_name("nonexistent").is_none());
    }

    #[test]
    fn test_topic_config_cache_all() {
        let cache = TopicConfigCache::new();

        // Insert multiple configs
        for i in 1..=5 {
            let config = super::super::config::TopicShadowConfig {
                topic_id: i,
                topic_name: format!("topic-{}", i),
                mode: ShadowMode::Shadow,
                forward_percentage: 100,
                external_topic_name: None,
                sync_mode: super::super::config::SyncMode::Async,
                write_mode: WriteMode::DualWrite,
            };
            cache.update(config);
        }

        let all = cache.all();
        assert_eq!(all.len(), 5);

        // Verify all topics present
        let names: Vec<String> = all.iter().map(|c| c.topic_name.clone()).collect();
        for i in 1..=5 {
            assert!(names.contains(&format!("topic-{}", i)));
        }
    }

    #[test]
    fn test_topic_config_cache_clear() {
        let cache = TopicConfigCache::new();

        // Add some configs
        for i in 1..=3 {
            let config = super::super::config::TopicShadowConfig {
                topic_id: i,
                topic_name: format!("topic-{}", i),
                mode: ShadowMode::Shadow,
                forward_percentage: 100,
                external_topic_name: None,
                sync_mode: super::super::config::SyncMode::Async,
                write_mode: WriteMode::DualWrite,
            };
            cache.update(config);
        }

        assert_eq!(cache.all().len(), 3);

        // Clear cache
        cache.clear();

        assert_eq!(cache.all().len(), 0);
        assert!(cache.get(1).is_none());
    }

    #[test]
    fn test_topic_config_cache_update_existing() {
        let cache = TopicConfigCache::new();

        // Initial config
        let config1 = super::super::config::TopicShadowConfig {
            topic_id: 1,
            topic_name: "topic".to_string(),
            mode: ShadowMode::Shadow,
            forward_percentage: 50,
            external_topic_name: None,
            sync_mode: super::super::config::SyncMode::Async,
            write_mode: WriteMode::DualWrite,
        };
        cache.update(config1);

        // Verify initial
        assert_eq!(cache.get(1).unwrap().forward_percentage, 50);

        // Update same topic
        let config2 = super::super::config::TopicShadowConfig {
            topic_id: 1,
            topic_name: "topic".to_string(),
            mode: ShadowMode::Shadow,
            forward_percentage: 100,
            external_topic_name: Some("external".to_string()),
            sync_mode: super::super::config::SyncMode::Sync,
            write_mode: WriteMode::ExternalOnly,
        };
        cache.update(config2);

        // Verify updated
        let updated = cache.get(1).unwrap();
        assert_eq!(updated.forward_percentage, 100);
        assert_eq!(updated.external_topic_name, Some("external".to_string()));
        assert!(matches!(
            updated.sync_mode,
            super::super::config::SyncMode::Sync
        ));
        assert!(matches!(updated.write_mode, WriteMode::ExternalOnly));
    }

    #[test]
    fn test_txn_key_epoch_rollover() {
        // Test epoch handling at boundaries
        let key_max_epoch: TxnKey = (1000, i16::MAX);
        let key_min_epoch: TxnKey = (1000, i16::MIN);
        let key_zero_epoch: TxnKey = (1000, 0);

        assert_ne!(key_max_epoch, key_min_epoch);
        assert_ne!(key_max_epoch, key_zero_epoch);
        assert_ne!(key_min_epoch, key_zero_epoch);

        // Verify HashMap handles edge cases
        let mut map: HashMap<TxnKey, String> = HashMap::new();
        map.insert(key_max_epoch, "max".to_string());
        map.insert(key_min_epoch, "min".to_string());
        map.insert(key_zero_epoch, "zero".to_string());

        assert_eq!(map.len(), 3);
        assert_eq!(map.get(&key_max_epoch), Some(&"max".to_string()));
        assert_eq!(map.get(&key_min_epoch), Some(&"min".to_string()));
        assert_eq!(map.get(&key_zero_epoch), Some(&"zero".to_string()));
    }

    #[test]
    fn test_txn_key_producer_id_boundaries() {
        // Test producer_id at boundaries
        let key_max_pid: TxnKey = (i64::MAX, 0);
        let key_min_pid: TxnKey = (i64::MIN, 0);
        let key_zero_pid: TxnKey = (0, 0);

        assert_ne!(key_max_pid, key_min_pid);
        assert_ne!(key_max_pid, key_zero_pid);

        let mut map: HashMap<TxnKey, String> = HashMap::new();
        map.insert(key_max_pid, "max".to_string());
        map.insert(key_min_pid, "min".to_string());

        assert_eq!(map.len(), 2);
    }
}

/// Unit tests of the outbox logic against the in-memory `ShadowDb`.
#[cfg(test)]
mod outbox_tests {
    use super::*;
    use crate::kafka::shadow::config::ShadowMode;
    use crate::kafka::shadow::outbox::fake::InMemoryShadowDb;
    use crate::testing::mocks::MockKafkaStore;

    const T: i32 = 7;

    type Store = ShadowStore<MockKafkaStore, InMemoryShadowDb>;

    fn topic_config(sync_mode: SyncMode, write_mode: WriteMode) -> TopicShadowConfig {
        TopicShadowConfig {
            topic_id: T,
            topic_name: "local".to_string(),
            mode: ShadowMode::Shadow,
            forward_percentage: 100,
            external_topic_name: Some("ext".to_string()),
            sync_mode,
            write_mode,
        }
    }

    struct Harness {
        store: Store,
        forwards: crossbeam_channel::Receiver<ForwardRequest>,
        acks: crossbeam_channel::Sender<ForwardAck>,
    }

    /// A store with shadow mode enabled, `configs` loaded into its cache, and
    /// forward/ack channels (forward capacity `cap`).
    fn harness(mock: MockKafkaStore, configs: Vec<TopicShadowConfig>, cap: usize) -> Harness {
        let db = InMemoryShadowDb::enabled();
        db.add_topic(T, "local", Some("ext"));
        db.state().configs = Some(configs);
        let store = ShadowStore::with_db(mock, db);
        store.load_topic_config_from_db().unwrap();
        let (forward_tx, forwards) = crossbeam_channel::bounded(cap);
        let (acks, ack_rx) = crossbeam_channel::unbounded();
        store.set_forward_channel(forward_tx);
        store.set_ack_channel(ack_rx);
        Harness {
            store,
            forwards,
            acks,
        }
    }

    fn record(key: &[u8], value: &[u8]) -> Record {
        Record {
            key: Some(key.to_vec()),
            value: Some(value.to_vec()),
            headers: vec![],
            timestamp: None,
        }
    }

    fn ok_ack(key: (i32, i32, i64), external_offset: i64) -> ForwardAck {
        ForwardAck::new(key, ForwardOutcome::Delivered(external_offset))
    }

    fn err_ack(key: (i32, i32, i64)) -> ForwardAck {
        ForwardAck::new(key, ForwardOutcome::Failed("broker down".to_string()))
    }

    fn skipped_ack(key: (i32, i32, i64)) -> ForwardAck {
        ForwardAck::new(key, ForwardOutcome::NotAttempted("fenced".to_string()))
    }

    /// Seed messages and pending outbox rows for `keys`.
    fn seed_rows(store: &Store, keys: &[(i32, i32, i64)]) {
        for &k in keys {
            store
                .db()
                .add_message(k, b"k", format!("v{}", k.2).as_bytes());
        }
        store.insert_outbox_rows(keys).unwrap();
    }

    fn async_harness(cap: usize) -> Harness {
        harness(
            MockKafkaStore::new(),
            vec![topic_config(SyncMode::Async, WriteMode::DualWrite)],
            cap,
        )
    }

    /// The records the poll sent, in order.
    fn sent_records(h: &Harness) -> Vec<ForwardRecord> {
        h.forwards
            .try_iter()
            .filter_map(|m| match m {
                ForwardRequest::Record(r) => Some(r),
                ForwardRequest::Hold(_) => None,
            })
            .collect()
    }

    fn offsets(records: &[ForwardRecord]) -> Vec<i64> {
        records.iter().map(|r| r.local_offset).collect()
    }

    #[test]
    fn produce_writes_one_pending_outbox_row_per_record() {
        let mut mock = MockKafkaStore::new();
        mock.expect_insert_records().returning(|_, _, _| Ok(10));
        let h = harness(
            mock,
            vec![topic_config(SyncMode::Async, WriteMode::DualWrite)],
            16,
        );
        h.store.db().add_message((T, 0, 10), b"a", b"1");
        h.store.db().add_message((T, 0, 11), b"b", b"2");

        let base = h
            .store
            .insert_records(T, 0, &[record(b"a", b"1"), record(b"b", b"2")])
            .unwrap();

        assert_eq!(base, 10);
        for off in [10, 11] {
            let row = h.store.db().row((T, 0, off)).expect("outbox row");
            assert_eq!(row.retry_count, 0);
            assert_eq!(row.external_offset, None);
            assert_eq!(row.forwarded_at, None);
            assert!(!row.attempt_open);
        }
        // Async: nothing is sent inline; the poll forwards.
        assert!(h.forwards.try_recv().is_err());
    }

    #[test]
    fn produce_with_shadow_disabled_writes_no_outbox_row() {
        let mut mock = MockKafkaStore::new();
        mock.expect_insert_records().returning(|_, _, _| Ok(0));
        let h = harness(
            mock,
            vec![topic_config(SyncMode::Async, WriteMode::DualWrite)],
            16,
        );
        h.store.db().state().enabled = false;
        h.store.db().add_message((T, 0, 0), b"a", b"1");

        h.store.insert_records(T, 0, &[record(b"a", b"1")]).unwrap();

        assert!(h.store.db().state().tracking.is_empty());
    }

    #[test]
    fn outbox_row_is_only_written_for_an_existing_message() {
        let h = async_harness(16);
        h.store.db().add_message((T, 0, 1), b"a", b"1");

        h.store.insert_outbox_rows(&[(T, 0, 1), (T, 0, 2)]).unwrap();

        assert!(h.store.db().row((T, 0, 1)).is_some());
        assert!(h.store.db().row((T, 0, 2)).is_none());
    }

    #[test]
    fn duplicate_success_ack_counts_once() {
        let h = async_harness(16);
        seed_rows(&h.store, &[(T, 0, 1)]);
        h.store.poll_and_forward_outbox().unwrap();

        sent_records(&h);
        h.acks.send(ok_ack((T, 0, 1), 9)).unwrap();
        h.acks.send(ok_ack((T, 0, 1), 10)).unwrap();
        assert_eq!(h.store.drain_forward_acks(), 2);

        assert_eq!(
            h.store.db().row((T, 0, 1)).unwrap().external_offset,
            Some(9)
        );
        let m = h.store.db().state().metrics[&(T, 0)].clone();
        assert_eq!(m.messages_forwarded, 1);
        assert_eq!(m.last_forwarded_offset, Some(1));
        assert_eq!(h.store.metrics().forwarded.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn poll_claim_failure_is_a_database_error() {
        let h = async_harness(16);
        h.store.db().state().fail_claim = true;
        assert!(matches!(
            h.store.poll_and_forward_outbox(),
            Err(super::super::error::ShadowError::DatabaseError(_))
        ));
    }

    #[test]
    fn commit_enqueues_the_transactions_records_read_from_messages() {
        let mut mock = MockKafkaStore::new();
        mock.expect_commit_transaction()
            .times(1)
            .returning(|_, _, _| Ok(()));
        let h = harness(
            mock,
            vec![topic_config(SyncMode::Async, WriteMode::DualWrite)],
            16,
        );
        for off in [5, 6] {
            let mut s = h.store.db().state();
            s.messages.insert(
                (T, 0, off),
                crate::kafka::shadow::outbox::fake::FakeMessage {
                    txn: Some((100, 0)),
                    txn_pending: true,
                    ..Default::default()
                },
            );
        }
        // Another producer's pending record is not part of this commit.
        h.store.db().state().messages.insert(
            (T, 0, 7),
            crate::kafka::shadow::outbox::fake::FakeMessage {
                txn: Some((200, 0)),
                txn_pending: true,
                ..Default::default()
            },
        );

        // No RAM buffer (e.g. after a worker restart): the DB read still finds them.
        h.store.commit_transaction("txn", 100, 0).unwrap();

        assert!(h.store.db().row((T, 0, 5)).is_some());
        assert!(h.store.db().row((T, 0, 6)).is_some());
        assert!(h.store.db().row((T, 0, 7)).is_none());
    }

    #[test]
    fn commit_falls_back_to_the_ram_buffer_when_the_lookup_fails() {
        let mut mock = MockKafkaStore::new();
        mock.expect_commit_transaction()
            .times(1)
            .returning(|_, _, _| Ok(()));
        let h = harness(
            mock,
            vec![topic_config(SyncMode::Async, WriteMode::DualWrite)],
            16,
        );
        h.store.db().add_message((T, 0, 5), b"k", b"v");
        h.store.db().state().fail_pending_txn_lookup = true;
        h.store.pending_txn_messages.write().unwrap().insert(
            (100, 0),
            PendingTxnBuffer::from(vec![PendingForwardRecord {
                topic_id: T,
                partition_id: 0,
                offset: 5,
                key: None,
            }]),
        );

        h.store.commit_transaction("txn", 100, 0).unwrap();

        assert!(h.store.db().row((T, 0, 5)).is_some());
    }

    #[test]
    fn commit_with_shadow_disabled_enqueues_nothing() {
        let mut mock = MockKafkaStore::new();
        mock.expect_commit_transaction()
            .times(1)
            .returning(|_, _, _| Ok(()));
        let h = harness(
            mock,
            vec![topic_config(SyncMode::Async, WriteMode::DualWrite)],
            16,
        );
        h.store.db().state().messages.insert(
            (T, 0, 5),
            crate::kafka::shadow::outbox::fake::FakeMessage {
                txn: Some((100, 0)),
                txn_pending: true,
                ..Default::default()
            },
        );
        h.store.db().state().enabled = false;

        h.store.commit_transaction("txn", 100, 0).unwrap();

        assert!(h.store.db().state().tracking.is_empty());
    }

    #[test]
    fn config_load_replaces_cache_and_forces_full_forwarding_for_external_primary() {
        let mut external = topic_config(SyncMode::Async, WriteMode::ExternalOnly);
        external.forward_percentage = 30;
        let mut other = topic_config(SyncMode::Async, WriteMode::DualWrite);
        other.topic_id = T + 1;
        other.forward_percentage = 30;
        let h = harness(MockKafkaStore::new(), vec![external, other], 16);

        assert_eq!(
            h.store.topic_cache().get(T).unwrap().forward_percentage,
            100
        );
        assert_eq!(
            h.store.topic_cache().get(T + 1).unwrap().forward_percentage,
            30
        );

        // A deleted row disappears from the cache on the next load.
        h.store.db().state().configs =
            Some(vec![topic_config(SyncMode::Async, WriteMode::DualWrite)]);
        assert_eq!(h.store.load_topic_config_from_db().unwrap(), 1);
        assert!(h.store.topic_cache().get(T + 1).is_none());

        // A failed query keeps the previous cache.
        h.store.db().state().configs = None;
        assert!(h.store.load_topic_config_from_db().is_err());
        assert!(h.store.topic_cache().get(T).is_some());
    }

    #[test]
    fn poll_claims_due_rows_in_order_with_payload_headers_and_timestamp() {
        let h = async_harness(16);
        seed_rows(&h.store, &[(T, 0, 2), (T, 0, 1)]);
        {
            let mut s = h.store.db().state();
            let m = s.messages.get_mut(&(T, 0, 1)).unwrap();
            m.headers = vec![("h".to_string(), b"x".to_vec())];
            m.timestamp_ms = Some(1234);
        }

        assert_eq!(h.store.poll_and_forward_outbox().unwrap(), 2);

        let sent = sent_records(&h);
        assert_eq!(offsets(&sent), vec![1, 2]);
        assert_eq!(sent[0].topic_name, "ext");
        assert_eq!(sent[0].value.as_deref(), Some(&b"v1"[..]));
        assert_eq!(sent[0].headers, vec![("h".to_string(), b"x".to_vec())]);
        assert_eq!(sent[0].timestamp_ms, Some(1234));
        assert!(sent[0].restart, "lowest pending row of the partition");
        assert!(!sent[1].restart);
        for off in [1, 2] {
            let row = h.store.db().row((T, 0, off)).unwrap();
            assert_eq!(row.retry_count, 0, "a claim is not a failure (wave 3)");
            assert!(row.attempt_open);
            assert_eq!(row.forwarded_at, Some(0));
        }
    }

    #[test]
    fn in_flight_rows_are_never_reclaimed_and_only_real_failures_count() {
        let h = async_harness(16);
        seed_rows(&h.store, &[(T, 0, 1), (T, 1, 2)]);
        assert_eq!(h.store.poll_and_forward_outbox().unwrap(), 2);
        sent_records(&h);

        // Still in flight, however long: excluded from the claim (issue #93).
        h.store.db().advance_ms(60_000);
        assert_eq!(h.store.poll_and_forward_outbox().unwrap(), 0);

        // Row (T,0,1) fails, (T,1,2) is delivered.
        h.acks.send(err_ack((T, 0, 1))).unwrap();
        h.acks.send(ok_ack((T, 1, 2), 500)).unwrap();
        assert_eq!(h.store.drain_forward_acks(), 2);
        let failed = h.store.db().row((T, 0, 1)).unwrap();
        assert_eq!(failed.error_message.as_deref(), Some("broker down"));
        assert_eq!(failed.retry_count, 1);
        assert!(!failed.attempt_open);
        assert_eq!(
            h.store.db().row((T, 1, 2)).unwrap().external_offset,
            Some(500)
        );

        // Backing off (100 ms base): not due yet, then due.
        assert_eq!(h.store.poll_and_forward_outbox().unwrap(), 0);
        h.store.db().advance_ms(101);
        assert_eq!(h.store.poll_and_forward_outbox().unwrap(), 1);
        assert_eq!(offsets(&sent_records(&h)), vec![1]);
        assert_eq!(h.store.db().row((T, 0, 1)).unwrap().retry_count, 1);
    }

    #[test]
    fn a_backing_off_row_blocks_later_rows_of_its_partition_only() {
        let h = async_harness(16);
        seed_rows(&h.store, &[(T, 0, 1), (T, 0, 2), (T, 0, 3), (T, 1, 1)]);
        h.store.poll_and_forward_outbox().unwrap();
        sent_records(&h);
        // 1 fails, 2 and 3 are fenced by the forwarder, partition 1 succeeds.
        for ack in [
            err_ack((T, 0, 1)),
            skipped_ack((T, 0, 2)),
            skipped_ack((T, 0, 3)),
            ok_ack((T, 1, 1), 7),
        ] {
            h.acks.send(ack).unwrap();
        }
        h.store.drain_forward_acks();
        // The fenced rows were not attempted: no retry counted.
        assert_eq!(h.store.db().row((T, 0, 2)).unwrap().retry_count, 0);

        // While 1 backs off, 2 and 3 must not overtake it.
        seed_rows(&h.store, &[(T, 1, 2)]);
        assert_eq!(h.store.poll_and_forward_outbox().unwrap(), 1);
        assert_eq!(sent_records(&h)[0].row_key(), (T, 1, 2));

        // After the backoff the partition resumes from 1, in order.
        h.store.db().advance_ms(101);
        h.store.poll_and_forward_outbox().unwrap();
        let sent = sent_records(&h);
        assert_eq!(offsets(&sent), vec![1, 2, 3]);
        assert!(sent[0].restart);
    }

    #[test]
    fn a_lost_attempt_counts_as_a_failure() {
        // The attempt stays open in the table but this worker has no in-flight
        // entry for it (worker restart): claimed again, counting one failure.
        let h = async_harness(16);
        seed_rows(&h.store, &[(T, 0, 1)]);
        h.store
            .db()
            .state()
            .tracking
            .get_mut(&(T, 0, 1))
            .unwrap()
            .attempt_open = true;
        assert_eq!(h.store.poll_and_forward_outbox().unwrap(), 1);
        assert_eq!(h.store.db().row((T, 0, 1)).unwrap().retry_count, 1);
    }

    #[test]
    fn failing_forward_is_dead_lettered_and_external_primary_reads_fall_back() {
        let mut mock = MockKafkaStore::new();
        // Called exactly once: only after the dead letter lifts suppression.
        mock.expect_fetch_records()
            .times(1)
            .returning(|_, _, _, _| Ok(vec![]));
        let h = harness(
            mock,
            vec![topic_config(SyncMode::Async, WriteMode::ExternalOnly)],
            16,
        );
        let cap = 4;
        h.store.db().state().settings.max_retries = cap;
        seed_rows(&h.store, &[(T, 0, 1), (T, 0, 2)]);

        // Healthy external-primary topic: local reads suppressed.
        assert!(h.store.fetch_records(T, 0, 0, 1024).unwrap().is_empty());

        for attempt in 1..=cap {
            h.store.poll_and_forward_outbox().unwrap();
            let sent = sent_records(&h);
            assert_eq!(sent[0].local_offset, 1, "attempt {attempt}");
            h.acks.send(err_ack((T, 0, 1))).unwrap();
            for r in &sent[1..] {
                h.acks.send(skipped_ack(r.row_key())).unwrap();
            }
            h.store.drain_forward_acks();
            h.store.db().advance_ms(OUTBOX_MAX_BACKOFF_MS + 1);
        }

        // At the cap the row is a dead letter: never claimed again, kept pending;
        // the next row is now the head of the partition (restart).
        h.store.poll_and_forward_outbox().unwrap();
        let sent = sent_records(&h);
        assert_eq!(offsets(&sent), vec![2]);
        assert!(sent[0].restart);
        let row = h.store.db().row((T, 0, 1)).unwrap();
        assert_eq!(row.retry_count as i64, cap);
        assert_eq!(row.external_offset, None);
        assert_eq!(h.store.db().row((T, 0, 2)).unwrap().retry_count, 0);
        assert_eq!(h.store.db().state().metrics[&(T, 0)].messages_failed, cap);
        assert!(h.store.topic_has_dead_letters(T));

        // RV-6: the dead letter lifts read suppression (inner is now called) ...
        h.store.fetch_records(T, 0, 0, 1024).unwrap();
        // ... unless strict mode keeps it.
        h.store.db().state().strict = true;
        assert!(h.store.fetch_records(T, 0, 0, 1024).unwrap().is_empty());
    }

    #[test]
    fn raising_the_retry_cap_revives_dead_letters() {
        let h = async_harness(16);
        h.store.db().state().settings.max_retries = 1;
        seed_rows(&h.store, &[(T, 0, 1)]);
        h.store.poll_and_forward_outbox().unwrap();
        h.acks.send(err_ack((T, 0, 1))).unwrap();
        h.store.drain_forward_acks();
        h.store.db().advance_ms(OUTBOX_MAX_BACKOFF_MS + 1);
        sent_records(&h);
        assert_eq!(h.store.poll_and_forward_outbox().unwrap(), 0);

        h.store.db().state().settings.max_retries = 2;
        assert_eq!(h.store.poll_and_forward_outbox().unwrap(), 1);
    }

    #[test]
    fn batch_size_bounds_the_claim_and_requests_another_poll() {
        let h = async_harness(64);
        h.store.db().state().settings.batch_size = 2;
        seed_rows(&h.store, &[(T, 0, 1), (T, 0, 2), (T, 0, 3)]);

        assert_eq!(h.store.poll_and_forward_outbox().unwrap(), 2);
        assert!(
            h.store.outbox_tick_due(Duration::ZERO),
            "full claim: poll again"
        );
        assert_eq!(h.store.poll_and_forward_outbox().unwrap(), 1);
        assert!(!h.store.outbox_tick_due(Duration::ZERO));
        assert!(h.store.outbox_tick_due(OUTBOX_POLL_INTERVAL));
    }

    #[test]
    fn full_forward_channel_closes_the_attempt_without_counting_it() {
        let h = async_harness(1);
        seed_rows(&h.store, &[(T, 0, 1), (T, 0, 2), (T, 0, 3)]);

        assert_eq!(h.store.poll_and_forward_outbox().unwrap(), 1);
        let (topics, _, offs) = h.store.inflight.live_keys();
        assert_eq!(topics.len(), 1, "undelivered rows must not stay in flight");
        assert_eq!(offs, vec![1]);
        for off in [2, 3] {
            let row = h.store.db().row((T, 0, off)).unwrap();
            assert!(!row.attempt_open);
            assert_eq!(row.retry_count, 0);
        }

        // They are due again at once.
        sent_records(&h);
        assert_eq!(h.store.poll_and_forward_outbox().unwrap(), 1);
        assert_eq!(h.store.db().row((T, 0, 2)).unwrap().retry_count, 0);
    }

    #[test]
    fn poll_without_forward_channel_is_a_no_op() {
        let db = InMemoryShadowDb::enabled();
        db.add_topic(T, "local", None);
        let store = ShadowStore::with_db(MockKafkaStore::new(), db);
        seed_rows(&store, &[(T, 0, 1)]);

        assert_eq!(store.poll_and_forward_outbox().unwrap(), 0);
        assert!(!store.db().row((T, 0, 1)).unwrap().attempt_open);
    }

    #[test]
    fn skipped_records_are_counted_in_shadow_metrics() {
        let mut mock = MockKafkaStore::new();
        mock.expect_insert_records().returning(|_, _, _| Ok(0));
        let mut cfg = topic_config(SyncMode::Async, WriteMode::DualWrite);
        cfg.forward_percentage = 50;
        let h = harness(mock, vec![cfg.clone()], 16);
        let records: Vec<Record> = (0..20).map(|i| record(&[i], b"v")).collect();
        let expected = (0..20)
            .filter(|&i| make_forward_decision(Some(&[i as u8]), i, 50) == ForwardDecision::Skip)
            .count() as i64;
        assert!(expected > 0);
        h.store.insert_records(T, 0, &records).unwrap();
        assert_eq!(
            h.store.db().state().metrics[&(T, 0)].messages_skipped,
            expected
        );

        // shadow_metrics_enabled = off: the table is not written.
        h.store.db().state().settings.metrics_enabled = false;
        h.store.insert_records(T, 0, &records).unwrap();
        assert_eq!(
            h.store.db().state().metrics[&(T, 0)].messages_skipped,
            expected
        );
    }

    #[test]
    fn metrics_disabled_still_finalizes_rows() {
        let h = async_harness(16);
        h.store.db().state().settings.metrics_enabled = false;
        seed_rows(&h.store, &[(T, 0, 1)]);
        h.store.poll_and_forward_outbox().unwrap();
        h.acks.send(ok_ack((T, 0, 1), 3)).unwrap();
        h.store.drain_forward_acks();
        assert_eq!(
            h.store.db().row((T, 0, 1)).unwrap().external_offset,
            Some(3)
        );
        assert!(!h.store.db().state().metrics.contains_key(&(T, 0)));
    }

    #[test]
    fn outbox_failures_back_off_and_log_sparsely() {
        let h = async_harness(16);
        assert!(h.store.outbox_tick_due(OUTBOX_POLL_INTERVAL));
        assert!(h.store.note_outbox_tick(false), "first failure is logged");
        assert!(!h.store.outbox_tick_due(OUTBOX_POLL_INTERVAL), "paused");
        assert!(h.store.note_outbox_tick(false), "2nd");
        assert!(!h.store.note_outbox_tick(false), "3rd is not logged");
        assert!(h.store.note_outbox_tick(false), "4th");
        // Success resets.
        assert!(!h.store.note_outbox_tick(true));
        assert!(h.store.outbox_tick_due(OUTBOX_POLL_INTERVAL));
    }

    fn sync_harness() -> Harness {
        let mut mock = MockKafkaStore::new();
        mock.expect_insert_records().returning(|_, _, _| Ok(10));
        let h = harness(
            mock,
            vec![topic_config(SyncMode::Sync, WriteMode::DualWrite)],
            16,
        );
        h.store.db().add_message((T, 0, 10), b"a", b"1");
        h.store.db().state().current_xid = 77;
        h
    }

    #[test]
    fn sync_produce_forwards_nothing_before_commit() {
        let h = sync_harness();

        h.store.begin_request();
        h.store.insert_records(T, 0, &[record(b"a", b"1")]).unwrap();

        // The outbox row is written in the request's transaction, but nothing is
        // sent while that transaction can still roll back.
        assert!(h.store.db().row((T, 0, 10)).is_some());
        assert!(h.forwards.try_recv().is_err());
        assert_eq!(h.store.sync_pending.lock().unwrap().len(), 1);
    }

    #[test]
    fn committed_sync_rows_are_reported_per_request_and_request_a_poll() {
        let h = sync_harness();
        let slot = h.store.begin_request();
        h.store.insert_records(T, 0, &[record(b"a", b"1")]).unwrap();
        let other = h.store.begin_request();
        assert_ne!(slot, other);
        h.store.db().state().committed_xids.insert(77);

        let holds = h.store.take_sync_holds();
        assert_eq!(holds.get(&slot), Some(&vec![(T, 0, 10)]));
        assert!(!holds.contains_key(&other));
        assert!(h.store.sync_pending.lock().unwrap().is_empty());
        assert!(h.store.outbox_tick_due(Duration::ZERO));
        // The DB thread did not wait for anything; the poll sends the row.
        assert_eq!(h.store.poll_and_forward_outbox().unwrap(), 1);
    }

    #[test]
    fn rolled_back_sync_rows_are_not_held() {
        let h = sync_harness();
        h.store.begin_request();
        h.store.insert_records(T, 0, &[record(b"a", b"1")]).unwrap();
        // xid 77 never committed (the request's subtransaction rolled back).
        assert!(h.store.take_sync_holds().is_empty());
        assert!(!h.store.outbox_tick_due(Duration::ZERO));
    }

    #[test]
    fn sync_timeout_zero_holds_nothing() {
        let h = sync_harness();
        h.store.db().state().settings.sync_timeout_ms = 0;
        h.store.begin_request();
        h.store.insert_records(T, 0, &[record(b"a", b"1")]).unwrap();
        h.store.db().state().committed_xids.insert(77);
        assert!(h.store.take_sync_holds().is_empty());
    }

    #[test]
    fn hold_goes_to_the_forwarder_or_is_released_at_once() {
        use std::sync::atomic::AtomicUsize;
        let h = sync_harness();
        let released = Arc::new(AtomicUsize::new(0));
        let r = released.clone();
        h.store.hold_response(
            vec![(T, 0, 10)],
            HeldRelease::new(move || {
                r.fetch_add(1, Ordering::SeqCst);
            }),
        );
        match h.forwards.try_recv().unwrap() {
            ForwardRequest::Hold(hold) => {
                assert_eq!(hold.keys, vec![(T, 0, 10)]);
                assert_eq!(hold.timeout, Duration::from_millis(5_000));
                assert_eq!(released.load(Ordering::SeqCst), 0);
                hold.release.release();
            }
            other => panic!("expected a hold, got {:?}", other),
        }
        assert_eq!(released.load(Ordering::SeqCst), 1);

        // No forwarder: released immediately (the client is never left hanging).
        let db = InMemoryShadowDb::enabled();
        let store = ShadowStore::with_db(MockKafkaStore::new(), db);
        let r = released.clone();
        store.hold_response(
            vec![(T, 0, 10)],
            HeldRelease::new(move || {
                r.fetch_add(1, Ordering::SeqCst);
            }),
        );
        assert_eq!(released.load(Ordering::SeqCst), 2);
    }
}
