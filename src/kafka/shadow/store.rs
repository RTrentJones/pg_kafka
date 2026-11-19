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
use super::license::LicenseValidator;
use super::outbox::{
    OutboxClaim, PendingForwardRecord, ShadowDb, SpiShadowDb, OUTBOX_MAX_BACKOFF_MS,
};
use super::primary::PrimaryStatus;
use super::routing::{make_forward_decision, ForwardDecision};
use super::{
    lock, read_lock, write_lock, ForwardAck, ForwardOutcome, ForwardRecord, ForwardRequest,
    HeldRelease, RowKey, SyncHold,
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
use std::sync::{Mutex, RwLock};
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

/// Pending transactional records awaiting commit, by producer.
type PendingTxnMessages = RwLock<HashMap<TxnKey, PendingTxnBuffer>>;

/// In-memory forwarding counters since the last [`ShadowStore::flush_metrics_to_db`]
/// (the durable per-partition counts are in `kafka.shadow_metrics`).
#[derive(Debug, Default)]
pub struct ShadowMetrics {
    /// Messages delivered to the external cluster.
    pub forwarded: AtomicU64,
    /// Messages sampled out by percentage routing.
    pub skipped: AtomicU64,
    /// Failed forward attempts.
    pub failed: AtomicU64,
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
    topic_cache: TopicConfigCache,
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
            topic_cache: TopicConfigCache::new(),
            primary_status: Mutex::new(PrimaryStatus::new()),
            metrics: ShadowMetrics::default(),
            forward_tx: RwLock::new(None),
            forward_ack_rx: RwLock::new(None),
            pending_txn_messages: RwLock::new(HashMap::new()),
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
        let mut guard = write_lock(&self.forward_tx);
        *guard = Some(tx);
    }

    /// Install the channel on which the network thread returns forward acks.
    ///
    /// Must be called after construction to enable the durable outbox; the DB
    /// thread drains this on its outbox tick to finalize
    /// `kafka.shadow_tracking` rows. (SH-9)
    pub fn set_ack_channel(&self, rx: crossbeam_channel::Receiver<ForwardAck>) {
        let mut guard = write_lock(&self.forward_ack_rx);
        *guard = Some(rx);
    }

    /// Set license key and initialize validator (Commercial License)
    pub fn set_license_key(&self, license_key: &str) {
        let validator = LicenseValidator::new(license_key);
        let mut guard = write_lock(&self.license);
        *guard = Some(validator);
    }

    /// Check license and emit rate-limited warnings if shadow mode is active
    fn check_license(&self) {
        let guard = read_lock(&self.license);
        if let Some(ref validator) = *guard {
            validator.check_and_warn();
        }
    }

    /// The per-topic configuration cache.
    #[cfg(test)]
    pub fn topic_cache(&self) -> &TopicConfigCache {
        &self.topic_cache
    }

    /// The in-memory forwarding counters.
    #[cfg(test)]
    pub fn metrics(&self) -> &ShadowMetrics {
        &self.metrics
    }

    /// Check if we're running on the primary (not a standby)
    fn is_primary(&self) -> bool {
        let mut status = lock(&self.primary_status);
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
        let mut status = lock(&self.primary_status);
        status.refresh()
    }

    /// Check if shadow mode is enabled globally by reading the GUC directly
    fn is_enabled(&self) -> bool {
        self.db.shadow_mode_enabled()
    }

    // ===== Durable forwarding outbox (SH-9, SH-7, SH-8, SH-14, SH-15) =====

    /// Try to hand a message to the network thread. Never blocks; gives the
    /// message back if no channel is wired up or it is full.
    fn try_send_forward(
        &self,
        msg: ForwardRequest,
    ) -> std::result::Result<(), Box<ForwardRequest>> {
        let guard = read_lock(&self.forward_tx);
        match guard.as_ref() {
            Some(tx) => tx.try_send(msg).map_err(|e| Box::new(e.into_inner())),
            None => Err(Box::new(msg)),
        }
    }

    /// Count sampled-out records of one partition in
    /// `kafka.shadow_metrics.messages_skipped` (when
    /// `pg_kafka.shadow_metrics_enabled`) and in the in-RAM counter.
    fn count_skipped(&self, (topic_id, partition_id): (i32, i32), n: u64, metrics_enabled: bool) {
        if n == 0 {
            return;
        }
        self.metrics.skipped.fetch_add(n, Ordering::Relaxed);
        if !metrics_enabled {
            return;
        }
        let Ok(n) = i64::try_from(n) else {
            return;
        };
        if let Err(e) = self.db.record_skipped(topic_id, partition_id, n) {
            tracing::warn!("Shadow: failed to count skipped records: {e:?}");
        }
    }

    /// Enqueue the records of a just-committed transaction into the durable
    /// outbox (RA-1/RA-2).
    ///
    /// Each record the topic's current configuration selects gets a pending
    /// `shadow_tracking` row written inside the commit transaction; the outbox
    /// poll then forwards it like a plain produce: non-blocking, at-least-once,
    /// re-partitioned by key, and durably retried (which matters most for
    /// external-primary topics, whose local reads are suppressed).
    fn enqueue_committed_txn_outbox(&self, records: &[PendingForwardRecord]) {
        if records.is_empty() {
            return;
        }
        self.check_license();

        let mut rows: Vec<RowKey> = Vec::with_capacity(records.len());
        let mut skipped: HashMap<(i32, i32), u64> = HashMap::new();
        for r in records {
            // The topic may have stopped forwarding since the records were
            // buffered (config reload): decide at commit time.
            let percentage = match self.topic_cache.get(r.row.topic_id) {
                Some(c) if c.should_forward() => c.forward_percentage,
                _ => continue,
            };
            match make_forward_decision(r.key.as_deref(), r.row.local_offset, percentage) {
                ForwardDecision::Skip => *skipped.entry(r.row.partition()).or_default() += 1,
                ForwardDecision::Forward => rows.push(r.row),
            }
        }
        let metrics_enabled = self.db.settings().metrics_enabled;
        for (partition, n) in skipped {
            self.count_skipped(partition, n, metrics_enabled);
        }

        if rows.is_empty() {
            return;
        }
        if let Err(e) = self.db.insert_outbox_rows(&rows) {
            tracing::warn!(
                "Shadow: failed to enqueue {} committed txn outbox row(s): {e:?}",
                rows.len()
            );
        }
    }

    /// Write durable outbox rows for a freshly-produced batch (SH-9).
    ///
    /// For each record percentage routing selects (SH-8), insert a *pending*
    /// `kafka.shadow_tracking` row in the CURRENT transaction (this runs inside
    /// the produce request's subtransaction), so the outbox is exactly
    /// consistent with what was persisted locally. No payload is copied: it
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
        let settings = self.db.settings();
        self.check_license();

        let mut rows: Vec<RowKey> = Vec::with_capacity(records.len());
        let mut skipped = 0u64;
        for (local_offset, record) in (base_offset..).zip(records) {
            match make_forward_decision(
                record.key.as_deref(),
                local_offset,
                config.forward_percentage,
            ) {
                ForwardDecision::Skip => skipped += 1,
                ForwardDecision::Forward => {
                    rows.push(RowKey::new(topic_id, partition_id, local_offset));
                }
            }
        }
        self.count_skipped((topic_id, partition_id), skipped, settings.metrics_enabled);

        if rows.is_empty() {
            return;
        }
        if let Err(e) = self.db.insert_outbox_rows(&rows) {
            tracing::warn!(
                "Shadow: failed to write {} outbox row(s) for topic_id={topic_id} partition={partition_id}: {e:?}",
                rows.len()
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
            let mut queue = lock(&self.sync_pending);
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
        let queued = std::mem::take(&mut *lock(&self.sync_pending));
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
        let failures = lock(&self.outbox_failures);
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
        let mut failures = lock(&self.outbox_failures);
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
            let guard = read_lock(&self.forward_tx);
            if guard.is_none() {
                return Ok(0);
            }
        }

        let settings = self.db.settings();
        let room = MAX_INFLIGHT.saturating_sub(self.inflight.len());
        let limit = usize::try_from(settings.batch_size.max(1))
            .unwrap_or(usize::MAX)
            .min(room);
        if limit == 0 {
            return Ok(0);
        }

        let records: Vec<ForwardRecord> = self
            .db
            .claim_due_outbox_rows(&OutboxClaim {
                batch_limit: i64::try_from(limit).unwrap_or(i64::MAX),
                max_retries: settings.max_retries,
                backoff_base_ms: settings.retry_backoff_ms,
                backoff_max_ms: OUTBOX_MAX_BACKOFF_MS,
                inflight: self.inflight.live_keys(),
            })
            .map_err(|e: KafkaError| {
                super::error::ShadowError::DatabaseError(format!("outbox poll failed: {e}"))
            })?;
        let claimed = records.len();

        let mut dispatched = 0usize;
        let mut not_sent: Vec<RowKey> = Vec::new();
        for record in records {
            let key = record.row;
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
            let guard = read_lock(&self.forward_ack_rx);
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
            self.inflight.complete(&ack.row);
            match &ack.outcome {
                ForwardOutcome::Delivered(x) => delivered.push((ack.row, *x)),
                ForwardOutcome::Failed(e) => failed.push((ack.row, e.clone())),
                ForwardOutcome::NotAttempted(_) => not_attempted.push(ack.row),
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
                        key.topic_id,
                        key.partition_id,
                        key.local_offset,
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
        self.topic_cache.replace(new_configs);

        Ok(count)
    }

    /// Log the in-memory counters (if any moved) and reset them. Called
    /// periodically by the worker.
    #[expect(
        clippy::unnecessary_wraps,
        reason = "the worker handles this like its other fallible maintenance steps"
    )]
    pub fn flush_metrics_to_db(&self) -> super::error::ShadowResult<()> {
        let forwarded = self.metrics.forwarded.swap(0, Ordering::Relaxed);
        let skipped = self.metrics.skipped.swap(0, Ordering::Relaxed);
        let failed = self.metrics.failed.swap(0, Ordering::Relaxed);
        if forwarded > 0 || skipped > 0 || failed > 0 {
            tracing::info!(
                "Shadow metrics: forwarded={forwarded}, skipped={skipped}, failed={failed}"
            );
        }
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
            let mut pending = write_lock(&self.pending_txn_messages);
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
            let pending_records =
                (base_offset..)
                    .zip(records)
                    .map(|(local_offset, r)| PendingForwardRecord {
                        row: RowKey::new(topic_id, partition_id, local_offset),
                        key: r.key.clone(),
                    });

            let key = (producer_id, producer_epoch);
            let mut pending = write_lock(&self.pending_txn_messages);
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
            let mut pending = write_lock(&self.pending_txn_messages);
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
            let mut pending = write_lock(&self.pending_txn_messages);
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
            let mut pending = write_lock(&self.pending_txn_messages);
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
mod tests;

#[cfg(test)]
mod outbox_tests;
