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
//! - **Sync (bounded)**: the produce additionally waits up to a short cap for
//!   the ack before returning. On timeout it returns anyway — the row stays
//!   durable and the poll retries it. This replaces the old unbounded
//!   `block_on`, which could stall every client for minutes (SH-14).
//!
//! ## Thread Safety
//!
//! The inner store and producer are wrapped appropriately for safe access.
//! The forwarder maintains internal mutable state for primary detection caching.

use super::config::{ShadowConfig, SyncMode, TopicConfigCache, TopicShadowConfig, WriteMode};
use super::forwarder::ForwardDecision;
use super::license::LicenseValidator;
use super::primary::PrimaryStatus;
use super::producer::ShadowProducer;
use super::routing::make_forward_decision;
use super::{ForwardAck, ForwardRequest};
use crate::kafka::error::{KafkaError, Result};
use crate::kafka::messages::Record;
use crate::kafka::storage::{
    CommittedOffset, FetchedMessage, IsolationLevel, KafkaStore, TopicMetadata, TransactionState,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

/// A committed transactional record's outbox pointer.
///
/// Buffered during `insert_transactional_records()` and enqueued into the
/// durable `kafka.shadow_tracking` outbox when `commit_transaction()` is called
/// (RA-1/RA-2). Only the row key + record key are needed — the payload is
/// re-read from `kafka.messages` by the forwarder, exactly like a non-txn
/// produce, so the buffer no longer carries the value/headers/timestamp.
#[derive(Clone, Debug)]
struct PendingForwardRecord {
    /// Topic ID (outbox row key + per-topic config lookup)
    topic_id: i32,
    /// Partition ID (outbox row key)
    partition_id: i32,
    /// Local offset the record was committed at (outbox row key)
    offset: i64,
    /// Record key — used only for percentage routing at enqueue time
    key: Option<Vec<u8>>,
}

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

/// Max `shadow_tracking` outbox rows claimed and forwarded per poll cycle.
const OUTBOX_BATCH_LIMIT: i64 = 256;

/// A claimed-but-unacked outbox row is re-attempted after this long — it covers
/// a crash between claim and ack, or a lost ack. `forwarded_at` doubles as the
/// last-attempt stamp until `external_offset` is set (which marks the row done).
const OUTBOX_RETRY_INTERVAL_MS: i64 = 5_000;

/// After this many failed forward attempts a row is left as a durable
/// dead-letter (RA-4): the poll stops re-dispatching it (no infinite retry of a
/// poison message — e.g. a payload over the external broker's max.message.bytes),
/// but the row stays in `kafka.shadow_tracking` (pending, with `retry_count` and
/// `error_message` set) so it is visible and can be re-queued with
/// `kafka.replay_shadow_messages` once the cause is fixed.
const MAX_FORWARD_RETRIES: i64 = 10;

/// Upper bound a bounded-sync produce waits for external confirmation before
/// returning anyway. The outbox row stays durable and the poll retries it, so
/// this is a latency cap, never a correctness boundary (SH-14). Kept short
/// because the wait holds the single DB thread; a healthy broker acks in
/// milliseconds, and a slow one just degrades sync toward the async poll.
const SYNC_FORWARD_WAIT_MS: u64 = 5_000;

/// How long a successful producer health check is trusted before it is
/// re-verified.
///
/// `is_healthy()` does a blocking metadata fetch (up to 5s). Running it on the
/// `ensure_producer` fast path meant every single produce paid that round-trip
/// — and in sync mode, on the DB thread, that stalls all clients. rdkafka
/// already reconnects transparently, so a periodic re-check is enough: within
/// this window we trust the cached producer and skip the fetch entirely.
const PRODUCER_HEALTH_TTL: Duration = Duration::from_secs(30);

/// A KafkaStore wrapper that adds shadow mode forwarding
///
/// Wraps an inner store (typically PostgresStore) and forwards produce
/// operations to an external Kafka cluster based on per-topic configuration.
///
/// The producer is lazily initialized on first use when shadow mode is enabled.
/// This allows enabling/disabling shadow mode at runtime via GUC changes.
pub struct ShadowStore<S: KafkaStore> {
    /// The wrapped inner store
    inner: S,
    /// Producer for sending to external Kafka (lazily initialized)
    producer: RwLock<Option<Arc<ShadowProducer>>>,
    /// Bootstrap servers used to create the cached producer (for change detection)
    producer_bootstrap_servers: RwLock<Option<String>>,
    /// When the cached producer was last verified healthy (see
    /// `PRODUCER_HEALTH_TTL`). `None` until the first check.
    producer_health_checked_at: Mutex<Option<Instant>>,
    /// Topic configuration cache
    topic_cache: Arc<TopicConfigCache>,
    /// Primary status checker
    primary_status: Mutex<PrimaryStatus>,
    /// Shadow forwarding metrics
    metrics: ShadowMetrics,
    /// Channel for async forwarding to network thread (Phase 11)
    forward_tx: RwLock<Option<crossbeam_channel::Sender<ForwardRequest>>>,
    /// Channel on which the network thread returns forward results (acks) for
    /// the durable outbox. Drained on the DB thread (periodic poll + bounded
    /// sync) to finalize `kafka.shadow_tracking` rows. (SH-9)
    forward_ack_rx: RwLock<Option<crossbeam_channel::Receiver<ForwardAck>>>,
    /// Pending transactional records awaiting commit for shadow forwarding
    pending_txn_messages: PendingTxnMessages,
    /// License validator for shadow mode (Commercial License)
    license: RwLock<Option<LicenseValidator>>,
}

impl<S: KafkaStore> ShadowStore<S> {
    /// Create a new ShadowStore wrapping the given inner store
    ///
    /// The producer is NOT created here - it's lazily initialized on first use
    /// when shadow mode is enabled. This allows enabling shadow mode at runtime
    /// via GUC changes without requiring a restart.
    ///
    /// # Arguments
    /// * `inner` - The store to wrap (typically PostgresStore)
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            producer: RwLock::new(None),
            producer_bootstrap_servers: RwLock::new(None),
            producer_health_checked_at: Mutex::new(None),
            topic_cache: Arc::new(TopicConfigCache::new()),
            primary_status: Mutex::new(PrimaryStatus::new()),
            metrics: ShadowMetrics::new(),
            forward_tx: RwLock::new(None),
            forward_ack_rx: RwLock::new(None),
            pending_txn_messages: Arc::new(RwLock::new(HashMap::new())),
            license: RwLock::new(None),
        }
    }

    /// Create a new ShadowStore with RuntimeContext for config access
    ///
    /// Note: RuntimeContext is no longer used for shadow producer config.
    /// The producer now loads config directly from GUCs to detect changes.
    /// This constructor is kept for API compatibility but simply calls new().
    ///
    /// # Arguments
    /// * `inner` - The store to wrap (typically PostgresStore)
    /// * `_runtime_context` - Unused (kept for API compatibility)
    pub fn with_context(inner: S, _runtime_context: Arc<crate::kafka::RuntimeContext>) -> Self {
        Self::new(inner)
    }

    /// Set the forward channel for async forwarding
    ///
    /// Must be called after construction to enable async forwarding mode.
    /// When sync_mode is Async, messages are sent via this channel to the
    /// network thread for non-blocking forwarding.
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
    /// thread drains this on its periodic poll (and during a bounded-sync wait)
    /// to finalize `kafka.shadow_tracking` rows. (SH-9)
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
        if crate::config::SHADOW_EXTERNAL_PRIMARY_STRICT.get() {
            return true;
        }
        !self.topic_has_dead_letters(topic_id)
    }

    /// True if the topic has a forward that exhausted the retry cap and will never be
    /// delivered externally (`external_offset` still NULL, `retry_count >= cap`).
    fn topic_has_dead_letters(&self, topic_id: i32) -> bool {
        use pgrx::prelude::*;
        let result: Result<bool> = Spi::connect(|client| {
            let table = client.select(
                "SELECT 1 FROM kafka.shadow_tracking \
                 WHERE topic_id = $1 AND external_offset IS NULL AND retry_count >= $2 \
                 LIMIT 1",
                Some(1),
                &[topic_id.into(), MAX_FORWARD_RETRIES.into()],
            )?;
            Ok(!table.is_empty())
        });
        result.unwrap_or(false)
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

    /// Whether the cached producer's last health check is still within
    /// `PRODUCER_HEALTH_TTL` (SH-5). When fresh, the fast path trusts the
    /// producer and skips the blocking metadata fetch.
    fn producer_health_fresh(&self) -> bool {
        let guard = self
            .producer_health_checked_at
            .lock()
            .unwrap_or_else(|poisoned| {
                tracing::warn!("producer_health_checked_at lock was poisoned, recovering");
                poisoned.into_inner()
            });
        matches!(*guard, Some(at) if at.elapsed() < PRODUCER_HEALTH_TTL)
    }

    /// Record that the producer was just verified (or created) healthy, so the
    /// next `PRODUCER_HEALTH_TTL` window can skip the metadata fetch.
    fn mark_producer_healthy(&self) {
        let mut guard = self
            .producer_health_checked_at
            .lock()
            .unwrap_or_else(|poisoned| {
                tracing::warn!("producer_health_checked_at lock was poisoned, recovering");
                poisoned.into_inner()
            });
        *guard = Some(Instant::now());
    }

    /// Clear the cached health timestamp so the next call re-verifies (used
    /// when a cached producer is found unhealthy or discarded).
    fn invalidate_producer_health(&self) {
        let mut guard = self
            .producer_health_checked_at
            .lock()
            .unwrap_or_else(|poisoned| {
                tracing::warn!("producer_health_checked_at lock was poisoned, recovering");
                poisoned.into_inner()
            });
        *guard = None;
    }

    /// Check if shadow mode is enabled globally by reading the GUC directly
    #[cfg(not(test))]
    fn is_enabled(&self) -> bool {
        crate::config::SHADOW_MODE_ENABLED.get()
    }

    /// Test version - always returns false unless overridden
    #[cfg(test)]
    fn is_enabled(&self) -> bool {
        false
    }

    /// Ensure the producer is initialized, creating it if necessary
    ///
    /// Returns Some(producer) if shadow mode is properly configured,
    /// None if configuration is missing or invalid.
    ///
    /// This function also handles config changes: if the bootstrap_servers
    /// GUC has changed since the producer was created, the old producer
    /// is discarded and a new one is created with the updated config.
    fn ensure_producer(&self) -> Option<Arc<ShadowProducer>> {
        // Always load fresh config directly from GUCs for shadow producer
        // We need fresh values to detect config changes (e.g., bootstrap_servers changed via pg_reload_conf)
        // RuntimeContext caches config and doesn't refresh on SIGHUP, so we bypass it here
        #[cfg(not(test))]
        let mut config = {
            use crate::config::Config;
            let cfg = Config::load();
            ShadowConfig::from_config(&Arc::new(cfg))
        };

        #[cfg(test)]
        let mut config = ShadowConfig::default();

        // Fast path: check if already initialized with same config
        {
            let guard = self.producer.read().unwrap_or_else(|poisoned| {
                tracing::warn!("producer read lock was poisoned, recovering");
                poisoned.into_inner()
            });
            let bs_guard = self
                .producer_bootstrap_servers
                .read()
                .unwrap_or_else(|poisoned| {
                    tracing::warn!("producer_bootstrap_servers read lock was poisoned, recovering");
                    poisoned.into_inner()
                });

            if let Some(ref producer) = *guard {
                // Check if config has changed
                if let Some(ref cached_bs) = *bs_guard {
                    if cached_bs == &config.bootstrap_servers {
                        // SH-5: within the health TTL, trust the cached producer
                        // and skip the blocking metadata fetch entirely.
                        if self.producer_health_fresh() {
                            return Some(producer.clone());
                        }
                        // TTL elapsed: re-verify once, then trust again.
                        if producer.is_healthy() {
                            self.mark_producer_healthy();
                            return Some(producer.clone());
                        }
                        // Producer unhealthy, fall through to recreate
                        self.invalidate_producer_health();
                        crate::pg_log!("Shadow: cached producer unhealthy, recreating");
                    } else {
                        // Config changed - need to recreate producer
                        crate::pg_log!(
                            "Shadow: bootstrap_servers changed from '{}' to '{}', recreating producer",
                            cached_bs,
                            config.bootstrap_servers
                        );
                    }
                }
            }
        }

        // Slow path: need to initialize or reinitialize
        let mut guard = self.producer.write().unwrap_or_else(|poisoned| {
            tracing::warn!("producer write lock was poisoned, recovering");
            poisoned.into_inner()
        });
        let mut bs_guard = self
            .producer_bootstrap_servers
            .write()
            .unwrap_or_else(|poisoned| {
                tracing::warn!("producer_bootstrap_servers write lock was poisoned, recovering");
                poisoned.into_inner()
            });

        // Double-check after acquiring write lock (another thread may have
        // created/verified the producer while we waited). Same TTL gate as the
        // fast path so we don't pay a second metadata fetch.
        if let Some(ref producer) = *guard {
            if let Some(ref cached_bs) = *bs_guard {
                if cached_bs == &config.bootstrap_servers
                    && (self.producer_health_fresh() || producer.is_healthy())
                {
                    self.mark_producer_healthy();
                    return Some(producer.clone());
                }
            }
        }

        // Clear old producer if exists (config changed or unhealthy)
        *guard = None;
        *bs_guard = None;
        self.invalidate_producer_health();

        if !config.is_configured() {
            crate::pg_warning!(
                "⚠️  SHADOW MODE WARNING: \
                 pg_kafka.shadow_mode_enabled={} \
                 pg_kafka.shadow_bootstrap_servers='{}' \
                 Shadow mode is NOT properly configured - messages will NOT be forwarded!",
                config.enabled,
                config.bootstrap_servers
            );
            return None;
        }

        // SH-1: validate (and normalize/clamp) the config before building a
        // producer from it. Previously `validate()` ran only in tests, so an
        // invalid security_protocol / sasl_mechanism / bootstrap_servers would
        // be handed straight to rdkafka. Run it here on the slow path only, so
        // the security warnings it emits fire once per (re)create rather than
        // on every produce.
        if let Err(e) = config.validate() {
            crate::pg_warning!(
                "⚠️  SHADOW MODE CONFIG INVALID: {} — messages will NOT be forwarded. \
                 Fix pg_kafka.shadow_* settings and reload.",
                e
            );
            return None;
        }

        tracing::info!(
            "Shadow mode: Attempting to connect to external Kafka at {}",
            config.bootstrap_servers
        );

        // Create the producer
        match ShadowProducer::new(Arc::new(config.clone())) {
            Ok(producer) => {
                let producer = Arc::new(producer);
                *guard = Some(producer.clone());
                *bs_guard = Some(config.bootstrap_servers.clone());
                // A freshly created producer is healthy; start its TTL window
                // so the next produce doesn't immediately re-probe (SH-5).
                self.mark_producer_healthy();
                tracing::info!(
                    "✅ Shadow producer successfully initialized and connected to {}",
                    config.bootstrap_servers
                );
                Some(producer)
            }
            Err(e) => {
                crate::pg_warning!(
                    "⚠️  SHADOW MODE CONNECTION FAILED: Cannot connect to external Kafka at '{}'. Error: {:?}\n\
                     Messages will be stored locally but NOT forwarded. Check:\n\
                     1. Is the Kafka broker accessible from PostgreSQL?\n\
                     2. Are bootstrap_servers correct? (use 'localhost:9093' if Kafka is on host, not 'external-kafka:9093')\n\
                     3. Are SASL/SSL credentials correct?\n\
                     4. Is the broker accepting connections?",
                    config.bootstrap_servers,
                    e
                );
                None
            }
        }
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

    /// Flush the producer if initialized
    pub fn flush(&self) -> super::error::ShadowResult<()> {
        if let Some(producer) = self.ensure_producer() {
            producer.flush(Duration::from_secs(30))?;
        }
        Ok(())
    }

    // ===== Durable forwarding outbox (SH-9, SH-7, SH-8, SH-14, SH-15) =====

    /// Try to hand a forward request to the network thread. Returns false if no
    /// channel is wired up or it is momentarily full (the caller leaves the row
    /// pending for the next poll). Never blocks.
    #[cfg(not(test))]
    fn try_send_forward(&self, req: ForwardRequest) -> bool {
        let guard = self.forward_tx.read().unwrap_or_else(|poisoned| {
            tracing::warn!("forward_tx read lock was poisoned, recovering");
            poisoned.into_inner()
        });
        guard.as_ref().is_some_and(|tx| tx.try_send(req).is_ok())
    }

    /// Receive a single forward ack, waiting up to `timeout`. Returns None on
    /// timeout or if no ack channel is installed.
    #[cfg(not(test))]
    fn recv_ack_timeout(&self, timeout: Duration) -> Option<ForwardAck> {
        let guard = self.forward_ack_rx.read().unwrap_or_else(|poisoned| {
            tracing::warn!("forward_ack_rx read lock was poisoned, recovering");
            poisoned.into_inner()
        });
        match guard.as_ref() {
            Some(rx) => rx.recv_timeout(timeout).ok(),
            None => None,
        }
    }

    /// Insert a batch of pending `(topic_id, partition_id, local_offset)` rows
    /// into the `kafka.shadow_tracking` outbox in the current transaction. The
    /// shared write path for both the live produce and the txn-commit enqueue.
    #[cfg(not(test))]
    fn insert_outbox_rows(&self, rows: &[(i32, i32, i64)]) -> Result<()> {
        use pgrx::prelude::*;
        Spi::connect_mut(|client| {
            for &(topic_id, partition_id, local_offset) in rows {
                client.update(
                    "INSERT INTO kafka.shadow_tracking (topic_id, partition_id, local_offset) \
                     VALUES ($1, $2, $3) \
                     ON CONFLICT (topic_id, partition_id, local_offset) DO NOTHING",
                    None,
                    &[topic_id.into(), partition_id.into(), local_offset.into()],
                )?;
            }
            Ok(())
        })
    }

    /// Enqueue the records of a just-committed transaction into the durable
    /// outbox (RA-1/RA-2).
    ///
    /// This replaces the old in-RAM per-record `send_sync` →
    /// `block_on` loop that ran on the single DB thread and could freeze every
    /// client for up to 60 s × N when the external broker was down. Each
    /// percentage-selected record (looked up against the current per-topic
    /// config) gets a pending `shadow_tracking` row written inside the commit
    /// transaction; the periodic poll then forwards it via the idempotent
    /// producer — non-blocking, at-least-once, re-partitioned by key, and
    /// (crucially for external-primary topics, whose local reads are suppressed)
    /// durably retried instead of best-effort-and-dropped.
    #[cfg(not(test))]
    fn enqueue_committed_txn_outbox(&self, records: &[PendingForwardRecord]) {
        if records.is_empty() {
            return;
        }
        self.check_license();

        let mut rows: Vec<(i32, i32, i64)> = Vec::with_capacity(records.len());
        for r in records {
            // The topic may have stopped forwarding since the records were
            // buffered (config reload); re-check at commit time.
            let percentage = match self.topic_cache.get(r.topic_id) {
                Some(c) if c.should_forward() => c.forward_percentage,
                _ => continue,
            };
            match self.decide_forward(r.key.as_deref(), r.offset, percentage) {
                ForwardDecision::Skip => {
                    self.metrics.skipped.fetch_add(1, Ordering::Relaxed);
                }
                ForwardDecision::Forward => rows.push((r.topic_id, r.partition_id, r.offset)),
            }
        }

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

    /// Test stub — see `enqueue_committed_txn_outbox`.
    #[cfg(test)]
    fn enqueue_committed_txn_outbox(&self, _records: &[PendingForwardRecord]) {}

    /// Write durable outbox rows for a freshly-produced batch (SH-9).
    ///
    /// For each record the percentage router selects (SH-8), insert a *pending*
    /// `kafka.shadow_tracking` row in the CURRENT transaction (this runs inside
    /// the produce request's subtransaction), so the outbox is exactly
    /// consistent with what was persisted locally. No payload is copied — it
    /// already lives in `kafka.messages`. The periodic poll forwards async
    /// topics; sync topics additionally wait (bounded) for confirmation here.
    #[cfg(not(test))]
    fn enqueue_outbox(
        &self,
        topic_id: i32,
        partition_id: i32,
        records: &[Record],
        base_offset: i64,
        config: &TopicShadowConfig,
    ) {
        let external_topic = config.effective_external_topic();
        let forward_percentage = config.forward_percentage;

        // License nag (rate-limited).
        self.check_license();

        // SH-8: select records by percentage first (decide_forward is pure).
        let mut selected: Vec<i64> = Vec::with_capacity(records.len());
        for (i, record) in records.iter().enumerate() {
            let local_offset = base_offset + i as i64;
            match self.decide_forward(record.key.as_deref(), local_offset, forward_percentage) {
                ForwardDecision::Skip => {
                    self.metrics.skipped.fetch_add(1, Ordering::Relaxed);
                }
                ForwardDecision::Forward => selected.push(local_offset),
            }
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

        // Async topics are forwarded by the periodic poll after commit. Sync
        // topics wait (bounded) for external confirmation now (SH-14).
        if config.sync_mode == SyncMode::Sync {
            self.forward_sync_bounded(
                topic_id,
                external_topic,
                partition_id,
                records,
                base_offset,
                &selected,
            );
        }
    }

    /// Bounded-sync forward (SH-14): for a sync-mode produce, eagerly hand the
    /// just-written outbox rows to the network thread and wait up to
    /// `SYNC_FORWARD_WAIT_MS` for their acks, applying every ack we observe. On
    /// timeout we return anyway — the rows are durable and the poll retries
    /// them. This replaces the old `block_on`, so a down broker delays a single
    /// produce by at most the cap instead of stalling every client forever.
    #[cfg(not(test))]
    fn forward_sync_bounded(
        &self,
        topic_id: i32,
        external_topic: &str,
        partition_id: i32,
        records: &[Record],
        base_offset: i64,
        selected: &[i64],
    ) {
        let mut pending: std::collections::HashSet<i64> = selected.iter().copied().collect();

        // Send a forward request for each selected record.
        for &local_offset in selected {
            let idx = (local_offset - base_offset) as usize;
            let record = match records.get(idx) {
                Some(r) => r,
                None => {
                    pending.remove(&local_offset);
                    continue;
                }
            };
            let req = ForwardRequest::new(
                topic_id,
                external_topic.to_string(),
                partition_id,
                record.key.clone(),
                record.value.clone(),
                local_offset,
            );
            if !self.try_send_forward(req) {
                // Could not enqueue — leave it for the poll, stop waiting on it.
                pending.remove(&local_offset);
            }
        }

        if pending.is_empty() {
            return;
        }

        // Wait (bounded) for acks, applying every one we drain so none are lost.
        let deadline = Instant::now() + Duration::from_millis(SYNC_FORWARD_WAIT_MS);
        loop {
            let remaining = match deadline.checked_duration_since(Instant::now()) {
                Some(r) if !r.is_zero() => r,
                _ => break,
            };
            match self.recv_ack_timeout(remaining) {
                Some(ack) => {
                    let is_ours = ack.topic_id == topic_id
                        && ack.partition_id == partition_id
                        && pending.contains(&ack.local_offset);
                    self.apply_ack(&ack);
                    if is_ours {
                        pending.remove(&ack.local_offset);
                        if pending.is_empty() {
                            break;
                        }
                    }
                }
                None => break, // timed out
            }
        }
    }

    /// Claim a batch of due outbox rows and hand them to the network thread
    /// (SH-9). A row is "due" when it is still pending (`external_offset IS
    /// NULL`) and was last attempted longer ago than the retry interval.
    /// Claiming sets `forwarded_at` so a row is not re-sent every tick while it
    /// is in flight. Returns the number of rows dispatched.
    #[cfg(not(test))]
    pub fn poll_and_forward_outbox(&self) -> super::error::ShadowResult<usize> {
        use pgrx::prelude::*;

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

        let reqs: Vec<ForwardRequest> = Spi::connect_mut(|client| {
            let table = client.update(
                r#"
                WITH due AS (
                    SELECT st.topic_id, st.partition_id, st.local_offset
                    FROM kafka.shadow_tracking st
                    WHERE st.external_offset IS NULL
                      AND st.retry_count < $3
                      AND (st.forwarded_at IS NULL
                           OR st.forwarded_at < NOW() - ($1 || ' milliseconds')::interval)
                      AND EXISTS (
                          SELECT 1 FROM kafka.messages m
                          WHERE m.topic_id = st.topic_id
                            AND m.partition_id = st.partition_id
                            AND m.partition_offset = st.local_offset
                      )
                    ORDER BY st.topic_id, st.partition_id, st.local_offset
                    LIMIT $2
                ),
                claimed AS (
                    -- RV-10: count each claim as a delivery attempt. Previously
                    -- retry_count was bumped only when a *failure* ack came back, so a
                    -- forward whose ack was dropped (RA-6 backpressure drop) left
                    -- retry_count unchanged and the row was re-claimed forever,
                    -- bypassing the `retry_count < $3` cap (re-forward livelock).
                    -- Bumping on claim bounds every re-forward loop — dropped or
                    -- failed — to $3 attempts before the row is dead-lettered.
                    UPDATE kafka.shadow_tracking st
                    SET forwarded_at = NOW(), retry_count = st.retry_count + 1
                    FROM due
                    WHERE st.topic_id = due.topic_id
                      AND st.partition_id = due.partition_id
                      AND st.local_offset = due.local_offset
                    RETURNING st.topic_id, st.partition_id, st.local_offset
                )
                SELECT c.topic_id    AS topic_id,
                       c.partition_id AS partition_id,
                       c.local_offset AS local_offset,
                       m.key          AS msg_key,
                       m.value        AS msg_value,
                       COALESCE(sc.external_topic_name, t.name) AS external_topic
                FROM claimed c
                JOIN kafka.topics t ON t.id = c.topic_id
                LEFT JOIN kafka.shadow_config sc ON sc.topic_id = c.topic_id
                JOIN kafka.messages m
                  ON m.topic_id = c.topic_id
                 AND m.partition_id = c.partition_id
                 AND m.partition_offset = c.local_offset
                "#,
                None,
                &[
                    OUTBOX_RETRY_INTERVAL_MS.into(),
                    OUTBOX_BATCH_LIMIT.into(),
                    MAX_FORWARD_RETRIES.into(),
                ],
            )?;

            let mut reqs: Vec<ForwardRequest> = Vec::new();
            for row in table {
                let topic_id: i32 = row.get_by_name("topic_id")?.unwrap_or(0);
                let partition_id: i32 = row.get_by_name("partition_id")?.unwrap_or(0);
                let local_offset: i64 = row.get_by_name("local_offset")?.unwrap_or(0);
                let key: Option<Vec<u8>> = row.get_by_name("msg_key")?;
                let value: Option<Vec<u8>> = row.get_by_name("msg_value")?;
                let external_topic: String = row.get_by_name("external_topic")?.unwrap_or_default();
                reqs.push(ForwardRequest::new(
                    topic_id,
                    external_topic,
                    partition_id,
                    key,
                    value,
                    local_offset,
                ));
            }
            Ok(reqs)
        })
        .map_err(|e: KafkaError| {
            super::error::ShadowError::DatabaseError(format!("outbox poll failed: {}", e))
        })?;

        let dispatched = reqs.len();
        for req in reqs {
            // If the channel is momentarily full the row stays pending (its
            // forwarded_at was just set) and is retried after the interval.
            if !self.try_send_forward(req) {
                tracing::debug!("Shadow: forward channel full, deferring outbox row");
            }
        }
        Ok(dispatched)
    }

    /// Drain all currently-available forward acks and finalize their outbox
    /// rows (SH-9). Non-blocking: stops as soon as the channel is empty. Returns
    /// the number of acks applied.
    #[cfg(not(test))]
    pub fn drain_forward_acks(&self) -> usize {
        let mut acks: Vec<ForwardAck> = Vec::new();
        {
            let guard = self.forward_ack_rx.read().unwrap_or_else(|poisoned| {
                tracing::warn!("forward_ack_rx read lock was poisoned, recovering");
                poisoned.into_inner()
            });
            if let Some(rx) = guard.as_ref() {
                while let Ok(ack) = rx.try_recv() {
                    acks.push(ack);
                }
            }
        }
        let n = acks.len();
        for ack in &acks {
            self.apply_ack(ack);
        }
        n
    }

    /// Finalize a single outbox row from a forward ack (SH-9, SH-7).
    ///
    /// Success → record the external offset and clear any error; the row is now
    /// delivered and will never be polled again (`external_offset IS NOT NULL`).
    /// Failure → leave it pending and store the error so the poll re-attempts it
    /// (RV-10: `retry_count` is bumped on claim in `poll_and_forward_outbox`, not
    /// here, so a dropped ack still counts against the cap). The forward metric is
    /// counted exactly once, here, on a confirmed delivery (SH-7).
    #[cfg(not(test))]
    fn apply_ack(&self, ack: &ForwardAck) {
        use pgrx::prelude::*;

        // SH-7: the metric is bumped in the SAME statement that flips the row,
        // guarded by `external_offset IS NULL`, so a duplicate ack (e.g. a sync
        // send and a poll resend both confirming) updates nothing and counts
        // nothing — exactly-once per row.
        // Each statement appends `RETURNING 1` so we learn whether the row
        // actually transitioned (`external_offset IS NULL` matched). A duplicate
        // ack updates nothing and returns no rows, so the in-RAM mirror below is
        // bumped exactly once too (RA-6), matching the persistent table.
        let result: Result<bool> = Spi::connect_mut(|client| {
            let transitioned = match &ack.result {
                Ok(external_offset) => {
                    let table = client.update(
                        "WITH done AS ( \
                             UPDATE kafka.shadow_tracking \
                             SET external_offset = $4, forwarded_at = NOW(), error_message = NULL \
                             WHERE topic_id = $1 AND partition_id = $2 AND local_offset = $3 \
                               AND external_offset IS NULL \
                             RETURNING topic_id, partition_id, local_offset \
                         ), \
                         bumped AS ( \
                             INSERT INTO kafka.shadow_metrics \
                                 (topic_id, partition_id, messages_forwarded, last_forwarded_offset, last_forwarded_at) \
                             SELECT topic_id, partition_id, 1, local_offset, NOW() FROM done \
                             ON CONFLICT (topic_id, partition_id) DO UPDATE SET \
                                messages_forwarded = kafka.shadow_metrics.messages_forwarded + 1, \
                                last_forwarded_offset = GREATEST(kafka.shadow_metrics.last_forwarded_offset, EXCLUDED.last_forwarded_offset), \
                                last_forwarded_at = NOW() \
                         ) \
                         SELECT 1 FROM done",
                        None,
                        &[
                            ack.topic_id.into(),
                            ack.partition_id.into(),
                            ack.local_offset.into(),
                            (*external_offset).into(),
                        ],
                    )?;
                    table.into_iter().count() > 0
                }
                Err(msg) => {
                    let table = client.update(
                        // RV-10: retry_count is now bumped on claim (see
                        // poll_and_forward_outbox), so the failure ack only records the
                        // error — incrementing here too would double-count and halve the
                        // effective retry budget. RETURNING retry_count still reports the
                        // claim-bumped value for the dead-letter check below.
                        "WITH failed AS ( \
                             UPDATE kafka.shadow_tracking \
                             SET error_message = $4 \
                             WHERE topic_id = $1 AND partition_id = $2 AND local_offset = $3 \
                               AND external_offset IS NULL \
                             RETURNING topic_id, partition_id, retry_count \
                         ), \
                         bumped AS ( \
                             INSERT INTO kafka.shadow_metrics (topic_id, partition_id, messages_failed) \
                             SELECT topic_id, partition_id, 1 FROM failed \
                             ON CONFLICT (topic_id, partition_id) DO UPDATE SET \
                                messages_failed = kafka.shadow_metrics.messages_failed + 1 \
                         ) \
                         SELECT retry_count FROM failed",
                        None,
                        &[
                            ack.topic_id.into(),
                            ack.partition_id.into(),
                            ack.local_offset.into(),
                            msg.clone().into(),
                        ],
                    )?;
                    let mut transitioned = false;
                    for row in table {
                        transitioned = true;
                        let retry_count: i32 = row.get_by_name("retry_count")?.unwrap_or(0);
                        // RV-6: at the retry cap the poll stops retrying this row (it is
                        // dead-lettered). For an external-primary topic the record then
                        // falls back to local reads unless strict — log loudly.
                        if retry_count as i64 >= MAX_FORWARD_RETRIES {
                            tracing::warn!(
                                "shadow forward dead-lettered after {} attempts (topic_id={}, partition_id={}, local_offset={}): {} — external-primary reads for this topic fall back to local unless pg_kafka.shadow_external_primary_strict is set",
                                retry_count,
                                ack.topic_id,
                                ack.partition_id,
                                ack.local_offset,
                                msg
                            );
                        }
                    }
                    transitioned
                }
            };
            Ok(transitioned)
        });

        let transitioned = match result {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(
                    "Shadow: failed to apply forward ack for {}[{}] offset {}: {:?}",
                    ack.topic_id,
                    ack.partition_id,
                    ack.local_offset,
                    e
                );
                false
            }
        };

        // Mirror into the in-RAM counters used by flush_metrics_to_db logging,
        // only when the row actually transitioned (exactly-once, RA-6).
        if transitioned {
            if ack.result.is_ok() {
                self.metrics.forwarded.fetch_add(1, Ordering::Relaxed);
            } else {
                self.metrics.failed.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Test stub — the outbox is SPI-backed and exercised by the shadow E2E.
    #[cfg(test)]
    fn enqueue_outbox(
        &self,
        _topic_id: i32,
        _partition_id: i32,
        _records: &[Record],
        _base_offset: i64,
        _config: &TopicShadowConfig,
    ) {
    }

    /// Test stub — see `poll_and_forward_outbox`.
    #[cfg(test)]
    pub fn poll_and_forward_outbox(&self) -> super::error::ShadowResult<usize> {
        Ok(0)
    }

    /// Test stub — see `drain_forward_acks`.
    #[cfg(test)]
    pub fn drain_forward_acks(&self) -> usize {
        0
    }

    /// Load topic shadow configurations from database
    ///
    /// Queries kafka.shadow_config and populates the topic_cache.
    /// Call this on startup and periodically to refresh config.
    #[cfg(not(test))]
    pub fn load_topic_config_from_db(&self) -> super::error::ShadowResult<usize> {
        use super::config::{ShadowMode, SyncMode};
        use pgrx::prelude::*;

        let query = r#"
            SELECT sc.topic_id, t.name, sc.mode, sc.forward_percentage,
                   sc.external_topic_name, sc.sync_mode,
                   COALESCE(sc.write_mode, 'dual_write') as write_mode
            FROM kafka.shadow_config sc
            JOIN kafka.topics t ON sc.topic_id = t.id
        "#;

        // Collect into a local vec first and only swap the cache once the query
        // has succeeded (SH-3). The cache must be *replaced*, not merely
        // updated: a topic whose row was deleted from kafka.shadow_config would
        // otherwise linger in the cache (and keep forwarding) forever. But a
        // transient query failure must NOT wipe a working cache, so we clear
        // only on the success path.
        let mut new_configs: Vec<TopicShadowConfig> = Vec::new();
        let mut query_ok = false;

        Spi::connect(|client| {
            let result = client.select(query, None, &[]);

            if let Ok(table) = result {
                query_ok = true;
                for row in table {
                    // Use named columns for clarity and safety
                    let topic_id: i32 = row.get_by_name("topic_id").unwrap_or(Some(0)).unwrap_or(0);
                    let topic_name: String = row
                        .get_by_name("name")
                        .unwrap_or(Some(String::new()))
                        .unwrap_or_default();
                    let mode_str: String = row
                        .get_by_name("mode")
                        .unwrap_or(Some("local_only".to_string()))
                        .unwrap_or_default();
                    let forward_percentage: i32 = row
                        .get_by_name("forward_percentage")
                        .unwrap_or(Some(0))
                        .unwrap_or(0);
                    let external_topic_name: Option<String> =
                        row.get_by_name("external_topic_name").unwrap_or(None);
                    let sync_mode_str: String = row
                        .get_by_name("sync_mode")
                        .unwrap_or(Some("sync".to_string()))
                        .unwrap_or_default();
                    let write_mode_str: String = row
                        .get_by_name("write_mode")
                        .unwrap_or(Some("dual_write".to_string()))
                        .unwrap_or_default();

                    let mut config = TopicShadowConfig {
                        topic_id,
                        topic_name: topic_name.clone(),
                        mode: ShadowMode::parse(&mode_str),
                        forward_percentage: forward_percentage.clamp(0, 100) as u8,
                        external_topic_name,
                        sync_mode: SyncMode::parse(&sync_mode_str),
                        write_mode: WriteMode::parse(&write_mode_str),
                    };

                    // SH-6 guard: an external-primary topic suppresses local
                    // reads, so a record sampled out by forward_percentage would
                    // be readable nowhere — neither locally (suppressed) nor
                    // externally (not forwarded). Force 100% so every record is
                    // forwarded; the percentage dial is meaningless here.
                    if config.write_mode == WriteMode::ExternalOnly
                        && config.forward_percentage != 100
                    {
                        tracing::warn!(
                            "Shadow: topic '{}' is external-primary (ExternalOnly) but \
                             forward_percentage={}; forcing 100% to avoid unreadable records",
                            config.topic_name,
                            config.forward_percentage
                        );
                        config.forward_percentage = 100;
                    }

                    new_configs.push(config);
                }
            }
        });

        // Only replace the cache when the query actually ran. On failure, keep
        // whatever was loaded previously rather than serving an empty config.
        if !query_ok {
            return Err(super::error::ShadowError::DatabaseError(
                "failed to query kafka.shadow_config".to_string(),
            ));
        }

        let count = new_configs.len();
        self.topic_cache.clear();
        for config in new_configs {
            self.topic_cache.update(config);
        }

        Ok(count)
    }

    /// Test version - no-op
    #[cfg(test)]
    pub fn load_topic_config_from_db(&self) -> super::error::ShadowResult<usize> {
        Ok(0)
    }

    /// Flush accumulated metrics to logs
    ///
    /// Logs metrics summary and resets counters. Call periodically from the worker loop.
    #[cfg(not(test))]
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

    /// Test version - just reset counters
    #[cfg(test)]
    pub fn flush_metrics_to_db(&self) -> super::error::ShadowResult<()> {
        self.metrics.forwarded.store(0, Ordering::Relaxed);
        self.metrics.skipped.store(0, Ordering::Relaxed);
        self.metrics.failed.store(0, Ordering::Relaxed);
        self.metrics.fallback_local.store(0, Ordering::Relaxed);
        Ok(())
    }
}

impl<S: KafkaStore> KafkaStore for ShadowStore<S> {
    // ===== Topic Operations =====

    fn get_or_create_topic(&self, name: &str, default_partitions: i32) -> Result<(i32, i32)> {
        self.inner.get_or_create_topic(name, default_partitions)
    }

    fn get_topic_metadata(&self, names: Option<&[String]>) -> Result<Vec<TopicMetadata>> {
        self.inner.get_topic_metadata(names)
    }

    // ===== Message Operations =====

    fn insert_records(&self, topic_id: i32, partition_id: i32, records: &[Record]) -> Result<i64> {
        // Skip shadow forwarding if:
        // 1. Shadow mode is not enabled globally
        // 2. We're running on a standby (only primary forwards)
        let is_enabled = self.is_enabled();
        let is_primary = self.is_primary();

        // Debug logging to trace shadow forwarding decisions
        crate::pg_log!(
            "Shadow insert_records: topic_id={}, records={}, is_enabled={}, is_primary={}",
            topic_id,
            records.len(),
            is_enabled,
            is_primary
        );

        if !is_enabled || !is_primary {
            crate::pg_log!(
                "Shadow: skipping forwarding (enabled={}, primary={})",
                is_enabled,
                is_primary
            );
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

    // ===== Consumer Offset Operations =====

    fn commit_offset(
        &self,
        group_id: &str,
        topic_id: i32,
        partition_id: i32,
        offset: i64,
        metadata: Option<&str>,
    ) -> Result<()> {
        self.inner
            .commit_offset(group_id, topic_id, partition_id, offset, metadata)
    }

    fn fetch_offset(
        &self,
        group_id: &str,
        topic_id: i32,
        partition_id: i32,
    ) -> Result<Option<CommittedOffset>> {
        self.inner.fetch_offset(group_id, topic_id, partition_id)
    }

    fn fetch_all_offsets(&self, group_id: &str) -> Result<Vec<(String, i32, CommittedOffset)>> {
        self.inner.fetch_all_offsets(group_id)
    }

    // ===== Admin Topic Operations =====

    fn topic_exists(&self, name: &str) -> Result<bool> {
        self.inner.topic_exists(name)
    }

    fn create_topic(&self, name: &str, partition_count: i32) -> Result<i32> {
        self.inner.create_topic(name, partition_count)
    }

    fn get_topic_id(&self, name: &str) -> Result<Option<i32>> {
        self.inner.get_topic_id(name)
    }

    fn delete_topic(&self, topic_id: i32) -> Result<()> {
        self.inner.delete_topic(topic_id)
    }

    fn get_topic_partition_count(&self, name: &str) -> Result<Option<i32>> {
        self.inner.get_topic_partition_count(name)
    }

    fn set_topic_partition_count(&self, name: &str, partition_count: i32) -> Result<()> {
        self.inner.set_topic_partition_count(name, partition_count)
    }

    // ===== Admin Consumer Group Operations =====

    fn delete_consumer_group_offsets(&self, group_id: &str) -> Result<()> {
        self.inner.delete_consumer_group_offsets(group_id)
    }

    // ===== Idempotent Producer Operations =====

    fn allocate_producer_id(
        &self,
        client_id: Option<&str>,
        transactional_id: Option<&str>,
    ) -> Result<(i64, i16)> {
        self.inner.allocate_producer_id(client_id, transactional_id)
    }

    fn get_producer_epoch(&self, producer_id: i64) -> Result<Option<i16>> {
        self.inner.get_producer_epoch(producer_id)
    }

    fn increment_producer_epoch(&self, producer_id: i64) -> Result<i16> {
        self.inner.increment_producer_epoch(producer_id)
    }

    fn check_and_update_sequence(
        &self,
        producer_id: i64,
        producer_epoch: i16,
        topic_id: i32,
        partition_id: i32,
        base_sequence: i32,
        record_count: i32,
    ) -> Result<bool> {
        self.inner.check_and_update_sequence(
            producer_id,
            producer_epoch,
            topic_id,
            partition_id,
            base_sequence,
            record_count,
        )
    }

    fn record_producer_sequence(
        &self,
        producer_id: i64,
        topic_id: i32,
        partition_id: i32,
        last_sequence: i32,
    ) -> Result<()> {
        self.inner
            .record_producer_sequence(producer_id, topic_id, partition_id, last_sequence)
    }

    // ===== Transaction Operations =====

    fn get_or_create_transactional_producer(
        &self,
        transactional_id: &str,
        transaction_timeout_ms: i32,
        client_id: Option<&str>,
    ) -> Result<(i64, i16)> {
        self.inner.get_or_create_transactional_producer(
            transactional_id,
            transaction_timeout_ms,
            client_id,
        )
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

        // 2. Collect the buffered records (all are about to be committed).
        let records = pending_records.map(|buf| buf.records).unwrap_or_default();

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

    fn get_last_stable_offset(&self, topic_id: i32, partition_id: i32) -> Result<i64> {
        if self.suppress_external_primary_reads(topic_id) {
            return Ok(0);
        }
        self.inner.get_last_stable_offset(topic_id, partition_id)
    }

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

        let store = ShadowStore::new(mock);
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

        let store = ShadowStore::new(mock);
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
    fn test_abort_timed_out_evicts_stale_pending_buffers() {
        // SH-4: the storage sweeper force-aborts timed-out transactions without
        // routing through abort_transaction(), so a buffer for an abandoned
        // producer must be evicted on the same timeout or it leaks forever.
        let mut mock = MockKafkaStore::new();
        mock.expect_abort_timed_out_transactions()
            .times(1)
            .returning(|_| Ok(vec![]));

        let store = ShadowStore::new(mock);
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
