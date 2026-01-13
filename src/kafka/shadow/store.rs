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
//! ## Sync and Async Forwarding
//!
//! Forwarding mode is controlled by the per-topic `sync_mode` setting:
//!
//! - **Sync mode**: Uses `futures::executor::block_on()` for sync forwarding, which
//!   works without a tokio runtime context. Blocks the DB worker thread until delivery.
//!
//! - **Async mode**: Sends messages via crossbeam channel to the network thread,
//!   which has a tokio runtime. Fire-and-forget, non-blocking on the DB worker thread.
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
use super::ForwardRequest;
use crate::kafka::error::Result;
use crate::kafka::messages::{Record, RecordHeader};
use crate::kafka::storage::{
    CommittedOffset, FetchedMessage, IsolationLevel, KafkaStore, TopicMetadata, TransactionState,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

/// Record pending shadow forwarding after transaction commit
///
/// These are cached during `insert_transactional_records()` and forwarded
/// when `commit_transaction()` is called.
#[derive(Clone, Debug)]
struct PendingForwardRecord {
    /// Topic ID for this record
    topic_id: i32,
    /// Topic name (cached to avoid lookup on commit)
    topic_name: String,
    /// Partition ID
    partition_id: i32,
    /// Local offset (-1 if not written locally yet, e.g., ExternalOnly mode)
    offset: i64,
    /// Record key
    key: Option<Vec<u8>>,
    /// Record value
    value: Option<Vec<u8>>,
    /// Record headers (matches Record struct type)
    headers: Vec<RecordHeader>,
    /// Timestamp (milliseconds since epoch, optional - matches Record struct)
    timestamp: Option<i64>,
    /// Write mode for this record (DualWrite or ExternalOnly)
    write_mode: WriteMode,
    /// Whether this record was written to local PostgreSQL
    written_locally: bool,
}

/// Key for pending transaction messages: (producer_id, producer_epoch)
type TxnKey = (i64, i16);

/// Cache of pending transactional records awaiting commit
type PendingTxnMessages = Arc<RwLock<HashMap<TxnKey, Vec<PendingForwardRecord>>>>;

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

/// Default timeout for sync forwards (milliseconds)
/// 60 seconds to handle fresh Kafka broker topic auto-creation
const DEFAULT_FORWARD_TIMEOUT_MS: u64 = 60_000;

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
    /// Topic configuration cache
    topic_cache: Arc<TopicConfigCache>,
    /// Primary status checker
    primary_status: Mutex<PrimaryStatus>,
    /// Shadow forwarding metrics
    metrics: ShadowMetrics,
    /// Channel for async forwarding to network thread (Phase 11)
    forward_tx: RwLock<Option<crossbeam_channel::Sender<ForwardRequest>>>,
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
            topic_cache: Arc::new(TopicConfigCache::new()),
            primary_status: Mutex::new(PrimaryStatus::new()),
            metrics: ShadowMetrics::new(),
            forward_tx: RwLock::new(None),
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
        let config = {
            use crate::config::Config;
            let cfg = Config::load();
            ShadowConfig::from_config(&Arc::new(cfg))
        };

        #[cfg(test)]
        let config = ShadowConfig::default();

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
                        // Verify cached producer is healthy before returning
                        if producer.is_healthy() {
                            return Some(producer.clone());
                        }
                        // Producer unhealthy, fall through to recreate
                        pgrx::log!("Shadow: cached producer unhealthy, recreating");
                    } else {
                        // Config changed - need to recreate producer
                        pgrx::log!(
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

        // Double-check after acquiring write lock
        if let Some(ref producer) = *guard {
            if let Some(ref cached_bs) = *bs_guard {
                if cached_bs == &config.bootstrap_servers && producer.is_healthy() {
                    return Some(producer.clone());
                }
            }
        }

        // Clear old producer if exists (config changed)
        *guard = None;
        *bs_guard = None;

        if !config.is_configured() {
            pgrx::warning!(
                "⚠️  SHADOW MODE WARNING: \
                 pg_kafka.shadow_mode_enabled={} \
                 pg_kafka.shadow_bootstrap_servers='{}' \
                 Shadow mode is NOT properly configured - messages will NOT be forwarded!",
                config.enabled,
                config.bootstrap_servers
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
                tracing::info!(
                    "✅ Shadow producer successfully initialized and connected to {}",
                    config.bootstrap_servers
                );
                Some(producer)
            }
            Err(e) => {
                pgrx::warning!(
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

    /// Decide whether to forward a message based on percentage routing
    ///
    /// Delegates to the shared routing module for consistent behavior
    /// with ShadowForwarder.
    fn decide_forward(
        &self,
        key: Option<&[u8]>,
        offset: i64,
        forward_percentage: u8,
    ) -> ForwardDecision {
        make_forward_decision(key, offset, forward_percentage)
    }

    /// Forward records to external Kafka
    ///
    /// Called after successful local insert for DualWrite mode.
    /// Logs errors but doesn't fail - local write already succeeded.
    ///
    /// Respects sync_mode from topic config:
    /// - Sync: Block on each send using block_on() (existing behavior)
    /// - Async: Send via channel to network thread (non-blocking, fire-and-forget)
    fn forward_records_best_effort(
        &self,
        topic_config: &TopicShadowConfig,
        partition_id: i32,
        records: &[Record],
        base_offset: i64,
    ) {
        // Check license (Commercial License) - emits rate-limited warnings
        self.check_license();

        let external_topic = topic_config.effective_external_topic();

        match topic_config.sync_mode {
            SyncMode::Async => {
                // Async mode: send via channel to network thread (non-blocking)
                let forward_tx = self.forward_tx.read().unwrap_or_else(|poisoned| {
                    tracing::warn!("forward_tx read lock was poisoned, recovering");
                    poisoned.into_inner()
                });
                let tx = match forward_tx.as_ref() {
                    Some(tx) => tx,
                    None => {
                        tracing::debug!("No forward channel available for async forwarding");
                        return;
                    }
                };

                for (i, record) in records.iter().enumerate() {
                    let offset = base_offset + i as i64;

                    // Check percentage routing
                    match self.decide_forward(
                        record.key.as_deref(),
                        offset,
                        topic_config.forward_percentage,
                    ) {
                        ForwardDecision::Skip => {
                            self.metrics.skipped.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                        ForwardDecision::Forward => {
                            // Non-blocking send to network thread
                            let req = ForwardRequest::new(
                                external_topic.to_string(),
                                partition_id,
                                record.key.clone(),
                                record.value.clone(),
                                offset,
                            );

                            // Backpressure: drop on channel full, increment failed counter
                            if tx.try_send(req).is_err() {
                                tracing::warn!(
                                    "Forward channel full, dropping async forward request"
                                );
                                self.metrics.failed.fetch_add(1, Ordering::Relaxed);
                            }
                            // Note: forwarded metric updated in network thread on success
                        }
                    }
                }
            }

            SyncMode::Sync => {
                // Sync mode: block on each send using block_on()
                let producer = match self.ensure_producer() {
                    Some(p) => p,
                    None => {
                        pgrx::warning!("Shadow mode: producer not available for forwarding");
                        return;
                    }
                };

                for (i, record) in records.iter().enumerate() {
                    let offset = base_offset + i as i64;

                    // Check percentage routing
                    match self.decide_forward(
                        record.key.as_deref(),
                        offset,
                        topic_config.forward_percentage,
                    ) {
                        ForwardDecision::Skip => {
                            tracing::trace!(
                                "Shadow mode: skipping record {} due to percentage routing",
                                offset
                            );
                            self.metrics.skipped.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                        ForwardDecision::Forward => {
                            tracing::trace!(
                                "Shadow mode: forwarding record {} to {}[{}]",
                                offset,
                                external_topic,
                                partition_id
                            );
                            // Forward synchronously using send_sync
                            if let Err(e) = producer.send_sync(
                                external_topic,
                                Some(partition_id),
                                record.key.as_deref(),
                                record.value.as_deref(),
                                DEFAULT_FORWARD_TIMEOUT_MS,
                            ) {
                                pgrx::warning!(
                                    "Shadow forward FAILED for {}[{}] offset {}: {:?}",
                                    external_topic,
                                    partition_id,
                                    offset,
                                    e
                                );
                                self.metrics.failed.fetch_add(1, Ordering::Relaxed);
                            } else {
                                tracing::trace!(
                                    "Shadow forward SUCCESS for {}[{}] offset {}",
                                    external_topic,
                                    partition_id,
                                    offset
                                );
                                self.metrics.forwarded.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                }
            }
        }
    }

    /// Forward records to external Kafka, returning success status
    ///
    /// Used for ExternalOnly mode where we need to know if forward succeeded.
    fn forward_records_required(
        &self,
        topic_config: &TopicShadowConfig,
        partition_id: i32,
        records: &[Record],
    ) -> bool {
        // Check license (Commercial License) - emits rate-limited warnings
        self.check_license();

        let producer = match self.ensure_producer() {
            Some(p) => p,
            None => {
                tracing::debug!("No producer available for shadow forwarding");
                return false;
            }
        };

        let external_topic = topic_config.effective_external_topic();

        for record in records {
            if let Err(e) = producer.send_sync(
                external_topic,
                Some(partition_id),
                record.key.as_deref(),
                record.value.as_deref(),
                DEFAULT_FORWARD_TIMEOUT_MS,
            ) {
                pgrx::warning!(
                    "Shadow external write failed for {}[{}]: {:?}",
                    external_topic,
                    partition_id,
                    e
                );
                self.metrics.failed.fetch_add(1, Ordering::Relaxed);
                return false; // Trigger fallback to local
            } else {
                self.metrics.forwarded.fetch_add(1, Ordering::Relaxed);
            }
        }

        true // All records forwarded successfully
    }

    /// Flush the producer if initialized
    pub fn flush(&self) -> super::error::ShadowResult<()> {
        if let Some(producer) = self.ensure_producer() {
            producer.flush(Duration::from_secs(30))?;
        }
        Ok(())
    }

    /// Forward a single record to external Kafka
    ///
    /// Returns true on success, false on failure.
    /// Used for transactional message forwarding on commit.
    fn forward_single_record(&self, record: &PendingForwardRecord) -> bool {
        // Get topic config for percentage routing and external topic name
        let topic_config = match self.topic_cache.get(record.topic_id) {
            Some(config) => config,
            None => {
                tracing::warn!(
                    "No shadow config for topic {} during txn forward",
                    record.topic_id
                );
                return false;
            }
        };

        // Check percentage routing (use offset if available, otherwise always forward)
        let should_forward = if record.offset >= 0 {
            match self.decide_forward(
                record.key.as_deref(),
                record.offset,
                topic_config.forward_percentage,
            ) {
                ForwardDecision::Forward => true,
                ForwardDecision::Skip => {
                    self.metrics.skipped.fetch_add(1, Ordering::Relaxed);
                    return true; // Skipped by design, not a failure
                }
            }
        } else {
            // ExternalOnly mode: no offset yet, check percentage with partition_id as fallback
            topic_config.forward_percentage == 100
                || (record.partition_id as u8 % 100) < topic_config.forward_percentage
        };

        if !should_forward {
            self.metrics.skipped.fetch_add(1, Ordering::Relaxed);
            return true;
        }

        // Get producer
        let producer = match self.ensure_producer() {
            Some(p) => p,
            None => {
                tracing::warn!("No producer available for txn forward");
                return false;
            }
        };

        let external_topic = topic_config.effective_external_topic();

        // Forward synchronously
        match producer.send_sync(
            external_topic,
            Some(record.partition_id),
            record.key.as_deref(),
            record.value.as_deref(),
            DEFAULT_FORWARD_TIMEOUT_MS,
        ) {
            Ok(_) => {
                tracing::trace!(
                    "Shadow txn forward SUCCESS for {}[{}]",
                    external_topic,
                    record.partition_id
                );
                self.metrics.forwarded.fetch_add(1, Ordering::Relaxed);
                true
            }
            Err(e) => {
                tracing::warn!(
                    "Shadow txn forward FAILED for {}[{}]: {:?}",
                    external_topic,
                    record.partition_id,
                    e
                );
                self.metrics.failed.fetch_add(1, Ordering::Relaxed);
                false
            }
        }
    }

    /// Write a record to PostgreSQL as fallback when external forward fails (ExternalOnly mode)
    ///
    /// This is called during commit when an ExternalOnly record fails to forward.
    /// The record is inserted as a committed (visible) record since the transaction
    /// is being committed.
    fn write_fallback_record(
        &self,
        record: &PendingForwardRecord,
        _producer_id: i64,
        _producer_epoch: i16,
    ) -> Result<i64> {
        // Convert PendingForwardRecord back to Record for insertion
        let kafka_record = Record {
            key: record.key.clone(),
            value: record.value.clone(),
            headers: record.headers.clone(),
            timestamp: record.timestamp,
        };

        // Insert as a committed (visible) record
        // Note: This uses insert_records, not insert_transactional_records,
        // because we're in fallback mode and want the message visible immediately
        let offset =
            self.inner
                .insert_records(record.topic_id, record.partition_id, &[kafka_record])?;

        self.metrics.fallback_local.fetch_add(1, Ordering::Relaxed);
        tracing::info!(
            "ExternalOnly fallback: wrote txn record to local storage for topic {} partition {}",
            record.topic_name,
            record.partition_id
        );

        Ok(offset)
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

        let mut count = 0;

        Spi::connect(|client| {
            let result = client.select(query, None, &[]);

            if let Ok(table) = result {
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

                    let config = TopicShadowConfig {
                        topic_id,
                        topic_name: topic_name.clone(),
                        mode: ShadowMode::parse(&mode_str),
                        forward_percentage: forward_percentage.clamp(0, 100) as u8,
                        external_topic_name,
                        sync_mode: SyncMode::parse(&sync_mode_str),
                        write_mode: WriteMode::parse(&write_mode_str),
                    };

                    self.topic_cache.update(config);
                    count += 1;
                }
            }
        });

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
        pgrx::log!(
            "Shadow insert_records: topic_id={}, records={}, is_enabled={}, is_primary={}",
            topic_id,
            records.len(),
            is_enabled,
            is_primary
        );

        if !is_enabled || !is_primary {
            pgrx::log!(
                "Shadow: skipping forwarding (enabled={}, primary={})",
                is_enabled,
                is_primary
            );
            return self.inner.insert_records(topic_id, partition_id, records);
        }

        // Get topic configuration (if any)
        let topic_config = self.topic_cache.get(topic_id);
        let has_config = topic_config.is_some();
        let should_forward = topic_config
            .as_ref()
            .map(|c| c.should_forward())
            .unwrap_or(false);

        pgrx::log!(
            "Shadow: topic_id={} has_config={} should_forward={}",
            topic_id,
            has_config,
            should_forward
        );

        // Determine write mode
        let write_mode = topic_config
            .as_ref()
            .filter(|c| c.should_forward())
            .map(|c| c.write_mode)
            .unwrap_or(WriteMode::DualWrite); // No config = local only (DualWrite but no forward)

        pgrx::log!("Shadow: write_mode={:?}", write_mode);

        match write_mode {
            WriteMode::DualWrite => {
                // Always write locally first
                let base_offset = self.inner.insert_records(topic_id, partition_id, records)?;

                // Then forward to external (best-effort, doesn't affect return)
                if let Some(config) = topic_config {
                    if config.should_forward() {
                        pgrx::log!(
                            "Shadow: forwarding {} records to external for topic_id={}",
                            records.len(),
                            topic_id
                        );
                        self.forward_records_best_effort(
                            &config,
                            partition_id,
                            records,
                            base_offset,
                        );
                    } else {
                        pgrx::log!("Shadow: config exists but should_forward=false");
                    }
                } else {
                    pgrx::log!("Shadow: no topic config, skipping forward");
                }

                Ok(base_offset)
            }

            WriteMode::ExternalOnly => {
                // Try external first
                let config = topic_config.expect("ExternalOnly requires shadow config");

                if self.forward_records_required(&config, partition_id, records) {
                    // External succeeded - skip local write entirely
                    // Consumers should be configured to read from external Kafka
                    tracing::debug!(
                        "ExternalOnly: {} records forwarded to external Kafka, skipping local",
                        records.len()
                    );
                    self.metrics
                        .forwarded
                        .fetch_add(records.len() as u64, Ordering::Relaxed);
                    // Return a synthetic offset (not stored locally)
                    // Clients should switch to consuming from external Kafka
                    Ok(0)
                } else {
                    // External failed - fallback to local storage
                    tracing::info!(
                        "ExternalOnly fallback: {} records written locally due to external failure",
                        records.len()
                    );
                    self.metrics
                        .fallback_local
                        .fetch_add(records.len() as u64, Ordering::Relaxed);
                    self.inner.insert_records(topic_id, partition_id, records)
                }
            }
        }
    }

    fn fetch_records(
        &self,
        topic_id: i32,
        partition_id: i32,
        fetch_offset: i64,
        max_bytes: i32,
    ) -> Result<Vec<FetchedMessage>> {
        self.inner
            .fetch_records(topic_id, partition_id, fetch_offset, max_bytes)
    }

    fn get_high_watermark(&self, topic_id: i32, partition_id: i32) -> Result<i64> {
        self.inner.get_high_watermark(topic_id, partition_id)
    }

    fn get_earliest_offset(&self, topic_id: i32, partition_id: i32) -> Result<i64> {
        self.inner.get_earliest_offset(topic_id, partition_id)
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

        // Determine write mode (default to DualWrite if no config)
        let (write_mode, should_forward, topic_name) = match &topic_config {
            Some(config) if config.should_forward() => {
                (config.write_mode, true, config.topic_name.clone())
            }
            Some(config) => (WriteMode::DualWrite, false, config.topic_name.clone()),
            None => (WriteMode::DualWrite, false, format!("topic-{}", topic_id)),
        };

        // Handle based on write mode
        let base_offset = match write_mode {
            WriteMode::DualWrite => {
                // DualWrite: Write locally first, buffer for forwarding on commit
                let offset = self.inner.insert_transactional_records(
                    topic_id,
                    partition_id,
                    records,
                    producer_id,
                    producer_epoch,
                )?;

                // Buffer records for forwarding on commit (if should_forward)
                if should_forward {
                    let pending_records: Vec<PendingForwardRecord> = records
                        .iter()
                        .enumerate()
                        .map(|(i, r)| PendingForwardRecord {
                            topic_id,
                            topic_name: topic_name.clone(),
                            partition_id,
                            offset: offset + i as i64,
                            key: r.key.clone(),
                            value: r.value.clone(),
                            headers: r.headers.clone(),
                            timestamp: r.timestamp,
                            write_mode: WriteMode::DualWrite,
                            written_locally: true,
                        })
                        .collect();

                    let key = (producer_id, producer_epoch);
                    let mut pending =
                        self.pending_txn_messages
                            .write()
                            .unwrap_or_else(|poisoned| {
                                tracing::warn!(
                                    "pending_txn_messages write lock was poisoned, recovering"
                                );
                                poisoned.into_inner()
                            });
                    pending.entry(key).or_default().extend(pending_records);

                    tracing::trace!(
                        "ShadowStore: buffered {} DualWrite txn records for topic {} partition {} (forward on commit)",
                        records.len(),
                        topic_id,
                        partition_id
                    );
                }

                offset
            }

            WriteMode::ExternalOnly => {
                // ExternalOnly: Buffer in memory ONLY, don't write to PostgreSQL
                // Records will be forwarded on commit, with fallback to local on failure
                if should_forward {
                    let pending_records: Vec<PendingForwardRecord> = records
                        .iter()
                        .map(|r| PendingForwardRecord {
                            topic_id,
                            topic_name: topic_name.clone(),
                            partition_id,
                            offset: -1, // Not written locally yet
                            key: r.key.clone(),
                            value: r.value.clone(),
                            headers: r.headers.clone(),
                            timestamp: r.timestamp,
                            write_mode: WriteMode::ExternalOnly,
                            written_locally: false,
                        })
                        .collect();

                    let key = (producer_id, producer_epoch);
                    let mut pending =
                        self.pending_txn_messages
                            .write()
                            .unwrap_or_else(|poisoned| {
                                tracing::warn!(
                                    "pending_txn_messages write lock was poisoned, recovering"
                                );
                                poisoned.into_inner()
                            });
                    pending.entry(key).or_default().extend(pending_records);

                    tracing::trace!(
                        "ShadowStore: buffered {} ExternalOnly txn records for topic {} partition {} (forward on commit)",
                        records.len(),
                        topic_id,
                        partition_id
                    );

                    // Return synthetic offset (not stored locally)
                    0
                } else {
                    // ExternalOnly but forwarding disabled - write locally
                    self.inner.insert_transactional_records(
                        topic_id,
                        partition_id,
                        records,
                        producer_id,
                        producer_epoch,
                    )?
                }
            }
        };

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

        // 2. Separate records by write mode
        let (dual_write_records, external_only_records): (Vec<_>, Vec<_>) = pending_records
            .unwrap_or_default()
            .into_iter()
            .partition(|r| r.write_mode == WriteMode::DualWrite);

        // 3. For DualWrite records: commit to PostgreSQL first (makes messages visible)
        // Also commit if there are no external_only records (normal case)
        let has_local_writes = !dual_write_records.is_empty() || external_only_records.is_empty();
        if has_local_writes {
            self.inner
                .commit_transaction(transactional_id, producer_id, producer_epoch)?;
        }

        // 4. Forward DualWrite records (best effort, already committed locally)
        for record in &dual_write_records {
            self.forward_single_record(record);
        }

        if !dual_write_records.is_empty() {
            tracing::debug!(
                "Shadow txn commit: forwarded {} DualWrite records",
                dual_write_records.len()
            );
        }

        // 5. For ExternalOnly: forward first, fallback to local on failure
        for record in external_only_records {
            let forward_success = self.forward_single_record(&record);

            if !forward_success {
                // Fallback: write to PostgreSQL since external forward failed
                tracing::warn!(
                    "ExternalOnly forward failed for topic {}, falling back to local write",
                    record.topic_name
                );
                if let Err(e) = self.write_fallback_record(&record, producer_id, producer_epoch) {
                    tracing::error!(
                        "ExternalOnly fallback write also failed for topic {}: {:?}",
                        record.topic_name,
                        e
                    );
                }
            }
        }

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

        // 2. Only call inner abort if we wrote to PostgreSQL (DualWrite records)
        let has_local_writes = pending_records
            .as_ref()
            .map(|r| r.iter().any(|rec| rec.written_locally))
            .unwrap_or(true); // Default to true if no pending records (normal abort path)

        if has_local_writes {
            self.inner
                .abort_transaction(transactional_id, producer_id, producer_epoch)?;
        }

        // ExternalOnly records were never written locally, so just discard the buffer
        // (already removed from pending_txn_messages above)

        if let Some(records) = &pending_records {
            let external_only_count = records
                .iter()
                .filter(|r| r.write_mode == WriteMode::ExternalOnly)
                .count();
            if external_only_count > 0 {
                tracing::debug!(
                    "Shadow txn abort: discarded {} ExternalOnly records (never written locally)",
                    external_only_count
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
        self.inner.fetch_records_with_isolation(
            topic_id,
            partition_id,
            fetch_offset,
            max_bytes,
            isolation_level,
        )
    }

    fn get_last_stable_offset(&self, topic_id: i32, partition_id: i32) -> Result<i64> {
        self.inner.get_last_stable_offset(topic_id, partition_id)
    }

    fn abort_timed_out_transactions(&self, timeout: Duration) -> Result<Vec<String>> {
        self.inner.abort_timed_out_transactions(timeout)
    }

    fn cleanup_aborted_messages(&self, older_than: Duration) -> Result<u64> {
        self.inner.cleanup_aborted_messages(older_than)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::messages::RecordHeader;
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
    fn test_pending_forward_record_creation() {
        let header = RecordHeader {
            key: "header-key".to_string(),
            value: b"header-value".to_vec(),
        };

        let record = PendingForwardRecord {
            topic_id: 1,
            topic_name: "test-topic".to_string(),
            partition_id: 0,
            offset: 100,
            key: Some(b"key".to_vec()),
            value: Some(b"value".to_vec()),
            headers: vec![header],
            timestamp: Some(1234567890),
            write_mode: WriteMode::DualWrite,
            written_locally: true,
        };

        assert_eq!(record.topic_id, 1);
        assert_eq!(record.topic_name, "test-topic");
        assert_eq!(record.partition_id, 0);
        assert_eq!(record.offset, 100);
        assert_eq!(record.key, Some(b"key".to_vec()));
        assert_eq!(record.value, Some(b"value".to_vec()));
        assert_eq!(record.headers.len(), 1);
        assert_eq!(record.headers[0].key, "header-key");
        assert_eq!(record.headers[0].value, b"header-value".to_vec());
        assert_eq!(record.timestamp, Some(1234567890));
        assert!(matches!(record.write_mode, WriteMode::DualWrite));
        assert!(record.written_locally);
    }

    #[test]
    fn test_pending_forward_record_with_null_fields() {
        let record = PendingForwardRecord {
            topic_id: 1,
            topic_name: "test".to_string(),
            partition_id: 0,
            offset: -1, // Not written locally (ExternalOnly)
            key: None,
            value: None,
            headers: vec![],
            timestamp: None,
            write_mode: WriteMode::ExternalOnly,
            written_locally: false,
        };

        assert!(record.key.is_none());
        assert!(record.value.is_none());
        assert!(record.timestamp.is_none());
        assert_eq!(record.offset, -1);
        assert!(!record.written_locally);
        assert!(matches!(record.write_mode, WriteMode::ExternalOnly));
    }

    #[test]
    fn test_pending_forward_record_clone() {
        let record = PendingForwardRecord {
            topic_id: 1,
            topic_name: "test".to_string(),
            partition_id: 0,
            offset: 50,
            key: Some(b"key".to_vec()),
            value: Some(b"value".to_vec()),
            headers: vec![],
            timestamp: Some(1000),
            write_mode: WriteMode::DualWrite,
            written_locally: true,
        };

        let cloned = record.clone();
        assert_eq!(cloned.topic_id, record.topic_id);
        assert_eq!(cloned.topic_name, record.topic_name);
        assert_eq!(cloned.key, record.key);
        assert_eq!(cloned.value, record.value);
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
    fn test_pending_txn_messages_type() {
        // Test the PendingTxnMessages type alias works correctly
        let pending: PendingTxnMessages = Arc::new(RwLock::new(HashMap::new()));

        // Write some pending records
        {
            let mut guard = pending.write().unwrap();
            let key: TxnKey = (1000, 0);
            let record = PendingForwardRecord {
                topic_id: 1,
                topic_name: "test".to_string(),
                partition_id: 0,
                offset: 0,
                key: None,
                value: Some(b"test".to_vec()),
                headers: vec![],
                timestamp: None,
                write_mode: WriteMode::DualWrite,
                written_locally: true,
            };
            guard.entry(key).or_default().push(record);
        }

        // Read back
        {
            let guard = pending.read().unwrap();
            assert_eq!(guard.len(), 1);
            let records = guard.get(&(1000, 0)).unwrap();
            assert_eq!(records.len(), 1);
            assert_eq!(records[0].value, Some(b"test".to_vec()));
        }
    }

    #[test]
    fn test_pending_txn_accumulation() {
        let pending: PendingTxnMessages = Arc::new(RwLock::new(HashMap::new()));
        let key: TxnKey = (1000, 0);

        // Add multiple records to same transaction
        {
            let mut guard = pending.write().unwrap();
            for i in 0..5 {
                let record = PendingForwardRecord {
                    topic_id: 1,
                    topic_name: "test".to_string(),
                    partition_id: i,
                    offset: i as i64,
                    key: None,
                    value: Some(format!("msg-{}", i).into_bytes()),
                    headers: vec![],
                    timestamp: None,
                    write_mode: WriteMode::DualWrite,
                    written_locally: true,
                };
                guard.entry(key).or_default().push(record);
            }
        }

        // Verify all records accumulated
        let guard = pending.read().unwrap();
        let records = guard.get(&key).unwrap();
        assert_eq!(records.len(), 5);

        // Verify ordering preserved
        for (i, record) in records.iter().enumerate() {
            assert_eq!(record.partition_id, i as i32);
            assert_eq!(record.value, Some(format!("msg-{}", i).into_bytes()));
        }
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
                    topic_name: "test".to_string(),
                    partition_id: 0,
                    offset: 0,
                    key: None,
                    value: Some(b"test".to_vec()),
                    headers: vec![],
                    timestamp: None,
                    write_mode: WriteMode::DualWrite,
                    written_locally: true,
                }],
            );
        }

        // Simulate commit by removing
        {
            let mut guard = pending.write().unwrap();
            let removed = guard.remove(&key);
            assert!(removed.is_some());
            assert_eq!(removed.unwrap().len(), 1);
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
                    topic_name: "test".to_string(),
                    partition_id: 0,
                    offset: 0,
                    key: None,
                    value: Some(b"test".to_vec()),
                    headers: vec![],
                    timestamp: None,
                    write_mode: WriteMode::DualWrite,
                    written_locally: true,
                }],
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

    #[test]
    fn test_multiple_transactions_isolation() {
        let pending: PendingTxnMessages = Arc::new(RwLock::new(HashMap::new()));

        // Two different transactions
        let txn1: TxnKey = (1000, 0);
        let txn2: TxnKey = (1001, 0);

        {
            let mut guard = pending.write().unwrap();
            guard.insert(
                txn1,
                vec![PendingForwardRecord {
                    topic_id: 1,
                    topic_name: "topic1".to_string(),
                    partition_id: 0,
                    offset: 0,
                    key: None,
                    value: Some(b"txn1-msg".to_vec()),
                    headers: vec![],
                    timestamp: None,
                    write_mode: WriteMode::DualWrite,
                    written_locally: true,
                }],
            );
            guard.insert(
                txn2,
                vec![PendingForwardRecord {
                    topic_id: 2,
                    topic_name: "topic2".to_string(),
                    partition_id: 0,
                    offset: 0,
                    key: None,
                    value: Some(b"txn2-msg".to_vec()),
                    headers: vec![],
                    timestamp: None,
                    write_mode: WriteMode::DualWrite,
                    written_locally: true,
                }],
            );
        }

        // Commit txn1, verify txn2 unaffected
        {
            let mut guard = pending.write().unwrap();
            guard.remove(&txn1);
        }

        let guard = pending.read().unwrap();
        assert!(guard.get(&txn1).is_none());
        assert!(guard.get(&txn2).is_some());
        assert_eq!(guard.get(&txn2).unwrap()[0].topic_name, "topic2");
    }

    #[test]
    fn test_write_mode_affects_written_locally() {
        // DualWrite should mark as written locally
        let dual_write = PendingForwardRecord {
            topic_id: 1,
            topic_name: "test".to_string(),
            partition_id: 0,
            offset: 100,
            key: None,
            value: Some(b"test".to_vec()),
            headers: vec![],
            timestamp: None,
            write_mode: WriteMode::DualWrite,
            written_locally: true,
        };
        assert!(dual_write.written_locally);
        assert!(dual_write.offset >= 0);

        // ExternalOnly should not write locally
        let external_only = PendingForwardRecord {
            topic_id: 1,
            topic_name: "test".to_string(),
            partition_id: 0,
            offset: -1, // No local offset
            key: None,
            value: Some(b"test".to_vec()),
            headers: vec![],
            timestamp: None,
            write_mode: WriteMode::ExternalOnly,
            written_locally: false,
        };
        assert!(!external_only.written_locally);
        assert_eq!(external_only.offset, -1);
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
    fn test_pending_record_with_headers() {
        let headers = vec![
            RecordHeader {
                key: "key1".to_string(),
                value: b"value1".to_vec(),
            },
            RecordHeader {
                key: "key2".to_string(),
                value: b"value2".to_vec(),
            },
            RecordHeader {
                key: "content-type".to_string(),
                value: b"application/json".to_vec(),
            },
        ];

        let record = PendingForwardRecord {
            topic_id: 1,
            topic_name: "test".to_string(),
            partition_id: 0,
            offset: 0,
            key: None,
            value: Some(b"test".to_vec()),
            headers: headers.clone(),
            timestamp: None,
            write_mode: WriteMode::DualWrite,
            written_locally: true,
        };

        assert_eq!(record.headers.len(), 3);
        assert_eq!(record.headers[0].key, "key1");
        assert_eq!(record.headers[1].key, "key2");
        assert_eq!(record.headers[2].key, "content-type");
        assert_eq!(record.headers[2].value, b"application/json".to_vec());
    }

    #[test]
    fn test_pending_record_large_payload() {
        // Test with a large payload (1MB)
        let large_value: Vec<u8> = (0..1_000_000).map(|i| (i % 256) as u8).collect();

        let record = PendingForwardRecord {
            topic_id: 1,
            topic_name: "test".to_string(),
            partition_id: 0,
            offset: 0,
            key: Some(b"large-key".to_vec()),
            value: Some(large_value.clone()),
            headers: vec![],
            timestamp: Some(1234567890),
            write_mode: WriteMode::DualWrite,
            written_locally: true,
        };

        assert_eq!(record.value.as_ref().unwrap().len(), 1_000_000);
        assert_eq!(record.value, Some(large_value));
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
