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
use super::primary::PrimaryStatus;
use super::producer::ShadowProducer;
use super::routing::make_forward_decision;
use super::ForwardRequest;
use crate::kafka::error::Result;
use crate::kafka::messages::Record;
use crate::kafka::storage::{
    CommittedOffset, FetchedMessage, IsolationLevel, KafkaStore, TopicMetadata, TransactionState,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

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
const DEFAULT_FORWARD_TIMEOUT_MS: u64 = 30_000;

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
    /// Topic configuration cache
    topic_cache: Arc<TopicConfigCache>,
    /// Primary status checker
    primary_status: Mutex<PrimaryStatus>,
    /// Shadow forwarding metrics
    metrics: ShadowMetrics,
    /// Channel for async forwarding to network thread (Phase 11)
    forward_tx: RwLock<Option<crossbeam_channel::Sender<ForwardRequest>>>,
    /// Runtime context for accessing configuration (optional to support tests)
    runtime_context: Option<Arc<crate::kafka::RuntimeContext>>,
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
            topic_cache: Arc::new(TopicConfigCache::new()),
            primary_status: Mutex::new(PrimaryStatus::new()),
            metrics: ShadowMetrics::new(),
            forward_tx: RwLock::new(None),
            runtime_context: None,
        }
    }

    /// Create a new ShadowStore with RuntimeContext for config access
    ///
    /// This constructor is preferred in production as it enables centralized
    /// config management via RuntimeContext, avoiding redundant Config::load() calls.
    ///
    /// # Arguments
    /// * `inner` - The store to wrap (typically PostgresStore)
    /// * `runtime_context` - Runtime context for accessing configuration
    pub fn with_context(inner: S, runtime_context: Arc<crate::kafka::RuntimeContext>) -> Self {
        Self {
            inner,
            producer: RwLock::new(None),
            topic_cache: Arc::new(TopicConfigCache::new()),
            primary_status: Mutex::new(PrimaryStatus::new()),
            metrics: ShadowMetrics::new(),
            forward_tx: RwLock::new(None),
            runtime_context: Some(runtime_context),
        }
    }

    /// Set the forward channel for async forwarding
    ///
    /// Must be called after construction to enable async forwarding mode.
    /// When sync_mode is Async, messages are sent via this channel to the
    /// network thread for non-blocking forwarding.
    pub fn set_forward_channel(&self, tx: crossbeam_channel::Sender<ForwardRequest>) {
        let mut guard = self.forward_tx.write().expect("forward_tx lock poisoned");
        *guard = Some(tx);
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
        let mut status = self
            .primary_status
            .lock()
            .expect("primary_status lock poisoned");
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
    fn ensure_producer(&self) -> Option<Arc<ShadowProducer>> {
        // Fast path: check if already initialized
        {
            let guard = self.producer.read().expect("producer lock poisoned");
            if let Some(ref producer) = *guard {
                return Some(producer.clone());
            }
        }

        // Slow path: need to initialize
        let mut guard = self.producer.write().expect("producer lock poisoned");

        // Double-check after acquiring write lock
        if let Some(ref producer) = *guard {
            return Some(producer.clone());
        }

        // Read current config from RuntimeContext if available, otherwise fallback to direct load
        #[cfg(not(test))]
        let config = {
            use crate::config::Config;
            let cfg = if let Some(ref ctx) = self.runtime_context {
                // Use RuntimeContext for centralized config access (preferred)
                ctx.config()
            } else {
                // Fallback for tests or if RuntimeContext not provided
                Arc::new(Config::load())
            };
            ShadowConfig::from_config(&cfg)
        };

        #[cfg(test)]
        let config = ShadowConfig::default();

        if !config.is_configured() {
            pgrx::warning!(
                "⚠️  SHADOW MODE WARNING: pg_kafka.shadow_mode_enabled=true but pg_kafka.shadow_bootstrap_servers is not configured. Messages will NOT be forwarded to external Kafka!"
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
        let external_topic = topic_config.effective_external_topic();

        match topic_config.sync_mode {
            SyncMode::Async => {
                // Async mode: send via channel to network thread (non-blocking)
                let forward_tx = self.forward_tx.read().expect("forward_tx lock poisoned");
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
                tracing::debug!("Shadow mode: sync mode, ensuring producer");
                // Sync mode: block on each send using block_on()
                let producer = match self.ensure_producer() {
                    Some(p) => {
                        tracing::debug!("Shadow mode: producer available");
                        p
                    }
                    None => {
                        tracing::warn!("Shadow mode: NO producer available for shadow forwarding");
                        return;
                    }
                };

                tracing::debug!(
                    "Shadow mode: forwarding {} records to {}",
                    records.len(),
                    external_topic
                );

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
                                tracing::warn!(
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
                tracing::warn!(
                    "External write failed for {}[{}]: {:?}",
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
                    let topic_id: i32 = row.get(1).unwrap_or(Some(0)).unwrap_or(0);
                    let topic_name: String = row
                        .get(2)
                        .unwrap_or(Some(String::new()))
                        .unwrap_or_default();
                    let mode_str: String = row
                        .get(3)
                        .unwrap_or(Some("local_only".to_string()))
                        .unwrap_or_default();
                    let forward_percentage: i32 = row.get(4).unwrap_or(Some(0)).unwrap_or(0);
                    let external_topic_name: Option<String> = row.get(5).unwrap_or(None);
                    let sync_mode_str: String = row
                        .get(6)
                        .unwrap_or(Some("sync".to_string()))
                        .unwrap_or_default();
                    let write_mode_str: String = row
                        .get(7)
                        .unwrap_or(Some("dual_write".to_string()))
                        .unwrap_or_default();

                    let config = TopicShadowConfig {
                        topic_id,
                        topic_name,
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

        if !is_enabled {
            tracing::debug!(
                "Shadow mode: is_enabled=false, skipping forward for topic_id={}",
                topic_id
            );
        }
        if !is_primary {
            tracing::debug!(
                "Shadow mode: is_primary=false, skipping forward for topic_id={}",
                topic_id
            );
        }

        if !is_enabled || !is_primary {
            return self.inner.insert_records(topic_id, partition_id, records);
        }

        tracing::debug!(
            "Shadow mode: enabled and primary, checking topic config for topic_id={}",
            topic_id
        );

        // Get topic configuration (if any)
        let topic_config = self.topic_cache.get(topic_id);

        if topic_config.is_none() {
            tracing::debug!(
                "Shadow mode: no topic config found for topic_id={}, skipping forward",
                topic_id
            );
        } else {
            tracing::debug!("Shadow mode: found topic config for topic_id={}", topic_id);
        }

        // Determine write mode
        let write_mode = topic_config
            .as_ref()
            .filter(|c| c.should_forward())
            .map(|c| c.write_mode)
            .unwrap_or(WriteMode::DualWrite); // No config = local only (DualWrite but no forward)

        tracing::debug!(
            "Shadow mode: write_mode={:?} for topic_id={}",
            write_mode,
            topic_id
        );

        match write_mode {
            WriteMode::DualWrite => {
                // Always write locally first
                let base_offset = self.inner.insert_records(topic_id, partition_id, records)?;

                // Then forward to external (best-effort, doesn't affect return)
                if let Some(config) = topic_config {
                    if config.should_forward() {
                        tracing::debug!(
                            "Shadow mode: calling forward_records_best_effort for topic_id={}",
                            topic_id
                        );
                        self.forward_records_best_effort(
                            &config,
                            partition_id,
                            records,
                            base_offset,
                        );
                    } else {
                        tracing::debug!(
                            "Shadow mode: should_forward()=false for topic_id={}",
                            topic_id
                        );
                    }
                } else {
                    tracing::debug!(
                        "Shadow mode: topic_config is None, not forwarding for topic_id={}",
                        topic_id
                    );
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
        // For transactional records, we always write locally first and forward on commit
        // This is because transaction semantics require atomicity - we can't partially
        // forward records from an uncommitted transaction
        //
        // Shadow forwarding for transactions happens in commit_transaction() instead
        // (not yet implemented - would need to track pending records)
        let base_offset = self.inner.insert_transactional_records(
            topic_id,
            partition_id,
            records,
            producer_id,
            producer_epoch,
        )?;

        tracing::trace!(
            "ShadowStore: inserted {} transactional records to topic {} partition {} (forward on commit)",
            records.len(),
            topic_id,
            partition_id
        );

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
        self.inner
            .commit_transaction(transactional_id, producer_id, producer_epoch)
    }

    fn abort_transaction(
        &self,
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
    ) -> Result<()> {
        self.inner
            .abort_transaction(transactional_id, producer_id, producer_epoch)
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
}
