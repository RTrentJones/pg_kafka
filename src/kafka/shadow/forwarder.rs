// Copyright (c) 2026 Robert Trent Jones.
// This file is part of the "Shadow Mode" feature of pg_kafka.
//
// Use of this source code for production purposes is governed by the
// Commercial License found in the LICENSE file in this directory.
// Development and evaluation use is permitted.
//
// GitHub Sponsors: https://github.com/sponsors/RTrentJones

//! Shadow mode message forwarder
//!
//! This module handles the decision and execution of forwarding messages to
//! external Kafka based on per-topic configuration and percentage routing.
//!
//! ## Percentage Routing
//!
//! Uses deterministic murmur2 hashing on the message key (or global_offset if no key)
//! to decide whether to forward. This ensures:
//! - Same message key always routes the same way
//! - Percentage dial-up affects new keys predictably
//! - No coordination needed between multiple instances

#[cfg(test)]
use super::config::WriteMode;
use super::config::{ShadowConfig, SyncMode, TopicConfigCache, TopicShadowConfig};
use super::error::{ShadowError, ShadowResult};
use super::primary::PrimaryStatus;
use super::producer::ShadowProducer;
use super::routing::make_forward_decision;
use std::sync::Arc;

/// Default timeout for sync forwards (milliseconds)
const DEFAULT_SYNC_TIMEOUT_MS: u64 = 30_000;

/// Message to be forwarded to external Kafka
#[derive(Debug, Clone)]
pub struct ForwardMessage {
    /// Topic name
    pub topic: String,
    /// Partition ID
    pub partition: i32,
    /// Message key (optional)
    pub key: Option<Vec<u8>>,
    /// Message value (optional)
    pub value: Option<Vec<u8>>,
    /// Global offset in pg_kafka (for tracking)
    pub global_offset: i64,
    /// Partition offset in pg_kafka
    pub partition_offset: i64,
}

/// Result of a forward decision
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForwardDecision {
    /// Forward this message to external Kafka
    Forward,
    /// Skip this message (local only)
    Skip,
}

/// Result of forwarding a batch of messages
#[derive(Debug, Default)]
pub struct ForwardResult {
    /// Number of messages forwarded successfully
    pub forwarded: u64,
    /// Number of messages skipped (not selected by percentage)
    pub skipped: u64,
    /// Number of messages failed
    pub failed: u64,
    /// First error encountered (if any)
    pub first_error: Option<ShadowError>,
}

/// Shadow forwarder that handles message forwarding decisions and execution
pub struct ShadowForwarder {
    /// Producer for sending to external Kafka
    producer: Arc<ShadowProducer>,
    /// Topic configuration cache
    topic_cache: Arc<TopicConfigCache>,
    /// Primary status checker
    primary_status: PrimaryStatus,
    /// Global configuration
    config: Arc<ShadowConfig>,
}

impl ShadowForwarder {
    /// Create a new shadow forwarder
    pub fn new(
        producer: Arc<ShadowProducer>,
        topic_cache: Arc<TopicConfigCache>,
        config: Arc<ShadowConfig>,
    ) -> Self {
        Self {
            producer,
            topic_cache,
            primary_status: PrimaryStatus::new(),
            config,
        }
    }

    /// Check if we should forward at all
    ///
    /// Returns false if:
    /// - Shadow mode is not enabled
    /// - We're running on a standby
    pub fn should_forward_globally(&mut self) -> bool {
        // Check if shadow mode is enabled
        if !self.config.enabled {
            return false;
        }

        // Only forward on primary
        self.primary_status.check()
    }

    /// Decide whether to forward a specific message
    pub fn decide_forward(
        &self,
        topic_id: i32,
        key: Option<&[u8]>,
        global_offset: i64,
    ) -> ForwardDecision {
        // Look up topic configuration
        let topic_config = match self.topic_cache.get(topic_id) {
            Some(config) => config,
            None => return ForwardDecision::Skip, // No config = local only
        };

        // Check if topic is in shadow mode
        if !topic_config.should_forward() {
            return ForwardDecision::Skip;
        }

        // Delegate percentage routing to shared function
        make_forward_decision(key, global_offset, topic_config.forward_percentage)
    }

    /// Forward a single message
    pub async fn forward_message(&self, msg: &ForwardMessage) -> ShadowResult<()> {
        let topic_config = self.topic_cache.get_by_name(&msg.topic).ok_or_else(|| {
            ShadowError::NotConfigured(format!("Topic {} not configured", msg.topic))
        })?;

        let external_topic = topic_config.effective_external_topic();
        let key_ref = msg.key.as_deref();
        let value_ref = msg.value.as_deref();

        match topic_config.sync_mode {
            SyncMode::Async => {
                self.producer
                    .send_async(external_topic, Some(msg.partition), key_ref, value_ref)
                    .await
            }
            SyncMode::Sync => self.producer.send_sync(
                external_topic,
                Some(msg.partition),
                key_ref,
                value_ref,
                DEFAULT_SYNC_TIMEOUT_MS,
            ),
        }
    }

    /// Forward a batch of messages
    ///
    /// Returns aggregate results including success/failure counts.
    pub async fn forward_batch(&mut self, messages: &[ForwardMessage]) -> ForwardResult {
        let mut result = ForwardResult::default();

        // Early exit if we shouldn't forward globally
        if !self.should_forward_globally() {
            result.skipped = messages.len() as u64;
            return result;
        }

        for msg in messages {
            // Get topic ID from cache
            let topic_id = match self.topic_cache.get_topic_id(&msg.topic) {
                Some(id) => id,
                None => {
                    result.skipped += 1;
                    continue;
                }
            };

            // Decide whether to forward
            match self.decide_forward(topic_id, msg.key.as_deref(), msg.global_offset) {
                ForwardDecision::Skip => {
                    result.skipped += 1;
                }
                ForwardDecision::Forward => match self.forward_message(msg).await {
                    Ok(()) => {
                        result.forwarded += 1;
                    }
                    Err(e) => {
                        result.failed += 1;
                        if result.first_error.is_none() {
                            result.first_error = Some(e);
                        }
                    }
                },
            }
        }

        result
    }

    /// Get the underlying producer
    pub fn producer(&self) -> &ShadowProducer {
        &self.producer
    }

    /// Get the topic cache
    pub fn topic_cache(&self) -> &TopicConfigCache {
        &self.topic_cache
    }
}

impl TopicConfigCache {
    /// Get configuration by topic name
    pub fn get_by_name(&self, name: &str) -> Option<TopicShadowConfig> {
        self.all().into_iter().find(|c| c.topic_name == name)
    }

    /// Get topic ID by name
    pub fn get_topic_id(&self, name: &str) -> Option<i32> {
        self.get_by_name(name).map(|c| c.topic_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::shadow::config::ShadowMode;

    #[test]
    fn test_forward_decision_with_percentage() {
        let cache = TopicConfigCache::new();
        let config = TopicShadowConfig {
            topic_id: 1,
            topic_name: "test".to_string(),
            mode: ShadowMode::Shadow,
            forward_percentage: 50,
            external_topic_name: None,
            sync_mode: SyncMode::Async,
            write_mode: WriteMode::DualWrite,
        };
        cache.update(config);

        // Create a minimal forwarder for testing
        // We can't fully test without a producer, but we can test the cache
        let retrieved = cache.get(1).unwrap();
        assert!(retrieved.should_forward());
        assert_eq!(retrieved.forward_percentage, 50);
    }

    #[test]
    fn test_forward_decision_local_only() {
        let cache = TopicConfigCache::new();
        let config = TopicShadowConfig {
            topic_id: 1,
            topic_name: "test".to_string(),
            mode: ShadowMode::LocalOnly,
            forward_percentage: 100,
            external_topic_name: None,
            sync_mode: SyncMode::Async,
            write_mode: WriteMode::DualWrite,
        };
        cache.update(config);

        let retrieved = cache.get(1).unwrap();
        assert!(!retrieved.should_forward(), "LocalOnly should not forward");
    }

    #[test]
    fn test_topic_cache_get_by_name() {
        let cache = TopicConfigCache::new();
        let config = TopicShadowConfig {
            topic_id: 1,
            topic_name: "my-topic".to_string(),
            mode: ShadowMode::Shadow,
            forward_percentage: 100,
            external_topic_name: Some("external-my-topic".to_string()),
            sync_mode: SyncMode::Async,
            write_mode: WriteMode::DualWrite,
        };
        cache.update(config);

        let found = cache.get_by_name("my-topic");
        assert!(found.is_some());
        assert_eq!(
            found.unwrap().external_topic_name,
            Some("external-my-topic".to_string())
        );

        let not_found = cache.get_by_name("other-topic");
        assert!(not_found.is_none());
    }

    #[test]
    fn test_forward_result_default() {
        let result = ForwardResult::default();
        assert_eq!(result.forwarded, 0);
        assert_eq!(result.skipped, 0);
        assert_eq!(result.failed, 0);
        assert!(result.first_error.is_none());
    }

    // ========== ForwardMessage Tests ==========

    #[test]
    fn test_forward_message_construction() {
        let msg = ForwardMessage {
            topic: "test-topic".to_string(),
            partition: 0,
            key: Some(b"key".to_vec()),
            value: Some(b"value".to_vec()),
            global_offset: 100,
            partition_offset: 50,
        };
        assert_eq!(msg.topic, "test-topic");
        assert_eq!(msg.partition, 0);
        assert_eq!(msg.key, Some(b"key".to_vec()));
        assert_eq!(msg.value, Some(b"value".to_vec()));
        assert_eq!(msg.global_offset, 100);
        assert_eq!(msg.partition_offset, 50);
    }

    #[test]
    fn test_forward_message_with_null_key() {
        let msg = ForwardMessage {
            topic: "null-key-topic".to_string(),
            partition: 1,
            key: None,
            value: Some(b"value-only".to_vec()),
            global_offset: 200,
            partition_offset: 100,
        };
        assert!(msg.key.is_none());
        assert!(msg.value.is_some());
    }

    #[test]
    fn test_forward_message_with_null_value() {
        let msg = ForwardMessage {
            topic: "null-value-topic".to_string(),
            partition: 2,
            key: Some(b"key-only".to_vec()),
            value: None,
            global_offset: 300,
            partition_offset: 150,
        };
        assert!(msg.key.is_some());
        assert!(msg.value.is_none());
    }

    #[test]
    fn test_forward_message_clone() {
        let msg = ForwardMessage {
            topic: "clone-topic".to_string(),
            partition: 5,
            key: Some(b"clone-key".to_vec()),
            value: Some(b"clone-value".to_vec()),
            global_offset: 999,
            partition_offset: 500,
        };
        let cloned = msg.clone();
        assert_eq!(cloned.topic, msg.topic);
        assert_eq!(cloned.partition, msg.partition);
        assert_eq!(cloned.key, msg.key);
        assert_eq!(cloned.value, msg.value);
        assert_eq!(cloned.global_offset, msg.global_offset);
        assert_eq!(cloned.partition_offset, msg.partition_offset);
    }

    #[test]
    fn test_forward_message_debug_format() {
        let msg = ForwardMessage {
            topic: "debug-topic".to_string(),
            partition: 3,
            key: None,
            value: None,
            global_offset: 0,
            partition_offset: 0,
        };
        let debug = format!("{:?}", msg);
        assert!(debug.contains("ForwardMessage"));
        assert!(debug.contains("debug-topic"));
    }

    // ========== ForwardDecision Tests ==========

    #[test]
    fn test_forward_decision_forward_variant() {
        let decision = ForwardDecision::Forward;
        assert!(matches!(decision, ForwardDecision::Forward));
        assert_eq!(decision, ForwardDecision::Forward);
    }

    #[test]
    fn test_forward_decision_skip_variant() {
        let decision = ForwardDecision::Skip;
        assert!(matches!(decision, ForwardDecision::Skip));
        assert_eq!(decision, ForwardDecision::Skip);
    }

    #[test]
    fn test_forward_decision_equality() {
        assert_eq!(ForwardDecision::Forward, ForwardDecision::Forward);
        assert_eq!(ForwardDecision::Skip, ForwardDecision::Skip);
        assert_ne!(ForwardDecision::Forward, ForwardDecision::Skip);
    }

    #[test]
    fn test_forward_decision_debug_format() {
        let forward = ForwardDecision::Forward;
        let skip = ForwardDecision::Skip;
        assert!(format!("{:?}", forward).contains("Forward"));
        assert!(format!("{:?}", skip).contains("Skip"));
    }

    // ========== ForwardResult Tests ==========

    #[test]
    fn test_forward_result_with_values() {
        let result = ForwardResult {
            forwarded: 10,
            skipped: 5,
            failed: 2,
            first_error: None,
        };
        assert_eq!(result.forwarded, 10);
        assert_eq!(result.skipped, 5);
        assert_eq!(result.failed, 2);
    }

    #[test]
    fn test_forward_result_with_error() {
        let result = ForwardResult {
            forwarded: 8,
            skipped: 0,
            failed: 2,
            first_error: Some(ShadowError::NotEnabled),
        };
        assert!(result.first_error.is_some());
        assert_eq!(result.failed, 2);
    }

    #[test]
    fn test_forward_result_debug_format() {
        let result = ForwardResult::default();
        let debug = format!("{:?}", result);
        assert!(debug.contains("ForwardResult"));
        assert!(debug.contains("forwarded"));
        assert!(debug.contains("skipped"));
    }

    // ========== TopicConfigCache Extension Tests ==========

    #[test]
    fn test_topic_cache_get_topic_id() {
        let cache = TopicConfigCache::new();
        let config = TopicShadowConfig {
            topic_id: 42,
            topic_name: "id-test-topic".to_string(),
            mode: ShadowMode::Shadow,
            forward_percentage: 100,
            external_topic_name: None,
            sync_mode: SyncMode::Async,
            write_mode: WriteMode::DualWrite,
        };
        cache.update(config);

        let id = cache.get_topic_id("id-test-topic");
        assert_eq!(id, Some(42));

        let missing = cache.get_topic_id("nonexistent");
        assert_eq!(missing, None);
    }

    #[test]
    fn test_forward_decision_shadow_mode_with_sync() {
        let cache = TopicConfigCache::new();
        let config = TopicShadowConfig {
            topic_id: 1,
            topic_name: "shadow-sync".to_string(),
            mode: ShadowMode::Shadow,
            forward_percentage: 100,
            external_topic_name: Some("external-topic".to_string()),
            sync_mode: SyncMode::Sync,
            write_mode: WriteMode::ExternalOnly,
        };
        cache.update(config);

        let retrieved = cache.get(1).unwrap();
        assert!(retrieved.should_forward(), "Shadow mode should forward");
        assert_eq!(retrieved.sync_mode, SyncMode::Sync);
    }
}
