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
use super::config::{SyncMode, WriteMode};
use super::config::{TopicConfigCache, TopicShadowConfig};

/// Result of a forward decision
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForwardDecision {
    /// Forward this message to external Kafka
    Forward,
    /// Skip this message (local only)
    Skip,
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
