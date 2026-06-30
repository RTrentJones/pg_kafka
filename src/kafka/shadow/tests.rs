// Copyright (c) 2026 Robert Trent Jones.
// This file is part of the "Shadow Mode" feature of pg_kafka.
//
// Use of this source code for production purposes is governed by the
// Commercial License found in the LICENSE file in this directory.
// Development and evaluation use is permitted.
//
// GitHub Sponsors: https://github.com/sponsors/RTrentJones

//! Integration tests for shadow mode components
//!
//! These tests verify that shadow mode components work correctly together.
//! They use mocks where external dependencies (rdkafka, SPI) are required.

use super::config::{
    ShadowConfig, ShadowMode, SyncMode, TopicConfigCache, TopicShadowConfig, WriteMode,
};
use super::error::ShadowError;
use super::primary::PrimaryStatus;

/// Test that shadow configuration properly validates
mod config_tests {
    use super::*;

    #[test]
    fn test_shadow_config_is_configured() {
        let mut config = ShadowConfig::default();
        assert!(
            !config.is_configured(),
            "Default config should not be configured"
        );

        config.enabled = true;
        assert!(
            !config.is_configured(),
            "Enabled but no servers should not be configured"
        );

        config.bootstrap_servers = "localhost:9092".to_string();
        assert!(
            config.is_configured(),
            "Enabled with servers should be configured"
        );

        config.enabled = false;
        assert!(!config.is_configured(), "Disabled should not be configured");
    }

    #[test]
    fn test_topic_config_effective_external_topic() {
        let config = TopicShadowConfig {
            topic_id: 1,
            topic_name: "internal-events".to_string(),
            mode: ShadowMode::Shadow,
            forward_percentage: 100,
            external_topic_name: None,
            sync_mode: SyncMode::Async,
            write_mode: WriteMode::DualWrite,
        };
        assert_eq!(config.effective_external_topic(), "internal-events");

        let config_with_mapping = TopicShadowConfig {
            topic_id: 1,
            topic_name: "internal-events".to_string(),
            mode: ShadowMode::Shadow,
            forward_percentage: 100,
            external_topic_name: Some("prod-events".to_string()),
            sync_mode: SyncMode::Async,
            write_mode: WriteMode::DualWrite,
        };
        assert_eq!(
            config_with_mapping.effective_external_topic(),
            "prod-events"
        );
    }

    #[test]
    fn test_topic_config_should_forward_conditions() {
        // Shadow mode with percentage > 0 should forward
        let shadow_active = TopicShadowConfig {
            topic_id: 1,
            topic_name: "test".to_string(),
            mode: ShadowMode::Shadow,
            forward_percentage: 50,
            external_topic_name: None,
            sync_mode: SyncMode::Async,
            write_mode: WriteMode::DualWrite,
        };
        assert!(shadow_active.should_forward());

        // LocalOnly should never forward
        let local_only = TopicShadowConfig {
            topic_id: 1,
            topic_name: "test".to_string(),
            mode: ShadowMode::LocalOnly,
            forward_percentage: 100,
            external_topic_name: None,
            sync_mode: SyncMode::Async,
            write_mode: WriteMode::DualWrite,
        };
        assert!(!local_only.should_forward());

        // Shadow with 0% should not forward
        let shadow_zero = TopicShadowConfig {
            topic_id: 1,
            topic_name: "test".to_string(),
            mode: ShadowMode::Shadow,
            forward_percentage: 0,
            external_topic_name: None,
            sync_mode: SyncMode::Async,
            write_mode: WriteMode::DualWrite,
        };
        assert!(!shadow_zero.should_forward());
    }
}

/// Test topic configuration cache behavior
mod cache_tests {
    use super::*;

    #[test]
    fn test_cache_crud_operations() {
        let cache = TopicConfigCache::new();

        // Create
        let config = TopicShadowConfig {
            topic_id: 42,
            topic_name: "events".to_string(),
            mode: ShadowMode::Shadow,
            forward_percentage: 75,
            external_topic_name: None,
            sync_mode: SyncMode::Sync,
            write_mode: WriteMode::DualWrite,
        };
        cache.update(config);

        // Read
        let retrieved = cache.get(42).expect("Should find config");
        assert_eq!(retrieved.topic_name, "events");
        assert_eq!(retrieved.forward_percentage, 75);
        assert_eq!(retrieved.sync_mode, SyncMode::Sync);

        // Update
        let updated = TopicShadowConfig {
            topic_id: 42,
            topic_name: "events".to_string(),
            mode: ShadowMode::Shadow,
            forward_percentage: 100,
            external_topic_name: Some("prod-events".to_string()),
            sync_mode: SyncMode::Async,
            write_mode: WriteMode::DualWrite,
        };
        cache.update(updated);

        let retrieved = cache.get(42).expect("Should find updated config");
        assert_eq!(retrieved.forward_percentage, 100);
        assert_eq!(
            retrieved.external_topic_name,
            Some("prod-events".to_string())
        );

        // Delete
        cache.remove(42);
        assert!(cache.get(42).is_none());
    }

    #[test]
    fn test_cache_all() {
        let cache = TopicConfigCache::new();

        // Add multiple configs
        for i in 1..=5 {
            cache.update(TopicShadowConfig {
                topic_id: i,
                topic_name: format!("topic-{}", i),
                mode: ShadowMode::Shadow,
                forward_percentage: (i * 20) as u8,
                external_topic_name: None,
                sync_mode: SyncMode::Async,
                write_mode: WriteMode::DualWrite,
            });
        }

        let all = cache.all();
        assert_eq!(all.len(), 5);

        // Verify all are present
        let topic_ids: Vec<i32> = all.iter().map(|c| c.topic_id).collect();
        for i in 1..=5 {
            assert!(topic_ids.contains(&i));
        }
    }

    #[test]
    fn test_cache_clear() {
        let cache = TopicConfigCache::new();

        for i in 1..=3 {
            cache.update(TopicShadowConfig {
                topic_id: i,
                topic_name: format!("topic-{}", i),
                mode: ShadowMode::Shadow,
                forward_percentage: 100,
                external_topic_name: None,
                sync_mode: SyncMode::Async,
                write_mode: WriteMode::DualWrite,
            });
        }

        assert_eq!(cache.all().len(), 3);

        cache.clear();
        assert_eq!(cache.all().len(), 0);
    }

    #[test]
    fn test_cache_get_by_name() {
        let cache = TopicConfigCache::new();

        cache.update(TopicShadowConfig {
            topic_id: 1,
            topic_name: "alpha".to_string(),
            mode: ShadowMode::Shadow,
            forward_percentage: 50,
            external_topic_name: None,
            sync_mode: SyncMode::Async,
            write_mode: WriteMode::DualWrite,
        });

        cache.update(TopicShadowConfig {
            topic_id: 2,
            topic_name: "beta".to_string(),
            mode: ShadowMode::LocalOnly,
            forward_percentage: 100,
            external_topic_name: None,
            sync_mode: SyncMode::Async,
            write_mode: WriteMode::DualWrite,
        });

        let alpha = cache.get_by_name("alpha");
        assert!(alpha.is_some());
        assert_eq!(alpha.unwrap().topic_id, 1);

        let beta = cache.get_by_name("beta");
        assert!(beta.is_some());
        assert_eq!(beta.unwrap().mode, ShadowMode::LocalOnly);

        let gamma = cache.get_by_name("gamma");
        assert!(gamma.is_none());
    }
}

/// Test primary status detection
mod primary_tests {
    use super::*;

    #[test]
    fn test_primary_status_lifecycle() {
        let mut status = PrimaryStatus::new();

        // Initially uncached
        assert!(!status.is_cached());

        // First check caches
        let result1 = status.check();
        assert!(status.is_cached());

        // Subsequent checks return cached value
        let result2 = status.check();
        let result3 = status.check();
        assert_eq!(result1, result2);
        assert_eq!(result2, result3);

        // Refresh clears and re-fetches
        let result4 = status.refresh();
        assert!(status.is_cached());
        // In test mode, always returns true
        assert!(result4);
    }
}

/// Test error types and codes
mod error_tests {
    use super::*;
    use crate::kafka::constants::*;

    #[test]
    fn test_error_codes_mapping() {
        assert_eq!(
            ShadowError::NotEnabled.to_error_code(),
            ERROR_SHADOW_NOT_CONFIGURED
        );

        assert_eq!(
            ShadowError::NotConfigured("test".to_string()).to_error_code(),
            ERROR_SHADOW_NOT_CONFIGURED
        );

        assert_eq!(
            ShadowError::KafkaUnavailable("test".to_string()).to_error_code(),
            ERROR_SHADOW_KAFKA_UNAVAILABLE
        );

        assert_eq!(
            ShadowError::ForwardFailed {
                topic: "t".to_string(),
                partition: 0,
                error: "e".to_string()
            }
            .to_error_code(),
            ERROR_SHADOW_FORWARD_FAILED
        );

        assert_eq!(
            ShadowError::ReplayFailed {
                topic: "t".to_string(),
                from_offset: 0,
                to_offset: 100,
                error: "e".to_string()
            }
            .to_error_code(),
            ERROR_SHADOW_REPLAY_FAILED
        );
    }

    #[test]
    fn test_error_display_messages() {
        let err = ShadowError::Timeout {
            topic: "events".to_string(),
            partition: 3,
            timeout_ms: 5000,
        };
        let msg = err.to_string();
        assert!(msg.contains("events"));
        assert!(msg.contains("3"));
        assert!(msg.contains("5000"));

        let err = ShadowError::InvalidTopicConfig {
            topic_id: 42,
            reason: "invalid percentage".to_string(),
        };
        let msg = err.to_string();
        assert!(msg.contains("42"));
        assert!(msg.contains("invalid percentage"));
    }
}

/// Test mode and sync mode parsing
mod mode_parsing_tests {
    use super::*;

    #[test]
    fn test_shadow_mode_round_trip() {
        for mode in [ShadowMode::LocalOnly, ShadowMode::Shadow] {
            let s = mode.as_str();
            let parsed = ShadowMode::parse(s);
            assert_eq!(parsed, mode);
        }
    }

    #[test]
    fn test_sync_mode_round_trip() {
        for mode in [SyncMode::Async, SyncMode::Sync] {
            let s = mode.as_str();
            let parsed = SyncMode::parse(s);
            assert_eq!(parsed, mode);
        }
    }

    #[test]
    fn test_mode_case_insensitive() {
        assert_eq!(ShadowMode::parse("SHADOW"), ShadowMode::Shadow);
        assert_eq!(ShadowMode::parse("Shadow"), ShadowMode::Shadow);
        assert_eq!(ShadowMode::parse("LOCAL_ONLY"), ShadowMode::LocalOnly);

        assert_eq!(SyncMode::parse("SYNC"), SyncMode::Sync);
        assert_eq!(SyncMode::parse("Sync"), SyncMode::Sync);
        assert_eq!(SyncMode::parse("ASYNC"), SyncMode::Async);
    }
}
