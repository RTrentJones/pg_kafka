//! Shadow mode configuration types
//!
//! This module defines the configuration structures for shadow mode,
//! including global settings from GUCs and per-topic settings from the database.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Shadow mode for a topic
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ShadowMode {
    /// Messages stored locally only, no forwarding
    #[default]
    LocalOnly,
    /// Messages stored locally AND forwarded to external Kafka
    Shadow,
}

impl ShadowMode {
    /// Parse from database string
    pub fn parse(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "shadow" => ShadowMode::Shadow,
            _ => ShadowMode::LocalOnly,
        }
    }

    /// Convert to database string
    pub fn as_str(&self) -> &'static str {
        match self {
            ShadowMode::LocalOnly => "local_only",
            ShadowMode::Shadow => "shadow",
        }
    }
}

/// Sync mode for shadow forwarding
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SyncMode {
    /// Non-blocking: local write completes immediately, forwarding is async
    #[default]
    Async,
    /// Blocking: wait for external Kafka acknowledgment before returning
    Sync,
}

impl SyncMode {
    /// Parse from database/config string
    pub fn parse(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "sync" => SyncMode::Sync,
            _ => SyncMode::Async,
        }
    }

    /// Convert to database string
    pub fn as_str(&self) -> &'static str {
        match self {
            SyncMode::Async => "async",
            SyncMode::Sync => "sync",
        }
    }
}

/// Write mode for shadow forwarding
///
/// Controls whether messages are written to local storage, external Kafka, or both.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WriteMode {
    /// Write to both local and external Kafka (safe for migration)
    /// On forward failure, message is still stored locally
    #[default]
    DualWrite,
    /// Write to external Kafka only, fallback to local on failure
    /// Messages successfully forwarded are NOT stored locally
    ExternalOnly,
}

impl WriteMode {
    /// Parse from database/config string
    pub fn parse(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "external_only" => WriteMode::ExternalOnly,
            _ => WriteMode::DualWrite,
        }
    }

    /// Convert to database string
    pub fn as_str(&self) -> &'static str {
        match self {
            WriteMode::DualWrite => "dual_write",
            WriteMode::ExternalOnly => "external_only",
        }
    }
}

/// Per-topic shadow configuration
#[derive(Debug, Clone)]
pub struct TopicShadowConfig {
    /// Topic ID in kafka.topics
    pub topic_id: i32,
    /// Local topic name
    pub topic_name: String,
    /// Shadow mode (local_only or shadow)
    pub mode: ShadowMode,
    /// Percentage of messages to forward (0-100)
    pub forward_percentage: u8,
    /// External topic name (None = same as local)
    pub external_topic_name: Option<String>,
    /// Sync mode for this topic (async = non-blocking via channel, sync = blocking)
    pub sync_mode: SyncMode,
    /// Write mode (dual_write or external_only)
    pub write_mode: WriteMode,
}

impl TopicShadowConfig {
    /// Get the effective external topic name
    pub fn effective_external_topic(&self) -> &str {
        self.external_topic_name
            .as_deref()
            .unwrap_or(&self.topic_name)
    }

    /// Check if this topic should forward messages
    pub fn should_forward(&self) -> bool {
        self.mode == ShadowMode::Shadow && self.forward_percentage > 0
    }
}

/// Global shadow mode configuration from GUCs
#[derive(Debug, Clone)]
pub struct ShadowConfig {
    /// Whether shadow mode is enabled globally
    pub enabled: bool,
    /// External Kafka bootstrap servers
    pub bootstrap_servers: String,
    /// Security protocol (SASL_SSL, SASL_PLAINTEXT, SSL, PLAINTEXT)
    pub security_protocol: String,
    /// SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)
    pub sasl_mechanism: String,
    /// SASL username
    pub sasl_username: String,
    /// SASL password
    pub sasl_password: String,
    /// SSL CA certificate location
    pub ssl_ca_location: String,
    /// Batch size for forwarding
    pub batch_size: i32,
    /// Linger time in milliseconds
    pub linger_ms: i32,
    /// Retry backoff in milliseconds
    pub retry_backoff_ms: i32,
    /// Maximum retries per message
    pub max_retries: i32,
    /// Default sync mode
    pub default_sync_mode: SyncMode,
    /// Whether metrics are enabled
    pub metrics_enabled: bool,
    /// OpenTelemetry endpoint
    pub otel_endpoint: String,
}

impl ShadowConfig {
    /// Create from the global Config struct
    pub fn from_config(config: &crate::config::Config) -> Self {
        ShadowConfig {
            enabled: config.shadow_mode_enabled,
            bootstrap_servers: config.shadow_bootstrap_servers.clone(),
            security_protocol: config.shadow_security_protocol.clone(),
            sasl_mechanism: config.shadow_sasl_mechanism.clone(),
            sasl_username: config.shadow_sasl_username.clone(),
            sasl_password: config.shadow_sasl_password.clone(),
            ssl_ca_location: config.shadow_ssl_ca_location.clone(),
            batch_size: config.shadow_batch_size,
            linger_ms: config.shadow_linger_ms,
            retry_backoff_ms: config.shadow_retry_backoff_ms,
            max_retries: config.shadow_max_retries,
            default_sync_mode: SyncMode::parse(&config.shadow_default_sync_mode),
            metrics_enabled: config.shadow_metrics_enabled,
            otel_endpoint: config.shadow_otel_endpoint.clone(),
        }
    }

    /// Check if shadow mode is properly configured
    pub fn is_configured(&self) -> bool {
        self.enabled && !self.bootstrap_servers.is_empty()
    }
}

impl Default for ShadowConfig {
    fn default() -> Self {
        use crate::kafka::constants::*;
        Self {
            enabled: DEFAULT_SHADOW_MODE_ENABLED,
            bootstrap_servers: DEFAULT_SHADOW_BOOTSTRAP_SERVERS.to_string(),
            security_protocol: DEFAULT_SHADOW_SECURITY_PROTOCOL.to_string(),
            sasl_mechanism: DEFAULT_SHADOW_SASL_MECHANISM.to_string(),
            sasl_username: String::new(),
            sasl_password: String::new(),
            ssl_ca_location: String::new(),
            batch_size: DEFAULT_SHADOW_BATCH_SIZE,
            linger_ms: DEFAULT_SHADOW_LINGER_MS,
            retry_backoff_ms: DEFAULT_SHADOW_RETRY_BACKOFF_MS,
            max_retries: DEFAULT_SHADOW_MAX_RETRIES,
            default_sync_mode: SyncMode::parse(DEFAULT_SHADOW_SYNC_MODE),
            metrics_enabled: DEFAULT_SHADOW_METRICS_ENABLED,
            otel_endpoint: DEFAULT_SHADOW_OTEL_ENDPOINT.to_string(),
        }
    }
}

/// Thread-safe cache of per-topic shadow configurations
#[derive(Debug, Default)]
pub struct TopicConfigCache {
    configs: Arc<RwLock<HashMap<i32, TopicShadowConfig>>>,
}

impl TopicConfigCache {
    /// Create a new empty cache
    pub fn new() -> Self {
        Self::default()
    }

    /// Get configuration for a topic
    pub fn get(&self, topic_id: i32) -> Option<TopicShadowConfig> {
        self.configs
            .read()
            .expect("TopicConfigCache lock poisoned")
            .get(&topic_id)
            .cloned()
    }

    /// Update configuration for a topic
    pub fn update(&self, config: TopicShadowConfig) {
        self.configs
            .write()
            .expect("TopicConfigCache lock poisoned")
            .insert(config.topic_id, config);
    }

    /// Remove configuration for a topic
    pub fn remove(&self, topic_id: i32) {
        self.configs
            .write()
            .expect("TopicConfigCache lock poisoned")
            .remove(&topic_id);
    }

    /// Clear all cached configurations
    pub fn clear(&self) {
        self.configs
            .write()
            .expect("TopicConfigCache lock poisoned")
            .clear();
    }

    /// Get all cached configurations
    pub fn all(&self) -> Vec<TopicShadowConfig> {
        self.configs
            .read()
            .expect("TopicConfigCache lock poisoned")
            .values()
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shadow_mode_from_str() {
        assert_eq!(ShadowMode::parse("shadow"), ShadowMode::Shadow);
        assert_eq!(ShadowMode::parse("SHADOW"), ShadowMode::Shadow);
        assert_eq!(ShadowMode::parse("local_only"), ShadowMode::LocalOnly);
        assert_eq!(ShadowMode::parse("invalid"), ShadowMode::LocalOnly);
    }

    #[test]
    fn test_sync_mode_from_str() {
        assert_eq!(SyncMode::parse("sync"), SyncMode::Sync);
        assert_eq!(SyncMode::parse("SYNC"), SyncMode::Sync);
        assert_eq!(SyncMode::parse("async"), SyncMode::Async);
        assert_eq!(SyncMode::parse("invalid"), SyncMode::Async);
    }

    #[test]
    fn test_topic_config_effective_external_topic() {
        let config = TopicShadowConfig {
            topic_id: 1,
            topic_name: "local-topic".to_string(),
            mode: ShadowMode::Shadow,
            forward_percentage: 100,
            external_topic_name: None,
            sync_mode: SyncMode::Async,
            write_mode: WriteMode::DualWrite,
        };
        assert_eq!(config.effective_external_topic(), "local-topic");

        let config_with_external = TopicShadowConfig {
            topic_id: 1,
            topic_name: "local-topic".to_string(),
            mode: ShadowMode::Shadow,
            forward_percentage: 100,
            external_topic_name: Some("external-topic".to_string()),
            sync_mode: SyncMode::Async,
            write_mode: WriteMode::DualWrite,
        };
        assert_eq!(
            config_with_external.effective_external_topic(),
            "external-topic"
        );
    }

    #[test]
    fn test_topic_config_should_forward() {
        let shadow_100 = TopicShadowConfig {
            topic_id: 1,
            topic_name: "test".to_string(),
            mode: ShadowMode::Shadow,
            forward_percentage: 100,
            external_topic_name: None,
            sync_mode: SyncMode::Async,
            write_mode: WriteMode::DualWrite,
        };
        assert!(shadow_100.should_forward());

        let shadow_0 = TopicShadowConfig {
            topic_id: 1,
            topic_name: "test".to_string(),
            mode: ShadowMode::Shadow,
            forward_percentage: 0,
            external_topic_name: None,
            sync_mode: SyncMode::Async,
            write_mode: WriteMode::DualWrite,
        };
        assert!(!shadow_0.should_forward());

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
    }

    #[test]
    fn test_topic_config_cache() {
        let cache = TopicConfigCache::new();

        // Initially empty
        assert!(cache.get(1).is_none());

        // Add a config
        let config = TopicShadowConfig {
            topic_id: 1,
            topic_name: "test".to_string(),
            mode: ShadowMode::Shadow,
            forward_percentage: 50,
            external_topic_name: None,
            sync_mode: SyncMode::Async,
            write_mode: WriteMode::DualWrite,
        };
        cache.update(config.clone());

        // Should be retrievable
        let retrieved = cache.get(1).unwrap();
        assert_eq!(retrieved.topic_id, 1);
        assert_eq!(retrieved.forward_percentage, 50);

        // Remove it
        cache.remove(1);
        assert!(cache.get(1).is_none());
    }

    #[test]
    fn test_write_mode_from_str() {
        assert_eq!(WriteMode::parse("dual_write"), WriteMode::DualWrite);
        assert_eq!(WriteMode::parse("DUAL_WRITE"), WriteMode::DualWrite);
        assert_eq!(WriteMode::parse("external_only"), WriteMode::ExternalOnly);
        assert_eq!(WriteMode::parse("EXTERNAL_ONLY"), WriteMode::ExternalOnly);
        assert_eq!(WriteMode::parse("invalid"), WriteMode::DualWrite);
    }
}
