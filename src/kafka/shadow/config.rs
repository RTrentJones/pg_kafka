// Copyright (c) 2026 Robert Trent Jones.
// This file is part of the "Shadow Mode" feature of pg_kafka.
//
// Use of this source code for production purposes is governed by the
// Commercial License found in the LICENSE file in this directory.
// Development and evaluation use is permitted.
//
// GitHub Sponsors: https://github.com/sponsors/RTrentJones

//! Shadow mode configuration: the global settings (from the shadow GUCs) that
//! build the external producer, and the per-topic settings (from
//! `kafka.shadow_config`) with the cache the DB thread consults per produce.

use std::collections::HashMap;
use std::sync::RwLock;

/// Shadow mode for a topic (`kafka.shadow_config.mode`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ShadowMode {
    /// Messages stored locally only, no forwarding.
    #[default]
    LocalOnly,
    /// Messages stored locally AND forwarded to external Kafka.
    Shadow,
}

impl ShadowMode {
    /// Parse a `kafka.shadow_config.mode` value (case-insensitive; anything
    /// unknown is `LocalOnly`).
    pub fn parse(s: &str) -> Self {
        if s.eq_ignore_ascii_case("shadow") {
            ShadowMode::Shadow
        } else {
            ShadowMode::LocalOnly
        }
    }
}

/// When a forward happens relative to the produce response
/// (`kafka.shadow_config.sync_mode`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SyncMode {
    /// The response goes out at commit; the outbox poll forwards later.
    #[default]
    Async,
    /// The response is held until the external broker acknowledges the
    /// records, or `pg_kafka.shadow_sync_timeout_ms` passes.
    Sync,
}

impl SyncMode {
    /// Parse a `kafka.shadow_config.sync_mode` value (case-insensitive;
    /// anything unknown is `Async`).
    pub fn parse(s: &str) -> Self {
        if s.eq_ignore_ascii_case("sync") {
            SyncMode::Sync
        } else {
            SyncMode::Async
        }
    }
}

/// Which copy of a forwarded topic consumers read
/// (`kafka.shadow_config.write_mode`). Both modes store every record locally
/// and forward it through the outbox; they differ only in reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WriteMode {
    /// Local reads are served as usual (dual write during a migration).
    #[default]
    DualWrite,
    /// The external cluster is the primary copy: local reads are suppressed
    /// so consumers cut over to it (see `ShadowStore`).
    ExternalOnly,
}

impl WriteMode {
    /// Parse a `kafka.shadow_config.write_mode` value (case-insensitive;
    /// anything unknown is `DualWrite`).
    pub fn parse(s: &str) -> Self {
        if s.eq_ignore_ascii_case("external_only") {
            WriteMode::ExternalOnly
        } else {
            WriteMode::DualWrite
        }
    }
}

/// One row of `kafka.shadow_config`.
#[derive(Debug, Clone)]
pub struct TopicShadowConfig {
    /// Topic ID in kafka.topics.
    pub topic_id: i32,
    /// Local topic name.
    pub topic_name: String,
    /// Shadow mode (local_only or shadow).
    pub mode: ShadowMode,
    /// Percentage of messages to forward (0-100).
    pub forward_percentage: u8,
    /// External topic name (None = same as local).
    pub external_topic_name: Option<String>,
    /// When the produce response goes out (see [`SyncMode`]).
    pub sync_mode: SyncMode,
    /// Which copy consumers read (see `WriteMode`).
    pub write_mode: WriteMode,
}

impl TopicShadowConfig {
    /// Whether records of this topic are forwarded at all.
    pub fn should_forward(&self) -> bool {
        self.mode == ShadowMode::Shadow && self.forward_percentage > 0
    }
}

/// Valid security protocols for Kafka connections.
pub const VALID_SECURITY_PROTOCOLS: &[&str] = &["PLAINTEXT", "SASL_PLAINTEXT", "SSL", "SASL_SSL"];

/// Valid SASL mechanisms.
pub const VALID_SASL_MECHANISMS: &[&str] = &["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"];

/// `validate` clamps `batch_size` into this range (the GUC's).
const BATCH_SIZE_RANGE: (i32, i32) = (
    crate::kafka::constants::MIN_SHADOW_BATCH_SIZE,
    crate::kafka::constants::MAX_SHADOW_BATCH_SIZE,
);
/// `validate` clamps `linger_ms` into this range (the GUC's, so a value the GUC
/// accepts is applied as set).
const LINGER_MS_RANGE: (i32, i32) = (
    crate::kafka::constants::MIN_SHADOW_LINGER_MS,
    crate::kafka::constants::MAX_SHADOW_LINGER_MS,
);

/// The settings the external producer is built from (the shadow GUCs).
#[derive(Clone)]
pub struct ShadowConfig {
    /// Whether shadow mode is enabled globally.
    pub enabled: bool,
    /// External Kafka bootstrap servers.
    pub bootstrap_servers: String,
    /// Security protocol (SASL_SSL, SASL_PLAINTEXT, SSL, PLAINTEXT).
    pub security_protocol: String,
    /// SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512).
    pub sasl_mechanism: String,
    /// SASL username.
    pub sasl_username: String,
    /// SASL password.
    pub sasl_password: String,
    /// SSL CA certificate location.
    pub ssl_ca_location: String,
    /// Messages per producer batch (`batch.num.messages`).
    pub batch_size: i32,
    /// Producer linger time in milliseconds.
    pub linger_ms: i32,
    /// librdkafka's retry backoff in milliseconds.
    pub retry_backoff_ms: i32,
}

/// Redacts the SASL credentials, so a `{:?}` of a `ShadowConfig` never leaks
/// the external-broker username or password into logs.
impl std::fmt::Debug for ShadowConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShadowConfig")
            .field("enabled", &self.enabled)
            .field("bootstrap_servers", &self.bootstrap_servers)
            .field("security_protocol", &self.security_protocol)
            .field("sasl_mechanism", &self.sasl_mechanism)
            .field("sasl_username", &"[REDACTED]")
            .field("sasl_password", &"[REDACTED]")
            .field("ssl_ca_location", &self.ssl_ca_location)
            .field("batch_size", &self.batch_size)
            .field("linger_ms", &self.linger_ms)
            .field("retry_backoff_ms", &self.retry_backoff_ms)
            .finish()
    }
}

impl ShadowConfig {
    /// The shadow part of the global configuration.
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
        }
    }

    /// Validate and normalize the configuration.
    ///
    /// - `security_protocol` must be one of `VALID_SECURITY_PROTOCOLS` and
    ///   `sasl_mechanism` (for a SASL protocol) one of `VALID_SASL_MECHANISMS`,
    ///   case-insensitively; both are normalized to upper case;
    /// - every `bootstrap_servers` entry must be `host:port` (when enabled);
    /// - `batch_size` and `linger_ms` are clamped into their ranges.
    ///
    /// SASL_PLAINTEXT (credentials in the clear) and PLAINTEXT (no security at
    /// all) are accepted with a warning.
    pub fn validate(&mut self) -> super::error::ShadowResult<()> {
        use super::error::ShadowError;

        let protocol = self.security_protocol.to_uppercase();
        if !VALID_SECURITY_PROTOCOLS.contains(&protocol.as_str()) {
            return Err(ShadowError::ConfigError(format!(
                "Invalid security_protocol '{}'. Must be one of: {}",
                self.security_protocol,
                VALID_SECURITY_PROTOCOLS.join(", ")
            )));
        }
        match protocol.as_str() {
            "SASL_PLAINTEXT" => tracing::warn!(
                "Shadow mode using SASL_PLAINTEXT: credentials will be sent over unencrypted connection. \
                 Consider using SASL_SSL for production deployments."
            ),
            "PLAINTEXT" => tracing::warn!(
                "Shadow mode using PLAINTEXT: no authentication or encryption. \
                 Only use for local development or trusted networks."
            ),
            _ => {}
        }

        if protocol.starts_with("SASL") {
            let mechanism = self.sasl_mechanism.to_uppercase();
            if !VALID_SASL_MECHANISMS.contains(&mechanism.as_str()) {
                return Err(ShadowError::ConfigError(format!(
                    "Invalid sasl_mechanism '{}'. Must be one of: {}",
                    self.sasl_mechanism,
                    VALID_SASL_MECHANISMS.join(", ")
                )));
            }
            self.sasl_mechanism = mechanism;
        }
        self.security_protocol = protocol;

        if self.enabled {
            let invalid = self
                .bootstrap_servers
                .split(',')
                .map(str::trim)
                .find(|server| !server.is_empty() && !is_valid_bootstrap_server(server));
            if let Some(server) = invalid {
                return Err(ShadowError::ConfigError(format!(
                    "Invalid bootstrap_servers format '{server}'. Expected host:port"
                )));
            }
        }

        self.batch_size = clamp_logged("batch_size", self.batch_size, BATCH_SIZE_RANGE, true);
        self.linger_ms = clamp_logged("linger_ms", self.linger_ms, LINGER_MS_RANGE, false);
        Ok(())
    }
}

/// Clamp `value` into `(min, max)`, warning when it exceeds the maximum (and,
/// with `warn_below`, when it is below the minimum).
fn clamp_logged(name: &str, value: i32, (min, max): (i32, i32), warn_below: bool) -> i32 {
    if value < min {
        if warn_below {
            tracing::warn!("Shadow mode {name} {value} is below minimum, clamping to {min}");
        }
        min
    } else if value > max {
        tracing::warn!("Shadow mode {name} {value} exceeds maximum, clamping to {max}");
        max
    } else {
        value
    }
}

/// Whether `server` is `host:port`: a non-zero port, a non-empty host without
/// spaces, or a bracketed IPv6 address (split at the last colon).
fn is_valid_bootstrap_server(server: &str) -> bool {
    let Some((host, port)) = server.rsplit_once(':') else {
        return false;
    };
    if !matches!(port.parse::<u16>(), Ok(p) if p != 0) || host.is_empty() {
        return false;
    }
    if host.starts_with('[') && host.ends_with(']') {
        return host.len() > 2;
    }
    !host.contains(' ')
}

impl Default for ShadowConfig {
    fn default() -> Self {
        use crate::kafka::constants::{
            DEFAULT_SHADOW_BATCH_SIZE, DEFAULT_SHADOW_BOOTSTRAP_SERVERS, DEFAULT_SHADOW_LINGER_MS,
            DEFAULT_SHADOW_MODE_ENABLED, DEFAULT_SHADOW_RETRY_BACKOFF_MS,
            DEFAULT_SHADOW_SASL_MECHANISM, DEFAULT_SHADOW_SECURITY_PROTOCOL,
        };
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
        }
    }
}

/// The per-topic configurations, by topic id. Written by the config reload,
/// read on every produce and fetch of the DB thread.
#[derive(Debug, Default)]
pub struct TopicConfigCache {
    configs: RwLock<HashMap<i32, TopicShadowConfig>>,
}

impl TopicConfigCache {
    /// An empty cache.
    pub fn new() -> Self {
        Self::default()
    }

    /// The configuration of a topic, if it has one.
    pub fn get(&self, topic_id: i32) -> Option<TopicShadowConfig> {
        super::read_lock(&self.configs).get(&topic_id).cloned()
    }

    /// Insert or replace one topic's configuration.
    #[cfg(test)]
    pub fn update(&self, config: TopicShadowConfig) {
        super::write_lock(&self.configs).insert(config.topic_id, config);
    }

    /// Replace the whole cache: a topic missing from `configs` stops forwarding.
    pub fn replace(&self, configs: Vec<TopicShadowConfig>) {
        let configs = configs.into_iter().map(|c| (c.topic_id, c)).collect();
        *super::write_lock(&self.configs) = configs;
    }

    /// Number of cached configurations.
    #[cfg(test)]
    pub fn len(&self) -> usize {
        super::read_lock(&self.configs).len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topic(topic_id: i32, mode: ShadowMode, forward_percentage: u8) -> TopicShadowConfig {
        TopicShadowConfig {
            topic_id,
            topic_name: format!("topic-{topic_id}"),
            mode,
            forward_percentage,
            external_topic_name: None,
            sync_mode: SyncMode::Async,
            write_mode: WriteMode::DualWrite,
        }
    }

    #[test]
    fn test_mode_parsing_is_case_insensitive_with_a_default() {
        for (s, mode) in [
            ("shadow", ShadowMode::Shadow),
            ("SHADOW", ShadowMode::Shadow),
            ("Shadow", ShadowMode::Shadow),
            ("local_only", ShadowMode::LocalOnly),
            ("LOCAL_ONLY", ShadowMode::LocalOnly),
            ("invalid", ShadowMode::LocalOnly),
        ] {
            assert_eq!(ShadowMode::parse(s), mode, "{s}");
        }
        for (s, mode) in [
            ("sync", SyncMode::Sync),
            ("SYNC", SyncMode::Sync),
            ("Sync", SyncMode::Sync),
            ("async", SyncMode::Async),
            ("ASYNC", SyncMode::Async),
            ("invalid", SyncMode::Async),
        ] {
            assert_eq!(SyncMode::parse(s), mode, "{s}");
        }
        for (s, mode) in [
            ("dual_write", WriteMode::DualWrite),
            ("DUAL_WRITE", WriteMode::DualWrite),
            ("external_only", WriteMode::ExternalOnly),
            ("EXTERNAL_ONLY", WriteMode::ExternalOnly),
            ("invalid", WriteMode::DualWrite),
        ] {
            assert_eq!(WriteMode::parse(s), mode, "{s}");
        }
    }

    #[test]
    fn test_topic_config_should_forward() {
        for (mode, percentage, forwards) in [
            (ShadowMode::Shadow, 100, true),
            (ShadowMode::Shadow, 50, true),
            (ShadowMode::Shadow, 0, false),
            (ShadowMode::LocalOnly, 100, false),
        ] {
            assert_eq!(
                topic(1, mode, percentage).should_forward(),
                forwards,
                "{mode:?} at {percentage}%"
            );
        }
    }

    #[test]
    fn test_topic_config_cache_get_update_replace() {
        let cache = TopicConfigCache::new();
        assert!(cache.get(1).is_none());

        cache.update(topic(1, ShadowMode::Shadow, 50));
        let got = cache.get(1).expect("cached");
        assert_eq!(got.topic_id, 1);
        assert_eq!(got.topic_name, "topic-1");
        assert_eq!(got.forward_percentage, 50);
        assert!(got.should_forward());

        // An update replaces the topic's entry.
        let mut changed = topic(1, ShadowMode::Shadow, 100);
        changed.external_topic_name = Some("prod-events".to_string());
        changed.sync_mode = SyncMode::Sync;
        changed.write_mode = WriteMode::ExternalOnly;
        cache.update(changed);
        let got = cache.get(1).expect("cached");
        assert_eq!(got.forward_percentage, 100);
        assert_eq!(got.external_topic_name.as_deref(), Some("prod-events"));
        assert_eq!(got.sync_mode, SyncMode::Sync);
        assert_eq!(got.write_mode, WriteMode::ExternalOnly);
        assert!(got.should_forward());
        assert_eq!(cache.len(), 1);

        // Every topic updated is held at once.
        for id in 2..=5 {
            cache.update(topic(id, ShadowMode::Shadow, 100));
        }
        assert_eq!(cache.len(), 5);
        for id in 1..=5 {
            assert_eq!(cache.get(id).expect("cached").topic_id, id);
        }

        // A replace drops every topic it does not list.
        cache.update(topic(2, ShadowMode::Shadow, 100));
        cache.replace(vec![topic(3, ShadowMode::LocalOnly, 0)]);
        assert!(cache.get(1).is_none());
        assert!(cache.get(2).is_none());
        assert_eq!(cache.get(3).expect("cached").mode, ShadowMode::LocalOnly);
        cache.replace(Vec::new());
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_valid_security_protocols() {
        let mut config = ShadowConfig {
            enabled: true,
            bootstrap_servers: "localhost:9092".to_string(),
            security_protocol: "PLAINTEXT".to_string(),
            ..Default::default()
        };
        assert!(config.validate().is_ok());
        assert_eq!(config.security_protocol, "PLAINTEXT");

        config.security_protocol = "sasl_ssl".to_string(); // lowercase
        assert!(config.validate().is_ok());
        assert_eq!(config.security_protocol, "SASL_SSL"); // normalized

        config.security_protocol = "ssl".to_string();
        assert!(config.validate().is_ok());
        assert_eq!(config.security_protocol, "SSL");

        config.security_protocol = "Sasl_Plaintext".to_string();
        assert!(config.validate().is_ok());
        assert_eq!(config.security_protocol, "SASL_PLAINTEXT");
    }

    #[test]
    fn test_invalid_security_protocol_rejected() {
        let mut config = ShadowConfig {
            enabled: true,
            bootstrap_servers: "localhost:9092".to_string(),
            security_protocol: "INVALID".to_string(),
            ..Default::default()
        };
        let result = config.validate();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Invalid security_protocol"));
        assert!(err.to_string().contains("INVALID"));
    }

    #[test]
    fn test_valid_sasl_mechanisms() {
        let mut config = ShadowConfig {
            enabled: true,
            bootstrap_servers: "localhost:9092".to_string(),
            security_protocol: "SASL_SSL".to_string(),
            sasl_mechanism: "PLAIN".to_string(),
            ..Default::default()
        };
        assert!(config.validate().is_ok());

        config.sasl_mechanism = "scram-sha-256".to_string(); // lowercase
        assert!(config.validate().is_ok());
        assert_eq!(config.sasl_mechanism, "SCRAM-SHA-256"); // normalized

        config.sasl_mechanism = "SCRAM-SHA-512".to_string();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_invalid_sasl_mechanism_rejected() {
        let mut config = ShadowConfig {
            enabled: true,
            bootstrap_servers: "localhost:9092".to_string(),
            security_protocol: "SASL_SSL".to_string(),
            sasl_mechanism: "INVALID".to_string(),
            ..Default::default()
        };
        let result = config.validate();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Invalid sasl_mechanism"));
    }

    #[test]
    fn test_sasl_mechanism_not_validated_for_non_sasl_protocol() {
        // When not using SASL, the sasl_mechanism doesn't matter
        let mut config = ShadowConfig {
            enabled: true,
            bootstrap_servers: "localhost:9092".to_string(),
            security_protocol: "PLAINTEXT".to_string(),
            sasl_mechanism: "INVALID".to_string(), // Should be ignored
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_bootstrap_servers_validation() {
        let mut config = ShadowConfig {
            enabled: true,
            bootstrap_servers: "localhost:9092".to_string(),
            security_protocol: "PLAINTEXT".to_string(),
            ..Default::default()
        };
        assert!(config.validate().is_ok());

        // Multiple servers
        config.bootstrap_servers = "kafka1:9092, kafka2:9092, kafka3:9092".to_string();
        assert!(config.validate().is_ok());

        // IPv4 address
        config.bootstrap_servers = "192.168.1.100:9092".to_string();
        assert!(config.validate().is_ok());

        // IPv6 address in brackets
        config.bootstrap_servers = "[::1]:9092".to_string();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_invalid_bootstrap_servers_rejected() {
        let mut config = ShadowConfig {
            enabled: true,
            bootstrap_servers: "localhost".to_string(), // Missing port
            security_protocol: "PLAINTEXT".to_string(),
            ..Default::default()
        };
        let result = config.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid bootstrap_servers"));

        // Invalid port
        config.bootstrap_servers = "localhost:abc".to_string();
        assert!(config.validate().is_err());

        // Port 0 is invalid
        config.bootstrap_servers = "localhost:0".to_string();
        assert!(config.validate().is_err());

        // Empty host
        config.bootstrap_servers = ":9092".to_string();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_batch_size_clamping() {
        let mut config = ShadowConfig {
            enabled: true,
            bootstrap_servers: "localhost:9092".to_string(),
            security_protocol: "PLAINTEXT".to_string(),
            batch_size: 0,
            ..Default::default()
        };
        assert!(config.validate().is_ok());
        assert_eq!(config.batch_size, 1); // Clamped to minimum

        config.batch_size = 200_000;
        assert!(config.validate().is_ok());
        assert_eq!(config.batch_size, 100_000); // Clamped to maximum

        config.batch_size = 500;
        assert!(config.validate().is_ok());
        assert_eq!(config.batch_size, 500); // Unchanged
    }

    #[test]
    fn test_linger_ms_clamping() {
        let mut config = ShadowConfig {
            enabled: true,
            bootstrap_servers: "localhost:9092".to_string(),
            security_protocol: "PLAINTEXT".to_string(),
            linger_ms: -10,
            ..Default::default()
        };
        assert!(config.validate().is_ok());
        assert_eq!(config.linger_ms, 0); // Clamped to minimum

        config.linger_ms = 10000;
        assert!(config.validate().is_ok());
        assert_eq!(config.linger_ms, 5000); // Clamped to maximum
    }

    #[test]
    fn test_disabled_config_skips_bootstrap_validation() {
        // When shadow mode is disabled, bootstrap_servers format is not validated
        let mut config = ShadowConfig {
            enabled: false,
            bootstrap_servers: "invalid format".to_string(),
            security_protocol: "PLAINTEXT".to_string(),
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    /// A `{:?}` of a ShadowConfig never exposes the SASL credentials.
    #[test]
    fn test_debug_redacts_sasl_credentials() {
        let config = ShadowConfig {
            enabled: true,
            bootstrap_servers: "broker:9092".to_string(),
            security_protocol: "SASL_SSL".to_string(),
            sasl_mechanism: "PLAIN".to_string(),
            sasl_username: "super-secret-user".to_string(),
            sasl_password: "super-secret-password".to_string(),
            ..Default::default()
        };

        let rendered = format!("{config:?}");

        // The credentials must not appear in any form.
        assert!(
            !rendered.contains("super-secret-password"),
            "sasl_password leaked into Debug output: {rendered}"
        );
        assert!(
            !rendered.contains("super-secret-user"),
            "sasl_username leaked into Debug output: {rendered}"
        );
        // They must be shown as redacted, and non-sensitive fields still render.
        assert!(rendered.contains("[REDACTED]"));
        assert!(rendered.contains("broker:9092"));
    }
}
