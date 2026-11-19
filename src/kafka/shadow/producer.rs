// Copyright (c) 2026 Robert Trent Jones.
// This file is part of the "Shadow Mode" feature of pg_kafka.
//
// Use of this source code for production purposes is governed by the
// Commercial License found in the LICENSE file in this directory.
// Development and evaluation use is permitted.
//
// GitHub Sponsors: https://github.com/sponsors/RTrentJones

//! Shadow mode Kafka producer
//!
//! This module provides a wrapper around rdkafka's FutureProducer configured
//! with SASL/SSL authentication for forwarding messages to external Kafka.
//!
//! ## Security
//!
//! SASL/SSL is required by default. Supported mechanisms:
//! - PLAIN (username/password over SSL)
//! - SCRAM-SHA-256
//! - SCRAM-SHA-512
//!
//! ## OpenSSL Warning
//!
//! This uses system OpenSSL via librdkafka. NEVER enable the `ssl-vendored`
//! feature as it will conflict with PostgreSQL's OpenSSL symbols and crash.

use super::config::ShadowConfig;
use super::error::{ShadowError, ShadowResult};
use super::ForwardRecord;
use rdkafka::config::ClientConfig;
use rdkafka::error::RDKafkaErrorCode;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{DeliveryFuture, FutureProducer, FutureRecord, Producer, PurgeConfig};
use rdkafka::util::Timeout;
use std::sync::Arc;
use std::time::Duration;

/// librdkafka tuning for the shadow forwarding producer. Named here rather than
/// inlined as string literals at the `client_config.set` call sites so the intent
/// (and the idempotence invariants) are explicit.
///
/// Max in-flight requests per connection. librdkafka requires this to be ≤ 5 for
/// `enable.idempotence=true` (ordering guarantee), so 5 is the idempotent maximum.
const PRODUCER_MAX_IN_FLIGHT: &str = "5";
/// Per-request timeout (30 s) and total message delivery timeout (120 s). One
/// outbox attempt therefore resolves (delivered or failed) within 120 s, which the
/// DB thread's in-flight eviction (`INFLIGHT_EVICT_AFTER`, 150 s) relies on.
const PRODUCER_REQUEST_TIMEOUT_MS: &str = "30000";
const PRODUCER_MESSAGE_TIMEOUT_MS: &str = "120000";

/// Assemble the ordered librdkafka settings for the shadow forwarding producer.
///
/// Pure — no client creation, no IO — so the security/idempotence invariants can
/// be unit-tested without a live broker (mirrors the `validate_sequence`
/// extraction in `storage/mod.rs`). [`ShadowProducer::create_producer`] applies
/// these to a `ClientConfig` and calls `.create()`.
///
/// What each shadow GUC maps to (wave 3: every GUC does what its doc says):
/// - `shadow_batch_size` (messages) → `batch.num.messages` (librdkafka's
///   per-batch message cap; it used to be passed to `batch.size`, which is bytes);
/// - `shadow_linger_ms` → `linger.ms`;
/// - `shadow_retry_backoff_ms` → `retry.backoff.ms` (librdkafka's own retries
///   inside one delivery attempt; the outbox uses the same base for its re-attempt
///   backoff, see `ShadowStore`);
/// - `shadow_max_retries` is NOT a librdkafka setting any more: it is the outbox's
///   dead-letter cap (failed delivery attempts per record). Inside one attempt
///   librdkafka retries as often as `message.timeout.ms` allows (its default
///   `retries`), which is what idempotence wants.
pub(crate) fn build_producer_settings(config: &ShadowConfig) -> Vec<(&'static str, String)> {
    // Basic configuration + security protocol are always set.
    // (SASL_SSL, SASL_PLAINTEXT, SSL, or PLAINTEXT for security.protocol.)
    let mut settings: Vec<(&'static str, String)> = vec![
        ("bootstrap.servers", config.bootstrap_servers.clone()),
        ("security.protocol", config.security_protocol.clone()),
    ];

    // SASL configuration (if using a SASL_* protocol)
    if config.security_protocol.starts_with("SASL") {
        settings.push(("sasl.mechanism", config.sasl_mechanism.clone()));

        // Empty credentials are omitted — librdkafka rejects empty SASL fields.
        if !config.sasl_username.is_empty() {
            settings.push(("sasl.username", config.sasl_username.clone()));
        }
        if !config.sasl_password.is_empty() {
            settings.push(("sasl.password", config.sasl_password.clone()));
        }
    }

    // SSL configuration (if using an *_SSL protocol)
    if config.security_protocol.ends_with("SSL") {
        if !config.ssl_ca_location.is_empty() {
            settings.push(("ssl.ca.location", config.ssl_ca_location.clone()));
        }
        // Enable hostname verification for security.
        let https = "https".to_string();
        settings.push(("ssl.endpoint.identification.algorithm", https));
    }

    // Producer performance settings (see the GUC mapping above).
    settings.push(("batch.num.messages", config.batch_size.to_string()));
    settings.push(("linger.ms", config.linger_ms.to_string()));
    settings.push(("retry.backoff.ms", config.retry_backoff_ms.to_string()));

    // Compression (use none for maximum compatibility)
    settings.push(("compression.type", "none".to_string()));

    // The partitioner of the Java client (murmur2 over the key, random for a
    // null key), so a keyed record forwarded by pg_kafka lands in the same
    // external partition as the same record produced by a Java client: a
    // migration that cuts producers over keeps per-key placement. librdkafka's
    // default (CRC32) would scatter keys differently.
    settings.push(("partitioner", "murmur2_random".to_string()));

    // SH-12 + wave 3: idempotent, order-preserving producer. Many deliveries are
    // in flight at once (the forwarder pipelines), so ordering rests on the
    // idempotent producer: per-partition sequence numbers, acks=all, at most 5
    // requests in flight, librdkafka's own retries. `enable.gapless.guarantee`
    // turns any error that could leave a gap (a later record delivered while an
    // earlier one failed) into a fatal producer error that fails every
    // outstanding record instead; the forwarder then rebuilds the producer and
    // the outbox re-sends from the first undelivered offset of each partition,
    // so a record is never delivered after a later record of its local
    // partition. Duplicates are suppressed only within one producer session
    // (stable PID): a re-send that crosses a producer rebuild is at-least-once.
    let max_in_flight = PRODUCER_MAX_IN_FLIGHT.to_string();
    settings.push(("enable.idempotence", "true".to_string()));
    settings.push(("enable.gapless.guarantee", "true".to_string()));
    settings.push(("max.in.flight.requests.per.connection", max_in_flight));
    settings.push(("acks", "all".to_string()));

    // NOTE: deprecated api.version.* settings are intentionally NOT set — they
    // caused "Required feature not supported by broker" errors with modern Kafka,
    // which negotiates the API version automatically.
    let request_timeout = PRODUCER_REQUEST_TIMEOUT_MS.to_string();
    let message_timeout = PRODUCER_MESSAGE_TIMEOUT_MS.to_string();
    settings.push(("request.timeout.ms", request_timeout));
    settings.push(("message.timeout.ms", message_timeout));

    settings
}

/// Why [`ShadowProducer::enqueue`] did not accept a record.
#[derive(Debug)]
pub enum EnqueueError {
    /// librdkafka's local queue is full: back off and retry the same record.
    QueueFull,
    /// The record was refused (too large, unknown topic, fatal producer...).
    Refused(String),
}

/// Shadow producer for forwarding messages to external Kafka
pub struct ShadowProducer {
    /// The underlying rdkafka producer
    producer: FutureProducer,
    /// Configuration used to create this producer
    config: Arc<ShadowConfig>,
}

impl ShadowProducer {
    /// Create a new shadow producer from configuration
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Shadow mode is not enabled
    /// - Bootstrap servers are not configured
    /// - Producer creation fails (connection, auth, etc.)
    pub fn new(config: Arc<ShadowConfig>) -> ShadowResult<Self> {
        if !config.enabled {
            return Err(ShadowError::NotEnabled);
        }

        if config.bootstrap_servers.is_empty() {
            return Err(ShadowError::NotConfigured(
                "bootstrap.servers is empty".to_string(),
            ));
        }

        let producer = Self::create_producer(&config)?;

        Ok(Self { producer, config })
    }

    /// Create the rdkafka producer with SASL/SSL configuration.
    ///
    /// The librdkafka key/value assembly lives in the pure
    /// [`build_producer_settings`] free function so the security and idempotence
    /// invariants can be unit-tested without a live broker (mirrors the
    /// `validate_sequence` extraction pattern). This shell only applies the
    /// settings and calls `.create()`, which connects and is therefore E2E-only.
    fn create_producer(config: &ShadowConfig) -> ShadowResult<FutureProducer> {
        let mut client_config = ClientConfig::new();
        for (key, value) in build_producer_settings(config) {
            client_config.set(key, value);
        }
        client_config
            .create()
            .map_err(|e| ShadowError::ProducerError(format!("Failed to create producer: {}", e)))
    }

    /// Hand one outbox record to librdkafka without waiting for its delivery
    /// (the forwarder keeps many deliveries in flight). The record keeps its key,
    /// value, headers and timestamp; `partition` is left to the partitioner (the
    /// external cluster re-partitions by key, SH-15).
    pub fn enqueue(&self, record: &ForwardRecord) -> Result<DeliveryFuture, EnqueueError> {
        let mut future_record: FutureRecord<'_, [u8], [u8]> = FutureRecord::to(&record.topic_name);
        if let Some(k) = record.key.as_deref() {
            future_record = future_record.key(k);
        }
        if let Some(v) = record.value.as_deref() {
            future_record = future_record.payload(v);
        }
        if let Some(ts) = record.timestamp_ms {
            future_record = future_record.timestamp(ts);
        }
        if !record.headers.is_empty() {
            let mut headers = OwnedHeaders::new_with_capacity(record.headers.len());
            for (key, value) in &record.headers {
                headers = headers.insert(Header {
                    key,
                    value: Some(value.as_slice()),
                });
            }
            future_record = future_record.headers(headers);
        }
        self.producer.send_result(future_record).map_err(|(e, _)| {
            if e.rdkafka_error_code() == Some(RDKafkaErrorCode::QueueFull) {
                EnqueueError::QueueFull
            } else {
                EnqueueError::Refused(e.to_string())
            }
        })
    }

    /// The producer's fatal error, if it has one (it then fails every record and
    /// must be rebuilt; see `enable.gapless.guarantee`).
    pub fn fatal_error(&self) -> Option<String> {
        self.producer
            .client()
            .fatal_error()
            .map(|(code, msg)| format!("{:?}: {}", code, msg))
    }

    /// Fail every queued and in-flight record now (their delivery futures
    /// resolve with a purge error). Used when the producer is replaced.
    pub fn purge_outstanding(&self) {
        self.producer
            .purge(PurgeConfig::default().queue().inflight());
    }

    /// Serve librdkafka's delivery callbacks for a moment (the FutureProducer's
    /// own polling thread does this too; used while waiting on a full queue).
    pub fn poll(&self, timeout: Duration) {
        self.producer.poll(Timeout::After(timeout));
    }

    /// Flush pending messages
    ///
    /// Waits for all pending messages to be delivered or timeout.
    pub fn flush(&self, timeout: Duration) -> ShadowResult<()> {
        self.producer
            .flush(Timeout::After(timeout))
            .map_err(|e| ShadowError::ProducerError(format!("Flush failed: {}", e)))
    }

    /// Get the configuration used by this producer
    pub fn config(&self) -> &ShadowConfig {
        &self.config
    }
}

impl std::fmt::Debug for ShadowProducer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShadowProducer")
            .field("bootstrap_servers", &self.config.bootstrap_servers)
            .field("security_protocol", &self.config.security_protocol)
            .finish()
    }
}

/// Builder for creating ShadowProducer with custom settings
pub struct ShadowProducerBuilder {
    config: ShadowConfig,
}

impl ShadowProducerBuilder {
    /// Create a new builder with the given bootstrap servers
    pub fn new(bootstrap_servers: &str) -> Self {
        Self {
            config: ShadowConfig {
                enabled: true,
                bootstrap_servers: bootstrap_servers.to_string(),
                ..Default::default()
            },
        }
    }

    /// Set the security protocol
    pub fn security_protocol(mut self, protocol: &str) -> Self {
        self.config.security_protocol = protocol.to_string();
        self
    }

    /// Set SASL mechanism
    pub fn sasl_mechanism(mut self, mechanism: &str) -> Self {
        self.config.sasl_mechanism = mechanism.to_string();
        self
    }

    /// Set SASL credentials
    pub fn sasl_credentials(mut self, username: &str, password: &str) -> Self {
        self.config.sasl_username = username.to_string();
        self.config.sasl_password = password.to_string();
        self
    }

    /// Set SSL CA certificate location
    pub fn ssl_ca_location(mut self, path: &str) -> Self {
        self.config.ssl_ca_location = path.to_string();
        self
    }

    /// Set batch size
    pub fn batch_size(mut self, size: i32) -> Self {
        self.config.batch_size = size;
        self
    }

    /// Set linger time in milliseconds
    pub fn linger_ms(mut self, ms: i32) -> Self {
        self.config.linger_ms = ms;
        self
    }

    /// Build the ShadowProducer
    pub fn build(self) -> ShadowResult<ShadowProducer> {
        ShadowProducer::new(Arc::new(self.config))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shadow_producer_not_enabled() {
        let config = Arc::new(ShadowConfig {
            enabled: false,
            bootstrap_servers: "localhost:9092".to_string(),
            ..Default::default()
        });

        let result = ShadowProducer::new(config);
        assert!(matches!(result, Err(ShadowError::NotEnabled)));
    }

    #[test]
    fn test_shadow_producer_no_bootstrap_servers() {
        let config = Arc::new(ShadowConfig {
            enabled: true,
            bootstrap_servers: String::new(),
            ..Default::default()
        });

        let result = ShadowProducer::new(config);
        assert!(matches!(result, Err(ShadowError::NotConfigured(_))));
    }

    #[test]
    fn test_shadow_producer_builder() {
        let builder = ShadowProducerBuilder::new("localhost:9092")
            .security_protocol("SASL_SSL")
            .sasl_mechanism("PLAIN")
            .sasl_credentials("user", "pass")
            .batch_size(2000)
            .linger_ms(20);

        assert_eq!(builder.config.bootstrap_servers, "localhost:9092");
        assert_eq!(builder.config.security_protocol, "SASL_SSL");
        assert_eq!(builder.config.sasl_mechanism, "PLAIN");
        assert_eq!(builder.config.sasl_username, "user");
        assert_eq!(builder.config.sasl_password, "pass");
        assert_eq!(builder.config.batch_size, 2000);
        assert_eq!(builder.config.linger_ms, 20);
    }

    #[test]
    fn test_shadow_config_default() {
        let config = ShadowConfig::default();
        assert!(!config.enabled);
        assert!(config.bootstrap_servers.is_empty());
        assert_eq!(config.security_protocol, "SASL_SSL");
        assert_eq!(config.sasl_mechanism, "PLAIN");
    }

    #[test]
    fn test_shadow_producer_builder_ssl_ca_location() {
        let builder = ShadowProducerBuilder::new("kafka:9092")
            .security_protocol("SSL")
            .ssl_ca_location("/path/to/ca-cert.pem");

        assert_eq!(builder.config.ssl_ca_location, "/path/to/ca-cert.pem");
        assert_eq!(builder.config.security_protocol, "SSL");
    }

    #[test]
    fn test_shadow_producer_builder_all_sasl_mechanisms() {
        // Test PLAIN
        let builder_plain = ShadowProducerBuilder::new("kafka:9092")
            .security_protocol("SASL_SSL")
            .sasl_mechanism("PLAIN");
        assert_eq!(builder_plain.config.sasl_mechanism, "PLAIN");

        // Test SCRAM-SHA-256
        let builder_scram256 = ShadowProducerBuilder::new("kafka:9092")
            .security_protocol("SASL_SSL")
            .sasl_mechanism("SCRAM-SHA-256");
        assert_eq!(builder_scram256.config.sasl_mechanism, "SCRAM-SHA-256");

        // Test SCRAM-SHA-512
        let builder_scram512 = ShadowProducerBuilder::new("kafka:9092")
            .security_protocol("SASL_SSL")
            .sasl_mechanism("SCRAM-SHA-512");
        assert_eq!(builder_scram512.config.sasl_mechanism, "SCRAM-SHA-512");
    }

    #[test]
    fn test_shadow_producer_builder_all_security_protocols() {
        let protocols = vec!["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"];

        for protocol in protocols {
            let builder = ShadowProducerBuilder::new("kafka:9092").security_protocol(protocol);
            assert_eq!(builder.config.security_protocol, protocol);
        }
    }

    #[test]
    fn test_shadow_producer_builder_chaining() {
        // Test that all builder methods return Self for chaining
        let builder = ShadowProducerBuilder::new("kafka1:9092,kafka2:9092")
            .security_protocol("SASL_SSL")
            .sasl_mechanism("SCRAM-SHA-256")
            .sasl_credentials("admin", "secret123")
            .ssl_ca_location("/etc/ssl/ca.pem")
            .batch_size(5000)
            .linger_ms(50);

        assert_eq!(builder.config.bootstrap_servers, "kafka1:9092,kafka2:9092");
        assert_eq!(builder.config.security_protocol, "SASL_SSL");
        assert_eq!(builder.config.sasl_mechanism, "SCRAM-SHA-256");
        assert_eq!(builder.config.sasl_username, "admin");
        assert_eq!(builder.config.sasl_password, "secret123");
        assert_eq!(builder.config.ssl_ca_location, "/etc/ssl/ca.pem");
        assert_eq!(builder.config.batch_size, 5000);
        assert_eq!(builder.config.linger_ms, 50);
    }

    #[test]
    fn test_shadow_producer_builder_defaults() {
        let builder = ShadowProducerBuilder::new("kafka:9092");

        // Check that defaults are reasonable
        assert!(builder.config.enabled); // Builder enables by default
        assert_eq!(builder.config.security_protocol, "SASL_SSL"); // From Default
        assert_eq!(builder.config.sasl_mechanism, "PLAIN"); // From Default
    }

    #[test]
    fn test_shadow_producer_builder_empty_credentials() {
        let builder = ShadowProducerBuilder::new("kafka:9092").sasl_credentials("", "");

        assert_eq!(builder.config.sasl_username, "");
        assert_eq!(builder.config.sasl_password, "");
    }

    #[test]
    fn test_shadow_producer_builder_multiple_bootstrap_servers() {
        let builder = ShadowProducerBuilder::new("kafka1:9092,kafka2:9092,kafka3:9092");
        assert_eq!(
            builder.config.bootstrap_servers,
            "kafka1:9092,kafka2:9092,kafka3:9092"
        );
    }

    #[test]
    fn test_shadow_producer_builder_batch_size_range() {
        // Test minimum batch size
        let builder_min = ShadowProducerBuilder::new("kafka:9092").batch_size(1);
        assert_eq!(builder_min.config.batch_size, 1);

        // Test large batch size
        let builder_large = ShadowProducerBuilder::new("kafka:9092").batch_size(100_000);
        assert_eq!(builder_large.config.batch_size, 100_000);
    }

    #[test]
    fn test_shadow_producer_builder_linger_range() {
        // Test zero linger (no batching delay)
        let builder_zero = ShadowProducerBuilder::new("kafka:9092").linger_ms(0);
        assert_eq!(builder_zero.config.linger_ms, 0);

        // Test max linger
        let builder_max = ShadowProducerBuilder::new("kafka:9092").linger_ms(5000);
        assert_eq!(builder_max.config.linger_ms, 5000);
    }

    #[test]
    fn test_shadow_config_is_configured() {
        // Not configured: disabled
        let config1 = ShadowConfig {
            enabled: false,
            bootstrap_servers: "kafka:9092".to_string(),
            ..Default::default()
        };
        assert!(!config1.is_configured());

        // Not configured: empty bootstrap servers
        let config2 = ShadowConfig {
            enabled: true,
            bootstrap_servers: String::new(),
            ..Default::default()
        };
        assert!(!config2.is_configured());

        // Configured: enabled with bootstrap servers
        let config3 = ShadowConfig {
            enabled: true,
            bootstrap_servers: "kafka:9092".to_string(),
            ..Default::default()
        };
        assert!(config3.is_configured());
    }

    /// True if the assembled settings contain `key` mapped exactly to `value`.
    fn has(settings: &[(&'static str, String)], key: &str, value: &str) -> bool {
        for (k, v) in settings {
            if *k == key && v.as_str() == value {
                return true;
            }
        }
        false
    }

    /// True if the assembled settings contain `key` with any value.
    fn has_key(settings: &[(&'static str, String)], key: &str) -> bool {
        settings.iter().any(|(k, _)| *k == key)
    }

    #[test]
    fn test_build_producer_settings_idempotence_invariants() {
        // A valid idempotent producer requires acks=all, a bounded in-flight
        // window (librdkafka caps this at 5 for enable.idempotence=true), and
        // idempotence enabled. These invariants must hold for any config.
        let config = ShadowConfig {
            enabled: true,
            bootstrap_servers: "kafka:9092".to_string(),
            security_protocol: "SASL_SSL".to_string(),
            max_retries: 5,
            ..Default::default()
        };
        let s = build_producer_settings(&config);
        assert!(has(&s, "enable.idempotence", "true"));
        assert!(has(&s, "max.in.flight.requests.per.connection", "5"));
        assert!(has(&s, "acks", "all"));
        assert!(has(&s, "compression.type", "none"));
    }

    #[test]
    fn test_build_producer_settings_max_retries_is_not_a_librdkafka_setting() {
        // Wave 3: shadow_max_retries is the outbox dead-letter cap. librdkafka
        // keeps its own (time-bounded) retries, as idempotence wants, and the
        // ordering guard is on.
        let config = ShadowConfig {
            enabled: true,
            bootstrap_servers: "kafka:9092".to_string(),
            max_retries: 0,
            ..Default::default()
        };
        let s = build_producer_settings(&config);
        assert!(!has_key(&s, "retries"));
        assert!(has(&s, "enable.gapless.guarantee", "true"));
        assert!(has(&s, "partitioner", "murmur2_random"));
    }

    #[test]
    fn test_build_producer_settings_sasl_only_for_sasl_protocol() {
        // SASL keys are present for SASL* protocols and absent otherwise.
        let sasl = ShadowConfig {
            enabled: true,
            bootstrap_servers: "kafka:9092".to_string(),
            security_protocol: "SASL_SSL".to_string(),
            sasl_mechanism: "SCRAM-SHA-256".to_string(),
            sasl_username: "user".to_string(),
            sasl_password: "pass".to_string(),
            ..Default::default()
        };
        let s = build_producer_settings(&sasl);
        assert!(has(&s, "sasl.mechanism", "SCRAM-SHA-256"));
        assert!(has(&s, "sasl.username", "user"));
        assert!(has(&s, "sasl.password", "pass"));

        let plaintext = ShadowConfig {
            enabled: true,
            bootstrap_servers: "kafka:9092".to_string(),
            security_protocol: "PLAINTEXT".to_string(),
            ..Default::default()
        };
        let s = build_producer_settings(&plaintext);
        assert!(!has_key(&s, "sasl.mechanism"));
        assert!(!has_key(&s, "sasl.username"));
        assert!(!has_key(&s, "sasl.password"));
    }

    #[test]
    fn test_build_producer_settings_empty_sasl_credentials_omitted() {
        // Empty username/password must not be emitted (librdkafka rejects empty
        // SASL fields); the mechanism is still set for a SASL protocol.
        let config = ShadowConfig {
            enabled: true,
            bootstrap_servers: "kafka:9092".to_string(),
            security_protocol: "SASL_PLAINTEXT".to_string(),
            sasl_username: String::new(),
            sasl_password: String::new(),
            ..Default::default()
        };
        let s = build_producer_settings(&config);
        assert!(has_key(&s, "sasl.mechanism"));
        assert!(!has_key(&s, "sasl.username"));
        assert!(!has_key(&s, "sasl.password"));
    }

    #[test]
    fn test_build_producer_settings_ssl_endpoint_only_for_ssl() {
        // Hostname verification is set only for *SSL protocols.
        let ssl = ShadowConfig {
            enabled: true,
            bootstrap_servers: "kafka:9092".to_string(),
            security_protocol: "SSL".to_string(),
            ..Default::default()
        };
        let key = "ssl.endpoint.identification.algorithm";
        assert!(has(&build_producer_settings(&ssl), key, "https"));

        let plaintext = ShadowConfig {
            enabled: true,
            bootstrap_servers: "kafka:9092".to_string(),
            security_protocol: "PLAINTEXT".to_string(),
            ..Default::default()
        };
        assert!(!has_key(&build_producer_settings(&plaintext), key));
    }

    #[test]
    fn test_build_producer_settings_ca_location_conditional() {
        // ssl.ca.location is emitted only when configured.
        let with_ca = ShadowConfig {
            enabled: true,
            bootstrap_servers: "kafka:9092".to_string(),
            security_protocol: "SSL".to_string(),
            ssl_ca_location: "/etc/ssl/ca.pem".to_string(),
            ..Default::default()
        };
        let s = build_producer_settings(&with_ca);
        assert!(has(&s, "ssl.ca.location", "/etc/ssl/ca.pem"));

        let without_ca = ShadowConfig {
            enabled: true,
            bootstrap_servers: "kafka:9092".to_string(),
            security_protocol: "SSL".to_string(),
            ssl_ca_location: String::new(),
            ..Default::default()
        };
        let s = build_producer_settings(&without_ca);
        assert!(!has_key(&s, "ssl.ca.location"));
    }

    #[test]
    fn test_build_producer_settings_maps_config_values() {
        // Bootstrap servers, timeouts, and performance knobs carry through.
        let config = ShadowConfig {
            enabled: true,
            bootstrap_servers: "a:9092,b:9092".to_string(),
            security_protocol: "PLAINTEXT".to_string(),
            batch_size: 4096,
            linger_ms: 25,
            retry_backoff_ms: 200,
            ..Default::default()
        };
        let s = build_producer_settings(&config);
        assert!(has(&s, "bootstrap.servers", "a:9092,b:9092"));
        assert!(has(&s, "security.protocol", "PLAINTEXT"));
        // shadow_batch_size counts messages: batch.num.messages, never the
        // byte-sized batch.size (wave 3 fix).
        assert!(has(&s, "batch.num.messages", "4096"));
        assert!(!has_key(&s, "batch.size"));
        assert!(has(&s, "linger.ms", "25"));
        assert!(has(&s, "retry.backoff.ms", "200"));
        assert!(has(&s, "request.timeout.ms", "30000"));
        assert!(has(&s, "message.timeout.ms", "120000"));
    }
}
