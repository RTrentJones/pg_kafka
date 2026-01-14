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
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;
use std::sync::Arc;
use std::time::Duration;

/// Default delivery timeout for async sends (30 seconds)
const DEFAULT_DELIVERY_TIMEOUT_MS: u64 = 30_000;

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

    /// Create the rdkafka producer with SASL/SSL configuration
    fn create_producer(config: &ShadowConfig) -> ShadowResult<FutureProducer> {
        let mut client_config = ClientConfig::new();

        // Basic configuration
        client_config.set("bootstrap.servers", &config.bootstrap_servers);

        // Security protocol (SASL_SSL, SASL_PLAINTEXT, SSL, PLAINTEXT)
        client_config.set("security.protocol", &config.security_protocol);

        // SASL configuration (if using SASL_* protocol)
        if config.security_protocol.starts_with("SASL") {
            client_config.set("sasl.mechanism", &config.sasl_mechanism);

            if !config.sasl_username.is_empty() {
                client_config.set("sasl.username", &config.sasl_username);
            }

            if !config.sasl_password.is_empty() {
                client_config.set("sasl.password", &config.sasl_password);
            }
        }

        // SSL configuration (if using *_SSL protocol)
        if config.security_protocol.ends_with("SSL") {
            if !config.ssl_ca_location.is_empty() {
                client_config.set("ssl.ca.location", &config.ssl_ca_location);
            }

            // Enable hostname verification for security
            client_config.set("ssl.endpoint.identification.algorithm", "https");
        }

        // Producer performance settings
        client_config.set("batch.size", config.batch_size.to_string());
        client_config.set("linger.ms", config.linger_ms.to_string());
        client_config.set("retry.backoff.ms", config.retry_backoff_ms.to_string());
        client_config.set("retries", config.max_retries.to_string());

        // Compression (use none for maximum compatibility)
        client_config.set("compression.type", "none");

        // Disable idempotent producer - external Kafka may not support it
        // in all configurations (e.g., single-node KRaft clusters)
        client_config.set("enable.idempotence", "false");

        // NOTE: Removed deprecated api.version.* settings that caused
        // "Required feature not supported by broker" errors with modern Kafka.
        // Modern librdkafka handles API negotiation automatically.

        // Request timeout and message timeout
        client_config.set("request.timeout.ms", "30000");
        client_config.set("message.timeout.ms", "120000");

        // Acknowledgments (use 1 for single-node compatibility)
        client_config.set("acks", "1");

        // Create the producer
        client_config
            .create()
            .map_err(|e| ShadowError::ProducerError(format!("Failed to create producer: {}", e)))
    }

    /// Send a message asynchronously
    ///
    /// Returns immediately after queueing the message. The message will be
    /// delivered in the background with retries as configured.
    pub async fn send_async(
        &self,
        topic: &str,
        partition: Option<i32>,
        key: Option<&[u8]>,
        payload: Option<&[u8]>,
    ) -> ShadowResult<()> {
        let mut record = FutureRecord::to(topic);

        if let Some(k) = key {
            record = record.key(k);
        }

        if let Some(p) = payload {
            record = record.payload(p);
        }

        if let Some(part) = partition {
            record = record.partition(part);
        }

        let timeout = Timeout::After(Duration::from_millis(DEFAULT_DELIVERY_TIMEOUT_MS));

        self.producer
            .send(record, timeout)
            .await
            .map_err(|(err, _)| ShadowError::ForwardFailed {
                topic: topic.to_string(),
                partition: partition.unwrap_or(-1),
                error: err.to_string(),
            })?;

        Ok(())
    }

    /// Send a message synchronously (blocking)
    ///
    /// Waits for delivery confirmation before returning. Use this for
    /// sync_mode = "sync" topics where we need to confirm delivery before
    /// acknowledging to the original producer.
    ///
    /// This uses `futures::executor::block_on` which works without a tokio
    /// runtime context. This is essential for the DB worker thread which
    /// does not have access to the tokio runtime (that lives in the network thread).
    #[allow(clippy::result_large_err)]
    pub fn send_sync(
        &self,
        topic: &str,
        partition: Option<i32>,
        key: Option<&[u8]>,
        payload: Option<&[u8]>,
        timeout_ms: u64,
    ) -> ShadowResult<()> {
        // Use futures::executor::block_on which works without a tokio runtime
        // This is critical: the DB thread doesn't have a tokio runtime context
        let result = futures::executor::block_on(async {
            let mut record = FutureRecord::to(topic);

            if let Some(k) = key {
                record = record.key(k);
            }

            if let Some(p) = payload {
                record = record.payload(p);
            }

            if let Some(part) = partition {
                record = record.partition(part);
            }

            let timeout = Timeout::After(Duration::from_millis(timeout_ms));

            self.producer.send(record, timeout).await
        });

        result.map_err(|(err, _)| ShadowError::ForwardFailed {
            topic: topic.to_string(),
            partition: partition.unwrap_or(-1),
            error: err.to_string(),
        })?;

        Ok(())
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

    /// Check if the producer is healthy by attempting metadata fetch
    pub async fn health_check(&self) -> ShadowResult<()> {
        let timeout = Duration::from_secs(10);
        self.producer
            .client()
            .fetch_metadata(None, timeout)
            .map_err(|e| {
                ShadowError::KafkaUnavailable(format!("Failed to fetch metadata: {}", e))
            })?;

        Ok(())
    }

    /// Check if the producer can communicate with Kafka (sync version)
    ///
    /// Returns true if metadata fetch succeeds, false otherwise.
    /// Uses a short timeout (5s) to avoid blocking too long on unhealthy connections.
    /// This is called from ensure_producer() to detect stale connections after
    /// network failures or broker restarts.
    pub fn is_healthy(&self) -> bool {
        // Use a short timeout to avoid blocking too long
        let timeout = Duration::from_secs(5);

        // Try to fetch metadata - this verifies broker connectivity
        match self.producer.client().fetch_metadata(None, timeout) {
            Ok(metadata) => {
                // Check that we have at least one broker
                !metadata.brokers().is_empty()
            }
            Err(_) => false,
        }
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
}
