// Configuration module for pg_kafka
//
// This module defines GUC (Grand Unified Configuration) parameters that can be
// set in postgresql.conf or via SQL commands.
//
// pgrx 0.16+ uses declarative macros for GUC definitions

use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting};

use crate::kafka::constants::{
    DEFAULT_COMPRESSION_TYPE,
    // Shadow mode constants (Phase 11)
    DEFAULT_CONFIG_RELOAD_MS,
    DEFAULT_DATABASE,
    DEFAULT_FETCH_POLL_INTERVAL_MS,
    DEFAULT_KAFKA_PORT,
    DEFAULT_SHADOW_BATCH_SIZE,
    DEFAULT_SHADOW_BOOTSTRAP_SERVERS,
    DEFAULT_SHADOW_LINGER_MS,
    DEFAULT_SHADOW_MAX_RETRIES,
    DEFAULT_SHADOW_METRICS_ENABLED,
    DEFAULT_SHADOW_MODE_ENABLED,
    DEFAULT_SHADOW_OTEL_ENDPOINT,
    DEFAULT_SHADOW_RETRY_BACKOFF_MS,
    DEFAULT_SHADOW_SASL_MECHANISM,
    DEFAULT_SHADOW_SECURITY_PROTOCOL,
    DEFAULT_SHADOW_SYNC_MODE,
    DEFAULT_SHUTDOWN_TIMEOUT_MS,
    DEFAULT_TOPIC_PARTITIONS,
    MAX_CONFIG_RELOAD_MS,
    MAX_FETCH_POLL_INTERVAL_MS,
    MAX_SHADOW_BATCH_SIZE,
    MAX_SHADOW_LINGER_MS,
    MAX_SHADOW_MAX_RETRIES,
    MAX_SHADOW_RETRY_BACKOFF_MS,
    MIN_CONFIG_RELOAD_MS,
    MIN_FETCH_POLL_INTERVAL_MS,
    MIN_SHADOW_BATCH_SIZE,
    MIN_SHADOW_LINGER_MS,
    MIN_SHADOW_MAX_RETRIES,
    MIN_SHADOW_RETRY_BACKOFF_MS,
    MIN_SHUTDOWN_TIMEOUT_MS,
};

#[cfg(not(test))]
use crate::kafka::constants::DEFAULT_HOST;

#[cfg(test)]
use crate::kafka::constants::TEST_HOST;

/// Configuration struct holding all pg_kafka settings
pub struct Config {
    pub port: i32,
    pub host: String,
    pub database: String,
    pub log_connections: bool,
    pub shutdown_timeout_ms: i32,
    pub default_partitions: i32,
    /// Polling interval for long polling fallback (milliseconds)
    pub fetch_poll_interval_ms: i32,
    /// Whether long polling is enabled
    pub enable_long_polling: bool,
    /// Compression type for FetchResponse encoding (none, gzip, snappy, lz4, zstd)
    pub compression_type: String,
    /// Enable timing instrumentation for performance research
    pub log_timing: bool,

    // ===== Shadow Mode Configuration (Phase 11) =====
    /// Whether shadow mode is enabled globally
    pub shadow_mode_enabled: bool,
    /// External Kafka bootstrap servers (e.g., "kafka1:9092,kafka2:9092")
    pub shadow_bootstrap_servers: String,
    /// Security protocol for external Kafka (SASL_SSL, SASL_PLAINTEXT, SSL, PLAINTEXT)
    pub shadow_security_protocol: String,
    /// SASL mechanism for external Kafka (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)
    pub shadow_sasl_mechanism: String,
    /// SASL username for external Kafka
    pub shadow_sasl_username: String,
    /// SASL password for external Kafka
    pub shadow_sasl_password: String,
    /// SSL CA certificate location for external Kafka
    pub shadow_ssl_ca_location: String,
    /// Messages per batch for shadow forwarding
    pub shadow_batch_size: i32,
    /// Batching delay in milliseconds
    pub shadow_linger_ms: i32,
    /// Retry backoff in milliseconds
    pub shadow_retry_backoff_ms: i32,
    /// Maximum retries per message
    pub shadow_max_retries: i32,
    /// Default sync mode for shadow forwarding (async, sync)
    pub shadow_default_sync_mode: String,
    /// Whether shadow metrics are enabled
    pub shadow_metrics_enabled: bool,
    /// OpenTelemetry OTLP endpoint for shadow tracing
    pub shadow_otel_endpoint: String,
    /// License key for Shadow Mode production use (Commercial License)
    /// Format: "sponsor_id:signature" or "eval" for evaluation
    pub shadow_license_key: String,
}

/// Custom Debug implementation that redacts sensitive credentials
impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("port", &self.port)
            .field("host", &self.host)
            .field("database", &self.database)
            .field("log_connections", &self.log_connections)
            .field("shutdown_timeout_ms", &self.shutdown_timeout_ms)
            .field("default_partitions", &self.default_partitions)
            .field("fetch_poll_interval_ms", &self.fetch_poll_interval_ms)
            .field("enable_long_polling", &self.enable_long_polling)
            .field("compression_type", &self.compression_type)
            .field("log_timing", &self.log_timing)
            .field("shadow_mode_enabled", &self.shadow_mode_enabled)
            .field("shadow_bootstrap_servers", &self.shadow_bootstrap_servers)
            .field("shadow_security_protocol", &self.shadow_security_protocol)
            .field("shadow_sasl_mechanism", &self.shadow_sasl_mechanism)
            // REDACT sensitive credentials to prevent log exposure
            .field("shadow_sasl_username", &"[REDACTED]")
            .field("shadow_sasl_password", &"[REDACTED]")
            .field("shadow_ssl_ca_location", &self.shadow_ssl_ca_location)
            .field("shadow_batch_size", &self.shadow_batch_size)
            .field("shadow_linger_ms", &self.shadow_linger_ms)
            .field("shadow_retry_backoff_ms", &self.shadow_retry_backoff_ms)
            .field("shadow_max_retries", &self.shadow_max_retries)
            .field("shadow_default_sync_mode", &self.shadow_default_sync_mode)
            .field("shadow_metrics_enabled", &self.shadow_metrics_enabled)
            .field("shadow_otel_endpoint", &self.shadow_otel_endpoint)
            .field("shadow_license_key", &"[REDACTED]")
            .finish()
    }
}

impl Config {
    /// Load configuration from GUC parameters
    #[cfg(not(test))]
    pub fn load() -> Self {
        Config {
            port: PORT.get(),
            host: HOST
                .get()
                .as_deref()
                .map(|c| c.to_string_lossy().into_owned())
                .unwrap_or_else(|| DEFAULT_HOST.to_string()),
            database: DATABASE
                .get()
                .as_deref()
                .map(|c| c.to_string_lossy().into_owned())
                .unwrap_or_else(|| DEFAULT_DATABASE.to_string()),
            log_connections: LOG_CONNECTIONS.get(),
            shutdown_timeout_ms: SHUTDOWN_TIMEOUT_MS.get(),
            default_partitions: DEFAULT_PARTITIONS.get(),
            fetch_poll_interval_ms: FETCH_POLL_INTERVAL_MS.get(),
            enable_long_polling: ENABLE_LONG_POLLING.get(),
            compression_type: COMPRESSION_TYPE
                .get()
                .as_deref()
                .map(|c| c.to_string_lossy().into_owned())
                .unwrap_or_else(|| DEFAULT_COMPRESSION_TYPE.to_string()),
            log_timing: LOG_TIMING.get(),
            // Shadow mode configuration (Phase 11)
            shadow_mode_enabled: SHADOW_MODE_ENABLED.get(),
            shadow_bootstrap_servers: SHADOW_BOOTSTRAP_SERVERS
                .get()
                .as_deref()
                .map(|c| c.to_string_lossy().into_owned())
                .unwrap_or_else(|| DEFAULT_SHADOW_BOOTSTRAP_SERVERS.to_string()),
            shadow_security_protocol: SHADOW_SECURITY_PROTOCOL
                .get()
                .as_deref()
                .map(|c| c.to_string_lossy().into_owned())
                .unwrap_or_else(|| DEFAULT_SHADOW_SECURITY_PROTOCOL.to_string()),
            shadow_sasl_mechanism: SHADOW_SASL_MECHANISM
                .get()
                .as_deref()
                .map(|c| c.to_string_lossy().into_owned())
                .unwrap_or_else(|| DEFAULT_SHADOW_SASL_MECHANISM.to_string()),
            shadow_sasl_username: SHADOW_SASL_USERNAME
                .get()
                .as_deref()
                .map(|c| c.to_string_lossy().into_owned())
                .unwrap_or_default(),
            shadow_sasl_password: SHADOW_SASL_PASSWORD
                .get()
                .as_deref()
                .map(|c| c.to_string_lossy().into_owned())
                .unwrap_or_default(),
            shadow_ssl_ca_location: SHADOW_SSL_CA_LOCATION
                .get()
                .as_deref()
                .map(|c| c.to_string_lossy().into_owned())
                .unwrap_or_default(),
            shadow_batch_size: SHADOW_BATCH_SIZE.get(),
            shadow_linger_ms: SHADOW_LINGER_MS.get(),
            shadow_retry_backoff_ms: SHADOW_RETRY_BACKOFF_MS.get(),
            shadow_max_retries: SHADOW_MAX_RETRIES.get(),
            shadow_default_sync_mode: SHADOW_DEFAULT_SYNC_MODE
                .get()
                .as_deref()
                .map(|c| c.to_string_lossy().into_owned())
                .unwrap_or_else(|| DEFAULT_SHADOW_SYNC_MODE.to_string()),
            shadow_metrics_enabled: SHADOW_METRICS_ENABLED.get(),
            shadow_otel_endpoint: SHADOW_OTEL_ENDPOINT
                .get()
                .as_deref()
                .map(|c| c.to_string_lossy().into_owned())
                .unwrap_or_else(|| DEFAULT_SHADOW_OTEL_ENDPOINT.to_string()),
            // License key (Commercial License)
            shadow_license_key: SHADOW_LICENSE_KEY
                .get()
                .as_deref()
                .map(|c| c.to_string_lossy().into_owned())
                .unwrap_or_default(),
        }
    }

    /// Test-only version that returns defaults without accessing GUC
    #[cfg(test)]
    pub fn load() -> Self {
        Config {
            port: DEFAULT_KAFKA_PORT,
            host: TEST_HOST.to_string(),
            database: DEFAULT_DATABASE.to_string(),
            log_connections: false,
            shutdown_timeout_ms: DEFAULT_SHUTDOWN_TIMEOUT_MS,
            default_partitions: DEFAULT_TOPIC_PARTITIONS,
            fetch_poll_interval_ms: DEFAULT_FETCH_POLL_INTERVAL_MS,
            enable_long_polling: true,
            compression_type: DEFAULT_COMPRESSION_TYPE.to_string(),
            log_timing: false,
            // Shadow mode configuration (Phase 11) - defaults for testing
            shadow_mode_enabled: DEFAULT_SHADOW_MODE_ENABLED,
            shadow_bootstrap_servers: DEFAULT_SHADOW_BOOTSTRAP_SERVERS.to_string(),
            shadow_security_protocol: DEFAULT_SHADOW_SECURITY_PROTOCOL.to_string(),
            shadow_sasl_mechanism: DEFAULT_SHADOW_SASL_MECHANISM.to_string(),
            shadow_sasl_username: String::new(),
            shadow_sasl_password: String::new(),
            shadow_ssl_ca_location: String::new(),
            shadow_batch_size: DEFAULT_SHADOW_BATCH_SIZE,
            shadow_linger_ms: DEFAULT_SHADOW_LINGER_MS,
            shadow_retry_backoff_ms: DEFAULT_SHADOW_RETRY_BACKOFF_MS,
            shadow_max_retries: DEFAULT_SHADOW_MAX_RETRIES,
            shadow_default_sync_mode: DEFAULT_SHADOW_SYNC_MODE.to_string(),
            shadow_metrics_enabled: DEFAULT_SHADOW_METRICS_ENABLED,
            shadow_otel_endpoint: DEFAULT_SHADOW_OTEL_ENDPOINT.to_string(),
            // Tests run in eval mode by default (Commercial License)
            shadow_license_key: "eval".to_string(),
        }
    }
}

// GUC parameter definitions
use std::ffi::CString;

static PORT: GucSetting<i32> = GucSetting::<i32>::new(DEFAULT_KAFKA_PORT);
static HOST: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
static DATABASE: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
static LOG_CONNECTIONS: GucSetting<bool> = GucSetting::<bool>::new(false);
static SHUTDOWN_TIMEOUT_MS: GucSetting<i32> = GucSetting::<i32>::new(DEFAULT_SHUTDOWN_TIMEOUT_MS);
static DEFAULT_PARTITIONS: GucSetting<i32> = GucSetting::<i32>::new(DEFAULT_TOPIC_PARTITIONS);
static FETCH_POLL_INTERVAL_MS: GucSetting<i32> =
    GucSetting::<i32>::new(DEFAULT_FETCH_POLL_INTERVAL_MS);
static ENABLE_LONG_POLLING: GucSetting<bool> = GucSetting::<bool>::new(true);
static COMPRESSION_TYPE: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
static LOG_TIMING: GucSetting<bool> = GucSetting::<bool>::new(false);

// Shadow mode GUC definitions (Phase 11)
// Made public so ShadowStore can check them at runtime without restart
pub static SHADOW_MODE_ENABLED: GucSetting<bool> =
    GucSetting::<bool>::new(DEFAULT_SHADOW_MODE_ENABLED);
pub static SHADOW_BOOTSTRAP_SERVERS: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);
static SHADOW_SECURITY_PROTOCOL: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);
static SHADOW_SASL_MECHANISM: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);
static SHADOW_SASL_USERNAME: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
static SHADOW_SASL_PASSWORD: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
static SHADOW_SSL_CA_LOCATION: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);
static SHADOW_BATCH_SIZE: GucSetting<i32> = GucSetting::<i32>::new(DEFAULT_SHADOW_BATCH_SIZE);
static SHADOW_LINGER_MS: GucSetting<i32> = GucSetting::<i32>::new(DEFAULT_SHADOW_LINGER_MS);
static SHADOW_RETRY_BACKOFF_MS: GucSetting<i32> =
    GucSetting::<i32>::new(DEFAULT_SHADOW_RETRY_BACKOFF_MS);
static SHADOW_MAX_RETRIES: GucSetting<i32> = GucSetting::<i32>::new(DEFAULT_SHADOW_MAX_RETRIES);
static SHADOW_DEFAULT_SYNC_MODE: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);
static SHADOW_METRICS_ENABLED: GucSetting<bool> =
    GucSetting::<bool>::new(DEFAULT_SHADOW_METRICS_ENABLED);
static SHADOW_OTEL_ENDPOINT: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
// License key for Shadow Mode (Commercial License)
static SHADOW_LICENSE_KEY: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
pub static CONFIG_RELOAD_INTERVAL_MS: GucSetting<i32> =
    GucSetting::<i32>::new(DEFAULT_CONFIG_RELOAD_MS);

/// Initialize GUC parameters
pub fn init() {
    GucRegistry::define_int_guc(
        c"pg_kafka.port",
        c"Port to listen on for Kafka protocol connections",
        c"The TCP port that pg_kafka will bind to. Default: 9092. Requires restart to take effect.",
        &PORT,
        1024,
        65535,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pg_kafka.host",
        c"Host/interface to bind to",
        c"The network interface to listen on. Use 0.0.0.0 for all interfaces. Requires restart.",
        &HOST,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pg_kafka.database",
        c"Database to connect to for SPI operations",
        c"The database where CREATE EXTENSION pg_kafka was run. Default: postgres. Requires restart.",
        &DATABASE,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        c"pg_kafka.log_connections",
        c"Whether to log each connection",
        c"When enabled, logs every incoming connection. Can be changed at runtime.",
        &LOG_CONNECTIONS,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_kafka.shutdown_timeout_ms",
        c"Timeout for graceful shutdown in milliseconds",
        c"How long to wait for the TCP listener to shut down cleanly. Default: 5000ms.",
        &SHUTDOWN_TIMEOUT_MS,
        MIN_SHUTDOWN_TIMEOUT_MS,
        60000,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_kafka.default_partitions",
        c"Default number of partitions for auto-created topics",
        c"When a topic is auto-created via produce, this sets the partition count. Default: 1.",
        &DEFAULT_PARTITIONS,
        1,
        10000,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_kafka.fetch_poll_interval_ms",
        c"Polling interval for long polling fallback (milliseconds)",
        c"How often to poll the database when waiting for data in FetchRequest. Default: 100ms.",
        &FETCH_POLL_INTERVAL_MS,
        MIN_FETCH_POLL_INTERVAL_MS,
        MAX_FETCH_POLL_INTERVAL_MS,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        c"pg_kafka.enable_long_polling",
        c"Enable long polling for FetchRequest",
        c"When enabled, FetchRequest will wait up to max_wait_ms for data. Default: true.",
        &ENABLE_LONG_POLLING,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pg_kafka.compression_type",
        c"Compression type for FetchResponse encoding",
        c"Compression codec for outbound messages: none, gzip, snappy, lz4, zstd. Default: none.",
        &COMPRESSION_TYPE,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        c"pg_kafka.log_timing",
        c"Enable timing instrumentation for performance research",
        c"When enabled, logs transaction and handler timing for each request. Use for benchmarking.",
        &LOG_TIMING,
        GucContext::Suset,
        GucFlags::default(),
    );

    // ===== Shadow Mode GUC Registration (Phase 11) =====

    GucRegistry::define_bool_guc(
        c"pg_kafka.shadow_mode_enabled",
        c"Enable shadow mode for forwarding to external Kafka",
        c"When enabled, messages can be forwarded to an external Kafka cluster. Can be changed with pg_reload_conf().",
        &SHADOW_MODE_ENABLED,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pg_kafka.shadow_bootstrap_servers",
        c"External Kafka bootstrap servers",
        c"Comma-separated list of external Kafka brokers (e.g., 'kafka1:9092,kafka2:9092'). Reloadable with pg_reload_conf().",
        &SHADOW_BOOTSTRAP_SERVERS,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pg_kafka.shadow_security_protocol",
        c"Security protocol for external Kafka",
        c"Security protocol: SASL_SSL, SASL_PLAINTEXT, SSL, PLAINTEXT. Default: SASL_SSL. Reloadable with pg_reload_conf().",
        &SHADOW_SECURITY_PROTOCOL,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pg_kafka.shadow_sasl_mechanism",
        c"SASL mechanism for external Kafka",
        c"SASL mechanism: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512. Default: PLAIN. Requires restart.",
        &SHADOW_SASL_MECHANISM,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pg_kafka.shadow_sasl_username",
        c"SASL username for external Kafka",
        c"Username for SASL authentication to external Kafka. Requires restart.",
        &SHADOW_SASL_USERNAME,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pg_kafka.shadow_sasl_password",
        c"SASL password for external Kafka",
        c"Password for SASL authentication to external Kafka. Requires restart.",
        &SHADOW_SASL_PASSWORD,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pg_kafka.shadow_ssl_ca_location",
        c"SSL CA certificate location for external Kafka",
        c"Path to CA certificate file for SSL verification. Leave empty to use system CAs. Requires restart.",
        &SHADOW_SSL_CA_LOCATION,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_kafka.shadow_batch_size",
        c"Messages per batch for shadow forwarding",
        c"Number of messages to batch before sending to external Kafka. Default: 1000.",
        &SHADOW_BATCH_SIZE,
        MIN_SHADOW_BATCH_SIZE,
        MAX_SHADOW_BATCH_SIZE,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_kafka.shadow_linger_ms",
        c"Batching delay for shadow forwarding (milliseconds)",
        c"How long to wait for more messages before sending a batch. Default: 10ms.",
        &SHADOW_LINGER_MS,
        MIN_SHADOW_LINGER_MS,
        MAX_SHADOW_LINGER_MS,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_kafka.shadow_retry_backoff_ms",
        c"Retry backoff for shadow forwarding (milliseconds)",
        c"Initial backoff between retries for failed forwards. Default: 100ms.",
        &SHADOW_RETRY_BACKOFF_MS,
        MIN_SHADOW_RETRY_BACKOFF_MS,
        MAX_SHADOW_RETRY_BACKOFF_MS,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_kafka.shadow_max_retries",
        c"Maximum retries per message for shadow forwarding",
        c"Maximum number of retry attempts for failed forwards. Default: 3.",
        &SHADOW_MAX_RETRIES,
        MIN_SHADOW_MAX_RETRIES,
        MAX_SHADOW_MAX_RETRIES,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pg_kafka.shadow_default_sync_mode",
        c"Default sync mode for shadow forwarding",
        c"Default forwarding mode: 'async' (non-blocking) or 'sync' (wait for ack). Default: async.",
        &SHADOW_DEFAULT_SYNC_MODE,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        c"pg_kafka.shadow_metrics_enabled",
        c"Enable shadow mode metrics",
        c"When enabled, tracks forwarding statistics in kafka.shadow_metrics table. Default: true.",
        &SHADOW_METRICS_ENABLED,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pg_kafka.shadow_otel_endpoint",
        c"OpenTelemetry OTLP endpoint for shadow tracing",
        c"OTLP endpoint for sending shadow mode traces (e.g., 'http://localhost:4317'). Empty to disable.",
        &SHADOW_OTEL_ENDPOINT,
        GucContext::Suset,
        GucFlags::default(),
    );

    // Shadow Mode License Key (Commercial License)
    GucRegistry::define_string_guc(
        c"pg_kafka.shadow_license_key",
        c"License key for Shadow Mode production use",
        c"Format: 'sponsor_id:signature' or 'eval' for evaluation. Obtain from https://github.com/sponsors/RTrentJones",
        &SHADOW_LICENSE_KEY,
        GucContext::Postmaster, // Requires restart
        GucFlags::NO_SHOW_ALL | GucFlags::SUPERUSER_ONLY,
    );

    GucRegistry::define_int_guc(
        c"pg_kafka.config_reload_interval_ms",
        c"Configuration reload interval in milliseconds",
        c"How often to reload GUCs and shadow config from the database. Default 30000ms (30s). Tests can set to 1000-2000ms for faster iteration. Reloadable with pg_reload_conf().",
        &CONFIG_RELOAD_INTERVAL_MS,
        MIN_CONFIG_RELOAD_MS,
        MAX_CONFIG_RELOAD_MS,
        GucContext::Sighup,
        GucFlags::default(),
    );

    pgrx::log!("pg_kafka GUC parameters initialized (including shadow mode)");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::constants::{
        DEFAULT_COMPRESSION_TYPE, DEFAULT_DATABASE, DEFAULT_FETCH_POLL_INTERVAL_MS,
        DEFAULT_KAFKA_PORT, DEFAULT_SHADOW_BATCH_SIZE, DEFAULT_SHADOW_BOOTSTRAP_SERVERS,
        DEFAULT_SHADOW_LINGER_MS, DEFAULT_SHADOW_MAX_RETRIES, DEFAULT_SHADOW_METRICS_ENABLED,
        DEFAULT_SHADOW_MODE_ENABLED, DEFAULT_SHADOW_OTEL_ENDPOINT, DEFAULT_SHADOW_RETRY_BACKOFF_MS,
        DEFAULT_SHADOW_SASL_MECHANISM, DEFAULT_SHADOW_SECURITY_PROTOCOL, DEFAULT_SHADOW_SYNC_MODE,
        DEFAULT_SHUTDOWN_TIMEOUT_MS, DEFAULT_TOPIC_PARTITIONS, MAX_CONFIG_RELOAD_MS,
        MAX_FETCH_POLL_INTERVAL_MS, MAX_SHADOW_BATCH_SIZE, MAX_SHADOW_LINGER_MS,
        MAX_SHADOW_MAX_RETRIES, MAX_SHADOW_RETRY_BACKOFF_MS, MIN_CONFIG_RELOAD_MS,
        MIN_FETCH_POLL_INTERVAL_MS, MIN_SHADOW_BATCH_SIZE, MIN_SHADOW_LINGER_MS,
        MIN_SHADOW_MAX_RETRIES, MIN_SHADOW_RETRY_BACKOFF_MS, MIN_SHUTDOWN_TIMEOUT_MS, TEST_HOST,
    };

    // ========== Config::load() Tests ==========

    #[test]
    fn test_config_load_returns_defaults() {
        let config = Config::load();
        assert_eq!(config.port, DEFAULT_KAFKA_PORT);
        assert_eq!(config.host, TEST_HOST);
        assert_eq!(config.database, DEFAULT_DATABASE);
    }

    #[test]
    fn test_config_default_port_value() {
        let config = Config::load();
        assert_eq!(config.port, 9092);
    }

    #[test]
    fn test_config_default_host_value() {
        let config = Config::load();
        assert_eq!(config.host, TEST_HOST);
        assert_eq!(config.host, "localhost");
    }

    #[test]
    fn test_config_default_database_value() {
        let config = Config::load();
        assert_eq!(config.database, "postgres");
    }

    #[test]
    fn test_config_default_partitions_value() {
        let config = Config::load();
        assert_eq!(config.default_partitions, DEFAULT_TOPIC_PARTITIONS);
        assert_eq!(config.default_partitions, 1);
    }

    #[test]
    fn test_config_default_compression_type() {
        let config = Config::load();
        assert_eq!(config.compression_type, DEFAULT_COMPRESSION_TYPE);
        assert_eq!(config.compression_type, "none");
    }

    #[test]
    fn test_config_default_long_polling_enabled() {
        let config = Config::load();
        assert!(config.enable_long_polling);
    }

    #[test]
    fn test_config_default_fetch_poll_interval() {
        let config = Config::load();
        assert_eq!(
            config.fetch_poll_interval_ms,
            DEFAULT_FETCH_POLL_INTERVAL_MS
        );
        assert_eq!(config.fetch_poll_interval_ms, 100);
    }

    #[test]
    fn test_config_default_shutdown_timeout() {
        let config = Config::load();
        assert_eq!(config.shutdown_timeout_ms, DEFAULT_SHUTDOWN_TIMEOUT_MS);
        assert_eq!(config.shutdown_timeout_ms, 5000);
    }

    #[test]
    fn test_config_default_log_connections_disabled() {
        let config = Config::load();
        assert!(!config.log_connections);
    }

    #[test]
    fn test_config_default_log_timing_disabled() {
        let config = Config::load();
        assert!(!config.log_timing);
    }

    // ========== Shadow Mode Config Tests ==========

    #[test]
    fn test_config_default_shadow_mode_disabled() {
        let config = Config::load();
        assert_eq!(config.shadow_mode_enabled, DEFAULT_SHADOW_MODE_ENABLED);
        assert!(!config.shadow_mode_enabled);
    }

    #[test]
    fn test_config_default_shadow_batch_size() {
        let config = Config::load();
        assert_eq!(config.shadow_batch_size, DEFAULT_SHADOW_BATCH_SIZE);
        assert_eq!(config.shadow_batch_size, 1000);
    }

    #[test]
    fn test_config_default_shadow_linger_ms() {
        let config = Config::load();
        assert_eq!(config.shadow_linger_ms, DEFAULT_SHADOW_LINGER_MS);
        assert_eq!(config.shadow_linger_ms, 10);
    }

    #[test]
    fn test_config_default_shadow_retry_settings() {
        let config = Config::load();
        assert_eq!(
            config.shadow_retry_backoff_ms,
            DEFAULT_SHADOW_RETRY_BACKOFF_MS
        );
        assert_eq!(config.shadow_max_retries, DEFAULT_SHADOW_MAX_RETRIES);
        assert_eq!(config.shadow_retry_backoff_ms, 100);
        assert_eq!(config.shadow_max_retries, 3);
    }

    #[test]
    fn test_config_default_shadow_security_protocol() {
        let config = Config::load();
        assert_eq!(
            config.shadow_security_protocol,
            DEFAULT_SHADOW_SECURITY_PROTOCOL
        );
        assert_eq!(config.shadow_security_protocol, "SASL_SSL");
    }

    #[test]
    fn test_config_default_shadow_sasl_mechanism() {
        let config = Config::load();
        assert_eq!(config.shadow_sasl_mechanism, DEFAULT_SHADOW_SASL_MECHANISM);
        assert_eq!(config.shadow_sasl_mechanism, "PLAIN");
    }

    #[test]
    fn test_config_default_shadow_sync_mode() {
        let config = Config::load();
        assert_eq!(config.shadow_default_sync_mode, DEFAULT_SHADOW_SYNC_MODE);
        assert_eq!(config.shadow_default_sync_mode, "async");
    }

    #[test]
    fn test_config_default_shadow_metrics_enabled() {
        let config = Config::load();
        assert_eq!(
            config.shadow_metrics_enabled,
            DEFAULT_SHADOW_METRICS_ENABLED
        );
        assert!(config.shadow_metrics_enabled);
    }

    #[test]
    fn test_config_default_shadow_credentials_empty() {
        let config = Config::load();
        assert!(config.shadow_sasl_username.is_empty());
        assert!(config.shadow_sasl_password.is_empty());
        assert!(config.shadow_ssl_ca_location.is_empty());
    }

    #[test]
    fn test_config_default_shadow_bootstrap_servers_empty() {
        let config = Config::load();
        assert_eq!(
            config.shadow_bootstrap_servers,
            DEFAULT_SHADOW_BOOTSTRAP_SERVERS
        );
        assert!(config.shadow_bootstrap_servers.is_empty());
    }

    #[test]
    fn test_config_default_shadow_otel_endpoint_empty() {
        let config = Config::load();
        assert_eq!(config.shadow_otel_endpoint, DEFAULT_SHADOW_OTEL_ENDPOINT);
        assert!(config.shadow_otel_endpoint.is_empty());
    }

    #[test]
    fn test_config_test_eval_license_key() {
        let config = Config::load();
        // Tests run in eval mode by default
        assert_eq!(config.shadow_license_key, "eval");
    }

    // ========== Constants Validation Tests ==========

    #[test]
    fn test_shadow_batch_size_range() {
        assert!(MIN_SHADOW_BATCH_SIZE <= DEFAULT_SHADOW_BATCH_SIZE);
        assert!(DEFAULT_SHADOW_BATCH_SIZE <= MAX_SHADOW_BATCH_SIZE);
        assert_eq!(MIN_SHADOW_BATCH_SIZE, 1);
        assert_eq!(MAX_SHADOW_BATCH_SIZE, 100000);
    }

    #[test]
    fn test_shadow_linger_ms_range() {
        assert!(MIN_SHADOW_LINGER_MS <= DEFAULT_SHADOW_LINGER_MS);
        assert!(DEFAULT_SHADOW_LINGER_MS <= MAX_SHADOW_LINGER_MS);
        assert_eq!(MIN_SHADOW_LINGER_MS, 0);
        assert_eq!(MAX_SHADOW_LINGER_MS, 60_000);
    }

    #[test]
    fn test_shadow_retry_backoff_range() {
        assert!(MIN_SHADOW_RETRY_BACKOFF_MS <= DEFAULT_SHADOW_RETRY_BACKOFF_MS);
        assert!(DEFAULT_SHADOW_RETRY_BACKOFF_MS <= MAX_SHADOW_RETRY_BACKOFF_MS);
        assert_eq!(MIN_SHADOW_RETRY_BACKOFF_MS, 10);
        assert_eq!(MAX_SHADOW_RETRY_BACKOFF_MS, 60000);
    }

    #[test]
    fn test_shadow_max_retries_range() {
        assert!(MIN_SHADOW_MAX_RETRIES <= DEFAULT_SHADOW_MAX_RETRIES);
        assert!(DEFAULT_SHADOW_MAX_RETRIES <= MAX_SHADOW_MAX_RETRIES);
        assert_eq!(MIN_SHADOW_MAX_RETRIES, 0);
        assert_eq!(MAX_SHADOW_MAX_RETRIES, 100);
    }

    #[test]
    fn test_fetch_poll_interval_range() {
        assert!(MIN_FETCH_POLL_INTERVAL_MS <= DEFAULT_FETCH_POLL_INTERVAL_MS);
        assert!(DEFAULT_FETCH_POLL_INTERVAL_MS <= MAX_FETCH_POLL_INTERVAL_MS);
        assert_eq!(MIN_FETCH_POLL_INTERVAL_MS, 10);
        assert_eq!(MAX_FETCH_POLL_INTERVAL_MS, 5000);
    }

    #[test]
    fn test_config_reload_interval_range() {
        assert!(MIN_CONFIG_RELOAD_MS <= DEFAULT_CONFIG_RELOAD_MS);
        assert!(DEFAULT_CONFIG_RELOAD_MS <= MAX_CONFIG_RELOAD_MS);
        assert_eq!(MIN_CONFIG_RELOAD_MS, 100);
        assert_eq!(MAX_CONFIG_RELOAD_MS, 300000);
    }

    #[test]
    fn test_shutdown_timeout_minimum() {
        assert!(MIN_SHUTDOWN_TIMEOUT_MS > 0);
        assert_eq!(MIN_SHUTDOWN_TIMEOUT_MS, 100);
    }

    // ========== Config Struct Tests ==========

    #[test]
    fn test_config_all_fields_accessible() {
        let config = Config::load();
        // Ensure all fields are accessible and have expected types
        let _port: i32 = config.port;
        let _host: String = config.host;
        let _database: String = config.database;
        let _log_connections: bool = config.log_connections;
        let _shutdown_timeout_ms: i32 = config.shutdown_timeout_ms;
        let _default_partitions: i32 = config.default_partitions;
        let _fetch_poll_interval_ms: i32 = config.fetch_poll_interval_ms;
        let _enable_long_polling: bool = config.enable_long_polling;
        let _compression_type: String = config.compression_type;
        let _log_timing: bool = config.log_timing;
        let _shadow_mode_enabled: bool = config.shadow_mode_enabled;
        let _shadow_bootstrap_servers: String = config.shadow_bootstrap_servers;
        let _shadow_security_protocol: String = config.shadow_security_protocol;
        let _shadow_sasl_mechanism: String = config.shadow_sasl_mechanism;
        let _shadow_sasl_username: String = config.shadow_sasl_username;
        let _shadow_sasl_password: String = config.shadow_sasl_password;
        let _shadow_ssl_ca_location: String = config.shadow_ssl_ca_location;
        let _shadow_batch_size: i32 = config.shadow_batch_size;
        let _shadow_linger_ms: i32 = config.shadow_linger_ms;
        let _shadow_retry_backoff_ms: i32 = config.shadow_retry_backoff_ms;
        let _shadow_max_retries: i32 = config.shadow_max_retries;
        let _shadow_default_sync_mode: String = config.shadow_default_sync_mode;
        let _shadow_metrics_enabled: bool = config.shadow_metrics_enabled;
        let _shadow_otel_endpoint: String = config.shadow_otel_endpoint;
        let _shadow_license_key: String = config.shadow_license_key;
    }

    #[test]
    fn test_config_multiple_loads_consistent() {
        let config1 = Config::load();
        let config2 = Config::load();
        assert_eq!(config1.port, config2.port);
        assert_eq!(config1.host, config2.host);
        assert_eq!(config1.shadow_mode_enabled, config2.shadow_mode_enabled);
        assert_eq!(config1.shadow_batch_size, config2.shadow_batch_size);
    }
}
