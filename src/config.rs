// Configuration module for pg_kafka
//
// This module defines GUC (Grand Unified Configuration) parameters that can be
// set in postgresql.conf or via SQL commands.
//
// pgrx 0.16+ uses declarative macros for GUC definitions

use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting};

use crate::kafka::constants::{
    DEFAULT_COMPRESSION_TYPE,
    DEFAULT_DATABASE,
    DEFAULT_FETCH_POLL_INTERVAL_MS,
    DEFAULT_KAFKA_PORT,
    // Shadow mode constants (Phase 11)
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
    MAX_FETCH_POLL_INTERVAL_MS,
    MAX_SHADOW_BATCH_SIZE,
    MAX_SHADOW_LINGER_MS,
    MAX_SHADOW_MAX_RETRIES,
    MAX_SHADOW_RETRY_BACKOFF_MS,
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

    pgrx::log!("pg_kafka GUC parameters initialized (including shadow mode)");
}
