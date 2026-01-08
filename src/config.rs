// Configuration module for pg_kafka
//
// This module defines GUC (Grand Unified Configuration) parameters that can be
// set in postgresql.conf or via SQL commands.
//
// pgrx 0.16+ uses declarative macros for GUC definitions

use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting};

use crate::kafka::constants::{
    DEFAULT_COMPRESSION_TYPE, DEFAULT_DATABASE, DEFAULT_FETCH_POLL_INTERVAL_MS, DEFAULT_KAFKA_PORT,
    DEFAULT_SHUTDOWN_TIMEOUT_MS, DEFAULT_TOPIC_PARTITIONS, MAX_FETCH_POLL_INTERVAL_MS,
    MIN_FETCH_POLL_INTERVAL_MS, MIN_SHUTDOWN_TIMEOUT_MS,
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

    pgrx::log!("pg_kafka GUC parameters initialized");
}
