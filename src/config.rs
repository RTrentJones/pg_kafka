// Configuration module for pg_kafka
//
// This module defines GUC (Grand Unified Configuration) parameters that can be
// set in postgresql.conf or via SQL commands.
//
// pgrx 0.16+ uses declarative macros for GUC definitions

use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting};

use crate::kafka::constants::{
    DEFAULT_DATABASE, DEFAULT_HOST, DEFAULT_KAFKA_PORT, DEFAULT_SHUTDOWN_TIMEOUT_MS,
    MIN_SHUTDOWN_TIMEOUT_MS,
};

#[cfg(test)]
use crate::kafka::constants::TEST_HOST;

/// Configuration struct holding all pg_kafka settings
pub struct Config {
    pub port: i32,
    pub host: String,
    pub database: String,
    pub log_connections: bool,
    pub shutdown_timeout_ms: i32,
    pub database: String,
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
            database: DATABASE
                .get()
                .as_deref()
                .map(|c| c.to_string_lossy().into_owned())
                .unwrap_or_else(|| "postgres".to_string()),
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
            database: "postgres".to_string(),
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
static DATABASE: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);

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

    GucRegistry::define_string_guc(
        c"pg_kafka.database",
        c"Database to connect to for SPI operations",
        c"The database name for storage operations. Defaults to 'postgres'.",
        &DATABASE,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    pgrx::log!("pg_kafka GUC parameters initialized");
}
