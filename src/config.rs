// Configuration module for pg_kafka
//
// This module defines GUC (Grand Unified Configuration) parameters that can be
// set in postgresql.conf or via SQL commands.
//
// pgrx 0.16+ uses declarative macros for GUC definitions

use pgrx::prelude::*;
use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting};

/// Configuration struct holding all pg_kafka settings
pub struct Config {
    pub port: i32,
    pub host: String,
    pub log_connections: bool,
    pub shutdown_timeout_ms: i32,
}

impl Config {
    /// Load configuration from GUC parameters
    pub fn load() -> Self {
        Config {
            port: PORT.get(),
            host: HOST
                .get()
                .as_deref()
                .map(|c| c.to_string_lossy().into_owned())
                .unwrap_or_else(|| "0.0.0.0".to_string()),
            log_connections: LOG_CONNECTIONS.get(),
            shutdown_timeout_ms: SHUTDOWN_TIMEOUT_MS.get(),
        }
    }
}

// GUC parameter definitions
use std::ffi::CString;

static PORT: GucSetting<i32> = GucSetting::<i32>::new(9092);
static HOST: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
static LOG_CONNECTIONS: GucSetting<bool> = GucSetting::<bool>::new(false);
static SHUTDOWN_TIMEOUT_MS: GucSetting<i32> = GucSetting::<i32>::new(5000);

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
        100,
        60000,
        GucContext::Suset,
        GucFlags::default(),
    );

    pgrx::log!("pg_kafka GUC parameters initialized");
}
