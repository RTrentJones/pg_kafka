// Configuration module for pg_kafka
//
// This module defines GUC (Grand Unified Configuration) parameters that can be
// set in postgresql.conf or via ALTER SYSTEM/SET commands.

use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting};
use std::ffi::CStr;

/// Configuration for the pg_kafka extension
///
/// These settings can be configured in postgresql.conf:
/// ```
/// pg_kafka.port = 9092
/// pg_kafka.host = '0.0.0.0'
/// pg_kafka.log_connections = true
/// ```
pub struct Config {
    /// Port to listen on for Kafka protocol connections
    pub port: i32,

    /// Host/interface to bind to (0.0.0.0 = all interfaces)
    pub host: String,

    /// Whether to log each connection (can be noisy under load)
    pub log_connections: bool,

    /// Timeout in milliseconds for graceful shutdown
    pub shutdown_timeout_ms: i32,
}

impl Config {
    /// Get the current configuration from GUCs
    pub fn load() -> Self {
        Self {
            port: PORT.get(),
            host: HOST
                .get()
                .map(|cstr: &CStr| cstr.to_str().unwrap_or("0.0.0.0").to_string())
                .unwrap_or_else(|| "0.0.0.0".to_string()),
            log_connections: LOG_CONNECTIONS.get(),
            shutdown_timeout_ms: SHUTDOWN_TIMEOUT_MS.get(),
        }
    }
}

// GUC definitions - these are the actual configuration parameters
// They are initialized in init() which is called from _PG_init()

static PORT: GucSetting<i32> = GucSetting::<i32>::new(9092);
static HOST: GucSetting<Option<&'static CStr>> = GucSetting::<Option<&'static CStr>>::new(None);
static LOG_CONNECTIONS: GucSetting<bool> = GucSetting::<bool>::new(false);
static SHUTDOWN_TIMEOUT_MS: GucSetting<i32> = GucSetting::<i32>::new(5000);

/// Initialize GUC parameters
///
/// This must be called from _PG_init() to register the parameters with Postgres.
/// After registration, they can be set in postgresql.conf or via SQL commands.
pub fn init() {
    // Network configuration
    GucRegistry::define_int_guc(
        "pg_kafka.port",
        "Port to listen on for Kafka protocol connections",
        "The TCP port that pg_kafka will bind to. Requires restart to take effect.",
        &PORT,
        1024,      // min: privileged ports require root
        65535,     // max: valid port range
        GucContext::Postmaster, // Requires restart to change
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        "pg_kafka.host",
        "Host/interface to bind to",
        "The network interface to listen on. Use 0.0.0.0 for all interfaces, \
         127.0.0.1 for localhost only. Requires restart to take effect.",
        &HOST,
        GucContext::Postmaster, // Requires restart to change
        GucFlags::default(),
    );

    // Observability configuration
    GucRegistry::define_bool_guc(
        "pg_kafka.log_connections",
        "Whether to log each connection",
        "When enabled, logs every incoming connection. Can be very noisy under high load. \
         Can be changed at runtime with SET or in postgresql.conf.",
        &LOG_CONNECTIONS,
        GucContext::Suset, // Superuser can change without restart
        GucFlags::default(),
    );

    // Performance configuration
    GucRegistry::define_int_guc(
        "pg_kafka.shutdown_timeout_ms",
        "Timeout for graceful shutdown in milliseconds",
        "How long to wait for the TCP listener to shut down cleanly before forcing termination.",
        &SHUTDOWN_TIMEOUT_MS,
        100,       // min: 100ms
        60000,     // max: 60 seconds
        GucContext::Suset, // Can be changed without restart
        GucFlags::default(),
    );

    pgrx::log!("pg_kafka GUC parameters initialized");
}
