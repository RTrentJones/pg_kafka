use pgrx::bgworkers::BackgroundWorkerBuilder;
use pgrx::prelude::*;
use std::sync::atomic::{AtomicBool, Ordering};

// Module declarations for our extension components
mod config; // Configuration (GUC parameters)
pub mod kafka; // Kafka protocol implementation (listener, protocol, messages)
pub mod worker; // Background worker implementation

// Test utilities (only compiled in test builds)
#[cfg(test)]
pub mod testing;

// ===== Shadow Config Reload Trigger =====
/// Flag to trigger immediate shadow config reload from SQL
/// Set by kafka.reload_shadow_config() SQL function
/// Checked by worker loop on every iteration (~100ms)
pub static RELOAD_SHADOW_CONFIG_REQUESTED: AtomicBool = AtomicBool::new(false);

// ===== Conditional Logging Macros =====
// These provide test-safe alternatives to pgrx logging functions

/// Production logging - uses pgrx::log!()
#[cfg(not(test))]
#[macro_export]
macro_rules! pg_log {
    ($($arg:tt)*) => { pgrx::log!($($arg)*) };
}

/// Test logging - consumes args to avoid unused variable warnings
#[cfg(test)]
#[macro_export]
macro_rules! pg_log {
    ($($arg:tt)*) => {
        // Consume args to avoid unused variable warnings in test mode
        // Uncomment for test debugging:
        // eprintln!("[LOG] {}", format!($($arg)*));
        let _ = format!($($arg)*);
    };
}

/// Production warning - uses pgrx::warning!()
#[cfg(not(test))]
#[macro_export]
macro_rules! pg_warning {
    ($($arg:tt)*) => { pgrx::warning!($($arg)*) };
}

/// Test warning - consumes args to avoid unused variable warnings
#[cfg(test)]
#[macro_export]
macro_rules! pg_warning {
    ($($arg:tt)*) => {
        // Consume args to avoid unused variable warnings in test mode
        // Uncomment for test debugging:
        // eprintln!("[WARNING] {}", format!($($arg)*));
        let _ = format!($($arg)*);
    };
}

/// Production debug logging - uses pgrx::debug1!()
#[cfg(not(test))]
#[macro_export]
macro_rules! pg_debug {
    ($($arg:tt)*) => { pgrx::debug1!($($arg)*) };
}

/// Test debug logging - consumes args to avoid unused variable warnings
#[cfg(test)]
#[macro_export]
macro_rules! pg_debug {
    ($($arg:tt)*) => {
        // Consume args to avoid unused variable warnings in test mode
        // Uncomment for test debugging:
        // eprintln!("[DEBUG] {}", format!($($arg)*));
        let _ = format!($($arg)*);
    };
}

::pgrx::pg_module_magic!();

/// Extension initialization hook - called exactly once when Postgres loads the extension.
///
/// This function is called by PostgreSQL when the extension is loaded via the
/// `shared_preload_libraries` configuration parameter in postgresql.conf.
///
/// IMPORTANT: Background workers MUST be registered in _PG_init(). They cannot
/// be registered later (e.g., in CREATE EXTENSION) because Postgres needs to
/// know about them before the postmaster has finished starting up.
///
/// For more details, see:
/// - https://www.postgresql.org/docs/current/bgworker.html
/// - https://docs.rs/pgrx/latest/pgrx/bgworkers/index.html
#[pg_guard]
pub unsafe extern "C-unwind" fn _PG_init() {
    use pgrx::bgworkers::BgWorkerStartTime;

    pgrx::log!("pg_kafka: _PG_init() called - extension loading");

    // Initialize GUC configuration parameters
    config::init();
    pgrx::log!("pg_kafka: GUC configuration initialized");

    // Register the pg_kafka background worker with PostgreSQL.
    //
    // BackgroundWorkerBuilder configures how Postgres should manage our worker:
    // - name: Identifier shown in pg_stat_activity
    // - function: The Rust function to run (entry point)
    // - library: Must match the shared library name (pg_kafka.so)
    // - start_time: When to start (PostmasterStart = at server startup)
    //
    // enable_spi_access() allows the worker to connect to the database and
    // execute SQL queries (we'll use this in Phase 2).
    pgrx::log!("pg_kafka: Registering background worker 'pg_kafka_listener'");
    BackgroundWorkerBuilder::new("pg_kafka_listener")
        .set_function("pg_kafka_listener_main")
        .set_library("pg_kafka")
        .set_start_time(BgWorkerStartTime::PostmasterStart)
        .enable_spi_access()
        .load();

    pgrx::log!("pg_kafka: Background worker registered successfully");
    pgrx::log!("pg_kafka: _PG_init() completed");
}

#[pg_extern]
fn hello_pg_kafka() -> &'static str {
    "Hello, pg_kafka"
}

/// Trigger immediate shadow mode configuration reload
///
/// This function sets a flag that the background worker checks on every
/// loop iteration. When set, the worker immediately reloads shadow mode
/// configuration from kafka.shadow_config table instead of waiting for
/// the next periodic reload (normally 30 seconds).
///
/// This is primarily useful for testing, where waiting 30+ seconds for
/// config changes to take effect is prohibitive.
///
/// Returns the approximate number of milliseconds until config will be reloaded.
///
/// # Example
/// ```sql
/// -- Update shadow config
/// UPDATE kafka.shadow_config SET forward_percentage = 100
/// WHERE topic_id = 1;
///
/// -- Trigger immediate reload (no wait needed)
/// SELECT reload_shadow_config();
///
/// -- Config is now active within ~100ms
/// ```
#[pg_extern]
fn reload_shadow_config() -> i32 {
    RELOAD_SHADOW_CONFIG_REQUESTED.store(true, Ordering::Relaxed);
    pgrx::log!("Shadow config reload requested via SQL function");
    100 // Approximate milliseconds until reload
}

// Include bootstrap SQL to create kafka schema and tables
pgrx::extension_sql_file!("../sql/bootstrap.sql");

// #[cfg(any(test, feature = "pg_test")]]
// #[pg_schema]
// mod tests {
//     use pgrx::prelude::*;

//     #[pg_test]
//     fn test_hello_pg_kafka() {
//         assert_eq!("Hello, pg_kafka", crate::hello_pg_kafka());
//     }
// }

/// This module is required by `cargo pgrx test` invocations.
/// It must be visible at the root of your extension crate.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}
