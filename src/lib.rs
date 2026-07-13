use pgrx::bgworkers::BackgroundWorkerBuilder;
use pgrx::prelude::*;

// Module declarations for our extension components
mod config; // Configuration (GUC parameters)
pub mod kafka; // Kafka protocol implementation (listener, protocol, messages)
pub mod worker; // Background worker implementation

// Test utilities (only compiled in test builds)
#[cfg(test)]
pub mod testing;

// Note: Previously had RELOAD_SHADOW_CONFIG_REQUESTED atomic flag here.
// Removed because PostgreSQL uses separate processes (not threads), so static
// variables aren't shared between the SQL function process and worker process.
// Use shadow_config_reload_interval_ms GUC instead for testing.

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

/// DR-1/DR-2 (DEEP-REVIEW-2026-07): run one storage-lifecycle retention sweep on
/// demand and report what it deleted. The background worker runs the identical
/// sweep periodically; this function exists for operators (reclaim space now,
/// observe what a sweep would reap) and for the E2E suite, which can't wait out
/// the worker's sweep interval.
///
/// * `message_retention_hours` — NULL (default) uses `pg_kafka.message_retention_hours`;
///   0 disables the expired-message delete for this pass.
/// * `aborted_grace_seconds` — minimum age of `txn_state='aborted'` rows to reclaim
///   (default matches the worker's ABORTED_MESSAGE_GRACE).
#[pg_extern(volatile)]
fn pg_kafka_run_retention_sweep(
    message_retention_hours: default!(Option<i32>, "NULL"),
    aborted_grace_seconds: default!(i64, 60),
) -> TableIterator<'static, (name!(category, String), name!(deleted, i64))> {
    use crate::kafka::storage::postgres::PostgresStore;

    let retention_hours =
        message_retention_hours.unwrap_or_else(|| config::MESSAGE_RETENTION_HOURS.get());
    let grace = std::time::Duration::from_secs(aborted_grace_seconds.max(0) as u64);

    let store = PostgresStore::new();
    let stats = store
        .run_retention_sweep(retention_hours, grace)
        .unwrap_or_else(|e| pgrx::error!("pg_kafka_run_retention_sweep failed: {}", e));

    TableIterator::new(vec![
        (
            "aborted_messages".to_string(),
            stats.aborted_messages as i64,
        ),
        (
            "expired_messages".to_string(),
            stats.expired_messages as i64,
        ),
        ("stale_producers".to_string(), stats.stale_producers as i64),
        (
            "terminal_transactions".to_string(),
            stats.terminal_transactions as i64,
        ),
        (
            "shadow_delivered_rows".to_string(),
            stats.shadow_delivered_rows as i64,
        ),
    ])
}

// Note: Removed reload_shadow_config() SQL function.
// It didn't work because PostgreSQL uses separate processes, not threads.
// Static variables aren't shared between backend and worker processes.
// Use shadow_config_reload_interval_ms GUC for fast reloads during testing.

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
