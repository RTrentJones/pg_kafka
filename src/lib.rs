//! pg_kafka: a PostgreSQL extension that serves the Kafka wire protocol from a background
//! worker, with Postgres tables as the log.
//!
//! This crate root holds the module tree, the logging macros the DB thread uses, the
//! `_PG_init` hook that registers the GUCs and the worker, the SQL-callable functions
//! defined here, and the inclusion of the bootstrap schema.

use pgrx::bgworkers::BackgroundWorkerBuilder;
use pgrx::prelude::*;

mod config;
pub mod kafka;
mod worker;

#[cfg(test)]
pub mod testing;

/// Log at LOG level through `pgrx::log!`. In unit tests (no Postgres to log to) the
/// arguments are only formatted, so they stay type-checked and used.
#[macro_export]
macro_rules! pg_log {
    ($($arg:tt)*) => {{
        #[cfg(not(test))]
        pgrx::log!($($arg)*);
        #[cfg(test)]
        let _ = ::std::format!($($arg)*);
    }};
}

/// Log a WARNING through `pgrx::warning!` (only formatted in unit tests, see
/// [`pg_log!`]).
#[macro_export]
macro_rules! pg_warning {
    ($($arg:tt)*) => {{
        #[cfg(not(test))]
        pgrx::warning!($($arg)*);
        #[cfg(test)]
        let _ = ::std::format!($($arg)*);
    }};
}

/// Log at DEBUG1 through `pgrx::debug1!` (only formatted in unit tests, see
/// [`pg_log!`]).
#[macro_export]
macro_rules! pg_debug {
    ($($arg:tt)*) => {{
        #[cfg(not(test))]
        pgrx::debug1!($($arg)*);
        #[cfg(test)]
        let _ = ::std::format!($($arg)*);
    }};
}

::pgrx::pg_module_magic!();

/// Extension initialization hook, called once when Postgres loads the library through
/// `shared_preload_libraries`.
///
/// Background workers must be registered here: the postmaster needs to know them
/// before it has finished starting, so `CREATE EXTENSION` is too late. See
/// <https://www.postgresql.org/docs/current/bgworker.html>.
#[pg_guard]
pub unsafe extern "C-unwind" fn _PG_init() {
    use pgrx::bgworkers::BgWorkerStartTime;

    pgrx::log!("pg_kafka: _PG_init() called - extension loading");
    config::init();
    pgrx::log!("pg_kafka: GUC configuration initialized");

    // - start_time RecoveryFinished: the worker needs a database connection (SPI),
    //   which Postgres allows only once recovery has finished (pgrx's
    //   enable_spi_access() forces this start time anyway). So the listener does not
    //   run on a hot standby: it starts on a primary, or on a standby once promoted.
    // - restart_time: if the worker exits for any reason other than a clean unregister
    //   (exit code 0) (a FATAL error, a failed bind, a caught commit failure, or
    //   `pg_terminate_backend()` on its pid), the postmaster starts a fresh one after
    //   WORKER_RESTART_INTERVAL, so the Kafka port does not stay dead until Postgres
    //   restarts. The worker exits with code 1 on SIGTERM for this reason; during a
    //   server shutdown the postmaster does not restart workers.
    pgrx::log!("pg_kafka: Registering background worker 'pg_kafka_listener'");
    BackgroundWorkerBuilder::new("pg_kafka_listener")
        .set_function("pg_kafka_listener_main")
        .set_library("pg_kafka")
        .set_start_time(BgWorkerStartTime::RecoveryFinished)
        .set_restart_time(Some(worker::WORKER_RESTART_INTERVAL))
        .enable_spi_access()
        .load();

    pgrx::log!("pg_kafka: Background worker registered successfully");
    pgrx::log!("pg_kafka: _PG_init() completed");
}

/// Smoke test: `SELECT hello_pg_kafka()` answers when the library is loaded.
#[pg_extern]
fn hello_pg_kafka() -> &'static str {
    "Hello, pg_kafka"
}

/// DR-1/DR-2: run one storage-lifecycle retention sweep on demand and report what it
/// deleted. The background worker runs the identical sweep periodically; this function
/// exists for operators (reclaim space now, observe what a sweep would reap) and for the
/// E2E suite, which cannot wait out the worker's sweep interval.
///
/// * `message_retention_hours`: NULL (default) uses `pg_kafka.message_retention_hours`;
///   0 disables the expired-message delete for this pass.
/// * `aborted_grace_seconds`: minimum age of `txn_state='aborted'` rows to reclaim
///   (the default matches the worker's `ABORTED_MESSAGE_GRACE`; negative means 0).
///
/// Sweeps and partition maintenance are serialized: a call first waits for the
/// partition-maintenance lock (bounded by the session's `lock_timeout`), so a
/// concurrent pass (another session's, or the worker's) finishes first. Call it in a
/// transaction of its own: a transaction that already locked kafka.messages could
/// deadlock with the pass it waits for.
#[pg_extern(volatile)]
fn pg_kafka_run_retention_sweep(
    message_retention_hours: default!(Option<i32>, "NULL"),
    aborted_grace_seconds: default!(i64, 60),
) -> TableIterator<'static, (name!(category, String), name!(deleted, i64))> {
    use crate::kafka::storage::postgres::PostgresStore;
    use crate::kafka::storage::retention::MaintenanceLockWait;

    let retention_hours =
        message_retention_hours.unwrap_or_else(|| config::MESSAGE_RETENTION_HOURS.get());
    let grace = std::time::Duration::from_secs(u64::try_from(aborted_grace_seconds).unwrap_or(0));

    let stats = PostgresStore::new()
        .run_retention_sweep(
            retention_hours,
            grace,
            config::MESSAGE_PARTITION_MINUTES.get(),
            MaintenanceLockWait::Wait,
        )
        .unwrap_or_else(|e| pgrx::error!("pg_kafka_run_retention_sweep failed: {}", e))
        .unwrap_or_default();

    let rows = [
        ("aborted_messages", stats.aborted_messages),
        ("expired_messages", stats.expired_messages),
        ("dropped_partitions", stats.dropped_partitions),
        ("stale_producers", stats.stale_producers),
        ("terminal_transactions", stats.terminal_transactions),
        ("shadow_delivered_rows", stats.shadow_delivered_rows),
    ];
    TableIterator::new(rows.map(|(category, deleted)| {
        (
            category.to_string(),
            i64::try_from(deleted).unwrap_or(i64::MAX),
        )
    }))
}

/// ADR-001: run one time-partition maintenance pass on demand: create the leaves of
/// kafka.messages for now and the next two widths, and move any row the DEFAULT leaf
/// caught into its proper leaf. The worker runs the same pass at start and every
/// retention interval. Returns the number of leaves created.
///
/// * `partition_minutes`: NULL (default) uses `pg_kafka.message_partition_minutes`.
#[pg_extern(volatile)]
fn pg_kafka_run_partition_maintenance(partition_minutes: default!(Option<i32>, "NULL")) -> i64 {
    use crate::kafka::storage::postgres::PostgresStore;

    let minutes = partition_minutes.unwrap_or_else(|| config::MESSAGE_PARTITION_MINUTES.get());
    PostgresStore::new()
        .ensure_time_partitions(minutes)
        .unwrap_or_else(|e| pgrx::error!("pg_kafka_run_partition_maintenance failed: {}", e))
}

// The kafka schema, tables, roles and functions CREATE EXTENSION installs.
pgrx::extension_sql_file!("../sql/bootstrap.sql");

/// Required by `cargo pgrx test` at the crate root (the extension's tests run as E2E
/// tests instead, see docs/PGRX_TESTING_GUIDE.md).
#[cfg(test)]
pub mod pg_test {
    /// One-off initialization when the pg_test framework starts (none needed).
    pub fn setup(_options: Vec<&str>) {}

    /// postgresql.conf settings the pg_test framework needs (none).
    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
