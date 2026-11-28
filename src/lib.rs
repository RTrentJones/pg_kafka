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

/// Log at LOG level through `pgrx::log!`, with interrupts held ([`InterruptsHeld`]). In unit
/// tests (no Postgres to log to) the arguments are only formatted, so they stay type-checked
/// and used.
///
/// Every log line of the extension goes through this macro, [`pg_warning!`] or
/// [`pg_debug!`] (clippy's `disallowed_macros` refuses pgrx's own logging macros): Postgres's
/// `errfinish` runs CHECK_FOR_INTERRUPTS after writing a message below ERROR, and pgrx calls it
/// without an error boundary, so a pending query cancel (`pg_cancel_backend()`) would raise its
/// ERROR from inside the macro and longjmp past the Rust frames of the caller (their
/// destructors, lock guards included, never run, and in the worker the ERROR reaches the
/// top-level handler, which exits). With interrupts held the cancel stays pending and is
/// served at the next CHECK_FOR_INTERRUPTS outside the macro, which in the worker is inside an
/// error boundary or discarded between transactions (`worker::txn::discard_idle_cancel`).
#[macro_export]
macro_rules! pg_log {
    ($($arg:tt)*) => {{
        #[cfg(not(test))]
        {
            let _held = $crate::InterruptsHeld::hold();
            #[expect(clippy::disallowed_macros, reason = "the one sanctioned use, interrupts held")]
            let () = pgrx::log!($($arg)*);
        }
        #[cfg(test)]
        let _ = ::std::format!($($arg)*);
    }};
}

/// Log a WARNING through `pgrx::warning!`, with interrupts held (see [`pg_log!`]; only
/// formatted in unit tests).
#[macro_export]
macro_rules! pg_warning {
    ($($arg:tt)*) => {{
        #[cfg(not(test))]
        {
            let _held = $crate::InterruptsHeld::hold();
            #[expect(clippy::disallowed_macros, reason = "the one sanctioned use, interrupts held")]
            let () = pgrx::warning!($($arg)*);
        }
        #[cfg(test)]
        let _ = ::std::format!($($arg)*);
    }};
}

/// Log at DEBUG1 through `pgrx::debug1!`, with interrupts held (see [`pg_log!`]; only
/// formatted in unit tests). The message is formatted only when DEBUG1 goes anywhere
/// ([`debug1_is_logged`]): `pgrx::debug1!` formats it before Postgres decides to drop it, and
/// with the default `log_min_messages` (WARNING) every call would otherwise cost the
/// formatting of arguments that may hold client strings of up to 32 KB.
#[macro_export]
macro_rules! pg_debug {
    ($($arg:tt)*) => {{
        #[cfg(not(test))]
        if $crate::debug1_is_logged() {
            let _held = $crate::InterruptsHeld::hold();
            #[expect(clippy::disallowed_macros, reason = "the one sanctioned use, interrupts held")]
            let () = pgrx::debug1!($($arg)*);
        }
        #[cfg(test)]
        let _ = ::std::format!($($arg)*);
    }};
}

/// Postgres's HOLD_INTERRUPTS() for as long as the value lives, RESUME_INTERRUPTS() when it is
/// dropped (also when the code it guards panics). While held, CHECK_FOR_INTERRUPTS leaves a
/// pending cancel or termination request pending instead of raising it. The logging macros
/// ([`pg_log!`]) hold interrupts around each message.
#[doc(hidden)]
#[cfg(not(test))]
pub struct InterruptsHeld(());

#[cfg(not(test))]
impl InterruptsHeld {
    /// HOLD_INTERRUPTS(). Only on the backend's main thread (where pgrx's logging macros run).
    pub fn hold() -> Self {
        // SAFETY: InterruptHoldoffCount is a plain counter of this backend, read and written
        // only by its main thread (the caller's contract); signal handlers do not touch it.
        unsafe {
            let count = &raw mut pg_sys::InterruptHoldoffCount;
            count.write_volatile(count.read_volatile().saturating_add(1));
        }
        Self(())
    }
}

#[cfg(not(test))]
impl Drop for InterruptsHeld {
    fn drop(&mut self) {
        // SAFETY: as in `hold`. Saturating: an ERROR raised while the count was held resets it
        // to 0 (errfinish does), and the count must not wrap to "held forever".
        unsafe {
            let count = &raw mut pg_sys::InterruptHoldoffCount;
            count.write_volatile(count.read_volatile().saturating_sub(1));
        }
    }
}

/// Whether a DEBUG1 message would be written to the server log or sent to a client (the
/// test [`pg_debug!`] runs first). Two comparisons of GUC values; like every `pg_debug!`, it
/// runs on the backend's main thread.
#[doc(hidden)]
#[cfg(not(test))]
pub fn debug1_is_logged() -> bool {
    #[cfg(not(feature = "pg13"))]
    {
        // SAFETY: reads the log_min_messages / client_min_messages globals and the output
        // destination; no allocation and no error path. pg_debug! runs only on the main
        // thread (pgrx refuses Postgres calls from others, debug1! included).
        unsafe {
            pg_sys::message_level_is_interesting(i32::try_from(pg_sys::DEBUG1).unwrap_or(i32::MAX))
        }
    }
    // PostgreSQL 13 has no message_level_is_interesting: format as before.
    #[cfg(feature = "pg13")]
    true
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

    pg_log!("pg_kafka: _PG_init() called - extension loading");
    config::init();
    pg_log!("pg_kafka: GUC configuration initialized");

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
    pg_log!("pg_kafka: Registering background worker 'pg_kafka_listener'");
    BackgroundWorkerBuilder::new("pg_kafka_listener")
        .set_function("pg_kafka_listener_main")
        .set_library("pg_kafka")
        .set_start_time(BgWorkerStartTime::RecoveryFinished)
        .set_restart_time(Some(worker::WORKER_RESTART_INTERVAL))
        .enable_spi_access()
        .load();

    pg_log!("pg_kafka: Background worker registered successfully");
    pg_log!("pg_kafka: _PG_init() completed");
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
/// deadlock with the pass it waits for. Only a READ COMMITTED call drops time
/// partitions: under REPEATABLE READ or SERIALIZABLE the snapshot predates the drop's
/// lock wait, so the pass skips its drops with a WARNING and does the rest.
/// Produce, InitProducerId and EndTxn are not serialized with it: a statement of the pass
/// that waits for one of their rows decides on the committed row (ADR-001, "A manual
/// sweep decides on committed rows"); at REPEATABLE READ or SERIALIZABLE such a wait
/// fails the call with a serialization error instead.
///
/// Superusers only: EXECUTE is revoked from PUBLIC (`sql/bootstrap.sql`), since a caller
/// that waits here holds the extension lock, and any role can hold the maintenance lock.
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

    lock_extension_for_maintenance("pg_kafka_run_retention_sweep");
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
        ("shadow_orphaned_rows", stats.shadow_orphaned_rows),
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
///
/// Like the sweep, call it in a transaction of its own: it takes the extension lock
/// first ([`lock_extension_for_maintenance`]), which only helps while it holds no member
/// table yet. Superusers only, like the sweep (EXECUTE is revoked from PUBLIC).
#[pg_extern(volatile)]
fn pg_kafka_run_partition_maintenance(partition_minutes: default!(Option<i32>, "NULL")) -> i64 {
    use crate::kafka::storage::postgres::PostgresStore;

    let minutes = partition_minutes.unwrap_or_else(|| config::MESSAGE_PARTITION_MINUTES.get());
    lock_extension_for_maintenance("pg_kafka_run_partition_maintenance");
    PostgresStore::new()
        .ensure_time_partitions(minutes)
        .unwrap_or_else(|e| pgrx::error!("pg_kafka_run_partition_maintenance failed: {}", e))
}

/// The first lock of the SQL maintenance functions, before any member table: the share
/// lock on the extension object every worker transaction also starts with
/// (`worker::txn::lock_extension`). DROP EXTENSION locks the extension and then its
/// members, and the partition DDL runs ALTER EXTENSION ... ADD/DROP TABLE, which locks
/// the extension too; a pass that already held kafka.messages when DROP took the
/// extension would wait for DROP (for kafka.topics, or the extension itself) while DROP
/// waited for kafka.messages: "deadlock detected" in one of them. Taken first, the
/// pass waits for a DROP in progress (bounded by the session's `lock_timeout`) or makes
/// DROP wait for it. A pass whose extension was dropped meanwhile fails.
fn lock_extension_for_maintenance(function: &str) {
    if worker::txn::lock_extension().is_none() {
        pgrx::error!("{function}: the pg_kafka extension was dropped");
    }
}

// The kafka schema, tables, roles and functions CREATE EXTENSION installs. It requires
// the SQL maintenance functions, so pgrx creates them first: the file ends by revoking
// EXECUTE on them from PUBLIC.
pgrx::extension_sql_file!(
    "../sql/bootstrap.sql",
    requires = [
        pg_kafka_run_retention_sweep,
        pg_kafka_run_partition_maintenance
    ]
);

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
