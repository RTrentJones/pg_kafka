//! Worker transactions: the schema locks every transaction of the main loop starts with,
//! recovery from a failed top-level transaction, and the subtransaction guard each request
//! and maintenance task runs in.

use pgrx::bgworkers::BackgroundWorker;
use pgrx::pg_sys::panic::CaughtError;
use pgrx::prelude::*;
use std::any::Any;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use super::lifecycle::{begin_db_work, end_db_work};
use crate::kafka::dispatch::RequestOutcome;
use crate::kafka::{ERROR_KAFKA_STORAGE_ERROR, ERROR_REQUEST_TIMED_OUT};
use crate::pg_warning;

/// Key of the pg_kafka schema lock: a transaction-level advisory lock every worker
/// transaction takes in SHARE mode right after the extension lock. Whatever must not
/// overlap any worker transaction takes it EXCLUSIVE: moving rows out of
/// kafka.messages_default (`kafka.create_message_partition`) and the upgrade scripts.
/// (8099547369649367905, one less, is the partition-maintenance lock.)
const SCHEMA_LOCK_KEY: i64 = 8_099_547_369_649_367_906;

/// Set when a worker transaction found the extension gone (DROP EXTENSION); the main
/// loop then goes back to waiting for the schema.
static SCHEMA_LOST: AtomicBool = AtomicBool::new(false);

/// OID of the pg_kafka extension the worker initialised against (0 = not yet known;
/// the next [`lock_schema`] records it). A different OID means the extension was
/// dropped and re-created between two worker transactions (possibly in one DDL
/// transaction the worker never saw "gone"): that is treated like a lost schema, so
/// the worker re-initialises (features, role, maintenance) for the new one.
static KNOWN_EXTENSION_OID: AtomicU32 = AtomicU32::new(0);

extern "C" {
    // Exported by the postgres executable (timeout.c); not in pgrx's bindings.
    fn disable_all_timeouts(keep_indicators: bool);
}

/// Whether a worker transaction found the extension gone since the last
/// [`schema_found_again`].
pub(super) fn schema_lost() -> bool {
    SCHEMA_LOST.load(Ordering::SeqCst)
}

/// The schema exists again after it was lost: the next worker transaction records the
/// (new) extension's OID.
pub(super) fn schema_found_again() {
    KNOWN_EXTENSION_OID.store(0, Ordering::SeqCst);
    SCHEMA_LOST.store(false, Ordering::SeqCst);
}

/// Why a worker transaction did not run or commit.
#[derive(Debug)]
pub(super) enum TxnError {
    /// The extension is gone (DROP EXTENSION); nothing ran.
    SchemaGone,
    /// A Postgres ERROR outside any request's subtransaction: taking the schema locks
    /// (lock_timeout), or the COMMIT itself. The transaction was rolled back.
    Failed { message: String, lock_timeout: bool },
}

impl TxnError {
    /// The Kafka error code for requests of a batch that failed this way.
    pub(super) fn kafka_error_code(&self) -> i16 {
        match self {
            TxnError::Failed {
                lock_timeout: true, ..
            } => ERROR_REQUEST_TIMED_OUT,
            _ => ERROR_KAFKA_STORAGE_ERROR,
        }
    }
}

/// The advisory LOCKTAG `pg_advisory_xact_lock_shared(key)` takes in database `database`.
#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "the int8 key is split into its two 32-bit halves, as pg_advisory_lock does; \
              the lock type and method ids are small enum values"
)]
fn advisory_locktag(database: pg_sys::Oid, key: i64) -> pg_sys::LOCKTAG {
    let key = key as u64;
    pg_sys::LOCKTAG {
        locktag_field1: database.to_u32(),
        locktag_field2: (key >> 32) as u32,
        locktag_field3: key as u32,
        locktag_field4: 1, // an int8 advisory key, like pg_advisory_xact_lock_shared(bigint)
        locktag_type: pg_sys::LockTagType::LOCKTAG_ADVISORY as u8,
        locktag_lockmethodid: pg_sys::USER_LOCKMETHOD as u8,
    }
}

/// Take `AccessShareLock` on the pg_kafka extension object, the first lock of every
/// worker transaction and of the SQL maintenance functions (`pg_kafka_run_*` in
/// `lib.rs`). DROP EXTENSION (and ALTER EXTENSION ... ADD/DROP, which partition
/// maintenance runs for each leaf it creates or drops) lock it before touching any
/// member, so a transaction that takes it before any member table never holds a member
/// while DROP waits for it and vice versa (a lock-order deadlock). The wait is bounded
/// by the session's `lock_timeout`. After the wait the extension is looked up again:
/// returns its OID, or `None` if it does not exist or was dropped (or dropped and
/// re-created) meanwhile.
///
/// Must be called inside a transaction; the lookups and the lock raise Postgres ERRORs.
pub(crate) fn lock_extension() -> Option<pg_sys::Oid> {
    let name = c"pg_kafka";
    #[expect(clippy::cast_possible_wrap, reason = "lock modes are small constants")]
    let access_share = pg_sys::AccessShareLock as pg_sys::LOCKMODE;
    // SAFETY: called inside a transaction (the caller's contract); the catalog lookups
    // and the lock call raise Postgres ERRORs, which the caller handles.
    unsafe {
        let oid = pg_sys::get_extension_oid(name.as_ptr(), true);
        if oid == pg_sys::Oid::INVALID {
            return None;
        }
        pg_sys::LockDatabaseObject(pg_sys::ExtensionRelationId, oid, 0, access_share);
        // LockDatabaseObject processed the invalidations of whatever we waited for;
        // look the extension up again with a fresh catalog snapshot.
        pg_sys::InvalidateCatalogSnapshot();
        (pg_sys::get_extension_oid(name.as_ptr(), true) == oid).then_some(oid)
    }
}

/// Take the locks every worker transaction starts with, in this order:
///
/// 1. `AccessShareLock` on the pg_kafka extension object ([`lock_extension`]).
/// 2. The schema advisory lock ([`SCHEMA_LOCK_KEY`]) in SHARE mode: ALTER EXTENSION
///    UPDATE's script and the move of rows out of the DEFAULT leaf take it exclusive,
///    so neither overlaps a worker transaction (EndTxn's or DeleteRecords' statements
///    could otherwise miss rows being moved, or deadlock with the script's DDL).
///
/// Both waits are bounded by `lock_timeout` (`pg_kafka.worker_lock_timeout_ms`).
/// Returns false if the extension does not exist, or was re-created since the worker
/// initialised against it ([`KNOWN_EXTENSION_OID`]).
fn lock_schema() -> bool {
    let Some(oid) = lock_extension() else {
        return false;
    };
    let known = KNOWN_EXTENSION_OID.load(Ordering::SeqCst);
    if known == 0 {
        KNOWN_EXTENSION_OID.store(oid.to_u32(), Ordering::SeqCst);
    } else if known != oid.to_u32() {
        return false; // re-created since the worker initialised
    }
    #[expect(clippy::cast_possible_wrap, reason = "lock modes are small constants")]
    let share = pg_sys::ShareLock as pg_sys::LOCKMODE;
    let tag = advisory_locktag(
        // SAFETY: MyDatabaseId is set once the worker connected to its database.
        unsafe { pg_sys::MyDatabaseId },
        SCHEMA_LOCK_KEY,
    );
    // SAFETY: inside the worker transaction; a lock wait ERROR (lock_timeout) is
    // handled by worker_transaction's catch_unwind.
    unsafe {
        pg_sys::LockAcquire(&raw const tag, share, false, false);
    }
    true
}

/// Clean up after a Postgres ERROR caught outside any subtransaction, the way
/// PostgresMain's error handler does: abort the transaction, flush the error state,
/// and return to TopMemoryContext.
fn recover_from_transaction_error() {
    // SAFETY: the same sequence as PostgresMain's sigsetjmp error path, on the main
    // thread, after the ERROR was caught.
    unsafe {
        pg_sys::InterruptHoldoffCount += 1; // HOLD_INTERRUPTS()
        disable_all_timeouts(false);
        (&raw mut pg_sys::QueryCancelPending).write_volatile(0);
        pg_sys::AbortCurrentTransaction();
        pg_sys::MemoryContextSwitchTo(pg_sys::TopMemoryContext);
        pg_sys::FlushErrorState();
        pg_sys::InterruptHoldoffCount -= 1; // RESUME_INTERRUPTS()
    }
}

/// Whether a caught panic payload is a Postgres lock-timeout ERROR (55P03).
fn is_lock_timeout(payload: &(dyn Any + Send)) -> bool {
    match payload.downcast_ref::<CaughtError>() {
        Some(
            CaughtError::PostgresError(r)
            | CaughtError::ErrorReport(r)
            | CaughtError::RustPanic { ereport: r, .. },
        ) => r.sql_error_code() == PgSqlErrorCode::ERRCODE_LOCK_NOT_AVAILABLE,
        None => false,
    }
}

/// Run `f` in one worker transaction: start it, take the schema locks
/// ([`lock_schema`]), run `f`, commit. Every database transaction of the main loop goes
/// through here.
///
/// * `Err(SchemaGone)`: the extension does not exist; `f` did not run, and the main
///   loop is told to wait for the schema again.
/// * `Err(Failed)`: a Postgres ERROR escaped `f` or the commit failed. The transaction
///   is rolled back and the worker carries on, so the caller can still answer the
///   requests it took.
///
/// While it runs, SIGTERM interrupts the backend (see [`begin_db_work`]).
pub(super) fn worker_transaction<R>(f: impl FnOnce() -> R) -> Result<R, TxnError> {
    begin_db_work();
    let result = catch_unwind(AssertUnwindSafe(|| {
        BackgroundWorker::transaction(AssertUnwindSafe(|| lock_schema().then(f)))
    }));
    end_db_work();
    match result {
        Ok(Some(value)) => Ok(value),
        Ok(None) => {
            SCHEMA_LOST.store(true, Ordering::SeqCst);
            Err(TxnError::SchemaGone)
        }
        Err(payload) => {
            let message = panic_payload_message(payload.as_ref());
            let lock_timeout = is_lock_timeout(payload.as_ref());
            recover_from_transaction_error();
            Err(TxnError::Failed {
                message,
                lock_timeout,
            })
        }
    }
}

/// Log a worker transaction that did not run (`what` names the task). A missing
/// schema is reported once by the schema wait, not here.
pub(super) fn log_txn_error(what: &str, err: &TxnError) {
    if let TxnError::Failed { message, .. } = err {
        pg_warning!("{what} failed and was rolled back (worker survived): {message}");
    }
}

/// Names a maintenance task in the worker's log lines.
#[derive(Debug, Clone, Copy)]
pub(super) struct TaskNames {
    /// The task, for a transaction that failed ("Retention sweep").
    pub(super) task: &'static str,
    /// The task, for a caught panic ("Retention-sweep maintenance").
    pub(super) panic: &'static str,
}

/// Run a maintenance task `f` in its own worker transaction, inside a subtransaction, so
/// a Postgres ERROR in it (a panic through pgrx) rolls it back and is logged instead of
/// ending the worker. Returns true if `f` returned and the transaction committed.
pub(super) fn run_maintenance_task(names: TaskNames, f: impl FnOnce()) -> bool {
    let outcome = worker_transaction(|| match run_request_in_subtransaction(f) {
        Ok(_) => true,
        Err(panic_msg) => {
            pg_warning!(
                "{} panicked, rolled back (worker survived): {panic_msg}",
                names.panic
            );
            false
        }
    });
    outcome.unwrap_or_else(|e| {
        log_txn_error(names.task, &e);
        false
    })
}

/// Apply `pg_kafka.worker_lock_timeout_ms` as the worker session's `lock_timeout`.
/// Called at start and after every configuration reload, outside any transaction.
pub(super) fn apply_worker_lock_timeout() {
    let ms = crate::config::WORKER_LOCK_TIMEOUT_MS.get();
    let value = std::ffi::CString::new(format!("{ms}ms"))
        .expect("a formatted integer contains no NUL byte");
    // SAFETY: both strings are NUL-terminated and outlive the call; SetConfigOption
    // copies them.
    unsafe {
        pg_sys::SetConfigOption(
            c"lock_timeout".as_ptr(),
            value.as_ptr(),
            pg_sys::GucContext::PGC_SUSET,
            pg_sys::GucSource::PGC_S_SESSION,
        );
    }
}

/// Run the worker session's transactions at READ COMMITTED whatever the cluster's
/// `default_transaction_isolation`: the worker's SQL relies on each statement taking a
/// fresh snapshot (a leaf drop re-checks the leaf after waiting for its locks, and
/// refuses at any other level). A session-level setting outranks postgresql.conf, so a
/// reload keeps it. Called at start, outside any transaction.
pub(super) fn apply_worker_isolation() {
    // SAFETY: both strings are NUL-terminated literals; SetConfigOption copies them.
    unsafe {
        pg_sys::SetConfigOption(
            c"default_transaction_isolation".as_ptr(),
            c"read committed".as_ptr(),
            pg_sys::GucContext::PGC_USERSET,
            pg_sys::GucSource::PGC_S_SESSION,
        );
    }
}

/// Extract a human-readable message from a caught panic payload.
///
/// Postgres ERRORs crossing the pgrx FFI boundary arrive as a panic whose
/// payload is a `CaughtError`, so try that in addition to the usual
/// `&str`/`String` panic payloads.
fn panic_payload_message(payload: &(dyn Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else if let Some(e) = payload.downcast_ref::<CaughtError>() {
        format!("{e:?}")
    } else {
        "Unknown panic".to_string()
    }
}

/// Run `f` inside an internal subtransaction and commit or roll back its
/// database writes according to what it reports.
///
/// `f` returns a [`RequestOutcome`] (or `()`, which means commit; the
/// maintenance tasks use that form):
/// - `Commit`: the subtransaction is released into the surrounding transaction.
/// - `Rollback`: the handler failed after possibly writing part of its work
///   (e.g. IncrementalAlterConfigs applied resource 1, then failed on resource
///   2); the client has been told the request failed, so the subtransaction is
///   rolled back. This is an ordinary return, not an unwind.
/// - a panic: Postgres ERRORs (constraint violations, OOM, ...) surface through
///   pgrx as Rust panics. The worker catches them to stay alive, but a caught
///   panic must not let the surrounding `BackgroundWorker::transaction` COMMIT
///   the request's partial writes, and after a caught Postgres ERROR the
///   backend is still inside the error subsystem until `FlushErrorState`.
///   `catch_unwind` is here only for this case.
///
/// Both rollback paths use the PL/pgSQL exception-block sequence:
/// `BeginInternalSubTransaction`, then `RollbackAndReleaseCurrentSubTransaction`
/// (preceded by `FlushErrorState` after a panic), restoring the caller's memory
/// context and resource owner on every path.
///
/// Returns `Ok(outcome)` when `f` returned, `Err(panic message)` if it panicked
/// (after rolling back).
pub(super) fn run_request_in_subtransaction<R, F>(f: F) -> Result<RequestOutcome, String>
where
    R: Into<RequestOutcome>,
    F: FnOnce() -> R,
{
    run_in_subtransaction(f).map_err(|panic| panic.message)
}

/// A panic [`run_in_subtransaction`] caught and rolled back.
#[derive(Debug)]
pub(super) struct SubtransactionPanic {
    pub(super) message: String,
    /// It was a lock-not-available ERROR (a `lock_timeout` ran out, a NOWAIT lock).
    pub(super) lock_timeout: bool,
}

/// [`run_request_in_subtransaction`], also telling whether a caught panic was a lock
/// timeout.
pub(super) fn run_in_subtransaction<R, F>(f: F) -> Result<RequestOutcome, SubtransactionPanic>
where
    R: Into<RequestOutcome>,
    F: FnOnce() -> R,
{
    // SAFETY: called inside a transaction on the main thread; this is the sequence
    // PL/pgSQL's exception blocks use, and every path restores the memory context and
    // resource owner saved here.
    unsafe {
        let old_context = pg_sys::CurrentMemoryContext;
        let old_owner = pg_sys::CurrentResourceOwner;
        pg_sys::BeginInternalSubTransaction(std::ptr::null());
        pg_sys::MemoryContextSwitchTo(old_context);

        let result = match catch_unwind(AssertUnwindSafe(|| f().into())) {
            Ok(RequestOutcome::Commit) => {
                pg_sys::ReleaseCurrentSubTransaction();
                Ok(RequestOutcome::Commit)
            }
            Ok(RequestOutcome::Rollback) => {
                pg_sys::RollbackAndReleaseCurrentSubTransaction();
                Ok(RequestOutcome::Rollback)
            }
            Err(payload) => {
                let message = panic_payload_message(payload.as_ref());
                let lock_timeout = is_lock_timeout(payload.as_ref());
                pg_sys::FlushErrorState();
                pg_sys::RollbackAndReleaseCurrentSubTransaction();
                Err(SubtransactionPanic {
                    message,
                    lock_timeout,
                })
            }
        };
        pg_sys::MemoryContextSwitchTo(old_context);
        pg_sys::CurrentResourceOwner = old_owner;
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_failure_codes_are_retriable_kafka_errors() {
        assert_eq!(
            TxnError::SchemaGone.kafka_error_code(),
            ERROR_KAFKA_STORAGE_ERROR
        );
        assert_eq!(
            TxnError::Failed {
                message: String::new(),
                lock_timeout: true
            }
            .kafka_error_code(),
            ERROR_REQUEST_TIMED_OUT
        );
        assert_eq!(
            TxnError::Failed {
                message: String::new(),
                lock_timeout: false
            }
            .kafka_error_code(),
            ERROR_KAFKA_STORAGE_ERROR
        );
    }

    #[test]
    fn test_advisory_locktag_splits_the_key_like_pg_advisory_lock() {
        let tag = advisory_locktag(pg_sys::Oid::from(5u32), SCHEMA_LOCK_KEY);
        assert_eq!(tag.locktag_field1, 5);
        let key = u64::from(tag.locktag_field2) << 32 | u64::from(tag.locktag_field3);
        assert_eq!(i64::try_from(key).unwrap(), SCHEMA_LOCK_KEY);
        assert_eq!(tag.locktag_field4, 1);
    }
}
