//! Worker transactions: the schema locks every transaction starts with, recovery from a
//! failed top-level transaction, and the per-request subtransaction guard.

use pgrx::bgworkers::BackgroundWorker;
use pgrx::prelude::*;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};

use super::lifecycle::{sigterm_received, IN_DB_WORK};
use crate::kafka::dispatch::RequestOutcome;
use crate::pg_warning;

extern "C" {
    // Exported by the postgres executable (timeout.c); not in pgrx's bindings.
    fn disable_all_timeouts(keep_indicators: bool);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Lifecycle (wave 3, stream `worker`): SIGTERM, postmaster death, the per-transaction
// schema locks, and recovery from a failed top-level transaction.
// ═══════════════════════════════════════════════════════════════════════════════

/// Kafka error code KAFKA_STORAGE_ERROR (retriable): what a request gets when its batch
/// could not run or commit (schema missing, commit failure).
pub(super) const ERROR_KAFKA_STORAGE_ERROR: i16 = 56;
/// Kafka error code REQUEST_TIMED_OUT (retriable): the batch timed out waiting on a lock.
pub(super) const ERROR_REQUEST_TIMED_OUT: i16 = 7;

/// Key of the pg_kafka schema lock: a transaction-level advisory lock every worker
/// transaction takes in SHARE mode right after the extension lock. Whatever must not
/// overlap any worker transaction takes it EXCLUSIVE: moving rows out of
/// kafka.messages_default (`kafka.create_message_partition`) and the upgrade scripts.
/// (8099547369649367905, one less, is the partition-maintenance lock.)
pub(crate) const SCHEMA_LOCK_KEY: i64 = 8099547369649367906;

/// Set when a worker transaction found the extension gone (DROP EXTENSION); the main
/// loop then goes back to waiting for the schema.
pub(super) static SCHEMA_LOST: AtomicBool = AtomicBool::new(false);
/// OID of the pg_kafka extension the worker initialised against (0 = not yet known;
/// the next [`lock_schema`] records it). A different OID means the extension was
/// dropped and re-created between two worker transactions (possibly in one DDL
/// transaction the worker never saw "gone"): that is treated like a lost schema, so
/// the worker re-initialises (features, role, maintenance) for the new one.
pub(super) static KNOWN_EXTENSION_OID: std::sync::atomic::AtomicU32 =
    std::sync::atomic::AtomicU32::new(0);

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

/// Take the locks every worker transaction starts with, in this order:
///
/// 1. `AccessShareLock` on the pg_kafka extension object. DROP EXTENSION (and ALTER
///    EXTENSION ... ADD/DROP) lock it before touching any member, so the worker can no
///    longer hold a member table while DROP waits for it and vice versa (the lock-order
///    deadlock DROP EXTENSION used to hit). After the wait the extension is looked up
///    again: if it was dropped (or dropped and re-created) meanwhile, report it gone.
/// 2. The schema advisory lock ([`SCHEMA_LOCK_KEY`]) in SHARE mode: ALTER EXTENSION
///    UPDATE's script and the move of rows out of the DEFAULT leaf take it exclusive,
///    so neither overlaps a worker transaction (EndTxn's or DeleteRecords' statements
///    could otherwise miss rows being moved, or deadlock with the script's DDL).
///
/// Both waits are bounded by `lock_timeout` (`pg_kafka.worker_lock_timeout_ms`).
/// Returns false if the extension does not exist, or was re-created since the worker
/// initialised against it ([`KNOWN_EXTENSION_OID`]).
pub(super) fn lock_schema() -> bool {
    unsafe {
        let name = c"pg_kafka";
        let oid = pg_sys::get_extension_oid(name.as_ptr(), true);
        if oid == pg_sys::Oid::INVALID {
            return false;
        }
        pg_sys::LockDatabaseObject(
            pg_sys::ExtensionRelationId,
            oid,
            0,
            pg_sys::AccessShareLock as pg_sys::LOCKMODE,
        );
        // LockDatabaseObject processed the invalidations of whatever we waited for;
        // look the extension up again with a fresh catalog snapshot.
        pg_sys::InvalidateCatalogSnapshot();
        if pg_sys::get_extension_oid(name.as_ptr(), true) != oid {
            return false;
        }
        let known = KNOWN_EXTENSION_OID.load(Ordering::SeqCst);
        if known == 0 {
            KNOWN_EXTENSION_OID.store(oid.to_u32(), Ordering::SeqCst);
        } else if known != oid.to_u32() {
            return false; // re-created since the worker initialised
        }
        let key = SCHEMA_LOCK_KEY as u64;
        let tag = pg_sys::LOCKTAG {
            locktag_field1: pg_sys::MyDatabaseId.to_u32(),
            locktag_field2: (key >> 32) as u32,
            locktag_field3: key as u32,
            locktag_field4: 1, // an int8 advisory key, like pg_advisory_xact_lock_shared(bigint)
            locktag_type: pg_sys::LockTagType::LOCKTAG_ADVISORY as u8,
            locktag_lockmethodid: pg_sys::USER_LOCKMETHOD as u8,
        };
        pg_sys::LockAcquire(&tag, pg_sys::ShareLock as pg_sys::LOCKMODE, false, false);
        true
    }
}

/// Clean up after a Postgres ERROR caught outside any subtransaction, the way
/// PostgresMain's error handler does: abort the transaction, flush the error state,
/// and return to TopMemoryContext.
pub(super) fn recover_from_transaction_error() {
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
pub(super) fn is_lock_timeout(payload: &(dyn std::any::Any + Send)) -> bool {
    use pgrx::pg_sys::panic::CaughtError;
    match payload.downcast_ref::<CaughtError>() {
        Some(CaughtError::PostgresError(r))
        | Some(CaughtError::ErrorReport(r))
        | Some(CaughtError::RustPanic { ereport: r, .. }) => {
            r.sql_error_code() == pgrx::PgSqlErrorCode::ERRCODE_LOCK_NOT_AVAILABLE
        }
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
///   is rolled back and the worker carries on (it used to exit, dropping the error
///   responses it had queued).
///
/// While it runs, SIGTERM interrupts the backend (see [`handle_sigterm`]).
pub(super) fn worker_transaction<R>(f: impl FnOnce() -> R) -> Result<R, TxnError> {
    IN_DB_WORK.store(true, Ordering::SeqCst);
    if sigterm_received() {
        // A SIGTERM that arrived just before: let it interrupt this transaction too.
        unsafe {
            (&raw mut pg_sys::InterruptPending).write_volatile(1);
            (&raw mut pg_sys::ProcDiePending).write_volatile(1);
        }
    }
    let result = catch_unwind(AssertUnwindSafe(|| {
        BackgroundWorker::transaction(AssertUnwindSafe(|| {
            if !lock_schema() {
                return None;
            }
            Some(f())
        }))
    }));
    IN_DB_WORK.store(false, Ordering::SeqCst);
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
        pg_warning!(
            "{} failed and was rolled back (worker survived): {}",
            what,
            message
        );
    }
}

/// Apply `pg_kafka.worker_lock_timeout_ms` as the worker session's `lock_timeout`.
/// Called at start and after every configuration reload, outside any transaction.
pub(super) fn apply_worker_lock_timeout() {
    let ms = crate::config::WORKER_LOCK_TIMEOUT_MS.get();
    let value = std::ffi::CString::new(format!("{}ms", ms)).expect("no NUL");
    unsafe {
        pg_sys::SetConfigOption(
            c"lock_timeout".as_ptr(),
            value.as_ptr(),
            pg_sys::GucContext::PGC_SUSET,
            pg_sys::GucSource::PGC_S_SESSION,
        );
    }
}

/// Extract a human-readable message from a caught panic payload.
///
/// Postgres ERRORs crossing the pgrx FFI boundary arrive as a panic whose
/// payload is a `CaughtError`, so try that in addition to the usual
/// `&str`/`String` panic payloads.
pub(super) fn panic_payload_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        s.to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else if let Some(e) = payload.downcast_ref::<pgrx::pg_sys::panic::CaughtError>() {
        format!("{:?}", e)
    } else {
        "Unknown panic".to_string()
    }
}

/// Run `f` inside an internal subtransaction and commit or roll back its
/// database writes according to what it reports.
///
/// `f` returns a [`RequestOutcome`] (or `()`, which means commit — the
/// maintenance tasks use that form):
/// - `Commit`: the subtransaction is released into the surrounding transaction.
/// - `Rollback`: the handler failed after possibly writing part of its work
///   (e.g. IncrementalAlterConfigs applied resource 1, then failed on resource
///   2); the client has been told the request failed, so the subtransaction is
///   rolled back. This is an ordinary return, not an unwind.
/// - a panic: Postgres ERRORs (constraint violations, OOM, ...) surface through
///   pgrx as Rust panics. The worker catches them to stay alive, but a caught
///   panic must not let the surrounding `BackgroundWorker::transaction` COMMIT
///   the request's partial writes — and after a caught Postgres ERROR the
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
    unsafe {
        let old_context = pgrx::pg_sys::CurrentMemoryContext;
        let old_owner = pgrx::pg_sys::CurrentResourceOwner;
        pgrx::pg_sys::BeginInternalSubTransaction(std::ptr::null());
        pgrx::pg_sys::MemoryContextSwitchTo(old_context);

        let result = match catch_unwind(AssertUnwindSafe(|| f().into())) {
            Ok(RequestOutcome::Commit) => {
                pgrx::pg_sys::ReleaseCurrentSubTransaction();
                Ok(RequestOutcome::Commit)
            }
            Ok(RequestOutcome::Rollback) => {
                pgrx::pg_sys::RollbackAndReleaseCurrentSubTransaction();
                Ok(RequestOutcome::Rollback)
            }
            Err(payload) => {
                let message = panic_payload_message(payload.as_ref());
                pgrx::pg_sys::FlushErrorState();
                pgrx::pg_sys::RollbackAndReleaseCurrentSubTransaction();
                Err(message)
            }
        };
        pgrx::pg_sys::MemoryContextSwitchTo(old_context);
        pgrx::pg_sys::CurrentResourceOwner = old_owner;
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
}
