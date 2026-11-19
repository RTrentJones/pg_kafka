// Background worker module for pg_kafka
//
// This module implements the persistent background worker that runs the Kafka
// protocol listener. The worker is started by Postgres when the extension loads
// (via shared_preload_libraries in postgresql.conf).
//
// ## Two-Thread Architecture
//
// This module uses a producer-consumer pattern with two threads:
//
// 1. **Network Thread** (spawned): Runs a tokio multi-thread runtime
//    - Handles all TCP connections and Kafka protocol parsing
//    - Sends requests via bounded crossbeam channel
//    - Uses `tracing` for logging (NOT pgrx - thread safety)
//
// 2. **Database Thread** (main BGWorker): Blocks on channel recv
//    - Processes requests via SPI, batching already-queued requests into one
//      transaction per batch (group commit: one WAL flush per batch)
//    - Instant wake-up when requests arrive (no polling delay)
//    - Uses pgrx logging (safe on main thread)
//
// This eliminates the 100ms latency floor from the previous time-slicing design.

use crossbeam_channel::RecvTimeoutError;
use pgrx::bgworkers::{BackgroundWorker, SignalWakeFlags};
use pgrx::prelude::*;
use pgrx::Spi;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

// Import conditional logging macros from lib.rs
use crate::kafka::constants::*;
use crate::kafka::dispatch::RequestOutcome;
use crate::kafka::shadow::ShadowStore;
use crate::kafka::{KafkaStore, PostgresStore};
use crate::{pg_log, pg_warning};

/// How often to check for member timeouts
const TIMEOUT_CHECK_INTERVAL: Duration = Duration::from_secs(1);

/// How often to check for timed-out transactions (less frequent than member timeouts)
const TXN_TIMEOUT_CHECK_INTERVAL: Duration = Duration::from_secs(10);

/// DR-1/DR-2 (DEEP-REVIEW-2026-07): how often the storage-lifecycle retention sweep
/// runs (reclaim aborted rows, enforce message retention, prune stale
/// producers/terminal transactions/delivered shadow-outbox rows). Each pass is
/// bounded (RETENTION_DELETE_BATCH), so a large backlog drains across sweeps.
const RETENTION_SWEEP_INTERVAL: Duration = Duration::from_secs(60);

// Configuration reload interval is now controlled by pg_kafka.config_reload_interval_ms GUC
// (default 30s, tests can set to 1-2s for fast iteration)

/// Timeout for blocking recv - allows periodic signal checks
const RECV_TIMEOUT: Duration = Duration::from_millis(100);

/// Capacity of the bounded request channel (backpressure threshold)
const REQUEST_CHANNEL_CAPACITY: usize = 10_000;

/// DR-12: capacity of the liveness (Heartbeat) lane. Heartbeats are small and
/// per-member periodic; 1,000 pending already implies severe DB-thread stall.
const LIVENESS_CHANNEL_CAPACITY: usize = 1_000;

// ═══════════════════════════════════════════════════════════════════════════════
// Lifecycle (wave 3, stream `worker`): SIGTERM, postmaster death, the per-transaction
// schema locks, and recovery from a failed top-level transaction.
// ═══════════════════════════════════════════════════════════════════════════════

/// Kafka error code KAFKA_STORAGE_ERROR (retriable): what a request gets when its batch
/// could not run or commit (schema missing, commit failure).
const ERROR_KAFKA_STORAGE_ERROR: i16 = 56;
/// Kafka error code REQUEST_TIMED_OUT (retriable): the batch timed out waiting on a lock.
const ERROR_REQUEST_TIMED_OUT: i16 = 7;

/// Key of the pg_kafka schema lock: a transaction-level advisory lock every worker
/// transaction takes in SHARE mode right after the extension lock. Whatever must not
/// overlap any worker transaction takes it EXCLUSIVE: moving rows out of
/// kafka.messages_default (`kafka.create_message_partition`) and the upgrade scripts.
/// (8099547369649367905, one less, is the partition-maintenance lock.)
pub const SCHEMA_LOCK_KEY: i64 = 8099547369649367906;

/// Set by our SIGTERM handler; never reset (the worker exits once it is set).
static GOT_SIGTERM: AtomicBool = AtomicBool::new(false);
/// True while the worker is inside a database transaction: a SIGTERM then also sets
/// Postgres's ProcDiePending, so a statement or lock wait is interrupted at its next
/// CHECK_FOR_INTERRUPTS (FATAL, exit status 1, restarted by the postmaster). Outside
/// database work the main loop sees GOT_SIGTERM and shuts down gracefully.
static IN_DB_WORK: AtomicBool = AtomicBool::new(false);
/// Set when a worker transaction found the extension gone (DROP EXTENSION); the main
/// loop then goes back to waiting for the schema.
static SCHEMA_LOST: AtomicBool = AtomicBool::new(false);
/// OID of the pg_kafka extension the worker initialised against (0 = not yet known;
/// the next [`lock_schema`] records it). A different OID means the extension was
/// dropped and re-created between two worker transactions (possibly in one DDL
/// transaction the worker never saw "gone"): that is treated like a lost schema, so
/// the worker re-initialises (features, role, maintenance) for the new one.
static KNOWN_EXTENSION_OID: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

extern "C" {
    // Exported by the postgres executable (pmsignal.c, latch support); not in pgrx's
    // bindings. Neither raises a Postgres error.
    fn PostmasterIsAliveInternal() -> bool;
    fn disable_all_timeouts(keep_indicators: bool);
    fn _exit(status: std::ffi::c_int) -> !;
    #[link_name = "SetLatch"]
    fn RawSetLatch(latch: *mut pg_sys::Latch);
    #[cfg(target_os = "linux")]
    fn __errno_location() -> *mut std::ffi::c_int;
}

/// SIGTERM handler. pgrx's handler only sets a flag the main loop polls between
/// requests, so a worker blocked in SQL or a lock wait could not be stopped. This one
/// also requests ProcDie while database work is in progress (what Postgres's own
/// `die()` does), and keeps errno intact like Postgres's handlers.
unsafe extern "C-unwind" fn handle_sigterm(_signal: std::ffi::c_int) {
    #[cfg(target_os = "linux")]
    let saved_errno = *__errno_location();
    GOT_SIGTERM.store(true, Ordering::SeqCst);
    (&raw mut pg_sys::ShutdownRequestPending).write_volatile(1);
    if IN_DB_WORK.load(Ordering::SeqCst) {
        (&raw mut pg_sys::InterruptPending).write_volatile(1);
        (&raw mut pg_sys::ProcDiePending).write_volatile(1);
    }
    // The raw C function, not pgrx's wrapper: the wrapper checks it runs on the main
    // thread and panics otherwise, and a panic in a signal handler unwinds into
    // whatever that thread was doing.
    RawSetLatch(pg_sys::MyLatch);
    #[cfg(target_os = "linux")]
    {
        *__errno_location() = saved_errno;
    }
}

/// Replace pgrx's SIGTERM handler with [`handle_sigterm`] (after
/// `attach_signal_handlers`, which unblocks signals).
fn install_sigterm_handler() {
    unsafe {
        #[cfg(not(feature = "pg18"))]
        pg_sys::pqsignal(pg_sys::SIGTERM as i32, Some(handle_sigterm));
        #[cfg(feature = "pg18")]
        pg_sys::pqsignal_be(pg_sys::SIGTERM as i32, Some(handle_sigterm));
    }
}

/// Whether SIGTERM has arrived (sticky).
fn sigterm_received() -> bool {
    GOT_SIGTERM.load(Ordering::SeqCst)
}

/// Whether the postmaster is still alive (one non-blocking read on the postmaster-death
/// pipe). The worker must not outlive it: an orphan would keep committing Kafka writes
/// and block a new postmaster from starting.
fn postmaster_alive() -> bool {
    unsafe { PostmasterIsAliveInternal() }
}

/// Why a worker transaction did not run or commit.
#[derive(Debug)]
enum TxnError {
    /// The extension is gone (DROP EXTENSION); nothing ran.
    SchemaGone,
    /// A Postgres ERROR outside any request's subtransaction: taking the schema locks
    /// (lock_timeout), or the COMMIT itself. The transaction was rolled back.
    Failed { message: String, lock_timeout: bool },
}

impl TxnError {
    /// The Kafka error code for requests of a batch that failed this way.
    fn kafka_error_code(&self) -> i16 {
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
fn lock_schema() -> bool {
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
fn recover_from_transaction_error() {
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
fn is_lock_timeout(payload: &(dyn std::any::Any + Send)) -> bool {
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
fn worker_transaction<R>(f: impl FnOnce() -> R) -> Result<R, TxnError> {
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
fn log_txn_error(what: &str, err: &TxnError) {
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
fn apply_worker_lock_timeout() {
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

/// Which optional parts of the schema this database has. A new binary can run against
/// the schema of an older extension version until `ALTER EXTENSION pg_kafka UPDATE`
/// (docs/UPGRADING.md step 2): it then skips what that schema lacks instead of failing
/// every cycle.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct SchemaFeatures {
    /// `extversion` of the installed extension.
    version: String,
    /// kafka.messages is time-partitioned (0.1.0, ADR-001): partition maintenance and
    /// the partition-aware retention sweep.
    time_partitions: bool,
    /// kafka.broker_api_stats / kafka.broker_gauges exist (broker metrics flush).
    broker_metrics: bool,
    /// kafka.sasl_credentials exists (SASL users).
    sasl_credentials: bool,
}

impl SchemaFeatures {
    /// Human-readable list of what this schema lacks (empty: nothing).
    fn missing(&self) -> Vec<&'static str> {
        let mut out = Vec::new();
        if !self.time_partitions {
            out.push("time partitions of kafka.messages (partition maintenance; retention runs row by row)");
        }
        if !self.broker_metrics {
            out.push("broker metrics (kafka.broker_api_stats / kafka.broker_gauges)");
        }
        if !self.sasl_credentials {
            out.push("SASL users (kafka.sasl_credentials)");
        }
        out
    }
}

/// Read the [`SchemaFeatures`] of the current database (inside a transaction).
fn detect_schema_features() -> SchemaFeatures {
    Spi::connect(|client| {
        let row = client
            .select(
                "SELECT (SELECT extversion FROM pg_extension WHERE extname = 'pg_kafka') AS v,
                        to_regprocedure('kafka.message_partitions()') IS NOT NULL AS tp,
                        to_regclass('kafka.broker_api_stats') IS NOT NULL
                          AND to_regclass('kafka.broker_gauges') IS NOT NULL AS bm,
                        to_regclass('kafka.sasl_credentials') IS NOT NULL AS sc",
                None,
                &[],
            )
            .ok()?;
        let row = row.first();
        Some(SchemaFeatures {
            version: row.get_by_name::<String, _>("v").ok()??,
            time_partitions: row.get_by_name("tp").ok()?.unwrap_or(false),
            broker_metrics: row.get_by_name("bm").ok()?.unwrap_or(false),
            sasl_credentials: row.get_by_name("sc").ok()?.unwrap_or(false),
        })
    })
    .unwrap_or_default()
}

/// Re-detect the schema features; log once whenever they change (a WARNING listing
/// what is unavailable, or a LOG line when everything is there again). Returns true
/// if they changed.
fn refresh_schema_features(features: &mut SchemaFeatures) -> bool {
    let now = match worker_transaction(detect_schema_features) {
        Ok(f) => f,
        Err(_) => return false,
    };
    if now == *features {
        return false;
    }
    let missing = now.missing();
    if missing.is_empty() {
        if !features.version.is_empty() {
            log!(
                "pg_kafka schema is now version {}: all features enabled",
                now.version
            );
        }
    } else {
        pg_warning!(
            "pg_kafka: running against an older pg_kafka schema (version {}, library {}); \
             unavailable until ALTER EXTENSION pg_kafka UPDATE: {}",
            now.version,
            env!("CARGO_PKG_VERSION"),
            missing.join("; ")
        );
    }
    *features = now;
    true
}

/// Re-decide the worker's role after the schema changed under it (DROP + CREATE
/// EXTENSION, ALTER EXTENSION UPDATE): go back to the bootstrap superuser (`SET ROLE
/// NONE`), then apply `pg_kafka.run_as_role` again if the new schema grants it what the
/// worker needs. Otherwise a worker that had switched to `pgkafka_worker` would be
/// denied everything on a re-created 0.0.0 schema (which grants nothing to that role),
/// and one that kept the superuser on 0.0.0 would never switch after the update.
fn reapply_worker_role() {
    let reset: Result<(), pgrx::spi::Error> = BackgroundWorker::transaction(|| {
        Spi::run("SELECT pg_catalog.set_config('role', 'none', false)")
    });
    if let Err(e) = reset {
        pg_warning!("pg_kafka: could not reset the worker role: {}", e);
        return;
    }
    crate::kafka::storage::postgres::apply_worker_role();
}

/// Answer a request that will not run (schema missing, worker stopping) with its API's
/// error response, on its own sender. An acks=0 produce gets nothing.
fn reject_request(request: crate::kafka::KafkaRequest, error_code: i16) {
    if !request.body.expects_response() {
        return;
    }
    let response = crate::kafka::KafkaResponse::new(
        request.header.correlation_id,
        request.header.api_version,
        request
            .body
            .echo()
            .error_response(request.header.api_version, error_code),
    );
    let _ = request.response_tx.send(response);
}

/// The network thread's shutdown sender, for [`stop_network_on_exit`].
static NETWORK_SHUTDOWN: std::sync::Mutex<Option<std::sync::mpsc::Sender<()>>> =
    std::sync::Mutex::new(None);
/// Set when the network thread has finished (its runtime is shut down).
static NETWORK_FINISHED: AtomicBool = AtomicBool::new(false);
/// How long [`stop_network_on_exit`] waits for the network thread, in ms.
static NETWORK_EXIT_WAIT_MS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(2000);

/// Sets [`NETWORK_FINISHED`] when the network thread's closure ends, however it ends.
struct NetworkFinished;
impl Drop for NetworkFinished {
    fn drop(&mut self) {
        NETWORK_FINISHED.store(true, Ordering::SeqCst);
    }
}

/// `before_shmem_exit` callback: ask the network thread to stop and wait for it (at
/// most 2 s). Never unwinds into Postgres.
unsafe extern "C-unwind" fn stop_network_on_exit(_code: std::ffi::c_int, _arg: pg_sys::Datum) {
    let _ = catch_unwind(|| {
        let tx = NETWORK_SHUTDOWN
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take();
        if let Some(tx) = tx {
            let _ = tx.send(());
        }
        let start = Instant::now();
        let wait = Duration::from_millis(NETWORK_EXIT_WAIT_MS.load(Ordering::SeqCst));
        while !NETWORK_FINISHED.load(Ordering::SeqCst) && start.elapsed() < wait {
            std::thread::sleep(Duration::from_millis(10));
        }
    });
}

/// Stop the network thread (bounded by `timeout`) and exit with status 1.
fn shutdown_and_exit(
    shutdown_tx: &std::sync::mpsc::Sender<()>,
    network_handle: &std::thread::JoinHandle<()>,
    timeout: Duration,
) -> ! {
    let _ = shutdown_tx.send(());
    // The exit hook must not wait longer than the caller allowed.
    NETWORK_EXIT_WAIT_MS.store(timeout.as_millis() as u64, Ordering::SeqCst);
    let join_start = Instant::now();
    loop {
        if network_handle.is_finished() {
            log!("pg_kafka network thread shut down cleanly");
            break;
        }
        if join_start.elapsed() > timeout {
            pgrx::warning!("pg_kafka network thread shutdown timed out");
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    // Status 1, not a plain return: a SIGTERM from pg_terminate_backend() must lead to
    // a restarted worker (see exit_for_restart).
    exit_for_restart();
}

/// Extract a human-readable message from a caught panic payload.
///
/// Postgres ERRORs crossing the pgrx FFI boundary arrive as a panic whose
/// payload is a `CaughtError`, so try that in addition to the usual
/// `&str`/`String` panic payloads.
fn panic_payload_message(payload: &(dyn std::any::Any + Send)) -> String {
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
fn run_request_in_subtransaction<R, F>(f: F) -> Result<RequestOutcome, String>
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

/// ADR-001 (0.1.0): one time-partition maintenance pass in its own transaction
/// (`PostgresStore::ensure_time_partitions`): leaves for now and the next widths, and
/// rows caught by kafka.messages_default moved into their leaf. Guarded like the other
/// maintenance blocks, so an ERROR (for example a schema that predates 0.1.0, before
/// `ALTER EXTENSION pg_kafka UPDATE`) is logged and the worker carries on.
///
/// Skipped on a schema without time partitions (an older extension version, see
/// [`SchemaFeatures`]): the old-schema WARNING already said so, once.
fn run_partition_maintenance(features: &SchemaFeatures) {
    if !features.time_partitions {
        return;
    }
    let outcome = worker_transaction(|| {
        let result = run_request_in_subtransaction(|| {
            let store = PostgresStore::new();
            match store.ensure_time_partitions(crate::config::MESSAGE_PARTITION_MINUTES.get()) {
                Ok(created) if created > 0 => {
                    pg_log!(
                        "Partition maintenance: created {} time partition(s)",
                        created
                    );
                }
                Ok(_) => {}
                Err(e) => {
                    pg_warning!("Partition maintenance failed: {}", e);
                }
            }
        });
        if let Err(panic_msg) = result {
            pg_warning!(
                "Partition maintenance panicked, rolled back (worker survived): {}",
                panic_msg
            );
        }
    });
    if let Err(e) = outcome {
        log_txn_error("Partition maintenance", &e);
    }
}

/// Verify that the kafka schema and required tables exist.
/// Returns Ok(()) if all tables exist, Err with description if any are missing.
fn verify_schema(database: &str) -> Result<(), String> {
    // QA-7: verify every table the runtime depends on, not just the core three, so a partial or
    // stale schema (e.g. missing the phase-9/10 idempotent/transaction tables, the BUG-3 offset
    // counter, or the shadow tables) is caught at worker start rather than as a late SPI failure on
    // the first produce/txn/shadow operation. `CREATE EXTENSION pg_kafka` creates all of these.
    // DR-6 (DEEP-REVIEW-2026-07): "consumer_groups" is intentionally absent — the table was
    // dead schema (membership lives in the in-memory GroupCoordinator) and was dropped.
    let required_tables = [
        "topics",
        "messages",
        "consumer_offsets",
        "partition_offsets",
        "producer_ids",
        "producer_sequences",
        "transactions",
        "txn_pending_offsets",
        "shadow_config",
        "shadow_metrics",
        "shadow_tracking",
    ];

    let result: Result<Option<String>, pgrx::spi::SpiError> = Spi::connect(|client| {
        for table in required_tables {
            // pg_catalog, not information_schema: information_schema.tables lists only
            // tables the current role may use, so a worker running as pgkafka_worker
            // would never see a schema that does not grant to it (e.g. a re-created
            // 0.0.0 schema).
            let query = format!(
                "SELECT 1 WHERE pg_catalog.to_regclass('kafka.{}') IS NOT NULL",
                table
            );
            match client.select(&query, None, &[]) {
                Ok(tup_table) => {
                    if tup_table.is_empty() {
                        return Ok(Some(format!(
                            "Required table kafka.{} does not exist. \
                             Run 'CREATE EXTENSION pg_kafka' in database '{}'",
                            table, database
                        )));
                    }
                }
                Err(e) => {
                    return Ok(Some(format!(
                        "Failed to check for table kafka.{}: {}",
                        table, e
                    )));
                }
            }
        }
        Ok(None) // All tables exist
    });

    match result {
        Ok(None) => Ok(()),
        Ok(Some(e)) => Err(e),
        Err(e) => Err(format!("Schema verification failed: {}", e)),
    }
}

/// How often `wait_for_schema` re-checks for the kafka schema.
const SCHEMA_CHECK_INTERVAL: Duration = Duration::from_millis(500);

/// How often `wait_for_schema` repeats its "waiting for schema" LOG line.
const SCHEMA_WAIT_LOG_INTERVAL: Duration = Duration::from_secs(60);

/// Whether the schema wait should log on this check: on the first failed check,
/// then once per `SCHEMA_WAIT_LOG_INTERVAL`, so an instance that never runs
/// `CREATE EXTENSION` does not fill the log.
fn schema_wait_should_log(waited: Duration, last_logged: Option<Duration>) -> bool {
    match last_logged {
        None => true,
        Some(last) => waited.saturating_sub(last) >= SCHEMA_WAIT_LOG_INTERVAL,
    }
}

/// Block until the kafka schema exists in `database`. Returns `false` if SIGTERM
/// arrived first.
///
/// The worker starts when Postgres finishes recovery, but `CREATE EXTENSION` may
/// not have run yet (or may run much later). The wait is unbounded: it used to
/// give up after 60 s and exit, and since nothing restarted the worker the Kafka
/// port then stayed dead until Postgres was restarted even after the extension
/// was created. Postgres has no cross-process event for "schema created", so this
/// polls; `wait_latch` sleeps on the process latch, which SIGTERM sets, so
/// shutdown is not delayed by the poll interval.
fn wait_for_schema(database: &str) -> bool {
    let mut waited = Duration::ZERO;
    let mut last_logged: Option<Duration> = None;
    loop {
        // SPI calls require a transaction context
        match BackgroundWorker::transaction(|| verify_schema(database)) {
            Ok(()) => {
                if last_logged.is_some() {
                    log!(
                        "pg_kafka schema found after waiting {} ms",
                        waited.as_millis()
                    );
                }
                return true;
            }
            Err(e) => {
                if schema_wait_should_log(waited, last_logged) {
                    log!(
                        "Waiting for pg_kafka schema (run 'CREATE EXTENSION pg_kafka' in database '{}'): {}",
                        database,
                        e
                    );
                    last_logged = Some(waited);
                }
            }
        }
        // wait_latch returns false once SIGTERM has been received (it consumes the
        // flag) or the postmaster has died.
        // (Our own SIGTERM handler replaces pgrx's flag, so check ours too.)
        if !BackgroundWorker::wait_latch(Some(SCHEMA_CHECK_INTERVAL)) || sigterm_received() {
            return false;
        }
        waited = waited.saturating_add(SCHEMA_CHECK_INTERVAL);
    }
}

/// Exit the worker process with status 1 so the postmaster restarts it after
/// `WORKER_RESTART_INTERVAL`.
///
/// A bgworker that returns from its main function exits with status 0, which
/// tells the postmaster to unregister it for good. That is wrong for every exit
/// this worker makes: a SIGTERM from `pg_terminate_backend()` or the network
/// thread dying should both lead to a fresh worker. During a server shutdown the
/// postmaster does not restart workers, so status 1 is also correct there (it is
/// what Postgres's own default SIGTERM handler for bgworkers does).
fn exit_for_restart() -> ! {
    unsafe { pg_sys::proc_exit(1) }
}

/// How long the postmaster waits before restarting a worker that exited
/// (registered in `_PG_init`).
pub const WORKER_RESTART_INTERVAL: Duration = Duration::from_secs(5);

/// Build the per-listener security (TLS acceptor, SASL mechanisms, advertised address)
/// for every configured listener, primary first. Loads the TLS files once.
fn build_listener_securities(
    settings: &crate::kafka::auth::SecuritySettings,
) -> Result<Vec<std::sync::Arc<crate::kafka::listener::ListenerSecurity>>, String> {
    let tls_config = if settings.needs_tls() {
        Some(crate::kafka::auth::tls::build_server_config(
            &settings.ssl_cert_file,
            &settings.ssl_key_file,
            settings
                .mutual_tls()
                .then_some(settings.ssl_ca_file.as_str()),
        )?)
    } else {
        None
    };
    let mechanisms = std::sync::Arc::new(settings.sasl_mechanisms.clone());
    Ok(settings
        .listeners
        .iter()
        .map(|spec| {
            std::sync::Arc::new(crate::kafka::listener::ListenerSecurity {
                protocol: spec.protocol,
                bind_addr: spec.bind_addr(),
                tls: spec
                    .protocol
                    .uses_tls()
                    .then(|| tls_config.clone())
                    .flatten()
                    .map(tokio_rustls::TlsAcceptor::from),
                sasl_mechanisms: mechanisms.clone(),
                advertised: spec.advertised.clone(),
            })
        })
        .collect())
}

/// Answer SASL credential lookups: `first` plus whatever else is queued, in one
/// transaction (each lookup in its own subtransaction, so one failure cannot affect the
/// others). The network thread waits on each reply.
fn serve_credential_requests(
    first: crate::kafka::auth::CredentialRequest,
    rx: &crossbeam_channel::Receiver<crate::kafka::auth::CredentialRequest>,
) {
    let mut batch = vec![first];
    while batch.len() < 64 {
        match rx.try_recv() {
            Ok(r) => batch.push(r),
            Err(_) => break,
        }
    }
    let mut batch = Some(batch);
    let outcome = worker_transaction(|| {
        for request in batch.take().unwrap_or_default() {
            let mut result = Err("credential lookup failed".to_string());
            let outcome = run_request_in_subtransaction(|| {
                result = crate::kafka::auth::credentials::lookup_credential(
                    &request.username,
                    request.mechanism,
                );
            });
            if let Err(panic_msg) = outcome {
                pg_warning!("SASL credential lookup rolled back: {}", panic_msg);
                result = Err("credential lookup failed".to_string());
            } else if let Err(e) = &result {
                pg_warning!("SASL credential lookup failed: {}", e);
            }
            // The receiver may have given up (timeout / disconnect); nothing to do then.
            let _ = request.reply.send(result);
        }
    });
    if let Err(e) = outcome {
        log_txn_error("SASL credential lookup", &e);
        // Not run (schema missing, lock timeout): fail the lookups, do not leave the
        // network thread waiting.
        for request in batch.take().unwrap_or_default() {
            let _ = request
                .reply
                .send(Err("credential lookup unavailable".to_string()));
        }
    }
}

/// Main entry point for the pg_kafka background worker.
///
/// IMPORTANT: This function MUST follow pgrx background worker conventions:
/// 1. Must be marked with #[pg_guard] to handle Postgres errors safely
/// 2. Must attach signal handlers BEFORE doing any work
/// 3. Must check for shutdown signals regularly and exit gracefully
/// 4. Must be marked with #[no_mangle] to ensure Postgres can find it
#[pg_guard]
#[no_mangle]
pub extern "C-unwind" fn pg_kafka_listener_main(_arg: pg_sys::Datum) {
    // Step 1: Attach to Postgres signal handling system
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    // wave3 worker: a SIGTERM handler that can interrupt SQL and lock waits.
    install_sigterm_handler();

    // Step 2: Load configuration (GUCs are available after _PG_init)
    // Create RuntimeContext for centralized config access across all modules
    let runtime_context = std::sync::Arc::new(crate::kafka::RuntimeContext::new(
        crate::config::Config::load(),
    ));
    let config = runtime_context.config();

    // Step 3: Connect to the configured database (required for SPI access)
    // IMPORTANT: Must connect to the database where CREATE EXTENSION was run
    BackgroundWorker::connect_worker_to_spi(Some(&config.database), None);

    // Step 3b: Wait for schema to exist before starting listener
    if !wait_for_schema(&config.database) {
        log!("Shutdown signal received while waiting for schema");
        exit_for_restart();
    }

    // 0.1.0: drop from the bootstrap superuser to pg_kafka.run_as_role.
    crate::kafka::storage::postgres::apply_worker_role();

    // wave3 worker: bounded lock waits for every worker transaction.
    apply_worker_lock_timeout();

    // What this schema has (an older extension version lacks some objects).
    let mut features = SchemaFeatures::default();
    refresh_schema_features(&mut features);

    // ADR-001: create the time partitions of kafka.messages before serving (rows that
    // arrive before a leaf exists land in kafka.messages_default, so this is about
    // keeping that leaf empty, not about Produce succeeding).
    run_partition_maintenance(&features);

    log!(
        "pg_kafka background worker started (port: {}, host: {}, database: {})",
        config.port,
        config.host,
        config.database
    );

    // Step 3c: Initialize shadow mode store (Phase 11)
    // Always create ShadowStore wrapping PostgresStore. Shadow mode can be
    // enabled/disabled at runtime via pg_kafka.shadow_mode_enabled GUC.
    // The producer is lazily initialized on first use when enabled.
    let shadow_store: std::sync::Arc<ShadowStore<PostgresStore>> =
        std::sync::Arc::new(ShadowStore::new(PostgresStore::new()));

    // Set license key for shadow mode (Commercial License)
    shadow_store.set_license_key(&config.shadow_license_key);

    if config.shadow_mode_enabled {
        log!(
            "Shadow mode enabled, configured to forward to: {}",
            config.shadow_bootstrap_servers
        );
    }

    // Step 4: Clone config values for use in threads
    let port = config.port;
    let host = config.host.clone();
    let shutdown_timeout = Duration::from_millis(config.shutdown_timeout_ms as u64);
    let default_partitions = config.default_partitions;
    let compression = parse_compression_type(&config.compression_type);
    let log_timing = config.log_timing;

    // Convert 0.0.0.0 to localhost for advertised host (used in Metadata and FindCoordinator)
    // 0.0.0.0 is not routable from clients - they need a resolvable hostname
    let advertised_host = if host == "0.0.0.0" || host.is_empty() {
        "localhost".to_string()
    } else {
        host.clone()
    };

    // Create BrokerMetadata once (eliminates string allocations on every Metadata/FindCoordinator request)
    let broker_metadata = crate::kafka::BrokerMetadata::new(advertised_host, port);

    // Step 5: Create bounded request channel (backpressure at 10,000 pending requests)
    // This is the bridge between the network thread and the database thread.
    let (request_tx, request_rx) =
        crossbeam_channel::bounded::<crate::kafka::KafkaRequest>(REQUEST_CHANNEL_CAPACITY);

    // DR-12 (DEEP-REVIEW-2026-07): dedicated liveness lane for Heartbeat. The DB
    // thread drains this lane before taking the next main-lane request, so group
    // liveness never waits behind a queue of heavy Produce/Fetch work (which
    // previously let DB pressure cascade into spurious rebalances). Small bound:
    // heartbeats are tiny and each member has at most one in flight; a full lane
    // applies the same backpressure as the main lane (the connection's reader
    // waits in send_with_backpressure), nothing is dropped.
    let (liveness_tx, liveness_rx) =
        crossbeam_channel::bounded::<crate::kafka::KafkaRequest>(LIVENESS_CHANNEL_CAPACITY);
    let request_lanes = crate::kafka::RequestLanes {
        main: request_tx,
        liveness: liveness_tx,
    };

    // Step 5b: Create notification channel for long polling support
    // This channel sends notifications from the database thread to the network thread
    // when new messages are produced. Used to wake up waiting FetchRequest handlers.
    // Bounded to prevent OOM if produce rate >> consume rate. Dropped notifications
    // are acceptable - fetch handlers fall back to polling.
    let (notify_tx, notify_rx) =
        crossbeam_channel::bounded::<crate::kafka::InternalNotification>(10_000);

    // Step 5c: Create forward channel for the durable shadow outbox (SH-9).
    // The DB thread sends ForwardRequest messages to the network thread, which
    // forwards them to external Kafka via the idempotent producer. On channel
    // full the outbox row simply stays pending and is retried by the next poll.
    let (forward_tx, forward_rx) =
        crossbeam_channel::bounded::<crate::kafka::shadow::ForwardRequest>(10_000);

    // The reverse channel: the network thread returns a ForwardAck per attempt
    // so the DB thread can finalize the kafka.shadow_tracking outbox row.
    let (forward_ack_tx, forward_ack_rx) =
        crossbeam_channel::bounded::<crate::kafka::shadow::ForwardAck>(10_000);

    // Wire both ends onto the ShadowStore (DB thread drains acks; sends forwards).
    shadow_store.set_forward_channel(forward_tx);
    shadow_store.set_ack_channel(forward_ack_rx);

    // Step 5d: Pass RuntimeContext to network thread for config access
    // The network thread will use this to access shadow config and other settings dynamically
    let runtime_context_for_network = runtime_context.clone();

    // Step 6: Create shutdown signal channel
    // Simple mpsc channel - network thread checks for message to shutdown
    let (shutdown_tx, shutdown_rx) = std::sync::mpsc::channel::<()>();

    // Step 7: Bind TCP listener on main thread BEFORE spawning network thread
    // This ensures bind failures are fatal and immediate
    //
    // IMPORTANT: We use std::net::TcpListener here, NOT tokio::net::TcpListener.
    // Tokio resources are tied to the runtime that created them. If we created a
    // tokio listener here and dropped the runtime, the listener would be broken.
    // Instead, we bind with std, then convert to tokio in the network thread.
    //
    // Restart path: a restarted worker binds again here. The previous worker's
    // listening socket closed with its process, and std's bind sets SO_REUSEADDR
    // on Unix, so lingering TIME_WAIT connections from the old process do not
    // block the new bind. If the bind still fails (port taken by something
    // else), pgrx::error! exits the worker with status 1 and the postmaster
    // retries after WORKER_RESTART_INTERVAL.
    let bind_addr = format!("{}:{}", host, port);

    // auth stream: listener security (pg_kafka.security_protocol for the primary
    // listener, pg_kafka.additional_listeners, TLS files, SASL mechanisms). Read once
    // here; a bad setting or unreadable file stops the worker with a clear error.
    let security_settings = crate::kafka::auth::SecuritySettings::resolve(
        &crate::kafka::auth::settings::raw_from_gucs(),
        &host,
        port,
    )
    .unwrap_or_else(|e| pgrx::error!("pg_kafka listener security: {}", e));
    let listener_securities = build_listener_securities(&security_settings)
        .unwrap_or_else(|e| pgrx::error!("pg_kafka listener security: {}", e));

    let std_listener = match std::net::TcpListener::bind(&bind_addr) {
        Ok(listener) => {
            // Set non-blocking mode for tokio compatibility
            if let Err(e) = listener.set_nonblocking(true) {
                pgrx::error!("Failed to set non-blocking mode: {}", e);
            }
            pg_log!(
                "TCP listener bound to {} ({})",
                bind_addr,
                security_settings.listeners[0].protocol.name()
            );
            listener
        }
        Err(e) => {
            pgrx::error!(
                "FATAL: Cannot bind to {}: {}. Check if port {} is already in use.",
                bind_addr,
                e,
                port
            );
        }
    };
    // Additional listeners (bind failures are fatal like the primary's).
    let mut std_listeners = vec![std_listener];
    for spec in &security_settings.listeners[1..] {
        let addr = spec.bind_addr();
        match std::net::TcpListener::bind(&addr) {
            Ok(l) => {
                if let Err(e) = l.set_nonblocking(true) {
                    pgrx::error!("Failed to set non-blocking mode: {}", e);
                }
                pg_log!("TCP listener bound to {} ({})", addr, spec.protocol.name());
                std_listeners.push(l);
            }
            Err(e) => pgrx::error!(
                "FATAL: Cannot bind pg_kafka listener {}://{}: {}",
                spec.protocol.name(),
                addr,
                e
            ),
        }
    }
    // SEC-7: warn loudly about listeners reachable from other hosts that do not
    // authenticate clients (and SASL/PLAIN in clear text), so an operator does not
    // silently expose unauthenticated read/write access to every topic.
    for w in security_settings.warnings() {
        pg_warning!("{}", w);
    }

    // auth stream: SASL credential lookups from the network thread (the DB thread
    // answers them between request batches).
    let (credential_tx, credential_rx) =
        crossbeam_channel::bounded::<crate::kafka::auth::CredentialRequest>(1024);

    // Step 8: Spawn the network thread
    // This thread runs a multi-threaded tokio runtime for handling all network I/O.
    // CRITICAL: This thread must NEVER call pgrx functions - use tracing for logging.
    pg_log!("Spawning network thread with tokio multi-thread runtime");
    let network_handle = std::thread::Builder::new()
        .name("pg_kafka-net".to_string())
        .spawn(move || {
            let _finished = NetworkFinished;
            // Network-thread logging to stderr: warnings by default, RUST_LOG overrides.
            crate::kafka::listener::init_network_tracing();

            // Create multi-threaded tokio runtime
            let rt = match tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .worker_threads(2)
                .thread_name("pg_kafka-io")
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    tracing::error!("Failed to create tokio runtime: {}", e);
                    return;
                }
            };

            // Run the listener inside the runtime context
            // IMPORTANT: from_std() must be called inside block_on() to have reactor context
            // A panic is contained here and the runtime is shut down with a bound, so no
            // destructor runs while this thread unwinds (a panic in a destructor during
            // unwinding aborts the process).
            let served = catch_unwind(AssertUnwindSafe(|| {
                rt.block_on(async move {
                    // Convert std listener to tokio listener inside the async context
                    // This ensures the listener is registered with THIS runtime's reactor
                    let mut tcp_listeners = Vec::new();
                    for (std_listener, security) in
                        std_listeners.into_iter().zip(listener_securities)
                    {
                        match tokio::net::TcpListener::from_std(std_listener) {
                            Ok(listener) => tcp_listeners.push((listener, security)),
                            Err(e) => {
                                tracing::error!("Failed to convert std listener to tokio: {}", e);
                                return;
                            }
                        }
                    }

                    if let Err(e) = crate::kafka::listener::run(
                        tcp_listeners,
                        request_lanes,
                        credential_tx,
                        notify_rx,
                        forward_rx,
                        forward_ack_tx,
                        runtime_context_for_network,
                        shutdown_rx,
                    )
                    .await
                    {
                        tracing::error!("TCP listener error: {}", e);
                    }
                })
            }));
            if served.is_err() {
                tracing::error!("pg_kafka network thread panicked; shutting its runtime down");
            }
            if catch_unwind(AssertUnwindSafe(|| {
                rt.shutdown_timeout(Duration::from_secs(1))
            }))
            .is_err()
            {
                tracing::error!("pg_kafka network runtime shutdown panicked");
            }
        })
        .expect("Failed to spawn network thread");
    // wave3 worker: whatever ends the process (a FATAL from SIGTERM during database
    // work, an ERROR escaping to the bgworker's top level, proc_exit) first stops the
    // network thread, bounded, instead of exiting under a running tokio runtime.
    *NETWORK_SHUTDOWN.lock().unwrap_or_else(|p| p.into_inner()) = Some(shutdown_tx.clone());
    unsafe { pg_sys::before_shmem_exit(Some(stop_network_on_exit), pg_sys::Datum::from(0)) };

    // Step 9: Create consumer group coordinator
    // This manages consumer group membership state in-memory
    // Arc<RwLock> makes it safe to access from the coordinator handlers
    let coordinator = std::sync::Arc::new(crate::kafka::GroupCoordinator::new());

    // Everything a request needs, borrowed once for the whole loop.
    let env = RequestEnv {
        coordinator: &coordinator,
        broker_metadata: &broker_metadata,
        default_partitions,
        notify_tx: &notify_tx,
        compression,
        shadow_store: &shadow_store,
        log_timing,
    };

    pg_log!("Entering main event loop (two-thread architecture)");

    // Step 10: Main event loop
    // ═══════════════════════════════════════════════════════════════════════════════
    // MAIN EVENT LOOP: Two-Thread Producer-Consumer Architecture
    // ═══════════════════════════════════════════════════════════════════════════════
    //
    // This loop is now dramatically simpler than the previous time-slicing design:
    //
    // 1. BLOCK ON CHANNEL (Instant Wake-up)
    //    - recv_timeout() blocks until a request arrives OR timeout expires
    //    - No polling, no CPU usage when idle
    //    - Instant wake-up when network thread sends a request
    //
    // 2. PROCESS DATABASE REQUESTS (group commit, see execute_batch)
    //    - The request plus everything already queued behind it (up to 64)
    //      run in ONE BackgroundWorker::transaction(), each in its own
    //      subtransaction
    //    - All their responses are released after the single commit (RB-1)
    //
    // 3. CHECK MEMBER TIMEOUTS (Periodic, ~1 second)
    //    - Scan consumer groups for timed-out members
    //    - Trigger rebalancing if needed
    //
    // 4. CHECK SHUTDOWN SIGNAL (On Each Timeout)
    //    - BackgroundWorker::sigterm_received() checks for Postgres shutdown
    //    - Graceful shutdown when detected
    //
    // The network thread runs independently, handling all TCP I/O in parallel.
    // This eliminates the 100ms latency floor from the previous design.
    // ═══════════════════════════════════════════════════════════════════════════════

    let mut last_timeout_check = Instant::now();
    let mut last_txn_timeout_check = Instant::now();
    let mut last_retention_sweep = Instant::now();
    let mut last_config_reload = Instant::now();
    // SH-9: how often the DB thread drains forward acks and forwards due outbox
    // rows. Short so async forwarding has sub-second latency, but throttled so
    // an idle instance isn't opening an SPI transaction on every 100ms wakeup.
    let outbox_poll_interval = Duration::from_millis(250);
    let mut last_outbox_poll = Instant::now();

    // Phase 11: Initial shadow config load
    load_shadow_config(&shadow_store);

    // Flag to track if SIGHUP was received (triggers immediate config reload)
    let mut sighup_pending = false;

    // Broker request metrics: published to kafka.broker_api_stats / kafka.broker_gauges.
    let mut metrics_flusher = crate::kafka::metrics::MetricsFlusher::new();

    loop {
        // ┌─────────────────────────────────────────────────────────────┐
        // │ Check for Postgres Shutdown Signal                          │
        // └─────────────────────────────────────────────────────────────┘
        if sigterm_received() {
            log!("pg_kafka background worker shutting down");
            shutdown_and_exit(&shutdown_tx, &network_handle, shutdown_timeout);
        }

        // wave3 worker: never outlive the postmaster. An orphaned worker would keep
        // committing Kafka writes and block a new postmaster from starting.
        if !postmaster_alive() {
            log!("pg_kafka background worker exiting: the postmaster died");
            // Stop the network thread (bounded), then leave at once: the shared memory
            // dies with the cluster anyway, and a new postmaster can start only once
            // every process of the old one is gone (proc_exit's cleanup took >1 s).
            let _ = shutdown_tx.send(());
            let start = Instant::now();
            while !network_handle.is_finished() && start.elapsed() < Duration::from_millis(300) {
                std::thread::sleep(Duration::from_millis(5));
            }
            unsafe { _exit(1) };
        }

        // wave3 worker: the extension was dropped (DROP EXTENSION). Answer requests with
        // a retriable error until it exists again, then re-initialize as at start.
        if SCHEMA_LOST.load(Ordering::SeqCst) {
            if !wait_for_schema_while_serving(
                &config.database,
                &request_rx,
                &liveness_rx,
                &credential_rx,
            ) {
                continue; // SIGTERM or postmaster death: handled at the top
            }
            reapply_worker_role();
            features = SchemaFeatures::default();
            refresh_schema_features(&mut features);
            run_partition_maintenance(&features);
            load_shadow_config(&shadow_store);
            last_retention_sweep = Instant::now();
            continue;
        }

        // ┌─────────────────────────────────────────────────────────────┐
        // │ Check for SIGHUP (Config Reload Signal)                     │
        // └─────────────────────────────────────────────────────────────┘
        // When pg_reload_conf() is called, we receive SIGHUP. This triggers
        // an immediate config reload on the next loop iteration, bypassing
        // the normal reload_interval check. This is critical for tests that
        // change GUCs and need the changes to take effect immediately.
        if BackgroundWorker::sighup_received() {
            sighup_pending = true;
            pg_log!("SIGHUP received, config reload pending");
        }

        // ┌─────────────────────────────────────────────────────────────┐
        // │ Configuration Reload (GUCs + Shadow Config)                 │
        // │ CRITICAL: Must happen BEFORE recv_timeout to ensure new     │
        // │ config is available when processing requests that arrive    │
        // │ immediately after pg_reload_conf().                         │
        // └─────────────────────────────────────────────────────────────┘
        // Triggers: Periodic (configurable via config_reload_interval_ms GUC, default 30s)
        // or immediately after SIGHUP.
        // Unified reload cycle:
        // 1. Reload GUCs (runtime-changeable settings like compression, log settings, etc.)
        // 2. Reload shadow topic configurations from database
        // 3. Flush shadow metrics
        // This allows configuration changes to take effect without restarting Postgres.
        // PostMasterContext GUCs (port, host) still require restart.
        let reload_interval_ms = crate::config::CONFIG_RELOAD_INTERVAL_MS.get();
        let reload_interval = Duration::from_millis(reload_interval_ms as u64);
        let should_reload = sighup_pending || last_config_reload.elapsed() >= reload_interval;

        if should_reload {
            if sighup_pending {
                pg_log!(
                    "Immediate config reload triggered by SIGHUP (interval={}ms)",
                    reload_interval_ms
                );
                sighup_pending = false;

                // CRITICAL: Call ProcessConfigFile to actually reload GUC values
                // Without this, GUC changes made via ALTER SYSTEM won't take effect
                // in this background worker. This is the standard PostgreSQL pattern.
                unsafe {
                    pgrx::pg_sys::ProcessConfigFile(pgrx::pg_sys::GucContext::PGC_SIGHUP);
                }
                pg_log!("ProcessConfigFile called, GUCs reloaded from postgresql.auto.conf");
            }
            apply_worker_lock_timeout();

            // wave3 worker: pick up a schema change (ALTER EXTENSION pg_kafka UPDATE).
            if refresh_schema_features(&mut features) {
                reapply_worker_role();
                run_partition_maintenance(&features);
            }

            // auth stream: listener security settings apply at worker start only.
            if let Ok(now) = crate::kafka::auth::SecuritySettings::resolve(
                &crate::kafka::auth::settings::raw_from_gucs(),
                &host,
                port,
            ) {
                if now != security_settings {
                    pg_warning!(
                        "pg_kafka listener security settings changed; they take effect when \
                         the pg_kafka_listener worker restarts (pg_terminate_backend() on it, \
                         or a server restart)"
                    );
                }
            }

            // 1. Reload GUCs (update RuntimeContext cache)
            runtime_context.reload();
            pg_log!("Reloaded GUC configuration");

            // 2. Reload shadow config and flush metrics
            let shadow_reload = &shadow_store;
            let outcome = worker_transaction(|| {
                // RV-10: run this periodic maintenance inside the same subtransaction
                // guard the request path uses, so a Postgres ERROR here (surfaced by
                // pgrx as a panic) rolls back and is logged instead of unwinding out
                // of BackgroundWorker::transaction and aborting the whole bgworker.
                let result = run_request_in_subtransaction(|| {
                    // Reload topic shadow configurations
                    match shadow_reload.load_topic_config_from_db() {
                        Ok(count) => {
                            pg_log!("Reloaded {} shadow topic configurations", count);
                        }
                        Err(e) => {
                            pg_warning!("Failed to reload shadow config: {:?}", e);
                        }
                    }

                    // SH-2: re-check primary/standby status. is_primary() caches its
                    // result for the life of the worker, so without this a standby
                    // promoted to primary would never start forwarding. Refreshing
                    // on the reload cycle picks up a failover within one interval.
                    let is_primary = shadow_reload.refresh_primary_status();
                    tracing::debug!(
                        "Shadow: refreshed primary status (is_primary={})",
                        is_primary
                    );

                    // Flush accumulated metrics to database/logs
                    if let Err(e) = shadow_reload.flush_metrics_to_db() {
                        pg_warning!("Failed to flush shadow metrics: {:?}", e);
                    }
                });
                if let Err(panic_msg) = result {
                    pg_warning!(
                        "Shadow config-reload/metrics maintenance panicked, rolled back (worker survived): {}",
                        panic_msg
                    );
                }
            });
            if let Err(e) = outcome {
                log_txn_error("Shadow config reload", &e);
            }

            last_config_reload = Instant::now();
        }

        // ┌─────────────────────────────────────────────────────────────┐
        // │ Durable shadow outbox: forward due rows + finalize acks (SH-9)│
        // └─────────────────────────────────────────────────────────────┘
        // Runs on the DB thread (SPI). Gated on shadow mode being enabled and
        // the poll interval, so an idle or non-shadow instance does no work.
        if crate::config::SHADOW_MODE_ENABLED.get()
            && last_outbox_poll.elapsed() >= outbox_poll_interval
        {
            let shadow_outbox = &shadow_store;
            let outcome = worker_transaction(|| {
                // RV-10: guard the outbox maintenance with the subtransaction wrapper
                // (see the config-reload block above) so a Postgres ERROR rolls back
                // and is logged rather than aborting the bgworker.
                // Apply results first (marks delivered rows done so the poll won't
                // re-dispatch them), in a subtransaction of their own: the acks are
                // consumed from the channel as they are read, so if they shared the
                // poll's subtransaction a Postgres ERROR in the poll would roll back
                // the applied acks and lose them (delivered rows left pending,
                // re-sent, and eventually dead-lettered).
                let mut applied = 0;
                if let Err(panic_msg) = run_request_in_subtransaction(|| {
                    applied = shadow_outbox.drain_forward_acks();
                }) {
                    pg_warning!(
                        "Shadow ack application panicked, rolled back (worker survived): {}",
                        panic_msg
                    );
                }
                let result = run_request_in_subtransaction(|| {
                    // Then dispatch the next due batch.
                    match shadow_outbox.poll_and_forward_outbox() {
                        Ok(dispatched) => {
                            if applied > 0 || dispatched > 0 {
                                tracing::debug!(
                                    "Shadow outbox: {} ack(s) applied, {} row(s) dispatched",
                                    applied,
                                    dispatched
                                );
                            }
                        }
                        Err(e) => {
                            pg_warning!("Shadow outbox poll failed: {:?}", e);
                        }
                    }
                });
                if let Err(panic_msg) = result {
                    pg_warning!(
                        "Shadow outbox maintenance panicked, rolled back (worker survived): {}",
                        panic_msg
                    );
                }
            });
            if let Err(e) = outcome {
                log_txn_error("Shadow outbox poll", &e);
            }
            last_outbox_poll = Instant::now();
        }

        // ┌─────────────────────────────────────────────────────────────┐
        // │ Wait for Next Request (Blocking with Timeout)              │
        // └─────────────────────────────────────────────────────────────┘
        // This is the key performance improvement: we block until work arrives.
        // No CPU usage when idle, instant wake-up when requests come in.
        // DR-12 (DEEP-REVIEW-2026-07): drain the liveness lane BEFORE taking the
        // next main-lane request. Heartbeats are served from the in-memory
        // coordinator, so each drains in microseconds. Heartbeats that arrive while
        // a main-lane batch runs are answered between its requests (see
        // execute_batch), so heartbeat queueing delay stays bounded by one
        // in-flight request (plus RECV_TIMEOUT when idle), not by the main queue,
        // and heartbeats no longer cut produce batches short (wave 3).
        // One liveness batch (up to 64) per iteration, not "until empty": a flood of
        // heartbeats must not starve the main lane (the rest are served between the
        // main batch's requests or on the next iteration).
        if let Ok(request) = liveness_rx.try_recv() {
            execute_batch(request, &liveness_rx, None, &env);
        }

        // auth stream: also wake for SASL credential lookups, answered in their own
        // short transaction.
        let received = crossbeam_channel::select! {
            recv(request_rx) -> r => r.map_err(|_| RecvTimeoutError::Disconnected),
            recv(credential_rx) -> c => {
                if let Ok(first) = c {
                    serve_credential_requests(first, &credential_rx);
                }
                Err(RecvTimeoutError::Timeout)
            },
            default(RECV_TIMEOUT) => Err(RecvTimeoutError::Timeout),
        };
        match received {
            Ok(request) => {
                // ┌─────────────────────────────────────────────────────┐
                // │ Process Database Requests (group commit)           │
                // └─────────────────────────────────────────────────────┘
                // This request plus whatever is already queued behind it run
                // in ONE top-level transaction (one WAL flush).
                execute_batch(request, &request_rx, Some(&liveness_rx), &env);
            }

            Err(RecvTimeoutError::Timeout) => {
                // No requests within timeout - this is normal, continue to check signals
            }

            Err(RecvTimeoutError::Disconnected) => {
                // Channel closed - the network thread has exited, so nothing is
                // serving the Kafka port. Exit for a restart rather than
                // unregistering the worker for good.
                pg_warning!(
                    "Request channel disconnected (network thread exited); restarting worker"
                );
                exit_for_restart();
            }
        }

        // ┌─────────────────────────────────────────────────────────────┐
        // │ Check Member Timeouts (Periodic, ~1 second)                │
        // └─────────────────────────────────────────────────────────────┘
        if last_timeout_check.elapsed() >= TIMEOUT_CHECK_INTERVAL {
            let removed = coordinator.check_and_remove_timed_out_members();
            for (group_id, member_id) in removed {
                pg_log!(
                    "Timeout: Removed member {} from group {}, triggering rebalance",
                    member_id,
                    group_id
                );
            }
            last_timeout_check = Instant::now();
        }

        // ┌─────────────────────────────────────────────────────────────┐
        // │ Check Transaction Timeouts (Periodic, ~10 seconds)         │
        // └─────────────────────────────────────────────────────────────┘
        if last_txn_timeout_check.elapsed() >= TXN_TIMEOUT_CHECK_INTERVAL {
            // Must run within a worker transaction for SPI access
            let mut aborted_any = false;
            let outcome = worker_transaction(|| {
                // RV-10: guard the timeout sweep with the subtransaction wrapper (see
                // the config-reload block above) so a Postgres ERROR rolls back and is
                // logged rather than aborting the bgworker.
                let result = run_request_in_subtransaction(|| {
                    use crate::kafka::storage::MaintenanceStore;
                    use crate::kafka::PostgresStore;
                    let store = PostgresStore::new();
                    let default_timeout = Duration::from_secs(60);
                    match store.abort_timed_out_transactions(default_timeout) {
                        Ok(aborted) => {
                            aborted_any = !aborted.is_empty();
                            for txn_id in aborted {
                                pg_log!(
                                    "Transaction timeout: Aborted timed-out transaction '{}'",
                                    txn_id
                                );
                            }
                        }
                        Err(e) => {
                            pg_warning!("Failed to check transaction timeouts: {}", e);
                        }
                    }
                });
                if let Err(panic_msg) = result {
                    pg_warning!(
                        "Transaction-timeout maintenance panicked, rolled back (worker survived): {}",
                        panic_msg
                    );
                    aborted_any = false;
                }
            });
            match outcome {
                // wave3 worker: a timeout abort ends transactions too; wake parked
                // read_committed fetches now (the LSO moved), not on the 1 s safety net.
                Ok(()) if aborted_any => notify_transaction_ended(&notify_tx),
                Ok(()) => {}
                Err(e) => log_txn_error("Transaction-timeout check", &e),
            }
            last_txn_timeout_check = Instant::now();
        }

        // ┌─────────────────────────────────────────────────────────────┐
        // │ Storage-Lifecycle Retention Sweep (Periodic, ~60 seconds)  │
        // └─────────────────────────────────────────────────────────────┘
        // DR-1/DR-2 (DEEP-REVIEW-2026-07): before this block, nothing ever deleted
        // anything — cleanup_aborted_messages had no production caller and expired
        // messages / stale producers / terminal txns / delivered outbox rows all
        // accumulated forever. The sweep is bounded per pass and honors the
        // pg_kafka.message_retention_hours GUC (0 = messages kept forever).
        if last_retention_sweep.elapsed() >= RETENTION_SWEEP_INTERVAL {
            // ADR-001: keep the next time partitions ready (and drain the DEFAULT
            // leaf) before the sweep decides which leaves to drop.
            run_partition_maintenance(&features);
            let outcome = worker_transaction(|| {
                // RV-10: guard with the subtransaction wrapper (see the config-reload
                // block above) so a Postgres ERROR rolls back and is logged rather
                // than aborting the bgworker.
                let result = run_request_in_subtransaction(|| {
                    use crate::kafka::storage::postgres::{PostgresStore, ABORTED_MESSAGE_GRACE};
                    let store = PostgresStore::new();
                    let retention_hours = crate::config::MESSAGE_RETENTION_HOURS.get();
                    let partition_minutes = crate::config::MESSAGE_PARTITION_MINUTES.get();
                    match store.run_retention_sweep(
                        retention_hours,
                        ABORTED_MESSAGE_GRACE,
                        partition_minutes,
                    ) {
                        Ok(stats) => {
                            if stats.total() > 0 {
                                pg_log!(
                                    "Retention sweep: {} aborted msg(s), {} expired msg(s), \
                                     {} time partition(s) dropped, \
                                     {} stale producer(s), {} terminal txn(s), \
                                     {} delivered shadow row(s) reclaimed",
                                    stats.aborted_messages,
                                    stats.expired_messages,
                                    stats.dropped_partitions,
                                    stats.stale_producers,
                                    stats.terminal_transactions,
                                    stats.shadow_delivered_rows
                                );
                            }
                        }
                        Err(e) => {
                            pg_warning!("Retention sweep failed: {}", e);
                        }
                    }
                });
                if let Err(panic_msg) = result {
                    pg_warning!(
                        "Retention-sweep maintenance panicked, rolled back (worker survived): {}",
                        panic_msg
                    );
                }
            });
            if let Err(e) = outcome {
                log_txn_error("Retention sweep", &e);
            }
            last_retention_sweep = Instant::now();
        }

        // ┌─────────────────────────────────────────────────────────────┐
        // │ Publish Broker Metrics (≤ once per second, only if changed) │
        // └─────────────────────────────────────────────────────────────┘
        // One upsert statement per flush; no SQL at all while nothing changed.
        // Skipped on a schema without the metrics tables (older extension version).
        let metrics_batch = if features.broker_metrics {
            metrics_flusher.poll(request_rx.len(), liveness_rx.len())
        } else {
            None
        };
        if let Some(batch) = metrics_batch {
            let written = worker_transaction(|| {
                let mut written = Ok(());
                let guard = run_request_in_subtransaction(|| {
                    written = crate::kafka::metrics::write_batch(&batch);
                });
                guard.and(written)
            });
            let written = match written {
                Ok(w) => w,
                // The schema wait reports a dropped extension; nothing to log here.
                Err(TxnError::SchemaGone) => continue,
                Err(e) => Err(format!("{:?}", e)),
            };
            if let Some(msg) = metrics_flusher.finish(batch, written) {
                pg_warning!("{}", msg);
            }
        }
    }
}

/// (Re)load the per-topic shadow configuration into the ShadowStore cache.
fn load_shadow_config(shadow_store: &std::sync::Arc<ShadowStore<PostgresStore>>) {
    let outcome = worker_transaction(|| match shadow_store.load_topic_config_from_db() {
        Ok(count) => {
            if count > 0 {
                log!("Loaded {} shadow topic configurations", count);
            }
        }
        Err(e) => {
            pg_warning!("Failed to load shadow config: {:?}", e);
        }
    });
    if let Err(e) = outcome {
        log_txn_error("Shadow config load", &e);
    }
}

/// Tell the network thread that transactions ended, so parked read_committed fetches
/// re-check their last stable offset now. Best effort: a full channel only means the
/// long-poll safety net picks it up.
fn notify_transaction_ended(
    notify_tx: &crossbeam_channel::Sender<crate::kafka::InternalNotification>,
) {
    let _ = notify_tx.try_send(crate::kafka::InternalNotification::TransactionEnded);
}

/// Whether a committed batch ended (or may have ended) transactions other than through
/// EndTxn, which notifies by itself: InitProducerId fences and aborts the previous
/// epoch's open transaction.
fn batch_may_end_transactions(api_keys: &[i16]) -> bool {
    api_keys.contains(&API_KEY_INIT_PRODUCER_ID)
}

/// The extension was dropped while the worker ran. Keep the listener up and answer
/// every request with a retriable KAFKA_STORAGE_ERROR (credential lookups fail) until
/// the schema exists again, re-checking every `SCHEMA_CHECK_INTERVAL`. Returns true
/// once it exists, false on SIGTERM or postmaster death (the caller's loop handles
/// those).
fn wait_for_schema_while_serving(
    database: &str,
    request_rx: &crossbeam_channel::Receiver<crate::kafka::KafkaRequest>,
    liveness_rx: &crossbeam_channel::Receiver<crate::kafka::KafkaRequest>,
    credential_rx: &crossbeam_channel::Receiver<crate::kafka::auth::CredentialRequest>,
) -> bool {
    log!(
        "pg_kafka extension is gone from database '{}' (DROP EXTENSION?); answering requests \
         with KAFKA_STORAGE_ERROR until CREATE EXTENSION pg_kafka",
        database
    );
    // Back to the bootstrap superuser while waiting: whatever schema appears next is
    // checked (and its grants re-evaluated by reapply_worker_role) from there.
    let _: Result<(), pgrx::spi::Error> = BackgroundWorker::transaction(|| {
        Spi::run("SELECT pg_catalog.set_config('role', 'none', false)")
    });
    let started = Instant::now();
    let mut next_check = Instant::now();
    loop {
        if sigterm_received() || !postmaster_alive() {
            return false;
        }
        if Instant::now() >= next_check {
            if BackgroundWorker::transaction(|| verify_schema(database)).is_ok() {
                // The next worker transaction records the (new) extension's OID.
                KNOWN_EXTENSION_OID.store(0, Ordering::SeqCst);
                SCHEMA_LOST.store(false, Ordering::SeqCst);
                log!(
                    "pg_kafka schema found again after {} ms; resuming",
                    started.elapsed().as_millis()
                );
                return true;
            }
            next_check = Instant::now() + SCHEMA_CHECK_INTERVAL;
        }
        while let Ok(r) = liveness_rx.try_recv() {
            reject_request(r, ERROR_KAFKA_STORAGE_ERROR);
        }
        while let Ok(c) = credential_rx.try_recv() {
            let _ = c.reply.send(Err("pg_kafka schema is missing".to_string()));
        }
        crossbeam_channel::select! {
            recv(request_rx) -> r => {
                match r {
                    Ok(r) => reject_request(r, ERROR_KAFKA_STORAGE_ERROR),
                    Err(_) => std::thread::sleep(RECV_TIMEOUT), // network thread gone
                }
            },
            default(RECV_TIMEOUT) => {},
        }
    }
}

/// Most requests one top-level transaction carries (group commit).
///
/// Each request runs in its own subtransaction, and every one that writes is
/// assigned a subtransaction XID. 64 is `PGPROC_MAX_CACHED_SUBXIDS`, so a batch
/// never overflows the worker's subxid cache (an overflowed cache forces other
/// backends' snapshot checks into `pg_subtrans` lookups until it commits).
const MAX_GROUP_COMMIT_REQUESTS: usize = 64;

/// A batch stops taking more requests once it has run this long, so group
/// commit delays the first request's response by at most about this much (plus
/// the request still running and the commit).
const GROUP_COMMIT_TIME_BUDGET: Duration = Duration::from_millis(50);

/// Everything `process_request` needs, borrowed once by the main loop.
struct RequestEnv<'a> {
    coordinator: &'a std::sync::Arc<crate::kafka::GroupCoordinator>,
    broker_metadata: &'a crate::kafka::BrokerMetadata,
    default_partitions: i32,
    notify_tx: &'a crossbeam_channel::Sender<crate::kafka::InternalNotification>,
    compression: kafka_protocol::records::Compression,
    shadow_store: &'a std::sync::Arc<ShadowStore<PostgresStore>>,
    log_timing: bool,
}

/// Whether a group-commit batch should take another already-queued request.
///
/// It stops at the request cap or at the time budget. A waiting Heartbeat no longer
/// ends it (wave 3): heartbeats are answered between the batch's requests instead
/// ([`serve_liveness_in_batch`]), so liveness traffic still waits behind at most one
/// in-flight request, and many heartbeating consumers no longer shrink produce
/// batches to one request each.
fn batch_has_room(taken: usize, elapsed: Duration) -> bool {
    taken < MAX_GROUP_COMMIT_REQUESTS && elapsed < GROUP_COMMIT_TIME_BUDGET
}

/// Answer every Heartbeat waiting on the liveness lane from inside a running batch.
///
/// A Heartbeat only reads and updates the in-memory coordinator (no SPI, nothing to
/// commit), so its response does not need the RB-1 commit barrier: it runs in its
/// own subtransaction like any request (an empty one, no XID) and its response is
/// released at once. Returns how many it served. `serve` runs one heartbeat (the
/// worker passes [`serve_heartbeat_now`]; unit tests pass a fake).
fn serve_liveness_in_batch(
    liveness: &crossbeam_channel::Receiver<crate::kafka::KafkaRequest>,
    mut serve: impl FnMut(crate::kafka::KafkaRequest),
) -> usize {
    // At most what was waiting on entry (and never more than one batch's worth), so a
    // stream of heartbeats cannot keep the batch from its own work.
    let limit = liveness.len().min(MAX_GROUP_COMMIT_REQUESTS);
    let mut served = 0;
    while served < limit {
        match liveness.try_recv() {
            Ok(request) => serve(request),
            Err(_) => break,
        }
        served += 1;
    }
    served
}

/// Run one liveness-lane request inside the current transaction and release its
/// response immediately (see [`serve_liveness_in_batch`]).
fn serve_heartbeat_now(mut request: crate::kafka::KafkaRequest, env: &RequestEnv) {
    let buffered = BufferedResponse::intercept(&mut request);
    let _ = run_one_request(request, env);
    buffered.release();
}

/// The RB-1 response buffer for one request in a batch.
///
/// ## RB-1: response-after-commit barrier
///
/// Invariant: **no response may reach the client before
/// `BackgroundWorker::transaction` has returned** (i.e. before the commit is
/// durable and visible to other Postgres connections). Handlers still "send"
/// their response as always, but the request's `response_tx` is swapped for a
/// same-typed buffer channel before dispatch; the buffered response is
/// forwarded to the real client sender only after the transaction commits.
/// Without this barrier an acked produce/offset-commit could be lost on a
/// crash between response send and commit (acks>=1 durability violation), and
/// a client that received an OK could race a separate Postgres connection into
/// pre-commit state. With group commit the barrier covers the whole batch: no
/// response of any request in it is released before the one commit.
///
/// This relies on the writer task queueing response slots at parse time
/// (listener.rs), so deferring the send cannot reorder responses. If that
/// ordering scheme ever changes, re-review this barrier.
struct BufferedResponse {
    client_tx: tokio::sync::mpsc::UnboundedSender<crate::kafka::KafkaResponse>,
    buffer_rx: tokio::sync::mpsc::UnboundedReceiver<crate::kafka::KafkaResponse>,
    api_version: i16,
    correlation_id: i32,
    /// What the request's error response echoes (its topics/partitions/groups), for
    /// a commit failure.
    echo: crate::kafka::api::Echo,
    /// False for an acks=0 produce: the client expects no response frame at all,
    /// not even an error.
    expects_response: bool,
    /// Per-request metrics: started when the request is taken into the batch,
    /// recorded when this value is dropped (after release or fail), so latency
    /// covers the handler and the batch commit.
    metrics: crate::kafka::metrics::RequestTimer<'static>,
}

impl BufferedResponse {
    /// Swap the request's client sender for a buffer the handler writes into.
    /// Both ends of the buffer live on this one DB thread — tokio's unbounded
    /// send/try_recv are plain synchronous calls, so no runtime or extra
    /// synchronization is involved.
    fn intercept(request: &mut crate::kafka::KafkaRequest) -> Self {
        let api_key = request.header.api_key;
        let (buffer_tx, buffer_rx) = tokio::sync::mpsc::unbounded_channel();
        let client_tx = request.swap_response_tx(buffer_tx);
        BufferedResponse {
            client_tx,
            buffer_rx,
            api_version: request.header.api_version,
            correlation_id: request.header.correlation_id,
            echo: request.body.echo(),
            expects_response: request.body.expects_response(),
            metrics: crate::kafka::metrics::RequestTimer::start(api_key),
        }
    }

    /// The batch committed: forward the buffered response(s) to the client, in
    /// order. Today a request yields at most one response, but the loop keeps a
    /// future multi-response request from silently dropping frames.
    fn release(mut self) {
        while let Ok(response) = self.buffer_rx.try_recv() {
            self.metrics.observe(&response);
            if self.client_tx.send(response).is_err() {
                pg_warning!(
                    "Client disconnected before response flush (correlation_id={})",
                    self.correlation_id
                );
            }
        }
    }

    /// The batch's commit failed. Any buffered response asserts success for work
    /// that did NOT commit — sending it would recreate the lost-ack bug at
    /// commit-failure granularity. Discard it and send an API-typed error frame
    /// instead (pure Rust construction + channel send; safe before Postgres
    /// error-state cleanup). An acks=0 produce gets nothing.
    fn fail(mut self, error_code: i16) {
        while self.buffer_rx.try_recv().is_ok() {}
        self.metrics.mark_error();
        if !self.expects_response {
            return;
        }
        // The API's own error response, echoing the request's topics/partitions/groups
        // so an API without a top-level error code still reports the failure.
        let error_response = crate::kafka::KafkaResponse::new(
            self.correlation_id,
            self.api_version,
            self.echo.error_response(self.api_version, error_code),
        );
        let _ = self.client_tx.send(error_response);
    }
}

/// Run one request of a batch in its own subtransaction (so a handler error or
/// panic rolls back only that request) and log a caught panic.
fn run_one_request(
    request: crate::kafka::KafkaRequest,
    env: &RequestEnv,
) -> Result<RequestOutcome, String> {
    // New request, new sync-forward budget (see ShadowStore::begin_request).
    env.shadow_store.begin_request();
    let result = run_request_in_subtransaction(|| {
        // Each request reads from its own snapshot. pgrx runs read-only SPI statements
        // (the transaction has not written yet) on the active snapshot, which
        // `BackgroundWorker::transaction` pushes once when the batch starts; without this,
        // a request that joins a batch late would not see rows other sessions committed
        // since (e.g. a topic created by SQL just before the client asked for it). In READ
        // COMMITTED, GetTransactionSnapshot takes a fresh snapshot. A panic unwinds to the
        // subtransaction abort, which pops snapshots pushed inside it.
        unsafe { pg_sys::PushActiveSnapshot(pg_sys::GetTransactionSnapshot()) };
        let outcome = process_request(
            request,
            env.coordinator,
            env.broker_metadata,
            env.default_partitions,
            env.notify_tx,
            env.compression,
            env.shadow_store,
        );
        unsafe { pg_sys::PopActiveSnapshot() };
        outcome
    });
    // A reported rollback (handler error) already logged itself and sent its
    // response; only a real panic is worth a warning here.
    if let Err(panic_msg) = &result {
        pg_warning!(
            "Handler panic caught, request rolled back (worker survived): {}",
            panic_msg
        );
    }
    result
}

/// RB-1 test hook: widen the response-send→commit window so the E2E barrier
/// test can prove the ack implies a visible commit. Produce-gated so heartbeats
/// (DR-12 lane) and unrelated traffic are unaffected while a test has the GUC
/// set. Applied once per batch, just before its commit.
fn apply_test_pre_commit_delay(batch_has_produce: bool) {
    if batch_has_produce {
        let delay_ms = crate::config::TEST_PRE_COMMIT_DELAY_MS.get();
        if delay_ms > 0 {
            std::thread::sleep(Duration::from_millis(delay_ms as u64));
        }
    }
}

/// The batch's transaction committed: release every buffered response, in
/// arrival order.
fn after_batch_commit(pending: Vec<BufferedResponse>) {
    for response in pending {
        response.release();
    }
}

/// The batch's commit failed: every request in it gets an API-typed error.
fn after_batch_commit_failure(pending: Vec<BufferedResponse>, error_code: i16) {
    for response in pending {
        response.fail(error_code);
    }
}

/// Execute `first` plus every request already queued behind it on `source`
/// (no waiting for more) in ONE top-level transaction: group commit.
///
/// Each request used to be its own top-level transaction and therefore its own
/// WAL flush, which capped produce throughput at the fsync rate (~220
/// requests/s). Now a batch pays one flush. Each request still runs in its own
/// subtransaction, so a handler error or panic rolls back only that request; the
/// responses of all requests are buffered and released only after the single
/// commit (RB-1, see [`BufferedResponse`]). A batch takes at most
/// `MAX_GROUP_COMMIT_REQUESTS` requests and stops early on
/// `GROUP_COMMIT_TIME_BUDGET` or when `yield_to` (the liveness lane) has work.
///
/// Used for both lanes: the DR-12 liveness lane passes `yield_to = None` (a
/// Heartbeat does no SPI, but taking the same guarded path keeps one code path
/// and costs only an empty transaction).
fn execute_batch(
    first: crate::kafka::KafkaRequest,
    source: &crossbeam_channel::Receiver<crate::kafka::KafkaRequest>,
    yield_to: Option<&crossbeam_channel::Receiver<crate::kafka::KafkaRequest>>,
    env: &RequestEnv,
) {
    let batch_start = Instant::now();
    let mut pending: Vec<BufferedResponse> = Vec::new();
    let mut api_keys: Vec<i16> = Vec::new();
    // Time spent inside handlers (timing instrumentation, pg_kafka.log_timing).
    let mut handler_time = Duration::ZERO;
    let mut first = Some(first);

    // worker_transaction takes the schema locks first; a Postgres ERROR outside the
    // requests' subtransactions (a lock timeout there, or the COMMIT itself) rolls the
    // whole batch back and is reported as Err, and the worker carries on.
    let txn_result = worker_transaction(|| {
        let mut next = first.take();
        while let Some(mut request) = next.take() {
            api_keys.push(request.header.api_key);
            pending.push(BufferedResponse::intercept(&mut request));

            let handler_start = Instant::now();
            let _ = run_one_request(request, env);
            handler_time += handler_start.elapsed();

            // wave3 worker: heartbeats that arrived meanwhile are answered now, and
            // the batch goes on taking queued work.
            if let Some(liveness) = yield_to {
                serve_liveness_in_batch(liveness, |hb| serve_heartbeat_now(hb, env));
            }
            if batch_has_room(pending.len(), batch_start.elapsed()) {
                next = source.try_recv().ok();
            }
        }
        apply_test_pre_commit_delay(api_keys.contains(&API_KEY_PRODUCE));
    });

    // Sync-mode shadow forwards go out only now that the local writes are committed;
    // responses are released after the (bounded) external wait. After a failed batch
    // this only discards the queue (forwards of uncommitted transactions are skipped).
    env.shadow_store.forward_sync_after_commit();
    let batch_size = pending.len();
    match txn_result {
        Ok(()) => {
            after_batch_commit(pending);
            if batch_may_end_transactions(&api_keys) {
                notify_transaction_ended(env.notify_tx);
            }
        }
        Err(err) => {
            // Nothing of the batch committed. Every request gets an API-typed error
            // (a retriable one) instead of its buffered success; a request the batch
            // never started (the schema locks failed first) is rejected the same way.
            log_txn_error("Request batch", &err);
            let code = err.kafka_error_code();
            after_batch_commit_failure(pending, code);
            if let Some(request) = first.take() {
                reject_request(request, code);
            }
        }
    }

    if env.log_timing {
        let total = batch_start.elapsed();
        let tx_overhead = total.saturating_sub(handler_time);
        log!(
            "TIMING: batch={} total={}us handler={}us tx_overhead={}us ({}%)",
            batch_size,
            total.as_micros(),
            handler_time.as_micros(),
            tx_overhead.as_micros(),
            if total.as_micros() > 0 {
                (tx_overhead.as_micros() * 100) / total.as_micros()
            } else {
                0
            }
        );
    }
}

/// Run one request on the DB thread: build its handler context and hand it to the
/// API's [`crate::kafka::api::KafkaApi::handle`] through the registry.
///
/// # Testing
/// This function is public to allow unit testing, but is not part of the
/// public API and should not be called directly by external code.
#[doc(hidden)]
#[allow(clippy::too_many_arguments)]
pub fn process_request(
    request: crate::kafka::KafkaRequest,
    coordinator: &std::sync::Arc<crate::kafka::GroupCoordinator>,
    broker_metadata: &crate::kafka::BrokerMetadata,
    default_partitions: i32,
    notify_tx: &crossbeam_channel::Sender<crate::kafka::InternalNotification>,
    compression: kafka_protocol::records::Compression,
    shadow_store: &std::sync::Arc<ShadowStore<PostgresStore>>,
) -> RequestOutcome {
    // ShadowStore wraps PostgresStore and handles shadow mode internally.
    // It checks the GUC at runtime to decide whether to forward.
    let store: &dyn KafkaStore = shadow_store.as_ref();
    let env = crate::kafka::api::ApiEnv {
        ctx: crate::kafka::HandlerContext::new(
            store,
            coordinator,
            broker_metadata,
            default_partitions,
            compression,
        ),
        notify_tx,
    };
    crate::kafka::api::dispatch(request, &env)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::api::{
        ApiVersionsRequestData, ProduceRequestData, RequestBody, RequestHeader, ResponseBody,
    };

    #[test]
    fn test_batch_has_room_stops_at_cap_and_budget_only() {
        assert!(batch_has_room(1, Duration::ZERO));
        assert!(batch_has_room(
            MAX_GROUP_COMMIT_REQUESTS - 1,
            Duration::from_millis(1)
        ));
        // Request cap.
        assert!(!batch_has_room(MAX_GROUP_COMMIT_REQUESTS, Duration::ZERO));
        // Time budget.
        assert!(!batch_has_room(1, GROUP_COMMIT_TIME_BUDGET));
        // The cap keeps a batch within the subxid cache.
        const { assert!(MAX_GROUP_COMMIT_REQUESTS <= 64) };
    }

    /// wave3 worker (item 7): heartbeats waiting on the liveness lane are served from
    /// inside the batch, and the batch keeps taking main-lane work. Before, the first
    /// waiting heartbeat ended the batch (`batch_has_room(.., liveness_waiting=true)`
    /// was false), so a steady heartbeat stream cut every batch to one request.
    #[test]
    fn test_heartbeats_are_served_inside_a_batch_without_ending_it() {
        let (main_tx, main_rx) = crossbeam_channel::unbounded();
        let (hb_tx, hb_rx) = crossbeam_channel::unbounded();
        for i in 0..10 {
            main_tx.send(api_versions_request(i).0).unwrap();
            // A heartbeat arrives behind every main-lane request.
            hb_tx.send(api_versions_request(100 + i).0).unwrap();
        }
        let mut batch = Vec::new();
        let mut heartbeats = Vec::new();
        let start = Instant::now();
        let mut next = main_rx.try_recv().ok();
        while let Some(r) = next.take() {
            batch.push(r.header.correlation_id);
            serve_liveness_in_batch(&hb_rx, |hb| heartbeats.push(hb.header.correlation_id));
            if batch_has_room(batch.len(), start.elapsed()) {
                next = main_rx.try_recv().ok();
            }
        }
        assert_eq!(batch, (0..10).collect::<Vec<_>>(), "one batch took all ten");
        assert_eq!(
            heartbeats,
            (100..110).collect::<Vec<_>>(),
            "every heartbeat served"
        );
        assert!(hb_rx.is_empty());
    }

    /// A flood of heartbeats is served at most one batch's worth per slot, so the batch
    /// still gets to its own requests.
    #[test]
    fn test_heartbeat_service_per_slot_is_bounded() {
        let (hb_tx, hb_rx) = crossbeam_channel::unbounded();
        for i in 0..(3 * MAX_GROUP_COMMIT_REQUESTS as i32) {
            hb_tx.send(api_versions_request(i).0).unwrap();
        }
        let served = serve_liveness_in_batch(&hb_rx, |_| {});
        assert_eq!(served, MAX_GROUP_COMMIT_REQUESTS);
        assert_eq!(hb_rx.len(), 2 * MAX_GROUP_COMMIT_REQUESTS);
    }

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
    fn test_init_producer_id_batches_notify_transaction_end() {
        assert!(batch_may_end_transactions(&[
            API_KEY_PRODUCE,
            API_KEY_INIT_PRODUCER_ID
        ]));
        assert!(!batch_may_end_transactions(&[
            API_KEY_PRODUCE,
            API_KEY_FETCH
        ]));
        assert!(!batch_may_end_transactions(&[]));
    }

    #[test]
    fn test_schema_features_missing_lists_what_an_old_schema_lacks() {
        let full = SchemaFeatures {
            version: "0.1.0".into(),
            time_partitions: true,
            broker_metrics: true,
            sasl_credentials: true,
        };
        assert!(full.missing().is_empty());
        let old = SchemaFeatures {
            version: "0.0.0".into(),
            ..Default::default()
        };
        let missing = old.missing().join("; ");
        assert!(missing.contains("time partitions"));
        assert!(missing.contains("broker metrics"));
        assert!(missing.contains("SASL users"));
    }

    fn request(
        api_key: i16,
        correlation_id: i32,
        body: RequestBody,
    ) -> (
        crate::kafka::KafkaRequest,
        tokio::sync::mpsc::UnboundedReceiver<crate::kafka::KafkaResponse>,
    ) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        (
            crate::kafka::KafkaRequest {
                header: RequestHeader {
                    api_key,
                    api_version: 7,
                    correlation_id,
                    client_id: None,
                },
                body,
                response_tx: tx,
            },
            rx,
        )
    }

    fn api_versions_request(
        correlation_id: i32,
    ) -> (
        crate::kafka::KafkaRequest,
        tokio::sync::mpsc::UnboundedReceiver<crate::kafka::KafkaResponse>,
    ) {
        let (mut r, rx) = request(
            API_KEY_API_VERSIONS,
            correlation_id,
            RequestBody::ApiVersions(ApiVersionsRequestData),
        );
        r.header.api_version = 3;
        (r, rx)
    }

    fn produce_request(
        correlation_id: i32,
        acks: i16,
    ) -> (
        crate::kafka::KafkaRequest,
        tokio::sync::mpsc::UnboundedReceiver<crate::kafka::KafkaResponse>,
    ) {
        request(
            API_KEY_PRODUCE,
            correlation_id,
            RequestBody::Produce(ProduceRequestData {
                acks,
                timeout_ms: 1000,
                topic_data: vec![],
                transactional_id: None,
            }),
        )
    }

    /// What a handler does: send its (success) response on the request's (swapped)
    /// response channel.
    fn handler_sends(request: crate::kafka::KafkaRequest, correlation_id: i32) {
        request
            .response_tx
            .send(crate::kafka::KafkaResponse::new(
                correlation_id,
                0,
                ResponseBody::UnknownApi { error_code: 0 },
            ))
            .unwrap();
    }

    #[test]
    fn test_buffered_responses_are_held_until_commit_then_released_in_order() {
        // RB-1 with group commit: nothing reaches a client before the batch's
        // commit; after it, every request's response is released.
        let (mut a, mut a_rx) = api_versions_request(1);
        let (mut b, mut b_rx) = api_versions_request(2);
        let pending = vec![
            BufferedResponse::intercept(&mut a),
            BufferedResponse::intercept(&mut b),
        ];
        handler_sends(a, 1);
        handler_sends(b, 2);
        assert!(a_rx.try_recv().is_err(), "no response before commit");
        assert!(b_rx.try_recv().is_err(), "no response before commit");

        after_batch_commit(pending);
        for (rx, expected) in [(&mut a_rx, 1), (&mut b_rx, 2)] {
            let response = rx.try_recv().expect("released after commit");
            assert_eq!(response.correlation_id, expected);
            assert!(
                matches!(response.body, ResponseBody::UnknownApi { error_code: 0 }),
                "the handler's own (success) response"
            );
        }
    }

    #[test]
    fn test_commit_failure_replaces_every_response_with_an_error() {
        let (mut a, mut a_rx) = api_versions_request(11);
        let (mut p, mut p_rx) = produce_request(12, 1);
        let pending = vec![
            BufferedResponse::intercept(&mut a),
            BufferedResponse::intercept(&mut p),
        ];
        handler_sends(a, 11);
        handler_sends(p, 12);

        after_batch_commit_failure(pending, ERROR_KAFKA_STORAGE_ERROR);
        // Each request gets its own API's error response — never the buffered success.
        let response = a_rx.try_recv().expect("an error response");
        assert_eq!(response.correlation_id, 11);
        match response.body {
            ResponseBody::ApiVersions(r) => assert_eq!(r.error_code, ERROR_KAFKA_STORAGE_ERROR),
            other => panic!("expected an ApiVersions error, got {:?}", other),
        }
        assert!(a_rx.try_recv().is_err(), "the buffered success is dropped");
        // Produce (acks=1): a Produce-typed error.
        let response = p_rx.try_recv().expect("an error response");
        assert_eq!(response.correlation_id, 12);
        assert!(matches!(response.body, ResponseBody::Produce(_)));
        assert!(p_rx.try_recv().is_err());
    }

    #[test]
    fn test_commit_failure_sends_nothing_to_acks0_produce() {
        // An acks=0 client expects no response frame at all; an error frame would
        // be read as the answer to its next request.
        let (mut p, mut p_rx) = produce_request(21, 0);
        let pending = vec![BufferedResponse::intercept(&mut p)];
        drop(p); // the acks=0 path sends nothing
        after_batch_commit_failure(pending, ERROR_KAFKA_STORAGE_ERROR);
        assert!(p_rx.try_recv().is_err());
    }

    #[test]
    fn test_schema_wait_logs_first_check_then_throttles() {
        // First failed check always logs.
        assert!(schema_wait_should_log(Duration::ZERO, None));
        // Within the interval after the last log line: stay quiet.
        assert!(!schema_wait_should_log(
            Duration::from_secs(30),
            Some(Duration::ZERO)
        ));
        // Once the interval has passed: log again.
        assert!(schema_wait_should_log(
            SCHEMA_WAIT_LOG_INTERVAL,
            Some(Duration::ZERO)
        ));
        assert!(schema_wait_should_log(
            Duration::from_secs(185),
            Some(Duration::from_secs(120))
        ));
        assert!(!schema_wait_should_log(
            Duration::from_secs(150),
            Some(Duration::from_secs(120))
        ));
    }
}
