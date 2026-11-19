//! Process lifecycle of the worker: the SIGTERM handler, the postmaster-death check, stopping
//! the network thread, and every way the process exits.
//!
//! Every exit uses status 1 so the postmaster restarts the worker after
//! [`WORKER_RESTART_INTERVAL`]; see [`exit_for_restart`].

use pgrx::prelude::*;
use std::panic::catch_unwind;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::Sender;
use std::sync::Mutex;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// How long the postmaster waits before restarting a worker that exited (registered in
/// `_PG_init`).
pub(crate) const WORKER_RESTART_INTERVAL: Duration = Duration::from_secs(5);

/// How long the `before_shmem_exit` hook waits for the network thread, in ms, unless
/// [`shutdown_and_exit`] set a different bound.
const DEFAULT_NETWORK_EXIT_WAIT_MS: u64 = 2000;

/// How long a worker whose postmaster died waits for the network thread before `_exit`.
/// Short: a new postmaster can start only once every process of the old one is gone.
const POSTMASTER_DEATH_NETWORK_WAIT: Duration = Duration::from_millis(300);

/// Poll interval while waiting for the network thread to finish.
const NETWORK_EXIT_POLL: Duration = Duration::from_millis(10);

/// Poll interval while waiting for the network thread after the postmaster died.
const POSTMASTER_DEATH_POLL: Duration = Duration::from_millis(5);

/// Set by [`handle_sigterm`]; never reset (the worker exits once it is set).
static GOT_SIGTERM: AtomicBool = AtomicBool::new(false);

/// True while the worker is inside a database transaction: a SIGTERM then also sets
/// Postgres's ProcDiePending, so a statement or lock wait is interrupted at its next
/// CHECK_FOR_INTERRUPTS (FATAL, exit status 1, restarted by the postmaster). Outside
/// database work the main loop sees [`sigterm_received`] and shuts down gracefully.
static IN_DB_WORK: AtomicBool = AtomicBool::new(false);

/// The network thread's shutdown sender, for [`stop_network_on_exit`].
static NETWORK_SHUTDOWN: Mutex<Option<Sender<()>>> = Mutex::new(None);

/// Set when the network thread has finished (its runtime is shut down).
static NETWORK_FINISHED: AtomicBool = AtomicBool::new(false);

/// How long [`stop_network_on_exit`] waits for the network thread, in ms.
static NETWORK_EXIT_WAIT_MS: AtomicU64 = AtomicU64::new(DEFAULT_NETWORK_EXIT_WAIT_MS);

extern "C" {
    // Exported by the postgres executable (pmsignal.c, latch support); not in pgrx's
    // bindings. Neither raises a Postgres error.
    fn PostmasterIsAliveInternal() -> bool;
    fn _exit(status: std::ffi::c_int) -> !;
    #[link_name = "SetLatch"]
    fn RawSetLatch(latch: *mut pg_sys::Latch);
    #[cfg(target_os = "linux")]
    fn __errno_location() -> *mut std::ffi::c_int;
}

/// Ask Postgres to die at its next CHECK_FOR_INTERRUPTS, as its own `die()` does.
///
/// # Safety
/// Writes two `volatile sig_atomic_t` flags of the backend, which is async-signal-safe
/// and allowed from any thread of the worker process.
unsafe fn request_proc_die() {
    // SAFETY: plain volatile stores to the backend's interrupt flags (see above).
    unsafe {
        (&raw mut pg_sys::InterruptPending).write_volatile(1);
        (&raw mut pg_sys::ProcDiePending).write_volatile(1);
    }
}

/// SIGTERM handler. pgrx's handler only sets a flag the main loop polls between
/// requests, so a worker blocked in SQL or a lock wait could not be stopped. This one
/// also requests ProcDie while database work is in progress (what Postgres's own
/// `die()` does), and keeps errno intact like Postgres's handlers.
unsafe extern "C-unwind" fn handle_sigterm(_signal: std::ffi::c_int) {
    // SAFETY: __errno_location returns this thread's errno slot, valid for its lifetime.
    #[cfg(target_os = "linux")]
    let saved_errno = unsafe { *__errno_location() };
    GOT_SIGTERM.store(true, Ordering::SeqCst);
    // SAFETY: async-signal-safe volatile store to the backend's shutdown flag.
    unsafe { (&raw mut pg_sys::ShutdownRequestPending).write_volatile(1) };
    if IN_DB_WORK.load(Ordering::SeqCst) {
        // SAFETY: see request_proc_die.
        unsafe { request_proc_die() };
    }
    // The raw C function, not pgrx's wrapper: the wrapper checks it runs on the main
    // thread and panics otherwise, and a panic in a signal handler unwinds into
    // whatever that thread was doing.
    // SAFETY: MyLatch is set before signal handlers are attached; SetLatch is
    // async-signal-safe.
    unsafe { RawSetLatch(pg_sys::MyLatch) };
    // SAFETY: as above, this thread's errno slot.
    #[cfg(target_os = "linux")]
    unsafe {
        *__errno_location() = saved_errno;
    }
}

/// Replace pgrx's SIGTERM handler with [`handle_sigterm`] (after
/// `attach_signal_handlers`, which unblocks signals).
pub(super) fn install_sigterm_handler() {
    #[expect(clippy::cast_possible_wrap, reason = "SIGTERM is 15")]
    let sigterm = pg_sys::SIGTERM as i32;
    // SAFETY: installs a handler that only touches async-signal-safe state.
    unsafe {
        #[cfg(not(feature = "pg18"))]
        pg_sys::pqsignal(sigterm, Some(handle_sigterm));
        #[cfg(feature = "pg18")]
        pg_sys::pqsignal_be(sigterm, Some(handle_sigterm));
    }
}

/// Whether SIGTERM has arrived (sticky).
pub(super) fn sigterm_received() -> bool {
    GOT_SIGTERM.load(Ordering::SeqCst)
}

/// Mark the start of database work: from now on a SIGTERM interrupts the backend. A
/// SIGTERM that arrived just before interrupts the work that is starting, too.
pub(super) fn begin_db_work() {
    IN_DB_WORK.store(true, Ordering::SeqCst);
    if sigterm_received() {
        // SAFETY: see request_proc_die.
        unsafe { request_proc_die() };
    }
}

/// Mark the end of database work (see [`begin_db_work`]).
pub(super) fn end_db_work() {
    IN_DB_WORK.store(false, Ordering::SeqCst);
}

/// Whether the postmaster is still alive (one non-blocking read on the postmaster-death
/// pipe). The worker must not outlive it: an orphan would keep committing Kafka writes
/// and block a new postmaster from starting.
pub(super) fn postmaster_alive() -> bool {
    // SAFETY: reads the postmaster-death pipe; raises no Postgres error.
    unsafe { PostmasterIsAliveInternal() }
}

/// Sets `NETWORK_FINISHED` when the network thread's closure ends, however it ends.
pub(super) struct NetworkFinished;

impl Drop for NetworkFinished {
    fn drop(&mut self) {
        NETWORK_FINISHED.store(true, Ordering::SeqCst);
    }
}

/// Make whatever ends the process (a FATAL from SIGTERM during database work, an ERROR
/// escaping to the bgworker's top level, `proc_exit`) first stop the network thread,
/// bounded, instead of exiting under a running tokio runtime.
pub(super) fn stop_network_thread_on_exit(shutdown_tx: Sender<()>) {
    *NETWORK_SHUTDOWN
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(shutdown_tx);
    // SAFETY: registers a callback that never unwinds into Postgres.
    unsafe { pg_sys::before_shmem_exit(Some(stop_network_on_exit), pg_sys::Datum::from(0)) };
}

/// `before_shmem_exit` callback: ask the network thread to stop and wait for it (at
/// most `NETWORK_EXIT_WAIT_MS`). Never unwinds into Postgres.
unsafe extern "C-unwind" fn stop_network_on_exit(_code: std::ffi::c_int, _arg: pg_sys::Datum) {
    let _ = catch_unwind(|| {
        let tx = NETWORK_SHUTDOWN
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(tx) = tx {
            let _ = tx.send(());
        }
        let start = Instant::now();
        let wait = Duration::from_millis(NETWORK_EXIT_WAIT_MS.load(Ordering::SeqCst));
        while !NETWORK_FINISHED.load(Ordering::SeqCst) && start.elapsed() < wait {
            std::thread::sleep(NETWORK_EXIT_POLL);
        }
    });
}

/// Stop the network thread (bounded by `timeout`) and exit with status 1.
pub(super) fn shutdown_and_exit(
    shutdown_tx: &Sender<()>,
    network_handle: &JoinHandle<()>,
    timeout: Duration,
) -> ! {
    let _ = shutdown_tx.send(());
    // The exit hook must not wait longer than the caller allowed.
    NETWORK_EXIT_WAIT_MS.store(
        u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX),
        Ordering::SeqCst,
    );
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
        std::thread::sleep(NETWORK_EXIT_POLL);
    }
    // Status 1, not a plain return: a SIGTERM from pg_terminate_backend() must lead to
    // a restarted worker (see exit_for_restart).
    exit_for_restart();
}

/// The postmaster died: stop the network thread (bounded), then leave at once. The
/// shared memory dies with the cluster anyway, and a new postmaster can start only once
/// every process of the old one is gone (`proc_exit`'s cleanup takes over a second).
pub(super) fn exit_after_postmaster_death(
    shutdown_tx: &Sender<()>,
    network_handle: &JoinHandle<()>,
) -> ! {
    log!("pg_kafka background worker exiting: the postmaster died");
    let _ = shutdown_tx.send(());
    let start = Instant::now();
    while !network_handle.is_finished() && start.elapsed() < POSTMASTER_DEATH_NETWORK_WAIT {
        std::thread::sleep(POSTMASTER_DEATH_POLL);
    }
    // SAFETY: _exit ends the process without running any handler.
    unsafe { _exit(1) }
}

/// Exit the worker process with status 1 so the postmaster restarts it after
/// [`WORKER_RESTART_INTERVAL`].
///
/// A bgworker that returns from its main function exits with status 0, which
/// tells the postmaster to unregister it for good. That is wrong for every exit
/// this worker makes: a SIGTERM from `pg_terminate_backend()` or the network
/// thread dying should both lead to a fresh worker. During a server shutdown the
/// postmaster does not restart workers, so status 1 is also correct there (it is
/// what Postgres's own default SIGTERM handler for bgworkers does).
pub(super) fn exit_for_restart() -> ! {
    // SAFETY: proc_exit runs the exit callbacks and ends the process.
    unsafe { pg_sys::proc_exit(1) };
    // PG18 declares proc_exit with a leading `pg_noreturn`, which bindgen does not turn
    // into `-> !`, so the binding returns `()` there.
    #[cfg(feature = "pg18")]
    unreachable!("proc_exit returned")
}
