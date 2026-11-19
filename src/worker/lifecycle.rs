//! Process lifecycle of the worker: the SIGTERM handler, postmaster-death checks,
//! stopping the network thread, and the exit-for-restart path.

use pgrx::prelude::*;
use std::panic::catch_unwind;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// Set by our SIGTERM handler; never reset (the worker exits once it is set).
pub(super) static GOT_SIGTERM: AtomicBool = AtomicBool::new(false);

/// True while the worker is inside a database transaction: a SIGTERM then also sets
/// Postgres's ProcDiePending, so a statement or lock wait is interrupted at its next
/// CHECK_FOR_INTERRUPTS (FATAL, exit status 1, restarted by the postmaster). Outside
/// database work the main loop sees GOT_SIGTERM and shuts down gracefully.
pub(super) static IN_DB_WORK: AtomicBool = AtomicBool::new(false);

extern "C" {
    // Exported by the postgres executable (pmsignal.c, latch support); not in pgrx's
    // bindings. Neither raises a Postgres error.
    fn PostmasterIsAliveInternal() -> bool;
    pub(super) fn _exit(status: std::ffi::c_int) -> !;
    #[link_name = "SetLatch"]
    fn RawSetLatch(latch: *mut pg_sys::Latch);
    #[cfg(target_os = "linux")]
    fn __errno_location() -> *mut std::ffi::c_int;
}

/// SIGTERM handler. pgrx's handler only sets a flag the main loop polls between
/// requests, so a worker blocked in SQL or a lock wait could not be stopped. This one
/// also requests ProcDie while database work is in progress (what Postgres's own
/// `die()` does), and keeps errno intact like Postgres's handlers.
pub(super) unsafe extern "C-unwind" fn handle_sigterm(_signal: std::ffi::c_int) {
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
pub(super) fn install_sigterm_handler() {
    unsafe {
        #[cfg(not(feature = "pg18"))]
        pg_sys::pqsignal(pg_sys::SIGTERM as i32, Some(handle_sigterm));
        #[cfg(feature = "pg18")]
        pg_sys::pqsignal_be(pg_sys::SIGTERM as i32, Some(handle_sigterm));
    }
}

/// Whether SIGTERM has arrived (sticky).
pub(super) fn sigterm_received() -> bool {
    GOT_SIGTERM.load(Ordering::SeqCst)
}

/// Whether the postmaster is still alive (one non-blocking read on the postmaster-death
/// pipe). The worker must not outlive it: an orphan would keep committing Kafka writes
/// and block a new postmaster from starting.
pub(super) fn postmaster_alive() -> bool {
    unsafe { PostmasterIsAliveInternal() }
}

/// The network thread's shutdown sender, for [`stop_network_on_exit`].
pub(super) static NETWORK_SHUTDOWN: std::sync::Mutex<Option<std::sync::mpsc::Sender<()>>> =
    std::sync::Mutex::new(None);
/// Set when the network thread has finished (its runtime is shut down).
pub(super) static NETWORK_FINISHED: AtomicBool = AtomicBool::new(false);
/// How long [`stop_network_on_exit`] waits for the network thread, in ms.
pub(super) static NETWORK_EXIT_WAIT_MS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(2000);

/// Sets [`NETWORK_FINISHED`] when the network thread's closure ends, however it ends.
pub(super) struct NetworkFinished;
impl Drop for NetworkFinished {
    fn drop(&mut self) {
        NETWORK_FINISHED.store(true, Ordering::SeqCst);
    }
}

/// `before_shmem_exit` callback: ask the network thread to stop and wait for it (at
/// most 2 s). Never unwinds into Postgres.
pub(super) unsafe extern "C-unwind" fn stop_network_on_exit(
    _code: std::ffi::c_int,
    _arg: pg_sys::Datum,
) {
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
pub(super) fn shutdown_and_exit(
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

/// Exit the worker process with status 1 so the postmaster restarts it after
/// `WORKER_RESTART_INTERVAL`.
///
/// A bgworker that returns from its main function exits with status 0, which
/// tells the postmaster to unregister it for good. That is wrong for every exit
/// this worker makes: a SIGTERM from `pg_terminate_backend()` or the network
/// thread dying should both lead to a fresh worker. During a server shutdown the
/// postmaster does not restart workers, so status 1 is also correct there (it is
/// what Postgres's own default SIGTERM handler for bgworkers does).
pub(super) fn exit_for_restart() -> ! {
    unsafe { pg_sys::proc_exit(1) };
    // PG18 declares proc_exit with a leading `pg_noreturn`, which bindgen does not turn
    // into `-> !`, so the binding returns `()` there.
    #[cfg(feature = "pg18")]
    unreachable!("proc_exit returned")
}

/// How long the postmaster waits before restarting a worker that exited
/// (registered in `_PG_init`).
pub(crate) const WORKER_RESTART_INTERVAL: Duration = Duration::from_secs(5);
