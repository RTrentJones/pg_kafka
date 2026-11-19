//! Setup of the network thread: its `tracing` subscriber and its signal mask.

use tracing::warn;

/// Default `tracing` filter for the network thread when `RUST_LOG` is unset.
///
/// The network thread cannot use pgrx logging, so its events go to the
/// postmaster's stderr (the server log). Warnings — rejected frames, exhausted
/// budgets, failed shadow forwards, a worker channel that closed — are
/// operationally relevant and must be visible by default; routine per-request
/// detail stays at debug.
const DEFAULT_NETWORK_LOG_FILTER: &str = "pg_kafka=warn";

/// The `tracing` filter directive string for the network thread: `RUST_LOG` when
/// it is set to something non-empty, otherwise [`DEFAULT_NETWORK_LOG_FILTER`].
/// A set `RUST_LOG` replaces the default entirely, so `RUST_LOG=pg_kafka=debug`
/// shows debug events.
fn network_log_filter(rust_log: Option<&str>) -> String {
    match rust_log.map(str::trim) {
        Some(v) if !v.is_empty() => v.to_string(),
        _ => DEFAULT_NETWORK_LOG_FILTER.to_string(),
    }
}

/// Install the global `tracing` subscriber for the network thread (stderr).
///
/// Called once by the worker before the tokio runtime starts. A second call (e.g.
/// after an in-process restart) is a no-op because `try_init` refuses to replace
/// an installed subscriber.
pub fn init_network_tracing() {
    let directives = network_log_filter(std::env::var("RUST_LOG").ok().as_deref());
    let filter = tracing_subscriber::EnvFilter::try_new(&directives)
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(DEFAULT_NETWORK_LOG_FILTER));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .with_writer(std::io::stderr)
        .try_init();
}

// The signal-mask calls of libc (not bound by pgrx).
extern "C" {
    fn sigfillset(set: *mut pgrx::pg_sys::__sigset_t) -> std::os::raw::c_int;
    fn pthread_sigmask(
        how: std::os::raw::c_int,
        set: *const pgrx::pg_sys::__sigset_t,
        old: *mut pgrx::pg_sys::__sigset_t,
    ) -> std::os::raw::c_int;
}

/// Block every signal on the calling thread. Called first thing on the network thread,
/// before it creates the tokio runtime (whose threads inherit the mask).
///
/// The worker's signal handlers (pgrx's SIGTERM / SIGHUP handlers) must run on the
/// worker's main thread. The kernel delivers a process-directed signal to any thread
/// that does not block it; if `pg_terminate_backend()`'s SIGTERM lands on the network
/// thread, pgrx's handler panics ("postgres FFI may not be called from multiple
/// threads"), and a panic in a signal handler aborts the process, which makes the
/// postmaster put the whole cluster through crash recovery.
pub fn block_signals_on_this_thread() {
    // SAFETY: plain libc calls on a local, zero-initialised sigset.
    unsafe {
        let mut set = std::mem::MaybeUninit::<pgrx::pg_sys::__sigset_t>::zeroed();
        sigfillset(set.as_mut_ptr());
        let rc = pthread_sigmask(
            pgrx::pg_sys::SIG_BLOCK.cast_signed(),
            set.as_ptr(),
            std::ptr::null_mut(),
        );
        if rc != 0 {
            warn!(
                "could not block signals on the network thread (error {})",
                rc
            );
        }
    }
}

/// Whether `signum` is blocked on the calling thread (tests).
#[cfg(test)]
fn signal_blocked_on_this_thread(signum: i32) -> bool {
    extern "C" {
        fn sigismember(
            set: *const pgrx::pg_sys::__sigset_t,
            signum: std::os::raw::c_int,
        ) -> std::os::raw::c_int;
    }
    // SAFETY: plain libc calls on a local, zero-initialised sigset.
    unsafe {
        let mut old = std::mem::MaybeUninit::<pgrx::pg_sys::__sigset_t>::zeroed();
        pthread_sigmask(
            pgrx::pg_sys::SIG_BLOCK.cast_signed(),
            std::ptr::null(),
            old.as_mut_ptr(),
        );
        sigismember(old.as_ptr(), signum) == 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // ===== Network-thread logging =====

    #[test]
    fn test_network_thread_blocks_termination_signals() {
        // SIGTERM / SIGHUP / SIGINT must never be handled on the network thread
        // (or the tokio threads it spawns, which inherit the mask).
        let (term, hup, int) = std::thread::spawn(|| {
            assert!(!signal_blocked_on_this_thread(15));
            block_signals_on_this_thread();
            std::thread::spawn(|| {
                (
                    signal_blocked_on_this_thread(15),
                    signal_blocked_on_this_thread(1),
                    signal_blocked_on_this_thread(2),
                )
            })
            .join()
            .unwrap()
        })
        .join()
        .unwrap();
        assert!(term && hup && int);
    }

    #[test]
    fn test_network_log_filter_defaults_to_warn() {
        assert_eq!(network_log_filter(None), "pg_kafka=warn");
        assert_eq!(network_log_filter(Some("")), "pg_kafka=warn");
        assert_eq!(network_log_filter(Some("   ")), "pg_kafka=warn");
        // The default must parse as a filter.
        assert!(tracing_subscriber::EnvFilter::try_new(DEFAULT_NETWORK_LOG_FILTER).is_ok());
    }

    #[test]
    fn test_network_log_filter_honours_rust_log() {
        assert_eq!(network_log_filter(Some("pg_kafka=debug")), "pg_kafka=debug");
    }
}
