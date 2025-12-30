// Background worker module for pg_kafka
//
// This module implements the persistent background worker that runs the Kafka
// protocol listener. The worker is started by Postgres when the extension loads
// (via shared_preload_libraries in postgresql.conf).

use pgrx::bgworkers::{BackgroundWorker, SignalWakeFlags};
use pgrx::prelude::*;

/// Main entry point for the pg_kafka background worker.
///
/// IMPORTANT: This function MUST follow pgrx background worker conventions:
/// 1. Must be marked with #[pg_guard] to handle Postgres errors safely
/// 2. Must attach signal handlers BEFORE doing any work
/// 3. Must check for shutdown signals regularly and exit gracefully
/// 4. Must be marked with #[no_mangle] to ensure Postgres can find it
#[pg_guard]
#[no_mangle]
pub extern "C" fn pg_kafka_listener_main(_arg: pg_sys::Datum) {
    // Step 1: Attach to Postgres signal handling system
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

    // Step 2: Connect to the database (required for SPI access in future)
    BackgroundWorker::connect_worker_to_spi(Some("postgres"), None);

    // Step 3: Log once that we started
    log!("pg_kafka background worker started");

    // Step 4: Main loop - wait for shutdown signal
    // wait_latch() returns false when shutdown is requested or postmaster dies
    while BackgroundWorker::wait_latch(Some(core::time::Duration::from_secs(1))) {
        // Worker is alive and waiting - nothing to do yet in Phase 1
        // In Phase 2, this is where we'll run the TCP listener
    }

    // Clean exit when shutdown requested
    log!("pg_kafka background worker shutting down");
}
