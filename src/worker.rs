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

    // Step 3: Load configuration
    let config = crate::config::Config::load();
    log!(
        "pg_kafka background worker started (port: {}, host: {})",
        config.port,
        config.host
    );

    // Step 4: Create shutdown channel for communicating between sync and async worlds
    // The watch channel allows the sync signal handler to notify async tasks
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    // Step 5: Create tokio runtime for async TCP listener
    // CRITICAL: Must use current_thread runtime, NOT multi-threaded!
    // Postgres FFI cannot be called from multiple threads, and pgrx enforces this.
    // The current_thread runtime runs all tasks on a single thread.
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            pgrx::error!("Failed to create tokio runtime: {}", e);
        }
    };

    // Step 6: Start the TCP listener as a local task
    // We use LocalSet for single-threaded async execution
    let local_set = tokio::task::LocalSet::new();

    // Clone config for async context
    let port = config.port;
    let host = config.host.clone();
    let log_connections = config.log_connections;
    let shutdown_timeout = core::time::Duration::from_millis(config.shutdown_timeout_ms as u64);

    // Spawn the listener task on the LocalSet
    let listener_handle = local_set.spawn_local(async move {
        if let Err(e) = crate::listener::run(shutdown_rx, &host, port, log_connections).await {
            pgrx::error!("TCP listener error: {}", e);
        }
    });

    // Step 7: Main event loop - alternates between driving async runtime and checking signals
    // This approach cleanly separates blocking signal checks from async execution
    loop {
        // Drive the async runtime forward for 100ms
        // This processes TCP connections, accepts new clients, etc.
        runtime.block_on(async {
            // Run all pending tasks on LocalSet for a short duration
            tokio::select! {
                _ = tokio::time::sleep(core::time::Duration::from_millis(100)) => {
                    // Timeout reached, return to signal checking
                }
                _ = local_set.run_until(async {
                    // This drives the LocalSet, but we'll timeout after 100ms
                    std::future::pending::<()>().await
                }) => {
                    // LocalSet completed (shouldn't happen unless listener exits)
                }
            }
        });

        // Step 8: Check for shutdown signal (blocking, but brief)
        // This is the sync boundary - we're outside async context here
        if !BackgroundWorker::wait_latch(Some(core::time::Duration::from_millis(1))) {
            log!("pg_kafka background worker shutting down");

            // Signal the async listener to stop
            let _ = shutdown_tx.send(true);

            // Step 9: Graceful shutdown - wait for listener to finish
            runtime.block_on(async {
                // Give the listener configured timeout to shut down cleanly
                // We need to drive the LocalSet to completion for the listener to actually finish
                let shutdown_result = tokio::time::timeout(
                    shutdown_timeout,
                    local_set.run_until(listener_handle),
                ).await;

                match shutdown_result {
                    Ok(Ok(_)) => {
                        log!("pg_kafka TCP listener shut down cleanly");
                    }
                    Ok(Err(e)) => {
                        pgrx::warning!("pg_kafka TCP listener error during shutdown: {:?}", e);
                    }
                    Err(_) => {
                        pgrx::warning!("pg_kafka TCP listener shutdown timed out");
                    }
                }
            });

            break;
        }
    }
}
