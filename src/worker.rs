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
pub extern "C-unwind" fn pg_kafka_listener_main(_arg: pg_sys::Datum) {
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
        if let Err(e) = crate::kafka::run_listener(shutdown_rx, &host, port, log_connections).await {
            pgrx::error!("TCP listener error: {}", e);
        }
    });

    // Step 7: Get the request queue receiver
    // This is how we receive Kafka requests from the async tokio tasks
    let request_rx = crate::kafka::request_receiver();

    // Step 8: Main event loop
    // ═══════════════════════════════════════════════════════════════════════════════
    // MAIN EVENT LOOP: The Heart of the Async/Sync Architecture
    // ═══════════════════════════════════════════════════════════════════════════════
    //
    // This loop serves THREE critical functions:
    //
    // 1. PROCESS DATABASE REQUESTS (Sync World)
    //    - Receive Kafka requests from the queue
    //    - Execute blocking database operations via Postgres SPI
    //    - Send responses back to async tasks
    //    - WHY HERE: SPI can ONLY be called from the main thread in sync context
    //
    // 2. DRIVE ASYNC NETWORK I/O (Async World)
    //    - Run tokio tasks that handle TCP connections
    //    - Parse Kafka binary protocol from sockets
    //    - Accept new client connections
    //    - WHY SEPARATE: Async I/O is non-blocking and handles thousands of connections
    //
    // 3. CHECK FOR SHUTDOWN SIGNALS (Postgres Integration)
    //    - Poll for SIGTERM from Postgres
    //    - Ensure graceful shutdown on server stop
    //    - WHY POLLING: pgrx requires regular latch checks for signal handling
    //
    // The TIMING is carefully balanced:
    // - 100ms for async I/O: Long enough to batch network operations efficiently
    // - 1ms for signal check: Short enough for responsive shutdown
    // - Non-blocking queue check: No delay for processing database requests
    //
    // This architecture solves the fundamental incompatibility:
    // - Tokio wants async functions (network I/O benefits from non-blocking)
    // - Postgres SPI wants sync functions on main thread (database safety)
    // - The queue bridges these worlds cleanly
    // ═══════════════════════════════════════════════════════════════════════════════

    loop {
        // ┌─────────────────────────────────────────────────────────────┐
        // │ PART 1: Process Database Requests (SYNC, Blocking)         │
        // └─────────────────────────────────────────────────────────────┘
        // Process all pending requests without blocking.
        // In Phase 2, this is where SPI INSERT/SELECT calls will happen.
        // RIGHT NOW: Just hardcoded responses (no database yet).
        // FUTURE: Spi::execute("INSERT INTO kafka.messages ...").
        while let Ok(request) = request_rx.try_recv() {
            process_request(request);
        }

        // ┌─────────────────────────────────────────────────────────────┐
        // │ PART 2: Drive Async Network I/O (ASYNC, Non-blocking)      │
        // └─────────────────────────────────────────────────────────────┘
        // block_on() is NOT a mistake here. We're running a sync loop
        // that periodically executes async code for 100ms.
        // This drives the LocalSet forward, processing:
        // - TCP accept() calls
        // - Socket read()/write() calls
        // - Kafka protocol parsing
        // Then we return to sync context to process database requests.
        runtime.block_on(async {
            tokio::select! {
                // Timeout after 100ms to return to sync processing
                _ = tokio::time::sleep(core::time::Duration::from_millis(100)) => {
                    // Normal path: timeout reached, go check for database requests
                }
                // Fallback: If listener exits (shouldn't happen during normal operation)
                _ = local_set.run_until(async {
                    std::future::pending::<()>().await
                }) => {
                    // LocalSet completed - listener task finished unexpectedly
                }
            }
        });

        // ┌─────────────────────────────────────────────────────────────┐
        // │ PART 3: Check for Shutdown Signal (POSTGRES Integration)   │
        // └─────────────────────────────────────────────────────────────┘
        // wait_latch(1ms) returns false when Postgres sends SIGTERM.
        // This is the SYNC boundary - we're fully outside async context.
        // CRITICAL: This must be polled regularly for graceful shutdown.
        if !BackgroundWorker::wait_latch(Some(core::time::Duration::from_millis(1))) {
            log!("pg_kafka background worker shutting down");

            // Signal the async listener to stop
            let _ = shutdown_tx.send(true);

            // Step 10: Graceful shutdown - wait for listener to finish
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

/// Process a Kafka request and send the response
///
/// In Step 3, we implemented ApiVersions.
/// In Step 4, we're adding Metadata support with hardcoded topics.
/// In Phase 2, we'll add SPI calls to query actual database tables.
fn process_request(request: crate::kafka::KafkaRequest) {
    use crate::kafka::messages::{
        ApiVersion, BrokerMetadata, KafkaResponse, PartitionMetadata, TopicMetadata,
    };

    pgrx::log!("process_request() called!");

    match request {
        crate::kafka::KafkaRequest::ApiVersions {
            correlation_id,
            client_id,
            response_tx,
        } => {
            pgrx::log!("Processing ApiVersions request, correlation_id: {}", correlation_id);
            if let Some(id) = client_id {
                pgrx::log!("ApiVersions request from client: {}", id);
            }

            // Build hardcoded ApiVersions response
            // We claim to support:
            // - ApiVersions (api_key 18): versions 0-3
            // - Metadata (api_key 3): versions 0-9
            let api_versions = vec![
                ApiVersion {
                    api_key: 18, // ApiVersions
                    min_version: 0,
                    max_version: 3,
                },
                ApiVersion {
                    api_key: 3, // Metadata
                    min_version: 0,
                    max_version: 9,
                },
            ];

            pgrx::log!("Building ApiVersions response with {} API versions", api_versions.len());

            let response = KafkaResponse::ApiVersions {
                correlation_id,
                api_versions,
            };

            if let Err(e) = response_tx.send(response) {
                pgrx::warning!("Failed to send ApiVersions response: {}", e);
            } else {
                pgrx::log!("ApiVersions response sent successfully to async task");
            }
        }
        crate::kafka::KafkaRequest::Metadata {
            correlation_id,
            client_id,
            topics: _requested_topics,
            response_tx,
        } => {
            pgrx::log!("Processing Metadata request, correlation_id: {}", correlation_id);
            if let Some(id) = client_id {
                pgrx::log!("Metadata request from client: {}", id);
            }

            // Get configuration for our broker info
            let config = crate::config::Config::load();

            // Step 4: Hardcoded metadata response
            // In Phase 2, we'll query kafka.topics and kafka.messages tables via SPI
            //
            // For now, we return:
            // - 1 broker (ourselves)
            // - 1 test topic with 1 partition
            let brokers = vec![BrokerMetadata {
                node_id: 1,                    // We are broker 1
                host: config.host.clone(),     // From GUC (default: 0.0.0.0)
                port: config.port,             // From GUC (default: 9092)
                rack: None,                    // No rack awareness in single-node setup
            }];

            let topics = vec![TopicMetadata {
                error_code: 0, // No error
                name: "test-topic".to_string(),
                partitions: vec![PartitionMetadata {
                    error_code: 0,      // No error
                    partition_index: 0, // Partition 0 (first and only partition)
                    leader_id: 1,       // Broker 1 (us) is the leader
                    replica_nodes: vec![1], // Only broker 1 has the data
                    isr_nodes: vec![1], // Broker 1 is in-sync (with itself)
                }],
            }];

            pgrx::log!(
                "Building Metadata response with {} brokers and {} topics",
                brokers.len(),
                topics.len()
            );

            let response = KafkaResponse::Metadata {
                correlation_id,
                brokers,
                topics,
            };

            if let Err(e) = response_tx.send(response) {
                pgrx::warning!("Failed to send Metadata response: {}", e);
            } else {
                pgrx::log!("Metadata response sent successfully to async task");
            }
        }
    }
}
