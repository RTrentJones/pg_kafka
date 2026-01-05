// Background worker module for pg_kafka
//
// This module implements the persistent background worker that runs the Kafka
// protocol listener. The worker is started by Postgres when the extension loads
// (via shared_preload_libraries in postgresql.conf).

use pgrx::bgworkers::{BackgroundWorker, SignalWakeFlags};
use pgrx::prelude::*;

// Import conditional logging macros from lib.rs
use crate::kafka::constants::*;
use crate::{pg_log, pg_warning};

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

    // Clone host for worker processing (will be borrowed by async closure)
    let broker_host = host.clone();

    // Spawn the listener task on the LocalSet
    pg_log!("Spawning TCP listener task on LocalSet");
    let listener_handle = local_set.spawn_local(async move {
        pg_log!("Listener task started, entering retry loop");
        loop {
            let rx = shutdown_rx.clone();
            if *rx.borrow() {
                pg_log!("Listener task: shutdown signal detected, exiting");
                break;
            }

            pg_log!("Listener task: calling run_listener on {}:{}", host, port);
            match crate::kafka::run_listener(rx, &host, port, log_connections).await {
                Ok(_) => {
                    pg_log!("TCP listener exited normally");
                    break;
                }
                Err(e) => {
                    pgrx::warning!("TCP listener error: {}. Restarting...", e);

                    // Check shutdown before sleeping to avoid unnecessary delay
                    if *shutdown_rx.borrow() {
                        break;
                    }
                    // Make sleep interruptible by shutdown signal
                    let mut rx = shutdown_rx.clone();
                    tokio::select! {
                        _ = tokio::time::sleep(core::time::Duration::from_millis(1000)) => {}
                        _ = rx.changed() => {}
                    }
                }
            }
        }
        pg_log!("Listener task exiting");
    });

    // Step 7: Create consumer group coordinator
    // This manages consumer group membership state in-memory
    let coordinator = std::sync::Arc::new(crate::kafka::GroupCoordinator::new());

    // Step 8: Get the request queue receiver
    // This is how we receive Kafka requests from the async tokio tasks
    let request_rx = crate::kafka::request_receiver();

    // CRITICAL: Drive the LocalSet once to ensure the listener task starts
    // before entering the main loop. Without this, the spawned task might not
    // begin execution until the first loop iteration.
    pg_log!("Driving LocalSet to start listener task");
    local_set.block_on(&runtime, async {
        // Give the listener task a chance to start and bind to the port
        tokio::time::sleep(core::time::Duration::from_millis(10)).await;
    });
    pg_log!("Entering main event loop");

    // Step 9: Main event loop
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
        // Each request is processed in its own transaction using BackgroundWorker::transaction().
        // This is required for SPI calls (INSERT/SELECT) to work correctly.
        while let Ok(request) = request_rx.try_recv() {
            // CRITICAL: Wrap each request in a transaction
            // Background workers don't have implicit transactions like client sessions do.
            // BackgroundWorker::transaction() starts a transaction, calls our closure,
            // and commits/rolls back based on the result.
            let coord = coordinator.clone();
            let broker_h = broker_host.clone();
            let broker_p = port;
            BackgroundWorker::transaction(move || {
                process_request(request, &coord, &broker_h, broker_p);
            });
        }

        // ┌─────────────────────────────────────────────────────────────┐
        // │ PART 2: Drive Async Network I/O (ASYNC, Non-blocking)      │
        // └─────────────────────────────────────────────────────────────┘
        // Drive the LocalSet forward for a short duration to process async tasks:
        // - TCP accept() calls
        // - Socket read()/write() calls
        // - Kafka protocol parsing
        // Then we return to sync context to process database requests.
        //
        // CRITICAL: Use LocalSet::block_on with the runtime to properly poll tasks.
        // The timeout ensures we return to sync context to process database requests.
        local_set.block_on(&runtime, async {
            let timeout_result = tokio::time::timeout(
                core::time::Duration::from_millis(crate::kafka::ASYNC_IO_INTERVAL_MS),
                std::future::pending::<()>(),
            )
            .await;
            // Timeout is expected - it means we processed I/O for 100ms and now returning to sync
            debug_assert!(timeout_result.is_err(), "pending() should never complete");
        });

        // ┌─────────────────────────────────────────────────────────────┐
        // │ PART 3: Check for Shutdown Signal (POSTGRES Integration)   │
        // └─────────────────────────────────────────────────────────────┘
        // wait_latch(SIGNAL_CHECK_INTERVAL_MS) returns false when Postgres sends SIGTERM.
        // This is the SYNC boundary - we're fully outside async context.
        // CRITICAL: This must be polled regularly for graceful shutdown.
        if !BackgroundWorker::wait_latch(Some(core::time::Duration::from_millis(
            crate::kafka::SIGNAL_CHECK_INTERVAL_MS,
        ))) {
            log!("pg_kafka background worker shutting down");

            // Signal the async listener to stop
            let _ = shutdown_tx.send(true);

            // Step 10: Graceful shutdown - wait for listener to finish
            runtime.block_on(async {
                // Give the listener configured timeout to shut down cleanly
                // We need to drive the LocalSet to completion for the listener to actually finish
                let shutdown_result =
                    tokio::time::timeout(shutdown_timeout, local_set.run_until(listener_handle))
                        .await;

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

// Helper functions removed - now using storage trait and handlers

/// Process a Kafka request and send the response
///
/// This function has been refactored to use the Repository Pattern:
/// - Storage operations are handled by PostgresStore (implements KafkaStore trait)
/// - Protocol logic is handled by pure handler functions
/// - Transaction boundaries remain explicit here (in worker.rs)
///
/// # Testing
/// This function is public to allow unit testing, but is not part of the
/// public API and should not be called directly by external code.
#[doc(hidden)]
pub fn process_request(
    request: crate::kafka::KafkaRequest,
    coordinator: &std::sync::Arc<crate::kafka::GroupCoordinator>,
    broker_host: &str,
    broker_port: i32,
) {
    use crate::kafka::messages::KafkaResponse;
    use crate::kafka::{handlers, PostgresStore};

    pg_log!("process_request() called!");

    match request {
        crate::kafka::KafkaRequest::ApiVersions {
            correlation_id,
            client_id,
            api_version,
            response_tx,
        } => {
            pg_log!(
                "Processing ApiVersions request, correlation_id: {}, api_version: {}",
                correlation_id,
                api_version
            );
            if let Some(id) = client_id {
                pg_log!("ApiVersions request from client: {}", id);
            }

            // Use handler - no storage needed for ApiVersions
            let kafka_response = handlers::handle_api_versions();

            pg_log!(
                "Building ApiVersions response with {} API versions",
                kafka_response.api_keys.len()
            );

            let response = KafkaResponse::ApiVersions {
                correlation_id,
                api_version,
                response: kafka_response,
            };

            if let Err(e) = response_tx.send(response) {
                pg_warning!("Failed to send ApiVersions response: {}", e);
            } else {
                pg_log!("ApiVersions response sent successfully to async task");
            }
        }
        crate::kafka::KafkaRequest::Metadata {
            correlation_id,
            client_id,
            api_version,
            topics: requested_topics,
            response_tx,
        } => {
            pg_log!(
                "Processing Metadata request, correlation_id: {}",
                correlation_id
            );
            if let Some(id) = client_id {
                pg_log!("Metadata request from client: {}", id);
            }

            // Get configuration for our broker info
            let config = crate::config::Config::load();

            // CRITICAL: Convert bind address to advertised address
            // We bind to 0.0.0.0 (all interfaces) but must advertise a routable address
            // that clients can actually connect to. If host is 0.0.0.0, advertise localhost.
            let advertised_host = if config.host == "0.0.0.0" || config.host.is_empty() {
                pg_log!(
                    "Converting bind address '{}' to advertised address 'localhost'",
                    config.host
                );
                "localhost".to_string()
            } else {
                pg_log!(
                    "Using configured host '{}' as advertised address",
                    config.host
                );
                config.host.clone()
            };

            // Log the final advertised address to help debug connection issues
            pg_log!("Kafka listener advertising address: {}", advertised_host);

            // Create storage and use handler
            let store = PostgresStore::new();
            let kafka_response = match handlers::handle_metadata(
                &store,
                requested_topics,
                advertised_host,
                config.port,
            ) {
                Ok(response) => response,
                Err(e) => {
                    pg_warning!("Failed to handle Metadata request: {}", e);
                    let error_response = KafkaResponse::Error {
                        correlation_id,
                        error_code: ERROR_UNKNOWN_SERVER_ERROR,
                        error_message: Some(format!("Failed to handle Metadata request: {}", e)),
                    };
                    if let Err(e) = response_tx.send(error_response) {
                        pg_warning!("Failed to send error response: {}", e);
                    }
                    return;
                }
            };

            pg_log!(
                "Building Metadata response with {} brokers and {} topics",
                kafka_response.brokers.len(),
                kafka_response.topics.len()
            );

            let response = KafkaResponse::Metadata {
                correlation_id,
                api_version,
                response: kafka_response,
            };

            if let Err(e) = response_tx.send(response) {
                pg_warning!("Failed to send Metadata response: {}", e);
            } else {
                pg_log!("Metadata response sent successfully to async task");
            }
        }
        crate::kafka::KafkaRequest::Produce {
            correlation_id,
            client_id,
            api_version,
            acks,
            timeout_ms,
            topic_data,
            response_tx,
        } => {
            pg_log!(
                "Processing Produce request, correlation_id: {}",
                correlation_id
            );
            if let Some(id) = &client_id {
                pg_log!("Produce request from client: {}", id);
            }
            pg_log!(
                "Produce parameters: acks={}, timeout_ms={}",
                acks,
                timeout_ms
            );

            // Handle acks=0 (fire-and-forget) - not yet supported
            if acks == 0 {
                pg_warning!("acks=0 (fire-and-forget) not yet supported");
                let error_response = KafkaResponse::Error {
                    correlation_id,
                    error_code: ERROR_UNSUPPORTED_VERSION,
                    error_message: Some("acks=0 not yet supported".to_string()),
                };
                if let Err(e) = response_tx.send(error_response) {
                    pg_warning!("Failed to send error response: {}", e);
                }
                return;
            }

            // Create storage and use handler
            let store = PostgresStore::new();
            let kafka_response = match handlers::handle_produce(&store, topic_data) {
                Ok(response) => response,
                Err(e) => {
                    pg_warning!("Failed to handle Produce request: {}", e);
                    let error_response = KafkaResponse::Error {
                        correlation_id,
                        error_code: ERROR_UNKNOWN_SERVER_ERROR,
                        error_message: Some(format!("Failed to handle Produce request: {}", e)),
                    };
                    if let Err(e) = response_tx.send(error_response) {
                        pg_warning!("Failed to send error response: {}", e);
                    }
                    return;
                }
            };

            // Send successful response
            let response = KafkaResponse::Produce {
                correlation_id,
                api_version,
                response: kafka_response,
            };

            if let Err(e) = response_tx.send(response) {
                pg_warning!("Failed to send Produce response: {}", e);
            } else {
                pg_log!("Produce response sent successfully");
            }
        }
        crate::kafka::KafkaRequest::Fetch {
            correlation_id,
            client_id: _,
            api_version,
            max_wait_ms: _,
            min_bytes: _,
            max_bytes: _,
            topic_data,
            response_tx,
        } => {
            pg_log!(
                "Processing Fetch request, correlation_id: {}",
                correlation_id
            );

            // Create storage and use handler
            let store = PostgresStore::new();
            let kafka_response = match handlers::handle_fetch(&store, topic_data) {
                Ok(response) => response,
                Err(e) => {
                    pg_warning!("Failed to handle Fetch request: {}", e);
                    let error_response = KafkaResponse::Error {
                        correlation_id,
                        error_code: ERROR_UNKNOWN_SERVER_ERROR,
                        error_message: Some(format!("Failed to handle Fetch request: {}", e)),
                    };
                    if let Err(e) = response_tx.send(error_response) {
                        pg_warning!("Failed to send error response: {}", e);
                    }
                    return;
                }
            };

            let response = KafkaResponse::Fetch {
                correlation_id,
                api_version,
                response: kafka_response,
            };

            if let Err(e) = response_tx.send(response) {
                pg_warning!("Failed to send Fetch response: {}", e);
            } else {
                pg_log!("Fetch response sent successfully");
            }
        }
        crate::kafka::KafkaRequest::OffsetCommit {
            correlation_id,
            client_id: _,
            api_version,
            group_id,
            topics,
            response_tx,
        } => {
            pg_log!(
                "Processing OffsetCommit request, correlation_id: {}, group_id: {}",
                correlation_id,
                group_id
            );

            // Create storage and use handler
            let store = PostgresStore::new();
            let kafka_response = match handlers::handle_offset_commit(&store, group_id, topics) {
                Ok(response) => response,
                Err(e) => {
                    pg_warning!("Failed to handle OffsetCommit request: {}", e);
                    let error_response = KafkaResponse::Error {
                        correlation_id,
                        error_code: ERROR_UNKNOWN_SERVER_ERROR,
                        error_message: Some(format!(
                            "Failed to handle OffsetCommit request: {}",
                            e
                        )),
                    };
                    if let Err(e) = response_tx.send(error_response) {
                        pg_warning!("Failed to send error response: {}", e);
                    }
                    return;
                }
            };

            let response = KafkaResponse::OffsetCommit {
                correlation_id,
                api_version,
                response: kafka_response,
            };

            if let Err(e) = response_tx.send(response) {
                pg_warning!("Failed to send OffsetCommit response: {}", e);
            } else {
                pg_log!("OffsetCommit response sent successfully");
            }
        }
        crate::kafka::KafkaRequest::OffsetFetch {
            correlation_id,
            client_id: _,
            api_version,
            group_id,
            topics,
            response_tx,
        } => {
            pg_log!(
                "Processing OffsetFetch request, correlation_id: {}, group_id: {}",
                correlation_id,
                group_id
            );

            // Create storage and use handler
            let store = PostgresStore::new();
            let kafka_response = match handlers::handle_offset_fetch(&store, group_id, topics) {
                Ok(response) => response,
                Err(e) => {
                    pg_warning!("Failed to handle OffsetFetch request: {}", e);
                    let error_response = KafkaResponse::Error {
                        correlation_id,
                        error_code: ERROR_UNKNOWN_SERVER_ERROR,
                        error_message: Some(format!("Failed to handle OffsetFetch request: {}", e)),
                    };
                    if let Err(e) = response_tx.send(error_response) {
                        pg_warning!("Failed to send error response: {}", e);
                    }
                    return;
                }
            };

            let response = KafkaResponse::OffsetFetch {
                correlation_id,
                api_version,
                response: kafka_response,
            };

            if let Err(e) = response_tx.send(response) {
                pg_warning!("Failed to send OffsetFetch response: {}", e);
            } else {
                pg_log!("OffsetFetch response sent successfully");
            }
        }
        crate::kafka::KafkaRequest::FindCoordinator {
            correlation_id,
            api_version,
            key,
            key_type,
            response_tx,
            ..
        } => {
            let kafka_response = match crate::kafka::handlers::handle_find_coordinator(
                broker_host.to_string(),
                broker_port,
                key,
                key_type,
            ) {
                Ok(response) => response,
                Err(e) => {
                    pg_warning!("Failed to handle FindCoordinator request: {}", e);
                    let error_response = KafkaResponse::Error {
                        correlation_id,
                        error_code: crate::kafka::ERROR_UNKNOWN_SERVER_ERROR,
                        error_message: Some(format!(
                            "Failed to handle FindCoordinator request: {}",
                            e
                        )),
                    };
                    if let Err(e) = response_tx.send(error_response) {
                        pg_warning!("Failed to send error response: {}", e);
                    }
                    return;
                }
            };

            let response = KafkaResponse::FindCoordinator {
                correlation_id,
                api_version,
                response: kafka_response,
            };

            if let Err(e) = response_tx.send(response) {
                pg_warning!("Failed to send FindCoordinator response: {}", e);
            } else {
                pg_log!("FindCoordinator response sent successfully");
            }
        }
        crate::kafka::KafkaRequest::JoinGroup {
            correlation_id,
            api_version,
            group_id,
            member_id,
            client_id,
            session_timeout_ms,
            rebalance_timeout_ms,
            protocol_type,
            protocols,
            response_tx,
            ..
        } => {
            let kafka_response = match crate::kafka::handlers::handle_join_group(
                coordinator,
                group_id,
                member_id,
                client_id.unwrap_or_else(|| "unknown".to_string()),
                session_timeout_ms,
                rebalance_timeout_ms,
                protocol_type,
                protocols,
            ) {
                Ok(response) => response,
                Err(e) => {
                    pg_warning!("Failed to handle JoinGroup request: {}", e);
                    let error_response = KafkaResponse::Error {
                        correlation_id,
                        error_code: crate::kafka::ERROR_UNKNOWN_SERVER_ERROR,
                        error_message: Some(format!("Failed to handle JoinGroup request: {}", e)),
                    };
                    if let Err(e) = response_tx.send(error_response) {
                        pg_warning!("Failed to send error response: {}", e);
                    }
                    return;
                }
            };

            let response = KafkaResponse::JoinGroup {
                correlation_id,
                api_version,
                response: kafka_response,
            };

            if let Err(e) = response_tx.send(response) {
                pg_warning!("Failed to send JoinGroup response: {}", e);
            } else {
                pg_log!("JoinGroup response sent successfully");
            }
        }
        crate::kafka::KafkaRequest::SyncGroup {
            correlation_id,
            api_version,
            group_id,
            member_id,
            generation_id,
            assignments,
            response_tx,
            ..
        } => {
            let kafka_response = match crate::kafka::handlers::handle_sync_group(
                coordinator,
                group_id,
                member_id,
                generation_id,
                assignments,
            ) {
                Ok(response) => response,
                Err(e) => {
                    pg_warning!("Failed to handle SyncGroup request: {}", e);
                    let error_response = KafkaResponse::Error {
                        correlation_id,
                        error_code: crate::kafka::ERROR_UNKNOWN_SERVER_ERROR,
                        error_message: Some(format!("Failed to handle SyncGroup request: {}", e)),
                    };
                    if let Err(e) = response_tx.send(error_response) {
                        pg_warning!("Failed to send error response: {}", e);
                    }
                    return;
                }
            };

            let response = KafkaResponse::SyncGroup {
                correlation_id,
                api_version,
                response: kafka_response,
            };

            if let Err(e) = response_tx.send(response) {
                pg_warning!("Failed to send SyncGroup response: {}", e);
            } else {
                pg_log!("SyncGroup response sent successfully");
            }
        }
        crate::kafka::KafkaRequest::Heartbeat {
            correlation_id,
            api_version,
            group_id,
            member_id,
            generation_id,
            response_tx,
            ..
        } => {
            let kafka_response = match crate::kafka::handlers::handle_heartbeat(
                coordinator,
                group_id,
                member_id,
                generation_id,
            ) {
                Ok(response) => response,
                Err(e) => {
                    pg_warning!("Failed to handle Heartbeat request: {}", e);
                    let error_response = KafkaResponse::Error {
                        correlation_id,
                        error_code: crate::kafka::ERROR_UNKNOWN_SERVER_ERROR,
                        error_message: Some(format!("Failed to handle Heartbeat request: {}", e)),
                    };
                    if let Err(e) = response_tx.send(error_response) {
                        pg_warning!("Failed to send error response: {}", e);
                    }
                    return;
                }
            };

            let response = KafkaResponse::Heartbeat {
                correlation_id,
                api_version,
                response: kafka_response,
            };

            if let Err(e) = response_tx.send(response) {
                pg_warning!("Failed to send Heartbeat response: {}", e);
            } else {
                pg_log!("Heartbeat response sent successfully");
            }
        }
        crate::kafka::KafkaRequest::LeaveGroup {
            correlation_id,
            api_version,
            group_id,
            member_id,
            response_tx,
            ..
        } => {
            let kafka_response = match crate::kafka::handlers::handle_leave_group(
                coordinator,
                group_id,
                member_id,
            ) {
                Ok(response) => response,
                Err(e) => {
                    pg_warning!("Failed to handle LeaveGroup request: {}", e);
                    let error_response = KafkaResponse::Error {
                        correlation_id,
                        error_code: crate::kafka::ERROR_UNKNOWN_SERVER_ERROR,
                        error_message: Some(format!("Failed to handle LeaveGroup request: {}", e)),
                    };
                    if let Err(e) = response_tx.send(error_response) {
                        pg_warning!("Failed to send error response: {}", e);
                    }
                    return;
                }
            };

            let response = KafkaResponse::LeaveGroup {
                correlation_id,
                api_version,
                response: kafka_response,
            };

            if let Err(e) = response_tx.send(response) {
                pg_warning!("Failed to send LeaveGroup response: {}", e);
            } else {
                pg_log!("LeaveGroup response sent successfully");
            }
        }
        crate::kafka::KafkaRequest::ListOffsets {
            correlation_id,
            api_version,
            topics,
            response_tx,
            ..
        } => {
            pg_log!(
                "Processing ListOffsets request: correlation_id={}, topics={}",
                correlation_id,
                topics.len()
            );

            // Create storage instance
            let store = PostgresStore;

            let kafka_response = match crate::kafka::handlers::handle_list_offsets(&store, topics) {
                Ok(response) => response,
                Err(e) => {
                    pg_warning!("Failed to handle ListOffsets request: {}", e);
                    let error_response = KafkaResponse::Error {
                        correlation_id,
                        error_code: crate::kafka::ERROR_UNKNOWN_SERVER_ERROR,
                        error_message: Some(format!("Failed to handle ListOffsets request: {}", e)),
                    };
                    if let Err(e) = response_tx.send(error_response) {
                        pg_warning!("Failed to send error response: {}", e);
                    }
                    return;
                }
            };

            let response = KafkaResponse::ListOffsets {
                correlation_id,
                api_version,
                response: kafka_response,
            };

            if let Err(e) = response_tx.send(response) {
                pg_warning!("Failed to send ListOffsets response: {}", e);
            } else {
                pg_log!("ListOffsets response sent successfully");
            }
        }
        crate::kafka::KafkaRequest::DescribeGroups {
            correlation_id,
            api_version,
            groups,
            response_tx,
            ..
        } => {
            pg_log!(
                "Processing DescribeGroups request: correlation_id={}, groups={}",
                correlation_id,
                groups.len()
            );

            let kafka_response =
                match crate::kafka::handlers::handle_describe_groups(coordinator, groups) {
                    Ok(response) => response,
                    Err(e) => {
                        pg_warning!("Failed to handle DescribeGroups request: {}", e);
                        let error_response = KafkaResponse::Error {
                            correlation_id,
                            error_code: crate::kafka::ERROR_UNKNOWN_SERVER_ERROR,
                            error_message: Some(format!(
                                "Failed to handle DescribeGroups request: {}",
                                e
                            )),
                        };
                        if let Err(e) = response_tx.send(error_response) {
                            pg_warning!("Failed to send error response: {}", e);
                        }
                        return;
                    }
                };

            let response = KafkaResponse::DescribeGroups {
                correlation_id,
                api_version,
                response: kafka_response,
            };

            if let Err(e) = response_tx.send(response) {
                pg_warning!("Failed to send DescribeGroups response: {}", e);
            } else {
                pg_log!("DescribeGroups response sent successfully");
            }
        }
        crate::kafka::KafkaRequest::ListGroups {
            correlation_id,
            api_version,
            states_filter,
            response_tx,
            ..
        } => {
            pg_log!(
                "Processing ListGroups request: correlation_id={}, states_filter={}",
                correlation_id,
                states_filter.len()
            );

            let kafka_response =
                match crate::kafka::handlers::handle_list_groups(coordinator, states_filter) {
                    Ok(response) => response,
                    Err(e) => {
                        pg_warning!("Failed to handle ListGroups request: {}", e);
                        let error_response = KafkaResponse::Error {
                            correlation_id,
                            error_code: crate::kafka::ERROR_UNKNOWN_SERVER_ERROR,
                            error_message: Some(format!(
                                "Failed to handle ListGroups request: {}",
                                e
                            )),
                        };
                        if let Err(e) = response_tx.send(error_response) {
                            pg_warning!("Failed to send error response: {}", e);
                        }
                        return;
                    }
                };

            let response = KafkaResponse::ListGroups {
                correlation_id,
                api_version,
                response: kafka_response,
            };

            if let Err(e) = response_tx.send(response) {
                pg_warning!("Failed to send ListGroups response: {}", e);
            } else {
                pg_log!("ListGroups response sent successfully");
            }
        }
    }
}

// // #[cfg(test)]
// // mod tests {
// //     use super::*;
// //     use crate::kafka::messages::KafkaResponse;
// //     use crate::testing::helpers::*;
// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::kafka::messages::KafkaResponse;
//     use crate::testing::helpers::*;

//     //     #[pgrx::pg_test]
//     //     fn test_api_versions_handler_success() {
//     //         // Test that ApiVersions request returns proper response
//     //         let (request, mut response_rx) =
//     //             mock_api_versions_request_with_client(42, "test-client".to_string());
//     #[pgrx::pg_test]
//     fn test_api_versions_handler_success() {
//         // Test that ApiVersions request returns proper response
//         let (request, mut response_rx) =
//             mock_api_versions_request_with_client(42, "test-client".to_string());

//         //         // Process the request
//         //         process_request(request);
//         // Process the request
//         process_request(request);

//         //         // Verify we got a response
//         //         let response = response_rx
//         //             .try_recv()
//         //             .expect("Should receive ApiVersions response");
//         // Verify we got a response
//         let response = response_rx
//             .try_recv()
//             .expect("Should receive ApiVersions response");

//         //         // Verify it's an ApiVersions response
//         //         match response {
//         //             KafkaResponse::ApiVersions {
//         //                 correlation_id,
//         //                 api_version: _,
//         //                 response,
//         //             } => {
//         //                 assert_eq!(correlation_id, 42);
//         //                 assert!(!response.api_keys.is_empty(), "Should have API versions");
//         //             }
//         //             _ => panic!("Expected ApiVersions response"),
//         //         }
//         //     }
//         // Verify it's an ApiVersions response
//         match response {
//             KafkaResponse::ApiVersions {
//                 correlation_id,
//                 api_version: _,
//                 response,
//             } => {
//                 assert_eq!(correlation_id, 42);
//                 assert!(!response.api_keys.is_empty(), "Should have API versions");
//             }
//             _ => panic!("Expected ApiVersions response"),
//         }
//     }

//     //     #[pgrx::pg_test]
//     //     fn test_api_versions_handler_correlation_id() {
//     //         // Test that correlation_id is preserved
//     //         for test_correlation_id in [0, 1, 42, 999, i32::MAX] {
//     //             let (request, mut response_rx) = mock_api_versions_request(test_correlation_id);
//     #[pgrx::pg_test]
//     fn test_api_versions_handler_correlation_id() {
//         // Test that correlation_id is preserved
//         for test_correlation_id in [0, 1, 42, 999, i32::MAX] {
//             let (request, mut response_rx) = mock_api_versions_request(test_correlation_id);

//             //             process_request(request);
//             process_request(request);

//             //             let response = response_rx.try_recv().expect("Should receive response");
//             let response = response_rx.try_recv().expect("Should receive response");

//             //             match response {
//             //                 KafkaResponse::ApiVersions {
//             //                     correlation_id,
//             //                     api_version: _,
//             //                     response: _,
//             //                 } => {
//             //                     assert_eq!(
//             //                         correlation_id, test_correlation_id,
//             //                         "Correlation ID should be preserved"
//             //                     );
//             //                 }
//             //                 _ => panic!("Expected ApiVersions response"),
//             //             }
//             //         }
//             //     }
//             match response {
//                 KafkaResponse::ApiVersions {
//                     correlation_id,
//                     api_version: _,
//                     response: _,
//                 } => {
//                     assert_eq!(
//                         correlation_id, test_correlation_id,
//                         "Correlation ID should be preserved"
//                     );
//                 }
//                 _ => panic!("Expected ApiVersions response"),
//             }
//         }
//     }

//     //     #[pgrx::pg_test]
//     //     fn test_api_versions_handler_supported_versions() {
//     //         // Test that response includes correct API versions
//     //         let (request, mut response_rx) = mock_api_versions_request(1);
//     #[pgrx::pg_test]
//     fn test_api_versions_handler_supported_versions() {
//         // Test that response includes correct API versions
//         let (request, mut response_rx) = mock_api_versions_request(1);

//         //         process_request(request);
//         process_request(request);

//         //         let response = response_rx.try_recv().expect("Should receive response");
//         let response = response_rx.try_recv().expect("Should receive response");

//         //         match response {
//         //             KafkaResponse::ApiVersions {
//         //                 api_version: _,
//         //                 correlation_id: _,
//         //                 response,
//         //             } => {
//         //                 assert_eq!(response.api_keys.len(), 3, "Should support 3 APIs");
//         match response {
//             KafkaResponse::ApiVersions {
//                 api_version: _,
//                 correlation_id: _,
//                 response,
//             } => {
//                 assert_eq!(response.api_keys.len(), 3, "Should support 3 APIs");

//                 //                 let api_versions_api = response
//                 //                     .api_keys
//                 //                     .iter()
//                 //                     .find(|av| av.api_key == 18)
//                 //                     .expect("Should support ApiVersions API");
//                 //                 assert_eq!(api_versions_api.min_version, 0);
//                 //                 assert_eq!(api_versions_api.max_version, 3);
//                 let api_versions_api = response
//                     .api_keys
//                     .iter()
//                     .find(|av| av.api_key == 18)
//                     .expect("Should support ApiVersions API");
//                 assert_eq!(api_versions_api.min_version, 0);
//                 assert_eq!(api_versions_api.max_version, 3);

//                 //                 let metadata_api = response
//                 //                     .api_keys
//                 //                     .iter()
//                 //                     .find(|av| av.api_key == 3)
//                 //                     .expect("Should support Metadata API");
//                 //                 assert_eq!(metadata_api.min_version, 0);
//                 //                 assert_eq!(metadata_api.max_version, 9);
//                 //             }
//                 //             _ => panic!("Expected ApiVersions response"),
//                 //         }
//                 //     }
//                 let metadata_api = response
//                     .api_keys
//                     .iter()
//                     .find(|av| av.api_key == 3)
//                     .expect("Should support Metadata API");
//                 assert_eq!(metadata_api.min_version, 0);
//                 assert_eq!(metadata_api.max_version, 9);
//             }
//             _ => panic!("Expected ApiVersions response"),
//         }
//     }

//     //     #[pgrx::pg_test]
//     //     fn test_metadata_handler_all_topics() {
//     //         let (request, mut response_rx) =
//     //             mock_metadata_request_with_client(99, "test-client".to_string(), None);
//     #[pgrx::pg_test]
//     fn test_metadata_handler_all_topics() {
//         let (request, mut response_rx) =
//             mock_metadata_request_with_client(99, "test-client".to_string(), None);

//         //         process_request(request);
//         process_request(request);

//         //         let response = response_rx.try_recv().expect("Should receive response");
//         let response = response_rx.try_recv().expect("Should receive response");

//         //         match response {
//         //             KafkaResponse::Metadata {
//         //                 correlation_id,
//         //                 api_version: _,
//         //                 response,
//         //             } => {
//         //                 assert_eq!(correlation_id, 99);
//         //                 assert!(!response.brokers.is_empty());
//         //                 assert!(!response.topics.is_empty());
//         match response {
//             KafkaResponse::Metadata {
//                 correlation_id,
//                 api_version: _,
//                 response,
//             } => {
//                 assert_eq!(correlation_id, 99);
//                 assert!(!response.brokers.is_empty());
//                 assert!(!response.topics.is_empty());

//                 //                 let test_topic = response
//                 //                     .topics
//                 //                     .iter()
//                 //                     .find(|t| t.name.as_ref().map(|n| n.0.as_str()) == Some("test-topic"))
//                 //                     .expect("Should have test-topic");
//                 //                 assert_eq!(test_topic.error_code, ERROR_NONE);
//                 //             }
//                 //             _ => panic!("Expected Metadata response"),
//                 //         }
//                 //     }
//                 let test_topic = response
//                     .topics
//                     .iter()
//                     .find(|t| t.name.as_ref().map(|n| n.0.as_str()) == Some("test-topic"))
//                     .expect("Should have test-topic");
//                 assert_eq!(test_topic.error_code, ERROR_NONE);
//             }
//             _ => panic!("Expected Metadata response"),
//         }
//     }

//     //     #[pgrx::pg_test]
//     //     fn test_metadata_handler_broker_info() {
//     //         let (request, mut response_rx) = mock_metadata_request(101, None);
//     #[pgrx::pg_test]
//     fn test_metadata_handler_broker_info() {
//         let (request, mut response_rx) = mock_metadata_request(101, None);

//         //         process_request(request);
//         process_request(request);

//         //         let response = response_rx.try_recv().expect("Should receive response");
//         let response = response_rx.try_recv().expect("Should receive response");

//         //         match response {
//         //             KafkaResponse::Metadata {
//         //                 api_version: _,
//         //                 correlation_id: _,
//         //                 response,
//         //             } => {
//         //                 assert_eq!(response.brokers.len(), 1);
//         //                 let broker = &response.brokers[0];
//         //                 assert_eq!(broker.node_id.0, DEFAULT_BROKER_ID);
//         //                 assert_eq!(broker.port, DEFAULT_KAFKA_PORT);
//         //                 assert!(!broker.host.is_empty());
//         //             }
//         //             _ => panic!("Expected Metadata response"),
//         //         }
//         //     }
//         match response {
//             KafkaResponse::Metadata {
//                 api_version: _,
//                 correlation_id: _,
//                 response,
//             } => {
//                 assert_eq!(response.brokers.len(), 1);
//                 let broker = &response.brokers[0];
//                 assert_eq!(broker.node_id.0, DEFAULT_BROKER_ID);
//                 assert_eq!(broker.port, DEFAULT_KAFKA_PORT);
//                 assert!(!broker.host.is_empty());
//             }
//             _ => panic!("Expected Metadata response"),
//         }
//     }

//     //     #[pgrx::pg_test]
//     //     fn test_metadata_handler_partition_info() {
//     //         let (request, mut response_rx) = mock_metadata_request(102, None);
//     #[pgrx::pg_test]
//     fn test_metadata_handler_partition_info() {
//         let (request, mut response_rx) = mock_metadata_request(102, None);

//         //         process_request(request);
//         process_request(request);

//         //         let response = response_rx.try_recv().expect("Should receive response");
//         let response = response_rx.try_recv().expect("Should receive response");

//         //         match response {
//         //             KafkaResponse::Metadata {
//         //                 api_version: _,
//         //                 correlation_id: _,
//         //                 response,
//         //             } => {
//         //                 let test_topic = response
//         //                     .topics
//         //                     .iter()
//         //                     .find(|t| t.name.as_ref().map(|n| n.0.as_str()) == Some("test-topic"))
//         //                     .expect("Should have test-topic");
//         match response {
//             KafkaResponse::Metadata {
//                 api_version: _,
//                 correlation_id: _,
//                 response,
//             } => {
//                 let test_topic = response
//                     .topics
//                     .iter()
//                     .find(|t| t.name.as_ref().map(|n| n.0.as_str()) == Some("test-topic"))
//                     .expect("Should have test-topic");

//                 //                 assert_eq!(test_topic.partitions.len(), 1);
//                 assert_eq!(test_topic.partitions.len(), 1);

//                 //                 let partition = &test_topic.partitions[0];
//                 //                 assert_eq!(partition.error_code, ERROR_NONE);
//                 //                 assert_eq!(partition.partition_index, 0);
//                 //                 assert_eq!(partition.leader_id.0, DEFAULT_BROKER_ID);
//                 //                 assert_eq!(
//                 //                     partition.replica_nodes,
//                 //                     vec![kafka_protocol::messages::BrokerId(DEFAULT_BROKER_ID)]
//                 //                 );
//                 //                 assert_eq!(
//                 //                     partition.isr_nodes,
//                 //                     vec![kafka_protocol::messages::BrokerId(DEFAULT_BROKER_ID)]
//                 //                 );
//                 //             }
//                 //             _ => panic!("Expected Metadata response"),
//                 //         }
//                 //     }
//                 let partition = &test_topic.partitions[0];
//                 assert_eq!(partition.error_code, ERROR_NONE);
//                 assert_eq!(partition.partition_index, 0);
//                 assert_eq!(partition.leader_id.0, DEFAULT_BROKER_ID);
//                 assert_eq!(
//                     partition.replica_nodes,
//                     vec![kafka_protocol::messages::BrokerId(DEFAULT_BROKER_ID)]
//                 );
//                 assert_eq!(
//                     partition.isr_nodes,
//                     vec![kafka_protocol::messages::BrokerId(DEFAULT_BROKER_ID)]
//                 );
//             }
//             _ => panic!("Expected Metadata response"),
//         }
//     }

//     //     #[pgrx::pg_test]
//     //     fn test_multiple_sequential_requests() {
//     //         // Test sequence: ApiVersions → Metadata
//     //         let (request1, mut response_rx1) = mock_api_versions_request(1);
//     //         process_request(request1);
//     //         let response1 = response_rx1.try_recv().expect("Should receive response 1");
//     //         assert!(matches!(response1, KafkaResponse::ApiVersions { .. }));
//     #[pgrx::pg_test]
//     fn test_multiple_sequential_requests() {
//         // Test sequence: ApiVersions → Metadata
//         let (request1, mut response_rx1) = mock_api_versions_request(1);
//         process_request(request1);
//         let response1 = response_rx1.try_recv().expect("Should receive response 1");
//         assert!(matches!(response1, KafkaResponse::ApiVersions { .. }));

//         //         let (request2, mut response_rx2) = mock_metadata_request(2, None);
//         //         process_request(request2);
//         //         let response2 = response_rx2.try_recv().expect("Should receive response 2");
//         //         assert!(matches!(response2, KafkaResponse::Metadata { .. }));
//         //     }
//         let (request2, mut response_rx2) = mock_metadata_request(2, None);
//         process_request(request2);
//         let response2 = response_rx2.try_recv().expect("Should receive response 2");
//         assert!(matches!(response2, KafkaResponse::Metadata { .. }));
//     }

//     //     #[pgrx::pg_test]
//     //     fn test_produce_creates_topic() {
//     //         // Test that Produce request auto-creates a new topic
//     //         let test_topic = "auto-created-topic";
//     //         let records = vec![simple_record(None, "test message")];
//     //         let (request, mut response_rx) = mock_produce_request(200, test_topic, 0, records);
//     #[pgrx::pg_test]
//     fn test_produce_creates_topic() {
//         // Test that Produce request auto-creates a new topic
//         let test_topic = "auto-created-topic";
//         let records = vec![simple_record(None, "test message")];
//         let (request, mut response_rx) = mock_produce_request(200, test_topic, 0, records);

//         //         process_request(request);
//         process_request(request);

//         //         let response = response_rx
//         //             .try_recv()
//         //             .expect("Should receive Produce response");
//         let response = response_rx
//             .try_recv()
//             .expect("Should receive Produce response");

//         //         match response {
//         //             KafkaResponse::Produce {
//         //                 correlation_id,
//         //                 api_version: _,
//         //                 response,
//         //             } => {
//         //                 assert_eq!(correlation_id, 200);
//         //                 assert_eq!(response.responses.len(), 1);
//         match response {
//             KafkaResponse::Produce {
//                 correlation_id,
//                 api_version: _,
//                 response,
//             } => {
//                 assert_eq!(correlation_id, 200);
//                 assert_eq!(response.responses.len(), 1);

//                 //                 let topic_response = &response.responses[0];
//                 //                 assert_eq!(topic_response.name.to_string(), test_topic);
//                 //                 assert_eq!(topic_response.partition_responses.len(), 1);
//                 let topic_response = &response.responses[0];
//                 assert_eq!(topic_response.name.to_string(), test_topic);
//                 assert_eq!(topic_response.partition_responses.len(), 1);

//                 //                 let partition_response = &topic_response.partition_responses[0];
//                 //                 assert_eq!(partition_response.error_code, ERROR_NONE);
//                 //                 assert_eq!(
//                 //                     partition_response.base_offset, 0,
//                 //                     "First message should have offset 0"
//                 //                 );
//                 //             }
//                 //             _ => panic!("Expected Produce response"),
//                 //         }
//                 let partition_response = &topic_response.partition_responses[0];
//                 assert_eq!(partition_response.error_code, ERROR_NONE);
//                 assert_eq!(
//                     partition_response.base_offset, 0,
//                     "First message should have offset 0"
//                 );
//             }
//             _ => panic!("Expected Produce response"),
//         }

//         //         // Verify topic was created in database
//         //         Spi::connect(|client| {
//         //             let result = client
//         //                 .select(
//         //                     "SELECT COUNT(*) as count FROM kafka.topics WHERE name = $1",
//         //                     None,
//         //                     &[test_topic.into()],
//         //                 )
//         //                 .expect("Query should succeed");
//         // Verify topic was created in database
//         Spi::connect(|client| {
//             let result = client
//                 .select(
//                     "SELECT COUNT(*) as count FROM kafka.topics WHERE name = $1",
//                     None,
//                     &[test_topic.into()],
//                 )
//                 .expect("Query should succeed");

//             //             let count: i64 = result.first().get_by_name("count").unwrap().unwrap();
//             //             assert_eq!(count, 1, "Topic should be created in database");
//             //         });
//             //     }
//             let count: i64 = result.first().get_by_name("count").unwrap().unwrap();
//             assert_eq!(count, 1, "Topic should be created in database");
//         });
//     }

//     //     #[pgrx::pg_test]
//     //     fn test_produce_single_message() {
//     //         // Test that a single message is inserted with correct offset
//     //         let test_topic = "single-message-topic";
//     //         let test_value = "Hello, Kafka!";
//     //         let records = vec![simple_record(Some("my-key"), test_value)];
//     //         let (request, mut response_rx) = mock_produce_request(201, test_topic, 0, records);
//     #[pgrx::pg_test]
//     fn test_produce_single_message() {
//         // Test that a single message is inserted with correct offset
//         let test_topic = "single-message-topic";
//         let test_value = "Hello, Kafka!";
//         let records = vec![simple_record(Some("my-key"), test_value)];
//         let (request, mut response_rx) = mock_produce_request(201, test_topic, 0, records);

//         //         process_request(request);
//         process_request(request);

//         //         let response = response_rx
//         //             .try_recv()
//         //             .expect("Should receive Produce response");
//         let response = response_rx
//             .try_recv()
//             .expect("Should receive Produce response");

//         //         match response {
//         //             KafkaResponse::Produce {
//         //                 correlation_id,
//         //                 api_version: _,
//         //                 response,
//         //             } => {
//         //                 assert_eq!(correlation_id, 201);
//         //                 let partition_response = &response.responses[0].partition_responses[0];
//         //                 assert_eq!(partition_response.error_code, ERROR_NONE);
//         //                 assert_eq!(partition_response.base_offset, 0);
//         //             }
//         //             _ => panic!("Expected Produce response"),
//         //         }
//         match response {
//             KafkaResponse::Produce {
//                 correlation_id,
//                 api_version: _,
//                 response,
//             } => {
//                 assert_eq!(correlation_id, 201);
//                 let partition_response = &response.responses[0].partition_responses[0];
//                 assert_eq!(partition_response.error_code, ERROR_NONE);
//                 assert_eq!(partition_response.base_offset, 0);
//             }
//             _ => panic!("Expected Produce response"),
//         }

//         //         // Verify message is in database
//         //         Spi::connect(|client| {
//         //             let result = client
//         //                 .select(
//         //                     "SELECT partition_offset, value FROM kafka.messages m
//         //                  JOIN kafka.topics t ON m.topic_id = t.id
//         //                  WHERE t.name = $1 AND m.partition_id = 0
//         //                  ORDER BY partition_offset",
//         //                     None,
//         //                     &[test_topic.into()],
//         //                 )
//         //                 .expect("Query should succeed");
//         // Verify message is in database
//         Spi::connect(|client| {
//             let result = client
//                 .select(
//                     "SELECT partition_offset, value FROM kafka.messages m
//                  JOIN kafka.topics t ON m.topic_id = t.id
//                  WHERE t.name = $1 AND m.partition_id = 0
//                  ORDER BY partition_offset",
//                     None,
//                     &[test_topic.into()],
//                 )
//                 .expect("Query should succeed");

//             //             assert_eq!(result.len(), 1, "Should have exactly 1 message");
//             //             let row = result.first();
//             //             let offset: i64 = row.get_by_name("partition_offset").unwrap().unwrap();
//             //             let value: Vec<u8> = row.get_by_name("value").unwrap().unwrap();
//             assert_eq!(result.len(), 1, "Should have exactly 1 message");
//             let row = result.first();
//             let offset: i64 = row.get_by_name("partition_offset").unwrap().unwrap();
//             let value: Vec<u8> = row.get_by_name("value").unwrap().unwrap();

//             //             assert_eq!(offset, 0);
//             //             assert_eq!(value, test_value.as_bytes());
//             //         });
//             //     }
//             assert_eq!(offset, 0);
//             assert_eq!(value, test_value.as_bytes());
//         });
//     }

//     //     #[pgrx::pg_test]
//     //     fn test_produce_batch_messages() {
//     //         // Test that batch of messages gets consecutive offsets
//     //         let test_topic = "batch-topic";
//     //         let mut records = Vec::new();
//     //         for i in 0..10 {
//     //             records.push(simple_record(None, &format!("message-{}", i)));
//     //         }
//     #[pgrx::pg_test]
//     fn test_produce_batch_messages() {
//         // Test that batch of messages gets consecutive offsets
//         let test_topic = "batch-topic";
//         let mut records = Vec::new();
//         for i in 0..10 {
//             records.push(simple_record(None, &format!("message-{}", i)));
//         }

//         //         let (request, mut response_rx) = mock_produce_request(202, test_topic, 0, records);
//         let (request, mut response_rx) = mock_produce_request(202, test_topic, 0, records);

//         //         process_request(request);
//         process_request(request);

//         //         let response = response_rx
//         //             .try_recv()
//         //             .expect("Should receive Produce response");
//         let response = response_rx
//             .try_recv()
//             .expect("Should receive Produce response");

//         //         match response {
//         //             KafkaResponse::Produce {
//         //                 correlation_id,
//         //                 api_version: _,
//         //                 response,
//         //             } => {
//         //                 assert_eq!(correlation_id, 202);
//         //                 let partition_response = &response.responses[0].partition_responses[0];
//         //                 assert_eq!(partition_response.error_code, ERROR_NONE);
//         //                 assert_eq!(
//         //                     partition_response.base_offset, 0,
//         //                     "Batch should start at offset 0"
//         //                 );
//         //             }
//         //             _ => panic!("Expected Produce response"),
//         //         }
//         match response {
//             KafkaResponse::Produce {
//                 correlation_id,
//                 api_version: _,
//                 response,
//             } => {
//                 assert_eq!(correlation_id, 202);
//                 let partition_response = &response.responses[0].partition_responses[0];
//                 assert_eq!(partition_response.error_code, ERROR_NONE);
//                 assert_eq!(
//                     partition_response.base_offset, 0,
//                     "Batch should start at offset 0"
//                 );
//             }
//             _ => panic!("Expected Produce response"),
//         }

//         //         // Verify all 10 messages have consecutive offsets
//         //         Spi::connect(|client| {
//         //             let result = client
//         //                 .select(
//         //                     "SELECT COUNT(*) as count,
//         //                         MIN(partition_offset) as min_offset,
//         //                         MAX(partition_offset) as max_offset
//         //                  FROM kafka.messages m
//         //                  JOIN kafka.topics t ON m.topic_id = t.id
//         //                  WHERE t.name = $1 AND m.partition_id = 0",
//         //                     None,
//         //                     &[test_topic.into()],
//         //                 )
//         //                 .expect("Query should succeed");
//         // Verify all 10 messages have consecutive offsets
//         Spi::connect(|client| {
//             let result = client
//                 .select(
//                     "SELECT COUNT(*) as count,
//                         MIN(partition_offset) as min_offset,
//                         MAX(partition_offset) as max_offset
//                  FROM kafka.messages m
//                  JOIN kafka.topics t ON m.topic_id = t.id
//                  WHERE t.name = $1 AND m.partition_id = 0",
//                     None,
//                     &[test_topic.into()],
//                 )
//                 .expect("Query should succeed");

//             //             let row = result.first();
//             //             let count: i64 = row.get_by_name("count").unwrap().unwrap();
//             //             let min_offset: i64 = row.get_by_name("min_offset").unwrap().unwrap();
//             //             let max_offset: i64 = row.get_by_name("max_offset").unwrap().unwrap();
//             let row = result.first();
//             let count: i64 = row.get_by_name("count").unwrap().unwrap();
//             let min_offset: i64 = row.get_by_name("min_offset").unwrap().unwrap();
//             let max_offset: i64 = row.get_by_name("max_offset").unwrap().unwrap();

//             //             assert_eq!(count, 10, "Should have exactly 10 messages");
//             //             assert_eq!(min_offset, 0, "First offset should be 0");
//             //             assert_eq!(
//             //                 max_offset, 9,
//             //                 "Last offset should be 9 (consecutive from 0)"
//             //             );
//             //         });
//             //     }
//             assert_eq!(count, 10, "Should have exactly 10 messages");
//             assert_eq!(min_offset, 0, "First offset should be 0");
//             assert_eq!(
//                 max_offset, 9,
//                 "Last offset should be 9 (consecutive from 0)"
//             );
//         });
//     }

//     //     #[pgrx::pg_test]
//     //     fn test_produce_with_headers() {
//     //         // Test that headers are correctly serialized to JSONB
//     //         let test_topic = "headers-topic";
//     //         let headers = vec![
//     //             ("correlation-id", b"12345".as_ref()),
//     //             ("source", b"test-client".as_ref()),
//     //         ];
//     //         let records = vec![record_with_headers(None, "message with headers", headers)];
//     //         let (request, mut response_rx) = mock_produce_request(203, test_topic, 0, records);
//     #[pgrx::pg_test]
//     fn test_produce_with_headers() {
//         // Test that headers are correctly serialized to JSONB
//         let test_topic = "headers-topic";
//         let headers = vec![
//             ("correlation-id", b"12345".as_ref()),
//             ("source", b"test-client".as_ref()),
//         ];
//         let records = vec![record_with_headers(None, "message with headers", headers)];
//         let (request, mut response_rx) = mock_produce_request(203, test_topic, 0, records);

//         //         process_request(request);
//         process_request(request);

//         //         let response = response_rx
//         //             .try_recv()
//         //             .expect("Should receive Produce response");
//         let response = response_rx
//             .try_recv()
//             .expect("Should receive Produce response");

//         //         match response {
//         //             KafkaResponse::Produce {
//         //                 correlation_id,
//         //                 api_version: _,
//         //                 response,
//         //             } => {
//         //                 assert_eq!(correlation_id, 203);
//         //                 let partition_response = &response.responses[0].partition_responses[0];
//         //                 assert_eq!(partition_response.error_code, ERROR_NONE);
//         //             }
//         //             _ => panic!("Expected Produce response"),
//         //         }
//         match response {
//             KafkaResponse::Produce {
//                 correlation_id,
//                 api_version: _,
//                 response,
//             } => {
//                 assert_eq!(correlation_id, 203);
//                 let partition_response = &response.responses[0].partition_responses[0];
//                 assert_eq!(partition_response.error_code, ERROR_NONE);
//             }
//             _ => panic!("Expected Produce response"),
//         }

//         //         // Verify headers are in JSONB format
//         //         Spi::connect(|client| {
//         //             let result = client
//         //                 .select(
//         //                     "SELECT headers FROM kafka.messages m
//         //                  JOIN kafka.topics t ON m.topic_id = t.id
//         //                  WHERE t.name = $1",
//         //                     None,
//         //                     &[test_topic.into()],
//         //                 )
//         //                 .expect("Query should succeed");
//         // Verify headers are in JSONB format
//         Spi::connect(|client| {
//             let result = client
//                 .select(
//                     "SELECT headers FROM kafka.messages m
//                  JOIN kafka.topics t ON m.topic_id = t.id
//                  WHERE t.name = $1",
//                     None,
//                     &[test_topic.into()],
//                 )
//                 .expect("Query should succeed");

//             //             assert_eq!(result.len(), 1);
//             //             let row = result.first();
//             //             let headers_json: pgrx::JsonB = row.get_by_name("headers").unwrap().unwrap();
//             assert_eq!(result.len(), 1);
//             let row = result.first();
//             let headers_json: pgrx::JsonB = row.get_by_name("headers").unwrap().unwrap();

//             //             // Headers should be a JSON array with 2 elements
//             //             let headers_value = headers_json.0;
//             //             assert!(headers_value.is_array(), "Headers should be JSONB array");
//             //             let headers_array = headers_value.as_array().unwrap();
//             //             assert_eq!(headers_array.len(), 2, "Should have 2 headers");
//             //         });
//             //     }
//             // Headers should be a JSON array with 2 elements
//             let headers_value = headers_json.0;
//             assert!(headers_value.is_array(), "Headers should be JSONB array");
//             let headers_array = headers_value.as_array().unwrap();
//             assert_eq!(headers_array.len(), 2, "Should have 2 headers");
//         });
//     }

//     //     #[pgrx::pg_test]
//     //     fn test_produce_invalid_partition() {
//     //         // Test that partition > 0 returns an error (we only support single-partition topics)
//     //         let test_topic = "invalid-partition-topic";
//     //         let records = vec![simple_record(None, "test message")];
//     //         let (request, mut response_rx) = mock_produce_request(204, test_topic, 1, records); // partition 1 (invalid)
//     #[pgrx::pg_test]
//     fn test_produce_invalid_partition() {
//         // Test that partition > 0 returns an error (we only support single-partition topics)
//         let test_topic = "invalid-partition-topic";
//         let records = vec![simple_record(None, "test message")];
//         let (request, mut response_rx) = mock_produce_request(204, test_topic, 1, records); // partition 1 (invalid)

//         //         process_request(request);
//         process_request(request);

//         //         let response = response_rx
//         //             .try_recv()
//         //             .expect("Should receive Produce response");
//         let response = response_rx
//             .try_recv()
//             .expect("Should receive Produce response");

//         //         match response {
//         //             KafkaResponse::Produce {
//         //                 correlation_id,
//         //                 api_version: _,
//         //                 response,
//         //             } => {
//         //                 assert_eq!(correlation_id, 204);
//         //                 let partition_response = &response.responses[0].partition_responses[0];
//         //                 assert_eq!(
//         //                     partition_response.error_code, ERROR_UNKNOWN_TOPIC_OR_PARTITION,
//         //                     "Should return error for invalid partition"
//         //                 );
//         //             }
//         //             _ => panic!("Expected Produce response"),
//         //         }
//         //     }
//         // }
//         match response {
//             KafkaResponse::Produce {
//                 correlation_id,
//                 api_version: _,
//                 response,
//             } => {
//                 assert_eq!(correlation_id, 204);
//                 let partition_response = &response.responses[0].partition_responses[0];
//                 assert_eq!(
//                     partition_response.error_code, ERROR_UNKNOWN_TOPIC_OR_PARTITION,
//                     "Should return error for invalid partition"
//                 );
//             }
//             _ => panic!("Expected Produce response"),
//         }
//     }
// }
