// Background worker module for pg_kafka
//
// This module implements the persistent background worker that runs the Kafka
// protocol listener. The worker is started by Postgres when the extension loads
// (via shared_preload_libraries in postgresql.conf).

use pgrx::bgworkers::{BackgroundWorker, SignalWakeFlags};
use pgrx::prelude::*;
use pgrx::Spi;
use std::panic::{catch_unwind, AssertUnwindSafe};

// Import conditional logging macros from lib.rs
use crate::kafka::constants::*;
use crate::{pg_log, pg_warning};

/// How often to check for member timeouts (in main loop iterations)
/// With ~100ms per iteration, 10 iterations = ~1 second between checks
const TIMEOUT_CHECK_INTERVAL_ITERATIONS: u32 = 10;

/// Verify that the kafka schema and required tables exist.
/// Returns Ok(()) if all tables exist, Err with description if any are missing.
fn verify_schema(database: &str) -> Result<(), String> {
    let required_tables = ["topics", "messages", "consumer_offsets"];

    let result: Result<Option<String>, pgrx::spi::SpiError> = Spi::connect(|client| {
        for table in required_tables {
            let query = format!(
                "SELECT 1 FROM information_schema.tables \
                 WHERE table_schema = 'kafka' AND table_name = '{}'",
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

    // Step 2: Load configuration (GUCs are available after _PG_init)
    let config = crate::config::Config::load();

    // Step 3: Connect to the configured database (required for SPI access)
    // IMPORTANT: Must connect to the database where CREATE EXTENSION was run
    BackgroundWorker::connect_worker_to_spi(Some(&config.database), None);

    // Step 3b: Wait for schema to exist before starting listener
    // The background worker starts when Postgres boots, but CREATE EXTENSION
    // may not have run yet. We retry with a delay to handle this timing issue.
    const MAX_SCHEMA_WAIT_MS: u64 = 60_000; // 60 seconds max wait
    const SCHEMA_CHECK_INTERVAL_MS: u64 = 500;
    let mut waited_ms = 0u64;
    let database_name = config.database.clone();
    loop {
        // SPI calls require a transaction context
        let result = BackgroundWorker::transaction(|| verify_schema(&database_name));
        match result {
            Ok(()) => {
                if waited_ms > 0 {
                    log!("pg_kafka schema found after waiting {} ms", waited_ms);
                }
                break;
            }
            Err(e) => {
                if waited_ms >= MAX_SCHEMA_WAIT_MS {
                    pgrx::error!(
                        "pg_kafka schema not initialized after {} seconds: {}. \
                         Run 'CREATE EXTENSION pg_kafka' in database '{}'",
                        MAX_SCHEMA_WAIT_MS / 1000,
                        e,
                        config.database
                    );
                }
                if waited_ms == 0 {
                    log!(
                        "Waiting for pg_kafka schema (run 'CREATE EXTENSION pg_kafka' in database '{}')",
                        config.database
                    );
                }
                std::thread::sleep(std::time::Duration::from_millis(SCHEMA_CHECK_INTERVAL_MS));
                waited_ms = waited_ms.saturating_add(SCHEMA_CHECK_INTERVAL_MS);
            }
        }
        // Check for shutdown signal while waiting
        if BackgroundWorker::sigterm_received() {
            log!("Shutdown signal received while waiting for schema");
            return;
        }
    }

    log!(
        "pg_kafka background worker started (port: {}, host: {}, database: {})",
        config.port,
        config.host,
        config.database
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

    // Step 6b: Bind TCP listener BEFORE starting async tasks
    // This ensures bind failures are fatal and immediate, not hidden in retry loops
    let bind_addr = format!("{}:{}", host, port);
    let tcp_listener = local_set.block_on(&runtime, async {
        tokio::net::TcpListener::bind(&bind_addr).await
    });

    let tcp_listener = match tcp_listener {
        Ok(listener) => {
            pg_log!("TCP listener bound to {}", bind_addr);
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

    // Spawn the listener task on the LocalSet
    pg_log!("Spawning TCP listener task on LocalSet");
    let listener_handle = local_set.spawn_local(async move {
        pg_log!("Listener task started");
        // No retry loop needed - binding already succeeded above
        if let Err(e) =
            crate::kafka::listener::run(shutdown_rx, tcp_listener, log_connections).await
        {
            pgrx::warning!("TCP listener error: {}", e);
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
    // This loop serves FOUR critical functions:
    //
    // 1. PROCESS DATABASE REQUESTS (Sync World)
    //    - Receive Kafka requests from the queue
    //    - Execute blocking database operations via Postgres SPI
    //    - Send responses back to async tasks
    //    - WHY HERE: SPI can ONLY be called from the main thread in sync context
    //
    // 1.5. CHECK MEMBER TIMEOUTS (Phase 5: Automatic Rebalancing)
    //    - Periodically scan consumer groups for timed-out members
    //    - Remove dead members and trigger rebalance
    //    - WHY HERE: Runs ~every 1 second to detect stale consumers
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
    // - ~1 second for timeout check: Reasonable granularity for session detection
    //
    // This architecture solves the fundamental incompatibility:
    // - Tokio wants async functions (network I/O benefits from non-blocking)
    // - Postgres SPI wants sync functions on main thread (database safety)
    // - The queue bridges these worlds cleanly
    // ═══════════════════════════════════════════════════════════════════════════════

    // Counter for throttling periodic tasks (timeout checks)
    let mut loop_iteration: u32 = 0;

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
                // Wrap in catch_unwind to prevent handler panics from crashing the worker
                let result = catch_unwind(AssertUnwindSafe(|| {
                    process_request(request, &coord, &broker_h, broker_p);
                }));

                if let Err(panic_info) = result {
                    // Extract panic message for logging
                    let panic_msg = if let Some(s) = panic_info.downcast_ref::<&str>() {
                        s.to_string()
                    } else if let Some(s) = panic_info.downcast_ref::<String>() {
                        s.clone()
                    } else {
                        "Unknown panic".to_string()
                    };
                    pg_warning!("Handler panic caught (worker survived): {}", panic_msg);
                    // Note: Response channel may be closed, but worker continues
                }
            });
        }

        // ┌─────────────────────────────────────────────────────────────┐
        // │ PART 1.5: Check Member Timeouts (Phase 5: Rebalancing)     │
        // └─────────────────────────────────────────────────────────────┘
        // Periodically check for members that have stopped sending heartbeats.
        // This enables automatic rebalancing when consumers die without LeaveGroup.
        loop_iteration = loop_iteration.wrapping_add(1);
        if loop_iteration.is_multiple_of(TIMEOUT_CHECK_INTERVAL_ITERATIONS) {
            let removed = coordinator.check_and_remove_timed_out_members();
            for (group_id, member_id) in removed {
                pg_log!(
                    "Timeout: Removed member {} from group {}, triggering rebalance",
                    member_id,
                    group_id
                );
            }
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
            // Use timeout to limit async processing duration, then return to sync context
            let _ = tokio::time::timeout(
                core::time::Duration::from_millis(crate::kafka::ASYNC_IO_INTERVAL_MS),
                std::future::pending::<()>(),
            )
            .await;
            // Timeout is expected (Err) - it means we processed I/O for 100ms
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
/// This function has been refactored to use:
/// - Repository Pattern: Storage via PostgresStore (implements KafkaStore trait)
/// - Dispatch Pattern: Common error handling via dispatch helpers
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
    use crate::kafka::dispatch::{dispatch_infallible, dispatch_response};
    use crate::kafka::messages::KafkaResponse;
    use crate::kafka::{handlers, PostgresStore};

    match request {
        // ===== ApiVersions (infallible - no storage) =====
        crate::kafka::KafkaRequest::ApiVersions {
            correlation_id,
            api_version,
            response_tx,
            ..
        } => {
            dispatch_infallible(
                "ApiVersions",
                response_tx,
                handlers::handle_api_versions,
                |r| KafkaResponse::ApiVersions {
                    correlation_id,
                    api_version,
                    response: r,
                },
            );
        }

        // ===== Metadata =====
        crate::kafka::KafkaRequest::Metadata {
            correlation_id,
            api_version,
            topics: requested_topics,
            response_tx,
            ..
        } => {
            let config = crate::config::Config::load();
            let advertised_host = if config.host == "0.0.0.0" || config.host.is_empty() {
                "localhost".to_string()
            } else {
                config.host.clone()
            };
            let store = PostgresStore::new();

            dispatch_response(
                "Metadata",
                correlation_id,
                response_tx,
                || {
                    handlers::handle_metadata(
                        &store,
                        requested_topics,
                        advertised_host,
                        config.port,
                    )
                },
                |r| KafkaResponse::Metadata {
                    correlation_id,
                    api_version,
                    response: r,
                },
            );
        }

        // ===== Produce =====
        crate::kafka::KafkaRequest::Produce {
            correlation_id,
            api_version,
            acks,
            topic_data,
            response_tx,
            ..
        } => {
            // Handle acks=0 (fire-and-forget) - not yet supported
            if acks == 0 {
                pg_warning!("acks=0 (fire-and-forget) not yet supported");
                let _ = response_tx.send(KafkaResponse::Error {
                    correlation_id,
                    error_code: ERROR_UNSUPPORTED_VERSION,
                    error_message: Some("acks=0 not yet supported".to_string()),
                });
                return;
            }

            let store = PostgresStore::new();
            dispatch_response(
                "Produce",
                correlation_id,
                response_tx,
                || handlers::handle_produce(&store, topic_data),
                |r| KafkaResponse::Produce {
                    correlation_id,
                    api_version,
                    response: r,
                },
            );
        }

        // ===== Fetch =====
        crate::kafka::KafkaRequest::Fetch {
            correlation_id,
            api_version,
            topic_data,
            response_tx,
            ..
        } => {
            let store = PostgresStore::new();
            dispatch_response(
                "Fetch",
                correlation_id,
                response_tx,
                || handlers::handle_fetch(&store, topic_data),
                |r| KafkaResponse::Fetch {
                    correlation_id,
                    api_version,
                    response: r,
                },
            );
        }

        // ===== OffsetCommit =====
        crate::kafka::KafkaRequest::OffsetCommit {
            correlation_id,
            api_version,
            group_id,
            topics,
            response_tx,
            ..
        } => {
            let store = PostgresStore::new();
            dispatch_response(
                "OffsetCommit",
                correlation_id,
                response_tx,
                || handlers::handle_offset_commit(&store, group_id, topics),
                |r| KafkaResponse::OffsetCommit {
                    correlation_id,
                    api_version,
                    response: r,
                },
            );
        }

        // ===== OffsetFetch =====
        crate::kafka::KafkaRequest::OffsetFetch {
            correlation_id,
            api_version,
            group_id,
            topics,
            response_tx,
            ..
        } => {
            let store = PostgresStore::new();
            dispatch_response(
                "OffsetFetch",
                correlation_id,
                response_tx,
                || handlers::handle_offset_fetch(&store, group_id, topics),
                |r| KafkaResponse::OffsetFetch {
                    correlation_id,
                    api_version,
                    response: r,
                },
            );
        }

        // ===== FindCoordinator =====
        crate::kafka::KafkaRequest::FindCoordinator {
            correlation_id,
            api_version,
            key,
            key_type,
            response_tx,
            ..
        } => {
            let host = broker_host.to_string();
            let port = broker_port;
            dispatch_response(
                "FindCoordinator",
                correlation_id,
                response_tx,
                || handlers::handle_find_coordinator(host, port, key, key_type),
                |r| KafkaResponse::FindCoordinator {
                    correlation_id,
                    api_version,
                    response: r,
                },
            );
        }

        // ===== JoinGroup =====
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
            let coord = coordinator.clone();
            let cid = client_id.unwrap_or_else(|| "unknown".to_string());
            dispatch_response(
                "JoinGroup",
                correlation_id,
                response_tx,
                || {
                    handlers::handle_join_group(
                        &coord,
                        group_id,
                        member_id,
                        cid,
                        session_timeout_ms,
                        rebalance_timeout_ms,
                        protocol_type,
                        protocols,
                    )
                },
                |r| KafkaResponse::JoinGroup {
                    correlation_id,
                    api_version,
                    response: r,
                },
            );
        }

        // ===== SyncGroup =====
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
            let coord = coordinator.clone();
            let store = PostgresStore::new();
            dispatch_response(
                "SyncGroup",
                correlation_id,
                response_tx,
                || {
                    handlers::handle_sync_group(
                        &coord,
                        &store,
                        group_id,
                        member_id,
                        generation_id,
                        assignments,
                    )
                },
                |r| KafkaResponse::SyncGroup {
                    correlation_id,
                    api_version,
                    response: r,
                },
            );
        }

        // ===== Heartbeat =====
        crate::kafka::KafkaRequest::Heartbeat {
            correlation_id,
            api_version,
            group_id,
            member_id,
            generation_id,
            response_tx,
            ..
        } => {
            let coord = coordinator.clone();
            dispatch_response(
                "Heartbeat",
                correlation_id,
                response_tx,
                || handlers::handle_heartbeat(&coord, group_id, member_id, generation_id),
                |r| KafkaResponse::Heartbeat {
                    correlation_id,
                    api_version,
                    response: r,
                },
            );
        }

        // ===== LeaveGroup =====
        crate::kafka::KafkaRequest::LeaveGroup {
            correlation_id,
            api_version,
            group_id,
            member_id,
            response_tx,
            ..
        } => {
            let coord = coordinator.clone();
            dispatch_response(
                "LeaveGroup",
                correlation_id,
                response_tx,
                || handlers::handle_leave_group(&coord, group_id, member_id),
                |r| KafkaResponse::LeaveGroup {
                    correlation_id,
                    api_version,
                    response: r,
                },
            );
        }

        // ===== ListOffsets =====
        crate::kafka::KafkaRequest::ListOffsets {
            correlation_id,
            api_version,
            topics,
            response_tx,
            ..
        } => {
            let store = PostgresStore::new();
            dispatch_response(
                "ListOffsets",
                correlation_id,
                response_tx,
                || handlers::handle_list_offsets(&store, topics),
                |r| KafkaResponse::ListOffsets {
                    correlation_id,
                    api_version,
                    response: r,
                },
            );
        }

        // ===== DescribeGroups =====
        crate::kafka::KafkaRequest::DescribeGroups {
            correlation_id,
            api_version,
            groups,
            response_tx,
            ..
        } => {
            let coord = coordinator.clone();
            dispatch_response(
                "DescribeGroups",
                correlation_id,
                response_tx,
                || handlers::handle_describe_groups(&coord, groups),
                |r| KafkaResponse::DescribeGroups {
                    correlation_id,
                    api_version,
                    response: r,
                },
            );
        }

        // ===== ListGroups =====
        crate::kafka::KafkaRequest::ListGroups {
            correlation_id,
            api_version,
            states_filter,
            response_tx,
            ..
        } => {
            let coord = coordinator.clone();
            dispatch_response(
                "ListGroups",
                correlation_id,
                response_tx,
                || handlers::handle_list_groups(&coord, states_filter),
                |r| KafkaResponse::ListGroups {
                    correlation_id,
                    api_version,
                    response: r,
                },
            );
        }

        // ===== CreateTopics (Phase 6) =====
        crate::kafka::KafkaRequest::CreateTopics {
            correlation_id,
            api_version,
            topics,
            validate_only,
            response_tx,
            ..
        } => {
            let store = PostgresStore::new();
            dispatch_response(
                "CreateTopics",
                correlation_id,
                response_tx,
                || handlers::handle_create_topics(&store, topics, validate_only),
                |r| KafkaResponse::CreateTopics {
                    correlation_id,
                    api_version,
                    response: r,
                },
            );
        }

        // ===== DeleteTopics (Phase 6) =====
        crate::kafka::KafkaRequest::DeleteTopics {
            correlation_id,
            api_version,
            topic_names,
            response_tx,
            ..
        } => {
            let store = PostgresStore::new();
            dispatch_response(
                "DeleteTopics",
                correlation_id,
                response_tx,
                || handlers::handle_delete_topics(&store, topic_names),
                |r| KafkaResponse::DeleteTopics {
                    correlation_id,
                    api_version,
                    response: r,
                },
            );
        }

        // ===== CreatePartitions (Phase 6) =====
        crate::kafka::KafkaRequest::CreatePartitions {
            correlation_id,
            api_version,
            topics,
            validate_only,
            response_tx,
            ..
        } => {
            let store = PostgresStore::new();
            dispatch_response(
                "CreatePartitions",
                correlation_id,
                response_tx,
                || handlers::handle_create_partitions(&store, topics, validate_only),
                |r| KafkaResponse::CreatePartitions {
                    correlation_id,
                    api_version,
                    response: r,
                },
            );
        }

        // ===== DeleteGroups (Phase 6) =====
        crate::kafka::KafkaRequest::DeleteGroups {
            correlation_id,
            api_version,
            groups_names,
            response_tx,
            ..
        } => {
            let store = PostgresStore::new();
            let coord = coordinator.clone();
            dispatch_response(
                "DeleteGroups",
                correlation_id,
                response_tx,
                || handlers::handle_delete_groups(&store, &coord, groups_names),
                |r| KafkaResponse::DeleteGroups {
                    correlation_id,
                    api_version,
                    response: r,
                },
            );
        }
    }
}
