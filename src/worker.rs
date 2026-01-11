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
//    - Processes requests via SPI within transactions
//    - Instant wake-up when requests arrive (no polling delay)
//    - Uses pgrx logging (safe on main thread)
//
// This eliminates the 100ms latency floor from the previous time-slicing design.

use crossbeam_channel::RecvTimeoutError;
use pgrx::bgworkers::{BackgroundWorker, SignalWakeFlags};
use pgrx::prelude::*;
use pgrx::Spi;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::time::{Duration, Instant};

// Import conditional logging macros from lib.rs
use crate::kafka::constants::*;
use crate::kafka::shadow::ShadowStore;
use crate::kafka::{KafkaStore, PostgresStore};
use crate::{pg_log, pg_warning};

/// How often to check for member timeouts
const TIMEOUT_CHECK_INTERVAL: Duration = Duration::from_secs(1);

/// How often to check for timed-out transactions (less frequent than member timeouts)
const TXN_TIMEOUT_CHECK_INTERVAL: Duration = Duration::from_secs(10);

/// How often to reload shadow config and flush metrics (Phase 11)
const SHADOW_CONFIG_RELOAD_INTERVAL: Duration = Duration::from_secs(30);

/// Timeout for blocking recv - allows periodic signal checks
const RECV_TIMEOUT: Duration = Duration::from_millis(100);

/// Capacity of the bounded request channel (backpressure threshold)
const REQUEST_CHANNEL_CAPACITY: usize = 10_000;

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

/// Extract producer metadata from the first partition with valid metadata (Phase 9)
///
/// Idempotent producers embed producer_id, producer_epoch, and base_sequence in the
/// RecordBatch header. This function scans all topic partitions to find the first
/// batch with valid producer metadata (producer_id >= 0).
///
/// Returns Some(ProducerMetadata) if found, None for non-idempotent producers.
fn extract_producer_metadata(
    topic_data: &[crate::kafka::messages::TopicProduceData],
) -> Option<crate::kafka::messages::ProducerMetadata> {
    for topic in topic_data {
        for partition_data in &topic.partitions {
            if let Some(ref metadata) = partition_data.producer_metadata {
                // Only return metadata for actual idempotent producers (producer_id >= 0)
                // Non-idempotent producers have producer_id = -1
                if metadata.producer_id >= 0 {
                    return Some(metadata.clone());
                }
            }
        }
    }
    None
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
    //
    // Design note: We use polling (500ms interval) rather than event-based notification
    // because PostgreSQL doesn't provide a cross-process notification mechanism for
    // schema creation that works reliably during startup. The polling overhead is
    // minimal since this only runs once at startup, and 500ms strikes a balance
    // between responsiveness and CPU usage. The loop also checks SIGTERM to ensure
    // clean shutdown if Postgres stops before the schema is ready.
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

    // Step 3c: Initialize shadow mode store (Phase 11)
    // Always create ShadowStore wrapping PostgresStore. Shadow mode can be
    // enabled/disabled at runtime via pg_kafka.shadow_mode_enabled GUC.
    // The producer is lazily initialized on first use when enabled.
    let shadow_store: std::sync::Arc<ShadowStore<PostgresStore>> =
        std::sync::Arc::new(ShadowStore::new(PostgresStore::new()));

    if config.shadow_mode_enabled {
        log!(
            "Shadow mode enabled, configured to forward to: {}",
            config.shadow_bootstrap_servers
        );
    }

    // Step 4: Clone config values for use in threads
    let port = config.port;
    let host = config.host.clone();
    let log_connections = config.log_connections;
    let shutdown_timeout = Duration::from_millis(config.shutdown_timeout_ms as u64);
    let default_partitions = config.default_partitions;
    let fetch_poll_interval_ms = config.fetch_poll_interval_ms;
    let enable_long_polling = config.enable_long_polling;
    let compression = parse_compression_type(&config.compression_type);
    let log_timing = config.log_timing;
    // Convert 0.0.0.0 to localhost for advertised host (used in Metadata and FindCoordinator)
    // 0.0.0.0 is not routable from clients - they need a resolvable hostname
    let broker_host = if host == "0.0.0.0" || host.is_empty() {
        "localhost".to_string()
    } else {
        host.clone()
    };

    // Step 5: Create bounded request channel (backpressure at 10,000 pending requests)
    // This is the bridge between the network thread and the database thread.
    let (request_tx, request_rx) =
        crossbeam_channel::bounded::<crate::kafka::KafkaRequest>(REQUEST_CHANNEL_CAPACITY);

    // Step 5b: Create notification channel for long polling support
    // This channel sends notifications from the database thread to the network thread
    // when new messages are produced. Used to wake up waiting FetchRequest handlers.
    // Bounded to prevent OOM if produce rate >> consume rate. Dropped notifications
    // are acceptable - fetch handlers fall back to polling.
    let (notify_tx, notify_rx) =
        crossbeam_channel::bounded::<crate::kafka::InternalNotification>(10_000);

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
    let bind_addr = format!("{}:{}", host, port);

    let std_listener = match std::net::TcpListener::bind(&bind_addr) {
        Ok(listener) => {
            // Set non-blocking mode for tokio compatibility
            if let Err(e) = listener.set_nonblocking(true) {
                pgrx::error!("Failed to set non-blocking mode: {}", e);
            }
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

    // Step 8: Spawn the network thread
    // This thread runs a multi-threaded tokio runtime for handling all network I/O.
    // CRITICAL: This thread must NEVER call pgrx functions - use tracing for logging.
    pg_log!("Spawning network thread with tokio multi-thread runtime");
    let network_handle = std::thread::Builder::new()
        .name("pg_kafka-net".to_string())
        .spawn(move || {
            // Initialize tracing subscriber for this thread
            // Use error level to reduce log output in production
            // Enable with RUST_LOG=pg_kafka=debug for debugging
            let _ = tracing_subscriber::fmt()
                .with_env_filter(
                    tracing_subscriber::EnvFilter::from_default_env()
                        .add_directive("pg_kafka=error".parse().unwrap()),
                )
                .with_target(false)
                .try_init();

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
            rt.block_on(async move {
                // Convert std listener to tokio listener inside the async context
                // This ensures the listener is registered with THIS runtime's reactor
                let tcp_listener = match tokio::net::TcpListener::from_std(std_listener) {
                    Ok(listener) => listener,
                    Err(e) => {
                        tracing::error!("Failed to convert std listener to tokio: {}", e);
                        return;
                    }
                };

                if let Err(e) = crate::kafka::listener::run(
                    tcp_listener,
                    request_tx,
                    notify_rx,
                    shutdown_rx,
                    log_connections,
                    enable_long_polling,
                    fetch_poll_interval_ms,
                )
                .await
                {
                    tracing::error!("TCP listener error: {}", e);
                }
            });
        })
        .expect("Failed to spawn network thread");

    // Step 9: Create consumer group coordinator
    // This manages consumer group membership state in-memory
    // Arc<RwLock> makes it safe to access from the coordinator handlers
    let coordinator = std::sync::Arc::new(crate::kafka::GroupCoordinator::new());

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
    // 2. PROCESS DATABASE REQUEST (SPI in Transaction)
    //    - Execute the request within BackgroundWorker::transaction()
    //    - Send response back via the request's response channel
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
    let mut last_shadow_config_check = Instant::now();

    // Phase 11: Initial shadow config load
    {
        let shadow_init = AssertUnwindSafe(&shadow_store);
        BackgroundWorker::transaction(move || match shadow_init.load_topic_config_from_db() {
            Ok(count) => {
                if count > 0 {
                    log!("Loaded {} shadow topic configurations", count);
                }
            }
            Err(e) => {
                pg_warning!("Failed to load shadow config: {:?}", e);
            }
        });
    }

    loop {
        // ┌─────────────────────────────────────────────────────────────┐
        // │ Check for Postgres Shutdown Signal                         │
        // └─────────────────────────────────────────────────────────────┘
        if BackgroundWorker::sigterm_received() {
            log!("pg_kafka background worker shutting down");

            // Signal the network thread to stop
            let _ = shutdown_tx.send(());

            // Wait for network thread to finish (with timeout)
            let join_start = Instant::now();
            loop {
                if network_handle.is_finished() {
                    log!("pg_kafka network thread shut down cleanly");
                    break;
                }
                if join_start.elapsed() > shutdown_timeout {
                    pgrx::warning!("pg_kafka network thread shutdown timed out");
                    break;
                }
                std::thread::sleep(Duration::from_millis(10));
            }

            break;
        }

        // ┌─────────────────────────────────────────────────────────────┐
        // │ Wait for Next Request (Blocking with Timeout)              │
        // └─────────────────────────────────────────────────────────────┘
        // This is the key performance improvement: we block until work arrives.
        // No CPU usage when idle, instant wake-up when requests come in.
        match request_rx.recv_timeout(RECV_TIMEOUT) {
            Ok(request) => {
                // ┌─────────────────────────────────────────────────────┐
                // │ Process Database Request (SPI in Transaction)      │
                // └─────────────────────────────────────────────────────┘
                let coord = coordinator.clone();
                let broker_h = broker_host.clone();
                let broker_p = port;
                let default_parts = default_partitions;
                let notifier = notify_tx.clone();
                let comp = compression;
                let shadow = shadow_store.clone();

                // Timing instrumentation (enabled via pg_kafka.log_timing GUC)
                // Measures transaction overhead vs handler execution time
                let tx_start = if log_timing {
                    Some(std::time::Instant::now())
                } else {
                    None
                };

                // Use a Cell to capture handler duration from inside the closure
                let handler_duration = std::cell::Cell::new(Duration::ZERO);
                // Wrap in AssertUnwindSafe to satisfy UnwindSafe requirement
                let handler_duration_ref = AssertUnwindSafe(&handler_duration);
                // ShadowStore contains FutureProducer which isn't RefUnwindSafe
                let shadow_ref = AssertUnwindSafe(&shadow);

                BackgroundWorker::transaction(move || {
                    let handler_start = std::time::Instant::now();

                    // Wrap in catch_unwind to prevent handler panics from crashing the worker
                    let result = catch_unwind(AssertUnwindSafe(|| {
                        process_request(
                            request,
                            &coord,
                            &broker_h,
                            broker_p,
                            default_parts,
                            &notifier,
                            comp,
                            *shadow_ref,
                        );
                    }));

                    // Capture handler duration before transaction commit
                    handler_duration_ref.set(handler_start.elapsed());

                    if let Err(panic_info) = result {
                        let panic_msg = if let Some(s) = panic_info.downcast_ref::<&str>() {
                            s.to_string()
                        } else if let Some(s) = panic_info.downcast_ref::<String>() {
                            s.clone()
                        } else {
                            "Unknown panic".to_string()
                        };
                        pg_warning!("Handler panic caught (worker survived): {}", panic_msg);
                    }
                });

                // Log timing results (only when log_timing is enabled)
                if let Some(start) = tx_start {
                    let total_duration = start.elapsed();
                    let handler_dur = handler_duration.get();
                    let tx_overhead = total_duration.saturating_sub(handler_dur);
                    log!(
                        "TIMING: total={}us handler={}us tx_overhead={}us ({}%)",
                        total_duration.as_micros(),
                        handler_dur.as_micros(),
                        tx_overhead.as_micros(),
                        if total_duration.as_micros() > 0 {
                            (tx_overhead.as_micros() * 100) / total_duration.as_micros()
                        } else {
                            0
                        }
                    );
                }
            }

            Err(RecvTimeoutError::Timeout) => {
                // No requests within timeout - this is normal, continue to check signals
            }

            Err(RecvTimeoutError::Disconnected) => {
                // Channel closed - network thread has exited
                log!("Request channel disconnected, shutting down");
                break;
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
            // Must run within a BackgroundWorker transaction for SPI access
            BackgroundWorker::transaction(|| {
                use crate::kafka::{KafkaStore, PostgresStore};
                let store = PostgresStore::new();
                let default_timeout = Duration::from_secs(60);
                match store.abort_timed_out_transactions(default_timeout) {
                    Ok(aborted) => {
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
            last_txn_timeout_check = Instant::now();
        }

        // ┌─────────────────────────────────────────────────────────────┐
        // │ Shadow Config Reload & Metrics Flush (Periodic, ~30 secs)  │
        // └─────────────────────────────────────────────────────────────┘
        if last_shadow_config_check.elapsed() >= SHADOW_CONFIG_RELOAD_INTERVAL {
            let shadow_reload = AssertUnwindSafe(&shadow_store);
            BackgroundWorker::transaction(move || {
                // Reload topic shadow configurations
                match shadow_reload.load_topic_config_from_db() {
                    Ok(count) => {
                        if count > 0 {
                            pg_log!("Reloaded {} shadow topic configurations", count);
                        }
                    }
                    Err(e) => {
                        pg_warning!("Failed to reload shadow config: {:?}", e);
                    }
                }

                // Flush accumulated metrics to database/logs
                if let Err(e) = shadow_reload.flush_metrics_to_db() {
                    pg_warning!("Failed to flush shadow metrics: {:?}", e);
                }
            });
            last_shadow_config_check = Instant::now();
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
#[allow(clippy::too_many_arguments)]
pub fn process_request(
    request: crate::kafka::KafkaRequest,
    coordinator: &std::sync::Arc<crate::kafka::GroupCoordinator>,
    broker_host: &str,
    broker_port: i32,
    default_partitions: i32,
    notify_tx: &crossbeam_channel::Sender<crate::kafka::InternalNotification>,
    compression: kafka_protocol::records::Compression,
    shadow_store: &std::sync::Arc<ShadowStore<PostgresStore>>,
) {
    use crate::kafka::dispatch::{dispatch_infallible, dispatch_response};
    use crate::kafka::handlers;
    use crate::kafka::messages::KafkaResponse;

    // ShadowStore wraps PostgresStore and handles shadow mode internally.
    // It checks the GUC at runtime to decide whether to forward.
    let store: &dyn KafkaStore = shadow_store.as_ref();

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
            let advertised_host = broker_host.to_string();
            let advertised_port = broker_port;

            dispatch_response(
                "Metadata",
                response_tx,
                || {
                    handlers::handle_metadata(
                        store,
                        requested_topics,
                        advertised_host,
                        advertised_port,
                        default_partitions,
                    )
                },
                |r| KafkaResponse::Metadata {
                    correlation_id,
                    api_version,
                    response: r,
                },
                |_error_code| KafkaResponse::Metadata {
                    correlation_id,
                    api_version,
                    response: crate::kafka::response_builders::build_metadata_error_response(),
                },
            );
        }

        // ===== Produce =====
        crate::kafka::KafkaRequest::Produce {
            correlation_id,
            api_version,
            acks,
            topic_data,
            transactional_id,
            response_tx,
            ..
        } => {
            // Handle acks=0 (fire-and-forget)
            // Per Kafka protocol: respond immediately without waiting for DB commit.
            // Data is still written best-effort; errors are logged but not sent to client.
            if acks == 0 {
                // Send immediate empty success response (true fire-and-forget semantics)
                let _ = response_tx.send(KafkaResponse::Produce {
                    correlation_id,
                    api_version,
                    response: crate::kafka::response_builders::build_produce_response(),
                });

                // Process the produce request best-effort (client already got response)
                // Phase 9 Note: For acks=0 (fire-and-forget), we intentionally skip idempotency validation
                // by passing None for producer_metadata. There's no delivery guarantee anyway, so
                // enforcing sequence validation would be misleading and waste resources.
                // Phase 10 Note: We also skip transactional support for acks=0 since there's no
                // guarantee of delivery - transactions require acks >= 1 for meaningful guarantees.
                match handlers::handle_produce(
                    store,
                    topic_data.clone(),
                    default_partitions,
                    None,
                    None,
                ) {
                    Ok(response) => {
                        // Send notifications for long-polling consumers
                        for topic_response in &response.responses {
                            let topic_name = topic_response.name.0.as_str();
                            if let Ok(Some(topic_id)) = store.get_topic_id(topic_name) {
                                for partition_response in &topic_response.partition_responses {
                                    if partition_response.error_code == ERROR_NONE {
                                        let partition_id = partition_response.index;
                                        if let Ok(hwm) =
                                            store.get_high_watermark(topic_id, partition_id)
                                        {
                                            let notification =
                                                crate::kafka::InternalNotification::NewMessages {
                                                    topic_id,
                                                    partition_id,
                                                    high_watermark: hwm,
                                                };
                                            let _ = notify_tx.send(notification);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        // Log error but don't notify client (per acks=0 contract)
                        if e.is_server_error() {
                            pg_warning!("acks=0 produce error (data may be lost): {}", e);
                        }
                    }
                }
                return;
            }

            // Handle produce and send notifications on success (acks >= 1)
            // Phase 9: Extract producer metadata for idempotent producer validation
            // This enables sequence checking, deduplication, and producer fencing
            let producer_metadata = extract_producer_metadata(&topic_data);

            // Phase 10: Pass transactional_id for transactional producers
            // Phase 11: Shadow forwarding is now handled internally by ShadowStore.insert_records()
            match handlers::handle_produce(
                store,
                topic_data,
                default_partitions,
                producer_metadata.as_ref(),
                transactional_id.as_deref(),
            ) {
                Ok(response) => {
                    // Send notifications for each successfully produced partition
                    // This wakes up any consumers waiting in long poll
                    for topic_response in &response.responses {
                        let topic_name = topic_response.name.0.as_str();
                        if let Ok(Some(topic_id)) = store.get_topic_id(topic_name) {
                            for partition_response in &topic_response.partition_responses {
                                // Only notify if produce succeeded (error_code == 0)
                                if partition_response.error_code == ERROR_NONE {
                                    let partition_id = partition_response.index;
                                    // Get the new high watermark
                                    if let Ok(hwm) =
                                        store.get_high_watermark(topic_id, partition_id)
                                    {
                                        let notification =
                                            crate::kafka::InternalNotification::NewMessages {
                                                topic_id,
                                                partition_id,
                                                high_watermark: hwm,
                                            };
                                        let _ = notify_tx.send(notification);
                                    }
                                }
                            }
                        }
                    }

                    // Send the response
                    let _ = response_tx.send(KafkaResponse::Produce {
                        correlation_id,
                        api_version,
                        response,
                    });
                }
                Err(e) => {
                    let error_code = e.to_kafka_error_code();
                    if e.is_server_error() {
                        pg_warning!("Produce handler error: {}", e);
                    }
                    let _ = response_tx.send(KafkaResponse::Produce {
                        correlation_id,
                        api_version,
                        response: crate::kafka::response_builders::build_produce_error_response(
                            error_code,
                        ),
                    });
                }
            }
        }

        // ===== Fetch =====
        crate::kafka::KafkaRequest::Fetch {
            correlation_id,
            api_version,
            topic_data,
            isolation_level,
            response_tx,
            ..
        } => {
            // Phase 10: Convert i8 isolation_level to IsolationLevel enum
            let isolation = if isolation_level == 1 {
                crate::kafka::storage::IsolationLevel::ReadCommitted
            } else {
                crate::kafka::storage::IsolationLevel::ReadUncommitted
            };

            dispatch_response(
                "Fetch",
                response_tx,
                || handlers::handle_fetch(store, topic_data, compression, isolation),
                |r| KafkaResponse::Fetch {
                    correlation_id,
                    api_version,
                    response: r,
                },
                |error_code| KafkaResponse::Fetch {
                    correlation_id,
                    api_version,
                    response: crate::kafka::response_builders::build_fetch_error_response(
                        error_code,
                    ),
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
            dispatch_response(
                "OffsetCommit",
                response_tx,
                || handlers::handle_offset_commit(store, group_id, topics),
                |r| KafkaResponse::OffsetCommit {
                    correlation_id,
                    api_version,
                    response: r,
                },
                |error_code| KafkaResponse::OffsetCommit {
                    correlation_id,
                    api_version,
                    response: crate::kafka::response_builders::build_offset_commit_error_response(
                        error_code,
                    ),
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
            dispatch_response(
                "OffsetFetch",
                response_tx,
                || handlers::handle_offset_fetch(store, group_id, topics),
                |r| KafkaResponse::OffsetFetch {
                    correlation_id,
                    api_version,
                    response: r,
                },
                |error_code| KafkaResponse::OffsetFetch {
                    correlation_id,
                    api_version,
                    response: crate::kafka::response_builders::build_offset_fetch_error_response(
                        error_code,
                    ),
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
                response_tx,
                || handlers::handle_find_coordinator(host, port, key, key_type),
                |r| KafkaResponse::FindCoordinator {
                    correlation_id,
                    api_version,
                    response: r,
                },
                |error_code| KafkaResponse::FindCoordinator {
                    correlation_id,
                    api_version,
                    response:
                        crate::kafka::response_builders::build_find_coordinator_error_response(
                            error_code,
                        ),
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
                |error_code| KafkaResponse::JoinGroup {
                    correlation_id,
                    api_version,
                    response: crate::kafka::response_builders::build_join_group_error_response(
                        error_code,
                    ),
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
            dispatch_response(
                "SyncGroup",
                response_tx,
                || {
                    handlers::handle_sync_group(
                        &coord,
                        store,
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
                |error_code| KafkaResponse::SyncGroup {
                    correlation_id,
                    api_version,
                    response: crate::kafka::response_builders::build_sync_group_error_response(
                        error_code,
                    ),
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
                response_tx,
                || handlers::handle_heartbeat(&coord, group_id, member_id, generation_id),
                |r| KafkaResponse::Heartbeat {
                    correlation_id,
                    api_version,
                    response: r,
                },
                |error_code| KafkaResponse::Heartbeat {
                    correlation_id,
                    api_version,
                    response: crate::kafka::response_builders::build_heartbeat_error_response(
                        error_code,
                    ),
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
                response_tx,
                || handlers::handle_leave_group(&coord, group_id, member_id),
                |r| KafkaResponse::LeaveGroup {
                    correlation_id,
                    api_version,
                    response: r,
                },
                |error_code| KafkaResponse::LeaveGroup {
                    correlation_id,
                    api_version,
                    response: crate::kafka::response_builders::build_leave_group_error_response(
                        error_code,
                    ),
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
            dispatch_response(
                "ListOffsets",
                response_tx,
                || handlers::handle_list_offsets(store, topics),
                |r| KafkaResponse::ListOffsets {
                    correlation_id,
                    api_version,
                    response: r,
                },
                |error_code| KafkaResponse::ListOffsets {
                    correlation_id,
                    api_version,
                    response: crate::kafka::response_builders::build_list_offsets_error_response(
                        error_code,
                    ),
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
                response_tx,
                || handlers::handle_describe_groups(&coord, groups),
                |r| KafkaResponse::DescribeGroups {
                    correlation_id,
                    api_version,
                    response: r,
                },
                |error_code| KafkaResponse::DescribeGroups {
                    correlation_id,
                    api_version,
                    response: crate::kafka::response_builders::build_describe_groups_error_response(
                        error_code,
                    ),
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
                response_tx,
                || handlers::handle_list_groups(&coord, states_filter),
                |r| KafkaResponse::ListGroups {
                    correlation_id,
                    api_version,
                    response: r,
                },
                |error_code| KafkaResponse::ListGroups {
                    correlation_id,
                    api_version,
                    response: crate::kafka::response_builders::build_list_groups_error_response(
                        error_code,
                    ),
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
            dispatch_response(
                "CreateTopics",
                response_tx,
                || handlers::handle_create_topics(store, topics, validate_only),
                |r| KafkaResponse::CreateTopics {
                    correlation_id,
                    api_version,
                    response: r,
                },
                |error_code| KafkaResponse::CreateTopics {
                    correlation_id,
                    api_version,
                    response: crate::kafka::response_builders::build_create_topics_error_response(
                        error_code,
                    ),
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
            dispatch_response(
                "DeleteTopics",
                response_tx,
                || handlers::handle_delete_topics(store, topic_names),
                |r| KafkaResponse::DeleteTopics {
                    correlation_id,
                    api_version,
                    response: r,
                },
                |error_code| KafkaResponse::DeleteTopics {
                    correlation_id,
                    api_version,
                    response: crate::kafka::response_builders::build_delete_topics_error_response(
                        error_code,
                    ),
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
            dispatch_response(
                "CreatePartitions",
                response_tx,
                || handlers::handle_create_partitions(store, topics, validate_only),
                |r| KafkaResponse::CreatePartitions {
                    correlation_id,
                    api_version,
                    response: r,
                },
                |error_code| KafkaResponse::CreatePartitions {
                    correlation_id,
                    api_version,
                    response:
                        crate::kafka::response_builders::build_create_partitions_error_response(
                            error_code,
                        ),
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
            let coord = coordinator.clone();
            dispatch_response(
                "DeleteGroups",
                response_tx,
                || handlers::handle_delete_groups(store, &coord, groups_names),
                |r| KafkaResponse::DeleteGroups {
                    correlation_id,
                    api_version,
                    response: r,
                },
                |error_code| KafkaResponse::DeleteGroups {
                    correlation_id,
                    api_version,
                    response: crate::kafka::response_builders::build_delete_groups_error_response(
                        error_code,
                    ),
                },
            );
        }

        // ===== InitProducerId (Phase 9, extended in Phase 10) =====
        crate::kafka::KafkaRequest::InitProducerId {
            correlation_id,
            api_version,
            transactional_id,
            transaction_timeout_ms,
            producer_id: existing_producer_id,
            producer_epoch: existing_epoch,
            client_id,
            response_tx,
        } => {
            dispatch_response(
                "InitProducerId",
                response_tx,
                || {
                    handlers::handle_init_producer_id(
                        store,
                        transactional_id,
                        transaction_timeout_ms,
                        existing_producer_id,
                        existing_epoch,
                        client_id.as_deref(),
                    )
                },
                |r| KafkaResponse::InitProducerId {
                    correlation_id,
                    api_version,
                    response: r,
                },
                |error_code| KafkaResponse::InitProducerId {
                    correlation_id,
                    api_version,
                    response:
                        crate::kafka::response_builders::build_init_producer_id_error_response(
                            error_code,
                        ),
                },
            );
        }

        // ===== Phase 10: Transaction APIs =====
        crate::kafka::KafkaRequest::AddPartitionsToTxn {
            correlation_id,
            api_version,
            transactional_id,
            producer_id,
            producer_epoch,
            topics,
            response_tx,
            ..
        } => {
            dispatch_response(
                "AddPartitionsToTxn",
                response_tx,
                || {
                    handlers::handle_add_partitions_to_txn(
                        store,
                        &transactional_id,
                        producer_id,
                        producer_epoch,
                        topics,
                    )
                },
                |r| KafkaResponse::AddPartitionsToTxn {
                    correlation_id,
                    api_version,
                    response: r,
                },
                |error_code| {
                    let mut response = kafka_protocol::messages::add_partitions_to_txn_response::AddPartitionsToTxnResponse::default();
                    response.throttle_time_ms = 0;
                    response.error_code = error_code;
                    KafkaResponse::AddPartitionsToTxn {
                        correlation_id,
                        api_version,
                        response,
                    }
                },
            );
        }

        crate::kafka::KafkaRequest::AddOffsetsToTxn {
            correlation_id,
            api_version,
            transactional_id,
            producer_id,
            producer_epoch,
            group_id,
            response_tx,
            ..
        } => {
            dispatch_response(
                "AddOffsetsToTxn",
                response_tx,
                || {
                    handlers::handle_add_offsets_to_txn(
                        store,
                        &transactional_id,
                        producer_id,
                        producer_epoch,
                        &group_id,
                    )
                },
                |r| KafkaResponse::AddOffsetsToTxn {
                    correlation_id,
                    api_version,
                    response: r,
                },
                |error_code| {
                    let mut response = kafka_protocol::messages::add_offsets_to_txn_response::AddOffsetsToTxnResponse::default();
                    response.throttle_time_ms = 0;
                    response.error_code = error_code;
                    KafkaResponse::AddOffsetsToTxn {
                        correlation_id,
                        api_version,
                        response,
                    }
                },
            );
        }

        crate::kafka::KafkaRequest::EndTxn {
            correlation_id,
            api_version,
            transactional_id,
            producer_id,
            producer_epoch,
            committed,
            response_tx,
            ..
        } => {
            dispatch_response(
                "EndTxn",
                response_tx,
                || {
                    handlers::handle_end_txn(
                        store,
                        &transactional_id,
                        producer_id,
                        producer_epoch,
                        committed,
                    )
                },
                |r| KafkaResponse::EndTxn {
                    correlation_id,
                    api_version,
                    response: r,
                },
                |error_code| {
                    let mut response =
                        kafka_protocol::messages::end_txn_response::EndTxnResponse::default();
                    response.throttle_time_ms = 0;
                    response.error_code = error_code;
                    KafkaResponse::EndTxn {
                        correlation_id,
                        api_version,
                        response,
                    }
                },
            );
        }

        crate::kafka::KafkaRequest::TxnOffsetCommit {
            correlation_id,
            api_version,
            transactional_id,
            group_id,
            producer_id,
            producer_epoch,
            topics,
            response_tx,
            ..
        } => {
            dispatch_response(
                "TxnOffsetCommit",
                response_tx,
                || {
                    handlers::handle_txn_offset_commit(
                        store,
                        &transactional_id,
                        producer_id,
                        producer_epoch,
                        &group_id,
                        topics,
                    )
                },
                |r| KafkaResponse::TxnOffsetCommit {
                    correlation_id,
                    api_version,
                    response: r,
                },
                |_error_code| {
                    // Note: TxnOffsetCommit doesn't have a top-level error_code field,
                    // errors are per-partition. For simplicity, return an empty response.
                    let mut response = kafka_protocol::messages::txn_offset_commit_response::TxnOffsetCommitResponse::default();
                    response.throttle_time_ms = 0;
                    KafkaResponse::TxnOffsetCommit {
                        correlation_id,
                        api_version,
                        response,
                    }
                },
            );
        }
    }
}
