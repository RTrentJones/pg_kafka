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
        if !BackgroundWorker::wait_latch(Some(SCHEMA_CHECK_INTERVAL)) {
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

/// Extract producer metadata from the first partition with valid metadata (Phase 9)
///
/// Idempotent producers embed producer_id, producer_epoch, and base_sequence in the
/// RecordBatch header. This function scans all topic partitions to find the first
/// batch with valid producer metadata (producer_id >= 0). The result identifies the
/// producer (id/epoch) for the request as a whole; its `base_sequence` belongs to that
/// first batch only, so `handle_produce` checks each partition against the metadata
/// of its own batch.
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
    // heartbeats are tiny and frequent, and try_send drops excess into the
    // client's retry path rather than blocking the network thread.
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

    let std_listener = match std::net::TcpListener::bind(&bind_addr) {
        Ok(listener) => {
            // Set non-blocking mode for tokio compatibility
            if let Err(e) = listener.set_nonblocking(true) {
                pgrx::error!("Failed to set non-blocking mode: {}", e);
            }
            pg_log!("TCP listener bound to {}", bind_addr);
            // SEC-7: pg_kafka performs no Kafka-wire authentication, so warn loudly when the
            // listener is reachable from other hosts — an operator must not silently expose
            // unauthenticated read/write access to every topic.
            if crate::config::bind_is_publicly_exposed(&host) {
                pg_warning!(
                    "pg_kafka is bound to {} (a non-loopback interface) and performs NO \
                     Kafka-wire authentication or authorization — anyone who can reach this port \
                     can read and write any topic. Protect it with a firewall/security group, or \
                     set pg_kafka.host = '127.0.0.1' to allow only local clients.",
                    bind_addr
                );
            }
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
                    request_lanes,
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
            });
        })
        .expect("Failed to spawn network thread");

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

    // Flag to track if SIGHUP was received (triggers immediate config reload)
    let mut sighup_pending = false;

    // Broker request metrics: published to kafka.broker_api_stats / kafka.broker_gauges.
    let mut metrics_flusher = crate::kafka::metrics::MetricsFlusher::new();

    loop {
        // ┌─────────────────────────────────────────────────────────────┐
        // │ Check for Postgres Shutdown Signal                          │
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

            // Status 1, not a plain return: a SIGTERM from pg_terminate_backend()
            // must lead to a restarted worker (see exit_for_restart).
            exit_for_restart();
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

            // 1. Reload GUCs (update RuntimeContext cache)
            runtime_context.reload();
            pg_log!("Reloaded GUC configuration");

            // 2. Reload shadow config and flush metrics
            let shadow_reload = AssertUnwindSafe(&shadow_store);
            BackgroundWorker::transaction(move || {
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
            let shadow_outbox = AssertUnwindSafe(&shadow_store);
            BackgroundWorker::transaction(move || {
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
            last_outbox_poll = Instant::now();
        }

        // ┌─────────────────────────────────────────────────────────────┐
        // │ Wait for Next Request (Blocking with Timeout)              │
        // └─────────────────────────────────────────────────────────────┘
        // This is the key performance improvement: we block until work arrives.
        // No CPU usage when idle, instant wake-up when requests come in.
        // DR-12 (DEEP-REVIEW-2026-07): drain the liveness lane BEFORE taking the
        // next main-lane request. Heartbeats are served from the in-memory
        // coordinator, so each drains in microseconds. A main-lane batch also
        // ends early as soon as a heartbeat is waiting (see execute_batch), so
        // heartbeat queueing delay stays bounded by one in-flight request plus
        // one commit (plus RECV_TIMEOUT when idle), not by the main queue.
        while let Ok(request) = liveness_rx.try_recv() {
            execute_batch(request, &liveness_rx, None, &env);
        }

        match request_rx.recv_timeout(RECV_TIMEOUT) {
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
            // Must run within a BackgroundWorker transaction for SPI access
            BackgroundWorker::transaction(|| {
                // RV-10: guard the timeout sweep with the subtransaction wrapper (see
                // the config-reload block above) so a Postgres ERROR rolls back and is
                // logged rather than aborting the bgworker.
                let result = run_request_in_subtransaction(|| {
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
                if let Err(panic_msg) = result {
                    pg_warning!(
                        "Transaction-timeout maintenance panicked, rolled back (worker survived): {}",
                        panic_msg
                    );
                }
            });
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
            BackgroundWorker::transaction(|| {
                // RV-10: guard with the subtransaction wrapper (see the config-reload
                // block above) so a Postgres ERROR rolls back and is logged rather
                // than aborting the bgworker.
                let result = run_request_in_subtransaction(|| {
                    use crate::kafka::storage::postgres::{PostgresStore, ABORTED_MESSAGE_GRACE};
                    let store = PostgresStore::new();
                    let retention_hours = crate::config::MESSAGE_RETENTION_HOURS.get();
                    match store.run_retention_sweep(retention_hours, ABORTED_MESSAGE_GRACE) {
                        Ok(stats) => {
                            if stats.total() > 0 {
                                pg_log!(
                                    "Retention sweep: {} aborted msg(s), {} expired msg(s), \
                                     {} stale producer(s), {} terminal txn(s), \
                                     {} delivered shadow row(s) reclaimed",
                                    stats.aborted_messages,
                                    stats.expired_messages,
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
            last_retention_sweep = Instant::now();
        }

        // ┌─────────────────────────────────────────────────────────────┐
        // │ Publish Broker Metrics (≤ once per second, only if changed) │
        // └─────────────────────────────────────────────────────────────┘
        // One upsert statement per flush; no SQL at all while nothing changed.
        if let Some(batch) = metrics_flusher.poll(request_rx.len(), liveness_rx.len()) {
            let written = BackgroundWorker::transaction(|| {
                let mut written = Ok(());
                let guard = run_request_in_subtransaction(|| {
                    written = crate::kafka::metrics::write_batch(&batch);
                });
                guard.and(written)
            });
            if let Some(msg) = metrics_flusher.finish(batch, written) {
                pg_warning!("{}", msg);
            }
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
/// It stops at the request cap, at the time budget, or as soon as a Heartbeat is
/// waiting on the liveness lane (DR-12), so liveness traffic never waits behind
/// more than one in-flight request and one commit.
fn batch_has_room(taken: usize, elapsed: Duration, liveness_waiting: bool) -> bool {
    taken < MAX_GROUP_COMMIT_REQUESTS && elapsed < GROUP_COMMIT_TIME_BUDGET && !liveness_waiting
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
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
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
        let (api_key, api_version, correlation_id) = request.wire_ids();
        let expects_response =
            !matches!(request, crate::kafka::KafkaRequest::Produce { acks: 0, .. });
        let (buffer_tx, buffer_rx) = tokio::sync::mpsc::unbounded_channel();
        let client_tx = request.swap_response_tx(buffer_tx);
        BufferedResponse {
            client_tx,
            buffer_rx,
            api_key,
            api_version,
            correlation_id,
            expects_response,
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
    fn fail(mut self) {
        while self.buffer_rx.try_recv().is_ok() {}
        self.metrics.mark_error();
        if !self.expects_response {
            return;
        }
        let error_response = crate::kafka::response_builders::error_response_for(
            self.api_key,
            self.api_version,
            self.correlation_id,
            ERROR_UNKNOWN_SERVER_ERROR,
        )
        .unwrap_or(crate::kafka::KafkaResponse::Error {
            correlation_id: self.correlation_id,
            error_code: ERROR_UNKNOWN_SERVER_ERROR,
            error_message: Some("transaction commit failed".to_string()),
        });
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
        process_request(
            request,
            env.coordinator,
            env.broker_metadata,
            env.default_partitions,
            env.notify_tx,
            env.compression,
            env.shadow_store,
        )
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
fn after_batch_commit_failure(pending: Vec<BufferedResponse>) {
    for response in pending {
        response.fail();
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
    let mut has_produce = false;
    // Time spent inside handlers (timing instrumentation, pg_kafka.log_timing).
    let mut handler_time = Duration::ZERO;

    // The closure is not UnwindSafe (it borrows the ShadowStore, whose
    // FutureProducer isn't); any unwind out of it is either re-raised below or
    // already rolled back by the subtransaction guard.
    let txn_result = catch_unwind(AssertUnwindSafe(|| {
        BackgroundWorker::transaction(AssertUnwindSafe(|| {
            let mut next = Some(first);
            while let Some(mut request) = next.take() {
                has_produce |= matches!(request, crate::kafka::KafkaRequest::Produce { .. });
                pending.push(BufferedResponse::intercept(&mut request));

                let handler_start = Instant::now();
                let _ = run_one_request(request, env);
                handler_time += handler_start.elapsed();

                let liveness_waiting = yield_to.is_some_and(|rx| !rx.is_empty());
                if batch_has_room(pending.len(), batch_start.elapsed(), liveness_waiting) {
                    next = source.try_recv().ok();
                }
            }
            apply_test_pre_commit_delay(has_produce);
        }))
    }));

    let batch_size = pending.len();
    match txn_result {
        Ok(()) => {
            // Sync-mode shadow forwards go out only now that the local writes are
            // committed; responses are released after the (bounded) external wait.
            env.shadow_store.forward_sync_after_commit();
            after_batch_commit(pending)
        }
        Err(panic_payload) => {
            // The transaction itself failed (commit-time panic). Answer every
            // request, then re-raise: the worker exits and the postmaster
            // restarts it (WORKER_RESTART_INTERVAL).
            after_batch_commit_failure(pending);
            std::panic::resume_unwind(panic_payload);
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

// Helper functions removed - now using storage trait and handlers

/// Process a Kafka request and send the response
///
/// This function has been refactored to use:
/// - Repository Pattern: Storage via PostgresStore (implements KafkaStore trait)
/// - Dispatch Pattern: Common error handling via dispatch helpers
/// - Transaction boundaries remain explicit here (in worker.rs)
///
/// Send a best-effort produce notification to the network thread without blocking the single DB
/// thread (SEC-4). The notify channel is bounded; a *blocking* send from here would stall all
/// request processing if the channel filled. `try_send` drops the notification instead — fetch
/// handlers fall back to polling, so a dropped wake-up only adds latency, never blocks.
fn notify_best_effort(
    tx: &crossbeam_channel::Sender<crate::kafka::InternalNotification>,
    notification: crate::kafka::InternalNotification,
) {
    let _ = tx.try_send(notification);
}

/// Wake any long-poll consumers for the partitions a produce just appended to, one best-effort
/// notification per successfully-written partition. Shared by the `acks=0` and `acks>=1` produce
/// paths, which previously carried identical copies of this loop. A missing topic id, a failed
/// high-watermark read, or a full notify channel simply skips that wake-up (consumers fall back to
/// the poll interval — see [`notify_best_effort`]).
fn emit_produce_notifications(
    response: &kafka_protocol::messages::produce_response::ProduceResponse,
    store: &dyn KafkaStore,
    notify_tx: &crossbeam_channel::Sender<crate::kafka::InternalNotification>,
) {
    for topic_response in &response.responses {
        let topic_name = topic_response.name.0.as_str();
        if let Ok(Some(topic_id)) = store.get_topic_id(topic_name) {
            for partition_response in &topic_response.partition_responses {
                if partition_response.error_code == ERROR_NONE {
                    let partition_id = partition_response.index;
                    if let Ok(hwm) = store.get_high_watermark(topic_id, partition_id) {
                        notify_best_effort(
                            notify_tx,
                            crate::kafka::InternalNotification::NewMessages {
                                topic_name: topic_name.to_string(),
                                topic_id,
                                partition_id,
                                high_watermark: hwm,
                            },
                        );
                    }
                }
            }
        }
    }
}

/// Wake `read_committed` long-poll consumers after a successful EndTxn (commit or abort), which
/// can advance the last stable offset of every partition the transaction wrote. Best-effort like
/// [`emit_produce_notifications`]: a dropped wake-up only delays delivery to the fallback poll.
fn emit_txn_end_notification(
    notify_tx: &crossbeam_channel::Sender<crate::kafka::InternalNotification>,
) {
    notify_best_effort(
        notify_tx,
        crate::kafka::InternalNotification::TransactionEnded,
    );
}

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
    use crate::kafka::dispatch::{dispatch_infallible, dispatch_produce, dispatch_response};
    use crate::kafka::handlers;
    use crate::kafka::messages::KafkaResponse;

    // ShadowStore wraps PostgresStore and handles shadow mode internally.
    // It checks the GUC at runtime to decide whether to forward.
    let store: &dyn KafkaStore = shadow_store.as_ref();

    // DR-24 (DEEP-REVIEW-2026-07): one HandlerContext for the whole dispatch —
    // previously every match arm rebuilt an identical context (21 copies). The
    // two produce arms still build their own via with_notifier (they carry the
    // long-poll wake-up channel).
    let ctx = crate::kafka::HandlerContext::new(
        store,
        coordinator,
        broker_metadata,
        default_partitions,
        compression,
    );

    match request {
        // ===== ApiVersions (infallible - no storage) =====
        crate::kafka::KafkaRequest::ApiVersions {
            correlation_id,
            api_version,
            response_tx,
            ..
        } => dispatch_infallible(
            "ApiVersions",
            response_tx,
            handlers::handle_api_versions,
            |r| KafkaResponse::ApiVersions {
                correlation_id,
                api_version,
                response: r,
            },
        ),

        // ===== Metadata =====
        crate::kafka::KafkaRequest::Metadata {
            correlation_id,
            api_version,
            topics: requested_topics,
            allow_auto_topic_creation,
            response_tx,
            ..
        } => dispatch_response(
            "Metadata",
            response_tx,
            || handlers::handle_metadata(&ctx, requested_topics, allow_auto_topic_creation),
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
        ),

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
            // Per Kafka protocol: the broker sends NO response frame at all for
            // acks=0. Sending one desyncs real clients (the Java client enforces
            // strict per-connection request/response pairing and kills the
            // connection on an unexpected response). Data is written best-effort;
            // errors are logged but never sent to the client.
            if acks == 0 {
                // Process the produce request best-effort (no response either way)
                // Phase 9 Note: For acks=0 (fire-and-forget), we intentionally skip idempotency validation
                // by passing None for producer_metadata. There's no delivery guarantee anyway, so
                // enforcing sequence validation would be misleading and waste resources.
                // Phase 10 Note: We also skip transactional support for acks=0 since there's no
                // guarantee of delivery - transactions require acks >= 1 for meaningful guarantees.
                let ctx = crate::kafka::HandlerContext::with_notifier(
                    store,
                    coordinator,
                    broker_metadata,
                    default_partitions,
                    compression,
                    notify_tx,
                );
                return match handlers::handle_produce(&ctx, topic_data, None, None) {
                    Ok(response) => {
                        // Wake long-poll consumers for the partitions just written.
                        emit_produce_notifications(&response, store, notify_tx);
                        RequestOutcome::Commit
                    }
                    Err(e) => {
                        // Log error but don't notify client (per acks=0 contract).
                        // Roll back like the acks>=1 path does: a request-level
                        // failure must not leave a partial write behind.
                        if e.is_server_error() {
                            pg_warning!("acks=0 produce error (data may be lost): {}", e);
                        }
                        RequestOutcome::Rollback
                    }
                };
            }

            // Handle produce (acks >= 1).
            // Phase 9: producer metadata enables sequence checking / dedup / fencing.
            // Phase 10: transactional_id for transactional producers.
            // Phase 11: shadow forwarding is internal to ShadowStore.insert_records().
            let producer_metadata = extract_producer_metadata(&topic_data);
            let ctx = crate::kafka::HandlerContext::with_notifier(
                store,
                coordinator,
                broker_metadata,
                default_partitions,
                compression,
                notify_tx,
            );

            // RA-3: route through dispatch_produce so a handler PANIC (a Postgres
            // ERROR surfaces through pgrx as a Rust panic) still replies
            // UNKNOWN_SERVER_ERROR and rolls back, instead of leaving the client to
            // hang. The previous hand-rolled match (kept for the acks=0 no-response
            // special case above) skipped that panic safety.
            dispatch_produce(
                response_tx,
                correlation_id,
                api_version,
                move || {
                    handlers::handle_produce(
                        &ctx,
                        topic_data,
                        producer_metadata.as_ref(),
                        transactional_id.as_deref(),
                    )
                },
                // Wake long-poll consumers for the partitions just written.
                |response| emit_produce_notifications(response, store, notify_tx),
            )
        }

        // ===== Fetch =====
        crate::kafka::KafkaRequest::Fetch {
            correlation_id,
            api_version,
            max_bytes,
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
                || handlers::handle_fetch_with_max_bytes(&ctx, topic_data, isolation, max_bytes),
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
            )
        }

        // ===== OffsetCommit =====
        crate::kafka::KafkaRequest::OffsetCommit {
            correlation_id,
            api_version,
            group_id,
            generation_id,
            member_id,
            topics,
            response_tx,
            ..
        } => dispatch_response(
            "OffsetCommit",
            response_tx,
            || handlers::handle_offset_commit(&ctx, group_id, generation_id, member_id, topics),
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
        ),

        // ===== OffsetFetch =====
        crate::kafka::KafkaRequest::OffsetFetch {
            correlation_id,
            api_version,
            group_id,
            topics,
            response_tx,
            ..
        } => dispatch_response(
            "OffsetFetch",
            response_tx,
            || handlers::handle_offset_fetch(&ctx, group_id, topics),
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
        ),

        // ===== FindCoordinator =====
        crate::kafka::KafkaRequest::FindCoordinator {
            correlation_id,
            api_version,
            key,
            key_type,
            response_tx,
            ..
        } => dispatch_response(
            "FindCoordinator",
            response_tx,
            || handlers::handle_find_coordinator(&ctx, key, key_type),
            |r| KafkaResponse::FindCoordinator {
                correlation_id,
                api_version,
                response: r,
            },
            |error_code| KafkaResponse::FindCoordinator {
                correlation_id,
                api_version,
                response: crate::kafka::response_builders::build_find_coordinator_error_response(
                    error_code,
                ),
            },
        ),

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
            group_instance_id,
            protocols,
            response_tx,
        } => {
            // The coordinator may hold this response until the group's join phase
            // completes (a later JoinGroup, LeaveGroup or the timeout sweep answers it),
            // so it answers on the client's own sender: swap_response_tx leaves that in
            // place for JoinGroup, which does no SPI and needs no commit barrier.
            let reply = crate::kafka::handlers::JoinGroupReply::new(
                correlation_id,
                api_version,
                response_tx,
            );
            handlers::handle_join_group(
                &ctx,
                handlers::JoinGroupRequestData {
                    group_id,
                    member_id,
                    client_id: client_id.unwrap_or_else(|| "unknown".to_string()),
                    group_instance_id,
                    session_timeout_ms,
                    rebalance_timeout_ms,
                    protocol_type,
                    protocols,
                },
                move |response| reply.send(response),
            );
            // JoinGroup writes nothing to the database, so there is nothing to roll back.
            RequestOutcome::Commit
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
            // A follower's SyncGroup may be parked until the leader's arrives, so, as
            // for JoinGroup, it answers on the client's own sender (swap_response_tx
            // leaves it in place for SyncGroup, which only reads and writes nothing).
            let reply = crate::kafka::handlers::SyncGroupReply::new(
                correlation_id,
                api_version,
                response_tx,
            );
            handlers::handle_sync_group(
                &ctx,
                group_id,
                member_id,
                generation_id,
                assignments,
                move |response| reply.send(response),
            );
            RequestOutcome::Commit
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
        } => dispatch_response(
            "Heartbeat",
            response_tx,
            || handlers::handle_heartbeat(&ctx, group_id, member_id, generation_id),
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
        ),

        // ===== LeaveGroup =====
        crate::kafka::KafkaRequest::LeaveGroup {
            correlation_id,
            api_version,
            group_id,
            member_id,
            members,
            response_tx,
            ..
        } => dispatch_response(
            "LeaveGroup",
            response_tx,
            || handlers::handle_leave_group(&ctx, group_id, member_id, members),
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
        ),

        // ===== ListOffsets =====
        crate::kafka::KafkaRequest::ListOffsets {
            correlation_id,
            api_version,
            topics,
            response_tx,
            ..
        } => dispatch_response(
            "ListOffsets",
            response_tx,
            || handlers::handle_list_offsets(&ctx, topics),
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
        ),

        // ===== DescribeGroups =====
        crate::kafka::KafkaRequest::DescribeGroups {
            correlation_id,
            api_version,
            groups,
            response_tx,
            ..
        } => dispatch_response(
            "DescribeGroups",
            response_tx,
            || handlers::handle_describe_groups(&ctx, groups),
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
        ),

        // ===== ListGroups =====
        crate::kafka::KafkaRequest::ListGroups {
            correlation_id,
            api_version,
            states_filter,
            response_tx,
            ..
        } => dispatch_response(
            "ListGroups",
            response_tx,
            || handlers::handle_list_groups(&ctx, states_filter),
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
        ),

        // ===== CreateTopics (Phase 6) =====
        crate::kafka::KafkaRequest::CreateTopics {
            correlation_id,
            api_version,
            topics,
            validate_only,
            response_tx,
            ..
        } => dispatch_response(
            "CreateTopics",
            response_tx,
            || handlers::handle_create_topics(&ctx, topics, validate_only),
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
        ),

        // ===== DeleteTopics (Phase 6) =====
        crate::kafka::KafkaRequest::DeleteTopics {
            correlation_id,
            api_version,
            topic_names,
            response_tx,
            ..
        } => dispatch_response(
            "DeleteTopics",
            response_tx,
            || handlers::handle_delete_topics(&ctx, topic_names),
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
        ),

        // ===== CreatePartitions (Phase 6) =====
        crate::kafka::KafkaRequest::CreatePartitions {
            correlation_id,
            api_version,
            topics,
            validate_only,
            response_tx,
            ..
        } => dispatch_response(
            "CreatePartitions",
            response_tx,
            || handlers::handle_create_partitions(&ctx, topics, validate_only),
            |r| KafkaResponse::CreatePartitions {
                correlation_id,
                api_version,
                response: r,
            },
            |error_code| KafkaResponse::CreatePartitions {
                correlation_id,
                api_version,
                response: crate::kafka::response_builders::build_create_partitions_error_response(
                    error_code,
                ),
            },
        ),

        // ===== DeleteGroups (Phase 6) =====
        crate::kafka::KafkaRequest::DeleteGroups {
            correlation_id,
            api_version,
            groups_names,
            response_tx,
            ..
        } => dispatch_response(
            "DeleteGroups",
            response_tx,
            || handlers::handle_delete_groups(&ctx, groups_names),
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
        ),

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
        } => dispatch_response(
            "InitProducerId",
            response_tx,
            || {
                handlers::handle_init_producer_id(
                    &ctx,
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
                response: crate::kafka::response_builders::build_init_producer_id_error_response(
                    error_code,
                ),
            },
        ),

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
        } => dispatch_response(
            "AddPartitionsToTxn",
            response_tx,
            || {
                handlers::handle_add_partitions_to_txn(
                    &ctx,
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
        ),

        crate::kafka::KafkaRequest::AddOffsetsToTxn {
            correlation_id,
            api_version,
            transactional_id,
            producer_id,
            producer_epoch,
            group_id,
            response_tx,
            ..
        } => dispatch_response(
            "AddOffsetsToTxn",
            response_tx,
            || {
                handlers::handle_add_offsets_to_txn(
                    &ctx,
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
        ),

        crate::kafka::KafkaRequest::EndTxn {
            correlation_id,
            api_version,
            transactional_id,
            producer_id,
            producer_epoch,
            committed,
            response_tx,
            ..
        } => dispatch_response(
            "EndTxn",
            response_tx,
            || {
                handlers::handle_end_txn(
                    &ctx,
                    &transactional_id,
                    producer_id,
                    producer_epoch,
                    committed,
                )
                // Wake read_committed long polls: commit/abort moves the LSO.
                .inspect(|_| emit_txn_end_notification(notify_tx))
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
        ),

        crate::kafka::KafkaRequest::TxnOffsetCommit {
            correlation_id,
            api_version,
            transactional_id,
            group_id,
            producer_id,
            producer_epoch,
            generation_id,
            member_id,
            topics,
            response_tx,
            ..
        } => {
            // Keep the requested topics/partitions so a request-level failure can be
            // echoed per partition (TxnOffsetCommitResponse has no top-level error code).
            let requested_topics = topics.clone();
            dispatch_response(
                "TxnOffsetCommit",
                response_tx,
                || {
                    handlers::handle_txn_offset_commit(
                        &ctx,
                        &transactional_id,
                        producer_id,
                        producer_epoch,
                        &group_id,
                        generation_id,
                        &member_id,
                        topics,
                    )
                },
                |r| KafkaResponse::TxnOffsetCommit {
                    correlation_id,
                    api_version,
                    response: r,
                },
                |error_code| {
                    // TxnOffsetCommit has no top-level error_code field, so report the
                    // error on every requested partition. An empty response would be
                    // read by the client as success.
                    let response =
                        handlers::txn_offset_commit_error_response(&requested_topics, error_code);
                    KafkaResponse::TxnOffsetCommit {
                        correlation_id,
                        api_version,
                        response,
                    }
                },
            )
        }

        // ===== DeleteRecords (API 21) =====
        crate::kafka::KafkaRequest::DeleteRecords {
            correlation_id,
            api_version,
            topics,
            response_tx,
            ..
        } => dispatch_response(
            "DeleteRecords",
            response_tx,
            || handlers::handle_delete_records(&ctx, topics),
            |r| KafkaResponse::DeleteRecords {
                correlation_id,
                api_version,
                response: r,
            },
            |error_code| KafkaResponse::DeleteRecords {
                correlation_id,
                api_version,
                response: crate::kafka::response_builders::build_delete_records_error_response(
                    error_code,
                ),
            },
        ),

        // ===== DescribeConfigs (API 32) =====
        crate::kafka::KafkaRequest::DescribeConfigs {
            correlation_id,
            api_version,
            resources,
            response_tx,
            ..
        } => {
            let global_retention_hours = crate::config::MESSAGE_RETENTION_HOURS.get();
            dispatch_response(
                "DescribeConfigs",
                response_tx,
                || handlers::handle_describe_configs(&ctx, resources, global_retention_hours),
                |r| KafkaResponse::DescribeConfigs {
                    correlation_id,
                    api_version,
                    response: r,
                },
                |error_code| KafkaResponse::DescribeConfigs {
                    correlation_id,
                    api_version,
                    response:
                        crate::kafka::response_builders::build_describe_configs_error_response(
                            error_code,
                        ),
                },
            )
        }

        // ===== IncrementalAlterConfigs (API 44) =====
        crate::kafka::KafkaRequest::IncrementalAlterConfigs {
            correlation_id,
            api_version,
            resources,
            validate_only,
            response_tx,
            ..
        } => dispatch_response(
            "IncrementalAlterConfigs",
            response_tx,
            || handlers::handle_incremental_alter_configs(&ctx, resources, validate_only),
            |r| KafkaResponse::IncrementalAlterConfigs {
                correlation_id,
                api_version,
                response: r,
            },
            |error_code| KafkaResponse::IncrementalAlterConfigs {
                correlation_id,
                api_version,
                response:
                    crate::kafka::response_builders::build_incremental_alter_configs_error_response(
                        error_code,
                    ),
            },
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::InternalNotification;

    #[test]
    fn test_notify_best_effort_does_not_block_when_channel_full() {
        // SEC-4: the single DB thread must never block sending a produce wake-up. A *blocking*
        // send on a full bounded channel would stall all request processing; notify_best_effort
        // uses try_send and drops instead. The channel below has capacity 1 and no receiver
        // draining it, so a blocking send on the second call would hang this test forever.
        let (tx, _rx) = crossbeam_channel::bounded::<InternalNotification>(1);
        let make = |topic_id| InternalNotification::NewMessages {
            topic_name: String::new(),
            topic_id,
            partition_id: 0,
            high_watermark: 0,
        };
        notify_best_effort(&tx, make(1)); // fills the channel
        notify_best_effort(&tx, make(2)); // must return immediately (dropped), not block
    }

    #[test]
    fn test_batch_has_room_stops_at_cap_budget_and_liveness() {
        assert!(batch_has_room(1, Duration::ZERO, false));
        assert!(batch_has_room(
            MAX_GROUP_COMMIT_REQUESTS - 1,
            Duration::from_millis(1),
            false
        ));
        // Request cap.
        assert!(!batch_has_room(
            MAX_GROUP_COMMIT_REQUESTS,
            Duration::ZERO,
            false
        ));
        // Time budget.
        assert!(!batch_has_room(1, GROUP_COMMIT_TIME_BUDGET, false));
        // A waiting heartbeat ends the batch (DR-12).
        assert!(!batch_has_room(1, Duration::ZERO, true));
        // The cap keeps a batch within the subxid cache.
        const { assert!(MAX_GROUP_COMMIT_REQUESTS <= 64) };
    }

    fn api_versions_request(
        correlation_id: i32,
    ) -> (
        crate::kafka::KafkaRequest,
        tokio::sync::mpsc::UnboundedReceiver<crate::kafka::KafkaResponse>,
    ) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        (
            crate::kafka::KafkaRequest::ApiVersions {
                correlation_id,
                client_id: None,
                api_version: 3,
                response_tx: tx,
            },
            rx,
        )
    }

    fn produce_request(
        correlation_id: i32,
        acks: i16,
    ) -> (
        crate::kafka::KafkaRequest,
        tokio::sync::mpsc::UnboundedReceiver<crate::kafka::KafkaResponse>,
    ) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        (
            crate::kafka::KafkaRequest::Produce {
                correlation_id,
                client_id: None,
                api_version: 7,
                acks,
                timeout_ms: 1000,
                topic_data: vec![],
                transactional_id: None,
                response_tx: tx,
            },
            rx,
        )
    }

    /// What a handler does: send on the request's (swapped) response channel.
    fn handler_sends(request: crate::kafka::KafkaRequest, correlation_id: i32) {
        let tx = match request {
            crate::kafka::KafkaRequest::ApiVersions { response_tx, .. }
            | crate::kafka::KafkaRequest::Produce { response_tx, .. } => response_tx,
            _ => unreachable!(),
        };
        tx.send(crate::kafka::KafkaResponse::Error {
            correlation_id,
            error_code: 0,
            error_message: None,
        })
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
            match rx.try_recv().expect("released after commit") {
                crate::kafka::KafkaResponse::Error {
                    correlation_id,
                    error_code,
                    ..
                } => {
                    assert_eq!(correlation_id, expected);
                    assert_eq!(error_code, 0, "the handler's own (success) response");
                }
                _ => panic!("unexpected response"),
            }
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

        after_batch_commit_failure(pending);
        // ApiVersions has no typed error builder, so it gets the generic error
        // frame — never the buffered success.
        match a_rx.try_recv().expect("an error frame") {
            crate::kafka::KafkaResponse::Error {
                correlation_id,
                error_code,
                ..
            } => {
                assert_eq!(correlation_id, 11);
                assert_eq!(error_code, ERROR_UNKNOWN_SERVER_ERROR);
            }
            other => panic!("expected an error frame, got {:?}", other),
        }
        assert!(a_rx.try_recv().is_err(), "the buffered success is dropped");
        // Produce (acks=1): a Produce-typed error.
        match p_rx.try_recv().expect("an error frame") {
            crate::kafka::KafkaResponse::Produce { correlation_id, .. } => {
                assert_eq!(correlation_id, 12)
            }
            other => panic!("expected a Produce error, got {:?}", other),
        }
        assert!(p_rx.try_recv().is_err());
    }

    #[test]
    fn test_commit_failure_sends_nothing_to_acks0_produce() {
        // An acks=0 client expects no response frame at all; an error frame would
        // be read as the answer to its next request.
        let (mut p, mut p_rx) = produce_request(21, 0);
        let pending = vec![BufferedResponse::intercept(&mut p)];
        drop(p); // the acks=0 path sends nothing
        after_batch_commit_failure(pending);
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

    #[test]
    fn test_emit_txn_end_notification_sends_transaction_ended() {
        let (tx, rx) = crossbeam_channel::bounded::<InternalNotification>(1);
        emit_txn_end_notification(&tx);
        assert!(matches!(
            rx.try_recv(),
            Ok(InternalNotification::TransactionEnded)
        ));
        // Full channel: dropped, never blocks the DB thread.
        emit_txn_end_notification(&tx);
        emit_txn_end_notification(&tx);
    }
}
