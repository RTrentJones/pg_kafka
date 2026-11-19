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

mod batch;
mod lifecycle;
mod maintenance;
mod network;
mod schema;
mod txn;

pub(crate) use lifecycle::WORKER_RESTART_INTERVAL;

use crossbeam_channel::RecvTimeoutError;
use pgrx::bgworkers::{BackgroundWorker, SignalWakeFlags};
use pgrx::prelude::*;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use crate::kafka::constants::parse_compression_type;
use crate::kafka::shadow::ShadowStore;
use crate::kafka::PostgresStore;
use crate::{pg_log, pg_warning};
use batch::{execute_batch, notify_transaction_ended, serve_credential_requests, RequestEnv};
use lifecycle::{
    _exit, exit_for_restart, install_sigterm_handler, postmaster_alive, shutdown_and_exit,
    sigterm_received, stop_network_on_exit, NetworkFinished, NETWORK_SHUTDOWN,
};
use maintenance::{
    load_shadow_config, run_partition_maintenance, RETENTION_SWEEP_INTERVAL,
    TIMEOUT_CHECK_INTERVAL, TXN_TIMEOUT_CHECK_INTERVAL,
};
use network::build_listener_securities;
use schema::{
    reapply_worker_role, refresh_schema_features, wait_for_schema, wait_for_schema_while_serving,
    SchemaFeatures,
};
use txn::{
    apply_worker_lock_timeout, log_txn_error, run_request_in_subtransaction, worker_transaction,
    TxnError, SCHEMA_LOST,
};

/// Timeout for blocking recv - allows periodic signal checks
pub(super) const RECV_TIMEOUT: Duration = Duration::from_millis(100);

/// Capacity of the bounded request channel (backpressure threshold)
const REQUEST_CHANNEL_CAPACITY: usize = 10_000;

/// DR-12: capacity of the liveness (Heartbeat) lane. Heartbeats are small and
/// per-member periodic; 1,000 pending already implies severe DB-thread stall.
const LIVENESS_CHANNEL_CAPACITY: usize = 1_000;

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
    // wave3 worker: a SIGTERM handler that can interrupt SQL and lock waits.
    install_sigterm_handler();

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

    // wave3 worker: bounded lock waits for every worker transaction.
    apply_worker_lock_timeout();

    // What this schema has (an older extension version lacks some objects).
    let mut features = SchemaFeatures::default();
    refresh_schema_features(&mut features);

    // ADR-001: create the time partitions of kafka.messages before serving (rows that
    // arrive before a leaf exists land in kafka.messages_default, so this is about
    // keeping that leaf empty, not about Produce succeeding).
    run_partition_maintenance(&features);

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
    // heartbeats are tiny and each member has at most one in flight; a full lane
    // applies the same backpressure as the main lane (the connection's reader
    // waits in send_with_backpressure), nothing is dropped.
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

    // auth stream: listener security (pg_kafka.security_protocol for the primary
    // listener, pg_kafka.additional_listeners, TLS files, SASL mechanisms). Read once
    // here; a bad setting or unreadable file stops the worker with a clear error.
    let security_settings = crate::kafka::auth::SecuritySettings::resolve(
        &crate::kafka::auth::settings::raw_from_gucs(),
        &host,
        port,
    )
    .unwrap_or_else(|e| pgrx::error!("pg_kafka listener security: {}", e));
    let listener_securities = build_listener_securities(&security_settings)
        .unwrap_or_else(|e| pgrx::error!("pg_kafka listener security: {}", e));

    let std_listener = match std::net::TcpListener::bind(&bind_addr) {
        Ok(listener) => {
            // Set non-blocking mode for tokio compatibility
            if let Err(e) = listener.set_nonblocking(true) {
                pgrx::error!("Failed to set non-blocking mode: {}", e);
            }
            pg_log!(
                "TCP listener bound to {} ({})",
                bind_addr,
                security_settings.listeners[0].protocol.name()
            );
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
    // Additional listeners (bind failures are fatal like the primary's).
    let mut std_listeners = vec![std_listener];
    for spec in &security_settings.listeners[1..] {
        let addr = spec.bind_addr();
        match std::net::TcpListener::bind(&addr) {
            Ok(l) => {
                if let Err(e) = l.set_nonblocking(true) {
                    pgrx::error!("Failed to set non-blocking mode: {}", e);
                }
                pg_log!("TCP listener bound to {} ({})", addr, spec.protocol.name());
                std_listeners.push(l);
            }
            Err(e) => pgrx::error!(
                "FATAL: Cannot bind pg_kafka listener {}://{}: {}",
                spec.protocol.name(),
                addr,
                e
            ),
        }
    }
    // SEC-7: warn loudly about listeners reachable from other hosts that do not
    // authenticate clients (and SASL/PLAIN in clear text), so an operator does not
    // silently expose unauthenticated read/write access to every topic.
    for w in security_settings.warnings() {
        pg_warning!("{}", w);
    }

    // auth stream: SASL credential lookups from the network thread (the DB thread
    // answers them between request batches).
    let (credential_tx, credential_rx) =
        crossbeam_channel::bounded::<crate::kafka::auth::CredentialRequest>(1024);

    // Step 8: Spawn the network thread
    // This thread runs a multi-threaded tokio runtime for handling all network I/O.
    // CRITICAL: This thread must NEVER call pgrx functions - use tracing for logging.
    pg_log!("Spawning network thread with tokio multi-thread runtime");
    let network_handle = std::thread::Builder::new()
        .name("pg_kafka-net".to_string())
        .spawn(move || {
            // wave3 net: signals belong to the main worker thread (pgrx's handlers
            // panic, and abort the process, on any other thread).
            crate::kafka::listener::block_signals_on_this_thread();
            let _finished = NetworkFinished;
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
            // A panic is contained here and the runtime is shut down with a bound, so no
            // destructor runs while this thread unwinds (a panic in a destructor during
            // unwinding aborts the process).
            let served = catch_unwind(AssertUnwindSafe(|| {
                rt.block_on(async move {
                    // Convert std listener to tokio listener inside the async context
                    // This ensures the listener is registered with THIS runtime's reactor
                    let mut tcp_listeners = Vec::new();
                    for (std_listener, security) in
                        std_listeners.into_iter().zip(listener_securities)
                    {
                        match tokio::net::TcpListener::from_std(std_listener) {
                            Ok(listener) => tcp_listeners.push((listener, security)),
                            Err(e) => {
                                tracing::error!("Failed to convert std listener to tokio: {}", e);
                                return;
                            }
                        }
                    }

                    if let Err(e) = crate::kafka::listener::run(
                        tcp_listeners,
                        request_lanes,
                        credential_tx,
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
                })
            }));
            if served.is_err() {
                tracing::error!("pg_kafka network thread panicked; shutting its runtime down");
            }
            if catch_unwind(AssertUnwindSafe(|| {
                rt.shutdown_timeout(Duration::from_secs(1))
            }))
            .is_err()
            {
                tracing::error!("pg_kafka network runtime shutdown panicked");
            }
        })
        .expect("Failed to spawn network thread");
    // wave3 worker: whatever ends the process (a FATAL from SIGTERM during database
    // work, an ERROR escaping to the bgworker's top level, proc_exit) first stops the
    // network thread, bounded, instead of exiting under a running tokio runtime.
    *NETWORK_SHUTDOWN.lock().unwrap_or_else(|p| p.into_inner()) = Some(shutdown_tx.clone());
    unsafe { pg_sys::before_shmem_exit(Some(stop_network_on_exit), pg_sys::Datum::from(0)) };

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
    // The interval and the "poll again now" requests live in the ShadowStore
    // (outbox_tick_due), which also backs off after failed ticks.
    let mut last_outbox_poll = Instant::now();

    // Phase 11: Initial shadow config load
    load_shadow_config(&shadow_store);

    // Flag to track if SIGHUP was received (triggers immediate config reload)
    let mut sighup_pending = false;

    // Broker request metrics: published to kafka.broker_api_stats / kafka.broker_gauges.
    let mut metrics_flusher = crate::kafka::metrics::MetricsFlusher::new();

    loop {
        // ┌─────────────────────────────────────────────────────────────┐
        // │ Check for Postgres Shutdown Signal                          │
        // └─────────────────────────────────────────────────────────────┘
        if sigterm_received() {
            log!("pg_kafka background worker shutting down");
            shutdown_and_exit(&shutdown_tx, &network_handle, shutdown_timeout);
        }

        // wave3 worker: never outlive the postmaster. An orphaned worker would keep
        // committing Kafka writes and block a new postmaster from starting.
        if !postmaster_alive() {
            log!("pg_kafka background worker exiting: the postmaster died");
            // Stop the network thread (bounded), then leave at once: the shared memory
            // dies with the cluster anyway, and a new postmaster can start only once
            // every process of the old one is gone (proc_exit's cleanup took >1 s).
            let _ = shutdown_tx.send(());
            let start = Instant::now();
            while !network_handle.is_finished() && start.elapsed() < Duration::from_millis(300) {
                std::thread::sleep(Duration::from_millis(5));
            }
            unsafe { _exit(1) };
        }

        // wave3 worker: the extension was dropped (DROP EXTENSION). Answer requests with
        // a retriable error until it exists again, then re-initialize as at start.
        if SCHEMA_LOST.load(Ordering::SeqCst) {
            if !wait_for_schema_while_serving(
                &config.database,
                &request_rx,
                &liveness_rx,
                &credential_rx,
            ) {
                continue; // SIGTERM or postmaster death: handled at the top
            }
            reapply_worker_role();
            features = SchemaFeatures::default();
            refresh_schema_features(&mut features);
            run_partition_maintenance(&features);
            load_shadow_config(&shadow_store);
            last_retention_sweep = Instant::now();
            continue;
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
            apply_worker_lock_timeout();

            // wave3 worker: pick up a schema change (ALTER EXTENSION pg_kafka UPDATE).
            if refresh_schema_features(&mut features) {
                reapply_worker_role();
                run_partition_maintenance(&features);
            }

            // auth stream: listener security settings apply at worker start only.
            if let Ok(now) = crate::kafka::auth::SecuritySettings::resolve(
                &crate::kafka::auth::settings::raw_from_gucs(),
                &host,
                port,
            ) {
                if now != security_settings {
                    pg_warning!(
                        "pg_kafka listener security settings changed; they take effect when \
                         the pg_kafka_listener worker restarts (pg_terminate_backend() on it, \
                         or a server restart)"
                    );
                }
            }

            // 1. Reload GUCs (update RuntimeContext cache)
            runtime_context.reload();
            pg_log!("Reloaded GUC configuration");

            // 2. Reload shadow config and flush metrics
            let shadow_reload = &shadow_store;
            let outcome = worker_transaction(|| {
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
            if let Err(e) = outcome {
                log_txn_error("Shadow config reload", &e);
            }

            last_config_reload = Instant::now();
        }

        // ┌─────────────────────────────────────────────────────────────┐
        // │ Durable shadow outbox: forward due rows + finalize acks (SH-9)│
        // └─────────────────────────────────────────────────────────────┘
        // Runs on the DB thread (SPI). Gated on shadow mode being enabled and
        // the poll interval, so an idle or non-shadow instance does no work.
        if crate::config::SHADOW_MODE_ENABLED.get()
            && shadow_store.outbox_tick_due(last_outbox_poll.elapsed())
        {
            let shadow_outbox = &shadow_store;
            let outcome = worker_transaction(|| {
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
                // Failures (for example a dropped schema) pause the tick
                // exponentially and are logged sparsely (note_outbox_tick).
                let mut failure: Option<String> = None;
                if let Err(panic_msg) = run_request_in_subtransaction(|| {
                    applied = shadow_outbox.drain_forward_acks();
                }) {
                    failure = Some(format!(
                        "ack application failed, rolled back: {}",
                        panic_msg
                    ));
                }
                let mut poll_error: Option<String> = None;
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
                        Err(e) => poll_error = Some(format!("poll failed: {:?}", e)),
                    }
                });
                if let Err(panic_msg) = result {
                    poll_error = Some(format!("poll failed, rolled back: {}", panic_msg));
                }
                failure = failure.or(poll_error);
                if let Some(msg) = failure {
                    if shadow_outbox.note_outbox_tick(false) {
                        pg_warning!("Shadow outbox {} (worker survived; backing off)", msg);
                    }
                } else {
                    shadow_outbox.note_outbox_tick(true);
                }
            });
            if let Err(e) = outcome {
                log_txn_error("Shadow outbox poll", &e);
            }
            last_outbox_poll = Instant::now();
        }

        // ┌─────────────────────────────────────────────────────────────┐
        // │ Wait for Next Request (Blocking with Timeout)              │
        // └─────────────────────────────────────────────────────────────┘
        // This is the key performance improvement: we block until work arrives.
        // No CPU usage when idle, instant wake-up when requests come in.
        // DR-12 (DEEP-REVIEW-2026-07): drain the liveness lane BEFORE taking the
        // next main-lane request. Heartbeats are served from the in-memory
        // coordinator, so each drains in microseconds. Heartbeats that arrive while
        // a main-lane batch runs are answered between its requests (see
        // execute_batch), so heartbeat queueing delay stays bounded by one
        // in-flight request (plus RECV_TIMEOUT when idle), not by the main queue,
        // and heartbeats no longer cut produce batches short (wave 3).
        // One liveness batch (up to 64) per iteration, not "until empty": a flood of
        // heartbeats must not starve the main lane (the rest are served between the
        // main batch's requests or on the next iteration).
        if let Ok(request) = liveness_rx.try_recv() {
            execute_batch(request, &liveness_rx, None, &env);
        }

        // auth stream: also wake for SASL credential lookups, answered in their own
        // short transaction.
        let received = crossbeam_channel::select! {
            recv(request_rx) -> r => r.map_err(|_| RecvTimeoutError::Disconnected),
            recv(credential_rx) -> c => {
                if let Ok(first) = c {
                    serve_credential_requests(first, &credential_rx);
                }
                Err(RecvTimeoutError::Timeout)
            },
            default(RECV_TIMEOUT) => Err(RecvTimeoutError::Timeout),
        };
        match received {
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
            // Must run within a worker transaction for SPI access
            let mut aborted_any = false;
            let outcome = worker_transaction(|| {
                // RV-10: guard the timeout sweep with the subtransaction wrapper (see
                // the config-reload block above) so a Postgres ERROR rolls back and is
                // logged rather than aborting the bgworker.
                let result = run_request_in_subtransaction(|| {
                    use crate::kafka::storage::MaintenanceStore;
                    use crate::kafka::PostgresStore;
                    let store = PostgresStore::new();
                    let default_timeout = Duration::from_secs(60);
                    match store.abort_timed_out_transactions(default_timeout) {
                        Ok(aborted) => {
                            aborted_any = !aborted.is_empty();
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
                    aborted_any = false;
                }
            });
            match outcome {
                // wave3 worker: a timeout abort ends transactions too; wake parked
                // read_committed fetches now (the LSO moved), not on the 1 s safety net.
                Ok(()) if aborted_any => notify_transaction_ended(&notify_tx),
                Ok(()) => {}
                Err(e) => log_txn_error("Transaction-timeout check", &e),
            }
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
            // ADR-001: keep the next time partitions ready (and drain the DEFAULT
            // leaf) before the sweep decides which leaves to drop.
            run_partition_maintenance(&features);
            let outcome = worker_transaction(|| {
                // RV-10: guard with the subtransaction wrapper (see the config-reload
                // block above) so a Postgres ERROR rolls back and is logged rather
                // than aborting the bgworker.
                let result = run_request_in_subtransaction(|| {
                    use crate::kafka::storage::postgres::{PostgresStore, ABORTED_MESSAGE_GRACE};
                    let store = PostgresStore::new();
                    let retention_hours = crate::config::MESSAGE_RETENTION_HOURS.get();
                    let partition_minutes = crate::config::MESSAGE_PARTITION_MINUTES.get();
                    match store.run_retention_sweep(
                        retention_hours,
                        ABORTED_MESSAGE_GRACE,
                        partition_minutes,
                    ) {
                        Ok(stats) => {
                            if stats.total() > 0 {
                                pg_log!(
                                    "Retention sweep: {} aborted msg(s), {} expired msg(s), \
                                     {} time partition(s) dropped, \
                                     {} stale producer(s), {} terminal txn(s), \
                                     {} delivered shadow row(s) reclaimed",
                                    stats.aborted_messages,
                                    stats.expired_messages,
                                    stats.dropped_partitions,
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
            if let Err(e) = outcome {
                log_txn_error("Retention sweep", &e);
            }
            last_retention_sweep = Instant::now();
        }

        // ┌─────────────────────────────────────────────────────────────┐
        // │ Publish Broker Metrics (≤ once per second, only if changed) │
        // └─────────────────────────────────────────────────────────────┘
        // One upsert statement per flush; no SQL at all while nothing changed.
        // Skipped on a schema without the metrics tables (older extension version).
        let metrics_batch = if features.broker_metrics {
            metrics_flusher.poll(request_rx.len(), liveness_rx.len())
        } else {
            None
        };
        if let Some(batch) = metrics_batch {
            let written = worker_transaction(|| {
                let mut written = Ok(());
                let guard = run_request_in_subtransaction(|| {
                    written = crate::kafka::metrics::write_batch(&batch);
                });
                guard.and(written)
            });
            let written = match written {
                Ok(w) => w,
                // The schema wait reports a dropped extension; nothing to log here.
                Err(TxnError::SchemaGone) => continue,
                Err(e) => Err(format!("{:?}", e)),
            };
            if let Some(msg) = metrics_flusher.finish(batch, written) {
                pg_warning!("{}", msg);
            }
        }
    }
}
