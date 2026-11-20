//! The pg_kafka background worker: the process Postgres starts for the Kafka listener
//! (registered in `_PG_init`, see `shared_preload_libraries`).
//!
//! It runs two threads:
//!
//! 1. **Network thread** (spawned, [`network`]): a tokio runtime that owns every TCP
//!    connection and parses the Kafka protocol. It never calls pgrx (it logs with
//!    `tracing`) and hands requests to the DB thread over bounded crossbeam channels.
//! 2. **DB thread** (the bgworker's main thread, this module): blocks on those
//!    channels, runs requests through SPI in group-commit batches ([`batch`]) and the
//!    periodic maintenance between them ([`maintenance`]), and logs with pgrx.
//!
//! Every database transaction of the DB thread goes through [`txn::worker_transaction`];
//! [`schema`] covers waiting for the extension, and [`lifecycle`] the signals and exits.

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
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::config::{guc_millis, Config};
use crate::kafka::auth::SecuritySettings;
use crate::kafka::constants::parse_compression_type;
use crate::kafka::metrics::MetricsFlusher;
use crate::kafka::shadow::ShadowStore;
use crate::kafka::{BrokerMetadata, GroupCoordinator, PostgresStore, RuntimeContext};
use crate::{pg_log, pg_warning};
use batch::{execute_batch, serve_credential_requests, RequestEnv};
use lifecycle::{
    exit_after_postmaster_death, exit_for_restart, install_sigterm_handler, postmaster_alive,
    shutdown_and_exit, sigterm_received, stop_network_thread_on_exit,
};
use maintenance::{
    check_member_timeouts, check_transaction_timeouts, flush_broker_metrics, load_shadow_config,
    reload_shadow_config, run_partition_maintenance, run_retention_sweep, tick_shadow_outbox,
    Periodic, MEMBER_TIMEOUT_CHECK_INTERVAL, RETENTION_SWEEP_INTERVAL, TXN_TIMEOUT_CHECK_INTERVAL,
};
use network::Network;
use schema::{
    reapply_worker_role, refresh_schema_state, wait_for_schema, wait_for_schema_update,
    wait_for_schema_while_serving, SchemaState,
};
use txn::{apply_worker_lock_timeout, schema_lost};

/// How long the DB thread blocks waiting for a request before it checks signals and
/// periodic work again.
const RECV_TIMEOUT: Duration = Duration::from_millis(100);

/// Main entry point of the pg_kafka background worker (named in `_PG_init`).
///
/// `#[pg_guard]` turns a Postgres ERROR into a clean worker exit (the postmaster then
/// restarts it), and `#[no_mangle]` lets Postgres find the symbol. Signal handlers are
/// attached before any other work.
#[pg_guard]
#[no_mangle]
pub extern "C-unwind" fn pg_kafka_listener_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    // A SIGTERM handler that can interrupt SQL and lock waits.
    install_sigterm_handler();

    // GUCs are available after _PG_init.
    let runtime_context = Arc::new(RuntimeContext::new(Config::load()));
    let config = runtime_context.config();

    // SPI needs the database where CREATE EXTENSION was run.
    BackgroundWorker::connect_worker_to_spi(Some(&config.database), None);
    if !wait_for_schema(&config.database) {
        log!("Shutdown signal received while waiting for schema");
        exit_for_restart();
    }
    // Drop from the bootstrap superuser to pg_kafka.run_as_role.
    crate::kafka::storage::postgres::apply_worker_role();
    // Bounded lock waits for every worker transaction.
    apply_worker_lock_timeout();

    // Whether the schema is current (an older extension version is not served until
    // ALTER EXTENSION pg_kafka UPDATE; the main loop waits for it).
    let mut schema = SchemaState::default();
    refresh_schema_state(&mut schema);
    if schema.is_current() {
        // ADR-001: create the time partitions of kafka.messages before serving (rows
        // that arrive before a leaf exists land in kafka.messages_default, so this is
        // about keeping that leaf empty, not about Produce succeeding).
        run_partition_maintenance();
    }

    log!(
        "pg_kafka background worker started (port: {}, host: {}, database: {})",
        config.port,
        config.host,
        config.database
    );

    let shadow_store = new_shadow_store(&config);

    // Listener security (pg_kafka.security_protocol for the primary listener,
    // pg_kafka.additional_listeners, TLS files, SASL mechanisms) is read once here; a
    // bad setting or unreadable file stops the worker with a clear error.
    let security = resolve_security(&config)
        .unwrap_or_else(|e| pgrx::error!("pg_kafka listener security: {}", e));
    let network = network::start(&security, Arc::clone(&runtime_context), &shadow_store);
    stop_network_thread_on_exit(network.shutdown_tx.clone());

    // The primary listener's advertised address (`pg_kafka.advertised_address`, else
    // derived from its bind address by the same rule as every other listener).
    let (advertised_host, advertised_port) = security.listeners[0].advertised.clone();
    let broker_metadata = BrokerMetadata::new(advertised_host, advertised_port);
    // Consumer group membership, in memory, on this thread only.
    let coordinator = GroupCoordinator::new();
    let env = RequestEnv {
        coordinator: &coordinator,
        broker_metadata: &broker_metadata,
        default_partitions: config.default_partitions,
        notify_tx: &network.notify_tx,
        compression: parse_compression_type(&config.compression_type),
        shadow_store: &shadow_store,
        log_timing: config.log_timing,
    };

    pg_log!("Entering main event loop (two-thread architecture)");
    load_shadow_config(&shadow_store);
    MainLoop {
        config: &config,
        shutdown_timeout: guc_millis(config.shutdown_timeout_ms),
        security: &security,
        runtime_context: &runtime_context,
        network: &network,
        env: &env,
        schema,
        sighup_pending: false,
        config_reload: Periodic::new(config_reload_interval()),
        last_outbox_tick: Instant::now(),
        member_timeouts: Periodic::new(MEMBER_TIMEOUT_CHECK_INTERVAL),
        txn_timeouts: Periodic::new(TXN_TIMEOUT_CHECK_INTERVAL),
        retention_sweep: Periodic::new(RETENTION_SWEEP_INTERVAL),
        metrics_flusher: MetricsFlusher::new(),
    }
    .run()
}

/// The store every request uses. Shadow mode is switched at runtime
/// (`pg_kafka.shadow_mode_enabled`), so it always wraps PostgresStore; its producer is
/// created on first use.
fn new_shadow_store(config: &Config) -> ShadowStore<PostgresStore> {
    let shadow_store = ShadowStore::new(PostgresStore::new());
    shadow_store.set_license_key(&config.shadow_license_key);
    if config.shadow_mode_enabled {
        log!(
            "Shadow mode enabled, configured to forward to: {}",
            config.shadow_bootstrap_servers
        );
    }
    shadow_store
}

/// The listener security settings the GUCs describe for the worker's host and port.
fn resolve_security(config: &Config) -> Result<SecuritySettings, String> {
    SecuritySettings::resolve(
        &crate::kafka::auth::settings::raw_from_gucs(),
        &config.host,
        config.port,
    )
}

/// `pg_kafka.config_reload_interval_ms`, read now.
fn config_reload_interval() -> Duration {
    guc_millis(crate::config::CONFIG_RELOAD_INTERVAL_MS.get())
}

/// The DB thread's event loop and the state it carries between iterations.
struct MainLoop<'a> {
    /// The configuration the worker started with: database, host and port (Postmaster
    /// GUCs) come from here.
    config: &'a Config,
    /// `pg_kafka.shutdown_timeout_ms`: how long a SIGTERM waits for the network thread.
    shutdown_timeout: Duration,
    /// The listener security in effect (read at start; changes need a worker restart).
    security: &'a SecuritySettings,
    runtime_context: &'a RuntimeContext,
    network: &'a Network,
    env: &'a RequestEnv<'a>,
    /// The installed schema as last detected (on every configuration reload, and every
    /// 500 ms while it is older than the library).
    schema: SchemaState,
    /// A SIGHUP arrived: reload the configuration on this iteration.
    sighup_pending: bool,
    config_reload: Periodic,
    /// SH-9: when the shadow outbox last ticked. Its poll interval and the back-off
    /// after failed ticks live in the ShadowStore (`outbox_tick_due`).
    last_outbox_tick: Instant,
    member_timeouts: Periodic,
    txn_timeouts: Periodic,
    retention_sweep: Periodic,
    metrics_flusher: MetricsFlusher<'static>,
}

impl MainLoop<'_> {
    /// Serve until the worker exits (SIGTERM, postmaster death, network thread gone).
    ///
    /// Each iteration: exit checks, a schema wait if the extension was dropped or is
    /// older than the library, the configuration reload (before taking requests, so a
    /// request that arrives right after `pg_reload_conf()` sees the new settings), the
    /// shadow outbox, one liveness batch, then one main-lane batch (or credential
    /// lookups, or the receive timeout), then the periodic maintenance.
    fn run(mut self) -> ! {
        loop {
            self.exit_if_stopping();
            if schema_lost() {
                self.wait_for_schema_again();
                continue;
            }
            if !self.schema.is_current() {
                self.wait_for_update();
                continue;
            }
            if BackgroundWorker::sighup_received() {
                self.sighup_pending = true;
                pg_log!("SIGHUP received, config reload pending");
            }
            self.config_reload.set_interval(config_reload_interval());
            if self.sighup_pending || self.config_reload.is_due() {
                self.reload_config();
                self.config_reload.ran();
            }
            if crate::config::SHADOW_MODE_ENABLED.get()
                && self
                    .env
                    .shadow_store
                    .outbox_tick_due(self.last_outbox_tick.elapsed())
            {
                tick_shadow_outbox(self.env.shadow_store);
                self.last_outbox_tick = Instant::now();
            }
            self.serve_requests();
            self.run_due_maintenance();
        }
    }

    /// Exit on SIGTERM (stopping the network thread first) or when the postmaster died:
    /// an orphaned worker would keep committing Kafka writes and keep a new postmaster
    /// from starting.
    fn exit_if_stopping(&self) {
        if sigterm_received() {
            log!("pg_kafka background worker shutting down");
            shutdown_and_exit(
                &self.network.shutdown_tx,
                &self.network.handle,
                self.shutdown_timeout,
            );
        }
        if !postmaster_alive() {
            exit_after_postmaster_death(&self.network.shutdown_tx, &self.network.handle);
        }
    }

    /// The extension was dropped (DROP EXTENSION): answer requests with a retriable
    /// error until it exists again, then re-initialise as at start. Returns early on
    /// SIGTERM or postmaster death, which the next iteration handles.
    fn wait_for_schema_again(&mut self) {
        if !wait_for_schema_while_serving(&self.config.database, &self.network.inbox) {
            return;
        }
        reapply_worker_role();
        self.schema = SchemaState::default();
        refresh_schema_state(&mut self.schema);
        self.resume_on_current_schema();
    }

    /// The schema is older than the library: answer requests with a retriable error
    /// until ALTER EXTENSION pg_kafka UPDATE, then switch role and resume as at start.
    /// Returns early on SIGTERM, postmaster death or a dropped extension, which the
    /// next iteration handles.
    fn wait_for_update(&mut self) {
        if !wait_for_schema_update(&mut self.schema, &self.network.inbox) || schema_lost() {
            return;
        }
        reapply_worker_role();
        self.resume_on_current_schema();
    }

    /// Bring a schema the worker has just found current up to date before serving it:
    /// partition maintenance, the shadow configuration, and the retention sweep's
    /// schedule. Nothing on an older schema (the next iteration waits for its update).
    fn resume_on_current_schema(&mut self) {
        if !self.schema.is_current() {
            return;
        }
        run_partition_maintenance();
        load_shadow_config(self.env.shadow_store);
        self.retention_sweep.ran();
    }

    /// Reload the configuration: the GUCs (after a SIGHUP, from the configuration
    /// files), the worker's lock timeout, the schema state (an ALTER EXTENSION
    /// UPDATE), the RuntimeContext snapshot, and the shadow configuration. Postmaster
    /// GUCs (port, host) still need a restart.
    fn reload_config(&mut self) {
        if self.sighup_pending {
            pg_log!(
                "Immediate config reload triggered by SIGHUP (interval={}ms)",
                crate::config::CONFIG_RELOAD_INTERVAL_MS.get()
            );
            self.sighup_pending = false;
            // A background worker sees ALTER SYSTEM changes only after it re-reads the
            // configuration files itself.
            // SAFETY: on the main thread, outside any transaction, as Postgres's own
            // SIGHUP handling does.
            unsafe { pg_sys::ProcessConfigFile(pg_sys::GucContext::PGC_SIGHUP) };
            pg_log!("ProcessConfigFile called, GUCs reloaded from postgresql.auto.conf");
        }
        apply_worker_lock_timeout();

        // A schema change (ALTER EXTENSION pg_kafka UPDATE).
        if refresh_schema_state(&mut self.schema) {
            reapply_worker_role();
            if self.schema.is_current() {
                run_partition_maintenance();
            }
        }

        // Listener security settings apply at worker start only.
        if resolve_security(self.config).is_ok_and(|now| now != *self.security) {
            pg_warning!(
                "pg_kafka listener security settings changed; they take effect when \
                 the pg_kafka_listener worker restarts (pg_terminate_backend() on it, \
                 or a server restart)"
            );
        }

        self.runtime_context.reload();
        pg_log!("Reloaded GUC configuration");
        reload_shadow_config(self.env.shadow_store);
    }

    /// Serve one liveness batch, then wait up to [`RECV_TIMEOUT`] for a main-lane request
    /// (served with everything queued behind it) or SASL credential lookups.
    ///
    /// DR-12: the liveness lane is drained before the next main-lane request.
    /// Heartbeats are served from the in-memory coordinator in microseconds, and those
    /// that arrive while a main-lane batch runs are answered between its requests, so
    /// a heartbeat waits behind at most one in-flight request (plus RECV_TIMEOUT when
    /// idle), never behind the main queue. One liveness batch per iteration, not "until
    /// empty", so a flood of heartbeats cannot starve the main lane.
    fn serve_requests(&self) {
        let inbox = &self.network.inbox;
        if let Ok(request) = inbox.liveness.try_recv() {
            execute_batch(request, &inbox.liveness, None, self.env);
        }
        let received = crossbeam_channel::select! {
            recv(inbox.requests) -> r => r.map_err(|_| RecvTimeoutError::Disconnected),
            recv(inbox.credentials) -> c => {
                if let Ok(first) = c {
                    serve_credential_requests(first, &inbox.credentials);
                }
                Err(RecvTimeoutError::Timeout)
            },
            default(RECV_TIMEOUT) => Err(RecvTimeoutError::Timeout),
        };
        match received {
            Ok(request) => {
                execute_batch(request, &inbox.requests, Some(&inbox.liveness), self.env);
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => {
                // The network thread has exited, so nothing serves the Kafka port. Exit
                // for a restart rather than unregistering the worker for good.
                pg_warning!(
                    "Request channel disconnected (network thread exited); restarting worker"
                );
                exit_for_restart();
            }
        }
    }

    /// Run the periodic maintenance tasks that are due, and publish the broker metrics.
    fn run_due_maintenance(&mut self) {
        if self.member_timeouts.is_due() {
            check_member_timeouts(self.env.coordinator);
            self.member_timeouts.ran();
        }
        if self.txn_timeouts.is_due() {
            check_transaction_timeouts(self.env.notify_tx);
            self.txn_timeouts.ran();
        }
        if self.retention_sweep.is_due() {
            run_retention_sweep();
            self.retention_sweep.ran();
        }
        flush_broker_metrics(&mut self.metrics_flusher, &self.network.inbox);
    }
}
