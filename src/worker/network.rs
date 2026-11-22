//! Starting the network thread: the channels between it and the DB thread, the bound
//! listening sockets with their security settings, and the thread with its tokio runtime.

use crossbeam_channel::{bounded, Receiver, Sender};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use super::lifecycle::NetworkFinished;
use crate::kafka::auth::{CredentialRequest, SecuritySettings};
use crate::kafka::listener::{ListenerSecurity, NetworkChannels};
use crate::kafka::shadow::{ForwardAck, ForwardRequest, ShadowStore};
use crate::kafka::{
    InternalNotification, KafkaRequest, PostgresStore, RequestLanes, RuntimeContext,
};
use crate::{pg_log, pg_warning};

/// Capacity of the main request lane (backpressure threshold).
const REQUEST_CHANNEL_CAPACITY: usize = 10_000;

/// DR-12: capacity of the liveness (Heartbeat) lane. Heartbeats are small and
/// per-member periodic; 1,000 pending already implies a severe DB-thread stall.
const LIVENESS_CHANNEL_CAPACITY: usize = 1_000;

/// Capacity of the long-poll notification channel (DB thread to network thread).
/// Dropped notifications are acceptable: waiting fetches fall back to polling.
const NOTIFICATION_CHANNEL_CAPACITY: usize = 10_000;

/// Capacity of the shadow forward and ack channels. On a full forward channel the
/// outbox row stays pending and is retried by the next poll.
const FORWARD_CHANNEL_CAPACITY: usize = 10_000;

/// Capacity of the SASL credential-lookup channel.
const CREDENTIAL_CHANNEL_CAPACITY: usize = 1024;

/// Worker threads of the network thread's tokio runtime.
const NETWORK_IO_THREADS: usize = 2;

/// How long the network thread waits for its runtime's tasks when it shuts down.
const RUNTIME_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);

/// The DB thread's ends of the channels the network thread sends work on.
pub(super) struct Inbox {
    /// Main request lane.
    pub(super) requests: Receiver<KafkaRequest>,
    /// DR-12 liveness lane (Heartbeat), drained before the main lane.
    pub(super) liveness: Receiver<KafkaRequest>,
    /// SASL credential lookups, answered between request batches.
    pub(super) credentials: Receiver<CredentialRequest>,
}

/// The running network thread, as the DB thread sees it.
pub(super) struct Network {
    pub(super) handle: JoinHandle<()>,
    /// Asks the network thread to shut down.
    pub(super) shutdown_tx: std::sync::mpsc::Sender<()>,
    pub(super) inbox: Inbox,
    /// Wake-ups for long-poll fetches.
    pub(super) notify_tx: Sender<InternalNotification>,
}

/// Everything the network thread takes ownership of.
struct NetworkThreadParts {
    listeners: Vec<(std::net::TcpListener, Arc<ListenerSecurity>)>,
    channels: NetworkChannels,
    runtime_context: Arc<RuntimeContext>,
    shutdown_rx: std::sync::mpsc::Receiver<()>,
}

/// Bind every configured listener, wire the channels (the shadow forward channels onto
/// `shadow_store`) and spawn the network thread. A bind failure or a bad security
/// setting is a Postgres ERROR: the worker exits and the postmaster retries.
pub(super) fn start(
    security: &SecuritySettings,
    runtime_context: Arc<RuntimeContext>,
    shadow_store: &ShadowStore<PostgresStore>,
) -> Network {
    let securities = build_listener_securities(security)
        .unwrap_or_else(|e| pgrx::error!("pg_kafka listener security: {}", e));
    let sockets = bind_listeners(security);
    // SEC-7: warn loudly about listeners reachable from other hosts that do not
    // authenticate clients (and SASL/PLAIN in clear text), so an operator does not
    // silently expose unauthenticated read/write access to every topic.
    for w in security.warnings() {
        pg_warning!("{}", w);
    }

    let (request_tx, request_rx) = bounded::<KafkaRequest>(REQUEST_CHANNEL_CAPACITY);
    // DR-12: a dedicated liveness lane for Heartbeat. The DB thread drains it before
    // taking the next main-lane request, so group liveness never waits behind a queue
    // of heavy Produce/Fetch work (DB pressure would otherwise cascade into spurious
    // rebalances). A full lane applies the same backpressure as the main lane (the
    // connection's reader waits in send_with_backpressure); nothing is dropped.
    let (liveness_tx, liveness_rx) = bounded::<KafkaRequest>(LIVENESS_CHANNEL_CAPACITY);
    let (notify_tx, notify_rx) = bounded::<InternalNotification>(NOTIFICATION_CHANNEL_CAPACITY);
    // SH-9: the DB thread sends ForwardRequests for the durable shadow outbox; the
    // network thread forwards them to the external Kafka and returns one ForwardAck per
    // attempt so the DB thread can finalize the kafka.shadow_tracking row.
    let (forward_tx, forward_rx) = bounded::<ForwardRequest>(FORWARD_CHANNEL_CAPACITY);
    let (forward_ack_tx, forward_ack_rx) = bounded::<ForwardAck>(FORWARD_CHANNEL_CAPACITY);
    shadow_store.set_forward_channel(forward_tx);
    shadow_store.set_ack_channel(forward_ack_rx);
    let (credential_tx, credential_rx) = bounded::<CredentialRequest>(CREDENTIAL_CHANNEL_CAPACITY);
    let (shutdown_tx, shutdown_rx) = std::sync::mpsc::channel::<()>();

    let parts = NetworkThreadParts {
        listeners: sockets.into_iter().zip(securities).collect(),
        channels: NetworkChannels {
            request_lanes: RequestLanes {
                main: request_tx,
                liveness: liveness_tx,
            },
            credential_tx,
            notify_rx,
            forward_rx,
            forward_ack_tx,
        },
        runtime_context,
        shutdown_rx,
    };
    // This thread must never call pgrx functions; it logs with `tracing`.
    pg_log!("Spawning network thread with tokio multi-thread runtime");
    let handle = std::thread::Builder::new()
        .name("pg_kafka-net".to_string())
        .spawn(move || run_network_thread(parts))
        .unwrap_or_else(|e| pgrx::error!("Failed to spawn network thread: {}", e));

    Network {
        handle,
        shutdown_tx,
        inbox: Inbox {
            requests: request_rx,
            liveness: liveness_rx,
            credentials: credential_rx,
        },
        notify_tx,
    }
}

/// Build the per-listener security (TLS acceptor, SASL mechanisms, advertised address)
/// for every configured listener, primary first. Loads the TLS files once.
fn build_listener_securities(
    settings: &SecuritySettings,
) -> Result<Vec<Arc<ListenerSecurity>>, String> {
    let tls_config = if settings.needs_tls() {
        Some(crate::kafka::auth::tls::build_server_config(
            &settings.ssl_cert_file,
            &settings.ssl_key_file,
            settings
                .mutual_tls()
                .then_some(settings.ssl_ca_file.as_str()),
        )?)
    } else {
        None
    };
    let mechanisms = Arc::new(settings.sasl_mechanisms.clone());
    Ok(settings
        .listeners
        .iter()
        .enumerate()
        .map(|(i, spec)| {
            Arc::new(ListenerSecurity {
                protocol: spec.protocol,
                bind_addr: spec.bind_addr(),
                tls: spec
                    .protocol
                    .uses_tls()
                    .then(|| tls_config.clone())
                    .flatten()
                    .map(tokio_rustls::TlsAcceptor::from),
                sasl_mechanisms: mechanisms.clone(),
                // The worker's BrokerMetadata already advertises the primary listener.
                advertised: (i > 0).then(|| spec.advertised.clone()),
                limits: settings.limits,
            })
        })
        .collect())
}

/// Bind every configured listener, primary first, in non-blocking mode.
///
/// The sockets are bound here, on the main thread, so a bind failure is fatal and
/// immediate. They are std listeners: tokio resources belong to the runtime that
/// created them, so the network thread converts them inside its own runtime.
///
/// A restarted worker binds again. The previous worker's listening sockets closed with
/// its process, and std's bind sets SO_REUSEADDR on Unix, so lingering TIME_WAIT
/// connections do not block the new bind. If it still fails (the port is taken by
/// something else), the ERROR exits the worker and the postmaster retries after
/// `WORKER_RESTART_INTERVAL`.
fn bind_listeners(settings: &SecuritySettings) -> Vec<std::net::TcpListener> {
    settings
        .listeners
        .iter()
        .enumerate()
        .map(|(i, spec)| {
            let addr = spec.bind_addr();
            let listener = std::net::TcpListener::bind(&addr).unwrap_or_else(|e| {
                if i == 0 {
                    pgrx::error!(
                        "FATAL: Cannot bind to {}: {}. Check if port {} is already in use.",
                        addr,
                        e,
                        spec.port
                    );
                }
                pgrx::error!(
                    "FATAL: Cannot bind pg_kafka listener {}://{}: {}",
                    spec.protocol.name(),
                    addr,
                    e
                )
            });
            if let Err(e) = listener.set_nonblocking(true) {
                pgrx::error!("Failed to set non-blocking mode: {}", e);
            }
            pg_log!("TCP listener bound to {} ({})", addr, spec.protocol.name());
            listener
        })
        .collect()
}

/// The network thread's body: a multi-threaded tokio runtime serving every listener
/// until shut down.
///
/// A panic is contained here and the runtime is shut down with a bound, so no
/// destructor runs while this thread unwinds (a panic in a destructor during
/// unwinding aborts the process).
fn run_network_thread(parts: NetworkThreadParts) {
    // Signals belong to the main worker thread (pgrx's handlers panic, and abort the
    // process, on any other thread).
    crate::kafka::listener::block_signals_on_this_thread();
    let _finished = NetworkFinished;
    // Network-thread logging to stderr: warnings by default, RUST_LOG overrides.
    crate::kafka::listener::init_network_tracing();

    let rt = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(NETWORK_IO_THREADS)
        .thread_name("pg_kafka-io")
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            tracing::error!("Failed to create tokio runtime: {}", e);
            return;
        }
    };

    let served = catch_unwind(AssertUnwindSafe(|| rt.block_on(serve(parts))));
    if served.is_err() {
        tracing::error!("pg_kafka network thread panicked; shutting its runtime down");
    }
    if catch_unwind(AssertUnwindSafe(|| {
        rt.shutdown_timeout(RUNTIME_SHUTDOWN_TIMEOUT);
    }))
    .is_err()
    {
        tracing::error!("pg_kafka network runtime shutdown panicked");
    }
}

/// Register the listeners with this runtime's reactor (`from_std` needs the runtime
/// context, hence inside `block_on`) and run the listener until shutdown.
async fn serve(parts: NetworkThreadParts) {
    let mut tcp_listeners = Vec::with_capacity(parts.listeners.len());
    for (std_listener, security) in parts.listeners {
        match tokio::net::TcpListener::from_std(std_listener) {
            Ok(listener) => tcp_listeners.push((listener, security)),
            Err(e) => {
                tracing::error!("Failed to convert std listener to tokio: {}", e);
                return;
            }
        }
    }
    crate::kafka::listener::run(
        tcp_listeners,
        parts.channels,
        parts.runtime_context,
        parts.shutdown_rx,
    )
    .await;
}
