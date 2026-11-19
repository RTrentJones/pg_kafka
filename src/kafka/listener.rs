// TCP listener module for pg_kafka
//
// This module implements the TCP listener that binds to the configured port
// and accepts Kafka client connections. In Step 3, we parse Kafka protocol requests.
//
// ## Thread Safety
//
// This module runs in a SPAWNED THREAD separate from the main Postgres BGWorker.
// It MUST NOT call any pgrx functions (pg_log!, pgrx::error!, SPI, etc.).
// All logging uses the `tracing` crate which is thread-safe.
//
// Architecture: The listener runs a multi-threaded tokio runtime and sends
// parsed Kafka requests to the main thread via a crossbeam bounded channel.
//
// ## Long Polling Support
//
// When enabled, FetchRequest handlers will wait up to max_wait_ms for data if
// min_bytes threshold is not met. This is implemented via:
// - PendingFetchRegistry: tracks waiting fetch requests
// - InternalNotification: signals from DB thread when new data arrives
// - Per-connection long poll handlers that manage the wait/retry logic

use bytes::BytesMut;
use crossbeam_channel::{Receiver, Sender};
use futures::sink::SinkExt;
use futures::stream::StreamExt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::UnboundedSender;
use tokio_util::codec::Framed;
use tracing::{debug, error, info, warn};

use super::api::{FetchRequestData, RequestBody, RequestHeader, ResponseBody};
use super::conn_limits::{IpPermit, IpTracker, LookupAdmission, NetworkLimits};
use super::messages::{KafkaRequest, KafkaResponse, TopicFetchData};
use super::notifications::InternalNotification;
use super::pending_fetches::PendingFetchRegistry;
use super::protocol;
use super::protocol::framing::{FrameClock, FrameCodec, PRE_AUTH_MAX_FRAME};
use super::shadow::{ForwardAck, ForwardRequest, ShadowConfig, ShadowProducer};
use crate::kafka::auth::scram::ScramMechanism;
use crate::kafka::auth::session::{CredentialLookup, LookupResult};
use crate::kafka::auth::session::{SaslOutcome, AUTH_FAILURE_DELAY};
use crate::kafka::auth::{
    ChannelLookup, ConnectionPrincipal, CredentialRequest, SaslMechanism, SaslSession,
    SecurityProtocol,
};

// Max concurrent long-poll fetch tasks per connection (RV-10 follow-up).
// Each long-poll fetch spawns a detached task; without a cap, a client
// pipelining fetches could hold MAX_CONNECTIONS × unbounded tasks. Fetches
// over the cap are served by the immediate (non-waiting) path — a valid,
// possibly-empty response — so the degradation is latency, never an error.
// 64 comfortably covers real consumers (rdkafka holds ~1 in-flight fetch per
// partition batch) while bounding a hostile pipeliner.
//
// Max requests per connection read but not yet answered on the socket: see
// MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION in constants.rs.
use super::constants::{MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, MAX_LONG_POLLS_PER_CONNECTION};

/// Default `tracing` filter for the network thread when `RUST_LOG` is unset.
///
/// The network thread cannot use pgrx logging, so its events go to the
/// postmaster's stderr (the server log). Warnings — rejected frames, exhausted
/// budgets, failed shadow forwards, a worker channel that closed — are
/// operationally relevant and must be visible by default; routine per-request
/// detail stays at debug.
const DEFAULT_NETWORK_LOG_FILTER: &str = "pg_kafka=warn";

/// The `tracing` filter directive string for the network thread: `RUST_LOG` when
/// it is set to something non-empty, otherwise [`DEFAULT_NETWORK_LOG_FILTER`].
///
/// (Previously the code appended a hard `pg_kafka=error` directive on top of
/// `RUST_LOG`, which both hid warnings and overrode `RUST_LOG=pg_kafka=debug`.)
fn network_log_filter(rust_log: Option<&str>) -> String {
    match rust_log.map(str::trim) {
        Some(v) if !v.is_empty() => v.to_string(),
        _ => DEFAULT_NETWORK_LOG_FILTER.to_string(),
    }
}

/// Install the global `tracing` subscriber for the network thread (stderr).
///
/// Called once by the worker before the tokio runtime starts. A second call (e.g.
/// after an in-process restart) is a no-op because `try_init` refuses to replace
/// an installed subscriber.
pub fn init_network_tracing() {
    let directives = network_log_filter(std::env::var("RUST_LOG").ok().as_deref());
    let filter = tracing_subscriber::EnvFilter::try_new(&directives)
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(DEFAULT_NETWORK_LOG_FILTER));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .with_writer(std::io::stderr)
        .try_init();
}

// wave3 net: signal mask of the network thread.
extern "C" {
    fn sigfillset(set: *mut pgrx::pg_sys::__sigset_t) -> std::os::raw::c_int;
    fn pthread_sigmask(
        how: std::os::raw::c_int,
        set: *const pgrx::pg_sys::__sigset_t,
        old: *mut pgrx::pg_sys::__sigset_t,
    ) -> std::os::raw::c_int;
}

/// Block every signal on the calling thread. Called first thing on the network thread,
/// before it creates the tokio runtime (whose threads inherit the mask).
///
/// The worker's signal handlers (pgrx's SIGTERM / SIGHUP handlers) must run on the
/// worker's main thread. The kernel delivers a process-directed signal to any thread
/// that does not block it; when `pg_terminate_backend()`'s SIGTERM landed on the network
/// thread, pgrx's handler panicked ("postgres FFI may not be called from multiple
/// threads"), the panic in the handler aborted the process, and the postmaster put the
/// whole cluster through crash recovery (seen in the E2E suite at listener restarts).
pub fn block_signals_on_this_thread() {
    // SAFETY: plain libc calls on a local, zero-initialised sigset.
    unsafe {
        let mut set = std::mem::MaybeUninit::<pgrx::pg_sys::__sigset_t>::zeroed();
        sigfillset(set.as_mut_ptr());
        let rc = pthread_sigmask(
            pgrx::pg_sys::SIG_BLOCK as std::os::raw::c_int,
            set.as_ptr(),
            std::ptr::null_mut(),
        );
        if rc != 0 {
            warn!(
                "could not block signals on the network thread (error {})",
                rc
            );
        }
    }
}

/// Whether `signum` is blocked on the calling thread (tests).
#[cfg(test)]
fn signal_blocked_on_this_thread(signum: i32) -> bool {
    extern "C" {
        fn sigismember(
            set: *const pgrx::pg_sys::__sigset_t,
            signum: std::os::raw::c_int,
        ) -> std::os::raw::c_int;
    }
    unsafe {
        let mut old = std::mem::MaybeUninit::<pgrx::pg_sys::__sigset_t>::zeroed();
        pthread_sigmask(
            pgrx::pg_sys::SIG_BLOCK as std::os::raw::c_int,
            std::ptr::null(),
            old.as_mut_ptr(),
        );
        sigismember(old.as_ptr(), signum) == 1
    }
}

/// Whether an I/O error is just the peer going away (reset / broken pipe /
/// aborted / EOF mid-frame). Those are routine for a broker and logged at
/// debug; anything else (oversized frame, exhausted budget, ...) is a warning.
fn is_peer_disconnect(e: &std::io::Error) -> bool {
    use std::io::ErrorKind::*;
    matches!(
        e.kind(),
        ConnectionReset | ConnectionAborted | BrokenPipe | UnexpectedEof | NotConnected
    )
}

/// Shadow forwarder task for async forwarding
///
/// Receives ForwardRequest messages from the DB thread via crossbeam channel
/// and forwards them to external Kafka asynchronously (fire-and-forget).
///
/// This task uses RuntimeContext to check configuration dynamically, allowing
/// shadow mode to be enabled/disabled at runtime via ALTER SYSTEM + pg_reload_conf().
/// The producer is lazily created when shadow mode is first enabled and recreated
/// if the bootstrap servers change.
async fn run_shadow_forwarder(
    forward_rx: Receiver<ForwardRequest>,
    ack_tx: Sender<ForwardAck>,
    runtime_context: Arc<crate::kafka::RuntimeContext>,
) {
    info!("Shadow forwarder task started (dynamic config mode)");

    // Track current producer and config for change detection
    let mut producer: Option<Arc<ShadowProducer>> = None;
    let mut current_bootstrap_servers: String = String::new();

    loop {
        // Use spawn_blocking to receive from crossbeam channel in async context
        let rx = forward_rx.clone();
        let result =
            tokio::task::spawn_blocking(move || rx.recv_timeout(Duration::from_millis(100))).await;

        match result {
            Ok(Ok(req)) => {
                // Check current config from RuntimeContext (dynamic reload support)
                let config = runtime_context.config();

                // Skip if shadow mode is not enabled or not configured
                if !config.shadow_mode_enabled || config.shadow_bootstrap_servers.is_empty() {
                    debug!(
                        "Shadow mode disabled or not configured, dropping forward request for {}[{}]",
                        req.topic_name, req.partition_id
                    );
                    continue;
                }

                // Check if config changed - recreate producer if bootstrap servers changed
                if config.shadow_bootstrap_servers != current_bootstrap_servers {
                    info!(
                        "Shadow config changed: bootstrap_servers '{}' -> '{}'",
                        current_bootstrap_servers, config.shadow_bootstrap_servers
                    );

                    let shadow_config = Arc::new(ShadowConfig::from_config(&config));
                    match ShadowProducer::new(shadow_config) {
                        Ok(p) => {
                            current_bootstrap_servers = config.shadow_bootstrap_servers.clone();
                            producer = Some(Arc::new(p));
                            info!(
                                "Shadow producer created/recreated for bootstrap_servers='{}'",
                                current_bootstrap_servers
                            );
                        }
                        Err(e) => {
                            warn!(
                                "Failed to create shadow producer for '{}': {:?}",
                                config.shadow_bootstrap_servers, e
                            );
                            // Clear producer so we retry on next config check
                            producer = None;
                            current_bootstrap_servers.clear();
                            continue;
                        }
                    }
                }

                // Get producer (should exist if we passed the config check)
                let p = match &producer {
                    Some(p) => p.clone(),
                    None => {
                        warn!("No producer available, dropping forward request");
                        continue;
                    }
                };

                // SH-15: forward with partition=None so the external cluster
                // re-partitions by key. The local partition count may differ
                // from the external topic's, so forcing the local partition id
                // would misroute.
                //
                // SH-9: this is the durable-outbox forward. Every attempt sends
                // a ForwardAck back to the DB thread, which finalizes the
                // kafka.shadow_tracking row (success → external_offset set;
                // failure → row stays pending and is retried). The send is no
                // longer fire-and-forget.
                let result = p
                    .send_async_offset(
                        &req.topic_name,
                        None,
                        req.key.as_deref(),
                        req.value.as_deref(),
                    )
                    .await;

                let ack_result = match result {
                    Ok(external_offset) => {
                        debug!(
                            "Forward OK {}[{}] local_offset {} -> external_offset {}",
                            req.topic_name, req.partition_id, req.local_offset, external_offset
                        );
                        Ok(external_offset)
                    }
                    Err(e) => {
                        warn!(
                            "Forward FAILED {}[{}] local_offset {}: {:?} (row stays pending)",
                            req.topic_name, req.partition_id, req.local_offset, e
                        );
                        Err(e.to_string())
                    }
                };

                let ack = ForwardAck {
                    topic_id: req.topic_id,
                    partition_id: req.partition_id,
                    local_offset: req.local_offset,
                    result: ack_result,
                };
                // Issue #93 test hook: hold the ack so it arrives later than
                // the outbox retry lease, letting the E2E suite prove the
                // in-flight gate prevents duplicate re-dispatch. 0 in
                // production (async sleep; only this forwarder task waits).
                let ack_delay_ms = config.test_forward_ack_delay_ms;
                if ack_delay_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(ack_delay_ms as u64)).await;
                }
                // RA-6: non-blocking send. A blocking `send` here could park this
                // tokio worker if the ack channel were full (the SEC-4
                // anti-pattern). On full/closed, drop the ack — the row stays
                // pending in the outbox and the poll re-forwards it, so the
                // external topic is at-least-once. The re-forward is a duplicate;
                // the idempotent producer suppresses it only if it happens within
                // the same producer session (stable PID) — a duplicate that spans
                // a producer restart still lands on the external topic.
                if ack_tx.try_send(ack).is_err() {
                    debug!("Forward ack channel full or closed, dropping ack (row stays pending)");
                }
            }
            Ok(Err(crossbeam_channel::RecvTimeoutError::Timeout)) => {
                // Normal timeout, continue
            }
            Ok(Err(crossbeam_channel::RecvTimeoutError::Disconnected)) => {
                info!("Forward channel disconnected, stopping shadow forwarder");
                break;
            }
            Err(_) => {
                // spawn_blocking panicked
                error!("Shadow forwarder recv task panicked");
                break;
            }
        }
    }

    info!("Shadow forwarder task stopped");
}

/// Try to admit a new connection under a listener's concurrent-connection cap (SEC-3).
///
/// Returns a permit to hold for the connection's lifetime, or `None` if the cap has been
/// reached (the caller must then close the new socket). Dropping the returned permit — which the
/// connection task does on exit — frees the slot for a future connection. `try_acquire` is
/// non-blocking so the accept loop keeps servicing shutdown checks while at capacity.
fn try_admit_connection(
    limiter: &Arc<tokio::sync::Semaphore>,
) -> Option<tokio::sync::OwnedSemaphorePermit> {
    Arc::clone(limiter).try_acquire_owned().ok()
}

/// wave3 net: how long a client has, from the moment its TCP connection is accepted, to
/// finish everything before it is authenticated: the TLS handshake (SSL / SASL_SSL) and,
/// on a SASL listener, SASL authentication, including any wait for a credential lookup
/// or a password check. One deadline from accept, so SASL_SSL gets 10 s in total.
const PRE_AUTH_TIMEOUT: Duration = Duration::from_secs(10);

/// wave3 net: how long a connection that is being closed (bad frame, SASL failure,
/// timeout, acks=0 error) waits for the responses of the requests it read before, so
/// that they are delivered first as Kafka's serial reader guarantees (a committed
/// produce is answered). Peers that went away get only a short grace.
const CLOSE_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

/// Grace for the writer when the peer disconnected (nothing can be delivered).
const PEER_GONE_GRACE: Duration = Duration::from_millis(100);

/// Security of one listener, shared by its connections.
pub struct ListenerSecurity {
    pub protocol: SecurityProtocol,
    /// `host:port` this listener is bound to (for logs).
    pub bind_addr: String,
    /// TLS acceptor for SSL / SASL_SSL listeners.
    pub tls: Option<tokio_rustls::TlsAcceptor>,
    /// Mechanisms offered on a SASL listener.
    pub sasl_mechanisms: Arc<Vec<SaslMechanism>>,
    /// The `(host, port)` advertised in Metadata/FindCoordinator responses served on
    /// this listener; `None` keeps what the worker built (the primary listener).
    pub advertised: Option<(String, i32)>,
    /// wave3 net: connection caps and timeouts (`pg_kafka.max_connections` is per
    /// listener; the per-IP cap is shared by all listeners).
    pub limits: NetworkLimits,
}

impl ListenerSecurity {
    /// A PLAINTEXT listener with no overrides (the default primary listener).
    pub fn plaintext(bind_addr: &str) -> Self {
        ListenerSecurity {
            protocol: SecurityProtocol::Plaintext,
            bind_addr: bind_addr.to_string(),
            tls: None,
            sasl_mechanisms: Arc::new(Vec::new()),
            advertised: None,
            limits: NetworkLimits::default(),
        }
    }
}

/// Per-connection security state: the listener, the principal, and the SASL session.
/// Lives on the network thread for the connection's lifetime.
pub struct ConnectionAuth {
    /// Peer address (for logs).
    pub peer: String,
    pub security: Arc<ListenerSecurity>,
    /// Who the connection is: `User:ANONYMOUS` until SASL succeeds (or the client
    /// certificate subject under mutual TLS). There are no ACLs yet; this is recorded
    /// so authorization can be added later.
    pub principal: ConnectionPrincipal,
    pub sasl: SaslSession,
    pub lookup: ChannelLookup,
    pub log_connections: bool,
    /// wave3 net: the per-IP credential-lookup rate limit (absent in unit tests).
    pub lookup_throttle: Option<(Arc<IpTracker>, std::net::IpAddr)>,
}

impl ConnectionAuth {
    pub fn new(
        peer: String,
        security: Arc<ListenerSecurity>,
        principal: ConnectionPrincipal,
        lookup: ChannelLookup,
        log_connections: bool,
    ) -> Self {
        let sasl = SaslSession::new(
            security.protocol.uses_sasl(),
            security.sasl_mechanisms.clone(),
        );
        ConnectionAuth {
            peer,
            security,
            principal,
            sasl,
            lookup,
            log_connections,
            lookup_throttle: None,
        }
    }
}

/// wave3 net: a credential lookup that first takes a token from the peer IP's lookup
/// bucket (net item 9: each SCRAM client-first / PLAIN token costs the DB thread a
/// SELECT, so an IP may start only so many per second). Over the sustained rate the
/// lookup waits; far over it, it never happens and the pre-auth deadline closes the
/// connection without an answer (a disconnect is retriable; an authentication failure
/// is fatal for the Java client).
struct ThrottledLookup<'a> {
    inner: &'a ChannelLookup,
    throttle: Option<&'a (Arc<IpTracker>, std::net::IpAddr)>,
}

impl CredentialLookup for ThrottledLookup<'_> {
    fn lookup(
        &self,
        username: &str,
        mechanism: Option<ScramMechanism>,
    ) -> impl std::future::Future<Output = LookupResult> + Send {
        let admission = self
            .throttle
            .map(|(tracker, ip)| tracker.admit_lookup(*ip))
            .unwrap_or(LookupAdmission::Now);
        let lookup = self.inner.lookup(username, mechanism);
        async move {
            match admission {
                LookupAdmission::Now => {}
                LookupAdmission::After(delay) => tokio::time::sleep(delay).await,
                LookupAdmission::Refused => std::future::pending::<()>().await,
            }
            lookup.await
        }
    }
}

/// Shared state every accepted connection needs.
#[derive(Clone)]
struct AcceptContext {
    request_lanes: crate::kafka::RequestLanes,
    registry: Arc<PendingFetchRegistry>,
    /// wave3 net: per-IP connection counts and lookup buckets, shared by all listeners.
    ip_tracker: Arc<IpTracker>,
    lookup: ChannelLookup,
    enable_long_polling: bool,
    fetch_poll_interval_ms: i32,
    log_connections: bool,
}

/// Run the TCP listener
///
/// This is the main entry point for the Kafka protocol listener.
/// It accepts the pre-bound TcpListeners (the primary one first) and spawns a new
/// async task for each incoming connection.
///
/// # Arguments
/// * `listeners` - Pre-bound TcpListeners (bound in worker.rs) and their security
/// * `request_lanes` - Channel senders (main + DR-12 liveness lanes) to the DB thread
/// * `credential_tx` - Channel to the DB thread for SASL credential lookups
/// * `notify_rx` - Channel receiver for notifications from DB thread (long polling)
/// * `forward_rx` - Channel receiver for async shadow forwarding requests
/// * `runtime_context` - Runtime context for accessing configuration
/// * `shutdown_rx` - Channel receiver for shutdown signal from main thread
///
/// # Thread Safety
/// This function runs in a spawned thread and MUST NOT call pgrx functions.
#[allow(clippy::too_many_arguments)]
pub async fn run(
    listeners: Vec<(TcpListener, Arc<ListenerSecurity>)>,
    request_lanes: crate::kafka::RequestLanes,
    credential_tx: Sender<CredentialRequest>,
    notify_rx: Receiver<InternalNotification>,
    forward_rx: Receiver<ForwardRequest>,
    forward_ack_tx: Sender<ForwardAck>,
    runtime_context: Arc<crate::kafka::RuntimeContext>,
    shutdown_rx: std::sync::mpsc::Receiver<()>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Extract config values from RuntimeContext
    let config = runtime_context.config();
    let enable_long_polling = config.enable_long_polling;
    let log_connections = config.log_connections;
    let fetch_poll_interval_ms = config.fetch_poll_interval_ms;

    // Note: Shadow config is now checked dynamically in run_shadow_forwarder
    // to support runtime enable/disable via ALTER SYSTEM + pg_reload_conf()

    info!(
        "pg_kafka TCP listener started (long_polling={}, log_connections={})",
        enable_long_polling, log_connections
    );

    // Create the pending fetch registry for long polling
    let registry = Arc::new(PendingFetchRegistry::new());

    // Spawn the notification receiver task
    // This task receives notifications from the DB thread and wakes waiting fetch handlers
    let registry_for_notifier = registry.clone();
    let notify_handle = tokio::spawn(async move {
        loop {
            // Use spawn_blocking to receive from crossbeam channel in async context
            let rx = notify_rx.clone();
            let result =
                tokio::task::spawn_blocking(move || rx.recv_timeout(Duration::from_millis(100)))
                    .await;

            match result {
                Ok(Ok(notification)) => match notification {
                    InternalNotification::TransactionEnded => {
                        debug!("Notification: transaction ended, waking read_committed waiters");
                        registry_for_notifier.notify_transaction_ended().await;
                    }
                    InternalNotification::NewMessages {
                        topic_name,
                        topic_id,
                        partition_id,
                        high_watermark,
                    } => {
                        debug!(
                            "Notification: new messages for topic={} (id={}), partition_id={}, hwm={}",
                            topic_name, topic_id, partition_id, high_watermark
                        );
                        registry_for_notifier
                            .notify_new_data(&topic_name, partition_id, high_watermark)
                            .await;
                    }
                },
                Ok(Err(crossbeam_channel::RecvTimeoutError::Timeout)) => {
                    // Normal timeout, continue
                }
                Ok(Err(crossbeam_channel::RecvTimeoutError::Disconnected)) => {
                    info!("Notification channel disconnected, stopping notifier task");
                    break;
                }
                Err(_) => {
                    // spawn_blocking panicked
                    error!("Notification receiver task panicked");
                    break;
                }
            }
        }
    });

    // Spawn the shadow forwarder task (async forwarding from DB thread)
    // This task receives ForwardRequest messages and sends them to external Kafka.
    // Always spawned - config is checked dynamically to support runtime enable/disable.
    let runtime_context_for_forwarder = runtime_context.clone();
    let _forward_handle = tokio::spawn(async move {
        run_shadow_forwarder(forward_rx, forward_ack_tx, runtime_context_for_forwarder).await;
    });

    // SEC-3 / wave3 net: bound concurrent client connections to prevent fd/memory
    // exhaustion. Each listener has its own cap (pg_kafka.max_connections, see
    // accept_loop) and each IP address one across all listeners
    // (pg_kafka.max_connections_per_ip). Every listener carries the same limits.
    let max_per_ip = listeners
        .first()
        .map(|(_, s)| s.limits.max_connections_per_ip)
        .unwrap_or(usize::MAX);
    let accept_context = AcceptContext {
        request_lanes,
        registry,
        ip_tracker: IpTracker::new(max_per_ip),
        lookup: ChannelLookup(credential_tx),
        enable_long_polling,
        fetch_poll_interval_ms,
        log_connections,
    };

    // One accept loop per listener (the primary listener plus
    // pg_kafka.additional_listeners).
    let accept_tasks: Vec<_> = listeners
        .into_iter()
        .map(|(listener, security)| {
            info!(
                "pg_kafka listener {}://{} accepting connections",
                security.protocol.name(),
                security.bind_addr
            );
            tokio::spawn(accept_loop(listener, security, accept_context.clone()))
        })
        .collect();

    // Wait for the shutdown signal.
    loop {
        match shutdown_rx.try_recv() {
            Ok(()) => {
                info!("TCP listener received shutdown signal");
                break;
            }
            Err(std::sync::mpsc::TryRecvError::Empty) => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                info!("Shutdown channel disconnected, exiting");
                break;
            }
        }
    }

    // Cleanup: stop accepting and abort the notifier task
    for task in &accept_tasks {
        task.abort();
    }
    notify_handle.abort();
    let _ = notify_handle.await;

    Ok(())
}

/// Accept connections on one listener until aborted.
async fn accept_loop(listener: TcpListener, security: Arc<ListenerSecurity>, ctx: AcceptContext) {
    // wave3 net: this listener's own cap, so a flood on another listener cannot lock its
    // clients out.
    let limiter = Arc::new(tokio::sync::Semaphore::new(security.limits.max_connections));
    loop {
        match listener.accept().await {
            Ok((socket, addr)) => {
                let accepted_at = tokio::time::Instant::now();
                // SEC-3: enforce the listener's concurrent-connection cap. try_acquire is
                // non-blocking; at the cap we close the new socket rather than buffer an
                // unbounded number of connections.
                let Some(permit) = try_admit_connection(&limiter) else {
                    if ctx.log_connections {
                        warn!(
                            "Connection limit ({}) of {}://{} reached; rejecting {}",
                            security.limits.max_connections,
                            security.protocol.name(),
                            security.bind_addr,
                            addr
                        );
                    }
                    crate::kafka::metrics::connection_rejected();
                    drop(socket);
                    continue;
                };
                // wave3 net: and the per-IP cap.
                let Some(ip_permit) = ctx.ip_tracker.try_open(addr.ip()) else {
                    if ctx.log_connections {
                        warn!(
                            "Per-IP connection limit ({}) reached for {}; rejecting",
                            security.limits.max_connections_per_ip,
                            addr.ip()
                        );
                    }
                    crate::kafka::metrics::connection_rejected();
                    drop(socket);
                    continue;
                };
                // wave3 net: Kafka clients pipeline small requests; with Nagle on, a
                // response that follows another unacknowledged one waits for the
                // client's delayed ACK (~40 ms). Kafka sets TCP_NODELAY on every socket.
                if let Err(e) = socket.set_nodelay(true) {
                    debug!("Could not set TCP_NODELAY for {}: {}", addr, e);
                }

                // Log connections if enabled
                if ctx.log_connections {
                    info!(
                        "Kafka client connected from {} on {}://{}",
                        addr,
                        security.protocol.name(),
                        security.bind_addr
                    );
                }

                let ctx = ctx.clone();
                let security = security.clone();
                // Spawn a new async task to handle this connection
                // Use tokio::spawn (not spawn_local) since we're in multi-thread runtime
                tokio::spawn(async move {
                    // Hold the connection permits for the task's lifetime; dropping them
                    // on exit frees the slots for new connections (SEC-3).
                    let _permit = permit;
                    let _conn_metrics = crate::kafka::metrics::ConnectionGuard::open();
                    let should_log = ctx.log_connections;
                    let _ip_permit: IpPermit = ip_permit;
                    let result = serve_connection(socket, addr, security, ctx, accepted_at).await;
                    match result {
                        Err(e) if should_log => {
                            warn!("Error handling connection from {}: {}", addr, e)
                        }
                        Ok(()) if should_log => info!("Kafka client disconnected from {}", addr),
                        _ => {}
                    }
                });
            }
            Err(e) => {
                warn!("Error accepting connection: {}", e);
                // Back off briefly (e.g. EMFILE) instead of spinning.
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }
}

/// TLS handshake (SSL / SASL_SSL listeners), then the Kafka protocol loop.
async fn serve_connection(
    socket: TcpStream,
    addr: std::net::SocketAddr,
    security: Arc<ListenerSecurity>,
    ctx: AcceptContext,
    accepted_at: tokio::time::Instant,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let peer = addr.to_string();
    // wave3 net (item 10): one pre-auth deadline from accept covers TLS and SASL.
    let pre_auth_deadline = accepted_at + PRE_AUTH_TIMEOUT;
    let limits = security.limits;
    let throttle = Some((ctx.ip_tracker.clone(), addr.ip()));
    let Some(acceptor) = security.tls.clone() else {
        let mut auth = ConnectionAuth::new(
            peer,
            security,
            ConnectionPrincipal::anonymous(),
            ctx.lookup,
            ctx.log_connections,
        );
        auth.lookup_throttle = throttle;
        return handle_connection(
            socket,
            ctx.request_lanes,
            ctx.registry,
            ctx.enable_long_polling,
            ctx.fetch_poll_interval_ms,
            auth,
            limits,
            pre_auth_deadline,
        )
        .await;
    };
    let tls = match tokio::time::timeout_at(pre_auth_deadline, acceptor.accept(socket)).await {
        Ok(Ok(tls)) => tls,
        Ok(Err(e)) => {
            // A peer that connects and leaves without a ClientHello (port probe, load
            // balancer health check) is routine; a real handshake failure is not.
            if is_peer_disconnect(&e) {
                debug!("TLS peer {} left before the handshake: {}", peer, e);
            } else {
                warn!(
                    "TLS handshake with {} on {}://{} failed: {}",
                    peer,
                    security.protocol.name(),
                    security.bind_addr,
                    e
                );
            }
            return Ok(());
        }
        Err(_) => {
            warn!("TLS handshake with {} timed out", peer);
            return Ok(());
        }
    };
    // wave3 net: Nagle off on the TLS stream's socket too (set at accept, on the TCP
    // socket underneath; rustls adds no buffering of its own between records).
    // Mutual TLS: the verified client certificate's subject is the principal (Kafka's
    // default SSL principal, `User:<RFC 2253 subject>`), until SASL replaces it.
    let principal = tls
        .get_ref()
        .1
        .peer_certificates()
        .and_then(|certs| certs.first())
        .and_then(|cert| crate::kafka::auth::tls::certificate_subject(cert.as_ref()))
        .map(|subject| ConnectionPrincipal {
            principal: format!("User:{}", subject),
            method: "SSL",
        })
        .unwrap_or_else(ConnectionPrincipal::anonymous);
    if ctx.log_connections && principal.method == "SSL" {
        info!(
            "Kafka client {} authenticated by TLS client certificate as {}",
            peer, principal.principal
        );
    }
    let mut auth = ConnectionAuth::new(peer, security, principal, ctx.lookup, ctx.log_connections);
    auth.lookup_throttle = throttle;
    handle_connection(
        tls,
        ctx.request_lanes,
        ctx.registry,
        ctx.enable_long_polling,
        ctx.fetch_poll_interval_ms,
        auth,
        limits,
        pre_auth_deadline,
    )
    .await
}

/// Handle a single client connection with request pipelining support
///
/// This function supports Kafka request pipelining by splitting the socket into
/// separate reader and writer halves that operate concurrently:
/// - Reader task: Continuously reads requests and dispatches them to the worker
/// - Writer task: Continuously waits for responses and sends them back
///
/// For FetchRequest with long polling enabled, a separate task is spawned to
/// handle the wait/retry logic without blocking other requests.
///
/// ## Ordering Guarantee
/// Kafka requires: if Request A comes before Request B, Response A must come before Response B.
/// Clients that pipeline requests (the official Java client enforces strict per-connection
/// FIFO and kills the connection on a correlation-id mismatch) depend on this.
///
/// The reader creates a fresh response channel per request and queues the receiving ends,
/// in request order, to the writer task. The writer drains one request's responses before
/// moving to the next, so responses hit the socket in request order even when a long-poll
/// fetch resolves in a detached task long after later requests completed. A request that
/// produces no response (acks=0 produce) closes its channel without sending and the writer
/// skips it. A long-poll fetch therefore blocks later responses on the same connection
/// until it resolves — the same behavior as a real Kafka broker, which mutes a connection
/// while a request waits in purgatory.
///
/// # Thread Safety
/// This function runs in the network thread and MUST NOT call pgrx functions.
#[allow(clippy::too_many_arguments)]
async fn handle_connection<S>(
    socket: S,
    request_lanes: crate::kafka::RequestLanes,
    registry: Arc<PendingFetchRegistry>,
    enable_long_polling: bool,
    poll_interval_ms: i32,
    mut auth: ConnectionAuth,
    limits: NetworkLimits,
    pre_auth_deadline: tokio::time::Instant,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    debug!("New connection established, starting pipelined handle loop");

    // Frame codec (protocol::framing): charges the global budget only for bytes actually
    // buffered of a large incomplete frame (RV-9, wave3 net item 1) and records when that
    // frame started arriving, for the per-frame read deadline.
    let frame_limit = Arc::new(AtomicUsize::new(
        if auth.sasl.is_authenticated() || !auth.security.protocol.uses_sasl() {
            super::constants::MAX_REQUEST_SIZE as usize
        } else {
            PRE_AUTH_MAX_FRAME
        },
    ));
    let clock = Arc::new(FrameClock::new());
    let framed = Framed::new(socket, FrameCodec::new(frame_limit.clone(), clock.clone()));

    // Split the framed socket into independent reader and writer halves
    // This enables concurrent reading and writing for pipelining support
    let (mut writer, mut reader) = framed.split();

    // Per-request response ordering: each request gets its own response
    // channel, and the receiving ends are queued to the writer in request
    // order. The writer fully drains one request's channel (closed when the
    // request's sender is dropped) before moving to the next, guaranteeing
    // responses are written in request order.
    //
    // Each queued slot carries an in-flight permit that the writer drops only
    // after the slot's response has been written to the socket. The reader
    // acquires a permit before reading the next frame, so at most
    // MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION requests per connection are read but
    // unanswered. A client that pipelines without reading responses fills its
    // TCP receive window, the writer blocks, permits stop coming back, and the
    // reader stops reading: the connection's memory is bounded and the
    // backpressure reaches the client through TCP.
    type ResponseSlot = (
        tokio::sync::mpsc::UnboundedReceiver<KafkaResponse>,
        tokio::sync::OwnedSemaphorePermit,
    );
    let (order_tx, mut order_rx) = tokio::sync::mpsc::unbounded_channel::<ResponseSlot>();
    let in_flight = Arc::new(tokio::sync::Semaphore::new(
        MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
    ));

    // RV-10 follow-up: each long-poll fetch spawns a detached task, previously
    // bounded only by MAX_CONNECTIONS × the client's pipelining rate. Cap the
    // concurrent long-poll tasks per connection; a fetch over the cap degrades
    // gracefully to the immediate (non-waiting) fetch path — a valid Kafka
    // response (possibly empty), never an error or a dropped request.
    let long_poll_permits = Arc::new(tokio::sync::Semaphore::new(MAX_LONG_POLLS_PER_CONNECTION));

    // An additional listener advertises its own host:port in Metadata/FindCoordinator,
    // so a client that bootstrapped on it stays on it (Kafka's per-listener
    // advertised.listeners).
    let advertised = auth.security.advertised.clone();

    // wave3 net: last time a frame was read or a response written (idle timeout).
    let activity = Arc::new(ActivityClock::new());
    let writer_activity = activity.clone();
    // wave3 net (item 2): the writer owns `writer_gone_tx`; however it exits (an encode
    // or write error, a panic, an acks=0 close request, all slots done), the sender is
    // dropped and the reader closes the connection instead of reading requests whose
    // responses can never be written.
    let (writer_gone_tx, mut writer_gone_rx) = tokio::sync::oneshot::channel::<()>();

    // Spawn the Writer Task
    // This task sits and waits for responses from the DB.
    // It runs completely independently of the reader.
    let peer_for_writer = auth.peer.clone();
    let mut writer_handle: tokio::task::JoinHandle<Result<(), String>> = tokio::spawn(async move {
        let _writer_gone = writer_gone_tx;
        while let Some((mut slot, _in_flight_permit)) = order_rx.recv().await {
            // A request yields at most one response; one that yields none
            // (acks=0 produce) closes the channel without sending and is
            // skipped here.
            while let Some(mut response) = slot.recv().await {
                if matches!(response.body, ResponseBody::CloseConnection) {
                    // wave3 net (item 4): an acks=0 produce hit a partition error;
                    // Kafka closes the connection (after the request's writes
                    // committed) so the client refreshes its metadata. Earlier
                    // responses are already on the socket.
                    debug!(
                        "Closing connection from {} after an acks=0 produce error",
                        peer_for_writer
                    );
                    let _ = writer.close().await;
                    return Ok(());
                }
                debug!("Writer received response, encoding...");
                if let Some((host, port)) = &advertised {
                    rewrite_advertised(&mut response, host, *port);
                }
                let response_bytes = match protocol::encode_response(response) {
                    Ok(bytes) => bytes,
                    Err(e) => {
                        error!("Failed to encode response: {}", e);
                        return Err(format!("Encoding error: {}", e));
                    }
                };

                debug!(
                    "Sending {} byte response to client...",
                    response_bytes.len()
                );
                if let Err(e) = writer.send(response_bytes.freeze()).await {
                    if is_peer_disconnect(&e) {
                        debug!("Client went away while sending response: {}", e);
                    } else {
                        warn!("Failed to send response to client: {}", e);
                    }
                    return Err(format!("Write error: {}", e));
                }
                writer_activity.touch();
                debug!("Response sent to client successfully");
            }
            // `_in_flight_permit` drops here: this request's response (if any) is
            // on the socket, so the reader may read one more frame.
        }
        Ok(())
    });

    // Run the Reader Loop (Main Task)
    // This loop ONLY reads and dispatches. It never waits for the DB response.
    let close = 'conn: loop {
        // Backpressure: wait for an in-flight slot before reading the next frame.
        // The semaphore is never closed, so acquire only fails if that changes.
        // An unauthenticated client that pipelines without reading its responses is still
        // bound by the pre-auth deadline here.
        let pre_auth = !auth.sasl.admits(-1);
        let in_flight_permit = tokio::select! {
            biased;
            _ = &mut writer_gone_rx => break 'conn Close::WriterGone,
            permit = in_flight.clone().acquire_owned() => match permit {
                Ok(permit) => permit,
                Err(_) => break 'conn Close::Protocol,
            },
            _ = tokio::time::sleep_until(pre_auth_deadline), if pre_auth => {
                warn!(
                    "Closing connection from {}: SASL authentication not completed within {:?}",
                    auth.peer, PRE_AUTH_TIMEOUT
                );
                break 'conn Close::Timeout;
            }
        };
        // Time spent not reading (backpressure) does not count against a frame the
        // client had started.
        clock.restart_if_pending();

        // wave3 net (items 1, 8): wait for the next frame under three deadlines: the
        // pre-auth deadline (SASL listeners, until authenticated), the per-frame read
        // deadline (from the first byte of a frame) and the idle timeout (nothing read,
        // nothing written, nothing outstanding).
        let next = loop {
            let now = tokio::time::Instant::now();
            let mut wake = now + limits.request_read_timeout;
            if !auth.sasl.admits(-1) {
                if now >= pre_auth_deadline {
                    warn!(
                        "Closing connection from {}: SASL authentication not completed within {:?}",
                        auth.peer, PRE_AUTH_TIMEOUT
                    );
                    break 'conn Close::Timeout;
                }
                wake = wake.min(pre_auth_deadline);
            }
            let pending_frame = clock.pending_since();
            if let Some(started) = pending_frame {
                let deadline =
                    tokio::time::Instant::from_std(started) + limits.request_read_timeout;
                if now >= deadline {
                    warn!(
                        "Closing connection from {}: request not received completely within {:?}",
                        auth.peer, limits.request_read_timeout
                    );
                    break 'conn Close::Timeout;
                }
                wake = wake.min(deadline);
            }
            // We hold one permit ourselves; every other missing one is a request whose
            // response is not written yet.
            let outstanding =
                MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION - 1 - in_flight.available_permits();
            if outstanding == 0 && pending_frame.is_none() {
                let deadline =
                    tokio::time::Instant::from_std(activity.last()) + limits.connections_max_idle;
                if now >= deadline {
                    debug!(
                        "Closing connection from {}: idle for {:?}",
                        auth.peer, limits.connections_max_idle
                    );
                    break 'conn Close::Timeout;
                }
                wake = wake.min(deadline);
            } else {
                // Re-check once the outstanding requests may have been answered.
                wake = wake.min(now + limits.connections_max_idle);
            }
            tokio::select! {
                biased;
                _ = &mut writer_gone_rx => break 'conn Close::WriterGone,
                read = tokio::time::timeout_at(wake, reader.next()) => match read {
                    Ok(next) => break next,
                    Err(_) => continue,
                },
            }
        };
        let frame = match next {
            None => break 'conn Close::PeerGone,
            Some(Ok(f)) => f,
            Some(Err(e)) => {
                if is_peer_disconnect(&e) {
                    debug!("Client disconnected mid-frame: {}", e);
                    break 'conn Close::PeerGone;
                }
                warn!("Error reading frame from {}: {}", auth.peer, e);
                break 'conn Close::Protocol;
            }
        };
        activity.touch();

        debug!("Received frame of {} bytes", frame.len());

        // Create this request's response channel and queue its receiver to
        // the writer BEFORE dispatching, so the writer's output order is
        // fixed by request arrival order.
        let (response_tx, response_rx) = tokio::sync::mpsc::unbounded_channel();
        if order_tx.send((response_rx, in_flight_permit)).is_err() {
            warn!("Writer task gone, closing connection");
            break 'conn Close::WriterGone;
        }

        // Listener security: SaslHandshake / SaslAuthenticate are answered here, on
        // the network thread, from the connection's SASL state. On a SASL listener
        // nothing but ApiVersions and those two is processed before authentication
        // succeeds; anything else closes the connection, as Kafka does.
        let api_key = crate::kafka::auth::session::peek_api_key(&frame);
        if let Some(key) = api_key.filter(|k| crate::kafka::auth::session::is_sasl_api(*k)) {
            debug!("SASL request api_key={} from {}", key, auth.peer);
            // wave3 net (item 8): the pre-auth deadline also covers the wait for the
            // credential lookup, the lookup rate limit and the password-check slots.
            let keep_open = if auth.sasl.is_authenticated() {
                handle_sasl_frame(&mut auth, frame, response_tx).await
            } else {
                match tokio::time::timeout_at(
                    pre_auth_deadline,
                    handle_sasl_frame(&mut auth, frame, response_tx),
                )
                .await
                {
                    Ok(keep_open) => keep_open,
                    Err(_) => {
                        warn!(
                            "Closing connection from {}: SASL authentication not completed within {:?}",
                            auth.peer, PRE_AUTH_TIMEOUT
                        );
                        break 'conn Close::Timeout;
                    }
                }
            };
            if keep_open {
                if auth.sasl.is_authenticated() {
                    frame_limit.store(
                        super::constants::MAX_REQUEST_SIZE as usize,
                        Ordering::Relaxed,
                    );
                }
                continue;
            }
            break 'conn Close::Protocol;
        }
        if !auth.sasl.admits(api_key.unwrap_or(-1)) {
            warn!(
                "Closing connection from {} on {}://{}: request api_key={:?} before SASL authentication",
                auth.peer,
                auth.security.protocol.name(),
                auth.security.bind_addr,
                api_key
            );
            break 'conn Close::Protocol;
        }

        match protocol::parse_request(frame, response_tx) {
            Ok(Some(request)) => {
                if !dispatch_request(
                    request,
                    &request_lanes,
                    &registry,
                    &long_poll_permits,
                    enable_long_polling,
                    poll_interval_ms,
                )
                .await
                {
                    error!("Worker channel closed");
                    break 'conn Close::Protocol;
                }
            }
            Ok(None) => {
                // Parse error - error response already queued via response_tx
                // Continue reading more requests (don't break connection)
                debug!("Parse returned None, continuing...");
            }
            Err(e) => {
                warn!("Framing/Parse error from {}: {}", auth.peer, e);
                break 'conn Close::Protocol;
            }
        }
    };

    // Cleanup: Drop order_tx so the writer exits once it has drained the
    // response slots of requests that were already dispatched
    drop(order_tx);

    // wave3 net (item 6): Kafka reads a connection serially, so every request read
    // before the one that closes the connection is answered first. Give the writer the
    // time to deliver them (bounded; a committed acks=1 produce must be answered),
    // unless the peer is gone, then abort it: an in-flight long poll could otherwise
    // hold the task (and the half-open socket) alive for up to max_wait_ms.
    let grace = match close {
        Close::PeerGone => PEER_GONE_GRACE,
        Close::WriterGone => Duration::ZERO,
        Close::Protocol | Close::Timeout => CLOSE_DRAIN_TIMEOUT,
    };
    if tokio::time::timeout(grace, &mut writer_handle)
        .await
        .is_err()
    {
        if grace >= CLOSE_DRAIN_TIMEOUT {
            warn!(
                "Closing connection from {}: responses still pending after {:?}",
                auth.peer, grace
            );
        }
        writer_handle.abort();
        let _ = writer_handle.await;
    }

    if auth.log_connections {
        info!(
            "Kafka connection from {} ({}) closed ({:?})",
            auth.peer, auth.principal, close
        );
    }
    debug!("Connection handler exiting");
    Ok(())
}

/// Why a connection's read loop ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Close {
    /// EOF or reset: the peer went away.
    PeerGone,
    /// The writer task exited (write/encode error, acks=0 close request).
    WriterGone,
    /// A frame the broker closes the connection for (malformed, refused, SASL failure).
    Protocol,
    /// Pre-auth, per-frame read or idle deadline.
    Timeout,
}

/// wave3 net: when a connection last read a frame or wrote a response.
struct ActivityClock {
    epoch: Instant,
    last_ms: std::sync::atomic::AtomicU64,
}

impl ActivityClock {
    fn new() -> Self {
        ActivityClock {
            epoch: Instant::now(),
            last_ms: std::sync::atomic::AtomicU64::new(0),
        }
    }

    fn touch(&self) {
        self.last_ms
            .store(self.epoch.elapsed().as_millis() as u64, Ordering::Relaxed);
    }

    fn last(&self) -> Instant {
        self.epoch + Duration::from_millis(self.last_ms.load(Ordering::Relaxed))
    }
}

/// Hand one parsed request to the DB thread: a Fetch with `max_wait_ms > 0` goes to a
/// long-poll task (while the per-connection cap allows), everything else straight to
/// its lane. Returns false when the worker channel is gone.
async fn dispatch_request(
    request: KafkaRequest,
    request_lanes: &crate::kafka::RequestLanes,
    registry: &Arc<PendingFetchRegistry>,
    long_poll_permits: &Arc<tokio::sync::Semaphore>,
    enable_long_polling: bool,
    poll_interval_ms: i32,
) -> bool {
    debug!("Request parsed successfully, dispatching to worker...");

    // Check if this is a Fetch request that should use long polling
    let should_long_poll = enable_long_polling
        && matches!(
            &request.body,
            RequestBody::Fetch(fetch) if fetch.max_wait_ms > 0
        );

    // Acquire a long-poll slot; at the cap the fetch falls through
    // to the immediate path below instead of spawning a task.
    let long_poll_permit = if should_long_poll {
        match long_poll_permits.clone().try_acquire_owned() {
            Ok(permit) => Some(permit),
            Err(_) => {
                debug!(
                    "Per-connection long-poll cap ({}) reached; serving fetch immediately",
                    MAX_LONG_POLLS_PER_CONNECTION
                );
                None
            }
        }
    } else {
        None
    };

    let request = match (long_poll_permit, request) {
        (
            Some(permit),
            KafkaRequest {
                header:
                    RequestHeader {
                        correlation_id,
                        client_id,
                        api_version,
                        ..
                    },
                body:
                    RequestBody::Fetch(FetchRequestData {
                        max_wait_ms,
                        min_bytes,
                        max_bytes,
                        isolation_level,
                        topic_data,
                    }),
                response_tx: original_response_tx,
            },
        ) => {
            debug!(
                "Fetch request with long polling: max_wait_ms={}, min_bytes={}, isolation_level={}",
                max_wait_ms, min_bytes, isolation_level
            );

            let long_poll_registry = registry.clone();
            let long_poll_request_tx = request_lanes.main.clone();

            tokio::spawn(async move {
                // Hold the per-connection slot for the task's lifetime.
                let _long_poll_slot = permit;
                if let Err(e) = handle_fetch_long_poll(
                    correlation_id,
                    client_id,
                    api_version,
                    max_wait_ms,
                    min_bytes,
                    max_bytes,
                    isolation_level,
                    topic_data,
                    long_poll_request_tx,
                    original_response_tx,
                    long_poll_registry,
                    poll_interval_ms,
                )
                .await
                {
                    warn!("Long poll handler error: {}", e);
                }
            });

            // Don't send to worker directly - the long poll handler will do it
            return true;
        }
        (_, request) => request,
    };

    // Normal path - send to worker directly (SEC-4: non-blocking handoff).
    // DR-12: Heartbeat routes to the liveness lane, everything else to
    // the main lane, so group liveness never queues behind heavy work.
    let lane = request_lanes.route(&request).clone();
    send_with_backpressure(&lane, request).await
}

/// Answer one SaslHandshake / SaslAuthenticate request. Returns false when the
/// connection must close (after its response, if any, is written).
async fn handle_sasl_frame(
    auth: &mut ConnectionAuth,
    frame: BytesMut,
    response_tx: UnboundedSender<KafkaResponse>,
) -> bool {
    // Counted in kafka.api_metrics like a worker request (the counters are atomics, safe
    // on this thread): latency covers the decode, the credential lookup and the
    // response; a request that closes the connection without one counts as an error.
    let api_key = crate::kafka::auth::session::peek_api_key(&frame).unwrap_or(-1);
    let mut timer = crate::kafka::metrics::RequestTimer::start(api_key);
    let request = match crate::kafka::auth::session::decode_sasl_request(frame) {
        Ok(r) => r,
        Err(e) => {
            timer.mark_error();
            warn!("Closing connection from {}: {}", auth.peer, e);
            return false;
        }
    };
    let lookup = auth.lookup.clone();
    let throttle = auth.lookup_throttle.clone();
    let lookup = ThrottledLookup {
        inner: &lookup,
        throttle: throttle.as_ref(),
    };
    match auth.sasl.handle(request, &lookup).await {
        SaslOutcome::Reply(response) => {
            timer.observe(&response);
            response_tx.send(response).is_ok()
        }
        SaslOutcome::Authenticated {
            response,
            principal,
            mechanism,
        } => {
            auth.principal = ConnectionPrincipal {
                principal,
                method: mechanism,
            };
            if auth.log_connections {
                info!(
                    "Kafka client {} authenticated as {} on {}://{}",
                    auth.peer,
                    auth.principal,
                    auth.security.protocol.name(),
                    auth.security.bind_addr
                );
            }
            timer.observe(&response);
            response_tx.send(response).is_ok()
        }
        SaslOutcome::ReplyAndClose(response) => {
            timer.observe(&response);
            let _ = response_tx.send(response);
            false
        }
        SaslOutcome::Failed { response, reason } => {
            // Never includes the password; no username either (it may be a mistyped
            // password).
            warn!(
                "SASL authentication failed for {} on {}://{}: {}",
                auth.peer,
                auth.security.protocol.name(),
                auth.security.bind_addr,
                reason
            );
            timer.observe(&response);
            // Record before the deliberate failure delay, which is not service time.
            drop(timer);
            tokio::time::sleep(AUTH_FAILURE_DELAY).await;
            let _ = response_tx.send(response);
            false
        }
    }
}

/// Point Metadata brokers and FindCoordinator coordinators at this listener's
/// advertised address (single-node broker: every broker entry is this node).
fn rewrite_advertised(response: &mut KafkaResponse, host: &str, port: i32) {
    use kafka_protocol::protocol::StrBytes;
    match &mut response.body {
        ResponseBody::Metadata(response) => {
            for broker in response.brokers.iter_mut() {
                broker.host = StrBytes::from_string(host.to_string());
                broker.port = port;
            }
        }
        ResponseBody::FindCoordinator(response) => {
            if response.error_code == 0 && !response.host.is_empty() {
                response.host = StrBytes::from_string(host.to_string());
                response.port = port;
            }
            for c in response.coordinators.iter_mut() {
                if c.error_code == 0 && !c.host.is_empty() {
                    c.host = StrBytes::from_string(host.to_string());
                    c.port = port;
                }
            }
        }
        _ => {}
    }
}

/// SEC-4: hand a request to the DB thread without blocking the async runtime.
///
/// The request channel is a bounded crossbeam channel; a plain `Sender::send` *blocks the calling
/// thread* when the channel is full (DB thread behind), which on the tokio runtime parks a whole
/// worker thread and starves every other connection scheduled on it. Instead we `try_send` and, on a
/// full channel, `await` a short sleep and retry — yielding the worker to other tasks. This preserves
/// the bounded channel's backpressure (we never drop or reorder the request, we wait for room) while
/// keeping the runtime responsive. The normal, not-full case takes the first `try_send` with no sleep.
///
/// Returns `true` once the item is enqueued, or `false` if the channel is disconnected (DB thread
/// gone) — the caller tears down the connection in that case.
async fn send_with_backpressure<T>(tx: &Sender<T>, mut item: T) -> bool {
    loop {
        match tx.try_send(item) {
            Ok(()) => return true,
            Err(crossbeam_channel::TrySendError::Full(returned)) => {
                item = returned;
                // Yield the worker thread instead of blocking it, then retry.
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            Err(crossbeam_channel::TrySendError::Disconnected(_)) => return false,
        }
    }
}

/// Handle a FetchRequest with long polling support
///
/// The fetch is re-run only when there is reason to expect new data:
/// 1. Register every requested topic-partition in the registry (one shared
///    `Notify`) BEFORE the first fetch attempt, and keep the registration for
///    the whole long poll. A notification that lands while an attempt is in
///    flight leaves a stored permit on the `Notify`, so there is no missed
///    wake-up window between "fetch returned too little" and "start waiting".
/// 2. Send the fetch to the worker; return it if it meets `min_bytes`, carries a
///    partition error, or the deadline has passed.
/// 3. Otherwise wait for a new-data notification (Produce for this partition,
///    or EndTxn for a `read_committed` fetch), the deadline, or the slow
///    safety-net interval `poll_interval_ms` (for a dropped notification or data
///    that became visible without one), then go to 2.
///
/// An idle long poll therefore costs one fetch at the start, one per safety-net
/// interval (default 1 s), and one at the deadline, instead of one per 100 ms.
#[allow(clippy::too_many_arguments)]
async fn handle_fetch_long_poll(
    correlation_id: i32,
    client_id: Option<String>,
    api_version: i16,
    max_wait_ms: i32,
    min_bytes: i32,
    max_bytes: i32,
    isolation_level: i8,
    topic_data: Vec<TopicFetchData>,
    request_tx: Sender<KafkaRequest>,
    final_response_tx: UnboundedSender<KafkaResponse>,
    registry: Arc<PendingFetchRegistry>,
    poll_interval_ms: i32,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let deadline = Instant::now() + Duration::from_millis(max_wait_ms.max(0) as u64);
    let poll_interval = Duration::from_millis(poll_interval_ms.max(1) as u64);
    let read_committed = isolation_level == 1;

    // The client is gone (its connection's writer, which owns the response slot,
    // has been torn down): do nothing rather than queue work for the DB thread.
    if final_response_tx.is_closed() {
        debug!("Long poll: client disconnected, abandoning fetch");
        return Ok(());
    }

    let notify = Arc::new(tokio::sync::Notify::new());
    for topic in &topic_data {
        for partition in &topic.partitions {
            registry
                .register(
                    &topic.name,
                    partition.partition_index,
                    partition.fetch_offset,
                    notify.clone(),
                    read_committed,
                )
                .await;
        }
    }

    let result = long_poll_loop(
        correlation_id,
        client_id,
        api_version,
        max_bytes,
        min_bytes,
        isolation_level,
        &topic_data,
        &request_tx,
        &final_response_tx,
        &notify,
        deadline,
        poll_interval,
    )
    .await;

    // Always drop our registrations, whatever the outcome.
    for topic in &topic_data {
        for partition in &topic.partitions {
            registry
                .unregister(&topic.name, partition.partition_index, &notify)
                .await;
        }
    }

    result
}

/// The fetch / wait loop of [`handle_fetch_long_poll`]; the caller owns the
/// registry registration around it.
#[allow(clippy::too_many_arguments)]
async fn long_poll_loop(
    correlation_id: i32,
    client_id: Option<String>,
    api_version: i16,
    max_bytes: i32,
    min_bytes: i32,
    isolation_level: i8,
    topic_data: &[TopicFetchData],
    request_tx: &Sender<KafkaRequest>,
    final_response_tx: &UnboundedSender<KafkaResponse>,
    notify: &tokio::sync::Notify,
    deadline: Instant,
    poll_interval: Duration,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    loop {
        // Stop instead of re-fetching for a client that has disconnected.
        if final_response_tx.is_closed() {
            debug!("Long poll: client disconnected, abandoning fetch");
            return Ok(());
        }

        // Create a response channel for this fetch attempt
        let (inner_tx, mut inner_rx) = tokio::sync::mpsc::unbounded_channel();

        // Reconstruct the Fetch request with our response channel
        let fetch_request = KafkaRequest {
            header: RequestHeader {
                api_key: crate::kafka::constants::API_KEY_FETCH,
                api_version,
                correlation_id,
                client_id: client_id.clone(),
            },
            body: RequestBody::Fetch(FetchRequestData {
                max_wait_ms: 0, // Don't long poll again in worker
                min_bytes: 0,   // Accept any amount of data
                max_bytes,
                isolation_level,
                topic_data: topic_data.to_vec(),
            }),
            response_tx: inner_tx,
        };

        // Send to worker (SEC-4: non-blocking handoff)
        if !send_with_backpressure(request_tx, fetch_request).await {
            warn!("Worker channel closed during long poll");
            return Err("Worker channel closed".into());
        }

        // Wait for response
        let response = match inner_rx.recv().await {
            Some(resp) => resp,
            None => {
                warn!("Response channel closed during long poll");
                return Err("Response channel closed".into());
            }
        };

        // Check if we have enough data
        let total_bytes = calculate_response_bytes(&response);
        debug!(
            "Long poll fetch returned {} bytes (min_bytes={})",
            total_bytes, min_bytes
        );

        // A partition error (unknown partition, offset out of range, ...) is returned
        // straight away, as Kafka does, so the consumer can react instead of waiting
        // out max_wait_ms for data that will not arrive.
        if total_bytes >= min_bytes as usize
            || min_bytes <= 0
            || response_has_partition_error(&response)
        {
            debug!("Long poll: min_bytes threshold met, returning response");
            let _ = final_response_tx.send(response);
            return Ok(());
        }

        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            debug!("Long poll: timeout reached, returning response");
            let _ = final_response_tx.send(response);
            return Ok(());
        }

        // Not enough data and not timed out: wait for a wake-up. The deadline
        // wait is exact (the final re-fetch happens at max_wait_ms); the
        // safety-net interval only bounds how long a missed notification can
        // delay delivery.
        let wait_time = remaining.min(poll_interval);
        debug!(
            "Long poll: waiting up to {:?} for new-data notification (remaining: {:?})",
            wait_time, remaining
        );

        tokio::select! {
            _ = notify.notified() => {
                debug!("Long poll: woken by new-data notification");
            }
            _ = tokio::time::sleep(wait_time) => {
                debug!("Long poll: deadline or safety-net interval elapsed, re-fetching");
            }
            _ = final_response_tx.closed() => {
                debug!("Long poll: client disconnected while waiting");
            }
        }
    }
}

/// Calculate the total bytes in a FetchResponse
fn calculate_response_bytes(response: &KafkaResponse) -> usize {
    match &response.body {
        ResponseBody::Fetch(response) => response
            .responses
            .iter()
            .flat_map(|t| t.partitions.iter())
            .filter_map(|p| p.records.as_ref())
            .map(|r| r.len())
            .sum(),
        _ => 0,
    }
}

/// Whether any partition in a Fetch response carries a non-zero error code.
fn response_has_partition_error(response: &KafkaResponse) -> bool {
    match &response.body {
        ResponseBody::Fetch(response) => response
            .responses
            .iter()
            .flat_map(|t| t.partitions.iter())
            .any(|p| p.error_code != 0),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn test_try_admit_connection_enforces_cap_and_frees_on_drop() {
        // SEC-3: admission must reject once the concurrent-connection cap is reached, and free a
        // slot when a permit (held for a connection's lifetime) is dropped. This is the policy the
        // accept loop relies on to bound fd/memory usage.
        let limiter = Arc::new(tokio::sync::Semaphore::new(2));
        let p1 = try_admit_connection(&limiter).expect("1st connection admitted under the cap");
        let _p2 = try_admit_connection(&limiter).expect("2nd connection admitted under the cap");
        assert!(
            try_admit_connection(&limiter).is_none(),
            "a 3rd connection must be rejected at the cap of 2"
        );
        drop(p1); // a connection closes, dropping its permit
        assert!(
            try_admit_connection(&limiter).is_some(),
            "closing a connection must free a slot for a new one"
        );
    }

    #[tokio::test]
    async fn test_send_with_backpressure_succeeds_once_room_frees() {
        // SEC-4: a full bounded channel must not make the send hang forever or drop the item — it
        // waits (yielding the runtime) until a slot frees, then enqueues the item in order.
        let (tx, rx) = crossbeam_channel::bounded::<i32>(1);
        tx.try_send(1).unwrap(); // fill to capacity

        let tx2 = tx.clone();
        let send = tokio::spawn(async move { send_with_backpressure(&tx2, 2).await });

        // Let the sender spin a few backoff iterations, then free a slot.
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(rx.recv().unwrap(), 1, "first item drains");

        let join = tokio::time::timeout(Duration::from_millis(500), send).await;
        assert!(join.is_ok(), "send task should finish, not hang");
        let sent = join.unwrap().expect("send task should not panic");
        assert!(sent, "send should report success once room frees");
        assert_eq!(
            rx.recv().unwrap(),
            2,
            "the backpressured item is enqueued in order"
        );
    }

    #[tokio::test]
    async fn test_send_with_backpressure_reports_disconnect() {
        // SEC-4: if the DB thread is gone (receiver dropped), the send reports failure so the caller
        // tears down the connection instead of looping forever.
        let (tx, rx) = crossbeam_channel::bounded::<i32>(1);
        drop(rx);
        assert!(
            !send_with_backpressure(&tx, 1).await,
            "a disconnected channel must report failure"
        );
    }

    #[test]
    fn test_long_poll_stops_when_client_is_gone() {
        // A long poll whose client has disconnected (response slot closed) must return
        // straight away instead of re-fetching on the DB thread until max_wait_ms.
        let (request_tx, request_rx) = crossbeam_channel::bounded::<KafkaRequest>(16);
        let (final_tx, final_rx) = tokio::sync::mpsc::unbounded_channel::<KafkaResponse>();
        drop(final_rx);
        let topic = TopicFetchData {
            name: "t".to_string(),
            partitions: vec![super::super::messages::PartitionFetchData {
                partition_index: 0,
                fetch_offset: 0,
                partition_max_bytes: 1024,
            }],
        };
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        let started = Instant::now();
        rt.block_on(handle_fetch_long_poll(
            1,
            None,
            4,
            i32::MAX,
            i32::MAX,
            1024,
            0,
            vec![topic],
            request_tx,
            final_tx,
            Arc::new(PendingFetchRegistry::new()),
            100,
        ))
        .unwrap();
        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(
            request_rx.try_recv().is_err(),
            "no fetch may reach the worker for a disconnected client"
        );
    }

    // ===== Network-thread logging =====

    #[test]
    fn test_network_thread_blocks_termination_signals() {
        // wave3 net: SIGTERM / SIGHUP / SIGINT must never be handled on the network thread
        // (or the tokio threads it spawns, which inherit the mask).
        let (term, hup, int) = std::thread::spawn(|| {
            assert!(!signal_blocked_on_this_thread(15));
            block_signals_on_this_thread();
            std::thread::spawn(|| {
                (
                    signal_blocked_on_this_thread(15),
                    signal_blocked_on_this_thread(1),
                    signal_blocked_on_this_thread(2),
                )
            })
            .join()
            .unwrap()
        })
        .join()
        .unwrap();
        assert!(term && hup && int);
    }

    #[test]
    fn test_network_log_filter_defaults_to_warn() {
        assert_eq!(network_log_filter(None), "pg_kafka=warn");
        assert_eq!(network_log_filter(Some("")), "pg_kafka=warn");
        assert_eq!(network_log_filter(Some("   ")), "pg_kafka=warn");
        // The default must parse as a filter.
        assert!(tracing_subscriber::EnvFilter::try_new(DEFAULT_NETWORK_LOG_FILTER).is_ok());
    }

    #[test]
    fn test_network_log_filter_honours_rust_log() {
        // RUST_LOG used to be overridden by a hard `pg_kafka=error` directive.
        assert_eq!(network_log_filter(Some("pg_kafka=debug")), "pg_kafka=debug");
    }

    #[test]
    fn test_peer_disconnect_classification() {
        use std::io::{Error, ErrorKind};
        assert!(is_peer_disconnect(&Error::from(ErrorKind::ConnectionReset)));
        assert!(is_peer_disconnect(&Error::from(ErrorKind::BrokenPipe)));
        assert!(is_peer_disconnect(&Error::from(ErrorKind::UnexpectedEof)));
        assert!(!is_peer_disconnect(&Error::other(
            "aggregate request reservation budget exhausted"
        )));
        assert!(!is_peer_disconnect(&Error::from(ErrorKind::InvalidData)));
    }

    // ===== Long poll: wake only on notification / deadline / safety net =====

    fn one_partition_topic(name: &str) -> TopicFetchData {
        TopicFetchData {
            name: name.to_string(),
            partitions: vec![super::super::messages::PartitionFetchData {
                partition_index: 0,
                fetch_offset: 0,
                partition_max_bytes: 1024,
            }],
        }
    }

    fn fetch_response(with_records: bool) -> KafkaResponse {
        use kafka_protocol::messages::fetch_response::{
            FetchResponse, FetchableTopicResponse, PartitionData,
        };
        let mut response = FetchResponse::default();
        if with_records {
            let mut partition = PartitionData::default();
            partition.records = Some(Bytes::from_static(b"some-record-bytes"));
            let mut topic = FetchableTopicResponse::default();
            topic.partitions = vec![partition];
            response.responses = vec![topic];
        }
        KafkaResponse::new(1, 4, ResponseBody::Fetch(response))
    }

    /// Fake DB thread: answers every Fetch it receives. `before_answer(n)` runs before
    /// answering the n-th fetch (1-based) and returns whether that answer carries records.
    /// Returns a counter of fetches served.
    fn spawn_fake_fetch_worker<F, Fut>(
        rx: crossbeam_channel::Receiver<KafkaRequest>,
        before_answer: F,
    ) -> Arc<AtomicUsize>
    where
        F: Fn(usize) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = bool> + Send,
    {
        let served = Arc::new(AtomicUsize::new(0));
        let served_out = served.clone();
        tokio::spawn(async move {
            loop {
                match rx.try_recv() {
                    Ok(KafkaRequest {
                        body: RequestBody::Fetch(_),
                        response_tx,
                        ..
                    }) => {
                        let n = served.fetch_add(1, Ordering::SeqCst) + 1;
                        let with_records = before_answer(n).await;
                        let _ = response_tx.send(fetch_response(with_records));
                    }
                    Ok(_) => {}
                    Err(crossbeam_channel::TryRecvError::Empty) => {
                        tokio::time::sleep(Duration::from_millis(1)).await
                    }
                    Err(crossbeam_channel::TryRecvError::Disconnected) => return,
                }
            }
        });
        served_out
    }

    #[tokio::test]
    async fn test_idle_long_poll_does_not_refetch_until_deadline() {
        // With no notification and a safety-net interval longer than max_wait, an idle long
        // poll runs exactly two fetches: the initial attempt and the one at the deadline. The
        // old loop re-fetched every fetch_poll_interval_ms (100 ms → ~7 fetches here).
        let (request_tx, request_rx) = crossbeam_channel::bounded::<KafkaRequest>(16);
        let served = spawn_fake_fetch_worker(request_rx, |_| async { false });
        let (final_tx, mut final_rx) = tokio::sync::mpsc::unbounded_channel();
        let registry = Arc::new(PendingFetchRegistry::new());

        let started = Instant::now();
        handle_fetch_long_poll(
            1,
            None,
            4,
            600,
            1,
            1024,
            0,
            vec![one_partition_topic("t")],
            request_tx,
            final_tx,
            registry.clone(),
            10_000,
        )
        .await
        .unwrap();

        assert!(started.elapsed() >= Duration::from_millis(600));
        assert!(final_rx.try_recv().is_ok(), "the deadline response is sent");
        assert_eq!(served.load(Ordering::SeqCst), 2);
        assert!(registry.is_empty().await, "registrations are removed");
    }

    #[tokio::test]
    async fn test_long_poll_safety_net_interval_still_refetches() {
        // Data that becomes visible without a notification is picked up at the safety-net
        // interval, well before max_wait_ms.
        let (request_tx, request_rx) = crossbeam_channel::bounded::<KafkaRequest>(16);
        let served = spawn_fake_fetch_worker(request_rx, |n| async move { n >= 2 });
        let (final_tx, mut final_rx) = tokio::sync::mpsc::unbounded_channel();

        let started = Instant::now();
        handle_fetch_long_poll(
            1,
            None,
            4,
            10_000,
            1,
            1024,
            0,
            vec![one_partition_topic("t")],
            request_tx,
            final_tx,
            Arc::new(PendingFetchRegistry::new()),
            200,
        )
        .await
        .unwrap();

        let elapsed = started.elapsed();
        assert!(elapsed >= Duration::from_millis(200), "waited {elapsed:?}");
        assert!(elapsed < Duration::from_secs(3), "waited {elapsed:?}");
        assert_eq!(served.load(Ordering::SeqCst), 2);
        assert!(calculate_response_bytes(&final_rx.try_recv().unwrap()) > 0);
    }

    #[tokio::test]
    async fn test_notification_during_fetch_attempt_is_not_missed() {
        // Missed-wakeup window: the produce notification lands while the first fetch attempt is
        // still on the DB thread (it returned "no data"). The long poll registered before that
        // attempt, so the wake-up is kept and the record is returned at once — not after the
        // safety-net interval (10 s here) or max_wait_ms (10 s).
        let (request_tx, request_rx) = crossbeam_channel::bounded::<KafkaRequest>(16);
        let registry = Arc::new(PendingFetchRegistry::new());
        let reg = registry.clone();
        let served = spawn_fake_fetch_worker(request_rx, move |n| {
            let reg = reg.clone();
            async move {
                if n == 1 {
                    reg.notify_new_data("t", 0, 1).await;
                    false
                } else {
                    true
                }
            }
        });
        let (final_tx, mut final_rx) = tokio::sync::mpsc::unbounded_channel();

        let started = Instant::now();
        handle_fetch_long_poll(
            1,
            None,
            4,
            10_000,
            1,
            1024,
            0,
            vec![one_partition_topic("t")],
            request_tx,
            final_tx,
            registry.clone(),
            10_000,
        )
        .await
        .unwrap();

        assert!(started.elapsed() < Duration::from_secs(2));
        assert_eq!(served.load(Ordering::SeqCst), 2);
        assert!(calculate_response_bytes(&final_rx.try_recv().unwrap()) > 0);
        assert!(registry.is_empty().await);
    }

    #[tokio::test]
    async fn test_read_committed_long_poll_wakes_on_transaction_end() {
        // EndTxn → TransactionEnded wakes a read_committed long poll promptly.
        let (request_tx, request_rx) = crossbeam_channel::bounded::<KafkaRequest>(16);
        let registry = Arc::new(PendingFetchRegistry::new());
        let reg = registry.clone();
        let served = spawn_fake_fetch_worker(request_rx, move |n| {
            let reg = reg.clone();
            async move {
                if n == 1 {
                    // Simulate the commit landing (after the first attempt saw nothing).
                    let reg = reg.clone();
                    tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_millis(150)).await;
                        reg.notify_transaction_ended().await;
                    });
                    false
                } else {
                    true
                }
            }
        });
        let (final_tx, mut final_rx) = tokio::sync::mpsc::unbounded_channel();

        let started = Instant::now();
        handle_fetch_long_poll(
            1,
            None,
            4,
            10_000,
            1,
            1024,
            1, // read_committed
            vec![one_partition_topic("t")],
            request_tx,
            final_tx,
            registry,
            10_000,
        )
        .await
        .unwrap();

        assert!(started.elapsed() < Duration::from_secs(2));
        assert_eq!(served.load(Ordering::SeqCst), 2);
        assert!(calculate_response_bytes(&final_rx.try_recv().unwrap()) > 0);
    }

    // ===== Per-connection in-flight bound =====

    /// An ApiVersions v0 request frame ([size][api_key][api_version][corr][client_id=null]).
    fn api_versions_v0_frame(correlation_id: i32) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&18i16.to_be_bytes());
        body.extend_from_slice(&0i16.to_be_bytes());
        body.extend_from_slice(&correlation_id.to_be_bytes());
        body.extend_from_slice(&(-1i16).to_be_bytes());
        let mut frame = (body.len() as i32).to_be_bytes().to_vec();
        frame.extend_from_slice(&body);
        frame
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_connection_bounds_in_flight_requests() {
        use tokio::io::AsyncWriteExt;

        // A client pipelines CAP + 20 requests. The fake DB thread holds every request
        // (unanswered), so no response slot completes: the reader must stop after exactly CAP
        // requests instead of reading (and buffering) all of them. Releasing the held requests
        // frees the slots and the rest flow through.
        const EXTRA: usize = 20;
        let total = MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION + EXTRA;

        let server = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = server.local_addr().unwrap();
        let (main_tx, main_rx) = crossbeam_channel::bounded::<KafkaRequest>(10_000);
        let (liveness_tx, _liveness_rx) = crossbeam_channel::bounded::<KafkaRequest>(16);
        let lanes = crate::kafka::RequestLanes {
            main: main_tx,
            liveness: liveness_tx,
        };
        tokio::spawn(async move {
            let (socket, _) = server.accept().await.unwrap();
            let (cred_tx, _cred_rx) = crossbeam_channel::bounded(1);
            let auth = ConnectionAuth::new(
                "test".into(),
                Arc::new(ListenerSecurity::plaintext("127.0.0.1:0")),
                ConnectionPrincipal::anonymous(),
                ChannelLookup(cred_tx),
                false,
            );
            let _ = handle_connection(
                socket,
                lanes,
                Arc::new(PendingFetchRegistry::new()),
                false,
                1000,
                auth,
                NetworkLimits::default(),
                tokio::time::Instant::now() + PRE_AUTH_TIMEOUT,
            )
            .await;
        });

        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let mut bytes = Vec::new();
        for i in 0..total {
            bytes.extend_from_slice(&api_versions_v0_frame(i as i32));
        }
        client.write_all(&bytes).await.unwrap();
        client.flush().await.unwrap();

        // Collect what reaches the "DB thread" while holding every request.
        let mut held = Vec::new();
        let collect_until = Instant::now() + Duration::from_millis(500);
        while Instant::now() < collect_until {
            while let Ok(req) = main_rx.try_recv() {
                held.push(req);
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(
            held.len(),
            MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
            "the reader must stop at the in-flight cap"
        );

        // Dropping the requests closes their response slots (no response, like acks=0), which
        // returns the permits; the remaining requests are then read.
        let first_batch = held.len();
        held.clear();
        let mut released = 0usize;
        let deadline = Instant::now() + Duration::from_secs(5);
        while first_batch + released < total && Instant::now() < deadline {
            while let Ok(req) = main_rx.try_recv() {
                drop(req);
                released += 1;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(
            first_batch + released,
            total,
            "all requests flow once slots free"
        );
    }

    // ===== wave3 net: connection lifecycle =====

    /// Serve one connection on an ephemeral port with `limits`; requests go to the
    /// returned receiver (the fake DB thread).
    async fn serve_one(
        limits: NetworkLimits,
    ) -> (
        std::net::SocketAddr,
        crossbeam_channel::Receiver<KafkaRequest>,
    ) {
        let server = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = server.local_addr().unwrap();
        let (main_tx, main_rx) = crossbeam_channel::bounded::<KafkaRequest>(1000);
        let (liveness_tx, _liveness_rx) = crossbeam_channel::bounded::<KafkaRequest>(16);
        tokio::spawn(async move {
            let (socket, _) = server.accept().await.unwrap();
            let (cred_tx, _cred_rx) = crossbeam_channel::bounded(1);
            let auth = ConnectionAuth::new(
                "test".into(),
                Arc::new(ListenerSecurity::plaintext("127.0.0.1:0")),
                ConnectionPrincipal::anonymous(),
                ChannelLookup(cred_tx),
                false,
            );
            let _ = handle_connection(
                socket,
                crate::kafka::RequestLanes {
                    main: main_tx,
                    liveness: liveness_tx,
                },
                Arc::new(PendingFetchRegistry::new()),
                false,
                1000,
                auth,
                limits,
                tokio::time::Instant::now() + PRE_AUTH_TIMEOUT,
            )
            .await;
        });
        (addr, main_rx)
    }

    async fn recv_request(rx: &crossbeam_channel::Receiver<KafkaRequest>) -> KafkaRequest {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Ok(r) = rx.try_recv() {
                return r;
            }
            assert!(Instant::now() < deadline, "no request reached the worker");
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }

    /// Bytes until EOF (or `None` if the connection stays open for `within`).
    async fn read_until_eof(
        client: &mut tokio::net::TcpStream,
        within: Duration,
    ) -> Option<(Vec<u8>, Duration)> {
        use tokio::io::AsyncReadExt;
        let started = Instant::now();
        let mut out = Vec::new();
        let mut buf = [0u8; 1024];
        loop {
            let left = within.checked_sub(started.elapsed())?;
            match tokio::time::timeout(left, client.read(&mut buf)).await {
                Ok(Ok(0)) | Ok(Err(_)) => return Some((out, started.elapsed())),
                Ok(Ok(n)) => out.extend_from_slice(&buf[..n]),
                Err(_) => return None,
            }
        }
    }

    fn api_versions_response(correlation_id: i32) -> KafkaResponse {
        KafkaResponse::new(
            correlation_id,
            0,
            ResponseBody::ApiVersions(kafka_protocol::messages::ApiVersionsResponse::default()),
        )
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_close_instruction_ends_the_connection_after_earlier_responses() {
        // Item 2/4: the writer exiting (here on the acks=0 CloseConnection instruction)
        // ends the connection; the earlier request's response is written first.
        use tokio::io::AsyncWriteExt;
        let (addr, rx) = serve_one(NetworkLimits::default()).await;
        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let mut bytes = api_versions_v0_frame(1);
        bytes.extend_from_slice(&api_versions_v0_frame(2));
        client.write_all(&bytes).await.unwrap();
        let first = recv_request(&rx).await;
        let second = recv_request(&rx).await;
        let _ = first.response_tx.send(api_versions_response(1));
        let _ = second
            .response_tx
            .send(KafkaResponse::new(-1, -1, ResponseBody::CloseConnection));
        // The worker drops a request once answered, closing its response slot.
        drop((first, second));
        let (bytes, _) = read_until_eof(&mut client, Duration::from_secs(3))
            .await
            .expect("the connection must close");
        assert!(bytes.len() > 8, "the first response is delivered");
        assert_eq!(i32::from_be_bytes(bytes[4..8].try_into().unwrap()), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_response_delivered_before_a_bad_frame_closes() {
        // Item 6: the old code gave the writer 100 ms after a bad frame and then aborted
        // it, so a response that took longer (a long poll, a slow commit) was lost.
        use tokio::io::AsyncWriteExt;
        let (addr, rx) = serve_one(NetworkLimits::default()).await;
        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let mut bytes = api_versions_v0_frame(7);
        bytes.extend_from_slice(&[0, 0, 0, 3, 0, 0x12, 0]); // truncated header
        client.write_all(&bytes).await.unwrap();
        let request = recv_request(&rx).await;
        tokio::time::sleep(Duration::from_millis(400)).await;
        let _ = request.response_tx.send(api_versions_response(7));
        drop(request);
        let (bytes, _) = read_until_eof(&mut client, Duration::from_secs(3))
            .await
            .expect("the connection must close");
        assert!(
            bytes.len() > 8,
            "the response is delivered before the close"
        );
        assert_eq!(i32::from_be_bytes(bytes[4..8].try_into().unwrap()), 7);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_idle_timeout_waits_for_outstanding_requests() {
        use tokio::io::AsyncWriteExt;
        let limits = NetworkLimits {
            connections_max_idle: Duration::from_millis(300),
            ..Default::default()
        };
        // Idle with nothing outstanding: closed after ~300 ms.
        let (addr, _rx) = serve_one(limits).await;
        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (_, after) = read_until_eof(&mut client, Duration::from_secs(3))
            .await
            .expect("an idle connection must close");
        assert!(after >= Duration::from_millis(250), "{after:?}");

        // A request outstanding for 800 ms keeps the connection open past the timeout.
        let (addr, rx) = serve_one(limits).await;
        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
        client.write_all(&api_versions_v0_frame(3)).await.unwrap();
        let request = recv_request(&rx).await;
        tokio::time::sleep(Duration::from_millis(800)).await;
        let _ = request.response_tx.send(api_versions_response(3));
        drop(request);
        let (bytes, after) = read_until_eof(&mut client, Duration::from_secs(3))
            .await
            .expect("idle again after the response: closes");
        assert_eq!(i32::from_be_bytes(bytes[4..8].try_into().unwrap()), 3);
        // Counted from the response (its write is activity), not from the request.
        assert!(after >= Duration::from_millis(250), "{after:?}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_partial_frame_read_deadline() {
        // Item 1: a frame that never completes used to be waited for forever.
        use tokio::io::AsyncWriteExt;
        let limits = NetworkLimits {
            request_read_timeout: Duration::from_millis(300),
            ..Default::default()
        };
        let (addr, _rx) = serve_one(limits).await;
        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
        client.write_all(&1000u32.to_be_bytes()).await.unwrap();
        client.write_all(&[1, 2, 3]).await.unwrap();
        let (_, after) = read_until_eof(&mut client, Duration::from_secs(3))
            .await
            .expect("a stalled frame must close the connection");
        assert!(after >= Duration::from_millis(250), "{after:?}");
    }
}
