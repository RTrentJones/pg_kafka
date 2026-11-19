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

use bytes::{Bytes, BytesMut};
use crossbeam_channel::{Receiver, Sender};
use futures::sink::SinkExt;
use futures::stream::StreamExt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::UnboundedSender;
use tokio_util::codec::{Decoder, Encoder, Framed, LengthDelimitedCodec};
use tracing::{debug, error, info, warn};

use super::api::{FetchRequestData, RequestBody, RequestHeader, ResponseBody};
use super::messages::{KafkaRequest, KafkaResponse, TopicFetchData};
use super::notifications::InternalNotification;
use super::pending_fetches::PendingFetchRegistry;
use super::protocol;
use super::shadow::{ForwardAck, ForwardRequest};
use crate::kafka::auth::session::{SaslOutcome, AUTH_FAILURE_DELAY};
use crate::kafka::auth::{
    ChannelLookup, ConnectionPrincipal, CredentialRequest, SaslMechanism, SaslSession,
    SecurityProtocol,
};

// ============================================================================
// RV-9: aggregate in-flight frame-reservation budget.
//
// LengthDelimitedCodec reserves the full declared frame length as soon as the
// 4-byte length prefix arrives, so MAX_CONNECTIONS (1024) connections each
// declaring a MAX_REQUEST_SIZE (100 MB) frame could reserve ~100 GB from a few KB
// of attacker input. This budget bounds the *aggregate* reservation across all
// connections while still admitting individual large frames until it is hit;
// small requests are effectively unrestricted.
// ============================================================================

/// Total bytes that may be reserved across all connections for in-flight frame
/// reads at once. 512 MiB comfortably admits several concurrent max-size frames
/// while keeping the worst case far below the ~100 GB the connection cap alone
/// allowed.
const FRAME_RESERVE_BUDGET_BYTES: usize = 512 * 1024 * 1024;

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

static FRAME_RESERVE_REMAINING: AtomicUsize = AtomicUsize::new(FRAME_RESERVE_BUDGET_BYTES);

/// Try to charge `n` bytes to `budget`. Returns false if it lacks `n` bytes, in
/// which case the caller rejects the frame.
fn charge_frame_budget(budget: &AtomicUsize, n: usize) -> bool {
    let mut remaining = budget.load(Ordering::Relaxed);
    loop {
        if n > remaining {
            return false;
        }
        match budget.compare_exchange_weak(
            remaining,
            remaining - n,
            Ordering::Acquire,
            Ordering::Relaxed,
        ) {
            Ok(_) => return true,
            Err(actual) => remaining = actual,
        }
    }
}

fn release_frame_budget(budget: &AtomicUsize, n: usize) {
    if n > 0 {
        budget.fetch_add(n, Ordering::Release);
    }
}

/// A `LengthDelimitedCodec` wrapper that charges the global reservation budget for
/// a frame's declared length before the inner codec reserves it, and releases it
/// once the frame is fully decoded (RV-9). Encoding is delegated unchanged.
struct BudgetedFrameCodec {
    inner: LengthDelimitedCodec,
    /// Bytes charged to the budget for the in-progress frame (0 = none charged).
    charged: usize,
    /// auth stream: largest frame accepted right now. A SASL listener starts at
    /// [`PRE_AUTH_MAX_FRAME`] and the connection raises it to `MAX_REQUEST_SIZE` once
    /// authentication succeeds (Kafka's `sasl.server.max.receive.size`), so an
    /// unauthenticated client cannot make the broker reserve 100 MB frames.
    limit: Arc<AtomicUsize>,
}

/// auth stream: largest frame an unauthenticated client may send on a SASL listener
/// (Kafka's `sasl.server.max.receive.size` default).
const PRE_AUTH_MAX_FRAME: usize = 512 * 1024;

impl BudgetedFrameCodec {
    #[cfg(test)]
    fn new() -> Self {
        Self::with_limit(Arc::new(AtomicUsize::new(
            super::constants::MAX_REQUEST_SIZE as usize,
        )))
    }

    fn with_limit(limit: Arc<AtomicUsize>) -> Self {
        Self {
            limit,
            inner: LengthDelimitedCodec::builder()
                .big_endian()
                .length_field_length(4)
                .max_frame_length(super::constants::MAX_REQUEST_SIZE as usize)
                .new_codec(),
            charged: 0,
        }
    }
}

impl Decoder for BudgetedFrameCodec {
    type Item = BytesMut;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> std::io::Result<Option<BytesMut>> {
        // Charge the budget for this frame's declared payload length as soon as the
        // 4-byte big-endian prefix is available, before the inner codec reserves it.
        if self.charged == 0 && src.len() >= 4 {
            let len = u32::from_be_bytes([src[0], src[1], src[2], src[3]]) as usize;
            let limit = self.limit.load(Ordering::Relaxed);
            if len > limit && limit < super::constants::MAX_REQUEST_SIZE as usize {
                return Err(std::io::Error::other(
                    "frame larger than allowed before SASL authentication",
                ));
            }
            if len <= super::constants::MAX_REQUEST_SIZE as usize {
                if !charge_frame_budget(&FRAME_RESERVE_REMAINING, len) {
                    return Err(std::io::Error::other(
                        "aggregate request reservation budget exhausted",
                    ));
                }
                self.charged = len;
            }
            // len > MAX_REQUEST_SIZE is left uncharged; the inner codec rejects it.
        }

        match self.inner.decode(src) {
            Ok(Some(frame)) => {
                release_frame_budget(&FRAME_RESERVE_REMAINING, self.charged);
                self.charged = 0;
                Ok(Some(frame))
            }
            Ok(None) => Ok(None),
            Err(e) => {
                release_frame_budget(&FRAME_RESERVE_REMAINING, self.charged);
                self.charged = 0;
                Err(e)
            }
        }
    }
}

/// Release any charge still held for a partially received frame when the codec goes
/// away (e.g. the client disconnects mid-frame). Without this, each abandoned frame
/// would permanently shrink the global reservation budget.
impl Drop for BudgetedFrameCodec {
    fn drop(&mut self) {
        release_frame_budget(&FRAME_RESERVE_REMAINING, self.charged);
        self.charged = 0;
    }
}

impl Encoder<Bytes> for BudgetedFrameCodec {
    type Error = std::io::Error;

    fn encode(&mut self, item: Bytes, dst: &mut BytesMut) -> std::io::Result<()> {
        self.inner.encode(item, dst)
    }
}

/// Try to admit a new connection under the concurrent-connection cap (SEC-3).
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

/// How long a client on a SASL listener may take, from connecting, to complete SASL
/// authentication before the connection is closed.
const SASL_AUTH_TIMEOUT: Duration = Duration::from_secs(10);

/// How long a client may take to complete the TLS handshake before the connection is
/// dropped (bounds what a client that connects and stalls can hold).
const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

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
        }
    }
}

/// Shared state every accepted connection needs.
#[derive(Clone)]
struct AcceptContext {
    request_lanes: crate::kafka::RequestLanes,
    registry: Arc<PendingFetchRegistry>,
    conn_limiter: Arc<tokio::sync::Semaphore>,
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
        super::shadow::forwarder_task::run_shadow_forwarder(
            forward_rx,
            forward_ack_tx,
            runtime_context_for_forwarder,
        )
        .await;
    });

    // SEC-3: bound concurrent client connections to prevent fd/memory exhaustion. Each
    // connection task can buffer up to one MAX_REQUEST_SIZE frame, so unbounded connections
    // are a DoS vector. A permit is held for the connection's lifetime and freed on close.
    // The cap is shared by all listeners.
    let accept_context = AcceptContext {
        request_lanes,
        registry,
        conn_limiter: Arc::new(tokio::sync::Semaphore::new(
            crate::kafka::constants::MAX_CONNECTIONS,
        )),
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
    loop {
        match listener.accept().await {
            Ok((socket, addr)) => {
                // SEC-3: enforce the concurrent-connection cap. try_acquire is non-blocking;
                // at the cap we close the new socket rather than buffer an unbounded number
                // of connections.
                let permit = match try_admit_connection(&ctx.conn_limiter) {
                    Some(permit) => permit,
                    None => {
                        if ctx.log_connections {
                            warn!(
                                "Connection limit ({}) reached; rejecting {}",
                                crate::kafka::constants::MAX_CONNECTIONS,
                                addr
                            );
                        }
                        crate::kafka::metrics::connection_rejected();
                        drop(socket);
                        continue;
                    }
                };

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
                    // Hold the connection permit for the task's lifetime; dropping it on exit
                    // frees a slot for a new connection (SEC-3).
                    let _permit = permit;
                    let _conn_metrics = crate::kafka::metrics::ConnectionGuard::open();
                    let should_log = ctx.log_connections;
                    let result = serve_connection(socket, addr, security, ctx).await;
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
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let peer = addr.to_string();
    let Some(acceptor) = security.tls.clone() else {
        let auth = ConnectionAuth::new(
            peer,
            security,
            ConnectionPrincipal::anonymous(),
            ctx.lookup,
            ctx.log_connections,
        );
        return handle_connection(
            socket,
            ctx.request_lanes,
            ctx.registry,
            ctx.enable_long_polling,
            ctx.fetch_poll_interval_ms,
            auth,
        )
        .await;
    };
    let tls = match tokio::time::timeout(TLS_HANDSHAKE_TIMEOUT, acceptor.accept(socket)).await {
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
    let auth = ConnectionAuth::new(peer, security, principal, ctx.lookup, ctx.log_connections);
    handle_connection(
        tls,
        ctx.request_lanes,
        ctx.registry,
        ctx.enable_long_polling,
        ctx.fetch_poll_interval_ms,
        auth,
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
async fn handle_connection<S>(
    socket: S,
    request_lanes: crate::kafka::RequestLanes,
    registry: Arc<PendingFetchRegistry>,
    enable_long_polling: bool,
    poll_interval_ms: i32,
    mut auth: ConnectionAuth,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    debug!("New connection established, starting pipelined handle loop");

    // Wrap socket in a budgeted length-delimited codec (big-endian 4-byte size
    // prefix). RV-9: the codec charges a global reservation budget for each frame's
    // declared length so aggregate in-flight reservations across connections are
    // bounded, not just the connection count.
    let frame_limit = Arc::new(AtomicUsize::new(
        if auth.sasl.is_authenticated() || !auth.security.protocol.uses_sasl() {
            super::constants::MAX_REQUEST_SIZE as usize
        } else {
            PRE_AUTH_MAX_FRAME
        },
    ));
    let framed = Framed::new(socket, BudgetedFrameCodec::with_limit(frame_limit.clone()));
    let auth_deadline = tokio::time::Instant::now() + SASL_AUTH_TIMEOUT;

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

    // Spawn the Writer Task
    // This task sits and waits for responses from the DB.
    // It runs completely independently of the reader.
    let writer_handle: tokio::task::JoinHandle<Result<(), String>> = tokio::spawn(async move {
        while let Some((mut slot, _in_flight_permit)) = order_rx.recv().await {
            // A request yields at most one response; one that yields none
            // (acks=0 produce) closes the channel without sending and is
            // skipped here.
            while let Some(mut response) = slot.recv().await {
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
                debug!("Response sent to client successfully");
            }
            // `_in_flight_permit` drops here: this request's response (if any) is
            // on the socket, so the reader may read one more frame.
        }
        Ok(())
    });

    // Run the Reader Loop (Main Task)
    // This loop ONLY reads and dispatches. It never waits for the DB response.
    loop {
        // Backpressure: wait for an in-flight slot before reading the next frame.
        // The semaphore is never closed, so acquire only fails if that changes.
        let in_flight_permit = match in_flight.clone().acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => break,
        };
        // auth stream: an unauthenticated connection on a SASL listener must finish
        // authenticating within SASL_AUTH_TIMEOUT, so idle unauthenticated clients
        // cannot hold connection slots forever.
        let next = if auth.sasl.admits(-1) {
            reader.next().await
        } else {
            match tokio::time::timeout_at(auth_deadline, reader.next()).await {
                Ok(next) => next,
                Err(_) => {
                    warn!(
                        "Closing connection from {}: SASL authentication not completed within {:?}",
                        auth.peer, SASL_AUTH_TIMEOUT
                    );
                    break;
                }
            }
        };
        let frame = match next {
            None => break,
            Some(Ok(f)) => f,
            Some(Err(e)) => {
                if is_peer_disconnect(&e) {
                    debug!("Client disconnected mid-frame: {}", e);
                } else {
                    warn!("Error reading frame: {}", e);
                }
                break;
            }
        };

        debug!("Received frame of {} bytes", frame.len());

        // Create this request's response channel and queue its receiver to
        // the writer BEFORE dispatching, so the writer's output order is
        // fixed by request arrival order.
        let (response_tx, response_rx) = tokio::sync::mpsc::unbounded_channel();
        if order_tx.send((response_rx, in_flight_permit)).is_err() {
            warn!("Writer task gone, closing connection");
            break;
        }

        // Listener security: SaslHandshake / SaslAuthenticate are answered here, on
        // the network thread, from the connection's SASL state. On a SASL listener
        // nothing but ApiVersions and those two is processed before authentication
        // succeeds; anything else closes the connection, as Kafka does.
        let api_key = crate::kafka::auth::session::peek_api_key(&frame);
        if let Some(key) = api_key.filter(|k| crate::kafka::auth::session::is_sasl_api(*k)) {
            debug!("SASL request api_key={} from {}", key, auth.peer);
            if handle_sasl_frame(&mut auth, frame, response_tx).await {
                if auth.sasl.is_authenticated() {
                    frame_limit.store(
                        super::constants::MAX_REQUEST_SIZE as usize,
                        Ordering::Relaxed,
                    );
                }
                continue;
            }
            break;
        }
        if !auth.sasl.admits(api_key.unwrap_or(-1)) {
            warn!(
                "Closing connection from {} on {}://{}: request api_key={:?} before SASL authentication",
                auth.peer,
                auth.security.protocol.name(),
                auth.security.bind_addr,
                api_key
            );
            break;
        }

        match protocol::parse_request(frame, response_tx) {
            Ok(Some(request)) => {
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

                if let Some(permit) = long_poll_permit {
                    // Destructure for long polling - we know it's a Fetch with max_wait_ms > 0
                    if let KafkaRequest {
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
                    } = request
                    {
                        debug!(
                            "Fetch request with long polling: max_wait_ms={}, min_bytes={}, isolation_level={}",
                            max_wait_ms, min_bytes, isolation_level
                        );

                        let long_poll_registry = registry.clone();
                        let long_poll_request_tx = request_lanes.main.clone();
                        let poll_interval = poll_interval_ms;

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
                                poll_interval,
                            )
                            .await
                            {
                                warn!("Long poll handler error: {}", e);
                            }
                        });

                        // Don't send to worker directly - the long poll handler will do it
                        continue;
                    }
                }

                // Normal path - send to worker directly (SEC-4: non-blocking handoff).
                // DR-12: Heartbeat routes to the liveness lane, everything else to
                // the main lane, so group liveness never queues behind heavy work.
                let lane = request_lanes.route(&request).clone();
                if !send_with_backpressure(&lane, request).await {
                    error!("Worker channel closed");
                    break;
                }
            }
            Ok(None) => {
                // Parse error - error response already queued via response_tx
                // Continue reading more requests (don't break connection)
                debug!("Parse returned None, continuing...");
            }
            Err(e) => {
                warn!("Framing/Parse error: {}", e);
                break;
            }
        }
    }

    // Cleanup: Drop order_tx so the writer exits once it has drained the
    // response slots of requests that were already dispatched
    drop(order_tx);

    // Wait briefly for writer to finish sending any pending responses, then
    // abort it: an in-flight long poll could otherwise hold the task (and
    // the half-open socket) alive for up to max_wait_ms after disconnect
    let mut writer_handle = writer_handle;
    if tokio::time::timeout(std::time::Duration::from_millis(100), &mut writer_handle)
        .await
        .is_err()
    {
        writer_handle.abort();
        let _ = writer_handle.await;
    }

    if auth.log_connections {
        info!(
            "Kafka connection from {} ({}) closed",
            auth.peer, auth.principal
        );
    }
    debug!("Connection handler exiting");
    Ok(())
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

    // RV-9: the aggregate frame-reservation budget admits reservations up to its
    // ceiling, rejects a frame that would overshoot without mutating the budget,
    // and restores capacity on release so completed frames free their reservation.
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

    #[test]
    fn test_budgeted_codec_releases_charge_when_dropped_mid_frame() {
        // A client that sends a length prefix and disconnects leaves the codec holding a
        // charge for a frame that never completes. Dropping the codec (the connection
        // closing) must return that charge to the global budget. This is the only test
        // that touches FRAME_RESERVE_REMAINING, so the before/after reads are stable.
        let before = FRAME_RESERVE_REMAINING.load(Ordering::Acquire);
        let mut codec = BudgetedFrameCodec::new();
        let mut src = BytesMut::from(&1_000_000u32.to_be_bytes()[..]);
        assert!(codec.decode(&mut src).unwrap().is_none());
        assert_eq!(
            FRAME_RESERVE_REMAINING.load(Ordering::Acquire),
            before - 1_000_000,
            "the declared frame length is charged as soon as the prefix arrives"
        );
        drop(codec);
        assert_eq!(FRAME_RESERVE_REMAINING.load(Ordering::Acquire), before);
    }

    #[test]
    fn test_codec_enforces_the_pre_auth_frame_limit() {
        // A SASL listener's codec refuses a frame declared larger than the pre-auth
        // limit before reserving anything. (Raising the limit after authentication is
        // covered by the auth E2E tests; decoding here would touch the global budget.)
        let limit = Arc::new(AtomicUsize::new(PRE_AUTH_MAX_FRAME));
        let mut codec = BudgetedFrameCodec::with_limit(limit);
        let big = (PRE_AUTH_MAX_FRAME as u32 + 1).to_be_bytes();
        let mut src = BytesMut::from(&big[..]);
        assert!(codec.decode(&mut src).is_err());
        // Nothing was charged to the global budget for the refused frame.
        assert_eq!(codec.charged, 0);
    }

    #[test]
    fn test_frame_reserve_budget_admits_then_rejects_then_releases() {
        let budget = AtomicUsize::new(100);

        // A reservation that fits is admitted and deducted.
        assert!(charge_frame_budget(&budget, 60));
        assert_eq!(budget.load(Ordering::Relaxed), 40);

        // A reservation larger than what remains is rejected and leaves the
        // budget untouched (no partial charge).
        assert!(!charge_frame_budget(&budget, 50));
        assert_eq!(budget.load(Ordering::Relaxed), 40);

        // A reservation that exactly fits the remainder is admitted, exhausting it.
        assert!(charge_frame_budget(&budget, 40));
        assert_eq!(budget.load(Ordering::Relaxed), 0);

        // With the budget exhausted, even a single byte is refused.
        assert!(!charge_frame_budget(&budget, 1));

        // Releasing a completed frame's reservation restores capacity, which the
        // next reservation can then draw on.
        release_frame_budget(&budget, 60);
        assert_eq!(budget.load(Ordering::Relaxed), 60);
        assert!(charge_frame_budget(&budget, 60));
        assert_eq!(budget.load(Ordering::Relaxed), 0);
    }
}
