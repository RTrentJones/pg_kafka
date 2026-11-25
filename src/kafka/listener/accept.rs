//! The accept loop of each listener: connection caps, TCP options, and handing each
//! admitted connection (after the TLS handshake on SSL / SASL_SSL listeners) to the
//! connection loop.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::{debug, info, warn};

use super::connection::{handle_connection, ActivityClock, Tracked, PRE_AUTH_TIMEOUT};
use super::dispatch::Dispatcher;
use super::sasl::{ConnectionAuth, LookupThrottle};
use crate::kafka::auth::{ChannelLookup, ConnectionPrincipal, SaslMechanism, SecurityProtocol};
use crate::kafka::conn_limits::{IpPermit, IpTracker, NetworkLimits};
use crate::kafka::metrics::ConnectionGuard;

/// How long the accept loop backs off after a failed `accept` (e.g. EMFILE) instead of
/// spinning.
const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_millis(10);

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
    /// Connection caps and timeouts (`pg_kafka.max_connections` is per listener; the
    /// per-IP cap is shared by all listeners).
    pub limits: NetworkLimits,
}

impl ListenerSecurity {
    /// A PLAINTEXT listener with no overrides and the default limits.
    #[cfg(test)]
    pub(super) fn plaintext(bind_addr: &str) -> Self {
        ListenerSecurity {
            protocol: SecurityProtocol::Plaintext,
            bind_addr: bind_addr.to_string(),
            tls: None,
            sasl_mechanisms: Arc::new(Vec::new()),
            advertised: None,
            limits: NetworkLimits::default(),
        }
    }

    /// `PROTOCOL://host:port`, for logs.
    pub(super) fn describe(&self) -> String {
        format!("{}://{}", self.protocol.name(), self.bind_addr)
    }
}

/// Shared state every accepted connection needs.
#[derive(Clone)]
pub(super) struct AcceptContext {
    pub(super) dispatcher: Dispatcher,
    /// Per-IP connection counts and lookup buckets, shared by all listeners.
    pub(super) ip_tracker: Arc<IpTracker>,
    pub(super) lookup: ChannelLookup,
    pub(super) log_connections: bool,
}

/// The slots an admitted connection holds for its lifetime; dropping it frees them.
struct Admission {
    _listener_slot: OwnedSemaphorePermit,
    _ip_slot: IpPermit,
    _metrics: ConnectionGuard<'static>,
}

/// Take a slot under the listener's connection cap, or `None` at the cap (the caller
/// then closes the new socket). The permit is held for the connection's lifetime;
/// dropping it frees the slot. Non-blocking, so the accept loop keeps accepting (and
/// refusing) while at capacity.
fn try_admit_connection(limiter: &Arc<Semaphore>) -> Option<OwnedSemaphorePermit> {
    Arc::clone(limiter).try_acquire_owned().ok()
}

/// Accept connections on one listener until aborted.
pub(super) async fn accept_loop(
    listener: TcpListener,
    security: Arc<ListenerSecurity>,
    ctx: AcceptContext,
) {
    // This listener's own cap, so a flood on another listener cannot lock its clients
    // out; together they bound fd and memory use.
    let limiter = Arc::new(Semaphore::new(security.limits.max_connections));
    loop {
        let (socket, addr) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(e) => {
                warn!("Error accepting connection: {e}");
                tokio::time::sleep(ACCEPT_ERROR_BACKOFF).await;
                continue;
            }
        };
        let accepted_at = tokio::time::Instant::now();
        let Some(admission) = admit(&limiter, &security, &ctx, addr) else {
            crate::kafka::metrics::connection_rejected();
            drop(socket);
            continue;
        };
        // Kafka clients pipeline small requests; with Nagle on, a response that follows
        // another unacknowledged one waits for the client's delayed ACK (~40 ms). Kafka
        // sets TCP_NODELAY on every socket; on a TLS listener this is the TCP socket
        // under the TLS stream (rustls adds no buffering of its own between records).
        if let Err(e) = socket.set_nodelay(true) {
            debug!("Could not set TCP_NODELAY for {addr}: {e}");
        }
        if ctx.log_connections {
            info!(
                "Kafka client connected from {addr} on {}",
                security.describe()
            );
        }
        let ctx = ctx.clone();
        let security = Arc::clone(&security);
        tokio::spawn(async move {
            let _admission = admission;
            let log_connections = ctx.log_connections;
            serve_connection(socket, addr, security, ctx, accepted_at).await;
            if log_connections {
                info!("Kafka client disconnected from {addr}");
            }
        });
    }
}

/// Admit a connection from `addr` under the listener's cap and the per-IP cap, or
/// `None` (logged when `pg_kafka.log_connections` is on) when either is reached.
fn admit(
    limiter: &Arc<Semaphore>,
    security: &ListenerSecurity,
    ctx: &AcceptContext,
    addr: SocketAddr,
) -> Option<Admission> {
    let Some(listener_slot) = try_admit_connection(limiter) else {
        if ctx.log_connections {
            warn!(
                "Connection limit ({}) of {} reached; rejecting {addr}",
                security.limits.max_connections,
                security.describe()
            );
        }
        return None;
    };
    let Some(ip_slot) = ctx.ip_tracker.try_open(addr.ip()) else {
        if ctx.log_connections {
            warn!(
                "Per-IP connection limit ({}) reached for {}; rejecting",
                security.limits.max_connections_per_ip,
                addr.ip()
            );
        }
        return None;
    };
    Some(Admission {
        _listener_slot: listener_slot,
        _ip_slot: ip_slot,
        _metrics: ConnectionGuard::open(),
    })
}

/// TLS handshake (SSL / SASL_SSL listeners), then the Kafka protocol loop. One
/// pre-authentication deadline, from accept, covers both the TLS handshake and SASL.
async fn serve_connection(
    socket: TcpStream,
    addr: SocketAddr,
    security: Arc<ListenerSecurity>,
    ctx: AcceptContext,
    accepted_at: tokio::time::Instant,
) {
    let peer = addr.to_string();
    let pre_auth_deadline = accepted_at + PRE_AUTH_TIMEOUT;
    // Every byte written to the TCP socket is activity (the idle timeout): under the TLS
    // stream, so the records rustls buffers count when they go out, on flush too.
    let activity = Arc::new(ActivityClock::new());
    let socket = Tracked::new(socket, Arc::clone(&activity));
    let throttle = Some(LookupThrottle {
        tracker: Arc::clone(&ctx.ip_tracker),
        ip: addr.ip(),
    });
    let Some(acceptor) = security.tls.clone() else {
        let auth = ConnectionAuth::new(
            peer,
            security,
            ConnectionPrincipal::anonymous(),
            ctx.lookup,
            ctx.log_connections,
            throttle,
        );
        handle_connection(socket, activity, ctx.dispatcher, auth, pre_auth_deadline).await;
        return;
    };
    let Some(tls) =
        super::tls::handshake(&acceptor, socket, pre_auth_deadline, &peer, &security).await
    else {
        return;
    };
    let principal = super::tls::client_principal(&tls);
    if ctx.log_connections && principal.method == "SSL" {
        info!(
            "Kafka client {peer} authenticated by TLS client certificate as {}",
            principal.principal
        );
    }
    let auth = ConnectionAuth::new(
        peer,
        security,
        principal,
        ctx.lookup,
        ctx.log_connections,
        throttle,
    );
    handle_connection(tls, activity, ctx.dispatcher, auth, pre_auth_deadline).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_try_admit_connection_enforces_cap_and_frees_on_drop() {
        // Admission must reject once the concurrent-connection cap is reached, and free a
        // slot when a permit (held for a connection's lifetime) is dropped. This is the policy the
        // accept loop relies on to bound fd/memory usage.
        let limiter = Arc::new(Semaphore::new(2));
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
}
