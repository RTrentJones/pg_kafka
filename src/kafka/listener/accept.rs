//! The accept loop of each listener: connection caps, TCP options, the TLS handshake.

use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, info, warn};

use super::connection::{handle_connection, PRE_AUTH_TIMEOUT};
use super::is_peer_disconnect;
use super::sasl::ConnectionAuth;
use crate::kafka::auth::{ChannelLookup, ConnectionPrincipal, SaslMechanism, SecurityProtocol};
use crate::kafka::conn_limits::{IpPermit, IpTracker, NetworkLimits};
use crate::kafka::pending_fetches::PendingFetchRegistry;

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

/// Shared state every accepted connection needs.
#[derive(Clone)]
pub(super) struct AcceptContext {
    pub(super) request_lanes: crate::kafka::RequestLanes,
    pub(super) registry: Arc<PendingFetchRegistry>,
    /// wave3 net: per-IP connection counts and lookup buckets, shared by all listeners.
    pub(super) ip_tracker: Arc<IpTracker>,
    pub(super) lookup: ChannelLookup,
    pub(super) enable_long_polling: bool,
    pub(super) fetch_poll_interval_ms: i32,
    pub(super) log_connections: bool,
}

/// Accept connections on one listener until aborted.
pub(super) async fn accept_loop(
    listener: TcpListener,
    security: Arc<ListenerSecurity>,
    ctx: AcceptContext,
) {
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
}
