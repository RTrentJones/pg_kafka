//! The TLS side of a connection on an SSL / SASL_SSL listener: the handshake under the
//! pre-authentication deadline, and the principal of a verified client certificate.

use tokio::net::TcpStream;
use tokio_rustls::server::TlsStream;
use tracing::{debug, warn};

use super::accept::ListenerSecurity;
use super::is_peer_disconnect;
use crate::kafka::auth::ConnectionPrincipal;

/// Run the TLS handshake, or `None` (logged) when it fails or misses `deadline`.
pub(super) async fn handshake(
    acceptor: &tokio_rustls::TlsAcceptor,
    socket: TcpStream,
    deadline: tokio::time::Instant,
    peer: &str,
    security: &ListenerSecurity,
) -> Option<TlsStream<TcpStream>> {
    match tokio::time::timeout_at(deadline, acceptor.accept(socket)).await {
        Ok(Ok(tls)) => Some(tls),
        Ok(Err(e)) => {
            // A peer that connects and leaves without a ClientHello (port probe, load
            // balancer health check) is routine; a real handshake failure is not.
            if is_peer_disconnect(&e) {
                debug!("TLS peer {peer} left before the handshake: {e}");
            } else {
                warn!(
                    "TLS handshake with {peer} on {} failed: {e}",
                    security.describe()
                );
            }
            None
        }
        Err(_) => {
            warn!("TLS handshake with {peer} timed out");
            None
        }
    }
}

/// Under mutual TLS, the verified client certificate's subject is the principal
/// (Kafka's default SSL principal, `User:<RFC 2253 subject>`) until SASL replaces it;
/// otherwise the connection is anonymous.
pub(super) fn client_principal(tls: &TlsStream<TcpStream>) -> ConnectionPrincipal {
    tls.get_ref()
        .1
        .peer_certificates()
        .and_then(|certs| certs.first())
        .and_then(|cert| crate::kafka::auth::tls::certificate_subject(cert.as_ref()))
        .map_or_else(ConnectionPrincipal::anonymous, |subject| {
            ConnectionPrincipal {
                principal: format!("User:{subject}"),
                method: "SSL",
            }
        })
}
