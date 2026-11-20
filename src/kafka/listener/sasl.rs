//! Per-connection authentication state and the SASL frames, answered on the network
//! thread (SaslHandshake / SaslAuthenticate never reach the DB thread; only their
//! credential lookups do).

use bytes::BytesMut;
use std::net::IpAddr;
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;
use tracing::{info, warn};

use super::accept::ListenerSecurity;
use crate::kafka::auth::scram::ScramMechanism;
use crate::kafka::auth::session::{
    decode_sasl_request, peek_api_key, CredentialLookup, LookupResult, SaslOutcome,
    AUTH_FAILURE_DELAY,
};
use crate::kafka::auth::{ChannelLookup, ConnectionPrincipal, SaslSession};
use crate::kafka::conn_limits::{IpTracker, LookupAdmission};
use crate::kafka::messages::KafkaResponse;
use crate::kafka::metrics::RequestTimer;

/// The per-IP rate limit on the credential lookups of one connection.
#[derive(Clone)]
pub(super) struct LookupThrottle {
    pub(super) tracker: Arc<IpTracker>,
    pub(super) ip: IpAddr,
}

/// Per-connection security state: the listener, the principal, and the SASL session.
/// Lives on the network thread for the connection's lifetime.
pub(super) struct ConnectionAuth {
    /// Peer address (for logs).
    pub(super) peer: String,
    pub(super) security: Arc<ListenerSecurity>,
    /// Who the connection is: `User:ANONYMOUS` until SASL succeeds (or the client
    /// certificate subject under mutual TLS). There are no ACLs yet; this is recorded
    /// so authorization can be added later.
    pub(super) principal: ConnectionPrincipal,
    pub(super) sasl: SaslSession,
    lookup: ChannelLookup,
    pub(super) log_connections: bool,
    /// The per-IP credential-lookup rate limit (absent in unit tests).
    lookup_throttle: Option<LookupThrottle>,
}

impl ConnectionAuth {
    pub(super) fn new(
        peer: String,
        security: Arc<ListenerSecurity>,
        principal: ConnectionPrincipal,
        lookup: ChannelLookup,
        log_connections: bool,
        lookup_throttle: Option<LookupThrottle>,
    ) -> Self {
        let sasl = SaslSession::new(
            security.protocol.uses_sasl(),
            Arc::clone(&security.sasl_mechanisms),
        );
        ConnectionAuth {
            peer,
            security,
            principal,
            sasl,
            lookup,
            log_connections,
            lookup_throttle,
        }
    }

    /// Answer one SaslHandshake / SaslAuthenticate request. Returns false when the
    /// connection must close (after its response, if any, is written).
    pub(super) async fn handle_sasl_frame(
        &mut self,
        frame: BytesMut,
        response_tx: UnboundedSender<KafkaResponse>,
    ) -> bool {
        // Counted in kafka.api_metrics like a worker request (the counters are atomics,
        // safe on this thread): latency covers the decode, the credential lookup and the
        // response; a request that closes the connection without one counts as an error.
        let api_key = peek_api_key(&frame).unwrap_or(-1);
        let mut timer = RequestTimer::start(api_key);
        let request = match decode_sasl_request(frame) {
            Ok(r) => r,
            Err(e) => {
                timer.mark_error();
                warn!("Closing connection from {}: {e}", self.peer);
                return false;
            }
        };
        let lookup = ThrottledLookup {
            inner: &self.lookup,
            throttle: self.lookup_throttle.as_ref(),
        };
        match self.sasl.handle(request, &lookup).await {
            SaslOutcome::Reply(response) => {
                timer.observe(&response);
                response_tx.send(response).is_ok()
            }
            SaslOutcome::Authenticated {
                response,
                principal,
                mechanism,
            } => {
                self.principal = ConnectionPrincipal {
                    principal,
                    method: mechanism,
                };
                if self.log_connections {
                    info!(
                        "Kafka client {} authenticated as {} on {}",
                        self.peer,
                        self.principal,
                        self.security.describe()
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
            SaslOutcome::Unavailable { reason } => {
                timer.mark_error();
                warn!(
                    "Closing connection from {} on {} without an answer: SASL credential \
                     lookup unavailable ({reason}); the client reconnects and retries",
                    self.peer,
                    self.security.describe()
                );
                false
            }
            SaslOutcome::Failed { response, reason } => {
                // Never includes the password; no username either (it may be a mistyped
                // password).
                warn!(
                    "SASL authentication failed for {} on {}: {reason}",
                    self.peer,
                    self.security.describe()
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
}

/// A credential lookup that first takes a token from the peer IP's lookup bucket: each
/// SCRAM client-first / PLAIN token costs the DB thread a SELECT, so an IP may start
/// only so many per second. Over the sustained rate the lookup waits; far over it, it
/// never happens and the pre-auth deadline closes the connection without an answer (a
/// disconnect is retriable; an authentication failure is fatal for the Java client).
struct ThrottledLookup<'a> {
    inner: &'a ChannelLookup,
    throttle: Option<&'a LookupThrottle>,
}

impl CredentialLookup for ThrottledLookup<'_> {
    fn lookup(
        &self,
        username: &str,
        mechanism: Option<ScramMechanism>,
    ) -> impl std::future::Future<Output = LookupResult> + Send {
        let admission = self
            .throttle
            .map_or(LookupAdmission::Now, |t| t.tracker.admit_lookup(t.ip));
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
