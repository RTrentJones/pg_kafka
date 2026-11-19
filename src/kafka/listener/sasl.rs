//! Per-connection authentication state and the SASL frames, answered on the network
//! thread.

use bytes::BytesMut;
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;
use tracing::{info, warn};

use super::accept::ListenerSecurity;
use crate::kafka::auth::scram::ScramMechanism;
use crate::kafka::auth::session::{
    CredentialLookup, LookupResult, SaslOutcome, AUTH_FAILURE_DELAY,
};
use crate::kafka::auth::{ChannelLookup, ConnectionPrincipal, SaslSession};
use crate::kafka::conn_limits::{IpTracker, LookupAdmission};
use crate::kafka::messages::KafkaResponse;

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

/// Answer one SaslHandshake / SaslAuthenticate request. Returns false when the
/// connection must close (after its response, if any, is written).
pub(super) async fn handle_sasl_frame(
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
