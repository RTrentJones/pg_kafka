//! Per-connection SASL state machine (SaslHandshake v1 + SaslAuthenticate v0-2).
//!
//! It lives on the network thread, one per connection; only the credential lookup
//! crosses to the DB thread (through [`CredentialLookup`], in production the
//! [`ChannelLookup`] over the worker's credential channel). Behaviour follows Kafka's
//! `SaslServerAuthenticator`:
//!
//! * On a SASL listener, before authentication succeeds only ApiVersions,
//!   SaslHandshake and SaslAuthenticate are accepted ([`SaslSession::admits`]); any
//!   other request closes the connection without a response.
//! * SaslHandshake with a mechanism that is not enabled answers
//!   UNSUPPORTED_SASL_MECHANISM (listing the enabled ones) and closes. A SaslHandshake
//!   or SaslAuthenticate out of order answers ILLEGAL_SASL_STATE and closes.
//! * A failed authentication answers SASL_AUTHENTICATION_FAILED with a message that does
//!   not say whether the user exists, after [`AUTH_FAILURE_DELAY`], then closes: one
//!   connection buys one attempt.
//! * After authentication (and on listeners without SASL) both SASL APIs answer
//!   ILLEGAL_SASL_STATE and the connection stays open, as Kafka's request handler does.
//!
//! SaslHandshake v0 (raw, unframed tokens after the handshake) is not supported. It is
//! advertised all the same, because librdkafka only enables SASL when the broker
//! advertises SaslHandshake v0 (and uses v1 + SaslAuthenticate when v1 is there too, as
//! every client since Kafka 1.0 does); a v0 handshake is answered UNSUPPORTED_VERSION and
//! the connection closes.

use super::plain::parse_plain;
use super::scram::{
    self, generate_server_nonce, parse_client_first, ScramCredential, ScramMechanism,
    ScramServerState,
};
use super::settings::SaslMechanism;
use crate::kafka::api::{ApiSchema, ResponseBody, SaslAuthenticateApi, SaslHandshakeApi};
use crate::kafka::constants::{
    API_KEY_API_VERSIONS, API_KEY_SASL_AUTHENTICATE, API_KEY_SASL_HANDSHAKE,
    ERROR_ILLEGAL_SASL_STATE, ERROR_NONE, ERROR_SASL_AUTHENTICATION_FAILED,
    ERROR_UNSUPPORTED_SASL_MECHANISM, ERROR_UNSUPPORTED_VERSION,
};
use crate::kafka::messages::KafkaResponse;
use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::sasl_authenticate_response::SaslAuthenticateResponse;
use kafka_protocol::messages::sasl_handshake_response::SaslHandshakeResponse;
use kafka_protocol::messages::{RequestHeader, SaslAuthenticateRequest, SaslHandshakeRequest};
use kafka_protocol::protocol::{Decodable, HeaderVersion, StrBytes};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

/// `(min, max)` versions of a registered API (the API registry is the one source: the
/// same ranges are advertised in ApiVersions). SaslHandshake v0 is decoded but answered
/// UNSUPPORTED_VERSION; see the module docs.
fn versions<A: ApiSchema>() -> (i16, i16) {
    (A::MIN_VERSION, A::MAX_VERSION)
}

/// Delay before a failed authentication is answered (Kafka's
/// `connection.failed.authentication.delay.ms` default is 100 ms).
pub const AUTH_FAILURE_DELAY: Duration = Duration::from_millis(100);
/// How long a credential lookup may wait for the DB thread.
pub const LOOKUP_TIMEOUT: Duration = Duration::from_secs(10);
/// Concurrent PBKDF2 computations for SASL/PLAIN, across all connections. Bounds the
/// CPU an attacker can make the broker spend on password guesses (each costs one
/// PBKDF2 of at most `scram::MAX_ITERATIONS`), and keeps them off the async workers.
pub const MAX_CONCURRENT_PASSWORD_CHECKS: usize = 2;

static PASSWORD_CHECKS: tokio::sync::Semaphore =
    tokio::sync::Semaphore::const_new(MAX_CONCURRENT_PASSWORD_CHECKS);

/// The principal of a connection that did not authenticate.
pub const ANONYMOUS_PRINCIPAL: &str = "User:ANONYMOUS";

/// Message sent to the client on any credential failure. It does not say whether the
/// user exists.
const AUTH_FAILED_MESSAGE: &str = "Authentication failed: invalid credentials";

/// What a credential lookup found (wave3 net).
#[derive(Debug, Clone)]
pub enum StoredCredential {
    /// The user's stored credential.
    Known(ScramCredential),
    /// No such user: a fake credential to run the exchange against (salt keyed by the
    /// durable secret, parameters imitating the stored credentials; see
    /// `scram::fake_credential_with`), so the answer does not reveal that the user is
    /// unknown. Verification against it always fails.
    Unknown(ScramCredential),
}

impl StoredCredential {
    /// The real credential, if the user exists.
    pub fn known(self) -> Option<ScramCredential> {
        match self {
            StoredCredential::Known(c) => Some(c),
            StoredCredential::Unknown(_) => None,
        }
    }
}

/// Result of a credential lookup: what was found, or an error (the DB thread could not
/// answer).
pub type LookupResult = Result<StoredCredential, String>;

/// A credential lookup handed to the DB thread.
#[derive(Debug)]
pub struct CredentialRequest {
    pub username: String,
    /// The SCRAM mechanism, or `None` for PLAIN (any stored mechanism will do; the
    /// SCRAM-SHA-256 credential is preferred).
    pub mechanism: Option<ScramMechanism>,
    pub reply: tokio::sync::oneshot::Sender<LookupResult>,
}

/// Where credentials come from (the DB thread in production, a map in tests).
pub trait CredentialLookup {
    fn lookup(
        &self,
        username: &str,
        mechanism: Option<ScramMechanism>,
    ) -> impl Future<Output = LookupResult> + Send;
}

/// [`CredentialLookup`] over the worker's credential channel.
#[derive(Clone)]
pub struct ChannelLookup(pub crossbeam_channel::Sender<CredentialRequest>);

impl CredentialLookup for ChannelLookup {
    fn lookup(
        &self,
        username: &str,
        mechanism: Option<ScramMechanism>,
    ) -> impl Future<Output = LookupResult> + Send {
        let tx = self.0.clone();
        let username = username.to_string();
        async move {
            let (reply, rx) = tokio::sync::oneshot::channel();
            let mut request = CredentialRequest {
                username,
                mechanism,
                reply,
            };
            let deadline = tokio::time::Instant::now() + LOOKUP_TIMEOUT;
            // Non-blocking handoff, as for Kafka requests (SEC-4).
            loop {
                match tx.try_send(request) {
                    Ok(()) => break,
                    Err(crossbeam_channel::TrySendError::Full(r)) => {
                        if tokio::time::Instant::now() >= deadline {
                            return Err("credential lookup queue is full".into());
                        }
                        request = r;
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                    Err(crossbeam_channel::TrySendError::Disconnected(_)) => {
                        return Err("credential lookup channel closed".into())
                    }
                }
            }
            match tokio::time::timeout_at(deadline, rx).await {
                Ok(Ok(result)) => result,
                Ok(Err(_)) => Err("credential lookup dropped".into()),
                Err(_) => Err("credential lookup timed out".into()),
            }
        }
    }
}

/// A decoded SASL request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SaslRequest {
    Handshake {
        correlation_id: i32,
        api_version: i16,
        mechanism: String,
    },
    Authenticate {
        correlation_id: i32,
        api_version: i16,
        auth_bytes: Bytes,
    },
}

impl SaslRequest {
    /// An ILLEGAL_SASL_STATE answer to this request.
    fn illegal_state(&self, message: &str) -> KafkaResponse {
        match self {
            SaslRequest::Handshake {
                correlation_id,
                api_version,
                ..
            } => handshake_response(*correlation_id, *api_version, ERROR_ILLEGAL_SASL_STATE, &[]),
            SaslRequest::Authenticate {
                correlation_id,
                api_version,
                ..
            } => authenticate_response(
                *correlation_id,
                *api_version,
                ERROR_ILLEGAL_SASL_STATE,
                Some(message),
                Bytes::new(),
            ),
        }
    }
}

/// The api_key of a frame (its first two bytes), if it has one.
pub fn peek_api_key(frame: &[u8]) -> Option<i16> {
    (frame.len() >= 2).then(|| i16::from_be_bytes([frame[0], frame[1]]))
}

/// Whether an api_key is one of the two SASL APIs handled by [`SaslSession`].
pub fn is_sasl_api(api_key: i16) -> bool {
    api_key == API_KEY_SASL_HANDSHAKE || api_key == API_KEY_SASL_AUTHENTICATE
}

fn decode_inner(mut frame: BytesMut) -> Result<SaslRequest, String> {
    if frame.len() < 4 {
        return Err("truncated request header".into());
    }
    let api_key = i16::from_be_bytes([frame[0], frame[1]]);
    let api_version = i16::from_be_bytes([frame[2], frame[3]]);
    let (range, header_version) = match api_key {
        API_KEY_SASL_HANDSHAKE => (
            versions::<SaslHandshakeApi>(),
            SaslHandshakeRequest::header_version(api_version),
        ),
        API_KEY_SASL_AUTHENTICATE => (
            versions::<SaslAuthenticateApi>(),
            SaslAuthenticateRequest::header_version(api_version),
        ),
        other => return Err(format!("api_key {other} is not a SASL API")),
    };
    if api_version < range.0 || api_version > range.1 {
        return Err(format!(
            "api_key {} version {} is not supported ({}..={})",
            api_key, api_version, range.0, range.1
        ));
    }
    let header = RequestHeader::decode(&mut frame, header_version).map_err(|e| e.to_string())?;
    let correlation_id = header.correlation_id;
    if api_key == API_KEY_SASL_HANDSHAKE {
        let body =
            SaslHandshakeRequest::decode(&mut frame, api_version).map_err(|e| e.to_string())?;
        Ok(SaslRequest::Handshake {
            correlation_id,
            api_version,
            mechanism: body.mechanism.to_string(),
        })
    } else {
        let body =
            SaslAuthenticateRequest::decode(&mut frame, api_version).map_err(|e| e.to_string())?;
        Ok(SaslRequest::Authenticate {
            correlation_id,
            api_version,
            auth_bytes: body.auth_bytes,
        })
    }
}

/// Decode a SaslHandshake / SaslAuthenticate frame. An error (bad version, malformed
/// body) means the connection is closed, as for any request a broker cannot parse.
pub fn decode_sasl_request(frame: BytesMut) -> Result<SaslRequest, String> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| decode_inner(frame)))
        .unwrap_or_else(|_| Err("malformed SASL request".into()))
}

/// What the connection must do after a SASL request.
#[derive(Debug)]
pub enum SaslOutcome {
    /// Send the response and keep reading.
    Reply(KafkaResponse),
    /// Authentication succeeded: send the response; the connection's principal is now
    /// `principal`.
    Authenticated {
        response: KafkaResponse,
        principal: String,
        mechanism: &'static str,
    },
    /// Send the response, then close the connection.
    ReplyAndClose(KafkaResponse),
    /// Authentication failed: wait [`AUTH_FAILURE_DELAY`], send the response, close.
    /// `reason` is for the server log only (it never contains a password).
    Failed {
        response: KafkaResponse,
        reason: String,
    },
}

enum State {
    AwaitingHandshake,
    AwaitingToken(SaslMechanism),
    ScramFinal(Box<ScramServerState>),
    Authenticated,
    Failed,
}

/// One connection's SASL state.
pub struct SaslSession {
    sasl_required: bool,
    mechanisms: Arc<Vec<SaslMechanism>>,
    state: State,
}

fn handshake_response(
    correlation_id: i32,
    api_version: i16,
    error_code: i16,
    mechanisms: &[SaslMechanism],
) -> KafkaResponse {
    let mut response = SaslHandshakeResponse::default();
    response.error_code = error_code;
    response.mechanisms = mechanisms
        .iter()
        .map(|m| StrBytes::from_static_str(m.name()))
        .collect();
    KafkaResponse::new(
        correlation_id,
        api_version,
        ResponseBody::SaslHandshake(response),
    )
}

fn authenticate_response(
    correlation_id: i32,
    api_version: i16,
    error_code: i16,
    message: Option<&str>,
    auth_bytes: Bytes,
) -> KafkaResponse {
    let mut response = SaslAuthenticateResponse::default();
    response.error_code = error_code;
    response.error_message = message.map(|m| StrBytes::from_string(m.to_string()));
    response.auth_bytes = auth_bytes;
    // 0 = the session never needs re-authentication (KIP-368 is not implemented).
    response.session_lifetime_ms = 0;
    KafkaResponse::new(
        correlation_id,
        api_version,
        ResponseBody::SaslAuthenticate(response),
    )
}

impl SaslSession {
    /// `sasl_required` is true on SASL_PLAINTEXT / SASL_SSL listeners.
    pub fn new(sasl_required: bool, mechanisms: Arc<Vec<SaslMechanism>>) -> Self {
        SaslSession {
            sasl_required,
            mechanisms,
            state: State::AwaitingHandshake,
        }
    }

    pub fn is_authenticated(&self) -> bool {
        matches!(self.state, State::Authenticated)
    }

    /// Whether a (non-SASL) request with this api_key may be processed now.
    pub fn admits(&self, api_key: i16) -> bool {
        !self.sasl_required || self.is_authenticated() || api_key == API_KEY_API_VERSIONS
    }

    /// Handle one SASL request.
    pub async fn handle<L: CredentialLookup>(
        &mut self,
        request: SaslRequest,
        lookup: &L,
    ) -> SaslOutcome {
        if !self.sasl_required {
            return SaslOutcome::Reply(
                request.illegal_state("SASL authentication is not enabled on this listener"),
            );
        }
        if self.is_authenticated() {
            return SaslOutcome::Reply(request.illegal_state("already authenticated"));
        }
        match request {
            SaslRequest::Handshake {
                correlation_id,
                api_version,
                ref mechanism,
            } => {
                if !matches!(self.state, State::AwaitingHandshake) {
                    return SaslOutcome::ReplyAndClose(
                        request.illegal_state("unexpected SaslHandshake"),
                    );
                }
                if api_version < 1 {
                    // v0 would switch the connection to unframed tokens; not served.
                    self.state = State::Failed;
                    return SaslOutcome::ReplyAndClose(handshake_response(
                        correlation_id,
                        api_version,
                        ERROR_UNSUPPORTED_VERSION,
                        &self.mechanisms,
                    ));
                }
                let wanted = SaslMechanism::parse(mechanism)
                    .ok()
                    .filter(|m| self.mechanisms.contains(m));
                if let Some(m) = wanted {
                    self.state = State::AwaitingToken(m);
                    SaslOutcome::Reply(handshake_response(
                        correlation_id,
                        api_version,
                        ERROR_NONE,
                        &self.mechanisms,
                    ))
                } else {
                    self.state = State::Failed;
                    SaslOutcome::ReplyAndClose(handshake_response(
                        correlation_id,
                        api_version,
                        ERROR_UNSUPPORTED_SASL_MECHANISM,
                        &self.mechanisms,
                    ))
                }
            }
            SaslRequest::Authenticate {
                correlation_id,
                api_version,
                ref auth_bytes,
            } => {
                let state = std::mem::replace(&mut self.state, State::Failed);
                let fail = |reason: String| SaslOutcome::Failed {
                    response: authenticate_response(
                        correlation_id,
                        api_version,
                        ERROR_SASL_AUTHENTICATION_FAILED,
                        Some(AUTH_FAILED_MESSAGE),
                        Bytes::new(),
                    ),
                    reason,
                };
                match state {
                    State::AwaitingToken(SaslMechanism::Plain) => {
                        let creds = match parse_plain(auth_bytes) {
                            Ok(c) => c,
                            Err(e) => return fail(format!("PLAIN: {e}")),
                        };
                        let stored = match lookup.lookup(&creds.username, None).await {
                            Ok(s) => s,
                            Err(e) => return fail(format!("PLAIN: {e}")),
                        };
                        let username = creds.username.clone();
                        if check_password(creds, stored).await {
                            self.state = State::Authenticated;
                            SaslOutcome::Authenticated {
                                response: authenticate_response(
                                    correlation_id,
                                    api_version,
                                    ERROR_NONE,
                                    None,
                                    Bytes::new(),
                                ),
                                principal: format!("User:{username}"),
                                mechanism: "PLAIN",
                            }
                        } else {
                            fail("PLAIN: invalid credentials".into())
                        }
                    }
                    State::AwaitingToken(SaslMechanism::Scram(m)) => {
                        let first = match parse_client_first(auth_bytes) {
                            Ok(f) => f,
                            Err(e) => return fail(format!("{}: {}", m.name(), e)),
                        };
                        let stored = match lookup.lookup(&first.username, Some(m)).await {
                            Ok(s) => s,
                            Err(e) => return fail(format!("{}: {}", m.name(), e)),
                        };
                        let (known, fake) = match stored {
                            StoredCredential::Known(c) => (Some(c), None),
                            StoredCredential::Unknown(f) => (None, Some(f)),
                        };
                        let server = ScramServerState::start(
                            m,
                            first,
                            known,
                            fake,
                            &generate_server_nonce(),
                        );
                        let server_first = Bytes::copy_from_slice(server.server_first().as_bytes());
                        self.state = State::ScramFinal(Box::new(server));
                        SaslOutcome::Reply(authenticate_response(
                            correlation_id,
                            api_version,
                            ERROR_NONE,
                            None,
                            server_first,
                        ))
                    }
                    State::ScramFinal(server) => match server.verify_client_final(auth_bytes) {
                        Ok(server_final) => {
                            self.state = State::Authenticated;
                            SaslOutcome::Authenticated {
                                response: authenticate_response(
                                    correlation_id,
                                    api_version,
                                    ERROR_NONE,
                                    None,
                                    Bytes::from(server_final.into_bytes()),
                                ),
                                principal: format!("User:{}", server.username()),
                                mechanism: server.mechanism().name(),
                            }
                        }
                        Err(e) => fail(format!("SCRAM: {e}")),
                    },
                    State::AwaitingHandshake => SaslOutcome::ReplyAndClose(authenticate_response(
                        correlation_id,
                        api_version,
                        ERROR_ILLEGAL_SASL_STATE,
                        Some("SaslAuthenticate before SaslHandshake"),
                        Bytes::new(),
                    )),
                    State::Authenticated | State::Failed => {
                        SaslOutcome::ReplyAndClose(authenticate_response(
                            correlation_id,
                            api_version,
                            ERROR_ILLEGAL_SASL_STATE,
                            Some("unexpected SaslAuthenticate"),
                            Bytes::new(),
                        ))
                    }
                }
            }
        }
    }
}

/// Check a PLAIN password off the async workers, at most
/// [`MAX_CONCURRENT_PASSWORD_CHECKS`] at a time. An unknown user costs the same PBKDF2
/// as a known one (against a fake credential) and then fails.
async fn check_password(creds: super::plain::PlainCredentials, stored: StoredCredential) -> bool {
    let Ok(_permit) = PASSWORD_CHECKS.acquire().await else {
        return false;
    };
    // wave3 net: an unknown user's fake credential imitates the stored credentials'
    // iteration count, so the PBKDF2 costs (and the response time) match a real user's
    // to within the spread of the stored iteration counts.
    tokio::task::spawn_blocking(move || match stored {
        StoredCredential::Known(cred) if cred.validate().is_ok() => {
            cred.verify_password(&creds.password)
        }
        StoredCredential::Unknown(fake) => {
            let _ = fake.verify_password(&creds.password);
            false
        }
        StoredCredential::Known(_) => {
            let fake = scram::fake_credential(ScramMechanism::Sha256, &creds.username);
            let _ = fake.verify_password(&creds.password);
            false
        }
    })
    .await
    .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::auth::scram::tests::client_final_for;
    use kafka_protocol::protocol::Encodable;
    use std::collections::HashMap;
    use std::sync::Mutex;

    /// In-memory credentials; counts lookups.
    #[derive(Default)]
    struct MapLookup {
        creds: HashMap<(String, ScramMechanism), ScramCredential>,
        calls: Mutex<Vec<(String, Option<ScramMechanism>)>>,
        fail: bool,
    }

    impl MapLookup {
        fn with_user(user: &str, password: &str) -> Self {
            let mut l = MapLookup::default();
            for m in ScramMechanism::STORED {
                l.creds.insert(
                    (user.to_string(), m),
                    ScramCredential::generate(m, password.as_bytes(), 4096),
                );
            }
            l
        }
    }

    impl CredentialLookup for MapLookup {
        fn lookup(
            &self,
            username: &str,
            mechanism: Option<ScramMechanism>,
        ) -> impl Future<Output = LookupResult> + Send {
            self.calls
                .lock()
                .unwrap()
                .push((username.to_string(), mechanism));
            let result = if self.fail {
                Err("db down".to_string())
            } else {
                let m = mechanism.unwrap_or(ScramMechanism::Sha256);
                Ok(match self.creds.get(&(username.to_string(), m)) {
                    Some(c) => StoredCredential::Known(c.clone()),
                    None => StoredCredential::Unknown(scram::fake_credential(m, username)),
                })
            };
            async move { result }
        }
    }

    fn all_mechanisms() -> Arc<Vec<SaslMechanism>> {
        Arc::new(vec![
            SaslMechanism::Plain,
            SaslMechanism::Scram(ScramMechanism::Sha256),
            SaslMechanism::Scram(ScramMechanism::Sha512),
        ])
    }

    fn handshake(mechanism: &str) -> SaslRequest {
        SaslRequest::Handshake {
            correlation_id: 1,
            api_version: 1,
            mechanism: mechanism.into(),
        }
    }

    fn authenticate(bytes: &[u8]) -> SaslRequest {
        SaslRequest::Authenticate {
            correlation_id: 2,
            api_version: 2,
            auth_bytes: Bytes::copy_from_slice(bytes),
        }
    }

    fn handshake_error(outcome: &SaslOutcome) -> i16 {
        match outcome {
            SaslOutcome::Reply(KafkaResponse {
                body: ResponseBody::SaslHandshake(response),
                ..
            })
            | SaslOutcome::ReplyAndClose(KafkaResponse {
                body: ResponseBody::SaslHandshake(response),
                ..
            }) => response.error_code,
            other => panic!("not a handshake reply: {other:?}"),
        }
    }

    fn auth_bytes_of(outcome: &SaslOutcome) -> Bytes {
        match outcome {
            SaslOutcome::Reply(KafkaResponse {
                body: ResponseBody::SaslAuthenticate(response),
                ..
            })
            | SaslOutcome::Authenticated {
                response:
                    KafkaResponse {
                        body: ResponseBody::SaslAuthenticate(response),
                        ..
                    },
                ..
            } => {
                assert_eq!(response.error_code, ERROR_NONE);
                response.auth_bytes.clone()
            }
            other => panic!("not a successful authenticate reply: {other:?}"),
        }
    }

    fn assert_failed(outcome: &SaslOutcome) {
        match outcome {
            SaslOutcome::Failed {
                response:
                    KafkaResponse {
                        body: ResponseBody::SaslAuthenticate(response),
                        ..
                    },
                ..
            } => {
                assert_eq!(response.error_code, ERROR_SASL_AUTHENTICATION_FAILED);
                assert_eq!(response.error_message.as_deref(), Some(AUTH_FAILED_MESSAGE));
            }
            other => panic!("expected an authentication failure: {other:?}"),
        }
    }

    #[tokio::test]
    async fn plain_success_and_gating() {
        let lookup = MapLookup::with_user("alice", "secret");
        let mut s = SaslSession::new(true, all_mechanisms());
        assert!(s.admits(API_KEY_API_VERSIONS));
        assert!(!s.admits(0), "Produce is refused before authentication");
        assert_eq!(
            handshake_error(&s.handle(handshake("PLAIN"), &lookup).await),
            0
        );
        assert!(!s.admits(3));
        match s.handle(authenticate(b"\0alice\0secret"), &lookup).await {
            SaslOutcome::Authenticated {
                principal,
                mechanism,
                ..
            } => {
                assert_eq!(principal, "User:alice");
                assert_eq!(mechanism, "PLAIN");
            }
            other => panic!("{other:?}"),
        }
        assert!(s.is_authenticated() && s.admits(0));
        // PLAIN looks up any mechanism.
        assert_eq!(lookup.calls.lock().unwrap()[0], ("alice".to_string(), None));
        // A second handshake after authentication: ILLEGAL_SASL_STATE, connection kept.
        match s.handle(handshake("PLAIN"), &lookup).await {
            SaslOutcome::Reply(KafkaResponse {
                body: ResponseBody::SaslHandshake(response),
                ..
            }) => {
                assert_eq!(response.error_code, ERROR_ILLEGAL_SASL_STATE);
            }
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn plain_wrong_password_unknown_user_and_db_error_fail() {
        for (lookup, msg) in [
            (
                MapLookup::with_user("alice", "secret"),
                &b"\0alice\0wrong"[..],
            ),
            (
                MapLookup::with_user("alice", "secret"),
                &b"\0bob\0secret"[..],
            ),
            (MapLookup::with_user("alice", "secret"), &b"no-nuls"[..]),
            (
                MapLookup {
                    fail: true,
                    ..MapLookup::with_user("alice", "secret")
                },
                &b"\0alice\0secret"[..],
            ),
        ] {
            let mut s = SaslSession::new(true, all_mechanisms());
            s.handle(handshake("PLAIN"), &lookup).await;
            let outcome = s.handle(authenticate(msg), &lookup).await;
            assert_failed(&outcome);
            if let SaslOutcome::Failed { reason, .. } = &outcome {
                assert!(!reason.contains("secret") && !reason.contains("wrong"));
            }
            assert!(!s.is_authenticated() && !s.admits(0));
        }
    }

    async fn scram_exchange(
        m: ScramMechanism,
        lookup: &MapLookup,
        user: &str,
        password: &str,
    ) -> SaslOutcome {
        let mut s = SaslSession::new(true, all_mechanisms());
        assert_eq!(
            handshake_error(&s.handle(handshake(m.name()), lookup).await),
            0
        );
        let bare = format!("n={user},r=clientnonce123");
        let first = s
            .handle(authenticate(format!("n,,{bare}").as_bytes()), lookup)
            .await;
        let server_first = String::from_utf8(auth_bytes_of(&first).to_vec()).unwrap();
        assert!(server_first.starts_with("r=clientnonce123"));
        let (client_final, expected) = client_final_for(m, password, &bare, "n,,", &server_first);
        let outcome = s
            .handle(authenticate(client_final.as_bytes()), lookup)
            .await;
        if let SaslOutcome::Authenticated { .. } = outcome {
            assert_eq!(auth_bytes_of(&outcome), expected.as_bytes());
            assert!(s.is_authenticated());
        }
        outcome
    }

    #[tokio::test]
    async fn scram_both_mechanisms_succeed_and_reject() {
        let lookup = MapLookup::with_user("alice", "secret");
        for m in ScramMechanism::STORED {
            match scram_exchange(m, &lookup, "alice", "secret").await {
                SaslOutcome::Authenticated {
                    principal,
                    mechanism,
                    ..
                } => {
                    assert_eq!(principal, "User:alice");
                    assert_eq!(mechanism, m.name());
                }
                other => panic!("{other:?}"),
            }
            assert_failed(&scram_exchange(m, &lookup, "alice", "wrong").await);
            // Unknown user: the exchange runs to the end, then fails the same way.
            assert_failed(&scram_exchange(m, &lookup, "mallory", "secret").await);
        }
        assert!(lookup
            .calls
            .lock()
            .unwrap()
            .contains(&("alice".to_string(), Some(ScramMechanism::Sha512))));
    }

    #[tokio::test]
    async fn handshake_rules() {
        let lookup = MapLookup::default();
        // Unsupported or disabled mechanism: UNSUPPORTED_SASL_MECHANISM + close.
        let only_scram = Arc::new(vec![SaslMechanism::Scram(ScramMechanism::Sha512)]);
        for mech in ["GSSAPI", "PLAIN", "SCRAM-SHA-256"] {
            let mut s = SaslSession::new(true, only_scram.clone());
            match s.handle(handshake(mech), &lookup).await {
                SaslOutcome::ReplyAndClose(KafkaResponse {
                    body: ResponseBody::SaslHandshake(response),
                    ..
                }) => {
                    assert_eq!(response.error_code, ERROR_UNSUPPORTED_SASL_MECHANISM);
                    assert_eq!(response.mechanisms.len(), 1);
                    assert_eq!(response.mechanisms[0].as_str(), "SCRAM-SHA-512");
                }
                other => panic!("{other:?}"),
            }
        }
        // SaslHandshake v0 (unframed tokens): UNSUPPORTED_VERSION + close.
        let mut s0 = SaslSession::new(true, all_mechanisms());
        let v0 = SaslRequest::Handshake {
            correlation_id: 1,
            api_version: 0,
            mechanism: "PLAIN".into(),
        };
        assert!(matches!(
            s0.handle(v0, &lookup).await,
            SaslOutcome::ReplyAndClose(KafkaResponse { body: ResponseBody::SaslHandshake(ref response), .. })
                if response.error_code == ERROR_UNSUPPORTED_VERSION
        ));
        // Authenticate before Handshake, and a second Handshake: ILLEGAL_SASL_STATE + close.
        let mut s = SaslSession::new(true, all_mechanisms());
        assert!(matches!(
            s.handle(authenticate(b"x"), &lookup).await,
            SaslOutcome::ReplyAndClose(KafkaResponse { body: ResponseBody::SaslAuthenticate(ref response), .. })
                if response.error_code == ERROR_ILLEGAL_SASL_STATE
        ));
        let mut s = SaslSession::new(true, all_mechanisms());
        s.handle(handshake("PLAIN"), &lookup).await;
        assert!(matches!(
            s.handle(handshake("PLAIN"), &lookup).await,
            SaslOutcome::ReplyAndClose(KafkaResponse { body: ResponseBody::SaslHandshake(ref response), .. })
                if response.error_code == ERROR_ILLEGAL_SASL_STATE
        ));
        // Listener without SASL: everything admitted, SASL APIs answer ILLEGAL_SASL_STATE.
        let mut s = SaslSession::new(false, all_mechanisms());
        assert!(s.admits(0));
        assert!(matches!(
            s.handle(handshake("PLAIN"), &lookup).await,
            SaslOutcome::Reply(KafkaResponse { body: ResponseBody::SaslHandshake(ref response), .. })
                if response.error_code == ERROR_ILLEGAL_SASL_STATE
        ));
        assert!(lookup.calls.lock().unwrap().is_empty());
    }

    fn encode_request<T: Encodable + HeaderVersion>(
        api_key: i16,
        version: i16,
        body: &T,
    ) -> BytesMut {
        let mut buf = BytesMut::new();
        let mut header = RequestHeader::default();
        header.request_api_key = api_key;
        header.request_api_version = version;
        header.correlation_id = 77;
        Encodable::encode(&header, &mut buf, T::header_version(version)).unwrap();
        body.encode(&mut buf, version).unwrap();
        buf
    }

    #[test]
    fn decodes_sasl_frames() {
        let mut hs = SaslHandshakeRequest::default();
        hs.mechanism = StrBytes::from_static_str("SCRAM-SHA-256");
        let frame = encode_request(API_KEY_SASL_HANDSHAKE, 1, &hs);
        assert_eq!(peek_api_key(&frame), Some(API_KEY_SASL_HANDSHAKE));
        assert_eq!(
            decode_sasl_request(frame).unwrap(),
            SaslRequest::Handshake {
                correlation_id: 77,
                api_version: 1,
                mechanism: "SCRAM-SHA-256".into()
            }
        );
        for v in 0..=2 {
            let mut au = SaslAuthenticateRequest::default();
            au.auth_bytes = Bytes::from_static(b"n,,n=u,r=x");
            let frame = encode_request(API_KEY_SASL_AUTHENTICATE, v, &au);
            assert_eq!(
                decode_sasl_request(frame).unwrap(),
                SaslRequest::Authenticate {
                    correlation_id: 77,
                    api_version: v,
                    auth_bytes: Bytes::from_static(b"n,,n=u,r=x")
                }
            );
        }
        // v0 decodes (it is answered UNSUPPORTED_VERSION); out-of-range versions are refused.
        assert!(decode_sasl_request(encode_request(API_KEY_SASL_HANDSHAKE, 0, &hs)).is_ok());
        let mut v2 = encode_request(API_KEY_SASL_HANDSHAKE, 1, &hs);
        v2[3] = 2;
        assert!(decode_sasl_request(v2).is_err());
        let au = SaslAuthenticateRequest::default();
        let mut v3 = encode_request(API_KEY_SASL_AUTHENTICATE, 2, &au);
        v3[3] = 3; // claim version 3
        assert!(decode_sasl_request(v3).is_err());
        // Truncated body, short header, wrong api.
        let mut frame = encode_request(API_KEY_SASL_HANDSHAKE, 1, &hs);
        frame.truncate(frame.len() - 3);
        assert!(decode_sasl_request(frame).is_err());
        assert!(decode_sasl_request(BytesMut::from(&[0u8, 17][..])).is_err());
        assert!(decode_sasl_request(encode_request(18, 1, &hs)).is_err());
        assert_eq!(peek_api_key(&[1]), None);
        assert!(is_sasl_api(17) && is_sasl_api(36) && !is_sasl_api(18));
    }

    #[test]
    fn sasl_responses_encode() {
        let r = handshake_response(5, 1, 0, &all_mechanisms());
        assert!(crate::kafka::protocol::encode_response(r).is_ok());
        for v in 0..=2 {
            let r = authenticate_response(5, v, 58, Some("x"), Bytes::from_static(b"abc"));
            let bytes = crate::kafka::protocol::encode_response(r).unwrap();
            // Correlation id first.
            assert_eq!(&bytes[..4], &5i32.to_be_bytes());
        }
    }
}
