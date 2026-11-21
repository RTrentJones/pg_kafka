//! SaslHandshake (17) and SaslAuthenticate (36): answered on the network thread.
//!
//! These two only describe the APIs ([`ApiSchema`]); the connection's SASL state machine
//! (`crate::kafka::auth::session`, driven by `listener::sasl`) decodes and answers them before
//! anything reaches the DB thread. See the `network_thread` section of the registry.

use kafka_protocol::messages::sasl_authenticate_request::SaslAuthenticateRequest;
use kafka_protocol::messages::sasl_authenticate_response::SaslAuthenticateResponse;
use kafka_protocol::messages::sasl_handshake_request::SaslHandshakeRequest;
use kafka_protocol::messages::sasl_handshake_response::SaslHandshakeResponse;

use super::ApiSchema;

/// SaslHandshake: pick the SASL mechanism.
pub struct SaslHandshakeApi;

impl ApiSchema for SaslHandshakeApi {
    type Wire = SaslHandshakeRequest;
    type Echo = ();
    const NAME: &'static str = "SaslHandshake";
    // v0 is advertised because librdkafka requires it before it enables SASL, but a v0
    // handshake (SASL tokens without SaslAuthenticate framing) is answered
    // UNSUPPORTED_VERSION and the connection closed; see auth::session.
    const MIN_VERSION: i16 = 0;
    const MAX_VERSION: i16 = 1;

    fn error_response(_echo: Option<&()>, _version: i16, error_code: i16) -> SaslHandshakeResponse {
        SaslHandshakeResponse::default().with_error_code(error_code)
    }

    fn response_has_error(response: &SaslHandshakeResponse) -> bool {
        response.error_code != 0
    }
}

/// SaslAuthenticate: carry SASL tokens (KIP-152).
pub struct SaslAuthenticateApi;

impl ApiSchema for SaslAuthenticateApi {
    type Wire = SaslAuthenticateRequest;
    type Echo = ();
    const NAME: &'static str = "SaslAuthenticate";
    const MIN_VERSION: i16 = 0;
    const MAX_VERSION: i16 = 2;

    fn error_response(
        _echo: Option<&()>,
        _version: i16,
        error_code: i16,
    ) -> SaslAuthenticateResponse {
        SaslAuthenticateResponse::default().with_error_code(error_code)
    }

    fn response_has_error(response: &SaslAuthenticateResponse) -> bool {
        response.error_code != 0
    }
}
