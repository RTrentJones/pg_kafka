// Request decoding module
//
// Handles parsing of Kafka requests from binary wire protocol format.
//
// ## Thread Safety
//
// This module runs in the network thread and MUST NOT use pgrx logging.
// All logging uses the `tracing` crate.

use bytes::BytesMut;
use kafka_protocol::protocol::decode_request_header_from_buffer;
use tracing::{debug, warn};

use super::super::api::{self, Decoded, ResponseBody};
use super::super::constants::ERROR_UNSUPPORTED_VERSION;
use super::super::error::{KafkaError, Result};
use super::super::messages::{KafkaRequest, KafkaResponse};

/// Parse a Kafka request from a frame
///
/// The frame has already been extracted by LengthDelimitedCodec, so we only need to parse:
/// [RequestHeader] [RequestBody]
///
/// RequestHeader contains:
/// - api_key: i16 (which API this is, e.g., 18 = ApiVersions)
/// - api_version: i16 (which version of the API)
/// - correlation_id: i32 (client-assigned ID for matching responses)
/// - client_id: nullable string (client identifier)
///
/// Returns `Ok(Some(request))` for a request the DB thread must run, `Ok(None)` when the
/// request was already answered here (an error response, or deliberately nothing for an
/// acks=0 Produce), and `Err` when no response can be shaped and the connection must close
/// (the header cannot be parsed, or the version has no response format).
///
/// The body is decoded by the API's [`api::KafkaApi`] description (see `api::decode_body`).
///
/// SEC-2: this is the untrusted-input boundary. The `kafka-protocol` crate's header and body
/// decoders index without bounds checks and PANIC on truncated/garbage frames rather than
/// returning Err. Since this runs on the network thread (panic = "unwind"), the whole parse is
/// wrapped in `catch_unwind` so a malformed frame degrades to a CorruptMessage error instead of
/// unwinding the connection. See `test_parse_{empty,truncated}_header_returns_err_no_panic`.
///
/// NOTE: catch_unwind only contains *unwinding* panics, not allocation aborts. A crafted
/// array-length prefix used to make the kafka-protocol body decoders `with_capacity` gigabytes and
/// *abort* the process (SIGABRT) — AUDIT-2026-06 SEC-9. That is fixed by capping the decoder's
/// pre-allocation in a patched kafka-protocol (see Cargo.toml `[patch.crates-io]`); the
/// `fuzz_parse_request_*` and `test_fetch_huge_topics_array_*` tests guard against regressions.
pub fn parse_request(
    frame: BytesMut,
    response_tx: tokio::sync::mpsc::UnboundedSender<KafkaResponse>,
) -> Result<Option<KafkaRequest>> {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        parse_request_inner(frame, response_tx)
    })) {
        Ok(result) => result,
        Err(_panic) => {
            warn!("Panic while parsing request (malformed or truncated frame)");
            Err(KafkaError::CorruptMessage {
                message: "malformed request".into(),
            })
        }
    }
}

fn parse_request_inner(
    frame: BytesMut,
    response_tx: tokio::sync::mpsc::UnboundedSender<KafkaResponse>,
) -> Result<Option<KafkaRequest>> {
    debug!("Parsing request from {} byte frame", frame.len());

    // Parse RequestHeader using kafka-protocol's decode function
    // This automatically handles both non-flexible and flexible header formats
    let mut payload_buf = frame;

    let header = match decode_request_header_from_buffer(&mut payload_buf) {
        Ok(h) => h,
        Err(e) => {
            warn!("Failed to decode RequestHeader: {}", e);
            return Err(KafkaError::ProtocolCodec(e));
        }
    };

    let header = api::RequestHeader {
        api_key: header.request_api_key,
        api_version: header.request_api_version,
        correlation_id: header.correlation_id,
        client_id: header.client_id.map(|s| s.to_string()),
    };

    debug!(
        "Parsed RequestHeader: api_key={}, api_version={}, correlation_id={}, client_id={:?}",
        header.api_key, header.api_version, header.correlation_id, header.client_id
    );

    match api::decode_body(&mut payload_buf, header, response_tx) {
        Ok(Decoded::Request(request)) => Ok(Some(request)),
        Ok(Decoded::Answered) => Ok(None),
        Ok(Decoded::Close(reason)) => Err(KafkaError::encoding(reason)),
        Err((header, response_tx)) => {
            // An API key the crate knows but pg_kafka does not serve: there is no
            // registered response schema, so answer with the generic error frame.
            warn!("Unsupported API key: {}", header.api_key);
            let _ = response_tx.send(KafkaResponse::new(
                header.correlation_id,
                header.api_version,
                ResponseBody::UnknownApi {
                    error_code: ERROR_UNSUPPORTED_VERSION,
                },
            ));
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests;
