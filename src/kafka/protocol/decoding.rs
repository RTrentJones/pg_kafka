//! Request decoding: the header of a frame, then the body through the API registry.
//!
//! Runs on the network thread, so it logs through `tracing`, never pgrx.

use bytes::BytesMut;
use kafka_protocol::protocol::decode_request_header_from_buffer;
use tokio::sync::mpsc::UnboundedSender;
use tracing::{debug, warn};

use crate::kafka::api::{self, Decoded, ResponseBody};
use crate::kafka::constants::ERROR_UNSUPPORTED_VERSION;
use crate::kafka::error::{KafkaError, Result};
use crate::kafka::messages::{KafkaRequest, KafkaResponse};

/// Parse one request frame (the length prefix already removed by the frame codec):
/// `[RequestHeader][RequestBody]`.
///
/// Returns `Ok(Some(request))` for a request the DB thread must run once
/// [`finish_request`] has finished it, `Ok(None)` when the
/// request was already answered here (an error response, or deliberately nothing for an
/// acks=0 Produce), and `Err` when no response can be shaped and the connection must close
/// (the header cannot be parsed, or the version has no response format).
///
/// This is the untrusted-input boundary. The `kafka-protocol` header and body decoders index
/// without bounds checks and panic on truncated or garbage frames, so the whole parse runs
/// under `catch_unwind` (the network thread is built with `panic = "unwind"`) and a
/// malformed frame becomes a CorruptMessage error. `catch_unwind` cannot contain an
/// allocation abort; the patched kafka-protocol in Cargo.toml `[patch.crates-io]` caps the
/// pre-allocation a crafted array length can ask for, which
/// `test_fetch_huge_topics_array_does_not_oom_abort` and the `fuzz_parse_request_*`
/// property tests guard.
pub fn parse_request(
    frame: BytesMut,
    response_tx: UnboundedSender<KafkaResponse>,
) -> Result<Option<KafkaRequest>> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        parse_request_inner(frame, response_tx)
    }))
    .unwrap_or_else(|_panic| {
        warn!("Panic while parsing request (malformed or truncated frame)");
        Err(KafkaError::CorruptMessage {
            message: "malformed request".into(),
        })
    })
}

/// Finish what [`parse_request`] left to do: decode the records of a Produce request that
/// waited for room to decode (`api::finish_decode`), with the same results as
/// [`parse_request`]. Runs on the network thread; the wait holds no thread.
pub async fn finish_request(request: KafkaRequest) -> Result<Option<KafkaRequest>> {
    decoded_to_result(api::finish_decode(request).await)
}

fn decoded_to_result(decoded: Decoded) -> Result<Option<KafkaRequest>> {
    match decoded {
        Decoded::Request(request) => Ok(Some(request)),
        Decoded::Answered => Ok(None),
        Decoded::Close(reason) => Err(KafkaError::encoding(reason)),
    }
}

fn parse_request_inner(
    mut frame: BytesMut,
    response_tx: UnboundedSender<KafkaResponse>,
) -> Result<Option<KafkaRequest>> {
    debug!("Parsing request from {} byte frame", frame.len());

    // Handles both the non-flexible and the flexible header formats.
    let wire_header = decode_request_header_from_buffer(&mut frame).map_err(|e| {
        warn!("Failed to decode RequestHeader: {e}");
        KafkaError::ProtocolCodec(e)
    })?;
    let header = api::RequestHeader {
        api_key: wire_header.request_api_key,
        api_version: wire_header.request_api_version,
        correlation_id: wire_header.correlation_id,
        client_id: wire_header.client_id.map(|s| s.to_string()),
    };
    debug!(
        "Parsed RequestHeader: api_key={}, api_version={}, correlation_id={}, client_id={:?}",
        header.api_key, header.api_version, header.correlation_id, header.client_id
    );

    match api::decode_body(&mut frame, header, response_tx) {
        Ok(decoded) => decoded_to_result(decoded),
        Err((header, response_tx)) => {
            // An API key the crate knows but pg_kafka does not serve: there is no
            // registered response schema, so answer with the generic error frame.
            warn!("Unsupported API key: {}", header.api_key);
            // The connection may already be gone; there is nobody left to tell.
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
