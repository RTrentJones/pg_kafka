//! Request decoding: the header of a frame, then the body through the API registry.
//!
//! Runs on the network thread, so it logs through `tracing`, never pgrx.

use bytes::{Bytes, BytesMut};
use kafka_protocol::protocol::decode_request_header_from_buffer;
use tokio::sync::mpsc::UnboundedSender;
use tracing::{debug, warn};

use super::decode_budget::{
    DecodePool, NotRun, DECODED_REQUESTS, INLINE_REQUEST_DECODE_BYTES, LARGE_DECODES,
    MAX_DECODE_WAIT, MAX_REQUEST_DECODE_BYTES,
};
use super::decode_meter::{metered, MeteredBuf};
use crate::kafka::api::{self, Decoded, ResponseBody};
use crate::kafka::constants::ERROR_UNSUPPORTED_VERSION;
use crate::kafka::error::{KafkaError, Result};
use crate::kafka::messages::{KafkaRequest, KafkaResponse};

/// Parse one request frame (the length prefix already removed by the frame codec):
/// `[RequestHeader][RequestBody]`, its decode limited to [`MAX_REQUEST_DECODE_BYTES`]
/// (`super::decode_meter`), on the calling thread. [`decode_request`] is the network
/// thread's way in: it decodes small requests inline and large ones off the I/O threads,
/// and charges what the request holds.
///
/// Returns `Ok(Some(request))` for a request the DB thread must run once
/// [`finish_request`] has finished it, `Ok(None)` when the
/// request was already answered here (an error response, or deliberately nothing for an
/// acks=0 Produce), and `Err` when no response can be shaped and the connection must close
/// (the header cannot be parsed, the version has no response format, or the request
/// decodes to more than the limit).
///
/// This is the untrusted-input boundary. The `kafka-protocol` header and body decoders index
/// without bounds checks and panic on truncated or garbage frames, so the whole parse runs
/// under `catch_unwind` (the network thread is built with `panic = "unwind"`) and a
/// malformed frame becomes a CorruptMessage error. `catch_unwind` cannot contain an
/// allocation abort: the patched kafka-protocol in Cargo.toml `[patch.crates-io]` caps the
/// pre-allocation a crafted array length can ask for
/// (`test_fetch_huge_topics_array_does_not_oom_abort`, the `fuzz_parse_request_*` property
/// tests), and the decode meter stops a decode of many real entries at its limit.
pub fn parse_request(
    frame: BytesMut,
    response_tx: UnboundedSender<KafkaResponse>,
) -> Result<Option<KafkaRequest>> {
    match parse_within(frame.freeze(), response_tx, MAX_REQUEST_DECODE_BYTES) {
        Attempt::Parsed(result, _held) => result,
        Attempt::OverLimit => Err(over_limit()),
    }
}

/// Decode one request frame on the network thread, with the results of [`parse_request`].
/// A request the DB thread must run carries the share of [`DECODED_REQUESTS`] it holds
/// ([`KafkaRequest::decode_charge`]), given back when the request is dropped once it ran.
///
/// The decode runs on the I/O thread under [`INLINE_REQUEST_DECODE_BYTES`]; one that
/// needs more is stopped there and runs again under a large-decode slot, off the I/O
/// threads, under [`MAX_REQUEST_DECODE_BYTES`]. Past that the connection closes, as Kafka
/// closes it for a frame over `socket.request.max.bytes`. A decoded request the budget has
/// no room for is dropped and decodes again once there is, waiting up to
/// [`MAX_DECODE_WAIT`] without holding a thread (this connection reads nothing more
/// meanwhile); past that the connection closes.
pub async fn decode_request(
    frame: BytesMut,
    response_tx: UnboundedSender<KafkaResponse>,
) -> Result<Option<KafkaRequest>> {
    decode_request_in(frame, response_tx, &DECODED_REQUESTS, MAX_DECODE_WAIT).await
}

/// [`decode_request`], charging `pool` and waiting for it up to `wait`.
async fn decode_request_in(
    frame: BytesMut,
    response_tx: UnboundedSender<KafkaResponse>,
    pool: &'static DecodePool,
    wait: std::time::Duration,
) -> Result<Option<KafkaRequest>> {
    let frame = frame.freeze();
    let deadline = tokio::time::Instant::now() + wait;
    let mut inline = true;
    loop {
        let attempt = if inline {
            parse_within(
                frame.clone(),
                response_tx.clone(),
                INLINE_REQUEST_DECODE_BYTES,
            )
        } else {
            let (frame, tx) = (frame.clone(), response_tx.clone());
            match LARGE_DECODES
                .run(deadline, move || {
                    parse_within(frame, tx, MAX_REQUEST_DECODE_BYTES)
                })
                .await
            {
                Ok(attempt) => attempt,
                Err(NotRun::TimedOut) => {
                    return Err(KafkaError::encoding(
                        "no large-decode slot came free for a large request in time",
                    ))
                }
                Err(NotRun::Panicked) => {
                    return Err(KafkaError::encoding("request decoder panicked"))
                }
            }
        };
        let (request, held) = match attempt {
            Attempt::OverLimit if inline => {
                debug!("Request decodes to more than {INLINE_REQUEST_DECODE_BYTES} bytes: decoding it again under a large-decode slot");
                inline = false;
                continue;
            }
            Attempt::OverLimit => return Err(over_limit()),
            Attempt::Parsed(Ok(Some(request)), held) => (request, held),
            Attempt::Parsed(other, _) => return other.map(|_| None),
        };
        if let Some(charge) = pool.charge_request(held) {
            let mut request = request;
            request.decode_charge = charge;
            return finish_request(request).await;
        }
        // No room for what the request holds: free it, and decode it again once there is.
        drop(request);
        if !pool
            .wait_for(DecodePool::request_room(held), deadline)
            .await
        {
            warn!(
                "Closing connection: no room for a decoded request of {held} bytes within {wait:?}"
            );
            return Err(KafkaError::encoding(format!(
                "no room for a decoded request of {held} bytes within {wait:?}"
            )));
        }
    }
}

/// The error a request whose decode passes its limit closes the connection with.
fn over_limit() -> KafkaError {
    warn!("Closing connection: request decodes to more than {MAX_REQUEST_DECODE_BYTES} bytes");
    KafkaError::encoding(format!(
        "request decodes to more than {MAX_REQUEST_DECODE_BYTES} bytes"
    ))
}

/// What one decode of a frame under a limit came to.
#[expect(
    clippy::large_enum_variant,
    reason = "one per decode attempt, moved once; boxing would cost an allocation per request"
)]
enum Attempt {
    /// The decode stayed within its limit: [`parse_request`]'s result, and the bytes the
    /// decoded request holds.
    Parsed(Result<Option<KafkaRequest>>, usize),
    /// The decode passed its limit: nothing was answered, and what it decoded is freed.
    OverLimit,
}

fn parse_within(
    frame: Bytes,
    response_tx: UnboundedSender<KafkaResponse>,
    limit: usize,
) -> Attempt {
    let (parsed, metering) = metered(limit, || {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            parse_request_inner(frame, response_tx)
        }))
        .unwrap_or_else(|_panic| {
            warn!("Panic while parsing request (malformed or truncated frame)");
            Err(KafkaError::CorruptMessage {
                message: "malformed request".into(),
            })
        })
    });
    match parsed {
        // `api::decode_body` reports a decode past its limit before it answers anything;
        // what it allocates after that check (an error response, the request envelope) is
        // small, and the request is kept or answered as decoded.
        Ok(Decoded::OverLimit) => Attempt::OverLimit,
        parsed => Attempt::Parsed(parsed.and_then(decoded_to_result), metering.held),
    }
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
        Decoded::OverLimit => Err(over_limit()),
    }
}

fn parse_request_inner(
    mut frame: Bytes,
    response_tx: UnboundedSender<KafkaResponse>,
) -> Result<Decoded> {
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

    match api::decode_body(&mut MeteredBuf::new(frame), header, response_tx) {
        Ok(decoded) => Ok(decoded),
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
            Ok(Decoded::Answered)
        }
    }
}

#[cfg(test)]
mod tests;
