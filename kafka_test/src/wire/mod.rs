//! The Kafka wire protocol, spoken directly for tests that need what rdkafka hides
//! (exact request versions, per-partition error codes, pipelining, malformed frames).
//!
//! The free functions here frame, send, read and decode single requests on a
//! [`TcpStream`]; [`Wire`] is a connection with typed calls; [`records`] builds and
//! parses record batches; [`requests`] holds request builders and one-call requests
//! shared by several categories.

mod connection;
pub mod records;
pub mod requests;

pub use connection::Wire;

use crate::common::DynResult as Result;
use bytes::{BufMut, BytesMut};
use kafka_protocol::messages::{RequestHeader, ResponseHeader};
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, Request, StrBytes};
use std::io::ErrorKind;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// The client id every framed request carries.
pub const CLIENT_ID: &str = "kafka-test";

/// The size prefix of `payload`, as Kafka frames it.
fn size_prefix(payload: &[u8]) -> i32 {
    i32::try_from(payload.len()).expect("a test request is far below 2 GiB")
}

/// `request` encoded at `version` with its header, size prefix included.
pub fn request_frame<R: Request>(version: i16, correlation_id: i32, request: &R) -> Vec<u8> {
    let header = RequestHeader::default()
        .with_request_api_key(R::KEY)
        .with_request_api_version(version)
        .with_correlation_id(correlation_id)
        .with_client_id(Some(StrBytes::from_static_str(CLIENT_ID)));
    let mut payload = BytesMut::new();
    header
        .encode(&mut payload, R::header_version(version))
        .expect("the request header encodes at this version");
    request
        .encode(&mut payload, version)
        .expect("the request encodes at this version");
    let mut frame = Vec::with_capacity(4 + payload.len());
    frame.put_i32(size_prefix(&payload));
    frame.extend_from_slice(&payload);
    frame
}

/// A frame of `api_key` v`version` with a v1 (non-flexible) header, a null client id
/// and `body` as given, for requests the protocol crate cannot (or must not) encode.
pub fn raw_frame(api_key: i16, version: i16, correlation_id: i32, body: &[u8]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(10 + body.len());
    payload.put_i16(api_key);
    payload.put_i16(version);
    payload.put_i32(correlation_id);
    payload.put_i16(-1);
    payload.extend_from_slice(body);
    let mut frame = Vec::with_capacity(4 + payload.len());
    frame.put_i32(size_prefix(&payload));
    frame.extend_from_slice(&payload);
    frame
}

/// Read one frame (without its size prefix); `None` when the broker closed the
/// connection (EOF or reset) before or inside it.
pub async fn read_frame(stream: &mut TcpStream) -> Result<Option<Vec<u8>>> {
    let closed = |e: &std::io::Error| {
        matches!(
            e.kind(),
            ErrorKind::UnexpectedEof | ErrorKind::ConnectionReset
        )
    };
    let mut size = [0u8; 4];
    match stream.read_exact(&mut size).await {
        Ok(_) => {}
        Err(e) if closed(&e) => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    let len = usize::try_from(i32::from_be_bytes(size))?;
    let mut frame = vec![0u8; len];
    match stream.read_exact(&mut frame).await {
        Ok(_) => Ok(Some(frame)),
        Err(e) if closed(&e) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// [`read_frame`], failing when neither a frame nor a close arrives `within`.
pub async fn read_frame_within(
    stream: &mut TcpStream,
    within: Duration,
) -> Result<Option<Vec<u8>>> {
    tokio::time::timeout(within, read_frame(stream))
        .await
        .map_err(|_| format!("no frame and no close within {within:?}"))?
}

/// The correlation id of a response frame (its first 4 bytes in every header version).
pub fn correlation_of(frame: &[u8]) -> i32 {
    i32::from_be_bytes([frame[0], frame[1], frame[2], frame[3]])
}

/// Decode a response frame of `R` at `version`: its correlation id and body.
pub fn decode_response<R: Request>(version: i16, frame: &[u8]) -> Result<(i32, R::Response)> {
    let mut buf = BytesMut::from(frame);
    let header = ResponseHeader::decode(
        &mut buf,
        <R::Response as HeaderVersion>::header_version(version),
    )?;
    Ok((
        header.correlation_id,
        R::Response::decode(&mut buf, version)?,
    ))
}

/// Write one request without waiting for its response.
pub async fn send<R: Request>(
    stream: &mut TcpStream,
    version: i16,
    correlation_id: i32,
    request: &R,
) -> Result<()> {
    stream
        .write_all(&request_frame(version, correlation_id, request))
        .await?;
    stream.flush().await?;
    Ok(())
}

/// Read the next response frame and decode it as `R` at `version`, with its
/// correlation id. A closed connection is an error.
pub async fn read_response<R: Request>(
    stream: &mut TcpStream,
    version: i16,
) -> Result<(i32, R::Response)> {
    let frame = read_frame(stream)
        .await?
        .ok_or("the broker closed the connection instead of answering")?;
    decode_response::<R>(version, &frame)
}

/// Read the next response frame as its correlation id and the bytes after it. A
/// closed connection is an error.
pub async fn read_raw_response(stream: &mut TcpStream) -> Result<(i32, Vec<u8>)> {
    let mut frame = read_frame(stream)
        .await?
        .ok_or("the broker closed the connection instead of answering")?;
    if frame.len() < 4 {
        return Err("response too short".into());
    }
    let body = frame.split_off(4);
    Ok((correlation_of(&frame), body))
}

/// Read and decode the response to a request sent with [`send`], checking its
/// correlation id. A closed connection is an error.
pub async fn recv<R: Request>(
    stream: &mut TcpStream,
    version: i16,
    correlation_id: i32,
) -> Result<R::Response> {
    let (got, response) = read_response::<R>(stream, version).await?;
    if got != correlation_id {
        return Err(format!("correlation id mismatch: sent {correlation_id}, got {got}").into());
    }
    Ok(response)
}

/// Send one request at `version` and decode its response.
pub async fn roundtrip<R: Request>(
    stream: &mut TcpStream,
    version: i16,
    correlation_id: i32,
    request: &R,
) -> Result<R::Response> {
    send(stream, version, correlation_id, request).await?;
    recv::<R>(stream, version, correlation_id).await
}
