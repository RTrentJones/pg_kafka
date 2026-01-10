//! Manual Kafka Protocol Encoding for TRUE Deduplication Testing
//!
//! This module provides low-level Kafka protocol encoding functions for testing
//! idempotent producer deduplication. Unlike smart clients (rdkafka), these functions
//! give us complete control over the wire protocol, allowing us to send IDENTICAL
//! requests multiple times to verify the broker correctly deduplicates them.
//!
//! ## Why Manual Encoding?
//!
//! Smart clients like rdkafka hide retry logic and make it impossible to distinguish
//! between "no retry needed" and "broker deduplicated a retry". To test TRUE
//! deduplication, we need a "dumb client" that forces duplicate transmissions.
//!
//! ## Test Strategy
//!
//! 1. Manually encode InitProducerId request → send via TCP → get producer_id
//! 2. Manually encode ProduceRequest with RecordBatch (producer_id, epoch=0, sequence=0)
//! 3. Send EXACT SAME request twice
//! 4. Verify both return error_code=0 (success)
//! 5. Verify database has exactly 1 message (not 2)

use bytes::{BufMut, Bytes, BytesMut};
use kafka_protocol::messages::init_producer_id_request::InitProducerIdRequest;
use kafka_protocol::messages::init_producer_id_response::InitProducerIdResponse;
use kafka_protocol::messages::produce_request::{
    PartitionProduceData as ProtoPartitionProduceData, ProduceRequest,
    TopicProduceData as ProtoTopicProduceData,
};
use kafka_protocol::messages::produce_response::ProduceResponse;
use kafka_protocol::messages::{ProducerId, RequestHeader, ResponseHeader, TopicName, TransactionalId};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use kafka_protocol::records::{Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

// Kafka API keys
const API_KEY_PRODUCE: i16 = 0;
const API_KEY_INIT_PRODUCER_ID: i16 = 22;

/// Encode InitProducerId request
///
/// This request allocates a new producer ID for idempotent message delivery.
///
/// # Arguments
/// * `correlation_id` - Request correlation ID (for matching responses)
/// * `transactional_id` - Optional transactional ID (None for non-transactional idempotent producer)
///
/// # Returns
/// Complete Kafka wire protocol request bytes ready to send over TCP:
/// [size: i32][RequestHeader][InitProducerIdRequest]
pub fn encode_init_producer_id_request(
    correlation_id: i32,
    transactional_id: Option<String>,
) -> Bytes {
    let mut request = InitProducerIdRequest::default();
    request.transactional_id = transactional_id.map(|s| TransactionalId::from(StrBytes::from_string(s)));
    request.transaction_timeout_ms = 60000;
    request.producer_id = ProducerId(-1); // -1 = request new ID
    request.producer_epoch = -1;

    let header = RequestHeader::default()
        .with_request_api_key(API_KEY_INIT_PRODUCER_ID)
        .with_request_api_version(4)
        .with_correlation_id(correlation_id)
        .with_client_id(Some(StrBytes::from_static_str("manual-test-client")));

    // Kafka wire format: [size: i32][header][body]
    // Header version 2 = flexible encoding with tagged fields (required for InitProducerId v2+)
    let mut body_buf = BytesMut::new();
    header.encode(&mut body_buf, 2).unwrap();
    request.encode(&mut body_buf, 4).unwrap();

    let mut buf = BytesMut::new();
    buf.put_i32(body_buf.len() as i32); // Prepend size
    buf.put(body_buf);

    buf.freeze()
}

/// Encode ProduceRequest with idempotent producer metadata
///
/// This creates a complete Produce request with RecordBatch v2 containing
/// producer metadata (producer_id, epoch, sequence) for idempotent delivery.
///
/// # Arguments
/// * `correlation_id` - Request correlation ID
/// * `topic` - Topic name
/// * `partition` - Partition index
/// * `producer_id` - Producer ID from InitProducerId
/// * `producer_epoch` - Producer epoch (usually 0)
/// * `base_sequence` - Base sequence number for this batch
/// * `records` - Record values to encode
///
/// # Returns
/// Complete Kafka wire protocol request bytes ready to send over TCP
pub fn encode_produce_request(
    correlation_id: i32,
    topic: &str,
    partition: i32,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    records: Vec<&str>,
) -> Bytes {
    // Step 1: Create Kafka Record objects with producer metadata
    let now = current_time_millis();
    let kafka_records: Vec<Record> = records
        .iter()
        .enumerate()
        .map(|(i, value)| Record {
            transactional: false,
            control: false,
            partition_leader_epoch: -1,
            producer_id,        // CRITICAL for idempotency
            producer_epoch,     // CRITICAL for idempotency
            timestamp_type: TimestampType::Creation,
            offset: i as i64,   // Offset delta within batch
            sequence: base_sequence + i as i32, // CRITICAL for idempotency
            timestamp: now,
            key: None,
            value: Some(Bytes::copy_from_slice(value.as_bytes())),
            headers: Default::default(),
        })
        .collect();

    // Step 2: Encode records using RecordBatchEncoder (handles CRC, varints, etc.)
    let mut encoded_batch = BytesMut::new();
    RecordBatchEncoder::encode(
        &mut encoded_batch,
        kafka_records.iter(),
        &RecordEncodeOptions {
            version: 2,  // RecordBatch v2 for idempotent producers
            compression: Compression::None,
        },
    )
    .expect("Failed to encode RecordBatch");

    // Step 3: Build ProduceRequest with the encoded RecordBatch
    let mut request = ProduceRequest::default();
    request.acks = 1; // Wait for broker ack
    request.timeout_ms = 5000;

    let mut topic_data = ProtoTopicProduceData::default();
    topic_data.name = TopicName::from(StrBytes::from_string(topic.to_string()));

    let mut partition_data = ProtoPartitionProduceData::default();
    partition_data.index = partition;
    partition_data.records = Some(encoded_batch.freeze());

    topic_data.partition_data.push(partition_data);
    request.topic_data.push(topic_data);

    // Step 4: Encode with header
    // Use Produce v7 (non-flexible) with header v1 for compatibility testing
    let header = RequestHeader::default()
        .with_request_api_key(API_KEY_PRODUCE)
        .with_request_api_version(7)
        .with_correlation_id(correlation_id)
        .with_client_id(Some(StrBytes::from_static_str("manual-test-client")));

    // Header version 1 = non-flexible (Produce v7 uses non-flexible format)
    let mut body_buf = BytesMut::new();
    header.encode(&mut body_buf, 1).unwrap();
    request.encode(&mut body_buf, 7).unwrap();

    let mut buf = BytesMut::new();
    buf.put_i32(body_buf.len() as i32);
    buf.put(body_buf);

    buf.freeze()
}

/// Get current time in milliseconds since Unix epoch
fn current_time_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// Send a Kafka request over TCP
///
/// The request bytes should already include the size prefix.
pub async fn send_request(stream: &mut TcpStream, request: &Bytes) -> Result<()> {
    stream.write_all(request).await?;
    stream.flush().await?;
    Ok(())
}

/// Read a Kafka response from TCP
///
/// Reads the 4-byte size prefix, then reads that many bytes for the response body.
pub async fn read_response(stream: &mut TcpStream) -> Result<Bytes> {
    // Read size prefix (4 bytes)
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).await?;
    let size = i32::from_be_bytes(size_buf);

    // Read response body
    let mut response_buf = vec![0u8; size as usize];
    stream.read_exact(&mut response_buf).await?;

    Ok(Bytes::from(response_buf))
}

/// Parse InitProducerId response and extract producer_id
pub fn parse_init_response(response: &Bytes) -> Result<i64> {
    let mut buf = BytesMut::from(response.as_ref());

    // For InitProducerId v4, response header version is 1 (flexible format)
    // Header version 1 includes: correlation_id (4 bytes) + tagged_fields
    let _header = ResponseHeader::decode(&mut buf, 1)?;

    let response = InitProducerIdResponse::decode(&mut buf, 4)?;

    if response.error_code != 0 {
        return Err(format!("InitProducerId failed: error_code={}", response.error_code).into());
    }

    Ok(response.producer_id.0) // Extract i64 from ProducerId wrapper
}

/// Parse ProduceResponse and extract error_code
pub fn parse_produce_response(response: &Bytes) -> Result<i16> {
    let mut buf = BytesMut::from(response.as_ref());

    // For Produce v7, response header version is 0 (non-flexible format)
    // Header version 0 includes: correlation_id (4 bytes) only
    let _header = ResponseHeader::decode(&mut buf, 0)?;

    let response = ProduceResponse::decode(&mut buf, 7)?;

    // Get error code from first partition response
    let error_code = response
        .responses
        .first()
        .and_then(|t| t.partition_responses.first())
        .map(|p| p.error_code)
        .unwrap_or(0);

    Ok(error_code)
}
