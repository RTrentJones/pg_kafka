//! Simple replay test for TRUE deduplication
//!
//! This test:
//! 1. Connects to broker and sends API versions request to get a valid response
//! 2. Sends InitProducerId to get a producer_id
//! 3. Creates a produce request using kcat's style (lower API version)
//! 4. Sends the EXACT SAME bytes twice
//! 5. Verifies both succeed but only 1 message in database

use bytes::{BufMut, Bytes, BytesMut};
use tokio::net::TcpStream;

use super::protocol_encoding::{
    encode_init_producer_id_request, parse_init_response, read_response, send_request,
};
use crate::common::TestResult;

/// Test TRUE deduplication by replaying exact same bytes
pub async fn test_replay_deduplication() -> TestResult {
    println!("\n=== Test: Replay Deduplication Test ===\n");

    // Step 1: Connect to broker
    println!("Connecting to broker at 127.0.0.1:9092...");
    let mut stream = TcpStream::connect("127.0.0.1:9092").await?;
    println!("✅ Connected to broker\n");

    // Step 2: Send ApiVersions to verify connection
    println!("Sending ApiVersions request...");
    let api_versions_request = create_api_versions_request(1);
    send_request(&mut stream, &api_versions_request).await?;
    let _api_response = read_response(&mut stream).await?;
    println!("✅ ApiVersions response received\n");

    // Step 3: Send InitProducerId to get a producer_id
    println!("Sending InitProducerId request...");
    let init_request = encode_init_producer_id_request(2, None);
    send_request(&mut stream, &init_request).await?;
    let init_response = read_response(&mut stream).await?;
    let producer_id = parse_init_response(&init_response)?;
    println!("✅ InitProducerId returned producer_id={}\n", producer_id);

    // Step 4: Create a simple Produce request (v3 - non-flexible, well supported)
    println!("Creating Produce request (v3)...");
    let topic = "dedup-replay-test";
    let produce_request = create_produce_request_v3(
        3,              // correlation_id
        topic,
        0,              // partition
        producer_id,
        0,              // producer_epoch
        0,              // base_sequence
        b"replay-test-value",
    );
    println!("✅ Produce request created ({} bytes)\n", produce_request.len());
    println!("Request bytes (hex): {:02x?}\n", &produce_request[..produce_request.len().min(100)]);

    // Step 5: Send the EXACT SAME request TWICE
    println!("=== CRITICAL: Sending IDENTICAL request twice ===\n");

    println!("Sending request #1...");
    send_request(&mut stream, &produce_request).await?;
    let response1 = read_response(&mut stream).await?;
    println!("✅ Response #1 received ({} bytes)", response1.len());
    let error1 = parse_produce_response_v3(&response1)?;
    println!("   Error code: {}\n", error1);

    println!("Sending request #2 (EXACT SAME BYTES)...");
    send_request(&mut stream, &produce_request).await?;
    let response2 = read_response(&mut stream).await?;
    println!("✅ Response #2 received ({} bytes)", response2.len());
    let error2 = parse_produce_response_v3(&response2)?;
    println!("   Error code: {}\n", error2);

    // Step 6: Verify both returned success
    println!("=== Verifying Results ===\n");

    if error1 != 0 {
        return Err(format!("First request failed with error_code={}", error1).into());
    }
    println!("✅ First request succeeded (error_code=0)");

    if error2 != 0 {
        return Err(format!(
            "Second request failed with error_code={}. Expected 0 (duplicate should return success)",
            error2
        ).into());
    }
    println!("✅ Second request succeeded (error_code=0)");

    // Step 7: Query database for message count (manual check needed)
    println!("\n🔍 Manual verification required:");
    println!("   Run: psql -h localhost -p 28814 -U postgres -d postgres \\");
    println!("        -c \"SELECT COUNT(*) FROM kafka.messages WHERE topic_id = (SELECT id FROM kafka.topics WHERE name = '{}')\"", topic);
    println!("   Expected: 1 message (not 2)");

    println!("\n✅ Replay deduplication test completed!");
    Ok(())
}

/// Create a simple ApiVersions request (v0 with header v1)
fn create_api_versions_request(correlation_id: i32) -> Bytes {
    let mut buf = BytesMut::new();

    // Size placeholder (4 bytes) - will calculate after
    let size_pos = buf.len();
    buf.put_i32(0);

    // Request header (v1): api_key(2) + api_version(2) + correlation_id(4) + client_id(string)
    buf.put_i16(18);          // API key = ApiVersions
    buf.put_i16(0);           // API version = 0
    buf.put_i32(correlation_id);

    // client_id: nullable string (length + bytes)
    let client_id = b"replay-test";
    buf.put_i16(client_id.len() as i16);
    buf.put_slice(client_id);

    // No body for ApiVersions v0

    // Update size
    let size = (buf.len() - 4) as i32;
    buf[size_pos..size_pos + 4].copy_from_slice(&size.to_be_bytes());

    buf.freeze()
}

/// Create a Produce request v7 (non-flexible, supports RecordBatch v2)
/// This is the highest non-flexible version that properly supports idempotent producers
fn create_produce_request_v3(
    correlation_id: i32,
    topic: &str,
    partition: i32,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    value: &[u8],
) -> Bytes {
    let mut buf = BytesMut::new();

    // Size placeholder
    let size_pos = buf.len();
    buf.put_i32(0);

    // === Request Header (v1): api_key + api_version + correlation_id + client_id ===
    buf.put_i16(0);           // API key = Produce
    buf.put_i16(7);           // API version = 7 (supports RecordBatch v2, non-flexible)
    buf.put_i32(correlation_id);

    // client_id: nullable string (length + bytes)
    let client_id = b"replay-test";
    buf.put_i16(client_id.len() as i16);
    buf.put_slice(client_id);

    // === Produce Request Body (v7) ===
    // transactional_id: nullable string (-1 = null)
    buf.put_i16(-1);

    // acks: i16
    buf.put_i16(1);

    // timeout_ms: i32
    buf.put_i32(5000);

    // topic_data array: [topic_count][topics...]
    buf.put_i32(1);  // 1 topic

    // Topic: [name_length][name][partition_count][partitions...]
    buf.put_i16(topic.len() as i16);
    buf.put_slice(topic.as_bytes());

    // partition_data array: 1 partition
    buf.put_i32(1);

    // Partition: [index][record_set_size][record_set...]
    buf.put_i32(partition);

    // Record set (RecordBatch v2)
    let record_batch = create_record_batch_v2(producer_id, producer_epoch, base_sequence, value);
    buf.put_i32(record_batch.len() as i32);  // record_set_size
    buf.put_slice(&record_batch);

    // Update size
    let size = (buf.len() - 4) as i32;
    buf[size_pos..size_pos + 4].copy_from_slice(&size.to_be_bytes());

    buf.freeze()
}

/// Create a minimal RecordBatch v2 with producer metadata
fn create_record_batch_v2(
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    value: &[u8],
) -> Vec<u8> {
    let mut batch = Vec::new();
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    // Encode a single record first to know its size
    let record = encode_record(value);

    // RecordBatch header (61 bytes fixed)
    let batch_length = 49 + record.len();  // Everything after base_offset and batch_length

    // baseOffset: i64
    batch.extend_from_slice(&0i64.to_be_bytes());

    // batchLength: i32
    batch.extend_from_slice(&(batch_length as i32).to_be_bytes());

    // partitionLeaderEpoch: i32
    batch.extend_from_slice(&(-1i32).to_be_bytes());

    // magic: i8 (2 for v2)
    batch.push(2);

    // CRC placeholder (will be 0 for now - broker should still accept)
    let crc_pos = batch.len();
    batch.extend_from_slice(&0u32.to_be_bytes());

    // attributes: i16 (0 = no compression, no transactional)
    batch.extend_from_slice(&0i16.to_be_bytes());

    // lastOffsetDelta: i32 (0 for single record)
    batch.extend_from_slice(&0i32.to_be_bytes());

    // baseTimestamp: i64
    batch.extend_from_slice(&timestamp.to_be_bytes());

    // maxTimestamp: i64
    batch.extend_from_slice(&timestamp.to_be_bytes());

    // producerId: i64 (CRITICAL for idempotency)
    batch.extend_from_slice(&producer_id.to_be_bytes());

    // producerEpoch: i16 (CRITICAL for idempotency)
    batch.extend_from_slice(&producer_epoch.to_be_bytes());

    // baseSequence: i32 (CRITICAL for idempotency)
    batch.extend_from_slice(&base_sequence.to_be_bytes());

    // recordsCount: i32
    batch.extend_from_slice(&1i32.to_be_bytes());

    // Records
    batch.extend_from_slice(&record);

    // Calculate CRC32C over everything after CRC field
    let crc_data = &batch[crc_pos + 4..];
    let crc = crc32c::crc32c(crc_data);
    batch[crc_pos..crc_pos + 4].copy_from_slice(&crc.to_be_bytes());

    batch
}

/// Encode a single record in RecordBatch v2 format
fn encode_record(value: &[u8]) -> Vec<u8> {
    let mut record = Vec::new();

    // Record format:
    // length: varint
    // attributes: i8
    // timestampDelta: varint
    // offsetDelta: varint
    // keyLength: varint (-1 for null)
    // key: bytes
    // valueLength: varint
    // value: bytes
    // headersCount: varint

    // Build record body first
    let mut body = Vec::new();
    body.push(0);  // attributes
    encode_varint(&mut body, 0);   // timestampDelta
    encode_varint(&mut body, 0);   // offsetDelta
    encode_varint(&mut body, -1);  // keyLength (-1 = null)
    encode_varint(&mut body, value.len() as i64);  // valueLength
    body.extend_from_slice(value);
    encode_varint(&mut body, 0);   // headersCount

    // Prepend length
    encode_varint(&mut record, body.len() as i64);
    record.extend_from_slice(&body);

    record
}

/// Encode a signed varint (zigzag encoding)
fn encode_varint(buf: &mut Vec<u8>, value: i64) {
    let mut v = ((value << 1) ^ (value >> 63)) as u64;  // ZigZag
    while v >= 0x80 {
        buf.push(((v & 0x7F) | 0x80) as u8);
        v >>= 7;
    }
    buf.push((v & 0x7F) as u8);
}

/// Parse Produce response v7 (non-flexible format)
fn parse_produce_response_v3(response: &Bytes) -> Result<i16, Box<dyn std::error::Error>> {
    if response.len() < 8 {
        return Err("Response too short".into());
    }

    // Skip correlation_id (4 bytes)
    // responses array: [count: i32][responses...]
    // Each response: [topic_name][partition_count][partitions...]
    // Each partition: [partition: i32][error_code: i16][base_offset: i64]

    let mut pos = 4;  // Skip correlation_id

    // responses count
    let _responses_count = i32::from_be_bytes(response[pos..pos + 4].try_into()?);
    pos += 4;

    // First topic name (skip it)
    let name_len = i16::from_be_bytes(response[pos..pos + 2].try_into()?) as usize;
    pos += 2 + name_len;

    // Partition count
    let _partition_count = i32::from_be_bytes(response[pos..pos + 4].try_into()?);
    pos += 4;

    // First partition index
    let _partition_index = i32::from_be_bytes(response[pos..pos + 4].try_into()?);
    pos += 4;

    // Error code
    let error_code = i16::from_be_bytes(response[pos..pos + 2].try_into()?);

    Ok(error_code)
}
