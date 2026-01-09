// RecordBatch parsing module
//
// Handles parsing of Kafka RecordBatch format into individual records.
// Supports both RecordBatch v2 (Kafka 0.11+) and legacy MessageSet v0/v1.
//
// ## Thread Safety
//
// This module runs in the network thread and MUST NOT use pgrx logging.
// All logging uses the `tracing` crate.

use super::super::error::{KafkaError, Result};
use super::super::messages::{ProducerMetadata, Record, RecordHeader};
use bytes::Buf;
use kafka_protocol::records::RecordBatchDecoder;
use tracing::{debug, warn};

/// Result of parsing a RecordBatch, containing both records and producer metadata
#[derive(Debug, Clone)]
pub struct ParsedRecordBatch {
    /// The parsed records from the batch
    pub records: Vec<Record>,
    /// Producer metadata for idempotent producer validation (Phase 9)
    pub producer_metadata: ProducerMetadata,
}

/// Extract producer metadata from RecordBatch v2 header bytes
///
/// RecordBatch v2 header layout (first 61 bytes):
/// - baseOffset: i64 (8 bytes) - offset 0
/// - batchLength: i32 (4 bytes) - offset 8
/// - partitionLeaderEpoch: i32 (4 bytes) - offset 12
/// - magic: i8 (1 byte, should be 2) - offset 16
/// - crc: u32 (4 bytes) - offset 17
/// - attributes: i16 (2 bytes) - offset 21
/// - lastOffsetDelta: i32 (4 bytes) - offset 23
/// - baseTimestamp: i64 (8 bytes) - offset 27
/// - maxTimestamp: i64 (8 bytes) - offset 35
/// - producerId: i64 (8 bytes) - offset 43 <-- what we need
/// - producerEpoch: i16 (2 bytes) - offset 51 <-- what we need
/// - baseSequence: i32 (4 bytes) - offset 53 <-- what we need
/// - recordsCount: i32 (4 bytes) - offset 57
fn extract_producer_metadata(batch_bytes: &bytes::Bytes) -> ProducerMetadata {
    // Need at least 57 bytes to read producer metadata
    if batch_bytes.len() < 57 {
        debug!(
            "RecordBatch too short ({} bytes) for producer metadata, using defaults",
            batch_bytes.len()
        );
        return ProducerMetadata::default();
    }

    let mut buf = batch_bytes.clone();

    // Skip to magic byte at offset 16 to verify it's RecordBatch v2
    buf.advance(16);
    let magic = buf.get_i8();

    if magic != 2 {
        debug!(
            "RecordBatch magic={}, expected 2 (v2 format), using default producer metadata",
            magic
        );
        return ProducerMetadata {
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
        };
    }

    // Skip crc (4), attributes (2), lastOffsetDelta (4), baseTimestamp (8), maxTimestamp (8)
    // That's 26 bytes from offset 17 to reach producerId at offset 43
    buf.advance(26); // Now at offset 43

    let producer_id = buf.get_i64();
    let producer_epoch = buf.get_i16();
    let base_sequence = buf.get_i32();

    debug!(
        "Extracted producer metadata: producer_id={}, producer_epoch={}, base_sequence={}",
        producer_id, producer_epoch, base_sequence
    );

    ProducerMetadata {
        producer_id,
        producer_epoch,
        base_sequence,
    }
}

/// Parse RecordBatch with producer metadata extraction (Phase 9)
///
/// Returns both records and producer metadata for idempotent producer validation.
pub fn parse_record_batch_with_metadata(batch_bytes: &bytes::Bytes) -> Result<ParsedRecordBatch> {
    // Extract producer metadata first (before decoding consumes the buffer)
    let producer_metadata = extract_producer_metadata(batch_bytes);

    // Parse records using existing function
    let records = parse_record_batch(batch_bytes)?;

    Ok(ParsedRecordBatch {
        records,
        producer_metadata,
    })
}

/// Parse RecordBatch into individual Records
///
/// RecordBatch format is complex (see Kafka protocol spec):
/// - Base offset (i64)
/// - Batch length (i32)
/// - Partition leader epoch (i32)
/// - Magic byte (i8 = 2 for v0.11+)
/// - CRC32C (u32)
/// - Attributes (i16) - compression, timestamp type
/// - Last offset delta (i32)
/// - Base timestamp (i64)
/// - Max timestamp (i64)
/// - Producer ID, epoch, base sequence (for idempotence)
/// - Records count (i32)
/// - Records (varint-encoded)
///
/// The kafka-protocol crate handles all this complexity for us.
pub fn parse_record_batch(batch_bytes: &bytes::Bytes) -> Result<Vec<Record>> {
    // Handle empty batch
    if batch_bytes.is_empty() {
        return Ok(Vec::new());
    }

    debug!("parse_record_batch: received {} bytes", batch_bytes.len());
    debug!(
        "First 20 bytes: {:?}",
        &batch_bytes[..batch_bytes.len().min(20)]
    );

    let mut batch_buf = batch_bytes.clone();

    // Try RecordBatch v2 format first (Kafka 0.11+)
    // If that fails, fall back to MessageSet v0/v1 (legacy format)
    match RecordBatchDecoder::decode(&mut batch_buf) {
        Ok(record_set) => {
            // RecordSet contains a Vec<Record> in the .records field
            let mut records = Vec::new();
            for record in record_set.records {
                let key = record.key.map(|k: bytes::Bytes| k.to_vec());
                let value = record.value.map(|v: bytes::Bytes| v.to_vec());
                let timestamp = Some(record.timestamp);

                // Parse headers from IndexMap to Vec
                // Headers are IndexMap<StrBytes, Option<Bytes>>
                let headers: Vec<RecordHeader> = record
                    .headers
                    .into_iter()
                    .map(|(k, v)| RecordHeader {
                        key: k.to_string(),
                        value: v.map(|b: bytes::Bytes| b.to_vec()).unwrap_or_default(),
                    })
                    .collect();

                records.push(Record {
                    key,
                    value,
                    headers,
                    timestamp,
                });
            }
            Ok(records)
        }
        Err(e) => {
            // RecordBatch v2 decode failed, try MessageSet v0/v1 (legacy format)
            debug!(
                "RecordBatch decode failed ({}), trying MessageSet v0/v1 format",
                e
            );

            parse_message_set_legacy(batch_bytes, e)
        }
    }
}

/// Parse legacy MessageSet v0/v1 format
///
/// MessageSet format (v0/v1):
/// Repeated: [Offset: 8 bytes][MessageSize: 4 bytes][Message]
/// Message: [CRC: 4 bytes][Magic: 1 byte][Attributes: 1 byte][Key][Value]
/// Key/Value: [Length: 4 bytes (i32, -1=null)][Data]
fn parse_message_set_legacy(
    batch_bytes: &bytes::Bytes,
    original_error: impl std::fmt::Display,
) -> Result<Vec<Record>> {
    let mut buf = batch_bytes.clone();
    let mut records = Vec::new();

    while buf.remaining() >= 12 {
        // Minimum: 8 (offset) + 4 (size)
        let _offset = buf.get_i64(); // Ignore offset (we assign our own)
        let message_size = buf.get_i32();

        if message_size < 0 || buf.remaining() < message_size as usize {
            warn!("Invalid message size in MessageSet: {}", message_size);
            break;
        }

        // Parse the Message struct
        let _crc = buf.get_u32(); // Skip CRC validation for now
        let _magic = buf.get_i8();
        let _attributes = buf.get_i8(); // Compression, timestamp type

        // Parse key (nullable bytes)
        let key = if buf.remaining() < 4 {
            None
        } else {
            let key_len = buf.get_i32();
            if key_len < 0 {
                None
            } else if buf.remaining() >= key_len as usize {
                let mut key_bytes = vec![0u8; key_len as usize];
                buf.copy_to_slice(&mut key_bytes);
                Some(key_bytes)
            } else {
                warn!("Insufficient bytes for key");
                break;
            }
        };

        // Parse value (nullable bytes)
        let value = if buf.remaining() < 4 {
            None
        } else {
            let value_len = buf.get_i32();
            if value_len < 0 {
                None
            } else if buf.remaining() >= value_len as usize {
                let mut value_bytes = vec![0u8; value_len as usize];
                buf.copy_to_slice(&mut value_bytes);
                Some(value_bytes)
            } else {
                warn!("Insufficient bytes for value");
                break;
            }
        };

        records.push(Record {
            key,
            value,
            headers: Vec::new(), // MessageSet v0/v1 doesn't support headers
            timestamp: None,     // v0 doesn't have timestamps, v1 does but we skip for simplicity
        });
    }

    if records.is_empty() {
        Err(KafkaError::Encoding(format!(
            "Failed to parse as both RecordBatch and MessageSet: {}",
            original_error
        )))
    } else {
        debug!(
            "Successfully parsed {} records from MessageSet format",
            records.len()
        );
        Ok(records)
    }
}
