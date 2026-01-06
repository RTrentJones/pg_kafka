// RecordBatch parsing module
//
// Handles parsing of Kafka RecordBatch format into individual records.
// Supports both RecordBatch v2 (Kafka 0.11+) and legacy MessageSet v0/v1.

use super::super::error::{KafkaError, Result};
use super::super::messages::{Record, RecordHeader};
use bytes::Buf;
use kafka_protocol::records::RecordBatchDecoder;

// Import conditional logging macros for test isolation
use crate::{pg_log, pg_warning};

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

    pg_log!("parse_record_batch: received {} bytes", batch_bytes.len());
    pg_log!(
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
            pg_log!(
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
            pg_warning!("Invalid message size in MessageSet: {}", message_size);
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
                pg_warning!("Insufficient bytes for key");
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
                pg_warning!("Insufficient bytes for value");
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
        pg_log!(
            "Successfully parsed {} records from MessageSet format",
            records.len()
        );
        Ok(records)
    }
}
