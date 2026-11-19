//! The records of one Produce partition: RecordBatch v2 (Kafka 0.11+), or the legacy
//! MessageSet v0/v1 as a fallback.
//!
//! Runs on the network thread, so it logs through `tracing`, never pgrx.

use bytes::{Buf, Bytes};
use kafka_protocol::records::RecordBatchDecoder;
use tracing::{debug, warn};

use crate::kafka::error::{KafkaError, Result};
use crate::kafka::messages::{ProducerMetadata, Record, RecordHeader};

/// Result of parsing a RecordBatch: its records and its producer identity.
#[derive(Debug, Clone)]
pub struct ParsedRecordBatch {
    /// The batch's records.
    pub records: Vec<Record>,
    /// Producer id, epoch and base sequence, for idempotent-producer validation.
    pub producer_metadata: ProducerMetadata,
}

// RecordBatch v2 header offsets (bytes from the start of the batch):
// baseOffset i64 @0, batchLength i32 @8, partitionLeaderEpoch i32 @12, magic i8 @16,
// crc u32 @17, attributes i16 @21, lastOffsetDelta i32 @23, baseTimestamp i64 @27,
// maxTimestamp i64 @35, producerId i64 @43, producerEpoch i16 @51, baseSequence i32 @53,
// recordsCount i32 @57.
const MAGIC_OFFSET: usize = 16;
const PRODUCER_ID_OFFSET: usize = 43;
/// End of baseSequence, the last producer field.
const PRODUCER_FIELDS_END: usize = 57;
/// The magic byte of a RecordBatch v2.
const RECORD_BATCH_V2_MAGIC: i8 = 2;

/// The producer id, epoch and base sequence in a RecordBatch v2 header.
///
/// A batch too short to hold them yields the zeroed default; any other format (legacy
/// MessageSet) yields -1 for all three, a non-idempotent producer.
fn extract_producer_metadata(batch_bytes: &Bytes) -> ProducerMetadata {
    if batch_bytes.len() < PRODUCER_FIELDS_END {
        debug!(
            "RecordBatch too short ({} bytes) for producer metadata, using defaults",
            batch_bytes.len()
        );
        return ProducerMetadata::default();
    }

    let magic = i8::from_be_bytes([batch_bytes[MAGIC_OFFSET]]);
    if magic != RECORD_BATCH_V2_MAGIC {
        debug!(
            "RecordBatch magic={magic}, expected 2 (v2 format), using default producer metadata"
        );
        return ProducerMetadata {
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
        };
    }

    // In bounds: the length check above covers every field read here.
    let mut fields = &batch_bytes[PRODUCER_ID_OFFSET..PRODUCER_FIELDS_END];
    let producer_id = fields.get_i64();
    let producer_epoch = fields.get_i16();
    let base_sequence = fields.get_i32();

    debug!(
        "Extracted producer metadata: producer_id={producer_id}, producer_epoch={producer_epoch}, base_sequence={base_sequence}"
    );

    ProducerMetadata {
        producer_id,
        producer_epoch,
        base_sequence,
    }
}

/// Parse a partition's record data into its records and producer identity.
pub fn parse_record_batch_with_metadata(batch_bytes: &Bytes) -> Result<ParsedRecordBatch> {
    Ok(ParsedRecordBatch {
        producer_metadata: extract_producer_metadata(batch_bytes),
        records: parse_record_batch(batch_bytes)?,
    })
}

/// Parse a partition's record data into its records.
///
/// The data is one RecordBatch v2 (kafka-protocol decodes it, checking the CRC32C and
/// decompressing). Data that does not decode as one is tried as a legacy MessageSet v0/v1.
pub fn parse_record_batch(batch_bytes: &Bytes) -> Result<Vec<Record>> {
    if batch_bytes.is_empty() {
        return Ok(Vec::new());
    }

    debug!("parse_record_batch: received {} bytes", batch_bytes.len());
    debug!(
        "First 20 bytes: {:?}",
        &batch_bytes[..batch_bytes.len().min(20)]
    );

    let mut batch_buf = batch_bytes.clone();
    match RecordBatchDecoder::decode(&mut batch_buf) {
        // Produce v3+ (the only Produce versions pg_kafka accepts) carries exactly one
        // record batch per partition. Decoding one and ignoring the rest would drop the
        // trailing batches while acking the request; reject it as Kafka does.
        Ok(_) if batch_buf.has_remaining() => Err(KafkaError::InvalidRecord {
            message: format!(
                "partition data carries more than one record batch ({} trailing bytes)",
                batch_buf.remaining()
            ),
        }),
        Ok(record_set) => Ok(record_set
            .records
            .into_iter()
            .map(|record| Record {
                key: record.key.map(|k| k.to_vec()),
                value: record.value.map(|v| v.to_vec()),
                headers: record
                    .headers
                    .into_iter()
                    .map(|(key, value)| RecordHeader {
                        key: key.to_string(),
                        value: value.map_or_else(Vec::new, |b| b.to_vec()),
                    })
                    .collect(),
                timestamp: Some(record.timestamp),
            })
            .collect()),
        Err(e) => {
            debug!("RecordBatch decode failed ({e}), trying MessageSet v0/v1 format");
            parse_message_set_legacy(batch_bytes, e)
        }
    }
}

/// CRC-32 (IEEE / `java.util.zip.CRC32`, reflected poly 0xEDB88320): the checksum a legacy
/// MessageSet v0/v1 message carries over its body. Computed bit by bit (these batches are
/// off the hot path), so no extra dependency is needed.
fn crc32_ieee(data: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for &byte in data {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0xEDB8_8320
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

/// Offset (8) + message size (4) in front of each MessageSet entry.
const MESSAGE_SET_ENTRY_HEADER_BYTES: usize = 12;
/// Smallest legacy message: CRC (4) + magic (1) + attributes (1).
const MIN_LEGACY_MESSAGE_BYTES: usize = 6;
/// Bytes of the CRC in front of a legacy message.
const LEGACY_CRC_BYTES: usize = 4;

/// A legacy message cut short inside a key or value.
struct Truncated;

/// An i32-length-prefixed nullable byte string of a legacy message: `None` for a null
/// (negative length) or absent one.
fn read_nullable_bytes(
    msg: &mut Bytes,
    what: &str,
) -> std::result::Result<Option<Vec<u8>>, Truncated> {
    if msg.remaining() < 4 {
        return Ok(None);
    }
    let Ok(len) = usize::try_from(msg.get_i32()) else {
        return Ok(None);
    };
    if msg.remaining() < len {
        warn!("Insufficient bytes for {what}");
        return Err(Truncated);
    }
    Ok(Some(msg.copy_to_bytes(len).to_vec()))
}

/// One legacy message (CRC, magic, attributes, optional timestamp, key, value) as a
/// record, or `None` when it fails its CRC or is cut short. `msg` holds at least
/// [`MIN_LEGACY_MESSAGE_BYTES`].
fn parse_legacy_message(msg: &Bytes) -> Option<Record> {
    // The CRC32 covers everything after the CRC field (magic, attributes, key, value);
    // a mismatch means a corrupt message, so stop rather than persist garbage. (The
    // RecordBatch v2 path has its CRC32C checked by the kafka-protocol decoder.)
    let stored_crc = u32::from_be_bytes([msg[0], msg[1], msg[2], msg[3]]);
    let computed_crc = crc32_ieee(&msg[LEGACY_CRC_BYTES..]);
    if stored_crc != computed_crc {
        warn!(
            "MessageSet CRC mismatch (corrupt message): stored={stored_crc:#010x} computed={computed_crc:#010x}"
        );
        return None;
    }

    // The size floor keeps these two reads within `msg`.
    let mut msg = msg.slice(LEGACY_CRC_BYTES..);
    let magic = msg.get_i8();
    let _attributes = msg.get_i8(); // compression, timestamp type

    // Message v1 (magic=1) carries an 8-byte timestamp between attributes and the key;
    // v0 does not.
    let timestamp = if magic >= 1 {
        if msg.remaining() < 8 {
            warn!("Insufficient bytes for v1 message timestamp");
            return None;
        }
        let ts = msg.get_i64();
        (ts >= 0).then_some(ts)
    } else {
        None
    };

    let key = read_nullable_bytes(&mut msg, "key").ok()?;
    let value = read_nullable_bytes(&mut msg, "value").ok()?;

    Some(Record {
        key,
        value,
        headers: Vec::new(), // MessageSet v0/v1 has no headers
        timestamp,           // None for v0; the producer timestamp for v1 (-1 = none)
    })
}

/// Parse a legacy MessageSet v0/v1:
///
/// ```text
/// repeated: [offset i64][message size i32][message]
/// message:  [crc u32][magic i8][attributes i8][timestamp i64, v1 only][key][value]
/// key/value: [length i32, -1 = null][bytes]
/// ```
///
/// Parsing stops at the first entry that is truncated or fails its CRC; it is an error
/// only when no record parsed, reported with `original_error` (why the data is not a
/// RecordBatch v2).
fn parse_message_set_legacy(
    batch_bytes: &Bytes,
    original_error: impl std::fmt::Display,
) -> Result<Vec<Record>> {
    let mut buf = batch_bytes.clone();
    let mut records = Vec::new();

    while buf.remaining() >= MESSAGE_SET_ENTRY_HEADER_BYTES {
        let _offset = buf.get_i64(); // the broker assigns its own offsets
        let message_size = buf.get_i32();

        // The size must cover the 6-byte message header and fit what remains. Without the
        // floor, a crafted size in 0..=5 would pass and the header reads of
        // `parse_legacy_message` would underflow the buffer and panic on the untrusted
        // network path.
        let Some(message_size) = usize::try_from(message_size)
            .ok()
            .filter(|&n| (MIN_LEGACY_MESSAGE_BYTES..=buf.remaining()).contains(&n))
        else {
            warn!("Invalid message size in MessageSet: {message_size}");
            break;
        };

        // Carve out exactly this message so every read of it is bounded by its size.
        let msg = buf.copy_to_bytes(message_size);

        let Some(record) = parse_legacy_message(&msg) else {
            break;
        };
        records.push(record);
    }

    if records.is_empty() {
        Err(KafkaError::encoding(format!(
            "Failed to parse as both RecordBatch and MessageSet: {original_error}"
        )))
    } else {
        debug!(
            "Successfully parsed {} records from MessageSet format",
            records.len()
        );
        Ok(records)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn encode_batch(values: &[&str]) -> bytes::BytesMut {
        use kafka_protocol::records::{
            Compression, Record as PRecord, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
        };
        let records: Vec<PRecord> = values
            .iter()
            .enumerate()
            .map(|(i, v)| PRecord {
                transactional: false,
                control: false,
                partition_leader_epoch: 0,
                producer_id: -1,
                producer_epoch: -1,
                timestamp_type: TimestampType::Creation,
                offset: i64::try_from(i).unwrap(),
                sequence: -1,
                timestamp: 0,
                key: None,
                value: Some(Bytes::from(v.to_string())),
                headers: kafka_protocol::indexmap::IndexMap::default(),
            })
            .collect();
        let mut buf = bytes::BytesMut::new();
        RecordBatchEncoder::encode(
            &mut buf,
            records.iter(),
            &RecordEncodeOptions {
                version: 2,
                compression: Compression::None,
            },
        )
        .unwrap();
        buf
    }

    /// One MessageSet entry: offset, message size, `crc`, then `body` (magic, attributes,
    /// key, value).
    fn legacy_entry_with_crc(body: &[u8], crc: u32) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&0i64.to_be_bytes()); // offset (ignored)
        buf.extend_from_slice(&i32::try_from(4 + body.len()).unwrap().to_be_bytes()); // CRC + body
        buf.extend_from_slice(&crc.to_be_bytes());
        buf.extend_from_slice(body);
        buf
    }

    /// One MessageSet entry with a valid CRC over `body`.
    fn legacy_entry(body: &[u8]) -> Vec<u8> {
        legacy_entry_with_crc(body, crc32_ieee(body))
    }

    #[test]
    fn test_second_record_batch_is_rejected_not_dropped() {
        // Produce v3+ allows exactly one batch per partition. Ignoring a second batch while
        // acking the request would lose its records silently, so it is rejected.
        let one = encode_batch(&["a"]);
        assert_eq!(parse_record_batch(&one.clone().freeze()).unwrap().len(), 1);

        let mut two = one;
        two.extend_from_slice(&encode_batch(&["b"]));
        let err = parse_record_batch(&two.freeze()).unwrap_err();
        assert!(matches!(err, KafkaError::InvalidRecord { .. }), "{err:?}");
        assert_eq!(
            err.to_kafka_error_code(),
            crate::kafka::constants::ERROR_INVALID_RECORD
        );
    }

    // ========== Empty/Edge Case Tests ==========

    #[test]
    fn test_parse_record_batch_empty() {
        let batch_bytes = Bytes::new();
        let result = parse_record_batch(&batch_bytes);

        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_parse_record_batch_with_metadata_empty() {
        let batch_bytes = Bytes::new();
        let result = parse_record_batch_with_metadata(&batch_bytes);

        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert!(parsed.records.is_empty());
        // Default producer metadata for empty batch (uses ProducerMetadata::default())
        assert_eq!(parsed.producer_metadata.producer_id, 0);
        assert_eq!(parsed.producer_metadata.producer_epoch, 0);
        assert_eq!(parsed.producer_metadata.base_sequence, 0);
    }

    // ========== Producer Metadata Extraction Tests ==========

    #[test]
    fn test_extract_producer_metadata_too_short() {
        // Less than 57 bytes - should return defaults (ProducerMetadata::default())
        let short_bytes = Bytes::from(vec![0u8; 30]);
        let metadata = extract_producer_metadata(&short_bytes);

        // Default() returns 0 values
        assert_eq!(metadata.producer_id, 0);
        assert_eq!(metadata.producer_epoch, 0);
        assert_eq!(metadata.base_sequence, 0);
    }

    #[test]
    fn test_extract_producer_metadata_wrong_magic() {
        // Create a buffer with enough bytes but wrong magic (not 2)
        let mut buf = vec![0u8; 60];
        buf[16] = 1; // Magic byte = 1 (legacy format)

        let bytes = Bytes::from(buf);
        let metadata = extract_producer_metadata(&bytes);

        // Should return defaults for non-v2 format
        assert_eq!(metadata.producer_id, -1);
        assert_eq!(metadata.producer_epoch, -1);
        assert_eq!(metadata.base_sequence, -1);
    }

    #[test]
    fn test_extract_producer_metadata_valid_v2() {
        // Create a minimal valid RecordBatch v2 header
        let mut buf = vec![0u8; 61];

        // Magic byte at offset 16
        buf[16] = 2;

        // Producer ID at offset 43 (i64 big-endian)
        let producer_id: i64 = 12345;
        buf[43..51].copy_from_slice(&producer_id.to_be_bytes());

        // Producer epoch at offset 51 (i16 big-endian)
        let producer_epoch: i16 = 5;
        buf[51..53].copy_from_slice(&producer_epoch.to_be_bytes());

        // Base sequence at offset 53 (i32 big-endian)
        let base_sequence: i32 = 100;
        buf[53..57].copy_from_slice(&base_sequence.to_be_bytes());

        let bytes = Bytes::from(buf);
        let metadata = extract_producer_metadata(&bytes);

        assert_eq!(metadata.producer_id, 12345);
        assert_eq!(metadata.producer_epoch, 5);
        assert_eq!(metadata.base_sequence, 100);
    }

    #[test]
    fn test_extract_producer_metadata_negative_values() {
        // Test with -1 values (non-idempotent producer)
        let mut buf = vec![0u8; 61];
        buf[16] = 2; // Magic = 2

        // Producer ID = -1
        buf[43..51].copy_from_slice(&(-1i64).to_be_bytes());
        // Producer epoch = -1
        buf[51..53].copy_from_slice(&(-1i16).to_be_bytes());
        // Base sequence = -1
        buf[53..57].copy_from_slice(&(-1i32).to_be_bytes());

        let bytes = Bytes::from(buf);
        let metadata = extract_producer_metadata(&bytes);

        assert_eq!(metadata.producer_id, -1);
        assert_eq!(metadata.producer_epoch, -1);
        assert_eq!(metadata.base_sequence, -1);
    }

    // ========== MessageSet Legacy Format Tests ==========

    #[test]
    fn test_parse_message_set_legacy_invalid() {
        // Too short for any valid message
        let bytes = Bytes::from(vec![0u8; 5]);
        let result = parse_message_set_legacy(&bytes, "test error");

        assert!(result.is_err());
    }

    #[test]
    fn test_parse_message_set_legacy_negative_message_size() {
        // Create buffer with negative message size
        let mut buf = vec![0u8; 20];
        // Offset (8 bytes) - don't care
        // Message size = -1 (invalid)
        buf[8..12].copy_from_slice(&(-1i32).to_be_bytes());

        let bytes = Bytes::from(buf);
        let result = parse_message_set_legacy(&bytes, "test error");

        // Should fail because no valid records parsed
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_message_set_legacy_simple_message() {
        // Build the message body — everything the CRC covers: magic, attributes, key, value.
        let mut body = Vec::new();
        body.push(0); // magic v0
        body.push(0); // attributes
        body.extend_from_slice(&(-1i32).to_be_bytes()); // null key
        body.extend_from_slice(&5i32.to_be_bytes()); // value length
        body.extend_from_slice(b"hello");

        let buf = legacy_entry(&body);

        let bytes = Bytes::from(buf);
        let result = parse_message_set_legacy(&bytes, "test error");

        assert!(result.is_ok());
        let records = result.unwrap();
        assert_eq!(records.len(), 1);
        assert!(records[0].key.is_none());
        assert_eq!(records[0].value, Some(b"hello".to_vec()));
        assert!(records[0].headers.is_empty()); // Legacy format has no headers
    }

    #[test]
    fn test_parse_message_set_legacy_v1_skips_timestamp() {
        // Message v1 (magic=1) has an 8-byte timestamp between attributes and key. It
        // must be read as the timestamp, not mistaken for the key length.
        let mut body = Vec::new();
        body.push(1); // magic v1
        body.push(0); // attributes
        body.extend_from_slice(&1_700_000_000_000i64.to_be_bytes()); // timestamp
        body.extend_from_slice(&(-1i32).to_be_bytes()); // null key
        body.extend_from_slice(&2i32.to_be_bytes()); // value length
        body.extend_from_slice(b"hi");

        let buf = legacy_entry(&body);

        let records = parse_message_set_legacy(&Bytes::from(buf), "test error").unwrap();
        assert_eq!(records.len(), 1);
        assert!(records[0].key.is_none());
        assert_eq!(records[0].value, Some(b"hi".to_vec()));
        assert_eq!(records[0].timestamp, Some(1_700_000_000_000));
    }

    #[test]
    fn test_parse_message_set_legacy_with_key() {
        // body = magic + attributes + keylen + "key" + vallen + "value"
        let mut body = Vec::new();
        body.push(0); // magic
        body.push(0); // attributes
        body.extend_from_slice(&3i32.to_be_bytes());
        body.extend_from_slice(b"key");
        body.extend_from_slice(&5i32.to_be_bytes());
        body.extend_from_slice(b"value");

        let buf = legacy_entry(&body);

        let bytes = Bytes::from(buf);
        let result = parse_message_set_legacy(&bytes, "test error");

        assert!(result.is_ok());
        let records = result.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].key, Some(b"key".to_vec()));
        assert_eq!(records[0].value, Some(b"value".to_vec()));
    }

    #[test]
    fn test_parse_message_set_legacy_rejects_bad_crc() {
        // A message whose CRC doesn't match its body is corrupt and must not be parsed into a
        // record. Build a valid body, then store a deliberately wrong CRC.
        let mut body = Vec::new();
        body.push(0); // magic
        body.push(0); // attributes
        body.extend_from_slice(&(-1i32).to_be_bytes()); // null key
        body.extend_from_slice(&5i32.to_be_bytes());
        body.extend_from_slice(b"hello");

        let wrong_crc = crc32_ieee(&body).wrapping_add(1); // off by one → mismatch

        let buf = legacy_entry_with_crc(&body, wrong_crc);

        let bytes = Bytes::from(buf);
        // No valid record parses → the function reports an error (and never persists the message).
        assert!(parse_message_set_legacy(&bytes, "test error").is_err());
    }

    #[test]
    fn test_parse_message_set_tiny_message_size_no_panic() {
        // A message_size in 0..=5 is below the 6-byte message-header floor and must be
        // rejected, not read as a header (the header reads would underflow and panic).
        for size in 0i32..=5 {
            let mut buf = Vec::new();
            buf.extend_from_slice(&0i64.to_be_bytes()); // offset
            buf.extend_from_slice(&size.to_be_bytes()); // tiny message_size
            let bytes = Bytes::from(buf);
            let result = parse_message_set_legacy(&bytes, "test error");
            assert!(result.is_err(), "size {size} should yield Err, not panic");
        }
    }

    #[test]
    fn test_parse_message_set_size_exceeds_remaining_no_panic() {
        // A message_size larger than the remaining buffer is rejected cleanly.
        let mut buf = Vec::new();
        buf.extend_from_slice(&0i64.to_be_bytes());
        buf.extend_from_slice(&1000i32.to_be_bytes()); // claims 1000 bytes
        buf.extend_from_slice(&[0u8; 4]); // but only 4 follow
        let bytes = Bytes::from(buf);
        assert!(parse_message_set_legacy(&bytes, "test error").is_err());
    }

    proptest::proptest! {
        // Fuzz the legacy MessageSet parser (hand-rolled code) directly with
        // [offset][size][maybe-body] framing — it must never panic (returns Ok or Err).
        #[test]
        fn fuzz_parse_message_set_legacy_never_panics(
            size in proptest::num::i32::ANY,
            body in proptest::collection::vec(proptest::num::u8::ANY, 0..256),
        ) {
            let mut buf = Vec::new();
            buf.extend_from_slice(&0i64.to_be_bytes());
            buf.extend_from_slice(&size.to_be_bytes());
            buf.extend_from_slice(&body);
            let bytes = Bytes::from(buf);
            let _ = parse_message_set_legacy(&bytes, "fuzz");
        }

        // The full `parse_record_batch` pipeline, including the v2 `RecordBatchDecoder` path,
        // must never panic or abort on arbitrary bytes. That holds because the patched
        // kafka-protocol bounds the record-count `reserve` and the string/bytes
        // `vec![0; wire_len]` allocations; an unbounded one could `abort()` on an
        // attacker-controlled count, which no `catch_unwind` can contain. The exact
        // huge-`record_count` regression test lives in the fork
        // (`records.rs::huge_record_count_errors_instead_of_aborting`).
        #[test]
        fn fuzz_parse_record_batch_never_aborts(
            bytes in proptest::collection::vec(proptest::num::u8::ANY, 0..1024),
        ) {
            let _ = parse_record_batch(&Bytes::from(bytes));
        }
    }

    // ========== ParsedRecordBatch Tests ==========

    #[test]
    fn test_parsed_record_batch_clone() {
        let batch = ParsedRecordBatch {
            records: vec![Record {
                key: Some(b"test".to_vec()),
                value: Some(b"data".to_vec()),
                headers: vec![],
                timestamp: Some(1_234_567_890),
            }],
            producer_metadata: ProducerMetadata {
                producer_id: 100,
                producer_epoch: 2,
                base_sequence: 50,
            },
        };

        let cloned = batch.clone();
        assert_eq!(cloned.records.len(), 1);
        assert_eq!(cloned.producer_metadata.producer_id, 100);
    }

    #[test]
    fn test_parsed_record_batch_debug() {
        let batch = ParsedRecordBatch {
            records: vec![],
            producer_metadata: ProducerMetadata::default(),
        };

        // Should be Debug printable
        let debug_str = format!("{batch:?}");
        assert!(debug_str.contains("ParsedRecordBatch"));
    }
}
