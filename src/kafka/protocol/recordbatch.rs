//! The records of one Produce partition: RecordBatch v2 (Kafka 0.11+), or the legacy
//! MessageSet v0/v1 as a fallback.
//!
//! Every decoded byte is charged to the request's [`DecodeBudget`] as it is produced: the
//! decompressors here write through it and stop at its limit, so a compressed batch can
//! never expand past what the request may hold (see [`super::decode_budget`]).
//!
//! Runs on the network thread, so it logs through `tracing`, never pgrx.

use std::io::{self, Write};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use kafka_protocol::records::{Compression, RecordBatchDecoder};
use tracing::{debug, warn};

use super::decode_budget::DecodeBudget;
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

/// Parse a partition's record data into its records and producer identity, charging the
/// decoded bytes to `budget`.
pub fn parse_record_batch_with_metadata(
    batch_bytes: &Bytes,
    budget: &DecodeBudget,
) -> Result<ParsedRecordBatch> {
    Ok(ParsedRecordBatch {
        producer_metadata: extract_producer_metadata(batch_bytes),
        records: parse_record_batch(batch_bytes, budget)?,
    })
}

/// zstd frames may ask for a window of at most 2^24 bytes (16 MiB). The decoder allocates
/// the window a frame header declares before it produces any output, so without a cap a
/// few bytes could make it allocate zstd's default limit of 128 MiB. Kafka's producers use
/// windows of 2^21 (Java, level 3) or the batch size (librdkafka, which declares it).
const ZSTD_WINDOW_LOG_MAX: u32 = 24;

/// Decoded output written through a [`DecodeBudget`]: a write that would pass the budget
/// fails before anything is buffered.
struct BudgetedWriter<'a> {
    out: BytesMut,
    budget: &'a DecodeBudget,
}

impl<'a> BudgetedWriter<'a> {
    fn new(budget: &'a DecodeBudget) -> Self {
        Self {
            out: BytesMut::new(),
            budget,
        }
    }

    fn into_bytes(self) -> Bytes {
        self.out.freeze()
    }
}

/// The error a decompressor stops with at the end of its budget.
fn over_budget() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "decoded records exceed the request's limit",
    )
}

impl Write for BudgetedWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.budget.take(buf.len()).map_err(|_| over_budget())?;
        self.out.put_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// The xerial framing Kafka's Java client wraps snappy data in: magic, version, compatible
/// version, then `[length u32][raw snappy block]` repeated.
const XERIAL_SNAPPY_HEADER: &[u8; 16] = b"\x82SNAPPY\x00\x00\x00\x00\x01\x00\x00\x00\x01";

/// Snappy (xerial-framed or raw), each block's declared length charged before it is
/// allocated.
fn decompress_snappy(data: &Bytes, budget: &DecodeBudget) -> anyhow::Result<Bytes> {
    use snap::raw::{decompress_len, Decoder};
    let decode_block = |block: &[u8], out: &mut BytesMut| -> anyhow::Result<()> {
        let len = decompress_len(block)?;
        budget.take(len).map_err(|_| over_budget())?;
        let start = out.len();
        out.resize(start + len, 0);
        Decoder::new().decompress(block, &mut out[start..])?;
        Ok(())
    };
    let mut out = BytesMut::new();
    let Some(mut blocks) = data.strip_prefix(&XERIAL_SNAPPY_HEADER[..]) else {
        decode_block(data, &mut out)?;
        return Ok(out.freeze());
    };
    while !blocks.is_empty() {
        anyhow::ensure!(blocks.len() >= 4, "snappy block length cut short");
        let len = usize::try_from(blocks.get_u32())?;
        anyhow::ensure!(blocks.len() >= len, "snappy block cut short");
        let (block, rest) = blocks.split_at(len);
        decode_block(block, &mut out)?;
        blocks = rest;
    }
    Ok(out.freeze())
}

/// The decompressed records section of a RecordBatch, every byte charged to `budget`
/// (kafka-protocol's custom decompressor hook, for every codec including none).
fn decompress(
    data: &mut Bytes,
    compression: Compression,
    budget: &DecodeBudget,
) -> anyhow::Result<Bytes> {
    let data = std::mem::take(data);
    let mut out = BudgetedWriter::new(budget);
    match compression {
        Compression::None => {
            budget.take(data.len()).map_err(|_| over_budget())?;
            return Ok(data);
        }
        Compression::Gzip => {
            let mut decoder = flate2::write::GzDecoder::new(&mut out);
            decoder.write_all(&data)?;
            decoder.finish()?;
        }
        Compression::Snappy => return decompress_snappy(&data, budget),
        Compression::Lz4 => {
            let mut decoder = lz4::Decoder::new(data.reader())?;
            io::copy(&mut decoder, &mut out)?;
        }
        Compression::Zstd => {
            let mut decoder = zstd::stream::read::Decoder::new(data.reader())?;
            decoder.window_log_max(ZSTD_WINDOW_LOG_MAX)?;
            io::copy(&mut decoder, &mut out)?;
        }
    }
    Ok(out.into_bytes())
}

/// Parse a partition's record data into its records, charging the decoded bytes to
/// `budget`.
///
/// The data is one RecordBatch v2 (kafka-protocol decodes it, checking the CRC32C, through
/// `decompress`). Data that does not decode as one is tried as a legacy MessageSet v0/v1,
/// unless the decode stopped at the end of the budget.
pub fn parse_record_batch(batch_bytes: &Bytes, budget: &DecodeBudget) -> Result<Vec<Record>> {
    if batch_bytes.is_empty() {
        return Ok(Vec::new());
    }

    debug!("parse_record_batch: received {} bytes", batch_bytes.len());
    debug!(
        "First 20 bytes: {:?}",
        &batch_bytes[..batch_bytes.len().min(20)]
    );

    let mut batch_buf = batch_bytes.clone();
    let decoded = RecordBatchDecoder::decode_with_custom_compression(
        &mut batch_buf,
        Some(|data: &mut Bytes, compression| decompress(data, compression, budget)),
    );
    match decoded {
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
        Err(e) if budget.exhausted().is_some() => Err(KafkaError::CorruptMessage {
            message: format!("record batch stopped at the decode limit: {e}"),
        }),
        Err(e) => {
            debug!("RecordBatch decode failed ({e}), trying MessageSet v0/v1 format");
            // The legacy records are copied out of the batch: at most its size.
            if budget.take(batch_bytes.len()).is_err() {
                return Err(KafkaError::CorruptMessage {
                    message: "legacy message set stopped at the decode limit".into(),
                });
            }
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

    use crate::kafka::protocol::decode_budget::{Exhausted, MAX_DECODED_REQUEST_BYTES};

    fn budget() -> DecodeBudget {
        DecodeBudget::new(MAX_DECODED_REQUEST_BYTES)
    }

    /// One record batch holding a `len`-byte value of zeros, compressed with `compression`.
    fn zeros_batch(len: usize, compression: Compression) -> Bytes {
        use kafka_protocol::records::{
            Record as PRecord, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
        };
        let record = PRecord {
            transactional: false,
            control: false,
            partition_leader_epoch: 0,
            producer_id: -1,
            producer_epoch: -1,
            timestamp_type: TimestampType::Creation,
            offset: 0,
            sequence: -1,
            timestamp: 0,
            key: None,
            value: Some(Bytes::from(vec![0u8; len])),
            headers: kafka_protocol::indexmap::IndexMap::default(),
        };
        let mut buf = BytesMut::new();
        RecordBatchEncoder::encode(
            &mut buf,
            [record].iter(),
            &RecordEncodeOptions {
                version: 2,
                compression,
            },
        )
        .unwrap();
        buf.freeze()
    }

    const CODECS: [Compression; 5] = [
        Compression::None,
        Compression::Gzip,
        Compression::Snappy,
        Compression::Lz4,
        Compression::Zstd,
    ];

    #[test]
    fn every_codec_decodes_within_the_budget_and_charges_what_it_decoded() {
        for compression in CODECS {
            let batch = zeros_batch(100_000, compression);
            let budget = budget();
            let records = parse_record_batch(&batch, &budget).unwrap();
            assert_eq!(records.len(), 1, "{compression:?}");
            assert_eq!(records[0].value.as_ref().unwrap().len(), 100_000);
            // The records section: the value plus a few bytes of record framing.
            let charged = budget.charged();
            assert!(
                (100_000..100_100).contains(&charged),
                "{compression:?} charged {charged}"
            );
        }
    }

    #[test]
    fn a_decompression_bomb_stops_at_the_limit_while_decompressing() {
        // 8 MiB of zeros against a 1 MiB budget: every codec stops at the budget, having
        // charged no more than it, and the batch is not retried as a legacy message set.
        for compression in CODECS {
            let batch = zeros_batch(8 * 1024 * 1024, compression);
            if compression != Compression::None {
                assert!(batch.len() < 1024 * 1024, "{compression:?} compresses");
            }
            let budget = DecodeBudget::new(1024 * 1024);
            let err = parse_record_batch(&batch, &budget).unwrap_err();
            assert_eq!(
                budget.exhausted(),
                Some(Exhausted::Limit),
                "{compression:?}"
            );
            assert!(budget.charged() <= 1024 * 1024, "{compression:?}");
            assert!(
                err.to_string().contains("decode limit"),
                "{compression:?}: {err}"
            );
        }
    }

    #[test]
    fn the_limit_spans_the_partitions_of_a_request() {
        // Two batches that each fit, against one budget that holds only one of them.
        let batch = zeros_batch(600_000, Compression::Gzip);
        let budget = DecodeBudget::new(1_000_000);
        assert!(parse_record_batch(&batch, &budget).is_ok());
        assert!(parse_record_batch(&batch, &budget).is_err());
        assert_eq!(budget.exhausted(), Some(Exhausted::Limit));
    }

    #[test]
    fn zstd_frames_asking_for_a_huge_window_are_refused() {
        // A zstd frame whose header declares a 2^27-byte window and no content size: the
        // decoder would allocate the window up front.
        let mut encoder = zstd::stream::write::Encoder::new(Vec::new(), 19).unwrap();
        encoder.window_log(27).unwrap();
        encoder.include_contentsize(false).unwrap();
        encoder.write_all(&[7u8; 1000]).unwrap();
        let frame = encoder.finish().unwrap();
        let budget = budget();
        assert!(decompress(&mut Bytes::from(frame), Compression::Zstd, &budget).is_err());
    }

    #[test]
    fn raw_and_framed_snappy_both_decode() {
        let raw = snap::raw::Encoder::new()
            .compress_vec(b"hello snappy")
            .unwrap();
        let budget = budget();
        assert_eq!(
            &decompress(&mut Bytes::from(raw.clone()), Compression::Snappy, &budget).unwrap()[..],
            b"hello snappy"
        );
        let mut framed = XERIAL_SNAPPY_HEADER.to_vec();
        for _ in 0..2 {
            framed.extend_from_slice(&u32::try_from(raw.len()).unwrap().to_be_bytes());
            framed.extend_from_slice(&raw);
        }
        assert_eq!(
            &decompress(
                &mut Bytes::from(framed.clone()),
                Compression::Snappy,
                &budget
            )
            .unwrap()[..],
            b"hello snappyhello snappy"
        );
        framed.truncate(framed.len() - 1);
        assert!(decompress(&mut Bytes::from(framed), Compression::Snappy, &budget).is_err());
    }

    fn encode_batch(values: &[&str]) -> BytesMut {
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
        let mut buf = BytesMut::new();
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
        assert_eq!(
            parse_record_batch(&one.clone().freeze(), &budget())
                .unwrap()
                .len(),
            1
        );

        let mut two = one;
        two.extend_from_slice(&encode_batch(&["b"]));
        let err = parse_record_batch(&two.freeze(), &budget()).unwrap_err();
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
        let result = parse_record_batch(&batch_bytes, &budget());

        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_parse_record_batch_with_metadata_empty() {
        let batch_bytes = Bytes::new();
        let result = parse_record_batch_with_metadata(&batch_bytes, &budget());

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
            let _ = parse_record_batch(&Bytes::from(bytes), &budget());
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
