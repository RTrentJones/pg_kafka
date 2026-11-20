//! The records of one Produce partition: exactly one RecordBatch v2, the only record
//! format of the Produce versions pg_kafka serves (v3+).
//!
//! Every decoded byte is charged to the request's [`DecodeBudget`] as it is produced: the
//! decompressors here write through it and stop at its limit, so a compressed batch can
//! never expand past what the request may hold (see [`super::decode_budget`]).
//!
//! Runs on the network thread, so it logs through `tracing`, never pgrx.

use std::io::{self, Write};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use kafka_protocol::records::{Compression, RecordBatchDecoder};
use tracing::debug;

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
// recordsCount i32 @57, records @61.
const BATCH_LENGTH_OFFSET: usize = 8;
/// baseOffset and batchLength, which batchLength does not count.
const LOG_OVERHEAD: usize = 12;
const MAGIC_OFFSET: usize = 16;
const PRODUCER_ID_OFFSET: usize = 43;
const RECORD_COUNT_OFFSET: usize = 57;
/// The header of a RecordBatch v2, up to its first record.
const RECORD_BATCH_OVERHEAD: usize = 61;
/// The smallest batchLength Kafka reads a magic byte behind (a legacy v0 message's).
const MIN_BATCH_LENGTH: usize = 14;
/// The magic byte of a RecordBatch v2.
const RECORD_BATCH_V2_MAGIC: i8 = 2;

fn invalid(message: impl Into<String>) -> KafkaError {
    KafkaError::InvalidRecord {
        message: message.into(),
    }
}

fn corrupt(message: impl Into<String>) -> KafkaError {
    KafkaError::CorruptMessage {
        message: message.into(),
    }
}

/// A big-endian i32 at `at` of `bytes`, which holds it.
fn i32_at(bytes: &[u8], at: usize) -> i32 {
    let mut field = &bytes[at..at + 4];
    field.get_i32()
}

/// Check the framing Kafka checks before it decodes a Produce v3+ partition's records,
/// and return the batch's declared record count.
///
/// In Kafka 3.7's order (`ByteBufferLogInputStream.nextBatchSize`, then
/// `ProduceRequest.validateRecords`): a batchLength too short for any batch, or a v2+ batch
/// shorter than its header, is CORRUPT_MESSAGE; data holding no complete batch, a batch of
/// another magic than 2 (a legacy MessageSet) and a negative record count are
/// INVALID_RECORD.
fn check_batch_header(batch: &[u8]) -> Result<usize> {
    let no_batch = || invalid("Produce v3+ partition data must hold exactly one record batch");
    if batch.len() < LOG_OVERHEAD {
        return Err(no_batch());
    }
    let batch_length = i32_at(batch, BATCH_LENGTH_OFFSET);
    let Some(batch_length) = usize::try_from(batch_length)
        .ok()
        .filter(|&n| n >= MIN_BATCH_LENGTH)
    else {
        return Err(corrupt(format!(
            "record batch length {batch_length} is too short"
        )));
    };
    if batch.len() <= MAGIC_OFFSET {
        return Err(no_batch());
    }
    let magic = i8::from_be_bytes([batch[MAGIC_OFFSET]]);
    if magic >= RECORD_BATCH_V2_MAGIC && batch_length + LOG_OVERHEAD < RECORD_BATCH_OVERHEAD {
        return Err(corrupt(format!(
            "record batch length {batch_length} is shorter than its header"
        )));
    }
    if batch.len() < batch_length + LOG_OVERHEAD {
        return Err(no_batch());
    }
    if magic != RECORD_BATCH_V2_MAGIC {
        return Err(invalid(format!(
            "Produce v3+ carries record batches of magic 2 only, not magic {magic}"
        )));
    }
    usize::try_from(i32_at(batch, RECORD_COUNT_OFFSET))
        .map_err(|_| invalid("record batch declares a negative record count"))
}

/// The producer id, epoch and base sequence in the header of a batch that passed
/// [`check_batch_header`].
fn extract_producer_metadata(batch: &[u8]) -> ProducerMetadata {
    let mut fields = &batch[PRODUCER_ID_OFFSET..RECORD_COUNT_OFFSET];
    ProducerMetadata {
        producer_id: fields.get_i64(),
        producer_epoch: fields.get_i16(),
        base_sequence: fields.get_i32(),
    }
}

/// Parse a partition's record data into its records and producer identity, charging the
/// decoded bytes to `budget`.
pub fn parse_record_batch_with_metadata(
    batch_bytes: &Bytes,
    budget: &DecodeBudget,
) -> Result<ParsedRecordBatch> {
    let records = parse_record_batch(batch_bytes, budget)?;
    Ok(ParsedRecordBatch {
        producer_metadata: extract_producer_metadata(batch_bytes),
        records,
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

/// Parse a partition's record data (one RecordBatch v2) into its records, charging the
/// decoded bytes to `budget`. kafka-protocol decodes the batch, checking its CRC32C, through
/// `decompress`.
pub fn parse_record_batch(batch_bytes: &Bytes, budget: &DecodeBudget) -> Result<Vec<Record>> {
    check_batch_header(batch_bytes)?;
    debug!("parse_record_batch: {} bytes", batch_bytes.len());

    let mut batch_buf = batch_bytes.clone();
    let decoded = RecordBatchDecoder::decode_with_custom_compression(
        &mut batch_buf,
        Some(|data: &mut Bytes, compression| decompress(data, compression, budget)),
    );
    match decoded {
        // Produce v3+ carries exactly one record batch per partition. Decoding one and
        // ignoring the rest would drop the trailing batches while acking the request;
        // reject it as Kafka does.
        Ok(_) if batch_buf.has_remaining() => Err(invalid(format!(
            "partition data carries more than one record batch ({} trailing bytes)",
            batch_buf.remaining()
        ))),
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
        Err(e) if budget.exhausted().is_some() => Err(corrupt(format!(
            "record batch stopped at the decode limit: {e}"
        ))),
        Err(e) => Err(corrupt(format!("record batch does not decode: {e}"))),
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
                // offset - sequence must not change within a batch, or the encoder starts a
                // new one.
                sequence: i32::try_from(i).unwrap() - 1,
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

    /// One legacy MessageSet v0/v1 entry (offset, message size, CRC, magic, attributes,
    /// a v1 timestamp, a null key and `value`). The broker refuses it before its CRC.
    fn legacy_entry(magic: u8, attributes: u8, value: &[u8]) -> Vec<u8> {
        let mut body = vec![magic, attributes];
        if magic == 1 {
            body.extend_from_slice(&0i64.to_be_bytes());
        }
        body.extend_from_slice(&(-1i32).to_be_bytes());
        body.extend_from_slice(&i32::try_from(value.len()).unwrap().to_be_bytes());
        body.extend_from_slice(value);
        let mut entry = 0i64.to_be_bytes().to_vec();
        entry.extend_from_slice(&i32::try_from(4 + body.len()).unwrap().to_be_bytes());
        entry.extend_from_slice(&0u32.to_be_bytes());
        entry.extend_from_slice(&body);
        entry
    }

    fn code(err: &KafkaError) -> i16 {
        err.to_kafka_error_code()
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

    #[test]
    fn legacy_message_sets_are_invalid_records() {
        use crate::kafka::constants::ERROR_INVALID_RECORD;
        // Produce v3+ carries RecordBatch v2 only (Kafka: INVALID_RECORD). A legacy set was
        // stored before: a gzip wrapper as one garbage record, a cut-short set as its prefix.
        let mut two = legacy_entry(1, 0, b"one");
        two.extend_from_slice(&legacy_entry(1, 0, b"two"));
        let mut cut_short = legacy_entry(0, 0, b"kept");
        cut_short.extend_from_slice(&legacy_entry(0, 0, b"lost")[..20]);
        let shapes = [
            legacy_entry(0, 0, b"v0"),
            legacy_entry(1, 0, b"v1"),
            legacy_entry(1, 1, b"gzip wrapper"),
            two,
            cut_short,
        ];
        for data in shapes {
            let err = parse_record_batch(&Bytes::from(data.clone()), &budget()).unwrap_err();
            assert_eq!(code(&err), ERROR_INVALID_RECORD, "{data:?}: {err}");
        }
    }

    #[test]
    fn batch_framing_errors_match_kafka() {
        use crate::kafka::constants::{ERROR_CORRUPT_MESSAGE, ERROR_INVALID_RECORD};
        let ok = encode_batch(&["a", "b"]).freeze();
        assert_eq!(parse_record_batch(&ok, &budget()).unwrap().len(), 2);
        let with = |at: usize, bytes: &[u8]| {
            let mut b = ok.to_vec();
            b[at..at + bytes.len()].copy_from_slice(bytes);
            Bytes::from(b)
        };
        let cases: [(&str, Bytes, i16); 7] = [
            ("empty", Bytes::new(), ERROR_INVALID_RECORD),
            ("no magic byte", ok.slice(..16), ERROR_INVALID_RECORD),
            ("cut short", ok.slice(..ok.len() - 3), ERROR_INVALID_RECORD),
            (
                "negative count",
                with(57, &(-1i32).to_be_bytes()),
                ERROR_INVALID_RECORD,
            ),
            (
                "zero batch length",
                with(8, &0i32.to_be_bytes()),
                ERROR_CORRUPT_MESSAGE,
            ),
            (
                "header cut short",
                with(8, &40i32.to_be_bytes()),
                ERROR_CORRUPT_MESSAGE,
            ),
            ("bad CRC", with(ok.len() - 1, b"x"), ERROR_CORRUPT_MESSAGE),
        ];
        for (what, data, expected) in cases {
            let err = parse_record_batch(&data, &budget()).unwrap_err();
            assert_eq!(code(&err), expected, "{what}: {err}");
        }
    }

    #[test]
    fn test_extract_producer_metadata_valid_v2() {
        let mut buf = vec![0u8; 61];
        buf[43..51].copy_from_slice(&12345i64.to_be_bytes());
        buf[51..53].copy_from_slice(&5i16.to_be_bytes());
        buf[53..57].copy_from_slice(&100i32.to_be_bytes());
        let metadata = extract_producer_metadata(&buf);
        assert_eq!(metadata.producer_id, 12345);
        assert_eq!(metadata.producer_epoch, 5);
        assert_eq!(metadata.base_sequence, 100);
    }

    #[test]
    fn test_parse_record_batch_with_metadata_reads_the_header() {
        let parsed =
            parse_record_batch_with_metadata(&encode_batch(&["x"]).freeze(), &budget()).unwrap();
        assert_eq!(parsed.records.len(), 1);
        assert_eq!(parsed.producer_metadata.producer_id, -1);
        assert_eq!(parsed.producer_metadata.producer_epoch, -1);
        assert_eq!(parsed.producer_metadata.base_sequence, -1);
    }

    proptest::proptest! {
        // A valid batch with any bytes of its header overwritten: the framing checks read
        // only bytes they checked are there, so it never panics.
        #[test]
        fn fuzz_batch_header_never_panics(
            at in 0usize..61,
            bytes in proptest::collection::vec(proptest::num::u8::ANY, 1..8),
        ) {
            let mut data = encode_batch(&["a", "b"]).to_vec();
            for (i, b) in bytes.iter().enumerate() {
                if let Some(slot) = data.get_mut(at + i) {
                    *slot = *b;
                }
            }
            let _ = parse_record_batch(&Bytes::from(data.clone()), &budget());
            data.truncate(at);
            let _ = parse_record_batch(&Bytes::from(data), &budget());
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
