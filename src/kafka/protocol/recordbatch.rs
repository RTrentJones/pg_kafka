//! The records of one Produce partition: exactly one RecordBatch v2, the only record
//! format of the Produce versions pg_kafka serves (v3+).
//!
//! The memory a batch decodes to is charged to the request's [`DecodeBudget`] before it is
//! allocated (see [`super::decode_budget`]):
//!
//! * the records, at [`RECORD_MEMORY`] each, from the record count in the batch header,
//!   before anything is decompressed;
//! * the decoded bytes, as the decompressors here produce them: they write through the
//!   budget and stop at its limit, so a compressed batch never expands past what the
//!   request may hold;
//! * the headers, at [`HEADER_MEMORY`] each, and the heap blocks of the copies the records
//!   are converted to, counted by a walk over the decoded records section that allocates
//!   nothing and also checks that it holds exactly the declared records, before
//!   kafka-protocol builds any of them.
//!
//! What kafka-protocol's own records took is given back once they are converted.
//!
//! Runs on the network thread, so it logs through `tracing`, never pgrx.

use std::cell::Cell;
use std::io::{self, Write};
use std::mem::size_of;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{Compression, Record as WireRecord, RecordBatchDecoder};
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

/// Memory of one header in kafka-protocol's decoded record: an `IndexMap` entry (hash, key,
/// value) and its slots in the map's index table.
const WIRE_HEADER_MEMORY: usize =
    size_of::<(u64, StrBytes, Option<Bytes>)>() + 2 * size_of::<usize>();

/// Memory one record takes while it decodes besides its bytes: kafka-protocol's record and
/// the one it is converted to. Charged from the declared record count before decoding, so
/// a batch of many tiny records (7 bytes on the wire, 264 here) cannot hold more memory
/// than the request may.
pub const RECORD_MEMORY: usize = size_of::<WireRecord>() + size_of::<Record>();

/// Memory one header takes while it decodes besides its bytes (2 bytes on the wire at the
/// least): in kafka-protocol's record and in ours.
pub const HEADER_MEMORY: usize = WIRE_HEADER_MEMORY + size_of::<RecordHeader>();

/// What one heap block costs beyond its bytes: the smallest block glibc's malloc hands out
/// (a 1-byte key copy takes 32), or the bookkeeping and rounding of a larger one.
const ALLOCATION_OVERHEAD: usize = 32;

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

/// The least memory `batch` decodes to: its own bytes and its declared records (more if it
/// is compressed or carries headers). Data that is not a record batch decodes to nothing.
pub fn least_decoded_memory(batch: &[u8]) -> usize {
    check_batch_header(batch).map_or(0, |count| {
        count
            .saturating_mul(RECORD_MEMORY)
            .saturating_add(batch.len())
    })
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

/// zstd frames may ask for a window of at most 2^27 bytes (128 MiB), zstd's own default
/// limit and Kafka's: Java producers at `compression.zstd.level` 20-22 (Kafka 3.8+) use
/// windows of 2^25-2^27. The decoder reserves the window a frame header declares up front
/// but touches it only as it decodes, so the window is charged as it fills: as much again
/// as the decoded bytes, up to the declared size ([`zstd_window`]).
const ZSTD_WINDOW_LOG_MAX: u32 = 27;

/// The first byte of a zstd frame (little-endian magic 0xFD2FB528).
const ZSTD_MAGIC: u32 = 0xFD2F_B528;
/// Skippable frames carry magics 0x184D2A50 to 0x184D2A5F and no window.
const ZSTD_SKIPPABLE_MAGIC: u32 = 0x184D_2A50;

/// The window a zstd frame header declares (RFC 8878 §3.1.1.1): from the Window_Descriptor,
/// or the content size of a single-segment frame.
fn zstd_frame_window(frame: &[u8]) -> anyhow::Result<u64> {
    let header = |n: usize| {
        frame
            .get(..n)
            .ok_or_else(|| anyhow::anyhow!("zstd frame header cut short"))
    };
    let magic = u32::from_le_bytes(header(4)?.try_into()?);
    if magic & 0xFFFF_FFF0 == ZSTD_SKIPPABLE_MAGIC {
        return Ok(0);
    }
    anyhow::ensure!(magic == ZSTD_MAGIC, "not a zstd frame");
    let descriptor = header(5)?[4];
    if descriptor & 0x20 == 0 {
        let window_descriptor = header(6)?[5];
        let log = 10 + u32::from(window_descriptor >> 3);
        let base = 1u64 << log;
        return Ok(base + base / 8 * u64::from(window_descriptor & 7));
    }
    // Single segment: the window is the frame content size, after the dictionary id.
    let dictionary_id_bytes = [0, 1, 2, 4][usize::from(descriptor & 3)];
    let content_size_bytes = [1, 2, 4, 8][usize::from(descriptor >> 6)];
    let at = 5 + dictionary_id_bytes;
    let field = header(at + content_size_bytes)?;
    let mut size = [0u8; 8];
    size[..content_size_bytes].copy_from_slice(&field[at..]);
    let size = u64::from_le_bytes(size);
    Ok(if content_size_bytes == 2 {
        size + 256
    } else {
        size
    })
}

/// The largest window any frame of `data` declares; past 2^[`ZSTD_WINDOW_LOG_MAX`] the data
/// is refused before the decoder reserves anything. A frame the walk cannot measure ends it:
/// the decoder reports what is wrong with it.
fn zstd_window(mut data: &[u8]) -> anyhow::Result<usize> {
    let mut largest = 0u64;
    while !data.is_empty() {
        largest = largest.max(zstd_frame_window(data)?);
        anyhow::ensure!(
            largest <= 1u64 << ZSTD_WINDOW_LOG_MAX,
            "zstd frame declares a window of {largest} bytes"
        );
        match zstd::zstd_safe::find_frame_compressed_size(data) {
            Ok(len) if len > 0 && len <= data.len() => data = &data[len..],
            _ => break,
        }
    }
    Ok(usize::try_from(largest)?)
}

/// Decoded output written through a [`DecodeBudget`]: a write that would pass the budget
/// fails before anything is buffered. A decoder's window that fills as it decodes is
/// charged with the output, up to `window` bytes.
struct BudgetedWriter<'a> {
    out: BytesMut,
    budget: &'a DecodeBudget,
    window: usize,
    window_charged: usize,
}

impl<'a> BudgetedWriter<'a> {
    fn new(budget: &'a DecodeBudget) -> Self {
        Self::with_window(budget, 0)
    }

    fn with_window(budget: &'a DecodeBudget, window: usize) -> Self {
        Self {
            out: BytesMut::new(),
            budget,
            window,
            window_charged: 0,
        }
    }

    /// The output; what its window was charged is given back (the decoder is gone).
    fn into_bytes(self) -> Bytes {
        self.budget.release(self.window_charged);
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
        let window = buf.len().min(self.window - self.window_charged);
        self.budget
            .take(buf.len() + window)
            .map_err(|_| over_budget())?;
        self.window_charged += window;
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
            out = BudgetedWriter::with_window(budget, zstd_window(&data)?);
            let mut decoder = zstd::stream::read::Decoder::new(data.reader())?;
            decoder.window_log_max(ZSTD_WINDOW_LOG_MAX)?;
            io::copy(&mut decoder, &mut out)?;
        }
    }
    Ok(out.into_bytes())
}

/// Read a varint (zigzag-encoded, at most 10 bytes) off the front of `buf`.
fn read_varint(buf: &mut &[u8]) -> Option<i64> {
    let mut value = 0u64;
    for shift in (0..70).step_by(7) {
        let (&byte, rest) = buf.split_first()?;
        *buf = rest;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            // Zigzag: the low bit is the sign.
            let magnitude = i64::try_from(value >> 1).ok()?;
            let sign = i64::try_from(value & 1).ok()?;
            return Some(magnitude ^ -sign);
        }
    }
    None
}

/// What decoding a records section allocates besides its records.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct SectionShape {
    /// Headers, over all records.
    headers: usize,
    /// Heap blocks: a copy of every non-empty key, value, header key and header value, and
    /// the header maps and vectors of records that have headers.
    allocations: usize,
}

/// Skip a varint-length-prefixed byte string (`-1` = null when `nullable`); whether it
/// holds any bytes (its copy is a heap block).
fn skip_bytes(buf: &mut &[u8], nullable: bool, what: &str) -> std::result::Result<bool, String> {
    let len = read_varint(buf).ok_or_else(|| format!("record {what} length cut short"))?;
    if nullable && len == -1 {
        return Ok(false);
    }
    let len = usize::try_from(len)
        .ok()
        .filter(|&n| n <= buf.len())
        .ok_or_else(|| format!("record {what} length {len} is invalid"))?;
    *buf = &buf[len..];
    Ok(len > 0)
}

/// Heap blocks a record with headers adds: kafka-protocol's map (entries and index table)
/// and our vector.
const HEADER_CONTAINER_ALLOCATIONS: usize = 3;

/// The shape of one record body (attributes, timestamp and offset deltas, key, value,
/// headers), which must be exactly its fields.
fn scan_record(mut body: &[u8]) -> std::result::Result<SectionShape, String> {
    let cut_short = || "record cut short".to_string();
    body = body.get(1..).ok_or_else(cut_short)?; // attributes
    read_varint(&mut body).ok_or_else(cut_short)?; // timestamp delta
    read_varint(&mut body).ok_or_else(cut_short)?; // offset delta
    let mut allocations = usize::from(skip_bytes(&mut body, true, "key")?);
    allocations += usize::from(skip_bytes(&mut body, true, "value")?);
    let headers = read_varint(&mut body).ok_or_else(cut_short)?;
    // Every header takes at least two bytes, which bounds the walk by the body.
    let headers = usize::try_from(headers)
        .ok()
        .filter(|&n| n <= body.len() / 2)
        .ok_or_else(|| format!("record header count {headers} is invalid"))?;
    if headers > 0 {
        allocations += HEADER_CONTAINER_ALLOCATIONS;
    }
    for _ in 0..headers {
        allocations += usize::from(skip_bytes(&mut body, false, "header key")?);
        allocations += usize::from(skip_bytes(&mut body, true, "header value")?);
    }
    if body.is_empty() {
        Ok(SectionShape {
            headers,
            allocations,
        })
    } else {
        Err(format!("record has {} bytes past its fields", body.len()))
    }
}

/// The shape of a decoded records section, which must hold exactly `count` well-formed
/// records (Kafka 3.7 refuses anything else as INVALID_RECORD). Allocates nothing, so a
/// section can be checked, and what it decodes to charged, before kafka-protocol builds
/// its records.
fn scan_records(mut section: &[u8], count: usize) -> std::result::Result<SectionShape, String> {
    let mut shape = SectionShape::default();
    for read in 0..count {
        let size = read_varint(&mut section)
            .ok_or_else(|| format!("records section holds {read} records, not {count}"))?;
        let size = usize::try_from(size)
            .ok()
            .filter(|&n| n <= section.len())
            .ok_or_else(|| format!("record size {size} is invalid"))?;
        let (body, rest) = section.split_at(size);
        let record = scan_record(body)?;
        shape.headers += record.headers;
        shape.allocations += record.allocations;
        section = rest;
    }
    if section.is_empty() {
        Ok(shape)
    } else {
        Err(format!(
            "{} bytes remain past the {count} records the batch declares",
            section.len()
        ))
    }
}

/// What the memory of a decode is charged for a section of `shape`, besides its bytes and
/// its records.
fn shape_memory(shape: SectionShape) -> usize {
    shape
        .headers
        .saturating_mul(HEADER_MEMORY)
        .saturating_add(shape.allocations.saturating_mul(ALLOCATION_OVERHEAD))
}

/// Our record from kafka-protocol's.
fn convert(record: WireRecord) -> Record {
    Record {
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
    }
}

/// Parse a partition's record data (one RecordBatch v2) into its records, charging their
/// memory to `budget` (see the module docs). kafka-protocol decodes the batch, checking its
/// CRC32C, through `decompress` and `scan_records`.
pub fn parse_record_batch(batch_bytes: &Bytes, budget: &DecodeBudget) -> Result<Vec<Record>> {
    let count = check_batch_header(batch_bytes)?;
    debug!(
        "parse_record_batch: {} bytes, {count} records",
        batch_bytes.len()
    );
    let at_the_limit = |e: &dyn std::fmt::Display| {
        corrupt(format!("record batch stopped at the decode limit: {e}"))
    };
    if budget.take(count.saturating_mul(RECORD_MEMORY)).is_err() {
        return Err(at_the_limit(&format!("{count} records")));
    }

    let not_the_declared_records = Cell::new(None);
    let headers = Cell::new(0usize);
    let mut batch_buf = batch_bytes.clone();
    let decoded = RecordBatchDecoder::decode_with_custom_compression(
        &mut batch_buf,
        Some(|data: &mut Bytes, compression| {
            let section = decompress(data, compression, budget)?;
            match scan_records(&section, count) {
                Ok(shape) => {
                    budget
                        .take(shape_memory(shape))
                        .map_err(|_| over_budget())?;
                    headers.set(shape.headers);
                    Ok(section)
                }
                Err(why) => {
                    not_the_declared_records.set(Some(why));
                    anyhow::bail!("records section does not match its batch header")
                }
            }
        }),
    );
    match decoded {
        // Produce v3+ carries exactly one record batch per partition. Decoding one and
        // ignoring the rest would drop the trailing batches while acking the request;
        // reject it as Kafka does.
        Ok(_) if batch_buf.has_remaining() => Err(invalid(format!(
            "partition data carries more than one record batch ({} trailing bytes)",
            batch_buf.remaining()
        ))),
        Ok(record_set) => {
            // A fresh vector of exactly the records: collecting in place would keep the
            // larger allocation of kafka-protocol's records.
            let mut records = Vec::with_capacity(record_set.records.len());
            for record in record_set.records {
                records.push(convert(record));
            }
            budget.release(
                count.saturating_mul(size_of::<WireRecord>())
                    + headers.get().saturating_mul(WIRE_HEADER_MEMORY),
            );
            Ok(records)
        }
        Err(e) if budget.exhausted().is_some() => Err(at_the_limit(&e)),
        Err(e) => match not_the_declared_records.take() {
            Some(why) => Err(invalid(why)),
            None => Err(corrupt(format!("record batch does not decode: {e}"))),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::test_batches::{header_records, raw_batch, tiny_records};
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
            // The records section (the value plus a few bytes of record framing), the
            // record and the value's heap block.
            let charged = budget.charged();
            let least = 100_000 + size_of::<Record>() + ALLOCATION_OVERHEAD;
            assert!(
                (least..least + 100).contains(&charged),
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

    /// A zstd frame of `data` with a `window_log` window and no content size, as a Java
    /// producer's streaming encoder writes it.
    fn zstd_frame(data: &[u8], level: i32, window_log: u32) -> Vec<u8> {
        let mut encoder = zstd::stream::write::Encoder::new(Vec::new(), level).unwrap();
        encoder.window_log(window_log).unwrap();
        encoder.include_contentsize(false).unwrap();
        encoder.write_all(data).unwrap();
        encoder.finish().unwrap()
    }

    #[test]
    fn zstd_frames_with_windows_up_to_kafkas_limit_decode() {
        // Kafka 3.8+ producers at compression.zstd.level 22 declare a 2^27-byte window;
        // Kafka accepts up to that. A larger window is refused before the decoder
        // reserves it.
        let data = vec![7u8; 1000];
        for window_log in [21, 25, 27] {
            let frame = zstd_frame(&data, 22, window_log);
            assert_eq!(
                zstd_window(&frame).unwrap(),
                1 << window_log,
                "window 2^{window_log}"
            );
            let out = decompress(&mut Bytes::from(frame), Compression::Zstd, &budget());
            assert_eq!(&out.unwrap()[..], &data[..], "window 2^{window_log}");
        }
        let frame = zstd_frame(&data, 19, 28);
        let budget = budget();
        assert!(decompress(&mut Bytes::from(frame), Compression::Zstd, &budget).is_err());
        assert_eq!(budget.charged(), 0, "refused before decoding");
    }

    #[test]
    fn a_zstd_window_is_charged_as_it_fills() {
        // While it decodes, a window holds as much again as the decoded bytes, up to its
        // size; it is given back once the decoder is gone.
        let data: Vec<u8> = (0..4_000_000u32).map(|i| (i % 251) as u8).collect();
        for (window_log, extra) in [(20, 1 << 20), (25, data.len())] {
            let frame = zstd_frame(&data, 3, window_log);
            let peak = DecodeBudget::new(data.len() + extra - 1);
            assert!(
                decompress(&mut Bytes::from(frame.clone()), Compression::Zstd, &peak).is_err(),
                "window 2^{window_log} is charged"
            );
            let budget = DecodeBudget::new(data.len() + extra);
            let out = decompress(&mut Bytes::from(frame), Compression::Zstd, &budget).unwrap();
            assert_eq!(out.len(), data.len());
            assert_eq!(budget.charged(), data.len(), "window 2^{window_log}");
        }
        // Concatenated frames: the largest window counts; skippable frames have none.
        let mut frames = vec![0x50, 0x2A, 0x4D, 0x18, 3, 0, 0, 0, 1, 2, 3];
        frames.extend(zstd_frame(b"small", 3, 10));
        frames.extend(zstd_frame(b"large", 3, 26));
        assert_eq!(zstd_window(&frames).unwrap(), 1 << 26);
        // A single-segment frame's window is its content size.
        let single = zstd::bulk::compress(&[1u8; 3000], 3).unwrap();
        assert_eq!(zstd_window(&single).unwrap(), 3000);
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

    #[test]
    fn the_per_record_and_per_header_costs_are_the_documented_ones() {
        // docs/PROTOCOL_DEVIATIONS.md (Compression) quotes these.
        assert_eq!(RECORD_MEMORY, 264);
        assert_eq!(HEADER_MEMORY, 136);
    }

    #[test]
    fn varints_are_zigzag_and_bounded() {
        let read = |bytes: &[u8]| read_varint(&mut &bytes[..]);
        assert_eq!(read(&[0]), Some(0));
        assert_eq!(read(&[1]), Some(-1));
        assert_eq!(read(&[2]), Some(1));
        assert_eq!(read(&[0xac, 0x02]), Some(150));
        assert_eq!(read(&[0x80]), None, "cut short");
        assert_eq!(read(&[0xff; 11]), None, "longer than 10 bytes");
        let mut rest = &[0x02u8, 0x7f][..];
        assert_eq!(read_varint(&mut rest), Some(1));
        assert_eq!(rest, &[0x7f]);
    }

    #[test]
    fn the_section_walk_counts_headers_and_heap_blocks() {
        // One record: key "k" (a heap block), null value, two headers ("a" -> "xy" and
        // "b" -> null): blocks for the key, both header keys, one header value, and the
        // header containers.
        let body = [0u8, 0, 0, 2, b'k', 1, 4, 2, b'a', 4, b'x', b'y', 2, b'b', 1];
        let mut section = vec![u8::try_from(body.len() * 2).unwrap()];
        section.extend_from_slice(&body);
        assert_eq!(
            scan_records(&section, 1),
            Ok(SectionShape {
                headers: 2,
                allocations: 4 + HEADER_CONTAINER_ALLOCATIONS
            })
        );
        assert!(scan_records(&section, 2).is_err(), "one record, not two");
        assert!(scan_records(&section[..section.len() - 1], 1).is_err());
        // A header count the body cannot hold is refused before it is walked.
        let mut huge = vec![0u8, 0, 0, 1, 1, 0xfe, 0xff, 0xff, 0xff, 0x0f];
        huge.insert(0, u8::try_from(huge.len() * 2).unwrap());
        assert!(scan_records(&huge, 1).unwrap_err().contains("header count"));
    }

    #[test]
    fn least_decoded_memory_counts_the_declared_records() {
        let batch = raw_batch(&tiny_records(1000), 1000, true);
        assert_eq!(
            least_decoded_memory(&batch),
            batch.len() + 1000 * RECORD_MEMORY
        );
        assert_eq!(least_decoded_memory(b"not a batch"), 0);
    }

    #[test]
    fn a_batch_of_tiny_records_is_charged_their_memory_before_it_decodes() {
        // 1.5 million 7-byte records: 10 MB decoded, 60 KB of gzip on the wire, and some
        // 400 MB once decoded into records. The declared count is charged before anything
        // decompresses, so the batch stops at the request limit having decoded nothing.
        let n = 1_500_000;
        let batch = raw_batch(&tiny_records(n), i32::try_from(n).unwrap(), true);
        assert!(batch.len() < 100_000, "{} bytes on the wire", batch.len());
        let budget = budget();
        let started = std::time::Instant::now();
        let err = parse_record_batch(&batch, &budget).unwrap_err();
        assert_eq!(budget.exhausted(), Some(Exhausted::Limit), "{err}");
        assert_eq!(budget.charged(), 0, "nothing decoded");
        assert!(started.elapsed() < std::time::Duration::from_millis(50));

        // Under the inline limit the same shape stops at a few thousand records.
        let small = DecodeBudget::new(1024 * 1024);
        let batch = raw_batch(&tiny_records(150_000), 150_000, true);
        assert!(parse_record_batch(&batch, &small).is_err());
        assert_eq!(small.exhausted(), Some(Exhausted::Limit));
    }

    #[test]
    fn a_batch_of_tiny_records_within_the_limit_holds_what_it_is_charged() {
        let n = 10_000;
        let batch = raw_batch(&tiny_records(n), i32::try_from(n).unwrap(), true);
        let budget = budget();
        let records = parse_record_batch(&batch, &budget).unwrap();
        assert_eq!(records.len(), n);
        // A fresh vector: collecting in place would keep kafka-protocol's twice-as-large
        // allocation.
        assert_eq!(records.capacity(), n);
        let held = n * size_of::<Record>();
        assert!(
            budget.charged() >= held && budget.charged() <= held + 7 * n,
            "charged {} for {held}",
            budget.charged()
        );
    }

    #[test]
    fn header_heavy_records_are_charged_their_headers() {
        // 1000 records of 5000 headers, 2 bytes each: 10 MB decoded, but each header
        // takes over 100 bytes decoded. They stop at the request limit before
        // kafka-protocol builds them.
        let batch = raw_batch(&header_records(1000, 5000), 1000, true);
        assert!(batch.len() < 1_000_000);
        let budget = budget();
        let started = std::time::Instant::now();
        let err = parse_record_batch(&batch, &budget).unwrap_err();
        assert_eq!(budget.exhausted(), Some(Exhausted::Limit), "{err}");
        assert!(budget.charged() <= MAX_DECODED_REQUEST_BYTES);
        assert!(started.elapsed() < std::time::Duration::from_secs(2));

        // Within the limit, each header is charged.
        let within = DecodeBudget::new(MAX_DECODED_REQUEST_BYTES);
        let records =
            parse_record_batch(&raw_batch(&header_records(10, 100), 10, false), &within).unwrap();
        // (Their keys are all empty, and a record holds its headers in a map.)
        assert_eq!(records.len(), 10);
        assert!(within.charged() >= 1000 * size_of::<RecordHeader>());
    }

    #[test]
    fn a_record_count_that_does_not_match_the_records_is_an_invalid_record() {
        use crate::kafka::constants::ERROR_INVALID_RECORD;
        // Kafka 3.7 refuses all of these as INVALID_RECORD. One record too few was acked
        // with the last record dropped, and one too many failed only after decoding all.
        let section = tiny_records(1000);
        for count in [999, 1001, 0, 1] {
            for gzip in [false, true] {
                let batch = raw_batch(&section, count, gzip);
                let err = parse_record_batch(&batch, &budget()).unwrap_err();
                assert_eq!(code(&err), ERROR_INVALID_RECORD, "count {count}: {err}");
            }
        }
        // A record with a byte past its fields.
        let err = parse_record_batch(&raw_batch(&[14, 0, 0, 0, 1, 1, 0, 0], 1, false), &budget())
            .unwrap_err();
        assert_eq!(code(&err), ERROR_INVALID_RECORD, "{err}");
        // The count that matches decodes.
        assert_eq!(
            parse_record_batch(&raw_batch(&section, 1000, true), &budget())
                .unwrap()
                .len(),
            1000
        );
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

/// Record batches built by hand, for tests of what the decoder refuses.
#[cfg(test)]
pub(crate) mod test_batches {
    use bytes::{BufMut, Bytes, BytesMut};
    use std::io::Write;

    /// CRC-32C (Castagnoli), the checksum of a RecordBatch v2, bit by bit.
    fn crc32c(data: &[u8]) -> u32 {
        let mut crc = !0u32;
        for &byte in data {
            crc ^= u32::from(byte);
            for _ in 0..8 {
                crc = if crc & 1 == 0 {
                    crc >> 1
                } else {
                    (crc >> 1) ^ 0x82F6_3B78
                };
            }
        }
        !crc
    }

    /// A RecordBatch v2 built by hand around the records section `section`, declaring
    /// `count` records and compressed with gzip when `gzip`.
    pub(crate) fn raw_batch(section: &[u8], count: i32, gzip: bool) -> Bytes {
        let records = if gzip {
            let mut encoder =
                flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::best());
            encoder.write_all(section).unwrap();
            encoder.finish().unwrap()
        } else {
            section.to_vec()
        };
        let mut tail = BytesMut::new();
        tail.put_i16(i16::from(gzip)); // attributes: the codec
        tail.put_i32(count.max(1) - 1);
        tail.put_i64(0);
        tail.put_i64(0);
        tail.put_i64(-1);
        tail.put_i16(-1);
        tail.put_i32(-1);
        tail.put_i32(count);
        tail.put_slice(&records);
        let mut batch = BytesMut::new();
        batch.put_i64(0);
        batch.put_i32(i32::try_from(9 + tail.len()).unwrap());
        batch.put_i32(0);
        batch.put_i8(2);
        batch.put_u32(crc32c(&tail));
        batch.put_slice(&tail);
        batch.freeze()
    }

    /// `n` of the smallest records: 7 bytes each (null key and value, no headers).
    pub(crate) fn tiny_records(n: usize) -> Vec<u8> {
        [12u8, 0, 0, 0, 1, 1, 0].repeat(n)
    }

    /// `n` records of `headers` headers each (an empty key and a null value: 2 bytes).
    pub(crate) fn header_records(n: usize, headers: usize) -> Vec<u8> {
        fn varint(v: i64, out: &mut Vec<u8>) {
            let mut z = ((v << 1) ^ (v >> 63)).cast_unsigned();
            while z >= 0x80 {
                out.push(u8::try_from(z & 0x7f).unwrap() | 0x80);
                z >>= 7;
            }
            out.push(u8::try_from(z).unwrap());
        }
        let mut body = vec![0u8, 0, 0, 1, 1];
        varint(i64::try_from(headers).unwrap(), &mut body);
        body.extend_from_slice(&[0, 1].repeat(headers));
        let mut record = Vec::new();
        varint(i64::try_from(body.len()).unwrap(), &mut record);
        record.extend_from_slice(&body);
        record.repeat(n)
    }
}
