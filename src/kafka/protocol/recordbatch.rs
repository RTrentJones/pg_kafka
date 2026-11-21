//! The records of one Produce partition: exactly one RecordBatch v2, the only record
//! format of the Produce versions pg_kafka serves (v3+).
//!
//! The batch is read here, field by field, with the grammar of Kafka 3.7's
//! `DefaultRecordBatch` and `DefaultRecord` (varints and varlongs as `ByteUtils` reads them),
//! so a record holds exactly what Kafka would store, and what it holds is what was charged.
//!
//! The memory a batch decodes to is charged to the request's [`DecodeBudget`] before it is
//! allocated (see [`super::decode_budget`]):
//!
//! * the records, at [`RECORD_MEMORY`] each, from the record count in the batch header,
//!   before anything is decompressed. A batch whose records alone take more than any
//!   request may hold ([`MAX_DECODED_REQUEST_BYTES`]) is refused MESSAGE_TOO_LARGE before
//!   anything is charged, and so is snappy data declaring more;
//! * the decoded bytes, as the decompressors here produce them: they write through the
//!   budget, which is charged their buffer's capacity before it grows, and stop at its
//!   limit, so a compressed batch never expands past what the request may hold. The
//!   copies the records keep of their keys, values and headers are covered by these
//!   bytes;
//! * every heap block of those copies, at [`ALLOCATION_OVERHEAD`], and the headers, at
//!   [`HEADER_MEMORY`] each, as the records are read.
//!
//! Runs on the network thread, so it logs through `tracing`, never pgrx.

use std::io::{self, Write};
use std::mem::size_of;

use bytes::{Buf, Bytes};
use kafka_protocol::records::Compression;
use tracing::debug;

use super::decode_budget::{DecodeBudget, MAX_DECODED_REQUEST_BYTES};
use crate::kafka::constants::{
    ERROR_INVALID_TIMESTAMP, ERROR_MESSAGE_TOO_LARGE, ERROR_UNSUPPORTED_COMPRESSION_TYPE,
};
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
/// The bytes up to and including the magic byte.
const HEADER_SIZE_UP_TO_MAGIC: usize = MAGIC_OFFSET + 1;
const CRC_OFFSET: usize = 17;
/// The CRC covers the batch from its attributes on.
const ATTRIBUTES_OFFSET: usize = 21;
const MAX_TIMESTAMP_OFFSET: usize = 35;
const PRODUCER_ID_OFFSET: usize = 43;
const RECORD_COUNT_OFFSET: usize = 57;
/// The header of a RecordBatch v2, up to its first record.
const RECORD_BATCH_OVERHEAD: usize = 61;
/// The smallest batchLength Kafka reads a magic byte behind (a legacy v0 message's).
const MIN_BATCH_LENGTH: usize = 14;
/// The magic byte of a RecordBatch v2, the highest there is.
const RECORD_BATCH_V2_MAGIC: i8 = 2;

/// Attribute bits of a RecordBatch v2.
const CODEC_MASK: i16 = 0x07;
const CONTROL_FLAG: i16 = 0x20;
const LOG_APPEND_TIME_FLAG: i16 = 0x08;
const TRANSACTIONAL_FLAG: i16 = 0x10;
/// The timestamp of a record that has none (`RecordBatch.NO_TIMESTAMP`).
const NO_TIMESTAMP: i64 = -1;
/// The codec id of zstd, which Produce carries from v7 on (KIP-110).
const ZSTD_CODEC: i16 = 4;

/// Memory one record takes while it decodes besides its bytes. Charged from the declared
/// record count before decoding, so a batch of many tiny records (7 bytes on the wire,
/// 88 here) cannot hold more memory than the request may.
pub const RECORD_MEMORY: usize = size_of::<Record>();

/// Memory one header takes while it decodes besides its bytes (2 bytes on the wire at the
/// least).
pub const HEADER_MEMORY: usize = size_of::<RecordHeader>();

/// The fewest bytes a record takes in a records section: its size varint and six one-byte
/// fields (attributes, timestamp delta, offset delta, key length, value length, header
/// count).
const MIN_RECORD_SIZE: usize = 7;

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

/// MESSAGE_TOO_LARGE: a batch that by what it declares takes more memory decoded than any
/// request may hold, whatever else the request holds.
fn too_large(message: impl Into<String>) -> KafkaError {
    KafkaError::Protocol {
        code: ERROR_MESSAGE_TOO_LARGE,
        message: message.into(),
    }
}

/// A big-endian i32 at `at` of `bytes`, which holds it.
fn i32_at(bytes: &[u8], at: usize) -> i32 {
    let mut field = &bytes[at..at + 4];
    field.get_i32()
}

/// The size of the batch at the start of `data` as Kafka 3.7 reads it
/// (`ByteBufferLogInputStream.nextBatchSize`): `None` when `data` is too short to say, a
/// batchLength too short for any batch or a magic byte past 2 is CORRUPT_MESSAGE. The size
/// may exceed `data` (a batch cut short).
fn next_batch_size(data: &[u8]) -> Result<Option<usize>> {
    if data.len() < LOG_OVERHEAD {
        return Ok(None);
    }
    let batch_length = i32_at(data, BATCH_LENGTH_OFFSET);
    let Some(batch_length) = usize::try_from(batch_length)
        .ok()
        .filter(|&n| n >= MIN_BATCH_LENGTH)
    else {
        return Err(corrupt(format!(
            "record batch length {batch_length} is too short"
        )));
    };
    if data.len() < HEADER_SIZE_UP_TO_MAGIC {
        return Ok(None);
    }
    let magic = i8::from_be_bytes([data[MAGIC_OFFSET]]);
    if !(0..=RECORD_BATCH_V2_MAGIC).contains(&magic) {
        return Err(corrupt(format!("record batch magic {magic} is invalid")));
    }
    Ok(Some(batch_length + LOG_OVERHEAD))
}

/// Check the framing Kafka checks before it reads a Produce v3+ partition's batch, and
/// return the batch's size.
///
/// In Kafka 3.7's order (`ByteBufferLogInputStream`, then `ProduceRequest.validateRecords`):
/// [`next_batch_size`]'s CORRUPT_MESSAGE; data holding no complete batch is INVALID_RECORD;
/// a batch of another magic than 2 (a legacy MessageSet) is INVALID_RECORD. The batch may
/// still be shorter than a v2 header (26 bytes at the least, so its attributes are there);
/// [`parse_record_batch`] refuses that later, where Kafka does.
fn check_batch_header(batch: &[u8]) -> Result<usize> {
    let size = next_batch_size(batch)?
        .filter(|&size| size <= batch.len())
        .ok_or_else(|| invalid("Produce v3+ partition data must hold exactly one record batch"))?;
    let magic = i8::from_be_bytes([batch[MAGIC_OFFSET]]);
    if magic != RECORD_BATCH_V2_MAGIC {
        return Err(invalid(format!(
            "Produce v3+ carries record batches of magic 2 only, not magic {magic}"
        )));
    }
    Ok(size)
}

/// Check what follows the batch, as Kafka does: data too short to hold another batch is
/// ignored (Kafka trims it), a second complete batch is INVALID_RECORD (Produce v3+ carries
/// exactly one per partition: decoding one and dropping the rest would lose records while
/// acking them).
fn check_after_batch(rest: &[u8]) -> Result<()> {
    match next_batch_size(rest)? {
        Some(size) if size <= rest.len() => Err(invalid(
            "Produce v3+ partition data must hold exactly one record batch, not more",
        )),
        _ => Ok(()),
    }
}

/// What Kafka 3.7 checks of a Produce partition's data before it reads its batch
/// (`ProduceRequest.validateRecords`): one record batch of magic 2 (`check_batch_header`,
/// `check_after_batch`) and, before Produce v7 (KIP-110), not zstd. A failure refuses this
/// partition only; `Err` is its error code.
pub fn validate_produce_records(batch: &[u8], zstd_allowed: bool) -> std::result::Result<(), i16> {
    let size = check_batch_header(batch).map_err(|e| e.to_kafka_error_code())?;
    let mut attributes = &batch[ATTRIBUTES_OFFSET..ATTRIBUTES_OFFSET + 2];
    if !zstd_allowed && attributes.get_i16() & CODEC_MASK == ZSTD_CODEC {
        return Err(ERROR_UNSUPPORTED_COMPRESSION_TYPE);
    }
    check_after_batch(&batch[size..]).map_err(|e| e.to_kafka_error_code())
}

/// Whether a partition's first batch is transactional, as Kafka 3.7 reads it before it
/// looks at any partition (`RequestUtils.hasTransactionalRecords`, which stops at the first
/// transactional batch of the request). `Err` is CORRUPT_MESSAGE when the first batch's
/// length or magic cannot be read (`next_batch_size`): that fails the whole request.
/// Data holding no complete batch, or a legacy one, is not transactional.
pub fn first_batch_is_transactional(data: &[u8]) -> std::result::Result<bool, i16> {
    match next_batch_size(data) {
        Err(e) => Err(e.to_kafka_error_code()),
        Ok(Some(size)) if size <= data.len() => Ok(i8::from_be_bytes([data[MAGIC_OFFSET]])
            == RECORD_BATCH_V2_MAGIC
            && (&data[ATTRIBUTES_OFFSET..]).get_i16() & TRANSACTIONAL_FLAG != 0),
        Ok(_) => Ok(false),
    }
}

/// The fields of a batch header Kafka 3.7 validates for a client's batch once its CRC and
/// codec passed (`LogValidator.validateBatch`), all INVALID_RECORD: a lastOffsetDelta,
/// record count or offset range that disagree (so a batch of no records is refused, and
/// cannot enter an idempotent producer's batch cache); a control batch; a producer id
/// without a sequence.
fn validate_batch(batch: &[u8]) -> Result<usize> {
    let mut fields = &batch[ATTRIBUTES_OFFSET..];
    let attributes = fields.get_i16();
    let last_offset_delta = fields.get_i32();
    let mut fields = &batch[PRODUCER_ID_OFFSET..];
    let producer_id = fields.get_i64();
    let _producer_epoch = fields.get_i16();
    let base_sequence = fields.get_i32();
    let count = fields.get_i32();
    if last_offset_delta < 0 {
        return Err(invalid(format!(
            "record batch has an invalid offset range (lastOffsetDelta {last_offset_delta})"
        )));
    }
    if count <= 0 {
        return Err(invalid(format!("record batch declares {count} records")));
    }
    if i64::from(last_offset_delta) + 1 != i64::from(count) {
        return Err(invalid(format!(
            "record batch declares {count} records over {} offsets",
            i64::from(last_offset_delta) + 1
        )));
    }
    if attributes & CONTROL_FLAG != 0 {
        return Err(invalid("clients may not write control batches"));
    }
    if producer_id >= 0 && base_sequence < 0 {
        return Err(invalid(format!(
            "record batch of producer {producer_id} has sequence {base_sequence}"
        )));
    }
    usize::try_from(count).map_err(|_| invalid("record batch declares a negative record count"))
}

/// The producer id, epoch, base sequence and transactional flag in the header of a batch
/// that passed [`check_batch_header`].
fn extract_producer_metadata(batch: &[u8]) -> ProducerMetadata {
    let mut fields = &batch[PRODUCER_ID_OFFSET..RECORD_COUNT_OFFSET];
    ProducerMetadata {
        producer_id: fields.get_i64(),
        producer_epoch: fields.get_i16(),
        base_sequence: fields.get_i32(),
        is_transactional: (&batch[ATTRIBUTES_OFFSET..]).get_i16() & TRANSACTIONAL_FLAG != 0,
    }
}

/// A batch that passed every check Kafka makes before it reads the records.
struct Presized {
    compression: Compression,
    count: usize,
}

/// The checks of a batch (one that passed [`check_batch_header`], cut to its size) before
/// its records are read or anything is charged: [`check_batch_integrity`],
/// [`validate_batch`], then two that bound what it may decode to. An uncompressed section
/// too short to hold the declared records is INVALID_RECORD, as Kafka answers once it runs
/// out of records; a batch whose declared records (and uncompressed section) alone take
/// more than any request may hold is MESSAGE_TOO_LARGE, as it could never decode.
fn presize(batch: &[u8]) -> Result<Presized> {
    let compression = check_batch_integrity(batch)?;
    let count = validate_batch(batch)?;
    let section = batch.len() - RECORD_BATCH_OVERHEAD;
    let mut least = count.saturating_mul(RECORD_MEMORY);
    if compression == Compression::None {
        if count > section / MIN_RECORD_SIZE {
            return Err(invalid(format!(
                "a records section of {section} bytes cannot hold {count} records"
            )));
        }
        least = least.saturating_add(section);
    }
    if least > MAX_DECODED_REQUEST_BYTES {
        return Err(too_large(format!(
            "record batch of {count} records takes at least {least} bytes decoded"
        )));
    }
    Ok(Presized { compression, count })
}

/// The least memory a Produce partition's data decodes to: its own bytes and its declared
/// records (more if it is compressed or carries headers). Data refused before its records
/// are read ([`validate_produce_records`], then the checks [`parse_record_batch`] makes
/// first) decodes to nothing.
pub fn least_decoded_memory(batch: &[u8], zstd_allowed: bool) -> usize {
    if validate_produce_records(batch, zstd_allowed).is_err() {
        return 0;
    }
    match check_batch_header(batch).and_then(|size| presize(&batch[..size])) {
        Ok(presized) => presized
            .count
            .saturating_mul(RECORD_MEMORY)
            .saturating_add(batch.len()),
        Err(_) => 0,
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
fn zstd_window(mut data: &[u8], budget: &DecodeBudget) -> anyhow::Result<usize> {
    let mut largest = 0u64;
    let mut clock = InputClock::new(budget);
    while !data.is_empty() {
        largest = largest.max(zstd_frame_window(data)?);
        anyhow::ensure!(
            largest <= 1u64 << ZSTD_WINDOW_LOG_MAX,
            "zstd frame declares a window of {largest} bytes"
        );
        match zstd::zstd_safe::find_frame_compressed_size(data) {
            Ok(len) if len > 0 && len <= data.len() => {
                clock.consumed(len)?;
                data = &data[len..];
            }
            _ => break,
        }
    }
    Ok(usize::try_from(largest)?)
}

/// Decoded output written through a [`DecodeBudget`]: the buffer's capacity is charged
/// before it grows, so a write that would pass the budget fails before anything is
/// allocated. The buffer doubles while the budget has room for that and otherwise grows to
/// exactly what it holds; once decoded it is shrunk to its bytes and the rest given back. A
/// decoder's window that fills as it decodes is charged with the output, up to `window`
/// bytes.
struct BudgetedWriter<'a> {
    out: Vec<u8>,
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
            out: Vec::new(),
            budget,
            window,
            window_charged: 0,
        }
    }

    /// The output, shrunk to its bytes; what its window and spare capacity were charged is
    /// given back (the decoder is gone).
    fn into_bytes(mut self) -> Bytes {
        let spare = self.out.capacity() - self.out.len();
        self.out.shrink_to_fit();
        self.budget.release(self.window_charged + spare);
        Bytes::from(self.out)
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
        let capacity = self.out.capacity();
        let needed = self.out.len() + buf.len();
        let grown = if needed > capacity {
            // The window still to charge keeps its room.
            let room = self
                .budget
                .room()
                .saturating_sub(self.window - self.window_charged);
            capacity
                .saturating_mul(2)
                .min(capacity.saturating_add(room))
                .max(needed)
        } else {
            capacity
        };
        self.budget
            .take(grown - capacity + window)
            .map_err(|_| over_budget())?;
        self.window_charged += window;
        // A Vec grown with `reserve_exact` has exactly the capacity asked for.
        self.out.reserve_exact(grown - self.out.len());
        self.out.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// The compressed input a decoder reads between checks of its decode's time
/// ([`DecodeBudget::check_time`]): at some 200 ns per byte, the slowest input known (gzip of
/// empty blocks), about 3 ms of decoding.
const INPUT_CHUNK: usize = 16 * 1024;

/// Counts the compressed input a decode consumed, checking its time at the first input and
/// then once per [`INPUT_CHUNK`].
struct InputClock<'a> {
    budget: &'a DecodeBudget,
    since_check: usize,
}

impl<'a> InputClock<'a> {
    fn new(budget: &'a DecodeBudget) -> Self {
        Self {
            budget,
            since_check: INPUT_CHUNK,
        }
    }

    /// `n` more bytes consumed; fails once the decode's time has passed.
    fn consumed(&mut self, n: usize) -> io::Result<()> {
        self.since_check += n;
        if self.since_check >= INPUT_CHUNK {
            self.since_check = 0;
            self.budget.check_time().map_err(|_| over_budget())?;
        }
        Ok(())
    }
}

/// A decoder's compressed input, read at most [`INPUT_CHUNK`] at a time with the decode's
/// time checked before each read.
struct MeteredInput<'a, R> {
    inner: R,
    budget: &'a DecodeBudget,
}

impl<R: io::Read> io::Read for MeteredInput<'_, R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.budget.check_time().map_err(|_| over_budget())?;
        let n = buf.len().min(INPUT_CHUNK);
        self.inner.read(&mut buf[..n])
    }
}

/// The xerial framing Kafka's Java client wraps snappy data in: magic, version, compatible
/// version, then `[length u32][raw snappy block]` repeated.
const XERIAL_SNAPPY_HEADER: &[u8; 16] = b"\x82SNAPPY\x00\x00\x00\x00\x01\x00\x00\x00\x01";

/// The raw snappy blocks of `data`: the blocks of its xerial framing, or `data` itself.
fn snappy_blocks(data: &[u8]) -> impl Iterator<Item = anyhow::Result<&[u8]>> {
    let mut framed = data.strip_prefix(&XERIAL_SNAPPY_HEADER[..]);
    let mut raw = framed.is_none().then_some(data);
    std::iter::from_fn(move || {
        if let Some(block) = raw.take() {
            return Some(Ok(block));
        }
        let blocks = framed.as_mut().filter(|b| !b.is_empty())?;
        let block = (|| {
            anyhow::ensure!(blocks.len() >= 4, "snappy block length cut short");
            let len = usize::try_from(blocks.get_u32())?;
            anyhow::ensure!(blocks.len() >= len, "snappy block cut short");
            let (block, rest) = blocks.split_at(len);
            *blocks = rest;
            Ok(block)
        })();
        if block.is_err() {
            framed = None;
        }
        Some(block)
    })
}

/// Decoded data that declares more bytes than any request may hold.
#[derive(Debug)]
struct NeverFits(usize);

impl std::fmt::Display for NeverFits {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "snappy data declares {} decoded bytes", self.0)
    }
}

impl std::error::Error for NeverFits {}

/// Snappy (xerial-framed or raw): the blocks' declared lengths are summed and charged, and
/// the output allocated once at that size, before any block decodes.
fn decompress_snappy(data: &Bytes, budget: &DecodeBudget) -> anyhow::Result<Bytes> {
    use snap::raw::{decompress_len, Decoder};
    let mut clock = InputClock::new(budget);
    let mut total = 0usize;
    for block in snappy_blocks(data) {
        let block = block?;
        clock.consumed(block.len() + 4)?;
        total = total.saturating_add(decompress_len(block)?);
    }
    if total > MAX_DECODED_REQUEST_BYTES {
        return Err(NeverFits(total).into());
    }
    budget.take(total).map_err(|_| over_budget())?;
    let mut out = vec![0; total];
    let mut at = 0;
    for block in snappy_blocks(data) {
        let block = block?;
        clock.consumed(block.len() + 4)?;
        let len = decompress_len(block)?;
        Decoder::new().decompress(block, &mut out[at..at + len])?;
        at += len;
    }
    Ok(Bytes::from(out))
}

/// The decompressed records section of a RecordBatch, every byte charged to `budget`.
fn decompress(
    data: Bytes,
    compression: Compression,
    budget: &DecodeBudget,
) -> anyhow::Result<Bytes> {
    let mut out = BudgetedWriter::new(budget);
    match compression {
        Compression::None => {
            budget.take(data.len()).map_err(|_| over_budget())?;
            return Ok(data);
        }
        Compression::Gzip => {
            let mut decoder = flate2::write::GzDecoder::new(&mut out);
            for chunk in data.chunks(INPUT_CHUNK) {
                budget.check_time().map_err(|_| over_budget())?;
                decoder.write_all(chunk)?;
            }
            decoder.finish()?;
        }
        Compression::Snappy => return decompress_snappy(&data, budget),
        Compression::Lz4 => {
            let input = MeteredInput {
                inner: data.reader(),
                budget,
            };
            let mut decoder = lz4::Decoder::new(input)?;
            io::copy(&mut decoder, &mut out)?;
        }
        Compression::Zstd => {
            out = BudgetedWriter::with_window(budget, zstd_window(&data, budget)?);
            let input = MeteredInput {
                inner: data.reader(),
                budget,
            };
            let mut decoder = zstd::stream::read::Decoder::new(input)?;
            decoder.window_log_max(ZSTD_WINDOW_LOG_MAX)?;
            io::copy(&mut decoder, &mut out)?;
        }
    }
    Ok(out.into_bytes())
}

/// The codec of a batch's attributes; ids past zstd's are CORRUPT_MESSAGE.
fn compression_of(attributes: i16) -> Result<Compression> {
    Ok(match attributes & CODEC_MASK {
        0 => Compression::None,
        1 => Compression::Gzip,
        2 => Compression::Snappy,
        3 => Compression::Lz4,
        ZSTD_CODEC => Compression::Zstd,
        other => return Err(corrupt(format!("unknown compression codec {other}"))),
    })
}

/// Kafka's `ByteUtils.readVarint`: a zigzag varint of at most 5 bytes off the front of
/// `buf`, the bits of the fifth byte past 32 dropped. `None` when it is cut short or its
/// fifth byte continues.
fn read_varint(buf: &mut &[u8]) -> Option<i32> {
    let mut value = 0u32;
    for shift in (0..35).step_by(7) {
        let (&byte, rest) = buf.split_first()?;
        *buf = rest;
        value |= u32::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Some((value >> 1).cast_signed() ^ -(value & 1).cast_signed());
        }
    }
    None
}

/// Kafka's `ByteUtils.readVarlong`: a zigzag varlong of at most 10 bytes, the bits of the
/// tenth byte past 64 dropped.
fn read_varlong(buf: &mut &[u8]) -> Option<i64> {
    let mut value = 0u64;
    for shift in (0..70).step_by(7) {
        let (&byte, rest) = buf.split_first()?;
        *buf = rest;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Some((value >> 1).cast_signed() ^ -(value & 1).cast_signed());
        }
    }
    None
}

/// One record body (`DefaultRecord.readFrom`), read off a records section.
struct RecordReader<'a> {
    body: &'a [u8],
    budget: &'a DecodeBudget,
}

impl<'a> RecordReader<'a> {
    fn varint(&mut self, what: &str) -> Result<i32> {
        read_varint(&mut self.body).ok_or_else(|| invalid(format!("record {what} is invalid")))
    }

    fn charge(&self, n: usize) -> Result<()> {
        self.budget
            .take(n)
            .map_err(|_| corrupt("record batch stopped at the decode limit: its records"))
    }

    /// `len` bytes, or INVALID_RECORD when the body holds fewer.
    fn bytes(&mut self, len: i32, what: &str) -> Result<&'a [u8]> {
        let len = usize::try_from(len)
            .ok()
            .filter(|&n| n <= self.body.len())
            .ok_or_else(|| invalid(format!("record {what} length {len} is invalid")))?;
        let (bytes, rest) = self.body.split_at(len);
        self.body = rest;
        Ok(bytes)
    }

    /// A copy of `bytes`, its heap block charged (the bytes are the section's).
    fn copy(&self, bytes: &[u8]) -> Result<Vec<u8>> {
        if !bytes.is_empty() {
            self.charge(ALLOCATION_OVERHEAD)?;
        }
        Ok(bytes.to_vec())
    }

    /// A length-prefixed byte string; a negative length is null, as in Kafka.
    fn nullable(&mut self, what: &str) -> Result<Option<Vec<u8>>> {
        let len = self.varint(what)?;
        if len < 0 {
            return Ok(None);
        }
        let bytes = self.bytes(len, what)?;
        self.copy(bytes).map(Some)
    }

    /// A header key: never null, read as UTF-8 with replacement characters, as Java's
    /// `Utils.utf8` reads it.
    fn header_key(&mut self) -> Result<String> {
        let len = self.varint("header key length")?;
        if len < 0 {
            return Err(invalid(format!(
                "record header key length {len} is invalid"
            )));
        }
        let bytes = self.bytes(len, "header key")?;
        if bytes.is_empty() {
            return Ok(String::new());
        }
        if let Ok(key) = std::str::from_utf8(bytes) {
            self.charge(ALLOCATION_OVERHEAD)?;
            return Ok(key.to_owned());
        }
        // Every invalid byte may become a 3-byte U+FFFD: the key is built in a block of that
        // size, what is charged, and shrunk to what it holds.
        self.charge(ALLOCATION_OVERHEAD + 2 * bytes.len())?;
        let mut key = String::with_capacity(3 * bytes.len());
        for chunk in bytes.utf8_chunks() {
            key.push_str(chunk.valid());
            if !chunk.invalid().is_empty() {
                key.push(char::REPLACEMENT_CHARACTER);
            }
        }
        key.shrink_to_fit();
        Ok(key)
    }

    /// The record, which must be exactly its fields.
    fn read(mut self, base_timestamp: i64) -> Result<Record> {
        let (_attributes, rest) = self
            .body
            .split_first()
            .ok_or_else(|| invalid("record cut short"))?;
        self.body = rest;
        let timestamp_delta = read_varlong(&mut self.body)
            .ok_or_else(|| invalid("record timestamp delta is invalid"))?;
        self.varint("offset delta")?;
        let key = self.nullable("key")?;
        let value = self.nullable("value")?;
        let count = self.varint("header count")?;
        // Every header takes at least two bytes, which bounds the count by the body.
        let count = usize::try_from(count)
            .ok()
            .filter(|&n| n <= self.body.len() / 2)
            .ok_or_else(|| invalid(format!("record header count {count} is invalid")))?;
        let mut headers = Vec::new();
        if count > 0 {
            self.charge(
                count
                    .saturating_mul(HEADER_MEMORY)
                    .saturating_add(ALLOCATION_OVERHEAD),
            )?;
            headers.reserve_exact(count);
        }
        for _ in 0..count {
            let key = self.header_key()?;
            let value = self.nullable("header value")?.unwrap_or_default();
            headers.push(RecordHeader { key, value });
        }
        if !self.body.is_empty() {
            return Err(invalid(format!(
                "record has {} bytes past its fields",
                self.body.len()
            )));
        }
        Ok(Record {
            key,
            value,
            headers,
            timestamp: Some(base_timestamp.wrapping_add(timestamp_delta)),
        })
    }
}

/// The records of a decoded records section, which must hold exactly `count` well-formed
/// records (Kafka 3.7 refuses anything else as INVALID_RECORD). The records themselves were
/// charged from the count; their heap blocks and headers are charged as they are read.
fn read_records(
    mut section: &[u8],
    count: usize,
    base_timestamp: i64,
    budget: &DecodeBudget,
) -> Result<Vec<Record>> {
    let mut records = Vec::with_capacity(count);
    for read in 0..count {
        let size = read_varint(&mut section)
            .ok_or_else(|| invalid(format!("records section holds {read} records, not {count}")))?;
        let size = usize::try_from(size)
            .ok()
            .filter(|&n| n <= section.len())
            .ok_or_else(|| invalid(format!("record size {size} is invalid")))?;
        let (body, rest) = section.split_at(size);
        records.push(RecordReader { body, budget }.read(base_timestamp)?);
        section = rest;
    }
    if section.is_empty() {
        Ok(records)
    } else {
        Err(invalid(format!(
            "{} bytes remain past the {count} records the batch declares",
            section.len()
        )))
    }
}

/// What Kafka 3.7 checks of a batch that passed the framing before its fields
/// (`UnifiedLog.analyzeAndValidateRecords`), in its order: a base offset other than 0 is
/// INVALID_RECORD; a batch shorter than a v2 header, or a CRC32C that does not match, is
/// CORRUPT_MESSAGE (`DefaultRecordBatch.isValid`); then the codec ([`compression_of`]).
fn check_batch_integrity(batch: &[u8]) -> Result<Compression> {
    let base_offset = (&batch[..8]).get_i64();
    if base_offset != 0 {
        return Err(invalid(format!(
            "record batch base offset is {base_offset}, not 0"
        )));
    }
    if batch.len() < RECORD_BATCH_OVERHEAD {
        return Err(corrupt(format!(
            "record batch length {} is shorter than its header",
            batch.len() - LOG_OVERHEAD
        )));
    }
    let stored_crc = (&batch[CRC_OFFSET..ATTRIBUTES_OFFSET]).get_u32();
    let crc = crc32c::crc32c(&batch[ATTRIBUTES_OFFSET..]);
    if crc != stored_crc {
        return Err(corrupt(format!(
            "record batch CRC {stored_crc:#010x} does not match its contents ({crc:#010x})"
        )));
    }
    compression_of((&batch[ATTRIBUTES_OFFSET..]).get_i16())
}

/// Kafka 3.7's timestamp check of a client's records on a CreateTime topic (the only kind
/// pg_kafka has; `LogValidator.validateTimestamp`): a record of a batch that sets the
/// LogAppendTime bit takes the batch's maxTimestamp as its timestamp, and when that is
/// NO_TIMESTAMP the batch is INVALID_TIMESTAMP. Any other timestamp is accepted, as Kafka's
/// default `log.message.timestamp.{before,after}.max.ms` (unbounded) accept it.
fn check_log_append_time(attributes: i16, max_timestamp: i64) -> Result<()> {
    if attributes & LOG_APPEND_TIME_FLAG != 0 && max_timestamp == NO_TIMESTAMP {
        return Err(KafkaError::Protocol {
            code: ERROR_INVALID_TIMESTAMP,
            message: "clients may not set the LogAppendTime timestamp type".to_string(),
        });
    }
    Ok(())
}

/// Parse a partition's record data (one RecordBatch v2) into its records, charging their
/// memory to `budget` (see the module docs). Checks run in Kafka 3.7's order: the framing
/// (`check_batch_header`, `check_after_batch`), the base offset, header size, CRC32C and
/// codec (`check_batch_integrity`), the header fields (`validate_batch`), what the batch
/// may decode to (`presize`), the records, then their timestamps
/// (`check_log_append_time`, after every record read, as Kafka collects record errors and
/// fails the batch with them once its records are read).
///
/// What a failed parse charged stays charged to `budget` (the caller gives it back,
/// `DecodeBudget::rewind`); the memory it stood for is freed when this returns.
pub fn parse_record_batch(batch_bytes: &Bytes, budget: &DecodeBudget) -> Result<Vec<Record>> {
    let size = check_batch_header(batch_bytes)?;
    check_after_batch(&batch_bytes[size..])?;
    let batch = batch_bytes.slice(..size);
    let Presized { compression, count } = presize(&batch)?;
    let mut fields = &batch[ATTRIBUTES_OFFSET..];
    let attributes = fields.get_i16();
    fields.advance(4); // lastOffsetDelta
    let base_timestamp = fields.get_i64();
    let max_timestamp = (&batch[MAX_TIMESTAMP_OFFSET..]).get_i64();
    debug!("parse_record_batch: {size} bytes, {count} records, {compression:?}");

    let at_the_limit = |what: &dyn std::fmt::Display| {
        corrupt(format!("record batch stopped at the decode limit: {what}"))
    };
    if budget.take(count.saturating_mul(RECORD_MEMORY)).is_err() {
        return Err(at_the_limit(&format!("{count} records")));
    }
    let section = match decompress(batch.slice(RECORD_BATCH_OVERHEAD..), compression, budget) {
        Ok(section) => section,
        Err(e) if e.is::<NeverFits>() => return Err(too_large(e.to_string())),
        Err(e) if budget.exhausted().is_some() => return Err(at_the_limit(&e)),
        Err(e) => return Err(corrupt(format!("record batch does not decompress: {e}"))),
    };
    let records = read_records(&section, count, base_timestamp, budget)?;
    check_log_append_time(attributes, max_timestamp)?;
    Ok(records)
}

#[cfg(test)]
mod tests {
    use super::test_batches::{
        batch_with, header_records, raw_batch, record, tiny_records, v, Header,
    };
    use super::*;
    use bytes::{Bytes, BytesMut};

    use crate::kafka::protocol::decode_budget::{Exhausted, MAX_DECODED_REQUEST_BYTES};
    use std::time::Duration;

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

    /// Every codec checks the decode's time as it reads its input: with no time left a
    /// batch stops at its first check, refused at the limit, whatever its codec.
    #[test]
    fn every_codec_stops_when_the_decode_time_has_passed() {
        for compression in CODECS {
            let batch = zeros_batch(100_000, compression);
            assert!(
                parse_record_batch(&batch, &budget().limit_time(Duration::from_secs(60))).is_ok()
            );
            if compression == Compression::None {
                continue;
            }
            let budget = budget().limit_time(Duration::ZERO);
            let err = parse_record_batch(&batch, &budget).unwrap_err();
            assert_eq!(
                budget.exhausted(),
                Some(Exhausted::Time),
                "{compression:?}: {err}"
            );
            assert!(budget.out_of_time());
        }
    }

    /// Input that decodes to nothing is bounded by the decode's time: 1.7 MB of empty
    /// gzip blocks (some 0.35 s of decoding, charging nothing) stops at a 20 ms limit after
    /// at most one more chunk of input, and snappy's and zstd's framing walks do too.
    #[test]
    fn input_that_decodes_to_nothing_stops_at_the_decode_time() {
        use super::test_batches::gzip_empty_blocks_batch;
        let limit = Duration::from_millis(20);
        let gzip = gzip_empty_blocks_batch(150_000);
        assert!(gzip.len() > 1_500_000, "{} bytes", gzip.len());
        let mut snappy = XERIAL_SNAPPY_HEADER.to_vec();
        for _ in 0..400_000 {
            snappy.extend_from_slice(&[0, 0, 0, 1, 0]);
        }
        let snappy = batch_with(
            &snappy,
            1,
            Header {
                attributes: 2,
                ..Header::default()
            },
        );
        let empty_zstd = zstd::bulk::compress(&[], 1).unwrap();
        let zstd = batch_with(
            &empty_zstd.repeat(200_000),
            1,
            Header {
                attributes: 4,
                ..Header::default()
            },
        );
        for (name, batch) in [("gzip", gzip), ("snappy", snappy), ("zstd", zstd)] {
            let budget = budget().limit_time(limit);
            let (err, spent) = crate::testing::cpu_time(|| parse_record_batch(&batch, &budget));
            assert!(err.is_err(), "{name}");
            assert_eq!(budget.exhausted(), Some(Exhausted::Time), "{name}");
            assert_eq!(
                budget.charged(),
                RECORD_MEMORY,
                "{name}: only the record charged"
            );
            assert!(
                spent < limit + Duration::from_millis(80),
                "{name}: {spent:?}"
            );
        }
    }

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

    /// What the decompressors and record readers hold beyond their charge besides the
    /// decoded bytes: their own state (inflate's window, an lz4 frame's block buffers, a
    /// zstd context), which does not grow with the batch.
    const DECODER_STATE: usize = 512 * 1024;

    /// lz4's frame decoder allocates its block buffers with the C runtime's `malloc`, which
    /// [`crate::testing::heap_peak`] does not see (gzip and snappy decode in Rust; zstd's
    /// contexts allocate through Rust's allocator, the `with-rust-allocator` feature).
    /// This decodes the lz4 frame `frame` with a context created through lz4frame.h's
    /// allocator hook (`LZ4F_createDecompressionContext_advanced`), its allocations going
    /// through Rust's allocator, and returns what it held at most: what decoding the same
    /// frame makes lz4 allocate, as its buffers depend on the frame header only (they are
    /// allocated as it is read and kept to the end).
    mod lz4_c {
        use std::alloc::{alloc, dealloc, Layout};
        use std::ffi::c_void;
        use std::ptr::{null, null_mut};

        /// Bytes before each block holding its allocation size (and keeping malloc's
        /// 16-byte alignment).
        const HEADER: usize = 16;

        unsafe extern "C" fn rust_alloc(_opaque: *mut c_void, size: usize) -> *mut c_void {
            let Ok(layout) = Layout::from_size_align(size.saturating_add(HEADER), HEADER) else {
                return null_mut();
            };
            // SAFETY: the layout's size is at least HEADER, never zero.
            let base = unsafe { alloc(layout) };
            if base.is_null() {
                return null_mut();
            }
            #[expect(
                clippy::cast_ptr_alignment,
                reason = "the allocation is aligned to HEADER, 16 bytes"
            )]
            // SAFETY: `base` is valid for `layout.size()` bytes and aligned for a usize; the
            // block handed out starts HEADER bytes in, within the allocation.
            unsafe {
                base.cast::<usize>().write(layout.size());
                base.add(HEADER).cast()
            }
        }

        unsafe extern "C" fn rust_free(_opaque: *mut c_void, address: *mut c_void) {
            if address.is_null() {
                return;
            }
            // SAFETY: lz4 frees only blocks `rust_alloc` handed out, HEADER bytes past the
            // start of an allocation whose size it wrote there, with this layout.
            unsafe {
                let base = address.cast::<u8>().sub(HEADER);
                #[expect(
                    clippy::cast_ptr_alignment,
                    reason = "the allocation is aligned to HEADER, 16 bytes"
                )]
                let size = base.cast::<usize>().read();
                dealloc(base, Layout::from_size_align_unchecked(size, HEADER));
            }
        }

        /// `LZ4F_CustomMem`.
        #[repr(C)]
        struct CustomMem {
            alloc: unsafe extern "C" fn(*mut c_void, usize) -> *mut c_void,
            calloc: Option<unsafe extern "C" fn(*mut c_void, usize) -> *mut c_void>,
            free: unsafe extern "C" fn(*mut c_void, *mut c_void),
            opaque: *mut c_void,
        }

        /// `LZ4F_VERSION`.
        const LZ4F_VERSION: u32 = 100;

        extern "C" {
            fn LZ4F_createDecompressionContext_advanced(
                custom_mem: CustomMem,
                version: u32,
            ) -> *mut c_void;
            fn LZ4F_decompress(
                ctx: *mut c_void,
                dst: *mut u8,
                dst_size: *mut usize,
                src: *const u8,
                src_size: *mut usize,
                options: *const c_void,
            ) -> usize;
            fn LZ4F_freeDecompressionContext(ctx: *mut c_void) -> usize;
            fn LZ4F_isError(code: usize) -> u32;
        }

        pub(super) fn peak(frame: &[u8]) -> usize {
            let ((), peak) = crate::testing::heap_peak(|| {
                let mem = CustomMem {
                    alloc: rust_alloc,
                    calloc: None,
                    free: rust_free,
                    opaque: null_mut(),
                };
                // SAFETY: `mem`'s functions allocate and free as lz4 expects of malloc and
                // free; the context is freed below.
                let ctx = unsafe { LZ4F_createDecompressionContext_advanced(mem, LZ4F_VERSION) };
                assert!(!ctx.is_null());
                let mut out = vec![0u8; 64 * 1024];
                let mut src = frame;
                loop {
                    let mut dst_size = out.len();
                    let mut src_size = src.len();
                    // SAFETY: `out` and `src` are valid for the sizes passed; lz4 writes back
                    // how much it wrote and read, within them.
                    let hint = unsafe {
                        LZ4F_decompress(
                            ctx,
                            out.as_mut_ptr(),
                            &raw mut dst_size,
                            src.as_ptr(),
                            &raw mut src_size,
                            null(),
                        )
                    };
                    // SAFETY: a pure function of its argument.
                    assert_eq!(unsafe { LZ4F_isError(hint) }, 0, "the frame decodes");
                    src = &src[src_size..];
                    if hint == 0 || (src.is_empty() && dst_size == 0) {
                        break;
                    }
                }
                // SAFETY: `ctx` came from LZ4F_createDecompressionContext_advanced and is
                // not used again.
                unsafe { LZ4F_freeDecompressionContext(ctx) };
            });
            // The 64 KiB output buffer is this measurement's, not lz4's.
            peak - 64 * 1024
        }
    }

    #[test]
    fn a_decode_holds_at_most_twice_its_charge() {
        // Decoded sizes just past a power of two, where an output buffer that doubles as
        // it fills holds the most it ever does beyond its bytes; one record of the value,
        // and 2000 records of a 600-byte value.
        for compression in CODECS {
            for batch in [
                zeros_batch(1_100_000, compression),
                zeros_batch(2_100_000, compression),
                many_records_batch(2000, 600, compression),
            ] {
                let budget = budget();
                let (records, rust) =
                    crate::testing::heap_peak(|| parse_record_batch(&batch, &budget).unwrap());
                let c = if compression == Compression::Lz4 {
                    lz4_c::peak(&batch[RECORD_BATCH_OVERHEAD..])
                } else {
                    0
                };
                let peak = rust + c;
                let charged = budget.most_charged();
                println!(
                    "{compression:?}: held {peak} ({c} by lz4's C side) for a charge of {charged}"
                );
                assert!(
                    peak <= 2 * charged + DECODER_STATE,
                    "{compression:?}: held {peak} bytes for a charge of {charged}"
                );
                // The work limits are three times what a decode keeps.
                assert!(
                    budget.work() <= 3 * budget.charged(),
                    "{compression:?}: worked {} for {} kept",
                    budget.work(),
                    budget.charged()
                );
                drop(records);
            }
        }
    }

    #[test]
    fn lz4_block_buffers_are_fixed_by_the_frame_header() {
        // What lz4 allocates in C for a frame is set by its block size and mode alone, not
        // by how much it decodes: some 2 x 64 KiB at Kafka's (and Java's) default block
        // size, up to 2 x 4 MiB + 128 KiB at the largest a frame may name. It is not
        // charged; at most two inline and two large decodes run at once.
        let frame = |len: usize, block_size| {
            let mut encoder = lz4::EncoderBuilder::new()
                .block_size(block_size)
                .build(Vec::new())
                .unwrap();
            encoder.write_all(&vec![0u8; len]).unwrap();
            let (frame, result) = encoder.finish();
            result.unwrap();
            frame
        };
        for (block_size, block) in [
            (lz4::BlockSize::Max64KB, 64 << 10),
            (lz4::BlockSize::Max4MB, 4 << 20),
        ] {
            let small = lz4_c::peak(&frame(1000, block_size.clone()));
            let large = lz4_c::peak(&frame(20_000_000, block_size));
            assert_eq!(small, large, "{block} bytes a block");
            assert!(
                (2 * block..=2 * block + (128 << 10) + 1024).contains(&large),
                "{block} bytes a block: {large}"
            );
        }
    }

    #[test]
    fn lossy_header_keys_hold_no_more_than_their_charge() {
        // A 1000-byte header key of invalid UTF-8 becomes 3000 bytes of U+FFFD; its charge
        // (its bytes in the section plus 2 per byte and a heap block) covers what the key
        // keeps.
        let invalid = vec![0xffu8; 1000];
        let mut body = vec![0u8, 0, 0, 1, 1, 2];
        body.extend(v(1000));
        body.extend_from_slice(&invalid);
        body.extend(v(-1));
        let budget = budget();
        let (record, held) = crate::testing::heap_peak(|| {
            RecordReader {
                body: &body,
                budget: &budget,
            }
            .read(0)
            .unwrap()
        });
        let key = &record.headers[0].key;
        assert_eq!(key.len(), 3000);
        let charge = budget.charged() + invalid.len();
        assert!(held <= charge, "held {held} bytes for a charge of {charge}");
    }

    /// One record batch of `n` records of a `len`-byte value, compressed with
    /// `compression`.
    fn many_records_batch(n: usize, len: usize, compression: Compression) -> Bytes {
        use kafka_protocol::records::{
            Record as PRecord, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
        };
        let records: Vec<PRecord> = (0..n)
            .map(|i| PRecord {
                transactional: false,
                control: false,
                partition_leader_epoch: 0,
                producer_id: -1,
                producer_epoch: -1,
                timestamp_type: TimestampType::Creation,
                offset: i64::try_from(i).unwrap(),
                sequence: i32::try_from(i).unwrap() - 1,
                timestamp: 0,
                key: Some(Bytes::from(format!("key-{i}"))),
                value: Some(Bytes::from(vec![u8::try_from(i % 251).unwrap(); len])),
                headers: kafka_protocol::indexmap::IndexMap::default(),
            })
            .collect();
        let mut buf = BytesMut::new();
        RecordBatchEncoder::encode(
            &mut buf,
            records.iter(),
            &RecordEncodeOptions {
                version: 2,
                compression,
            },
        )
        .unwrap();
        buf.freeze()
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
                zstd_window(&frame, &budget()).unwrap(),
                1 << window_log,
                "window 2^{window_log}"
            );
            let out = decompress(Bytes::from(frame), Compression::Zstd, &budget());
            assert_eq!(&out.unwrap()[..], &data[..], "window 2^{window_log}");
        }
        let frame = zstd_frame(&data, 19, 28);
        let budget = budget();
        assert!(decompress(Bytes::from(frame), Compression::Zstd, &budget).is_err());
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
                decompress(Bytes::from(frame.clone()), Compression::Zstd, &peak).is_err(),
                "window 2^{window_log} is charged"
            );
            let budget = DecodeBudget::new(data.len() + extra);
            let out = decompress(Bytes::from(frame), Compression::Zstd, &budget).unwrap();
            assert_eq!(out.len(), data.len());
            assert_eq!(budget.charged(), data.len(), "window 2^{window_log}");
        }
        // Concatenated frames: the largest window counts; skippable frames have none.
        let mut frames = vec![0x50, 0x2A, 0x4D, 0x18, 3, 0, 0, 0, 1, 2, 3];
        frames.extend(zstd_frame(b"small", 3, 10));
        frames.extend(zstd_frame(b"large", 3, 26));
        assert_eq!(zstd_window(&frames, &budget()).unwrap(), 1 << 26);
        // A single-segment frame's window is its content size.
        let single = zstd::bulk::compress(&[1u8; 3000], 3).unwrap();
        assert_eq!(zstd_window(&single, &budget()).unwrap(), 3000);
    }

    #[test]
    fn raw_and_framed_snappy_both_decode() {
        let raw = snap::raw::Encoder::new()
            .compress_vec(b"hello snappy")
            .unwrap();
        let budget = budget();
        assert_eq!(
            &decompress(Bytes::from(raw.clone()), Compression::Snappy, &budget).unwrap()[..],
            b"hello snappy"
        );
        let mut framed = XERIAL_SNAPPY_HEADER.to_vec();
        for _ in 0..2 {
            framed.extend_from_slice(&u32::try_from(raw.len()).unwrap().to_be_bytes());
            framed.extend_from_slice(&raw);
        }
        assert_eq!(
            &decompress(Bytes::from(framed.clone()), Compression::Snappy, &budget).unwrap()[..],
            b"hello snappyhello snappy"
        );
        framed.truncate(framed.len() - 1);
        assert!(decompress(Bytes::from(framed), Compression::Snappy, &budget).is_err());
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
                raw_batch(&tiny_records(1), -1, false),
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

        // The full `parse_record_batch` pipeline must never panic or abort on arbitrary
        // bytes: every allocation it makes is charged to the budget first, so an
        // attacker-controlled count or length cannot reach `handle_alloc_error`.
        #[test]
        fn fuzz_parse_record_batch_never_aborts(
            bytes in proptest::collection::vec(proptest::num::u8::ANY, 0..1024),
        ) {
            let _ = parse_record_batch(&Bytes::from(bytes), &budget());
        }

        // Arbitrary records sections behind a valid header and CRC, so the record walk itself
        // sees the bytes.
        #[test]
        fn fuzz_records_sections_never_panic(
            section in proptest::collection::vec(proptest::num::u8::ANY, 0..512),
            count in 0i32..64,
        ) {
            let _ = parse_record_batch(&raw_batch(&section, count, false), &budget());
        }
    }

    #[test]
    fn the_per_record_and_per_header_costs_are_the_documented_ones() {
        // docs/PROTOCOL_DEVIATIONS.md (Compression) quotes these.
        assert_eq!(RECORD_MEMORY, 88);
        assert_eq!(HEADER_MEMORY, 48);
    }

    #[test]
    fn varints_are_zigzag_and_bounded() {
        let read = |bytes: &[u8]| read_varint(&mut &bytes[..]);
        assert_eq!(read(&[0]), Some(0));
        assert_eq!(read(&[1]), Some(-1));
        assert_eq!(read(&[2]), Some(1));
        assert_eq!(read(&[0xac, 0x02]), Some(150));
        assert_eq!(read(&[0xfe, 0xff, 0xff, 0xff, 0x0f]), Some(i32::MAX));
        assert_eq!(read(&[0xff, 0xff, 0xff, 0xff, 0x0f]), Some(i32::MIN));
        assert_eq!(read(&[0x80]), None, "cut short");
        assert_eq!(read(&[0xff; 5]), None, "longer than 5 bytes");
        let mut rest = &[0x02u8, 0x7f][..];
        assert_eq!(read_varint(&mut rest), Some(1));
        assert_eq!(rest, &[0x7f]);
        let long = |bytes: &[u8]| read_varlong(&mut &bytes[..]);
        assert_eq!(long(&[0x80, 0x80, 0x80, 0x80, 0x80, 0x00]), Some(0));
        assert_eq!(
            long(&[0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01]),
            Some(i64::MAX)
        );
        assert_eq!(long(&[0xff; 10]), None, "longer than 10 bytes");
    }

    #[test]
    fn headers_and_heap_blocks_are_charged_as_they_are_read() {
        // One record: key "k" (a heap block), null value, two headers ("a" -> "xy" and
        // "b" -> null): blocks for the key, both header keys, one header value, and the
        // header vector.
        let body = [0u8, 0, 0, 2, b'k', 1, 4, 2, b'a', 4, b'x', b'y', 2, b'b', 1];
        let mut section = vec![u8::try_from(body.len() * 2).unwrap()];
        section.extend_from_slice(&body);
        let budget = budget();
        let records = read_records(&section, 1, 0, &budget).unwrap();
        assert_eq!(records[0].headers.len(), 2);
        assert_eq!(
            budget.charged(),
            2 * HEADER_MEMORY + 5 * ALLOCATION_OVERHEAD
        );
        assert!(
            read_records(&section, 2, 0, &budget).is_err(),
            "one record, not two"
        );
        assert!(read_records(&section[..section.len() - 1], 1, 0, &budget).is_err());
        // A header count the body cannot hold is refused before anything is reserved.
        let mut huge = vec![0u8, 0, 0, 1, 1, 0xfe, 0xff, 0xff, 0xff, 0x07];
        huge.insert(0, u8::try_from(huge.len() * 2).unwrap());
        let err = read_records(&huge, 1, 0, &budget).unwrap_err();
        assert!(err.to_string().contains("header count"), "{err}");
    }

    #[test]
    fn least_decoded_memory_counts_the_declared_records() {
        let batch = raw_batch(&tiny_records(1000), 1000, true);
        assert_eq!(
            least_decoded_memory(&batch, true),
            batch.len() + 1000 * RECORD_MEMORY
        );
        assert_eq!(least_decoded_memory(b"not a batch", true), 0);
        // Batches refused before they decode take nothing: their records do not match
        // their count, or take more than any request may hold.
        let section = tiny_records(1000);
        assert_eq!(
            least_decoded_memory(&raw_batch(&section, 1001, false), true),
            0
        );
        let n = 1_200_000;
        let too_many = raw_batch(&tiny_records(n), i32::try_from(n).unwrap(), false);
        assert_eq!(least_decoded_memory(&too_many, true), 0);
    }

    #[test]
    fn a_batch_of_tiny_records_is_charged_their_memory_before_it_decodes() {
        // 1.5 million 7-byte records: 10 MB decoded, 60 KB of gzip on the wire, and some
        // 130 MB once decoded into records, more than any request may hold. Refused
        // MESSAGE_TOO_LARGE from the declared count, before anything is charged or
        // decompressed.
        let n = 1_500_000;
        let batch = raw_batch(&tiny_records(n), i32::try_from(n).unwrap(), true);
        assert!(batch.len() < 100_000, "{} bytes on the wire", batch.len());
        let budget = budget();
        let (err, spent) = crate::testing::cpu_time(|| parse_record_batch(&batch, &budget));
        let err = err.unwrap_err();
        assert_eq!(code(&err), ERROR_MESSAGE_TOO_LARGE, "{err}");
        assert_eq!(budget.exhausted(), None, "{err}");
        assert_eq!(budget.charged(), 0, "nothing charged");
        assert!(spent < Duration::from_millis(50), "{spent:?}");

        // Within the request limit, the count is charged before anything decompresses:
        // against the inline limit the same shape stops at once, having decoded nothing.
        let small = DecodeBudget::new(1024 * 1024);
        let batch = raw_batch(&tiny_records(150_000), 150_000, true);
        assert!(parse_record_batch(&batch, &small).is_err());
        assert_eq!(small.exhausted(), Some(Exhausted::Limit));
        assert_eq!(small.charged(), 0, "nothing decoded");
    }

    #[test]
    fn records_an_uncompressed_section_cannot_hold_are_refused_before_they_are_charged() {
        use crate::kafka::constants::ERROR_INVALID_RECORD;
        // One 7-byte record declared as 2 million, consistent with lastOffsetDelta: Kafka
        // answers INVALID_RECORD once it runs out of records; so does this, before the
        // 176 MB of records are charged.
        let batch = raw_batch(&tiny_records(1), 2_000_000, false);
        let budget = budget();
        let err = parse_record_batch(&batch, &budget).unwrap_err();
        assert_eq!(code(&err), ERROR_INVALID_RECORD, "{err}");
        assert_eq!(budget.charged(), 0);
        // Seven bytes a record is the least there is: a section that holds them decodes.
        let section = tiny_records(1000);
        assert_eq!(
            parse_record_batch(&raw_batch(&section, 1000, false), &budget)
                .unwrap()
                .len(),
            1000
        );
    }

    #[test]
    fn snappy_declaring_more_than_a_request_may_hold_is_refused_before_it_is_charged() {
        // A raw snappy block whose preamble declares 4 GB - 1, and xerial blocks summing
        // past 100 MB: MESSAGE_TOO_LARGE, nothing charged for the output.
        let raw = batch_with(
            &[0xff, 0xff, 0xff, 0xff, 0x0f, 0, 0, 0, 0],
            1,
            Header {
                attributes: 2,
                ..Header::default()
            },
        );
        let block = [0x80, 0x80, 0x80, 0x19, 0];
        let mut xerial = XERIAL_SNAPPY_HEADER.to_vec();
        for _ in 0..2 {
            xerial.extend_from_slice(&u32::try_from(block.len()).unwrap().to_be_bytes());
            xerial.extend_from_slice(&block);
        }
        let xerial = batch_with(
            &xerial,
            1,
            Header {
                attributes: 2,
                ..Header::default()
            },
        );
        for batch in [raw, xerial] {
            let budget = budget();
            let err = parse_record_batch(&batch, &budget).unwrap_err();
            assert_eq!(code(&err), ERROR_MESSAGE_TOO_LARGE, "{err}");
            assert_eq!(budget.exhausted(), None);
            assert_eq!(budget.charged(), RECORD_MEMORY, "the one record only");
        }
    }

    #[test]
    fn a_batch_of_tiny_records_within_the_limit_holds_what_it_is_charged() {
        let n = 10_000;
        let batch = raw_batch(&tiny_records(n), i32::try_from(n).unwrap(), true);
        let budget = budget();
        let records = parse_record_batch(&batch, &budget).unwrap();
        assert_eq!(records.len(), n);
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
        // takes 48 bytes decoded. They stop at the request limit.
        let batch = raw_batch(&header_records(1000, 5000), 1000, true);
        assert!(batch.len() < 1_000_000);
        let budget = budget();
        let (err, spent) = crate::testing::cpu_time(|| parse_record_batch(&batch, &budget));
        let err = err.unwrap_err();
        assert_eq!(budget.exhausted(), Some(Exhausted::Limit), "{err}");
        assert!(budget.charged() <= MAX_DECODED_REQUEST_BYTES);
        assert!(spent < Duration::from_secs(2), "{spent:?}");

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

    /// A records section of `n` records whose timestamp delta is a 6-byte varlong, and the
    /// key every record holds. A decoder reading that delta as a 5-byte varint takes its last
    /// byte for the offset delta and every later field shifted by one: the key bytes then
    /// read as a null value and `headers` headers.
    fn shifted_varint_section(n: usize, headers: i64) -> (Vec<u8>, Vec<u8>) {
        let mut key = v(-1);
        key.extend(v(headers));
        for i in 0..headers {
            let (high, low) = (u8::try_from(i / 96).unwrap(), u8::try_from(i % 96).unwrap());
            key.extend_from_slice(&[0x04, 0x20 + high, 0x20 + low, 0x01]);
        }
        let key_length = v(i64::try_from(key.len()).unwrap());
        let one = record(&[
            &[0],
            &[0x80, 0x80, 0x80, 0x80, 0x80, 0x00],
            &v(i64::try_from(key_length.len()).unwrap()),
            &key_length,
            &key,
            &v(-1),
            &v(0),
        ]);
        (one.repeat(n), key)
    }

    #[test]
    fn records_decode_as_kafka_reads_their_varints() {
        // Kafka reads the timestamp delta as a varlong of up to 10 bytes. Read as a 5-byte
        // varint, these records each carried 100 headers the walk charged nothing for.
        let (section, key) = shifted_varint_section(10, 100);
        let budget = budget();
        let records = parse_record_batch(&raw_batch(&section, 10, false), &budget).unwrap();
        assert_eq!(records.len(), 10);
        for r in &records {
            assert!(r.headers.is_empty(), "{} headers", r.headers.len());
            assert_eq!(r.key.as_deref(), Some(&key[..]));
            assert_eq!(r.value, None);
            assert_eq!(r.timestamp, Some(0));
        }
        let held: usize = records
            .iter()
            .map(|r| size_of::<Record>() + r.key.as_ref().map_or(0, Vec::len))
            .sum();
        assert!(
            budget.charged() >= held,
            "charged {} for {held}",
            budget.charged()
        );
    }

    #[test]
    fn varints_follow_kafkas_byte_utils() {
        use crate::kafka::constants::ERROR_INVALID_RECORD;
        let one =
            |fields: &[&[u8]]| parse_record_batch(&raw_batch(&record(fields), 1, false), &budget());
        let value = [&v(1)[..], b"v"].concat();
        // A varlong of 10 bytes (bits past 64 dropped) is a timestamp delta; 11 are not.
        let ten = [[0x80u8; 9].as_slice(), &[0x7f]].concat();
        assert!(one(&[&[0], &ten, &v(0), &v(-1), &value, &v(0)]).is_ok());
        let eleven = [[0x80u8; 10].as_slice(), &[0]].concat();
        let err = one(&[&[0], &eleven, &v(0), &v(-1), &value, &v(0)]).unwrap_err();
        assert_eq!(code(&err), ERROR_INVALID_RECORD, "{err}");
        // A varint takes at most 5 bytes; the high bits of the fifth are dropped.
        let five = [0x80, 0x80, 0x80, 0x80, 0x7f];
        assert!(one(&[&[0], &v(0), &five, &v(-1), &value, &v(0)]).is_ok());
        let six = [0x80, 0x80, 0x80, 0x80, 0x80, 0x00];
        let err = one(&[&[0], &v(0), &six, &v(-1), &value, &v(0)]).unwrap_err();
        assert_eq!(code(&err), ERROR_INVALID_RECORD, "{err}");
        let key_length_one = [0x82, 0x80, 0x80, 0x80, 0x10];
        let r = one(&[&[0], &v(0), &v(0), &key_length_one, b"k", &value, &v(0)]).unwrap();
        assert_eq!(r[0].key.as_deref(), Some(&b"k"[..]));
        let overlong_one = [0x82, 0x80, 0x80, 0x80, 0x00];
        let r = one(&[&[0], &v(0), &v(0), &overlong_one, b"k", &value, &v(0)]).unwrap();
        assert_eq!(r[0].key.as_deref(), Some(&b"k"[..]));
        // Any negative key, value or header value length is null; a negative header key
        // length or header count is invalid.
        let r = one(&[
            &[0],
            &v(0),
            &v(0),
            &v(-5),
            &v(-7),
            &v(1),
            &v(1),
            b"a",
            &v(-9),
        ])
        .unwrap();
        assert_eq!((r[0].key.as_ref(), r[0].value.as_ref()), (None, None));
        assert_eq!(r[0].headers.len(), 1);
        assert_eq!(r[0].headers[0].value, b"");
        for bad in [
            one(&[&[0], &v(0), &v(0), &v(-1), &value, &v(1), &v(-1), &v(-1)]),
            one(&[&[0], &v(0), &v(0), &v(-1), &value, &v(-1)]),
        ] {
            assert_eq!(code(&bad.unwrap_err()), ERROR_INVALID_RECORD);
        }
    }

    #[test]
    fn headers_keep_their_order_and_duplicates() {
        // Kafka keeps every header as sent, duplicate keys included, and reads a key that is
        // not UTF-8 with replacement characters, as Java's String does.
        let section = record(&[
            &[0],
            &v(0),
            &v(0),
            &v(-1),
            &v(1),
            b"h",
            &v(3),
            &v(1),
            b"a",
            &v(1),
            b"1",
            &v(1),
            b"a",
            &v(1),
            b"2",
            &v(2),
            &[0xff, 0xfe],
            &v(-1),
        ]);
        let records = parse_record_batch(&raw_batch(&section, 1, false), &budget()).unwrap();
        let headers: Vec<(&str, &[u8])> = records[0]
            .headers
            .iter()
            .map(|h| (h.key.as_str(), h.value.as_slice()))
            .collect();
        assert_eq!(
            headers,
            [("a", &b"1"[..]), ("a", b"2"), ("\u{fffd}\u{fffd}", b"")]
        );
    }

    #[test]
    fn timestamp_deltas_are_varlongs() {
        // A record 30 days older than the batch's first, and one 2^40 ms later: deltas past
        // the 32 bits of a varint.
        let base = 1_763_600_000_000i64;
        let month = 30 * 24 * 3600 * 1000i64;
        let section = [
            record(&[&[0], &v(0), &v(0), &v(-1), &v(1), b"u", &v(0)]),
            record(&[&[0], &v(-month), &v(1), &v(-1), &v(1), b"t", &v(0)]),
            record(&[&[0], &v(1 << 40), &v(2), &v(-1), &v(-1), &v(0)]),
        ]
        .concat();
        let header = Header {
            base_timestamp: base,
            ..Header::default()
        };
        let records = parse_record_batch(&batch_with(&section, 3, header), &budget()).unwrap();
        let timestamps: Vec<Option<i64>> = records.iter().map(|r| r.timestamp).collect();
        assert_eq!(
            timestamps,
            [Some(base), Some(base - month), Some(base + (1 << 40))]
        );
    }

    #[test]
    fn batch_headers_are_validated_as_kafka_validates_them() {
        use crate::kafka::constants::{ERROR_CORRUPT_MESSAGE, ERROR_INVALID_RECORD};
        // Each case as Kafka 3.7 answers it.
        let x = record(&[&[0], &v(0), &v(0), &v(-1), &v(1), b"x", &v(0)]);
        let xy = [
            x.clone(),
            record(&[&[0], &v(0), &v(1), &v(-1), &v(1), b"y", &v(0)]),
        ]
        .concat();
        let h = Header::default();
        let ok = batch_with(&x, 1, h);
        let then = |tail: &[u8]| Bytes::from([&ok[..], tail].concat());
        let prefix = |length: i32, magic: i8| {
            let mut b = 0i64.to_be_bytes().to_vec();
            b.extend_from_slice(&length.to_be_bytes());
            b.extend_from_slice(&0i32.to_be_bytes());
            b.extend_from_slice(&magic.to_be_bytes());
            b
        };
        let with_magic = |magic: u8| {
            let mut b = ok.to_vec();
            b[16] = magic;
            Bytes::from(b)
        };
        let mut bad_crc = batch_with(&[], 0, h).to_vec();
        bad_crc[17..21].copy_from_slice(&[0; 4]);
        let short_v2 = [prefix(30, 2), vec![0; 29]].concat();
        let mut base_5_bad_crc = batch_with(
            &x,
            1,
            Header {
                base_offset: 5,
                ..h
            },
        )
        .to_vec();
        base_5_bad_crc[17..21].copy_from_slice(&[0; 4]);
        let mut short_at_5 = [prefix(30, 2), vec![0; 25]].concat();
        short_at_5[..8].copy_from_slice(&5i64.to_be_bytes());
        let log_append_time = |section: &[u8], attributes: i16| {
            batch_with(
                section,
                1,
                Header {
                    attributes: attributes | 0x08,
                    max_timestamp: Some(-1),
                    ..h
                },
            )
        };
        let mut gzip = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        gzip.write_all(&x).unwrap();
        let gzip_x = gzip.finish().unwrap();
        let refused: [(&str, Bytes, i16); 22] = [
            ("no records", batch_with(&[], 0, h), ERROR_INVALID_RECORD),
            (
                "no records of an idempotent producer",
                batch_with(
                    &[],
                    0,
                    Header {
                        producer_id: 7,
                        producer_epoch: 0,
                        base_sequence: 0,
                        last_offset_delta: Some(0),
                        ..h
                    },
                ),
                ERROR_INVALID_RECORD,
            ),
            (
                "lastOffsetDelta past the records",
                batch_with(
                    &xy,
                    2,
                    Header {
                        last_offset_delta: Some(5),
                        ..h
                    },
                ),
                ERROR_INVALID_RECORD,
            ),
            (
                "lastOffsetDelta short of the records",
                batch_with(
                    &xy,
                    2,
                    Header {
                        last_offset_delta: Some(0),
                        ..h
                    },
                ),
                ERROR_INVALID_RECORD,
            ),
            (
                "a control batch",
                batch_with(
                    &x,
                    1,
                    Header {
                        attributes: 0x20,
                        ..h
                    },
                ),
                ERROR_INVALID_RECORD,
            ),
            (
                "a base offset other than 0",
                batch_with(
                    &x,
                    1,
                    Header {
                        base_offset: 5,
                        ..h
                    },
                ),
                ERROR_INVALID_RECORD,
            ),
            (
                "a producer id without a sequence",
                batch_with(
                    &x,
                    1,
                    Header {
                        producer_id: 7,
                        producer_epoch: 0,
                        ..h
                    },
                ),
                ERROR_INVALID_RECORD,
            ),
            ("magic 9", with_magic(9), ERROR_CORRUPT_MESSAGE),
            ("magic -1", with_magic(0xff), ERROR_CORRUPT_MESSAGE),
            (
                "bad CRC, no records",
                Bytes::from(bad_crc),
                ERROR_CORRUPT_MESSAGE,
            ),
            ("12 zero bytes after", then(&[0; 12]), ERROR_CORRUPT_MESSAGE),
            (
                "a magic 9 header after",
                then(&prefix(100, 9)),
                ERROR_CORRUPT_MESSAGE,
            ),
            (
                "a complete short batch after",
                then(&short_v2),
                ERROR_INVALID_RECORD,
            ),
            (
                "a cut-short short batch",
                Bytes::from([prefix(30, 2), vec![0; 3]].concat()),
                ERROR_INVALID_RECORD,
            ),
            (
                "a short batch, then a batch",
                Bytes::from([&short_v2[..], &ok[..]].concat()),
                ERROR_CORRUPT_MESSAGE,
            ),
            // Kafka checks the base offset before the batch's size and CRC. The codec is
            // checked before the header fields here too; an unknown codec id is
            // CORRUPT_MESSAGE, where Kafka answers UNKNOWN_SERVER_ERROR whatever the header
            // fields hold (a documented deviation).
            (
                "a base offset other than 0 and a bad CRC",
                Bytes::from(base_5_bad_crc),
                ERROR_INVALID_RECORD,
            ),
            (
                "a short batch at base offset 5",
                Bytes::from(short_at_5),
                ERROR_INVALID_RECORD,
            ),
            (
                "an unknown codec on a control batch",
                batch_with(
                    &x,
                    1,
                    Header {
                        attributes: 0x25,
                        ..h
                    },
                ),
                ERROR_CORRUPT_MESSAGE,
            ),
            // The LogAppendTime bit without a maxTimestamp, once the batch and its records
            // passed every other check.
            (
                "LogAppendTime without a maxTimestamp",
                log_append_time(&x, 0),
                ERROR_INVALID_TIMESTAMP,
            ),
            (
                "LogAppendTime without a maxTimestamp, gzip",
                log_append_time(&gzip_x, 1),
                ERROR_INVALID_TIMESTAMP,
            ),
            (
                "LogAppendTime without a maxTimestamp, a control batch",
                log_append_time(&x, 0x20),
                ERROR_INVALID_RECORD,
            ),
            (
                "LogAppendTime without a maxTimestamp, a record with a byte too many",
                log_append_time(
                    &record(&[&[0], &v(0), &v(0), &v(-1), &v(-1), &v(0), &[0]]),
                    0,
                ),
                ERROR_INVALID_RECORD,
            ),
        ];
        for (what, data, expected) in refused {
            let err = parse_record_batch(&data, &budget()).unwrap_err();
            assert_eq!(code(&err), expected, "{what}: {err}");
        }
        // Kafka ignores data after the batch that holds no complete batch, takes the
        // records' offset deltas as they are, and sets the timestamp type itself.
        let accepted: [(&str, Bytes, usize); 6] = [
            ("5 bytes after", then(&[0; 5]), 1),
            ("a batch length after", then(&prefix(20, 2)[..12]), 1),
            ("a cut-short batch after", then(&prefix(100, 2)), 1),
            (
                "offset deltas other than 0, 1, ...",
                batch_with(
                    &[
                        x.clone(),
                        record(&[&[0], &v(0), &v(7), &v(-1), &v(1), b"y", &v(0)]),
                    ]
                    .concat(),
                    2,
                    h,
                ),
                2,
            ),
            (
                "the log-append-time bit with a maxTimestamp",
                batch_with(
                    &x,
                    1,
                    Header {
                        attributes: 0x08,
                        ..h
                    },
                ),
                1,
            ),
            (
                "CreateTime records without timestamps",
                batch_with(
                    &x,
                    1,
                    Header {
                        base_timestamp: -1,
                        ..h
                    },
                ),
                1,
            ),
        ];
        for (what, data, records) in accepted {
            let parsed = parse_record_batch(&data, &budget());
            assert_eq!(parsed.map(|r| r.len()).ok(), Some(records), "{what}");
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
                is_transactional: false,
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

    /// The header fields of a batch built by hand ([`batch_with`]).
    #[derive(Debug, Clone, Copy)]
    pub(crate) struct Header {
        pub(crate) base_offset: i64,
        /// Attributes, codec bits included.
        pub(crate) attributes: i16,
        /// `None`: one less than the record count (at least 0).
        pub(crate) last_offset_delta: Option<i32>,
        pub(crate) base_timestamp: i64,
        /// `None`: the base timestamp.
        pub(crate) max_timestamp: Option<i64>,
        pub(crate) producer_id: i64,
        pub(crate) producer_epoch: i16,
        pub(crate) base_sequence: i32,
    }

    impl Default for Header {
        fn default() -> Self {
            Self {
                base_offset: 0,
                attributes: 0,
                last_offset_delta: None,
                base_timestamp: 0,
                max_timestamp: None,
                producer_id: -1,
                producer_epoch: -1,
                base_sequence: -1,
            }
        }
    }

    /// A RecordBatch v2 built by hand around the (already compressed, if the attributes
    /// say so) records section `records`, declaring `count` records.
    pub(crate) fn batch_with(records: &[u8], count: i32, header: Header) -> Bytes {
        let mut tail = BytesMut::new();
        tail.put_i16(header.attributes);
        tail.put_i32(header.last_offset_delta.unwrap_or_else(|| count.max(1) - 1));
        tail.put_i64(header.base_timestamp);
        tail.put_i64(header.max_timestamp.unwrap_or(header.base_timestamp));
        tail.put_i64(header.producer_id);
        tail.put_i16(header.producer_epoch);
        tail.put_i32(header.base_sequence);
        tail.put_i32(count);
        tail.put_slice(records);
        let mut batch = BytesMut::new();
        batch.put_i64(header.base_offset);
        batch.put_i32(i32::try_from(9 + tail.len()).unwrap());
        batch.put_i32(0);
        batch.put_i8(2);
        batch.put_u32(crc32c(&tail));
        batch.put_slice(&tail);
        batch.freeze()
    }

    /// A RecordBatch v2 built by hand around the records section `section`, declaring
    /// `count` records and compressed with gzip when `gzip`.
    pub(crate) fn raw_batch(section: &[u8], count: i32, gzip: bool) -> Bytes {
        if !gzip {
            return batch_with(section, count, Header::default());
        }
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::best());
        encoder.write_all(section).unwrap();
        let header = Header {
            attributes: 1,
            ..Header::default()
        };
        batch_with(&encoder.finish().unwrap(), count, header)
    }

    /// `v` as a zigzag varint (any length up to 10 bytes) appended to `out`.
    pub(crate) fn varint(v: i64, out: &mut Vec<u8>) {
        let mut z = ((v << 1) ^ (v >> 63)).cast_unsigned();
        while z >= 0x80 {
            out.push(u8::try_from(z & 0x7f).unwrap() | 0x80);
            z >>= 7;
        }
        out.push(u8::try_from(z).unwrap());
    }

    /// One record: its body (the concatenated `fields`) behind its varint length.
    pub(crate) fn record(fields: &[&[u8]]) -> Vec<u8> {
        let body = fields.concat();
        let mut record = Vec::new();
        varint(i64::try_from(body.len()).unwrap(), &mut record);
        record.extend_from_slice(&body);
        record
    }

    /// `v` as a zigzag varint.
    pub(crate) fn v(v: i64) -> Vec<u8> {
        let mut out = Vec::new();
        varint(v, &mut out);
        out
    }

    /// A gzip stream of `blocks` dynamic-Huffman deflate blocks that decode to nothing
    /// (each declares its own code tables, some 11 bytes), then a final empty block: the
    /// input that costs a decoder most per byte, decoding no output.
    pub(crate) fn gzip_of_empty_blocks(blocks: usize) -> Vec<u8> {
        struct Bits {
            acc: u64,
            n: u32,
            out: Vec<u8>,
        }
        impl Bits {
            fn put(&mut self, value: u64, bits: u32) {
                self.acc |= value << self.n;
                self.n += bits;
                while self.n >= 8 {
                    self.out.push(u8::try_from(self.acc & 0xff).unwrap());
                    self.acc >>= 8;
                    self.n -= 8;
                }
            }
        }
        // The order code-length code lengths are sent in (RFC 1951 §3.2.7).
        const ORDER: [u8; 18] = [16, 17, 18, 0, 8, 7, 9, 6, 10, 5, 11, 4, 12, 3, 13, 2, 14, 1];
        let mut bits = Bits {
            acc: 0,
            n: 0,
            out: vec![0x1f, 0x8b, 8, 0, 0, 0, 0, 0, 0, 0xff],
        };
        for _ in 0..blocks {
            // Not final, dynamic; 257 literal/length codes, 1 distance code, 18
            // code-length codes, of which only 1 and 18 have (1-bit) codes.
            bits.put(0, 1);
            bits.put(2, 2);
            bits.put(0, 5);
            bits.put(0, 5);
            bits.put(14, 4);
            for symbol in ORDER {
                bits.put(u64::from(symbol == 1 || symbol == 18), 3);
            }
            // Literals 0-255 have no code (18 repeats zero: 138 + 118), the end of block
            // and the one distance code length 1.
            for (value, n) in [(0, 1), (1, 1), (127, 7), (1, 1), (106, 7), (0, 1), (0, 1)] {
                bits.put(value, n);
            }
            bits.put(1, 1); // end of block
        }
        bits.put(1, 1); // final, fixed: end of block (seven zero bits)
        bits.put(1, 2);
        bits.put(0, 7);
        if bits.n > 0 {
            bits.out.push(u8::try_from(bits.acc & 0xff).unwrap());
        }
        bits.out.extend_from_slice(&[0; 8]); // CRC-32 and length of nothing
        bits.out
    }

    /// A batch of one record whose gzip section is [`gzip_of_empty_blocks`].
    pub(crate) fn gzip_empty_blocks_batch(blocks: usize) -> Bytes {
        batch_with(
            &gzip_of_empty_blocks(blocks),
            1,
            Header {
                attributes: 1,
                ..Header::default()
            },
        )
    }

    /// `n` of the smallest records: 7 bytes each (null key and value, no headers).
    pub(crate) fn tiny_records(n: usize) -> Vec<u8> {
        [12u8, 0, 0, 0, 1, 1, 0].repeat(n)
    }

    /// `n` records of `headers` headers each (an empty key and a null value: 2 bytes).
    pub(crate) fn header_records(n: usize, headers: usize) -> Vec<u8> {
        let mut body = vec![0u8, 0, 0, 1, 1];
        varint(i64::try_from(headers).unwrap(), &mut body);
        body.extend_from_slice(&[0, 1].repeat(headers));
        let mut record = Vec::new();
        varint(i64::try_from(body.len()).unwrap(), &mut record);
        record.extend_from_slice(&body);
        record.repeat(n)
    }
}
