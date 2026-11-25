//! Fetch and ListOffsets: reading records (isolation-aware, re-encoded as RecordBatch v2) and
//! resolving offsets by position or timestamp.

use super::helpers::{merge_by_partition, resolve_topics};
use crate::kafka::constants::{
    ERROR_INVALID_REQUEST, ERROR_KAFKA_STORAGE_ERROR, ERROR_NONE, ERROR_OFFSET_OUT_OF_RANGE,
    ERROR_UNKNOWN_TOPIC_OR_PARTITION,
};
use crate::kafka::error::Result;
use crate::kafka::handler_context::HandlerContext;
use crate::kafka::messages::{
    ListOffsetsPartitionData, ListOffsetsTopicData, PartitionFetchData, TopicFetchData,
};
use crate::kafka::storage::{
    record_fetch_bytes, FetchedMessage, IsolationLevel, KafkaStore, PartitionBounds,
};
use bytes::BufMut;
use kafka_protocol::indexmap::IndexMap;
use kafka_protocol::messages::fetch_response::{
    FetchResponse, FetchableTopicResponse, PartitionData,
};
use kafka_protocol::messages::list_offsets_response::{
    ListOffsetsPartitionResponse, ListOffsetsResponse, ListOffsetsTopicResponse,
};
use kafka_protocol::messages::TopicName;
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{
    Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};
use std::collections::{HashMap, HashSet};

/// Handle Fetch request
///
/// Fetches messages from storage and encodes them as Kafka RecordBatch.
/// The compression parameter controls the encoding compression for the response.
///
/// Added isolation_level support:
/// - ReadUncommitted (0): Returns all messages including pending transactions
/// - ReadCommitted (1): Only returns committed messages (skips pending/aborted)
///
/// For ReadCommitted consumers, last_stable_offset (LSO) indicates the lowest
/// offset of any pending transaction. Consumers can safely process up to LSO.
pub fn handle_fetch(
    ctx: &HandlerContext,
    topic_data: Vec<TopicFetchData>,
    isolation_level: IsolationLevel,
) -> Result<FetchResponse> {
    handle_fetch_with_max_bytes(ctx, topic_data, isolation_level, i32::MAX, true)
}

/// The bytes one record adds to a Fetch response, the size both byte limits
/// (`partition_max_bytes` and the request-level `max_bytes`) count: the uncompressed key,
/// value and header keys and values, 84 bytes per record and 10 per header for its framing
/// (what a record alone in its uncompressed RecordBatch adds at most, see
/// [`crate::kafka::storage::RECORD_OVERHEAD`]; a compressed batch can take more, which
/// [`encode_within_limit`] checks).
/// [`record_fetch_bytes`] is also what Produce stores with each row for the storage layer's
/// own accounting, so storage and this handler cut a partition at the same record.
pub(crate) fn message_size_estimate(msg: &FetchedMessage) -> i64 {
    record_fetch_bytes(
        msg.key.as_ref().map_or(0, Vec::len),
        msg.value.as_ref().map_or(0, Vec::len),
        msg.headers.iter().map(|(k, v)| (k.len(), v.len())),
    )
}

/// The byte limit to pass to storage for one partition, or `None` to return no
/// records for it. `remaining` is what is left of the request-level `max_bytes`;
/// `returned_any` is whether an earlier partition already returned records. As in
/// Kafka (KIP-74), the first partition with data always returns at least one message
/// (storage guarantees that), so the response makes progress even when one message
/// exceeds the budget; later partitions only return what still fits.
pub(crate) fn partition_fetch_limit(
    partition_max_bytes: i32,
    remaining: i64,
    returned_any: bool,
) -> Option<i32> {
    if returned_any && remaining <= 0 {
        return None;
    }
    let limit = i64::from(partition_max_bytes).min(remaining.max(0));
    // At most partition_max_bytes, so the conversion cannot fail.
    Some(i32::try_from(limit).unwrap_or(partition_max_bytes))
}

/// Drop trailing messages past `limit`, the partition's own limit: its
/// `partition_max_bytes` within what is left of the request-level `max_bytes` (see
/// [`partition_fetch_limit`]). The first message is kept whatever its size when no earlier
/// partition returned records, as Kafka returns the first record batch of the first
/// partition with data. Returns the kept messages and their estimated size.
pub(crate) fn trim_to_response_budget(
    messages: Vec<FetchedMessage>,
    limit: i64,
    returned_any: bool,
) -> (Vec<FetchedMessage>, i64) {
    let mut kept = Vec::with_capacity(messages.len());
    let mut used = 0i64;
    for msg in messages {
        let size = message_size_estimate(&msg);
        let first_of_response = !returned_any && kept.is_empty();
        if !first_of_response && used + size > limit {
            break;
        }
        used += size;
        kept.push(msg);
    }
    (kept, used)
}

/// The records of fetched rows, for RecordBatch v2. Consecutive offsets written by the
/// same producer id and epoch form one batch (rows without a producer id, i.e. not
/// written by a transactional producer, likewise): the batch header (61 bytes) and the
/// compression are paid per batch instead of per record. Batches of transactional rows
/// carry the producer id and epoch and the transactional flag; the base sequence is not
/// stored and is sent as -1. Rows are in offset order.
fn batch_records(messages: Vec<FetchedMessage>) -> std::result::Result<Vec<Record>, String> {
    let mut records: Vec<Record> = Vec::with_capacity(messages.len());
    let mut group_base = 0i64;
    // Smallest and largest timestamp of the current batch: the encoder stores each
    // record's timestamp as an i32 delta from the batch minimum, so a record that would
    // stretch the span past i32::MAX milliseconds starts a new batch.
    let mut group_timestamps = (0i64, 0i64);
    let mut previous: Option<(i64, Option<i64>, Option<i16>)> = None;
    for msg in messages {
        let producer = (msg.producer_id, msg.producer_epoch);
        let span_fits = || {
            let (min, max) = group_timestamps;
            msg.timestamp
                .max(max)
                .saturating_sub(msg.timestamp.min(min))
                <= i64::from(i32::MAX)
        };
        let continues = previous.is_some_and(|(offset, pid, epoch)| {
            msg.partition_offset == offset + 1 && (pid, epoch) == producer
        }) && span_fits();
        if continues {
            group_timestamps = (
                group_timestamps.0.min(msg.timestamp),
                group_timestamps.1.max(msg.timestamp),
            );
        } else {
            group_base = msg.partition_offset;
            group_timestamps = (msg.timestamp, msg.timestamp);
        }
        previous = Some((msg.partition_offset, msg.producer_id, msg.producer_epoch));
        // The encoder keeps records in one batch while `offset - sequence` stays the
        // same: -1 + (offset - group start) gives every group its own constant and a
        // base sequence of -1 (NO_SEQUENCE).
        let sequence = i32::try_from(msg.partition_offset - group_base).map_err(|_| {
            format!("batch at offset {group_base} spans more than i32::MAX offsets")
        })? - 1;
        let mut record = Record {
            transactional: msg.producer_id.is_some(),
            control: false,
            partition_leader_epoch: 0,
            producer_id: msg.producer_id.unwrap_or(-1),
            producer_epoch: msg.producer_epoch.unwrap_or(-1),
            timestamp_type: TimestampType::Creation,
            offset: msg.partition_offset,
            sequence,
            timestamp: msg.timestamp,
            key: msg.key.map(bytes::Bytes::from),
            value: msg.value.map(bytes::Bytes::from),
            headers: IndexMap::default(),
        };
        // Return stored headers to the consumer.
        for (k, v) in msg.headers {
            record
                .headers
                .insert(StrBytes::from_string(k), Some(bytes::Bytes::from(v)));
        }
        records.push(record);
    }
    Ok(records)
}

/// Encode records built by [`batch_records`] (a prefix of them encodes as the batches of
/// the rows it holds: the encoder splits batches on each record's producer and sequence).
fn encode_records(
    records: &[Record],
    compression: Compression,
) -> std::result::Result<bytes::Bytes, String> {
    let mut encoded = bytes::BytesMut::new();
    RecordBatchEncoder::encode(
        &mut encoded,
        records.iter(),
        &RecordEncodeOptions {
            version: 2,
            compression,
        },
    )
    .map_err(|e| e.to_string())?;
    Ok(encoded.freeze())
}

/// Encode `messages`, already trimmed to `limit` by their estimates
/// ([`trim_to_response_budget`]), as record batches of at most `limit` bytes, and the bytes
/// they count against the response budget: their estimated size, or their encoded size
/// when larger. `None` when no record fits.
///
/// The estimate bounds what a record adds to an uncompressed batch, not to a compressed
/// one: a codec adds its own framing per batch (gzip some 18 bytes, the xerial snappy
/// header 16 and 4 per block, an lz4 frame 15 to 19, zstd a frame header) and per block
/// (gzip about 5 bytes per 64 KiB of incompressible data), so records that compress
/// poorly can encode past what their estimates sum to. Trailing records are then dropped
/// until the batches fit, but for the first record of the response (`returned_any`
/// false), which is returned alone whatever its size, as Kafka returns the first record
/// batch.
fn encode_within_limit(
    messages: Vec<FetchedMessage>,
    compression: Compression,
    limit: i64,
    returned_any: bool,
) -> std::result::Result<Option<(bytes::Bytes, i64)>, String> {
    let estimates: Vec<i64> = messages.iter().map(message_size_estimate).collect();
    let records = batch_records(messages)?;
    let min_kept = usize::from(!returned_any);
    let mut kept = records.len();
    while kept > 0 {
        let encoded = encode_records(&records[..kept], compression)?;
        let len = i64::try_from(encoded.len()).unwrap_or(i64::MAX);
        if len <= limit || kept <= min_kept {
            let counted = estimates[..kept].iter().sum::<i64>().max(len);
            return Ok(Some((encoded, counted)));
        }
        // Drop the trailing records whose estimates cover the excess (at least one) and
        // encode again: what they leave out of the encoding can be less than their estimates.
        let mut excess = len - limit;
        while kept > min_kept && excess > 0 {
            kept -= 1;
            excess -= estimates[kept];
        }
    }
    Ok(None)
}

/// Bytes of a RecordBatch v2 header (everything but the records).
const BATCH_HEADER_LEN: usize = 61;

/// The one record of a [`gap_batch`]: a control record (key: version 0, type 1 =
/// COMMIT; value: an EndTransactionMarker, version 0, coordinator epoch 0) at offset
/// delta `offset_delta`, timestamp delta 0.
fn gap_marker_record(offset_delta: i32) -> bytes::Bytes {
    #[expect(
        clippy::cast_possible_truncation,
        reason = "each byte takes the low 7 bits of the zigzag value by design"
    )]
    fn varint(out: &mut bytes::BytesMut, v: i64) {
        let mut z = ((v << 1) ^ (v >> 63)).cast_unsigned();
        while z >= 0x80 {
            out.put_u8((z as u8) | 0x80);
            z >>= 7;
        }
        out.put_u8(z as u8);
    }
    let mut body = bytes::BytesMut::new();
    body.put_i8(0); // attributes
    varint(&mut body, 0); // timestamp delta
    varint(&mut body, i64::from(offset_delta));
    varint(&mut body, 4); // key length
    body.put_i16(0); // control record key version
    body.put_i16(1); // control record type: COMMIT
    varint(&mut body, 6); // value length
    body.put_i16(0); // EndTransactionMarker version
    body.put_i32(0); // coordinator epoch
    varint(&mut body, 0); // header count
    let mut out = bytes::BytesMut::new();
    varint(
        &mut out,
        i64::try_from(body.len()).expect("a control record is a few bytes"),
    );
    out.put(body);
    out.freeze()
}

/// A batch covering offsets `[from, to)` without data, or `None` when the
/// range is empty: a control batch holding one COMMIT marker at offset `to - 1`, like
/// the marker Kafka writes when a transaction ends. Clients skip control records but
/// move their position past them: the Java client to the batch's last offset + 1,
/// librdkafka to the marker's offset + 1 (it advances the application position, and
/// so the committed offset and the lag, only on a record, which is why the batch is not
/// simply empty). A range longer than `i32::MAX + 1` offsets is covered up to that
/// length; the next fetch covers the rest.
pub(crate) fn gap_batch(from: i64, to: i64) -> Option<bytes::Bytes> {
    if to <= from {
        return None;
    }
    let last_offset_delta = i32::try_from(to - from - 1).unwrap_or(i32::MAX);
    let record = gap_marker_record(last_offset_delta);
    // Everything after the CRC field: what the CRC covers.
    let mut body = bytes::BytesMut::with_capacity(BATCH_HEADER_LEN - 21 + record.len());
    body.put_i16(0x30); // attributes: transactional | control, no compression
    body.put_i32(last_offset_delta);
    body.put_i64(-1); // base timestamp (none)
    body.put_i64(-1); // max timestamp
    body.put_i64(-1); // producer id
    body.put_i16(-1); // producer epoch
    body.put_i32(-1); // base sequence
    body.put_i32(1); // record count
    body.put(record);
    let mut out = bytes::BytesMut::with_capacity(BATCH_HEADER_LEN + body.len());
    out.put_i64(from); // base offset
                       // Batch length (after this field): a fixed header and one control record.
    out.put_i32(i32::try_from(4 + 1 + 4 + body.len()).expect("a gap batch is under 100 bytes"));
    out.put_i32(0); // partition leader epoch
    out.put_i8(2); // magic
    out.put_u32(crc32c(&body));
    out.put(body);
    Some(out.freeze())
}

/// CRC-32C (Castagnoli), the RecordBatch v2 checksum.
fn crc32c(data: &[u8]) -> u32 {
    #[expect(
        clippy::cast_possible_truncation,
        reason = "i < 256; TryFrom is not const"
    )]
    const fn table() -> [u32; 256] {
        let mut t = [0u32; 256];
        let mut i = 0;
        while i < 256 {
            let mut c = i as u32;
            let mut k = 0;
            while k < 8 {
                c = if c & 1 != 0 {
                    (c >> 1) ^ 0x82F6_3B78
                } else {
                    c >> 1
                };
                k += 1;
            }
            t[i] = c;
            i += 1;
        }
        t
    }
    static TABLE: [u32; 256] = table();
    let mut crc = 0xFFFF_FFFFu32;
    for &b in data {
        crc = TABLE[((crc ^ u32::from(b)) & 0xFF) as usize] ^ (crc >> 8);
    }
    crc ^ 0xFFFF_FFFF
}

/// [`handle_fetch`] honouring the request-level `max_bytes` (Fetch v3+): partitions
/// are filled in request order until the response budget is spent, in addition to
/// each partition's own `partition_max_bytes`. `whole_first_record` returns the first record
/// of the first partition with data even when it is larger than both (KIP-74); without it
/// no record past the limits is returned.
pub fn handle_fetch_with_max_bytes(
    ctx: &HandlerContext,
    topic_data: Vec<TopicFetchData>,
    isolation_level: IsolationLevel,
    max_bytes: i32,
    whole_first_record: bool,
) -> Result<FetchResponse> {
    let store = ctx.store;
    // Without `whole_first_record` every partition is read as if an earlier one had
    // returned records: nothing past its limits is returned.
    let mut budget = FetchBudget {
        remaining: i64::from(max_bytes.max(0)),
        returned_any: !whole_first_record,
    };
    let settings = FetchSettings {
        isolation_level,
        compression: ctx.compression,
    };
    // Kafka 3.7 reads a repeated partition once, at its last entry's offset, and answers
    // it once (its fetch data is a map by partition).
    let topic_data = merge_by_partition(
        topic_data.into_iter().map(|t| (t.name, t.partitions)),
        |p| p.partition_index,
    );
    let resolved = resolve_topics(store, topic_data.iter().map(|(name, _)| name.as_str()));
    let mut responses = Vec::with_capacity(topic_data.len());
    for (name, requested) in topic_data {
        let found = match &resolved {
            Ok(found) => found
                .get(&name)
                .copied()
                .ok_or(ERROR_UNKNOWN_TOPIC_OR_PARTITION),
            Err(code) => Err(*code),
        };
        // A missing topic (or a failed lookup) is reported on every requested partition.
        let partitions = match found {
            Ok((topic_id, partition_count)) => {
                let topic = TopicPartitions {
                    id: topic_id,
                    partition_count,
                };
                requested
                    .iter()
                    .map(|partition| {
                        fetch_partition(store, topic, partition, settings, &mut budget)
                    })
                    .collect()
            }
            Err(error_code) => requested
                .iter()
                .map(|p| partition_error(p.partition_index, error_code))
                .collect(),
        };
        let mut topic_response = FetchableTopicResponse::default();
        topic_response.topic = TopicName(StrBytes::from_string(name));
        topic_response.partitions = partitions;
        responses.push(topic_response);
    }

    let mut kafka_response = FetchResponse::default();
    kafka_response.throttle_time_ms = 0;
    kafka_response.error_code = ERROR_NONE;
    kafka_response.session_id = 0;
    kafka_response.responses = responses;

    Ok(kafka_response)
}

/// The topic a fetch reads from.
#[derive(Clone, Copy)]
struct TopicPartitions {
    id: i32,
    partition_count: i32,
}

/// How a fetch request reads: its isolation level and the compression of the batches
/// it returns.
#[derive(Clone, Copy)]
struct FetchSettings {
    isolation_level: IsolationLevel,
    compression: Compression,
}

/// What is left of a fetch request's `max_bytes`, and whether an earlier partition
/// already returned records (see [`partition_fetch_limit`]).
struct FetchBudget {
    remaining: i64,
    returned_any: bool,
}

/// Read one partition for a fetch and build its response entry.
fn fetch_partition(
    store: &dyn KafkaStore,
    topic: TopicPartitions,
    partition_fetch: &PartitionFetchData,
    settings: FetchSettings,
    budget: &mut FetchBudget,
) -> PartitionData {
    let isolation_level = settings.isolation_level;
    let topic_id = topic.id;
    let partition_id = partition_fetch.partition_index;
    let fetch_offset = partition_fetch.fetch_offset;

    // A partition outside [0, partition_count) does not exist: report
    // UNKNOWN_TOPIC_OR_PARTITION instead of an empty success, which would leave the
    // consumer polling it forever.
    if partition_id < 0 || partition_id >= topic.partition_count {
        return partition_error(partition_id, ERROR_UNKNOWN_TOPIC_OR_PARTITION);
    }

    // All bounds come from one storage call. A storage error is a per-partition error
    // code: offset 0 would make a transient Postgres error look like an empty
    // partition, which can reset a consumer's position.
    let bounds = match store.get_partition_bounds(topic_id, partition_id, isolation_level) {
        Ok(b) => b,
        Err(e) => {
            if e.is_server_error() {
                crate::pg_warning!(
                    "Failed to compute offset bounds for topic_id={}, partition={}: {}",
                    topic_id,
                    partition_id,
                    e
                );
            }
            return partition_error(partition_id, e.to_kafka_error_code());
        }
    };

    // An offset before the durable log start (retired by DeleteRecords or retention) or
    // past the high watermark is OFFSET_OUT_OF_RANGE, so the consumer applies its
    // auto.offset.reset policy. The lower bound is the durable log start, not the first
    // surviving row: aborted-message cleanup can delete leading rows without retiring
    // their offsets, and a fetch from such a hole must skip ahead to the next committed
    // record. fetch_offset == high_watermark is valid (caught up, waiting for data).
    if fetch_offset < bounds.log_start_offset || fetch_offset > bounds.high_watermark {
        let mut partition_data = partition_with_bounds(partition_id, &bounds);
        partition_data.error_code = ERROR_OFFSET_OUT_OF_RANGE;
        partition_data.records = None;
        return partition_data;
    }

    // Once the request-level max_bytes is spent, later partitions report their bounds
    // but no records.
    let Some(limit) = partition_fetch_limit(
        partition_fetch.partition_max_bytes,
        budget.remaining,
        budget.returned_any,
    ) else {
        return partition_with_bounds(partition_id, &bounds);
    };
    read_partition(
        store,
        topic_id,
        partition_fetch,
        limit,
        &bounds,
        settings,
        budget,
    )
}

/// Read up to `limit` bytes of records of an in-range partition from `fetch_offset`, trim
/// them to the response budget and encode them (or a gap batch over invisible offsets).
fn read_partition(
    store: &dyn KafkaStore,
    topic_id: i32,
    partition_fetch: &PartitionFetchData,
    limit: i32,
    bounds: &PartitionBounds,
    settings: FetchSettings,
    budget: &mut FetchBudget,
) -> PartitionData {
    let partition_id = partition_fetch.partition_index;
    let fetch_offset = partition_fetch.fetch_offset;
    let FetchSettings {
        isolation_level,
        compression,
    } = settings;
    let readable_end = match isolation_level {
        IsolationLevel::ReadCommitted => bounds.last_stable_offset,
        IsolationLevel::ReadUncommitted => bounds.high_watermark,
    };
    // Nothing is readable from the readable end on (for read_committed, the last stable
    // offset: a consumer waiting behind an open transaction), and a gap batch covers no
    // offset there, so there is nothing to read. Kafka reads only up to it. Every long-poll
    // attempt reruns the fetch, so a read here would cost the DB thread a scan per Produce,
    // EndTxn and re-check for as long as the transaction stays open.
    //
    // With an earlier partition's records in the response, a limit below the smallest gap
    // batch fits neither a gap batch nor a record (each counts at least RECORD_OVERHEAD
    // bytes, more than a gap batch): reading would only scan for what cannot be returned.
    if fetch_offset >= readable_end || (budget.returned_any && i64::from(limit) < MIN_GAP_BATCH_LEN)
    {
        return partition_with_bounds(partition_id, bounds);
    }
    // Only a partition that may keep an oversized first record reads one: any other would
    // read it only to drop it (Kafka 3.7 applies minOneMessage to the first partition only).
    let fetched = match store.fetch_records_with_isolation(
        topic_id,
        partition_id,
        fetch_offset,
        limit,
        !budget.returned_any,
        isolation_level,
    ) {
        Ok(records) => records,
        Err(e) => {
            if e.is_server_error() {
                crate::pg_warning!(
                    "Failed to fetch records for topic_id={}, partition={}, offset={}: {}",
                    topic_id,
                    partition_id,
                    fetch_offset,
                    e
                );
            }
            return partition_error(partition_id, e.to_kafka_error_code());
        }
    };
    // The first visible record from fetch_offset, whether read or left out for its size.
    let first_visible = fetched
        .first_left_out
        .or_else(|| fetched.messages.first().map(|m| m.partition_offset));
    let (db_records, _) =
        trim_to_response_budget(fetched.messages, i64::from(limit), budget.returned_any);

    // One RecordBatch per run of consecutive offsets of one producer.
    let encoded = if db_records.is_empty() {
        Ok(None)
    } else {
        encode_within_limit(
            db_records,
            compression,
            i64::from(limit),
            budget.returned_any,
        )
    };
    let records = match encoded {
        Ok(Some((batches, counted))) => {
            budget.remaining -= counted;
            budget.returned_any = true;
            Ok(batches)
        }
        // Nothing visible from fetch_offset up to the first visible record, or to the
        // readable end without one. With offsets in between (aborted rows filtered for
        // read_committed, rows deleted by aborted-row cleanup or retention), an empty
        // response would leave the consumer polling the same offset forever, and each poll
        // would scan those rows again; a marker batch covering them moves it past them, as
        // the abort marker after an aborted transaction does in Kafka. A first visible
        // record that does not fit is not covered: the consumer stops at it.
        Ok(None) => {
            let gap_end = first_visible.unwrap_or(readable_end);
            Ok(take_gap_batch(fetch_offset, gap_end, i64::from(limit), budget).unwrap_or_default())
        }
        Err(e) => Err(e),
    };
    fetched_partition_data(topic_id, partition_id, bounds, records)
}

/// Bytes of the smallest [`gap_batch`] (one whose offset delta is a one-byte varint). Every
/// record counts more than this against the byte limits.
const MIN_GAP_BATCH_LEN: i64 = 78;
const _: () = assert!(MIN_GAP_BATCH_LEN < crate::kafka::storage::RECORD_OVERHEAD);

/// A gap batch over `[from, to)` if it fits the partition's `limit`, counted against the
/// response budget as records are: Kafka 3.7 counts the abort marker batches it returns in
/// the fetch size, and a partition with only them still stops the later ones returning
/// anything past their limits. As the first record of the response, it covers the whole
/// range whatever its size. Otherwise, when the whole range does not fit, it covers the
/// longest prefix whose batch does (a shorter offset delta takes fewer varint bytes): any
/// prefix holds nothing visible, and covering some of it moves the consumer forward.
fn take_gap_batch(
    from: i64,
    to: i64,
    limit: i64,
    budget: &mut FetchBudget,
) -> Option<bytes::Bytes> {
    let fits = |gap: &bytes::Bytes| {
        !budget.returned_any || i64::try_from(gap.len()).unwrap_or(i64::MAX) <= limit
    };
    // The prefix lengths at which the varint of the offset delta gets a byte shorter.
    let prefix_ends = [1i64 << 27, 1 << 20, 1 << 13, 64].map(|len| from.saturating_add(len));
    let gap = std::iter::once(to)
        .chain(prefix_ends.into_iter().filter(|&end| end < to))
        .filter_map(|end| gap_batch(from, end))
        .find(fits)?;
    budget.remaining -= i64::try_from(gap.len()).unwrap_or(i64::MAX);
    budget.returned_any = true;
    Some(gap)
}

/// A partition entry for an error that leaves its bounds unknown.
fn partition_error(partition_id: i32, error_code: i16) -> PartitionData {
    let mut partition_data = PartitionData::default();
    partition_data.partition_index = partition_id;
    partition_data.error_code = error_code;
    partition_data.high_watermark = -1;
    partition_data
}

/// A successful partition entry with its bounds and no records. Empty bytes rather than
/// None: a null records field is an "invalid MessageSetSize -1" in the flexible format.
fn partition_with_bounds(partition_id: i32, bounds: &PartitionBounds) -> PartitionData {
    let mut partition_data = PartitionData::default();
    partition_data.partition_index = partition_id;
    partition_data.error_code = ERROR_NONE;
    partition_data.high_watermark = bounds.high_watermark;
    partition_data.last_stable_offset = bounds.last_stable_offset;
    partition_data.log_start_offset = bounds.earliest_offset;
    partition_data.records = Some(bytes::Bytes::new());
    partition_data
}

/// The response entry of a partition whose records were read: `records` holds the
/// encoded record batches, or why they could not be encoded.
///
/// Rows that cannot be encoded are answered KAFKA_STORAGE_ERROR, Kafka's code for a
/// log it cannot serve: an empty success would leave the consumer refetching the
/// same offset forever with nothing on its side to show why. The error is
/// retriable, so a client keeps trying (and reports it) rather than failing over a
/// condition that a fix of the stored rows clears.
fn fetched_partition_data(
    topic_id: i32,
    partition_id: i32,
    bounds: &PartitionBounds,
    records: std::result::Result<bytes::Bytes, String>,
) -> PartitionData {
    match records {
        Ok(records) => {
            let mut partition_data = partition_with_bounds(partition_id, bounds);
            partition_data.records = Some(records);
            partition_data
        }
        Err(e) => {
            crate::pg_warning!(
                "Failed to encode RecordBatch for topic_id={}, partition={}: {}",
                topic_id,
                partition_id,
                e
            );
            let mut partition_data = partition_error(partition_id, ERROR_KAFKA_STORAGE_ERROR);
            partition_data.records = Some(bytes::Bytes::new());
            partition_data
        }
    }
}

/// Handle ListOffsets request
///
/// Kafka's special timestamps:
/// - -2 = earliest offset (log start)
/// - -1 = latest offset: the high watermark, or the last stable offset for a
///   read_committed request
/// - -3 = MAX_TIMESTAMP (KIP-734): offset and timestamp of the record with the largest
///   timestamp
/// - >= 0 = the earliest offset whose timestamp is at or after it
///
/// Timestamp lookups (-3 and >= 0) are bounded like Kafka's: a match at or beyond the
/// high watermark (read_committed: the last stable offset) is reported as offset -1.
/// A partition outside the topic is UNKNOWN_TOPIC_OR_PARTITION.
pub fn handle_list_offsets(
    ctx: &HandlerContext,
    topics: Vec<ListOffsetsTopicData>,
    isolation_level: IsolationLevel,
) -> Result<ListOffsetsResponse> {
    // Kafka 3.7 answers every entry of a partition the request names more than once
    // INVALID_REQUEST and looks nothing up for it. Each distinct name is numbered once per
    // topic entry and partitions are keyed by that number, so a long name repeated over
    // many partitions is hashed once, not once per partition.
    let mut name_at: HashMap<&str, usize> = HashMap::new();
    let numbers: Vec<usize> = topics
        .iter()
        .map(|topic| {
            let next = name_at.len();
            *name_at.entry(topic.name.as_str()).or_insert(next)
        })
        .collect();
    drop(name_at);
    let mut named: HashMap<(usize, i32), usize> = HashMap::new();
    for (topic, &number) in topics.iter().zip(&numbers) {
        for p in &topic.partitions {
            *named.entry((number, p.partition_index)).or_default() += 1;
        }
    }
    // The repeated partitions of each topic entry, by entry position: keyed by the entry
    // rather than by a copy of its name, so a long name repeated over many partitions
    // costs nothing per partition.
    let repeated: Vec<HashSet<i32>> = topics
        .iter()
        .zip(&numbers)
        .map(|(topic, &number)| {
            topic
                .partitions
                .iter()
                .map(|p| p.partition_index)
                .filter(|&partition| named.get(&(number, partition)).is_some_and(|&n| n > 1))
                .collect()
        })
        .collect();
    drop(named);
    let resolved = resolve_topics(ctx.store, topics.iter().map(|t| t.name.as_str()));
    let mut kafka_response = ListOffsetsResponse::default();
    kafka_response.throttle_time_ms = 0;
    kafka_response.topics = topics
        .into_iter()
        .zip(repeated)
        .map(|(topic, repeated)| {
            let found = match &resolved {
                Ok(found) => Ok(found.get(&topic.name).copied()),
                Err(code) => Err(*code),
            };
            let partitions = topic
                .partitions
                .iter()
                .map(|p| {
                    if repeated.contains(&p.partition_index) {
                        return list_offsets_error(p.partition_index, ERROR_INVALID_REQUEST);
                    }
                    match found {
                        Ok(Some((topic_id, partition_count))) => list_offsets_partition(
                            ctx.store,
                            topic_id,
                            partition_count,
                            p,
                            isolation_level,
                        ),
                        // A missing topic (or a failed lookup) is reported on every
                        // requested partition.
                        Ok(None) => {
                            list_offsets_error(p.partition_index, ERROR_UNKNOWN_TOPIC_OR_PARTITION)
                        }
                        Err(code) => list_offsets_error(p.partition_index, code),
                    }
                })
                .collect();
            let mut topic_response = ListOffsetsTopicResponse::default();
            topic_response.name = TopicName(StrBytes::from_string(topic.name));
            topic_response.partitions = partitions;
            topic_response
        })
        .collect();
    Ok(kafka_response)
}

/// Answer one ListOffsets partition of a resolved topic.
fn list_offsets_partition(
    store: &dyn KafkaStore,
    topic_id: i32,
    partition_count: i32,
    request: &ListOffsetsPartitionData,
    isolation_level: IsolationLevel,
) -> ListOffsetsPartitionResponse {
    let partition_id = request.partition_index;
    if partition_id < 0 || partition_id >= partition_count {
        return list_offsets_error(partition_id, ERROR_UNKNOWN_TOPIC_OR_PARTITION);
    }
    match list_partition_offset(
        store,
        topic_id,
        partition_id,
        request.timestamp,
        isolation_level,
    ) {
        Ok((offset, timestamp)) => {
            let mut partition_response = ListOffsetsPartitionResponse::default();
            partition_response.partition_index = partition_id;
            partition_response.error_code = ERROR_NONE;
            partition_response.offset = offset;
            partition_response.timestamp = timestamp;
            partition_response
        }
        Err(e) => {
            if e.is_server_error() {
                crate::pg_warning!(
                    "ListOffsets failed for topic_id={}, partition={}, timestamp={}: {}",
                    topic_id,
                    partition_id,
                    request.timestamp,
                    e
                );
            }
            list_offsets_error(partition_id, e.to_kafka_error_code())
        }
    }
}

/// A ListOffsets partition entry carrying `error_code` (offset and timestamp -1).
fn list_offsets_error(partition_index: i32, error_code: i16) -> ListOffsetsPartitionResponse {
    let mut p = ListOffsetsPartitionResponse::default();
    p.partition_index = partition_index;
    p.error_code = error_code;
    p.offset = -1;
    p.timestamp = -1;
    p
}

/// `(offset, timestamp)` for one ListOffsets partition query (see
/// [`handle_list_offsets`]).
fn list_partition_offset(
    store: &dyn KafkaStore,
    topic_id: i32,
    partition_id: i32,
    timestamp: i64,
    isolation_level: IsolationLevel,
) -> Result<(i64, i64)> {
    // The end of what a consumer at this isolation level may read.
    let readable_end = || match isolation_level {
        IsolationLevel::ReadCommitted => store.get_last_stable_offset(topic_id, partition_id),
        IsolationLevel::ReadUncommitted => store.get_high_watermark(topic_id, partition_id),
    };
    match timestamp {
        // EARLIEST, and EARLIEST_LOCAL (-4): no tiered storage, so the same.
        -2 | -4 => Ok((store.get_earliest_offset(topic_id, partition_id)?, -1)),
        -1 => Ok((readable_end()?, -1)),
        _ => {
            let found = store.get_offset_for_timestamp(topic_id, partition_id, timestamp)?;
            match found {
                Some((offset, record_ts)) if offset < readable_end()? => Ok((offset, record_ts)),
                _ => Ok((-1, -1)),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::messages::{PartitionFetchData, TopicFetchData};
    use crate::kafka::storage::roles::*;
    use crate::kafka::storage::{PartitionBounds, TopicMetadata};
    use crate::testing::mocks::MockKafkaStore;
    use crate::testing::TestEnv;
    use kafka_protocol::records::Compression;
    use kafka_protocol::records::RecordBatchDecoder;

    fn msg(offset: i64, value_len: usize) -> FetchedMessage {
        FetchedMessage {
            partition_offset: offset,
            key: None,
            value: Some(vec![b'x'; value_len]),
            timestamp: 0,
            headers: vec![],
            producer_id: None,
            producer_epoch: None,
        }
    }

    /// Every row encoded as one response's batches, with no byte limit.
    fn encode_record_batches(
        messages: Vec<FetchedMessage>,
        compression: Compression,
    ) -> std::result::Result<bytes::Bytes, String> {
        encode_records(&batch_records(messages)?, compression)
    }

    fn bounds() -> PartitionBounds {
        PartitionBounds {
            high_watermark: 9,
            last_stable_offset: 8,
            earliest_offset: 2,
            log_start_offset: 1,
        }
    }

    /// Rows that cannot be encoded are a per-partition KAFKA_STORAGE_ERROR the client
    /// sees, not an empty success that stalls it at the same offset.
    #[test]
    fn unencodable_records_are_a_storage_error() {
        let p = fetched_partition_data(1, 3, &bounds(), Err("too far apart".to_string()));
        assert_eq!(p.partition_index, 3);
        assert_eq!(p.error_code, ERROR_KAFKA_STORAGE_ERROR);
        assert_eq!(p.high_watermark, -1);
        assert_eq!(p.records.as_deref(), Some(&[][..]));

        let encoded = bytes::Bytes::from_static(b"batch");
        let p = fetched_partition_data(1, 3, &bounds(), Ok(encoded.clone()));
        assert_eq!(p.error_code, ERROR_NONE);
        assert_eq!(
            (p.high_watermark, p.last_stable_offset, p.log_start_offset),
            (9, 8, 2)
        );
        assert_eq!(p.records, Some(encoded));
    }

    #[test]
    fn size_estimate_counts_key_value_and_overhead() {
        let mut m = msg(0, 10);
        assert_eq!(message_size_estimate(&m), 94);
        m.key = Some(vec![1, 2, 3]);
        assert_eq!(message_size_estimate(&m), 97);
        m.value = None;
        assert_eq!(message_size_estimate(&m), 87);
    }

    /// The estimate is never under what a record adds to an uncompressed response, alone in
    /// its batch (the most it can add) or sharing one. Compressed batches can take more and
    /// are checked at their encoded size (`compressed_records_stay_within_the_byte_limits`).
    #[test]
    fn size_estimate_covers_the_encoded_record() {
        let mut lone = Vec::new();
        for (key, value, headers) in [
            (None, Some(0), 0),
            (None, Some(10), 0),
            (Some(3), None, 0),
            (Some(100), Some(10_000), 2),
            (Some(70), Some(1 << 20), 1),
            (None, None, 0),
        ] {
            let mut m = msg(0, 0);
            m.key = key.map(|n| vec![b'k'; n]);
            m.value = value.map(|n| vec![b'v'; n]);
            m.timestamp = 1_700_000_000_000;
            m.headers = (0..headers)
                .map(|i| (format!("h{i}"), vec![b'x'; 200]))
                .collect();
            lone.push(m);
        }
        for m in &lone {
            let estimate = message_size_estimate(m);
            let encoded = encode_record_batches(vec![m.clone()], Compression::None).unwrap();
            assert!(
                i64::try_from(encoded.len()).unwrap() <= estimate,
                "a record alone in its batch takes {} bytes, counted {estimate}",
                encoded.len()
            );
        }
        // Alternating producers: one batch a record.
        let alternating: Vec<_> = (0..6)
            .map(|o| {
                let mut m = msg(o, 10);
                m.producer_id = (o % 2 == 1).then_some(7);
                m.producer_epoch = m.producer_id.map(|_| 0);
                m
            })
            .collect();
        // Consecutive records of no producer: one batch.
        let shared: Vec<_> = (0..6).map(|o| msg(o, 10)).collect();
        for records in [alternating, shared] {
            let estimate: i64 = records.iter().map(message_size_estimate).sum();
            let encoded = encode_record_batches(records, Compression::None).unwrap();
            assert!(i64::try_from(encoded.len()).unwrap() <= estimate);
        }
    }

    /// Record sets the byte limits must hold for under every codec: tiny records (whose
    /// compression framing outweighs their bytes) and incompressible ones, each alone in its
    /// batch and sharing one, up to values spanning several compression blocks.
    fn byte_limit_record_sets() -> Vec<(&'static str, Vec<FetchedMessage>)> {
        use rand::{RngCore, SeedableRng};
        let mut rng = rand::rngs::StdRng::seed_from_u64(7);
        let mut random = |n: usize| {
            let mut v = vec![0u8; n];
            rng.fill_bytes(&mut v);
            v
        };
        let record = |offset: i64, key, value, producer_id: Option<i64>| FetchedMessage {
            partition_offset: offset,
            key,
            value,
            timestamp: 1_700_000_000_000 + offset,
            headers: vec![],
            producer_id,
            producer_epoch: producer_id.map(|_| 0),
        };
        let alternating = |offset: i64| (offset % 2 == 1).then_some(7);
        let mut with_headers = record(0, Some(random(70)), Some(random(100_000)), None);
        with_headers.headers = vec![("h0".to_string(), random(200))];
        vec![
            (
                "lone 1-byte value",
                vec![record(0, None, Some(vec![b'v']), None)],
            ),
            ("lone empty record", vec![record(0, None, None, None)]),
            (
                "lone 1 MiB random value",
                vec![record(0, None, Some(random(1 << 20)), None)],
            ),
            ("lone random key, value and header", vec![with_headers]),
            (
                "alternating producers, 1-byte values",
                (0..40)
                    .map(|o| record(o, None, Some(vec![b'v']), alternating(o)))
                    .collect(),
            ),
            (
                "alternating producers, random 300-byte values",
                (0..40)
                    .map(|o| record(o, None, Some(random(300)), alternating(o)))
                    .collect(),
            ),
            (
                "shared batch, 1-byte values",
                (0..40)
                    .map(|o| record(o, None, Some(vec![b'v']), None))
                    .collect(),
            ),
            (
                "shared batch, random 1000-byte values",
                (0..300)
                    .map(|o| record(o, None, Some(random(1000)), None))
                    .collect(),
            ),
        ]
    }

    /// Under every codec the record batches a partition returns stay within its limits (but
    /// for the first record of a response, returned alone whatever its size), and a response
    /// within its `max_bytes`: compression adds framing per batch (and per block) that the
    /// uncompressed size estimate does not count, so a record set that fits by its estimate
    /// can encode to more.
    #[test]
    fn compressed_records_stay_within_the_byte_limits() {
        let codecs = [
            Compression::None,
            Compression::Gzip,
            Compression::Snappy,
            Compression::Lz4,
            Compression::Zstd,
        ];
        for compression in codecs {
            for (name, set) in byte_limit_record_sets() {
                let limit =
                    i32::try_from(set.iter().map(message_size_estimate).sum::<i64>()).unwrap();
                let end = i64::try_from(set.len()).unwrap();
                let records_len = |b: &bytes::Bytes| i32::try_from(b.len()).unwrap();
                let decoded_count = |b: &bytes::Bytes| {
                    let mut b = b.clone();
                    RecordBatchDecoder::decode_all(&mut b)
                        .unwrap()
                        .iter()
                        .map(|batch| batch.records.len())
                        .sum::<usize>()
                };
                // One partition, its limit and max_bytes the set's estimate, with and without
                // the whole first record.
                for whole_first_record in [false, true] {
                    let mut mock = one_partition_mock(end, end);
                    let rows = set.clone();
                    mock.expect_fetch_records_with_isolation()
                        .returning(move |_, _, _, _, _, _| Ok(rows.clone().into()));
                    let env = TestEnv::new();
                    let mut ctx = env.ctx(&mock);
                    ctx.compression = compression;
                    let mut request = two_partition_request();
                    request[0].partitions.truncate(1);
                    request[0].partitions[0].partition_max_bytes = limit;
                    let response = handle_fetch_with_max_bytes(
                        &ctx,
                        request,
                        IsolationLevel::ReadUncommitted,
                        limit,
                        whole_first_record,
                    )
                    .unwrap();
                    let part = &response.responses[0].partitions[0];
                    assert_eq!(part.error_code, ERROR_NONE);
                    let records = part.records.clone().unwrap();
                    let within = records_len(&records) <= limit;
                    let lone_first = whole_first_record && decoded_count(&records) == 1;
                    assert!(
                        within || lone_first,
                        "{compression:?}, {name}, whole_first_record {whole_first_record}: \
                         {} bytes returned for a limit of {limit}",
                        records.len()
                    );
                    if whole_first_record {
                        assert!(decoded_count(&records) >= 1, "{compression:?}, {name}");
                    }
                }
                // Two partitions sharing a max_bytes of both estimates.
                let mut mock = mock_with_two_partitions_ending_at(end);
                let rows = set.clone();
                mock.expect_fetch_records_with_isolation()
                    .returning(move |_, _, _, _, _, _| Ok(rows.clone().into()));
                let env = TestEnv::new();
                let mut ctx = env.ctx(&mock);
                ctx.compression = compression;
                let mut request = two_partition_request();
                for p in &mut request[0].partitions {
                    p.partition_max_bytes = limit;
                }
                let response = handle_fetch_with_max_bytes(
                    &ctx,
                    request,
                    IsolationLevel::ReadUncommitted,
                    2 * limit,
                    false,
                )
                .unwrap();
                let total: i32 = response.responses[0]
                    .partitions
                    .iter()
                    .map(|p| records_len(p.records.as_ref().unwrap()))
                    .sum();
                assert!(
                    total <= 2 * limit,
                    "{compression:?}, {name}: {total} bytes returned for a max_bytes of {}",
                    2 * limit
                );
            }
        }
    }

    #[test]
    fn fetch_limit_is_min_of_partition_and_response_budget() {
        assert_eq!(partition_fetch_limit(1024, 100, false), Some(100));
        assert_eq!(partition_fetch_limit(50, 100, true), Some(50));
        // The first partition always fetches (storage returns >= 1 message).
        assert_eq!(partition_fetch_limit(1024, 0, false), Some(0));
        // Budget spent after an earlier partition returned data: skip.
        assert_eq!(partition_fetch_limit(1024, 0, true), None);
        assert_eq!(partition_fetch_limit(1024, -5, true), None);
    }

    #[test]
    fn trim_keeps_first_message_of_response_even_if_oversized() {
        let (kept, used) = trim_to_response_budget(vec![msg(0, 500), msg(1, 10)], 100, false);
        assert_eq!(kept.len(), 1);
        assert_eq!(used, 584);
    }

    #[test]
    fn trim_drops_overflow_after_first_partition() {
        let (kept, used) = trim_to_response_budget(vec![msg(0, 500)], 100, true);
        assert!(kept.is_empty());
        assert_eq!(used, 0);
        let (kept, used) =
            trim_to_response_budget(vec![msg(0, 10), msg(1, 10), msg(2, 10)], 200, true);
        assert_eq!(kept.len(), 2);
        assert_eq!(used, 188);
    }

    fn two_partition_request() -> Vec<TopicFetchData> {
        vec![TopicFetchData {
            name: "t".to_string(),
            partitions: vec![
                PartitionFetchData {
                    partition_index: 0,
                    fetch_offset: 0,
                    partition_max_bytes: 1 << 20,
                },
                PartitionFetchData {
                    partition_index: 1,
                    fetch_offset: 0,
                    partition_max_bytes: 1 << 20,
                },
            ],
        }]
    }

    fn mock_with_two_partitions() -> MockKafkaStore {
        mock_with_two_partitions_ending_at(3)
    }

    /// Two partitions whose high watermark and last stable offset are `end`.
    fn mock_with_two_partitions_ending_at(end: i64) -> MockKafkaStore {
        let mut mock = MockKafkaStore::new();
        mock.expect_get_topic_metadata().returning(|_| {
            Ok(vec![TopicMetadata {
                name: "t".to_string(),
                id: 1,
                partition_count: 2,
            }])
        });
        mock.expect_get_high_watermark()
            .returning(move |_, _| Ok(end));
        mock.expect_get_last_stable_offset()
            .returning(move |_, _| Ok(end));
        mock.expect_get_earliest_offset().returning(|_, _| Ok(0));
        mock.expect_get_log_start_offset().returning(|_, _| Ok(0));
        mock
    }

    /// The request-level max_bytes stops filling partitions once spent: partition 0
    /// returns its (oversized) first message, partition 1 is not read at all but still
    /// reports its bounds.
    #[test]
    fn request_max_bytes_limits_later_partitions() {
        let mut mock = mock_with_two_partitions();
        mock.expect_fetch_records_with_isolation()
            .times(1)
            .withf(|_, partition, _, limit, _, _| *partition == 0 && *limit == 1)
            .returning(|_, _, _, _, _, _| Ok(vec![msg(0, 100), msg(1, 100)].into()));
        let env = TestEnv::new();
        let ctx = env.ctx(&mock);

        let response = handle_fetch_with_max_bytes(
            &ctx,
            two_partition_request(),
            IsolationLevel::ReadUncommitted,
            1,
            true,
        )
        .unwrap();
        let parts = &response.responses[0].partitions;
        assert_eq!(parts.len(), 2);
        assert!(!parts[0].records.as_ref().unwrap().is_empty());
        assert_eq!(parts[1].error_code, ERROR_NONE);
        assert_eq!(parts[1].high_watermark, 3);
        assert!(parts[1].records.as_ref().unwrap().is_empty());
    }

    #[test]
    fn size_estimate_counts_headers() {
        let mut m = msg(0, 10);
        m.headers = vec![("trace".to_string(), vec![0; 100_000])];
        assert_eq!(message_size_estimate(&m), 94 + 5 + 100_000 + 10);
        m.headers.push((String::new(), vec![]));
        assert_eq!(message_size_estimate(&m), 94 + 5 + 100_000 + 10 + 10);
    }

    /// Records a store returns past a partition's `partition_max_bytes` are dropped even when
    /// the request-level budget has room, and after the first partition with data a record
    /// larger than its partition's limit is not returned at all; header bytes count.
    #[test]
    fn each_partition_is_trimmed_to_its_partition_max_bytes() {
        let mut mock = mock_with_two_partitions();
        let with_headers = |offset| {
            let mut m = msg(offset, 0);
            m.headers = vec![("h".to_string(), vec![0; 400])];
            m
        };
        mock.expect_fetch_records_with_isolation()
            .times(2)
            .returning(move |_, partition, _, _, _, _| {
                Ok(crate::kafka::storage::FetchedRecords::from(
                    if partition == 0 {
                        (0..10).map(|o| msg(o, 400)).collect::<Vec<_>>()
                    } else {
                        (0..10).map(with_headers).collect::<Vec<_>>()
                    },
                ))
            });
        let env = TestEnv::new();
        let ctx = env.ctx(&mock);
        let mut request = two_partition_request();
        request[0].partitions[0].partition_max_bytes = 1000;
        request[0].partitions[1].partition_max_bytes = 400;

        let response = handle_fetch_with_max_bytes(
            &ctx,
            request,
            IsolationLevel::ReadUncommitted,
            1 << 20,
            true,
        )
        .unwrap();
        let parts = &response.responses[0].partitions;
        let mut records = parts[0].records.clone().unwrap();
        let batches = RecordBatchDecoder::decode_all(&mut records).unwrap();
        // 484 bytes a record: two fit 1000.
        assert_eq!(batches.iter().map(|b| b.records.len()).sum::<usize>(), 2);
        // 495 bytes a record (its header counts), over partition 1's 400.
        assert!(parts[1].records.as_ref().unwrap().is_empty());
    }

    /// Only the first partition with data may return a first record larger than its limits
    /// (KIP-74; Kafka 3.7's minOneMessage), so only it asks storage for one: a later partition
    /// that asked would read an oversized first record from the database just to drop it.
    #[test]
    fn only_the_first_partition_with_data_reads_an_oversized_first_record() {
        // (records partition 0 returns, the request's whole_first_record) -> the flag each
        // partition passes to storage.
        let cases = [
            (1, true, [true, false]),
            (0, true, [true, true]),
            (1, false, [false, false]),
        ];
        for (returned, whole_first_record, expected) in cases {
            let mut mock = mock_with_two_partitions();
            let asked = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
            let seen = std::sync::Arc::clone(&asked);
            // Partition 0 fetching from its end (3) is not read at all.
            mock.expect_fetch_records_with_isolation()
                .times(if returned == 0 { 1 } else { 2 })
                .returning(move |_, partition, _, _, whole, _| {
                    seen.lock().unwrap().push((partition, whole));
                    Ok(if partition == 0 {
                        (0..returned).map(|o| msg(o, 100)).collect::<Vec<_>>()
                    } else {
                        Vec::new()
                    }
                    .into())
                });
            let env = TestEnv::new();
            let ctx = env.ctx(&mock);
            // Partition 0 fetches from its end (3): it returns no record and no gap batch
            // (one would count as data, see gap_batches_count_against_the_byte_limits).
            let mut request = two_partition_request();
            request[0].partitions[0].fetch_offset = if returned == 0 { 3 } else { 0 };
            handle_fetch_with_max_bytes(
                &ctx,
                request,
                IsolationLevel::ReadUncommitted,
                1 << 20,
                whole_first_record,
            )
            .unwrap();
            let read = if returned == 0 {
                vec![(1, expected[1])]
            } else {
                vec![(0, expected[0]), (1, expected[1])]
            };
            assert_eq!(
                *asked.lock().unwrap(),
                read,
                "partition 0 returned {returned} records, whole_first_record {whole_first_record}"
            );
        }
    }

    /// A first record storage left out for its size is a visible record: the partition
    /// returns nothing, not a gap batch that would move the consumer past it.
    #[test]
    fn a_first_record_left_out_is_not_covered_by_a_gap_batch() {
        let mut mock = mock_with_two_partitions();
        mock.expect_fetch_records_with_isolation()
            .times(2)
            .returning(|_, partition, _, _, _, _| {
                Ok(if partition == 0 {
                    vec![msg(0, 100)].into()
                } else {
                    crate::kafka::storage::FetchedRecords {
                        messages: Vec::new(),
                        first_left_out: Some(0),
                    }
                })
            });
        let env = TestEnv::new();
        let ctx = env.ctx(&mock);
        let response = handle_fetch_with_max_bytes(
            &ctx,
            two_partition_request(),
            IsolationLevel::ReadUncommitted,
            1 << 20,
            true,
        )
        .unwrap();
        let parts = &response.responses[0].partitions;
        assert!(!parts[0].records.as_ref().unwrap().is_empty());
        assert_eq!(parts[1].error_code, ERROR_NONE);
        assert_eq!(parts[1].high_watermark, 3);
        assert!(parts[1].records.as_ref().unwrap().is_empty());
    }

    /// Without a response limit (handle_fetch) every partition is read.
    #[test]
    fn unlimited_fetch_reads_every_partition() {
        let mut mock = mock_with_two_partitions();
        mock.expect_fetch_records_with_isolation()
            .times(2)
            .returning(|_, _, _, _, _, _| Ok(vec![msg(0, 100)].into()));
        let env = TestEnv::new();
        let ctx = env.ctx(&mock);

        let response = handle_fetch(
            &ctx,
            two_partition_request(),
            IsolationLevel::ReadUncommitted,
        )
        .unwrap();
        let parts = &response.responses[0].partitions;
        assert!(parts
            .iter()
            .all(|p| !p.records.as_ref().unwrap().is_empty()));
    }

    /// ReadCommitted reports the store's LSO; offsets below the durable log start
    /// (not merely below the first surviving row) are out of range.
    #[test]
    fn bounds_flow_into_the_partition_response() {
        let mut mock = MockKafkaStore::new();
        mock.expect_get_topic_metadata().returning(|_| {
            Ok(vec![TopicMetadata {
                name: "t".to_string(),
                id: 1,
                partition_count: 1,
            }])
        });
        mock.expect_get_high_watermark().returning(|_, _| Ok(9));
        mock.expect_get_last_stable_offset().returning(|_, _| Ok(7));
        mock.expect_get_earliest_offset().returning(|_, _| Ok(2));
        mock.expect_get_log_start_offset().returning(|_, _| Ok(1));
        mock.expect_fetch_records_with_isolation()
            .returning(|_, _, _, _, _, _| Ok(vec![].into()));
        let env = TestEnv::new();
        let ctx = env.ctx(&mock);

        let mut req = two_partition_request();
        req[0].partitions.truncate(1);
        req[0].partitions[0].fetch_offset = 1;
        let response = handle_fetch(&ctx, req, IsolationLevel::ReadCommitted).unwrap();
        let p = &response.responses[0].partitions[0];
        assert_eq!(p.error_code, ERROR_NONE);
        assert_eq!(p.high_watermark, 9);
        assert_eq!(p.last_stable_offset, 7);
        assert_eq!(p.log_start_offset, 2);

        let mut req = two_partition_request();
        req[0].partitions.truncate(1);
        req[0].partitions[0].fetch_offset = 0;
        let response = handle_fetch(&ctx, req, IsolationLevel::ReadCommitted).unwrap();
        assert_eq!(
            response.responses[0].partitions[0].error_code,
            ERROR_OFFSET_OUT_OF_RANGE
        );
    }

    // ===== Batch grouping and gap batches =====

    fn txn_msg(offset: i64, pid: Option<i64>) -> FetchedMessage {
        FetchedMessage {
            producer_id: pid,
            producer_epoch: pid.map(|_| 3),
            ..msg(offset, 10)
        }
    }

    /// (base offset, record count, producer id, transactional) of every batch.
    fn decode_batches(bytes: bytes::Bytes) -> Vec<(i64, usize, i64, bool)> {
        use kafka_protocol::records::RecordBatchDecoder;
        let mut buf = bytes;
        let mut out = Vec::new();
        let set = RecordBatchDecoder::decode_all(&mut buf).unwrap();
        for batch in set {
            let first = &batch.records[0];
            out.push((
                first.offset,
                batch.records.len(),
                first.producer_id,
                first.transactional,
            ));
        }
        out
    }

    #[test]
    fn crc32c_matches_the_standard_check_value() {
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
    }

    #[test]
    fn timestamps_too_far_apart_start_a_new_batch() {
        // A batch stores timestamps as i32 deltas from its minimum. A record stamped 0
        // (a client that sent no time) followed by records stamped with the current
        // time cannot share a batch with it: the batch could not be encoded, and every
        // fetch of the partition would fail.
        let now = 1_795_078_665_492;
        let mut rows: Vec<FetchedMessage> = (0..4).map(|o| msg(o, 10)).collect();
        rows[1].timestamp = now;
        rows[2].timestamp = now + 5;
        rows[3].timestamp = now - i64::from(i32::MAX) - 1;
        let bytes = encode_record_batches(rows, Compression::None).unwrap();
        assert_eq!(
            decode_batches(bytes),
            vec![(0, 1, -1, false), (1, 2, -1, false), (3, 1, -1, false)]
        );
    }

    #[test]
    fn records_are_grouped_into_batches() {
        // Consecutive records share a batch instead of paying 61 header bytes each.
        let plain: Vec<FetchedMessage> = (0..100).map(|o| msg(o, 10)).collect();
        let bytes = encode_record_batches(plain, Compression::None).unwrap();
        assert_eq!(decode_batches(bytes.clone()), vec![(0, 100, -1, false)]);
        assert!(bytes.len() < 100 * 40, "{} bytes", bytes.len());

        // Runs split on a producer change and on an offset gap; transactional rows
        // carry their producer id and the transactional flag.
        let mixed = vec![
            txn_msg(0, None),
            txn_msg(1, None),
            txn_msg(2, Some(7)),
            txn_msg(3, Some(7)),
            txn_msg(5, Some(7)), // gap: 4 was filtered (aborted)
            txn_msg(6, Some(8)),
            txn_msg(7, None),
        ];
        let bytes = encode_record_batches(mixed, Compression::None).unwrap();
        assert_eq!(
            decode_batches(bytes),
            vec![
                (0, 2, -1, false),
                (2, 2, 7, true),
                (5, 1, 7, true),
                (6, 1, 8, true),
                (7, 1, -1, false),
            ]
        );
    }

    #[test]
    fn grouped_batches_survive_compression() {
        let plain: Vec<FetchedMessage> = (10..60).map(|o| msg(o, 100)).collect();
        let bytes = encode_record_batches(plain, Compression::Gzip).unwrap();
        assert_eq!(decode_batches(bytes), vec![(10, 50, -1, false)]);
    }

    #[test]
    fn gap_batch_is_a_control_batch_covering_the_range() {
        assert!(gap_batch(5, 5).is_none());
        assert!(gap_batch(6, 5).is_none());
        let b = gap_batch(2, 5).unwrap();
        assert_eq!(i64::from_be_bytes(b[0..8].try_into().unwrap()), 2); // base offset
        let len = usize::try_from(i32::from_be_bytes(b[8..12].try_into().unwrap())).unwrap();
        assert_eq!(len + 12, b.len());
        assert_eq!(b[16], 2); // magic
        let crc = u32::from_be_bytes(b[17..21].try_into().unwrap());
        assert_eq!(crc, crc32c(&b[21..]));
        assert_eq!(i16::from_be_bytes(b[21..23].try_into().unwrap()), 0x30);
        assert_eq!(i32::from_be_bytes(b[23..27].try_into().unwrap()), 2); // last delta
        assert_eq!(i32::from_be_bytes(b[57..61].try_into().unwrap()), 1); // records

        // What a client decodes: one control record (COMMIT marker) at offset 4.
        let mut buf = b.clone();
        let set = RecordBatchDecoder::decode_all(&mut buf).unwrap();
        assert_eq!(set.len(), 1);
        let r = &set[0].records[0];
        assert!(r.control && r.transactional);
        assert_eq!(r.offset, 4);
        assert_eq!(r.key.as_deref(), Some(&[0u8, 0, 0, 1][..]));
        // A huge gap is covered up to i32::MAX + 1 offsets.
        let b = gap_batch(0, i64::MAX).unwrap();
        assert_eq!(i32::from_be_bytes(b[23..27].try_into().unwrap()), i32::MAX);
        let mut buf = b.clone();
        let set = RecordBatchDecoder::decode_all(&mut buf).unwrap();
        assert_eq!(set[0].records[0].offset, i64::from(i32::MAX));
    }

    fn one_partition_mock(hwm: i64, lso: i64) -> MockKafkaStore {
        let mut mock = MockKafkaStore::new();
        mock.expect_get_topic_metadata().returning(|_| {
            Ok(vec![TopicMetadata {
                name: "t".to_string(),
                id: 1,
                partition_count: 1,
            }])
        });
        mock.expect_get_high_watermark()
            .returning(move |_, _| Ok(hwm));
        mock.expect_get_last_stable_offset()
            .returning(move |_, _| Ok(lso));
        mock.expect_get_earliest_offset().returning(|_, _| Ok(0));
        mock.expect_get_log_start_offset().returning(|_, _| Ok(0));
        mock
    }

    fn fetch_one(mock: &MockKafkaStore, offset: i64, isolation: IsolationLevel) -> bytes::Bytes {
        let env = TestEnv::new();
        let ctx = env.ctx(mock);
        let mut req = two_partition_request();
        req[0].partitions.truncate(1);
        req[0].partitions[0].fetch_offset = offset;
        let r = handle_fetch(&ctx, req, isolation).unwrap();
        assert_eq!(r.responses[0].partitions[0].error_code, ERROR_NONE);
        r.responses[0].partitions[0].records.clone().unwrap()
    }

    /// Offsets 2..5 are aborted (filtered) and nothing visible is left: a gap batch
    /// moves the consumer to the LSO (= HWM 5). An empty response would make it
    /// re-fetch offset 2 forever.
    #[test]
    fn invisible_tail_is_covered_by_a_gap_batch() {
        let mut mock = one_partition_mock(5, 5);
        mock.expect_fetch_records_with_isolation()
            .returning(|_, _, _, _, _, _| Ok(vec![].into()));
        let b = fetch_one(&mock, 2, IsolationLevel::ReadCommitted);
        assert_eq!(b, gap_batch(2, 5).unwrap());
        // read_uncommitted after cleanup deleted the rows: up to the HWM.
        let b = fetch_one(&mock, 3, IsolationLevel::ReadUncommitted);
        assert_eq!(b, gap_batch(3, 5).unwrap());
        // Caught up: nothing to cover.
        assert!(fetch_one(&mock, 5, IsolationLevel::ReadCommitted).is_empty());
    }

    /// An open transaction at offset 3 keeps blocking at the LSO: the gap batch ends
    /// there, never beyond it.
    #[test]
    fn gap_batch_stops_at_the_last_stable_offset() {
        let mut mock = one_partition_mock(9, 3);
        mock.expect_fetch_records_with_isolation()
            .returning(|_, _, _, _, _, _| Ok(vec![].into()));
        assert_eq!(
            fetch_one(&mock, 1, IsolationLevel::ReadCommitted),
            gap_batch(1, 3).unwrap()
        );
        assert!(fetch_one(&mock, 3, IsolationLevel::ReadCommitted).is_empty());
    }

    /// A gap batch counts against the byte limits as records do (Kafka 3.7 counts the abort
    /// markers it returns): the first one of a response is returned whatever its size, a later
    /// one only if it fits what is left of `max_bytes` and its `partition_max_bytes`, and after
    /// one no later partition reads an oversized first record.
    #[test]
    fn gap_batches_count_against_the_byte_limits() {
        let gap_len = i32::try_from(gap_batch(0, 3).unwrap().len()).unwrap();
        // (max_bytes, partition 1's partition_max_bytes) -> whether partition 1 has a gap.
        let cases = [
            (1 << 20, 1 << 20, true),
            (2 * gap_len, 1 << 20, true),
            (2 * gap_len - 1, 1 << 20, false),
            (1, 1 << 20, false),
            (1 << 20, gap_len - 1, false),
            (1 << 20, gap_len, true),
        ];
        for (max_bytes, partition_max, second_gap) in cases {
            let mut mock = mock_with_two_partitions();
            let asked = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
            let seen = std::sync::Arc::clone(&asked);
            mock.expect_fetch_records_with_isolation().returning(
                move |_, partition, _, _, whole, _| {
                    seen.lock().unwrap().push((partition, whole));
                    Ok(Vec::new().into())
                },
            );
            let env = TestEnv::new();
            let ctx = env.ctx(&mock);
            let mut request = two_partition_request();
            request[0].partitions[1].partition_max_bytes = partition_max;
            let response = handle_fetch_with_max_bytes(
                &ctx,
                request,
                IsolationLevel::ReadCommitted,
                max_bytes,
                true,
            )
            .unwrap();
            let parts = &response.responses[0].partitions;
            let case = format!("max_bytes {max_bytes}, partition_max_bytes {partition_max}");
            assert_eq!(parts[0].records, gap_batch(0, 3), "{case}");
            assert_eq!(parts[1].error_code, ERROR_NONE, "{case}");
            if second_gap {
                assert_eq!(parts[1].records, gap_batch(0, 3), "{case}");
            } else {
                assert!(parts[1].records.as_ref().unwrap().is_empty(), "{case}");
            }
            // Partition 1 is not the first with data: it reads no oversized first record.
            for &(partition, whole) in asked.lock().unwrap().iter() {
                assert_eq!(whole, partition == 0, "{case}: partition {partition}");
            }
        }
    }

    /// Partition 1 of a fetch whose partition 0 returns one 100-byte record, with
    /// `remaining` bytes of `max_bytes` left for partition 1 and `fetched` its storage read
    /// (`None`: it must not be read). Returns partition 1's records.
    fn second_partition_records(
        end: i64,
        remaining: i32,
        fetched: Option<crate::kafka::storage::FetchedRecords>,
    ) -> Option<bytes::Bytes> {
        let first = msg(0, 100);
        let first_len = i32::try_from(message_size_estimate(&first)).unwrap();
        let mut mock = mock_with_two_partitions_ending_at(end);
        let reads = if fetched.is_some() { 2 } else { 1 };
        mock.expect_fetch_records_with_isolation()
            .times(reads)
            .returning(move |_, partition, _, _, _, _| {
                Ok(if partition == 0 {
                    vec![first.clone()].into()
                } else {
                    fetched.clone().unwrap()
                })
            });
        let env = TestEnv::new();
        let ctx = env.ctx(&mock);
        let response = handle_fetch_with_max_bytes(
            &ctx,
            two_partition_request(),
            IsolationLevel::ReadCommitted,
            first_len + remaining,
            true,
        )
        .unwrap();
        let parts = &response.responses[0].partitions;
        assert_eq!(parts[1].error_code, ERROR_NONE);
        parts[1].records.clone()
    }

    /// Invisible offsets before a visible record that does not fit (left out by storage, or
    /// read and trimmed) are covered by a gap batch up to that record, so the consumer moves
    /// past them rather than having them scanned again by every fetch from the same offset.
    #[test]
    fn invisible_offsets_before_a_record_that_does_not_fit_are_covered() {
        let left_out = crate::kafka::storage::FetchedRecords {
            messages: Vec::new(),
            first_left_out: Some(2),
        };
        assert_eq!(
            second_partition_records(3, 1000, Some(left_out)),
            gap_batch(0, 2)
        );
        let trimmed = vec![msg(2, 2000)].into();
        assert_eq!(
            second_partition_records(3, 1000, Some(trimmed)),
            gap_batch(0, 2)
        );
    }

    /// A gap whose whole range does not fit the limit covers the longest prefix that does: a
    /// shorter offset delta is a shorter varint.
    #[test]
    fn a_gap_batch_too_long_for_the_limit_covers_a_prefix() {
        let whole = gap_batch(0, 1 << 20).unwrap();
        let prefix = gap_batch(0, 1 << 13).unwrap();
        assert_eq!(prefix.len() + 1, whole.len());
        let remaining = i32::try_from(prefix.len()).unwrap();
        assert_eq!(
            second_partition_records(1 << 20, remaining, Some(Vec::new().into())),
            Some(prefix)
        );
        assert_eq!(
            second_partition_records(1 << 20, remaining + 1, Some(Vec::new().into())),
            Some(whole)
        );
        // Below the smallest gap batch nothing fits and storage is not read.
        let smallest = i32::try_from(MIN_GAP_BATCH_LEN).unwrap();
        assert_eq!(
            second_partition_records(1 << 20, smallest - 1, None),
            Some(bytes::Bytes::new())
        );
        assert_eq!(
            second_partition_records(3, smallest, Some(Vec::new().into())),
            gap_batch(0, 3)
        );
    }

    /// The smallest gap batch is MIN_GAP_BATCH_LEN bytes.
    #[test]
    fn min_gap_batch_len_is_the_smallest_gap_batch() {
        let smallest = i64::try_from(gap_batch(0, 1).unwrap().len()).unwrap();
        assert_eq!(smallest, MIN_GAP_BATCH_LEN);
        assert_eq!(
            i64::try_from(gap_batch(0, 64).unwrap().len()).unwrap(),
            MIN_GAP_BATCH_LEN
        );
    }

    /// A read_committed fetch at or past the last stable offset, behind an open transaction,
    /// has nothing to read and does not read storage (Kafka reads only up to it); likewise a
    /// fetch at the high watermark.
    #[test]
    fn a_fetch_at_the_readable_end_does_not_read_storage() {
        for (isolation, lso) in [
            (IsolationLevel::ReadCommitted, 2),
            (IsolationLevel::ReadUncommitted, 5),
        ] {
            let mut mock = MockKafkaStore::new();
            mock.expect_get_topic_metadata().returning(|_| {
                Ok(vec![TopicMetadata {
                    name: "t".to_string(),
                    id: 1,
                    partition_count: 1,
                }])
            });
            mock.expect_get_high_watermark().returning(|_, _| Ok(5));
            mock.expect_get_last_stable_offset()
                .returning(move |_, _| Ok(lso));
            mock.expect_get_earliest_offset().returning(|_, _| Ok(0));
            mock.expect_get_log_start_offset().returning(|_, _| Ok(0));
            mock.expect_fetch_records_with_isolation().times(0);
            let env = TestEnv::new();
            let ctx = env.ctx(&mock);
            let mut request = two_partition_request();
            request[0].partitions.truncate(1);
            request[0].partitions[0].fetch_offset = lso;
            let response =
                handle_fetch_with_max_bytes(&ctx, request, isolation, 1 << 20, true).unwrap();
            let p = &response.responses[0].partitions[0];
            assert_eq!(p.error_code, ERROR_NONE, "{isolation:?}");
            assert_eq!(p.last_stable_offset, lso, "{isolation:?}");
            assert!(p.records.as_ref().unwrap().is_empty(), "{isolation:?}");
        }
    }

    /// The default get_partition_bounds reports the last stable offset to a
    /// read_uncommitted fetch too, as Kafka does.
    #[test]
    fn read_uncommitted_bounds_report_the_lso() {
        let mut mock = MockKafkaStore::new();
        mock.expect_get_high_watermark().returning(|_, _| Ok(4));
        mock.expect_get_last_stable_offset().returning(|_, _| Ok(2));
        mock.expect_get_earliest_offset().returning(|_, _| Ok(0));
        mock.expect_get_log_start_offset().returning(|_, _| Ok(0));
        let b = mock
            .get_partition_bounds(1, 0, IsolationLevel::ReadUncommitted)
            .unwrap();
        assert_eq!(
            b,
            PartitionBounds {
                high_watermark: 4,
                last_stable_offset: 2,
                earliest_offset: 0,
                log_start_offset: 0,
            }
        );
    }
}
