// Fetch handlers
//
// Handlers for FetchRequest and ListOffsetsRequest.
// These are read-focused APIs for consuming messages from topics.
// Phase 10: Added isolation_level support for transactional reads.

use super::helpers::resolve_topic;
use crate::kafka::constants::ERROR_NONE;
use crate::kafka::error::Result;
use crate::kafka::handler_context::HandlerContext;
use crate::kafka::storage::{FetchedMessage, IsolationLevel};
use kafka_protocol::messages::TopicName;
use kafka_protocol::protocol::StrBytes;

/// Handle Fetch request
///
/// Fetches messages from storage and encodes them as Kafka RecordBatch.
/// The compression parameter controls the encoding compression for the response.
///
/// Phase 10: Added isolation_level support:
/// - ReadUncommitted (0): Returns all messages including pending transactions
/// - ReadCommitted (1): Only returns committed messages (skips pending/aborted)
///
/// For ReadCommitted consumers, last_stable_offset (LSO) indicates the lowest
/// offset of any pending transaction. Consumers can safely process up to LSO.
pub fn handle_fetch(
    ctx: &HandlerContext,
    topic_data: Vec<crate::kafka::messages::TopicFetchData>,
    isolation_level: IsolationLevel,
) -> Result<kafka_protocol::messages::fetch_response::FetchResponse> {
    handle_fetch_with_max_bytes(ctx, topic_data, isolation_level, i32::MAX)
}

/// Per-message size estimate used for the response-level `max_bytes` budget. Matches
/// the storage layer's own byte accounting (key + value + 64 bytes of overhead).
pub(crate) fn message_size_estimate(msg: &FetchedMessage) -> i64 {
    msg.key.as_ref().map_or(0, |k| k.len() as i64)
        + msg.value.as_ref().map_or(0, |v| v.len() as i64)
        + 64
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
    Some((partition_max_bytes as i64).min(remaining.max(0)) as i32)
}

/// Drop trailing messages that overflow the remaining response budget. The first
/// message is always kept when no earlier partition returned records (see
/// [`partition_fetch_limit`]). Returns the kept messages and their estimated size.
pub(crate) fn trim_to_response_budget(
    messages: Vec<FetchedMessage>,
    remaining: i64,
    returned_any: bool,
) -> (Vec<FetchedMessage>, i64) {
    let mut kept = Vec::with_capacity(messages.len());
    let mut used = 0i64;
    for msg in messages {
        let size = message_size_estimate(&msg);
        let first_of_response = !returned_any && kept.is_empty();
        if !first_of_response && used + size > remaining {
            break;
        }
        used += size;
        kept.push(msg);
    }
    (kept, used)
}

/// Encode fetched rows as RecordBatch v2 (wave 3). Consecutive offsets written by the
/// same producer id and epoch form one batch (rows without a producer id, i.e. not
/// written by a transactional producer, likewise): the batch header (61 bytes) and the
/// compression are paid per batch instead of per record. Batches of transactional rows
/// carry the producer id and epoch and the transactional flag; the base sequence is not
/// stored and is sent as -1. Rows are in offset order.
pub(crate) fn encode_record_batches(
    messages: Vec<FetchedMessage>,
    compression: kafka_protocol::records::Compression,
) -> std::result::Result<bytes::Bytes, String> {
    use kafka_protocol::records::{Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType};

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
        let sequence = (msg.partition_offset - group_base) as i32 - 1;
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
            headers: Default::default(),
        };
        // DR-8: return stored headers to the consumer.
        for (k, v) in msg.headers {
            record
                .headers
                .insert(StrBytes::from_string(k), Some(bytes::Bytes::from(v)));
        }
        records.push(record);
    }
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

/// Bytes of a RecordBatch v2 header (everything but the records).
const BATCH_HEADER_LEN: usize = 61;

/// The one record of a [`gap_batch`]: a control record (key: version 0, type 1 =
/// COMMIT; value: an EndTransactionMarker, version 0, coordinator epoch 0) at offset
/// delta `offset_delta`, timestamp delta 0.
fn gap_marker_record(offset_delta: i32) -> bytes::Bytes {
    use bytes::BufMut;
    fn varint(out: &mut bytes::BytesMut, v: i64) {
        let mut z = ((v << 1) ^ (v >> 63)) as u64;
        while z >= 0x80 {
            out.put_u8((z as u8) | 0x80);
            z >>= 7;
        }
        out.put_u8(z as u8);
    }
    let mut body = bytes::BytesMut::new();
    body.put_i8(0); // attributes
    varint(&mut body, 0); // timestamp delta
    varint(&mut body, offset_delta as i64);
    varint(&mut body, 4); // key length
    body.put_i16(0); // control record key version
    body.put_i16(1); // control record type: COMMIT
    varint(&mut body, 6); // value length
    body.put_i16(0); // EndTransactionMarker version
    body.put_i32(0); // coordinator epoch
    varint(&mut body, 0); // header count
    let mut out = bytes::BytesMut::new();
    varint(&mut out, body.len() as i64);
    out.put(body);
    out.freeze()
}

/// A batch covering offsets `[from, to)` without data (wave 3), or `None` when the
/// range is empty: a control batch holding one COMMIT marker at offset `to - 1`, like
/// the marker Kafka writes when a transaction ends. Clients skip control records but
/// move their position past them: the Java client to the batch's last offset + 1,
/// librdkafka to the marker's offset + 1 (it advances the application position, and
/// so the committed offset and the lag, only on a record, which is why the batch is not
/// simply empty). A range longer than `i32::MAX + 1` offsets is covered up to that
/// length; the next fetch covers the rest.
pub(crate) fn gap_batch(from: i64, to: i64) -> Option<bytes::Bytes> {
    use bytes::BufMut;
    if to <= from {
        return None;
    }
    let last_offset_delta = (to - from - 1).min(i32::MAX as i64) as i32;
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
    out.put_i32((4 + 1 + 4 + body.len()) as i32); // batch length (after this field)
    out.put_i32(0); // partition leader epoch
    out.put_i8(2); // magic
    out.put_u32(crc32c(&body));
    out.put(body);
    Some(out.freeze())
}

/// CRC-32C (Castagnoli), the RecordBatch v2 checksum.
fn crc32c(data: &[u8]) -> u32 {
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
        crc = TABLE[((crc ^ b as u32) & 0xFF) as usize] ^ (crc >> 8);
    }
    crc ^ 0xFFFF_FFFF
}

/// [`handle_fetch`] honouring the request-level `max_bytes` (Fetch v3+): partitions
/// are filled in request order until the response budget is spent, in addition to
/// each partition's own `partition_max_bytes`.
pub fn handle_fetch_with_max_bytes(
    ctx: &HandlerContext,
    topic_data: Vec<crate::kafka::messages::TopicFetchData>,
    isolation_level: IsolationLevel,
    max_bytes: i32,
) -> Result<kafka_protocol::messages::fetch_response::FetchResponse> {
    let store = ctx.store;
    let mut response_remaining: i64 = max_bytes.max(0) as i64;
    let mut returned_any = false;
    let compression = ctx.compression;
    use kafka_protocol::messages::fetch_response::{
        FetchResponse, FetchableTopicResponse, PartitionData,
    };
    let mut responses = Vec::new();

    // Process each topic requested
    for topic_fetch in topic_data {
        let topic_name = topic_fetch.name.clone();

        // Look up topic by name (id + partition count in one lookup)
        let (topic_id, partition_count) = match resolve_topic(store, &topic_name) {
            Ok(Some(found)) => found,
            unresolved => {
                // Topic not found or error - return error for all partitions
                let error_code = match unresolved {
                    Err(code) => code,
                    _ => crate::kafka::constants::ERROR_UNKNOWN_TOPIC_OR_PARTITION,
                };
                let mut error_partitions = Vec::new();
                for p in topic_fetch.partitions {
                    let mut partition_data = PartitionData::default();
                    partition_data.partition_index = p.partition_index;
                    partition_data.error_code = error_code;
                    partition_data.high_watermark = -1;
                    error_partitions.push(partition_data);
                }
                let mut topic_response = FetchableTopicResponse::default();
                topic_response.topic = TopicName(StrBytes::from_string(topic_name));
                topic_response.partitions = error_partitions;
                responses.push(topic_response);
                continue;
            }
        };

        let mut partition_responses = Vec::new();

        // Process each partition
        for partition_fetch in topic_fetch.partitions {
            let partition_id = partition_fetch.partition_index;
            let fetch_offset = partition_fetch.fetch_offset;
            let partition_max_bytes = partition_fetch.partition_max_bytes;

            // A partition outside [0, partition_count) does not exist: report
            // UNKNOWN_TOPIC_OR_PARTITION instead of an empty success, which would
            // leave the consumer polling it forever.
            if partition_id < 0 || partition_id >= partition_count {
                let mut partition_data = PartitionData::default();
                partition_data.partition_index = partition_id;
                partition_data.error_code =
                    crate::kafka::constants::ERROR_UNKNOWN_TOPIC_OR_PARTITION;
                partition_data.high_watermark = -1;
                partition_responses.push(partition_data);
                continue;
            }

            // DR-10 (DEEP-REVIEW-2026-07): a storage error computing the offset
            // bounds must surface as a per-partition error code, not be swallowed
            // as offset 0 — `unwrap_or(0)` made a transient Postgres error look
            // like an empty partition, which can reset a consumer's position.
            // All bounds come from one storage call (one statement in
            // PostgresStore) instead of four or five.
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
                    let mut partition_data = PartitionData::default();
                    partition_data.partition_index = partition_id;
                    partition_data.error_code = e.to_kafka_error_code();
                    partition_data.high_watermark = -1;
                    partition_responses.push(partition_data);
                    continue;
                }
            };
            let high_watermark = bounds.high_watermark;
            // Phase 10: LSO is the lowest offset of any pending transaction, or
            // the HWM if none pending (always the HWM for ReadUncommitted).
            let last_stable_offset = bounds.last_stable_offset;
            // RV-10: report the real log-start offset — the earliest retained
            // offset after cleanup/retention — rather than a hardcoded 0.
            let log_start_offset = bounds.earliest_offset;

            // An offset before the durable log start (retired by DeleteRecords or
            // retention) or past the high watermark is OFFSET_OUT_OF_RANGE, so the
            // consumer applies its auto.offset.reset policy. The lower bound is the
            // durable log start, not the first surviving row: aborted-message cleanup
            // can delete leading rows without retiring their offsets, and a fetch
            // from such a hole must skip ahead to the next committed record.
            // fetch_offset == high_watermark is valid (caught up, waiting for data).
            let durable_log_start = bounds.log_start_offset;
            if fetch_offset < durable_log_start || fetch_offset > high_watermark {
                let mut partition_data = PartitionData::default();
                partition_data.partition_index = partition_id;
                partition_data.error_code = crate::kafka::constants::ERROR_OFFSET_OUT_OF_RANGE;
                partition_data.high_watermark = high_watermark;
                partition_data.last_stable_offset = last_stable_offset;
                partition_data.log_start_offset = log_start_offset;
                partition_responses.push(partition_data);
                continue;
            }

            // Fetch messages from storage
            // Phase 10: Use isolation-aware fetch for transactional reads
            // Request-level max_bytes: once the response budget is spent, later
            // partitions report their bounds but no records.
            let Some(limit) =
                partition_fetch_limit(partition_max_bytes, response_remaining, returned_any)
            else {
                let mut partition_data = PartitionData::default();
                partition_data.partition_index = partition_id;
                partition_data.error_code = ERROR_NONE;
                partition_data.high_watermark = high_watermark;
                partition_data.last_stable_offset = last_stable_offset;
                partition_data.log_start_offset = log_start_offset;
                partition_data.records = Some(bytes::Bytes::new());
                partition_responses.push(partition_data);
                continue;
            };
            let storage_empty: bool;
            let db_records = match store.fetch_records_with_isolation(
                topic_id,
                partition_id,
                fetch_offset,
                limit,
                isolation_level,
            ) {
                Ok(records) => {
                    storage_empty = records.is_empty();
                    let (kept, used) =
                        trim_to_response_budget(records, response_remaining, returned_any);
                    response_remaining -= used;
                    returned_any |= !kept.is_empty();
                    kept
                }
                Err(e) => {
                    // Use typed error's Kafka error code for proper protocol response
                    let error_code = e.to_kafka_error_code();
                    if e.is_server_error() {
                        crate::pg_warning!(
                            "Failed to fetch records for topic_id={}, partition={}, offset={}: {}",
                            topic_id,
                            partition_id,
                            fetch_offset,
                            e
                        );
                    }
                    let mut partition_data = PartitionData::default();
                    partition_data.partition_index = partition_id;
                    partition_data.error_code = error_code;
                    partition_data.high_watermark = -1;
                    partition_responses.push(partition_data);
                    continue;
                }
            };

            // Convert database records to Kafka RecordBatch format: one batch per run of
            // consecutive offsets of one producer.
            let records = if !db_records.is_empty() {
                encode_record_batches(db_records, compression)
            } else if storage_empty {
                // Nothing visible from fetch_offset up to the readable end. With offsets in
                // between (aborted rows filtered for read_committed, rows deleted by
                // aborted-row cleanup or retention), an empty response would leave the
                // consumer polling the same offset forever; a marker batch covering them
                // moves it to the readable end, as the abort marker after an aborted
                // transaction does in Kafka.
                let readable_end = match isolation_level {
                    IsolationLevel::ReadCommitted => last_stable_offset,
                    IsolationLevel::ReadUncommitted => high_watermark,
                };
                Ok(gap_batch(fetch_offset, readable_end).unwrap_or_default())
            } else {
                // Empty bytes rather than None: a null records field is an "invalid
                // MessageSetSize -1" in the flexible format.
                Ok(bytes::Bytes::new())
            };
            partition_responses.push(fetched_partition_data(
                topic_id,
                partition_id,
                &bounds,
                records,
            ));
        }

        let mut topic_response = FetchableTopicResponse::default();
        topic_response.topic = TopicName(StrBytes::from_string(topic_name));
        topic_response.partitions = partition_responses;
        responses.push(topic_response);
    }

    let mut kafka_response = FetchResponse::default();
    kafka_response.throttle_time_ms = 0;
    kafka_response.error_code = ERROR_NONE;
    kafka_response.session_id = 0;
    kafka_response.responses = responses;

    Ok(kafka_response)
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
    bounds: &crate::kafka::storage::PartitionBounds,
    records: std::result::Result<bytes::Bytes, String>,
) -> kafka_protocol::messages::fetch_response::PartitionData {
    let mut partition_data = kafka_protocol::messages::fetch_response::PartitionData::default();
    partition_data.partition_index = partition_id;
    let records = match records {
        Ok(records) => records,
        Err(e) => {
            crate::pg_warning!(
                "Failed to encode RecordBatch for topic_id={}, partition={}: {}",
                topic_id,
                partition_id,
                e
            );
            partition_data.error_code = crate::kafka::constants::ERROR_KAFKA_STORAGE_ERROR;
            partition_data.high_watermark = -1;
            partition_data.records = Some(bytes::Bytes::new());
            return partition_data;
        }
    };
    partition_data.error_code = ERROR_NONE;
    partition_data.high_watermark = bounds.high_watermark;
    partition_data.last_stable_offset = bounds.last_stable_offset;
    partition_data.log_start_offset = bounds.earliest_offset;
    partition_data.records = Some(records);
    partition_data
}

/// Handle ListOffsets request
///
/// Kafka's special timestamps:
/// - -2 = earliest offset (log start)
/// - -1 = latest offset: the high watermark, or the last stable offset for a
///   read_committed request (wave 3)
/// - -3 = MAX_TIMESTAMP (KIP-734): offset and timestamp of the record with the largest
///   timestamp (wave 3)
/// - >= 0 = the earliest offset whose timestamp is at or after it
///
/// Timestamp lookups (-3 and >= 0) are bounded like Kafka's: a match at or beyond the
/// high watermark (read_committed: the last stable offset) is reported as offset -1.
/// A partition outside the topic is UNKNOWN_TOPIC_OR_PARTITION (wave 3; was offset 0).
pub fn handle_list_offsets(
    ctx: &HandlerContext,
    topics: Vec<crate::kafka::messages::ListOffsetsTopicData>,
    isolation_level: IsolationLevel,
) -> Result<kafka_protocol::messages::list_offsets_response::ListOffsetsResponse> {
    let store = ctx.store;
    use kafka_protocol::messages::list_offsets_response::{
        ListOffsetsPartitionResponse, ListOffsetsResponse, ListOffsetsTopicResponse,
    };

    let mut response_topics = Vec::new();

    for topic_request in topics {
        let topic_name = topic_request.name.clone();

        let (topic_id, partition_count) = match resolve_topic(store, &topic_name) {
            Ok(Some(found)) => found,
            unresolved => {
                let error_code = match unresolved {
                    Err(code) => code,
                    _ => crate::kafka::constants::ERROR_UNKNOWN_TOPIC_OR_PARTITION,
                };
                let mut topic_response = ListOffsetsTopicResponse::default();
                topic_response.name = TopicName(StrBytes::from_string(topic_name));
                topic_response.partitions = topic_request
                    .partitions
                    .iter()
                    .map(|p| list_offsets_error(p.partition_index, error_code))
                    .collect();
                response_topics.push(topic_response);
                continue;
            }
        };

        let mut partition_responses = Vec::new();
        for partition_request in topic_request.partitions {
            let partition_id = partition_request.partition_index;
            if partition_id < 0 || partition_id >= partition_count {
                partition_responses.push(list_offsets_error(
                    partition_id,
                    crate::kafka::constants::ERROR_UNKNOWN_TOPIC_OR_PARTITION,
                ));
                continue;
            }
            let result = list_partition_offset(
                store,
                topic_id,
                partition_id,
                partition_request.timestamp,
                isolation_level,
            );
            let mut partition_response = ListOffsetsPartitionResponse::default();
            partition_response.partition_index = partition_id;
            match result {
                Ok((offset, timestamp)) => {
                    partition_response.error_code = ERROR_NONE;
                    partition_response.offset = offset;
                    partition_response.timestamp = timestamp;
                }
                Err(e) => {
                    if e.is_server_error() {
                        crate::pg_warning!(
                            "ListOffsets failed for topic_id={}, partition={}, timestamp={}: {}",
                            topic_id,
                            partition_id,
                            partition_request.timestamp,
                            e
                        );
                    }
                    partition_response = list_offsets_error(partition_id, e.to_kafka_error_code());
                }
            }
            partition_responses.push(partition_response);
        }

        let mut topic_response = ListOffsetsTopicResponse::default();
        topic_response.name = TopicName(StrBytes::from_string(topic_name));
        topic_response.partitions = partition_responses;
        response_topics.push(topic_response);
    }

    let mut kafka_response = ListOffsetsResponse::default();
    kafka_response.throttle_time_ms = 0;
    kafka_response.topics = response_topics;
    Ok(kafka_response)
}

/// A ListOffsets partition entry carrying `error_code` (offset and timestamp -1).
fn list_offsets_error(
    partition_index: i32,
    error_code: i16,
) -> kafka_protocol::messages::list_offsets_response::ListOffsetsPartitionResponse {
    let mut p =
        kafka_protocol::messages::list_offsets_response::ListOffsetsPartitionResponse::default();
    p.partition_index = partition_index;
    p.error_code = error_code;
    p.offset = -1;
    p.timestamp = -1;
    p
}

/// `(offset, timestamp)` for one ListOffsets partition query (see
/// [`handle_list_offsets`]).
fn list_partition_offset(
    store: &dyn crate::kafka::storage::KafkaStore,
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
mod storage_stream_tests {
    use super::*;
    use crate::kafka::messages::{PartitionFetchData, TopicFetchData};
    use crate::kafka::storage::roles::*;
    use crate::kafka::storage::{PartitionBounds, TopicMetadata};
    use crate::kafka::{BrokerMetadata, GroupCoordinator};
    use crate::testing::mocks::MockKafkaStore;
    use kafka_protocol::records::Compression;

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
        assert_eq!(
            p.error_code,
            crate::kafka::constants::ERROR_KAFKA_STORAGE_ERROR
        );
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
        assert_eq!(message_size_estimate(&m), 74);
        m.key = Some(vec![1, 2, 3]);
        assert_eq!(message_size_estimate(&m), 77);
        m.value = None;
        assert_eq!(message_size_estimate(&m), 67);
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
        assert_eq!(used, 564);
    }

    #[test]
    fn trim_drops_overflow_after_first_partition() {
        let (kept, used) = trim_to_response_budget(vec![msg(0, 500)], 100, true);
        assert!(kept.is_empty());
        assert_eq!(used, 0);
        let (kept, used) =
            trim_to_response_budget(vec![msg(0, 10), msg(1, 10), msg(2, 10)], 150, true);
        assert_eq!(kept.len(), 2);
        assert_eq!(used, 148);
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
        let mut mock = MockKafkaStore::new();
        mock.expect_get_topic_metadata().returning(|_| {
            Ok(vec![TopicMetadata {
                name: "t".to_string(),
                id: 1,
                partition_count: 2,
            }])
        });
        mock.expect_get_high_watermark().returning(|_, _| Ok(3));
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
            .withf(|_, partition, _, limit, _| *partition == 0 && *limit == 1)
            .returning(|_, _, _, _, _| Ok(vec![msg(0, 100), msg(1, 100)]));
        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        let response = handle_fetch_with_max_bytes(
            &ctx,
            two_partition_request(),
            IsolationLevel::ReadUncommitted,
            1,
        )
        .unwrap();
        let parts = &response.responses[0].partitions;
        assert_eq!(parts.len(), 2);
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
            .returning(|_, _, _, _, _| Ok(vec![msg(0, 100)]));
        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

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
            .returning(|_, _, _, _, _| Ok(vec![]));
        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

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
            crate::kafka::constants::ERROR_OFFSET_OUT_OF_RANGE
        );
    }

    // ===== wave3 txn: batch grouping and gap batches =====

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
        // time used to be grouped into one batch that could not be encoded, and every
        // fetch of the partition failed.
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
        // Before wave 3 every record was its own batch (61 bytes of header each).
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
        let len = i32::from_be_bytes(b[8..12].try_into().unwrap()) as usize;
        assert_eq!(len + 12, b.len());
        assert_eq!(b[16], 2); // magic
        let crc = u32::from_be_bytes(b[17..21].try_into().unwrap());
        assert_eq!(crc, crc32c(&b[21..]));
        assert_eq!(i16::from_be_bytes(b[21..23].try_into().unwrap()), 0x30);
        assert_eq!(i32::from_be_bytes(b[23..27].try_into().unwrap()), 2); // last delta
        assert_eq!(i32::from_be_bytes(b[57..61].try_into().unwrap()), 1); // records
                                                                          // What a client decodes: one control record (COMMIT marker) at offset 4.
        use kafka_protocol::records::RecordBatchDecoder;
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
        assert_eq!(set[0].records[0].offset, i32::MAX as i64);
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
        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(mock, &coordinator, &broker, 1, Compression::None);
        let mut req = two_partition_request();
        req[0].partitions.truncate(1);
        req[0].partitions[0].fetch_offset = offset;
        let r = handle_fetch(&ctx, req, isolation).unwrap();
        assert_eq!(r.responses[0].partitions[0].error_code, ERROR_NONE);
        r.responses[0].partitions[0].records.clone().unwrap()
    }

    /// The read_committed stall: offsets 2..5 are aborted (filtered), nothing visible
    /// is left. Before wave 3 the response was empty and the consumer re-fetched offset
    /// 2 forever; now an empty batch moves it to the LSO (= HWM 5).
    #[test]
    fn invisible_tail_is_covered_by_a_gap_batch() {
        let mut mock = one_partition_mock(5, 5);
        mock.expect_fetch_records_with_isolation()
            .returning(|_, _, _, _, _| Ok(vec![]));
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
            .returning(|_, _, _, _, _| Ok(vec![]));
        assert_eq!(
            fetch_one(&mock, 1, IsolationLevel::ReadCommitted),
            gap_batch(1, 3).unwrap()
        );
        assert!(fetch_one(&mock, 3, IsolationLevel::ReadCommitted).is_empty());
    }

    /// The default get_partition_bounds skips the LSO query for ReadUncommitted.
    #[test]
    fn read_uncommitted_bounds_do_not_query_lso() {
        let mut mock = MockKafkaStore::new();
        mock.expect_get_high_watermark().returning(|_, _| Ok(4));
        mock.expect_get_last_stable_offset().never();
        mock.expect_get_earliest_offset().returning(|_, _| Ok(0));
        mock.expect_get_log_start_offset().returning(|_, _| Ok(0));
        let b = mock
            .get_partition_bounds(1, 0, IsolationLevel::ReadUncommitted)
            .unwrap();
        assert_eq!(
            b,
            PartitionBounds {
                high_watermark: 4,
                last_stable_offset: 4,
                earliest_offset: 0,
                log_start_offset: 0,
            }
        );
    }
}
