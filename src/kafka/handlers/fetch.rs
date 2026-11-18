// Fetch handlers
//
// Handlers for FetchRequest and ListOffsetsRequest.
// These are read-focused APIs for consuming messages from topics.
// Phase 10: Added isolation_level support for transactional reads.

use super::helpers::{
    resolve_topic, resolve_topic_id, topic_resolution_error_code, TopicResolution,
};
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
    use kafka_protocol::records::{Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType};

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
            let db_records = match store.fetch_records_with_isolation(
                topic_id,
                partition_id,
                fetch_offset,
                limit,
                isolation_level,
            ) {
                Ok(records) => {
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

            // Convert database records to Kafka RecordBatch format
            let records_bytes = if !db_records.is_empty() {
                let kafka_records: Vec<Record> = db_records
                    .into_iter()
                    .map(|msg| {
                        let mut record = Record {
                            transactional: false,
                            control: false,
                            partition_leader_epoch: 0,
                            producer_id: -1,
                            producer_epoch: -1,
                            timestamp_type: TimestampType::Creation,
                            offset: msg.partition_offset,
                            sequence: 0, // Sequence in fetch response is not used; avoid i64→i32 overflow
                            timestamp: msg.timestamp,
                            key: msg.key.map(bytes::Bytes::from),
                            value: msg.value.map(bytes::Bytes::from),
                            headers: Default::default(),
                        };
                        // DR-8: return stored headers to the consumer (they were
                        // written on produce but never fetched before this).
                        for (k, v) in msg.headers {
                            record
                                .headers
                                .insert(StrBytes::from_string(k), Some(bytes::Bytes::from(v)));
                        }
                        record
                    })
                    .collect();

                // Encode records as RecordBatch
                let mut encoded = bytes::BytesMut::new();
                if let Err(e) = RecordBatchEncoder::encode(
                    &mut encoded,
                    kafka_records.iter(),
                    &RecordEncodeOptions {
                        version: 2,
                        compression,
                    },
                ) {
                    // On error, return empty bytes to avoid protocol errors
                    crate::pg_warning!(
                        "Failed to encode RecordBatch for topic_id={}, partition={}: {}",
                        topic_id,
                        partition_id,
                        e
                    );
                    Some(bytes::Bytes::new())
                } else {
                    Some(encoded.freeze())
                }
            } else {
                // Empty result set - return empty bytes instead of None
                // This avoids "invalid MessageSetSize -1" errors in flexible format
                Some(bytes::Bytes::new())
            };

            let mut partition_data = PartitionData::default();
            partition_data.partition_index = partition_id;
            partition_data.error_code = ERROR_NONE;
            partition_data.high_watermark = high_watermark;
            partition_data.last_stable_offset = last_stable_offset;
            partition_data.log_start_offset = log_start_offset;
            partition_data.records = records_bytes;
            partition_responses.push(partition_data);
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

/// Handle ListOffsets request
///
/// Returns earliest or latest offsets for requested partitions.
/// Supports Kafka's special timestamps:
/// - -2 = earliest offset
/// - -1 = latest offset (high watermark)
/// - >= 0 = offset at timestamp (not yet implemented)
pub fn handle_list_offsets(
    ctx: &HandlerContext,
    topics: Vec<crate::kafka::messages::ListOffsetsTopicData>,
) -> Result<kafka_protocol::messages::list_offsets_response::ListOffsetsResponse> {
    let store = ctx.store;
    use kafka_protocol::messages::list_offsets_response::{
        ListOffsetsPartitionResponse, ListOffsetsResponse, ListOffsetsTopicResponse,
    };

    let mut response_topics = Vec::new();

    // Process each topic
    for topic_request in topics {
        let topic_name = topic_request.name.clone();

        // Look up topic_id using helper
        let topic_id = match resolve_topic_id(store, &topic_name) {
            TopicResolution::Found(id) => id,
            resolution => {
                // Topic not found or error - return error for all partitions
                let error_code = topic_resolution_error_code(&resolution);
                let mut error_partitions = Vec::new();
                for partition in topic_request.partitions {
                    let mut partition_response = ListOffsetsPartitionResponse::default();
                    partition_response.partition_index = partition.partition_index;
                    partition_response.error_code = error_code;
                    error_partitions.push(partition_response);
                }

                let mut topic_response = ListOffsetsTopicResponse::default();
                topic_response.name = TopicName(StrBytes::from_string(topic_name));
                topic_response.partitions = error_partitions;
                response_topics.push(topic_response);
                continue;
            }
        };

        let mut partition_responses = Vec::new();

        // Process each partition
        for partition_request in topic_request.partitions {
            let partition_id = partition_request.partition_index;
            let timestamp = partition_request.timestamp;

            let mut partition_response = ListOffsetsPartitionResponse::default();
            partition_response.partition_index = partition_id;

            // Determine which offset to return based on timestamp
            let offset = match timestamp {
                -2 => {
                    // Earliest offset
                    match store.get_earliest_offset(topic_id, partition_id) {
                        Ok(offset) => offset,
                        Err(e) => {
                            // Use typed error's Kafka error code
                            let error_code = e.to_kafka_error_code();
                            if e.is_server_error() {
                                crate::pg_warning!(
                                    "Failed to get earliest offset for topic_id={}, partition={}: {}",
                                    topic_id,
                                    partition_id,
                                    e
                                );
                            }
                            partition_response.error_code = error_code;
                            partition_responses.push(partition_response);
                            continue;
                        }
                    }
                }
                -1 => {
                    // Latest offset (high watermark)
                    match store.get_high_watermark(topic_id, partition_id) {
                        Ok(offset) => offset,
                        Err(e) => {
                            // Use typed error's Kafka error code
                            let error_code = e.to_kafka_error_code();
                            if e.is_server_error() {
                                crate::pg_warning!(
                                    "Failed to get high watermark for topic_id={}, partition={}: {}",
                                    topic_id,
                                    partition_id,
                                    e
                                );
                            }
                            partition_response.error_code = error_code;
                            partition_responses.push(partition_response);
                            continue;
                        }
                    }
                }
                _ => {
                    // CONF-7: a real timestamp lookup — the earliest offset whose stored record
                    // timestamp (BUG-7) is at or after the requested one. Per the protocol, return
                    // offset -1 (and timestamp -1) when no record qualifies, rather than erroring.
                    match store.get_offset_for_timestamp(topic_id, partition_id, timestamp) {
                        Ok(Some((offset, record_ts))) => {
                            partition_response.error_code = ERROR_NONE;
                            partition_response.offset = offset;
                            partition_response.timestamp = record_ts;
                        }
                        Ok(None) => {
                            partition_response.error_code = ERROR_NONE;
                            partition_response.offset = -1;
                            partition_response.timestamp = -1;
                        }
                        Err(e) => {
                            let error_code = e.to_kafka_error_code();
                            if e.is_server_error() {
                                crate::pg_warning!(
                                    "Failed timestamp lookup for topic_id={}, partition={}: {}",
                                    topic_id,
                                    partition_id,
                                    e
                                );
                            }
                            partition_response.error_code = error_code;
                        }
                    }
                    partition_responses.push(partition_response);
                    continue;
                }
            };

            partition_response.error_code = ERROR_NONE;
            partition_response.offset = offset;
            partition_response.timestamp = -1; // Not used for special offsets
            partition_responses.push(partition_response);
        }

        let mut topic_response = ListOffsetsTopicResponse::default();
        topic_response.name = TopicName(StrBytes::from_string(topic_name));
        topic_response.partitions = partition_responses;
        response_topics.push(topic_response);
    }

    // Build response
    let mut kafka_response = ListOffsetsResponse::default();
    kafka_response.throttle_time_ms = 0;
    kafka_response.topics = response_topics;

    Ok(kafka_response)
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
        }
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
