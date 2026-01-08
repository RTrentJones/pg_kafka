// Fetch handlers
//
// Handlers for FetchRequest and ListOffsetsRequest.
// These are read-focused APIs for consuming messages from topics.

use super::helpers::{resolve_topic_id, topic_resolution_error_code, TopicResolution};
use crate::kafka::constants::{ERROR_NONE, ERROR_UNSUPPORTED_VERSION};
use crate::kafka::error::Result;
use crate::kafka::storage::KafkaStore;
use kafka_protocol::messages::TopicName;
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::Compression;

/// Handle Fetch request
///
/// Fetches messages from storage and encodes them as Kafka RecordBatch.
/// The compression parameter controls the encoding compression for the response.
pub fn handle_fetch(
    store: &impl KafkaStore,
    topic_data: Vec<crate::kafka::messages::TopicFetchData>,
    compression: Compression,
) -> Result<kafka_protocol::messages::fetch_response::FetchResponse> {
    use kafka_protocol::messages::fetch_response::{
        FetchResponse, FetchableTopicResponse, PartitionData,
    };
    use kafka_protocol::records::{Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType};

    let mut responses = Vec::new();

    // Process each topic requested
    for topic_fetch in topic_data {
        let topic_name = topic_fetch.name.clone();

        // Look up topic by name using helper
        let topic_id = match resolve_topic_id(store, &topic_name) {
            TopicResolution::Found(id) => id,
            resolution => {
                // Topic not found or error - return error for all partitions
                let error_code = topic_resolution_error_code(&resolution);
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
            let max_bytes = partition_fetch.partition_max_bytes;

            // Fetch messages from storage
            let db_records =
                match store.fetch_records(topic_id, partition_id, fetch_offset, max_bytes) {
                    Ok(records) => records,
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

            // Get high watermark
            let high_watermark = store
                .get_high_watermark(topic_id, partition_id)
                .unwrap_or(0);

            // Convert database records to Kafka RecordBatch format
            let records_bytes = if !db_records.is_empty() {
                let kafka_records: Vec<Record> = db_records
                    .into_iter()
                    .map(|msg| Record {
                        transactional: false,
                        control: false,
                        partition_leader_epoch: 0,
                        producer_id: -1,
                        producer_epoch: -1,
                        timestamp_type: TimestampType::Creation,
                        offset: msg.partition_offset,
                        sequence: msg.partition_offset as i32,
                        timestamp: msg.timestamp,
                        key: msg.key.map(bytes::Bytes::from),
                        value: msg.value.map(bytes::Bytes::from),
                        headers: Default::default(),
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
            partition_data.last_stable_offset = high_watermark;
            partition_data.log_start_offset = 0;
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
    store: &impl KafkaStore,
    topics: Vec<crate::kafka::messages::ListOffsetsTopicData>,
) -> Result<kafka_protocol::messages::list_offsets_response::ListOffsetsResponse> {
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
                    // Timestamp-based lookup not yet implemented
                    // For now, return UNSUPPORTED_VERSION error
                    partition_response.error_code = ERROR_UNSUPPORTED_VERSION;
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
