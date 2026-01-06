// Consumer offset handlers
//
// Handlers for OffsetCommit and OffsetFetch requests.
// These track consumer group progress through partitions.

use super::helpers::{resolve_topic_id, topic_resolution_error_code, TopicResolution};
use crate::kafka::constants::*;
use crate::kafka::error::Result;
use crate::kafka::storage::{CommittedOffset, KafkaStore};
use kafka_protocol::messages::TopicName;
use kafka_protocol::protocol::StrBytes;

/// Handle OffsetCommit request
///
/// Commits consumer offsets for a consumer group
pub fn handle_offset_commit(
    store: &impl KafkaStore,
    group_id: String,
    topics: Vec<crate::kafka::messages::OffsetCommitTopicData>,
) -> Result<kafka_protocol::messages::offset_commit_response::OffsetCommitResponse> {
    use kafka_protocol::messages::offset_commit_response::{
        OffsetCommitResponse, OffsetCommitResponsePartition, OffsetCommitResponseTopic,
    };

    let mut response_topics = Vec::new();

    // Process each topic
    for topic in topics {
        let topic_name = topic.name.clone();

        // Look up topic_id using helper
        let topic_id = match resolve_topic_id(store, &topic_name) {
            TopicResolution::Found(id) => id,
            resolution => {
                // Topic not found or error - return error for all partitions
                let error_code = topic_resolution_error_code(&resolution);
                let mut error_partitions = Vec::new();
                for partition in topic.partitions {
                    let mut partition_response = OffsetCommitResponsePartition::default();
                    partition_response.partition_index = partition.partition_index;
                    partition_response.error_code = error_code;
                    error_partitions.push(partition_response);
                }

                let mut topic_response = OffsetCommitResponseTopic::default();
                topic_response.name = TopicName(StrBytes::from_string(topic_name));
                topic_response.partitions = error_partitions;
                response_topics.push(topic_response);
                continue;
            }
        };

        let mut partition_responses = Vec::new();

        // Process each partition offset commit
        for partition in topic.partitions {
            let partition_id = partition.partition_index;
            let committed_offset = partition.committed_offset;
            let metadata = partition.metadata.as_deref();

            // Commit offset
            let error_code = match store.commit_offset(
                &group_id,
                topic_id,
                partition_id,
                committed_offset,
                metadata,
            ) {
                Ok(_) => ERROR_NONE,
                Err(e) => {
                    crate::pg_warning!(
                        "Failed to commit offset for group='{}', topic_id={}, partition={}: {}",
                        group_id,
                        topic_id,
                        partition_id,
                        e
                    );
                    ERROR_UNKNOWN_SERVER_ERROR
                }
            };

            let mut partition_response = OffsetCommitResponsePartition::default();
            partition_response.partition_index = partition_id;
            partition_response.error_code = error_code;
            partition_responses.push(partition_response);
        }

        let mut topic_response = OffsetCommitResponseTopic::default();
        topic_response.name = TopicName(StrBytes::from_string(topic_name));
        topic_response.partitions = partition_responses;
        response_topics.push(topic_response);
    }

    // Build response
    let mut kafka_response = OffsetCommitResponse::default();
    kafka_response.throttle_time_ms = 0;
    kafka_response.topics = response_topics;

    Ok(kafka_response)
}

/// Handle OffsetFetch request
///
/// Fetches committed offsets for a consumer group
pub fn handle_offset_fetch(
    store: &impl KafkaStore,
    group_id: String,
    topics: Option<Vec<crate::kafka::messages::OffsetFetchTopicData>>,
) -> Result<kafka_protocol::messages::offset_fetch_response::OffsetFetchResponse> {
    use kafka_protocol::messages::offset_fetch_response::{
        OffsetFetchResponse, OffsetFetchResponsePartition, OffsetFetchResponseTopic,
    };

    let mut response_topics = Vec::new();

    if let Some(topic_list) = topics {
        // Fetch specific topics and partitions
        for topic_request in topic_list {
            let topic_name = topic_request.name.clone();

            // Look up topic_id using helper
            let topic_id = match resolve_topic_id(store, &topic_name) {
                TopicResolution::Found(id) => id,
                resolution => {
                    // Topic not found or error - return error for all partitions
                    let error_code = topic_resolution_error_code(&resolution);
                    let mut error_partitions = Vec::new();
                    for partition_index in topic_request.partition_indexes {
                        let mut partition_response = OffsetFetchResponsePartition::default();
                        partition_response.partition_index = partition_index;
                        partition_response.error_code = error_code;
                        partition_response.committed_offset = -1;
                        error_partitions.push(partition_response);
                    }

                    let mut topic_response = OffsetFetchResponseTopic::default();
                    topic_response.name = TopicName(StrBytes::from_string(topic_name));
                    topic_response.partitions = error_partitions;
                    response_topics.push(topic_response);
                    continue;
                }
            };

            let mut partition_responses = Vec::new();

            // Fetch committed offset for each requested partition
            for partition_index in topic_request.partition_indexes {
                let offset_result = store.fetch_offset(&group_id, topic_id, partition_index);

                let mut partition_response = OffsetFetchResponsePartition::default();
                partition_response.partition_index = partition_index;

                match offset_result {
                    Ok(Some(CommittedOffset { offset, metadata })) => {
                        partition_response.committed_offset = offset;
                        partition_response.metadata = metadata.map(StrBytes::from_string);
                        partition_response.error_code = ERROR_NONE;
                    }
                    Ok(None) => {
                        // No committed offset yet, return -1
                        partition_response.committed_offset = -1;
                        partition_response.error_code = ERROR_NONE;
                    }
                    Err(e) => {
                        crate::pg_warning!(
                            "Failed to fetch offset for group='{}', topic_id={}, partition={}: {}",
                            group_id,
                            topic_id,
                            partition_index,
                            e
                        );
                        partition_response.committed_offset = -1;
                        partition_response.error_code = ERROR_UNKNOWN_SERVER_ERROR;
                    }
                }

                partition_responses.push(partition_response);
            }

            let mut topic_response = OffsetFetchResponseTopic::default();
            topic_response.name = TopicName(StrBytes::from_string(topic_name));
            topic_response.partitions = partition_responses;
            response_topics.push(topic_response);
        }
    } else {
        // Fetch all topics for this consumer group
        let all_offsets_result = store.fetch_all_offsets(&group_id);

        match all_offsets_result {
            Ok(offsets) => {
                // Group by topic
                let mut topics_map: std::collections::HashMap<
                    String,
                    Vec<OffsetFetchResponsePartition>,
                > = std::collections::HashMap::new();

                for (topic_name, partition_id, CommittedOffset { offset, metadata }) in offsets {
                    let mut partition_response = OffsetFetchResponsePartition::default();
                    partition_response.partition_index = partition_id;
                    partition_response.committed_offset = offset;
                    partition_response.metadata = metadata.map(StrBytes::from_string);
                    partition_response.error_code = ERROR_NONE;

                    topics_map
                        .entry(topic_name)
                        .or_default()
                        .push(partition_response);
                }

                for (topic_name, partitions) in topics_map {
                    let mut topic_response = OffsetFetchResponseTopic::default();
                    topic_response.name = TopicName(StrBytes::from_string(topic_name));
                    topic_response.partitions = partitions;
                    response_topics.push(topic_response);
                }
            }
            Err(e) => {
                // Error - return empty response
                crate::pg_warning!(
                    "Failed to fetch all offsets for group='{}': {}",
                    group_id,
                    e
                );
            }
        }
    }

    // Build response
    let mut kafka_response = OffsetFetchResponse::default();
    kafka_response.throttle_time_ms = 0;
    kafka_response.error_code = ERROR_NONE;
    kafka_response.topics = response_topics;

    Ok(kafka_response)
}
