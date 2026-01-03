// Pure Kafka protocol handlers
//
// This module contains pure protocol logic that is decoupled from the storage implementation.
// Each handler accepts a storage trait and returns protocol responses without knowing about SQL.
//
// Architecture:
// - Handlers accept &impl KafkaStore (dependency injection)
// - They focus on protocol logic: parsing requests, coordinating storage calls, building responses
// - They know nothing about SQL, SPI, or transactions
// - Transaction boundaries remain explicit in worker.rs

use super::constants::*;
use super::error::{KafkaError, Result};
use super::storage::{CommittedOffset, KafkaStore};
use kafka_protocol::messages::TopicName;
use kafka_protocol::protocol::StrBytes;

/// Handle ApiVersions request
///
/// This handler doesn't need storage - it just returns static protocol information
pub fn handle_api_versions() -> kafka_protocol::messages::api_versions_response::ApiVersionsResponse
{
    crate::kafka::build_api_versions_response()
}

/// Handle Metadata request
///
/// Returns metadata for all topics or specific requested topics.
/// Auto-creates topics if they don't exist when specifically requested.
pub fn handle_metadata(
    store: &impl KafkaStore,
    requested_topics: Option<Vec<String>>,
    broker_host: String,
    broker_port: i32,
) -> Result<kafka_protocol::messages::metadata_response::MetadataResponse> {
    let mut response = kafka_protocol::messages::metadata_response::MetadataResponse::default();

    // Add broker
    response.brokers.push(crate::kafka::build_broker_metadata(
        DEFAULT_BROKER_ID,
        broker_host,
        broker_port,
    ));

    // Build topic metadata based on what was requested
    let topics_to_add = match requested_topics {
        None => {
            // Client wants all topics - query from storage
            let stored_topics = store.get_topic_metadata(None)?;
            stored_topics
                .into_iter()
                .map(|tm| {
                    let partition = crate::kafka::build_partition_metadata(
                        0,
                        DEFAULT_BROKER_ID,
                        vec![DEFAULT_BROKER_ID],
                        vec![DEFAULT_BROKER_ID],
                    );
                    crate::kafka::build_topic_metadata(tm.name, ERROR_NONE, vec![partition])
                })
                .collect()
        }
        Some(topic_names) => {
            // Client wants specific topics - create them if needed and return metadata
            let mut topics_metadata = Vec::new();
            for topic_name in topic_names {
                // Auto-create topic if it doesn't exist
                match store.get_or_create_topic(&topic_name) {
                    Ok(_topic_id) => {
                        // Return metadata for this topic
                        let partition = crate::kafka::build_partition_metadata(
                            0,
                            DEFAULT_BROKER_ID,
                            vec![DEFAULT_BROKER_ID],
                            vec![DEFAULT_BROKER_ID],
                        );
                        let topic = crate::kafka::build_topic_metadata(
                            topic_name,
                            ERROR_NONE,
                            vec![partition],
                        );
                        topics_metadata.push(topic);
                    }
                    Err(_e) => {
                        // Return error for this topic
                        let topic = crate::kafka::build_topic_metadata(
                            topic_name,
                            ERROR_UNKNOWN_SERVER_ERROR,
                            vec![],
                        );
                        topics_metadata.push(topic);
                    }
                }
            }
            topics_metadata
        }
    };

    response.topics = topics_to_add;
    Ok(response)
}

/// Handle Produce request
///
/// Inserts records into storage and returns base offsets for each partition
pub fn handle_produce(
    store: &impl KafkaStore,
    topic_data: Vec<crate::kafka::messages::TopicProduceData>,
) -> Result<kafka_protocol::messages::produce_response::ProduceResponse> {
    let mut kafka_response = crate::kafka::build_produce_response();

    // Process each topic
    for topic in topic_data {
        let topic_name: String = topic.name.clone();

        // Get or create topic
        let topic_id = store.get_or_create_topic(&topic_name)?;

        // Build topic response
        let mut topic_response =
            kafka_protocol::messages::produce_response::TopicProduceResponse::default();
        topic_response.name = kafka_protocol::messages::TopicName(topic_name.clone().into());

        // Process each partition
        for partition in topic.partitions {
            let partition_index = partition.partition_index;

            let mut partition_response =
                kafka_protocol::messages::produce_response::PartitionProduceResponse::default();
            partition_response.index = partition_index;

            // Validate partition index (we only support partition 0 for now)
            if partition_index != 0 {
                partition_response.error_code = ERROR_UNKNOWN_TOPIC_OR_PARTITION;
                partition_response.base_offset = -1;
                partition_response.log_append_time_ms = -1;
                partition_response.log_start_offset = -1;
                topic_response.partition_responses.push(partition_response);
                continue;
            }

            // Insert records
            match store.insert_records(topic_id, partition_index, &partition.records) {
                Ok(base_offset) => {
                    partition_response.error_code = ERROR_NONE;
                    partition_response.base_offset = base_offset;
                    partition_response.log_append_time_ms = -1;
                    partition_response.log_start_offset = -1;
                }
                Err(_e) => {
                    partition_response.error_code = ERROR_UNKNOWN_SERVER_ERROR;
                    partition_response.base_offset = -1;
                    partition_response.log_append_time_ms = -1;
                    partition_response.log_start_offset = -1;
                }
            }

            topic_response.partition_responses.push(partition_response);
        }

        kafka_response.responses.push(topic_response);
    }

    Ok(kafka_response)
}

/// Handle Fetch request
///
/// Fetches messages from storage and encodes them as Kafka RecordBatch
pub fn handle_fetch(
    store: &impl KafkaStore,
    topic_data: Vec<crate::kafka::messages::TopicFetchData>,
) -> Result<kafka_protocol::messages::fetch_response::FetchResponse> {
    use kafka_protocol::messages::fetch_response::{
        FetchResponse, FetchableTopicResponse, PartitionData,
    };
    use kafka_protocol::records::{
        Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
    };

    let mut responses = Vec::new();

    // Process each topic requested
    for topic_fetch in topic_data {
        let topic_name = topic_fetch.name.clone();

        // Look up topic by name
        let topics = store.get_topic_metadata(Some(std::slice::from_ref(&topic_name)))?;
        let topic_id = if let Some(tm) = topics.first() {
            tm.id
        } else {
            // Topic not found - return error for all partitions
            let mut error_partitions = Vec::new();
            for p in topic_fetch.partitions {
                let mut partition_data = PartitionData::default();
                partition_data.partition_index = p.partition_index;
                partition_data.error_code = ERROR_UNKNOWN_TOPIC_OR_PARTITION;
                partition_data.high_watermark = -1;
                error_partitions.push(partition_data);
            }
            let mut topic_response = FetchableTopicResponse::default();
            topic_response.topic = TopicName(StrBytes::from_string(topic_name));
            topic_response.partitions = error_partitions;
            responses.push(topic_response);
            continue;
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
                    Err(_e) => {
                        let mut partition_data = PartitionData::default();
                        partition_data.partition_index = partition_id;
                        partition_data.error_code = ERROR_UNKNOWN_SERVER_ERROR;
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
                if let Err(_e) = RecordBatchEncoder::encode(
                    &mut encoded,
                    kafka_records.iter(),
                    &RecordEncodeOptions {
                        version: 2,
                        compression: Compression::None,
                    },
                ) {
                    // On error, return empty bytes to avoid protocol errors
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

        // Look up topic_id
        let topics_result = store.get_topic_metadata(Some(std::slice::from_ref(&topic_name)));
        let topic_id = match topics_result {
            Ok(topics) => {
                if let Some(tm) = topics.first() {
                    tm.id
                } else {
                    // Topic not found
                    let mut error_partitions = Vec::new();
                    for partition in topic.partitions {
                        let mut partition_response = OffsetCommitResponsePartition::default();
                        partition_response.partition_index = partition.partition_index;
                        partition_response.error_code = ERROR_UNKNOWN_TOPIC_OR_PARTITION;
                        error_partitions.push(partition_response);
                    }

                    let mut topic_response = OffsetCommitResponseTopic::default();
                    topic_response.name = TopicName(StrBytes::from_string(topic_name));
                    topic_response.partitions = error_partitions;
                    response_topics.push(topic_response);
                    continue;
                }
            }
            Err(_e) => {
                // Error looking up topic
                let mut error_partitions = Vec::new();
                for partition in topic.partitions {
                    let mut partition_response = OffsetCommitResponsePartition::default();
                    partition_response.partition_index = partition.partition_index;
                    partition_response.error_code = ERROR_UNKNOWN_SERVER_ERROR;
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
                Err(_e) => ERROR_UNKNOWN_SERVER_ERROR,
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

            // Look up topic_id
            let topics_result = store.get_topic_metadata(Some(std::slice::from_ref(&topic_name)));
            let topic_id = match topics_result {
                Ok(topics) => {
                    if let Some(tm) = topics.first() {
                        tm.id
                    } else {
                        // Topic not found
                        let mut error_partitions = Vec::new();
                        for partition_index in topic_request.partition_indexes {
                            let mut partition_response = OffsetFetchResponsePartition::default();
                            partition_response.partition_index = partition_index;
                            partition_response.error_code = ERROR_UNKNOWN_TOPIC_OR_PARTITION;
                            partition_response.committed_offset = -1;
                            error_partitions.push(partition_response);
                        }

                        let mut topic_response = OffsetFetchResponseTopic::default();
                        topic_response.name = TopicName(StrBytes::from_string(topic_name));
                        topic_response.partitions = error_partitions;
                        response_topics.push(topic_response);
                        continue;
                    }
                }
                Err(_e) => {
                    // Error looking up topic
                    let mut error_partitions = Vec::new();
                    for partition_index in topic_request.partition_indexes {
                        let mut partition_response = OffsetFetchResponsePartition::default();
                        partition_response.partition_index = partition_index;
                        partition_response.error_code = ERROR_UNKNOWN_SERVER_ERROR;
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
                    Err(_e) => {
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
            Err(_e) => {
                // Error - return empty response
                // The error is logged by the storage layer
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

/// Handle FindCoordinator request
///
/// Returns the coordinator broker for a consumer group.
/// In our single-node setup, we always return ourselves as the coordinator.
pub fn handle_find_coordinator(
    broker_host: String,
    broker_port: i32,
    key: String,
    key_type: i8,
) -> Result<kafka_protocol::messages::find_coordinator_response::FindCoordinatorResponse> {
    use kafka_protocol::messages::find_coordinator_response::FindCoordinatorResponse;

    pgrx::debug1!("FindCoordinator: key={}, key_type={}", key, key_type);

    // Build response - we are always the coordinator for all groups
    let mut response = FindCoordinatorResponse::default();
    response.error_code = ERROR_NONE;
    response.error_message = None;
    response.node_id = DEFAULT_BROKER_ID.into();
    response.host = broker_host.into();
    response.port = broker_port;

    Ok(response)
}

/// Handle JoinGroup request
///
/// Consumer joins a consumer group and receives:
/// - Assigned member ID
/// - Generation ID
/// - Leader status
/// - Member list (if leader)
#[allow(clippy::too_many_arguments)]
pub fn handle_join_group(
    coordinator: &crate::kafka::GroupCoordinator,
    group_id: String,
    member_id: String,
    client_id: String,
    session_timeout_ms: i32,
    rebalance_timeout_ms: i32,
    protocol_type: String,
    protocols: Vec<crate::kafka::messages::JoinGroupProtocol>,
) -> Result<kafka_protocol::messages::join_group_response::JoinGroupResponse> {
    use kafka_protocol::messages::join_group_response::{
        JoinGroupResponse, JoinGroupResponseMember,
    };

    pgrx::debug1!(
        "JoinGroup: group_id={}, member_id={}, client_id={}",
        group_id,
        member_id,
        client_id
    );

    // Convert protocols to coordinator format
    let coord_protocols: Vec<(String, Vec<u8>)> = protocols
        .into_iter()
        .map(|p| (p.name, p.metadata))
        .collect();

    // Get client host (use localhost for now)
    let client_host = "localhost".to_string();

    // Join group via coordinator
    let existing_member_id = if member_id.is_empty() {
        None
    } else {
        Some(member_id)
    };

    let (assigned_member_id, generation_id, is_leader, members_metadata) = coordinator
        .join_group(
            group_id.clone(),
            existing_member_id,
            client_id,
            client_host,
            session_timeout_ms,
            rebalance_timeout_ms,
            protocol_type.clone(),
            coord_protocols.clone(),
            None, // group_instance_id
        )
        .map_err(|e| match e {
            KafkaError::Internal(msg) if msg.contains("Unknown member_id") => {
                KafkaError::CoordinatorError(ERROR_UNKNOWN_MEMBER_ID, msg)
            }
            KafkaError::Internal(msg) if msg.contains("Illegal generation") => {
                KafkaError::CoordinatorError(ERROR_ILLEGAL_GENERATION, msg)
            }
            _ => e,
        })?;

    // Build response
    let mut response = JoinGroupResponse::default();
    response.error_code = ERROR_NONE;
    response.generation_id = generation_id;
    response.protocol_type = Some(protocol_type.into());
    response.protocol_name = coord_protocols.first().map(|(name, _)| name.clone().into());
    response.leader = assigned_member_id.clone().into();
    response.member_id = assigned_member_id.into();

    // If this is the leader, include member list
    if is_leader {
        response.members = members_metadata
            .into_iter()
            .map(|(mid, metadata)| {
                let mut member = JoinGroupResponseMember::default();
                member.member_id = mid.into();
                member.metadata = metadata.into();
                member
            })
            .collect();
    }

    Ok(response)
}

/// Handle SyncGroup request
///
/// Leader provides partition assignments, followers receive their assignment.
pub fn handle_sync_group(
    coordinator: &crate::kafka::GroupCoordinator,
    group_id: String,
    member_id: String,
    generation_id: i32,
    assignments: Vec<crate::kafka::messages::SyncGroupAssignment>,
) -> Result<kafka_protocol::messages::sync_group_response::SyncGroupResponse> {
    use kafka_protocol::messages::sync_group_response::SyncGroupResponse;

    pgrx::debug1!(
        "SyncGroup: group_id={}, member_id={}, generation_id={}, assignments={}",
        group_id,
        member_id,
        generation_id,
        assignments.len()
    );

    // Convert assignments to coordinator format
    let coord_assignments: Vec<(String, Vec<u8>)> = assignments
        .into_iter()
        .map(|a| (a.member_id, a.assignment))
        .collect();

    // Sync group via coordinator
    let assignment = coordinator
        .sync_group(group_id, member_id, generation_id, coord_assignments)
        .map_err(|e| match e {
            KafkaError::Internal(msg) if msg.contains("Unknown member_id") => {
                KafkaError::CoordinatorError(ERROR_UNKNOWN_MEMBER_ID, msg)
            }
            KafkaError::Internal(msg) if msg.contains("Illegal generation") => {
                KafkaError::CoordinatorError(ERROR_ILLEGAL_GENERATION, msg)
            }
            KafkaError::Internal(msg) if msg.contains("Unknown group_id") => {
                KafkaError::CoordinatorError(ERROR_COORDINATOR_NOT_AVAILABLE, msg)
            }
            _ => e,
        })?;

    // Build response
    let mut response = SyncGroupResponse::default();
    response.error_code = ERROR_NONE;
    response.assignment = assignment.into();

    Ok(response)
}

/// Handle Heartbeat request
///
/// Updates member's last heartbeat timestamp.
pub fn handle_heartbeat(
    coordinator: &crate::kafka::GroupCoordinator,
    group_id: String,
    member_id: String,
    generation_id: i32,
) -> Result<kafka_protocol::messages::heartbeat_response::HeartbeatResponse> {
    use kafka_protocol::messages::heartbeat_response::HeartbeatResponse;

    pgrx::debug1!(
        "Heartbeat: group_id={}, member_id={}, generation_id={}",
        group_id,
        member_id,
        generation_id
    );

    // Send heartbeat via coordinator
    coordinator
        .heartbeat(group_id, member_id, generation_id)
        .map_err(|e| match e {
            KafkaError::Internal(msg) if msg.contains("Unknown member_id") => {
                KafkaError::CoordinatorError(ERROR_UNKNOWN_MEMBER_ID, msg)
            }
            KafkaError::Internal(msg) if msg.contains("Illegal generation") => {
                KafkaError::CoordinatorError(ERROR_ILLEGAL_GENERATION, msg)
            }
            KafkaError::Internal(msg) if msg.contains("Unknown group_id") => {
                KafkaError::CoordinatorError(ERROR_COORDINATOR_NOT_AVAILABLE, msg)
            }
            _ => e,
        })?;

    // Build response
    let mut response = HeartbeatResponse::default();
    response.error_code = ERROR_NONE;

    Ok(response)
}

/// Handle LeaveGroup request
///
/// Consumer gracefully leaves the consumer group.
pub fn handle_leave_group(
    coordinator: &crate::kafka::GroupCoordinator,
    group_id: String,
    member_id: String,
) -> Result<kafka_protocol::messages::leave_group_response::LeaveGroupResponse> {
    use kafka_protocol::messages::leave_group_response::LeaveGroupResponse;

    pgrx::debug1!("LeaveGroup: group_id={}, member_id={}", group_id, member_id);

    // Leave group via coordinator
    coordinator
        .leave_group(group_id, member_id)
        .map_err(|e| match e {
            KafkaError::Internal(msg) if msg.contains("Unknown member_id") => {
                KafkaError::CoordinatorError(ERROR_UNKNOWN_MEMBER_ID, msg)
            }
            KafkaError::Internal(msg) if msg.contains("Unknown group_id") => {
                KafkaError::CoordinatorError(ERROR_COORDINATOR_NOT_AVAILABLE, msg)
            }
            _ => e,
        })?;

    // Build response
    let mut response = LeaveGroupResponse::default();
    response.error_code = ERROR_NONE;

    Ok(response)
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

        // Look up topic_id
        let topics_result = store.get_topic_metadata(Some(std::slice::from_ref(&topic_name)));
        let topic_id = match topics_result {
            Ok(topics) => {
                if let Some(tm) = topics.first() {
                    tm.id
                } else {
                    // Topic not found - return error for all partitions
                    let mut error_partitions = Vec::new();
                    for partition in topic_request.partitions {
                        let mut partition_response = ListOffsetsPartitionResponse::default();
                        partition_response.partition_index = partition.partition_index;
                        partition_response.error_code = ERROR_UNKNOWN_TOPIC_OR_PARTITION;
                        error_partitions.push(partition_response);
                    }

                    let mut topic_response = ListOffsetsTopicResponse::default();
                    topic_response.name = TopicName(StrBytes::from_string(topic_name));
                    topic_response.partitions = error_partitions;
                    response_topics.push(topic_response);
                    continue;
                }
            }
            Err(_e) => {
                // Error looking up topic
                let mut error_partitions = Vec::new();
                for partition in topic_request.partitions {
                    let mut partition_response = ListOffsetsPartitionResponse::default();
                    partition_response.partition_index = partition.partition_index;
                    partition_response.error_code = ERROR_UNKNOWN_SERVER_ERROR;
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
                        Err(_e) => {
                            partition_response.error_code = ERROR_UNKNOWN_SERVER_ERROR;
                            partition_responses.push(partition_response);
                            continue;
                        }
                    }
                }
                -1 => {
                    // Latest offset (high watermark)
                    match store.get_high_watermark(topic_id, partition_id) {
                        Ok(offset) => offset,
                        Err(_e) => {
                            partition_response.error_code = ERROR_UNKNOWN_SERVER_ERROR;
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

/// Handle DescribeGroups request
///
/// Returns detailed information about consumer groups including members and state.
/// This is critical for debugging consumer group issues and monitoring group health.
pub fn handle_describe_groups(
    coordinator: &super::GroupCoordinator,
    groups: Vec<String>,
) -> Result<kafka_protocol::messages::describe_groups_response::DescribeGroupsResponse> {
    use kafka_protocol::messages::describe_groups_response::{
        DescribeGroupsResponse, DescribedGroup, DescribedGroupMember,
    };
    use kafka_protocol::messages::GroupId;
    use kafka_protocol::protocol::StrBytes;

    let mut response = DescribeGroupsResponse::default();
    response.throttle_time_ms = 0;

    // Get coordinator state
    let coordinator_groups = coordinator.groups.read().unwrap();

    for group_id in groups {
        let mut described_group = DescribedGroup::default();
        described_group.group_id = GroupId(StrBytes::from_string(group_id.clone()));

        match coordinator_groups.get(&group_id) {
            Some(group) => {
                // Group exists - return detailed information
                described_group.error_code = ERROR_NONE;
                described_group.group_state = StrBytes::from_string(match group.state {
                    super::GroupState::Empty => "Empty".to_string(),
                    super::GroupState::PreparingRebalance => "PreparingRebalance".to_string(),
                    super::GroupState::CompletingRebalance => "CompletingRebalance".to_string(),
                    super::GroupState::Stable => "Stable".to_string(),
                    super::GroupState::Dead => "Dead".to_string(),
                });
                described_group.protocol_type = StrBytes::from_string(
                    group
                        .members
                        .values()
                        .next()
                        .map(|m| m.protocol_type.clone())
                        .unwrap_or_else(|| "consumer".to_string()),
                );
                described_group.protocol_data =
                    StrBytes::from_string(group.protocol_name.clone().unwrap_or_default());

                // Add member information
                for member in group.members.values() {
                    let mut described_member = DescribedGroupMember::default();
                    described_member.member_id = StrBytes::from_string(member.member_id.clone());
                    described_member.client_id = StrBytes::from_string(member.client_id.clone());
                    described_member.client_host =
                        StrBytes::from_string(member.client_host.clone());

                    // Member metadata (protocol subscription information)
                    // For now, we just store the first protocol's metadata
                    if let Some((_, metadata)) = member.protocols.first() {
                        described_member.member_metadata = metadata.clone().into();
                    }

                    // Member assignment
                    if let Some(assignment) = &member.assignment {
                        described_member.member_assignment = assignment.clone().into();
                    }

                    // Static group instance ID (KIP-345)
                    if let Some(instance_id) = &member.group_instance_id {
                        described_member.group_instance_id =
                            Some(StrBytes::from_string(instance_id.clone()));
                    }

                    described_group.members.push(described_member);
                }
            }
            None => {
                // Group doesn't exist - return error
                described_group.error_code = ERROR_UNKNOWN_SERVER_ERROR;
                described_group.group_state = StrBytes::from_string("Dead".to_string());
                described_group.protocol_type = StrBytes::from_string("".to_string());
                described_group.protocol_data = StrBytes::from_string("".to_string());
            }
        }

        response.groups.push(described_group);
    }

    Ok(response)
}

/// Handle ListGroups request
///
/// Returns a list of all consumer groups currently known to the coordinator.
/// This is useful for discovery, administration, and monitoring.
pub fn handle_list_groups(
    coordinator: &super::GroupCoordinator,
    states_filter: Vec<String>,
) -> Result<kafka_protocol::messages::list_groups_response::ListGroupsResponse> {
    use kafka_protocol::messages::list_groups_response::{ListGroupsResponse, ListedGroup};
    use kafka_protocol::messages::GroupId;
    use kafka_protocol::protocol::StrBytes;

    let mut response = ListGroupsResponse::default();
    response.error_code = ERROR_NONE;
    response.throttle_time_ms = 0;

    // Get coordinator state
    let coordinator_groups = coordinator.groups.read().unwrap();

    // Filter groups by state if requested (v4+)
    let filter_enabled = !states_filter.is_empty();

    for (group_id, group) in coordinator_groups.iter() {
        // Apply state filter if provided
        if filter_enabled {
            let state_str = match group.state {
                super::GroupState::Empty => "Empty",
                super::GroupState::PreparingRebalance => "PreparingRebalance",
                super::GroupState::CompletingRebalance => "CompletingRebalance",
                super::GroupState::Stable => "Stable",
                super::GroupState::Dead => "Dead",
            };

            if !states_filter.contains(&state_str.to_string()) {
                continue;
            }
        }

        let mut listed_group = ListedGroup::default();
        listed_group.group_id = GroupId(StrBytes::from_string(group_id.clone()));
        listed_group.protocol_type = StrBytes::from_string(
            group
                .members
                .values()
                .next()
                .map(|m| m.protocol_type.clone())
                .unwrap_or_else(|| "consumer".to_string()),
        );

        // Add group state (v4+)
        listed_group.group_state = StrBytes::from_string(match group.state {
            super::GroupState::Empty => "Empty".to_string(),
            super::GroupState::PreparingRebalance => "PreparingRebalance".to_string(),
            super::GroupState::CompletingRebalance => "CompletingRebalance".to_string(),
            super::GroupState::Stable => "Stable".to_string(),
            super::GroupState::Dead => "Dead".to_string(),
        });

        response.groups.push(listed_group);
    }

    Ok(response)
}
