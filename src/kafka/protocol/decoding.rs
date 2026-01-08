// Request decoding module
//
// Handles parsing of Kafka requests from binary wire protocol format.
//
// ## Thread Safety
//
// This module runs in the network thread and MUST NOT use pgrx logging.
// All logging uses the `tracing` crate.

use bytes::BytesMut;
use kafka_protocol::messages::fetch_request::FetchRequest;
use kafka_protocol::messages::offset_commit_request::OffsetCommitRequest;
use kafka_protocol::messages::offset_fetch_request::OffsetFetchRequest;
use kafka_protocol::messages::produce_request::ProduceRequest;
use kafka_protocol::protocol::{decode_request_header_from_buffer, Decodable};
use tracing::{debug, warn};

use super::super::constants::*;
use super::super::error::{KafkaError, Result};
use super::super::messages::KafkaRequest;
use super::recordbatch::parse_record_batch;

/// Parse a Kafka request from a frame
///
/// The frame has already been extracted by LengthDelimitedCodec, so we only need to parse:
/// [RequestHeader] [RequestBody]
///
/// RequestHeader contains:
/// - api_key: i16 (which API this is, e.g., 18 = ApiVersions)
/// - api_version: i16 (which version of the API)
/// - correlation_id: i32 (client-assigned ID for matching responses)
/// - client_id: nullable string (client identifier)
///
/// Returns None if there's a parse error with error response already sent
pub fn parse_request(
    frame: BytesMut,
    response_tx: tokio::sync::mpsc::UnboundedSender<super::super::messages::KafkaResponse>,
) -> Result<Option<KafkaRequest>> {
    debug!("Parsing request from {} byte frame", frame.len());

    // Parse RequestHeader using kafka-protocol's decode function
    // This automatically handles both non-flexible and flexible header formats
    let mut payload_buf = frame;

    let header = match decode_request_header_from_buffer(&mut payload_buf) {
        Ok(h) => h,
        Err(e) => {
            warn!("Failed to decode RequestHeader: {}", e);
            return Err(KafkaError::ProtocolCodec(e));
        }
    };

    let api_key = header.request_api_key;
    let api_version = header.request_api_version;
    let correlation_id = header.correlation_id;
    let client_id = header.client_id.map(|s| s.to_string());

    debug!(
        "Parsed RequestHeader: api_key={}, api_version={}, correlation_id={}, client_id={:?}",
        api_key, api_version, correlation_id, client_id
    );

    // Match on api_key to determine request type
    match api_key {
        API_KEY_API_VERSIONS => {
            parse_api_versions(correlation_id, client_id, api_version, response_tx)
        }
        API_KEY_METADATA => parse_metadata(
            &mut payload_buf,
            correlation_id,
            client_id,
            api_version,
            response_tx,
        ),
        API_KEY_PRODUCE => parse_produce(
            &mut payload_buf,
            correlation_id,
            client_id,
            api_version,
            response_tx,
        ),
        API_KEY_FETCH => parse_fetch(
            &mut payload_buf,
            correlation_id,
            client_id,
            api_version,
            response_tx,
        ),
        API_KEY_OFFSET_COMMIT => parse_offset_commit(
            &mut payload_buf,
            correlation_id,
            client_id,
            api_version,
            response_tx,
        ),
        API_KEY_OFFSET_FETCH => parse_offset_fetch(
            &mut payload_buf,
            correlation_id,
            client_id,
            api_version,
            response_tx,
        ),
        API_KEY_FIND_COORDINATOR => parse_find_coordinator(
            &mut payload_buf,
            correlation_id,
            client_id,
            api_version,
            response_tx,
        ),
        API_KEY_JOIN_GROUP => parse_join_group(
            &mut payload_buf,
            correlation_id,
            client_id,
            api_version,
            response_tx,
        ),
        API_KEY_SYNC_GROUP => parse_sync_group(
            &mut payload_buf,
            correlation_id,
            client_id,
            api_version,
            response_tx,
        ),
        API_KEY_HEARTBEAT => parse_heartbeat(
            &mut payload_buf,
            correlation_id,
            client_id,
            api_version,
            response_tx,
        ),
        API_KEY_LEAVE_GROUP => parse_leave_group(
            &mut payload_buf,
            correlation_id,
            client_id,
            api_version,
            response_tx,
        ),
        API_KEY_LIST_OFFSETS => parse_list_offsets(
            &mut payload_buf,
            correlation_id,
            client_id,
            api_version,
            response_tx,
        ),
        API_KEY_DESCRIBE_GROUPS => parse_describe_groups(
            &mut payload_buf,
            correlation_id,
            client_id,
            api_version,
            response_tx,
        ),
        API_KEY_LIST_GROUPS => parse_list_groups(
            &mut payload_buf,
            correlation_id,
            client_id,
            api_version,
            response_tx,
        ),
        API_KEY_CREATE_TOPICS => parse_create_topics(
            &mut payload_buf,
            correlation_id,
            client_id,
            api_version,
            response_tx,
        ),
        API_KEY_DELETE_TOPICS => parse_delete_topics(
            &mut payload_buf,
            correlation_id,
            client_id,
            api_version,
            response_tx,
        ),
        API_KEY_CREATE_PARTITIONS => parse_create_partitions(
            &mut payload_buf,
            correlation_id,
            client_id,
            api_version,
            response_tx,
        ),
        API_KEY_DELETE_GROUPS => parse_delete_groups(
            &mut payload_buf,
            correlation_id,
            client_id,
            api_version,
            response_tx,
        ),
        _ => {
            // Unsupported API
            warn!("Unsupported API key: {}", api_key);

            // Send error response immediately
            let error_response = super::super::messages::KafkaResponse::Error {
                correlation_id,
                error_code: ERROR_UNSUPPORTED_VERSION,
                error_message: Some(format!("Unsupported API key: {}", api_key)),
            };
            let _ = response_tx.send(error_response);

            Ok(None)
        }
    }
}

fn parse_api_versions(
    correlation_id: i32,
    client_id: Option<String>,
    api_version: i16,
    response_tx: tokio::sync::mpsc::UnboundedSender<super::super::messages::KafkaResponse>,
) -> Result<Option<KafkaRequest>> {
    // ApiVersions request - the body is empty
    debug!(
        "Parsed ApiVersions request (api_key={}, api_version={})",
        API_KEY_API_VERSIONS, api_version
    );
    Ok(Some(KafkaRequest::ApiVersions {
        correlation_id,
        client_id,
        api_version,
        response_tx,
    }))
}

fn parse_metadata(
    payload_buf: &mut BytesMut,
    correlation_id: i32,
    client_id: Option<String>,
    api_version: i16,
    response_tx: tokio::sync::mpsc::UnboundedSender<super::super::messages::KafkaResponse>,
) -> Result<Option<KafkaRequest>> {
    debug!(
        "Parsed Metadata request (api_key={}, version={})",
        API_KEY_METADATA, api_version
    );

    // Use kafka-protocol crate to decode MetadataRequest
    let metadata_req = match kafka_protocol::messages::metadata_request::MetadataRequest::decode(
        payload_buf,
        api_version,
    ) {
        Ok(req) => req,
        Err(e) => {
            warn!("Failed to decode MetadataRequest: {}", e);
            let error_response = super::super::messages::KafkaResponse::Error {
                correlation_id,
                error_code: ERROR_CORRUPT_MESSAGE,
                error_message: Some(format!("Malformed MetadataRequest: {}", e)),
            };
            let _ = response_tx.send(error_response);
            return Ok(None);
        }
    };

    // Extract requested topics (None means "all topics")
    let topics = match metadata_req.topics {
        Some(ref t) if !t.is_empty() => {
            let topic_names: Vec<String> = t
                .iter()
                .filter_map(|t| t.name.as_ref().map(|n| n.to_string()))
                .collect();
            debug!("Metadata request for specific topics: {:?}", topic_names);
            if topic_names.is_empty() {
                None
            } else {
                Some(topic_names)
            }
        }
        _ => {
            debug!("Metadata request for ALL topics");
            None
        }
    };

    Ok(Some(KafkaRequest::Metadata {
        correlation_id,
        client_id,
        api_version,
        topics,
        response_tx,
    }))
}

fn parse_produce(
    payload_buf: &mut BytesMut,
    correlation_id: i32,
    client_id: Option<String>,
    api_version: i16,
    response_tx: tokio::sync::mpsc::UnboundedSender<super::super::messages::KafkaResponse>,
) -> Result<Option<KafkaRequest>> {
    debug!(
        "Parsed Produce request (api_key={}, version={})",
        API_KEY_PRODUCE, api_version
    );

    // Use kafka-protocol crate to decode ProduceRequest
    let produce_req = match ProduceRequest::decode(payload_buf, api_version) {
        Ok(req) => req,
        Err(e) => {
            warn!("Failed to decode ProduceRequest: {}", e);
            let error_response = super::super::messages::KafkaResponse::Error {
                correlation_id,
                error_code: ERROR_CORRUPT_MESSAGE,
                error_message: Some(format!("Malformed ProduceRequest: {}", e)),
            };
            let _ = response_tx.send(error_response);
            return Ok(None);
        }
    };

    let acks = produce_req.acks;
    let timeout_ms = produce_req.timeout_ms;

    debug!(
        "ProduceRequest: acks={}, timeout_ms={}, topics={}",
        acks,
        timeout_ms,
        produce_req.topic_data.len()
    );

    // Extract topic data from kafka-protocol types to our types
    let mut topic_data = Vec::new();
    for topic in produce_req.topic_data {
        let topic_name = topic.name.to_string();
        let mut partitions = Vec::new();

        for partition in topic.partition_data {
            let partition_index = partition.index;

            // Parse RecordBatch from partition.records
            let records = match &partition.records {
                Some(batch_bytes) => match parse_record_batch(batch_bytes) {
                    Ok(records) => records,
                    Err(e) => {
                        warn!(
                            "Failed to parse RecordBatch for topic={}, partition={}: {}",
                            topic_name, partition_index, e
                        );
                        let error_response = super::super::messages::KafkaResponse::Error {
                            correlation_id,
                            error_code: ERROR_CORRUPT_MESSAGE,
                            error_message: Some(format!("Invalid RecordBatch: {}", e)),
                        };
                        let _ = response_tx.send(error_response);
                        return Ok(None);
                    }
                },
                None => Vec::new(),
            };

            debug!(
                "Parsed {} records for topic={}, partition={}",
                records.len(),
                topic_name,
                partition_index
            );

            partitions.push(super::super::messages::PartitionProduceData {
                partition_index,
                records,
            });
        }

        topic_data.push(super::super::messages::TopicProduceData {
            name: topic_name,
            partitions,
        });
    }

    Ok(Some(KafkaRequest::Produce {
        correlation_id,
        client_id,
        api_version,
        acks,
        timeout_ms,
        topic_data,
        response_tx,
    }))
}

fn parse_fetch(
    payload_buf: &mut BytesMut,
    correlation_id: i32,
    client_id: Option<String>,
    api_version: i16,
    response_tx: tokio::sync::mpsc::UnboundedSender<super::super::messages::KafkaResponse>,
) -> Result<Option<KafkaRequest>> {
    debug!(
        "Parsed Fetch request (api_key={}, version={})",
        API_KEY_FETCH, api_version
    );

    let fetch_req = match FetchRequest::decode(payload_buf, api_version) {
        Ok(req) => req,
        Err(e) => {
            warn!("Failed to decode FetchRequest: {}", e);
            let error_response = super::super::messages::KafkaResponse::Error {
                correlation_id,
                error_code: ERROR_CORRUPT_MESSAGE,
                error_message: Some(format!("Malformed FetchRequest: {}", e)),
            };
            let _ = response_tx.send(error_response);
            return Ok(None);
        }
    };

    let max_wait_ms = fetch_req.max_wait_ms;
    let min_bytes = fetch_req.min_bytes;
    let max_bytes = fetch_req.max_bytes;

    debug!(
        "FetchRequest: max_wait_ms={}, min_bytes={}, max_bytes={}, topics={}",
        max_wait_ms,
        min_bytes,
        max_bytes,
        fetch_req.topics.len()
    );

    let mut topic_data = Vec::new();
    for topic in fetch_req.topics {
        let topic_name = topic.topic.to_string();
        let mut partitions = Vec::new();

        for partition in topic.partitions {
            partitions.push(super::super::messages::PartitionFetchData {
                partition_index: partition.partition,
                fetch_offset: partition.fetch_offset,
                partition_max_bytes: partition.partition_max_bytes,
            });
        }

        debug!(
            "Fetch from topic={}, {} partitions",
            topic_name,
            partitions.len()
        );

        topic_data.push(super::super::messages::TopicFetchData {
            name: topic_name,
            partitions,
        });
    }

    Ok(Some(KafkaRequest::Fetch {
        correlation_id,
        client_id,
        api_version,
        max_wait_ms,
        min_bytes,
        max_bytes,
        topic_data,
        response_tx,
    }))
}

fn parse_offset_commit(
    payload_buf: &mut BytesMut,
    correlation_id: i32,
    client_id: Option<String>,
    api_version: i16,
    response_tx: tokio::sync::mpsc::UnboundedSender<super::super::messages::KafkaResponse>,
) -> Result<Option<KafkaRequest>> {
    debug!(
        "Parsed OffsetCommit request (api_key={}, version={})",
        API_KEY_OFFSET_COMMIT, api_version
    );

    let commit_req = match OffsetCommitRequest::decode(payload_buf, api_version) {
        Ok(req) => req,
        Err(e) => {
            warn!("Failed to decode OffsetCommitRequest: {}", e);
            let error_response = super::super::messages::KafkaResponse::Error {
                correlation_id,
                error_code: ERROR_CORRUPT_MESSAGE,
                error_message: Some(format!("Malformed OffsetCommitRequest: {}", e)),
            };
            let _ = response_tx.send(error_response);
            return Ok(None);
        }
    };

    let group_id = commit_req.group_id.to_string();

    debug!(
        "OffsetCommit for group_id={}, {} topics",
        group_id,
        commit_req.topics.len()
    );

    let mut topics = Vec::new();
    for topic in commit_req.topics {
        let topic_name = topic.name.to_string();
        let mut partitions = Vec::new();

        for partition in topic.partitions {
            partitions.push(super::super::messages::OffsetCommitPartitionData {
                partition_index: partition.partition_index,
                committed_offset: partition.committed_offset,
                metadata: partition
                    .committed_metadata
                    .map(|s| s.to_string())
                    .filter(|s| !s.is_empty()),
            });
        }

        topics.push(super::super::messages::OffsetCommitTopicData {
            name: topic_name,
            partitions,
        });
    }

    Ok(Some(KafkaRequest::OffsetCommit {
        correlation_id,
        client_id,
        api_version,
        group_id,
        topics,
        response_tx,
    }))
}

fn parse_offset_fetch(
    payload_buf: &mut BytesMut,
    correlation_id: i32,
    client_id: Option<String>,
    api_version: i16,
    response_tx: tokio::sync::mpsc::UnboundedSender<super::super::messages::KafkaResponse>,
) -> Result<Option<KafkaRequest>> {
    debug!(
        "Parsed OffsetFetch request (api_key={}, version={})",
        API_KEY_OFFSET_FETCH, api_version
    );

    let fetch_req = match OffsetFetchRequest::decode(payload_buf, api_version) {
        Ok(req) => req,
        Err(e) => {
            warn!("Failed to decode OffsetFetchRequest: {}", e);
            let error_response = super::super::messages::KafkaResponse::Error {
                correlation_id,
                error_code: ERROR_CORRUPT_MESSAGE,
                error_message: Some(format!("Malformed OffsetFetchRequest: {}", e)),
            };
            let _ = response_tx.send(error_response);
            return Ok(None);
        }
    };

    let group_id = fetch_req.group_id.to_string();
    debug!("OffsetFetch for group_id={}", group_id);

    let topics = if let Some(topic_vec) = fetch_req.topics {
        if topic_vec.is_empty() {
            None
        } else {
            let mut topic_list = Vec::new();
            for topic in topic_vec {
                let topic_name = topic.name.to_string();
                let partition_indexes = topic.partition_indexes;

                topic_list.push(super::super::messages::OffsetFetchTopicData {
                    name: topic_name,
                    partition_indexes,
                });
            }
            Some(topic_list)
        }
    } else {
        None
    };

    Ok(Some(KafkaRequest::OffsetFetch {
        correlation_id,
        client_id,
        api_version,
        group_id,
        topics,
        response_tx,
    }))
}

fn parse_find_coordinator(
    payload_buf: &mut BytesMut,
    correlation_id: i32,
    client_id: Option<String>,
    api_version: i16,
    response_tx: tokio::sync::mpsc::UnboundedSender<super::super::messages::KafkaResponse>,
) -> Result<Option<KafkaRequest>> {
    debug!(
        "Parsed FindCoordinator request (api_key={}, version={})",
        API_KEY_FIND_COORDINATOR, api_version
    );

    let coord_req =
        match kafka_protocol::messages::find_coordinator_request::FindCoordinatorRequest::decode(
            payload_buf,
            api_version,
        ) {
            Ok(req) => req,
            Err(e) => {
                warn!("Failed to decode FindCoordinatorRequest: {}", e);
                let error_response = super::super::messages::KafkaResponse::Error {
                    correlation_id,
                    error_code: ERROR_CORRUPT_MESSAGE,
                    error_message: Some(format!("Malformed FindCoordinatorRequest: {}", e)),
                };
                let _ = response_tx.send(error_response);
                return Ok(None);
            }
        };

    let key = coord_req.key.to_string();
    let key_type = coord_req.key_type;

    Ok(Some(KafkaRequest::FindCoordinator {
        correlation_id,
        client_id,
        api_version,
        key,
        key_type,
        response_tx,
    }))
}

fn parse_join_group(
    payload_buf: &mut BytesMut,
    correlation_id: i32,
    client_id: Option<String>,
    api_version: i16,
    response_tx: tokio::sync::mpsc::UnboundedSender<super::super::messages::KafkaResponse>,
) -> Result<Option<KafkaRequest>> {
    debug!(
        "Parsed JoinGroup request (api_key={}, version={})",
        API_KEY_JOIN_GROUP, api_version
    );

    let join_req = match kafka_protocol::messages::join_group_request::JoinGroupRequest::decode(
        payload_buf,
        api_version,
    ) {
        Ok(req) => req,
        Err(e) => {
            warn!("Failed to decode JoinGroupRequest: {}", e);
            let error_response = super::super::messages::KafkaResponse::Error {
                correlation_id,
                error_code: ERROR_CORRUPT_MESSAGE,
                error_message: Some(format!("Malformed JoinGroupRequest: {}", e)),
            };
            let _ = response_tx.send(error_response);
            return Ok(None);
        }
    };

    let group_id = join_req.group_id.to_string();
    let session_timeout_ms = join_req.session_timeout_ms;
    let rebalance_timeout_ms = join_req.rebalance_timeout_ms;
    let member_id = join_req.member_id.to_string();
    let group_instance_id = join_req.group_instance_id.map(|s| s.to_string());
    let protocol_type = join_req.protocol_type.to_string();

    let protocols = join_req
        .protocols
        .into_iter()
        .map(super::super::messages::JoinGroupProtocol::from)
        .collect();

    Ok(Some(KafkaRequest::JoinGroup {
        correlation_id,
        client_id,
        api_version,
        group_id,
        session_timeout_ms,
        rebalance_timeout_ms,
        member_id,
        group_instance_id,
        protocol_type,
        protocols,
        response_tx,
    }))
}

fn parse_sync_group(
    payload_buf: &mut BytesMut,
    correlation_id: i32,
    client_id: Option<String>,
    api_version: i16,
    response_tx: tokio::sync::mpsc::UnboundedSender<super::super::messages::KafkaResponse>,
) -> Result<Option<KafkaRequest>> {
    debug!(
        "Parsed SyncGroup request (api_key={}, version={})",
        API_KEY_SYNC_GROUP, api_version
    );

    let sync_req = match kafka_protocol::messages::sync_group_request::SyncGroupRequest::decode(
        payload_buf,
        api_version,
    ) {
        Ok(req) => req,
        Err(e) => {
            warn!("Failed to decode SyncGroupRequest: {}", e);
            let error_response = super::super::messages::KafkaResponse::Error {
                correlation_id,
                error_code: ERROR_CORRUPT_MESSAGE,
                error_message: Some(format!("Malformed SyncGroupRequest: {}", e)),
            };
            let _ = response_tx.send(error_response);
            return Ok(None);
        }
    };

    let group_id = sync_req.group_id.to_string();
    let generation_id = sync_req.generation_id;
    let member_id = sync_req.member_id.to_string();
    let group_instance_id = sync_req.group_instance_id.map(|s| s.to_string());
    let protocol_type = sync_req.protocol_type.map(|s| s.to_string());
    let protocol_name = sync_req.protocol_name.map(|s| s.to_string());

    let assignments = sync_req
        .assignments
        .into_iter()
        .map(super::super::messages::SyncGroupAssignment::from)
        .collect();

    Ok(Some(KafkaRequest::SyncGroup {
        correlation_id,
        client_id,
        api_version,
        group_id,
        generation_id,
        member_id,
        group_instance_id,
        protocol_type,
        protocol_name,
        assignments,
        response_tx,
    }))
}

fn parse_heartbeat(
    payload_buf: &mut BytesMut,
    correlation_id: i32,
    client_id: Option<String>,
    api_version: i16,
    response_tx: tokio::sync::mpsc::UnboundedSender<super::super::messages::KafkaResponse>,
) -> Result<Option<KafkaRequest>> {
    debug!(
        "Parsed Heartbeat request (api_key={}, version={})",
        API_KEY_HEARTBEAT, api_version
    );

    let heartbeat_req = match kafka_protocol::messages::heartbeat_request::HeartbeatRequest::decode(
        payload_buf,
        api_version,
    ) {
        Ok(req) => req,
        Err(e) => {
            warn!("Failed to decode HeartbeatRequest: {}", e);
            let error_response = super::super::messages::KafkaResponse::Error {
                correlation_id,
                error_code: ERROR_CORRUPT_MESSAGE,
                error_message: Some(format!("Malformed HeartbeatRequest: {}", e)),
            };
            let _ = response_tx.send(error_response);
            return Ok(None);
        }
    };

    let group_id = heartbeat_req.group_id.to_string();
    let generation_id = heartbeat_req.generation_id;
    let member_id = heartbeat_req.member_id.to_string();
    let group_instance_id = heartbeat_req.group_instance_id.map(|s| s.to_string());

    Ok(Some(KafkaRequest::Heartbeat {
        correlation_id,
        client_id,
        api_version,
        group_id,
        generation_id,
        member_id,
        group_instance_id,
        response_tx,
    }))
}

fn parse_leave_group(
    payload_buf: &mut BytesMut,
    correlation_id: i32,
    client_id: Option<String>,
    api_version: i16,
    response_tx: tokio::sync::mpsc::UnboundedSender<super::super::messages::KafkaResponse>,
) -> Result<Option<KafkaRequest>> {
    debug!(
        "Parsed LeaveGroup request (api_key={}, version={})",
        API_KEY_LEAVE_GROUP, api_version
    );

    let leave_req = match kafka_protocol::messages::leave_group_request::LeaveGroupRequest::decode(
        payload_buf,
        api_version,
    ) {
        Ok(req) => req,
        Err(e) => {
            warn!("Failed to decode LeaveGroupRequest: {}", e);
            let error_response = super::super::messages::KafkaResponse::Error {
                correlation_id,
                error_code: ERROR_CORRUPT_MESSAGE,
                error_message: Some(format!("Malformed LeaveGroupRequest: {}", e)),
            };
            let _ = response_tx.send(error_response);
            return Ok(None);
        }
    };

    let group_id = leave_req.group_id.to_string();
    let member_id = leave_req.member_id.to_string();

    let members = leave_req
        .members
        .into_iter()
        .map(super::super::messages::MemberIdentity::from)
        .collect();

    Ok(Some(KafkaRequest::LeaveGroup {
        correlation_id,
        client_id,
        api_version,
        group_id,
        member_id,
        members,
        response_tx,
    }))
}

fn parse_list_offsets(
    payload_buf: &mut BytesMut,
    correlation_id: i32,
    client_id: Option<String>,
    api_version: i16,
    response_tx: tokio::sync::mpsc::UnboundedSender<super::super::messages::KafkaResponse>,
) -> Result<Option<KafkaRequest>> {
    debug!(
        "Parsed ListOffsets request (api_key={}, version={})",
        API_KEY_LIST_OFFSETS, api_version
    );

    let list_req = match kafka_protocol::messages::list_offsets_request::ListOffsetsRequest::decode(
        payload_buf,
        api_version,
    ) {
        Ok(req) => req,
        Err(e) => {
            warn!("Failed to decode ListOffsetsRequest: {}", e);
            let error_response = super::super::messages::KafkaResponse::Error {
                correlation_id,
                error_code: ERROR_CORRUPT_MESSAGE,
                error_message: Some(format!("Malformed ListOffsetsRequest: {}", e)),
            };
            let _ = response_tx.send(error_response);
            return Ok(None);
        }
    };

    let replica_id = list_req.replica_id.0; // Extract i32 from BrokerId
    let isolation_level = list_req.isolation_level;

    let topics: Vec<super::super::messages::ListOffsetsTopicData> = list_req
        .topics
        .into_iter()
        .map(|topic| {
            let partitions = topic
                .partitions
                .into_iter()
                .map(super::super::messages::ListOffsetsPartitionData::from)
                .collect();

            super::super::messages::ListOffsetsTopicData {
                name: topic.name.to_string(),
                partitions,
            }
        })
        .collect();

    Ok(Some(KafkaRequest::ListOffsets {
        correlation_id,
        client_id,
        api_version,
        replica_id,
        isolation_level,
        topics,
        response_tx,
    }))
}

fn parse_describe_groups(
    payload_buf: &mut BytesMut,
    correlation_id: i32,
    client_id: Option<String>,
    api_version: i16,
    response_tx: tokio::sync::mpsc::UnboundedSender<super::super::messages::KafkaResponse>,
) -> Result<Option<KafkaRequest>> {
    debug!(
        "Parsed DescribeGroups request (api_key={}, version={})",
        API_KEY_DESCRIBE_GROUPS, api_version
    );

    let describe_req =
        match kafka_protocol::messages::describe_groups_request::DescribeGroupsRequest::decode(
            payload_buf,
            api_version,
        ) {
            Ok(req) => req,
            Err(e) => {
                warn!("Failed to decode DescribeGroupsRequest: {}", e);
                let error_response = super::super::messages::KafkaResponse::Error {
                    correlation_id,
                    error_code: ERROR_CORRUPT_MESSAGE,
                    error_message: Some(format!("Malformed DescribeGroupsRequest: {}", e)),
                };
                let _ = response_tx.send(error_response);
                return Ok(None);
            }
        };

    let groups: Vec<String> = describe_req
        .groups
        .into_iter()
        .map(|g| g.to_string())
        .collect();

    let include_authorized_operations = describe_req.include_authorized_operations;

    Ok(Some(KafkaRequest::DescribeGroups {
        correlation_id,
        client_id,
        api_version,
        groups,
        include_authorized_operations,
        response_tx,
    }))
}

fn parse_list_groups(
    payload_buf: &mut BytesMut,
    correlation_id: i32,
    client_id: Option<String>,
    api_version: i16,
    response_tx: tokio::sync::mpsc::UnboundedSender<super::super::messages::KafkaResponse>,
) -> Result<Option<KafkaRequest>> {
    debug!(
        "Parsed ListGroups request (api_key={}, version={})",
        API_KEY_LIST_GROUPS, api_version
    );

    let list_req = match kafka_protocol::messages::list_groups_request::ListGroupsRequest::decode(
        payload_buf,
        api_version,
    ) {
        Ok(req) => req,
        Err(e) => {
            warn!("Failed to decode ListGroupsRequest: {}", e);
            let error_response = super::super::messages::KafkaResponse::Error {
                correlation_id,
                error_code: ERROR_CORRUPT_MESSAGE,
                error_message: Some(format!("Malformed ListGroupsRequest: {}", e)),
            };
            let _ = response_tx.send(error_response);
            return Ok(None);
        }
    };

    let states_filter: Vec<String> = list_req
        .states_filter
        .into_iter()
        .map(|s| s.to_string())
        .collect();

    Ok(Some(KafkaRequest::ListGroups {
        correlation_id,
        client_id,
        api_version,
        states_filter,
        response_tx,
    }))
}

// ========== Admin API Parsers (Phase 6) ==========

fn parse_create_topics(
    payload_buf: &mut BytesMut,
    correlation_id: i32,
    client_id: Option<String>,
    api_version: i16,
    response_tx: tokio::sync::mpsc::UnboundedSender<super::super::messages::KafkaResponse>,
) -> Result<Option<KafkaRequest>> {
    debug!(
        "Parsed CreateTopics request (api_key={}, version={})",
        API_KEY_CREATE_TOPICS, api_version
    );

    let create_req =
        match kafka_protocol::messages::create_topics_request::CreateTopicsRequest::decode(
            payload_buf,
            api_version,
        ) {
            Ok(req) => req,
            Err(e) => {
                warn!("Failed to decode CreateTopicsRequest: {}", e);
                let error_response = super::super::messages::KafkaResponse::Error {
                    correlation_id,
                    error_code: ERROR_CORRUPT_MESSAGE,
                    error_message: Some(format!("Malformed CreateTopicsRequest: {}", e)),
                };
                let _ = response_tx.send(error_response);
                return Ok(None);
            }
        };

    let topics: Vec<super::super::messages::CreateTopicRequest> = create_req
        .topics
        .into_iter()
        .map(|t| super::super::messages::CreateTopicRequest {
            name: t.name.to_string(),
            num_partitions: t.num_partitions,
            replication_factor: t.replication_factor,
        })
        .collect();

    let timeout_ms = create_req.timeout_ms;
    let validate_only = create_req.validate_only;

    Ok(Some(KafkaRequest::CreateTopics {
        correlation_id,
        client_id,
        api_version,
        topics,
        timeout_ms,
        validate_only,
        response_tx,
    }))
}

fn parse_delete_topics(
    payload_buf: &mut BytesMut,
    correlation_id: i32,
    client_id: Option<String>,
    api_version: i16,
    response_tx: tokio::sync::mpsc::UnboundedSender<super::super::messages::KafkaResponse>,
) -> Result<Option<KafkaRequest>> {
    debug!(
        "Parsed DeleteTopics request (api_key={}, version={})",
        API_KEY_DELETE_TOPICS, api_version
    );

    let delete_req =
        match kafka_protocol::messages::delete_topics_request::DeleteTopicsRequest::decode(
            payload_buf,
            api_version,
        ) {
            Ok(req) => req,
            Err(e) => {
                warn!("Failed to decode DeleteTopicsRequest: {}", e);
                let error_response = super::super::messages::KafkaResponse::Error {
                    correlation_id,
                    error_code: ERROR_CORRUPT_MESSAGE,
                    error_message: Some(format!("Malformed DeleteTopicsRequest: {}", e)),
                };
                let _ = response_tx.send(error_response);
                return Ok(None);
            }
        };

    // Extract topic names from the request
    // Note: In newer versions, topics field contains TopicName, in older versions it's topic_names
    let topic_names: Vec<String> = delete_req
        .topic_names
        .into_iter()
        .map(|t| t.to_string())
        .collect();

    let timeout_ms = delete_req.timeout_ms;

    Ok(Some(KafkaRequest::DeleteTopics {
        correlation_id,
        client_id,
        api_version,
        topic_names,
        timeout_ms,
        response_tx,
    }))
}

fn parse_create_partitions(
    payload_buf: &mut BytesMut,
    correlation_id: i32,
    client_id: Option<String>,
    api_version: i16,
    response_tx: tokio::sync::mpsc::UnboundedSender<super::super::messages::KafkaResponse>,
) -> Result<Option<KafkaRequest>> {
    debug!(
        "Parsed CreatePartitions request (api_key={}, version={})",
        API_KEY_CREATE_PARTITIONS, api_version
    );

    let create_req =
        match kafka_protocol::messages::create_partitions_request::CreatePartitionsRequest::decode(
            payload_buf,
            api_version,
        ) {
            Ok(req) => req,
            Err(e) => {
                warn!("Failed to decode CreatePartitionsRequest: {}", e);
                let error_response = super::super::messages::KafkaResponse::Error {
                    correlation_id,
                    error_code: ERROR_CORRUPT_MESSAGE,
                    error_message: Some(format!("Malformed CreatePartitionsRequest: {}", e)),
                };
                let _ = response_tx.send(error_response);
                return Ok(None);
            }
        };

    let topics: Vec<super::super::messages::CreatePartitionsTopicRequest> = create_req
        .topics
        .into_iter()
        .map(|t| super::super::messages::CreatePartitionsTopicRequest {
            name: t.name.to_string(),
            count: t.count,
        })
        .collect();

    let timeout_ms = create_req.timeout_ms;
    let validate_only = create_req.validate_only;

    Ok(Some(KafkaRequest::CreatePartitions {
        correlation_id,
        client_id,
        api_version,
        topics,
        timeout_ms,
        validate_only,
        response_tx,
    }))
}

fn parse_delete_groups(
    payload_buf: &mut BytesMut,
    correlation_id: i32,
    client_id: Option<String>,
    api_version: i16,
    response_tx: tokio::sync::mpsc::UnboundedSender<super::super::messages::KafkaResponse>,
) -> Result<Option<KafkaRequest>> {
    debug!(
        "Parsed DeleteGroups request (api_key={}, version={})",
        API_KEY_DELETE_GROUPS, api_version
    );

    let delete_req =
        match kafka_protocol::messages::delete_groups_request::DeleteGroupsRequest::decode(
            payload_buf,
            api_version,
        ) {
            Ok(req) => req,
            Err(e) => {
                warn!("Failed to decode DeleteGroupsRequest: {}", e);
                let error_response = super::super::messages::KafkaResponse::Error {
                    correlation_id,
                    error_code: ERROR_CORRUPT_MESSAGE,
                    error_message: Some(format!("Malformed DeleteGroupsRequest: {}", e)),
                };
                let _ = response_tx.send(error_response);
                return Ok(None);
            }
        };

    let groups_names: Vec<String> = delete_req
        .groups_names
        .into_iter()
        .map(|g| g.to_string())
        .collect();

    Ok(Some(KafkaRequest::DeleteGroups {
        correlation_id,
        client_id,
        api_version,
        groups_names,
        response_tx,
    }))
}
