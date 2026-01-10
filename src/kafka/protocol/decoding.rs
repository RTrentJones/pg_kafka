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
use super::super::messages::{KafkaRequest, TxnOffsetCommitTopics};
use super::recordbatch::{parse_record_batch_with_metadata, ParsedRecordBatch};

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
        API_KEY_INIT_PRODUCER_ID => parse_init_producer_id(
            &mut payload_buf,
            correlation_id,
            client_id,
            api_version,
            response_tx,
        ),
        API_KEY_ADD_PARTITIONS_TO_TXN => parse_add_partitions_to_txn(
            &mut payload_buf,
            correlation_id,
            client_id,
            api_version,
            response_tx,
        ),
        API_KEY_ADD_OFFSETS_TO_TXN => parse_add_offsets_to_txn(
            &mut payload_buf,
            correlation_id,
            client_id,
            api_version,
            response_tx,
        ),
        API_KEY_END_TXN => parse_end_txn(
            &mut payload_buf,
            correlation_id,
            client_id,
            api_version,
            response_tx,
        ),
        API_KEY_TXN_OFFSET_COMMIT => parse_txn_offset_commit(
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
    // Phase 10: Extract transactional_id for transactional producers (v3+)
    let transactional_id = produce_req.transactional_id.map(|s| s.to_string());

    debug!(
        "ProduceRequest: acks={}, timeout_ms={}, transactional_id={:?}, topics={}",
        acks,
        timeout_ms,
        transactional_id,
        produce_req.topic_data.len()
    );

    // Extract topic data from kafka-protocol types to our types
    let mut topic_data = Vec::new();
    for topic in produce_req.topic_data {
        let topic_name = topic.name.to_string();
        let mut partitions = Vec::new();

        for partition in topic.partition_data {
            let partition_index = partition.index;

            // Parse RecordBatch from partition.records (Phase 9: extract producer metadata)
            let (records, producer_metadata) = match &partition.records {
                Some(batch_bytes) => match parse_record_batch_with_metadata(batch_bytes) {
                    Ok(ParsedRecordBatch {
                        records,
                        producer_metadata,
                    }) => (records, Some(producer_metadata)),
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
                None => (Vec::new(), None),
            };

            debug!(
                "Parsed {} records for topic={}, partition={} (producer_id={:?})",
                records.len(),
                topic_name,
                partition_index,
                producer_metadata.as_ref().map(|m| m.producer_id)
            );

            partitions.push(super::super::messages::PartitionProduceData {
                partition_index,
                records,
                producer_metadata,
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
        transactional_id,
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
    // Phase 10: Extract isolation_level (v4+, defaults to 0 for older versions)
    let isolation_level = fetch_req.isolation_level;

    debug!(
        "FetchRequest: max_wait_ms={}, min_bytes={}, max_bytes={}, isolation_level={}, topics={}",
        max_wait_ms,
        min_bytes,
        max_bytes,
        isolation_level,
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
        isolation_level,
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

// ========== Idempotent Producer API Parsers (Phase 9) ==========

fn parse_init_producer_id(
    payload_buf: &mut BytesMut,
    correlation_id: i32,
    client_id: Option<String>,
    api_version: i16,
    response_tx: tokio::sync::mpsc::UnboundedSender<super::super::messages::KafkaResponse>,
) -> Result<Option<KafkaRequest>> {
    debug!(
        "Parsed InitProducerId request (api_key={}, version={})",
        API_KEY_INIT_PRODUCER_ID, api_version
    );

    let init_req =
        match kafka_protocol::messages::init_producer_id_request::InitProducerIdRequest::decode(
            payload_buf,
            api_version,
        ) {
            Ok(req) => req,
            Err(e) => {
                warn!("Failed to decode InitProducerIdRequest: {}", e);
                let error_response = super::super::messages::KafkaResponse::Error {
                    correlation_id,
                    error_code: ERROR_CORRUPT_MESSAGE,
                    error_message: Some(format!("Malformed InitProducerIdRequest: {}", e)),
                };
                let _ = response_tx.send(error_response);
                return Ok(None);
            }
        };

    let transactional_id = init_req.transactional_id.map(|s| s.to_string());
    let transaction_timeout_ms = init_req.transaction_timeout_ms;
    // Extract the inner i64 from ProducerId wrapper type
    let producer_id = init_req.producer_id.0;
    let producer_epoch = init_req.producer_epoch;

    debug!(
        "InitProducerIdRequest: transactional_id={:?}, producer_id={}, producer_epoch={}",
        transactional_id, producer_id, producer_epoch
    );

    Ok(Some(KafkaRequest::InitProducerId {
        correlation_id,
        client_id,
        api_version,
        transactional_id,
        transaction_timeout_ms,
        producer_id,
        producer_epoch,
        response_tx,
    }))
}

// ===== Phase 10: Transaction API Parsing =====

fn parse_add_partitions_to_txn(
    payload_buf: &mut BytesMut,
    correlation_id: i32,
    client_id: Option<String>,
    api_version: i16,
    response_tx: tokio::sync::mpsc::UnboundedSender<super::super::messages::KafkaResponse>,
) -> Result<Option<KafkaRequest>> {
    debug!(
        "Parsing AddPartitionsToTxn request (api_key={}, version={})",
        API_KEY_ADD_PARTITIONS_TO_TXN, api_version
    );

    let req = match kafka_protocol::messages::add_partitions_to_txn_request::AddPartitionsToTxnRequest::decode(
        payload_buf,
        api_version,
    ) {
        Ok(req) => req,
        Err(e) => {
            warn!("Failed to decode AddPartitionsToTxnRequest: {}", e);
            let error_response = super::super::messages::KafkaResponse::Error {
                correlation_id,
                error_code: ERROR_CORRUPT_MESSAGE,
                error_message: Some(format!("Malformed AddPartitionsToTxnRequest: {}", e)),
            };
            let _ = response_tx.send(error_response);
            return Ok(None);
        }
    };

    // Handle both v3 and below (single transaction) and v4+ (batch transactions) formats
    // v4+ introduced batching multiple transactions in one request, but we extract the first one
    let (transactional_id, producer_id, producer_epoch, topics) = if api_version >= 4 {
        // v4+: Use transactions field (array of batched transactions)
        // For now, we only support single transaction per request (extract first)
        if let Some(first_txn) = req.transactions.first() {
            let txn_id = first_txn.transactional_id.to_string();
            let pid = first_txn.producer_id.0;
            let epoch = first_txn.producer_epoch;
            let topics_data: Vec<(String, Vec<i32>)> = first_txn
                .topics
                .iter()
                .map(|t| {
                    let topic_name = t.name.to_string();
                    let partition_ids: Vec<i32> = t.partitions.to_vec();
                    (topic_name, partition_ids)
                })
                .collect();
            (txn_id, pid, epoch, topics_data)
        } else {
            warn!("AddPartitionsToTxn v4+ with empty transactions array");
            let error_response = super::super::messages::KafkaResponse::Error {
                correlation_id,
                error_code: ERROR_CORRUPT_MESSAGE,
                error_message: Some("Empty transactions array".to_string()),
            };
            let _ = response_tx.send(error_response);
            return Ok(None);
        }
    } else {
        // v0-v3: Use v3_and_below fields (single transaction)
        let txn_id = req.v3_and_below_transactional_id.to_string();
        let pid = req.v3_and_below_producer_id.0;
        let epoch = req.v3_and_below_producer_epoch;
        let topics_data: Vec<(String, Vec<i32>)> = req
            .v3_and_below_topics
            .iter()
            .map(|t| {
                let topic_name = t.name.to_string();
                let partition_ids: Vec<i32> = t.partitions.to_vec();
                (topic_name, partition_ids)
            })
            .collect();
        (txn_id, pid, epoch, topics_data)
    };

    debug!(
        "AddPartitionsToTxnRequest: transactional_id={}, producer_id={}, topics={:?}",
        transactional_id, producer_id, topics
    );

    Ok(Some(KafkaRequest::AddPartitionsToTxn {
        correlation_id,
        client_id,
        api_version,
        transactional_id,
        producer_id,
        producer_epoch,
        topics,
        response_tx,
    }))
}

fn parse_add_offsets_to_txn(
    payload_buf: &mut BytesMut,
    correlation_id: i32,
    client_id: Option<String>,
    api_version: i16,
    response_tx: tokio::sync::mpsc::UnboundedSender<super::super::messages::KafkaResponse>,
) -> Result<Option<KafkaRequest>> {
    debug!(
        "Parsing AddOffsetsToTxn request (api_key={}, version={})",
        API_KEY_ADD_OFFSETS_TO_TXN, api_version
    );

    let req =
        match kafka_protocol::messages::add_offsets_to_txn_request::AddOffsetsToTxnRequest::decode(
            payload_buf,
            api_version,
        ) {
            Ok(req) => req,
            Err(e) => {
                warn!("Failed to decode AddOffsetsToTxnRequest: {}", e);
                let error_response = super::super::messages::KafkaResponse::Error {
                    correlation_id,
                    error_code: ERROR_CORRUPT_MESSAGE,
                    error_message: Some(format!("Malformed AddOffsetsToTxnRequest: {}", e)),
                };
                let _ = response_tx.send(error_response);
                return Ok(None);
            }
        };

    let transactional_id = req.transactional_id.to_string();
    let producer_id = req.producer_id.0;
    let producer_epoch = req.producer_epoch;
    let group_id = req.group_id.to_string();

    debug!(
        "AddOffsetsToTxnRequest: transactional_id={}, producer_id={}, group_id={}",
        transactional_id, producer_id, group_id
    );

    Ok(Some(KafkaRequest::AddOffsetsToTxn {
        correlation_id,
        client_id,
        api_version,
        transactional_id,
        producer_id,
        producer_epoch,
        group_id,
        response_tx,
    }))
}

fn parse_end_txn(
    payload_buf: &mut BytesMut,
    correlation_id: i32,
    client_id: Option<String>,
    api_version: i16,
    response_tx: tokio::sync::mpsc::UnboundedSender<super::super::messages::KafkaResponse>,
) -> Result<Option<KafkaRequest>> {
    debug!(
        "Parsing EndTxn request (api_key={}, version={})",
        API_KEY_END_TXN, api_version
    );

    let req = match kafka_protocol::messages::end_txn_request::EndTxnRequest::decode(
        payload_buf,
        api_version,
    ) {
        Ok(req) => req,
        Err(e) => {
            warn!("Failed to decode EndTxnRequest: {}", e);
            let error_response = super::super::messages::KafkaResponse::Error {
                correlation_id,
                error_code: ERROR_CORRUPT_MESSAGE,
                error_message: Some(format!("Malformed EndTxnRequest: {}", e)),
            };
            let _ = response_tx.send(error_response);
            return Ok(None);
        }
    };

    let transactional_id = req.transactional_id.to_string();
    let producer_id = req.producer_id.0;
    let producer_epoch = req.producer_epoch;
    let committed = req.committed;

    debug!(
        "EndTxnRequest: transactional_id={}, producer_id={}, committed={}",
        transactional_id, producer_id, committed
    );

    Ok(Some(KafkaRequest::EndTxn {
        correlation_id,
        client_id,
        api_version,
        transactional_id,
        producer_id,
        producer_epoch,
        committed,
        response_tx,
    }))
}

fn parse_txn_offset_commit(
    payload_buf: &mut BytesMut,
    correlation_id: i32,
    client_id: Option<String>,
    api_version: i16,
    response_tx: tokio::sync::mpsc::UnboundedSender<super::super::messages::KafkaResponse>,
) -> Result<Option<KafkaRequest>> {
    debug!(
        "Parsing TxnOffsetCommit request (api_key={}, version={})",
        API_KEY_TXN_OFFSET_COMMIT, api_version
    );

    let req =
        match kafka_protocol::messages::txn_offset_commit_request::TxnOffsetCommitRequest::decode(
            payload_buf,
            api_version,
        ) {
            Ok(req) => req,
            Err(e) => {
                warn!("Failed to decode TxnOffsetCommitRequest: {}", e);
                let error_response = super::super::messages::KafkaResponse::Error {
                    correlation_id,
                    error_code: ERROR_CORRUPT_MESSAGE,
                    error_message: Some(format!("Malformed TxnOffsetCommitRequest: {}", e)),
                };
                let _ = response_tx.send(error_response);
                return Ok(None);
            }
        };

    let transactional_id = req.transactional_id.to_string();
    let group_id = req.group_id.to_string();
    let producer_id = req.producer_id.0;
    let producer_epoch = req.producer_epoch;

    // Convert topics to (topic_name, partitions) format
    // where partitions is Vec<(partition_id, offset, metadata)>
    let topics: TxnOffsetCommitTopics = req
        .topics
        .iter()
        .map(|t| {
            let topic_name = t.name.to_string();
            let partitions: Vec<(i32, i64, Option<String>)> = t
                .partitions
                .iter()
                .map(|p| {
                    let metadata = p.committed_metadata.as_ref().map(|s| s.to_string());
                    (p.partition_index, p.committed_offset, metadata)
                })
                .collect();
            (topic_name, partitions)
        })
        .collect();

    debug!(
        "TxnOffsetCommitRequest: transactional_id={}, group_id={}, producer_id={}, topics={:?}",
        transactional_id, group_id, producer_id, topics
    );

    Ok(Some(KafkaRequest::TxnOffsetCommit {
        correlation_id,
        client_id,
        api_version,
        transactional_id,
        group_id,
        producer_id,
        producer_epoch,
        topics,
        response_tx,
    }))
}

// ========== Unit Tests ==========

#[cfg(test)]
mod tests {
    use super::*;
    #[allow(unused_imports)]
    use bytes::BufMut;
    use kafka_protocol::messages::*;
    use kafka_protocol::protocol::{Encodable, StrBytes};

    /// Helper to build a complete request frame (header + body)
    /// Note: Does NOT include the 4-byte size prefix (that's handled by LengthDelimitedCodec)
    fn build_request_frame<R: Encodable>(
        api_key: i16,
        api_version: i16,
        correlation_id: i32,
        header_version: i16,
        request: &R,
        request_version: i16,
    ) -> BytesMut {
        let header = RequestHeader::default()
            .with_request_api_key(api_key)
            .with_request_api_version(api_version)
            .with_correlation_id(correlation_id)
            .with_client_id(Some(StrBytes::from_static_str("test-client")));

        let mut buf = BytesMut::new();
        header.encode(&mut buf, header_version).unwrap();
        request.encode(&mut buf, request_version).unwrap();
        buf
    }

    /// Create a channel for testing
    fn create_test_channel() -> (
        tokio::sync::mpsc::UnboundedSender<super::super::super::messages::KafkaResponse>,
        tokio::sync::mpsc::UnboundedReceiver<super::super::super::messages::KafkaResponse>,
    ) {
        tokio::sync::mpsc::unbounded_channel()
    }

    // ========== ApiVersions Request Tests ==========

    #[test]
    fn test_parse_api_versions_request() {
        let (tx, _rx) = create_test_channel();

        // ApiVersions request has empty body
        let request = api_versions_request::ApiVersionsRequest::default();
        let frame = build_request_frame(
            API_KEY_API_VERSIONS,
            3,
            12345,
            2, // Flexible header
            &request,
            3,
        );

        let result = parse_request(frame, tx);
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert!(parsed.is_some());

        if let Some(KafkaRequest::ApiVersions {
            correlation_id,
            api_version,
            ..
        }) = parsed
        {
            assert_eq!(correlation_id, 12345);
            assert_eq!(api_version, 3);
        } else {
            panic!("Expected ApiVersions request");
        }
    }

    // ========== Metadata Request Tests ==========

    #[test]
    fn test_parse_metadata_request_all_topics() {
        let (tx, _rx) = create_test_channel();

        let request = metadata_request::MetadataRequest::default();
        let frame = build_request_frame(API_KEY_METADATA, 9, 100, 2, &request, 9);

        let result = parse_request(frame, tx);
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert!(parsed.is_some());

        if let Some(KafkaRequest::Metadata {
            correlation_id,
            topics,
            ..
        }) = parsed
        {
            assert_eq!(correlation_id, 100);
            assert!(topics.is_none()); // All topics
        } else {
            panic!("Expected Metadata request");
        }
    }

    #[test]
    fn test_parse_metadata_request_specific_topics() {
        let (tx, _rx) = create_test_channel();

        let mut request = metadata_request::MetadataRequest::default();
        let mut topic = metadata_request::MetadataRequestTopic::default();
        topic.name = Some(TopicName::from(StrBytes::from_static_str("test-topic")));
        request.topics = Some(vec![topic]);

        let frame = build_request_frame(API_KEY_METADATA, 9, 101, 2, &request, 9);

        let result = parse_request(frame, tx);
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert!(parsed.is_some());

        if let Some(KafkaRequest::Metadata { topics, .. }) = parsed {
            assert!(topics.is_some());
            let topic_list = topics.unwrap();
            assert_eq!(topic_list.len(), 1);
            assert_eq!(topic_list[0], "test-topic");
        } else {
            panic!("Expected Metadata request");
        }
    }

    // ========== FindCoordinator Request Tests ==========

    #[test]
    fn test_parse_find_coordinator_request() {
        let (tx, _rx) = create_test_channel();

        let mut request = find_coordinator_request::FindCoordinatorRequest::default();
        request.key = StrBytes::from_static_str("test-group");
        request.key_type = 0; // Consumer group

        let frame = build_request_frame(API_KEY_FIND_COORDINATOR, 3, 200, 2, &request, 3);

        let result = parse_request(frame, tx);
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert!(parsed.is_some());

        if let Some(KafkaRequest::FindCoordinator { key, key_type, .. }) = parsed {
            assert_eq!(key, "test-group");
            assert_eq!(key_type, 0);
        } else {
            panic!("Expected FindCoordinator request");
        }
    }

    // ========== JoinGroup Request Tests ==========

    #[test]
    fn test_parse_join_group_request() {
        let (tx, _rx) = create_test_channel();

        let mut request = join_group_request::JoinGroupRequest::default();
        request.group_id =
            kafka_protocol::messages::GroupId::from(StrBytes::from_static_str("consumer-group-1"));
        request.session_timeout_ms = 30000;
        request.rebalance_timeout_ms = 60000;
        request.member_id = StrBytes::from_static_str("");
        request.protocol_type = StrBytes::from_static_str("consumer");

        let frame = build_request_frame(API_KEY_JOIN_GROUP, 7, 300, 2, &request, 7);

        let result = parse_request(frame, tx);
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert!(parsed.is_some());

        if let Some(KafkaRequest::JoinGroup {
            group_id,
            session_timeout_ms,
            protocol_type,
            ..
        }) = parsed
        {
            assert_eq!(group_id, "consumer-group-1");
            assert_eq!(session_timeout_ms, 30000);
            assert_eq!(protocol_type, "consumer");
        } else {
            panic!("Expected JoinGroup request");
        }
    }

    // ========== SyncGroup Request Tests ==========

    #[test]
    fn test_parse_sync_group_request() {
        let (tx, _rx) = create_test_channel();

        let mut request = sync_group_request::SyncGroupRequest::default();
        request.group_id =
            kafka_protocol::messages::GroupId::from(StrBytes::from_static_str("sync-group"));
        request.generation_id = 1;
        request.member_id = StrBytes::from_static_str("member-123");

        let frame = build_request_frame(API_KEY_SYNC_GROUP, 5, 400, 2, &request, 5);

        let result = parse_request(frame, tx);
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert!(parsed.is_some());

        if let Some(KafkaRequest::SyncGroup {
            group_id,
            generation_id,
            member_id,
            ..
        }) = parsed
        {
            assert_eq!(group_id, "sync-group");
            assert_eq!(generation_id, 1);
            assert_eq!(member_id, "member-123");
        } else {
            panic!("Expected SyncGroup request");
        }
    }

    // ========== Heartbeat Request Tests ==========

    #[test]
    fn test_parse_heartbeat_request() {
        let (tx, _rx) = create_test_channel();

        let mut request = heartbeat_request::HeartbeatRequest::default();
        request.group_id =
            kafka_protocol::messages::GroupId::from(StrBytes::from_static_str("heartbeat-group"));
        request.generation_id = 5;
        request.member_id = StrBytes::from_static_str("hb-member");

        let frame = build_request_frame(API_KEY_HEARTBEAT, 4, 500, 2, &request, 4);

        let result = parse_request(frame, tx);
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert!(parsed.is_some());

        if let Some(KafkaRequest::Heartbeat {
            group_id,
            generation_id,
            member_id,
            ..
        }) = parsed
        {
            assert_eq!(group_id, "heartbeat-group");
            assert_eq!(generation_id, 5);
            assert_eq!(member_id, "hb-member");
        } else {
            panic!("Expected Heartbeat request");
        }
    }

    // ========== LeaveGroup Request Tests ==========

    #[test]
    fn test_parse_leave_group_request() {
        let (tx, _rx) = create_test_channel();

        // Use v2 which supports member_id directly (v3+ uses members array)
        let mut request = leave_group_request::LeaveGroupRequest::default();
        request.group_id =
            kafka_protocol::messages::GroupId::from(StrBytes::from_static_str("leave-group"));
        request.member_id = StrBytes::from_static_str("leaving-member");

        // Use API version 2, header version 1 (non-flexible), request version 2
        let frame = build_request_frame(API_KEY_LEAVE_GROUP, 2, 600, 1, &request, 2);

        let result = parse_request(frame, tx);
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert!(parsed.is_some());

        if let Some(KafkaRequest::LeaveGroup {
            group_id,
            member_id,
            ..
        }) = parsed
        {
            assert_eq!(group_id, "leave-group");
            assert_eq!(member_id, "leaving-member");
        } else {
            panic!("Expected LeaveGroup request");
        }
    }

    // ========== OffsetCommit Request Tests ==========

    #[test]
    fn test_parse_offset_commit_request() {
        let (tx, _rx) = create_test_channel();

        let mut request = offset_commit_request::OffsetCommitRequest::default();
        request.group_id =
            kafka_protocol::messages::GroupId::from(StrBytes::from_static_str("commit-group"));

        let mut topic = offset_commit_request::OffsetCommitRequestTopic::default();
        topic.name = TopicName::from(StrBytes::from_static_str("commit-topic"));
        let mut partition = offset_commit_request::OffsetCommitRequestPartition::default();
        partition.partition_index = 0;
        partition.committed_offset = 100;
        topic.partitions.push(partition);
        request.topics.push(topic);

        let frame = build_request_frame(API_KEY_OFFSET_COMMIT, 8, 700, 2, &request, 8);

        let result = parse_request(frame, tx);
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert!(parsed.is_some());

        if let Some(KafkaRequest::OffsetCommit {
            group_id, topics, ..
        }) = parsed
        {
            assert_eq!(group_id, "commit-group");
            assert_eq!(topics.len(), 1);
            assert_eq!(topics[0].name, "commit-topic");
            assert_eq!(topics[0].partitions[0].committed_offset, 100);
        } else {
            panic!("Expected OffsetCommit request");
        }
    }

    // ========== OffsetFetch Request Tests ==========

    #[test]
    fn test_parse_offset_fetch_request() {
        let (tx, _rx) = create_test_channel();

        let mut request = offset_fetch_request::OffsetFetchRequest::default();
        request.group_id =
            kafka_protocol::messages::GroupId::from(StrBytes::from_static_str("fetch-group"));

        let frame = build_request_frame(API_KEY_OFFSET_FETCH, 7, 800, 2, &request, 7);

        let result = parse_request(frame, tx);
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert!(parsed.is_some());

        if let Some(KafkaRequest::OffsetFetch { group_id, .. }) = parsed {
            assert_eq!(group_id, "fetch-group");
        } else {
            panic!("Expected OffsetFetch request");
        }
    }

    // ========== ListOffsets Request Tests ==========

    #[test]
    fn test_parse_list_offsets_request() {
        let (tx, _rx) = create_test_channel();

        let mut request = list_offsets_request::ListOffsetsRequest::default();
        request.replica_id = BrokerId(-1);
        request.isolation_level = 0;

        let mut topic = list_offsets_request::ListOffsetsTopic::default();
        topic.name = TopicName::from(StrBytes::from_static_str("list-topic"));
        let mut partition = list_offsets_request::ListOffsetsPartition::default();
        partition.partition_index = 0;
        partition.timestamp = -2; // Earliest
        topic.partitions.push(partition);
        request.topics.push(topic);

        // Use v2 with header v1 (non-flexible) for cleaner encoding
        let frame = build_request_frame(API_KEY_LIST_OFFSETS, 2, 900, 1, &request, 2);

        let result = parse_request(frame, tx);
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert!(parsed.is_some());

        if let Some(KafkaRequest::ListOffsets { topics, .. }) = parsed {
            assert_eq!(topics.len(), 1);
            assert_eq!(topics[0].name, "list-topic");
        } else {
            panic!("Expected ListOffsets request");
        }
    }

    // ========== DescribeGroups Request Tests ==========

    #[test]
    fn test_parse_describe_groups_request() {
        let (tx, _rx) = create_test_channel();

        let mut request = describe_groups_request::DescribeGroupsRequest::default();
        request.groups = vec![kafka_protocol::messages::GroupId::from(
            StrBytes::from_static_str("describe-group"),
        )];

        // Use v3 with header v1 (non-flexible) for cleaner encoding
        let frame = build_request_frame(API_KEY_DESCRIBE_GROUPS, 3, 1000, 1, &request, 3);

        let result = parse_request(frame, tx);
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert!(parsed.is_some());

        if let Some(KafkaRequest::DescribeGroups { groups, .. }) = parsed {
            assert_eq!(groups.len(), 1);
            assert_eq!(groups[0], "describe-group");
        } else {
            panic!("Expected DescribeGroups request");
        }
    }

    // ========== ListGroups Request Tests ==========

    #[test]
    fn test_parse_list_groups_request() {
        let (tx, _rx) = create_test_channel();

        let request = list_groups_request::ListGroupsRequest::default();
        let frame = build_request_frame(API_KEY_LIST_GROUPS, 4, 1100, 2, &request, 4);

        let result = parse_request(frame, tx);
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert!(parsed.is_some());

        if let Some(KafkaRequest::ListGroups { correlation_id, .. }) = parsed {
            assert_eq!(correlation_id, 1100);
        } else {
            panic!("Expected ListGroups request");
        }
    }

    // ========== Admin API Tests (Phase 6) ==========

    #[test]
    fn test_parse_create_topics_request() {
        let (tx, _rx) = create_test_channel();

        let mut request = create_topics_request::CreateTopicsRequest::default();
        let mut topic = create_topics_request::CreatableTopic::default();
        topic.name = TopicName::from(StrBytes::from_static_str("new-topic"));
        topic.num_partitions = 3;
        topic.replication_factor = 1;
        request.topics.push(topic);
        request.timeout_ms = 5000;
        request.validate_only = false;

        let frame = build_request_frame(API_KEY_CREATE_TOPICS, 5, 1200, 2, &request, 5);

        let result = parse_request(frame, tx);
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert!(parsed.is_some());

        if let Some(KafkaRequest::CreateTopics {
            topics,
            timeout_ms,
            validate_only,
            ..
        }) = parsed
        {
            assert_eq!(topics.len(), 1);
            assert_eq!(topics[0].name, "new-topic");
            assert_eq!(topics[0].num_partitions, 3);
            assert_eq!(timeout_ms, 5000);
            assert!(!validate_only);
        } else {
            panic!("Expected CreateTopics request");
        }
    }

    #[test]
    fn test_parse_delete_topics_request() {
        let (tx, _rx) = create_test_channel();

        let mut request = delete_topics_request::DeleteTopicsRequest::default();
        request.topic_names = vec![TopicName::from(StrBytes::from_static_str("delete-me"))];
        request.timeout_ms = 5000;

        let frame = build_request_frame(API_KEY_DELETE_TOPICS, 5, 1300, 2, &request, 5);

        let result = parse_request(frame, tx);
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert!(parsed.is_some());

        if let Some(KafkaRequest::DeleteTopics {
            topic_names,
            timeout_ms,
            ..
        }) = parsed
        {
            assert_eq!(topic_names.len(), 1);
            assert_eq!(topic_names[0], "delete-me");
            assert_eq!(timeout_ms, 5000);
        } else {
            panic!("Expected DeleteTopics request");
        }
    }

    #[test]
    fn test_parse_create_partitions_request() {
        let (tx, _rx) = create_test_channel();

        let mut request = create_partitions_request::CreatePartitionsRequest::default();
        let mut topic = create_partitions_request::CreatePartitionsTopic::default();
        topic.name = TopicName::from(StrBytes::from_static_str("expand-topic"));
        topic.count = 6;
        request.topics.push(topic);
        request.timeout_ms = 5000;
        request.validate_only = true;

        let frame = build_request_frame(API_KEY_CREATE_PARTITIONS, 3, 1400, 2, &request, 3);

        let result = parse_request(frame, tx);
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert!(parsed.is_some());

        if let Some(KafkaRequest::CreatePartitions {
            topics,
            validate_only,
            ..
        }) = parsed
        {
            assert_eq!(topics.len(), 1);
            assert_eq!(topics[0].name, "expand-topic");
            assert_eq!(topics[0].count, 6);
            assert!(validate_only);
        } else {
            panic!("Expected CreatePartitions request");
        }
    }

    #[test]
    fn test_parse_delete_groups_request() {
        let (tx, _rx) = create_test_channel();

        let mut request = delete_groups_request::DeleteGroupsRequest::default();
        request.groups_names = vec![kafka_protocol::messages::GroupId::from(
            StrBytes::from_static_str("old-group"),
        )];

        let frame = build_request_frame(API_KEY_DELETE_GROUPS, 2, 1500, 2, &request, 2);

        let result = parse_request(frame, tx);
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert!(parsed.is_some());

        if let Some(KafkaRequest::DeleteGroups { groups_names, .. }) = parsed {
            assert_eq!(groups_names.len(), 1);
            assert_eq!(groups_names[0], "old-group");
        } else {
            panic!("Expected DeleteGroups request");
        }
    }

    // ========== Idempotent Producer Tests (Phase 9) ==========

    #[test]
    fn test_parse_init_producer_id_request() {
        let (tx, _rx) = create_test_channel();

        let mut request = init_producer_id_request::InitProducerIdRequest::default();
        request.transactional_id = None;
        request.transaction_timeout_ms = 60000;
        request.producer_id = ProducerId(-1); // New producer
        request.producer_epoch = -1;

        let frame = build_request_frame(API_KEY_INIT_PRODUCER_ID, 4, 1600, 2, &request, 4);

        let result = parse_request(frame, tx);
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert!(parsed.is_some());

        if let Some(KafkaRequest::InitProducerId {
            producer_id,
            producer_epoch,
            transaction_timeout_ms,
            ..
        }) = parsed
        {
            assert_eq!(producer_id, -1);
            assert_eq!(producer_epoch, -1);
            assert_eq!(transaction_timeout_ms, 60000);
        } else {
            panic!("Expected InitProducerId request");
        }
    }

    #[test]
    fn test_parse_init_producer_id_with_transactional_id() {
        let (tx, _rx) = create_test_channel();

        let mut request = init_producer_id_request::InitProducerIdRequest::default();
        request.transactional_id = Some(TransactionalId::from(StrBytes::from_static_str(
            "txn-producer-1",
        )));
        request.transaction_timeout_ms = 30000;
        request.producer_id = ProducerId(-1);
        request.producer_epoch = -1;

        let frame = build_request_frame(API_KEY_INIT_PRODUCER_ID, 4, 1601, 2, &request, 4);

        let result = parse_request(frame, tx);
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert!(parsed.is_some());

        if let Some(KafkaRequest::InitProducerId {
            transactional_id, ..
        }) = parsed
        {
            assert!(transactional_id.is_some());
            assert_eq!(transactional_id.unwrap(), "txn-producer-1");
        } else {
            panic!("Expected InitProducerId request");
        }
    }

    // ========== Transaction API Tests (Phase 10) ==========

    #[test]
    fn test_parse_add_offsets_to_txn_request() {
        let (tx, _rx) = create_test_channel();

        let mut request = add_offsets_to_txn_request::AddOffsetsToTxnRequest::default();
        request.transactional_id = TransactionalId::from(StrBytes::from_static_str("txn-1"));
        request.producer_id = ProducerId(12345);
        request.producer_epoch = 0;
        request.group_id =
            kafka_protocol::messages::GroupId::from(StrBytes::from_static_str("consumer-group"));

        let frame = build_request_frame(API_KEY_ADD_OFFSETS_TO_TXN, 3, 1700, 2, &request, 3);

        let result = parse_request(frame, tx);
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert!(parsed.is_some());

        if let Some(KafkaRequest::AddOffsetsToTxn {
            transactional_id,
            producer_id,
            group_id,
            ..
        }) = parsed
        {
            assert_eq!(transactional_id, "txn-1");
            assert_eq!(producer_id, 12345);
            assert_eq!(group_id, "consumer-group");
        } else {
            panic!("Expected AddOffsetsToTxn request");
        }
    }

    #[test]
    fn test_parse_end_txn_request_commit() {
        let (tx, _rx) = create_test_channel();

        let mut request = end_txn_request::EndTxnRequest::default();
        request.transactional_id = TransactionalId::from(StrBytes::from_static_str("txn-commit"));
        request.producer_id = ProducerId(99999);
        request.producer_epoch = 1;
        request.committed = true;

        let frame = build_request_frame(API_KEY_END_TXN, 3, 1800, 2, &request, 3);

        let result = parse_request(frame, tx);
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert!(parsed.is_some());

        if let Some(KafkaRequest::EndTxn {
            transactional_id,
            committed,
            ..
        }) = parsed
        {
            assert_eq!(transactional_id, "txn-commit");
            assert!(committed);
        } else {
            panic!("Expected EndTxn request");
        }
    }

    #[test]
    fn test_parse_end_txn_request_abort() {
        let (tx, _rx) = create_test_channel();

        let mut request = end_txn_request::EndTxnRequest::default();
        request.transactional_id = TransactionalId::from(StrBytes::from_static_str("txn-abort"));
        request.producer_id = ProducerId(88888);
        request.producer_epoch = 2;
        request.committed = false;

        let frame = build_request_frame(API_KEY_END_TXN, 3, 1801, 2, &request, 3);

        let result = parse_request(frame, tx);
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert!(parsed.is_some());

        if let Some(KafkaRequest::EndTxn { committed, .. }) = parsed {
            assert!(!committed);
        } else {
            panic!("Expected EndTxn request");
        }
    }

    // ========== Fetch Request Tests ==========

    #[test]
    fn test_parse_fetch_request() {
        let (tx, mut rx) = create_test_channel();

        let mut request = fetch_request::FetchRequest::default();
        request.max_wait_ms = 5000;
        request.min_bytes = 1;
        request.max_bytes = 1048576;
        request.isolation_level = 0;

        let mut topic = fetch_request::FetchTopic::default();
        topic.topic = TopicName::from(StrBytes::from_static_str("fetch-topic"));
        let mut partition = fetch_request::FetchPartition::default();
        partition.partition = 0;
        partition.fetch_offset = 100;
        partition.partition_max_bytes = 65536;
        topic.partitions.push(partition);
        request.topics.push(topic);

        // Use Fetch v4 with header v1 (non-flexible, simpler encoding)
        let frame = build_request_frame(API_KEY_FETCH, 4, 2000, 1, &request, 4);

        let result = parse_request(frame, tx);
        assert!(result.is_ok(), "parse_request failed");
        let parsed = result.unwrap();

        // Check if an error response was sent
        if parsed.is_none() {
            if let Ok(error) = rx.try_recv() {
                panic!("Received error response: {:?}", error);
            }
        }

        assert!(parsed.is_some(), "Expected Some, got None");

        if let Some(KafkaRequest::Fetch {
            max_wait_ms,
            min_bytes,
            max_bytes,
            topic_data,
            ..
        }) = parsed
        {
            assert_eq!(max_wait_ms, 5000);
            assert_eq!(min_bytes, 1);
            assert_eq!(max_bytes, 1048576);
            assert_eq!(topic_data.len(), 1);
            assert_eq!(topic_data[0].name, "fetch-topic");
            assert_eq!(topic_data[0].partitions[0].fetch_offset, 100);
        } else {
            panic!("Expected Fetch request");
        }
    }

    #[test]
    fn test_parse_fetch_request_with_isolation_level() {
        let (tx, mut rx) = create_test_channel();

        let mut request = fetch_request::FetchRequest::default();
        request.max_wait_ms = 1000;
        request.min_bytes = 0;
        request.max_bytes = 1048576;
        request.isolation_level = 1; // READ_COMMITTED

        // Use Fetch v4 with header v1 (simpler, stable encoding)
        let frame = build_request_frame(API_KEY_FETCH, 4, 2001, 1, &request, 4);

        let result = parse_request(frame, tx);
        assert!(result.is_ok(), "parse_request failed");
        let parsed = result.unwrap();

        // Check if an error response was sent
        if parsed.is_none() {
            if let Ok(error) = rx.try_recv() {
                panic!("Received error response: {:?}", error);
            }
        }

        assert!(parsed.is_some(), "Expected Some, got None");

        if let Some(KafkaRequest::Fetch {
            isolation_level, ..
        }) = parsed
        {
            assert_eq!(isolation_level, 1);
        } else {
            panic!("Expected Fetch request");
        }
    }

    // ========== Produce Request Tests ==========

    #[test]
    fn test_parse_produce_request_empty() {
        let (tx, _rx) = create_test_channel();

        let mut request = produce_request::ProduceRequest::default();
        request.acks = 1;
        request.timeout_ms = 5000;

        // Use Produce v7 with header v1 (non-flexible)
        let frame = build_request_frame(API_KEY_PRODUCE, 7, 3000, 1, &request, 7);

        let result = parse_request(frame, tx);
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert!(parsed.is_some());

        if let Some(KafkaRequest::Produce {
            acks, timeout_ms, ..
        }) = parsed
        {
            assert_eq!(acks, 1);
            assert_eq!(timeout_ms, 5000);
        } else {
            panic!("Expected Produce request");
        }
    }

    #[test]
    fn test_parse_produce_request_acks_zero() {
        let (tx, _rx) = create_test_channel();

        let mut request = produce_request::ProduceRequest::default();
        request.acks = 0; // Fire and forget
        request.timeout_ms = 1000;

        let frame = build_request_frame(API_KEY_PRODUCE, 7, 3001, 1, &request, 7);

        let result = parse_request(frame, tx);
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert!(parsed.is_some());

        if let Some(KafkaRequest::Produce { acks, .. }) = parsed {
            assert_eq!(acks, 0);
        } else {
            panic!("Expected Produce request");
        }
    }

    #[test]
    fn test_parse_produce_request_acks_all() {
        let (tx, _rx) = create_test_channel();

        let mut request = produce_request::ProduceRequest::default();
        request.acks = -1; // All ISRs
        request.timeout_ms = 30000;

        let frame = build_request_frame(API_KEY_PRODUCE, 7, 3002, 1, &request, 7);

        let result = parse_request(frame, tx);
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert!(parsed.is_some());

        if let Some(KafkaRequest::Produce { acks, .. }) = parsed {
            assert_eq!(acks, -1);
        } else {
            panic!("Expected Produce request");
        }
    }

    // ========== AddPartitionsToTxn Request Tests ==========

    #[test]
    fn test_parse_add_partitions_to_txn_request_v3() {
        let (tx, _rx) = create_test_channel();

        let mut request = add_partitions_to_txn_request::AddPartitionsToTxnRequest::default();
        request.v3_and_below_transactional_id =
            TransactionalId::from(StrBytes::from_static_str("txn-test"));
        request.v3_and_below_producer_id = ProducerId(11111);
        request.v3_and_below_producer_epoch = 0;

        let mut topic = add_partitions_to_txn_request::AddPartitionsToTxnTopic::default();
        topic.name = TopicName::from(StrBytes::from_static_str("txn-topic"));
        topic.partitions = vec![0, 1, 2];
        request.v3_and_below_topics.push(topic);

        // Use v3 (before batch API)
        let frame = build_request_frame(API_KEY_ADD_PARTITIONS_TO_TXN, 3, 4000, 2, &request, 3);

        let result = parse_request(frame, tx);
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert!(parsed.is_some());

        if let Some(KafkaRequest::AddPartitionsToTxn {
            transactional_id,
            producer_id,
            topics,
            ..
        }) = parsed
        {
            assert_eq!(transactional_id, "txn-test");
            assert_eq!(producer_id, 11111);
            assert_eq!(topics.len(), 1);
            assert_eq!(topics[0].0, "txn-topic");
            assert_eq!(topics[0].1, vec![0, 1, 2]);
        } else {
            panic!("Expected AddPartitionsToTxn request");
        }
    }

    // ========== TxnOffsetCommit Request Tests ==========

    #[test]
    fn test_parse_txn_offset_commit_request() {
        let (tx, _rx) = create_test_channel();

        let mut request = txn_offset_commit_request::TxnOffsetCommitRequest::default();
        request.transactional_id = TransactionalId::from(StrBytes::from_static_str("txn-commit"));
        request.group_id =
            kafka_protocol::messages::GroupId::from(StrBytes::from_static_str("commit-group"));
        request.producer_id = ProducerId(22222);
        request.producer_epoch = 1;

        let mut topic = txn_offset_commit_request::TxnOffsetCommitRequestTopic::default();
        topic.name = TopicName::from(StrBytes::from_static_str("offset-topic"));
        let mut partition = txn_offset_commit_request::TxnOffsetCommitRequestPartition::default();
        partition.partition_index = 0;
        partition.committed_offset = 500;
        partition.committed_metadata = Some(StrBytes::from_static_str("metadata"));
        topic.partitions.push(partition);
        request.topics.push(topic);

        let frame = build_request_frame(API_KEY_TXN_OFFSET_COMMIT, 3, 5000, 2, &request, 3);

        let result = parse_request(frame, tx);
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert!(parsed.is_some());

        if let Some(KafkaRequest::TxnOffsetCommit {
            transactional_id,
            group_id,
            producer_id,
            topics,
            ..
        }) = parsed
        {
            assert_eq!(transactional_id, "txn-commit");
            assert_eq!(group_id, "commit-group");
            assert_eq!(producer_id, 22222);
            assert_eq!(topics.len(), 1);
            assert_eq!(topics[0].0, "offset-topic");
            assert_eq!(topics[0].1.len(), 1);
            assert_eq!(topics[0].1[0].0, 0); // partition_index
            assert_eq!(topics[0].1[0].1, 500); // committed_offset
            assert_eq!(topics[0].1[0].2, Some("metadata".to_string()));
        } else {
            panic!("Expected TxnOffsetCommit request");
        }
    }

    // ========== OffsetFetch Request Tests (Additional) ==========

    #[test]
    fn test_parse_offset_fetch_request_with_topics() {
        let (tx, _rx) = create_test_channel();

        let mut request = offset_fetch_request::OffsetFetchRequest::default();
        request.group_id = kafka_protocol::messages::GroupId::from(StrBytes::from_static_str(
            "fetch-offsets-group",
        ));

        let mut topic = offset_fetch_request::OffsetFetchRequestTopic::default();
        topic.name = TopicName::from(StrBytes::from_static_str("offset-topic"));
        topic.partition_indexes = vec![0, 1, 2];
        request.topics = Some(vec![topic]);

        let frame = build_request_frame(API_KEY_OFFSET_FETCH, 7, 6000, 2, &request, 7);

        let result = parse_request(frame, tx);
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert!(parsed.is_some());

        if let Some(KafkaRequest::OffsetFetch {
            group_id, topics, ..
        }) = parsed
        {
            assert_eq!(group_id, "fetch-offsets-group");
            assert!(topics.is_some());
            let topic_list = topics.unwrap();
            assert_eq!(topic_list.len(), 1);
            assert_eq!(topic_list[0].name, "offset-topic");
            assert_eq!(topic_list[0].partition_indexes, vec![0, 1, 2]);
        } else {
            panic!("Expected OffsetFetch request");
        }
    }

    // ========== Error Handling Tests ==========

    #[test]
    fn test_parse_invalid_header_panics() {
        // The kafka-protocol crate panics on invalid input (empty buffer)
        // This test verifies that behavior using catch_unwind
        use std::panic;

        let result = panic::catch_unwind(|| {
            let (tx, _rx) = create_test_channel();
            let buf = BytesMut::new();
            let _ = parse_request(buf, tx);
        });

        // Should panic on empty buffer
        assert!(result.is_err(), "Expected panic on empty buffer");
    }

    #[test]
    fn test_parse_truncated_header_panics() {
        // The kafka-protocol crate panics on truncated input
        use std::panic;

        let result = panic::catch_unwind(|| {
            let (tx, _rx) = create_test_channel();
            let mut buf = BytesMut::new();
            buf.put_i16(0); // Just API key, no version/correlation_id
            let _ = parse_request(buf, tx);
        });

        // Should panic on truncated header
        assert!(result.is_err(), "Expected panic on truncated header");
    }

    // ========== Correlation ID Preservation Tests ==========

    #[test]
    fn test_correlation_id_preserved() {
        let correlation_ids = [0i32, 1, -1, i32::MAX, i32::MIN];

        for &corr_id in &correlation_ids {
            let (tx, _rx) = create_test_channel();
            let request = api_versions_request::ApiVersionsRequest::default();
            let frame = build_request_frame(API_KEY_API_VERSIONS, 3, corr_id, 2, &request, 3);

            let result = parse_request(frame, tx);
            assert!(result.is_ok());
            let parsed = result.unwrap();
            assert!(parsed.is_some());

            if let Some(KafkaRequest::ApiVersions { correlation_id, .. }) = parsed {
                assert_eq!(
                    correlation_id, corr_id,
                    "Correlation ID not preserved for {}",
                    corr_id
                );
            } else {
                panic!("Expected ApiVersions request");
            }
        }
    }

    // ========== Client ID Extraction Tests ==========

    #[test]
    fn test_client_id_extracted() {
        let (tx, _rx) = create_test_channel();

        let header = RequestHeader::default()
            .with_request_api_key(API_KEY_API_VERSIONS)
            .with_request_api_version(3)
            .with_correlation_id(1)
            .with_client_id(Some(StrBytes::from_static_str("my-special-client")));

        let mut buf = BytesMut::new();
        header.encode(&mut buf, 2).unwrap();
        api_versions_request::ApiVersionsRequest::default()
            .encode(&mut buf, 3)
            .unwrap();

        let result = parse_request(buf, tx);
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert!(parsed.is_some());

        if let Some(KafkaRequest::ApiVersions { client_id, .. }) = parsed {
            assert!(client_id.is_some());
            assert_eq!(client_id.unwrap(), "my-special-client");
        } else {
            panic!("Expected ApiVersions request");
        }
    }

    #[test]
    fn test_client_id_none() {
        let (tx, _rx) = create_test_channel();

        let header = RequestHeader::default()
            .with_request_api_key(API_KEY_API_VERSIONS)
            .with_request_api_version(3)
            .with_correlation_id(1)
            .with_client_id(None);

        let mut buf = BytesMut::new();
        header.encode(&mut buf, 2).unwrap();
        api_versions_request::ApiVersionsRequest::default()
            .encode(&mut buf, 3)
            .unwrap();

        let result = parse_request(buf, tx);
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert!(parsed.is_some());

        if let Some(KafkaRequest::ApiVersions { client_id, .. }) = parsed {
            assert!(client_id.is_none());
        } else {
            panic!("Expected ApiVersions request");
        }
    }

    // ========== Edge Case Tests (Added for Coverage) ==========

    #[test]
    fn test_parse_add_partitions_to_txn_request_v4() {
        // Tests the v4+ batch transaction format (different from v3)
        let (tx, _rx) = create_test_channel();

        let mut request = add_partitions_to_txn_request::AddPartitionsToTxnRequest::default();

        // v4+ uses the transactions array field for batched transactions
        let mut txn = add_partitions_to_txn_request::AddPartitionsToTxnTransaction::default();
        txn.transactional_id = TransactionalId::from(StrBytes::from_static_str("batch-txn"));
        txn.producer_id = ProducerId(44444);
        txn.producer_epoch = 1;

        let mut topic = add_partitions_to_txn_request::AddPartitionsToTxnTopic::default();
        topic.name = TopicName::from(StrBytes::from_static_str("batch-topic"));
        topic.partitions = vec![0, 1];
        txn.topics.push(topic);

        request.transactions.push(txn);

        let frame = build_request_frame(API_KEY_ADD_PARTITIONS_TO_TXN, 4, 4001, 2, &request, 4);

        let result = parse_request(frame, tx);
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert!(parsed.is_some());

        if let Some(KafkaRequest::AddPartitionsToTxn {
            transactional_id,
            producer_id,
            producer_epoch,
            topics,
            ..
        }) = parsed
        {
            assert_eq!(transactional_id, "batch-txn");
            assert_eq!(producer_id, 44444);
            assert_eq!(producer_epoch, 1);
            assert_eq!(topics.len(), 1);
            assert_eq!(topics[0].0, "batch-topic");
            assert_eq!(topics[0].1, vec![0, 1]);
        } else {
            panic!("Expected AddPartitionsToTxn request");
        }
    }

    #[test]
    fn test_parse_fetch_request_multiple_topics() {
        // Tests multi-topic fetch requests
        let (tx, mut rx) = create_test_channel();

        let mut request = fetch_request::FetchRequest::default();
        request.max_wait_ms = 1000;
        request.min_bytes = 1;
        request.max_bytes = 1048576;

        // Topic 1 with 2 partitions
        let mut topic1 = fetch_request::FetchTopic::default();
        topic1.topic = TopicName::from(StrBytes::from_static_str("topic-a"));
        let mut p0 = fetch_request::FetchPartition::default();
        p0.partition = 0;
        p0.fetch_offset = 0;
        p0.partition_max_bytes = 32768;
        let mut p1 = fetch_request::FetchPartition::default();
        p1.partition = 1;
        p1.fetch_offset = 100;
        p1.partition_max_bytes = 32768;
        topic1.partitions.push(p0);
        topic1.partitions.push(p1);

        // Topic 2 with 1 partition
        let mut topic2 = fetch_request::FetchTopic::default();
        topic2.topic = TopicName::from(StrBytes::from_static_str("topic-b"));
        let mut p2 = fetch_request::FetchPartition::default();
        p2.partition = 0;
        p2.fetch_offset = 50;
        p2.partition_max_bytes = 32768;
        topic2.partitions.push(p2);

        request.topics.push(topic1);
        request.topics.push(topic2);

        let frame = build_request_frame(API_KEY_FETCH, 4, 2002, 1, &request, 4);

        let result = parse_request(frame, tx);
        assert!(result.is_ok(), "parse_request failed");
        let parsed = result.unwrap();

        // Check if an error response was sent
        if parsed.is_none() {
            if let Ok(error) = rx.try_recv() {
                panic!("Received error response: {:?}", error);
            }
        }

        assert!(parsed.is_some(), "Expected Some, got None");

        if let Some(KafkaRequest::Fetch { topic_data, .. }) = parsed {
            assert_eq!(topic_data.len(), 2);
            assert_eq!(topic_data[0].name, "topic-a");
            assert_eq!(topic_data[0].partitions.len(), 2);
            assert_eq!(topic_data[0].partitions[0].fetch_offset, 0);
            assert_eq!(topic_data[0].partitions[1].fetch_offset, 100);
            assert_eq!(topic_data[1].name, "topic-b");
            assert_eq!(topic_data[1].partitions.len(), 1);
            assert_eq!(topic_data[1].partitions[0].fetch_offset, 50);
        } else {
            panic!("Expected Fetch request");
        }
    }

    #[test]
    fn test_parse_produce_request_with_transactional_id() {
        // Tests transactional produce (v3+ supports transactional_id)
        let (tx, _rx) = create_test_channel();

        let mut request = produce_request::ProduceRequest::default();
        request.transactional_id = Some(TransactionalId::from(StrBytes::from_static_str(
            "txn-producer",
        )));
        request.acks = -1;
        request.timeout_ms = 30000;

        let frame = build_request_frame(API_KEY_PRODUCE, 7, 3003, 1, &request, 7);

        let result = parse_request(frame, tx);
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert!(parsed.is_some());

        if let Some(KafkaRequest::Produce {
            transactional_id,
            acks,
            ..
        }) = parsed
        {
            assert!(transactional_id.is_some());
            assert_eq!(transactional_id.unwrap(), "txn-producer");
            assert_eq!(acks, -1);
        } else {
            panic!("Expected Produce request");
        }
    }

    #[test]
    fn test_parse_offset_commit_request_with_metadata() {
        // Tests offset commit with consumer metadata field
        let (tx, _rx) = create_test_channel();

        let mut request = offset_commit_request::OffsetCommitRequest::default();
        request.group_id =
            kafka_protocol::messages::GroupId::from(StrBytes::from_static_str("metadata-group"));

        let mut topic = offset_commit_request::OffsetCommitRequestTopic::default();
        topic.name = TopicName::from(StrBytes::from_static_str("metadata-topic"));

        let mut partition = offset_commit_request::OffsetCommitRequestPartition::default();
        partition.partition_index = 0;
        partition.committed_offset = 999;
        partition.committed_metadata = Some(StrBytes::from_static_str("consumer-state-data"));
        topic.partitions.push(partition);
        request.topics.push(topic);

        let frame = build_request_frame(API_KEY_OFFSET_COMMIT, 8, 701, 2, &request, 8);

        let result = parse_request(frame, tx);
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert!(parsed.is_some());

        if let Some(KafkaRequest::OffsetCommit {
            group_id, topics, ..
        }) = parsed
        {
            assert_eq!(group_id, "metadata-group");
            assert_eq!(topics.len(), 1);
            assert_eq!(topics[0].partitions[0].committed_offset, 999);
            assert_eq!(
                topics[0].partitions[0].metadata,
                Some("consumer-state-data".to_string())
            );
        } else {
            panic!("Expected OffsetCommit request");
        }
    }

    #[test]
    fn test_parse_unsupported_api_key() {
        // Tests error handling for valid-but-unsupported API keys
        // API key 17 = SaslHandshake (valid Kafka API, but not implemented here)
        let (tx, mut rx) = create_test_channel();

        use kafka_protocol::messages::sasl_handshake_request::SaslHandshakeRequest;

        let mut request = SaslHandshakeRequest::default();
        request.mechanism = StrBytes::from_static_str("PLAIN");

        // SaslHandshake uses non-flexible header (version 1)
        let frame = build_request_frame(17, 1, 9999, 1, &request, 1);

        let result = parse_request(frame, tx);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none()); // Should return None for unsupported API

        // Should have sent an error response
        let response = rx.try_recv();
        assert!(response.is_ok(), "Expected error response to be sent");

        if let Ok(super::super::super::messages::KafkaResponse::Error { error_code, .. }) = response
        {
            assert_eq!(error_code, ERROR_UNSUPPORTED_VERSION);
        } else {
            panic!("Expected Error response");
        }
    }
}
