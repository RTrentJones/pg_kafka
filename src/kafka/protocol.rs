// Kafka protocol parsing and encoding module
//
// This module handles the binary Kafka wire protocol format:
// [4 bytes: Size (big-endian i32)] [RequestHeader] [RequestBody]
//
// The kafka-protocol crate provides auto-generated structs for all Kafka messages,
// but we need to handle the framing (size prefix) and routing (api_key matching) ourselves.

use bytes::{BufMut, BytesMut};
use kafka_protocol::messages::fetch_request::FetchRequest;
use kafka_protocol::messages::offset_commit_request::OffsetCommitRequest;
use kafka_protocol::messages::offset_fetch_request::OffsetFetchRequest;
use kafka_protocol::messages::produce_request::ProduceRequest;
use kafka_protocol::messages::ResponseHeader;
use kafka_protocol::protocol::{decode_request_header_from_buffer, Decodable, Encodable};
use kafka_protocol::records::RecordBatchDecoder;

use super::constants::*;
use super::error::{KafkaError, Result};
use super::messages::{KafkaRequest, KafkaResponse};

// Import conditional logging macros for test isolation
use crate::{pg_log, pg_warning};

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
    response_tx: tokio::sync::mpsc::UnboundedSender<KafkaResponse>,
) -> Result<Option<KafkaRequest>> {
    pg_log!("Parsing request from {} byte frame", frame.len());

    // Parse RequestHeader using kafka-protocol's decode function
    // This automatically handles both non-flexible and flexible header formats
    let mut payload_buf = frame;

    let header = match decode_request_header_from_buffer(&mut payload_buf) {
        Ok(h) => h,
        Err(e) => {
            pg_warning!("Failed to decode RequestHeader: {}", e);
            return Err(KafkaError::ProtocolCodec(e));
        }
    };

    let api_key = header.request_api_key;
    let api_version = header.request_api_version;
    let correlation_id = header.correlation_id;
    let client_id = header.client_id.map(|s| s.to_string());

    pg_log!(
        "Parsed RequestHeader: api_key={}, api_version={}, correlation_id={}, client_id={:?}",
        api_key,
        api_version,
        correlation_id,
        client_id
    );

    // Match on api_key to determine request type
    match api_key {
        API_KEY_API_VERSIONS => {
            // ApiVersions request
            // The body is empty for ApiVersions, so we don't need to parse it
            pg_log!(
                "Parsed ApiVersions request (api_key={}, api_version={})",
                API_KEY_API_VERSIONS,
                api_version
            );
            Ok(Some(KafkaRequest::ApiVersions {
                correlation_id,
                client_id,
                api_version,
                response_tx,
            }))
        }
        API_KEY_METADATA => {
            // Metadata request - parse using kafka-protocol crate
            pg_log!(
                "Parsed Metadata request (api_key={}, version={})",
                API_KEY_METADATA,
                api_version
            );

            // Use kafka-protocol crate to decode MetadataRequest
            let metadata_req =
                match kafka_protocol::messages::metadata_request::MetadataRequest::decode(
                    &mut payload_buf,
                    api_version,
                ) {
                    Ok(req) => req,
                    Err(e) => {
                        pg_warning!("Failed to decode MetadataRequest: {}", e);
                        let error_response = KafkaResponse::Error {
                            correlation_id,
                            error_code: ERROR_CORRUPT_MESSAGE,
                            error_message: Some(format!("Malformed MetadataRequest: {}", e)),
                        };
                        let _ = response_tx.send(error_response);
                        return Ok(None);
                    }
                };

            // Extract requested topics (None means "all topics")
            // Extract requested topics (None means "all topics")
            let topics = match metadata_req.topics {
                Some(ref t) if !t.is_empty() => {
                    let topic_names: Vec<String> = t
                        .iter()
                        .filter_map(|t| t.name.as_ref().map(|n| n.to_string()))
                        .collect();
                    pg_log!("Metadata request for specific topics: {:?}", topic_names);
                    if topic_names.is_empty() {
                        None
                    } else {
                        Some(topic_names)
                    }
                }
                _ => {
                    pg_log!("Metadata request for ALL topics");
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
        API_KEY_PRODUCE => {
            // Produce request - write messages to topics
            pg_log!(
                "Parsed Produce request (api_key={}, version={})",
                API_KEY_PRODUCE,
                api_version
            );

            // Use kafka-protocol crate to decode ProduceRequest
            let produce_req = match ProduceRequest::decode(&mut payload_buf, api_version) {
                Ok(req) => req,
                Err(e) => {
                    pg_warning!("Failed to decode ProduceRequest: {}", e);
                    let error_response = KafkaResponse::Error {
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

            pg_log!(
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
                    // RecordBatch is the binary format containing actual messages
                    let records = match &partition.records {
                        Some(batch_bytes) => match parse_record_batch(batch_bytes) {
                            Ok(records) => records,
                            Err(e) => {
                                pg_warning!(
                                    "Failed to parse RecordBatch for topic={}, partition={}: {}",
                                    topic_name,
                                    partition_index,
                                    e
                                );
                                let error_response = KafkaResponse::Error {
                                    correlation_id,
                                    error_code: ERROR_CORRUPT_MESSAGE,
                                    error_message: Some(format!("Invalid RecordBatch: {}", e)),
                                };
                                let _ = response_tx.send(error_response);
                                return Ok(None);
                            }
                        },
                        None => Vec::new(), // No records in this partition
                    };

                    pg_log!(
                        "Parsed {} records for topic={}, partition={}",
                        records.len(),
                        topic_name,
                        partition_index
                    );

                    partitions.push(super::messages::PartitionProduceData {
                        partition_index,
                        records,
                    });
                }

                topic_data.push(super::messages::TopicProduceData {
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
        API_KEY_FETCH => {
            // Fetch request - read messages from topics
            pg_log!(
                "Parsed Fetch request (api_key={}, version={})",
                API_KEY_FETCH,
                api_version
            );

            // Use kafka-protocol crate to decode FetchRequest
            let fetch_req = match FetchRequest::decode(&mut payload_buf, api_version) {
                Ok(req) => req,
                Err(e) => {
                    pg_warning!("Failed to decode FetchRequest: {}", e);
                    let error_response = KafkaResponse::Error {
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

            pg_log!(
                "FetchRequest: max_wait_ms={}, min_bytes={}, max_bytes={}, topics={}",
                max_wait_ms,
                min_bytes,
                max_bytes,
                fetch_req.topics.len()
            );

            // Extract topic data from kafka-protocol types to our types
            let mut topic_data = Vec::new();
            for topic in fetch_req.topics {
                let topic_name = topic.topic.to_string();
                let mut partitions = Vec::new();

                for partition in topic.partitions {
                    partitions.push(super::messages::PartitionFetchData {
                        partition_index: partition.partition,
                        fetch_offset: partition.fetch_offset,
                        partition_max_bytes: partition.partition_max_bytes,
                    });
                }

                pg_log!(
                    "Fetch from topic={}, {} partitions",
                    topic_name,
                    partitions.len()
                );

                topic_data.push(super::messages::TopicFetchData {
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
        API_KEY_OFFSET_COMMIT => {
            // OffsetCommit request - commit consumed offsets
            pg_log!(
                "Parsed OffsetCommit request (api_key={}, version={})",
                API_KEY_OFFSET_COMMIT,
                api_version
            );

            // Use kafka-protocol crate to decode OffsetCommitRequest
            let commit_req = match OffsetCommitRequest::decode(&mut payload_buf, api_version) {
                Ok(req) => req,
                Err(e) => {
                    pg_warning!("Failed to decode OffsetCommitRequest: {}", e);
                    let error_response = KafkaResponse::Error {
                        correlation_id,
                        error_code: ERROR_CORRUPT_MESSAGE,
                        error_message: Some(format!("Malformed OffsetCommitRequest: {}", e)),
                    };
                    let _ = response_tx.send(error_response);
                    return Ok(None);
                }
            };

            let group_id = commit_req.group_id.to_string();

            pg_log!(
                "OffsetCommit for group_id={}, {} topics",
                group_id,
                commit_req.topics.len()
            );

            // Extract topic data from kafka-protocol types to our types
            let mut topics = Vec::new();
            for topic in commit_req.topics {
                let topic_name = topic.name.to_string();
                let mut partitions = Vec::new();

                for partition in topic.partitions {
                    partitions.push(super::messages::OffsetCommitPartitionData {
                        partition_index: partition.partition_index,
                        committed_offset: partition.committed_offset,
                        metadata: partition
                            .committed_metadata
                            .map(|s| s.to_string())
                            .filter(|s| !s.is_empty()),
                    });
                }

                topics.push(super::messages::OffsetCommitTopicData {
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
        API_KEY_OFFSET_FETCH => {
            // OffsetFetch request - fetch committed offsets
            pg_log!(
                "Parsed OffsetFetch request (api_key={}, version={})",
                API_KEY_OFFSET_FETCH,
                api_version
            );

            // Use kafka-protocol crate to decode OffsetFetchRequest
            let fetch_req = match OffsetFetchRequest::decode(&mut payload_buf, api_version) {
                Ok(req) => req,
                Err(e) => {
                    pg_warning!("Failed to decode OffsetFetchRequest: {}", e);
                    let error_response = KafkaResponse::Error {
                        correlation_id,
                        error_code: ERROR_CORRUPT_MESSAGE,
                        error_message: Some(format!("Malformed OffsetFetchRequest: {}", e)),
                    };
                    let _ = response_tx.send(error_response);
                    return Ok(None);
                }
            };

            let group_id = fetch_req.group_id.to_string();

            pg_log!("OffsetFetch for group_id={}", group_id);

            // Extract topic data (None = fetch all topics for this group)
            let topics = if let Some(topic_vec) = fetch_req.topics {
                if topic_vec.is_empty() {
                    None
                } else {
                    let mut topic_list = Vec::new();
                    for topic in topic_vec {
                        let topic_name = topic.name.to_string();
                        let partition_indexes = topic.partition_indexes;

                        topic_list.push(super::messages::OffsetFetchTopicData {
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
        API_KEY_FIND_COORDINATOR => {
            // FindCoordinator request - discover the coordinator for a consumer group
            pg_log!(
                "Parsed FindCoordinator request (api_key={}, version={})",
                API_KEY_FIND_COORDINATOR,
                api_version
            );

            // Use kafka-protocol crate to decode FindCoordinatorRequest
            let coord_req = match kafka_protocol::messages::find_coordinator_request::FindCoordinatorRequest::decode(&mut payload_buf, api_version) {
                Ok(req) => req,
                Err(e) => {
                    pg_warning!("Failed to decode FindCoordinatorRequest: {}", e);
                    let error_response = KafkaResponse::Error {
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
        API_KEY_JOIN_GROUP => {
            // JoinGroup request - consumer joins a consumer group
            pg_log!(
                "Parsed JoinGroup request (api_key={}, version={})",
                API_KEY_JOIN_GROUP,
                api_version
            );

            // Use kafka-protocol crate to decode JoinGroupRequest
            let join_req =
                match kafka_protocol::messages::join_group_request::JoinGroupRequest::decode(
                    &mut payload_buf,
                    api_version,
                ) {
                    Ok(req) => req,
                    Err(e) => {
                        pg_warning!("Failed to decode JoinGroupRequest: {}", e);
                        let error_response = KafkaResponse::Error {
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

            // Extract protocols
            let protocols = join_req
                .protocols
                .into_iter()
                .map(|p| super::messages::JoinGroupProtocol {
                    name: p.name.to_string(),
                    metadata: p.metadata.to_vec(),
                })
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
        API_KEY_SYNC_GROUP => {
            // SyncGroup request - synchronize partition assignments
            pg_log!(
                "Parsed SyncGroup request (api_key={}, version={})",
                API_KEY_SYNC_GROUP,
                api_version
            );

            // Use kafka-protocol crate to decode SyncGroupRequest
            let sync_req =
                match kafka_protocol::messages::sync_group_request::SyncGroupRequest::decode(
                    &mut payload_buf,
                    api_version,
                ) {
                    Ok(req) => req,
                    Err(e) => {
                        pg_warning!("Failed to decode SyncGroupRequest: {}", e);
                        let error_response = KafkaResponse::Error {
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

            // Extract assignments
            let assignments = sync_req
                .assignments
                .into_iter()
                .map(|a| super::messages::SyncGroupAssignment {
                    member_id: a.member_id.to_string(),
                    assignment: a.assignment.to_vec(),
                })
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
        API_KEY_HEARTBEAT => {
            // Heartbeat request - maintain consumer group membership
            pg_log!(
                "Parsed Heartbeat request (api_key={}, version={})",
                API_KEY_HEARTBEAT,
                api_version
            );

            // Use kafka-protocol crate to decode HeartbeatRequest
            let heartbeat_req =
                match kafka_protocol::messages::heartbeat_request::HeartbeatRequest::decode(
                    &mut payload_buf,
                    api_version,
                ) {
                    Ok(req) => req,
                    Err(e) => {
                        pg_warning!("Failed to decode HeartbeatRequest: {}", e);
                        let error_response = KafkaResponse::Error {
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
        API_KEY_LEAVE_GROUP => {
            // LeaveGroup request - consumer leaves a consumer group
            pg_log!(
                "Parsed LeaveGroup request (api_key={}, version={})",
                API_KEY_LEAVE_GROUP,
                api_version
            );

            // Use kafka-protocol crate to decode LeaveGroupRequest
            let leave_req =
                match kafka_protocol::messages::leave_group_request::LeaveGroupRequest::decode(
                    &mut payload_buf,
                    api_version,
                ) {
                    Ok(req) => req,
                    Err(e) => {
                        pg_warning!("Failed to decode LeaveGroupRequest: {}", e);
                        let error_response = KafkaResponse::Error {
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

            // Extract members (v3+)
            let members = leave_req
                .members
                .into_iter()
                .map(|m| super::messages::MemberIdentity {
                    member_id: m.member_id.to_string(),
                    group_instance_id: m.group_instance_id.map(|s| s.to_string()),
                })
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
        API_KEY_LIST_OFFSETS => {
            // ListOffsets request - query earliest/latest offsets
            pg_log!(
                "Parsed ListOffsets request (api_key={}, version={})",
                API_KEY_LIST_OFFSETS,
                api_version
            );

            // Use kafka-protocol crate to decode ListOffsetsRequest
            let list_req =
                match kafka_protocol::messages::list_offsets_request::ListOffsetsRequest::decode(
                    &mut payload_buf,
                    api_version,
                ) {
                    Ok(req) => req,
                    Err(e) => {
                        pg_warning!("Failed to decode ListOffsetsRequest: {}", e);
                        let error_response = KafkaResponse::Error {
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

            // Extract topics and partitions
            let topics: Vec<super::messages::ListOffsetsTopicData> = list_req
                .topics
                .into_iter()
                .map(|topic| {
                    let partitions = topic
                        .partitions
                        .into_iter()
                        .map(|partition| super::messages::ListOffsetsPartitionData {
                            partition_index: partition.partition_index,
                            current_leader_epoch: partition.current_leader_epoch,
                            timestamp: partition.timestamp,
                        })
                        .collect();

                    super::messages::ListOffsetsTopicData {
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
        API_KEY_DESCRIBE_GROUPS => {
            // DescribeGroups request - get consumer group state and members
            pg_log!(
                "Parsed DescribeGroups request (api_key={}, version={})",
                API_KEY_DESCRIBE_GROUPS,
                api_version
            );

            // Use kafka-protocol crate to decode DescribeGroupsRequest
            let describe_req =
                match kafka_protocol::messages::describe_groups_request::DescribeGroupsRequest::decode(
                    &mut payload_buf,
                    api_version,
                ) {
                    Ok(req) => req,
                    Err(e) => {
                        pg_warning!("Failed to decode DescribeGroupsRequest: {}", e);
                        let error_response = KafkaResponse::Error {
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
        API_KEY_LIST_GROUPS => {
            // ListGroups request - list all consumer groups
            pg_log!(
                "Parsed ListGroups request (api_key={}, version={})",
                API_KEY_LIST_GROUPS,
                api_version
            );

            // Use kafka-protocol crate to decode ListGroupsRequest
            let list_req =
                match kafka_protocol::messages::list_groups_request::ListGroupsRequest::decode(
                    &mut payload_buf,
                    api_version,
                ) {
                    Ok(req) => req,
                    Err(e) => {
                        pg_warning!("Failed to decode ListGroupsRequest: {}", e);
                        let error_response = KafkaResponse::Error {
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
        _ => {
            // Unsupported API
            pg_warning!("Unsupported API key: {}", api_key);

            // Send error response immediately
            let error_response = KafkaResponse::Error {
                correlation_id,
                error_code: ERROR_UNSUPPORTED_VERSION,
                error_message: Some(format!("Unsupported API key: {}", api_key)),
            };
            let _ = response_tx.send(error_response);

            Ok(None)
        }
    }
}

/// Parse RecordBatch into individual Records
///
/// RecordBatch format is complex (see Kafka protocol spec):
/// - Base offset (i64)
/// - Batch length (i32)
/// - Partition leader epoch (i32)
/// - Magic byte (i8 = 2 for v0.11+)
/// - CRC32C (u32)
/// - Attributes (i16) - compression, timestamp type
/// - Last offset delta (i32)
/// - Base timestamp (i64)
/// - Max timestamp (i64)
/// - Producer ID, epoch, base sequence (for idempotence)
/// - Records count (i32)
/// - Records (varint-encoded)
///
/// The kafka-protocol crate handles all this complexity for us.
fn parse_record_batch(batch_bytes: &bytes::Bytes) -> Result<Vec<super::messages::Record>> {
    use super::messages::{Record, RecordHeader};
    use bytes::Buf;

    // Handle empty batch
    if batch_bytes.is_empty() {
        return Ok(Vec::new());
    }

    pg_log!("parse_record_batch: received {} bytes", batch_bytes.len());
    pg_log!(
        "First 20 bytes: {:?}",
        &batch_bytes[..batch_bytes.len().min(20)]
    );

    let mut batch_buf = batch_bytes.clone();

    // Try RecordBatch v2 format first (Kafka 0.11+)
    // If that fails, fall back to MessageSet v0/v1 (legacy format)
    match RecordBatchDecoder::decode(&mut batch_buf) {
        Ok(record_set) => {
            // RecordSet contains a Vec<Record> in the .records field
            let mut records = Vec::new();
            for record in record_set.records {
                let key = record.key.map(|k: bytes::Bytes| k.to_vec());
                let value = record.value.map(|v: bytes::Bytes| v.to_vec());
                let timestamp = Some(record.timestamp);

                // Parse headers from IndexMap to Vec
                // Headers are IndexMap<StrBytes, Option<Bytes>>
                let headers: Vec<RecordHeader> = record
                    .headers
                    .into_iter()
                    .map(|(k, v)| RecordHeader {
                        key: k.to_string(),
                        value: v.map(|b: bytes::Bytes| b.to_vec()).unwrap_or_default(),
                    })
                    .collect();

                records.push(Record {
                    key,
                    value,
                    headers,
                    timestamp,
                });
            }
            Ok(records)
        }
        Err(e) => {
            // RecordBatch v2 decode failed, try MessageSet v0/v1 (legacy format)
            pg_log!(
                "RecordBatch decode failed ({}), trying MessageSet v0/v1 format",
                e
            );

            let mut buf = batch_bytes.clone();
            let mut records = Vec::new();

            // MessageSet format (v0/v1):
            // Repeated: [Offset: 8 bytes][MessageSize: 4 bytes][Message]
            // Message: [CRC: 4 bytes][Magic: 1 byte][Attributes: 1 byte][Key][Value]
            // Key/Value: [Length: 4 bytes (i32, -1=null)][Data]

            while buf.remaining() >= 12 {
                // Minimum: 8 (offset) + 4 (size)
                let _offset = buf.get_i64(); // Ignore offset (we assign our own)
                let message_size = buf.get_i32();

                if message_size < 0 || buf.remaining() < message_size as usize {
                    pg_warning!("Invalid message size in MessageSet: {}", message_size);
                    break;
                }

                // Parse the Message struct
                let _crc = buf.get_u32(); // Skip CRC validation for now
                let magic = buf.get_i8();
                let _attributes = buf.get_i8(); // Compression, timestamp type

                pg_log!("MessageSet magic byte: {}", magic);

                // Parse key (nullable bytes)
                let key = if buf.remaining() < 4 {
                    None
                } else {
                    let key_len = buf.get_i32();
                    if key_len < 0 {
                        None
                    } else if buf.remaining() >= key_len as usize {
                        let mut key_bytes = vec![0u8; key_len as usize];
                        buf.copy_to_slice(&mut key_bytes);
                        Some(key_bytes)
                    } else {
                        pg_warning!("Insufficient bytes for key");
                        break;
                    }
                };

                // Parse value (nullable bytes)
                let value = if buf.remaining() < 4 {
                    None
                } else {
                    let value_len = buf.get_i32();
                    if value_len < 0 {
                        None
                    } else if buf.remaining() >= value_len as usize {
                        let mut value_bytes = vec![0u8; value_len as usize];
                        buf.copy_to_slice(&mut value_bytes);
                        Some(value_bytes)
                    } else {
                        pg_warning!("Insufficient bytes for value");
                        break;
                    }
                };

                records.push(Record {
                    key,
                    value,
                    headers: Vec::new(), // MessageSet v0/v1 doesn't support headers
                    timestamp: None, // v0 doesn't have timestamps, v1 does but we skip for simplicity
                });
            }

            if records.is_empty() {
                Err(KafkaError::Encoding(format!(
                    "Failed to parse as both RecordBatch and MessageSet: {}",
                    e
                )))
            } else {
                pg_log!(
                    "Successfully parsed {} records from MessageSet format",
                    records.len()
                );
                Ok(records)
            }
        }
    }
}

/// Encode and send a Kafka response to a TCP socket
///
/// Kafka response format:
/// ```
/// [4 bytes: Size] [ResponseHeader] [ResponseBody]
/// ```
/// Encode a Kafka response into bytes
///
/// Returns the response payload (without size prefix, as LengthDelimitedCodec handles that)
pub fn encode_response(response: KafkaResponse) -> Result<BytesMut> {
    let mut response_buf = BytesMut::new();

    match response {
        KafkaResponse::ApiVersions {
            correlation_id,
            api_version,
            response: api_version_response,
        } => {
            // Build ResponseHeader
            let header = ResponseHeader::default().with_correlation_id(correlation_id);

            // Encode response header and body
            // IMPORTANT: Use the API version from the request for encoding
            // Different versions have different wire formats (e.g., flexible vs non-flexible)
            // CRITICAL: ApiVersions always uses ResponseHeader v0, even for v3+!
            // This is different from other APIs where v3+ uses ResponseHeader v1.
            // See: https://github.com/Baylox/kafka-mock (19-byte example for v3)
            let response_header_version = API_VERSIONS_RESPONSE_HEADER_VERSION;

            header.encode(&mut response_buf, response_header_version)?;
            api_version_response.encode(&mut response_buf, api_version)?;
        }
        KafkaResponse::Metadata {
            correlation_id,
            api_version,
            response: metadata_response,
        } => {
            // Build ResponseHeader
            let header = ResponseHeader::default().with_correlation_id(correlation_id);

            // Encode response header
            // Metadata v9+ uses flexible format (ResponseHeader v1)
            // Metadata v0-v8 uses non-flexible format (ResponseHeader v0)
            let response_header_version = if api_version >= FLEXIBLE_FORMAT_MIN_VERSION {
                1
            } else {
                0
            };
            header.encode(&mut response_buf, response_header_version)?;

            // Encode response body using the requested API version
            metadata_response.encode(&mut response_buf, api_version)?;
        }
        KafkaResponse::Produce {
            correlation_id,
            api_version,
            response: produce_response,
        } => {
            // Build ResponseHeader
            let header = ResponseHeader::default().with_correlation_id(correlation_id);

            // Encode response header
            // Produce v9+ uses flexible format (ResponseHeader v1)
            // Produce v0-v8 uses non-flexible format (ResponseHeader v0)
            let response_header_version = if api_version >= FLEXIBLE_FORMAT_MIN_VERSION {
                1
            } else {
                0
            };
            header.encode(&mut response_buf, response_header_version)?;

            // Encode response body using the requested API version
            produce_response.encode(&mut response_buf, api_version)?;
        }
        KafkaResponse::Fetch {
            correlation_id,
            api_version,
            response: fetch_response,
        } => {
            // Build ResponseHeader
            let header = ResponseHeader::default().with_correlation_id(correlation_id);

            // Encode response header
            // Fetch v12+ uses flexible format (ResponseHeader v1)
            // Fetch v0-v11 uses non-flexible format (ResponseHeader v0)
            let response_header_version = if api_version >= 12 { 1 } else { 0 };
            header.encode(&mut response_buf, response_header_version)?;

            // Encode response body using the requested API version
            fetch_response.encode(&mut response_buf, api_version)?;
        }
        KafkaResponse::OffsetCommit {
            correlation_id,
            api_version,
            response: commit_response,
        } => {
            // Build ResponseHeader
            let header = ResponseHeader::default().with_correlation_id(correlation_id);

            // Encode response header
            // OffsetCommit v8+ uses flexible format (ResponseHeader v1)
            // OffsetCommit v0-v7 uses non-flexible format (ResponseHeader v0)
            let response_header_version = if api_version >= 8 { 1 } else { 0 };
            header.encode(&mut response_buf, response_header_version)?;

            // Encode response body using the requested API version
            commit_response.encode(&mut response_buf, api_version)?;
        }
        KafkaResponse::OffsetFetch {
            correlation_id,
            api_version,
            response: fetch_response,
        } => {
            // Build ResponseHeader
            let header = ResponseHeader::default().with_correlation_id(correlation_id);

            // Encode response header
            // OffsetFetch v6+ uses flexible format (ResponseHeader v1)
            // OffsetFetch v0-v5 uses non-flexible format (ResponseHeader v0)
            let response_header_version = if api_version >= 6 { 1 } else { 0 };
            header.encode(&mut response_buf, response_header_version)?;

            // Encode response body using the requested API version
            fetch_response.encode(&mut response_buf, api_version)?;
        }
        KafkaResponse::FindCoordinator {
            correlation_id,
            api_version,
            response: coord_response,
        } => {
            // Build ResponseHeader
            let header = ResponseHeader::default().with_correlation_id(correlation_id);

            // Encode response header
            // FindCoordinator v3+ uses flexible format (ResponseHeader v1)
            // FindCoordinator v0-v2 uses non-flexible format (ResponseHeader v0)
            let response_header_version = if api_version >= 3 { 1 } else { 0 };
            header.encode(&mut response_buf, response_header_version)?;

            // Encode response body using the requested API version
            coord_response.encode(&mut response_buf, api_version)?;
        }
        KafkaResponse::JoinGroup {
            correlation_id,
            api_version,
            response: join_response,
        } => {
            // Build ResponseHeader
            let header = ResponseHeader::default().with_correlation_id(correlation_id);

            // Encode response header
            // JoinGroup v6+ uses flexible format (ResponseHeader v1)
            // JoinGroup v0-v5 uses non-flexible format (ResponseHeader v0)
            let response_header_version = if api_version >= 6 { 1 } else { 0 };
            header.encode(&mut response_buf, response_header_version)?;

            // Encode response body using the requested API version
            join_response.encode(&mut response_buf, api_version)?;
        }
        KafkaResponse::SyncGroup {
            correlation_id,
            api_version,
            response: sync_response,
        } => {
            // Build ResponseHeader
            let header = ResponseHeader::default().with_correlation_id(correlation_id);

            // Encode response header
            // SyncGroup v4+ uses flexible format (ResponseHeader v1)
            // SyncGroup v0-v3 uses non-flexible format (ResponseHeader v0)
            let response_header_version = if api_version >= 4 { 1 } else { 0 };
            header.encode(&mut response_buf, response_header_version)?;

            // Encode response body using the requested API version
            sync_response.encode(&mut response_buf, api_version)?;
        }
        KafkaResponse::Heartbeat {
            correlation_id,
            api_version,
            response: heartbeat_response,
        } => {
            // Build ResponseHeader
            let header = ResponseHeader::default().with_correlation_id(correlation_id);

            // Encode response header
            // Heartbeat v4+ uses flexible format (ResponseHeader v1)
            // Heartbeat v0-v3 uses non-flexible format (ResponseHeader v0)
            let response_header_version = if api_version >= 4 { 1 } else { 0 };
            header.encode(&mut response_buf, response_header_version)?;

            // Encode response body using the requested API version
            heartbeat_response.encode(&mut response_buf, api_version)?;
        }
        KafkaResponse::LeaveGroup {
            correlation_id,
            api_version,
            response: leave_response,
        } => {
            // Build ResponseHeader
            let header = ResponseHeader::default().with_correlation_id(correlation_id);

            // Encode response header
            // LeaveGroup v4+ uses flexible format (ResponseHeader v1)
            // LeaveGroup v0-v3 uses non-flexible format (ResponseHeader v0)
            let response_header_version = if api_version >= 4 { 1 } else { 0 };
            header.encode(&mut response_buf, response_header_version)?;

            // Encode response body using the requested API version
            leave_response.encode(&mut response_buf, api_version)?;
        }
        KafkaResponse::ListOffsets {
            correlation_id,
            api_version,
            response: list_response,
        } => {
            // Build ResponseHeader
            let header = ResponseHeader::default().with_correlation_id(correlation_id);

            // Encode response header
            // ListOffsets v6+ uses flexible format (ResponseHeader v1)
            // ListOffsets v0-v5 uses non-flexible format (ResponseHeader v0)
            let response_header_version = if api_version >= 6 { 1 } else { 0 };
            header.encode(&mut response_buf, response_header_version)?;

            // Encode response body using the requested API version
            list_response.encode(&mut response_buf, api_version)?;
        }
        KafkaResponse::DescribeGroups {
            correlation_id,
            api_version,
            response: describe_response,
        } => {
            // Build ResponseHeader
            let header = ResponseHeader::default().with_correlation_id(correlation_id);

            // Encode response header
            // DescribeGroups v5+ uses flexible format (ResponseHeader v1)
            // DescribeGroups v0-v4 uses non-flexible format (ResponseHeader v0)
            let response_header_version = if api_version >= 5 { 1 } else { 0 };
            header.encode(&mut response_buf, response_header_version)?;

            // Encode response body using the requested API version
            describe_response.encode(&mut response_buf, api_version)?;
        }
        KafkaResponse::ListGroups {
            correlation_id,
            api_version,
            response: list_groups_response,
        } => {
            // Build ResponseHeader
            let header = ResponseHeader::default().with_correlation_id(correlation_id);

            // Encode response header
            // ListGroups v3+ uses flexible format (ResponseHeader v1)
            // ListGroups v0-v2 uses non-flexible format (ResponseHeader v0)
            let response_header_version = if api_version >= 3 { 1 } else { 0 };
            header.encode(&mut response_buf, response_header_version)?;

            // Encode response body using the requested API version
            list_groups_response.encode(&mut response_buf, api_version)?;
        }
        KafkaResponse::Error {
            correlation_id,
            error_code,
            error_message,
        } => {
            // LIMITATION: Hand-rolled error response encoding
            // ===============================================
            // This is a simplified error response that may not match the expected format
            // for the specific API that triggered the error. Real Kafka clients might fail
            // to parse this if they expect a properly-formatted error response for their API.
            //
            // TODO(Phase 2): Match error response format to the specific API key that failed.
            // For example, if ApiVersions fails, return a proper ApiVersionsResponse with error_code set.
            // For now, this works with kcat which is lenient about error responses.
            let header = ResponseHeader::default().with_correlation_id(correlation_id);
            header.encode(&mut response_buf, 0)?;

            // Encode error code
            response_buf.put_i16(error_code);

            // Encode error message if present
            if let Some(msg) = error_message {
                response_buf.put_i16(msg.len() as i16);
                response_buf.put_slice(msg.as_bytes());
            } else {
                response_buf.put_i16(-1); // Null string
            }
        }
    }

    // Return the response payload
    // LengthDelimitedCodec will automatically add the 4-byte size prefix
    Ok(response_buf)
}
