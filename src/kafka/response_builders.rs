// Response builder utilities for kafka-protocol types
//
// This module provides helper functions to construct kafka-protocol response types.
// By centralizing this logic, we keep worker.rs clean and focused on business logic.

use kafka_protocol::messages::api_versions_response::{ApiVersion, ApiVersionsResponse};
use kafka_protocol::messages::metadata_response::{
    MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
};
use kafka_protocol::messages::produce_response::ProduceResponse;
use kafka_protocol::messages::{BrokerId, TopicName};

use super::constants::*;

/// Build an ApiVersionsResponse with the API versions we support
pub fn build_api_versions_response() -> ApiVersionsResponse {
    let mut response = ApiVersionsResponse::default();
    response.error_code = ERROR_NONE;
    response.throttle_time_ms = 0;

    // ApiVersions (API_KEY_API_VERSIONS): versions 0-3
    let mut av1 = ApiVersion::default();
    av1.api_key = API_KEY_API_VERSIONS;
    av1.min_version = 0;
    av1.max_version = 3;
    response.api_keys.push(av1);

    // Metadata (API_KEY_METADATA): versions 0-9
    let mut av2 = ApiVersion::default();
    av2.api_key = API_KEY_METADATA;
    av2.min_version = 0;
    av2.max_version = 9;
    response.api_keys.push(av2);

    // Produce (API_KEY_PRODUCE): versions 3-9
    // Note: We only support v3+ because it uses RecordBatch format.
    // v0-v2 use legacy MessageSet format which kafka-protocol crate doesn't support.
    let mut av3 = ApiVersion::default();
    av3.api_key = API_KEY_PRODUCE;
    av3.min_version = 3; // RecordBatch format only (v3+)
    av3.max_version = 9;
    response.api_keys.push(av3);

    // Fetch (API_KEY_FETCH): versions 0-13
    let mut av4 = ApiVersion::default();
    av4.api_key = API_KEY_FETCH;
    av4.min_version = 0;
    av4.max_version = 13;
    response.api_keys.push(av4);

    // ListOffsets (API_KEY_LIST_OFFSETS): versions 0-7
    let mut av5 = ApiVersion::default();
    av5.api_key = API_KEY_LIST_OFFSETS;
    av5.min_version = 0;
    av5.max_version = 7;
    response.api_keys.push(av5);

    // OffsetCommit (API_KEY_OFFSET_COMMIT): versions 0-8
    let mut av6 = ApiVersion::default();
    av6.api_key = API_KEY_OFFSET_COMMIT;
    av6.min_version = 0;
    av6.max_version = 8;
    response.api_keys.push(av6);

    // OffsetFetch (API_KEY_OFFSET_FETCH): versions 0-7
    // Note: v8+ changed response format to support multiple groups
    // We currently only support the single-group format (v0-7)
    let mut av7 = ApiVersion::default();
    av7.api_key = API_KEY_OFFSET_FETCH;
    av7.min_version = 0;
    av7.max_version = 7;
    response.api_keys.push(av7);

    // FindCoordinator (API_KEY_FIND_COORDINATOR): versions 0-3
    let mut av8 = ApiVersion::default();
    av8.api_key = API_KEY_FIND_COORDINATOR;
    av8.min_version = 0;
    av8.max_version = 3;
    response.api_keys.push(av8);

    // JoinGroup (API_KEY_JOIN_GROUP): versions 0-7
    let mut av9 = ApiVersion::default();
    av9.api_key = API_KEY_JOIN_GROUP;
    av9.min_version = 0;
    av9.max_version = 7;
    response.api_keys.push(av9);

    // Heartbeat (API_KEY_HEARTBEAT): versions 0-4
    let mut av10 = ApiVersion::default();
    av10.api_key = API_KEY_HEARTBEAT;
    av10.min_version = 0;
    av10.max_version = 4;
    response.api_keys.push(av10);

    // LeaveGroup (API_KEY_LEAVE_GROUP): versions 0-4
    let mut av11 = ApiVersion::default();
    av11.api_key = API_KEY_LEAVE_GROUP;
    av11.min_version = 0;
    av11.max_version = 4;
    response.api_keys.push(av11);

    // SyncGroup (API_KEY_SYNC_GROUP): versions 0-4
    let mut av12 = ApiVersion::default();
    av12.api_key = API_KEY_SYNC_GROUP;
    av12.min_version = 0;
    av12.max_version = 4;
    response.api_keys.push(av12);

    // DescribeGroups (API_KEY_DESCRIBE_GROUPS): versions 0-5
    let mut av13 = ApiVersion::default();
    av13.api_key = API_KEY_DESCRIBE_GROUPS;
    av13.min_version = 0;
    av13.max_version = 5;
    response.api_keys.push(av13);

    // ListGroups (API_KEY_LIST_GROUPS): versions 0-4
    let mut av14 = ApiVersion::default();
    av14.api_key = API_KEY_LIST_GROUPS;
    av14.min_version = 0;
    av14.max_version = 4;
    response.api_keys.push(av14);

    response
}

/// Build a MetadataResponseBroker for our single-node broker
pub fn build_broker_metadata(node_id: i32, host: String, port: i32) -> MetadataResponseBroker {
    let mut broker = MetadataResponseBroker::default();
    broker.node_id = BrokerId(node_id);
    broker.host = host.into();
    broker.port = port;
    broker.rack = None;
    broker
}

/// Build a MetadataResponseTopic with partitions
pub fn build_topic_metadata(
    topic_name: String,
    error_code: i16,
    partitions: Vec<MetadataResponsePartition>,
) -> MetadataResponseTopic {
    let mut topic = MetadataResponseTopic::default();
    topic.error_code = error_code;
    topic.name = Some(TopicName(topic_name.into()));
    topic.partitions = partitions;
    topic
}

/// Build a MetadataResponsePartition
pub fn build_partition_metadata(
    partition_index: i32,
    leader_id: i32,
    replica_nodes: Vec<i32>,
    isr_nodes: Vec<i32>,
) -> MetadataResponsePartition {
    let mut partition = MetadataResponsePartition::default();
    partition.error_code = ERROR_NONE;
    partition.partition_index = partition_index;
    partition.leader_id = BrokerId(leader_id);
    partition.replica_nodes = replica_nodes.into_iter().map(BrokerId).collect();
    partition.isr_nodes = isr_nodes.into_iter().map(BrokerId).collect();
    partition
}

/// Build an empty ProduceResponse (to be populated with topic responses)
pub fn build_produce_response() -> ProduceResponse {
    let mut response = ProduceResponse::default();
    response.throttle_time_ms = 0;
    response
}

/// Build an error MetadataResponse for protocol-level errors
pub fn build_metadata_error_response(
) -> kafka_protocol::messages::metadata_response::MetadataResponse {
    let mut response = kafka_protocol::messages::metadata_response::MetadataResponse::default();
    response.throttle_time_ms = 0;
    response.cluster_id = None;
    response.controller_id = BrokerId(-1);
    // Empty brokers and topics list for error response
    response
}
