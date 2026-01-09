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

    // CreateTopics (API_KEY_CREATE_TOPICS): versions 0-5
    let mut av15 = ApiVersion::default();
    av15.api_key = API_KEY_CREATE_TOPICS;
    av15.min_version = 0;
    av15.max_version = 5;
    response.api_keys.push(av15);

    // DeleteTopics (API_KEY_DELETE_TOPICS): versions 0-4
    let mut av16 = ApiVersion::default();
    av16.api_key = API_KEY_DELETE_TOPICS;
    av16.min_version = 0;
    av16.max_version = 4;
    response.api_keys.push(av16);

    // CreatePartitions (API_KEY_CREATE_PARTITIONS): versions 0-2
    let mut av17 = ApiVersion::default();
    av17.api_key = API_KEY_CREATE_PARTITIONS;
    av17.min_version = 0;
    av17.max_version = 2;
    response.api_keys.push(av17);

    // DeleteGroups (API_KEY_DELETE_GROUPS): versions 0-2
    let mut av18 = ApiVersion::default();
    av18.api_key = API_KEY_DELETE_GROUPS;
    av18.min_version = 0;
    av18.max_version = 2;
    response.api_keys.push(av18);

    // InitProducerId (API_KEY_INIT_PRODUCER_ID): versions 0-4 (Phase 9)
    let mut av19 = ApiVersion::default();
    av19.api_key = API_KEY_INIT_PRODUCER_ID;
    av19.min_version = 0;
    av19.max_version = 4;
    response.api_keys.push(av19);

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

// ========== Error Response Builders ==========
// These functions build API-specific error responses with the error code
// embedded in the correct location for each Kafka API.

use kafka_protocol::messages::create_partitions_response::CreatePartitionsResponse;
use kafka_protocol::messages::create_topics_response::CreateTopicsResponse;
use kafka_protocol::messages::delete_groups_response::DeleteGroupsResponse;
use kafka_protocol::messages::delete_topics_response::DeleteTopicsResponse;
use kafka_protocol::messages::describe_groups_response::DescribeGroupsResponse;
use kafka_protocol::messages::fetch_response::FetchResponse;
use kafka_protocol::messages::find_coordinator_response::FindCoordinatorResponse;
use kafka_protocol::messages::heartbeat_response::HeartbeatResponse;
use kafka_protocol::messages::init_producer_id_response::InitProducerIdResponse;
use kafka_protocol::messages::join_group_response::JoinGroupResponse;
use kafka_protocol::messages::leave_group_response::LeaveGroupResponse;
use kafka_protocol::messages::list_groups_response::ListGroupsResponse;
use kafka_protocol::messages::list_offsets_response::ListOffsetsResponse;
use kafka_protocol::messages::offset_commit_response::OffsetCommitResponse;
use kafka_protocol::messages::offset_fetch_response::OffsetFetchResponse;
use kafka_protocol::messages::sync_group_response::SyncGroupResponse;

/// Build a ProduceResponse with an error code (top-level error, no topic data)
pub fn build_produce_error_response(_error_code: i16) -> ProduceResponse {
    // ProduceResponse doesn't have a top-level error_code field.
    // Errors are per-topic/partition. Return empty response for protocol-level errors.
    let mut response = ProduceResponse::default();
    response.throttle_time_ms = 0;
    response
}

/// Build a FetchResponse with an error code
pub fn build_fetch_error_response(error_code: i16) -> FetchResponse {
    let mut response = FetchResponse::default();
    response.throttle_time_ms = 0;
    response.error_code = error_code;
    response
}

/// Build an OffsetCommitResponse with an error code (empty topics)
pub fn build_offset_commit_error_response(_error_code: i16) -> OffsetCommitResponse {
    // OffsetCommitResponse doesn't have a top-level error_code.
    // Errors are per-topic/partition. Return empty response.
    let mut response = OffsetCommitResponse::default();
    response.throttle_time_ms = 0;
    response
}

/// Build an OffsetFetchResponse with an error code
pub fn build_offset_fetch_error_response(error_code: i16) -> OffsetFetchResponse {
    let mut response = OffsetFetchResponse::default();
    response.throttle_time_ms = 0;
    response.error_code = error_code;
    response
}

/// Build a FindCoordinatorResponse with an error code
pub fn build_find_coordinator_error_response(error_code: i16) -> FindCoordinatorResponse {
    let mut response = FindCoordinatorResponse::default();
    response.throttle_time_ms = 0;
    response.error_code = error_code;
    response.node_id = BrokerId(-1);
    response.host = "".into();
    response.port = -1;
    response
}

/// Build a JoinGroupResponse with an error code
pub fn build_join_group_error_response(error_code: i16) -> JoinGroupResponse {
    let mut response = JoinGroupResponse::default();
    response.throttle_time_ms = 0;
    response.error_code = error_code;
    response.generation_id = -1;
    response.protocol_type = None;
    response.protocol_name = None;
    response.leader = "".into();
    response.member_id = "".into();
    response
}

/// Build a SyncGroupResponse with an error code
pub fn build_sync_group_error_response(error_code: i16) -> SyncGroupResponse {
    let mut response = SyncGroupResponse::default();
    response.throttle_time_ms = 0;
    response.error_code = error_code;
    response
}

/// Build a HeartbeatResponse with an error code
pub fn build_heartbeat_error_response(error_code: i16) -> HeartbeatResponse {
    let mut response = HeartbeatResponse::default();
    response.throttle_time_ms = 0;
    response.error_code = error_code;
    response
}

/// Build a LeaveGroupResponse with an error code
pub fn build_leave_group_error_response(error_code: i16) -> LeaveGroupResponse {
    let mut response = LeaveGroupResponse::default();
    response.throttle_time_ms = 0;
    response.error_code = error_code;
    response
}

/// Build a ListOffsetsResponse with an error code (empty topics)
pub fn build_list_offsets_error_response(_error_code: i16) -> ListOffsetsResponse {
    // ListOffsetsResponse doesn't have a top-level error_code.
    // Errors are per-topic/partition. Return empty response.
    let mut response = ListOffsetsResponse::default();
    response.throttle_time_ms = 0;
    response
}

/// Build a DescribeGroupsResponse with an error code (empty groups)
pub fn build_describe_groups_error_response(_error_code: i16) -> DescribeGroupsResponse {
    // DescribeGroupsResponse errors are per-group. Return empty response.
    let mut response = DescribeGroupsResponse::default();
    response.throttle_time_ms = 0;
    response
}

/// Build a ListGroupsResponse with an error code
pub fn build_list_groups_error_response(error_code: i16) -> ListGroupsResponse {
    let mut response = ListGroupsResponse::default();
    response.throttle_time_ms = 0;
    response.error_code = error_code;
    response
}

/// Build a CreateTopicsResponse with an error code (empty topics)
pub fn build_create_topics_error_response(_error_code: i16) -> CreateTopicsResponse {
    // CreateTopicsResponse errors are per-topic. Return empty response.
    let mut response = CreateTopicsResponse::default();
    response.throttle_time_ms = 0;
    response
}

/// Build a DeleteTopicsResponse with an error code (empty responses)
pub fn build_delete_topics_error_response(_error_code: i16) -> DeleteTopicsResponse {
    // DeleteTopicsResponse errors are per-topic. Return empty response.
    let mut response = DeleteTopicsResponse::default();
    response.throttle_time_ms = 0;
    response
}

/// Build a CreatePartitionsResponse with an error code (empty results)
pub fn build_create_partitions_error_response(_error_code: i16) -> CreatePartitionsResponse {
    // CreatePartitionsResponse errors are per-topic. Return empty response.
    let mut response = CreatePartitionsResponse::default();
    response.throttle_time_ms = 0;
    response
}

/// Build a DeleteGroupsResponse with an error code (empty results)
pub fn build_delete_groups_error_response(_error_code: i16) -> DeleteGroupsResponse {
    // DeleteGroupsResponse errors are per-group. Return empty response.
    let mut response = DeleteGroupsResponse::default();
    response.throttle_time_ms = 0;
    response
}

/// Build an InitProducerIdResponse with an error code (Phase 9)
pub fn build_init_producer_id_error_response(error_code: i16) -> InitProducerIdResponse {
    use kafka_protocol::messages::ProducerId;
    let mut response = InitProducerIdResponse::default();
    response.throttle_time_ms = 0;
    response.error_code = error_code;
    response.producer_id = ProducerId(-1);
    response.producer_epoch = -1;
    response
}
