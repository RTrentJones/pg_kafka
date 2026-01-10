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

    // ===== Phase 10: Transaction APIs =====

    // AddPartitionsToTxn (API_KEY_ADD_PARTITIONS_TO_TXN): versions 0-3
    let mut av20 = ApiVersion::default();
    av20.api_key = API_KEY_ADD_PARTITIONS_TO_TXN;
    av20.min_version = 0;
    av20.max_version = 3;
    response.api_keys.push(av20);

    // AddOffsetsToTxn (API_KEY_ADD_OFFSETS_TO_TXN): versions 0-3
    let mut av21 = ApiVersion::default();
    av21.api_key = API_KEY_ADD_OFFSETS_TO_TXN;
    av21.min_version = 0;
    av21.max_version = 3;
    response.api_keys.push(av21);

    // EndTxn (API_KEY_END_TXN): versions 0-3
    let mut av22 = ApiVersion::default();
    av22.api_key = API_KEY_END_TXN;
    av22.min_version = 0;
    av22.max_version = 3;
    response.api_keys.push(av22);

    // TxnOffsetCommit (API_KEY_TXN_OFFSET_COMMIT): versions 0-3
    let mut av23 = ApiVersion::default();
    av23.api_key = API_KEY_TXN_OFFSET_COMMIT;
    av23.min_version = 0;
    av23.max_version = 3;
    response.api_keys.push(av23);

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

#[cfg(test)]
mod tests {
    use super::*;

    // ========== API Versions Response Tests ==========

    #[test]
    fn test_build_api_versions_response() {
        let response = build_api_versions_response();

        assert_eq!(response.error_code, ERROR_NONE);
        assert_eq!(response.throttle_time_ms, 0);

        // Should have 23 API versions (all supported APIs)
        assert_eq!(response.api_keys.len(), 23);

        // Verify ApiVersions entry
        let api_versions = response
            .api_keys
            .iter()
            .find(|a| a.api_key == API_KEY_API_VERSIONS)
            .expect("ApiVersions should be present");
        assert_eq!(api_versions.min_version, 0);
        assert_eq!(api_versions.max_version, 3);

        // Verify Produce entry (should support v3+ only for RecordBatch)
        let produce = response
            .api_keys
            .iter()
            .find(|a| a.api_key == API_KEY_PRODUCE)
            .expect("Produce should be present");
        assert_eq!(produce.min_version, 3);
        assert_eq!(produce.max_version, 9);

        // Verify Metadata entry
        let metadata = response
            .api_keys
            .iter()
            .find(|a| a.api_key == API_KEY_METADATA)
            .expect("Metadata should be present");
        assert_eq!(metadata.min_version, 0);
        assert_eq!(metadata.max_version, 9);

        // Verify InitProducerId entry (Phase 9)
        let init_producer = response
            .api_keys
            .iter()
            .find(|a| a.api_key == API_KEY_INIT_PRODUCER_ID)
            .expect("InitProducerId should be present");
        assert_eq!(init_producer.min_version, 0);
        assert_eq!(init_producer.max_version, 4);

        // Verify transaction APIs (Phase 10)
        let add_partitions = response
            .api_keys
            .iter()
            .find(|a| a.api_key == API_KEY_ADD_PARTITIONS_TO_TXN)
            .expect("AddPartitionsToTxn should be present");
        assert_eq!(add_partitions.min_version, 0);
        assert_eq!(add_partitions.max_version, 3);
    }

    #[test]
    fn test_api_versions_includes_all_consumer_apis() {
        let response = build_api_versions_response();

        // Consumer group APIs
        let api_keys: Vec<i16> = response.api_keys.iter().map(|a| a.api_key).collect();

        assert!(api_keys.contains(&API_KEY_FIND_COORDINATOR));
        assert!(api_keys.contains(&API_KEY_JOIN_GROUP));
        assert!(api_keys.contains(&API_KEY_SYNC_GROUP));
        assert!(api_keys.contains(&API_KEY_HEARTBEAT));
        assert!(api_keys.contains(&API_KEY_LEAVE_GROUP));
        assert!(api_keys.contains(&API_KEY_OFFSET_COMMIT));
        assert!(api_keys.contains(&API_KEY_OFFSET_FETCH));
    }

    #[test]
    fn test_api_versions_includes_admin_apis() {
        let response = build_api_versions_response();

        let api_keys: Vec<i16> = response.api_keys.iter().map(|a| a.api_key).collect();

        assert!(api_keys.contains(&API_KEY_CREATE_TOPICS));
        assert!(api_keys.contains(&API_KEY_DELETE_TOPICS));
        assert!(api_keys.contains(&API_KEY_CREATE_PARTITIONS));
        assert!(api_keys.contains(&API_KEY_DELETE_GROUPS));
    }

    // ========== Metadata Builder Tests ==========

    #[test]
    fn test_build_broker_metadata() {
        let broker = build_broker_metadata(0, "localhost".to_string(), 9092);

        assert_eq!(broker.node_id.0, 0);
        assert_eq!(broker.host.as_str(), "localhost");
        assert_eq!(broker.port, 9092);
        assert!(broker.rack.is_none());
    }

    #[test]
    fn test_build_broker_metadata_with_custom_values() {
        let broker = build_broker_metadata(42, "kafka.example.com".to_string(), 19092);

        assert_eq!(broker.node_id.0, 42);
        assert_eq!(broker.host.as_str(), "kafka.example.com");
        assert_eq!(broker.port, 19092);
    }

    #[test]
    fn test_build_topic_metadata() {
        let partitions = vec![build_partition_metadata(0, 0, vec![0], vec![0])];
        let topic = build_topic_metadata("test-topic".to_string(), ERROR_NONE, partitions);

        assert_eq!(topic.error_code, ERROR_NONE);
        assert_eq!(topic.name.unwrap().0.as_str(), "test-topic");
        assert_eq!(topic.partitions.len(), 1);
    }

    #[test]
    fn test_build_topic_metadata_with_error() {
        let topic = build_topic_metadata(
            "unknown-topic".to_string(),
            ERROR_UNKNOWN_TOPIC_OR_PARTITION,
            vec![],
        );

        assert_eq!(topic.error_code, ERROR_UNKNOWN_TOPIC_OR_PARTITION);
        assert_eq!(topic.name.unwrap().0.as_str(), "unknown-topic");
        assert!(topic.partitions.is_empty());
    }

    #[test]
    fn test_build_partition_metadata() {
        let partition = build_partition_metadata(0, 1, vec![1, 2, 3], vec![1, 2]);

        assert_eq!(partition.error_code, ERROR_NONE);
        assert_eq!(partition.partition_index, 0);
        assert_eq!(partition.leader_id.0, 1);
        assert_eq!(partition.replica_nodes.len(), 3);
        assert_eq!(partition.isr_nodes.len(), 2);
    }

    #[test]
    fn test_build_partition_metadata_single_replica() {
        let partition = build_partition_metadata(5, 0, vec![0], vec![0]);

        assert_eq!(partition.partition_index, 5);
        assert_eq!(partition.leader_id.0, 0);
        assert_eq!(partition.replica_nodes.len(), 1);
        assert_eq!(partition.isr_nodes.len(), 1);
    }

    // ========== Produce Response Tests ==========

    #[test]
    fn test_build_produce_response() {
        let response = build_produce_response();

        assert_eq!(response.throttle_time_ms, 0);
        assert!(response.responses.is_empty());
    }

    #[test]
    fn test_build_produce_error_response() {
        let response = build_produce_error_response(ERROR_UNKNOWN_TOPIC_OR_PARTITION);

        // ProduceResponse doesn't have top-level error, so we just verify it's valid
        assert_eq!(response.throttle_time_ms, 0);
    }

    // ========== Fetch Response Tests ==========

    #[test]
    fn test_build_fetch_error_response() {
        let response = build_fetch_error_response(ERROR_UNKNOWN_TOPIC_OR_PARTITION);

        assert_eq!(response.throttle_time_ms, 0);
        assert_eq!(response.error_code, ERROR_UNKNOWN_TOPIC_OR_PARTITION);
    }

    #[test]
    fn test_build_fetch_error_response_none() {
        let response = build_fetch_error_response(ERROR_NONE);

        assert_eq!(response.error_code, ERROR_NONE);
    }

    // ========== Offset Commit/Fetch Response Tests ==========

    #[test]
    fn test_build_offset_commit_error_response() {
        let response = build_offset_commit_error_response(ERROR_NOT_COORDINATOR);

        assert_eq!(response.throttle_time_ms, 0);
    }

    #[test]
    fn test_build_offset_fetch_error_response() {
        let response = build_offset_fetch_error_response(ERROR_NOT_COORDINATOR);

        assert_eq!(response.throttle_time_ms, 0);
        assert_eq!(response.error_code, ERROR_NOT_COORDINATOR);
    }

    // ========== Coordinator Response Tests ==========

    #[test]
    fn test_build_find_coordinator_error_response() {
        let response = build_find_coordinator_error_response(ERROR_COORDINATOR_NOT_AVAILABLE);

        assert_eq!(response.throttle_time_ms, 0);
        assert_eq!(response.error_code, ERROR_COORDINATOR_NOT_AVAILABLE);
        assert_eq!(response.node_id.0, -1);
        assert_eq!(response.host.as_str(), "");
        assert_eq!(response.port, -1);
    }

    #[test]
    fn test_build_join_group_error_response() {
        let response = build_join_group_error_response(ERROR_UNKNOWN_MEMBER_ID);

        assert_eq!(response.throttle_time_ms, 0);
        assert_eq!(response.error_code, ERROR_UNKNOWN_MEMBER_ID);
        assert_eq!(response.generation_id, -1);
        assert!(response.protocol_type.is_none());
        assert!(response.protocol_name.is_none());
        assert_eq!(response.leader.as_str(), "");
        assert_eq!(response.member_id.as_str(), "");
    }

    #[test]
    fn test_build_sync_group_error_response() {
        let response = build_sync_group_error_response(ERROR_REBALANCE_IN_PROGRESS);

        assert_eq!(response.throttle_time_ms, 0);
        assert_eq!(response.error_code, ERROR_REBALANCE_IN_PROGRESS);
    }

    #[test]
    fn test_build_heartbeat_error_response() {
        let response = build_heartbeat_error_response(ERROR_REBALANCE_IN_PROGRESS);

        assert_eq!(response.throttle_time_ms, 0);
        assert_eq!(response.error_code, ERROR_REBALANCE_IN_PROGRESS);
    }

    #[test]
    fn test_build_leave_group_error_response() {
        let response = build_leave_group_error_response(ERROR_UNKNOWN_MEMBER_ID);

        assert_eq!(response.throttle_time_ms, 0);
        assert_eq!(response.error_code, ERROR_UNKNOWN_MEMBER_ID);
    }

    // ========== List/Describe Response Tests ==========

    #[test]
    fn test_build_list_offsets_error_response() {
        let response = build_list_offsets_error_response(ERROR_UNKNOWN_TOPIC_OR_PARTITION);

        assert_eq!(response.throttle_time_ms, 0);
    }

    #[test]
    fn test_build_describe_groups_error_response() {
        let response = build_describe_groups_error_response(ERROR_NOT_COORDINATOR);

        assert_eq!(response.throttle_time_ms, 0);
    }

    #[test]
    fn test_build_list_groups_error_response() {
        let response = build_list_groups_error_response(ERROR_COORDINATOR_NOT_AVAILABLE);

        assert_eq!(response.throttle_time_ms, 0);
        assert_eq!(response.error_code, ERROR_COORDINATOR_NOT_AVAILABLE);
    }

    // ========== Admin API Response Tests ==========

    #[test]
    fn test_build_create_topics_error_response() {
        let response = build_create_topics_error_response(ERROR_TOPIC_ALREADY_EXISTS);

        assert_eq!(response.throttle_time_ms, 0);
    }

    #[test]
    fn test_build_delete_topics_error_response() {
        let response = build_delete_topics_error_response(ERROR_UNKNOWN_TOPIC_OR_PARTITION);

        assert_eq!(response.throttle_time_ms, 0);
    }

    #[test]
    fn test_build_create_partitions_error_response() {
        let response = build_create_partitions_error_response(ERROR_INVALID_PARTITIONS);

        assert_eq!(response.throttle_time_ms, 0);
    }

    #[test]
    fn test_build_delete_groups_error_response() {
        let response = build_delete_groups_error_response(ERROR_NON_EMPTY_GROUP);

        assert_eq!(response.throttle_time_ms, 0);
    }

    // ========== Idempotent Producer Response Tests ==========

    #[test]
    fn test_build_init_producer_id_error_response() {
        let response = build_init_producer_id_error_response(ERROR_PRODUCER_FENCED);

        assert_eq!(response.throttle_time_ms, 0);
        assert_eq!(response.error_code, ERROR_PRODUCER_FENCED);
        assert_eq!(response.producer_id.0, -1);
        assert_eq!(response.producer_epoch, -1);
    }

    #[test]
    fn test_build_init_producer_id_error_response_concurrent_transactions() {
        let response = build_init_producer_id_error_response(ERROR_CONCURRENT_TRANSACTIONS);

        assert_eq!(response.error_code, ERROR_CONCURRENT_TRANSACTIONS);
    }

    // ========== Metadata Error Response Tests ==========

    #[test]
    fn test_build_metadata_error_response() {
        let response = build_metadata_error_response();

        assert_eq!(response.throttle_time_ms, 0);
        assert!(response.cluster_id.is_none());
        assert_eq!(response.controller_id.0, -1);
        assert!(response.brokers.is_empty());
        assert!(response.topics.is_empty());
    }
}
