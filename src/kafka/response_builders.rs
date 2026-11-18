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

/// Build an ApiVersionsResponse advertising every registered API (`crate::kafka::api::APIS`)
/// with its supported version range. The same ranges are enforced on decode (CONF-6), so
/// pg_kafka never accepts a version it does not advertise. The minimums follow the
/// kafka-protocol crate (it cannot decode or encode Fetch v0-3, ListOffsets v0,
/// OffsetCommit v0-1, OffsetFetch v0, CreateTopics v0-1, DeleteTopics v0 or
/// DescribeConfigs v0); `api::tests::every_advertised_version_encodes` keeps the registry and
/// the crate in step.
pub fn build_api_versions_response() -> ApiVersionsResponse {
    let mut response = ApiVersionsResponse::default();
    response.error_code = ERROR_NONE;
    response.throttle_time_ms = 0;

    for info in crate::kafka::api::APIS {
        let mut av = ApiVersion::default();
        av.api_key = info.key;
        av.min_version = info.min_version;
        av.max_version = info.max_version;
        response.api_keys.push(av);
    }

    response
}

/// Build a MetadataResponseBroker for our single-node broker
pub fn build_broker_metadata(node_id: i32, host: &str, port: i32) -> MetadataResponseBroker {
    let mut broker = MetadataResponseBroker::default();
    broker.node_id = BrokerId(node_id);
    broker.host = host.to_string().into();
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
// Builders for the APIs whose error lives in a top-level field. The full error
// response of every API (echoing requested topics/partitions/groups where the
// response has no top-level code) is `KafkaApi::error_response` in `crate::kafka::api`.

use kafka_protocol::messages::find_coordinator_response::FindCoordinatorResponse;
use kafka_protocol::messages::heartbeat_response::HeartbeatResponse;
use kafka_protocol::messages::init_producer_id_response::InitProducerIdResponse;
use kafka_protocol::messages::join_group_response::JoinGroupResponse;
use kafka_protocol::messages::leave_group_response::LeaveGroupResponse;
use kafka_protocol::messages::list_groups_response::ListGroupsResponse;
use kafka_protocol::messages::sync_group_response::SyncGroupResponse;

/// A ProduceResponse reporting `error_code` on every requested partition.
pub fn build_produce_partition_error_response(
    topic_data: &[crate::kafka::messages::TopicProduceData],
    error_code: i16,
) -> kafka_protocol::messages::produce_response::ProduceResponse {
    use kafka_protocol::messages::produce_response::{
        PartitionProduceResponse, TopicProduceResponse,
    };
    let mut response = build_produce_response();
    for topic in topic_data {
        let mut t = TopicProduceResponse::default();
        t.name = kafka_protocol::messages::TopicName(topic.name.clone().into());
        for p in &topic.partitions {
            let mut pr = PartitionProduceResponse::default();
            pr.index = p.partition_index;
            pr.error_code = error_code;
            pr.base_offset = -1;
            pr.log_append_time_ms = -1;
            pr.log_start_offset = -1;
            t.partition_responses.push(pr);
        }
        response.responses.push(t);
    }
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

/// Build a ListGroupsResponse with an error code
pub fn build_list_groups_error_response(error_code: i16) -> ListGroupsResponse {
    let mut response = ListGroupsResponse::default();
    response.throttle_time_ms = 0;
    response.error_code = error_code;
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

        // Should have 26 API versions (all supported APIs)
        assert_eq!(response.api_keys.len(), 26);

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
        assert_eq!(metadata.max_version, 10);

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

        // Verify Fetch entry — capped at v11. KIP-516 (Fetch v13) replaces the topic *name* with a
        // topic ID, which pg_kafka can't resolve (it keys topics by name); advertising v13 made
        // librdkafka >= 2.4 fetch by ID and get UNKNOWN_TOPIC_OR_PARTITION. v11 keeps the name.
        let fetch = response
            .api_keys
            .iter()
            .find(|a| a.api_key == API_KEY_FETCH)
            .expect("Fetch should be present");
        assert_eq!(fetch.min_version, 4); // crate minimum
        assert_eq!(fetch.max_version, 11);
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
        let broker = build_broker_metadata(0, "localhost", 9092);

        assert_eq!(broker.node_id.0, 0);
        assert_eq!(broker.host.as_str(), "localhost");
        assert_eq!(broker.port, 9092);
        assert!(broker.rack.is_none());
    }

    #[test]
    fn test_build_broker_metadata_with_custom_values() {
        let broker = build_broker_metadata(42, "kafka.example.com", 19092);

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
    fn test_build_list_groups_error_response() {
        let response = build_list_groups_error_response(ERROR_COORDINATOR_NOT_AVAILABLE);

        assert_eq!(response.throttle_time_ms, 0);
        assert_eq!(response.error_code, ERROR_COORDINATOR_NOT_AVAILABLE);
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
