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

/// The single source of truth for the request versions pg_kafka supports, as
/// `(api_key, min_version, max_version)`. This table is used both to *advertise* supported versions
/// in the ApiVersions response and to *enforce* them on inbound requests (CONF-6, see
/// `decoding::parse_request_inner`) — keeping the two in lockstep so we never accept a version we
/// don't advertise.
///
/// Notable caps:
/// - Produce min 3: we only support the RecordBatch format (v3+); v0-2 use the legacy MessageSet
///   format the kafka-protocol crate doesn't decode.
/// - Fetch max 11: KIP-516 (v13) drops the topic *name* in favour of topic IDs, but pg_kafka
///   resolves topics by name only, so a v13 client would get UNKNOWN_TOPIC_OR_PARTITION. v11 is the
///   highest version that still carries the topic name.
/// - OffsetFetch max 7: v8+ changed the response shape to support multiple groups, which we don't
///   implement yet.
/// - Metadata max 10: v10 (KIP-516) added a per-topic `topic_id` to the *response*; pg_kafka resolves
///   by name and leaves it as the zero UUID, which by-name clients ignore. We advertise (and serve)
///   v10 because clients with an explicit broker version that don't down-negotiate — notably Sarama
///   `V2_8_0_0` — send Metadata v10 unconditionally; rejecting it broke their bootstrap (the rejection
///   is an error frame strict decoders can't parse). v11+ adds fields/semantics we don't serve.
pub const API_VERSION_RANGES: &[(i16, i16, i16)] = &[
    (API_KEY_API_VERSIONS, 0, 3),
    (API_KEY_METADATA, 0, 10),
    (API_KEY_PRODUCE, 3, 9),
    (API_KEY_FETCH, 0, 11),
    (API_KEY_LIST_OFFSETS, 0, 7),
    (API_KEY_OFFSET_COMMIT, 0, 8),
    (API_KEY_OFFSET_FETCH, 0, 7),
    (API_KEY_FIND_COORDINATOR, 0, 3),
    (API_KEY_JOIN_GROUP, 0, 7),
    (API_KEY_HEARTBEAT, 0, 4),
    (API_KEY_LEAVE_GROUP, 0, 4),
    (API_KEY_SYNC_GROUP, 0, 4),
    (API_KEY_DESCRIBE_GROUPS, 0, 5),
    (API_KEY_LIST_GROUPS, 0, 4),
    (API_KEY_CREATE_TOPICS, 0, 5),
    (API_KEY_DELETE_TOPICS, 0, 4),
    (API_KEY_CREATE_PARTITIONS, 0, 2),
    (API_KEY_DELETE_GROUPS, 0, 2),
    (API_KEY_INIT_PRODUCER_ID, 0, 4),
    (API_KEY_ADD_PARTITIONS_TO_TXN, 0, 3),
    (API_KEY_ADD_OFFSETS_TO_TXN, 0, 3),
    (API_KEY_END_TXN, 0, 3),
    (API_KEY_TXN_OFFSET_COMMIT, 0, 3),
];

/// Return the supported `(min_version, max_version)` for an API key, or `None` if the key is not
/// one pg_kafka handles. Backs CONF-6 request-version enforcement.
pub fn supported_version_range(api_key: i16) -> Option<(i16, i16)> {
    API_VERSION_RANGES
        .iter()
        .find(|(key, _, _)| *key == api_key)
        .map(|(_, min, max)| (*min, *max))
}

/// Build an ApiVersionsResponse advertising the versions in [`API_VERSION_RANGES`].
pub fn build_api_versions_response() -> ApiVersionsResponse {
    let mut response = ApiVersionsResponse::default();
    response.error_code = ERROR_NONE;
    response.throttle_time_ms = 0;

    for &(api_key, min_version, max_version) in API_VERSION_RANGES {
        let mut av = ApiVersion::default();
        av.api_key = api_key;
        av.min_version = min_version;
        av.max_version = max_version;
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

/// Build a *decodable*, API-specific error response for a request whose `api_key` is known.
///
/// Wraps the matching `build_*_error_response` builder in the corresponding [`KafkaResponse`] variant,
/// tagged with the request's `api_version` so the existing encoder writes it at the version the client
/// sent. The protocol layer prefers this over the hand-rolled `KafkaResponse::Error` frame, which uses
/// a fixed v0 header that strict clients (e.g. Sarama) mis-decode and panic on. Used by the
/// request-version-range rejection (CONF-6) and the per-API decode-failure arms in `decoding.rs`.
///
/// Returns `None` for an `api_key` with no typed error builder — ApiVersions (which never reaches the
/// error path) and the transaction APIs — so the caller falls back to the generic frame, which is then
/// only reachable on a truly-unknown `api_key` or a builder-less API's malformed-input path.
pub fn error_response_for(
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    error_code: i16,
) -> Option<crate::kafka::messages::KafkaResponse> {
    use crate::kafka::messages::KafkaResponse;
    let response = match api_key {
        API_KEY_METADATA => KafkaResponse::Metadata {
            correlation_id,
            api_version,
            response: build_metadata_error_response(),
        },
        API_KEY_PRODUCE => KafkaResponse::Produce {
            correlation_id,
            api_version,
            response: build_produce_error_response(error_code),
        },
        API_KEY_FETCH => KafkaResponse::Fetch {
            correlation_id,
            api_version,
            response: build_fetch_error_response(error_code),
        },
        API_KEY_LIST_OFFSETS => KafkaResponse::ListOffsets {
            correlation_id,
            api_version,
            response: build_list_offsets_error_response(error_code),
        },
        API_KEY_OFFSET_COMMIT => KafkaResponse::OffsetCommit {
            correlation_id,
            api_version,
            response: build_offset_commit_error_response(error_code),
        },
        API_KEY_OFFSET_FETCH => KafkaResponse::OffsetFetch {
            correlation_id,
            api_version,
            response: build_offset_fetch_error_response(error_code),
        },
        API_KEY_FIND_COORDINATOR => KafkaResponse::FindCoordinator {
            correlation_id,
            api_version,
            response: build_find_coordinator_error_response(error_code),
        },
        API_KEY_JOIN_GROUP => KafkaResponse::JoinGroup {
            correlation_id,
            api_version,
            response: build_join_group_error_response(error_code),
        },
        API_KEY_SYNC_GROUP => KafkaResponse::SyncGroup {
            correlation_id,
            api_version,
            response: build_sync_group_error_response(error_code),
        },
        API_KEY_HEARTBEAT => KafkaResponse::Heartbeat {
            correlation_id,
            api_version,
            response: build_heartbeat_error_response(error_code),
        },
        API_KEY_LEAVE_GROUP => KafkaResponse::LeaveGroup {
            correlation_id,
            api_version,
            response: build_leave_group_error_response(error_code),
        },
        API_KEY_DESCRIBE_GROUPS => KafkaResponse::DescribeGroups {
            correlation_id,
            api_version,
            response: build_describe_groups_error_response(error_code),
        },
        API_KEY_LIST_GROUPS => KafkaResponse::ListGroups {
            correlation_id,
            api_version,
            response: build_list_groups_error_response(error_code),
        },
        API_KEY_CREATE_TOPICS => KafkaResponse::CreateTopics {
            correlation_id,
            api_version,
            response: build_create_topics_error_response(error_code),
        },
        API_KEY_DELETE_TOPICS => KafkaResponse::DeleteTopics {
            correlation_id,
            api_version,
            response: build_delete_topics_error_response(error_code),
        },
        API_KEY_CREATE_PARTITIONS => KafkaResponse::CreatePartitions {
            correlation_id,
            api_version,
            response: build_create_partitions_error_response(error_code),
        },
        API_KEY_DELETE_GROUPS => KafkaResponse::DeleteGroups {
            correlation_id,
            api_version,
            response: build_delete_groups_error_response(error_code),
        },
        API_KEY_INIT_PRODUCER_ID => KafkaResponse::InitProducerId {
            correlation_id,
            api_version,
            response: build_init_producer_id_error_response(error_code),
        },
        _ => return None,
    };
    Some(response)
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
        assert_eq!(fetch.min_version, 0);
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

    #[test]
    fn test_supported_version_range_matches_advertised() {
        // The enforcement table (CONF-6) must agree with what ApiVersions advertises.
        assert_eq!(supported_version_range(API_KEY_PRODUCE), Some((3, 9)));
        assert_eq!(supported_version_range(API_KEY_FETCH), Some((0, 11)));
        assert_eq!(supported_version_range(API_KEY_METADATA), Some((0, 10)));
        assert_eq!(supported_version_range(API_KEY_OFFSET_FETCH), Some((0, 7)));

        // Every advertised API has a matching enforcement range, and vice versa.
        let advertised = build_api_versions_response();
        for entry in &advertised.api_keys {
            assert_eq!(
                supported_version_range(entry.api_key),
                Some((entry.min_version, entry.max_version)),
                "api_key {} advertised but enforcement range differs",
                entry.api_key
            );
        }

        // An API key pg_kafka doesn't handle has no range (so enforcement skips it and the
        // dispatcher's unsupported-api-key path handles it instead).
        assert_eq!(supported_version_range(17), None); // SaslHandshake — not implemented
        assert_eq!(supported_version_range(9999), None);
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

    // ========== error_response_for dispatcher (decodable protocol errors) ==========

    #[test]
    fn test_error_response_for_known_api_is_typed_and_versioned() {
        use crate::kafka::messages::KafkaResponse;
        // Fetch has a top-level error_code → it is carried through, tagged with the request version.
        match error_response_for(API_KEY_FETCH, 11, 42, ERROR_UNSUPPORTED_VERSION) {
            Some(KafkaResponse::Fetch {
                correlation_id,
                api_version,
                response,
            }) => {
                assert_eq!(correlation_id, 42);
                assert_eq!(api_version, 11);
                assert_eq!(response.error_code, ERROR_UNSUPPORTED_VERSION);
            }
            _ => panic!("expected a typed Fetch error response"),
        }
        // Metadata carries no top-level error_code, but still maps to a decodable Metadata variant.
        assert!(matches!(
            error_response_for(API_KEY_METADATA, 10, 1, ERROR_UNSUPPORTED_VERSION),
            Some(KafkaResponse::Metadata {
                api_version: 10,
                ..
            })
        ));
    }

    #[test]
    fn test_error_response_for_builderless_or_unknown_api_is_none() {
        // Transaction APIs and unknown keys have no typed error builder → None, so the protocol
        // layer falls back to the generic frame (its only remaining use).
        assert!(error_response_for(API_KEY_END_TXN, 3, 1, ERROR_UNSUPPORTED_VERSION).is_none());
        assert!(error_response_for(9999, 0, 1, ERROR_UNSUPPORTED_VERSION).is_none());
    }
}
