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
    av3.min_version = 3;  // RecordBatch format only (v3+)
    av3.max_version = 9;
    response.api_keys.push(av3);

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
pub fn build_metadata_error_response() -> kafka_protocol::messages::metadata_response::MetadataResponse {
    let mut response = kafka_protocol::messages::metadata_response::MetadataResponse::default();
    response.throttle_time_ms = 0;
    response.cluster_id = None;
    response.controller_id = BrokerId(-1);
    // Empty brokers and topics list for error response
    response
}
