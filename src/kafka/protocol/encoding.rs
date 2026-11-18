// Response encoding module
//
// Handles encoding of Kafka responses into binary wire protocol format.

use bytes::BytesMut;

use super::super::error::Result;
use super::super::messages::KafkaResponse;

/// Encode a Kafka response into bytes
///
/// The response header version (v1 = flexible, with tagged fields) and the body encoding
/// both come from the API's kafka-protocol response type (see
/// `api::ResponseBody::encode_frame`); ApiVersions always uses header v0.
///
/// Returns the response payload (without size prefix, as LengthDelimitedCodec handles that)
pub fn encode_response(response: KafkaResponse) -> Result<BytesMut> {
    let mut response_buf = BytesMut::new();
    response.body.encode_frame(
        response.correlation_id,
        response.api_version,
        &mut response_buf,
    )?;
    // LengthDelimitedCodec will automatically add the 4-byte size prefix
    Ok(response_buf)
}

// ========== Unit Tests ==========

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::api::ResponseBody;
    use crate::kafka::messages::KafkaResponse;
    use kafka_protocol::messages::*;

    // ========== ApiVersions Response Tests ==========

    #[test]
    fn test_encode_api_versions_response() {
        let response = KafkaResponse::new(
            123,
            3,
            ResponseBody::ApiVersions(api_versions_response::ApiVersionsResponse::default()),
        );

        let encoded = encode_response(response).unwrap();
        assert!(!encoded.is_empty());
        // Header should contain correlation_id (4 bytes)
        assert!(encoded.len() >= 4);
    }

    #[test]
    fn test_encode_api_versions_response_v0() {
        let response = KafkaResponse::new(
            1,
            0,
            ResponseBody::ApiVersions(api_versions_response::ApiVersionsResponse::default()),
        );

        let encoded = encode_response(response).unwrap();
        assert!(!encoded.is_empty());
    }

    // ========== Metadata Response Tests ==========

    #[test]
    fn test_encode_metadata_response() {
        let response = KafkaResponse::new(
            456,
            9,
            ResponseBody::Metadata(metadata_response::MetadataResponse::default()),
        );

        let encoded = encode_response(response).unwrap();
        assert!(!encoded.is_empty());
    }

    #[test]
    fn test_encode_metadata_response_v0() {
        let response = KafkaResponse::new(
            1,
            0,
            ResponseBody::Metadata(metadata_response::MetadataResponse::default()),
        );

        let encoded = encode_response(response).unwrap();
        assert!(!encoded.is_empty());
    }

    // ========== Produce Response Tests ==========

    #[test]
    fn test_encode_produce_response() {
        let response = KafkaResponse::new(
            789,
            8,
            ResponseBody::Produce(produce_response::ProduceResponse::default()),
        );

        let encoded = encode_response(response).unwrap();
        assert!(!encoded.is_empty());
    }

    #[test]
    fn test_encode_produce_response_v3() {
        // Note: kafka-protocol crate doesn't support v0 for Produce, use v3+
        let response = KafkaResponse::new(
            1,
            3,
            ResponseBody::Produce(produce_response::ProduceResponse::default()),
        );

        let encoded = encode_response(response).unwrap();
        assert!(!encoded.is_empty());
    }

    // ========== Fetch Response Tests ==========

    #[test]
    fn test_encode_fetch_response() {
        let response = KafkaResponse::new(
            111,
            11,
            ResponseBody::Fetch(fetch_response::FetchResponse::default()),
        );

        let encoded = encode_response(response).unwrap();
        assert!(!encoded.is_empty());
    }

    #[test]
    fn test_encode_fetch_response_v4() {
        // Note: kafka-protocol crate doesn't support v0 for Fetch, use v4+
        let response = KafkaResponse::new(
            1,
            4,
            ResponseBody::Fetch(fetch_response::FetchResponse::default()),
        );

        let encoded = encode_response(response).unwrap();
        assert!(!encoded.is_empty());
    }

    // ========== OffsetCommit Response Tests ==========

    #[test]
    fn test_encode_offset_commit_response() {
        let response = KafkaResponse::new(
            222,
            8,
            ResponseBody::OffsetCommit(offset_commit_response::OffsetCommitResponse::default()),
        );

        let encoded = encode_response(response).unwrap();
        assert!(!encoded.is_empty());
    }

    // ========== OffsetFetch Response Tests ==========

    #[test]
    fn test_encode_offset_fetch_response() {
        let response = KafkaResponse::new(
            333,
            7,
            ResponseBody::OffsetFetch(offset_fetch_response::OffsetFetchResponse::default()),
        );

        let encoded = encode_response(response).unwrap();
        assert!(!encoded.is_empty());
    }

    // ========== FindCoordinator Response Tests ==========

    #[test]
    fn test_encode_find_coordinator_response() {
        let response = KafkaResponse::new(
            444,
            3,
            ResponseBody::FindCoordinator(
                find_coordinator_response::FindCoordinatorResponse::default(),
            ),
        );

        let encoded = encode_response(response).unwrap();
        assert!(!encoded.is_empty());
    }

    // ========== JoinGroup Response Tests ==========

    #[test]
    fn test_encode_join_group_response() {
        let response = KafkaResponse::new(
            555,
            7,
            ResponseBody::JoinGroup(join_group_response::JoinGroupResponse::default()),
        );

        let encoded = encode_response(response).unwrap();
        assert!(!encoded.is_empty());
    }

    // ========== SyncGroup Response Tests ==========

    #[test]
    fn test_encode_sync_group_response() {
        let response = KafkaResponse::new(
            666,
            5,
            ResponseBody::SyncGroup(sync_group_response::SyncGroupResponse::default()),
        );

        let encoded = encode_response(response).unwrap();
        assert!(!encoded.is_empty());
    }

    // ========== Heartbeat Response Tests ==========

    #[test]
    fn test_encode_heartbeat_response() {
        let response = KafkaResponse::new(
            777,
            4,
            ResponseBody::Heartbeat(heartbeat_response::HeartbeatResponse::default()),
        );

        let encoded = encode_response(response).unwrap();
        assert!(!encoded.is_empty());
    }

    // ========== LeaveGroup Response Tests ==========

    #[test]
    fn test_encode_leave_group_response() {
        let response = KafkaResponse::new(
            888,
            4,
            ResponseBody::LeaveGroup(leave_group_response::LeaveGroupResponse::default()),
        );

        let encoded = encode_response(response).unwrap();
        assert!(!encoded.is_empty());
    }

    // ========== ListOffsets Response Tests ==========

    #[test]
    fn test_encode_list_offsets_response() {
        let response = KafkaResponse::new(
            999,
            5,
            ResponseBody::ListOffsets(list_offsets_response::ListOffsetsResponse::default()),
        );

        let encoded = encode_response(response).unwrap();
        assert!(!encoded.is_empty());
    }

    // ========== DescribeGroups Response Tests ==========

    #[test]
    fn test_encode_describe_groups_response() {
        let response = KafkaResponse::new(
            1000,
            4,
            ResponseBody::DescribeGroups(
                describe_groups_response::DescribeGroupsResponse::default(),
            ),
        );

        let encoded = encode_response(response).unwrap();
        assert!(!encoded.is_empty());
    }

    // ========== ListGroups Response Tests ==========

    #[test]
    fn test_encode_list_groups_response() {
        let response = KafkaResponse::new(
            1001,
            4,
            ResponseBody::ListGroups(list_groups_response::ListGroupsResponse::default()),
        );

        let encoded = encode_response(response).unwrap();
        assert!(!encoded.is_empty());
    }

    // ========== CreateTopics Response Tests ==========

    #[test]
    fn test_encode_create_topics_response() {
        let response = KafkaResponse::new(
            1002,
            5,
            ResponseBody::CreateTopics(create_topics_response::CreateTopicsResponse::default()),
        );

        let encoded = encode_response(response).unwrap();
        assert!(!encoded.is_empty());
    }

    // ========== DeleteTopics Response Tests ==========

    #[test]
    fn test_encode_delete_topics_response() {
        let response = KafkaResponse::new(
            1003,
            5,
            ResponseBody::DeleteTopics(delete_topics_response::DeleteTopicsResponse::default()),
        );

        let encoded = encode_response(response).unwrap();
        assert!(!encoded.is_empty());
    }

    // ========== CreatePartitions Response Tests ==========

    #[test]
    fn test_encode_create_partitions_response() {
        let response = KafkaResponse::new(
            1004,
            3,
            ResponseBody::CreatePartitions(
                create_partitions_response::CreatePartitionsResponse::default(),
            ),
        );

        let encoded = encode_response(response).unwrap();
        assert!(!encoded.is_empty());
    }

    // ========== DeleteGroups Response Tests ==========

    #[test]
    fn test_encode_delete_groups_response() {
        let response = KafkaResponse::new(
            1005,
            2,
            ResponseBody::DeleteGroups(delete_groups_response::DeleteGroupsResponse::default()),
        );

        let encoded = encode_response(response).unwrap();
        assert!(!encoded.is_empty());
    }

    // ========== InitProducerId Response Tests ==========

    #[test]
    fn test_encode_init_producer_id_response() {
        let response = KafkaResponse::new(
            1006,
            4,
            ResponseBody::InitProducerId(
                init_producer_id_response::InitProducerIdResponse::default(),
            ),
        );

        let encoded = encode_response(response).unwrap();
        assert!(!encoded.is_empty());
    }

    // ========== Transaction API Response Tests (Phase 10) ==========

    #[test]
    fn test_encode_add_partitions_to_txn_response() {
        let response = KafkaResponse::new(
            1007,
            3,
            ResponseBody::AddPartitionsToTxn(
                add_partitions_to_txn_response::AddPartitionsToTxnResponse::default(),
            ),
        );

        let encoded = encode_response(response).unwrap();
        assert!(!encoded.is_empty());
    }

    #[test]
    fn test_encode_add_offsets_to_txn_response() {
        let response = KafkaResponse::new(
            1008,
            3,
            ResponseBody::AddOffsetsToTxn(
                add_offsets_to_txn_response::AddOffsetsToTxnResponse::default(),
            ),
        );

        let encoded = encode_response(response).unwrap();
        assert!(!encoded.is_empty());
    }

    #[test]
    fn test_encode_end_txn_response() {
        let response = KafkaResponse::new(
            1009,
            3,
            ResponseBody::EndTxn(end_txn_response::EndTxnResponse::default()),
        );

        let encoded = encode_response(response).unwrap();
        assert!(!encoded.is_empty());
    }

    #[test]
    fn test_encode_txn_offset_commit_response() {
        let response = KafkaResponse::new(
            1010,
            3,
            ResponseBody::TxnOffsetCommit(
                txn_offset_commit_response::TxnOffsetCommitResponse::default(),
            ),
        );

        let encoded = encode_response(response).unwrap();
        assert!(!encoded.is_empty());
    }

    // ========== Error Response Tests ==========

    #[test]
    fn test_encode_error_response_with_message() {
        let response = KafkaResponse::new(
            2000,
            0,
            ResponseBody::UnknownApi {
                error_code: crate::kafka::constants::ERROR_UNSUPPORTED_VERSION,
            },
        );

        let encoded = encode_response(response).unwrap();
        assert!(!encoded.is_empty());
        // Should contain correlation_id + error_code + message length + message
        assert!(encoded.len() > 6);
    }

    #[test]
    fn test_encode_error_response_without_message() {
        let response = KafkaResponse::new(
            2001,
            0,
            ResponseBody::UnknownApi {
                error_code: crate::kafka::constants::ERROR_UNKNOWN_TOPIC_OR_PARTITION,
            },
        );

        let encoded = encode_response(response).unwrap();
        assert!(!encoded.is_empty());
        // Should contain correlation_id + error_code + null string marker (-1)
        assert!(encoded.len() >= 6);
    }

    #[test]
    fn test_encode_error_response_corrupt_message() {
        let response = KafkaResponse::new(
            2002,
            0,
            ResponseBody::UnknownApi {
                error_code: crate::kafka::constants::ERROR_CORRUPT_MESSAGE,
            },
        );

        let encoded = encode_response(response).unwrap();
        assert!(!encoded.is_empty());
    }

    // ========== Correlation ID Verification Tests ==========

    #[test]
    fn test_encode_preserves_correlation_id() {
        // Different correlation IDs should produce different encoded outputs
        let response1 = KafkaResponse::new(
            12345,
            3,
            ResponseBody::ApiVersions(api_versions_response::ApiVersionsResponse::default()),
        );
        let response2 = KafkaResponse::new(
            54321,
            3,
            ResponseBody::ApiVersions(api_versions_response::ApiVersionsResponse::default()),
        );

        let encoded1 = encode_response(response1).unwrap();
        let encoded2 = encode_response(response2).unwrap();

        // Both should encode successfully
        assert!(!encoded1.is_empty());
        assert!(!encoded2.is_empty());
        // They should differ (different correlation IDs)
        assert_ne!(encoded1, encoded2);
    }

    // ========== API Version Variation Tests ==========

    #[test]
    fn test_encode_produce_multiple_versions() {
        // Test that different API versions produce valid encodings
        // Note: kafka-protocol crate supports Produce v3+
        for version in [3i16, 5, 7, 8] {
            let response = KafkaResponse::new(
                100,
                version,
                ResponseBody::Produce(produce_response::ProduceResponse::default()),
            );

            let encoded = encode_response(response).unwrap();
            assert!(!encoded.is_empty(), "Failed to encode Produce v{}", version);
        }
    }

    #[test]
    fn test_encode_fetch_multiple_versions() {
        // Note: kafka-protocol crate supports Fetch v4+
        for version in [4i16, 7, 11, 12] {
            let response = KafkaResponse::new(
                200,
                version,
                ResponseBody::Fetch(fetch_response::FetchResponse::default()),
            );

            let encoded = encode_response(response).unwrap();
            assert!(!encoded.is_empty(), "Failed to encode Fetch v{}", version);
        }
    }

    // ========== Config/Log-management Response Tests (API 21/32/44) ==========

    #[test]
    fn test_encode_delete_records_response() {
        let response = KafkaResponse::new(
            21,
            1,
            ResponseBody::DeleteRecords(delete_records_response::DeleteRecordsResponse::default()),
        );
        assert!(!encode_response(response).unwrap().is_empty());
    }

    #[test]
    fn test_encode_describe_configs_response() {
        // v4 is flexible (header v1); exercise both the encode arm and the
        // flexible-header path.
        let response = KafkaResponse::new(
            32,
            4,
            ResponseBody::DescribeConfigs(
                describe_configs_response::DescribeConfigsResponse::default(),
            ),
        );
        assert!(!encode_response(response).unwrap().is_empty());
    }

    #[test]
    fn test_encode_incremental_alter_configs_response() {
        let response = KafkaResponse::new(
            44,
            1,
            ResponseBody::IncrementalAlterConfigs(
                incremental_alter_configs_response::IncrementalAlterConfigsResponse::default(),
            ),
        );
        assert!(!encode_response(response).unwrap().is_empty());
    }
}
