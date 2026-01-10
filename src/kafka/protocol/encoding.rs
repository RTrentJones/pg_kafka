// Response encoding module
//
// Handles encoding of Kafka responses into binary wire protocol format.

use bytes::{BufMut, BytesMut};
use kafka_protocol::messages::ResponseHeader;
use kafka_protocol::protocol::Encodable;

use super::super::constants;
use super::super::constants::*;
use super::super::error::Result;
use super::super::messages::KafkaResponse;

/// Helper macro for encoding standard Kafka responses.
///
/// All standard API responses follow the same pattern:
/// 1. Create ResponseHeader with correlation_id
/// 2. Look up header version based on API key and version
/// 3. Encode header, then encode body
macro_rules! encode_standard_response {
    ($buf:expr, $corr_id:expr, $api_ver:expr, $api_key:expr, $body:expr) => {{
        let header = ResponseHeader::default().with_correlation_id($corr_id);
        let header_version = constants::get_response_header_version($api_key, $api_ver);
        header.encode($buf, header_version)?;
        $body.encode($buf, $api_ver)?;
    }};
}

/// Encode a Kafka response into bytes
///
/// Returns the response payload (without size prefix, as LengthDelimitedCodec handles that)
pub fn encode_response(response: KafkaResponse) -> Result<BytesMut> {
    let mut response_buf = BytesMut::new();

    match response {
        KafkaResponse::ApiVersions {
            correlation_id,
            api_version,
            response: body,
        } => encode_standard_response!(
            &mut response_buf,
            correlation_id,
            api_version,
            API_KEY_API_VERSIONS,
            body
        ),

        KafkaResponse::Metadata {
            correlation_id,
            api_version,
            response: body,
        } => encode_standard_response!(
            &mut response_buf,
            correlation_id,
            api_version,
            API_KEY_METADATA,
            body
        ),

        KafkaResponse::Produce {
            correlation_id,
            api_version,
            response: body,
        } => encode_standard_response!(
            &mut response_buf,
            correlation_id,
            api_version,
            API_KEY_PRODUCE,
            body
        ),

        KafkaResponse::Fetch {
            correlation_id,
            api_version,
            response: body,
        } => encode_standard_response!(
            &mut response_buf,
            correlation_id,
            api_version,
            API_KEY_FETCH,
            body
        ),

        KafkaResponse::OffsetCommit {
            correlation_id,
            api_version,
            response: body,
        } => encode_standard_response!(
            &mut response_buf,
            correlation_id,
            api_version,
            API_KEY_OFFSET_COMMIT,
            body
        ),

        KafkaResponse::OffsetFetch {
            correlation_id,
            api_version,
            response: body,
        } => encode_standard_response!(
            &mut response_buf,
            correlation_id,
            api_version,
            API_KEY_OFFSET_FETCH,
            body
        ),

        KafkaResponse::FindCoordinator {
            correlation_id,
            api_version,
            response: body,
        } => encode_standard_response!(
            &mut response_buf,
            correlation_id,
            api_version,
            API_KEY_FIND_COORDINATOR,
            body
        ),

        KafkaResponse::JoinGroup {
            correlation_id,
            api_version,
            response: body,
        } => encode_standard_response!(
            &mut response_buf,
            correlation_id,
            api_version,
            API_KEY_JOIN_GROUP,
            body
        ),

        KafkaResponse::SyncGroup {
            correlation_id,
            api_version,
            response: body,
        } => encode_standard_response!(
            &mut response_buf,
            correlation_id,
            api_version,
            API_KEY_SYNC_GROUP,
            body
        ),

        KafkaResponse::Heartbeat {
            correlation_id,
            api_version,
            response: body,
        } => encode_standard_response!(
            &mut response_buf,
            correlation_id,
            api_version,
            API_KEY_HEARTBEAT,
            body
        ),

        KafkaResponse::LeaveGroup {
            correlation_id,
            api_version,
            response: body,
        } => encode_standard_response!(
            &mut response_buf,
            correlation_id,
            api_version,
            API_KEY_LEAVE_GROUP,
            body
        ),

        KafkaResponse::ListOffsets {
            correlation_id,
            api_version,
            response: body,
        } => encode_standard_response!(
            &mut response_buf,
            correlation_id,
            api_version,
            API_KEY_LIST_OFFSETS,
            body
        ),

        KafkaResponse::DescribeGroups {
            correlation_id,
            api_version,
            response: body,
        } => encode_standard_response!(
            &mut response_buf,
            correlation_id,
            api_version,
            API_KEY_DESCRIBE_GROUPS,
            body
        ),

        KafkaResponse::ListGroups {
            correlation_id,
            api_version,
            response: body,
        } => encode_standard_response!(
            &mut response_buf,
            correlation_id,
            api_version,
            API_KEY_LIST_GROUPS,
            body
        ),

        KafkaResponse::CreateTopics {
            correlation_id,
            api_version,
            response: body,
        } => encode_standard_response!(
            &mut response_buf,
            correlation_id,
            api_version,
            API_KEY_CREATE_TOPICS,
            body
        ),

        KafkaResponse::DeleteTopics {
            correlation_id,
            api_version,
            response: body,
        } => encode_standard_response!(
            &mut response_buf,
            correlation_id,
            api_version,
            API_KEY_DELETE_TOPICS,
            body
        ),

        KafkaResponse::CreatePartitions {
            correlation_id,
            api_version,
            response: body,
        } => encode_standard_response!(
            &mut response_buf,
            correlation_id,
            api_version,
            API_KEY_CREATE_PARTITIONS,
            body
        ),

        KafkaResponse::DeleteGroups {
            correlation_id,
            api_version,
            response: body,
        } => encode_standard_response!(
            &mut response_buf,
            correlation_id,
            api_version,
            API_KEY_DELETE_GROUPS,
            body
        ),

        KafkaResponse::InitProducerId {
            correlation_id,
            api_version,
            response: body,
        } => encode_standard_response!(
            &mut response_buf,
            correlation_id,
            api_version,
            API_KEY_INIT_PRODUCER_ID,
            body
        ),

        // Phase 10: Transaction API responses
        KafkaResponse::AddPartitionsToTxn {
            correlation_id,
            api_version,
            response: body,
        } => encode_standard_response!(
            &mut response_buf,
            correlation_id,
            api_version,
            crate::kafka::constants::API_KEY_ADD_PARTITIONS_TO_TXN,
            body
        ),

        KafkaResponse::AddOffsetsToTxn {
            correlation_id,
            api_version,
            response: body,
        } => encode_standard_response!(
            &mut response_buf,
            correlation_id,
            api_version,
            crate::kafka::constants::API_KEY_ADD_OFFSETS_TO_TXN,
            body
        ),

        KafkaResponse::EndTxn {
            correlation_id,
            api_version,
            response: body,
        } => encode_standard_response!(
            &mut response_buf,
            correlation_id,
            api_version,
            crate::kafka::constants::API_KEY_END_TXN,
            body
        ),

        KafkaResponse::TxnOffsetCommit {
            correlation_id,
            api_version,
            response: body,
        } => encode_standard_response!(
            &mut response_buf,
            correlation_id,
            api_version,
            crate::kafka::constants::API_KEY_TXN_OFFSET_COMMIT,
            body
        ),

        KafkaResponse::Error {
            correlation_id,
            error_code,
            error_message,
        } => {
            // Protocol/Decoding Error Response
            // =================================
            // This variant is used ONLY for protocol-level errors that occur during request
            // decoding, BEFORE we know which API is being requested. Examples:
            // - Invalid request header
            // - Unsupported API key
            // - Decode failures before API type is determined
            //
            // For handler-level errors (after we know the API type), use the appropriate
            // API-specific error response via the dispatch mechanism in worker.rs.
            //
            // Note: This hand-rolled format works with lenient clients like kcat, but strict
            // clients may fail to parse it. This is acceptable since these are protocol errors
            // that shouldn't happen in normal operation.
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

// ========== Unit Tests ==========

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::messages::KafkaResponse;
    use kafka_protocol::messages::*;

    // ========== ApiVersions Response Tests ==========

    #[test]
    fn test_encode_api_versions_response() {
        let response = KafkaResponse::ApiVersions {
            correlation_id: 123,
            api_version: 3,
            response: api_versions_response::ApiVersionsResponse::default(),
        };

        let encoded = encode_response(response).unwrap();
        assert!(!encoded.is_empty());
        // Header should contain correlation_id (4 bytes)
        assert!(encoded.len() >= 4);
    }

    #[test]
    fn test_encode_api_versions_response_v0() {
        let response = KafkaResponse::ApiVersions {
            correlation_id: 1,
            api_version: 0,
            response: api_versions_response::ApiVersionsResponse::default(),
        };

        let encoded = encode_response(response).unwrap();
        assert!(!encoded.is_empty());
    }

    // ========== Metadata Response Tests ==========

    #[test]
    fn test_encode_metadata_response() {
        let response = KafkaResponse::Metadata {
            correlation_id: 456,
            api_version: 9,
            response: metadata_response::MetadataResponse::default(),
        };

        let encoded = encode_response(response).unwrap();
        assert!(!encoded.is_empty());
    }

    #[test]
    fn test_encode_metadata_response_v0() {
        let response = KafkaResponse::Metadata {
            correlation_id: 1,
            api_version: 0,
            response: metadata_response::MetadataResponse::default(),
        };

        let encoded = encode_response(response).unwrap();
        assert!(!encoded.is_empty());
    }

    // ========== Produce Response Tests ==========

    #[test]
    fn test_encode_produce_response() {
        let response = KafkaResponse::Produce {
            correlation_id: 789,
            api_version: 8,
            response: produce_response::ProduceResponse::default(),
        };

        let encoded = encode_response(response).unwrap();
        assert!(!encoded.is_empty());
    }

    #[test]
    fn test_encode_produce_response_v3() {
        // Note: kafka-protocol crate doesn't support v0 for Produce, use v3+
        let response = KafkaResponse::Produce {
            correlation_id: 1,
            api_version: 3,
            response: produce_response::ProduceResponse::default(),
        };

        let encoded = encode_response(response).unwrap();
        assert!(!encoded.is_empty());
    }

    // ========== Fetch Response Tests ==========

    #[test]
    fn test_encode_fetch_response() {
        let response = KafkaResponse::Fetch {
            correlation_id: 111,
            api_version: 11,
            response: fetch_response::FetchResponse::default(),
        };

        let encoded = encode_response(response).unwrap();
        assert!(!encoded.is_empty());
    }

    #[test]
    fn test_encode_fetch_response_v4() {
        // Note: kafka-protocol crate doesn't support v0 for Fetch, use v4+
        let response = KafkaResponse::Fetch {
            correlation_id: 1,
            api_version: 4,
            response: fetch_response::FetchResponse::default(),
        };

        let encoded = encode_response(response).unwrap();
        assert!(!encoded.is_empty());
    }

    // ========== OffsetCommit Response Tests ==========

    #[test]
    fn test_encode_offset_commit_response() {
        let response = KafkaResponse::OffsetCommit {
            correlation_id: 222,
            api_version: 8,
            response: offset_commit_response::OffsetCommitResponse::default(),
        };

        let encoded = encode_response(response).unwrap();
        assert!(!encoded.is_empty());
    }

    // ========== OffsetFetch Response Tests ==========

    #[test]
    fn test_encode_offset_fetch_response() {
        let response = KafkaResponse::OffsetFetch {
            correlation_id: 333,
            api_version: 7,
            response: offset_fetch_response::OffsetFetchResponse::default(),
        };

        let encoded = encode_response(response).unwrap();
        assert!(!encoded.is_empty());
    }

    // ========== FindCoordinator Response Tests ==========

    #[test]
    fn test_encode_find_coordinator_response() {
        let response = KafkaResponse::FindCoordinator {
            correlation_id: 444,
            api_version: 3,
            response: find_coordinator_response::FindCoordinatorResponse::default(),
        };

        let encoded = encode_response(response).unwrap();
        assert!(!encoded.is_empty());
    }

    // ========== JoinGroup Response Tests ==========

    #[test]
    fn test_encode_join_group_response() {
        let response = KafkaResponse::JoinGroup {
            correlation_id: 555,
            api_version: 7,
            response: join_group_response::JoinGroupResponse::default(),
        };

        let encoded = encode_response(response).unwrap();
        assert!(!encoded.is_empty());
    }

    // ========== SyncGroup Response Tests ==========

    #[test]
    fn test_encode_sync_group_response() {
        let response = KafkaResponse::SyncGroup {
            correlation_id: 666,
            api_version: 5,
            response: sync_group_response::SyncGroupResponse::default(),
        };

        let encoded = encode_response(response).unwrap();
        assert!(!encoded.is_empty());
    }

    // ========== Heartbeat Response Tests ==========

    #[test]
    fn test_encode_heartbeat_response() {
        let response = KafkaResponse::Heartbeat {
            correlation_id: 777,
            api_version: 4,
            response: heartbeat_response::HeartbeatResponse::default(),
        };

        let encoded = encode_response(response).unwrap();
        assert!(!encoded.is_empty());
    }

    // ========== LeaveGroup Response Tests ==========

    #[test]
    fn test_encode_leave_group_response() {
        let response = KafkaResponse::LeaveGroup {
            correlation_id: 888,
            api_version: 4,
            response: leave_group_response::LeaveGroupResponse::default(),
        };

        let encoded = encode_response(response).unwrap();
        assert!(!encoded.is_empty());
    }

    // ========== ListOffsets Response Tests ==========

    #[test]
    fn test_encode_list_offsets_response() {
        let response = KafkaResponse::ListOffsets {
            correlation_id: 999,
            api_version: 5,
            response: list_offsets_response::ListOffsetsResponse::default(),
        };

        let encoded = encode_response(response).unwrap();
        assert!(!encoded.is_empty());
    }

    // ========== DescribeGroups Response Tests ==========

    #[test]
    fn test_encode_describe_groups_response() {
        let response = KafkaResponse::DescribeGroups {
            correlation_id: 1000,
            api_version: 4,
            response: describe_groups_response::DescribeGroupsResponse::default(),
        };

        let encoded = encode_response(response).unwrap();
        assert!(!encoded.is_empty());
    }

    // ========== ListGroups Response Tests ==========

    #[test]
    fn test_encode_list_groups_response() {
        let response = KafkaResponse::ListGroups {
            correlation_id: 1001,
            api_version: 4,
            response: list_groups_response::ListGroupsResponse::default(),
        };

        let encoded = encode_response(response).unwrap();
        assert!(!encoded.is_empty());
    }

    // ========== CreateTopics Response Tests ==========

    #[test]
    fn test_encode_create_topics_response() {
        let response = KafkaResponse::CreateTopics {
            correlation_id: 1002,
            api_version: 5,
            response: create_topics_response::CreateTopicsResponse::default(),
        };

        let encoded = encode_response(response).unwrap();
        assert!(!encoded.is_empty());
    }

    // ========== DeleteTopics Response Tests ==========

    #[test]
    fn test_encode_delete_topics_response() {
        let response = KafkaResponse::DeleteTopics {
            correlation_id: 1003,
            api_version: 5,
            response: delete_topics_response::DeleteTopicsResponse::default(),
        };

        let encoded = encode_response(response).unwrap();
        assert!(!encoded.is_empty());
    }

    // ========== CreatePartitions Response Tests ==========

    #[test]
    fn test_encode_create_partitions_response() {
        let response = KafkaResponse::CreatePartitions {
            correlation_id: 1004,
            api_version: 3,
            response: create_partitions_response::CreatePartitionsResponse::default(),
        };

        let encoded = encode_response(response).unwrap();
        assert!(!encoded.is_empty());
    }

    // ========== DeleteGroups Response Tests ==========

    #[test]
    fn test_encode_delete_groups_response() {
        let response = KafkaResponse::DeleteGroups {
            correlation_id: 1005,
            api_version: 2,
            response: delete_groups_response::DeleteGroupsResponse::default(),
        };

        let encoded = encode_response(response).unwrap();
        assert!(!encoded.is_empty());
    }

    // ========== InitProducerId Response Tests ==========

    #[test]
    fn test_encode_init_producer_id_response() {
        let response = KafkaResponse::InitProducerId {
            correlation_id: 1006,
            api_version: 4,
            response: init_producer_id_response::InitProducerIdResponse::default(),
        };

        let encoded = encode_response(response).unwrap();
        assert!(!encoded.is_empty());
    }

    // ========== Transaction API Response Tests (Phase 10) ==========

    #[test]
    fn test_encode_add_partitions_to_txn_response() {
        let response = KafkaResponse::AddPartitionsToTxn {
            correlation_id: 1007,
            api_version: 3,
            response: add_partitions_to_txn_response::AddPartitionsToTxnResponse::default(),
        };

        let encoded = encode_response(response).unwrap();
        assert!(!encoded.is_empty());
    }

    #[test]
    fn test_encode_add_offsets_to_txn_response() {
        let response = KafkaResponse::AddOffsetsToTxn {
            correlation_id: 1008,
            api_version: 3,
            response: add_offsets_to_txn_response::AddOffsetsToTxnResponse::default(),
        };

        let encoded = encode_response(response).unwrap();
        assert!(!encoded.is_empty());
    }

    #[test]
    fn test_encode_end_txn_response() {
        let response = KafkaResponse::EndTxn {
            correlation_id: 1009,
            api_version: 3,
            response: end_txn_response::EndTxnResponse::default(),
        };

        let encoded = encode_response(response).unwrap();
        assert!(!encoded.is_empty());
    }

    #[test]
    fn test_encode_txn_offset_commit_response() {
        let response = KafkaResponse::TxnOffsetCommit {
            correlation_id: 1010,
            api_version: 3,
            response: txn_offset_commit_response::TxnOffsetCommitResponse::default(),
        };

        let encoded = encode_response(response).unwrap();
        assert!(!encoded.is_empty());
    }

    // ========== Error Response Tests ==========

    #[test]
    fn test_encode_error_response_with_message() {
        let response = KafkaResponse::Error {
            correlation_id: 2000,
            error_code: crate::kafka::constants::ERROR_UNSUPPORTED_VERSION,
            error_message: Some("Unsupported API".to_string()),
        };

        let encoded = encode_response(response).unwrap();
        assert!(!encoded.is_empty());
        // Should contain correlation_id + error_code + message length + message
        assert!(encoded.len() > 6);
    }

    #[test]
    fn test_encode_error_response_without_message() {
        let response = KafkaResponse::Error {
            correlation_id: 2001,
            error_code: crate::kafka::constants::ERROR_UNKNOWN_TOPIC_OR_PARTITION,
            error_message: None,
        };

        let encoded = encode_response(response).unwrap();
        assert!(!encoded.is_empty());
        // Should contain correlation_id + error_code + null string marker (-1)
        assert!(encoded.len() >= 6);
    }

    #[test]
    fn test_encode_error_response_corrupt_message() {
        let response = KafkaResponse::Error {
            correlation_id: 2002,
            error_code: crate::kafka::constants::ERROR_CORRUPT_MESSAGE,
            error_message: Some("Malformed request".to_string()),
        };

        let encoded = encode_response(response).unwrap();
        assert!(!encoded.is_empty());
    }

    // ========== Correlation ID Verification Tests ==========

    #[test]
    fn test_encode_preserves_correlation_id() {
        // Different correlation IDs should produce different encoded outputs
        let response1 = KafkaResponse::ApiVersions {
            correlation_id: 12345,
            api_version: 3,
            response: api_versions_response::ApiVersionsResponse::default(),
        };
        let response2 = KafkaResponse::ApiVersions {
            correlation_id: 54321,
            api_version: 3,
            response: api_versions_response::ApiVersionsResponse::default(),
        };

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
            let response = KafkaResponse::Produce {
                correlation_id: 100,
                api_version: version,
                response: produce_response::ProduceResponse::default(),
            };

            let encoded = encode_response(response).unwrap();
            assert!(!encoded.is_empty(), "Failed to encode Produce v{}", version);
        }
    }

    #[test]
    fn test_encode_fetch_multiple_versions() {
        // Note: kafka-protocol crate supports Fetch v4+
        for version in [4i16, 7, 11, 12] {
            let response = KafkaResponse::Fetch {
                correlation_id: 200,
                api_version: version,
                response: fetch_response::FetchResponse::default(),
            };

            let encoded = encode_response(response).unwrap();
            assert!(!encoded.is_empty(), "Failed to encode Fetch v{}", version);
        }
    }
}
