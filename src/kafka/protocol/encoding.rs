//! Response encoding: a [`KafkaResponse`] to the bytes of one frame.

use bytes::BytesMut;

use crate::kafka::error::Result;
use crate::kafka::messages::KafkaResponse;

/// Encode `response` as a frame payload: response header, then body, without the length
/// prefix (the frame codec adds it).
///
/// The response header version (v1 = flexible, with tagged fields) and the body encoding
/// both come from the API's kafka-protocol response type (see
/// `api::ResponseBody::encode_frame`).
#[expect(
    clippy::needless_pass_by_value,
    reason = "a response is sent once: encoding consumes it so the caller cannot send it again"
)]
pub fn encode_response(response: KafkaResponse) -> Result<BytesMut> {
    let mut buf = BytesMut::new();
    response
        .body
        .encode_frame(response.correlation_id, response.api_version, &mut buf)?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::api::ResponseBody;
    use crate::kafka::constants::{
        ERROR_CORRUPT_MESSAGE, ERROR_UNKNOWN_TOPIC_OR_PARTITION, ERROR_UNSUPPORTED_VERSION,
    };
    use kafka_protocol::messages::*;

    /// Encode `body` at `version` and check the frame starts with `correlation_id`.
    fn encode(correlation_id: i32, version: i16, body: ResponseBody) -> BytesMut {
        let encoded = encode_response(KafkaResponse::new(correlation_id, version, body))
            .unwrap_or_else(|e| panic!("v{version} does not encode: {e}"));
        assert_eq!(
            encoded[..4],
            correlation_id.to_be_bytes(),
            "v{version}: the frame must start with the correlation id"
        );
        encoded
    }

    /// The default response of every worker API, at versions that span the non-flexible and
    /// flexible encodings (the kafka-protocol crate starts Produce at v3 and Fetch at v4).
    #[test]
    fn test_encode_every_api_response() {
        let cases: Vec<(i16, ResponseBody)> = vec![
            (0, ResponseBody::ApiVersions(ApiVersionsResponse::default())),
            (3, ResponseBody::ApiVersions(ApiVersionsResponse::default())),
            (0, ResponseBody::Metadata(MetadataResponse::default())),
            (9, ResponseBody::Metadata(MetadataResponse::default())),
            (3, ResponseBody::Produce(ProduceResponse::default())),
            (8, ResponseBody::Produce(ProduceResponse::default())),
            (4, ResponseBody::Fetch(FetchResponse::default())),
            (11, ResponseBody::Fetch(FetchResponse::default())),
            (
                8,
                ResponseBody::OffsetCommit(OffsetCommitResponse::default()),
            ),
            (7, ResponseBody::OffsetFetch(OffsetFetchResponse::default())),
            (
                3,
                ResponseBody::FindCoordinator(FindCoordinatorResponse::default()),
            ),
            (7, ResponseBody::JoinGroup(JoinGroupResponse::default())),
            (5, ResponseBody::SyncGroup(SyncGroupResponse::default())),
            (4, ResponseBody::Heartbeat(HeartbeatResponse::default())),
            (4, ResponseBody::LeaveGroup(LeaveGroupResponse::default())),
            (5, ResponseBody::ListOffsets(ListOffsetsResponse::default())),
            (
                4,
                ResponseBody::DescribeGroups(DescribeGroupsResponse::default()),
            ),
            (4, ResponseBody::ListGroups(ListGroupsResponse::default())),
            (
                5,
                ResponseBody::CreateTopics(CreateTopicsResponse::default()),
            ),
            (
                5,
                ResponseBody::DeleteTopics(DeleteTopicsResponse::default()),
            ),
            (
                3,
                ResponseBody::CreatePartitions(CreatePartitionsResponse::default()),
            ),
            (
                2,
                ResponseBody::DeleteGroups(DeleteGroupsResponse::default()),
            ),
            (
                4,
                ResponseBody::InitProducerId(InitProducerIdResponse::default()),
            ),
            (
                3,
                ResponseBody::AddPartitionsToTxn(AddPartitionsToTxnResponse::default()),
            ),
            (
                3,
                ResponseBody::AddOffsetsToTxn(AddOffsetsToTxnResponse::default()),
            ),
            (3, ResponseBody::EndTxn(EndTxnResponse::default())),
            (
                3,
                ResponseBody::TxnOffsetCommit(TxnOffsetCommitResponse::default()),
            ),
            (
                1,
                ResponseBody::DeleteRecords(DeleteRecordsResponse::default()),
            ),
            (
                4,
                ResponseBody::DescribeConfigs(DescribeConfigsResponse::default()),
            ),
            (
                1,
                ResponseBody::IncrementalAlterConfigs(IncrementalAlterConfigsResponse::default()),
            ),
        ];
        for (correlation_id, (version, body)) in (100..).zip(cases) {
            let name = format!("{body:?}");
            let encoded = encode(correlation_id, version, body);
            assert!(encoded.len() > 4, "{name}: empty body");
        }
    }

    #[test]
    fn test_encode_produce_multiple_versions() {
        for version in [3i16, 5, 7, 8] {
            encode(
                100,
                version,
                ResponseBody::Produce(ProduceResponse::default()),
            );
        }
    }

    #[test]
    fn test_encode_fetch_multiple_versions() {
        for version in [4i16, 7, 11, 12] {
            encode(200, version, ResponseBody::Fetch(FetchResponse::default()));
        }
    }

    /// The generic error frame for an unknown API: response header v0 (correlation id),
    /// error code, null error message.
    #[test]
    fn test_encode_unknown_api_frame() {
        for error_code in [
            ERROR_UNSUPPORTED_VERSION,
            ERROR_UNKNOWN_TOPIC_OR_PARTITION,
            ERROR_CORRUPT_MESSAGE,
        ] {
            let encoded = encode(2000, 0, ResponseBody::UnknownApi { error_code });
            assert_eq!(encoded[4..6], error_code.to_be_bytes());
            assert_eq!(encoded[6..], (-1i16).to_be_bytes(), "null error message");
        }
    }

    #[test]
    fn test_encode_preserves_correlation_id() {
        let first = encode(
            12345,
            3,
            ResponseBody::ApiVersions(ApiVersionsResponse::default()),
        );
        let second = encode(
            54321,
            3,
            ResponseBody::ApiVersions(ApiVersionsResponse::default()),
        );
        assert_ne!(first, second);
    }

    #[test]
    fn test_close_connection_is_not_a_frame() {
        assert!(encode_response(KafkaResponse::new(1, 0, ResponseBody::CloseConnection)).is_err());
    }
}
