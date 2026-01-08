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
