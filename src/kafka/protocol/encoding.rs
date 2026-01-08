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

/// Encode and send a Kafka response to a TCP socket
///
/// Kafka response format:
/// ```text
/// [4 bytes: Size] [ResponseHeader] [ResponseBody]
/// ```
/// Encode a Kafka response into bytes
///
/// Returns the response payload (without size prefix, as LengthDelimitedCodec handles that)
pub fn encode_response(response: KafkaResponse) -> Result<BytesMut> {
    let mut response_buf = BytesMut::new();

    match response {
        KafkaResponse::ApiVersions {
            correlation_id,
            api_version,
            response: api_version_response,
        } => {
            let header = ResponseHeader::default().with_correlation_id(correlation_id);
            let response_header_version =
                constants::get_response_header_version(API_KEY_API_VERSIONS, api_version);
            header.encode(&mut response_buf, response_header_version)?;
            api_version_response.encode(&mut response_buf, api_version)?;
        }
        KafkaResponse::Metadata {
            correlation_id,
            api_version,
            response: metadata_response,
        } => {
            let header = ResponseHeader::default().with_correlation_id(correlation_id);
            let response_header_version =
                constants::get_response_header_version(API_KEY_METADATA, api_version);
            header.encode(&mut response_buf, response_header_version)?;
            metadata_response.encode(&mut response_buf, api_version)?;
        }
        KafkaResponse::Produce {
            correlation_id,
            api_version,
            response: produce_response,
        } => {
            let header = ResponseHeader::default().with_correlation_id(correlation_id);
            let response_header_version =
                constants::get_response_header_version(API_KEY_PRODUCE, api_version);
            header.encode(&mut response_buf, response_header_version)?;
            produce_response.encode(&mut response_buf, api_version)?;
        }
        KafkaResponse::Fetch {
            correlation_id,
            api_version,
            response: fetch_response,
        } => {
            let header = ResponseHeader::default().with_correlation_id(correlation_id);
            let response_header_version =
                constants::get_response_header_version(API_KEY_FETCH, api_version);
            header.encode(&mut response_buf, response_header_version)?;
            fetch_response.encode(&mut response_buf, api_version)?;
        }
        KafkaResponse::OffsetCommit {
            correlation_id,
            api_version,
            response: commit_response,
        } => {
            let header = ResponseHeader::default().with_correlation_id(correlation_id);
            let response_header_version =
                constants::get_response_header_version(API_KEY_OFFSET_COMMIT, api_version);
            header.encode(&mut response_buf, response_header_version)?;
            commit_response.encode(&mut response_buf, api_version)?;
        }
        KafkaResponse::OffsetFetch {
            correlation_id,
            api_version,
            response: fetch_response,
        } => {
            let header = ResponseHeader::default().with_correlation_id(correlation_id);
            let response_header_version =
                constants::get_response_header_version(API_KEY_OFFSET_FETCH, api_version);
            header.encode(&mut response_buf, response_header_version)?;
            fetch_response.encode(&mut response_buf, api_version)?;
        }
        KafkaResponse::FindCoordinator {
            correlation_id,
            api_version,
            response: coord_response,
        } => {
            let header = ResponseHeader::default().with_correlation_id(correlation_id);
            let response_header_version =
                constants::get_response_header_version(API_KEY_FIND_COORDINATOR, api_version);
            header.encode(&mut response_buf, response_header_version)?;
            coord_response.encode(&mut response_buf, api_version)?;
        }
        KafkaResponse::JoinGroup {
            correlation_id,
            api_version,
            response: join_response,
        } => {
            let header = ResponseHeader::default().with_correlation_id(correlation_id);
            let response_header_version =
                constants::get_response_header_version(API_KEY_JOIN_GROUP, api_version);
            header.encode(&mut response_buf, response_header_version)?;
            join_response.encode(&mut response_buf, api_version)?;
        }
        KafkaResponse::SyncGroup {
            correlation_id,
            api_version,
            response: sync_response,
        } => {
            let header = ResponseHeader::default().with_correlation_id(correlation_id);
            let response_header_version =
                constants::get_response_header_version(API_KEY_SYNC_GROUP, api_version);
            header.encode(&mut response_buf, response_header_version)?;
            sync_response.encode(&mut response_buf, api_version)?;
        }
        KafkaResponse::Heartbeat {
            correlation_id,
            api_version,
            response: heartbeat_response,
        } => {
            let header = ResponseHeader::default().with_correlation_id(correlation_id);
            let response_header_version =
                constants::get_response_header_version(API_KEY_HEARTBEAT, api_version);
            header.encode(&mut response_buf, response_header_version)?;
            heartbeat_response.encode(&mut response_buf, api_version)?;
        }
        KafkaResponse::LeaveGroup {
            correlation_id,
            api_version,
            response: leave_response,
        } => {
            let header = ResponseHeader::default().with_correlation_id(correlation_id);
            let response_header_version =
                constants::get_response_header_version(API_KEY_LEAVE_GROUP, api_version);
            header.encode(&mut response_buf, response_header_version)?;
            leave_response.encode(&mut response_buf, api_version)?;
        }
        KafkaResponse::ListOffsets {
            correlation_id,
            api_version,
            response: list_response,
        } => {
            let header = ResponseHeader::default().with_correlation_id(correlation_id);
            let response_header_version =
                constants::get_response_header_version(API_KEY_LIST_OFFSETS, api_version);
            header.encode(&mut response_buf, response_header_version)?;
            list_response.encode(&mut response_buf, api_version)?;
        }
        KafkaResponse::DescribeGroups {
            correlation_id,
            api_version,
            response: describe_response,
        } => {
            let header = ResponseHeader::default().with_correlation_id(correlation_id);
            let response_header_version =
                constants::get_response_header_version(API_KEY_DESCRIBE_GROUPS, api_version);
            header.encode(&mut response_buf, response_header_version)?;
            describe_response.encode(&mut response_buf, api_version)?;
        }
        KafkaResponse::ListGroups {
            correlation_id,
            api_version,
            response: list_groups_response,
        } => {
            let header = ResponseHeader::default().with_correlation_id(correlation_id);
            let response_header_version =
                constants::get_response_header_version(API_KEY_LIST_GROUPS, api_version);
            header.encode(&mut response_buf, response_header_version)?;
            list_groups_response.encode(&mut response_buf, api_version)?;
        }
        KafkaResponse::CreateTopics {
            correlation_id,
            api_version,
            response: create_topics_response,
        } => {
            let header = ResponseHeader::default().with_correlation_id(correlation_id);
            let response_header_version =
                constants::get_response_header_version(API_KEY_CREATE_TOPICS, api_version);
            header.encode(&mut response_buf, response_header_version)?;
            create_topics_response.encode(&mut response_buf, api_version)?;
        }
        KafkaResponse::DeleteTopics {
            correlation_id,
            api_version,
            response: delete_topics_response,
        } => {
            let header = ResponseHeader::default().with_correlation_id(correlation_id);
            let response_header_version =
                constants::get_response_header_version(API_KEY_DELETE_TOPICS, api_version);
            header.encode(&mut response_buf, response_header_version)?;
            delete_topics_response.encode(&mut response_buf, api_version)?;
        }
        KafkaResponse::CreatePartitions {
            correlation_id,
            api_version,
            response: create_partitions_response,
        } => {
            let header = ResponseHeader::default().with_correlation_id(correlation_id);
            let response_header_version =
                constants::get_response_header_version(API_KEY_CREATE_PARTITIONS, api_version);
            header.encode(&mut response_buf, response_header_version)?;
            create_partitions_response.encode(&mut response_buf, api_version)?;
        }
        KafkaResponse::DeleteGroups {
            correlation_id,
            api_version,
            response: delete_groups_response,
        } => {
            let header = ResponseHeader::default().with_correlation_id(correlation_id);
            let response_header_version =
                constants::get_response_header_version(API_KEY_DELETE_GROUPS, api_version);
            header.encode(&mut response_buf, response_header_version)?;
            delete_groups_response.encode(&mut response_buf, api_version)?;
        }
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
