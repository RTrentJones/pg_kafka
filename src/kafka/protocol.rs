// Kafka protocol parsing and encoding module
//
// This module handles the binary Kafka wire protocol format:
// [4 bytes: Size (big-endian i32)] [RequestHeader] [RequestBody]
//
// The kafka-protocol crate provides auto-generated structs for all Kafka messages,
// but we need to handle the framing (size prefix) and routing (api_key matching) ourselves.

use bytes::{Buf, BufMut, BytesMut};
use kafka_protocol::messages::api_versions_response::ApiVersionsResponse;
use kafka_protocol::messages::metadata_response::{
    MetadataResponse, MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
};
use kafka_protocol::messages::{BrokerId, ResponseHeader, TopicName};
use kafka_protocol::protocol::Encodable;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use super::constants::*;
use super::error::{KafkaError, Result};
use super::messages::{KafkaRequest, KafkaResponse};

/// Parse a Kafka request from a TCP socket
///
/// Kafka request format:
/// ```
/// [4 bytes: Size] [RequestHeader] [RequestBody]
/// ```
///
/// RequestHeader contains:
/// - api_key: i16 (which API this is, e.g., 18 = ApiVersions)
/// - api_version: i16 (which version of the API)
/// - correlation_id: i32 (client-assigned ID for matching responses)
/// - client_id: nullable string (client identifier)
///
/// Returns None if the connection is closed gracefully
pub async fn parse_request(
    socket: &mut TcpStream,
    response_tx: tokio::sync::mpsc::UnboundedSender<KafkaResponse>,
) -> Result<Option<KafkaRequest>> {
    // Step 1: Read the 4-byte size header (big-endian)
    pgrx::log!("Waiting to read size header...");
    let mut size_buf = [0u8; 4];
    match socket.read_exact(&mut size_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            // Client closed connection gracefully
            pgrx::log!("Client closed connection (EOF on size read)");
            return Ok(None);
        }
        Err(e) => {
            pgrx::warning!("Error reading size header: {}", e);
            return Err(e.into());
        }
    }

    let size = i32::from_be_bytes(size_buf);
    pgrx::log!("Received request of size: {} bytes (size_buf: {:?})", size, size_buf);

    if size <= 0 || size > MAX_REQUEST_SIZE {
        return Err(KafkaError::InvalidRequestSize(size));
    }

    // Step 2: Read the request payload (header + body)
    let mut payload = vec![0u8; size as usize];
    socket.read_exact(&mut payload).await?;
    pgrx::log!("Read payload: {} bytes, first 10 bytes: {:?}", payload.len(), &payload[..payload.len().min(10)]);

    // Step 3: Parse RequestHeader to determine which API this is
    let mut payload_buf = bytes::Bytes::from(payload);

    // Read api_key and api_version first to know which API this is
    pgrx::log!("Buffer remaining before parsing: {} bytes", payload_buf.remaining());
    if payload_buf.remaining() < 4 {
        return Err(KafkaError::RequestTooShort {
            expected: 4,
            actual: payload_buf.remaining(),
        });
    }
    let api_key = payload_buf.get_i16();
    let _api_version = payload_buf.get_i16();

    // Parse the rest of the RequestHeader (correlation_id + client_id)
    // For simplicity, we'll manually parse instead of using RequestHeader::decode
    if payload_buf.remaining() < 4 {
        return Err(KafkaError::RequestTooShort {
            expected: 4,
            actual: payload_buf.remaining(),
        });
    }
    let correlation_id = payload_buf.get_i32();

    // Client ID is a nullable string (i16 length, then bytes)
    // For ApiVersions, we'll just skip parsing the client_id properly for now
    let client_id = None; // TODO: Parse client_id properly in future

    // Step 4: Match on api_key to determine request type
    match api_key {
        API_KEY_API_VERSIONS => {
            // ApiVersions request
            // The body is empty for ApiVersions, so we don't need to parse it
            pgrx::log!("Parsed ApiVersions request (api_key={})", API_KEY_API_VERSIONS);
            Ok(Some(KafkaRequest::ApiVersions {
                correlation_id,
                client_id,
                response_tx,
            }))
        }
        API_KEY_METADATA => {
            // Metadata request
            // For now, we'll parse the basic structure but accept any topic list
            // In Phase 2, we'll actually query the database for topic metadata
            pgrx::log!("Parsed Metadata request (api_key={}, version={})", API_KEY_METADATA, _api_version);

            // Parse the rest of the request body to extract topic names
            // For Step 4, we'll just use a simplified approach and ignore the topic list
            // The client_id was already skipped, and we have the remaining body in payload_buf

            // TODO: Properly parse MetadataRequest body using kafka-protocol crate
            // For now, return a request with empty topics list (means "all topics")
            Ok(Some(KafkaRequest::Metadata {
                correlation_id,
                client_id,
                topics: None, // None means "all topics"
                response_tx,
            }))
        }
        _ => {
            // Unsupported API
            pgrx::warning!("Unsupported API key: {}", api_key);

            // Send error response immediately
            let error_response = KafkaResponse::Error {
                correlation_id,
                error_code: ERROR_UNSUPPORTED_VERSION,
                error_message: Some(format!("Unsupported API key: {}", api_key)),
            };
            let _ = response_tx.send(error_response);

            Ok(None)
        }
    }
}

/// Encode and send a Kafka response to a TCP socket
///
/// Kafka response format:
/// ```
/// [4 bytes: Size] [ResponseHeader] [ResponseBody]
/// ```
pub async fn send_response(
    socket: &mut TcpStream,
    response: KafkaResponse,
) -> Result<()> {
    let mut response_buf = BytesMut::new();

    match response {
        KafkaResponse::ApiVersions {
            correlation_id,
            api_versions,
        } => {
            // Build ResponseHeader
            let header = ResponseHeader::default().with_correlation_id(correlation_id);

            // Build ApiVersionsResponse
            let mut api_version_response = ApiVersionsResponse::default();
            api_version_response.error_code = 0; // No error

            // Convert our ApiVersion structs to kafka-protocol's ApiVersion structs
            for av in api_versions {
                let mut kafka_av = kafka_protocol::messages::api_versions_response::ApiVersion::default();
                kafka_av.api_key = av.api_key;
                kafka_av.min_version = av.min_version;
                kafka_av.max_version = av.max_version;
                api_version_response.api_keys.push(kafka_av);
            }

            // Encode response header and body
            // ResponseHeader version should match the API version from the request
            // ApiVersions v3 uses ResponseHeader v1
            header.encode(&mut response_buf, 1)?; // ResponseHeader v1 for ApiVersions v3

            // Encode response body
            api_version_response.encode(&mut response_buf, 3)?; // ApiVersions v3
        }
        KafkaResponse::Metadata {
            correlation_id,
            brokers,
            topics,
        } => {
            // Build ResponseHeader
            let header = ResponseHeader::default().with_correlation_id(correlation_id);

            // Build MetadataResponse
            let mut metadata_response = MetadataResponse::default();

            // Add brokers
            for broker in brokers {
                let mut kafka_broker = MetadataResponseBroker::default();
                kafka_broker.node_id = BrokerId(broker.node_id);
                kafka_broker.host = broker.host.into();
                kafka_broker.port = broker.port;
                kafka_broker.rack = broker.rack.map(|r| r.into());
                metadata_response.brokers.push(kafka_broker);
            }

            // Add topics
            for topic in topics {
                let mut kafka_topic = MetadataResponseTopic::default();
                kafka_topic.error_code = topic.error_code;
                kafka_topic.name = Some(TopicName(topic.name.into()));

                // Add partitions for this topic
                for partition in topic.partitions {
                    let mut kafka_partition = MetadataResponsePartition::default();
                    kafka_partition.error_code = partition.error_code;
                    kafka_partition.partition_index = partition.partition_index;
                    kafka_partition.leader_id = BrokerId(partition.leader_id);
                    kafka_partition.replica_nodes = partition
                        .replica_nodes
                        .into_iter()
                        .map(BrokerId)
                        .collect();
                    kafka_partition.isr_nodes = partition.isr_nodes.into_iter().map(BrokerId).collect();
                    kafka_topic.partitions.push(kafka_partition);
                }

                metadata_response.topics.push(kafka_topic);
            }

            // Encode response header (v1 for Metadata v9)
            header.encode(&mut response_buf, 1)?;

            // Encode response body (using version 9 for broad compatibility)
            metadata_response.encode(&mut response_buf, 9)?;
        }
        KafkaResponse::Error {
            correlation_id,
            error_code,
            error_message,
        } => {
            // For error responses, we send a minimal response with the error code
            // This is a simplified error response - in production we'd match the expected response format
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

    // Prepend the size header (4 bytes, big-endian)
    let size = response_buf.len() as i32;
    socket.write_all(&size.to_be_bytes()).await?;

    // Write the response payload
    socket.write_all(&response_buf).await?;

    // Flush to ensure data is sent
    socket.flush().await?;

    Ok(())
}
