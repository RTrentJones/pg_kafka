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
use kafka_protocol::messages::produce_request::ProduceRequest;
use kafka_protocol::messages::produce_response::{
    PartitionProduceResponse as KafkaPartitionProduceResponse, ProduceResponse,
    TopicProduceResponse as KafkaTopicProduceResponse,
};
use kafka_protocol::messages::{BrokerId, ResponseHeader, TopicName};
use kafka_protocol::protocol::{Decodable, Encodable};
use kafka_protocol::records::RecordBatchDecoder;

use super::constants::*;
use super::error::{KafkaError, Result};
use super::messages::{KafkaRequest, KafkaResponse};

// Import conditional logging macros for test isolation
use crate::{pg_log, pg_warning};

/// Parse a Kafka request from a frame
///
/// The frame has already been extracted by LengthDelimitedCodec, so we only need to parse:
/// [RequestHeader] [RequestBody]
///
/// RequestHeader contains:
/// - api_key: i16 (which API this is, e.g., 18 = ApiVersions)
/// - api_version: i16 (which version of the API)
/// - correlation_id: i32 (client-assigned ID for matching responses)
/// - client_id: nullable string (client identifier)
///
/// Returns None if there's a parse error with error response already sent
pub fn parse_request(
    frame: BytesMut,
    response_tx: tokio::sync::mpsc::UnboundedSender<KafkaResponse>,
) -> Result<Option<KafkaRequest>> {
    pg_log!("Parsing request from {} byte frame", frame.len());

    // Parse RequestHeader to determine which API this is
    let mut payload_buf = frame.freeze();

    // Read api_key and api_version first to know which API this is
    pg_log!("Buffer remaining before parsing: {} bytes", payload_buf.remaining());
    if payload_buf.remaining() < 4 {
        return Err(KafkaError::RequestTooShort {
            expected: 4,
            actual: payload_buf.remaining(),
        });
    }
    let api_key = payload_buf.get_i16();
    let api_version = payload_buf.get_i16();

    // Parse the rest of the RequestHeader (correlation_id + client_id)
    // For simplicity, we'll manually parse instead of using RequestHeader::decode
    if payload_buf.remaining() < 4 {
        return Err(KafkaError::RequestTooShort {
            expected: 4,
            actual: payload_buf.remaining(),
        });
    }
    let correlation_id = payload_buf.get_i32();

    // Parse client_id from RequestHeader
    // client_id is a nullable string: i16 length prefix, then UTF-8 bytes
    // -1 length means null, 0 means empty string, >0 means that many bytes follow
    if payload_buf.remaining() < 2 {
        return Err(KafkaError::RequestTooShort {
            expected: 2,
            actual: payload_buf.remaining(),
        });
    }
    let client_id_len = payload_buf.get_i16();
    let client_id = if client_id_len < 0 {
        None // Null client_id
    } else {
        // Read client_id bytes
        if payload_buf.remaining() < client_id_len as usize {
            return Err(KafkaError::RequestTooShort {
                expected: client_id_len as usize,
                actual: payload_buf.remaining(),
            });
        }
        let mut client_id_bytes = vec![0u8; client_id_len as usize];
        payload_buf.copy_to_slice(&mut client_id_bytes);
        match String::from_utf8(client_id_bytes) {
            Ok(s) => Some(s),
            Err(e) => {
                pg_warning!("Invalid UTF-8 in client_id: {}", e);
                None
            }
        }
    };

    // Step 4: Match on api_key to determine request type
    match api_key {
        API_KEY_API_VERSIONS => {
            // ApiVersions request
            // The body is empty for ApiVersions, so we don't need to parse it
            pg_log!("Parsed ApiVersions request (api_key={}, api_version={})", API_KEY_API_VERSIONS, api_version);
            Ok(Some(KafkaRequest::ApiVersions {
                correlation_id,
                client_id,
                api_version,
                response_tx,
            }))
        }
        API_KEY_METADATA => {
            // Metadata request - parse using kafka-protocol crate
            pg_log!("Parsed Metadata request (api_key={}, version={})", API_KEY_METADATA, api_version);

            // Use kafka-protocol crate to decode MetadataRequest
            let metadata_req = match kafka_protocol::messages::metadata_request::MetadataRequest::decode(&mut payload_buf, api_version) {
                Ok(req) => req,
                Err(e) => {
                    pg_warning!("Failed to decode MetadataRequest: {}", e);
                    let error_response = KafkaResponse::Error {
                        correlation_id,
                        error_code: ERROR_CORRUPT_MESSAGE,
                        error_message: Some(format!("Malformed MetadataRequest: {}", e)),
                    };
                    let _ = response_tx.send(error_response);
                    return Ok(None);
                }
            };

            // Extract requested topics (None means "all topics")
            let topics = if metadata_req.topics.is_none() || metadata_req.topics.as_ref().unwrap().is_empty() {
                pg_log!("Metadata request for ALL topics");
                None
            } else {
                let topic_names: Vec<String> = metadata_req.topics.unwrap()
                    .iter()
                    .filter_map(|t| t.name.as_ref().map(|n| n.to_string()))
                    .collect();
                pg_log!("Metadata request for specific topics: {:?}", topic_names);
                if topic_names.is_empty() {
                    None
                } else {
                    Some(topic_names)
                }
            };

            Ok(Some(KafkaRequest::Metadata {
                correlation_id,
                client_id,
                api_version,
                topics,
                response_tx,
            }))
        }
        API_KEY_PRODUCE => {
            // Produce request - write messages to topics
            pg_log!("Parsed Produce request (api_key={}, version={})", API_KEY_PRODUCE, api_version);

            // Use kafka-protocol crate to decode ProduceRequest
            let produce_req = match ProduceRequest::decode(&mut payload_buf, api_version) {
                Ok(req) => req,
                Err(e) => {
                    pg_warning!("Failed to decode ProduceRequest: {}", e);
                    let error_response = KafkaResponse::Error {
                        correlation_id,
                        error_code: ERROR_CORRUPT_MESSAGE,
                        error_message: Some(format!("Malformed ProduceRequest: {}", e)),
                    };
                    let _ = response_tx.send(error_response);
                    return Ok(None);
                }
            };

            let acks = produce_req.acks;
            let timeout_ms = produce_req.timeout_ms;

            pg_log!(
                "ProduceRequest: acks={}, timeout_ms={}, topics={}",
                acks,
                timeout_ms,
                produce_req.topic_data.len()
            );

            // Extract topic data from kafka-protocol types to our types
            let mut topic_data = Vec::new();
            for topic in produce_req.topic_data {
                let topic_name = topic.name.to_string();
                let mut partitions = Vec::new();

                for partition in topic.partition_data {
                    let partition_index = partition.index;

                    // Parse RecordBatch from partition.records
                    // RecordBatch is the binary format containing actual messages
                    let records = match &partition.records {
                        Some(batch_bytes) => match parse_record_batch(batch_bytes) {
                            Ok(records) => records,
                            Err(e) => {
                                pg_warning!(
                                    "Failed to parse RecordBatch for topic={}, partition={}: {}",
                                    topic_name,
                                    partition_index,
                                    e
                                );
                                let error_response = KafkaResponse::Error {
                                    correlation_id,
                                    error_code: ERROR_CORRUPT_MESSAGE,
                                    error_message: Some(format!("Invalid RecordBatch: {}", e)),
                                };
                                let _ = response_tx.send(error_response);
                                return Ok(None);
                            }
                        },
                        None => Vec::new(), // No records in this partition
                    };

                    pg_log!(
                        "Parsed {} records for topic={}, partition={}",
                        records.len(),
                        topic_name,
                        partition_index
                    );

                    partitions.push(super::messages::PartitionProduceData {
                        partition_index,
                        records,
                    });
                }

                topic_data.push(super::messages::TopicProduceData {
                    name: topic_name,
                    partitions,
                });
            }

            Ok(Some(KafkaRequest::Produce {
                correlation_id,
                client_id,
                api_version,
                acks,
                timeout_ms,
                topic_data,
                response_tx,
            }))
        }
        _ => {
            // Unsupported API
            pg_warning!("Unsupported API key: {}", api_key);

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

/// Parse RecordBatch into individual Records
///
/// RecordBatch format is complex (see Kafka protocol spec):
/// - Base offset (i64)
/// - Batch length (i32)
/// - Partition leader epoch (i32)
/// - Magic byte (i8 = 2 for v0.11+)
/// - CRC32C (u32)
/// - Attributes (i16) - compression, timestamp type
/// - Last offset delta (i32)
/// - Base timestamp (i64)
/// - Max timestamp (i64)
/// - Producer ID, epoch, base sequence (for idempotence)
/// - Records count (i32)
/// - Records (varint-encoded)
///
/// The kafka-protocol crate handles all this complexity for us.
fn parse_record_batch(batch_bytes: &bytes::Bytes) -> Result<Vec<super::messages::Record>> {
    use super::messages::{Record, RecordHeader};
    use bytes::Buf;

    // Handle empty batch
    if batch_bytes.is_empty() {
        return Ok(Vec::new());
    }

    pg_log!("parse_record_batch: received {} bytes", batch_bytes.len());
    pg_log!("First 20 bytes: {:?}", &batch_bytes[..batch_bytes.len().min(20)]);

    let mut batch_buf = batch_bytes.clone();

    // Try RecordBatch v2 format first (Kafka 0.11+)
    // If that fails, fall back to MessageSet v0/v1 (legacy format)
    match RecordBatchDecoder::decode(&mut batch_buf) {
        Ok(record_set) => {
            // RecordSet contains a Vec<Record> in the .records field
            let mut records = Vec::new();
            for record in record_set.records {
                let key = record.key.map(|k: bytes::Bytes| k.to_vec());
                let value = record.value.map(|v: bytes::Bytes| v.to_vec());
                let timestamp = Some(record.timestamp);

                // Parse headers from IndexMap to Vec
                // Headers are IndexMap<StrBytes, Option<Bytes>>
                let headers: Vec<RecordHeader> = record
                    .headers
                    .into_iter()
                    .map(|(k, v)| RecordHeader {
                        key: k.to_string(),
                        value: v.map(|b: bytes::Bytes| b.to_vec()).unwrap_or_default(),
                    })
                    .collect();

                records.push(Record {
                    key,
                    value,
                    headers,
                    timestamp,
                });
            }
            Ok(records)
        }
        Err(e) => {
            // RecordBatch v2 decode failed, try MessageSet v0/v1 (legacy format)
            pg_log!("RecordBatch decode failed ({}), trying MessageSet v0/v1 format", e);

            let mut buf = batch_bytes.clone();
            let mut records = Vec::new();

            // MessageSet format (v0/v1):
            // Repeated: [Offset: 8 bytes][MessageSize: 4 bytes][Message]
            // Message: [CRC: 4 bytes][Magic: 1 byte][Attributes: 1 byte][Key][Value]
            // Key/Value: [Length: 4 bytes (i32, -1=null)][Data]

            while buf.remaining() >= 12 {  // Minimum: 8 (offset) + 4 (size)
                let _offset = buf.get_i64();  // Ignore offset (we assign our own)
                let message_size = buf.get_i32();

                if message_size < 0 || buf.remaining() < message_size as usize {
                    pg_warning!("Invalid message size in MessageSet: {}", message_size);
                    break;
                }

                // Parse the Message struct
                let _crc = buf.get_u32();  // Skip CRC validation for now
                let magic = buf.get_i8();
                let _attributes = buf.get_i8();  // Compression, timestamp type

                pg_log!("MessageSet magic byte: {}", magic);

                // Parse key (nullable bytes)
                let key = if buf.remaining() < 4 {
                    None
                } else {
                    let key_len = buf.get_i32();
                    if key_len < 0 {
                        None
                    } else if buf.remaining() >= key_len as usize {
                        let mut key_bytes = vec![0u8; key_len as usize];
                        buf.copy_to_slice(&mut key_bytes);
                        Some(key_bytes)
                    } else {
                        pg_warning!("Insufficient bytes for key");
                        break;
                    }
                };

                // Parse value (nullable bytes)
                let value = if buf.remaining() < 4 {
                    None
                } else {
                    let value_len = buf.get_i32();
                    if value_len < 0 {
                        None
                    } else if buf.remaining() >= value_len as usize {
                        let mut value_bytes = vec![0u8; value_len as usize];
                        buf.copy_to_slice(&mut value_bytes);
                        Some(value_bytes)
                    } else {
                        pg_warning!("Insufficient bytes for value");
                        break;
                    }
                };

                records.push(Record {
                    key,
                    value,
                    headers: Vec::new(),  // MessageSet v0/v1 doesn't support headers
                    timestamp: None,  // v0 doesn't have timestamps, v1 does but we skip for simplicity
                });
            }

            if records.is_empty() {
                Err(KafkaError::Encoding(format!(
                    "Failed to parse as both RecordBatch and MessageSet: {}",
                    e
                )))
            } else {
                pg_log!("Successfully parsed {} records from MessageSet format", records.len());
                Ok(records)
            }
        }
    }
}

/// Encode and send a Kafka response to a TCP socket
///
/// Kafka response format:
/// ```
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
            api_versions,
        } => {
            // Build ResponseHeader
            let header = ResponseHeader::default().with_correlation_id(correlation_id);

            // Build ApiVersionsResponse
            let mut api_version_response = ApiVersionsResponse::default();
            api_version_response.error_code = 0; // No error
            api_version_response.throttle_time_ms = 0; // No throttling

            // Convert our ApiVersion structs to kafka-protocol's ApiVersion structs
            for av in api_versions {
                let mut kafka_av = kafka_protocol::messages::api_versions_response::ApiVersion::default();
                kafka_av.api_key = av.api_key;
                kafka_av.min_version = av.min_version;
                kafka_av.max_version = av.max_version;
                api_version_response.api_keys.push(kafka_av);
            }

            // Encode response header and body
            // IMPORTANT: Use the API version from the request for encoding
            // Different versions have different wire formats (e.g., flexible vs non-flexible)
            // CRITICAL: ApiVersions always uses ResponseHeader v0, even for v3+!
            // This is different from other APIs where v3+ uses ResponseHeader v1.
            // See: https://github.com/Baylox/kafka-mock (19-byte example for v3)
            let response_header_version = 0;  // Always v0 for ApiVersions!
            let response_version = api_version;

            // DEBUG: Track sizes
            let before_header = response_buf.len();
            header.encode(&mut response_buf, response_header_version)?;
            let after_header = response_buf.len();

            // Encode response body with the requested API version
            api_version_response.encode(&mut response_buf, response_version)?;
            let after_body = response_buf.len();

            // DEBUG: Write response bytes to file for analysis
            if let Ok(mut f) = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open("/tmp/pg_kafka_responses.log")
            {
                use std::io::Write;
                let _ = writeln!(f, "ApiVersions v{} response: header={} bytes, body={} bytes, total={} bytes",
                    api_version, after_header - before_header, after_body - after_header, response_buf.len());
                let _ = writeln!(f, "  Bytes: {:?}", &response_buf[..]);
            }
        }
        KafkaResponse::Metadata {
            correlation_id,
            api_version,
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

            // Encode response header
            // Metadata v9+ uses flexible format (ResponseHeader v1)
            // Metadata v0-v8 uses non-flexible format (ResponseHeader v0)
            let response_header_version = if api_version >= 9 { 1 } else { 0 };
            header.encode(&mut response_buf, response_header_version)?;

            // Encode response body using the requested API version
            metadata_response.encode(&mut response_buf, api_version)?;
        }
        KafkaResponse::Produce {
            correlation_id,
            api_version,
            responses,
        } => {
            // Build ResponseHeader
            let header = ResponseHeader::default().with_correlation_id(correlation_id);

            // Build ProduceResponse
            let mut produce_response = ProduceResponse::default();
            produce_response.throttle_time_ms = 0;  // No throttling

            // Convert our response types to kafka-protocol types
            for topic_response in responses {
                let mut kafka_topic_response = KafkaTopicProduceResponse::default();
                kafka_topic_response.name = TopicName(topic_response.name.into());

                // Add partition responses
                for partition_response in topic_response.partitions {
                    let mut kafka_partition_response = KafkaPartitionProduceResponse::default();
                    kafka_partition_response.index = partition_response.partition_index;
                    kafka_partition_response.error_code = partition_response.error_code;
                    kafka_partition_response.base_offset = partition_response.base_offset;
                    kafka_partition_response.log_append_time_ms = partition_response.log_append_time;
                    kafka_partition_response.log_start_offset = partition_response.log_start_offset;

                    kafka_topic_response.partition_responses.push(kafka_partition_response);
                }

                produce_response.responses.push(kafka_topic_response);
            }

            // Encode response header
            // Produce v9+ uses flexible format (ResponseHeader v1)
            // Produce v0-v8 uses non-flexible format (ResponseHeader v0)
            let response_header_version = if api_version >= 9 { 1 } else { 0 };
            header.encode(&mut response_buf, response_header_version)?;

            // Encode response body using the requested API version
            produce_response.encode(&mut response_buf, api_version)?;
        }
        KafkaResponse::Error {
            correlation_id,
            error_code,
            error_message,
        } => {
            // LIMITATION: Hand-rolled error response encoding
            // ===============================================
            // This is a simplified error response that may not match the expected format
            // for the specific API that triggered the error. Real Kafka clients might fail
            // to parse this if they expect a properly-formatted error response for their API.
            //
            // TODO(Phase 2): Match error response format to the specific API key that failed.
            // For example, if ApiVersions fails, return a proper ApiVersionsResponse with error_code set.
            // For now, this works with kcat which is lenient about error responses.
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

    // DEBUG: Log what we're encoding
    if let Ok(mut f) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open("/tmp/pg_kafka_send.log")
    {
        use std::io::Write;
        let _ = writeln!(f, "Encoded response: {} bytes", response_buf.len());
        let _ = writeln!(f, "  Payload: {:?}", &response_buf[..]);
    }

    // Return the response payload
    // LengthDelimitedCodec will automatically add the 4-byte size prefix
    Ok(response_buf)
}
