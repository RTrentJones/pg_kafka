// Kafka protocol parsing and encoding module
//
// This module handles the binary Kafka wire protocol format:
// [4 bytes: Size (big-endian i32)] [RequestHeader] [RequestBody]
//
// The kafka-protocol crate provides auto-generated structs for all Kafka messages,
// but we need to handle the framing (size prefix) and routing (api_key matching) ourselves.
//
// Module organization:
// - decoding: Request parsing (parse_request and per-API parsers)
// - encoding: Response encoding (encode_response)
// - recordbatch: RecordBatch/MessageSet parsing

mod decoding;
mod encoding;
mod recordbatch;

// Re-export public functions
pub use decoding::parse_request;
pub use encoding::encode_response;
pub use recordbatch::{parse_record_batch, parse_record_batch_with_metadata, ParsedRecordBatch};
