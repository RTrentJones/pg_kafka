//! The Kafka wire format on the network thread.
//!
//! A request arrives as `[4-byte big-endian length][RequestHeader][RequestBody]`:
//!
//! - [`framing`]: the length prefix ([`framing::FrameCodec`]), with the frame-size budget and
//!   the per-frame read clock;
//! - `decoding`: the request header ([`parse_request`]); the body goes to the API registry
//!   (`crate::kafka::api`);
//! - `encoding`: response frames ([`encode_response`]);
//! - `recordbatch`: the records of a Produce partition, one RecordBatch v2
//!   ([`parse_record_batch_with_metadata`]);
//! - [`decode_budget`]: how much memory decoded records may take, per request and across
//!   all requests.

pub mod decode_budget;
mod decoding;
mod encoding;
pub mod framing;
mod recordbatch;
#[cfg(test)]
pub(crate) use recordbatch::test_batches as recordbatch_test_batches;

pub use decode_budget::{DecodeBudget, DecodedCharge};
pub use decoding::parse_request;
pub use encoding::encode_response;
pub use recordbatch::{
    least_decoded_memory, parse_record_batch, parse_record_batch_with_metadata,
    validate_produce_records, ParsedRecordBatch, HEADER_MEMORY, RECORD_MEMORY,
};
