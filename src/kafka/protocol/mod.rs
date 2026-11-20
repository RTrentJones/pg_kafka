//! The Kafka wire format on the network thread.
//!
//! A request arrives as `[4-byte big-endian length][RequestHeader][RequestBody]`:
//!
//! - [`framing`]: the length prefix ([`framing::FrameCodec`]), with the frame-size budget and
//!   the per-frame read clock;
//! - `decoding`: the request header ([`parse_request`]); the body goes to the API registry
//!   (`crate::kafka::api`);
//! - `encoding`: response frames ([`encode_response`]);
//! - `recordbatch`: the records of a Produce partition, RecordBatch v2 or legacy
//!   MessageSet v0/v1 ([`parse_record_batch_with_metadata`]);
//! - [`decode_budget`]: how many decoded record bytes a request, and all requests together,
//!   may hold.

pub mod decode_budget;
mod decoding;
mod encoding;
pub mod framing;
mod recordbatch;

pub use decode_budget::{DecodeBudget, DecodedCharge};
pub use decoding::parse_request;
pub use encoding::encode_response;
pub use recordbatch::{parse_record_batch, parse_record_batch_with_metadata, ParsedRecordBatch};
