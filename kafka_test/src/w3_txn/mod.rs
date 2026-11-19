//! Wave 3, stream `txn`: fetch past invisible offsets, ListOffsets isolation and
//! MAX_TIMESTAMP, OffsetFetch require_stable, Kafka's idempotent producer model,
//! transaction validation and RecordBatch grouping.

pub mod consume;
pub mod protocol;
pub mod wire;
