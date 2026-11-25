//! Consumer E2E tests
//!
//! Tests for Kafka consumer functionality including:
//! - Basic single-message consume
//! - Multiple message consumption
//! - Consume from specific offsets

mod basic;
mod batching;
mod from_offset;
mod headers;
mod multiple;

/// The `consumer` tests, in run order.
pub fn tests() -> Vec<crate::registry::TestDef> {
    crate::test_defs!["consumer";
        basic::test_consumer_basic,
        headers::test_consumer_receives_headers,
        headers::test_header_key_with_nul_roundtrips,
        multiple::test_consumer_multiple_messages,
        from_offset::test_consumer_from_offset,
        batching::test_fetch_groups_records_into_batches,
    ]
}
