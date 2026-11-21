//! Producer E2E tests
//!
//! Tests for Kafka producer functionality including:
//! - Basic single-message produce
//! - Batch produce operations
//! - acks=0 (fire-and-forget) protocol compliance
//! - Database verification of stored messages

mod ack_visibility;
mod acks_zero;
mod basic;
mod batch;
mod invalid_names;
mod timestamp;

/// The `producer` tests, in run order.
pub fn tests() -> Vec<crate::registry::TestDef> {
    crate::test_defs!["producer";
        basic::test_producer,
        invalid_names::test_produce_to_an_invalid_topic_name_is_unknown_topic,
        batch::test_batch_produce,
        acks_zero::test_producer_acks_zero,
        ack_visibility::test_produce_ack_implies_committed_visibility: sequential, // Mutates global GUC (test_pre_commit_delay_ms)
        timestamp::test_producer_timestamp_roundtrip,
        timestamp::test_producer_timestamps_far_from_the_batch_base_roundtrip,
    ]
}
