//! Idempotent Producer E2E tests (Phase 9)
//!
//! Tests for idempotent producer functionality including:
//! - InitProducerId API
//! - Idempotent message delivery
//! - Producer ID and sequence verification in database
//! - TRUE deduplication testing with manual protocol bytes
//! - Edge cases: epoch bumping, multi-partition, restart, concurrent

mod basic;
mod deduplication;
mod edge_cases;
mod kafka_model;
mod unknown_producer;

/// The `idempotent` tests, in run order.
pub fn tests() -> Vec<crate::registry::TestDef> {
    crate::test_defs!["idempotent";
        basic::test_idempotent_producer_basic,
        deduplication::test_true_deduplication_manual_replay,
        edge_cases::test_idempotent_producer_epoch_bump,
        edge_cases::test_idempotent_multi_partition,
        edge_cases::test_idempotent_producer_restart,
        edge_cases::test_idempotent_concurrent_producers: sequential, // Concurrent producers
        edge_cases::test_idempotent_high_sequence,
        kafka_model::test_idempotent_duplicates_and_epochs_follow_kafka: sequential,
        kafka_model::test_idempotent_sequence_wraps_at_i32_max,
        unknown_producer::test_unknown_producer_is_accepted_as_new: sequential, // reads the id sequence
        unknown_producer::test_idempotent_producer_survives_pruned_state,
    ]
}
