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
mod protocol_encoding;

pub use basic::test_idempotent_producer_basic;
pub use deduplication::test_true_deduplication_manual_replay;
pub use edge_cases::{
    test_idempotent_concurrent_producers, test_idempotent_high_sequence,
    test_idempotent_multi_partition, test_idempotent_producer_epoch_bump,
    test_idempotent_producer_restart,
};
