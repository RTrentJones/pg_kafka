//! Idempotent Producer E2E tests (Phase 9)
//!
//! Tests for idempotent producer functionality including:
//! - InitProducerId API
//! - Idempotent message delivery
//! - Producer ID and sequence verification in database
//! - TRUE deduplication testing with manual protocol bytes

mod basic;
mod deduplication;
mod protocol_encoding;
mod replay_test;

pub use basic::test_idempotent_producer_basic;
pub use deduplication::test_true_deduplication_manual_replay;
pub use replay_test::test_replay_deduplication;
