//! Producer E2E tests
//!
//! Tests for Kafka producer functionality including:
//! - Basic single-message produce
//! - Batch produce operations
//! - Database verification of stored messages

mod basic;
mod batch;

pub use basic::test_producer;
pub use batch::test_batch_produce;
