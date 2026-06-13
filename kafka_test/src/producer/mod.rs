//! Producer E2E tests
//!
//! Tests for Kafka producer functionality including:
//! - Basic single-message produce
//! - Batch produce operations
//! - acks=0 (fire-and-forget) protocol compliance
//! - Database verification of stored messages

mod acks_zero;
mod basic;
mod batch;

pub use acks_zero::test_producer_acks_zero;
pub use basic::test_producer;
pub use batch::test_batch_produce;
