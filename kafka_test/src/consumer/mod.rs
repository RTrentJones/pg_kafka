//! Consumer E2E tests
//!
//! Tests for Kafka consumer functionality including:
//! - Basic single-message consume
//! - Multiple message consumption
//! - Consume from specific offsets

mod basic;
mod from_offset;
mod multiple;

pub use basic::test_consumer_basic;
pub use from_offset::test_consumer_from_offset;
pub use multiple::test_consumer_multiple_messages;
