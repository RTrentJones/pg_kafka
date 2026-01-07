//! Consumer group E2E tests
//!
//! Tests for Kafka consumer group coordinator functionality including:
//! - Full consumer group lifecycle (Join → Sync → Heartbeat → Leave)
//! - Group state management

mod lifecycle;

pub use lifecycle::test_consumer_group_lifecycle;
