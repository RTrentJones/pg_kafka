//! Consumer group E2E tests
//!
//! Tests for Kafka consumer group coordinator functionality including:
//! - Full consumer group lifecycle (Join → Sync → Heartbeat → Leave)
//! - Group state management
//! - Automatic rebalancing (Phase 5)

mod lifecycle;
mod rebalance;

pub use lifecycle::test_consumer_group_lifecycle;
pub use rebalance::{test_rebalance_after_leave, test_session_timeout_rebalance};
