//! Consumer group E2E tests
//!
//! Tests for Kafka consumer group coordinator functionality including:
//! - Full consumer group lifecycle (Join → Sync → Heartbeat → Leave)
//! - Group state management
//! - Automatic rebalancing (Phase 5)
//! - Rebalancing edge cases
//! - Coordinator state machine transitions

mod coordinator_state;
mod lifecycle;
mod rebalance;
mod rebalance_edge_cases;

pub use coordinator_state::{
    test_find_coordinator_bootstrap, test_group_state_transitions, test_heartbeat_keeps_membership,
    test_leave_during_rebalance, test_partition_assignment_strategies,
};
pub use lifecycle::test_consumer_group_lifecycle;
pub use rebalance::{test_rebalance_after_leave, test_session_timeout_rebalance};
pub use rebalance_edge_cases::{
    test_heartbeat_during_rebalance_window, test_multiple_concurrent_timeouts,
    test_rapid_rebalance_cycles, test_rebalance_mixed_timeout_values,
    test_rebalance_with_minimal_session_timeout,
};
