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
mod protocol_rules;
mod rebalance;
mod rebalance_edge_cases;

/// The `consumer_group` tests, in run order.
pub fn tests() -> Vec<crate::registry::TestDef> {
    crate::test_defs!["consumer_group";
        lifecycle::test_consumer_group_lifecycle,
        rebalance::test_rebalance_after_leave: sequential, // Uses shared group state
        rebalance::test_session_timeout_rebalance: sequential, // Has sleep delays
        // Rebalancing edge case tests
        rebalance_edge_cases::test_rapid_rebalance_cycles: sequential, // Uses shared group state
        rebalance_edge_cases::test_heartbeat_during_rebalance_window: sequential, // Uses shared group state
        rebalance_edge_cases::test_multiple_concurrent_timeouts: sequential, // Has sleep delays
        rebalance_edge_cases::test_rebalance_with_minimal_session_timeout: sequential, // Has sleep delays
        rebalance_edge_cases::test_rebalance_mixed_timeout_values: sequential, // Has sleep delays
        // Coordinator state machine tests
        coordinator_state::test_find_coordinator_bootstrap,
        coordinator_state::test_partition_assignment_strategies: sequential, // Multiple consumers
        coordinator_state::test_leave_during_rebalance: sequential, // Tests rebalance timing
        coordinator_state::test_heartbeat_keeps_membership,
        coordinator_state::test_group_state_transitions,
        protocol_rules::test_w3g_join_group_protocol_rules,
        protocol_rules::test_w3g_different_assignor_is_refused_group_unaffected,
        protocol_rules::test_w3g_offsets_only_group_survives_restart: sequential, // restarts the listener worker
    ]
}
