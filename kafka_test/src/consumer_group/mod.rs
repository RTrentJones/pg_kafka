//! Consumer group E2E tests
//!
//! Tests for Kafka consumer group coordinator functionality including:
//! - Full consumer group lifecycle (Join → Sync → Heartbeat → Leave)
//! - Group state management
//! - Automatic rebalancing (Phase 5)
//! - Rebalancing edge cases
//! - Coordinator state machine transitions

mod coordinator_state;
mod group_cap;
mod lifecycle;
mod member_memory;
mod protocol_rules;
mod rebalance;
mod rebalance_edge_cases;
mod state_reporting;
mod sync_liveness;

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
        protocol_rules::test_join_group_protocol_and_member_id_rules,
        protocol_rules::test_join_with_no_common_assignor_is_refused,
        protocol_rules::test_offsets_only_group_is_listed_after_restart: sequential, // restarts the listener worker
        sync_liveness::test_slow_leader_sync_keeps_silent_follower: sequential, // Has sleep delays
        group_cap::test_lowered_group_cap_still_admits_new_groups: sequential, // changes a GUC
        member_memory::test_member_state_beyond_the_memory_budget_is_refused: sequential, // changes a GUC
        sync_liveness::test_member_that_never_syncs_is_removed: sequential, // Has sleep delays
        state_reporting::test_describe_groups_reports_metadata_only_when_stable,
        state_reporting::test_list_groups_state_filter_is_case_sensitive,
        state_reporting::test_find_coordinator_unknown_key_type_is_invalid_request,
    ]
}
