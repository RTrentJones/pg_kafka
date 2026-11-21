//! Edge case E2E tests
//!
//! Tests for boundary conditions and edge cases.

mod boundaries;
mod empty_states;
mod large_data;
mod retention_sweep;

/// The `edge_cases` tests, in run order.
pub fn tests() -> Vec<crate::registry::TestDef> {
    crate::test_defs!["edge_cases";
        boundaries::test_offset_zero_boundary,
        boundaries::test_partition_zero,
        boundaries::test_single_partition_topic,
        boundaries::test_high_offset_values,
        empty_states::test_consume_empty_partition,
        empty_states::test_list_offsets_empty_topic,
        empty_states::test_fetch_committed_no_history,
        empty_states::test_consumer_group_empty,
        large_data::test_large_message_key,
        large_data::test_large_message_value,
        large_data::test_batch_1000_messages,
        // DR-1/DR-2: retention sweep — NOT parallel safe (the grace-0 aborted pass
        // reclaims any aborted row, racing concurrent transaction tests)
        retention_sweep::test_retention_sweep_reclaims_aborted_messages: sequential,
        retention_sweep::test_retention_sweep_expires_old_messages: sequential,
    ]
}
