//! Offset management E2E tests
//!
//! Tests for Kafka offset management functionality including:
//! - OffsetCommit/OffsetFetch operations
//! - Offset boundary conditions (earliest, latest, specific)
//! - Edge cases: metadata, reset policy, multi-partition, seek

mod boundaries;
mod by_timestamp;
mod commit_fetch;
mod commit_indexes;
mod commit_rules;
mod edge_cases;
mod fetch_parity;
mod list_offsets_isolation;
mod no_reuse;
mod require_stable;
mod timestamp_index;

/// The `offset_management` tests, in run order.
pub fn tests() -> Vec<crate::registry::TestDef> {
    crate::test_defs!["offset_management";
        commit_fetch::test_offset_commit_fetch,
        boundaries::test_offset_boundaries,
        no_reuse::test_offset_no_reuse_after_cleanup,
        no_reuse::test_high_watermark_no_regress_after_cleanup,
        by_timestamp::test_list_offsets_by_timestamp,
        // Sequential: it ends the worker to flush its statistics.
        timestamp_index::test_list_offsets_by_timestamp_reads_through_an_index: sequential,
        edge_cases::test_offset_commit_with_metadata,
        edge_cases::test_fetch_offset_new_group,
        edge_cases::test_offset_reset_policy,
        edge_cases::test_offset_commit_multi_partition,
        edge_cases::test_offset_seek,
        commit_rules::test_offset_commit_generation_and_member_rules,
        list_offsets_isolation::test_list_offsets_isolation_max_timestamp_and_partitions,
        require_stable::test_offset_fetch_require_stable,
        fetch_parity::test_offset_fetch_of_a_missing_topic_is_no_offset,
        fetch_parity::test_offset_commit_metadata_longer_than_4096_is_refused,
        commit_indexes::test_offset_commit_changes_no_indexed_column,
    ]
}
