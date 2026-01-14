//! Offset management E2E tests
//!
//! Tests for Kafka offset management functionality including:
//! - OffsetCommit/OffsetFetch operations
//! - Offset boundary conditions (earliest, latest, specific)
//! - Edge cases: metadata, reset policy, multi-partition, seek

mod boundaries;
mod commit_fetch;
mod edge_cases;

pub use boundaries::test_offset_boundaries;
pub use commit_fetch::test_offset_commit_fetch;
pub use edge_cases::{
    test_fetch_offset_new_group, test_offset_commit_multi_partition,
    test_offset_commit_with_metadata, test_offset_reset_policy, test_offset_seek,
};
