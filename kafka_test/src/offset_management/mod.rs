//! Offset management E2E tests
//!
//! Tests for Kafka offset management functionality including:
//! - OffsetCommit/OffsetFetch operations
//! - Offset boundary conditions (earliest, latest, specific)

mod boundaries;
mod commit_fetch;

pub use boundaries::test_offset_boundaries;
pub use commit_fetch::test_offset_commit_fetch;
