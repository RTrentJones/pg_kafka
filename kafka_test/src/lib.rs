//! pg_kafka E2E Test Suite
//!
//! This library provides comprehensive end-to-end tests for pg_kafka,
//! organized by test flow:
//!
//! - **producer**: Producer functionality tests (basic, batch)
//! - **consumer**: Consumer functionality tests (basic, multiple, from offset)
//! - **offset_management**: Offset commit/fetch and boundary tests
//! - **consumer_group**: Consumer group lifecycle tests
//! - **partition**: Multi-partition functionality tests
//!
//! ## Usage
//!
//! Run all tests via the main binary:
//! ```bash
//! cargo run --release
//! ```
//!
//! Or use individual test modules in custom test harnesses.

pub mod common;
pub mod consumer;
pub mod consumer_group;
pub mod offset_management;
pub mod partition;
pub mod producer;

// Re-export test functions for convenience
pub use consumer::{
    test_consumer_basic, test_consumer_from_offset, test_consumer_multiple_messages,
};
pub use consumer_group::test_consumer_group_lifecycle;
pub use offset_management::{test_offset_boundaries, test_offset_commit_fetch};
pub use partition::test_multi_partition_produce;
pub use producer::{test_batch_produce, test_producer};
