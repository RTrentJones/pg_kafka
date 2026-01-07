//! pg_kafka E2E Test Suite
//!
//! A comprehensive end-to-end test framework for pg_kafka with:
//! - Test isolation via unique topic/group names
//! - Automatic cleanup via RAII
//! - Custom assertions for database verification
//! - Fixtures and builders for test data
//!
//! ## Test Categories
//!
//! - **producer**: Producer functionality tests (basic, batch)
//! - **consumer**: Consumer functionality tests (basic, multiple, from offset)
//! - **offset_management**: Offset commit/fetch and boundary tests
//! - **consumer_group**: Consumer group lifecycle tests
//! - **partition**: Multi-partition functionality tests
//! - **error_paths**: Error condition tests (16 tests)
//! - **edge_cases**: Boundary and edge case tests (11 tests)
//! - **concurrent**: Concurrency tests (7 tests)
//! - **negative**: Expected failure tests (4 tests)
//! - **performance**: Throughput baseline tests (3 tests)
//!
//! ## Usage
//!
//! ```bash
//! # Run all tests
//! cargo run --release
//!
//! # Run specific category
//! cargo run --release -- --category producer
//!
//! # Run single test
//! cargo run --release -- --test test_producer
//!
//! # JSON output for CI
//! cargo run --release -- --json
//! ```

// Infrastructure modules
pub mod assertions;
pub mod common;
pub mod fixtures;
pub mod setup;

// Test modules
pub mod concurrent;
pub mod consumer;
pub mod consumer_group;
pub mod edge_cases;
pub mod error_paths;
pub mod negative;
pub mod offset_management;
pub mod partition;
pub mod performance;
pub mod producer;

// Re-export infrastructure
pub use assertions::*;
pub use fixtures::*;
pub use setup::TestContext;

// Re-export test functions for convenience
pub use consumer::{
    test_consumer_basic, test_consumer_from_offset, test_consumer_multiple_messages,
};
pub use consumer_group::test_consumer_group_lifecycle;
pub use offset_management::{test_offset_boundaries, test_offset_commit_fetch};
pub use partition::test_multi_partition_produce;
pub use producer::{test_batch_produce, test_producer};

// Re-export new test functions
pub use concurrent::{
    test_concurrent_producers_different_partitions, test_concurrent_producers_same_topic,
    test_consumer_catches_up, test_consumer_group_two_members, test_consumer_rejoin_after_leave,
    test_multiple_consumer_groups, test_produce_while_consuming,
};
pub use edge_cases::{
    test_batch_1000_messages, test_consume_empty_partition, test_consumer_group_empty,
    test_fetch_committed_no_history, test_high_offset_values, test_large_message_key,
    test_large_message_value, test_list_offsets_empty_topic, test_offset_zero_boundary,
    test_partition_zero, test_single_partition_topic,
};
pub use error_paths::{
    test_commit_new_group, test_commit_offset_zero, test_commit_then_fetch_offset,
    test_consume_empty_topic, test_empty_group_id, test_fetch_invalid_partition,
    test_fetch_offset_out_of_range, test_fetch_uncommitted_offset, test_fetch_unknown_topic,
    test_heartbeat_after_leave, test_multiple_consumers_same_group, test_produce_any_partition,
    test_produce_empty_batch, test_produce_invalid_partition, test_produce_large_key,
    test_rejoin_after_leave,
};
pub use negative::{
    test_connection_refused, test_duplicate_consumer_join, test_invalid_group_id,
    test_produce_timeout,
};
pub use performance::{
    test_batch_vs_single_performance, test_consume_throughput_baseline,
    test_produce_throughput_baseline,
};
