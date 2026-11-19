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
//! - **concurrent**: Concurrency tests (8 tests)
//! - **negative**: Expected failure tests (4 tests)
//! - **performance**: Throughput baseline tests (3 tests)
//! - **long_poll**: Long polling tests (4 tests)
//! - **compression**: Compression tests (5 tests)
//! - **idempotent**: Idempotent producer tests (1 test)
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
pub mod registry;
pub mod setup;

// Test modules
pub mod admin;
pub mod compression;
pub mod concurrent;
pub mod configs;
pub mod consumer;
pub mod consumer_group;
pub mod edge_cases;
pub mod error_paths;
pub mod idempotent;
pub mod long_poll;
pub mod metadata;
pub mod negative;
pub mod offset_management;
pub mod partition;
pub mod performance;
pub mod producer;
pub mod protocol;
pub mod shadow;
pub mod transaction;
// Stream `part` (wave 2): time-partitioned kafka.messages
pub mod part;
// auth stream: TLS + SASL listener tests
pub mod auth;
// wave3 worker: lifecycle, locking, maintenance races (kafka_test/src/w3_worker)
pub mod w3_worker;

// Re-export infrastructure
pub use assertions::*;
pub use fixtures::*;
pub use setup::TestContext;

pub mod w3_groups;
pub mod w3_net;
pub mod w3_shadow;
pub mod w3_txn;

/// Every registered test, category by category, in run order.
pub fn all_tests() -> Vec<registry::TestDef> {
    [
        admin::tests(),
        producer::tests(),
        consumer::tests(),
        offset_management::tests(),
        consumer_group::tests(),
        partition::tests(),
        error_paths::tests(),
        edge_cases::tests(),
        concurrent::tests(),
        negative::tests(),
        performance::tests(),
        long_poll::tests(),
        compression::tests(),
        idempotent::tests(),
        transaction::tests(),
        shadow::tests(),
        metadata::tests(),
        protocol::tests(),
        part::tests(),
        auth::tests(),
        w3_groups::tests(),
        w3_txn::tests(),
        w3_net::tests(),
        w3_shadow::tests(),
        w3_worker::tests(),
    ]
    .into_iter()
    .flatten()
    .collect()
}
