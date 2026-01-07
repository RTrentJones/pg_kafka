//! Partition E2E tests
//!
//! Tests for Kafka multi-partition functionality including:
//! - Multi-partition topic support
//! - Partition-specific message routing
//! - Independent offset sequences per partition

mod multi_partition;

pub use multi_partition::test_multi_partition_produce;
