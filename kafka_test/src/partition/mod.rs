//! Partition E2E tests
//!
//! Tests for Kafka multi-partition functionality including:
//! - Multi-partition topic support
//! - Partition-specific message routing
//! - Independent offset sequences per partition
//! - Key-based partition routing (Phase 7)

mod key_routing;
mod multi_partition;

pub use key_routing::{
    test_key_distribution, test_key_routing_deterministic, test_null_key_distribution,
};
pub use multi_partition::test_multi_partition_produce;
