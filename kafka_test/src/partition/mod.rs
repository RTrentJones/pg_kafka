//! Partition E2E tests
//!
//! Tests for Kafka multi-partition functionality including:
//! - Multi-partition topic support
//! - Partition-specific message routing
//! - Independent offset sequences per partition
//! - Key-based partition routing (Phase 7)

mod key_routing;
mod multi_partition;
mod routing_edge_cases;

pub use key_routing::{
    test_key_distribution, test_key_routing_deterministic, test_null_key_distribution,
};
pub use multi_partition::test_multi_partition_produce;
pub use routing_edge_cases::{
    test_empty_vs_null_key_routing, test_key_routing_across_producers,
    test_key_routing_after_partition_expansion, test_large_key_routing,
    test_special_character_key_routing,
};
