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

/// The `partition` tests, in run order.
pub fn tests() -> Vec<crate::registry::TestDef> {
    crate::test_defs!["partition";
        multi_partition::test_multi_partition_produce,
        key_routing::test_key_routing_deterministic,
        key_routing::test_key_distribution,
        key_routing::test_null_key_distribution,
        // Partition routing edge cases
        routing_edge_cases::test_large_key_routing,
        routing_edge_cases::test_special_character_key_routing,
        routing_edge_cases::test_empty_vs_null_key_routing,
        routing_edge_cases::test_key_routing_after_partition_expansion,
        routing_edge_cases::test_key_routing_across_producers,
    ]
}
