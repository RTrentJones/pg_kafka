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
pub mod setup;

// Test modules
pub mod admin;
pub mod compression;
pub mod concurrent;
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

// Re-export infrastructure
pub use assertions::*;
pub use fixtures::*;
pub use setup::TestContext;

// Re-export test functions for convenience
pub use consumer::{
    test_consumer_basic, test_consumer_from_offset, test_consumer_multiple_messages,
};
pub use consumer_group::{
    test_consumer_group_lifecycle, test_find_coordinator_bootstrap, test_group_state_transitions,
    test_heartbeat_during_rebalance_window, test_heartbeat_keeps_membership,
    test_leave_during_rebalance, test_multiple_concurrent_timeouts,
    test_partition_assignment_strategies, test_rapid_rebalance_cycles, test_rebalance_after_leave,
    test_rebalance_mixed_timeout_values, test_rebalance_with_minimal_session_timeout,
    test_session_timeout_rebalance,
};
pub use offset_management::{
    test_fetch_offset_new_group, test_offset_boundaries, test_offset_commit_fetch,
    test_offset_commit_multi_partition, test_offset_commit_with_metadata, test_offset_reset_policy,
    test_offset_seek,
};
pub use partition::{
    test_empty_vs_null_key_routing, test_key_distribution, test_key_routing_across_producers,
    test_key_routing_after_partition_expansion, test_key_routing_deterministic,
    test_large_key_routing, test_multi_partition_produce, test_null_key_distribution,
    test_special_character_key_routing,
};
pub use producer::{test_batch_produce, test_producer};

// Re-export new test functions
pub use concurrent::{
    test_concurrent_producers_different_partitions, test_concurrent_producers_same_topic,
    test_consumer_catches_up, test_consumer_group_two_members, test_consumer_rejoin_after_leave,
    test_coordinator_state_race, test_group_rejoin_race, test_multiple_consumer_groups,
    test_offset_commit_race, test_partition_assignment_race, test_produce_consume_race,
    test_produce_while_consuming, test_request_pipelining,
};
pub use edge_cases::{
    test_batch_1000_messages, test_consume_empty_partition, test_consumer_group_empty,
    test_fetch_committed_no_history, test_high_offset_values, test_large_message_key,
    test_large_message_value, test_list_offsets_empty_topic, test_offset_zero_boundary,
    test_partition_zero, test_single_partition_topic,
};
pub use error_paths::{
    test_commit_new_group, test_commit_offset_zero, test_commit_then_fetch_offset,
    test_consume_empty_topic, test_empty_group_id, test_fetch_after_offset_reset,
    test_fetch_from_new_partition, test_fetch_invalid_partition,
    test_fetch_isolation_level_enforcement, test_fetch_offset_out_of_range,
    test_fetch_respects_min_one_message, test_fetch_uncommitted_offset, test_fetch_unknown_topic,
    test_fetch_with_deleted_topic, test_heartbeat_after_leave, test_multiple_consumers_same_group,
    test_produce_any_partition, test_produce_empty_batch, test_produce_invalid_partition,
    test_produce_large_key, test_rejoin_after_leave,
};
pub use negative::{
    test_connection_refused, test_duplicate_consumer_join, test_invalid_group_id,
    test_produce_timeout,
};
pub use performance::{
    test_batch_vs_single_performance, test_concurrent_connection_scaling,
    test_consume_throughput_baseline, test_large_batch_throughput, test_long_poll_cpu_efficiency,
    test_produce_latency_percentiles, test_produce_throughput_baseline,
};

// Admin API tests
pub use admin::{
    test_create_multiple_topics, test_create_partitions, test_create_partitions_cannot_decrease,
    test_create_partitions_not_found, test_create_topic, test_create_topic_already_exists,
    test_create_topic_invalid_name, test_create_topic_invalid_partitions,
    test_create_topic_with_config, test_delete_group_empty, test_delete_group_idempotent,
    test_delete_group_non_empty, test_delete_topic, test_delete_topic_not_found,
};

// Long polling tests
pub use long_poll::{
    test_long_poll_auto_commit_interval, test_long_poll_consumer_disconnect,
    test_long_poll_immediate_return, test_long_poll_min_bytes_threshold,
    test_long_poll_multiple_consumers_same_partition, test_long_poll_multiple_waiters,
    test_long_poll_producer_wakeup, test_long_poll_timeout, test_long_poll_timeout_precision,
};

// Compression tests (Phase 8)
pub use compression::{
    test_compressed_producer_gzip, test_compressed_producer_lz4, test_compressed_producer_snappy,
    test_compressed_producer_zstd, test_compression_incompressible_data,
    test_compression_mixed_formats, test_compression_ratio_verification,
    test_compression_roundtrip, test_compression_small_message_overhead,
    test_large_value_compression,
};

// Idempotent producer tests (Phase 9)
pub use idempotent::{
    test_idempotent_concurrent_producers, test_idempotent_high_sequence,
    test_idempotent_multi_partition, test_idempotent_producer_basic,
    test_idempotent_producer_epoch_bump, test_idempotent_producer_restart,
    test_true_deduplication_manual_replay,
};

// Transaction tests (Phase 10)
pub use transaction::{
    test_abort_transaction_discards_pending_offsets, test_add_partitions_to_txn_idempotent,
    test_concurrent_transactions_same_producer, test_producer_fencing,
    test_producer_fencing_mid_transaction, test_read_committed_after_commit,
    test_read_committed_filters_pending, test_read_uncommitted_sees_pending,
    test_transaction_boundary_isolation, test_transaction_partial_failure_atomicity,
    test_transaction_timeout_auto_abort, test_transactional_batch,
    test_transactional_producer_abort, test_transactional_producer_commit, test_txn_offset_commit,
    test_txn_offset_commit_visibility_timing,
};

// Shadow mode tests (Phase 11)
pub use shadow::{
    // Transaction integration
    test_aborted_transaction_not_forwarded,
    test_committed_transaction_forwarded,
    // Percentage routing
    test_deterministic_routing,
    // Dial-up tests
    test_dialup_0_percent,
    test_dialup_100_percent,
    test_dialup_10_percent,
    test_dialup_25_percent,
    test_dialup_50_percent,
    test_dialup_75_percent,
    // Basic forwarding
    test_dual_write_async,
    // Error handling
    test_dual_write_external_down,
    test_dual_write_sync,
    test_external_only_fallback,
    test_external_only_mode,
    test_fifty_percent_forwarding,
    test_hundred_percent_forwarding,
    test_local_only_mode,
    // Replay
    test_replay_historical_messages,
    // Topic mapping
    test_topic_name_mapping,
    test_zero_percent_forwarding,
};

// Metadata API tests (Phase 4)
pub use metadata::{
    test_metadata_all_topics, test_metadata_nonexistent_topic, test_metadata_refresh_after_create,
};

// Protocol compliance tests (Phase 4)
pub use protocol::{
    test_api_versions_negotiation, test_correlation_id_preserved, test_protocol_request_pipelining,
    test_unknown_api_key_handling,
};
