//! Shadow mode E2E tests (Phase 11)
//!
//! This module contains end-to-end tests for pg_kafka's shadow mode functionality,
//! which enables forwarding messages to an external Kafka cluster.
//!
//! ## Test Categories
//!
//! - **Basic Forwarding**: DualWrite/ExternalOnly modes with sync/async
//! - **Percentage Routing**: 0%, 50%, 100% routing with murmur2 hash
//! - **Dial-Up Tests**: High-volume tests (500 messages) at various percentages
//! - **Topic Mapping**: External topic name mapping
//! - **Transaction Integration**: Commit/abort behavior with shadow mode
//! - **Error Handling**: External Kafka down/recovery
//! - **Replay**: Historical message replay
//!
//! ## Requirements
//!
//! These tests require an external Kafka broker running on port 9093.
//! Use `docker-compose up -d` to start both pg_kafka and external Kafka.

pub mod assertions;
pub mod basic_forwarding;
pub mod dialup;
pub mod error_handling;
pub mod external_client;
pub mod helpers;
mod lag_view;
pub mod no_duplicates;
mod outbox_robustness;
pub mod percentage_routing;
pub mod pipelined;
pub mod replay;
pub mod rollback;
pub mod topic_mapping;
pub mod transaction_integration;

/// The `shadow` tests, in run order.
pub fn tests() -> Vec<crate::registry::TestDef> {
    crate::test_defs!["shadow";
        // Basic forwarding tests
        basic_forwarding::test_dual_write_sync: sequential,
        basic_forwarding::test_dual_write_async: sequential,
        no_duplicates::test_async_forwarding_no_duplicates_on_slow_ack: sequential, // Mutates global GUC (test_forward_ack_delay_ms)
        basic_forwarding::test_external_only_mode: sequential,
        basic_forwarding::test_local_only_mode: sequential,
        basic_forwarding::test_reload_clears_deleted_topic_config: sequential,
        basic_forwarding::test_outbox_row_written_and_finalized: sequential,
        // Percentage routing tests
        percentage_routing::test_zero_percent_forwarding: sequential,
        percentage_routing::test_hundred_percent_forwarding: sequential,
        percentage_routing::test_fifty_percent_forwarding: sequential,
        percentage_routing::test_deterministic_routing: sequential,
        // Dial-up tests (high volume)
        dialup::test_dialup_0_percent: sequential,
        dialup::test_dialup_10_percent: sequential,
        dialup::test_dialup_25_percent: sequential,
        dialup::test_dialup_50_percent: sequential,
        dialup::test_dialup_75_percent: sequential,
        dialup::test_dialup_100_percent: sequential,
        // Topic mapping test
        topic_mapping::test_topic_name_mapping: sequential,
        transaction_integration::test_committed_transaction_forwards_without_ram_buffer: sequential,
        transaction_integration::test_committed_transaction_forwarded: sequential,
        transaction_integration::test_aborted_transaction_not_forwarded: sequential,
        transaction_integration::test_committed_transaction_uses_durable_outbox: sequential,
        transaction_integration::test_committed_txn_leaves_no_orphan_outbox_rows: sequential,
        transaction_integration::test_large_committed_transaction_is_enqueued_exactly_once: sequential, // measures the worker's memory
        // Error handling tests
        error_handling::test_dual_write_external_down: sequential, // Pauses the external broker
        error_handling::test_external_only_fallback: sequential, // Mutates shadow_max_retries, shadow_retry_backoff_ms
        error_handling::test_shadow_unbuildable_settings_dead_letter_after_a_working_producer: sequential, // Mutates shadow_bootstrap_servers, shadow_security_protocol, shadow_max_retries, shadow_retry_backoff_ms
        error_handling::test_external_primary_dead_letter_serves_local: sequential,
        // Replay test
        replay::test_replay_historical_messages: sequential,
        replay::test_replay_skips_aborted_records: sequential,
        // Sync forwarding happens only after the local commit
        rollback::test_sync_forward_skips_rolled_back_request: sequential,
        pipelined::test_shadow_forward_throughput: sequential,
        pipelined::test_shadow_forwards_headers_and_timestamps: sequential,
        pipelined::test_shadow_order_preserved_across_a_retried_failure: sequential,
        pipelined::test_shadow_order_preserved_across_a_delivery_timeout: sequential, // Mutates global GUC (test_shadow_delivery_timeout_ms)
        pipelined::test_shadow_sync_stall_is_bounded_to_its_own_response: sequential,
        pipelined::test_shadow_gucs_take_effect_on_reload: sequential,
        lag_view::test_shadow_lag_view_is_per_partition,
        // Outbox under stress and failure
        outbox_robustness::test_shadow_max_retries_refuses_zero: sequential,
        outbox_robustness::test_shadow_backlog_does_not_starve_other_topics: sequential,
        outbox_robustness::test_shadow_full_producer_queue_does_not_block_sync_timeouts: sequential,
        outbox_robustness::test_shadow_backlogged_records_are_answered_before_their_in_flight_entry_expires: sequential, // Mutates shadow_bootstrap_servers, test_shadow_delivery_timeout_ms
        outbox_robustness::test_shadow_timed_out_transaction_is_not_forwarded: sequential,
        outbox_robustness::test_shadow_ack_application_error_loses_no_ack: sequential,
        outbox_robustness::test_shadow_failed_tick_commit_loses_no_ack: sequential,
        outbox_robustness::test_shadow_stuck_partitions_do_not_throttle_a_backlog: sequential,
        outbox_robustness::test_shadow_unconfigured_broker_does_not_spin_the_worker: sequential,
        outbox_robustness::test_shadow_claims_stay_fast_with_a_full_in_flight_window: sequential, // Mutates global GUCs (test_forward_ack_delay_ms, shadow_bootstrap_servers)
        outbox_robustness::test_shadow_claims_stay_fast_behind_a_large_backlog: sequential, // Mutates global GUCs (test_forward_ack_delay_ms, shadow_bootstrap_servers, jit, shadow_batch_size)
        outbox_robustness::test_shadow_claims_stay_fast_with_a_tiny_work_mem: sequential, // Mutates global GUCs (test_forward_ack_delay_ms, shadow_bootstrap_servers, work_mem, jit)
        outbox_robustness::test_shadow_claims_are_not_jit_compiled: sequential, // Mutates global GUCs (test_forward_ack_delay_ms, shadow_bootstrap_servers, jit*)
        outbox_robustness::test_shadow_worker_stays_idle_beside_a_blocked_partition: sequential, // Mutates shadow_bootstrap_servers; measures the worker's CPU
        outbox_robustness::test_shadow_worker_stays_idle_beside_dead_letters: sequential, // Mutates shadow_bootstrap_servers; measures the worker's CPU
        outbox_robustness::test_shadow_dead_letters_follow_the_retry_cap: sequential, // Mutates shadow_bootstrap_servers, shadow_max_retries
        outbox_robustness::test_shadow_failure_acks_make_dead_letters: sequential, // Mutates shadow_max_retries, shadow_retry_backoff_ms
    ]
}
