//! Error path E2E tests
//!
//! Tests for error conditions and edge cases in Kafka protocol handling.
//! These tests verify that pg_kafka returns appropriate error codes
//! when given invalid inputs.

mod api_errors;
mod bug_hunt;
mod consumer_errors;
mod coordinator_errors;
mod coordinator_tests;
mod fetch_errors;
mod kafka_parity;
mod lifecycle_tests;
mod listener_tests;
mod metrics_tests;
mod offset_errors;
mod producer_errors;
mod review_regressions;
mod storage_tests;

/// The `error_paths` tests, in run order.
pub fn tests() -> Vec<crate::registry::TestDef> {
    crate::test_defs!["error_paths";
        review_regressions::test_group_rebalance_keeps_old_generation_usable,
        coordinator_tests::test_join_group_held_until_all_members_rejoin,
        coordinator_tests::test_follower_sync_held_until_leader_syncs,
        coordinator_tests::test_held_join_completed_by_leave_group,
        coordinator_tests::test_held_join_completed_by_session_timeout_sweep: sequential,
        review_regressions::test_headers_keep_their_order,
        review_regressions::test_auto_create_rejects_invalid_topic_name,
        review_regressions::test_end_txn_retry_is_idempotent,
        review_regressions::test_timed_out_transaction_cannot_be_resumed,
        review_regressions::test_huge_retention_ms_does_not_break_sweep,
        review_regressions::test_corrupt_acks0_produce_sends_no_reply,
        review_regressions::test_produce_two_batches_in_one_partition_is_rejected,
        review_regressions::test_fetch_advertised_minimum_is_usable,
        review_regressions::test_api_versions_above_range_negotiates_down,
        review_regressions::test_fenced_producer_cannot_write,
        review_regressions::test_active_idempotent_producer_survives_retention_sweep,
        storage_tests::test_fetch_bounds_from_single_statement,
        storage_tests::test_fetch_honours_request_max_bytes,
        storage_tests::test_worker_runs_as_least_privilege_role: sequential,
        storage_tests::test_retention_sweep_catches_up_on_backlog,
        review_regressions::test_producer_restart_new_epoch_records_are_stored,
        listener_tests::test_listener_idle_long_poll_does_not_refetch: sequential,
        listener_tests::test_listener_produce_wakes_long_poll_promptly: sequential,
        listener_tests::test_listener_read_committed_long_poll_wakes_on_commit: sequential,
        listener_tests::test_listener_pipelined_requests_beyond_in_flight_cap: sequential,
        lifecycle_tests::test_worker_restarts_after_terminate: sequential, // takes the broker down for ~5 s
        lifecycle_tests::test_failed_request_rolls_back_alone,
        lifecycle_tests::test_group_commit_concurrent_acks_visible,
        lifecycle_tests::test_group_commit_pipelined_produces_in_order,
        metrics_tests::test_metrics_count_produce_and_fetch,
        metrics_tests::test_consumer_lag_view,
        bug_hunt::test_fetch_into_cleaned_up_aborted_rows_is_in_range,
        bug_hunt::test_fetch_bad_partition_or_offset_returns_error,
        bug_hunt::test_txn_offset_commit_failure_reports_partition_errors,
        bug_hunt::test_create_topics_default_and_zero_partitions,
        bug_hunt::test_frame_budget_released_on_mid_frame_disconnect: sequential,
        consumer_errors::test_fetch_unknown_topic,
        consumer_errors::test_fetch_invalid_partition,
        consumer_errors::test_fetch_offset_out_of_range,
        consumer_errors::test_consume_empty_topic,
        coordinator_errors::test_heartbeat_after_leave,
        coordinator_errors::test_rejoin_after_leave,
        coordinator_errors::test_commit_new_group,
        coordinator_errors::test_empty_group_id,
        coordinator_errors::test_multiple_consumers_same_group,
        offset_errors::test_commit_offset_zero,
        offset_errors::test_fetch_uncommitted_offset,
        offset_errors::test_commit_then_fetch_offset,
        producer_errors::test_produce_invalid_partition,
        producer_errors::test_produce_any_partition,
        producer_errors::test_produce_empty_batch,
        producer_errors::test_produce_large_key,
        // Fetch API edge case tests
        fetch_errors::test_fetch_respects_min_one_message,
        fetch_errors::test_fetch_isolation_level_enforcement,
        fetch_errors::test_fetch_after_offset_reset,
        fetch_errors::test_fetch_with_deleted_topic,
        fetch_errors::test_fetch_from_new_partition,
        api_errors::test_unsupported_versions_get_typed_error_responses,
        api_errors::test_truncated_body_gets_invalid_request,
        api_errors::test_handler_failure_gets_typed_error_response: sequential, // revokes a worker privilege for a moment
        api_errors::test_request_sees_rows_committed_during_its_batch: sequential, // holds a lock on kafka.partition_offsets
        kafka_parity::test_produce_response_reports_log_start_offset,
        kafka_parity::test_list_offsets_without_a_matching_record_is_offset_minus_one,
        kafka_parity::test_offset_commit_to_a_missing_partition_is_refused,
        kafka_parity::test_idempotent_reinit_with_an_old_producer_id_gets_a_new_one,
        kafka_parity::test_empty_group_id_is_invalid_group_id,
    ]
}
