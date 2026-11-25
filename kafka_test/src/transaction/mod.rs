//! Transaction E2E tests (Phase 10)
//!
//! Tests for Kafka transaction support including:
//! - Transactional producer commit/abort flows
//! - Read isolation levels (read_committed vs read_uncommitted)
//! - Producer fencing (epoch bumping)
//! - Batch transactions
//! - Exactly-once semantics (EOS) with TxnOffsetCommit
//! - Transaction atomicity edge cases

mod atomicity;
mod basic;
mod coordinator_codes;
mod eos;
mod invisible_offsets;
mod isolation;
mod kafka_parity;
mod producer_epochs;
mod timeouts;
mod validation;

/// The `transaction` tests, in run order.
pub fn tests() -> Vec<crate::registry::TestDef> {
    crate::test_defs!["transaction";
        basic::test_transactional_producer_commit,
        basic::test_transactional_producer_abort,
        basic::test_transactional_batch,
        basic::test_producer_fencing,
        isolation::test_read_committed_filters_pending,
        isolation::test_read_uncommitted_sees_pending,
        isolation::test_read_committed_after_commit,
        isolation::test_reinit_aborts_in_flight_transaction,
        isolation::test_read_committed_clamped_to_lso,
        eos::test_txn_offset_commit,
        atomicity::test_transaction_timeout_auto_abort,
        atomicity::test_concurrent_transactions_same_producer,
        atomicity::test_producer_fencing_mid_transaction,
        atomicity::test_add_partitions_to_txn_idempotent,
        atomicity::test_txn_offset_commit_visibility_timing,
        atomicity::test_abort_transaction_discards_pending_offsets,
        atomicity::test_transaction_partial_failure_atomicity,
        atomicity::test_transaction_boundary_isolation,
        atomicity::test_transaction_honors_per_txn_timeout_ms,
        atomicity::test_producer_id_reallocated_on_epoch_exhaustion,
        invisible_offsets::test_read_committed_consumer_passes_aborted_tail,
        invisible_offsets::test_read_committed_consumer_passes_aborted_middle,
        invisible_offsets::test_consumers_pass_cleaned_up_aborted_rows: sequential,
        invisible_offsets::test_consumers_pass_delete_records_gap,
        invisible_offsets::test_read_committed_consumer_stops_at_open_transaction,
        validation::test_transactional_requests_are_validated,
        validation::test_transactional_produce_is_validated_per_partition,
        validation::test_transactional_batches_of_two_producers_are_refused,
        validation::test_non_transactional_batch_inside_an_open_transaction_is_refused,
        coordinator_codes::test_fenced_epoch_code_follows_the_request_version,
        coordinator_codes::test_unknown_transactional_id_is_invalid_producer_id_mapping,
        coordinator_codes::test_txn_offset_commit_needs_the_group_added,
        timeouts::test_second_transaction_gets_its_own_timeout,
        timeouts::test_timeout_abort_fences_an_exhausted_epoch,
        producer_epochs::test_init_producer_id_checks_the_presented_epoch,
        kafka_parity::test_end_txn_of_an_empty_transaction_is_invalid_txn_state,
        kafka_parity::test_txn_offset_commit_metadata_longer_than_4096_is_refused,
        kafka_parity::test_read_uncommitted_fetch_reports_the_last_stable_offset,
        kafka_parity::test_delete_groups_drops_pending_txn_offsets,
    ]
}
