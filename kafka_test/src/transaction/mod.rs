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
mod eos;
mod isolation;

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
    ]
}
