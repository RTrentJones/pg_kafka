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

pub use atomicity::{
    test_abort_transaction_discards_pending_offsets, test_add_partitions_to_txn_idempotent,
    test_concurrent_transactions_same_producer, test_producer_fencing_mid_transaction,
    test_transaction_boundary_isolation, test_transaction_partial_failure_atomicity,
    test_transaction_timeout_auto_abort, test_txn_offset_commit_visibility_timing,
};
pub use basic::{
    test_producer_fencing, test_transactional_batch, test_transactional_producer_abort,
    test_transactional_producer_commit,
};
pub use eos::test_txn_offset_commit;
pub use isolation::{
    test_read_committed_after_commit, test_read_committed_filters_pending,
    test_read_uncommitted_sees_pending,
};
