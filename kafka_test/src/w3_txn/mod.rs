//! Wave 3, stream `txn`: fetch past invisible offsets, ListOffsets isolation and
//! MAX_TIMESTAMP, OffsetFetch require_stable, Kafka's idempotent producer model,
//! transaction validation and RecordBatch grouping.

pub mod consume;
pub mod protocol;
pub mod wire;

/// The `w3_txn` tests, in run order.
pub fn tests() -> Vec<crate::registry::TestDef> {
    crate::test_defs!["w3_txn";
        // idempotence, transaction validation, batching (kafka_test/src/w3_txn).
        // Not parallel safe: the cleanup test runs the retention sweep (deletes every
        // aborted row), the idempotence test restarts the listener.
        consume::test_w3_read_committed_passes_aborted_tail,
        consume::test_w3_read_committed_passes_aborted_middle,
        consume::test_w3_consumers_pass_cleaned_up_aborted_rows: sequential,
        consume::test_w3_consumers_pass_delete_records_gap,
        consume::test_w3_read_committed_stops_at_open_transaction,
        protocol::test_w3_list_offsets_isolation_max_timestamp_and_partitions,
        protocol::test_w3_offset_fetch_require_stable,
        protocol::test_w3_idempotent_producer_follows_kafka: sequential,
        protocol::test_w3_idempotent_sequence_wraps,
        protocol::test_w3_transaction_validation,
        protocol::test_w3_fetch_groups_records_into_batches,
    ]
}
