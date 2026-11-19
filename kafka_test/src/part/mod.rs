//! Time-partitioned `kafka.messages` and retention by dropping whole time partitions
//! (ADR-001).

mod lock_races;
pub mod time_partitions;

/// The `part` tests, in run order.
pub fn tests() -> Vec<crate::registry::TestDef> {
    crate::test_defs!["part";
        // All sequential: they create and drop leaves of kafka.messages and run the
        // retention sweep.
        time_partitions::test_time_partition_retention_drops_whole_partitions: sequential,
        time_partitions::test_time_partition_retention_waits_for_pending_transaction: sequential,
        time_partitions::test_time_partition_maintenance_and_least_privilege: sequential,
        lock_races::test_w3_partition_maintenance_lock_waits_are_bounded: sequential,
        lock_races::test_w3_end_txn_during_default_leaf_move: sequential,
    ]
}
