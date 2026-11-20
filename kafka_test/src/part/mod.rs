//! Time-partitioned `kafka.messages` and retention by dropping whole time partitions
//! (ADR-001).

mod lock_races;
mod sweep_concurrency;
pub mod time_partitions;

/// The `part` tests, in run order.
pub fn tests() -> Vec<crate::registry::TestDef> {
    crate::test_defs!["part";
        // All sequential: they create and drop leaves of kafka.messages and run the
        // retention sweep.
        time_partitions::test_time_partition_retention_drops_whole_partitions: sequential,
        time_partitions::test_time_partition_retention_waits_for_pending_transaction: sequential,
        time_partitions::test_time_partition_maintenance_and_least_privilege: sequential,
        time_partitions::test_retention_without_offset_counter_keeps_offsets: sequential,
        lock_races::test_partition_maintenance_lock_waits_are_bounded: sequential,
        lock_races::test_end_txn_during_default_leaf_move: sequential,
        lock_races::test_wide_maintenance_moving_default_rows_under_load: sequential,
        sweep_concurrency::test_concurrent_sweeps_and_maintenance_do_not_deadlock: sequential,
        sweep_concurrency::test_worker_sweep_skips_while_maintenance_lock_held: sequential,
    ]
}
