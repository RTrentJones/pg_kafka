//! Time-partitioned `kafka.messages` and retention by dropping whole time partitions
//! (ADR-001).

mod drop_extension;
mod lock_order;
mod lock_races;
mod sweep_budget;
mod sweep_concurrency;
mod sweep_cost;
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
        lock_races::test_rows_reaching_default_during_leaf_lock_wait: sequential,
        lock_order::test_partition_maintenance_locks_parent_before_leaves: sequential,
        drop_extension::test_partition_maintenance_vs_drop_extension: sequential,
        drop_extension::test_retention_sweep_waits_for_drop_extension_first: sequential,
        drop_extension::test_maintenance_functions_refuse_public: sequential,
        sweep_concurrency::test_concurrent_sweeps_and_maintenance_do_not_deadlock: sequential,
        sweep_concurrency::test_worker_sweep_skips_while_maintenance_lock_held: sequential,
        sweep_cost::test_sweep_reads_only_what_it_deletes: sequential,
        sweep_cost::test_sweep_reaps_outbox_rows_of_removed_messages: sequential,
        sweep_budget::test_sweep_probe_rotation_reaches_every_leaf: sequential,
    ]
}
