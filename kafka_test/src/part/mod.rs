//! Time-partitioned `kafka.messages` and retention by dropping whole time partitions
//! (ADR-001).

mod drop_extension;
pub(crate) use drop_extension::MAINTENANCE_LOCK_KEY;
mod lock_order;
mod lock_races;
mod sweep_budget;
mod sweep_concurrency;
mod sweep_cost;
mod sweep_datestyle;
mod sweep_isolation;
mod sweep_liveness;
mod sweep_races;
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
        sweep_cost::test_sweep_drop_recheck_is_linear_in_the_leaf_topics: sequential,
        sweep_budget::test_system_guc_guard_restores_on_every_path: sequential,
        sweep_budget::test_sweep_probe_rotation_reaches_every_leaf: sequential,
        sweep_budget::test_sweep_drops_bounded_behind_a_held_reader: sequential,
        sweep_budget::test_sweep_aborted_cleanup_reaches_every_leaf: sequential,
        sweep_budget::test_sweep_expiry_reaches_the_current_leaf_on_a_spent_budget: sequential,
        sweep_budget::test_sweep_expiry_keeps_its_share_behind_an_aborted_backlog: sequential,
        sweep_budget::test_sweep_expiry_items_do_not_grow_with_the_leaves: sequential,
        sweep_budget::test_sweep_probe_of_many_expired_topics_stays_within_its_budget: sequential,
        sweep_budget::test_sweep_prunes_producers_within_its_budget: sequential,
        sweep_budget::test_sweep_trims_reach_every_kept_leaf: sequential,
        sweep_budget::test_sweep_producer_prune_reads_through_indexes: sequential,
        sweep_budget::test_sweep_forgets_a_deferred_drop_when_retention_is_raised: sequential,
        sweep_datestyle::test_sweep_bounds_survive_the_session_datestyle: sequential,
        sweep_isolation::test_sweep_keeps_rows_committed_during_a_repeatable_read_drop: sequential,
        sweep_isolation::test_worker_sweep_drops_under_a_repeatable_read_default: sequential,
        sweep_races::test_sweep_prune_keeps_a_producer_revived_after_its_snapshot: sequential,
        sweep_races::test_sweep_log_start_never_passes_a_concurrent_produce: sequential,
        sweep_races::test_sweep_prune_keeps_a_producer_whose_produce_is_open: sequential,
        sweep_races::test_sweep_prune_then_produce_keeps_producer_state: sequential,
        sweep_races::test_sweep_never_deadlocks_with_a_batch_writing_two_rows: sequential,
        sweep_races::test_sweep_never_deadlocks_with_a_batch_holding_a_counter: sequential,
        sweep_races::test_sweep_reaper_never_deadlocks_with_an_ack_batch: sequential,
        sweep_liveness::test_partition_ddl_ignores_locks_on_other_leaves: sequential,
    ]
}
