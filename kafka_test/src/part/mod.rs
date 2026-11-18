//! Stream `part` (wave 2): time-partitioned `kafka.messages` and retention by
//! dropping whole time partitions (ADR-001).

pub mod time_partitions;

pub use time_partitions::{
    test_time_partition_maintenance_and_least_privilege,
    test_time_partition_retention_drops_whole_partitions,
    test_time_partition_retention_waits_for_pending_transaction,
};
