//! Bounds on client-controlled state.
//!
//! Every count a client can choose and the broker then materialises in memory is capped here:
//! partitions per topic (Metadata and the assignors allocate one entry per partition, so an
//! unbounded count could abort the worker with an allocation failure and send Postgres into crash
//! recovery on every Metadata request), members per consumer group, and the number of groups the
//! in-memory coordinator holds.
//!
//! The GUCs are `Sighup`: the worker re-reads them on its config reload. Code reads them through
//! the accessors below, which also work in unit tests (they return the boot values there).

use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting};

/// Hard ceiling on a topic's partition count, whatever `pg_kafka.max_partitions_per_topic` says.
/// `kafka.topics` carries `CHECK (partitions BETWEEN 1 AND 100000)` with the same value, so a
/// stored count above it can only come from a DBA bypassing the constraint; readers still treat
/// such a row as invalid instead of allocating for it (see [`sane_partition_count`]).
pub const HARD_MAX_PARTITIONS: i32 = 100_000;

/// Default for `pg_kafka.max_partitions_per_topic` (the upper bound of
/// `pg_kafka.default_partitions`).
pub const DEFAULT_MAX_PARTITIONS_PER_TOPIC: i32 = 10_000;

/// Default for `pg_kafka.group_max_size` (Kafka's `group.max.size` defaults to unlimited; a bound
/// keeps one group's member table, which lives in worker memory, finite).
pub const DEFAULT_GROUP_MAX_SIZE: i32 = 1_000;
/// Upper bound of `pg_kafka.group_max_size`.
pub const MAX_GROUP_MAX_SIZE: i32 = 100_000;

/// Default for `pg_kafka.max_consumer_groups`, the number of groups the coordinator keeps in memory.
pub const DEFAULT_MAX_CONSUMER_GROUPS: i32 = 100_000;
/// Upper bound of `pg_kafka.max_consumer_groups`.
pub const MAX_MAX_CONSUMER_GROUPS: i32 = 1_000_000;

/// Largest protocol list (names + metadata, bytes) one member may register with JoinGroup. Kafka
/// stores a group's member metadata in one `__consumer_offsets` record, so it is bounded by
/// `message.max.bytes` (1 MiB by default); the same bound keeps a member from parking an
/// arbitrary amount of memory in the coordinator.
pub const MAX_MEMBER_PROTOCOL_BYTES: usize = 1024 * 1024;

/// Largest number of protocols (assignors) one member may advertise.
pub const MAX_MEMBER_PROTOCOLS: usize = 64;

static MAX_PARTITIONS_PER_TOPIC: GucSetting<i32> =
    GucSetting::<i32>::new(DEFAULT_MAX_PARTITIONS_PER_TOPIC);
static GROUP_MAX_SIZE: GucSetting<i32> = GucSetting::<i32>::new(DEFAULT_GROUP_MAX_SIZE);
static MAX_CONSUMER_GROUPS: GucSetting<i32> = GucSetting::<i32>::new(DEFAULT_MAX_CONSUMER_GROUPS);

/// Register the limit GUCs (called from `config::init`).
pub fn register_gucs() {
    GucRegistry::define_int_guc(
        c"pg_kafka.max_partitions_per_topic",
        c"Maximum partition count of a topic",
        c"CreateTopics and CreatePartitions above it fail with INVALID_PARTITIONS; auto-created topics use min(pg_kafka.default_partitions, this). Default 10000. Reloadable.",
        &MAX_PARTITIONS_PER_TOPIC,
        1,
        HARD_MAX_PARTITIONS,
        GucContext::Sighup,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"pg_kafka.group_max_size",
        c"Maximum number of members in one consumer group",
        c"A new member joining a full group gets GROUP_MAX_SIZE_REACHED (Kafka group.max.size). Default 1000. Reloadable.",
        &GROUP_MAX_SIZE,
        1,
        MAX_GROUP_MAX_SIZE,
        GucContext::Sighup,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"pg_kafka.max_consumer_groups",
        c"Maximum number of consumer groups held in memory",
        c"When full, least recently active Empty groups are dropped from memory until the count is below the cap (their committed offsets stay); if none is Empty, a JoinGroup for a new group gets COORDINATOR_NOT_AVAILABLE. Default 100000. Reloadable.",
        &MAX_CONSUMER_GROUPS,
        1,
        MAX_MAX_CONSUMER_GROUPS,
        GucContext::Sighup,
        GucFlags::default(),
    );
}

/// A GUC's current value. pgrx only allows GUC reads on the backend's main thread, so unit tests
/// (which run on many threads, with no backend) see the boot value.
#[cfg(not(test))]
fn read(setting: &GucSetting<i32>, _boot: i32) -> i32 {
    setting.get()
}

#[cfg(test)]
fn read(_setting: &GucSetting<i32>, boot: i32) -> i32 {
    boot
}

/// The configured per-topic partition cap (`pg_kafka.max_partitions_per_topic`).
pub fn max_partitions_per_topic() -> i32 {
    read(&MAX_PARTITIONS_PER_TOPIC, DEFAULT_MAX_PARTITIONS_PER_TOPIC).clamp(1, HARD_MAX_PARTITIONS)
}

/// The configured member cap per group (`pg_kafka.group_max_size`).
pub fn group_max_size() -> usize {
    usize::try_from(read(&GROUP_MAX_SIZE, DEFAULT_GROUP_MAX_SIZE).clamp(1, MAX_GROUP_MAX_SIZE))
        .expect("clamped to a positive i32")
}

/// The configured cap on in-memory groups (`pg_kafka.max_consumer_groups`).
pub fn max_consumer_groups() -> usize {
    usize::try_from(
        read(&MAX_CONSUMER_GROUPS, DEFAULT_MAX_CONSUMER_GROUPS).clamp(1, MAX_MAX_CONSUMER_GROUPS),
    )
    .expect("clamped to a positive i32")
}

/// Why a requested partition count is refused, or `Ok` if `1 <= count <= max`.
pub fn check_partition_count(count: i32, max: i32) -> Result<(), String> {
    if count < 1 {
        Err("Number of partitions must be at least 1".to_string())
    } else if count > max {
        Err(format!(
            "Number of partitions {count} exceeds the maximum of {max} (pg_kafka.max_partitions_per_topic)"
        ))
    } else {
        Ok(())
    }
}

/// A stored partition count that is safe to iterate over, or `None` for a corrupt value (below 1
/// or above [`HARD_MAX_PARTITIONS`]). The CHECK constraint on `kafka.topics.partitions` makes this
/// unreachable in practice; readers that allocate per partition use it so a bad row cannot take
/// the worker (and with it the whole server) down.
pub fn sane_partition_count(count: i32) -> Option<i32> {
    (1..=HARD_MAX_PARTITIONS).contains(&count).then_some(count)
}

/// The partition count for an auto-created topic: the broker default, capped at the limit.
pub fn auto_create_partitions(default_partitions: i32) -> i32 {
    default_partitions.clamp(1, max_partitions_per_topic())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_check_partition_count() {
        assert!(check_partition_count(1, 10).is_ok());
        assert!(check_partition_count(10, 10).is_ok());
        assert!(check_partition_count(0, 10).is_err());
        assert!(check_partition_count(-5, 10).is_err());
        assert!(check_partition_count(11, 10).is_err());
        assert!(check_partition_count(2_000_000_000, 10_000).is_err());
    }

    #[test]
    fn test_sane_partition_count() {
        assert_eq!(sane_partition_count(1), Some(1));
        assert_eq!(
            sane_partition_count(HARD_MAX_PARTITIONS),
            Some(HARD_MAX_PARTITIONS)
        );
        assert_eq!(sane_partition_count(0), None);
        assert_eq!(sane_partition_count(-1), None);
        assert_eq!(sane_partition_count(HARD_MAX_PARTITIONS + 1), None);
        assert_eq!(sane_partition_count(2_000_000_000), None);
    }

    #[test]
    fn test_defaults() {
        assert_eq!(max_partitions_per_topic(), DEFAULT_MAX_PARTITIONS_PER_TOPIC);
        assert_eq!(group_max_size(), DEFAULT_GROUP_MAX_SIZE as usize);
        assert_eq!(max_consumer_groups(), DEFAULT_MAX_CONSUMER_GROUPS as usize);
        assert_eq!(auto_create_partitions(3), 3);
        assert_eq!(
            auto_create_partitions(50_000),
            DEFAULT_MAX_PARTITIONS_PER_TOPIC
        );
    }
}
