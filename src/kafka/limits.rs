//! Bounds on client-controlled state.
//!
//! Every count a client can choose and the broker then materialises in memory is capped here:
//! partitions per topic and partitions of all topics together (Metadata and the assignors
//! allocate one entry per partition; partitions cost nothing to create, so without the total
//! cap one CreateTopics of 10,000 topics of 10,000 partitions each would make every all-topics
//! Metadata request allocate about 17 GB, abort the worker with an allocation failure and send
//! Postgres into crash recovery, again after every restart), members per consumer group, the
//! number of groups the in-memory coordinator holds, the memory all groups' member state may
//! take together, and the work of one server-side assignment.
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

/// Default for `pg_kafka.max_total_partitions`: twenty topics at the default per-topic cap. An
/// all-topics Metadata response at that size takes about 35 MB in the worker.
pub const DEFAULT_MAX_TOTAL_PARTITIONS: i32 = 200_000;
/// Upper bound of `pg_kafka.max_total_partitions` (about 1.75 GB per all-topics Metadata).
pub const MAX_MAX_TOTAL_PARTITIONS: i32 = 10_000_000;

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

/// Most entries one server-side group assignment may take: the topics in the members'
/// subscriptions (repeats included), the topic entries of the sticky assignor's previous
/// assignments (also those naming no existing topic or listing no partitions) and the
/// partitions they list, and the partitions of the subscribed topics. A leader SyncGroup over
/// it is answered POLICY_VIOLATION before anything is computed, instead of stalling every
/// other client on the single DB thread while it runs. Range and round-robin run in about one
/// step per entry; the sticky assignor's balancing also depends on how the subscriptions
/// overlap and how far the previous assignment is from balanced, so it is bounded by
/// [`MAX_ASSIGNMENT_STEPS`] as well. A thousand members subscribed to a thousand topics
/// each stay well within it. Member ids (up to 32 KB of client id each) are not entries: the
/// assignors work on member indexes, so an id costs its length only when the ids are sorted
/// and copied into the result, once per assignment.
pub const MAX_ASSIGNMENT_WORK: usize = 2_000_000;

/// Most steps one server-side sticky assignment may take (a step is about one ordered-set or
/// map operation, some 30 to 110 ns measured, whatever the member ids' lengths; a partition
/// move counts 16), so a refused assignment has held the DB thread for about a second at
/// most. An ordinary rebalance (the members share a few distinct subscriptions) takes about
/// the subscription entries plus the partitions plus 16 per move, and moves each partition at
/// most about once, so an assignment within [`MAX_ASSIGNMENT_WORK`] and the default
/// `max_total_partitions` fits; with `max_total_partitions` raised past about 500,000, a
/// rebalance moving most of a group's partitions can exceed it. Many distinct, overlapping
/// subscriptions cost more per move (up to a look at every subscription class). Measured
/// with 999 members over 999 topics (just under the default `group_max_size`):
///
/// - member i subscribed to the topics from the i-th on, rebalancing from member 0 owning
///   everything: 1,779,066 steps at 8 partitions per topic, 2,385,850 at 40, 5,419,770 at
///   200 (about the default `max_total_partitions`) and 9,212,170 at 400, the same with
///   ordered and random member ids; it fits up to about 430 partitions per topic.
///   From a fresh group the shape takes 702,298 steps at 200.
/// - member i subscribed to the first i + 1 topics (its subscribers nest the other way):
///   from a fresh group 8,211,630 steps at 10 partitions per topic with member ids in
///   subscription order, refused at 12; with random ids it fits up to about 24. Rebalancing
///   from the member subscribed to everything owning everything it fits up to 10
///   partitions per topic with ordered ids and up to about 18 with random ids.
///
/// Past the budget the assignment is answered POLICY_VIOLATION, and the group stays in its
/// sync phase; the leader's retries in that generation get the refusal without computing
/// again.
pub const MAX_ASSIGNMENT_STEPS: usize = 10_000_000;

/// Default for `pg_kafka.max_group_memory`, in kB: 128 MB. The per-member and per-group caps
/// above still let the coordinator hold `max_consumer_groups * group_max_size` members of up
/// to 1 MiB each (about 1e14 bytes); members of groups that never sync stay in memory until
/// their session timeout (up to 30 minutes), so without a total bound a client could grow the
/// worker until an allocation fails, which aborts the backend and sends Postgres into crash
/// recovery. Real member state is small (a subscription and an assignment, typically well
/// under 10 kB per member), so 128 MB holds over ten thousand members.
pub const DEFAULT_MAX_GROUP_MEMORY_KB: i32 = 128 * 1024;
/// Lower bound of `pg_kafka.max_group_memory`, in kB: one member of the largest protocol list
/// must fit.
pub const MIN_MAX_GROUP_MEMORY_KB: i32 = 2 * 1024;
/// `DEFAULT_MAX_GROUP_MEMORY_KB` in bytes.
pub const DEFAULT_MAX_GROUP_MEMORY_BYTES: usize = DEFAULT_MAX_GROUP_MEMORY_KB as usize * 1024;

static MAX_PARTITIONS_PER_TOPIC: GucSetting<i32> =
    GucSetting::<i32>::new(DEFAULT_MAX_PARTITIONS_PER_TOPIC);
static MAX_TOTAL_PARTITIONS: GucSetting<i32> = GucSetting::<i32>::new(DEFAULT_MAX_TOTAL_PARTITIONS);
static GROUP_MAX_SIZE: GucSetting<i32> = GucSetting::<i32>::new(DEFAULT_GROUP_MAX_SIZE);
static MAX_CONSUMER_GROUPS: GucSetting<i32> = GucSetting::<i32>::new(DEFAULT_MAX_CONSUMER_GROUPS);
static MAX_GROUP_MEMORY_KB: GucSetting<i32> = GucSetting::<i32>::new(DEFAULT_MAX_GROUP_MEMORY_KB);

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
        c"pg_kafka.max_total_partitions",
        c"Maximum number of partitions of all topics together",
        c"A CreateTopics, CreatePartitions or auto-created topic (Metadata, Produce) that would take the total above it fails with POLICY_VIOLATION; topics already above it stay. Default 200000. Reloadable.",
        &MAX_TOTAL_PARTITIONS,
        1,
        MAX_MAX_TOTAL_PARTITIONS,
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
    GucRegistry::define_int_guc(
        c"pg_kafka.max_group_memory",
        c"Maximum memory consumer group member state may take in the coordinator",
        c"The member ids, protocols, metadata and assignments of all groups together; a group's copies of its leader's id and of its protocol name count as its longest member id and protocol name, so the count exceeds the state held by at most those two lengths per group. Empty groups (no members) are forgotten, least recently active first, to make room; a JoinGroup or a leader's SyncGroup whose state does not fit even so gets COORDINATOR_NOT_AVAILABLE (retriable; the memory frees as members leave or time out). Default 128MB. Reloadable.",
        &MAX_GROUP_MEMORY_KB,
        MIN_MAX_GROUP_MEMORY_KB,
        i32::MAX,
        GucContext::Sighup,
        GucFlags::UNIT_KB,
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

/// The configured cap on the partitions of all topics (`pg_kafka.max_total_partitions`).
pub fn max_total_partitions() -> i64 {
    i64::from(
        read(&MAX_TOTAL_PARTITIONS, DEFAULT_MAX_TOTAL_PARTITIONS)
            .clamp(1, MAX_MAX_TOTAL_PARTITIONS),
    )
}

/// Why adding `adding` partitions to the `current` total is refused, or `Ok` if the total
/// stays within `max` (adding nothing is always accepted).
pub fn check_total_partitions(current: i64, adding: i64, max: i64) -> Result<(), String> {
    if adding <= 0 || current.saturating_add(adding) <= max {
        Ok(())
    } else {
        Err(format!(
            "Adding {adding} partitions to the {current} of all topics exceeds the maximum of {max} (pg_kafka.max_total_partitions)"
        ))
    }
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

/// The configured memory budget for coordinator member state, in bytes
/// (`pg_kafka.max_group_memory`).
pub fn max_group_memory_bytes() -> usize {
    let kb = read(&MAX_GROUP_MEMORY_KB, DEFAULT_MAX_GROUP_MEMORY_KB).max(MIN_MAX_GROUP_MEMORY_KB);
    usize::try_from(kb)
        .expect("clamped to a positive i32")
        .saturating_mul(1024)
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
    fn test_check_total_partitions() {
        assert!(check_total_partitions(0, 10, 10).is_ok());
        assert!(check_total_partitions(5, 5, 10).is_ok());
        assert!(check_total_partitions(5, 6, 10).is_err());
        // Topics already above the cap stay; only growth is refused.
        assert!(check_total_partitions(20, 0, 10).is_ok());
        assert!(check_total_partitions(20, 1, 10).is_err());
        // 10,000 topics of 10,000 partitions.
        assert!(
            check_total_partitions(0, 100_000_000, DEFAULT_MAX_TOTAL_PARTITIONS.into()).is_err()
        );
        assert!(check_total_partitions(i64::MAX, i64::MAX, i64::MAX).is_ok());
        assert!(check_total_partitions(i64::MAX, 1, 10).is_err());
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
        assert_eq!(
            max_total_partitions(),
            i64::from(DEFAULT_MAX_TOTAL_PARTITIONS)
        );
        const { assert!(DEFAULT_MAX_TOTAL_PARTITIONS >= HARD_MAX_PARTITIONS) };
        assert_eq!(group_max_size(), DEFAULT_GROUP_MAX_SIZE as usize);
        assert_eq!(max_consumer_groups(), DEFAULT_MAX_CONSUMER_GROUPS as usize);
        assert_eq!(max_group_memory_bytes(), DEFAULT_MAX_GROUP_MEMORY_BYTES);
        assert_eq!(DEFAULT_MAX_GROUP_MEMORY_BYTES, 128 << 20);
        // The smallest budget holds one member of the largest protocol list.
        assert!(MIN_MAX_GROUP_MEMORY_KB as usize * 1024 >= MAX_MEMBER_PROTOCOL_BYTES + (64 << 10));
        assert_eq!(auto_create_partitions(3), 3);
        assert_eq!(
            auto_create_partitions(50_000),
            DEFAULT_MAX_PARTITIONS_PER_TOPIC
        );
    }
}
