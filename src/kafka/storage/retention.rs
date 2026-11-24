//! Pure planning logic for the expired-message part of the retention sweep.
//!
//! `PostgresStore::run_retention_sweep` (SPI, not unit-testable) expires messages by
//! dropping whole time partitions or deleting rows in bounded batches. The decisions it
//! makes (which retention window a topic has, what to do with a leaf, whether to run
//! another batch, which topic a pass starts from) live here so they are unit tested.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

/// Rows deleted per DELETE statement. Small enough that one statement holds the DB
/// thread briefly; the sweep runs as many batches as fit in [`RETENTION_PASS_BUDGET`].
pub const RETENTION_DELETE_BATCH: i64 = 10_000;

/// Wall-clock budget for one sweep pass. The sweep runs on the single worker DB
/// thread, so a pass must stay bounded: every step whose cost grows with the data
/// (the aborted-row cleanup, the leaf contents probes, the expired-message deletes,
/// the shadow-outbox reaping) checks it between statements, each statement itself
/// bounded ([`RETENTION_DELETE_BATCH`] rows, one leaf), and runs at least one
/// statement per pass ([`step_may_continue`]) so no step starves behind another's
/// backlog. A backlog larger than one pass can clear drains over the following passes.
pub const RETENTION_PASS_BUDGET: Duration = Duration::from_millis(1500);

/// The pass budget: [`RETENTION_PASS_BUDGET`], unless the test-only
/// `pg_kafka.test_retention_pass_budget_ms` (`test_override_ms` >= 0) replaces it.
pub fn pass_budget(test_override_ms: i32) -> Duration {
    u64::try_from(test_override_ms).map_or(RETENTION_PASS_BUDGET, Duration::from_millis)
}

/// Upper clamp for a retention window in milliseconds (1e13 ms, about 317 years).
/// Larger values, such as `i64::MAX` used for "forever", overflow the interval or
/// timestamp arithmetic in SQL and would abort the whole sweep.
pub const MAX_RETENTION_WINDOW_MS: i64 = 10_000_000_000_000;

const MS_PER_MINUTE: i64 = 60_000;
const MS_PER_HOUR: i64 = 60 * MS_PER_MINUTE;

/// The retention window (milliseconds) that applies to a topic, or `None` when its
/// messages are kept forever.
///
/// * a per-topic `retention.ms` >= 0 (kafka.topics.retention_ms) enforces that
///   window, even when the global sweep is disabled;
/// * a per-topic value < 0 pins the topic to infinite retention, even when a global
///   window is set;
/// * no per-topic value (NULL) falls back to `pg_kafka.message_retention_hours`,
///   where 0 or less means keep forever.
pub fn effective_retention_window_ms(
    topic_retention_ms: Option<i64>,
    global_retention_hours: i32,
) -> Option<i64> {
    match topic_retention_ms {
        Some(ms) if ms >= 0 => Some(ms.min(MAX_RETENTION_WINDOW_MS)),
        None if global_retention_hours > 0 => {
            Some((i64::from(global_retention_hours) * MS_PER_HOUR).min(MAX_RETENTION_WINDOW_MS))
        }
        // A negative per-topic value, or no window at all: kept forever.
        Some(_) | None => None,
    }
}

/// Whether the sweep should run another delete batch for the current topic: the
/// last batch was full (so more expired rows may remain) and the pass is still
/// within its time budget.
pub fn should_run_another_batch(
    last_batch_deleted: u64,
    batch_size: i64,
    elapsed: Duration,
    budget: Duration,
) -> bool {
    u64::try_from(batch_size).is_ok_and(|size| size > 0 && last_batch_deleted >= size)
        && elapsed < budget
}

/// Whether a bounded sweep step may run another statement: always its first one in
/// a pass (so a step never starves behind another step's backlog), then only while the
/// pass is within its budget. A pass therefore costs at most the budget plus one
/// statement per step.
pub fn step_may_continue(statements_run: usize, elapsed: Duration, budget: Duration) -> bool {
    statements_run == 0 || elapsed < budget
}

/// Whether the whole-leaf drop step may try another leaf: a bounded step
/// ([`step_may_continue`]) that also stops after a drop that ran out of a lock wait,
/// since the session holding that lock blocks the next drop just as long.
pub fn drop_may_continue(
    attempts: usize,
    last_lock_timed_out: bool,
    elapsed: Duration,
    budget: Duration,
) -> bool {
    !last_lock_timed_out && step_may_continue(attempts, elapsed, budget)
}

/// Rotation cursor for the topic order, so a pass that runs out of budget on one
/// large backlog does not starve the topics after it: the next pass starts one
/// topic further along.
static TOPIC_CURSOR: AtomicUsize = AtomicUsize::new(0);

/// The index to start this pass's topic loop from (and advance the cursor).
pub fn next_start_index(topic_count: usize) -> usize {
    if topic_count == 0 {
        return 0;
    }
    TOPIC_CURSOR.fetch_add(1, Ordering::Relaxed) % topic_count
}

/// `items` rotated so that iteration starts at `start` and wraps around.
pub fn rotated<T: Clone>(items: &[T], start: usize) -> Vec<T> {
    if items.is_empty() {
        return Vec::new();
    }
    let start = start % items.len();
    items[start..]
        .iter()
        .chain(&items[..start])
        .cloned()
        .collect()
}

// Time partitions (ADR-001): kafka.messages is RANGE-partitioned on created_at. The
// decisions below drive `PostgresStore::run_retention_sweep`.

/// Key of the partition-maintenance advisory lock (`'pg_kafka'` as an int8, one less
/// than the schema lock). Every pass that deletes rows of kafka.messages or changes its
/// leaves holds it for its whole transaction: the retention sweep (taken before its
/// first statement), `kafka.ensure_message_partitions` and
/// `kafka.drop_message_partition` (through `kafka.try_maintenance_lock()`). A sweep that
/// deleted rows and then waited for ACCESS EXCLUSIVE on kafka.messages to drop a leaf
/// would otherwise deadlock with a concurrent sweep holding ROW EXCLUSIVE there and
/// waiting for those rows (or for this lock).
pub const MAINTENANCE_LOCK_KEY: i64 = 8_099_547_369_649_367_905;

/// How a retention sweep takes [`MAINTENANCE_LOCK_KEY`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaintenanceLockWait {
    /// Queue for it (bounded by the session's `lock_timeout`): an on-demand sweep,
    /// which holds no other lock yet, runs after the pass ahead of it.
    Wait,
    /// Take it only if it is free, else skip this pass: the worker, whose transaction
    /// holds the schema lock in SHARE mode, must never wait for it (a manual
    /// `create_message_partition` holding it may be waiting for the schema lock).
    Skip,
}

impl MaintenanceLockWait {
    /// The statement that takes the lock, returning one boolean column `locked`.
    pub fn lock_sql(self) -> &'static str {
        match self {
            MaintenanceLockWait::Wait => "SELECT true AS locked FROM pg_advisory_xact_lock($1)",
            MaintenanceLockWait::Skip => "SELECT pg_try_advisory_xact_lock($1) AS locked",
        }
    }
}

/// Default width of one time partition, minutes (`pg_kafka.message_partition_minutes`).
pub const DEFAULT_PARTITION_MINUTES: i32 = 1440;
/// Lower bound of `pg_kafka.message_partition_minutes` (5 minutes).
pub const MIN_PARTITION_MINUTES: i32 = 5;
/// Upper bound of `pg_kafka.message_partition_minutes` (30 days).
pub const MAX_PARTITION_MINUTES: i32 = 43_200;
/// Leaves the worker keeps ready beyond the current one.
pub const PARTITIONS_AHEAD: i32 = 2;
/// Upper bound on `kafka.message_partition_contents` probes per sweep pass (each costs
/// one btree descent per (topic, partition) present in the leaf). Leaves kept past
/// their window by a longer one stay candidates every pass, so the probes rotate
/// over them ([`PROBE_ROTATION`]) instead of always starting from the oldest.
pub const MAX_CONTENTS_PROBES_PER_PASS: usize = 64;

/// A rotation over a list that a budget-limited sweep step walks: each pass starts at
/// the first item the previous pass did not finish, whatever the list size, so a pass
/// cut short (by a cap, or by the pass budget another step may have spent) does not
/// walk the same leading items again, and consecutive passes cover every item.
pub struct Rotation(AtomicUsize);

impl Rotation {
    pub const fn new() -> Self {
        Rotation(AtomicUsize::new(0))
    }

    /// The index to start this pass from in a list of `items`.
    pub fn start(&self, items: usize) -> usize {
        probe_start(self.0.load(Ordering::Relaxed), items)
    }

    /// The pass that started at `start` finished `done` items: the next starts after them.
    pub fn record(&self, start: usize, done: usize) {
        self.0.store(start.wrapping_add(done), Ordering::Relaxed);
    }
}

impl Default for Rotation {
    fn default() -> Self {
        Rotation::new()
    }
}

/// Rotation of the contents probes over their candidates (see
/// [`MAX_CONTENTS_PROBES_PER_PASS`]); a pass records the probes it made.
pub static PROBE_ROTATION: Rotation = Rotation::new();

/// Rotation of the aborted-row cleanup over the leaves that may hold aborted rows past
/// the grace: each leaf costs at least one probe of its partial index, so a pass whose
/// budget runs out before the newest leaves must not start from the oldest again.
pub static ABORTED_ROTATION: Rotation = Rotation::new();

/// Where a pass over `items` starts, given a rotation cursor.
pub fn probe_start(cursor: usize, items: usize) -> usize {
    if items == 0 {
        return 0;
    }
    cursor % items
}

/// Whether a past leaf needs the full contents probe: only a leaf where some topic
/// can be wholly expired (`upper + shortest finite window <= now`) can be dropped
/// with rows in it or trimmed. Every other past leaf can only be dropped if it is
/// empty, which a cheap emptiness check answers. `None`: no topic has a finite
/// window (`pg_kafka.message_retention_hours = 0` and no per-topic `retention.ms`).
pub fn needs_contents_probe(upper_ms: i64, now_ms: i64, shortest_window_ms: Option<i64>) -> bool {
    wholly_expired(upper_ms, now_ms, shortest_window_ms)
}

/// Upper bound on whole-leaf drops per sweep pass (each takes a short ACCESS
/// EXCLUSIVE lock on kafka.messages; a long backlog drains over a few passes).
pub const MAX_PARTITION_DROPS_PER_PASS: usize = 64;

/// One leaf of kafka.messages, as `kafka.message_partitions()` reports it, with the
/// bounds in epoch milliseconds (`None` = MINVALUE / MAXVALUE).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimePartition {
    pub oid: u32,
    pub name: String,
    pub lower_ms: Option<i64>,
    pub upper_ms: Option<i64>,
    pub is_default: bool,
}

/// The time leaves whose whole range is in the past (upper bound <= now): the only
/// ones a drop can apply to. Never the DEFAULT leaf. Oldest first.
pub fn past_time_partitions(leaves: &[TimePartition], now_ms: i64) -> Vec<&TimePartition> {
    let mut out: Vec<&TimePartition> = leaves
        .iter()
        .filter(|l| !l.is_default && l.upper_ms.is_some_and(|u| u <= now_ms))
        .collect();
    out.sort_by_key(|l| l.upper_ms);
    out
}

/// Whether every row of a topic in a leaf with this upper bound is expired: all rows
/// have `created_at < upper`, so they are all past the window once `upper + window <= now`.
/// `None` (infinite retention) is never expired.
pub fn wholly_expired(upper_ms: i64, now_ms: i64, window_ms: Option<i64>) -> bool {
    match window_ms {
        Some(w) => upper_ms.saturating_add(w) <= now_ms,
        None => false,
    }
}

/// What the sweep does with one past time leaf.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeafAction {
    /// Every topic in it is wholly expired and nothing is pending: drop the leaf.
    /// Carries the expired topic set the drop function re-checks under its lock.
    Drop { expired_topics: Vec<i32> },
    /// The leaf must stay (a topic with a longer or infinite window, or a pending
    /// transactional row), but these topics' rows in it are all expired: fall back
    /// to the bounded row delete for them, in this leaf.
    Trim { topics: Vec<i32> },
    /// Nothing in the leaf is due.
    Keep,
}

/// Decide what to do with a past leaf from what it holds. `window_of(topic)` is the
/// topic's effective window (`effective_retention_window_ms`); an unknown topic
/// counts as infinite, so it can only block a drop, never cause one.
pub fn classify_leaf(
    upper_ms: i64,
    now_ms: i64,
    present_topics: &[i32],
    has_pending: bool,
    window_of: impl Fn(i32) -> Option<i64>,
) -> LeafAction {
    let mut topics: Vec<i32> = present_topics.to_vec();
    topics.sort_unstable();
    topics.dedup();
    let expired: Vec<i32> = topics
        .iter()
        .copied()
        .filter(|t| wholly_expired(upper_ms, now_ms, window_of(*t)))
        .collect();
    if !has_pending && expired.len() == topics.len() {
        LeafAction::Drop {
            expired_topics: expired,
        }
    } else if !expired.is_empty() {
        LeafAction::Trim { topics: expired }
    } else {
        LeafAction::Keep
    }
}

/// Whether a topic's retention is enforced row by row (as before partitioning) rather
/// than a leaf at a time. Windows shorter than one leaf keep exact row expiry, so a
/// short `retention.ms` is not stretched to a whole leaf width. Windows of at least
/// one leaf are enforced at leaf granularity, like Kafka's segment retention (a
/// segment is deleted once its newest record is past `retention.ms`): a row then
/// lives at most `window + width`, and retention costs a DROP instead of row deletes.
/// (Rows still in the DEFAULT leaf are always expired row by row.)
pub fn row_level_expiry(window_ms: i64, partition_width_ms: i64) -> bool {
    window_ms < partition_width_ms
}

/// Whether a leaf can hold rows older than `now - window` (its lower bound is below
/// the cutoff, or unbounded).
pub fn may_hold_expired(leaf: &TimePartition, now_ms: i64, window_ms: i64) -> bool {
    match leaf.lower_ms {
        None => true,
        Some(lower) => lower < now_ms.saturating_sub(window_ms),
    }
}

/// `pg_kafka.message_partition_minutes` as milliseconds, clamped to its GUC range.
pub fn partition_width_ms(minutes: i32) -> i64 {
    i64::from(minutes.clamp(MIN_PARTITION_MINUTES, MAX_PARTITION_MINUTES)) * MS_PER_MINUTE
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn per_topic_window_overrides_global() {
        assert_eq!(effective_retention_window_ms(Some(5_000), 0), Some(5_000));
        assert_eq!(effective_retention_window_ms(Some(5_000), 24), Some(5_000));
        assert_eq!(effective_retention_window_ms(Some(0), 24), Some(0));
    }

    #[test]
    fn negative_topic_window_is_infinite() {
        assert_eq!(effective_retention_window_ms(Some(-1), 24), None);
        assert_eq!(effective_retention_window_ms(Some(i64::MIN), 0), None);
    }

    #[test]
    fn null_topic_window_uses_global_hours() {
        assert_eq!(effective_retention_window_ms(None, 2), Some(7_200_000));
        assert_eq!(effective_retention_window_ms(None, 0), None);
        assert_eq!(effective_retention_window_ms(None, -3), None);
    }

    #[test]
    fn huge_windows_are_clamped() {
        assert_eq!(
            effective_retention_window_ms(Some(i64::MAX), 0),
            Some(MAX_RETENTION_WINDOW_MS)
        );
        assert_eq!(
            effective_retention_window_ms(None, 87_600),
            Some(87_600 * 3_600_000)
        );
    }

    #[test]
    fn batches_continue_only_when_full_and_within_budget() {
        let budget = Duration::from_millis(1500);
        assert!(should_run_another_batch(
            10_000,
            10_000,
            Duration::from_millis(10),
            budget
        ));
        // A short batch means the topic is caught up.
        assert!(!should_run_another_batch(
            9_999,
            10_000,
            Duration::from_millis(10),
            budget
        ));
        assert!(!should_run_another_batch(0, 10_000, Duration::ZERO, budget));
        // Budget spent: stop even though more rows may remain.
        assert!(!should_run_another_batch(10_000, 10_000, budget, budget));
        assert!(!should_run_another_batch(10_000, 0, Duration::ZERO, budget));
    }

    #[test]
    fn steps_run_once_then_only_within_budget() {
        let budget = Duration::from_millis(1500);
        assert!(step_may_continue(0, budget * 2, budget));
        assert!(step_may_continue(3, Duration::from_millis(10), budget));
        assert!(!step_may_continue(1, budget, budget));
    }

    #[test]
    fn drops_stop_at_the_budget_or_after_a_lock_timeout() {
        let budget = Duration::from_millis(1500);
        // The first drop of a pass always runs, even with the budget spent.
        assert!(drop_may_continue(0, false, budget * 2, budget));
        assert!(drop_may_continue(
            5,
            false,
            Duration::from_millis(10),
            budget
        ));
        assert!(!drop_may_continue(1, false, budget, budget));
        assert!(!drop_may_continue(
            1,
            true,
            Duration::from_millis(10),
            budget
        ));
    }

    #[test]
    fn contents_probe_only_where_a_topic_can_be_expired() {
        // No finite window anywhere: no leaf needs the probe (only the emptiness check).
        assert!(!needs_contents_probe(1_000, i64::MAX, None));
        assert!(needs_contents_probe(1_000, 2_000, Some(1_000)));
        assert!(!needs_contents_probe(1_000, 1_999, Some(1_000)));
    }

    #[test]
    fn rotation_covers_every_item() {
        assert_eq!(probe_start(7, 0), 0);
        assert_eq!(probe_start(7, 5), 2);
        // Passes that each finish only one item (the budget spent elsewhere), on a list
        // shorter than the probe cap and on one longer than it: every item is reached.
        for n in [3, MAX_CONTENTS_PROBES_PER_PASS * 2] {
            let rotation = Rotation::new();
            let mut seen = vec![false; n];
            for _ in 0..n {
                let start = rotation.start(n);
                assert!(start < n);
                seen[start] = true;
                rotation.record(start, 1);
            }
            assert!(seen.iter().all(|&s| s), "{n} items: {seen:?}");
        }
        // A pass that finished nothing (cut short inside its first item) resumes there.
        let rotation = Rotation::new();
        rotation.record(rotation.start(4), 2);
        let start = rotation.start(4);
        rotation.record(start, 0);
        assert_eq!(rotation.start(4), 2);
        // A full pass of the cap moves the next one past every item it finished.
        let n = MAX_CONTENTS_PROBES_PER_PASS + 5;
        let start = rotation.start(n);
        rotation.record(start, MAX_CONTENTS_PROBES_PER_PASS);
        assert_eq!(
            rotation.start(n),
            (start + MAX_CONTENTS_PROBES_PER_PASS) % n
        );
    }

    #[test]
    fn pass_budget_override_is_test_only() {
        assert_eq!(pass_budget(-1), RETENTION_PASS_BUDGET);
        assert_eq!(pass_budget(0), Duration::ZERO);
        assert_eq!(pass_budget(40), Duration::from_millis(40));
    }

    #[test]
    fn rotation_wraps_and_keeps_every_item() {
        let v = vec![1, 2, 3, 4];
        assert_eq!(rotated(&v, 0), vec![1, 2, 3, 4]);
        assert_eq!(rotated(&v, 1), vec![2, 3, 4, 1]);
        assert_eq!(rotated(&v, 5), vec![2, 3, 4, 1]);
        assert_eq!(rotated::<i32>(&[], 3), Vec::<i32>::new());
    }

    #[test]
    fn start_index_stays_in_range() {
        assert_eq!(next_start_index(0), 0);
        for _ in 0..10 {
            assert!(next_start_index(3) < 3);
        }
    }

    fn leaf(oid: u32, lower: Option<i64>, upper: Option<i64>, is_default: bool) -> TimePartition {
        TimePartition {
            oid,
            name: format!("l{oid}"),
            lower_ms: lower,
            upper_ms: upper,
            is_default,
        }
    }

    #[test]
    fn past_partitions_exclude_default_and_current_and_sort_oldest_first() {
        let leaves = vec![
            leaf(1, None, None, true),
            leaf(2, Some(200), Some(300), false),
            leaf(3, None, Some(100), false), // legacy [MINVALUE, 100)
            leaf(4, Some(100), Some(200), false),
            leaf(5, Some(300), Some(400), false), // current at now=350
        ];
        let past: Vec<u32> = past_time_partitions(&leaves, 350)
            .iter()
            .map(|l| l.oid)
            .collect();
        assert_eq!(past, vec![3, 4, 2]);
        // upper == now counts as past (the range is half-open).
        assert_eq!(past_time_partitions(&leaves, 400).len(), 4);
    }

    #[test]
    fn wholly_expired_needs_the_upper_bound_past_the_window() {
        assert!(wholly_expired(1_000, 2_000, Some(1_000)));
        assert!(!wholly_expired(1_000, 1_999, Some(1_000)));
        assert!(!wholly_expired(1_000, i64::MAX, None));
        // Saturating: a huge bound plus a window never wraps into the past.
        assert!(!wholly_expired(i64::MAX - 1, i64::MAX - 1, Some(10)));
    }

    #[test]
    fn leaf_drops_only_when_every_topic_is_expired_and_nothing_is_pending() {
        let windows = |t: i32| match t {
            1 => Some(100),
            2 => Some(500),
            _ => None, // 3: infinite; anything else: unknown
        };
        // upper=1000, now=1200: topic 1 expired (1100<=1200), topic 2 not (1500).
        assert_eq!(
            classify_leaf(1000, 1200, &[1, 1], false, windows),
            LeafAction::Drop {
                expired_topics: vec![1]
            }
        );
        assert_eq!(
            classify_leaf(1000, 1200, &[1, 2], false, windows),
            LeafAction::Trim { topics: vec![1] }
        );
        assert_eq!(
            classify_leaf(1000, 1200, &[2], false, windows),
            LeafAction::Keep
        );
        // An infinite-retention topic keeps the leaf; the others fall back to row deletes.
        assert_eq!(
            classify_leaf(1000, 9999, &[3, 1, 2], false, windows),
            LeafAction::Trim { topics: vec![1, 2] }
        );
        // A pending transactional row blocks the drop but not the trim.
        assert_eq!(
            classify_leaf(1000, 9999, &[1], true, windows),
            LeafAction::Trim { topics: vec![1] }
        );
        // An unknown topic is treated as infinite.
        assert_eq!(
            classify_leaf(1000, 9999, &[42], false, windows),
            LeafAction::Keep
        );
        // An empty past leaf holds nothing to keep.
        assert_eq!(
            classify_leaf(1000, 1000, &[], false, windows),
            LeafAction::Drop {
                expired_topics: vec![]
            }
        );
    }

    #[test]
    fn short_windows_keep_row_level_expiry() {
        let day = partition_width_ms(DEFAULT_PARTITION_MINUTES);
        assert_eq!(day, 86_400_000);
        assert!(row_level_expiry(3_600_000, day));
        assert!(row_level_expiry(0, day));
        assert!(!row_level_expiry(day, day));
        assert!(!row_level_expiry(7 * day, day));
    }

    #[test]
    fn partition_width_is_clamped_to_the_guc_range() {
        assert_eq!(
            partition_width_ms(0),
            i64::from(MIN_PARTITION_MINUTES) * 60_000
        );
        assert_eq!(
            partition_width_ms(i32::MAX),
            i64::from(MAX_PARTITION_MINUTES) * 60_000
        );
        assert_eq!(partition_width_ms(60), 3_600_000);
    }

    #[test]
    fn expired_rows_are_only_looked_for_below_the_cutoff() {
        let l = leaf(1, Some(1_000), Some(2_000), false);
        assert!(may_hold_expired(&l, 1_500, 100)); // cutoff 1400 > lower
        assert!(!may_hold_expired(&l, 1_050, 100)); // cutoff 950 <= lower
        assert!(may_hold_expired(&leaf(2, None, None, true), 0, 100));
    }

    #[test]
    fn maintenance_lock_statements() {
        assert_eq!(
            MaintenanceLockWait::Wait.lock_sql(),
            "SELECT true AS locked FROM pg_advisory_xact_lock($1)"
        );
        assert_eq!(
            MaintenanceLockWait::Skip.lock_sql(),
            "SELECT pg_try_advisory_xact_lock($1) AS locked"
        );
    }

    /// The SQL partition functions take the same key the sweep does, and nothing there
    /// queues for it (a queued wait while holding the schema lock could deadlock).
    #[test]
    fn maintenance_lock_key_matches_the_partition_functions() {
        for sql in [
            include_str!("../../../sql/bootstrap.sql"),
            include_str!("../../../sql/pg_kafka--0.0.0--0.1.0.sql"),
        ] {
            let key = MAINTENANCE_LOCK_KEY.to_string();
            assert!(sql.contains(&format!("pg_try_advisory_xact_lock({key})")));
            assert!(!sql.contains(&format!("pg_advisory_xact_lock({key})")));
            assert!(sql.contains("IF NOT kafka.try_maintenance_lock() THEN"));
        }
    }
}
