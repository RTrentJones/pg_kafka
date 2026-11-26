//! Pure planning and scheduling logic of the retention sweep.
//!
//! `PostgresStore::run_retention_sweep` (SPI, not unit-testable) expires messages by
//! dropping whole time partitions or deleting rows in bounded batches, and prunes the
//! auxiliary tables. The decisions it makes (which retention window a topic has, what to
//! do with a leaf, which step runs the next statement, where a pass resumes each step,
//! what earlier passes found still to do) live here so they are unit tested.
//!
//! # How a pass is scheduled
//!
//! A pass is a list of steps (the aborted-row cleanup, the contents probes, row expiry,
//! the leaf drops, the transaction and producer prunes, the two outbox reapers). A step
//! does its work in *turns*, each one bounded statement (at most
//! [`RETENTION_DELETE_BATCH`] rows, one leaf, one probe or one drop), and says whether it
//! has work left ([`PassStep`]). [`run_pass`] runs them in two phases:
//!
//! 1. every step that has work takes one turn, in list order, whatever the budget;
//! 2. then, while the pass is within its budget and some step has work, the step with
//!    work that has spent the least time this pass takes the next turn.
//!
//! A turn that finds a row another transaction keeps (an offset counter it cannot skip)
//! ends the pass in either phase ([`PassClock::ended`]); the guarantees below hold for
//! the passes no other transaction cuts short this way, and a cut pass leaves every
//! step's items and cursor where they were, so it only delays them by one pass.
//!
//! The steps that walk a list of items (leaves, (topic, leaf) pairs, queued drops) do so
//! through a [`Lap`]: the items in key order, starting after the key of the last item
//! the step served ([`KeyCursor`]), wrapping, each item served until caught up. Probes
//! *find* work (a leaf to drop, a topic to trim in a leaf to keep) and record it in
//! [`FoundWork`], which carries it across passes until it is done or no longer valid, so
//! the expiry and drop steps take their items from it, not from what this pass's probes
//! reached.
//!
//! # Guarantees
//!
//! 1. **Bounded passes to every item, at any budget >= 0, whatever the other steps'
//!    backlogs.** Phase 1 gives every step with work a turn every pass, so a step's
//!    progress does not depend on the budget or on another step. A lapped step's items
//!    persist from pass to pass (a leaf, a (topic, leaf) pair, a found drop or trim stays
//!    an item until it is done or gone), and each pass serves at least the first item
//!    after the cursor, so with `N` items every item gets a turn within `N` passes
//!    ([`KeyCursor`]), and one that needs `b` turns (`b` batches of rows) is done within
//!    `b * N`. So a trim or a drop of a leaf among `P` probe candidates is found within
//!    `P` passes of becoming due (an empty leaf: the next pass), a found trim is done
//!    within `b * E` more passes (`E` expiry items: the found trims plus at most two
//!    row-level items per short-window topic and one per topic with a window in a
//!    non-empty DEFAULT leaf, [`expiry_items`]), and a found drop within `D` more passes (`D` found
//!    drops), one more for each drop of that leaf that ran out of its lock wait. The
//!    prunes and reapers delete their due rows oldest first (or by key), one batch a
//!    turn, so a due row goes within one pass per batch of due rows ahead of it, and the
//!    producer prune's cursor examines every idle id within one pass per
//!    [`RETENTION_DELETE_BATCH`] idle ids.
//! 2. **A pass costs at most the budget plus one turn per step.** Phase 1 is one turn per
//!    step; phase 2 starts a turn only within the budget, so a pass ends by
//!    `max(budget + one turn, one turn per step)`. (Plus the pass's fixed setup reads, a
//!    few statements; a turn that deletes rows includes their log-start update, which
//!    polls a counter another transaction holds for at most 1 s.) The planning between the
//!    statements is O(n log n) in what the pass reads and holds (topics, leaves, found
//!    work), so no turn hides work the budget does not see: a lap's push is O(log n)
//!    ([`Lap`]), a probe replaces only its own leaf's trims ([`FoundWork`]), and the
//!    row-level expiry items grow with the topics, not with topics times leaves
//!    ([`expiry_items`]).
//! 3. **No share is wasted while a step has work.** Phase 2 stops only at the budget or
//!    when no step has work; the time one step does not use goes to the others, and a
//!    step with work all pass gets at least `budget / steps - one turn` of it (each turn
//!    goes to the least-served step with work, so no step ends more than one turn ahead
//!    of it).
//! 4. **Found work does not depend on the pass that found it.** A trim or drop a probe
//!    finds stays in [`FoundWork`] until its delete catches up or its drop runs (or the
//!    leaf, the topic or its window changes so that it no longer applies), so the turn
//!    that does it can come in any later pass; the steps also take work found earlier in
//!    the same pass.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Duration;

/// Rows deleted per DELETE statement. Small enough that one statement holds the DB
/// thread briefly; a pass runs as many as fit in [`RETENTION_PASS_BUDGET`].
pub const RETENTION_DELETE_BATCH: i64 = 10_000;

/// Wall-clock budget for one sweep pass. The sweep runs on the single worker DB
/// thread, so a pass must stay bounded: every step whose cost grows with the data runs
/// bounded statements ([`RETENTION_DELETE_BATCH`] rows, one leaf, one probe, one drop),
/// and [`run_pass`] starts a statement past the first one per step only within the
/// budget, giving the budget to the steps that have work, least-served first. A pass
/// costs at most the budget plus one statement per step (a drop's statement includes its
/// lock wait of up to 2 s); a backlog larger than one pass can clear drains over the
/// following passes, each step resuming where the last pass left it.
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

/// Whether a delete batch that removed `deleted` rows was full, so more rows may
/// remain: a short batch means its item is caught up.
pub fn batch_was_full(deleted: u64, batch_size: i64) -> bool {
    u64::try_from(batch_size).is_ok_and(|size| size > 0 && deleted >= size)
}

/// The clock a pass is scheduled by: time since the pass started, and whether the pass
/// must end now whatever its budget.
pub trait PassClock {
    fn elapsed(&self) -> Duration;
    /// Whether the pass ends before its next turn: a turn found a row another
    /// transaction holds and gave up its wait for it (`PostgresStore::run_retention_sweep`,
    /// "Lock waits"), so the pass commits now and frees the rows it holds for that
    /// transaction instead of running more turns while it waits.
    fn ended(&self) -> bool {
        false
    }
    /// Called after every turn, before the next [`ended`](PassClock::ended): the
    /// context may look at what the turn left (`PostgresStore::run_retention_sweep`
    /// ends the pass when another session waits for a lock it holds).
    fn after_turn(&mut self) {}
}

/// One step of a sweep pass, as [`run_pass`] schedules it. `C` is the pass's shared
/// state (the SPI client, what the steps found and removed), `E` the error a statement
/// can fail with.
pub trait PassStep<C, E> {
    /// Whether the step has work left in this pass. A step that can only learn this
    /// from a statement (a prune) answers `true` until a batch comes back short.
    fn has_work(&self, ctx: &C) -> bool;
    /// One turn: one bounded statement (for the orphan reaper, the log-start update its
    /// delete reads, then the delete).
    fn turn(&mut self, ctx: &mut C) -> Result<(), E>;
}

/// What [`run_pass`] did: per step, the turns it took and the time they took.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PassReport {
    pub turns: Vec<u32>,
    pub service: Vec<Duration>,
}

/// Run one pass of `steps` (see the module docs, "How a pass is scheduled"): one turn
/// for every step with work, in order, then, within `budget`, turns for the steps with
/// work, least time spent this pass first (ties in list order). Once the context says
/// the pass has [ended](PassClock::ended), no step takes another turn.
pub fn run_pass<C: PassClock, E>(
    steps: &mut [&mut dyn PassStep<C, E>],
    ctx: &mut C,
    budget: Duration,
) -> Result<PassReport, E> {
    let mut report = PassReport {
        turns: vec![0; steps.len()],
        service: vec![Duration::ZERO; steps.len()],
    };
    for i in 0..steps.len() {
        if ctx.ended() {
            return Ok(report);
        }
        if steps[i].has_work(ctx) {
            take_turn(i, steps, ctx, &mut report)?;
        }
    }
    while ctx.elapsed() < budget && !ctx.ended() {
        let next = (0..steps.len())
            .filter(|&i| steps[i].has_work(ctx))
            .min_by_key(|&i| (report.service[i], i));
        let Some(i) = next else { break };
        take_turn(i, steps, ctx, &mut report)?;
    }
    Ok(report)
}

/// Step `i` takes one turn; its time goes to its service.
fn take_turn<C: PassClock, E>(
    i: usize,
    steps: &mut [&mut dyn PassStep<C, E>],
    ctx: &mut C,
    report: &mut PassReport,
) -> Result<(), E> {
    let before = ctx.elapsed();
    steps[i].turn(ctx)?;
    ctx.after_turn();
    report.service[i] += ctx.elapsed().saturating_sub(before);
    report.turns[i] += 1;
    Ok(())
}

/// One pass of a step over a list of keyed items: the items in key order, starting at
/// the first whose key is greater than the step's [`KeyCursor`] and wrapping, at most
/// `cap` of them. Each turn serves the current item; the cursor then holds its key, so
/// the next pass starts after it, and the lap moves on once the item is caught up.
/// Items found during the pass ([`Lap::push`]) join at the end.
///
/// Cost: building a lap of `n` items takes O(n log n) key comparisons and O(n) memory
/// (the items and a set of their keys); each push O(log n), so a probe that finds a
/// trim for each of a leaf's `T` topics adds them in O(T log n), not one scan of the
/// lap per trim.
pub struct Lap<'c, K, T> {
    items: Vec<(K, T)>,
    /// The keys of `items`, for [`Lap::push`]'s duplicate check.
    keys: BTreeSet<K>,
    pos: usize,
    cap: usize,
    cursor: &'c KeyCursor<K>,
}

impl<'c, K: Ord + Copy, T> Lap<'c, K, T> {
    /// The lap over `items` (any order; of items with the same key, the first is kept).
    pub fn new(mut items: Vec<(K, T)>, cursor: &'c KeyCursor<K>, cap: usize) -> Self {
        items.sort_by_key(|item| item.0);
        items.dedup_by(|later, first| later.0 == first.0);
        let start = cursor.start_in(&items);
        items.rotate_left(start);
        items.truncate(cap);
        let keys = items.iter().map(|(k, _)| *k).collect();
        Lap {
            items,
            keys,
            pos: 0,
            cap,
            cursor,
        }
    }

    /// The item the next turn serves, if any.
    pub fn current(&self) -> Option<&(K, T)> {
        self.items.get(self.pos)
    }

    /// Whether every item of the lap is done (or the lap was stopped).
    pub fn is_done(&self) -> bool {
        self.pos >= self.items.len()
    }

    /// A turn ran a statement on the current item: the next pass starts after it, and
    /// `caught_up` moves this lap on to the next item.
    pub fn served(&mut self, caught_up: bool) {
        if let Some((key, _)) = self.items.get(self.pos) {
            self.cursor.set(Some(*key));
        }
        if caught_up {
            self.pos += 1;
        }
    }

    /// Add an item found during the pass, unless the lap already has it or is full.
    pub fn push(&mut self, key: K, item: T) {
        if self.items.len() < self.cap && self.keys.insert(key) {
            self.items.push((key, item));
        }
    }

    /// End the lap for this pass (the step cannot go on: a drop ran out of a lock wait).
    pub fn stop(&mut self) {
        self.pos = self.items.len();
    }
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
/// over them ([`SweepState::probe`]) instead of always starting from the oldest.
pub const MAX_CONTENTS_PROBES_PER_PASS: usize = 64;

/// Where a lapped sweep step resumes ([`Lap`]): the key of the last item a pass ran a
/// statement on. A step walks its items in key order (an item's identity: a leaf's
/// upper bound, a (topic, leaf) pair) and starts each pass at the first item whose key
/// is greater than the cursor, wrapping to the first ([`start_after`]). So a pass cut
/// short (by a cap, or by the pass budget) does not walk the same leading items again,
/// and since the cursor is a key, not a position, items that disappear (a leaf dropped)
/// or appear between passes do not shift it. An item the previous pass was cut short in
/// gets its next turn after the others, not first: a backlog in one item takes one turn
/// per rotation (and whatever budget is left once the others are caught up) instead of
/// every pass, so one topic's backlog cannot stall the expiry of the others.
///
/// The bound: every pass serves at least the first item after the cursor (phase 1 of
/// [`run_pass`]), and serves items in key order from there, so the cursor moves forward
/// (wrapping) every pass. An item present at every pass is therefore served within `N`
/// passes, where `N` is the number of distinct items present over those passes: each
/// pass serves at least one more of the items between the cursor and it.
pub struct KeyCursor<K>(Mutex<Option<K>>);

impl<K: Ord + Copy> KeyCursor<K> {
    pub const fn new() -> Self {
        KeyCursor(Mutex::new(None))
    }

    /// The cursor: `None` before the first pass, or after [`KeyCursor::set`] cleared it.
    pub fn get(&self) -> Option<K> {
        *self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Replace the cursor.
    pub fn set(&self, key: Option<K>) {
        *self.0.lock().unwrap_or_else(PoisonError::into_inner) = key;
    }

    /// Where this pass starts in `sorted_keys` (ascending).
    pub fn start(&self, sorted_keys: &[K]) -> usize {
        start_after(sorted_keys, self.get().as_ref())
    }

    /// Where this pass starts in `sorted` (ascending by key): [`KeyCursor::start`] over
    /// the items' keys, by binary search.
    fn start_in<T>(&self, sorted: &[(K, T)]) -> usize {
        match self.get() {
            None => 0,
            Some(c) => {
                let at = sorted.partition_point(|(k, _)| *k <= c);
                if at < sorted.len() {
                    at
                } else {
                    0
                }
            }
        }
    }
}

impl<K: Ord + Copy> Default for KeyCursor<K> {
    fn default() -> Self {
        KeyCursor::new()
    }
}

/// The index of the first of `sorted_keys` (ascending) greater than `cursor`, or 0
/// when there is none (wrap around) or no cursor yet.
pub fn start_after<K: Ord>(sorted_keys: &[K], cursor: Option<&K>) -> usize {
    cursor
        .and_then(|c| sorted_keys.iter().position(|k| k > c))
        .unwrap_or(0)
}

/// A leaf's key for the sweep's cursors: its upper bound, unique among the time leaves
/// (they do not overlap); the DEFAULT leaf (and an unpartitioned table), with none, sorts
/// last.
pub fn leaf_key(leaf: &TimePartition) -> i64 {
    leaf.upper_ms.unwrap_or(i64::MAX)
}

/// What the sweep carries from pass to pass in one backend: each step's cursor and the
/// work the probes found. All of it is a hint, re-checked every pass (and a drop under
/// its lock), so losing it (a new backend, a worker restart) only means rediscovering it.
pub struct SweepState {
    /// The contents probes over their candidates, keyed by [`leaf_key`].
    pub probe: KeyCursor<i64>,
    /// The aborted-row cleanup over the leaves that may hold aborted rows past the grace,
    /// keyed by [`leaf_key`]: each leaf costs at least one probe of its partial index.
    pub aborted: KeyCursor<i64>,
    /// Row expiry over its items, keyed by [`ExpiryItem::key`].
    pub expiry: KeyCursor<(i32, i64)>,
    /// The drops over the found drops, keyed by [`leaf_key`].
    pub drop: KeyCursor<i64>,
    /// Where the stale-producer prune resumes: the (last_active_at in epoch microseconds,
    /// producer id) of the last candidate it examined. Candidates it keeps (a producer
    /// still referenced by a transaction, or active through its sequences) stay below the
    /// idle cutoff, so without a cursor every pass would examine them again first.
    pub producer_prune: KeyCursor<(i64, i64)>,
    found: Mutex<FoundWork>,
}

impl SweepState {
    pub const fn new() -> Self {
        SweepState {
            probe: KeyCursor::new(),
            aborted: KeyCursor::new(),
            expiry: KeyCursor::new(),
            drop: KeyCursor::new(),
            producer_prune: KeyCursor::new(),
            found: Mutex::new(FoundWork::new()),
        }
    }

    /// The found work, for the length of a pass.
    pub fn found(&self) -> MutexGuard<'_, FoundWork> {
        self.found.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Default for SweepState {
    fn default() -> Self {
        SweepState::new()
    }
}

/// The state of this backend's sweeps (the worker's, or a session's on-demand ones).
pub static SWEEP_STATE: SweepState = SweepState::new();

/// The prune cursor after a statement that examined `examined` candidates (at most
/// `batch`), the last one `last`: a short batch reached the end of the candidates, so
/// the next statement starts from the oldest again (`None`).
pub fn next_prune_cursor<K>(examined: u64, batch: i64, last: Option<K>) -> Option<K> {
    if u64::try_from(batch).is_ok_and(|b| examined >= b) {
        last
    } else {
        None
    }
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

/// The past leaves that need the contents probe ([`needs_contents_probe`]) and the
/// others, which can only be dropped if empty; both oldest first.
pub fn split_past_leaves(
    leaves: &[TimePartition],
    now_ms: i64,
    shortest_window_ms: Option<i64>,
) -> (Vec<&TimePartition>, Vec<&TimePartition>) {
    past_time_partitions(leaves, now_ms)
        .into_iter()
        .partition(|l| needs_contents_probe(leaf_key(l), now_ms, shortest_window_ms))
}

/// A past leaf found droppable (every topic in it wholly expired and nothing pending,
/// or empty), with the expired topics the drop function re-checks under its lock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FoundDrop {
    pub oid: u32,
    pub expired_topics: Vec<i32>,
}

/// The work the contents probes and the emptiness check found, carried from pass to
/// pass until it is done: the leaves to drop, keyed by [`leaf_key`], and the (topic,
/// leaf) trims (a wholly expired topic's rows in a leaf that must stay). A trim leaves
/// once its delete comes back short, a drop once it is tried (dropped, or refused by the
/// re-check under the lock; one that ran out of its lock wait stays), and both once the
/// leaf is gone or a new probe of it decides otherwise; a trim also once its topic has no
/// window that expires the whole leaf. So a probe's finding does not depend on the
/// expiry or drop step reaching it in the same pass.
///
/// Size: at most one drop per past leaf and one trim per (topic, past leaf) pair the
/// probes found holding rows. Cost: a probe replaces only its own leaf's trims, in
/// O(r log n) for `r` trims there before and after (an index by leaf, not a scan of all
/// trims); [`FoundWork::retain_current`] and [`FoundWork::trims`] are linear in the
/// found work, once per pass.
#[derive(Debug, Default)]
pub struct FoundWork {
    drops: BTreeMap<i64, FoundDrop>,
    /// Leaf key -> that leaf's trims.
    trims: BTreeMap<i64, LeafTrims>,
    /// Trim entries [`FoundWork::record_probe`] examined, for the cost tests.
    #[cfg(test)]
    probe_work: u64,
}

/// The found trims in one leaf: its oid and the wholly expired topics.
#[derive(Debug)]
struct LeafTrims {
    oid: u32,
    topics: BTreeSet<i32>,
}

impl FoundWork {
    pub const fn new() -> Self {
        FoundWork {
            drops: BTreeMap::new(),
            trims: BTreeMap::new(),
            #[cfg(test)]
            probe_work: 0,
        }
    }

    /// Record what a probe of `leaf` decided; it replaces whatever was found there before.
    pub fn record_probe(&mut self, leaf: &TimePartition, action: &LeafAction) {
        let key = leaf_key(leaf);
        let replaced = self.trims.remove(&key).map_or(0, |t| t.topics.len());
        #[cfg(test)]
        {
            self.probe_work += u64::try_from(replaced).unwrap_or(u64::MAX);
        }
        #[cfg(not(test))]
        let _ = replaced;
        self.drops.remove(&key);
        match action {
            LeafAction::Drop { expired_topics } => {
                self.drops.insert(
                    key,
                    FoundDrop {
                        oid: leaf.oid,
                        expired_topics: expired_topics.clone(),
                    },
                );
            }
            LeafAction::Trim { topics } if !topics.is_empty() => {
                self.trims.insert(
                    key,
                    LeafTrims {
                        oid: leaf.oid,
                        topics: topics.iter().copied().collect(),
                    },
                );
            }
            LeafAction::Trim { .. } | LeafAction::Keep => {}
        }
    }

    /// Record a past leaf the emptiness check found empty.
    pub fn record_empty(&mut self, leaf: &TimePartition) {
        self.record_probe(
            leaf,
            &LeafAction::Drop {
                expired_topics: Vec::new(),
            },
        );
    }

    /// Forget what no longer applies: work in leaves that are gone (or whose key now
    /// names another leaf), trims whose topic has no window that wholly expires the leaf
    /// now (`window_of`, as in [`classify_leaf`]: a deleted topic has none), and drops
    /// of which any expired topic has none. `kafka.drop_message_partition` re-checks the
    /// leaf's topics against the drop's expired set, not against retention, so a drop kept
    /// across a raised window would remove rows the new window keeps.
    pub fn retain_current(
        &mut self,
        leaves: &[TimePartition],
        now_ms: i64,
        window_of: impl Fn(i32) -> Option<i64>,
    ) {
        let past: BTreeMap<i64, u32> = past_time_partitions(leaves, now_ms)
            .into_iter()
            .map(|l| (leaf_key(l), l.oid))
            .collect();
        self.drops.retain(|&k, d| {
            past.get(&k) == Some(&d.oid)
                && d.expired_topics
                    .iter()
                    .all(|&topic| wholly_expired(k, now_ms, window_of(topic)))
        });
        self.trims.retain(|&k, leaf| {
            if past.get(&k) != Some(&leaf.oid) {
                return false;
            }
            leaf.topics
                .retain(|&topic| wholly_expired(k, now_ms, window_of(topic)));
            !leaf.topics.is_empty()
        });
    }

    /// The found drops, by leaf key.
    pub fn drops(&self) -> Vec<(i64, FoundDrop)> {
        self.drops.iter().map(|(k, d)| (*k, d.clone())).collect()
    }

    /// Whether the leaf `oid` with key `key` is a found drop.
    pub fn has_drop(&self, key: i64, oid: u32) -> bool {
        self.drops.get(&key).is_some_and(|d| d.oid == oid)
    }

    /// The found trims: ((topic, leaf key), leaf oid), by leaf, then topic.
    pub fn trims(&self) -> Vec<((i32, i64), u32)> {
        self.trims
            .iter()
            .flat_map(|(&k, leaf)| leaf.topics.iter().map(move |&t| ((t, k), leaf.oid)))
            .collect()
    }

    /// The drop of the leaf with this key was tried: dropped, or refused by its re-check.
    pub fn drop_done(&mut self, key: i64) {
        self.drops.remove(&key);
    }

    /// The trim with this key is caught up.
    pub fn trim_done(&mut self, (topic, leaf_key): (i32, i64)) {
        if let Some(leaf) = self.trims.get_mut(&leaf_key) {
            leaf.topics.remove(&topic);
            if leaf.topics.is_empty() {
                self.trims.remove(&leaf_key);
            }
        }
    }
}

/// One unit of row expiry: delete `topic_id`'s rows older than `window_ms` in the leaf
/// `leaf_oid`, whose [`leaf_key`] is `leaf_key`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExpiryItem {
    pub topic_id: i32,
    pub window_ms: i64,
    pub leaf_oid: u32,
    pub leaf_key: i64,
}

impl ExpiryItem {
    /// The item's identity for [`SweepState::expiry`]: a trim and a row-level item of
    /// the same (topic, leaf) are the same delete.
    pub fn key(&self) -> (i32, i64) {
        (self.topic_id, self.leaf_key)
    }
}

/// When a pass decides: the database time and the partition width (epoch ms, ms).
#[derive(Debug, Clone, Copy)]
pub struct SweepClock {
    pub now_ms: i64,
    pub width_ms: i64,
}

/// Row expiry's items for a pass: the found trims, plus the row-level items: a topic
/// whose window is shorter than one leaf ([`row_level_expiry`]) in the leaf holding its
/// cutoff (`now - window`, when that leaf may hold rows older than it) and in the newest
/// leaf wholly behind the cutoff, and every topic with a window in the DEFAULT leaf,
/// unless it is empty (`default_has_rows`); none in a leaf found droppable (its drop
/// removes them). `finite` lists the topics with a window.
///
/// The older leaves are wholly expired for such a topic (`upper + window <= now`), so
/// they are contents-probe candidates ([`needs_contents_probe`]: the shortest window is
/// at most the topic's), and the topic's rows there are found trims; the newest of them
/// keeps its row-level item so that the rows that expired since the last pass, as the
/// cutoff left it, go at the next expiry turn instead of waiting for a probe. So every
/// (topic, leaf) with expired rows has an item, from here or from a probe, and a pass
/// has at most `F + 2 * S + W` items (`F` found trims, `S` short-window topics, `W`
/// topics with a window when the DEFAULT leaf holds rows), whatever the number of
/// leaves.
///
/// Cost: O(L log L) to order the `L` leaves by bound, O(log L) per topic to find its
/// cutoff, O(F) for the found trims, and the items' memory.
pub fn expiry_items(
    leaves: &[TimePartition],
    finite: &[(i32, i64)],
    clock: SweepClock,
    default_has_rows: bool,
    found: &FoundWork,
) -> Vec<ExpiryItem> {
    let windows: BTreeMap<i32, i64> = finite.iter().copied().collect();
    let mut items: Vec<ExpiryItem> = found
        .trims()
        .into_iter()
        .filter_map(|((topic_id, leaf_key), leaf_oid)| {
            windows.get(&topic_id).map(|&window_ms| ExpiryItem {
                topic_id,
                window_ms,
                leaf_oid,
                leaf_key,
            })
        })
        .collect();
    let mut time_leaves: Vec<&TimePartition> = leaves.iter().filter(|l| !l.is_default).collect();
    time_leaves.sort_by_key(|l| leaf_key(l));
    let defaults: Vec<&TimePartition> = leaves
        .iter()
        .filter(|l| l.is_default && default_has_rows)
        .collect();
    for &(topic_id, window_ms) in finite {
        let mut push = |leaf: &TimePartition| {
            if !found.has_drop(leaf_key(leaf), leaf.oid) {
                items.push(ExpiryItem {
                    topic_id,
                    window_ms,
                    leaf_oid: leaf.oid,
                    leaf_key: leaf_key(leaf),
                });
            }
        };
        for &leaf in &defaults {
            push(leaf);
        }
        if !row_level_expiry(window_ms, clock.width_ms) {
            continue;
        }
        let cutoff = clock.now_ms.saturating_sub(window_ms);
        let at = time_leaves.partition_point(|l| leaf_key(l) <= cutoff);
        if let Some(&holding) = time_leaves.get(at) {
            if may_hold_expired(holding, clock.now_ms, window_ms) {
                push(holding);
            }
        }
        if let Some(&behind) = at.checked_sub(1).and_then(|i| time_leaves.get(i)) {
            push(behind);
        }
    }
    items
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
mod cost_tests;
#[cfg(test)]
mod scheduling_tests;

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
    fn contents_probe_only_where_a_topic_can_be_expired() {
        // No finite window anywhere: no leaf needs the probe (only the emptiness check).
        assert!(!needs_contents_probe(1_000, i64::MAX, None));
        assert!(needs_contents_probe(1_000, 2_000, Some(1_000)));
        assert!(!needs_contents_probe(1_000, 1_999, Some(1_000)));
    }

    #[test]
    fn start_after_finds_the_next_key_and_wraps() {
        let keys = [10, 20, 30];
        assert_eq!(start_after(&keys, None), 0);
        assert_eq!(start_after(&keys, Some(&5)), 0);
        assert_eq!(start_after(&keys, Some(&10)), 1);
        assert_eq!(start_after(&keys, Some(&25)), 2);
        assert_eq!(start_after(&keys, Some(&30)), 0);
        assert_eq!(start_after::<i32>(&[], Some(&1)), 0);
    }

    #[test]
    fn key_cursor_covers_every_item() {
        // Passes that each serve only one item (the budget spent elsewhere), on a list
        // shorter than the probe cap and on one longer than it: every item is reached.
        for n in [3_i64, 2 * 64] {
            let keys: Vec<i64> = (0..n).map(|k| k * 100).collect();
            let cursor = KeyCursor::new();
            let mut seen = vec![false; keys.len()];
            for _ in 0..n {
                let start = cursor.start(&keys);
                seen[start] = true;
                cursor.set(Some(keys[start]));
            }
            assert!(seen.iter().all(|&s| s), "{n} items: {seen:?}");
        }
    }

    #[test]
    fn key_cursor_ignores_items_that_disappear_or_appear() {
        let cursor = KeyCursor::new();
        cursor.set(Some(30));
        // Leaves before the cursor dropped: a positional cursor (3) would skip 40.
        assert_eq!(cursor.start(&[30, 40, 50]), 1);
        // The served leaf itself gone, a new one before it: still the next key.
        assert_eq!(cursor.start(&[5, 40, 50]), 1);
        // Everything after the cursor gone: wrap to the first.
        assert_eq!(cursor.start(&[5, 10]), 0);
    }

    #[test]
    fn key_cursor_rotates_pairs_by_topic_then_leaf() {
        let keys = [(1, 100), (1, i64::MAX), (2, 100), (2, i64::MAX)];
        let cursor = KeyCursor::new();
        cursor.set(Some((1, i64::MAX)));
        assert_eq!(cursor.start(&keys), 2);
        cursor.set(Some((2, i64::MAX)));
        assert_eq!(cursor.start(&keys), 0);
        cursor.set(None);
        assert_eq!(cursor.get(), None);
        assert_eq!(cursor.start(&keys), 0);
    }

    #[test]
    fn leaf_keys_put_the_default_leaf_last() {
        assert_eq!(leaf_key(&leaf(1, None, None, true)), i64::MAX);
        assert_eq!(leaf_key(&leaf(2, None, Some(100), false)), 100);
        assert_eq!(leaf_key(&leaf(3, Some(100), Some(200), false)), 200);
    }

    #[test]
    fn prune_cursor_wraps_on_a_short_batch() {
        assert_eq!(next_prune_cursor(10, 10, Some((5, 7))), Some((5, 7)));
        assert_eq!(next_prune_cursor(9, 10, Some((5, 7))), None);
        assert_eq!(next_prune_cursor::<(i64, i64)>(0, 10, None), None);
        assert_eq!(next_prune_cursor(10, 0, Some(1)), Some(1));
        assert_eq!(next_prune_cursor(10, -1, Some(1)), None);
    }

    #[test]
    fn full_batches_mean_more_rows() {
        assert!(batch_was_full(10_000, 10_000));
        assert!(!batch_was_full(9_999, 10_000));
        assert!(!batch_was_full(0, 10_000));
        assert!(!batch_was_full(10_000, 0));
        assert!(!batch_was_full(10_000, -1));
    }

    #[test]
    fn pass_budget_override_is_test_only() {
        assert_eq!(pass_budget(-1), RETENTION_PASS_BUDGET);
        assert_eq!(pass_budget(0), Duration::ZERO);
        assert_eq!(pass_budget(40), Duration::from_millis(40));
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
