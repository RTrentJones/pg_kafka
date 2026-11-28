//! The sweep's scheduling guarantees (the module docs of `retention`, "Guarantees"),
//! checked on simulated passes: the scheduler over random steps and budgets, laps over
//! changing item lists, and a model of the probes, the found work and row expiry over
//! many passes (leaves kept by an infinite-retention topic, short-window topics expired
//! row by row everywhere, a long-window topic to trim in every kept leaf).

use super::*;
use std::collections::{BTreeSet, VecDeque};

/// A deterministic pseudo-random sequence (xorshift64*), so a failure reproduces.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    /// Uniform in `0..n` (n > 0).
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    /// Uniform in `0..n` (n > 0), as a count.
    fn count_below(&mut self, n: usize) -> usize {
        usize::try_from(self.below(u64::try_from(n).unwrap_or(u64::MAX))).unwrap_or(0)
    }
}

/// A pass context whose clock only the steps move.
#[derive(Default)]
struct FakeClock {
    now: Duration,
}

impl PassClock for FakeClock {
    fn elapsed(&self) -> Duration {
        self.now
    }
}

/// A step with a fixed list of turn costs.
struct FakeStep {
    costs: VecDeque<Duration>,
}

impl PassStep<FakeClock, ()> for FakeStep {
    fn has_work(&self, _: &FakeClock) -> bool {
        !self.costs.is_empty()
    }

    fn turn(&mut self, ctx: &mut FakeClock) -> Result<(), ()> {
        if let Some(cost) = self.costs.pop_front() {
            ctx.now += cost;
        }
        Ok(())
    }
}

/// Guarantees 1 to 3 on random passes: every step with work takes a turn whatever the
/// budget (0 included); a pass ends by the budget plus one turn per step (in fact by
/// `max(budget + one turn, one turn per step)`); it ends under the budget only when no
/// step has work; and a step that has work all pass gets at least `budget / steps - one
/// turn`.
#[test]
fn run_pass_turns_budget_and_fair_shares() {
    let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
    for case in 0..5_000 {
        let n = 1 + rng.count_below(8);
        let budget = Duration::from_millis(match rng.below(4) {
            0 => 0,
            1 => rng.below(10),
            _ => rng.below(500),
        });
        let mut steps: Vec<FakeStep> = (0..n)
            .map(|_| {
                let turns = match rng.below(4) {
                    0 => 0,
                    1 => 1_000, // a backlog no pass clears
                    _ => rng.count_below(40),
                };
                FakeStep {
                    costs: (0..turns)
                        .map(|_| Duration::from_millis(1 + rng.below(30)))
                        .collect(),
                }
            })
            .collect();
        let had_work: Vec<bool> = steps.iter().map(|s| !s.costs.is_empty()).collect();
        let first_turns: Duration = steps.iter().filter_map(|s| s.costs.front()).sum();
        let max_turn = steps
            .iter()
            .flat_map(|s| s.costs.iter())
            .max()
            .copied()
            .unwrap_or_default();
        let mut ctx = FakeClock::default();
        let mut refs: Vec<&mut dyn PassStep<FakeClock, ()>> = steps
            .iter_mut()
            .map(|s| s as &mut dyn PassStep<FakeClock, ()>)
            .collect();
        let report = run_pass(&mut refs, &mut ctx, budget).unwrap_or_else(|()| unreachable!());
        let elapsed = ctx.now;
        let ctx_msg = format!("case {case}: budget {budget:?}, {report:?}, elapsed {elapsed:?}");

        for i in 0..n {
            assert_eq!(report.turns[i] > 0, had_work[i], "{ctx_msg}: step {i}");
        }
        assert!(
            elapsed <= first_turns.max(budget + max_turn),
            "{ctx_msg}: over budget + one turn"
        );
        assert!(
            elapsed <= budget + max_turn * u32::try_from(n).unwrap_or(u32::MAX),
            "{ctx_msg}: over budget + one turn per step"
        );
        let still_busy: Vec<usize> = (0..n).filter(|&i| !steps[i].costs.is_empty()).collect();
        if elapsed < budget {
            assert!(
                still_busy.is_empty(),
                "{ctx_msg}: idle budget, {still_busy:?} busy"
            );
        }
        for &i in &still_busy {
            // Busy at the end, so busy all pass.
            assert!(
                (report.service[i] + max_turn) * u32::try_from(n).unwrap_or(u32::MAX) >= budget,
                "{ctx_msg}: step {i} got {:?} of a {budget:?} budget over {n} steps",
                report.service[i]
            );
        }
    }
}

/// A pass context that records which step took each turn.
#[derive(Default)]
struct LogClock {
    now: Duration,
    /// (step, cost) of each turn, in order.
    log: Vec<(usize, Duration)>,
}

impl PassClock for LogClock {
    fn elapsed(&self) -> Duration {
        self.now
    }
}

/// A step with a fixed list of turn costs that logs its turns.
struct LogStep {
    id: usize,
    costs: VecDeque<Duration>,
}

impl PassStep<LogClock, ()> for LogStep {
    fn has_work(&self, _: &LogClock) -> bool {
        !self.costs.is_empty()
    }

    fn turn(&mut self, ctx: &mut LogClock) -> Result<(), ()> {
        if let Some(cost) = self.costs.pop_front() {
            ctx.now += cost;
            ctx.log.push((self.id, cost));
        }
        Ok(())
    }
}

/// Phase 0 of [`run_pass_led`] on random passes: no step after the lead takes a turn
/// before every lead step has run out of work or the lead has spent its share; every
/// step with work still takes a turn; the pass ends by `max(budget + one turn, share +
/// one turn per step)`, under the budget only when no step has work; and a step busy all
/// pass gets at least `budget / steps - one turn`, lead or not.
#[test]
fn run_pass_led_runs_the_lead_first_and_keeps_the_guarantees() {
    let mut rng = Rng(0x2545_f491_4f6c_dd1d);
    for case in 0..5_000 {
        let n = 1 + rng.count_below(8);
        let lead = rng.count_below(n + 1);
        let budget = Duration::from_millis(match rng.below(4) {
            0 => 0,
            1 => rng.below(10),
            _ => rng.below(500),
        });
        let mut steps: Vec<LogStep> = (0..n)
            .map(|id| {
                let turns = match rng.below(4) {
                    0 => 0,
                    1 => 1_000,
                    _ => rng.count_below(40),
                };
                LogStep {
                    id,
                    costs: (0..turns)
                        .map(|_| Duration::from_millis(1 + rng.below(30)))
                        .collect(),
                }
            })
            .collect();
        let had_work: Vec<bool> = steps.iter().map(|s| !s.costs.is_empty()).collect();
        let lead_turns: Vec<usize> = steps[..lead].iter().map(|s| s.costs.len()).collect();
        let max_turn = steps
            .iter()
            .flat_map(|s| s.costs.iter())
            .max()
            .copied()
            .unwrap_or_default();
        let share = lead_share(budget, lead, n);
        let mut ctx = LogClock::default();
        let mut refs: Vec<&mut dyn PassStep<LogClock, ()>> = steps
            .iter_mut()
            .map(|s| s as &mut dyn PassStep<LogClock, ()>)
            .collect();
        let report =
            run_pass_led(&mut refs, &mut ctx, budget, lead).unwrap_or_else(|()| unreachable!());
        let elapsed = ctx.now;
        let msg = format!("case {case}: lead {lead}/{n}, budget {budget:?}, {report:?}");

        for i in 0..n {
            assert_eq!(report.turns[i] > 0, had_work[i], "{msg}: step {i}");
        }
        if let Some(at) = ctx.log.iter().position(|&(id, _)| id >= lead) {
            let spent: Duration = ctx.log[..at].iter().map(|&(_, c)| c).sum();
            let turns_before = |j: usize| ctx.log[..at].iter().filter(|&&(id, _)| id == j).count();
            for (j, &turns) in lead_turns.iter().enumerate() {
                assert!(
                    turns == 0 || turns_before(j) > 0,
                    "{msg}: lead step {j} had no turn before the others"
                );
            }
            let lead_done = (0..lead).all(|j| turns_before(j) == lead_turns[j]);
            assert!(
                spent >= share || lead_done,
                "{msg}: the others ran after the lead spent {spent:?} of its {share:?} share"
            );
        }
        assert!(
            elapsed
                <= (budget + max_turn).max(share + max_turn * u32::try_from(n).unwrap_or(u32::MAX)),
            "{msg}: elapsed {elapsed:?} over its bound"
        );
        let still_busy: Vec<usize> = (0..n).filter(|&i| !steps[i].costs.is_empty()).collect();
        if elapsed < budget {
            assert!(
                still_busy.is_empty(),
                "{msg}: idle budget, {still_busy:?} busy"
            );
        }
        for &i in &still_busy {
            assert!(
                (report.service[i] + max_turn) * u32::try_from(n).unwrap_or(u32::MAX) >= budget,
                "{msg}: step {i} got {:?}",
                report.service[i]
            );
        }
    }
}

/// A pass context that ends the pass once a step's turn sets `end`.
#[derive(Default)]
struct EndingClock {
    now: Duration,
    end: bool,
}

impl PassClock for EndingClock {
    fn elapsed(&self) -> Duration {
        self.now
    }

    fn ended(&self) -> bool {
        self.end
    }
}

/// A step that counts its turns and ends the pass at its `ends_at`-th turn.
struct EndingStep {
    turns: u32,
    ends_at: Option<u32>,
}

impl PassStep<EndingClock, ()> for EndingStep {
    fn has_work(&self, _: &EndingClock) -> bool {
        true
    }

    fn turn(&mut self, ctx: &mut EndingClock) -> Result<(), ()> {
        self.turns += 1;
        ctx.now += Duration::from_millis(1);
        if self.ends_at == Some(self.turns) {
            ctx.end = true;
        }
        Ok(())
    }
}

/// A turn that ends the pass is its last one, in the first round (the later steps get
/// no turn) and in the budgeted rounds alike.
#[test]
fn run_pass_stops_once_a_turn_ends_the_pass() {
    for (ends_at_step, ends_at_turn, expected) in [
        (0, 1, vec![1, 0, 0]),
        (1, 1, vec![1, 1, 0]),
        (2, 3, vec![3, 3, 3]),
    ] {
        let mut steps: Vec<EndingStep> = (0..3)
            .map(|i| EndingStep {
                turns: 0,
                ends_at: (i == ends_at_step).then_some(ends_at_turn),
            })
            .collect();
        let mut ctx = EndingClock::default();
        let mut refs: Vec<&mut dyn PassStep<EndingClock, ()>> = steps
            .iter_mut()
            .map(|s| s as &mut dyn PassStep<EndingClock, ()>)
            .collect();
        let report = run_pass(&mut refs, &mut ctx, Duration::from_secs(1))
            .unwrap_or_else(|()| unreachable!());
        assert_eq!(report.turns, expected, "ending at step {ends_at_step}");
        assert_eq!(steps.iter().map(|s| s.turns).collect::<Vec<_>>(), expected);
    }
}

/// The documented promise at budget 0: one turn for every step with work, no more.
#[test]
fn run_pass_at_budget_zero_gives_each_busy_step_one_turn() {
    let ms = Duration::from_millis;
    let mut a = FakeStep {
        costs: vec![ms(5); 10].into(),
    };
    let mut idle = FakeStep {
        costs: VecDeque::new(),
    };
    let mut b = FakeStep {
        costs: vec![ms(7); 10].into(),
    };
    let mut ctx = FakeClock::default();
    let report = run_pass(&mut [&mut a, &mut idle, &mut b], &mut ctx, Duration::ZERO)
        .unwrap_or_else(|()| unreachable!());
    assert_eq!(report.turns, vec![1, 0, 1]);
    assert_eq!(ctx.now, ms(12));
}

/// Time one step does not use goes to the others: with one step finishing early, the
/// other gets the rest of the budget, not a fixed share.
#[test]
fn run_pass_gives_unused_shares_to_steps_with_work() {
    let ms = Duration::from_millis;
    let mut short = FakeStep {
        costs: vec![ms(10); 2].into(),
    };
    let mut long = FakeStep {
        costs: vec![ms(10); 1_000].into(),
    };
    let mut ctx = FakeClock::default();
    let report = run_pass(&mut [&mut short, &mut long], &mut ctx, ms(1_500))
        .unwrap_or_else(|()| unreachable!());
    assert_eq!(report.turns, vec![2, 148]);
    assert_eq!(ctx.now, ms(1_500));
}

#[test]
fn lap_starts_after_the_cursor_and_serves_until_caught_up() {
    let cursor = KeyCursor::new();
    cursor.set(Some(20));
    let mut lap = Lap::new(
        vec![(30, 'c'), (10, 'a'), (20, 'b'), (30, 'x')],
        &cursor,
        10,
    );
    assert_eq!(lap.current(), Some(&(30, 'c')), "duplicates keep the first");
    lap.served(false);
    assert_eq!(cursor.get(), Some(30));
    assert_eq!(lap.current(), Some(&(30, 'c')), "not caught up: same item");
    lap.served(true);
    assert_eq!(lap.current(), Some(&(10, 'a')), "wraps");
    lap.push(10, 'z');
    lap.push(15, 'p');
    lap.served(true);
    lap.served(true);
    assert_eq!(
        lap.current(),
        Some(&(15, 'p')),
        "a pushed item joins at the end"
    );
    lap.stop();
    assert!(lap.is_done());
    assert_eq!(cursor.get(), Some(20));

    // The cap bounds the lap, pushed items included.
    let cursor = KeyCursor::new();
    let mut lap = Lap::new(vec![(1, ()), (2, ()), (3, ())], &cursor, 2);
    lap.push(4, ());
    lap.served(true);
    lap.served(true);
    assert!(lap.is_done());
}

/// Guarantee 1 for a lapped step: whatever the turns per pass (at least one) and
/// however items come and go between passes, an item present for `U` passes in a row
/// (`U` the number of keys that can exist) gets a turn among them. Items may need
/// several turns (a backlog), served in a row while the pass lasts.
#[test]
fn laps_reach_every_lasting_item_over_changing_lists() {
    const U: u64 = 24;
    let mut rng = Rng(0xdead_beef_cafe_f00d);
    for case in 0..300 {
        let cursor = KeyCursor::new();
        let mut present: BTreeSet<u64> = (0..U).filter(|_| rng.below(2) == 0).collect();
        let mut backlog: BTreeMap<u64, u64> = BTreeMap::new();
        // Since when each present item has waited for a turn.
        let mut waiting: BTreeMap<u64, u64> = present.iter().map(|&k| (k, 0)).collect();
        for pass in 0..400 {
            let mut lap = Lap::new(present.iter().map(|&k| (k, ())).collect(), &cursor, 64);
            let turns = 1 + rng.below(3);
            for _ in 0..turns {
                let Some(&(key, ())) = lap.current() else {
                    break;
                };
                waiting.insert(key, pass + 1);
                let left = backlog.entry(key).or_insert_with(|| rng.below(3));
                let caught_up = *left == 0;
                *left = left.saturating_sub(1);
                if caught_up {
                    backlog.remove(&key);
                }
                lap.served(caught_up);
            }
            for (&key, &since) in &waiting {
                assert!(
                    pass + 1 - since <= U,
                    "case {case} pass {pass}: item {key} waited since pass {since}"
                );
            }
            // Items come and go.
            for _ in 0..rng.below(3) {
                let key = rng.below(U);
                if present.remove(&key) {
                    waiting.remove(&key);
                    backlog.remove(&key);
                } else {
                    present.insert(key);
                    waiting.insert(key, pass + 1);
                }
            }
        }
    }
}

fn leaf_at(oid: u32, lower: i64, upper: i64) -> TimePartition {
    TimePartition {
        oid,
        name: format!("l{oid}"),
        lower_ms: Some(lower),
        upper_ms: Some(upper),
        is_default: false,
    }
}

#[test]
fn found_work_follows_the_latest_probe_and_forgets_what_no_longer_applies() {
    let a = leaf_at(1, 0, 100);
    let b = leaf_at(2, 100, 200);
    let mut found = FoundWork::new();
    found.record_probe(&a, &LeafAction::Trim { topics: vec![7, 8] });
    found.record_empty(&b);
    assert_eq!(found.trims(), vec![((7, 100), 1), ((8, 100), 1)]);
    assert!(found.has_drop(200, 2));
    assert!(!found.has_drop(200, 3), "another leaf with that key");

    // A new probe replaces what an older one found in that leaf.
    found.record_probe(&a, &LeafAction::Trim { topics: vec![8] });
    assert_eq!(found.trims(), vec![((8, 100), 1)]);
    found.record_probe(&b, &LeafAction::Keep);
    assert_eq!(found.drops(), vec![]);
    found.record_probe(
        &b,
        &LeafAction::Drop {
            expired_topics: vec![9],
        },
    );
    assert_eq!(
        found.drops(),
        vec![(
            200,
            FoundDrop {
                oid: 2,
                expired_topics: vec![9]
            }
        )]
    );

    // Still valid at now = 1000 with windows of 10 for topics 8 and 9: kept.
    let windows = |t: i32| (t == 8 || t == 9).then_some(10);
    found.retain_current(&[a.clone(), b.clone()], 1_000, windows);
    assert_eq!(found.trims().len(), 1);
    assert_eq!(found.drops().len(), 1);
    // A longer window no longer expires the whole leaf: the trim goes, and so does the
    // drop, which would otherwise remove rows the new window keeps.
    found.retain_current(&[a.clone(), b.clone()], 1_000, |_| Some(5_000));
    assert_eq!(found.trims(), vec![]);
    assert_eq!(found.drops(), vec![]);
    // The same for a window made infinite, or a topic deleted: neither has a window.
    found.record_probe(
        &b,
        &LeafAction::Drop {
            expired_topics: vec![9],
        },
    );
    found.retain_current(&[a.clone(), b.clone()], 1_000, |_| None);
    assert_eq!(found.drops(), vec![]);
    // An empty leaf's drop names no topic, so no window change touches it.
    found.record_empty(&b);
    found.retain_current(&[a.clone(), b.clone()], 1_000, |_| None);
    assert_eq!(found.drops().len(), 1);
    found.record_probe(
        &b,
        &LeafAction::Drop {
            expired_topics: vec![9],
        },
    );
    // A leaf gone (or a new leaf with its key): its work goes.
    found.retain_current(&[a.clone(), leaf_at(3, 100, 200)], 1_000, windows);
    assert_eq!(found.drops(), vec![]);

    found.record_probe(&a, &LeafAction::Trim { topics: vec![8] });
    found.trim_done((8, 100));
    found.record_empty(&b);
    found.drop_done(200);
    assert!(found.trims().is_empty() && found.drops().is_empty());
}

#[test]
fn expiry_items_are_found_trims_plus_row_level_items() {
    let width = 1_000;
    let past = leaf_at(1, 0, 1_000);
    let dropping = leaf_at(2, 1_000, 2_000);
    let behind = leaf_at(5, 4_000, 5_000);
    let current = leaf_at(3, 5_000, 6_000);
    let default = TimePartition {
        oid: 4,
        name: "d".into(),
        lower_ms: None,
        upper_ms: None,
        is_default: true,
    };
    let leaves = vec![
        default,
        past.clone(),
        dropping.clone(),
        current,
        behind.clone(),
    ];
    let mut found = FoundWork::new();
    // Topic 1 (window < width): the leaf holding its cutoff (5,490) and the newest leaf
    // wholly behind it, not the older past leaf (a probe finds its rows there as a
    // trim). Topic 2 (window >= width): only its found trim and the DEFAULT leaf.
    found.record_probe(&past, &LeafAction::Trim { topics: vec![2] });
    found.record_empty(&dropping);
    let finite = [(1, 10), (2, 2_000)];
    let clock = SweepClock {
        now_ms: 5_500,
        width_ms: width,
    };
    let keys = |found: &FoundWork, default_has_rows| {
        let mut k: Vec<(i32, u32)> = expiry_items(&leaves, &finite, clock, default_has_rows, found)
            .iter()
            .map(|i| (i.topic_id, i.leaf_oid))
            .collect();
        k.sort_unstable();
        k
    };
    assert_eq!(keys(&found, false), vec![(1, 3), (1, 5), (2, 1)]);
    assert_eq!(
        keys(&found, true),
        vec![(1, 3), (1, 4), (1, 5), (2, 1), (2, 4)]
    );
    // The leaf behind the cutoff found droppable: its drop removes the rows.
    found.record_empty(&behind);
    assert_eq!(keys(&found, false), vec![(1, 3), (2, 1)]);
}

/// One topic's rows in one leaf, in the model.
type Rows = BTreeMap<(i32, i64), u64>;

/// The model pass's shared state: the clock (one turn = 1 ms), the found work and
/// what the probes found this pass, and the rows.
struct ModelPass {
    clock: Duration,
    found: FoundWork,
    new_trims: Vec<ExpiryItem>,
    rows: Rows,
}

impl PassClock for ModelPass {
    fn elapsed(&self) -> Duration {
        self.clock
    }
}

/// The contents probes, as `ProbeStep` runs them: present topics are those with rows
/// in the leaf, plus the infinite-retention topic that keeps it.
struct ModelProbe<'a> {
    lap: Lap<'a, i64, TimePartition>,
    keeper: i32,
    now_ms: i64,
    windows: &'a BTreeMap<i32, i64>,
}

impl PassStep<ModelPass, ()> for ModelProbe<'_> {
    fn has_work(&self, _: &ModelPass) -> bool {
        !self.lap.is_done()
    }

    fn turn(&mut self, ctx: &mut ModelPass) -> Result<(), ()> {
        ctx.clock += Duration::from_millis(1);
        let Some((key, leaf)) = self.lap.current().cloned() else {
            return Ok(());
        };
        let mut present: Vec<i32> = ctx
            .rows
            .iter()
            .filter(|(&(_, k), &n)| k == key && n > 0)
            .map(|(&(t, _), _)| t)
            .collect();
        present.push(self.keeper);
        let action = classify_leaf(key, self.now_ms, &present, false, |t| {
            self.windows.get(&t).copied()
        });
        ctx.found.record_probe(&leaf, &action);
        if let LeafAction::Trim { topics } = action {
            for topic_id in topics {
                if let Some(&window_ms) = self.windows.get(&topic_id) {
                    ctx.new_trims.push(ExpiryItem {
                        topic_id,
                        window_ms,
                        leaf_oid: leaf.oid,
                        leaf_key: key,
                    });
                }
            }
        }
        self.lap.served(true);
        Ok(())
    }
}

/// Row expiry, as `ExpiryStep` runs it.
struct ModelExpiry<'a> {
    lap: Lap<'a, (i32, i64), ExpiryItem>,
}

impl PassStep<ModelPass, ()> for ModelExpiry<'_> {
    fn has_work(&self, ctx: &ModelPass) -> bool {
        !self.lap.is_done() || !ctx.new_trims.is_empty()
    }

    fn turn(&mut self, ctx: &mut ModelPass) -> Result<(), ()> {
        ctx.clock += Duration::from_millis(1);
        for item in ctx.new_trims.drain(..) {
            self.lap.push(item.key(), item);
        }
        let Some(&(key, _)) = self.lap.current() else {
            return Ok(());
        };
        let batch = RETENTION_DELETE_BATCH.unsigned_abs();
        let left = ctx.rows.get(&key).copied().unwrap_or(0);
        let deleted = left.min(batch);
        if left > 0 {
            ctx.rows.insert(key, left - deleted);
        }
        let caught_up = !batch_was_full(deleted, RETENTION_DELETE_BATCH);
        if caught_up {
            ctx.found.trim_done(key);
        }
        self.lap.served(caught_up);
        Ok(())
    }
}

/// Another step's backlog no pass clears (a client aborting transactions faster than
/// the cleanup deletes them).
struct Backlog;

impl PassStep<ModelPass, ()> for Backlog {
    fn has_work(&self, _: &ModelPass) -> bool {
        true
    }

    fn turn(&mut self, ctx: &mut ModelPass) -> Result<(), ()> {
        ctx.clock += Duration::from_millis(1);
        Ok(())
    }
}

/// Run the model until the long-window topic has no rows left, or past the bound of
/// guarantee 1; returns (passes taken, the bound, leaves never trimmed).
fn model_sweep(
    leaves_n: i64,
    shorts: i32,
    long_first: bool,
    target_window: i64,
    budget_ms: u64,
    backlog: bool,
    rng: &mut Rng,
) -> (u64, u64, Vec<i64>) {
    let width = DAY;
    // A1..An, then the current leaf C a few days later.
    let mut leaves: Vec<TimePartition> = (0..leaves_n)
        .map(|i| leaf_at(u32::try_from(i + 1).unwrap_or(0), i * DAY, (i + 1) * DAY))
        .collect();
    let c_lower = (leaves_n + 3) * DAY;
    leaves.push(leaf_at(1_000, c_lower, c_lower + DAY));
    let now_ms = c_lower + DAY / 2;
    // Topic ids: the long-window topic first or last among those with a window; the
    // infinite-retention keeper after them.
    let long = if long_first { 1 } else { shorts + 1 };
    let short_ids: Vec<i32> = (1..=shorts + 1).filter(|&t| t != long).collect();
    let keeper = shorts + 2;
    let mut windows: BTreeMap<i32, i64> = short_ids.iter().map(|&t| (t, 1_000)).collect();
    windows.insert(long, target_window);
    let finite: Vec<(i32, i64)> = windows.iter().map(|(&t, &w)| (t, w)).collect();

    let batch = RETENTION_DELETE_BATCH.unsigned_abs();
    let mut rows = Rows::new();
    for leaf in &leaves[..leaves.len() - 1] {
        // Some leaves without the topic, some backlogs of several batches.
        let n = rng.below(3 * batch + 1);
        if n > 0 {
            rows.insert((long, leaf_key(leaf)), n);
        }
    }
    let mut ctx = ModelPass {
        clock: Duration::ZERO,
        found: FoundWork::new(),
        new_trims: Vec::new(),
        rows,
    };
    let state = SweepState::new();
    let probe_candidates = u64::try_from(leaves_n).unwrap_or(0);
    let mut bound = 0;
    let mut pass = 0;
    while pass <= bound.max(1) {
        if ctx.rows.values().all(|&n| n == 0) {
            break;
        }
        pass += 1;
        ctx.clock = Duration::ZERO;
        let window_of = |t: i32| windows.get(&t).copied();
        ctx.found.retain_current(&leaves, now_ms, window_of);
        let (probe, empty) = split_past_leaves(&leaves, now_ms, Some(1_000));
        assert_eq!(empty, Vec::<&TimePartition>::new());
        let mut probes = ModelProbe {
            lap: Lap::new(
                probe.iter().map(|l| (leaf_key(l), (*l).clone())).collect(),
                &state.probe,
                MAX_CONTENTS_PROBES_PER_PASS,
            ),
            keeper,
            now_ms,
            windows: &windows,
        };
        let items = expiry_items(
            &leaves,
            &finite,
            SweepClock {
                now_ms,
                width_ms: width,
            },
            false,
            &ctx.found,
        );
        // Guarantee 1's bound: found within the probe rotation, then up to 4 turns
        // (3 batches and the short one) of one per expiry rotation. Every pass counts
        // the expiry items its lap holds, and the found trims grow towards one per leaf.
        let expiry_items_max =
            u64::try_from(items.len()).unwrap_or(0) + u64::try_from(leaves_n).unwrap_or(0);
        bound = bound.max(probe_candidates + 4 * expiry_items_max);
        let mut expiry = ModelExpiry {
            lap: Lap::new(
                items.into_iter().map(|i| (i.key(), i)).collect(),
                &state.expiry,
                usize::MAX,
            ),
        };
        let mut other = Backlog;
        let budget = Duration::from_millis(budget_ms);
        if backlog {
            run_pass(
                &mut [&mut other, &mut probes, &mut expiry],
                &mut ctx,
                budget,
            )
        } else {
            run_pass(&mut [&mut probes, &mut expiry], &mut ctx, budget)
        }
        .unwrap_or_else(|()| unreachable!());
    }
    let never: Vec<i64> = ctx
        .rows
        .iter()
        .filter(|(_, &n)| n > 0)
        .map(|(&(_, k), _)| k / DAY)
        .collect();
    (pass, bound, never)
}

/// One day, the model's leaf width, in milliseconds.
const DAY: i64 = 86_400_000;

/// Guarantee 4 (and 1 for the trims): a long-window topic's rows in leaves kept by an
/// infinite-retention topic are trimmed in every such leaf, within the bound, at every
/// budget (0 included), with any number of short-window topics whose row-level items
/// fill the expiry rotation, with the long topic's items first or last, and behind
/// another step's endless backlog. When a trim existed only in the pass whose probe
/// reached its leaf, the probe rotation and the expiry rotation phase-locked and some
/// leaves were never trimmed (5 leaves, one short topic, budget 0: never the first).
/// The same for a target topic whose window is shorter than one leaf (a backlog left
/// in every past leaf, as when a window is shortened): its row-level items cover only
/// the leaves at its cutoff, so the probes must find its rows in the older ones.
#[test]
fn trims_reach_every_kept_leaf_at_any_budget() {
    let mut rng = Rng(0x0123_4567_89ab_cdef);
    let mut configs = 0;
    for target_window in [DAY, 1_000] {
        for leaves in [2, 3, 4, 5, 6, 8, 13, 40, 70] {
            for shorts in [1, 2, 3, 5] {
                for long_first in [false, true] {
                    for budget_ms in [0, 1, 2, 3, 7, 64] {
                        for backlog in [false, true] {
                            let (passes, bound, never) = model_sweep(
                                leaves,
                                shorts,
                                long_first,
                                target_window,
                                budget_ms,
                                backlog,
                                &mut rng,
                            );
                            assert!(
                                never.is_empty() && passes <= bound,
                                "{leaves} leaves, {shorts} short topics, long first \
                                 {long_first}, target window {target_window} ms, budget \
                                 {budget_ms} ms, backlog {backlog}: after {passes} passes \
                                 (bound {bound}) leaves {never:?} still hold expired rows"
                            );
                            configs += 1;
                        }
                    }
                }
            }
        }
    }
    assert_eq!(configs, 2 * 9 * 4 * 2 * 6 * 2);
}
