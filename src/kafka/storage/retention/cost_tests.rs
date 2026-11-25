//! The per-pass cost of the sweep's planning as a function of what clients can create
//! (topics, their windows, leaves kept by their rows, the trims the probes find),
//! counted rather than timed: comparisons of a lap's keys, trim entries a probe
//! examines.

use super::*;
use std::cell::Cell;
use std::cmp::Ordering;

thread_local! {
    /// Key comparisons (`==` and `cmp`) made by [`Counted`] on this thread.
    static COMPARISONS: Cell<u64> = const { Cell::new(0) };
}

fn comparisons() -> u64 {
    COMPARISONS.with(Cell::get)
}

/// A lap key that counts every comparison made with it.
#[derive(Debug, Clone, Copy, Eq)]
struct Counted(i64);

impl PartialEq for Counted {
    fn eq(&self, other: &Self) -> bool {
        COMPARISONS.with(|c| c.set(c.get() + 1));
        self.0 == other.0
    }
}

impl Ord for Counted {
    fn cmp(&self, other: &Self) -> Ordering {
        COMPARISONS.with(|c| c.set(c.get() + 1));
        self.0.cmp(&other.0)
    }
}

impl PartialOrd for Counted {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// `n * log2(n)`, rounded up, for a comparison bound.
fn n_log_n(n: u64) -> u64 {
    n * u64::from(n.bit_width())
}

/// A probe of a kept leaf can find a trim for every topic in it, and the expiry step
/// pushes each into its lap: the push must not compare the key with every item the lap
/// holds (that was T^2/2 comparisons for T topics, 5.6 s at 100,000, every pass until
/// drained). Pushing `n` new keys and then each again costs O(n log n) comparisons.
#[test]
fn lap_push_costs_log_comparisons_per_item() {
    let n: u64 = 5_000;
    let cursor = KeyCursor::new();
    let mut lap: Lap<'_, Counted, u32> = Lap::new(Vec::new(), &cursor, usize::MAX);
    let before = comparisons();
    for round in 0..2 {
        for k in 0..n {
            lap.push(Counted(i64::try_from(k).unwrap_or(0)), round);
        }
    }
    let used = comparisons() - before;
    let mut served = 0;
    while let Some(&(_, round)) = lap.current() {
        assert_eq!(round, 0, "a repeated key replaced the first");
        served += 1;
        lap.served(true);
    }
    assert_eq!(served, n, "every distinct key is an item, once");
    assert!(
        used <= 4 * n_log_n(2 * n),
        "{used} comparisons to push {n} keys twice (bound {})",
        4 * n_log_n(2 * n)
    );
}

/// Probing a leaf replaces what was found there before; it must touch only that leaf's
/// trims, not every trim found in every leaf (an O(all trims) scan per probe, 64 probes a
/// pass). Probing `L` leaves of `T` wholly expired topics each, twice, examines at most
/// the trims it removes and records.
#[test]
fn probe_touches_only_its_leaf_trims() {
    let topics: i32 = 2_000;
    let leaves: u32 = 64;
    let mut found = FoundWork::new();
    let leaf = |i: u32| TimePartition {
        oid: i + 1,
        name: format!("l{i}"),
        lower_ms: Some(i64::from(i) * 1_000),
        upper_ms: Some(i64::from(i + 1) * 1_000),
        is_default: false,
    };
    let trim = LeafAction::Trim {
        topics: (0..topics).collect(),
    };
    for _ in 0..2 {
        for i in 0..leaves {
            found.record_probe(&leaf(i), &trim);
        }
    }
    let total = u64::from(leaves) * u64::try_from(topics).unwrap_or(0);
    assert_eq!(u64::try_from(found.trims().len()).unwrap_or(0), total);
    // The second round removes what the first recorded: each trim once.
    assert!(
        found.probe_work <= total,
        "{} trim entries examined for {total} trims replaced",
        found.probe_work
    );
}
