//! Pure planning logic for the expired-message part of the retention sweep.
//!
//! `PostgresStore::run_retention_sweep` (SPI, not unit-testable) deletes expired
//! messages in bounded batches. The decisions it makes — which retention window a
//! topic has, whether to run another batch, and which topic a pass starts from —
//! live here so they can be unit tested.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

/// Rows deleted per DELETE statement. Small enough that one statement holds the DB
/// thread briefly; the sweep runs as many batches as fit in [`RETENTION_PASS_BUDGET`].
pub const RETENTION_DELETE_BATCH: i64 = 10_000;

/// Wall-clock budget for the expired-message deletes of one sweep pass. The sweep
/// runs on the single worker DB thread, so a pass must stay bounded; within the
/// budget it keeps deleting batches until every topic is caught up. A backlog
/// larger than one pass can clear drains over the following passes.
pub const RETENTION_PASS_BUDGET: Duration = Duration::from_millis(1500);

/// Upper clamp for a retention window in milliseconds (1e13 ms, about 317 years).
/// Larger values, such as `i64::MAX` used for "forever", overflow the interval or
/// timestamp arithmetic in SQL and would abort the whole sweep.
pub const MAX_RETENTION_WINDOW_MS: i64 = 10_000_000_000_000;

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
        Some(_) => None,
        None if global_retention_hours > 0 => {
            Some((global_retention_hours as i64 * 3_600_000).min(MAX_RETENTION_WINDOW_MS))
        }
        None => None,
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
    batch_size > 0 && last_batch_deleted >= batch_size as u64 && elapsed < budget
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
    fn rotation_wraps_and_keeps_every_item() {
        let v = vec![1, 2, 3, 4];
        assert_eq!(rotated(&v, 0), vec![1, 2, 3, 4]);
        assert_eq!(rotated(&v, 1), vec![2, 3, 4, 1]);
        assert_eq!(rotated(&v, 5), vec![2, 3, 4, 1]);
        assert!(rotated::<i32>(&[], 3).is_empty());
    }

    #[test]
    fn start_index_stays_in_range() {
        assert_eq!(next_start_index(0), 0);
        for _ in 0..10 {
            assert!(next_start_index(3) < 3);
        }
    }
}
