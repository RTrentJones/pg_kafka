//! In-flight forward tracking for the durable outbox.
//!
//! The DB thread registers each outbox row when it hands the forward to the
//! network thread and clears it when the ack is applied. The outbox claim
//! excludes registered rows, so a forward that is merely slow (external-broker
//! warmup, batching, a long delivery) is never claimed and sent a second time;
//! a row whose attempt is still open in the table but not registered here lost
//! its ack, and the claim counts that lost attempt as a failure.
//!
//! Delivery stays at-least-once: entries are evicted after `evict_after`
//! (sized above rdkafka's `message.timeout.ms`, after which a delivery report,
//! success or failure, is guaranteed to have been produced), so a genuinely
//! lost ack (network-thread crash) only delays the re-claim, never prevents it.
//! The tracker is in memory: a worker restart forgets it, and the open
//! attempts in the table drive recovery.
//!
//! Thread model: touched only from the single DB worker thread (the poll and
//! the ack drain both run there). The mutex exists because `ShadowStore` must
//! be `Sync`; it is never contended.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use super::{KeyColumns, RowKey};

/// Tracks outbox rows whose forward has been dispatched to the network thread
/// but whose `ForwardAck` has not yet been applied.
pub struct InflightTracker {
    entries: Mutex<HashMap<RowKey, Instant>>,
    evict_after: Duration,
}

impl InflightTracker {
    pub fn new(evict_after: Duration) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            evict_after,
        }
    }

    /// Register `key` as in flight and report whether the caller should
    /// dispatch it. Returns `false` when a live (non-evicted) forward for the
    /// same row is already outstanding — the caller must skip the re-send.
    /// A stale entry (older than `evict_after`, meaning its ack was lost) is
    /// replaced and dispatch proceeds.
    pub fn try_begin(&self, key: RowKey) -> bool {
        self.try_begin_at(key, Instant::now())
    }

    /// Clear `key` after its ack (success or failure) has been applied, so a
    /// later claim may dispatch it again.
    pub fn complete(&self, key: &RowKey) {
        self.lock().remove(key);
    }

    /// Number of live forwards. Stale entries (older than `evict_after`: their
    /// ack was lost, and the claim treats their rows as claimable again) are
    /// dropped first, so lost acks never use up the in-flight room.
    pub fn len(&self) -> usize {
        self.len_at(Instant::now())
    }

    fn len_at(&self, now: Instant) -> usize {
        let mut entries = self.lock();
        entries.retain(|_, started| now.saturating_duration_since(*started) < self.evict_after);
        entries.len()
    }

    /// The live (non-evicted) in-flight keys, shaped for the outbox claim's
    /// SQL exclusion: claiming an in-flight row would bump its `retry_count`
    /// and erode the dead-letter budget even though the dispatcher skips it.
    pub fn live_keys(&self) -> KeyColumns {
        self.live_keys_at(Instant::now())
    }

    fn live_keys_at(&self, now: Instant) -> KeyColumns {
        let entries = self.lock();
        entries
            .iter()
            .filter(|(_, started)| now.saturating_duration_since(**started) < self.evict_after)
            .map(|(key, _)| key)
            .collect()
    }

    /// Clock-injected core of [`try_begin`], separated for deterministic tests.
    fn try_begin_at(&self, key: RowKey, now: Instant) -> bool {
        let mut entries = self.lock();
        match entries.get(&key) {
            Some(started) if now.saturating_duration_since(*started) < self.evict_after => false,
            _ => {
                // Absent, or stale (ack lost): (re-)register and dispatch.
                entries.insert(key, now);
                true
            }
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<RowKey, Instant>> {
        super::lock(&self.entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EVICT: Duration = Duration::from_secs(150);
    const KEY: RowKey = RowKey::new(1, 0, 42);

    /// A dispatched row is not re-dispatched while its ack is pending (it
    /// would be delivered twice).
    #[test]
    fn test_second_dispatch_blocked_while_in_flight() {
        let tracker = InflightTracker::new(EVICT);
        assert!(tracker.try_begin(KEY), "first dispatch must proceed");
        assert!(
            !tracker.try_begin(KEY),
            "re-claimed row with a pending ack must be skipped"
        );
        assert_eq!(tracker.len(), 1);
    }

    /// After the ack is applied the row may be dispatched again (a later
    /// legitimate retry after a failure ack).
    #[test]
    fn test_dispatch_allowed_again_after_complete() {
        let tracker = InflightTracker::new(EVICT);
        assert!(tracker.try_begin(KEY));
        tracker.complete(&KEY);
        assert_eq!(tracker.len(), 0);
        assert!(tracker.try_begin(KEY), "completed row must be dispatchable");
    }

    /// A stale entry (ack lost — e.g. dropped on a full channel) must not
    /// block the row forever: past `evict_after` the dispatch proceeds and the
    /// entry is re-stamped. This is what keeps delivery at-least-once.
    #[test]
    fn test_stale_entry_evicted_and_redispatched() {
        let tracker = InflightTracker::new(EVICT);
        let t0 = Instant::now();
        assert!(tracker.try_begin_at(KEY, t0));
        // Just under the eviction horizon: still blocked.
        assert!(!tracker.try_begin_at(KEY, t0 + EVICT.saturating_sub(Duration::from_millis(1))));
        // Past it: the lost-ack forward is presumed dead, dispatch resumes.
        assert!(tracker.try_begin_at(KEY, t0 + EVICT));
        // And the new dispatch is itself tracked from the new timestamp.
        assert!(!tracker.try_begin_at(KEY, t0 + EVICT + Duration::from_millis(1)));
    }

    /// The SQL-exclusion snapshot must list live entries and omit evicted
    /// ones — a stale (lost-ack) entry left in the snapshot would block the
    /// row's re-claim forever.
    #[test]
    fn test_live_keys_snapshot_excludes_evicted() {
        let tracker = InflightTracker::new(EVICT);
        let t0 = Instant::now();
        assert!(tracker.try_begin_at(RowKey::new(1, 0, 10), t0));
        assert!(tracker.try_begin_at(RowKey::new(1, 0, 11), t0 + EVICT / 2));

        let live = tracker.live_keys_at(t0 + EVICT / 2);
        assert_eq!(
            live.topic_ids.len(),
            2,
            "both entries live at half the horizon"
        );
        assert_eq!(live.partition_ids.len(), 2);
        assert_eq!(live.local_offsets.len(), 2);

        // Past the first entry's horizon only the second remains.
        let live = tracker.live_keys_at(t0 + EVICT);
        assert_eq!(live.topic_ids, vec![1]);
        assert_eq!(live.local_offsets, vec![11]);
    }

    /// Entries whose ack was lost stop counting once they are stale, so a
    /// tracker filled with them recovers its room instead of staying full.
    #[test]
    fn test_len_drops_stale_entries() {
        let tracker = InflightTracker::new(EVICT);
        let t0 = Instant::now();
        for o in 0..8_192 {
            assert!(tracker.try_begin_at(RowKey::new(1, 0, o), t0));
        }
        assert!(tracker.try_begin_at(RowKey::new(2, 0, 0), t0 + EVICT / 2));
        assert_eq!(tracker.len_at(t0 + EVICT / 2), 8_193);
        assert_eq!(tracker.len_at(t0 + EVICT), 1, "only the live entry counts");
        assert!(
            !tracker.try_begin_at(RowKey::new(2, 0, 0), t0 + EVICT),
            "a live entry is kept"
        );
        assert!(tracker.try_begin_at(RowKey::new(1, 0, 7), t0 + EVICT));
    }

    /// Distinct rows are tracked independently.
    #[test]
    fn test_keys_are_independent() {
        let tracker = InflightTracker::new(EVICT);
        assert!(tracker.try_begin(RowKey::new(1, 0, 1)));
        assert!(tracker.try_begin(RowKey::new(1, 0, 2)));
        assert!(tracker.try_begin(RowKey::new(2, 0, 1)));
        assert_eq!(tracker.len(), 3);
        tracker.complete(&RowKey::new(1, 0, 2));
        assert!(!tracker.try_begin(RowKey::new(1, 0, 1)));
        assert!(tracker.try_begin(RowKey::new(1, 0, 2)));
    }
}
