//! In-flight forward tracking for the durable outbox (issue #93).
//!
//! The outbox claim query re-claims a pending row once its retry lease
//! (`OUTBOX_RETRY_INTERVAL_MS`) expires. That lease is a crash-recovery
//! mechanism, but it also fired while a first forward was still legitimately
//! in flight (external-broker warmup, batching, slow acks), re-sending the
//! record and delivering ~8% duplicates to the external broker on async
//! paths. This tracker closes that gap: the DB thread registers each row when
//! it hands the forward to the network thread and clears it when the ack
//! comes back, and the outbox dispatcher skips any re-claimed row that is
//! still registered — the re-send now happens only when the ack was truly
//! lost, not merely late.
//!
//! Delivery semantics stay at-least-once: entries are evicted after
//! `evict_after` (sized above rdkafka's `message.timeout.ms`, after which a
//! delivery ack — success or failure — is guaranteed to have been produced),
//! so a genuinely lost ack (dropped on a full channel, network-thread crash)
//! only delays the lease-based retry, never prevents it. The tracker is
//! in-memory: a worker restart forgets in-flight state, and the lease alone
//! governs recovery, exactly as before.
//!
//! Thread model: touched only from the single DB worker thread (the poll and
//! the ack drain both run there). The mutex exists because `ShadowStore` must
//! be `Sync`; it is never contended.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Outbox row identity: (topic_id, partition_id, local_offset).
pub type ForwardKey = (i32, i32, i64);

/// Tracks outbox rows whose forward has been dispatched to the network thread
/// but whose `ForwardAck` has not yet been applied.
pub struct InflightTracker {
    entries: Mutex<HashMap<ForwardKey, Instant>>,
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
    pub fn try_begin(&self, key: ForwardKey) -> bool {
        self.try_begin_at(key, Instant::now())
    }

    /// Clear `key` after its ack (success or failure) has been applied, so a
    /// future lease-expiry re-claim may dispatch it again.
    pub fn complete(&self, key: &ForwardKey) {
        self.lock().remove(key);
    }

    /// Number of currently registered forwards (stale entries included).
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Snapshot of the live (non-evicted) in-flight keys as three parallel
    /// column vectors, shaped for a SQL `unnest($t::int4[], $p::int4[],
    /// $o::int8[])` exclusion in the outbox claim query — claiming an
    /// in-flight row would bump its `retry_count` and erode the dead-letter
    /// budget even though the dispatcher skips it.
    pub fn live_keys(&self) -> (Vec<i32>, Vec<i32>, Vec<i64>) {
        self.live_keys_at(Instant::now())
    }

    fn live_keys_at(&self, now: Instant) -> (Vec<i32>, Vec<i32>, Vec<i64>) {
        let entries = self.lock();
        let mut topics = Vec::with_capacity(entries.len());
        let mut partitions = Vec::with_capacity(entries.len());
        let mut offsets = Vec::with_capacity(entries.len());
        for ((t, p, o), started) in entries.iter() {
            if now.saturating_duration_since(*started) < self.evict_after {
                topics.push(*t);
                partitions.push(*p);
                offsets.push(*o);
            }
        }
        (topics, partitions, offsets)
    }

    /// Clock-injected core of [`try_begin`], separated for deterministic tests.
    fn try_begin_at(&self, key: ForwardKey, now: Instant) -> bool {
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

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<ForwardKey, Instant>> {
        self.entries.lock().unwrap_or_else(|poisoned| {
            tracing::warn!("inflight tracker lock was poisoned, recovering");
            poisoned.into_inner()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EVICT: Duration = Duration::from_secs(150);
    const KEY: ForwardKey = (1, 0, 42);

    /// A dispatched row must not be re-dispatched while its ack is pending —
    /// this is the exact mechanism behind the issue #93 duplicates.
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
        assert!(tracker.is_empty());
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
        assert!(!tracker.try_begin_at(KEY, t0 + EVICT - Duration::from_millis(1)));
        // Past it: the lost-ack forward is presumed dead, dispatch resumes.
        assert!(tracker.try_begin_at(KEY, t0 + EVICT));
        // And the new dispatch is itself tracked from the new timestamp.
        assert!(!tracker.try_begin_at(KEY, t0 + EVICT + Duration::from_millis(1)));
    }

    /// The SQL-exclusion snapshot must list live entries and omit evicted
    /// ones — a stale (lost-ack) entry left in the snapshot would block the
    /// row's lease-based retry at the claim query forever.
    #[test]
    fn test_live_keys_snapshot_excludes_evicted() {
        let tracker = InflightTracker::new(EVICT);
        let t0 = Instant::now();
        assert!(tracker.try_begin_at((1, 0, 10), t0));
        assert!(tracker.try_begin_at((1, 0, 11), t0 + EVICT / 2));

        let (t, p, o) = tracker.live_keys_at(t0 + EVICT / 2);
        assert_eq!(t.len(), 2, "both entries live at half the horizon");
        assert_eq!(p.len(), 2);
        assert_eq!(o.len(), 2);

        // Past the first entry's horizon only the second remains.
        let (t, _, o) = tracker.live_keys_at(t0 + EVICT);
        assert_eq!(t, vec![1]);
        assert_eq!(o, vec![11]);
    }

    /// Distinct rows are tracked independently.
    #[test]
    fn test_keys_are_independent() {
        let tracker = InflightTracker::new(EVICT);
        assert!(tracker.try_begin((1, 0, 1)));
        assert!(tracker.try_begin((1, 0, 2)));
        assert!(tracker.try_begin((2, 0, 1)));
        assert_eq!(tracker.len(), 3);
        tracker.complete(&(1, 0, 2));
        assert!(!tracker.try_begin((1, 0, 1)));
        assert!(tracker.try_begin((1, 0, 2)));
    }
}
