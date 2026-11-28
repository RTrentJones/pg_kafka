//! Registry of the Fetch long polls waiting for data (network thread only).
//!
//! It maps a topic-partition to the long polls waiting on it. A notification from the DB
//! thread marks, in the long poll's [`Wake`], WHICH of its partitions changed and wakes it;
//! the long poll then re-reads only those partitions (`listener::long_poll`), as Kafka's
//! `DelayedFetch` re-checks a fetch when one of the partitions it watches changes. It uses
//! only tokio and std primitives and never calls pgrx.
//!
//! Every operation runs under the registry's lock on a network I/O thread, so its cost is
//! bounded by what the caller sent or by the long polls the notification concerns, never by
//! what other long polls registered: a long poll's entries are keyed by its [`FetchId`], so
//! registering or unregistering one costs one hash map step per entry it names, whatever else
//! waits on the same partitions, and a partition a Fetch names twice is held once (Kafka's
//! `DelayedFetch` watches each partition once).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};
use tokio::sync::futures::Notified;
use tokio::sync::{Notify, RwLock};

/// Identifies one long poll's registration (see [`PendingFetchRegistry::register`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FetchId(u64);

/// How one long poll is woken and learns what changed since it last looked.
///
/// A registration numbers the long poll's partition entries 0, 1, ... in the order it names
/// them (its slots); a notification marks the slots it concerns and wakes the long poll,
/// which [`take`](Self::take)s the marks before re-reading. Each slot is marked at most once
/// between two takes, so the marks hold at most one entry per registered slot: O(slots)
/// memory, O(1) per mark and O(marked slots) per take.
#[derive(Debug, Default)]
pub struct Wake {
    notify: Notify,
    changes: Mutex<Changes>,
}

#[derive(Debug, Default)]
struct Changes {
    /// Whether each slot is in `slots`.
    marked: Vec<bool>,
    /// The slots marked since the last take, each once.
    slots: Vec<usize>,
    /// Something may have changed that no notification names (see
    /// [`PendingFetchRegistry::notify_coarse`]).
    coarse: bool,
}

/// What changed for a long poll since its last [`Wake::take`].
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Taken {
    /// The slots a notification named, each once.
    pub slots: Vec<usize>,
    /// Something may have changed that no notification names.
    pub coarse: bool,
}

impl Wake {
    /// A wake handle with no slots yet (a registration sizes it).
    pub fn new() -> Self {
        Self::default()
    }

    fn changes(&self) -> MutexGuard<'_, Changes> {
        // The lock is held only for a few vector operations that cannot panic midway; a
        // poisoned lock still holds consistent marks.
        self.changes.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Size the marks for `slots` slots.
    fn set_slots(&self, slots: usize) {
        let mut changes = self.changes();
        changes.marked = vec![false; slots];
        changes.slots.clear();
    }

    /// Wait for the next wake-up. `Notify::notify_one` stores a permit when nobody is
    /// waiting, so a mark made while the long poll is busy elsewhere wakes this at once.
    pub fn notified(&self) -> Notified<'_> {
        self.notify.notified()
    }

    /// Mark `slot` as changed and wake the long poll.
    fn mark(&self, slot: usize) {
        {
            let mut changes = self.changes();
            if changes.marked.get(slot) == Some(&false) {
                changes.marked[slot] = true;
                changes.slots.push(slot);
            }
        }
        self.notify.notify_one();
    }

    /// Mark that something may have changed unnamed, and wake the long poll.
    fn mark_coarse(&self) {
        self.changes().coarse = true;
        self.notify.notify_one();
    }

    /// The changes marked since the last take; clears them.
    pub fn take(&self) -> Taken {
        let mut changes = self.changes();
        let slots = std::mem::take(&mut changes.slots);
        for &slot in &slots {
            changes.marked[slot] = false;
        }
        let taken = Taken {
            slots,
            coarse: changes.coarse,
        };
        changes.coarse = false;
        taken
    }
}

/// The long polls waiting on one topic: partition -> long poll -> its entry.
///
/// Keyed by topic **name** (not the internal topic ID) because a Fetch on the network thread
/// only has the client-supplied topic name; resolving it to an ID would take an SPI round-trip
/// on the DB thread. The DB thread's notifications carry the name. The name is held once per
/// topic, however many long polls wait on it: a partition entry takes 16 bytes on the wire
/// and a name up to the whole frame.
type TopicWaiters = HashMap<i32, HashMap<FetchId, PendingFetch>>;

/// One long poll waiting on one topic-partition.
#[derive(Debug)]
struct PendingFetch {
    /// The handle that wakes the long poll (shared by all its partitions).
    wake: Arc<Wake>,
    /// This partition's slot in the long poll.
    slot: usize,
    /// The offset the long poll fetches from: new data is relevant when the high watermark
    /// passes it.
    fetch_offset: i64,
    /// Whether the long poll reads committed records only (a transaction end concerns it).
    read_committed: bool,
}

#[derive(Debug, Default)]
struct Waiters {
    /// Topic name -> partition -> the long polls waiting on it.
    by_topic: HashMap<String, TopicWaiters>,
    /// Every long poll, each once.
    all: HashMap<FetchId, Arc<Wake>>,
}

/// How many partitions the safety-net re-checks of all long polls together may re-read per
/// `pg_kafka.fetch_poll_interval_ms` (see [`PendingFetchRegistry::take_safety_net`]).
pub const SAFETY_NET_PARTITIONS_PER_INTERVAL: u64 = 256;

/// The safety net's allowance in the current interval.
#[derive(Debug)]
struct SafetyNet {
    /// When the current interval started.
    since: Instant,
    /// Partitions left to re-read in it.
    left: u64,
}

/// Registry of pending fetch requests waiting for data
///
/// This is the central coordination point for long polling. Fetch handlers
/// register themselves when waiting, and the notification receiver wakes
/// them when new data arrives.
pub struct PendingFetchRegistry {
    inner: RwLock<Waiters>,
    next_id: AtomicU64,
    safety_net: Mutex<SafetyNet>,
}

impl Default for PendingFetchRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl PendingFetchRegistry {
    /// Create a new empty registry
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(Waiters::default()),
            next_id: AtomicU64::new(0),
            safety_net: Mutex::new(SafetyNet {
                since: Instant::now(),
                left: SAFETY_NET_PARTITIONS_PER_INTERVAL,
            }),
        }
    }

    /// Register one long poll on `topics`, each a topic name and its `(partition id, offset
    /// fetched from)` entries, under one `wake` handle the long poll awaits; returns the id
    /// [`unregister`](Self::unregister) takes. The entries are the long poll's slots, numbered
    /// in the order given.
    ///
    /// A partition named more than once is registered once, with its last entry's offset and
    /// slot (the offset the Fetch handler reads it from). Cost: one hash of each topic name
    /// entry and one hash map insert per partition entry, O(bytes of `topics`) time; memory
    /// O(entries), plus each distinct topic name once if no other long poll holds it.
    ///
    /// The caller should register every requested partition BEFORE its first fetch attempt and
    /// keep the registration until the long poll ends, then `unregister`: a notification that
    /// arrives while a fetch attempt is in flight stays marked, and the next
    /// [`Wake::notified`] returns at once.
    pub async fn register<'a, P>(
        &self,
        topics: impl IntoIterator<Item = (&'a str, P)>,
        wake: &Arc<Wake>,
        read_committed: bool,
    ) -> FetchId
    where
        P: IntoIterator<Item = (i32, i64)>,
    {
        let id = FetchId(self.next_id.fetch_add(1, Ordering::Relaxed));
        let mut guard = self.inner.write().await;
        let waiters = &mut *guard;
        let mut slot = 0usize;
        for (topic_name, partitions) in topics {
            let topic = if let Some(topic) = waiters.by_topic.get_mut(topic_name) {
                topic
            } else {
                waiters.by_topic.entry(topic_name.to_string()).or_default()
            };
            for (partition_id, fetch_offset) in partitions {
                topic.entry(partition_id).or_default().insert(
                    id,
                    PendingFetch {
                        wake: Arc::clone(wake),
                        slot,
                        fetch_offset,
                        read_committed,
                    },
                );
                slot += 1;
            }
        }
        wake.set_slots(slot);
        waiters.all.insert(id, Arc::clone(wake));
        id
    }

    /// Records were stored in a topic-partition, up to `high_watermark`: wake the long polls
    /// waiting on it from an offset below that, marking that partition (a long poll at or
    /// past it has nothing new to read). Cost: one step per long poll waiting on the
    /// partition (each holds it once).
    pub async fn notify_new_data(&self, topic_name: &str, partition_id: i32, high_watermark: i64) {
        let guard = self.inner.read().await;
        let waiting = guard
            .by_topic
            .get(topic_name)
            .and_then(|waiters| waiters.get(&partition_id));
        for pending in waiting.into_iter().flat_map(HashMap::values) {
            if high_watermark > pending.fetch_offset {
                pending.wake.mark(pending.slot);
            }
        }
    }

    /// A topic-partition's log start moved (DeleteRecords): wake every long poll waiting on
    /// it, marking that partition, whatever its offset (one below the new start is now out
    /// of range). Cost: one step per long poll waiting on the partition.
    pub async fn notify_partition_changed(&self, topic_name: &str, partition_id: i32) {
        self.notify_new_data(topic_name, partition_id, i64::MAX)
            .await;
    }

    /// A topic was deleted: wake every long poll waiting on any of its partitions, marking
    /// them. Cost: one step per (long poll, partition) registered on the topic.
    pub async fn notify_topic_changed(&self, topic_name: &str) {
        let guard = self.inner.read().await;
        let waiting = guard.by_topic.get(topic_name);
        for pending in waiting
            .into_iter()
            .flat_map(HashMap::values)
            .flat_map(HashMap::values)
        {
            pending.wake.mark(pending.slot);
        }
    }

    /// A transaction that added `partitions` of `topic_name` ended: wake the
    /// `read_committed` long polls waiting on any of them, marking those partitions only
    /// (the end can move their last stable offset, whatever the offset a long poll reads
    /// from: an open transaction holds back what it may read). `read_uncommitted` long
    /// polls saw the records when they were produced and are left alone; a long poll's
    /// other partitions are not marked, as Kafka's transaction markers complete only the
    /// `DelayedFetch`es watching the partitions they are written to. Cost: one step per
    /// long poll waiting on each named partition, as for a Produce to them.
    pub async fn notify_transaction_ended(&self, topic_name: &str, partitions: &[i32]) {
        let guard = self.inner.read().await;
        let Some(topic) = guard.by_topic.get(topic_name) else {
            return;
        };
        for pending in partitions
            .iter()
            .filter_map(|partition_id| topic.get(partition_id))
            .flat_map(HashMap::values)
        {
            if pending.read_committed {
                pending.wake.mark(pending.slot);
            }
        }
    }

    /// Something may have changed that no notification names (retention removed records,
    /// or notifications were dropped): wake every long poll with the coarse mark, which it
    /// re-checks only within the safety net's allowance ([`Self::take_safety_net`]) and
    /// otherwise answers from a fresh read at its deadline. Cost: one step per long poll.
    pub async fn notify_coarse(&self) {
        let guard = self.inner.read().await;
        for wake in guard.all.values() {
            wake.mark_coarse();
        }
    }

    /// Take `partitions` from the safety net's allowance for this `interval`, if that many
    /// are left: every long poll's re-checks that no notification asked for (its safety-net
    /// interval elapsed, or a coarse wake) together re-read at most
    /// [`SAFETY_NET_PARTITIONS_PER_INTERVAL`] partitions per interval, whatever the number of
    /// long polls and their partitions. A long poll of more partitions than that never takes
    /// it, and answers from a fresh read at its deadline. O(1).
    pub fn take_safety_net(&self, partitions: usize, interval: Duration) -> bool {
        let mut net = self
            .safety_net
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let now = Instant::now();
        if now.duration_since(net.since) >= interval {
            net.since = now;
            net.left = SAFETY_NET_PARTITIONS_PER_INTERVAL;
        }
        match u64::try_from(partitions) {
            Ok(partitions) if partitions <= net.left => {
                net.left -= partitions;
                true
            }
            _ => false,
        }
    }

    /// Remove the registration `id` from `topics`, the topic names and partition ids it was
    /// registered with (repeats allowed). Cost: one hash of each topic name entry and one
    /// hash map removal per partition entry, O(bytes of `topics`) time, whatever else waits
    /// on those partitions.
    pub async fn unregister<'a, P>(
        &self,
        id: FetchId,
        topics: impl IntoIterator<Item = (&'a str, P)>,
    ) where
        P: IntoIterator<Item = i32>,
    {
        let mut guard = self.inner.write().await;
        let waiters = &mut *guard;
        waiters.all.remove(&id);
        for (topic_name, partitions) in topics {
            let Some(topic) = waiters.by_topic.get_mut(topic_name) else {
                continue;
            };
            for partition_id in partitions {
                if let Some(pending) = topic.get_mut(&partition_id) {
                    pending.remove(&id);
                    if pending.is_empty() {
                        topic.remove(&partition_id);
                    }
                }
            }
            if topic.is_empty() {
                waiters.by_topic.remove(topic_name);
            }
        }
    }

    /// The number of (long poll, topic-partition) registrations (for testing/debugging)
    #[cfg(test)]
    pub async fn len(&self) -> usize {
        let guard = self.inner.read().await;
        guard
            .by_topic
            .values()
            .flat_map(HashMap::values)
            .map(HashMap::len)
            .sum()
    }

    /// Check if registry is empty (for testing/debugging)
    #[cfg(test)]
    pub async fn is_empty(&self) -> bool {
        let guard = self.inner.read().await;
        guard.by_topic.is_empty() && guard.all.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::timeout;

    /// A long poll of many partitions of one long topic name (32,000 bytes, as long as a
    /// frame allows; each partition 16 bytes on the wire) holds the name once: 5,000 copies
    /// would be 160 MB.
    #[test]
    fn a_topic_name_is_held_once_per_topic() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let registry = PendingFetchRegistry::new();
        let notify = Arc::new(Wake::new());
        let name = "n".repeat(32_000);
        let (id, peak) = crate::testing::heap_peak(|| {
            runtime.block_on(registry.register(
                [(name.as_str(), (0..5_000).map(|p| (p, 0)))],
                &notify,
                false,
            ))
        });
        assert!(peak < 2 << 20, "registering held {peak} bytes");
        assert_eq!(runtime.block_on(registry.len()), 5_000);
        runtime.block_on(registry.unregister(id, [(name.as_str(), 0..5_000)]));
        assert!(runtime.block_on(registry.is_empty()));
    }

    /// A Fetch that names one partition 100,000 times (16 bytes each on the wire, so 1.6 MB,
    /// within what the large-decode path takes) holds it once, at its last entry's offset.
    #[tokio::test]
    async fn a_repeated_partition_is_registered_once_with_its_last_offset() {
        const K: i64 = 100_000;
        let registry = PendingFetchRegistry::new();
        let notify = Arc::new(Wake::new());
        let id = registry
            .register([("t", (0..K).map(|offset| (0, offset)))], &notify, false)
            .await;
        assert_eq!(registry.len().await, 1);
        // The last entry's offset is K - 1: a high watermark of K - 1 is not new data.
        registry.notify_new_data("t", 0, K - 1).await;
        let woke = timeout(Duration::from_millis(50), notify.notified()).await;
        assert!(
            woke.is_err(),
            "woken for data before its last entry's offset"
        );
        registry.notify_new_data("t", 0, K).await;
        let woke = timeout(Duration::from_millis(100), notify.notified()).await;
        assert!(
            woke.is_ok(),
            "not woken for data past its last entry's offset"
        );
        registry.unregister(id, [("t", (0..K).map(|_| 0))]).await;
        assert!(registry.is_empty().await);
    }

    /// Unregistering costs one step per entry the long poll names, whatever else waits on the
    /// same partition: with 30,000 other long polls on partition 0 (one of them naming it
    /// 30,000 times), a long poll naming it 30,000 times unregisters in milliseconds. A
    /// per-partition list scanned once per entry costs 30,000 x 60,000 steps, about a second.
    #[tokio::test]
    async fn unregistering_does_not_scan_other_waiters() {
        const K: usize = 30_000;
        let registry = PendingFetchRegistry::new();
        let others: Vec<Arc<Wake>> = (0..K).map(|_| Arc::new(Wake::new())).collect();
        registry
            .register([("t", (0..K).map(|_| (0, 0)))], &others[0], false)
            .await;
        for notify in &others[1..] {
            registry.register([("t", [(0, 0)])], notify, false).await;
        }
        let notify = Arc::new(Wake::new());
        let id = registry
            .register([("t", (0..K).map(|_| (0, 0)))], &notify, true)
            .await;
        assert_eq!(registry.len().await, K + 1);
        let started = Instant::now();
        registry.unregister(id, [("t", (0..K).map(|_| 0))]).await;
        let took = started.elapsed();
        assert!(
            took < Duration::from_millis(100),
            "unregistering took {took:?}"
        );
        assert_eq!(registry.len().await, K);
    }

    #[tokio::test]
    async fn test_register_and_notify() {
        let registry = PendingFetchRegistry::new();

        // Register a pending fetch
        let notify = Arc::new(Wake::new());
        registry
            .register([("topic-a", [(0, 0)])], &notify, false)
            .await;

        assert_eq!(registry.len().await, 1);

        // Notify with high watermark > fetch_offset should wake
        registry.notify_new_data("topic-a", 0, 10).await;

        // The notify should be triggered
        let result = timeout(Duration::from_millis(100), notify.notified()).await;
        assert!(result.is_ok(), "Should have been notified");
    }

    #[tokio::test]
    async fn test_notify_wrong_partition() {
        let registry = PendingFetchRegistry::new();

        // Register for partition 0
        let notify = Arc::new(Wake::new());
        registry
            .register([("topic-a", [(0, 0)])], &notify, false)
            .await;

        // Notify partition 1 - should NOT wake partition 0
        registry.notify_new_data("topic-a", 1, 10).await;

        // The notify should NOT be triggered
        let result = timeout(Duration::from_millis(50), notify.notified()).await;
        assert!(result.is_err(), "Should NOT have been notified");
    }

    #[tokio::test]
    async fn test_notify_wrong_topic() {
        let registry = PendingFetchRegistry::new();

        // The registry is keyed by topic name, so a produce to a different topic must not
        // wake a waiter on this one.
        let notify = Arc::new(Wake::new());
        registry
            .register([("topic-a", [(0, 0)])], &notify, false)
            .await;

        registry.notify_new_data("topic-b", 0, 10).await;

        let result = timeout(Duration::from_millis(50), notify.notified()).await;
        assert!(
            result.is_err(),
            "Should NOT have been notified for another topic"
        );
    }

    #[tokio::test]
    async fn test_notify_only_relevant_offsets() {
        let registry = PendingFetchRegistry::new();

        // Register waiting from offset 100
        let notify = Arc::new(Wake::new());
        registry
            .register([("topic-a", [(0, 100)])], &notify, false)
            .await;

        // Notify with high watermark 50 - should NOT wake (already past that)
        registry.notify_new_data("topic-a", 0, 50).await;

        let result = timeout(Duration::from_millis(50), notify.notified()).await;
        assert!(
            result.is_err(),
            "Should NOT have been notified for old offset"
        );

        // Notify with high watermark 150 - SHOULD wake
        registry.notify_new_data("topic-a", 0, 150).await;

        let result = timeout(Duration::from_millis(100), notify.notified()).await;
        assert!(result.is_ok(), "Should have been notified for new offset");
    }

    #[tokio::test]
    async fn test_unregister() {
        let registry = PendingFetchRegistry::new();

        let notify = Arc::new(Wake::new());
        let id = registry
            .register([("topic-a", [(0, 0)])], &notify, false)
            .await;
        assert_eq!(registry.len().await, 1);

        registry.unregister(id, [("topic-a", [0])]).await;
        assert!(registry.is_empty().await);
    }

    #[tokio::test]
    async fn test_multiple_waiters_same_partition() {
        let registry = PendingFetchRegistry::new();

        // Register multiple waiters for same partition (each caller has its own Notify)
        let notify1 = Arc::new(Wake::new());
        let notify2 = Arc::new(Wake::new());
        let notify3 = Arc::new(Wake::new());
        registry
            .register([("topic-a", [(0, 0)])], &notify1, false)
            .await;
        registry
            .register([("topic-a", [(0, 5)])], &notify2, false)
            .await;
        registry
            .register([("topic-a", [(0, 10)])], &notify3, false)
            .await;

        assert_eq!(registry.len().await, 3);

        // Notify with high watermark 8 - should wake notify1 and notify2, not notify3
        registry.notify_new_data("topic-a", 0, 8).await;

        let result1 = timeout(Duration::from_millis(100), notify1.notified()).await;
        let result2 = timeout(Duration::from_millis(100), notify2.notified()).await;
        let result3 = timeout(Duration::from_millis(50), notify3.notified()).await;

        assert!(result1.is_ok(), "notify1 should have been notified");
        assert!(result2.is_ok(), "notify2 should have been notified");
        assert!(result3.is_err(), "notify3 should NOT have been notified");
    }

    #[tokio::test]
    async fn test_transaction_ended_wakes_only_read_committed_waiters() {
        let registry = PendingFetchRegistry::new();

        let committed = Arc::new(Wake::new());
        let uncommitted = Arc::new(Wake::new());
        registry
            .register([("topic-a", [(0, 0)])], &committed, true)
            .await;
        registry
            .register([("topic-a", [(0, 7)])], &uncommitted, false)
            .await;

        registry.notify_transaction_ended("topic-a", &[0]).await;

        let woke = timeout(Duration::from_millis(100), committed.notified()).await;
        assert!(woke.is_ok(), "read_committed waiter must wake on EndTxn");
        let woke = timeout(Duration::from_millis(50), uncommitted.notified()).await;
        assert!(
            woke.is_err(),
            "read_uncommitted waiter must not wake on EndTxn"
        );
    }

    #[tokio::test]
    async fn test_notification_before_await_is_not_lost() {
        // The long poll registers before its fetch attempt; a notification that lands while
        // the attempt is in flight (nobody awaiting yet) must still wake the next await.
        let registry = PendingFetchRegistry::new();
        let notify = Arc::new(Wake::new());
        registry
            .register([("topic-a", [(0, 0)])], &notify, false)
            .await;

        registry.notify_new_data("topic-a", 0, 1).await; // no one awaiting yet

        let woke = timeout(Duration::from_millis(100), notify.notified()).await;
        assert!(woke.is_ok(), "a stored wake-up permit must not be lost");
    }

    #[tokio::test]
    async fn test_concurrent_register_notify() {
        let registry = Arc::new(PendingFetchRegistry::new());

        // Spawn multiple tasks that register and wait
        let mut handles = vec![];
        for i in 0..10 {
            let reg = registry.clone();
            handles.push(tokio::spawn(async move {
                let notify = Arc::new(Wake::new());
                reg.register([("topic-a", [(0, i64::from(i))])], &notify, false)
                    .await;
                timeout(Duration::from_millis(500), notify.notified()).await
            }));
        }

        // Give tasks time to register
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Notify all
        registry.notify_new_data("topic-a", 0, 100).await;

        // All should complete
        for handle in handles {
            let result = handle.await.unwrap();
            assert!(result.is_ok(), "All waiters should have been notified");
        }
    }

    /// A notification marks the slot of the partition it names, once however often it
    /// repeats before the long poll takes the marks; a deleted topic marks every partition
    /// of it the long poll watches; a log-start move marks the partition whatever the
    /// offset; a transaction end marks the slots of its partitions only; a coarse wake
    /// marks none.
    #[tokio::test]
    async fn notifications_mark_the_slots_they_name() {
        let registry = PendingFetchRegistry::new();
        let wake = Arc::new(Wake::new());
        registry
            .register(
                [("a", vec![(0, 5), (1, 5)]), ("b", vec![(0, 5), (7, 5)])],
                &wake,
                true,
            )
            .await;
        for _ in 0..1_000 {
            registry.notify_new_data("b", 7, 6).await;
        }
        registry.notify_new_data("a", 0, 5).await; // not past its offset
        assert_eq!(
            wake.take(),
            Taken {
                slots: vec![3],
                ..Taken::default()
            }
        );
        assert_eq!(wake.take(), Taken::default());
        registry.notify_partition_changed("a", 0).await;
        registry.notify_topic_changed("b").await;
        let mut taken = wake.take();
        taken.slots.sort_unstable();
        assert_eq!(taken.slots, vec![0, 2, 3]);
        registry.notify_transaction_ended("a", &[1, 9]).await;
        registry.notify_transaction_ended("c", &[0]).await;
        assert_eq!(
            wake.take(),
            Taken {
                slots: vec![1],
                ..Taken::default()
            }
        );
        registry.notify_coarse().await;
        assert_eq!(
            wake.take(),
            Taken {
                coarse: true,
                ..Taken::default()
            }
        );
    }

    /// The safety net gives at most its allowance of partitions per interval, to all long
    /// polls together, and never more than the allowance to one.
    #[test]
    fn the_safety_net_allowance_is_shared_and_refilled_per_interval() {
        let registry = PendingFetchRegistry::new();
        let interval = Duration::from_millis(50);
        let allowance = usize::try_from(SAFETY_NET_PARTITIONS_PER_INTERVAL).unwrap();
        assert!(!registry.take_safety_net(allowance + 1, interval));
        assert!(registry.take_safety_net(allowance - 1, interval));
        assert!(registry.take_safety_net(1, interval));
        assert!(!registry.take_safety_net(1, interval));
        std::thread::sleep(interval);
        assert!(registry.take_safety_net(allowance, interval));
    }

    /// A transaction end costs one step per long poll waiting on its partitions and marks
    /// only those partitions' slots: the end of a one-partition transaction, against 200
    /// read_committed long polls of 1,000 other partitions each (and of that one), marks one
    /// slot of each, 200 in all. Marking every slot of every read_committed long poll marks
    /// 200,200, and the long polls then re-read every partition.
    #[tokio::test]
    async fn a_transaction_end_marks_only_its_partitions() {
        const POLLS: usize = 200;
        const PARTITIONS: i32 = 1_000;
        let registry = PendingFetchRegistry::new();
        let mut wakes = Vec::new();
        for _ in 0..POLLS {
            let wake = Arc::new(Wake::new());
            registry
                .register(
                    [
                        (
                            "watched",
                            (0..PARTITIONS).map(|p| (p, 0)).collect::<Vec<_>>(),
                        ),
                        ("txn", vec![(0, 0)]),
                    ],
                    &wake,
                    true,
                )
                .await;
            wakes.push(wake);
        }
        for _ in 0..100 {
            registry.notify_transaction_ended("txn", &[0]).await;
        }
        let marked: usize = wakes.iter().map(|wake| wake.take().slots.len()).sum();
        assert_eq!(marked, POLLS);
        for wake in &wakes {
            assert_eq!(wake.take(), Taken::default());
        }
    }
}
