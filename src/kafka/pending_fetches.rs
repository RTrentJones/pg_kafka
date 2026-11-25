//! Registry of the Fetch long polls waiting for data (network thread only).
//!
//! It maps a topic-partition to the long polls waiting on it; a notification from the
//! DB thread wakes them through their `tokio::sync::Notify`, and each decides whether to
//! re-fetch. It uses only tokio primitives and never calls pgrx.
//!
//! Every operation runs under the registry's lock on a network I/O thread, so its cost is
//! bounded by what the caller sent, never by what other long polls registered: a long poll's
//! entries are keyed by its [`FetchId`], so registering or unregistering one costs one hash
//! map step per entry it names, whatever else waits on the same partitions, and a partition
//! a Fetch names twice is held once (Kafka's `DelayedFetch` watches each partition once).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{Notify, RwLock};

/// Identifies one long poll's registration (see [`PendingFetchRegistry::register`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FetchId(u64);

/// The long polls waiting on one topic: partition -> long poll -> its entry.
///
/// Keyed by topic **name** (not the internal topic ID) because a Fetch on the network thread
/// only has the client-supplied topic name; resolving it to an ID would take an SPI round-trip
/// on the DB thread. The produce path already has the name, so it travels with the
/// notification (`InternalNotification::NewMessages.topic_name`). The name is held once per
/// topic, however many long polls wait on it: a partition entry takes 16 bytes on the wire
/// and a name up to the whole frame.
type TopicWaiters = HashMap<i32, HashMap<FetchId, PendingFetch>>;

/// One long poll waiting on one topic-partition.
#[derive(Debug)]
struct PendingFetch {
    /// The handle that wakes the long poll (shared by all its partitions).
    notify: Arc<Notify>,
    /// The offset the long poll fetches from: new data is relevant when the high watermark
    /// passes it.
    fetch_offset: i64,
}

#[derive(Debug, Default)]
struct Waiters {
    /// Topic name -> partition -> the long polls waiting on it.
    by_topic: HashMap<String, TopicWaiters>,
    /// The `read_committed` long polls, each once, woken when a transaction ends.
    read_committed: HashMap<FetchId, Arc<Notify>>,
}

/// Registry of pending fetch requests waiting for data
///
/// This is the central coordination point for long polling. Fetch handlers
/// register themselves when waiting, and the notification receiver wakes
/// them when new data arrives.
pub struct PendingFetchRegistry {
    inner: RwLock<Waiters>,
    next_id: AtomicU64,
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
        }
    }

    /// Register one long poll on `topics`, each a topic name and its `(partition id, offset
    /// fetched from)` entries, under one `notify` handle the long poll awaits; returns the id
    /// [`unregister`](Self::unregister) takes.
    ///
    /// A partition named more than once is registered once, with its last entry's offset (the
    /// offset the Fetch handler reads it from). Cost: one hash of each topic name entry and
    /// one hash map insert per partition entry, O(bytes of `topics`) time; memory O(distinct
    /// partitions), plus each distinct topic name once if no other long poll holds it.
    ///
    /// The caller should register every requested partition BEFORE its first fetch attempt and
    /// keep the registration until the long poll ends, then `unregister`. `Notify::notify_one`
    /// stores a permit when nobody is awaiting, so a notification that arrives while a fetch
    /// attempt is in flight is not lost: the next `notified().await` returns at once.
    pub async fn register<'a, P>(
        &self,
        topics: impl IntoIterator<Item = (&'a str, P)>,
        notify: &Arc<Notify>,
        read_committed: bool,
    ) -> FetchId
    where
        P: IntoIterator<Item = (i32, i64)>,
    {
        let id = FetchId(self.next_id.fetch_add(1, Ordering::Relaxed));
        let mut guard = self.inner.write().await;
        let waiters = &mut *guard;
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
                        notify: Arc::clone(notify),
                        fetch_offset,
                    },
                );
            }
        }
        if read_committed {
            waiters.read_committed.insert(id, Arc::clone(notify));
        }
        id
    }

    /// Notify all pending fetches for a topic-partition that new data is available
    ///
    /// This is called when a ProduceRequest completes. All fetches waiting on
    /// this topic-partition with fetch_offset < high_watermark will be woken.
    /// Cost: one step per long poll waiting on the partition (each holds it once).
    ///
    /// # Arguments
    /// * `topic_name` - The topic name
    /// * `partition_id` - The partition ID
    /// * `high_watermark` - The new high watermark after the produce
    pub async fn notify_new_data(&self, topic_name: &str, partition_id: i32, high_watermark: i64) {
        let guard = self.inner.read().await;
        let waiting = guard
            .by_topic
            .get(topic_name)
            .and_then(|waiters| waiters.get(&partition_id));
        for pending in waiting.into_iter().flat_map(HashMap::values) {
            // Only notify if the new data is at or after their fetch offset
            // This avoids waking fetches that have already caught up
            if high_watermark > pending.fetch_offset {
                pending.notify.notify_one();
            }
        }
    }

    /// Wake every waiting `read_committed` fetch because a transaction ended.
    ///
    /// EndTxn (commit or abort) can advance the last stable offset of any partition the
    /// transaction wrote to; the notification does not say which, so every `read_committed`
    /// waiter is woken once and re-fetches. `read_uncommitted` waiters are left alone.
    /// Cost: one step per `read_committed` long poll, however many partitions each waits on.
    pub async fn notify_transaction_ended(&self) {
        let guard = self.inner.read().await;
        for notify in guard.read_committed.values() {
            notify.notify_one();
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
        waiters.read_committed.remove(&id);
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
        guard.by_topic.is_empty() && guard.read_committed.is_empty()
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
        let notify = Arc::new(Notify::new());
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
        let notify = Arc::new(Notify::new());
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
        let others: Vec<Arc<Notify>> = (0..K).map(|_| Arc::new(Notify::new())).collect();
        registry
            .register([("t", (0..K).map(|_| (0, 0)))], &others[0], false)
            .await;
        for notify in &others[1..] {
            registry.register([("t", [(0, 0)])], notify, false).await;
        }
        let notify = Arc::new(Notify::new());
        let id = registry
            .register([("t", (0..K).map(|_| (0, 0)))], &notify, true)
            .await;
        assert_eq!(registry.len().await, K + 1);
        let started = std::time::Instant::now();
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
        let notify = Arc::new(Notify::new());
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
        let notify = Arc::new(Notify::new());
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
        let notify = Arc::new(Notify::new());
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
        let notify = Arc::new(Notify::new());
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

        let notify = Arc::new(Notify::new());
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
        let notify1 = Arc::new(Notify::new());
        let notify2 = Arc::new(Notify::new());
        let notify3 = Arc::new(Notify::new());
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

        let committed = Arc::new(Notify::new());
        let uncommitted = Arc::new(Notify::new());
        registry
            .register([("topic-a", [(0, 0)])], &committed, true)
            .await;
        registry
            .register([("topic-b", [(3, 7)])], &uncommitted, false)
            .await;

        registry.notify_transaction_ended().await;

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
        let notify = Arc::new(Notify::new());
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
                let notify = Arc::new(Notify::new());
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
}
