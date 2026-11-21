//! Registry of the Fetch long polls waiting for data (network thread only).
//!
//! It maps a topic-partition to the long polls waiting on it; a notification from the
//! DB thread wakes them through their `tokio::sync::Notify`, and each decides whether to
//! re-fetch. It uses only tokio primitives and never calls pgrx.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Notify, RwLock};

/// The long polls waiting on one topic, by partition.
///
/// Keyed by topic **name** (not the internal topic ID) because a Fetch on the network thread
/// only has the client-supplied topic name; resolving it to an ID would take an SPI round-trip
/// on the DB thread. The produce path already has the name, so it travels with the
/// notification (`InternalNotification::NewMessages.topic_name`). The name is held and
/// hashed once per topic a Fetch names, not once per partition: a partition entry takes 16
/// bytes on the wire and a name up to the whole frame.
type TopicWaiters = HashMap<i32, Vec<PendingFetch>>;

/// A pending fetch request waiting for data
///
/// Each PendingFetch represents one FetchRequest handler waiting on one
/// topic-partition. A single FetchRequest may have multiple PendingFetch
/// entries if it requests multiple topic-partitions.
#[derive(Debug)]
pub struct PendingFetch {
    /// Notify handle to wake this fetch when data arrives
    pub notify: Arc<Notify>,
    /// The offset this fetch is waiting from
    /// Used to determine if new data is relevant (high_watermark > fetch_offset)
    pub fetch_offset: i64,
    /// Whether the fetch uses `read_committed` isolation; such fetches are also
    /// woken when a transaction ends (see [`PendingFetchRegistry::notify_transaction_ended`]).
    pub read_committed: bool,
}

/// Registry of pending fetch requests waiting for data
///
/// This is the central coordination point for long polling. Fetch handlers
/// register themselves when waiting, and the notification receiver wakes
/// them when new data arrives.
pub struct PendingFetchRegistry {
    /// Topic name -> partition -> the pending fetches waiting on it
    inner: RwLock<HashMap<String, TopicWaiters>>,
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
            inner: RwLock::new(HashMap::new()),
        }
    }

    /// Register a pending fetch for partitions of one topic, each `(partition id, offset
    /// fetched from)`.
    ///
    /// The caller supplies the `Notify` so that a single FetchRequest spanning several
    /// topic-partitions can register one shared handle under each key and `await` it once.
    ///
    /// The caller should register every requested partition BEFORE its first fetch attempt and
    /// keep the registration until the long poll ends, then `unregister`. `Notify::notify_one`
    /// stores a permit when nobody is awaiting, so a notification that arrives while a fetch
    /// attempt is in flight is not lost: the next `notified().await` returns at once.
    ///
    /// # Arguments
    /// * `topic_name` - The topic name (as the client sees it)
    /// * `partitions` - The partition IDs and the offsets fetched from (for relevance
    ///   filtering)
    /// * `notify` - The shared handle to wake when data arrives for these partitions
    /// * `read_committed` - Whether the fetch uses `read_committed` isolation
    pub async fn register(
        &self,
        topic_name: &str,
        partitions: impl IntoIterator<Item = (i32, i64)>,
        notify: &Arc<Notify>,
        read_committed: bool,
    ) {
        let mut guard = self.inner.write().await;
        let waiters = guard.entry(topic_name.to_string()).or_default();
        for (partition_id, fetch_offset) in partitions {
            waiters.entry(partition_id).or_default().push(PendingFetch {
                notify: Arc::clone(notify),
                fetch_offset,
                read_committed,
            });
        }
    }

    /// Notify all pending fetches for a topic-partition that new data is available
    ///
    /// This is called when a ProduceRequest completes. All fetches waiting on
    /// this topic-partition with fetch_offset < high_watermark will be woken.
    ///
    /// # Arguments
    /// * `topic_name` - The topic name
    /// * `partition_id` - The partition ID
    /// * `high_watermark` - The new high watermark after the produce
    pub async fn notify_new_data(&self, topic_name: &str, partition_id: i32, high_watermark: i64) {
        let guard = self.inner.read().await;
        let waiting = guard
            .get(topic_name)
            .and_then(|waiters| waiters.get(&partition_id));
        for pending in waiting.into_iter().flatten() {
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
    pub async fn notify_transaction_ended(&self) {
        let guard = self.inner.read().await;
        for pending in guard.values().flat_map(HashMap::values).flatten() {
            if pending.read_committed {
                pending.notify.notify_one();
            }
        }
    }

    /// Unregister a pending fetch from partitions of one topic
    ///
    /// Called when a fetch completes (either with data or timeout) to clean up
    /// the registry entry.
    ///
    /// # Arguments
    /// * `topic_name` - The topic name
    /// * `partitions` - The partition IDs
    /// * `notify` - The shared Notify handle that was registered
    pub async fn unregister(
        &self,
        topic_name: &str,
        partitions: impl IntoIterator<Item = i32>,
        notify: &Arc<Notify>,
    ) {
        let mut guard = self.inner.write().await;
        let Some(waiters) = guard.get_mut(topic_name) else {
            return;
        };
        for partition_id in partitions {
            if let Some(pending_list) = waiters.get_mut(&partition_id) {
                // Remove the entry with matching notify handle
                pending_list.retain(|p| !Arc::ptr_eq(&p.notify, notify));
                // Clean up empty entries to avoid memory leaks
                if pending_list.is_empty() {
                    waiters.remove(&partition_id);
                }
            }
        }
        if waiters.is_empty() {
            guard.remove(topic_name);
        }
    }

    /// Get the number of pending fetches (for testing/debugging)
    #[cfg(test)]
    pub async fn len(&self) -> usize {
        let guard = self.inner.read().await;
        guard.values().flat_map(HashMap::values).map(Vec::len).sum()
    }

    /// Check if registry is empty (for testing/debugging)
    #[cfg(test)]
    pub async fn is_empty(&self) -> bool {
        let guard = self.inner.read().await;
        guard.is_empty()
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
        let ((), peak) = crate::testing::heap_peak(|| {
            runtime.block_on(registry.register(&name, (0..5_000).map(|p| (p, 0)), &notify, false));
        });
        assert!(peak < 2 << 20, "registering held {peak} bytes");
        assert_eq!(runtime.block_on(registry.len()), 5_000);
        runtime.block_on(registry.unregister(&name, 0..5_000, &notify));
        assert!(runtime.block_on(registry.is_empty()));
    }

    #[tokio::test]
    async fn test_register_and_notify() {
        let registry = PendingFetchRegistry::new();

        // Register a pending fetch
        let notify = Arc::new(Notify::new());
        registry.register("topic-a", [(0, 0)], &notify, false).await;

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
        registry.register("topic-a", [(0, 0)], &notify, false).await;

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
        registry.register("topic-a", [(0, 0)], &notify, false).await;

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
            .register("topic-a", [(0, 100)], &notify, false)
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
        registry.register("topic-a", [(0, 0)], &notify, false).await;
        assert_eq!(registry.len().await, 1);

        registry.unregister("topic-a", [0], &notify).await;
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
            .register("topic-a", [(0, 0)], &notify1, false)
            .await;
        registry
            .register("topic-a", [(0, 5)], &notify2, false)
            .await;
        registry
            .register("topic-a", [(0, 10)], &notify3, false)
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
            .register("topic-a", [(0, 0)], &committed, true)
            .await;
        registry
            .register("topic-b", [(3, 7)], &uncommitted, false)
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
        registry.register("topic-a", [(0, 0)], &notify, false).await;

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
                reg.register("topic-a", [(0, i64::from(i))], &notify, false)
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
