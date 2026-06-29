// Pending fetch registry for long polling support
//
// This module implements a registry for tracking FetchRequest handlers that are
// waiting for new data. When a ProduceRequest completes, the registry is notified
// and wakes up any handlers waiting for that topic-partition.
//
// ## Thread Safety
//
// This module runs entirely in the network thread (tokio runtime). It uses
// tokio::sync primitives for async-safe access. It does NOT call any pgrx
// functions and is safe to use from async code.
//
// ## Design
//
// The registry maps TopicPartitionKey -> Vec<PendingFetch>. When new data arrives
// for a topic-partition, all waiting fetches are notified via tokio::sync::Notify.
// The fetch handler then decides whether to re-fetch or continue waiting based on
// whether min_bytes threshold is met.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Notify, RwLock};

/// Key for identifying a topic-partition.
///
/// QA-5: keyed by topic **name** (not the internal topic ID) because the FetchRequest handler on
/// the network thread only has the client-supplied topic name; resolving it to an ID would require
/// an SPI round-trip on the DB thread. The produce path already has the name, so it travels with the
/// notification (`InternalNotification::NewMessages.topic_name`).
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct TopicPartitionKey {
    pub topic_name: String,
    pub partition_id: i32,
}

impl TopicPartitionKey {
    pub fn new(topic_name: &str, partition_id: i32) -> Self {
        Self {
            topic_name: topic_name.to_string(),
            partition_id,
        }
    }
}

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
}

/// Registry of pending fetch requests waiting for data
///
/// This is the central coordination point for long polling. Fetch handlers
/// register themselves when waiting, and the notification receiver wakes
/// them when new data arrives.
pub struct PendingFetchRegistry {
    /// Map of topic-partition -> list of pending fetches
    inner: RwLock<HashMap<TopicPartitionKey, Vec<PendingFetch>>>,
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

    /// Register a pending fetch for a topic-partition.
    ///
    /// The caller supplies the `Notify` so that a single FetchRequest spanning several
    /// topic-partitions can register one shared handle under each key and `await` it once. The
    /// caller should register every requested partition, then await the handle (with a poll-interval
    /// timeout) and re-fetch, then `unregister`.
    ///
    /// # Arguments
    /// * `topic_name` - The topic name (as the client sees it)
    /// * `partition_id` - The partition ID
    /// * `fetch_offset` - The offset being fetched from (for relevance filtering)
    /// * `notify` - The shared handle to wake when data arrives for this partition
    pub async fn register(
        &self,
        topic_name: &str,
        partition_id: i32,
        fetch_offset: i64,
        notify: Arc<Notify>,
    ) {
        let key = TopicPartitionKey::new(topic_name, partition_id);

        let pending = PendingFetch {
            notify,
            fetch_offset,
        };

        let mut guard = self.inner.write().await;
        guard.entry(key).or_default().push(pending);
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
        let key = TopicPartitionKey::new(topic_name, partition_id);

        let guard = self.inner.read().await;
        if let Some(pending_list) = guard.get(&key) {
            for pending in pending_list {
                // Only notify if the new data is at or after their fetch offset
                // This avoids waking fetches that have already caught up
                if high_watermark > pending.fetch_offset {
                    pending.notify.notify_one();
                }
            }
        }
    }

    /// Unregister a pending fetch
    ///
    /// Called when a fetch completes (either with data or timeout) to clean up
    /// the registry entry.
    ///
    /// # Arguments
    /// * `topic_name` - The topic name
    /// * `partition_id` - The partition ID
    /// * `notify` - The shared Notify handle that was registered
    pub async fn unregister(&self, topic_name: &str, partition_id: i32, notify: &Arc<Notify>) {
        let key = TopicPartitionKey::new(topic_name, partition_id);

        let mut guard = self.inner.write().await;
        if let Some(pending_list) = guard.get_mut(&key) {
            // Remove the entry with matching notify handle
            pending_list.retain(|p| !Arc::ptr_eq(&p.notify, notify));

            // Clean up empty entries to avoid memory leaks
            if pending_list.is_empty() {
                guard.remove(&key);
            }
        }
    }

    /// Get the number of pending fetches (for testing/debugging)
    #[cfg(test)]
    pub async fn len(&self) -> usize {
        let guard = self.inner.read().await;
        guard.values().map(|v| v.len()).sum()
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

    #[tokio::test]
    async fn test_register_and_notify() {
        let registry = PendingFetchRegistry::new();

        // Register a pending fetch
        let notify = Arc::new(Notify::new());
        registry.register("topic-a", 0, 0, notify.clone()).await;

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
        registry.register("topic-a", 0, 0, notify.clone()).await;

        // Notify partition 1 - should NOT wake partition 0
        registry.notify_new_data("topic-a", 1, 10).await;

        // The notify should NOT be triggered
        let result = timeout(Duration::from_millis(50), notify.notified()).await;
        assert!(result.is_err(), "Should NOT have been notified");
    }

    #[tokio::test]
    async fn test_notify_wrong_topic() {
        let registry = PendingFetchRegistry::new();

        // QA-5: the registry is keyed by topic name, so a produce to a different topic must not
        // wake a waiter on this one.
        let notify = Arc::new(Notify::new());
        registry.register("topic-a", 0, 0, notify.clone()).await;

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
        registry.register("topic-a", 0, 100, notify.clone()).await;

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
        registry.register("topic-a", 0, 0, notify.clone()).await;
        assert_eq!(registry.len().await, 1);

        registry.unregister("topic-a", 0, &notify).await;
        assert!(registry.is_empty().await);
    }

    #[tokio::test]
    async fn test_multiple_waiters_same_partition() {
        let registry = PendingFetchRegistry::new();

        // Register multiple waiters for same partition (each caller has its own Notify)
        let notify1 = Arc::new(Notify::new());
        let notify2 = Arc::new(Notify::new());
        let notify3 = Arc::new(Notify::new());
        registry.register("topic-a", 0, 0, notify1.clone()).await;
        registry.register("topic-a", 0, 5, notify2.clone()).await;
        registry.register("topic-a", 0, 10, notify3.clone()).await;

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
    async fn test_concurrent_register_notify() {
        let registry = Arc::new(PendingFetchRegistry::new());

        // Spawn multiple tasks that register and wait
        let mut handles = vec![];
        for i in 0..10 {
            let reg = registry.clone();
            handles.push(tokio::spawn(async move {
                let notify = Arc::new(Notify::new());
                reg.register("topic-a", 0, i as i64, notify.clone()).await;
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
