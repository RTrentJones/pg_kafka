//! Notifications from the DB thread to the network thread that drive Fetch long polling.
//!
//! When a Produce request stores records, the DB thread sends
//! [`InternalNotification::NewMessages`] over a crossbeam channel; the network thread
//! wakes the long polls waiting on that topic-partition (see
//! `listener::long_poll`). A waiting fetch is re-run ONLY on such a notification, on its
//! `max_wait_ms` deadline, or on the slow safety-net poll
//! (`pg_kafka.fetch_poll_interval_ms`), so idle consumers cost almost no DB work.
//!
//! Notifications are sent from inside the request's transaction, i.e. before its
//! commit. That is safe because the woken fetch is re-submitted to the same single DB
//! thread, which processes requests in order: the re-fetch always runs after the
//! notifying request (and sees its writes).

/// Internal notification from database thread to network thread
///
/// These notifications are NOT part of the Kafka protocol - they are used
/// internally to implement long polling efficiently.
#[derive(Debug, Clone)]
pub enum InternalNotification {
    /// New messages have been written to a topic-partition
    ///
    /// Sent after a successful ProduceRequest to wake any consumers
    /// waiting on FetchRequest for this topic-partition.
    NewMessages {
        /// The topic name (as the client sees it). The long-poll registry is keyed by topic
        /// name because a Fetch on the network thread only has the topic name, not the
        /// internal topic ID.
        topic_name: String,
        /// The topic ID (from kafka.topics table)
        topic_id: i32,
        /// The partition ID
        partition_id: i32,
        /// The new high watermark (offset of next message to be written)
        /// Used to determine which waiting fetches should be woken
        high_watermark: i64,
    },

    /// A transaction was committed or aborted (EndTxn).
    ///
    /// Either outcome can advance a partition's last stable offset, making records
    /// visible to `read_committed` consumers without any new produce. The DB thread
    /// does not resolve which partitions the transaction touched, so this wakes every
    /// waiting `read_committed` fetch (each re-fetches once); `read_uncommitted`
    /// waiters already saw the records when they were produced and are not woken.
    TransactionEnded,
}
