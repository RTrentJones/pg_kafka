//! Notifications from the DB thread to the network thread that drive Fetch long polling.
//!
//! Every event on the DB thread that can change what a waiting Fetch answers sends one:
//! Produce ([`InternalNotification::NewMessages`] per partition stored to), EndTxn, a
//! transaction timeout and an InitProducerId that aborts an open transaction
//! (`TransactionEnded`, naming the transaction's partitions), DeleteRecords
//! (`LogStartMoved` per partition), DeleteTopics (`TopicDeleted`) and a retention sweep
//! that removed records (`RecordsExpired`). The
//! network thread marks the partitions a notification names in each long poll waiting on
//! them and wakes it (see `pending_fetches` and `listener::long_poll`); a long poll re-reads
//! only on such a wake-up, on its `max_wait_ms` deadline, or on its rationed safety-net
//! re-check (`pg_kafka.fetch_poll_interval_ms`), so an idle long poll costs the DB thread
//! nothing between notifications.
//!
//! What a Fetch answers can also change outside the worker (an on-demand
//! `pg_kafka_run_retention_sweep()` in another session, DROP EXTENSION); a long poll sees
//! that at its deadline or safety-net re-check.
//!
//! Notifications are sent from inside the request's transaction, i.e. before its
//! commit. That is safe because the woken fetch is re-submitted to the same single DB
//! thread, which processes requests in order: the re-fetch always runs after the
//! notifying request (and sees its writes).
//!
//! The channel is bounded and the DB thread never waits for room: a notification that does
//! not fit is dropped and [`NOTIFICATIONS_DROPPED`] set, and the network thread then wakes
//! every long poll with a coarse wake-up (see [`take_dropped`]).

use crossbeam_channel::{Sender, TrySendError};
use std::sync::atomic::{AtomicBool, Ordering};

/// Set when a notification was dropped on a full channel; cleared by [`take_dropped`].
static NOTIFICATIONS_DROPPED: AtomicBool = AtomicBool::new(false);

/// Send `notification` without blocking the single DB thread; on a full channel it is dropped
/// and the drop recorded for the network thread ([`take_dropped`]).
pub fn send_best_effort(tx: &Sender<InternalNotification>, notification: InternalNotification) {
    if let Err(TrySendError::Full(_)) = tx.try_send(notification) {
        NOTIFICATIONS_DROPPED.store(true, Ordering::Release);
    }
}

/// Whether a notification was dropped since the last call (network thread). The network
/// thread checks after every notification it receives and at least every 100 ms, so a dropped
/// notification costs one coarse wake-up of every long poll, at most one per check.
pub fn take_dropped() -> bool {
    NOTIFICATIONS_DROPPED.swap(false, Ordering::AcqRel)
}

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

    /// A transaction ended: committed or aborted by EndTxn, aborted by a fencing
    /// InitProducerId or by its timeout.
    ///
    /// Either outcome can advance the last stable offset of the partitions the transaction
    /// added (AddPartitionsToTxn), making records visible to `read_committed` consumers
    /// without any new produce. It wakes the `read_committed` long polls waiting on those
    /// partitions, marking only them; `read_uncommitted` waiters already saw the records
    /// when they were produced and are not woken. Sent only when a transaction did end,
    /// and naming at least one partition.
    TransactionEnded {
        /// The transaction's partitions, by topic name (as the client sees it), each
        /// topic once.
        partitions: crate::kafka::storage::TxnPartitions,
    },

    /// DeleteRecords moved a topic-partition's log start: a long poll fetching it from below
    /// the new start is answered OFFSET_OUT_OF_RANGE without waiting for its deadline.
    LogStartMoved {
        /// The topic name (as the client sees it).
        topic_name: String,
        /// The partition ID.
        partition_id: i32,
    },

    /// A topic was deleted: its long polls are answered UNKNOWN_TOPIC_OR_PARTITION.
    TopicDeleted {
        /// The topic name (as the client sees it).
        topic_name: String,
    },

    /// A retention sweep removed records, from partitions it does not name: every long poll
    /// gets a coarse wake-up (re-checked within the safety net's allowance, else at its
    /// deadline). At most one per sweep.
    RecordsExpired,
}
