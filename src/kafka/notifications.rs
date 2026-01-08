// Internal notifications for long polling support
//
// This module defines notification types used to signal between the database
// thread (SPI) and the network thread (tokio) for implementing Kafka-style
// long polling on FetchRequest.
//
// ## Architecture
//
// When messages are produced via the Kafka API:
// 1. Database thread inserts records via SPI
// 2. Database thread sends InternalNotification::NewMessages via crossbeam channel
// 3. Network thread receives notification and wakes any waiting FetchRequest handlers
//
// This enables sub-10ms latency for the produce→consume path.

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
        /// The topic ID (from kafka.topics table)
        topic_id: i32,
        /// The partition ID
        partition_id: i32,
        /// The new high watermark (offset of next message to be written)
        /// Used to determine which waiting fetches should be woken
        high_watermark: i64,
    },
}
