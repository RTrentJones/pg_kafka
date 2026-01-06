// Storage abstraction layer for pg_kafka
//
// This module defines the KafkaStore trait that abstracts data persistence operations.
// By separating storage concerns from protocol logic, we achieve:
// 1. Testability - handlers can be tested with mock stores
// 2. Clean separation of concerns - protocol logic doesn't know about SQL
// 3. Transaction safety - transaction boundaries remain explicit in worker.rs

use super::error::Result;
use super::messages::Record;

/// Metadata about a topic (storage layer representation)
///
/// This is the internal storage representation. Use this when working
/// with the database layer. The protocol layer uses kafka_protocol types.
#[derive(Debug, Clone)]
pub struct TopicMetadata {
    /// Topic name
    pub name: String,
    /// Topic ID in the database
    pub id: i32,
    /// Number of partitions
    pub partition_count: i32,
}

/// A fetched message from storage (internal representation)
///
/// This represents a message as stored in the database, before
/// conversion to Kafka RecordBatch format.
#[derive(Debug, Clone)]
pub struct FetchedMessage {
    /// Partition offset
    pub partition_offset: i64,
    /// Message key (nullable)
    pub key: Option<Vec<u8>>,
    /// Message value (nullable)
    pub value: Option<Vec<u8>>,
    /// Timestamp (milliseconds since epoch)
    pub timestamp: i64,
}

/// Committed offset information (internal representation)
#[derive(Debug, Clone)]
pub struct CommittedOffset {
    /// Committed offset value
    pub offset: i64,
    /// Optional metadata
    pub metadata: Option<String>,
}

/// Abstract storage interface for Kafka data operations
///
/// This trait defines the contract between the Kafka protocol handlers
/// and the underlying storage implementation. Implementors handle the
/// actual data persistence (SQL, file system, etc.) while handlers
/// focus on protocol logic.
///
/// All methods are assumed to run within a transaction context managed
/// by the caller (typically the background worker).
pub trait KafkaStore {
    // ===== Topic Operations =====

    /// Get or create a topic by name
    ///
    /// If the topic exists, returns its ID.
    /// If it doesn't exist, creates it with default partition count and returns the new ID.
    ///
    /// # Arguments
    /// * `name` - Topic name
    ///
    /// # Returns
    /// Topic ID on success, error otherwise
    fn get_or_create_topic(&self, name: &str) -> Result<i32>;

    /// Get metadata for topics
    ///
    /// # Arguments
    /// * `names` - Optional list of topic names. If None, returns all topics.
    ///
    /// # Returns
    /// Vector of topic metadata
    fn get_topic_metadata(&self, names: Option<&[String]>) -> Result<Vec<TopicMetadata>>;

    // ===== Message Operations =====

    /// Insert records into a topic partition
    ///
    /// # Arguments
    /// * `topic_id` - Topic ID
    /// * `partition_id` - Partition ID
    /// * `records` - Slice of records to insert
    ///
    /// # Returns
    /// Base partition offset assigned to the first record
    fn insert_records(&self, topic_id: i32, partition_id: i32, records: &[Record]) -> Result<i64>;

    /// Fetch messages from a topic partition
    ///
    /// # Arguments
    /// * `topic_id` - Topic ID
    /// * `partition_id` - Partition ID
    /// * `fetch_offset` - Offset to start fetching from
    /// * `max_bytes` - Maximum bytes to fetch (approximate limit)
    ///
    /// # Returns
    /// Vector of fetched messages
    fn fetch_records(
        &self,
        topic_id: i32,
        partition_id: i32,
        fetch_offset: i64,
        max_bytes: i32,
    ) -> Result<Vec<FetchedMessage>>;

    /// Get high watermark for a topic partition
    ///
    /// The high watermark is the offset of the next message to be written.
    ///
    /// # Arguments
    /// * `topic_id` - Topic ID
    /// * `partition_id` - Partition ID
    ///
    /// # Returns
    /// High watermark offset (0 if partition is empty)
    fn get_high_watermark(&self, topic_id: i32, partition_id: i32) -> Result<i64>;

    /// Get earliest offset for a topic partition
    ///
    /// The earliest offset is the offset of the first available message.
    ///
    /// # Arguments
    /// * `topic_id` - Topic ID
    /// * `partition_id` - Partition ID
    ///
    /// # Returns
    /// Earliest offset (0 if partition is empty)
    fn get_earliest_offset(&self, topic_id: i32, partition_id: i32) -> Result<i64>;

    // ===== Consumer Offset Operations =====

    /// Commit consumer offsets for a group
    ///
    /// # Arguments
    /// * `group_id` - Consumer group ID
    /// * `topic_id` - Topic ID
    /// * `partition_id` - Partition ID
    /// * `offset` - Offset to commit
    /// * `metadata` - Optional metadata
    ///
    /// # Returns
    /// Ok on success
    fn commit_offset(
        &self,
        group_id: &str,
        topic_id: i32,
        partition_id: i32,
        offset: i64,
        metadata: Option<&str>,
    ) -> Result<()>;

    /// Fetch committed offset for a consumer group partition
    ///
    /// # Arguments
    /// * `group_id` - Consumer group ID
    /// * `topic_id` - Topic ID
    /// * `partition_id` - Partition ID
    ///
    /// # Returns
    /// Committed offset info, or None if not committed
    fn fetch_offset(
        &self,
        group_id: &str,
        topic_id: i32,
        partition_id: i32,
    ) -> Result<Option<CommittedOffset>>;

    /// Fetch all committed offsets for a consumer group
    ///
    /// # Arguments
    /// * `group_id` - Consumer group ID
    ///
    /// # Returns
    /// Map of (topic_name, partition_id) -> CommittedOffset
    fn fetch_all_offsets(&self, group_id: &str) -> Result<Vec<(String, i32, CommittedOffset)>>;
}

// Submodules
pub mod postgres;

#[cfg(test)]
mod tests;

// Re-export main types
pub use postgres::PostgresStore;
