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
    /// If the topic exists, returns its ID and partition count.
    /// If it doesn't exist, creates it with specified partition count and returns the new ID.
    ///
    /// # Arguments
    /// * `name` - Topic name
    /// * `default_partitions` - Number of partitions to create if topic doesn't exist
    ///
    /// # Returns
    /// Tuple of (topic_id, partition_count) on success, error otherwise
    fn get_or_create_topic(&self, name: &str, default_partitions: i32) -> Result<(i32, i32)>;

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

    // ===== Admin Topic Operations (Phase 6) =====

    /// Check if a topic exists
    ///
    /// # Arguments
    /// * `name` - Topic name
    ///
    /// # Returns
    /// true if topic exists, false otherwise
    fn topic_exists(&self, name: &str) -> Result<bool>;

    /// Create a topic with specified partition count
    ///
    /// # Arguments
    /// * `name` - Topic name
    /// * `partition_count` - Number of partitions
    ///
    /// # Returns
    /// Topic ID on success
    fn create_topic(&self, name: &str, partition_count: i32) -> Result<i32>;

    /// Get topic ID by name
    ///
    /// # Arguments
    /// * `name` - Topic name
    ///
    /// # Returns
    /// Topic ID if exists, None otherwise
    fn get_topic_id(&self, name: &str) -> Result<Option<i32>>;

    /// Delete a topic and all its messages
    ///
    /// # Arguments
    /// * `topic_id` - Topic ID
    ///
    /// # Returns
    /// Ok on success
    fn delete_topic(&self, topic_id: i32) -> Result<()>;

    /// Get the partition count for a topic
    ///
    /// # Arguments
    /// * `name` - Topic name
    ///
    /// # Returns
    /// Partition count if topic exists, None otherwise
    fn get_topic_partition_count(&self, name: &str) -> Result<Option<i32>>;

    /// Set the partition count for a topic
    ///
    /// # Arguments
    /// * `name` - Topic name
    /// * `partition_count` - New partition count
    ///
    /// # Returns
    /// Ok on success
    fn set_topic_partition_count(&self, name: &str, partition_count: i32) -> Result<()>;

    // ===== Admin Consumer Group Operations (Phase 6) =====

    /// Delete all committed offsets for a consumer group
    ///
    /// # Arguments
    /// * `group_id` - Consumer group ID
    ///
    /// # Returns
    /// Ok on success
    fn delete_consumer_group_offsets(&self, group_id: &str) -> Result<()>;

    // ===== Idempotent Producer Operations (Phase 9) =====

    /// Allocate a new producer ID with epoch 0
    ///
    /// Creates a new entry in the producer_ids table and returns the allocated
    /// producer ID with initial epoch 0.
    ///
    /// # Arguments
    /// * `client_id` - Optional client identifier for debugging
    /// * `transactional_id` - Optional transactional ID (for Phase 10)
    ///
    /// # Returns
    /// Tuple of (producer_id, epoch) where epoch is always 0 for new producers
    fn allocate_producer_id(
        &self,
        client_id: Option<&str>,
        transactional_id: Option<&str>,
    ) -> Result<(i64, i16)>;

    /// Get the current epoch for a producer ID
    ///
    /// # Arguments
    /// * `producer_id` - The producer ID to look up
    ///
    /// # Returns
    /// The current epoch if producer exists, None otherwise
    fn get_producer_epoch(&self, producer_id: i64) -> Result<Option<i16>>;

    /// Increment the producer epoch (for reconnection/fencing)
    ///
    /// Atomically increments the epoch for the given producer ID.
    /// Used when a producer reconnects and needs a new epoch.
    ///
    /// # Arguments
    /// * `producer_id` - The producer ID to update
    ///
    /// # Returns
    /// The new epoch value, or error if producer not found
    fn increment_producer_epoch(&self, producer_id: i64) -> Result<i16>;

    /// Check and update sequence number for idempotent producer
    ///
    /// Validates that the sequence number is exactly `last_sequence + 1` for
    /// the given (producer_id, topic_id, partition_id) tuple. If valid, updates
    /// the last_sequence to `base_sequence + record_count - 1`.
    ///
    /// # Arguments
    /// * `producer_id` - The producer ID
    /// * `producer_epoch` - The producer epoch (for fencing check)
    /// * `topic_id` - The topic ID
    /// * `partition_id` - The partition ID
    /// * `base_sequence` - The first sequence number in the batch
    /// * `record_count` - Number of records in the batch
    ///
    /// # Returns
    /// * `Ok(())` if sequence is valid and was recorded
    /// * `Err(DuplicateSequence)` if sequence already seen
    /// * `Err(OutOfOrderSequence)` if there's a gap in the sequence
    /// * `Err(ProducerFenced)` if the epoch is stale
    fn check_and_update_sequence(
        &self,
        producer_id: i64,
        producer_epoch: i16,
        topic_id: i32,
        partition_id: i32,
        base_sequence: i32,
        record_count: i32,
    ) -> Result<()>;
}

// Submodules
pub mod postgres;

#[cfg(test)]
mod tests;

// Re-export main types
pub use postgres::PostgresStore;
