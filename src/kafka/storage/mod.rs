// Storage abstraction layer for pg_kafka
//
// This module defines the KafkaStore trait that abstracts data persistence operations.
// By separating storage concerns from protocol logic, we achieve:
// 1. Testability - handlers can be tested with mock stores
// 2. Clean separation of concerns - protocol logic doesn't know about SQL
// 3. Transaction safety - transaction boundaries remain explicit in worker.rs

use std::time::Duration;

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

/// Transaction state (Phase 10)
///
/// Represents the lifecycle state of a Kafka transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransactionState {
    /// No active transaction
    Empty,
    /// Transaction is active and accepting records
    Ongoing,
    /// Transaction is preparing to commit
    PrepareCommit,
    /// Transaction is preparing to abort
    PrepareAbort,
    /// Transaction has been committed
    CompleteCommit,
    /// Transaction has been aborted
    CompleteAbort,
}

impl TransactionState {
    /// Convert from database string representation
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "Empty" => Some(Self::Empty),
            "Ongoing" => Some(Self::Ongoing),
            "PrepareCommit" => Some(Self::PrepareCommit),
            "PrepareAbort" => Some(Self::PrepareAbort),
            "CompleteCommit" => Some(Self::CompleteCommit),
            "CompleteAbort" => Some(Self::CompleteAbort),
            _ => None,
        }
    }

    /// Convert to database string representation
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Empty => "Empty",
            Self::Ongoing => "Ongoing",
            Self::PrepareCommit => "PrepareCommit",
            Self::PrepareAbort => "PrepareAbort",
            Self::CompleteCommit => "CompleteCommit",
            Self::CompleteAbort => "CompleteAbort",
        }
    }
}

/// Consumer isolation level (Phase 10)
///
/// Controls visibility of transactional messages during fetch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IsolationLevel {
    /// See all records including uncommitted transactional messages
    #[default]
    ReadUncommitted = 0,
    /// Only see committed records (filters out pending and aborted)
    ReadCommitted = 1,
}

impl IsolationLevel {
    /// Create from Kafka protocol value
    pub fn from_i8(value: i8) -> Self {
        match value {
            1 => Self::ReadCommitted,
            _ => Self::ReadUncommitted,
        }
    }
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
    /// * `Ok(true)` if sequence is valid and should be inserted (new message)
    /// * `Ok(false)` if sequence is a duplicate (skip insert, return success to client)
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
    ) -> Result<bool>;

    // ===== Transaction Operations (Phase 10) =====

    /// Look up or create producer by transactional_id
    ///
    /// If transactional_id exists: bump epoch (fences old producer), return existing producer_id.
    /// If not found: allocate new producer_id, set transactional_id.
    /// Also creates/updates row in kafka.transactions table with state='Empty'.
    ///
    /// # Arguments
    /// * `transactional_id` - The transactional ID to look up or create
    /// * `transaction_timeout_ms` - Transaction timeout in milliseconds
    /// * `client_id` - Optional client identifier for debugging
    ///
    /// # Returns
    /// Tuple of (producer_id, epoch)
    fn get_or_create_transactional_producer(
        &self,
        transactional_id: &str,
        transaction_timeout_ms: i32,
        client_id: Option<&str>,
    ) -> Result<(i64, i16)>;

    /// Begin a transaction
    ///
    /// Sets transaction state to 'Ongoing' and records start time.
    /// Must be called before producing transactional records.
    ///
    /// # Arguments
    /// * `transactional_id` - The transactional ID
    /// * `producer_id` - The producer ID
    /// * `producer_epoch` - The producer epoch (for fencing check)
    fn begin_transaction(
        &self,
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
    ) -> Result<()>;

    /// Atomically begin or continue a transaction
    ///
    /// This method handles the race condition between checking transaction state
    /// and updating it. It uses an atomic compare-and-swap operation to:
    /// 1. Check if transaction exists and is in a valid starting state (Empty, CompleteCommit, CompleteAbort)
    /// 2. If valid, atomically transition to 'Ongoing' state
    /// 3. If already 'Ongoing' with matching producer, continue (idempotent)
    /// 4. If producer mismatch or invalid state, return appropriate error
    ///
    /// # Arguments
    /// * `transactional_id` - The transactional ID
    /// * `producer_id` - The producer ID
    /// * `producer_epoch` - The producer epoch (for fencing check)
    fn begin_or_continue_transaction(
        &self,
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
    ) -> Result<()>;

    /// Validate that producer owns an active transaction
    ///
    /// Checks that the transaction exists, is in 'Ongoing' state, and the
    /// producer_id/epoch match.
    ///
    /// # Arguments
    /// * `transactional_id` - The transactional ID
    /// * `producer_id` - The producer ID to validate
    /// * `producer_epoch` - The producer epoch to validate
    fn validate_transaction(
        &self,
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
    ) -> Result<()>;

    /// Insert records with transaction metadata
    ///
    /// Similar to insert_records but sets txn_state='pending' and records
    /// producer_id/epoch for transaction tracking.
    ///
    /// # Arguments
    /// * `topic_id` - Topic ID
    /// * `partition_id` - Partition ID
    /// * `records` - Slice of records to insert
    /// * `producer_id` - Producer ID for transaction tracking
    /// * `producer_epoch` - Producer epoch for fencing
    ///
    /// # Returns
    /// Base partition offset assigned to the first record
    fn insert_transactional_records(
        &self,
        topic_id: i32,
        partition_id: i32,
        records: &[Record],
        producer_id: i64,
        producer_epoch: i16,
    ) -> Result<i64>;

    /// Store pending offset commit within a transaction
    ///
    /// Stores offset in txn_pending_offsets table. Will be moved to
    /// consumer_offsets on commit, deleted on abort.
    ///
    /// # Arguments
    /// * `transactional_id` - The transactional ID
    /// * `group_id` - Consumer group ID
    /// * `topic_id` - Topic ID
    /// * `partition_id` - Partition ID
    /// * `offset` - Offset to commit
    /// * `metadata` - Optional metadata
    fn store_txn_pending_offset(
        &self,
        transactional_id: &str,
        group_id: &str,
        topic_id: i32,
        partition_id: i32,
        offset: i64,
        metadata: Option<&str>,
    ) -> Result<()>;

    /// Commit a transaction
    ///
    /// 1. UPDATE messages SET txn_state=NULL WHERE producer_id=X AND txn_state='pending'
    /// 2. INSERT INTO consumer_offsets SELECT ... FROM txn_pending_offsets
    /// 3. DELETE FROM txn_pending_offsets WHERE transactional_id=X
    /// 4. UPDATE transactions SET state='CompleteCommit'
    ///
    /// # Arguments
    /// * `transactional_id` - The transactional ID
    /// * `producer_id` - The producer ID (for fencing check)
    /// * `producer_epoch` - The producer epoch (for fencing check)
    fn commit_transaction(
        &self,
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
    ) -> Result<()>;

    /// Abort a transaction
    ///
    /// 1. UPDATE messages SET txn_state='aborted' WHERE producer_id=X AND txn_state='pending'
    /// 2. DELETE FROM txn_pending_offsets WHERE transactional_id=X
    /// 3. UPDATE transactions SET state='CompleteAbort'
    ///
    /// # Arguments
    /// * `transactional_id` - The transactional ID
    /// * `producer_id` - The producer ID (for fencing check)
    /// * `producer_epoch` - The producer epoch (for fencing check)
    fn abort_transaction(
        &self,
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
    ) -> Result<()>;

    /// Get current transaction state
    ///
    /// # Arguments
    /// * `transactional_id` - The transactional ID
    ///
    /// # Returns
    /// Current transaction state, or None if transactional_id not found
    fn get_transaction_state(&self, transactional_id: &str) -> Result<Option<TransactionState>>;

    /// Fetch records with isolation level filtering
    ///
    /// Like fetch_records but respects isolation level:
    /// - ReadUncommitted: returns all records (existing behavior)
    /// - ReadCommitted: filters out records where txn_state='pending' or 'aborted'
    ///
    /// # Arguments
    /// * `topic_id` - Topic ID
    /// * `partition_id` - Partition ID
    /// * `fetch_offset` - Offset to start fetching from
    /// * `max_bytes` - Maximum bytes to fetch (approximate limit)
    /// * `isolation_level` - Consumer isolation level
    ///
    /// # Returns
    /// Vector of fetched messages respecting isolation level
    fn fetch_records_with_isolation(
        &self,
        topic_id: i32,
        partition_id: i32,
        fetch_offset: i64,
        max_bytes: i32,
        isolation_level: IsolationLevel,
    ) -> Result<Vec<FetchedMessage>>;

    /// Get last stable offset for a partition
    ///
    /// Returns the lowest offset of any pending transaction, or the high
    /// watermark if no transactions are pending. Used by read_committed
    /// consumers to know how far they can safely read.
    ///
    /// # Arguments
    /// * `topic_id` - Topic ID
    /// * `partition_id` - Partition ID
    ///
    /// # Returns
    /// Last stable offset
    fn get_last_stable_offset(&self, topic_id: i32, partition_id: i32) -> Result<i64>;

    /// Abort timed-out transactions
    ///
    /// Finds transactions in 'Ongoing' state that have exceeded their timeout
    /// and aborts them.
    ///
    /// # Arguments
    /// * `timeout` - Duration after which transactions are considered timed out
    ///
    /// # Returns
    /// List of transactional_ids that were aborted
    fn abort_timed_out_transactions(&self, timeout: Duration) -> Result<Vec<String>>;

    /// Clean up aborted messages
    ///
    /// Deletes messages with txn_state='aborted' that are older than the
    /// specified duration. Optional background cleanup.
    ///
    /// # Arguments
    /// * `older_than` - Only delete aborted messages older than this duration
    ///
    /// # Returns
    /// Number of messages deleted
    fn cleanup_aborted_messages(&self, older_than: Duration) -> Result<u64>;
}

// Submodules
pub mod postgres;

#[cfg(test)]
mod tests;

// Re-export main types
pub use postgres::PostgresStore;
