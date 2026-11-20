//! The storage layer: the contract between the Kafka protocol handlers and the store.
//!
//! The contract is split into role traits ([`TopicStore`], [`LogStore`],
//! [`OffsetStore`], [`ProducerStore`], [`TransactionStore`], [`MaintenanceStore`]);
//! [`KafkaStore`] is their sum and is what handlers see. Each role groups the methods
//! one concern needs, so a wrapper (`ShadowStore`) can forward the roles it does not
//! change wholesale (the `delegate_*_store!` macros) and code that needs one concern
//! can take just that role. Handlers stay testable against `MockKafkaStore`, and
//! transaction boundaries stay with the caller: every method assumes it runs inside a
//! transaction the background worker (or a SQL function) manages.
//!
//! [`PostgresStore`] is the implementation; the pure logic it relies on (header
//! encoding, sequence validation, retention planning, the role grants) lives in
//! submodules that unit tests reach without a database.

use std::time::Duration;

use super::error::Result;
use super::messages::Record;

mod delegate;
mod headers;
mod sequence;

pub(crate) use delegate::{
    delegate_log_store, delegate_maintenance_store, delegate_offset_store, delegate_producer_store,
    delegate_topic_store, delegate_transaction_store,
};
pub use headers::{decode_headers_json, encode_headers_json};
pub use sequence::{
    batch_last_sequence, next_sequence, transactional_epoch_is_stale, validate_sequence,
    validate_unknown_producer, CachedBatch, PartitionSequence, ProducerBatch, SequenceCheck,
    SequenceValidation, PRODUCER_BATCH_CACHE,
};

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
    /// Record headers, decoded from JSONB storage (DR-8). They are stored as an
    /// ordered array of pairs, so their order survives; duplicate keys do not, because
    /// the kafka-protocol record type holds headers in a map (PROTOCOL_DEVIATIONS.md).
    pub headers: Vec<(String, Vec<u8>)>,
    /// Producer id stamped on the row: set for records written by a
    /// transactional producer, `None` otherwise. Fetch groups consecutive records of
    /// one producer id/epoch into one RecordBatch and marks it transactional.
    pub producer_id: Option<i64>,
    /// Producer epoch stamped with `producer_id`.
    pub producer_epoch: Option<i16>,
}

/// The producer that owns a transactional id, as InitProducerId judges a presented
/// producer id and epoch against it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransactionalProducer {
    /// Current producer id.
    pub producer_id: i64,
    /// Current epoch.
    pub epoch: i16,
    /// The epoch before the last bump the current producer asked for itself (an
    /// InitProducerId presenting its id and epoch); a retry of that request presents it.
    /// None after any other change of epoch (a new producer fencing the old, a timeout
    /// abort), as Kafka's lastProducerEpoch.
    pub previous_epoch: Option<i16>,
}

/// Committed offset information (internal representation)
#[derive(Debug, Clone)]
pub struct CommittedOffset {
    /// Committed offset value
    pub offset: i64,
    /// Optional metadata
    pub metadata: Option<String>,
}

/// Transaction state (the `kafka.transactions.state` column).
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

/// Consumer isolation level.
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

/// Offset bounds of one partition, as reported in a Fetch response
/// (see [`LogStore::get_partition_bounds`]).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PartitionBounds {
    /// Log-end offset; never regresses (BUG-4).
    pub high_watermark: i64,
    /// Lowest pending transactional offset (ReadCommitted), or the high watermark.
    pub last_stable_offset: i64,
    /// Earliest retained offset: GREATEST(first surviving row, durable log start).
    pub earliest_offset: i64,
    /// Durable log start (`kafka.partition_offsets.log_start_offset`).
    pub log_start_offset: i64,
}

impl PartitionBounds {
    /// Build the bounds from the raw per-partition values one SQL statement reads:
    /// the offset counter row (`next_offset`, `log_start_offset`; `None` when the
    /// partition has no counter row), MIN/MAX `partition_offset` over the stored rows,
    /// and the lowest pending transactional offset. Mirrors the single-value queries
    /// in `PostgresStore` so the combined statement cannot drift from them.
    pub fn from_raw(
        next_offset: Option<i64>,
        log_start_offset: Option<i64>,
        min_offset: Option<i64>,
        max_offset: Option<i64>,
        min_pending_offset: Option<i64>,
        isolation_level: IsolationLevel,
    ) -> Self {
        let log_start = log_start_offset.unwrap_or(0);
        let high_watermark = next_offset
            .unwrap_or(0)
            .max(max_offset.map_or(0, |m| m.saturating_add(1)));
        let last_stable_offset = match isolation_level {
            IsolationLevel::ReadCommitted => min_pending_offset.unwrap_or(high_watermark),
            IsolationLevel::ReadUncommitted => high_watermark,
        };
        PartitionBounds {
            high_watermark,
            last_stable_offset,
            earliest_offset: min_offset.unwrap_or(0).max(log_start),
            log_start_offset: log_start,
        }
    }
}

/// Order `rows` (`(name, id, partition_count)` from one batched lookup) as the
/// requested `names`, dropping names with no row. A name requested twice appears
/// twice.
pub fn order_topic_metadata(names: &[String], rows: Vec<(String, i32, i32)>) -> Vec<TopicMetadata> {
    let by_name: std::collections::HashMap<String, (i32, i32)> = rows
        .into_iter()
        .map(|(name, id, partitions)| (name, (id, partitions)))
        .collect();
    names
        .iter()
        .filter_map(|name| {
            by_name
                .get(name)
                .map(|&(id, partition_count)| TopicMetadata {
                    name: name.clone(),
                    id,
                    partition_count,
                })
        })
        .collect()
}

/// Topic catalog: lookup, auto-create, admin (create/delete/partition count) and per-topic config.
pub trait TopicStore {
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

    /// Get a topic's per-topic `retention.ms` override (API 32/44 support).
    ///
    /// # Returns
    /// `None` when the topic inherits the global
    /// `pg_kafka.message_retention_hours`; `Some(ms)` when an override is set
    /// (`ms < 0` = infinite retention for this topic).
    fn get_topic_retention_ms(&self, topic_id: i32) -> Result<Option<i64>>;

    /// Set or clear a topic's `retention.ms` override (IncrementalAlterConfigs).
    ///
    /// # Arguments
    /// * `retention_ms` - `Some(ms)` to set (negative = infinite), `None` to
    ///   clear the override and fall back to the global GUC
    fn set_topic_retention_ms(&self, topic_id: i32, retention_ms: Option<i64>) -> Result<()>;

    /// An existing topic whose name collides with `name` once '.' and '_' are treated as equal
    /// (Kafka's metric-name collision rule), other than `name` itself; `None` if there is none.
    fn find_colliding_topic(&self, name: &str) -> Result<Option<String>>;
}

/// The message log: append, read (with isolation), offset bounds and truncation.
pub trait LogStore {
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

    /// Get the durable log start offset for a topic partition
    /// (`kafka.partition_offsets.log_start_offset`).
    ///
    /// Unlike [`get_earliest_offset`](Self::get_earliest_offset), this ignores which
    /// rows happen to survive: aborted-message cleanup can delete a partition's leading
    /// rows without retiring those offsets, so only DeleteRecords and the retention
    /// sweep (which advance this column) make an offset out of range. Returns 0 when the
    /// partition has no counter row.
    fn get_log_start_offset(&self, topic_id: i32, partition_id: i32) -> Result<i64>;

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

    /// All offset bounds a Fetch response reports for one partition, in one storage
    /// round trip: high watermark, last stable offset (for `isolation_level`; equal
    /// to the high watermark for ReadUncommitted), earliest offset and durable log
    /// start. Each field matches the corresponding single-value method
    /// (`get_high_watermark`, `get_last_stable_offset`, `get_earliest_offset`,
    /// `get_log_start_offset`).
    ///
    /// The default composes those methods (four statements). `PostgresStore`
    /// overrides it with a single statement, and `ShadowStore` forwards it; the
    /// mockall `MockKafkaStore` deliberately keeps this default so expectations set
    /// on the single-value methods keep driving handler tests.
    fn get_partition_bounds(
        &self,
        topic_id: i32,
        partition_id: i32,
        isolation_level: IsolationLevel,
    ) -> Result<PartitionBounds> {
        let high_watermark = self.get_high_watermark(topic_id, partition_id)?;
        let last_stable_offset = match isolation_level {
            IsolationLevel::ReadCommitted => self.get_last_stable_offset(topic_id, partition_id)?,
            IsolationLevel::ReadUncommitted => high_watermark,
        };
        let earliest_offset = self.get_earliest_offset(topic_id, partition_id)?;
        let log_start_offset = self.get_log_start_offset(topic_id, partition_id)?;
        Ok(PartitionBounds {
            high_watermark,
            last_stable_offset,
            earliest_offset,
            log_start_offset,
        })
    }

    /// Find the earliest offset whose record timestamp is at or after `timestamp_ms` (CONF-7,
    /// Kafka's ListOffsets-by-timestamp). Returns `(offset, record_timestamp_ms)`, or `None` when no
    /// record qualifies. Records that carried no timestamp (stored as -1) and aborted records are
    /// excluded; pending transactional records are included (the caller bounds the result by the
    /// high watermark or the last stable offset, as Kafka does).
    ///
    /// `timestamp_ms == MAX_TIMESTAMP` (-3, KIP-734) instead returns the record with the
    /// largest timestamp (the lowest offset among equal ones).
    fn get_offset_for_timestamp(
        &self,
        topic_id: i32,
        partition_id: i32,
        timestamp_ms: i64,
    ) -> Result<Option<(i64, i64)>>;

    /// Delete records below `before_offset` in a partition (DeleteRecords, API 21).
    ///
    /// Rows with `partition_offset < before_offset` are removed (pending
    /// transactional rows included — the caller has explicitly asked for
    /// truncation, matching Kafka semantics where DeleteRecords advances the
    /// log start offset unconditionally).
    ///
    /// # Arguments
    /// * `before_offset` - Exclusive upper bound; caller must already have
    ///   clamped/validated it against the high watermark
    ///
    /// # Returns
    /// The partition's new log start offset (earliest remaining offset, or the
    /// high watermark when the partition is now empty)
    fn delete_records_before(
        &self,
        topic_id: i32,
        partition_id: i32,
        before_offset: i64,
    ) -> Result<i64>;
}

/// Committed consumer-group offsets.
pub trait OffsetStore {
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

    /// Delete all committed offsets for a consumer group
    ///
    /// # Arguments
    /// * `group_id` - Consumer group ID
    ///
    /// # Returns
    /// Ok on success
    fn delete_consumer_group_offsets(&self, group_id: &str) -> Result<()>;

    /// The ids of every group that has at least one committed offset (sorted). These groups
    /// exist for DescribeGroups / ListGroups even when the in-memory coordinator does not know
    /// them (e.g. after a worker restart), as Kafka reports them as Empty groups.
    fn list_offset_groups(&self) -> Result<Vec<String>>;

    /// Whether `group_id` has at least one committed offset.
    fn group_has_offsets(&self, group_id: &str) -> Result<bool>;
    /// The `(topic name, partition)` pairs of `group_id` that have a transactional
    /// offset commit (TxnOffsetCommit) whose transaction has not ended yet. OffsetFetch
    /// with `require_stable` (KIP-447) answers UNSTABLE_OFFSET_COMMIT for them.
    fn pending_txn_offset_partitions(&self, group_id: &str) -> Result<Vec<(String, i32)>>;
}

/// Idempotent producer state: producer ids, epochs and sequence numbers.
pub trait ProducerStore {
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

    /// Validate an idempotent producer's batch against the producer state of one
    /// partition (Kafka's model, see [`validate_sequence`]).
    ///
    /// The batch is appended when it starts right after the partition's last sequence
    /// (at any sequence on a partition where the producer has no entry, at 0 on one
    /// whose entry an epoch change reset), and is a duplicate when its exact sequence
    /// range is one of the producer's last [`PRODUCER_BATCH_CACHE`] batches on the
    /// partition; then the original base offset is returned. Nothing is written for a
    /// duplicate or an error; a batch at a new (client-bumped) epoch moves the
    /// producer's stored epoch and resets its sequence state first.
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
    /// * `Ok(SequenceCheck::Append)`: insert the batch, then [`record_producer_batch`](Self::record_producer_batch)
    /// * `Ok(SequenceCheck::Duplicate { base_offset })`: a retry; skip the insert and
    ///   answer success with the original base offset
    /// * `Err(OutOfOrderSequence)`: anything else out of sequence
    /// * `Err(Protocol { code: INVALID_PRODUCER_EPOCH })`: a stale epoch
    fn check_and_update_sequence(
        &self,
        producer_id: i64,
        producer_epoch: i16,
        topic_id: i32,
        partition_id: i32,
        base_sequence: i32,
        record_count: i32,
    ) -> Result<SequenceCheck>;

    /// Persist an appended idempotent batch AFTER its records were inserted (RV-8):
    /// advance the partition's last sequence and remember the batch (sequence range and
    /// base offset) among the producer's last [`PRODUCER_BATCH_CACHE`] batches there, so
    /// a retry of it is answered with its original offset.
    fn record_producer_batch(&self, batch: ProducerBatch) -> Result<()>;

    /// Register partitions in a transaction (AddPartitionsToTxn). A transactional
    /// Produce is accepted only for registered partitions. The registry is
    /// cleared when the next transaction of the transactional id begins and on
    /// InitProducerId.
    fn add_txn_partitions(&self, transactional_id: &str, partitions: &[(i32, i32)]) -> Result<()>;

    /// Whether `(topic_id, partition_id)` was registered for the current transaction of
    /// `transactional_id` with [`add_txn_partitions`](Self::add_txn_partitions).
    fn txn_has_partition(
        &self,
        transactional_id: &str,
        topic_id: i32,
        partition_id: i32,
    ) -> Result<bool>;

    /// Persist a producer's advanced `last_sequence` AFTER a successful insert (RV-8).
    ///
    /// `check_and_update_sequence` only validates, so the advance happens only once
    /// the records are durably inserted: a failed insert must not leave the sequence
    /// advanced, or a retry of the same batch would be taken for a duplicate and
    /// dropped.
    ///
    /// Sets only the last sequence; the produce path uses
    /// [`record_producer_batch`](Self::record_producer_batch), which also remembers
    /// the batch for duplicate detection.
    fn record_producer_sequence(
        &self,
        producer_id: i64,
        topic_id: i32,
        partition_id: i32,
        last_sequence: i32,
    ) -> Result<()>;

    /// The producer that owns `transactional_id`, if it has one.
    fn transactional_producer(
        &self,
        transactional_id: &str,
    ) -> Result<Option<TransactionalProducer>>;

    /// Record `previous_epoch` as the epoch before the producer's current one, after a
    /// bump the producer asked for (see [`TransactionalProducer::previous_epoch`]).
    fn remember_previous_epoch(&self, producer_id: i64, previous_epoch: i16) -> Result<()>;
}

/// Transactions: transactional producers, state, transactional writes and offsets, commit/abort.
pub trait TransactionStore {
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
}

/// Background maintenance the worker runs between requests.
pub trait MaintenanceStore {
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

/// Every storage role: what handlers see through `HandlerContext::store`. Implemented for
/// every type that implements all roles.
pub trait KafkaStore:
    TopicStore + LogStore + OffsetStore + ProducerStore + TransactionStore + MaintenanceStore
{
}

impl<T> KafkaStore for T where
    T: TopicStore
        + LogStore
        + OffsetStore
        + ProducerStore
        + TransactionStore
        + MaintenanceStore
        + ?Sized
{
}

/// Every storage role trait, for code that calls store methods on a concrete store type
/// (`use crate::kafka::storage::roles::*`); through `&dyn KafkaStore` they are in scope
/// already.
pub mod roles {
    pub use super::{
        LogStore, MaintenanceStore, OffsetStore, ProducerStore, TopicStore, TransactionStore,
    };
}

/// ListOffsets' MAX_TIMESTAMP (-3, KIP-734): the record with the largest timestamp.
pub const MAX_TIMESTAMP: i64 = -3;

pub mod postgres;
pub mod privileges;
pub mod retention;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod schema_tests;

pub use postgres::PostgresStore;
