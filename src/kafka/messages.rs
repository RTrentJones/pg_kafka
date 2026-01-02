// Message queue module for pg_kafka
//
// This module defines the message types and queues used to communicate between
// the async tokio tasks (network I/O) and the sync background worker main thread
// (SPI database operations).
//
// Architecture:
// - Tokio tasks parse Kafka requests from TCP sockets → send to REQUEST_QUEUE
// - Background worker receives from REQUEST_QUEUE → processes → sends to RESPONSE_QUEUE
// - Tokio tasks receive from RESPONSE_QUEUE → encode → write to TCP sockets
//
// Why this design?
// - Postgres SPI (database operations) cannot be called from async/tokio context
// - SPI must run on the background worker's main thread
// - The queue bridges the async networking world and sync database world

use crossbeam_channel::{unbounded, Receiver, Sender};
use once_cell::sync::Lazy;

/// Kafka request types that can be sent from async tasks to the main worker thread
#[derive(Debug)]
pub enum KafkaRequest {
    /// ApiVersions request - asks which API versions the broker supports
    ApiVersions {
        /// Correlation ID from client - MUST be echoed back in response
        correlation_id: i32,
        /// Optional client identifier string
        client_id: Option<String>,
        /// API version from the request (needed for response encoding)
        api_version: i16,
        /// Channel to send the response back to the specific connection
        /// Using tokio::sync::mpsc for async-friendly response delivery
        response_tx: tokio::sync::mpsc::UnboundedSender<KafkaResponse>,
    },
    /// Metadata request - asks for topic and broker metadata
    Metadata {
        /// Correlation ID from client - MUST be echoed back in response
        correlation_id: i32,
        /// Optional client identifier string
        client_id: Option<String>,
        /// API version from the request (needed for response encoding)
        api_version: i16,
        /// List of topics to get metadata for (None = all topics)
        topics: Option<Vec<String>>,
        /// Channel to send the response back to the specific connection
        response_tx: tokio::sync::mpsc::UnboundedSender<KafkaResponse>,
    },
    /// Produce request - write messages to topic partitions
    Produce {
        /// Correlation ID from client - MUST be echoed back in response
        correlation_id: i32,
        /// Optional client identifier string
        client_id: Option<String>,
        /// API version from the request (needed for response encoding)
        api_version: i16,
        /// Acknowledgment level (0=none, 1=leader, -1=all ISR)
        acks: i16,
        /// Timeout for waiting for acknowledgments (milliseconds)
        timeout_ms: i32,
        /// Topic data (topic → partitions → records)
        topic_data: Vec<TopicProduceData>,
        /// Channel to send the response back to the specific connection
        response_tx: tokio::sync::mpsc::UnboundedSender<KafkaResponse>,
    },
    // Future requests to add:
    // Fetch { ... },
}

/// Kafka response types sent back from main thread to async tasks
///
/// Phase 2 Refactoring: We wrap kafka-protocol response types directly to eliminate
/// the verbose conversion code in protocol.rs::encode_response()
#[derive(Debug, Clone)]
pub enum KafkaResponse {
    /// ApiVersions response - wraps kafka-protocol's ApiVersionsResponse
    ApiVersions {
        /// Correlation ID from request - client uses this to match responses
        correlation_id: i32,
        /// API version to use for encoding the response
        api_version: i16,
        /// The kafka-protocol response struct (ready to encode)
        response: kafka_protocol::messages::api_versions_response::ApiVersionsResponse,
    },
    /// Metadata response - wraps kafka-protocol's MetadataResponse
    Metadata {
        /// Correlation ID from request
        correlation_id: i32,
        /// API version from the request (needed for response encoding)
        api_version: i16,
        /// The kafka-protocol response struct (ready to encode)
        response: kafka_protocol::messages::metadata_response::MetadataResponse,
    },
    /// Produce response - wraps kafka-protocol's ProduceResponse
    Produce {
        /// Correlation ID from request
        correlation_id: i32,
        /// API version from the request (needed for response encoding)
        api_version: i16,
        /// The kafka-protocol response struct (ready to encode)
        response: kafka_protocol::messages::produce_response::ProduceResponse,
    },
    /// Error response for unsupported or malformed requests
    Error {
        /// Correlation ID from request
        correlation_id: i32,
        /// Error code (Kafka error codes)
        error_code: i16,
        /// Human-readable error message
        error_message: Option<String>,
    },
}

/// Data for producing to a topic
#[derive(Debug, Clone)]
pub struct TopicProduceData {
    /// Topic name
    pub name: String,
    /// Partitions to write to
    pub partitions: Vec<PartitionProduceData>,
}

/// Data for producing to a partition
#[derive(Debug, Clone)]
pub struct PartitionProduceData {
    /// Partition ID
    pub partition_index: i32,
    /// Records to write
    pub records: Vec<Record>,
}

/// A single Kafka record/message
#[derive(Debug, Clone)]
pub struct Record {
    /// Optional message key (used for partitioning and log compaction)
    pub key: Option<Vec<u8>>,
    /// Optional message value (payload)
    pub value: Option<Vec<u8>>,
    /// Message headers (key-value metadata)
    pub headers: Vec<RecordHeader>,
    /// Timestamp (milliseconds since epoch, optional)
    pub timestamp: Option<i64>,
}

/// Record header (key-value metadata)
#[derive(Debug, Clone)]
pub struct RecordHeader {
    /// Header key (UTF-8 string)
    pub key: String,
    /// Header value (binary data)
    pub value: Vec<u8>,
}

/// Response for a topic in ProduceResponse
#[derive(Debug, Clone)]
pub struct TopicProduceResponse {
    /// Topic name
    pub name: String,
    /// Per-partition responses
    pub partitions: Vec<PartitionProduceResponse>,
}

/// Response for a partition in ProduceResponse
#[derive(Debug, Clone)]
pub struct PartitionProduceResponse {
    /// Partition ID
    pub partition_index: i32,
    /// Error code (0 = success)
    pub error_code: i16,
    /// Base offset assigned to the first message in the batch
    /// This is the partition_offset (NOT global_offset)
    pub base_offset: i64,
    /// Timestamp when the log was appended (-1 if not used)
    pub log_append_time: i64,
    /// Earliest available offset in this partition (-1 if not tracked)
    pub log_start_offset: i64,
}

// Global request queue: async tasks → main thread
//
// WHY CROSSBEAM INSTEAD OF TOKIO CHANNELS?
// =========================================
//
// We use crossbeam_channel instead of tokio::sync::mpsc because:
//
// 1. SYNC/ASYNC BOUNDARY:
//    - The main worker thread is SYNC (cannot use .await)
//    - Tokio channels require async context for recv()
//    - Crossbeam provides sync try_recv() that works in sync code
//
// 2. ARCHITECTURE:
//    - Async tokio tasks send requests via Sender (works from async)
//    - Sync main thread receives via try_recv() (works from sync)
//    - This is the bridge between the async network I/O and sync SPI
//
// 3. UNBOUNDED QUEUE:
//    - Kafka clients can pipeline many requests
//    - Backpressure is handled at TCP level (flow control)
//    - We don't want to block async tasks on queue capacity
//
// See src/worker.rs:120 for the sync recv side: `while let Ok(req) = request_rx.try_recv()`
static REQUEST_QUEUE: Lazy<(Sender<KafkaRequest>, Receiver<KafkaRequest>)> =
    Lazy::new(unbounded);

/// Get the sender side of the request queue (for async tasks)
pub fn request_sender() -> Sender<KafkaRequest> {
    REQUEST_QUEUE.0.clone()
}

/// Get the receiver side of the request queue (for main worker thread)
pub fn request_receiver() -> Receiver<KafkaRequest> {
    REQUEST_QUEUE.1.clone()
}
