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
        /// List of topics to get metadata for (None = all topics)
        topics: Option<Vec<String>>,
        /// Channel to send the response back to the specific connection
        response_tx: tokio::sync::mpsc::UnboundedSender<KafkaResponse>,
    },
    // Future requests to add:
    // Produce { ... },
    // Fetch { ... },
}

/// Kafka response types sent back from main thread to async tasks
#[derive(Debug, Clone)]
pub enum KafkaResponse {
    /// ApiVersions response - lists supported API versions
    ApiVersions {
        /// Correlation ID from request - client uses this to match responses
        correlation_id: i32,
        /// List of supported API keys and their version ranges
        api_versions: Vec<ApiVersion>,
    },
    /// Metadata response - provides topic and broker metadata
    Metadata {
        /// Correlation ID from request
        correlation_id: i32,
        /// List of brokers in the cluster
        brokers: Vec<BrokerMetadata>,
        /// List of topics and their partitions
        topics: Vec<TopicMetadata>,
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

/// Represents a supported API version range
#[derive(Debug, Clone)]
pub struct ApiVersion {
    /// API key (e.g., 18 = ApiVersions, 3 = Metadata)
    pub api_key: i16,
    /// Minimum supported version
    pub min_version: i16,
    /// Maximum supported version
    pub max_version: i16,
}

/// Represents a Kafka broker in the cluster
///
/// In a real Kafka cluster, there are multiple brokers (servers) that handle
/// reads/writes and store data. For pg_kafka, we're a single-node "broker"
/// running inside PostgreSQL.
#[derive(Debug, Clone)]
pub struct BrokerMetadata {
    /// Broker ID (node_id) - unique identifier for this broker
    pub node_id: i32,
    /// Hostname or IP address where this broker is reachable
    pub host: String,
    /// Port number for Kafka protocol (e.g., 9092)
    pub port: i32,
    /// Optional rack ID for rack-aware replica placement (used in multi-DC setups)
    pub rack: Option<String>,
}

/// Represents a Kafka topic and its partitions
///
/// A topic is a category/feed name to which messages are published.
/// Topics are split into partitions for parallelism and scalability.
#[derive(Debug, Clone)]
pub struct TopicMetadata {
    /// Error code for this topic (0 = no error, non-zero = error like "topic not found")
    pub error_code: i16,
    /// Topic name (e.g., "user-events", "orders", "logs")
    pub name: String,
    /// List of partitions for this topic
    /// Each partition is an ordered, immutable sequence of messages
    pub partitions: Vec<PartitionMetadata>,
}

/// Represents a partition within a topic
///
/// Partitions are the unit of parallelism in Kafka:
/// - Each partition is an ordered log (offset 0, 1, 2, 3, ...)
/// - Messages within a partition maintain order
/// - Different partitions can be read/written in parallel
/// - In pg_kafka, partitions map to (topic_id, partition_id) in the messages table
///
/// Example: Topic "orders" with 3 partitions
///   Partition 0: [order1, order4, order7, ...]  <- All have partition_id=0
///   Partition 1: [order2, order5, order8, ...]  <- All have partition_id=1
///   Partition 2: [order3, order6, order9, ...]  <- All have partition_id=2
#[derive(Debug, Clone)]
pub struct PartitionMetadata {
    /// Error code for this partition (0 = no error)
    pub error_code: i16,
    /// Partition ID (0-based index, e.g., 0, 1, 2 for a topic with 3 partitions)
    pub partition_index: i32,
    /// Broker ID of the leader for this partition
    /// The leader handles all reads and writes for this partition
    /// For pg_kafka (single-node), this is always our broker ID (1)
    pub leader_id: i32,
    /// Broker IDs of replicas (copies of this partition's data)
    /// In real Kafka: [1, 2, 3] means brokers 1, 2, 3 have copies
    /// For pg_kafka (single-node): [1] means only we have the data
    pub replica_nodes: Vec<i32>,
    /// Broker IDs of in-sync replicas (replicas that are up-to-date with leader)
    /// In real Kafka: subset of replica_nodes that are caught up
    /// For pg_kafka: [1] (we're always in-sync with ourselves)
    pub isr_nodes: Vec<i32>,
}

// Global request queue: async tasks → main thread
// We use unbounded channels because:
// 1. Kafka clients can pipeline many requests
// 2. Backpressure is handled at TCP level (flow control)
// 3. We don't want to block async tasks on queue capacity
static REQUEST_QUEUE: Lazy<(Sender<KafkaRequest>, Receiver<KafkaRequest>)> =
    Lazy::new(|| unbounded());

/// Get the sender side of the request queue (for async tasks)
pub fn request_sender() -> Sender<KafkaRequest> {
    REQUEST_QUEUE.0.clone()
}

/// Get the receiver side of the request queue (for main worker thread)
pub fn request_receiver() -> Receiver<KafkaRequest> {
    REQUEST_QUEUE.1.clone()
}
