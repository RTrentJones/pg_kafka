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
    // Future requests to add:
    // Metadata { ... },
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
