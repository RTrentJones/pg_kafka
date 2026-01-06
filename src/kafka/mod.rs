// Kafka protocol implementation module
//
// This module contains all Kafka-specific code:
// - Binary protocol parsing/encoding
// - Request/response types and message queues
// - TCP listener for accepting Kafka client connections
// - Request handlers (ApiVersions, Metadata, Produce, Fetch)
//
// Architecture Overview:
// =====================
//
// This module bridges two worlds:
//
// 1. ASYNC WORLD (Tokio):
//    - Fast, non-blocking network I/O
//    - Handles thousands of concurrent TCP connections
//    - Parses binary Kafka protocol
//    - Runs in a single-threaded tokio LocalSet (pgrx requirement)
//
// 2. SYNC WORLD (Main Thread):
//    - Blocking database operations via Postgres SPI
//    - Thread-safe (required for Postgres FFI)
//    - Processes requests sequentially
//
// The MESSAGE QUEUE is the bridge:
//   Async tasks → [Request Queue] → Main thread processes → [Response Queue] → Async tasks
//
// Why this architecture?
// - Postgres SPI is NOT thread-safe and CANNOT be called from async/tokio context
// - Network I/O benefits massively from async (one thread handles many connections)
// - Database operations are inherently blocking anyway
// - The queue cleanly separates concerns and prevents deadlocks

pub mod constants;
pub mod coordinator;
pub mod dispatch;
pub mod error;
pub mod handlers;
pub mod listener;
pub mod messages;
pub mod protocol;
pub mod response_builders;
pub mod storage;

// Re-export commonly used types for convenience
pub use constants::*;
pub use coordinator::{GroupCoordinator, GroupState};
pub use error::{KafkaError, Result};
pub use listener::run as run_listener;
pub use messages::{request_receiver, request_sender, KafkaRequest, KafkaResponse};
pub use response_builders::*;
pub use storage::{KafkaStore, PostgresStore};
