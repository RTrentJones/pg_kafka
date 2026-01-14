// Kafka protocol implementation module
//
// This module contains all Kafka-specific code:
// - Binary protocol parsing/encoding
// - Request/response types
// - TCP listener for accepting Kafka client connections
// - Request handlers (ApiVersions, Metadata, Produce, Fetch, etc.)
//
// ## Two-Thread Architecture
//
// This module bridges two threads:
//
// 1. **Network Thread** (spawned, runs tokio multi-thread runtime):
//    - Fast, non-blocking network I/O
//    - Handles thousands of concurrent TCP connections
//    - Parses binary Kafka protocol
//    - Sends requests to main thread via crossbeam bounded channel
//    - Uses `tracing` crate for logging (NOT pgrx - thread safety)
//
// 2. **Database Thread** (main BGWorker):
//    - Blocking database operations via Postgres SPI
//    - Blocks on channel recv (instant wake-up when requests arrive)
//    - Processes requests sequentially within transactions
//    - Uses pgrx logging (safe on main thread)
//
// The CROSSBEAM CHANNEL is the bridge:
//   Network thread → [bounded(10_000)] → Database thread → [per-connection mpsc] → Network
//
// Why this architecture?
// - Postgres SPI is NOT thread-safe and CANNOT be called from spawned threads
// - Network I/O runs in parallel with database operations
// - Blocking recv eliminates the 100ms latency floor of the old time-slicing design
// - Bounded channel provides backpressure when DB can't keep up

pub mod assignment;
pub mod broker_metadata;
pub mod constants;
pub mod context;
pub mod coordinator;
pub mod dispatch;
pub mod error;
pub mod handler_context;
pub mod handlers;
pub mod listener;
pub mod messages;
pub mod notifications;
pub mod partitioner;
pub mod pending_fetches;
pub mod protocol;
pub mod response_builders;
pub mod shadow;
pub mod storage;

// Re-export commonly used types for convenience
pub use broker_metadata::BrokerMetadata;
pub use constants::*;
pub use context::RuntimeContext;
pub use coordinator::{GroupCoordinator, GroupState};
pub use error::{KafkaError, Result};
pub use handler_context::HandlerContext;
pub use listener::run as run_listener;
pub use messages::{KafkaRequest, KafkaResponse};
pub use notifications::InternalNotification;
pub use pending_fetches::PendingFetchRegistry;
pub use response_builders::*;
pub use shadow::{ShadowConfig, ShadowError, ShadowMode, SyncMode, TopicShadowConfig};
pub use storage::{KafkaStore, PostgresStore};
