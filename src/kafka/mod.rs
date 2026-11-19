//! Everything Kafka: the wire protocol, the listener, the API handlers, the consumer-group
//! coordinator, storage and shadow mode.
//!
//! ## Two threads
//!
//! The extension's background worker runs two threads:
//!
//! 1. **Network thread** (a tokio runtime, [`listener`]): accepts connections, splits and
//!    decodes frames ([`protocol`], [`api`]), answers SASL itself, and hands every other
//!    request to the DB thread. It logs through `tracing`, never pgrx, which is not
//!    thread-safe.
//! 2. **DB thread** (the background worker's main thread, `crate::worker`): runs the
//!    requests through SPI inside Postgres transactions and sends each response back on
//!    the connection's own channel.
//!
//! Requests cross on bounded crossbeam channels ([`RequestLanes`]: a main lane and a
//! liveness lane for Heartbeat), which give the network thread backpressure when the
//! database falls behind; responses return on per-connection tokio channels. SPI can only
//! be called from the DB thread, so every database operation goes through it.

pub mod api;
pub mod assignment;
pub mod auth;
pub mod broker_metadata;
pub mod conn_limits;
pub mod constants;
pub mod context;
pub mod coordinator;
pub mod dispatch;
pub mod error;
pub mod handler_context;
pub mod handlers;
pub mod limits;
pub mod listener;
pub mod messages;
pub mod metrics;
pub mod notifications;
pub mod partitioner;
pub mod pending_fetches;
pub mod protocol;
pub mod response_builders;
pub mod shadow;
pub mod storage;

pub use broker_metadata::BrokerMetadata;
pub use constants::*;
pub use context::RuntimeContext;
pub use coordinator::{GroupCoordinator, GroupState};
pub use error::{KafkaError, Result};
pub use handler_context::HandlerContext;
pub use listener::run as run_listener;
pub use messages::{KafkaRequest, KafkaResponse, RequestLanes};
pub use notifications::InternalNotification;
pub use pending_fetches::PendingFetchRegistry;
pub use response_builders::*;
pub use shadow::{ShadowConfig, ShadowError, ShadowMode, SyncMode, TopicShadowConfig};
pub use storage::{KafkaStore, PostgresStore};
