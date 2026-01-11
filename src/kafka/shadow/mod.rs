//! Shadow Mode Module (Phase 11)
//!
//! This module provides shadow mode functionality for forwarding messages to an
//! external Kafka cluster, enabling gradual migration from pg_kafka to production Kafka.
//!
//! ## Features
//!
//! - **Per-topic configuration**: Enable shadow mode for specific topics
//! - **Percentage dial-up**: Gradually increase forwarding (0-100%)
//! - **Primary-only forwarding**: Only the primary forwards (standbys don't receive writes)
//! - **Sync/async modes**: Configurable per-topic or globally
//! - **Tracking and replay**: Track which messages were forwarded
//! - **Observability**: Metrics and OpenTelemetry integration
//!
//! ## Architecture
//!
//! ```text
//! ShadowStore (wrapper)
//!     │
//!     ├──> PostgresStore (local write)
//!     │
//!     └──> ShadowForwarder (external forward)
//!              │
//!              ├──> is_primary() check (pg_is_in_recovery)
//!              │
//!              └──> ShadowProducer (rdkafka)
//! ```
//!
//! ## Configuration
//!
//! Global settings via GUCs:
//! - `pg_kafka.shadow_mode_enabled`: Enable shadow mode globally
//! - `pg_kafka.shadow_bootstrap_servers`: External Kafka brokers
//! - `pg_kafka.shadow_security_protocol`: SASL_SSL, SASL_PLAINTEXT, etc.
//! - `pg_kafka.shadow_sasl_*`: SASL authentication settings
//!
//! Per-topic settings via `kafka.shadow_config` table:
//! - `mode`: local_only or shadow
//! - `forward_percentage`: 0-100
//! - `sync_mode`: async or sync

pub mod config;
pub mod error;
pub mod forwarder;
pub mod primary;
pub mod producer;
pub mod replay;
pub mod routing;
pub mod store;

#[cfg(test)]
mod tests;

// Re-export commonly used types
pub use config::{
    ShadowConfig, ShadowMode, SyncMode, TopicConfigCache, TopicShadowConfig, WriteMode,
};
pub use error::{ShadowError, ShadowResult};
pub use forwarder::{ForwardDecision, ForwardMessage, ForwardResult, ShadowForwarder};
pub use primary::{is_primary, PrimaryStatus};
pub use producer::{ShadowProducer, ShadowProducerBuilder};
pub use replay::{ReplayEngine, ReplayProgress, ReplayRequest, ReplayResult};
pub use routing::{compute_routing_hash, make_forward_decision};
pub use store::{ShadowMetrics, ShadowStore};

/// Message sent from DB thread to network thread for async forwarding
///
/// When sync_mode is Async, the ShadowStore sends ForwardRequest messages
/// via a crossbeam channel to the network thread, which handles the actual
/// forwarding asynchronously without blocking the DB worker thread.
#[derive(Debug, Clone)]
pub struct ForwardRequest {
    /// External topic name to forward to
    pub topic_name: String,
    /// Partition ID for the message
    pub partition_id: i32,
    /// Message key (optional)
    pub key: Option<Vec<u8>>,
    /// Message value (optional)
    pub value: Option<Vec<u8>>,
    /// Local offset for metrics/replay tracking
    pub local_offset: i64,
}

impl ForwardRequest {
    /// Create a new forward request
    pub fn new(
        topic_name: String,
        partition_id: i32,
        key: Option<Vec<u8>>,
        value: Option<Vec<u8>>,
        local_offset: i64,
    ) -> Self {
        Self {
            topic_name,
            partition_id,
            key,
            value,
            local_offset,
        }
    }
}
