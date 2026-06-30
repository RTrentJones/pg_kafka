// Copyright (c) 2026 Robert Trent Jones.
// This file is part of the "Shadow Mode" feature of pg_kafka.
//
// Use of this source code for production purposes is governed by the
// Commercial License found in the LICENSE file in this directory.
// Development and evaluation use is permitted.
//
// GitHub Sponsors: https://github.com/sponsors/RTrentJones

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
//! produce ──> ShadowStore ──> PostgresStore                 (local write)
//!                  └────────> kafka.shadow_tracking         (durable outbox row, same txn)
//!
//! DB-thread poll: claim due rows ──ForwardRequest──> network thread ──> ShadowProducer
//!                 finalize rows  <──ForwardAck─────── network thread      (rdkafka, idempotent)
//! ```
//! Only the primary forwards (is_primary / pg_is_in_recovery).
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
pub mod license;
pub mod primary;
pub mod producer;
pub mod routing;
pub mod store;

#[cfg(test)]
mod tests;

// Re-export commonly used types
pub use config::{
    ShadowConfig, ShadowMode, SyncMode, TopicConfigCache, TopicShadowConfig, WriteMode,
};
pub use error::{ShadowError, ShadowResult};
pub use forwarder::ForwardDecision;
pub use primary::{is_primary, PrimaryStatus};
pub use producer::{ShadowProducer, ShadowProducerBuilder};
pub use routing::{compute_routing_hash, make_forward_decision};
pub use store::{ShadowMetrics, ShadowStore};

// License validation (Commercial License)
pub use license::{LicenseStatus, LicenseValidator};

/// Message sent from DB thread to network thread for async forwarding
///
/// When sync_mode is Async, the ShadowStore sends ForwardRequest messages
/// via a crossbeam channel to the network thread, which handles the actual
/// forwarding asynchronously without blocking the DB worker thread.
#[derive(Debug, Clone)]
pub struct ForwardRequest {
    /// Local pg_kafka topic id — identifies the `kafka.shadow_tracking` outbox
    /// row so the resulting [`ForwardAck`] can be matched back to it.
    pub topic_id: i32,
    /// External topic name to forward to
    pub topic_name: String,
    /// Partition ID for the message (local; part of the outbox row key)
    pub partition_id: i32,
    /// Message key (optional)
    pub key: Option<Vec<u8>>,
    /// Message value (optional)
    pub value: Option<Vec<u8>>,
    /// Local offset — the third component of the outbox row key
    pub local_offset: i64,
}

impl ForwardRequest {
    /// Create a new forward request
    pub fn new(
        topic_id: i32,
        topic_name: String,
        partition_id: i32,
        key: Option<Vec<u8>>,
        value: Option<Vec<u8>>,
        local_offset: i64,
    ) -> Self {
        Self {
            topic_id,
            topic_name,
            partition_id,
            key,
            value,
            local_offset,
        }
    }
}

/// Result of a single durable-outbox forward attempt, sent from the network
/// thread back to the DB thread so it can finalize the `kafka.shadow_tracking`
/// row.
///
/// The `(topic_id, partition_id, local_offset)` triple is the outbox row's
/// primary key. `result` is `Ok(external_offset)` on a confirmed external write
/// or `Err(message)` when the forward failed (the row stays pending and is
/// retried).
#[derive(Debug, Clone)]
pub struct ForwardAck {
    /// Local topic id (outbox row key)
    pub topic_id: i32,
    /// Local partition id (outbox row key)
    pub partition_id: i32,
    /// Local offset (outbox row key)
    pub local_offset: i64,
    /// `Ok(external_offset)` on success, `Err(error_message)` on failure.
    pub result: Result<i64, String>,
}

// Unit tests for ForwardRequest (exported from mod.rs)
#[cfg(test)]
mod forward_request_tests {
    use super::ForwardRequest;

    #[test]
    fn test_forward_request_new() {
        let req = ForwardRequest::new(
            7,
            "test-topic".to_string(),
            0,
            Some(b"key".to_vec()),
            Some(b"value".to_vec()),
            100,
        );
        assert_eq!(req.topic_id, 7);
        assert_eq!(req.topic_name, "test-topic");
        assert_eq!(req.partition_id, 0);
        assert_eq!(req.key, Some(b"key".to_vec()));
        assert_eq!(req.value, Some(b"value".to_vec()));
        assert_eq!(req.local_offset, 100);
    }

    #[test]
    fn test_forward_request_with_null_key() {
        let req = ForwardRequest::new(1, "topic".to_string(), 1, None, Some(b"v".to_vec()), 50);
        assert!(req.key.is_none());
        assert!(req.value.is_some());
    }

    #[test]
    fn test_forward_request_with_null_value() {
        let req = ForwardRequest::new(1, "topic".to_string(), 2, Some(b"k".to_vec()), None, 75);
        assert!(req.key.is_some());
        assert!(req.value.is_none());
    }

    #[test]
    fn test_forward_request_clone() {
        let req = ForwardRequest::new(2, "clone-topic".to_string(), 3, None, None, 999);
        let cloned = req.clone();
        assert_eq!(cloned.topic_id, req.topic_id);
        assert_eq!(cloned.topic_name, req.topic_name);
        assert_eq!(cloned.partition_id, req.partition_id);
        assert_eq!(cloned.local_offset, req.local_offset);
    }

    #[test]
    fn test_forward_request_debug_format() {
        let req = ForwardRequest::new(0, "debug".to_string(), 0, None, None, 0);
        let debug = format!("{:?}", req);
        assert!(debug.contains("ForwardRequest"));
        assert!(debug.contains("debug"));
    }
}
