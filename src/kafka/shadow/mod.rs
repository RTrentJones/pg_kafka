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
//!                 finalize rows  <──ForwardAck─────── network thread      (rdkafka, idempotent,
//!                                                                          pipelined)
//! sync produce:   commit, then hand the response ──Hold──> released by the forwarder
//!                                                         on delivery or timeout
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

pub(crate) mod config;
pub(crate) mod error;
pub(crate) mod forwarder_task;
pub(crate) mod inflight;
pub(crate) mod license;
pub(crate) mod outbox;
pub(crate) mod primary;
pub(crate) mod producer;
pub(crate) mod routing;
pub(crate) mod store;

// The types the rest of the crate (and `crate::kafka`'s public re-exports) name.
pub use config::{ShadowConfig, ShadowMode, SyncMode, TopicShadowConfig};
pub use error::ShadowError;
pub(crate) use producer::ShadowProducer;
pub(crate) use store::ShadowStore;

use std::sync::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

/// Lock a mutex. Shadow mode's locks guard plain data that a panicking holder
/// cannot leave half-updated, so a poisoned lock is recovered (with a warning
/// naming the call site) instead of taking the worker down.
#[track_caller]
pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poisoned| {
        warn_poisoned();
        poisoned.into_inner()
    })
}

/// Read-lock an `RwLock`, recovering a poisoned one (see [`lock`]).
#[track_caller]
pub(crate) fn read_lock<T>(rwlock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    rwlock.read().unwrap_or_else(|poisoned| {
        warn_poisoned();
        poisoned.into_inner()
    })
}

/// Write-lock an `RwLock`, recovering a poisoned one (see [`lock`]).
#[track_caller]
pub(crate) fn write_lock<T>(rwlock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    rwlock.write().unwrap_or_else(|poisoned| {
        warn_poisoned();
        poisoned.into_inner()
    })
}

#[track_caller]
fn warn_poisoned() {
    tracing::warn!(
        "shadow mode lock at {} was poisoned, recovering",
        std::panic::Location::caller()
    );
}

/// Outbox row key: `(topic_id, partition_id, local_offset)`.
pub type RowKey = (i32, i32, i64);

/// One outbox record the DB thread hands to the network-thread forwarder.
#[derive(Debug, Clone, PartialEq)]
pub struct ForwardRecord {
    /// Local pg_kafka topic id — identifies the `kafka.shadow_tracking` outbox
    /// row so the resulting [`ForwardAck`] can be matched back to it.
    pub topic_id: i32,
    /// External topic name to forward to
    pub topic_name: String,
    /// Local partition id (part of the outbox row key; the external cluster
    /// re-partitions by key)
    pub partition_id: i32,
    /// Message key (optional)
    pub key: Option<Vec<u8>>,
    /// Message value (optional)
    pub value: Option<Vec<u8>>,
    /// Record headers, in order (forwarded as Kafka record headers)
    pub headers: Vec<(String, Vec<u8>)>,
    /// Record timestamp in ms (the producer's CreateTime, or the broker insert
    /// time when the producer sent none — what a local consumer sees)
    pub timestamp_ms: Option<i64>,
    /// Local offset — the third component of the outbox row key
    pub local_offset: i64,
    /// True when this is the first undelivered (and not dead-lettered) record of
    /// its local partition: the forwarder may restart that partition's sequence
    /// here after an earlier failure (see `forwarder_task`).
    pub restart: bool,
}

impl ForwardRecord {
    /// A record with no headers or timestamp, not a partition restart.
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
            headers: Vec::new(),
            timestamp_ms: None,
            local_offset,
            restart: false,
        }
    }

    /// The outbox row key.
    pub fn row_key(&self) -> RowKey {
        (self.topic_id, self.partition_id, self.local_offset)
    }
}

/// Sends a sync-mode produce's buffered response(s) to the client exactly once:
/// when the forwarder calls [`HeldRelease::release`], or when the value is
/// dropped (so a hold that is lost — a full channel, a forwarder that stopped —
/// still answers the client instead of hanging it). The closure runs on whatever
/// thread releases it and therefore must not call pgrx.
pub struct HeldRelease(Option<Box<dyn FnOnce() + Send>>);

impl HeldRelease {
    pub fn new(release: impl FnOnce() + Send + 'static) -> Self {
        Self(Some(Box::new(release)))
    }

    /// Release now (idempotent with the drop).
    pub fn release(mut self) {
        if let Some(f) = self.0.take() {
            f();
        }
    }
}

impl Drop for HeldRelease {
    fn drop(&mut self) {
        if let Some(f) = self.0.take() {
            f();
        }
    }
}

impl std::fmt::Debug for HeldRelease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("HeldRelease")
    }
}

/// A committed sync-mode produce whose response waits for the external broker
/// (wave 3: the response is held, not the DB thread). Released when every key
/// is delivered or after `timeout`, whichever comes first.
#[derive(Debug)]
pub struct SyncHold {
    pub keys: Vec<RowKey>,
    pub timeout: std::time::Duration,
    pub release: HeldRelease,
}

/// Message sent from the DB thread to the network-thread forwarder.
#[derive(Debug)]
pub enum ForwardRequest {
    /// Forward one claimed outbox record.
    Record(ForwardRecord),
    /// Hold a sync-mode produce response until its records are delivered.
    Hold(SyncHold),
}

/// How one forward attempt ended.
#[derive(Debug, Clone, PartialEq)]
pub enum ForwardOutcome {
    /// Delivered at this external offset.
    Delivered(i64),
    /// The delivery really failed (counts against `pg_kafka.shadow_max_retries`).
    Failed(String),
    /// Never handed to the external broker, or failed only as a consequence of
    /// an earlier record's failure (ordering fence, a fatal producer error caused
    /// by another record, shadow mode switched off). Does not count as an
    /// attempt; the row becomes due again behind its partition's head.
    NotAttempted(String),
}

/// Result of a single durable-outbox forward attempt, sent from the network
/// thread back to the DB thread so it can finalize the `kafka.shadow_tracking`
/// row. The `(topic_id, partition_id, local_offset)` triple is the outbox row's
/// primary key.
#[derive(Debug, Clone, PartialEq)]
pub struct ForwardAck {
    /// Local topic id (outbox row key)
    pub topic_id: i32,
    /// Local partition id (outbox row key)
    pub partition_id: i32,
    /// Local offset (outbox row key)
    pub local_offset: i64,
    pub outcome: ForwardOutcome,
}

impl ForwardAck {
    pub fn new(key: RowKey, outcome: ForwardOutcome) -> Self {
        Self {
            topic_id: key.0,
            partition_id: key.1,
            local_offset: key.2,
            outcome,
        }
    }

    pub fn row_key(&self) -> RowKey {
        (self.topic_id, self.partition_id, self.local_offset)
    }
}

#[cfg(test)]
mod forward_record_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[test]
    fn test_forward_record_new() {
        let req = ForwardRecord::new(
            7,
            "test-topic".to_string(),
            0,
            Some(b"key".to_vec()),
            Some(b"value".to_vec()),
            100,
        );
        assert_eq!(req.row_key(), (7, 0, 100));
        assert_eq!(req.topic_name, "test-topic");
        assert_eq!(req.key, Some(b"key".to_vec()));
        assert_eq!(req.value, Some(b"value".to_vec()));
        assert!(req.headers.is_empty());
        assert_eq!(req.timestamp_ms, None);
        assert!(!req.restart);
    }

    #[test]
    fn test_held_release_runs_exactly_once_on_release_or_drop() {
        let n = Arc::new(AtomicUsize::new(0));
        let c = n.clone();
        HeldRelease::new(move || {
            c.fetch_add(1, Ordering::SeqCst);
        })
        .release();
        assert_eq!(n.load(Ordering::SeqCst), 1);

        // A hold that is dropped (lost) still answers the client.
        let c = n.clone();
        drop(HeldRelease::new(move || {
            c.fetch_add(1, Ordering::SeqCst);
        }));
        assert_eq!(n.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn test_forward_ack_row_key() {
        let ack = ForwardAck::new((1, 2, 3), ForwardOutcome::Delivered(9));
        assert_eq!(ack.row_key(), (1, 2, 3));
        assert_eq!(ack.outcome, ForwardOutcome::Delivered(9));
    }
}
