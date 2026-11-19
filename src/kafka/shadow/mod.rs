// Copyright (c) 2026 Robert Trent Jones.
// This file is part of the "Shadow Mode" feature of pg_kafka.
//
// Use of this source code for production purposes is governed by the
// Commercial License found in the LICENSE file in this directory.
// Development and evaluation use is permitted.
//
// GitHub Sponsors: https://github.com/sponsors/RTrentJones

//! Shadow mode: forwarding produced records to an external Kafka cluster
//! (Phase 11), for a gradual migration from pg_kafka to Kafka or a hybrid
//! setup that keeps a local copy.
//!
//! - **Per-topic configuration** (`kafka.shadow_config`): `mode`
//!   (`local_only` | `shadow`), `forward_percentage` (0-100, a deterministic
//!   dial, see [`routing`]), `sync_mode` (`async` | `sync`: whether the produce
//!   response waits for the external ack) and `write_mode` (`dual_write` |
//!   `external_only`: whether local reads are suppressed).
//! - **Durable outbox**: a forwarded record gets a `kafka.shadow_tracking` row
//!   in the produce's own transaction; delivery is at-least-once and retried
//!   up to `pg_kafka.shadow_max_retries`, then dead-lettered.
//! - **Primary only**: a standby never forwards ([`primary`]).
//! - **Global settings**: the `pg_kafka.shadow_*` GUCs (bootstrap servers,
//!   security protocol, SASL, batching, retries), read at runtime so a reload
//!   takes effect without a restart.
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
//!
//! The DB-thread half is [`store`] (with its SQL in [`outbox`]); the
//! network-thread half is [`forwarder_task`] (with the rdkafka producer in
//! [`producer`]). The types below are the messages between them.

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
use std::time::Duration;

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

/// The primary key of a `kafka.shadow_tracking` outbox row (and of the
/// `kafka.messages` row it points at).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RowKey {
    /// Local topic id.
    pub topic_id: i32,
    /// Local partition id.
    pub partition_id: i32,
    /// Local partition offset.
    pub local_offset: i64,
}

impl RowKey {
    pub const fn new(topic_id: i32, partition_id: i32, local_offset: i64) -> Self {
        Self {
            topic_id,
            partition_id,
            local_offset,
        }
    }

    /// The local partition `(topic_id, partition_id)` the row belongs to.
    pub const fn partition(&self) -> (i32, i32) {
        (self.topic_id, self.partition_id)
    }
}

/// Row keys as the three parallel arrays a SQL `unnest($1::int4[],
/// $2::int4[], $3::int8[])` takes.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct KeyColumns {
    pub topic_ids: Vec<i32>,
    pub partition_ids: Vec<i32>,
    pub local_offsets: Vec<i64>,
}

impl<'a> FromIterator<&'a RowKey> for KeyColumns {
    fn from_iter<I: IntoIterator<Item = &'a RowKey>>(keys: I) -> Self {
        let mut columns = Self::default();
        for key in keys {
            columns.topic_ids.push(key.topic_id);
            columns.partition_ids.push(key.partition_id);
            columns.local_offsets.push(key.local_offset);
        }
        columns
    }
}

/// One outbox record the DB thread hands to the network-thread forwarder.
#[derive(Debug, Clone, PartialEq)]
pub struct ForwardRecord {
    /// The outbox row, so the resulting [`ForwardAck`] can be matched back to
    /// it. The external cluster re-partitions by key: the local partition only
    /// orders the records.
    pub row: RowKey,
    /// External topic name to forward to.
    pub topic_name: String,
    /// Message key.
    pub key: Option<Vec<u8>>,
    /// Message value.
    pub value: Option<Vec<u8>>,
    /// Record headers, in order (forwarded as Kafka record headers).
    pub headers: Vec<(String, Vec<u8>)>,
    /// Record timestamp in ms (the producer's CreateTime, or the broker insert
    /// time when the producer sent none — what a local consumer sees).
    pub timestamp_ms: Option<i64>,
    /// True when this is the first undelivered (and not dead-lettered) record of
    /// its local partition: the forwarder may restart that partition's sequence
    /// here after an earlier failure (see `forwarder_task`).
    pub restart: bool,
}

/// Sends a sync-mode produce's buffered response(s) to the client exactly once:
/// when the forwarder calls [`HeldRelease::release`], or when the value is
/// dropped (so a hold that is lost — a full channel, a forwarder that stopped —
/// still answers the client instead of hanging it). The closure runs on whatever
/// thread releases it and therefore must not call pgrx.
pub struct HeldRelease(Option<Box<dyn FnOnce() + Send>>);

impl HeldRelease {
    /// Wrap the closure that sends the held response(s).
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
/// (the response is held, not the DB thread). Released when every key is
/// delivered or after `timeout`, whichever comes first.
#[derive(Debug)]
pub struct SyncHold {
    /// The outbox rows the response waits for.
    pub keys: Vec<RowKey>,
    /// The longest the response waits (`pg_kafka.shadow_sync_timeout_ms`).
    pub timeout: Duration,
    /// Sends the response.
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

/// Result of a single forward attempt, sent from the network thread back to
/// the DB thread so it can finalize the outbox row.
#[derive(Debug, Clone, PartialEq)]
pub struct ForwardAck {
    /// The outbox row.
    pub row: RowKey,
    /// How the attempt ended.
    pub outcome: ForwardOutcome,
}

impl ForwardAck {
    pub fn new(row: RowKey, outcome: ForwardOutcome) -> Self {
        Self { row, outcome }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

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
    fn test_key_columns_keep_the_key_order() {
        let keys = [RowKey::new(1, 0, 5), RowKey::new(2, 3, 4)];
        assert_eq!(
            keys.iter().collect::<KeyColumns>(),
            KeyColumns {
                topic_ids: vec![1, 2],
                partition_ids: vec![0, 3],
                local_offsets: vec![5, 4],
            }
        );
        assert_eq!(RowKey::new(1, 2, 3).partition(), (1, 2));
    }
}
