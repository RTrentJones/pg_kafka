// Copyright (c) 2026 Robert Trent Jones.
// This file is part of the "Shadow Mode" feature of pg_kafka.
//
// Use of this source code for production purposes is governed by the
// Commercial License found in the LICENSE file in this directory.
// Development and evaluation use is permitted.
//
// GitHub Sponsors: https://github.com/sponsors/RTrentJones

//! In-memory [`ShadowDb`] for unit tests. It models the semantics the SPI
//! statements implement (see each method), with a manual clock in place of
//! `NOW()`.

use super::*;
use crate::kafka::error::KafkaError;
use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;

/// A `kafka.messages` row as far as shadow mode cares.
#[derive(Clone, Debug, Default)]
pub struct FakeMessage {
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<(String, Vec<u8>)>,
    pub timestamp_ms: Option<i64>,
    /// `(producer_id, producer_epoch)` and whether the row is still pending.
    pub txn: Option<(i64, i16)>,
    pub txn_pending: bool,
}

/// A `kafka.shadow_tracking` row.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct FakeTrackingRow {
    pub external_offset: Option<i64>,
    pub retry_count: i64,
    /// Last attempt, in fake-clock ms.
    pub forwarded_at: Option<i64>,
    pub error_message: Option<String>,
    pub attempt_open: bool,
}

/// A `kafka.shadow_metrics` row.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct FakeMetrics {
    pub messages_forwarded: i64,
    pub messages_failed: i64,
    pub messages_skipped: i64,
    pub last_forwarded_offset: Option<i64>,
}

#[derive(Default)]
pub struct State {
    pub enabled: bool,
    pub strict: bool,
    pub settings: OutboxSettings,
    pub now_ms: i64,
    /// topic_id -> local topic name
    pub topics: HashMap<i32, String>,
    /// topic_id -> shadow_config.external_topic_name
    pub external_names: HashMap<i32, Option<String>>,
    pub messages: BTreeMap<RowKey, FakeMessage>,
    pub tracking: BTreeMap<RowKey, FakeTrackingRow>,
    pub metrics: HashMap<(i32, i32), FakeMetrics>,
    /// `None` makes `load_topic_configs` fail.
    pub configs: Option<Vec<TopicShadowConfig>>,
    /// Make `pending_txn_records` fail (None).
    pub fail_pending_txn_lookup: bool,
    /// Make `claim_due_outbox_rows` fail.
    pub fail_claim: bool,
    /// What `current_transaction_id` returns.
    pub current_xid: u32,
    /// Xids `transaction_committed` reports as committed.
    pub committed_xids: std::collections::HashSet<u32>,
}

/// In-memory [`ShadowDb`].
#[derive(Default)]
pub struct InMemoryShadowDb {
    pub state: Mutex<State>,
}

impl InMemoryShadowDb {
    /// Shadow mode enabled, no strict, empty tables.
    pub fn enabled() -> Self {
        let db = Self::default();
        db.state().enabled = true;
        db.state().configs = Some(Vec::new());
        db
    }

    pub fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap()
    }

    pub fn add_topic(&self, topic_id: i32, name: &str, external_name: Option<&str>) {
        let mut s = self.state();
        s.topics.insert(topic_id, name.to_string());
        s.external_names
            .insert(topic_id, external_name.map(str::to_string));
    }

    pub fn add_message(&self, key: RowKey, record_key: &[u8], value: &[u8]) {
        self.state().messages.insert(
            key,
            FakeMessage {
                key: Some(record_key.to_vec()),
                value: Some(value.to_vec()),
                ..Default::default()
            },
        );
    }

    pub fn row(&self, key: RowKey) -> Option<FakeTrackingRow> {
        self.state().tracking.get(&key).cloned()
    }

    pub fn advance_ms(&self, ms: i64) {
        self.state().now_ms += ms;
    }
}

impl ShadowDb for InMemoryShadowDb {
    fn shadow_mode_enabled(&self) -> bool {
        self.state().enabled
    }

    fn external_primary_strict(&self) -> bool {
        self.state().strict
    }

    fn settings(&self) -> OutboxSettings {
        self.state().settings.clone()
    }

    fn topic_has_dead_letters(&self, topic_id: i32, max_retries: i64) -> Result<bool> {
        Ok(self.state().tracking.iter().any(|(k, r)| {
            k.topic_id == topic_id && r.external_offset.is_none() && r.retry_count >= max_retries
        }))
    }

    fn insert_outbox_rows(&self, rows: &[RowKey]) -> Result<()> {
        let mut s = self.state();
        for &key in rows {
            if s.messages.contains_key(&key) {
                s.tracking.entry(key).or_default();
            }
        }
        Ok(())
    }

    fn record_skipped(&self, topic_id: i32, partition_id: i32, n: i64) -> Result<()> {
        self.state()
            .metrics
            .entry((topic_id, partition_id))
            .or_default()
            .messages_skipped += n;
        Ok(())
    }

    fn pending_txn_records(
        &self,
        producer_id: i64,
        producer_epoch: i16,
    ) -> Option<Vec<PendingForwardRecord>> {
        let s = self.state();
        if s.fail_pending_txn_lookup {
            return None;
        }
        Some(
            s.messages
                .iter()
                .filter(|(_, m)| m.txn == Some((producer_id, producer_epoch)) && m.txn_pending)
                .map(|(&row, m)| PendingForwardRecord {
                    row,
                    key: m.key.clone(),
                })
                .collect(),
        )
    }

    fn claim_due_outbox_rows(&self, claim: &OutboxClaim) -> Result<Vec<ForwardRecord>> {
        // cand: pending, under the cap, message exists (BTreeMap = key order).
        struct Cand {
            key: RowKey,
            in_flight: bool,
            backing_off: bool,
        }
        let mut s = self.state();
        if s.fail_claim {
            return Err(KafkaError::Internal("claim failed".to_string()));
        }
        let now = s.now_ms;
        let inflight: std::collections::HashSet<RowKey> = claim
            .inflight
            .topic_ids
            .iter()
            .zip(&claim.inflight.partition_ids)
            .zip(&claim.inflight.local_offsets)
            .map(|((&t, &p), &o)| RowKey::new(t, p, o))
            .collect();
        let cands: Vec<Cand> = s
            .tracking
            .iter()
            .filter(|(k, r)| {
                r.external_offset.is_none()
                    && r.retry_count < claim.max_retries
                    && s.messages.contains_key(k)
            })
            .map(|(k, r)| Cand {
                key: *k,
                in_flight: inflight.contains(k),
                backing_off: !r.attempt_open
                    && r.retry_count > 0
                    && r.forwarded_at.is_some_and(|at| {
                        at > now
                            - retry_backoff_ms(
                                r.retry_count,
                                claim.backoff_base_ms,
                                claim.backoff_max_ms,
                            )
                    }),
            })
            .collect();
        let mut head: HashMap<(i32, i32), i64> = HashMap::new();
        let mut blocked: HashMap<(i32, i32), i64> = HashMap::new();
        for c in &cands {
            let part = c.key.partition();
            head.entry(part).or_insert(c.key.local_offset);
            if c.backing_off && !c.in_flight {
                blocked.entry(part).or_insert(c.key.local_offset);
            }
        }
        let due: Vec<(RowKey, bool)> = cands
            .iter()
            .filter(|c| {
                !c.in_flight
                    && !c.backing_off
                    && blocked
                        .get(&c.key.partition())
                        .is_none_or(|&b| c.key.local_offset < b)
            })
            .map(|c| (c.key, head[&c.key.partition()] == c.key.local_offset))
            .take(usize::try_from(claim.batch_limit).unwrap_or(0))
            .collect();
        let mut out = Vec::new();
        for (key, restart) in due {
            let row = s.tracking.get_mut(&key).unwrap();
            if row.attempt_open {
                row.retry_count += 1;
            }
            row.attempt_open = true;
            row.forwarded_at = Some(now);
            let Some(topic_name) = s.topics.get(&key.topic_id).cloned() else {
                continue; // JOIN kafka.topics drops it
            };
            let external = s
                .external_names
                .get(&key.topic_id)
                .cloned()
                .flatten()
                .unwrap_or(topic_name);
            let m = &s.messages[&key];
            out.push(ForwardRecord {
                row: key,
                topic_name: external,
                key: m.key.clone(),
                value: m.value.clone(),
                headers: m.headers.clone(),
                timestamp_ms: m.timestamp_ms,
                restart,
            });
        }
        Ok(out)
    }

    fn apply_delivered(&self, rows: &[(RowKey, i64)], metrics: bool) -> Result<usize> {
        let mut s = self.state();
        let now = s.now_ms;
        let mut n = 0;
        for &(key, external_offset) in rows {
            let Some(row) = s.tracking.get_mut(&key) else {
                continue;
            };
            if row.external_offset.is_some() {
                continue;
            }
            row.external_offset = Some(external_offset);
            row.forwarded_at = Some(now);
            row.error_message = None;
            row.attempt_open = false;
            n += 1;
            if metrics {
                let m = s.metrics.entry(key.partition()).or_default();
                m.messages_forwarded += 1;
                m.last_forwarded_offset = Some(
                    m.last_forwarded_offset
                        .unwrap_or(i64::MIN)
                        .max(key.local_offset),
                );
            }
        }
        Ok(n)
    }

    fn apply_failed(
        &self,
        rows: &[(RowKey, String)],
        max_retries: i64,
        metrics: bool,
    ) -> Result<FailedApplied> {
        let mut s = self.state();
        let now = s.now_ms;
        let mut out = FailedApplied::default();
        for (key, error) in rows {
            let Some(row) = s.tracking.get_mut(key) else {
                continue;
            };
            if row.external_offset.is_some() {
                continue;
            }
            row.retry_count += 1;
            row.error_message = Some(error.clone());
            row.forwarded_at = Some(now);
            row.attempt_open = false;
            out.transitioned += 1;
            if row.retry_count >= max_retries {
                out.dead_lettered.push((*key, error.clone()));
            }
            if metrics {
                s.metrics
                    .entry(key.partition())
                    .or_default()
                    .messages_failed += 1;
            }
        }
        Ok(out)
    }

    fn apply_not_attempted(&self, rows: &[RowKey]) -> Result<()> {
        let mut s = self.state();
        for key in rows {
            if let Some(row) = s.tracking.get_mut(key) {
                if row.external_offset.is_none() {
                    row.attempt_open = false;
                }
            }
        }
        Ok(())
    }

    fn load_topic_configs(&self) -> Option<Vec<TopicShadowConfig>> {
        self.state().configs.clone()
    }

    fn current_transaction_id(&self) -> u32 {
        self.state().current_xid
    }

    fn transaction_committed(&self, xid: u32) -> bool {
        xid != 0 && self.state().committed_xids.contains(&xid)
    }
}

/// Backoff before re-attempting a row that failed `retry_count` times:
/// `base * 2^(retry_count-1)`, capped at `max`. Zero for a row that never failed.
fn retry_backoff_ms(retry_count: i64, base: i64, max: i64) -> i64 {
    if retry_count <= 0 {
        return 0;
    }
    let shift = u32::try_from((retry_count - 1).min(30)).expect("0..=30 fits in u32");
    base.saturating_mul(1i64 << shift).min(max)
}

mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_from_the_base_and_is_capped() {
        assert_eq!(retry_backoff_ms(0, 100, 30_000), 0);
        assert_eq!(retry_backoff_ms(1, 100, 30_000), 100);
        assert_eq!(retry_backoff_ms(2, 100, 30_000), 200);
        assert_eq!(retry_backoff_ms(4, 100, 30_000), 800);
        assert_eq!(retry_backoff_ms(20, 100, 30_000), 30_000);
        assert_eq!(retry_backoff_ms(1000, 60_000, 30_000), 30_000);
    }
}
