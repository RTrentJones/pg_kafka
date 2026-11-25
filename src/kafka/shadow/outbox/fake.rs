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
    /// `pg_kafka.shadow_bootstrap_servers` is empty (`forwarding_configured`
    /// is false).
    pub bootstrap_unset: bool,
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
    /// Make `enqueue_pending_txn_records` fail (`Err`).
    pub fail_txn_enqueue: bool,
    /// Make `apply_delivered` fail (`Err`).
    pub fail_apply_delivered: bool,
    /// Make `apply_delivered` raise (panic, as a Postgres ERROR unwinds).
    pub raise_in_apply_delivered: bool,
    /// Make `claim_due_outbox_rows` fail.
    pub fail_claim: bool,
    /// The cap of every claim that ran the full dead-flag sync, in order. (The fake
    /// does not model the `dead` column: its claim filters on `retry_count`.)
    pub dead_flag_syncs: Vec<i64>,
    /// What `current_transaction_id` returns.
    pub current_xid: u32,
    /// Xids `transaction_committed` reports as committed.
    pub committed_xids: std::collections::HashSet<u32>,
}

/// A claimable outbox row: pending, under the retry cap, its message still present.
struct Candidate {
    key: RowKey,
    /// The forwarder holds it already (`OutboxClaim::inflight`).
    in_flight: bool,
    /// It failed and its retry backoff has not elapsed.
    backing_off: bool,
}

impl State {
    /// The claimable rows in key order (the `BTreeMap` order is the claim's
    /// `ORDER BY topic_id, partition_id, local_offset`).
    fn claim_candidates(&self, claim: &OutboxClaim, now: i64) -> Vec<Candidate> {
        let inflight: std::collections::HashSet<RowKey> = claim
            .inflight
            .topic_ids
            .iter()
            .zip(&claim.inflight.partition_ids)
            .zip(&claim.inflight.local_offsets)
            .map(|((&t, &p), &o)| RowKey::new(t, p, o))
            .collect();
        self.tracking
            .iter()
            .filter(|(k, r)| {
                r.external_offset.is_none()
                    && r.retry_count < claim.max_retries
                    && self.messages.contains_key(k)
            })
            .map(|(k, r)| Candidate {
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
            .collect()
    }

    /// Open a forward attempt on a claimed row and stamp `forwarded_at`. A row whose
    /// previous attempt is still open lost its ack, and that attempt counts as one
    /// failure.
    fn open_attempt(&mut self, key: RowKey, now: i64) {
        let row = self
            .tracking
            .get_mut(&key)
            .expect("a claimed row is a tracking row");
        if row.attempt_open {
            row.retry_count += 1;
        }
        row.attempt_open = true;
        row.forwarded_at = Some(now);
    }

    /// The record to forward for a claimed row, under the topic's external name (the
    /// local name when none is configured); `None` when the topic is gone.
    fn forward_record(&self, key: RowKey, restart: bool) -> Option<ForwardRecord> {
        let topic_name = self.topics.get(&key.topic_id).cloned()?;
        let external = self
            .external_names
            .get(&key.topic_id)
            .cloned()
            .flatten()
            .unwrap_or(topic_name);
        let m = &self.messages[&key];
        Some(ForwardRecord {
            row: key,
            topic_name: external,
            key: m.key.clone(),
            value: m.value.clone(),
            headers: m.headers.clone(),
            timestamp_ms: m.timestamp_ms,
            restart,
        })
    }
}

/// The candidates due now, at most `batch_limit`, each with its `restart` flag
/// (it is its partition's lowest candidate), in key order, and the per-partition
/// share. Rows in flight or backing off are not due, and a backing-off row that
/// is not in flight blocks every later row of its partition, so a retry is never
/// overtaken by a later record. Each partition with a due row contributes its
/// lowest due rows, at most [`per_partition_claim_cap`] over the partitions with
/// a due row, and the rows are taken round-robin across partitions up to the
/// limit.
fn due_rows(candidates: &[Candidate], batch_limit: i64) -> (Vec<(RowKey, bool)>, i64) {
    let mut head: HashMap<(i32, i32), i64> = HashMap::new();
    let mut blocked: HashMap<(i32, i32), i64> = HashMap::new();
    for c in candidates {
        let part = c.key.partition();
        head.entry(part).or_insert(c.key.local_offset);
        if c.backing_off && !c.in_flight {
            blocked.entry(part).or_insert(c.key.local_offset);
        }
    }
    let eligible: Vec<&Candidate> = candidates
        .iter()
        .filter(|c| {
            !c.in_flight
                && !c.backing_off
                && blocked
                    .get(&c.key.partition())
                    .is_none_or(|&b| c.key.local_offset < b)
        })
        .collect();
    // The claim's `due_parts`: only partitions with a due row share the batch.
    let due_partitions = eligible
        .iter()
        .map(|c| c.key.partition())
        .collect::<std::collections::HashSet<_>>()
        .len();
    let share = per_partition_claim_cap(batch_limit, due_partitions);
    let cap = usize::try_from(share).unwrap_or(0);
    // Per partition, its first `cap` due rows, ranked from 0.
    let mut taken: HashMap<(i32, i32), usize> = HashMap::new();
    let mut ranked: Vec<(usize, RowKey)> = Vec::new();
    for c in eligible {
        let n = taken.entry(c.key.partition()).or_default();
        if *n < cap {
            ranked.push((*n, c.key));
            *n += 1;
        }
    }
    // ORDER BY rn, topic_id, partition_id LIMIT batch_limit, then key order.
    ranked.sort_by_key(|&(rank, key)| (rank, key.partition()));
    ranked.truncate(usize::try_from(batch_limit).unwrap_or(0));
    let mut due: Vec<(RowKey, bool)> = ranked
        .into_iter()
        .map(|(_, key)| (key, head[&key.partition()] == key.local_offset))
        .collect();
    due.sort_by_key(|&(key, _)| key);
    (due, share)
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

    fn forwarding_configured(&self) -> bool {
        !self.state().bootstrap_unset
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

    fn enqueue_pending_txn_records(
        &self,
        producer_id: i64,
        producer_epoch: i16,
        forward_all: &[i32],
        sampled: &[(i32, u8)],
    ) -> Result<HashMap<(i32, i32), u64>> {
        use crate::kafka::shadow::routing::{make_forward_decision, ForwardDecision};
        let mut s = self.state();
        if s.fail_txn_enqueue {
            return Err(KafkaError::Internal("txn enqueue failed".to_string()));
        }
        let pending: Vec<(RowKey, Option<Vec<u8>>)> = s
            .messages
            .iter()
            .filter(|(_, m)| m.txn == Some((producer_id, producer_epoch)) && m.txn_pending)
            .map(|(&row, m)| (row, m.key.clone()))
            .collect();
        let mut skipped: HashMap<(i32, i32), u64> = HashMap::new();
        for (row, key) in pending {
            let forward = if forward_all.contains(&row.topic_id) {
                true
            } else if let Some(&(_, pct)) = sampled.iter().find(|(t, _)| *t == row.topic_id) {
                make_forward_decision(key.as_deref(), row.local_offset, pct)
                    == ForwardDecision::Forward
            } else {
                continue;
            };
            if forward {
                s.tracking.entry(row).or_default();
            } else {
                *skipped.entry(row.partition()).or_default() += 1;
            }
        }
        Ok(skipped)
    }

    fn claim_due_outbox_rows(&self, claim: &OutboxClaim) -> Result<ClaimedRows> {
        let mut s = self.state();
        if s.fail_claim {
            return Err(KafkaError::Internal("claim failed".to_string()));
        }
        let now = s.now_ms;
        let mut out = ClaimedRows::default();
        if claim.sync_dead_flags {
            s.dead_flag_syncs.push(claim.max_retries);
            out.dead_flags_synced = true;
        }
        let candidates = s.claim_candidates(claim, now);
        let (due, share) = due_rows(&candidates, claim.batch_limit);
        for (key, restart) in due {
            s.open_attempt(key, now);
            // The JOIN with kafka.topics drops a row whose topic is gone.
            if let Some(record) = s.forward_record(key, restart) {
                out.records.push(record);
                out.partition_share = share;
            }
        }
        Ok(out)
    }

    fn apply_delivered(&self, rows: &[(RowKey, i64)], metrics: bool) -> Result<usize> {
        let mut s = self.state();
        if s.fail_apply_delivered {
            return Err(KafkaError::Internal("apply_delivered failed".to_string()));
        }
        if s.raise_in_apply_delivered {
            drop(s);
            panic!("ERROR: apply_delivered raised");
        }
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
