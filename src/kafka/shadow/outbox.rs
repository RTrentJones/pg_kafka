// Copyright (c) 2026 Robert Trent Jones.
// This file is part of the "Shadow Mode" feature of pg_kafka.
//
// Use of this source code for production purposes is governed by the
// Commercial License found in the LICENSE file in this directory.
// Development and evaluation use is permitted.
//
// GitHub Sponsors: https://github.com/sponsors/RTrentJones

//! The database side of shadow mode, behind a small trait.
//!
//! `ShadowStore` keeps the outbox *logic* (what to enqueue, which claimed rows
//! to dispatch, how an ack finalizes a row, the sync wait, the config-cache
//! swap). Every SQL statement and GUC read it needs goes through [`ShadowDb`]:
//!
//! - [`SpiShadowDb`] is the production implementation. Its statements are the
//!   ones `ShadowStore` used to run inline, unchanged and in the same order.
//! - `InMemoryShadowDb` (test builds only) models the `kafka.shadow_tracking`
//!   outbox, the `kafka.messages` rows it points at, and `kafka.shadow_metrics`
//!   in memory, so the claim/ack/retry/dead-letter behaviour is unit-tested.

use super::config::TopicShadowConfig;
use super::ForwardRequest;
use crate::kafka::error::Result;

/// A transactional record's outbox pointer: the row key plus the record key
/// (used only for percentage routing at enqueue time). The payload is re-read
/// from `kafka.messages` by the forwarder.
#[derive(Clone, Debug, PartialEq)]
pub struct PendingForwardRecord {
    /// Topic ID (outbox row key + per-topic config lookup)
    pub topic_id: i32,
    /// Partition ID (outbox row key)
    pub partition_id: i32,
    /// Local offset the record was committed at (outbox row key)
    pub offset: i64,
    /// Record key — used only for percentage routing at enqueue time
    pub key: Option<Vec<u8>>,
}

/// Parameters of one outbox claim (see [`ShadowDb::claim_due_outbox_rows`]).
#[derive(Clone, Debug, Default)]
pub struct OutboxClaim {
    /// A claimed-but-unacked row becomes due again after this many ms.
    pub retry_interval_ms: i64,
    /// Max rows claimed per call.
    pub batch_limit: i64,
    /// Rows with `retry_count >= max_retries` are dead letters and never claimed.
    pub max_retries: i64,
    /// In-flight row keys (topic ids, partition ids, offsets; parallel arrays),
    /// excluded from the claim.
    pub inflight_topics: Vec<i32>,
    pub inflight_partitions: Vec<i32>,
    pub inflight_offsets: Vec<i64>,
}

/// Every database/GUC access shadow mode makes.
pub trait ShadowDb {
    /// `pg_kafka.shadow_mode_enabled`.
    fn shadow_mode_enabled(&self) -> bool;

    /// `pg_kafka.shadow_external_primary_strict`.
    fn external_primary_strict(&self) -> bool;

    /// Whether the topic has a dead-lettered forward (`external_offset` NULL,
    /// `retry_count >= max_retries`).
    fn topic_has_dead_letters(&self, topic_id: i32, max_retries: i64) -> Result<bool>;

    /// Insert pending outbox rows `(topic_id, partition_id, local_offset)` in the
    /// current transaction, each only if its `kafka.messages` row exists;
    /// existing rows are left alone.
    fn insert_outbox_rows(&self, rows: &[(i32, i32, i64)]) -> Result<()>;

    /// The pending (uncommitted) records of a producer's transaction, read from
    /// `kafka.messages`, ordered by row key. `None` if the lookup failed.
    fn pending_txn_records(
        &self,
        producer_id: i64,
        producer_epoch: i16,
    ) -> Option<Vec<PendingForwardRecord>>;

    /// Claim due outbox rows and return a forward request (payload + external
    /// topic name) for each. A row is due when it is pending, under the retry
    /// cap, not in flight, its message still exists, and it was never attempted
    /// or last attempted over `retry_interval_ms` ago. Claiming stamps the
    /// attempt time and bumps `retry_count`.
    fn claim_due_outbox_rows(&self, claim: &OutboxClaim) -> Result<Vec<ForwardRequest>>;

    /// Mark a pending row delivered at `external_offset` and count it in
    /// `kafka.shadow_metrics`. Returns whether the row transitioned (false for a
    /// duplicate ack or an unknown row).
    fn mark_forward_succeeded(
        &self,
        topic_id: i32,
        partition_id: i32,
        local_offset: i64,
        external_offset: i64,
    ) -> Result<bool>;

    /// Record a failed attempt's error on a pending row and count it in
    /// `kafka.shadow_metrics`. Returns the row's `retry_count` if it
    /// transitioned, `None` for a delivered or unknown row.
    fn mark_forward_failed(
        &self,
        topic_id: i32,
        partition_id: i32,
        local_offset: i64,
        error_message: &str,
    ) -> Result<Option<i32>>;

    /// Every row of `kafka.shadow_config`, parsed. `None` if the query failed.
    fn load_topic_configs(&self) -> Option<Vec<TopicShadowConfig>>;

    /// The xid of the current (sub)transaction, or 0 if it has none. Taken right
    /// after a sync-mode produce writes its outbox rows, so it identifies the
    /// innermost subtransaction those writes belong to.
    fn current_transaction_id(&self) -> u32;

    /// Whether `xid` committed — for a subtransaction xid, whether it and every
    /// enclosing transaction committed. False for 0, an aborted xid, or one
    /// still in progress. Called after the worker's commit.
    fn transaction_committed(&self, xid: u32) -> bool;
}

/// Production [`ShadowDb`]: SPI and the real GUCs.
#[derive(Debug, Default, Clone, Copy)]
pub struct SpiShadowDb;

impl ShadowDb for SpiShadowDb {
    fn shadow_mode_enabled(&self) -> bool {
        crate::config::SHADOW_MODE_ENABLED.get()
    }

    fn external_primary_strict(&self) -> bool {
        crate::config::SHADOW_EXTERNAL_PRIMARY_STRICT.get()
    }

    fn topic_has_dead_letters(&self, topic_id: i32, max_retries: i64) -> Result<bool> {
        use pgrx::prelude::*;
        Spi::connect(|client| {
            let table = client.select(
                "SELECT 1 FROM kafka.shadow_tracking \
                 WHERE topic_id = $1 AND external_offset IS NULL AND retry_count >= $2 \
                 LIMIT 1",
                Some(1),
                &[topic_id.into(), max_retries.into()],
            )?;
            Ok(!table.is_empty())
        })
    }

    fn insert_outbox_rows(&self, rows: &[(i32, i32, i64)]) -> Result<()> {
        use pgrx::prelude::*;
        Spi::connect_mut(|client| {
            for &(topic_id, partition_id, local_offset) in rows {
                // B1: only enqueue an outbox pointer when the backing
                // kafka.messages row actually exists. The live-produce enqueue
                // runs in the same subtransaction as the message insert, so the
                // row is always visible and this is a no-op there. On the txn
                // commit path it drops orphan pointers left by a transactional
                // produce that buffered offsets in RAM but whose message rows were
                // rolled back (a panic mid-txn) — which would otherwise be
                // never-claimed, never-cleaned rows. Symmetric with the poll's own
                // EXISTS guard in claim_due_outbox_rows.
                client.update(
                    "INSERT INTO kafka.shadow_tracking (topic_id, partition_id, local_offset) \
                     SELECT $1, $2, $3 \
                     WHERE EXISTS ( \
                     SELECT 1 FROM kafka.messages m \
                     WHERE m.topic_id = $1 AND m.partition_id = $2 \
                     AND m.partition_offset = $3 \
                     ) \
                     ON CONFLICT (topic_id, partition_id, local_offset) DO NOTHING",
                    None,
                    &[topic_id.into(), partition_id.into(), local_offset.into()],
                )?;
            }
            Ok(())
        })
    }

    fn pending_txn_records(
        &self,
        producer_id: i64,
        producer_epoch: i16,
    ) -> Option<Vec<PendingForwardRecord>> {
        use pgrx::prelude::*;
        let result: std::result::Result<Vec<PendingForwardRecord>, pgrx::spi::SpiError> =
            Spi::connect(|client| {
                let table = client.select(
                    "SELECT topic_id, partition_id, partition_offset, key
                     FROM kafka.messages
                     WHERE producer_id = $1 AND producer_epoch = $2 AND txn_state = 'pending'
                     ORDER BY topic_id, partition_id, partition_offset",
                    None,
                    &[producer_id.into(), producer_epoch.into()],
                )?;
                let mut records = Vec::new();
                for row in table {
                    records.push(PendingForwardRecord {
                        topic_id: row.get_by_name("topic_id")?.unwrap_or(0),
                        partition_id: row.get_by_name("partition_id")?.unwrap_or(0),
                        offset: row.get_by_name("partition_offset")?.unwrap_or(0),
                        key: row.get_by_name::<Vec<u8>, _>("key")?,
                    });
                }
                Ok(records)
            });
        match result {
            Ok(records) => Some(records),
            Err(e) => {
                tracing::warn!("Shadow: failed to read pending txn records: {:?}", e);
                None
            }
        }
    }

    fn claim_due_outbox_rows(&self, claim: &OutboxClaim) -> Result<Vec<ForwardRequest>> {
        use pgrx::prelude::*;
        Spi::connect_mut(|client| {
            let table = client.update(
                r#"
                WITH due AS (
                    SELECT st.topic_id, st.partition_id, st.local_offset
                    FROM kafka.shadow_tracking st
                    WHERE st.external_offset IS NULL
                      AND st.retry_count < $3
                      AND (st.forwarded_at IS NULL
                           OR st.forwarded_at < NOW() - ($1 || ' milliseconds')::interval)
                      AND NOT EXISTS (
                          SELECT 1
                          FROM unnest($4::int4[], $5::int4[], $6::int8[]) AS inf(t, p, o)
                          WHERE inf.t = st.topic_id
                            AND inf.p = st.partition_id
                            AND inf.o = st.local_offset
                      )
                      AND EXISTS (
                          SELECT 1 FROM kafka.messages m
                          WHERE m.topic_id = st.topic_id
                            AND m.partition_id = st.partition_id
                            AND m.partition_offset = st.local_offset
                      )
                    ORDER BY st.topic_id, st.partition_id, st.local_offset
                    LIMIT $2
                ),
                claimed AS (
                    -- RV-10: count each claim as a delivery attempt. Previously
                    -- retry_count was bumped only when a *failure* ack came back, so a
                    -- forward whose ack was dropped (RA-6 backpressure drop) left
                    -- retry_count unchanged and the row was re-claimed forever,
                    -- bypassing the `retry_count < $3` cap (re-forward livelock).
                    -- Bumping on claim bounds every re-forward loop — dropped or
                    -- failed — to $3 attempts before the row is dead-lettered.
                    UPDATE kafka.shadow_tracking st
                    SET forwarded_at = NOW(), retry_count = st.retry_count + 1
                    FROM due
                    WHERE st.topic_id = due.topic_id
                      AND st.partition_id = due.partition_id
                      AND st.local_offset = due.local_offset
                    RETURNING st.topic_id, st.partition_id, st.local_offset
                )
                SELECT c.topic_id    AS topic_id,
                       c.partition_id AS partition_id,
                       c.local_offset AS local_offset,
                       m.key          AS msg_key,
                       m.value        AS msg_value,
                       COALESCE(sc.external_topic_name, t.name) AS external_topic
                FROM claimed c
                JOIN kafka.topics t ON t.id = c.topic_id
                LEFT JOIN kafka.shadow_config sc ON sc.topic_id = c.topic_id
                JOIN kafka.messages m
                  ON m.topic_id = c.topic_id
                 AND m.partition_id = c.partition_id
                 AND m.partition_offset = c.local_offset
                "#,
                None,
                &[
                    claim.retry_interval_ms.into(),
                    claim.batch_limit.into(),
                    claim.max_retries.into(),
                    claim.inflight_topics.clone().into(),
                    claim.inflight_partitions.clone().into(),
                    claim.inflight_offsets.clone().into(),
                ],
            )?;

            let mut reqs: Vec<ForwardRequest> = Vec::new();
            for row in table {
                let topic_id: i32 = row.get_by_name("topic_id")?.unwrap_or(0);
                let partition_id: i32 = row.get_by_name("partition_id")?.unwrap_or(0);
                let local_offset: i64 = row.get_by_name("local_offset")?.unwrap_or(0);
                let key: Option<Vec<u8>> = row.get_by_name("msg_key")?;
                let value: Option<Vec<u8>> = row.get_by_name("msg_value")?;
                let external_topic: String = row.get_by_name("external_topic")?.unwrap_or_default();
                reqs.push(ForwardRequest::new(
                    topic_id,
                    external_topic,
                    partition_id,
                    key,
                    value,
                    local_offset,
                ));
            }
            Ok(reqs)
        })
    }

    fn mark_forward_succeeded(
        &self,
        topic_id: i32,
        partition_id: i32,
        local_offset: i64,
        external_offset: i64,
    ) -> Result<bool> {
        use pgrx::prelude::*;
        // SH-7: the metric is bumped in the SAME statement that flips the row,
        // guarded by `external_offset IS NULL`, so a duplicate ack (e.g. a sync
        // send and a poll resend both confirming) updates nothing and counts
        // nothing — exactly-once per row. `RETURNING 1` reports whether the row
        // actually transitioned.
        Spi::connect_mut(|client| {
            let table = client.update(
                "WITH done AS ( \
                     UPDATE kafka.shadow_tracking \
                     SET external_offset = $4, forwarded_at = NOW(), error_message = NULL \
                     WHERE topic_id = $1 AND partition_id = $2 AND local_offset = $3 \
                       AND external_offset IS NULL \
                     RETURNING topic_id, partition_id, local_offset \
                 ), \
                 bumped AS ( \
                     INSERT INTO kafka.shadow_metrics \
                         (topic_id, partition_id, messages_forwarded, last_forwarded_offset, last_forwarded_at) \
                     SELECT topic_id, partition_id, 1, local_offset, NOW() FROM done \
                     ON CONFLICT (topic_id, partition_id) DO UPDATE SET \
                        messages_forwarded = kafka.shadow_metrics.messages_forwarded + 1, \
                        last_forwarded_offset = GREATEST(kafka.shadow_metrics.last_forwarded_offset, EXCLUDED.last_forwarded_offset), \
                        last_forwarded_at = NOW() \
                 ) \
                 SELECT 1 FROM done",
                None,
                &[
                    topic_id.into(),
                    partition_id.into(),
                    local_offset.into(),
                    external_offset.into(),
                ],
            )?;
            Ok(table.into_iter().count() > 0)
        })
    }

    fn mark_forward_failed(
        &self,
        topic_id: i32,
        partition_id: i32,
        local_offset: i64,
        error_message: &str,
    ) -> Result<Option<i32>> {
        use pgrx::prelude::*;
        Spi::connect_mut(|client| {
            let table = client.update(
                // RV-10: retry_count is bumped on claim (see claim_due_outbox_rows),
                // so the failure ack only records the error — incrementing here too
                // would double-count and halve the effective retry budget. RETURNING
                // retry_count still reports the claim-bumped value for the
                // dead-letter check.
                "WITH failed AS ( \
                     UPDATE kafka.shadow_tracking \
                     SET error_message = $4 \
                     WHERE topic_id = $1 AND partition_id = $2 AND local_offset = $3 \
                       AND external_offset IS NULL \
                     RETURNING topic_id, partition_id, retry_count \
                 ), \
                 bumped AS ( \
                     INSERT INTO kafka.shadow_metrics (topic_id, partition_id, messages_failed) \
                     SELECT topic_id, partition_id, 1 FROM failed \
                     ON CONFLICT (topic_id, partition_id) DO UPDATE SET \
                        messages_failed = kafka.shadow_metrics.messages_failed + 1 \
                 ) \
                 SELECT retry_count FROM failed",
                None,
                &[
                    topic_id.into(),
                    partition_id.into(),
                    local_offset.into(),
                    error_message.to_string().into(),
                ],
            )?;
            let mut retry_count = None;
            for row in table {
                retry_count = Some(row.get_by_name::<i32, _>("retry_count")?.unwrap_or(0));
            }
            Ok(retry_count)
        })
    }

    fn load_topic_configs(&self) -> Option<Vec<TopicShadowConfig>> {
        use super::config::{ShadowMode, SyncMode, WriteMode};
        use pgrx::prelude::*;

        let query = r#"
            SELECT sc.topic_id, t.name, sc.mode, sc.forward_percentage,
                   sc.external_topic_name, sc.sync_mode,
                   COALESCE(sc.write_mode, 'dual_write') as write_mode
            FROM kafka.shadow_config sc
            JOIN kafka.topics t ON sc.topic_id = t.id
        "#;

        let mut configs: Vec<TopicShadowConfig> = Vec::new();
        let mut query_ok = false;

        Spi::connect(|client| {
            let result = client.select(query, None, &[]);

            if let Ok(table) = result {
                query_ok = true;
                for row in table {
                    // Use named columns for clarity and safety
                    let topic_id: i32 = row.get_by_name("topic_id").unwrap_or(Some(0)).unwrap_or(0);
                    let topic_name: String = row
                        .get_by_name("name")
                        .unwrap_or(Some(String::new()))
                        .unwrap_or_default();
                    let mode_str: String = row
                        .get_by_name("mode")
                        .unwrap_or(Some("local_only".to_string()))
                        .unwrap_or_default();
                    let forward_percentage: i32 = row
                        .get_by_name("forward_percentage")
                        .unwrap_or(Some(0))
                        .unwrap_or(0);
                    let external_topic_name: Option<String> =
                        row.get_by_name("external_topic_name").unwrap_or(None);
                    let sync_mode_str: String = row
                        .get_by_name("sync_mode")
                        .unwrap_or(Some("sync".to_string()))
                        .unwrap_or_default();
                    let write_mode_str: String = row
                        .get_by_name("write_mode")
                        .unwrap_or(Some("dual_write".to_string()))
                        .unwrap_or_default();

                    configs.push(TopicShadowConfig {
                        topic_id,
                        topic_name,
                        mode: ShadowMode::parse(&mode_str),
                        forward_percentage: forward_percentage.clamp(0, 100) as u8,
                        external_topic_name,
                        sync_mode: SyncMode::parse(&sync_mode_str),
                        write_mode: WriteMode::parse(&write_mode_str),
                    });
                }
            }
        });

        query_ok.then_some(configs)
    }

    fn current_transaction_id(&self) -> u32 {
        // SAFETY: reads the backend's own transaction state; no allocation.
        unsafe { pgrx::pg_sys::GetCurrentTransactionIdIfAny() }.into_inner()
    }

    fn transaction_committed(&self, xid: u32) -> bool {
        if xid == 0 {
            return false;
        }
        // SAFETY: a clog lookup; valid outside a transaction. The worker calls
        // this after its own commit, so the xid is never still in progress here.
        unsafe {
            pgrx::pg_sys::TransactionIdDidCommit(pgrx::pg_sys::TransactionId::from_inner(xid))
        }
    }
}

#[cfg(test)]
pub(crate) mod fake {
    //! In-memory [`ShadowDb`] for unit tests. It models the semantics the SPI
    //! statements implement (see each method), with a manual clock in place of
    //! `NOW()`.

    use super::*;
    use crate::kafka::error::KafkaError;
    use std::collections::{BTreeMap, HashMap};
    use std::sync::Mutex;

    /// Row key: (topic_id, partition_id, local offset).
    pub type RowKey = (i32, i32, i64);

    /// A `kafka.messages` row as far as shadow mode cares.
    #[derive(Clone, Debug, Default)]
    pub struct FakeMessage {
        pub key: Option<Vec<u8>>,
        pub value: Option<Vec<u8>>,
        /// `(producer_id, producer_epoch)` and whether the row is still pending.
        pub txn: Option<(i64, i16)>,
        pub txn_pending: bool,
    }

    /// A `kafka.shadow_tracking` row.
    #[derive(Clone, Debug, Default, PartialEq)]
    pub struct FakeTrackingRow {
        pub external_offset: Option<i64>,
        pub retry_count: i32,
        /// Last attempt, in fake-clock ms.
        pub forwarded_at: Option<i64>,
        pub error_message: Option<String>,
    }

    /// A `kafka.shadow_metrics` row.
    #[derive(Clone, Debug, Default, PartialEq)]
    pub struct FakeMetrics {
        pub messages_forwarded: i64,
        pub messages_failed: i64,
        pub last_forwarded_offset: Option<i64>,
    }

    #[derive(Default)]
    pub struct State {
        pub enabled: bool,
        pub strict: bool,
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

        fn topic_has_dead_letters(&self, topic_id: i32, max_retries: i64) -> Result<bool> {
            Ok(self.state().tracking.iter().any(|(k, r)| {
                k.0 == topic_id
                    && r.external_offset.is_none()
                    && r.retry_count as i64 >= max_retries
            }))
        }

        fn insert_outbox_rows(&self, rows: &[(i32, i32, i64)]) -> Result<()> {
            let mut s = self.state();
            for &key in rows {
                if s.messages.contains_key(&key) {
                    s.tracking.entry(key).or_default();
                }
            }
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
                    .map(|(&(t, p, o), m)| PendingForwardRecord {
                        topic_id: t,
                        partition_id: p,
                        offset: o,
                        key: m.key.clone(),
                    })
                    .collect(),
            )
        }

        fn claim_due_outbox_rows(&self, claim: &OutboxClaim) -> Result<Vec<ForwardRequest>> {
            let mut s = self.state();
            if s.fail_claim {
                return Err(KafkaError::Internal("claim failed".to_string()));
            }
            let now = s.now_ms;
            let inflight: Vec<RowKey> = claim
                .inflight_topics
                .iter()
                .zip(&claim.inflight_partitions)
                .zip(&claim.inflight_offsets)
                .map(|((&t, &p), &o)| (t, p, o))
                .collect();
            let due: Vec<RowKey> = s
                .tracking
                .iter()
                .filter(|(k, r)| {
                    r.external_offset.is_none()
                        && (r.retry_count as i64) < claim.max_retries
                        && r.forwarded_at
                            .is_none_or(|at| at < now - claim.retry_interval_ms)
                        && !inflight.contains(k)
                        && s.messages.contains_key(k)
                })
                .map(|(k, _)| *k)
                .take(claim.batch_limit.max(0) as usize)
                .collect();
            let mut reqs = Vec::new();
            for key in due {
                let row = s.tracking.get_mut(&key).unwrap();
                row.forwarded_at = Some(now);
                row.retry_count += 1;
                let Some(topic_name) = s.topics.get(&key.0).cloned() else {
                    continue; // JOIN kafka.topics drops it
                };
                let external = s
                    .external_names
                    .get(&key.0)
                    .cloned()
                    .flatten()
                    .unwrap_or(topic_name);
                let m = &s.messages[&key];
                reqs.push(ForwardRequest::new(
                    key.0,
                    external,
                    key.1,
                    m.key.clone(),
                    m.value.clone(),
                    key.2,
                ));
            }
            Ok(reqs)
        }

        fn mark_forward_succeeded(
            &self,
            topic_id: i32,
            partition_id: i32,
            local_offset: i64,
            external_offset: i64,
        ) -> Result<bool> {
            let mut s = self.state();
            let now = s.now_ms;
            let Some(row) = s.tracking.get_mut(&(topic_id, partition_id, local_offset)) else {
                return Ok(false);
            };
            if row.external_offset.is_some() {
                return Ok(false);
            }
            row.external_offset = Some(external_offset);
            row.forwarded_at = Some(now);
            row.error_message = None;
            let m = s.metrics.entry((topic_id, partition_id)).or_default();
            m.messages_forwarded += 1;
            m.last_forwarded_offset = Some(
                m.last_forwarded_offset
                    .unwrap_or(i64::MIN)
                    .max(local_offset),
            );
            Ok(true)
        }

        fn mark_forward_failed(
            &self,
            topic_id: i32,
            partition_id: i32,
            local_offset: i64,
            error_message: &str,
        ) -> Result<Option<i32>> {
            let mut s = self.state();
            let Some(row) = s.tracking.get_mut(&(topic_id, partition_id, local_offset)) else {
                return Ok(None);
            };
            if row.external_offset.is_some() {
                return Ok(None);
            }
            row.error_message = Some(error_message.to_string());
            let retry_count = row.retry_count;
            s.metrics
                .entry((topic_id, partition_id))
                .or_default()
                .messages_failed += 1;
            Ok(Some(retry_count))
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
}
