//! Background maintenance SQL: the retention sweep, time-partition maintenance,
//! timed-out transactions and aborted-row cleanup.

use super::PostgresStore;
use crate::kafka::error::{KafkaError, Result};
use crate::kafka::storage::retention;
use pgrx::prelude::*;
use std::time::Duration;

// DR-1/DR-2 (DEEP-REVIEW-2026-07): retention constants for the periodic sweep.
// These are deliberately constants rather than GUCs — only the messages-table
// retention (the policy an operator genuinely tunes) is exposed as
// `pg_kafka.message_retention_hours`; the auxiliary windows match Kafka's own
// defaults where one exists.

/// Grace period before physically deleting `txn_state='aborted'` rows. Aborted rows
/// are already invisible to consumers; the grace only avoids churning rows a
/// concurrent diagnostic query might be looking at.
pub const ABORTED_MESSAGE_GRACE: Duration = Duration::from_secs(60);
/// Idle window after which a producer id (and its sequences) is reclaimed.
/// Kafka: `transactional.id.expiration.ms` defaults to 7 days.
pub const PRODUCER_ID_RETENTION: Duration = Duration::from_secs(7 * 24 * 3600);
/// Age after which terminal (`CompleteCommit`/`CompleteAbort`) transaction rows are
/// deleted. Matches PRODUCER_ID_RETENTION so a txn row never outlives its producer.
pub const TERMINAL_TXN_RETENTION: Duration = Duration::from_secs(7 * 24 * 3600);
/// Age after which successfully forwarded shadow-outbox rows are deleted.
pub const SHADOW_DELIVERED_RETENTION: Duration = Duration::from_secs(24 * 3600);
// Expired-message batching: see crate::kafka::storage::retention (RETENTION_DELETE_BATCH,
// RETENTION_PASS_BUDGET), re-exported here for existing callers.
pub use crate::kafka::storage::retention::{RETENTION_DELETE_BATCH, RETENTION_PASS_BUDGET};

/// Row counts deleted by one retention sweep (see `run_retention_sweep`).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RetentionSweepStats {
    pub aborted_messages: u64,
    pub expired_messages: u64,
    /// Whole time partitions (leaves of kafka.messages) dropped by retention.
    pub dropped_partitions: u64,
    pub stale_producers: u64,
    pub terminal_transactions: u64,
    pub shadow_delivered_rows: u64,
}

impl RetentionSweepStats {
    pub fn total(&self) -> u64 {
        self.aborted_messages
            + self.expired_messages
            + self.dropped_partitions
            + self.stale_producers
            + self.terminal_transactions
            + self.shadow_delivered_rows
    }
}

impl PostgresStore {
    /// DR-1/DR-2 (DEEP-REVIEW-2026-07): one pass of the storage-lifecycle sweep.
    ///
    /// Before this existed the system only ever grew: `cleanup_aborted_messages` was
    /// implemented but had no production caller, and nothing pruned expired messages,
    /// stale producer ids/sequences, terminal transaction rows, or delivered
    /// shadow-outbox rows. The worker calls this every RETENTION_SWEEP_INTERVAL; the
    /// SQL function `pg_kafka_run_retention_sweep()` exposes the same pass on demand.
    ///
    /// Expired messages (ADR-001, 0.1.0: kafka.messages is time-partitioned):
    /// * a past time leaf whose every topic is wholly expired (the leaf's upper bound
    ///   is older than the topic's window) and that holds no pending transactional row
    ///   is DROPPED whole (`kafka.drop_message_partition`, which re-checks under its
    ///   lock);
    /// * a past leaf that must stay (a topic with a longer or infinite window, or a
    ///   pending row) gets the bounded row delete for its wholly expired topics;
    /// * a topic whose window is shorter than one leaf (`partition_width_ms`) is still
    ///   expired row by row everywhere, as before partitioning; longer windows are
    ///   enforced a leaf at a time (Kafka segment semantics, see
    ///   `retention::row_level_expiry`), and rows still in the DEFAULT leaf are always
    ///   expired row by row.
    ///
    /// `message_retention_hours` ≤ 0 disables the global window (per-topic
    /// `retention.ms` still applies); pending transactional rows are never deleted
    /// regardless of age. `aborted_grace` is the ABORTED_MESSAGE_GRACE window
    /// (parameterized so the on-demand SQL function can shrink it for testing).
    ///
    /// Offset-monotonicity safety: neither deletes nor drops touch
    /// `kafka.partition_offsets.next_offset`, and every offset producer/read path
    /// takes `GREATEST(next_offset, MAX+1)` (BUG-3), so removing rows — oldest or
    /// newest — cannot cause offset reuse or HWM regression. Both *do* advance
    /// `log_start_offset` for every partition they touched (only ever forward,
    /// GREATEST) so the reported earliest offset tracks retention instead of
    /// regressing to 0. Consumers positioned before a retention cutoff get a
    /// standard Kafka out-of-range reset, exactly as with a real broker's retention.
    pub fn run_retention_sweep(
        &self,
        message_retention_hours: i32,
        aborted_grace: Duration,
        partition_minutes: i32,
    ) -> Result<RetentionSweepStats> {
        let mut stats = RetentionSweepStats {
            aborted_messages: self.cleanup_aborted_messages(aborted_grace)?,
            ..Default::default()
        };
        let width_ms = retention::partition_width_ms(partition_minutes);

        Spi::connect_mut(|client| {
            // Each topic's effective window comes from `effective_retention_window_ms`:
            // per-topic `retention.ms` (kafka.topics.retention_ms, settable via
            // IncrementalAlterConfigs) overrides the global GUC — >= 0 enforces that
            // window even when the global sweep is off; < 0 pins the topic to infinite
            // retention even when a global window is set; NULL falls back to the GUC.
            let topics = client.select("SELECT id, retention_ms FROM kafka.topics ORDER BY id", None, &[])?;
            let mut all_windows: std::collections::HashMap<i32, Option<i64>> =
                std::collections::HashMap::new();
            let mut windows: Vec<(i32, i64)> = Vec::new();
            for row in topics {
                let id: i32 = row.get_by_name("id")?.unwrap_or(0);
                let retention_ms: Option<i64> = row.get_by_name("retention_ms")?;
                let window = retention::effective_retention_window_ms(
                    retention_ms,
                    message_retention_hours,
                );
                all_windows.insert(id, window);
                if let Some(window_ms) = window {
                    windows.push((id, window_ms));
                }
            }
            let windows = retention::rotated(&windows, retention::next_start_index(windows.len()));

            let now_ms: i64 = client
                .select("SELECT (EXTRACT(EPOCH FROM NOW()) * 1000)::bigint AS now_ms", None, &[])?
                .first()
                .get_by_name("now_ms")?
                .unwrap_or(0);

            // wave3 worker: a schema older than 0.1.0 (new binary before ALTER EXTENSION
            // UPDATE) has no time partitions and no kafka.message_partitions(). Treat the
            // whole table as one DEFAULT-like leaf: expiry row by row, nothing to drop.
            let partitioned: bool = client
                .select(
                    "SELECT to_regprocedure('kafka.message_partitions()') IS NOT NULL AS p",
                    None,
                    &[],
                )?
                .first()
                .get_by_name("p")?
                .unwrap_or(false);

            // The leaves, oldest first (DEFAULT and the upgrade's MINVALUE leaf first).
            // Bounds travel back into SQL as text: same session, same DateStyle, so the
            // timestamptz round-trips exactly (to the microsecond).
            let leaf_sql = if partitioned {
                "SELECT partition_oid::bigint AS oid, partition_name, is_default,
                        lower_bound::text AS lower_txt, upper_bound::text AS upper_txt,
                        (EXTRACT(EPOCH FROM lower_bound) * 1000)::bigint AS lower_ms,
                        (EXTRACT(EPOCH FROM upper_bound) * 1000)::bigint AS upper_ms
                 FROM kafka.message_partitions()
                 ORDER BY lower_bound NULLS FIRST, upper_bound"
            } else {
                "SELECT 0::bigint AS oid, 'messages'::text AS partition_name, true AS is_default,
                        NULL::text AS lower_txt, NULL::text AS upper_txt,
                        NULL::bigint AS lower_ms, NULL::bigint AS upper_ms"
            };
            let table = client.select(
                leaf_sql,
                None,
                &[],
            )?;
            let mut leaves: Vec<retention::TimePartition> = Vec::new();
            let mut bounds: std::collections::HashMap<u32, (Option<String>, Option<String>)> =
                std::collections::HashMap::new();
            for row in table {
                let oid: i64 = row.get_by_name("oid")?.unwrap_or(0);
                let oid = oid as u32;
                bounds.insert(oid, (row.get_by_name("lower_txt")?, row.get_by_name("upper_txt")?));
                leaves.push(retention::TimePartition {
                    oid,
                    name: row.get_by_name("partition_name")?.unwrap_or_default(),
                    lower_ms: row.get_by_name("lower_ms")?,
                    upper_ms: row.get_by_name("upper_ms")?,
                    is_default: row.get_by_name("is_default")?.unwrap_or(false),
                });
            }

            // 1. Inspect every past time leaf: drop it whole, trim it, or keep it.
            let mut to_drop: Vec<(u32, Vec<i32>)> = Vec::new();
            // (topic, window_ms, leaf oid) row-delete work, in order.
            let mut work: Vec<(i32, i64, u32)> = Vec::new();
            for leaf in retention::past_time_partitions(&leaves, now_ms) {
                let row = client.select(
                    "SELECT topic_ids, has_pending
                     FROM kafka.message_partition_contents($1::bigint::oid::regclass)",
                    None,
                    &[(leaf.oid as i64).into()],
                )?;
                let row = row.first();
                let present: Vec<Option<i32>> = row.get_by_name("topic_ids")?.unwrap_or_default();
                let present: Vec<i32> = present.into_iter().flatten().collect();
                let has_pending: bool = row.get_by_name("has_pending")?.unwrap_or(true);
                let upper_ms = leaf.upper_ms.unwrap_or(i64::MAX);
                match retention::classify_leaf(upper_ms, now_ms, &present, has_pending, |t| {
                    all_windows.get(&t).copied().flatten()
                }) {
                    retention::LeafAction::Drop { expired_topics } => {
                        if to_drop.len() < retention::MAX_PARTITION_DROPS_PER_PASS {
                            to_drop.push((leaf.oid, expired_topics));
                        }
                    }
                    retention::LeafAction::Trim { topics } => {
                        for t in topics {
                            if let Some(Some(w)) = all_windows.get(&t) {
                                work.push((t, *w, leaf.oid));
                            }
                        }
                    }
                    retention::LeafAction::Keep => {}
                }
            }
            // 2. Row-level expiry: short windows everywhere they may have expired rows;
            //    longer windows only in the DEFAULT leaf (the rest is leaf-granular).
            for (topic_id, window_ms) in &windows {
                for leaf in &leaves {
                    if to_drop.iter().any(|(oid, _)| *oid == leaf.oid) {
                        continue;
                    }
                    let in_scope = leaf.is_default
                        || retention::row_level_expiry(*window_ms, width_ms);
                    if in_scope && retention::may_hold_expired(leaf, now_ms, *window_ms) {
                        work.push((*topic_id, *window_ms, leaf.oid));
                    }
                }
            }

            // Deletes run in RETENTION_DELETE_BATCH batches, per (topic, leaf), until
            // caught up (a short batch) or the pass has spent RETENTION_PASS_BUDGET.
            // Each statement is confined to one leaf: through the parent with the
            // leaf's bounds (so the planner prunes to it) and its tableoid (ctid is
            // only unique within one table), or directly on kafka.messages_default.
            // The per-topic cutoff is one constant per statement, so the planner can
            // use the leaf's idx_messages_created_at_brin or its topic_id-prefixed
            // unique index. Pending transactional rows are never deleted.
            let default_oid = leaves.iter().find(|l| l.is_default).map(|l| l.oid);
            let pass_start = std::time::Instant::now();
            let mut affected: std::collections::HashSet<(i32, i32)> = std::collections::HashSet::new();
            let mut expired = 0u64;
            'work: for (topic_id, window_ms, leaf_oid) in work {
                loop {
                    if pass_start.elapsed() >= RETENTION_PASS_BUDGET {
                        break 'work;
                    }
                    // Window built by multiplication: PG14 rejects a text
                    // 'N milliseconds' literal above the int32 range.
                    let table = if !partitioned {
                        client.update(
                            "DELETE FROM kafka.messages
                             WHERE ctid IN (
                                 SELECT d.ctid FROM kafka.messages d
                                 WHERE d.topic_id = $1
                                   AND d.created_at < NOW() - $2::bigint * INTERVAL '1 millisecond'
                                   AND (d.txn_state IS NULL OR d.txn_state <> 'pending')
                                 LIMIT $3
                             )
                             RETURNING topic_id, partition_id",
                            None,
                            &[topic_id.into(), window_ms.into(), RETENTION_DELETE_BATCH.into()],
                        )?
                    } else if Some(leaf_oid) == default_oid {
                        client.update(
                            "DELETE FROM kafka.messages_default
                             WHERE ctid IN (
                                 SELECT d.ctid FROM kafka.messages_default d
                                 WHERE d.topic_id = $1
                                   AND d.created_at < NOW() - $2::bigint * INTERVAL '1 millisecond'
                                   AND (d.txn_state IS NULL OR d.txn_state <> 'pending')
                                 LIMIT $3
                             )
                             RETURNING topic_id, partition_id",
                            None,
                            &[topic_id.into(), window_ms.into(), RETENTION_DELETE_BATCH.into()],
                        )?
                    } else {
                        let (lower, upper) = bounds.get(&leaf_oid).cloned().unwrap_or((None, None));
                        client.update(
                            "DELETE FROM kafka.messages
                             WHERE created_at >= COALESCE($4::text::timestamptz, '-infinity')
                               AND created_at < COALESCE($5::text::timestamptz, 'infinity')
                               AND tableoid = $6::bigint::oid
                               AND ctid = ANY (ARRAY(
                                   SELECT m.ctid FROM kafka.messages m
                                   WHERE m.created_at >= COALESCE($4::text::timestamptz, '-infinity')
                                     AND m.created_at < COALESCE($5::text::timestamptz, 'infinity')
                                     AND m.tableoid = $6::bigint::oid
                                     AND m.topic_id = $1
                                     AND m.created_at < NOW() - $2::bigint * INTERVAL '1 millisecond'
                                     AND (m.txn_state IS NULL OR m.txn_state <> 'pending')
                                   LIMIT $3))
                             RETURNING topic_id, partition_id",
                            None,
                            &[
                                topic_id.into(),
                                window_ms.into(),
                                RETENTION_DELETE_BATCH.into(),
                                lower.into(),
                                upper.into(),
                                (leaf_oid as i64).into(),
                            ],
                        )?
                    };
                    let mut deleted = 0u64;
                    for row in table {
                        deleted += 1;
                        let tid: i32 = row.get_by_name("topic_id")?.unwrap_or(0);
                        let pid: i32 = row.get_by_name("partition_id")?.unwrap_or(0);
                        affected.insert((tid, pid));
                    }
                    expired += deleted;
                    if !retention::should_run_another_batch(
                        deleted,
                        RETENTION_DELETE_BATCH,
                        pass_start.elapsed(),
                        RETENTION_PASS_BUDGET,
                    ) {
                        break;
                    }
                }
            }
            stats.expired_messages = expired;

            // 3. Whole-leaf drops, last, so the ACCESS EXCLUSIVE lock a drop takes on
            //    kafka.messages is held only until this transaction commits.
            for (leaf_oid, expired_topics) in to_drop {
                let row = client.update(
                    "SELECT dropped, topic_ids, partition_ids
                     FROM kafka.drop_message_partition($1::bigint::oid::regclass, $2::int4[])",
                    None,
                    &[(leaf_oid as i64).into(), expired_topics.into()],
                )?;
                let row = row.first();
                if row.get_by_name::<bool, _>("dropped")?.unwrap_or(false) {
                    stats.dropped_partitions += 1;
                    let tids: Vec<Option<i32>> = row.get_by_name("topic_ids")?.unwrap_or_default();
                    let pids: Vec<Option<i32>> =
                        row.get_by_name("partition_ids")?.unwrap_or_default();
                    for (t, p) in tids.into_iter().zip(pids) {
                        if let (Some(t), Some(p)) = (t, p) {
                            affected.insert((t, p));
                        }
                    }
                }
            }

            // Advance the durable log start of every partition the deletes or drops
            // touched (Codex review, PR #95): like DeleteRecords, retention must
            // advance partition_offsets.log_start_offset — otherwise a partition
            // emptied by retention reports EARLIEST=0 via get_earliest_offset and a
            // consumer resets to offsets retention already removed. Offsets are
            // assigned monotonically with created_at, so retention removes an
            // offset-contiguous prefix; the new log start is the oldest surviving
            // offset, or next_offset (the HWM) when the partition is now empty.
            if !affected.is_empty() {
                let (topic_ids, partition_ids): (Vec<i32>, Vec<i32>) =
                    affected.into_iter().unzip();
                client.update(
                    "UPDATE kafka.partition_offsets po
                     SET log_start_offset = GREATEST(
                             po.log_start_offset,
                             COALESCE((SELECT MIN(m.partition_offset) FROM kafka.messages m
                                       WHERE m.topic_id = po.topic_id AND m.partition_id = po.partition_id),
                                      po.next_offset))
                     FROM unnest($1::int4[], $2::int4[]) AS a(topic_id, partition_id)
                     WHERE po.topic_id = a.topic_id AND po.partition_id = a.partition_id",
                    None,
                    &[topic_ids.into(), partition_ids.into()],
                )?;
            }

            // Terminal transactions first (their FK on producer_ids would otherwise
            // block the producer prune below). Only rows idle past the window: a
            // producer that resumes a terminal transactional_id within the window
            // keeps its row; one that resumes after the prune re-creates it via
            // InitProducerId, matching Kafka's transactional.id expiration.
            let terminal_txn_secs = TERMINAL_TXN_RETENTION.as_secs() as i64;
            let table = client.update(
                "DELETE FROM kafka.transactions
                 WHERE state IN ('CompleteCommit', 'CompleteAbort', 'Empty')
                   AND last_updated_at < NOW() - ($1 || ' seconds')::interval
                 RETURNING 1",
                None,
                &[terminal_txn_secs.into()],
            )?;
            stats.terminal_transactions = table.len() as u64;

            // Stale producers: idle past the window and not referenced by any
            // remaining transaction row. Sequences go with their producer (no FK
            // between the two tables, so the CTE keeps it atomic).
            let producer_secs = PRODUCER_ID_RETENTION.as_secs() as i64;
            let table = client.update(
                "WITH doomed AS (
                     DELETE FROM kafka.producer_ids p
                     WHERE p.last_active_at < NOW() - ($1 || ' seconds')::interval
                       AND NOT EXISTS (
                           SELECT 1 FROM kafka.transactions t
                           WHERE t.producer_id = p.producer_id
                       )
                       -- last_active_at only moves on InitProducerId; a long-running
                       -- producer shows its activity through its sequence rows.
                       AND NOT EXISTS (
                           SELECT 1 FROM kafka.producer_sequences s
                           WHERE s.producer_id = p.producer_id
                             AND s.updated_at >= NOW() - ($1 || ' seconds')::interval
                       )
                     RETURNING p.producer_id
                 ),
                 seqs AS (
                     DELETE FROM kafka.producer_sequences s
                     USING doomed d
                     WHERE s.producer_id = d.producer_id
                 )
                 SELECT COUNT(*)::BIGINT AS n FROM doomed",
                None,
                &[producer_secs.into()],
            )?;
            stats.stale_producers = table
                .first()
                .get_by_name::<i64, _>("n")?
                .unwrap_or(0)
                .max(0) as u64;

            // Delivered shadow-outbox rows: forwarding is complete (external_offset
            // set); keep a day of history for diagnostics, then reap.
            let shadow_secs = SHADOW_DELIVERED_RETENTION.as_secs() as i64;
            let table = client.update(
                "DELETE FROM kafka.shadow_tracking
                 WHERE external_offset IS NOT NULL
                   AND forwarded_at < NOW() - ($1 || ' seconds')::interval
                 RETURNING 1",
                None,
                &[shadow_secs.into()],
            )?;
            stats.shadow_delivered_rows = table.len() as u64;

            Ok(stats)
        })
        .map_err(|e: KafkaError| KafkaError::Internal(format!("run_retention_sweep failed: {}", e)))
    }

    /// ADR-001 partition maintenance: make sure time partitions of `partition_minutes`
    /// exist for now and the next [`retention::PARTITIONS_AHEAD`] widths, and move any
    /// row the DEFAULT leaf caught into its proper leaf. The DDL runs inside the
    /// SECURITY DEFINER function `kafka.ensure_message_partitions`, so the worker role
    /// needs only EXECUTE on it. Returns the number of leaves created. The worker
    /// calls this at start and every retention interval; the SQL function
    /// `pg_kafka_run_partition_maintenance()` runs it on demand.
    pub fn ensure_time_partitions(&self, partition_minutes: i32) -> Result<i64> {
        let minutes = partition_minutes.clamp(
            retention::MIN_PARTITION_MINUTES,
            retention::MAX_PARTITION_MINUTES,
        );
        Spi::connect_mut(|client| {
            let table = client.update(
                "SELECT kafka.ensure_message_partitions($1, $2)::bigint AS created",
                None,
                &[minutes.into(), retention::PARTITIONS_AHEAD.into()],
            )?;
            Ok(table.first().get_by_name::<i64, _>("created")?.unwrap_or(0))
        })
        .map_err(|e: KafkaError| {
            KafkaError::Internal(format!("ensure_time_partitions failed: {}", e))
        })
    }

    pub(super) fn abort_timed_out_transactions(&self, timeout: Duration) -> Result<Vec<String>> {
        // RV-7: enforce each transaction's own timeout_ms (set from the client's
        // transaction.timeout.ms at InitProducerId), not a single hardcoded value.
        // The passed `timeout` is only the fallback when a row's timeout_ms is NULL.
        let default_timeout_ms = timeout.as_millis() as i64;
        crate::pg_debug!(
            "PostgresStore::abort_timed_out_transactions: default_timeout={}ms",
            default_timeout_ms
        );

        Spi::connect_mut(|client| {
            // Find and abort timed-out transactions
            let table = client.select(
                "SELECT transactional_id, producer_id, producer_epoch
                 FROM kafka.transactions
                 WHERE state = 'Ongoing'
                   AND started_at < NOW() - (COALESCE(timeout_ms, $1) || ' milliseconds')::interval",
                None,
                &[default_timeout_ms.into()],
            )?;

            let mut aborted = Vec::new();

            for row in table {
                let txn_id: String = row.get_by_name("transactional_id")?.unwrap_or_default();
                let producer_id: i64 = row.get_by_name("producer_id")?.unwrap_or(0);
                let producer_epoch: i16 = row.get_by_name("producer_epoch")?.unwrap_or(0);

                // Mark messages as aborted
                client.update(
                    "UPDATE kafka.messages SET txn_state = 'aborted'
                     WHERE producer_id = $1 AND producer_epoch = $2 AND txn_state = 'pending'",
                    None,
                    &[producer_id.into(), producer_epoch.into()],
                )?;

                // Delete pending offsets
                client.update(
                    "DELETE FROM kafka.txn_pending_offsets WHERE transactional_id = $1",
                    None,
                    &[txn_id.clone().into()],
                )?;

                // Fence the timed-out producer, as Kafka does, by bumping its epoch.
                // Without this the stalled producer could resume with the same epoch
                // (AddPartitionsToTxn accepts CompleteAbort -> Ongoing), add more
                // partitions and commit only the part written after the abort. The
                // guard leaves the epoch alone if it already moved or is exhausted
                // (the next InitProducerId then allocates a fresh producer id).
                let bumped = client.update(
                    "UPDATE kafka.producer_ids SET epoch = epoch + 1
                     WHERE producer_id = $1 AND epoch = $2 AND epoch < 32767
                     RETURNING epoch",
                    None,
                    &[producer_id.into(), producer_epoch.into()],
                )?;
                let new_epoch: i16 = if bumped.is_empty() {
                    producer_epoch
                } else {
                    bumped.first().get_by_name("epoch")?.unwrap_or(producer_epoch)
                };
                // The new epoch restarts sequence numbers.
                client.update(
                    "DELETE FROM kafka.producer_sequences WHERE producer_id = $1",
                    None,
                    &[producer_id.into()],
                )?;

                // Update transaction state
                client.update(
                    "UPDATE kafka.transactions
                     SET state = 'CompleteAbort', producer_epoch = $2, last_updated_at = NOW()
                     WHERE transactional_id = $1",
                    None,
                    &[txn_id.clone().into(), new_epoch.into()],
                )?;

                crate::pg_debug!("Aborted timed-out transaction: {}", txn_id);
                aborted.push(txn_id);
            }

            Ok(aborted)
        })
        .map_err(|e: KafkaError| {
            KafkaError::Internal(format!("abort_timed_out_transactions failed: {}", e))
        })
    }

    pub(super) fn cleanup_aborted_messages(&self, older_than: Duration) -> Result<u64> {
        let older_than_secs = older_than.as_secs() as i64;
        crate::pg_debug!(
            "PostgresStore::cleanup_aborted_messages: older_than={}s",
            older_than_secs
        );

        Spi::connect_mut(|client| {
            // Use DELETE ... RETURNING to count deleted rows.
            // DR-1 (DEEP-REVIEW-2026-07): must be client.update — client.select runs
            // SPI read-only, and Postgres rejects DML there ("DELETE is not allowed in
            // a non-volatile function"). This was latent for as long as this method had
            // no production caller; the retention-sweep E2E test now pins it.
            let table = client.update(
                "DELETE FROM kafka.messages
                 WHERE txn_state = 'aborted'
                   AND created_at < NOW() - ($1 || ' seconds')::interval
                 RETURNING 1",
                None,
                &[older_than_secs.into()],
            )?;

            let deleted = table.len() as u64;
            crate::pg_debug!("Deleted {} aborted messages", deleted);
            Ok(deleted)
        })
        .map_err(|e: KafkaError| {
            KafkaError::Internal(format!("cleanup_aborted_messages failed: {}", e))
        })
    }
}
