// PostgreSQL implementation of the KafkaStore trait
//
// This module implements the storage layer using PostgreSQL and pgrx SPI.
// It assumes it runs within a transaction context managed by the caller.

use super::{
    CommittedOffset, FetchedMessage, IsolationLevel, KafkaStore, TopicMetadata, TransactionState,
};
use crate::kafka::error::{KafkaError, Result};
use crate::kafka::messages::Record;
use pgrx::prelude::*;
use std::collections::HashMap;
use std::time::Duration;

/// PostgreSQL-backed implementation of KafkaStore
///
/// This implementation uses pgrx's SPI (Server Programming Interface) to execute
/// SQL queries. All operations assume they run within an active transaction
/// started by BackgroundWorker::transaction() in worker.rs.
pub struct PostgresStore;

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
/// Per-sweep cap on expired-message deletes, so one sweep can't hold the DB thread
/// (and its transaction) for an unbounded scan after retention is first enabled on
/// a large backlog. The sweep runs every RETENTION_SWEEP_INTERVAL, so the backlog
/// drains incrementally.
pub const RETENTION_DELETE_BATCH: i64 = 10_000;

/// Row counts deleted by one retention sweep (see `run_retention_sweep`).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RetentionSweepStats {
    pub aborted_messages: u64,
    pub expired_messages: u64,
    pub stale_producers: u64,
    pub terminal_transactions: u64,
    pub shadow_delivered_rows: u64,
}

impl RetentionSweepStats {
    pub fn total(&self) -> u64 {
        self.aborted_messages
            + self.expired_messages
            + self.stale_producers
            + self.terminal_transactions
            + self.shadow_delivered_rows
    }
}

impl PostgresStore {
    /// Create a new PostgresStore instance
    pub fn new() -> Self {
        PostgresStore
    }

    /// Shared completion path for `commit_transaction` / `abort_transaction`
    /// (PR #84 documented follow-up). The two flows were ~90% duplicated SQL:
    /// identical validation (existence, fencing, Ongoing-state check), a
    /// terminal-visibility UPDATE on `kafka.messages` (NULL = visible vs
    /// 'aborted'), the pending-offset DELETE, and the terminal
    /// `kafka.transactions` state UPDATE — commit additionally promotes
    /// pending offsets into `kafka.consumer_offsets` before the delete.
    fn end_transaction(
        &self,
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        commit: bool,
    ) -> Result<()> {
        let op = if commit { "commit" } else { "abort" };
        crate::pg_debug!(
            "PostgresStore::end_transaction ({}): transactional_id={}, producer_id={}, epoch={}",
            op,
            transactional_id,
            producer_id,
            producer_epoch
        );

        Spi::connect_mut(|client| {
            // Step 1: Validate transaction state
            let txn_table = client.select(
                "SELECT state, producer_id, producer_epoch FROM kafka.transactions WHERE transactional_id = $1",
                Some(1),
                &[transactional_id.into()],
            )?;

            if txn_table.is_empty() {
                return Err(KafkaError::transactional_id_not_found(transactional_id));
            }

            // `unwrap_or`/`unwrap_or_default` on these column reads is safe by
            // schema, not by luck: kafka.transactions.{producer_id, producer_epoch,
            // state} are all declared NOT NULL (sql/bootstrap.sql), so get_by_name
            // never returns None for a row that exists (and non-existence was already
            // handled by the is_empty() check above). The fallback value is therefore
            // unreachable and never masks a real NULL. The same reasoning applies to
            // the other `get_by_name(...).unwrap_or*` reads of NOT NULL columns
            // throughout this module.
            let row = txn_table.first();
            let current_producer_id: i64 = row.get_by_name("producer_id")?.unwrap_or(0);
            let current_epoch: i16 = row.get_by_name("producer_epoch")?.unwrap_or(0);
            let state: String = row.get_by_name("state")?.unwrap_or_default();

            if current_producer_id != producer_id || current_epoch != producer_epoch {
                return Err(KafkaError::producer_fenced(
                    producer_id,
                    producer_epoch,
                    current_epoch,
                ));
            }

            if state != "Ongoing" {
                return Err(KafkaError::invalid_txn_state(
                    transactional_id,
                    "Ongoing",
                    &state,
                ));
            }

            // Step 2: Terminal message visibility — commit makes pending rows
            // visible (txn_state = NULL); abort marks them 'aborted'.
            if commit {
                client.update(
                    "UPDATE kafka.messages SET txn_state = NULL
                     WHERE producer_id = $1 AND producer_epoch = $2 AND txn_state = 'pending'",
                    None,
                    &[producer_id.into(), producer_epoch.into()],
                )?;

                // Step 3 (commit only): Move pending offsets to consumer_offsets
                client.update(
                    "INSERT INTO kafka.consumer_offsets (group_id, topic_id, partition_id, committed_offset, metadata)
                     SELECT group_id, topic_id, partition_id, pending_offset, metadata
                     FROM kafka.txn_pending_offsets
                     WHERE transactional_id = $1
                     ON CONFLICT (group_id, topic_id, partition_id) DO UPDATE SET
                         committed_offset = EXCLUDED.committed_offset,
                         metadata = EXCLUDED.metadata,
                         commit_timestamp = NOW()",
                    None,
                    &[transactional_id.into()],
                )?;
            } else {
                client.update(
                    "UPDATE kafka.messages SET txn_state = 'aborted'
                     WHERE producer_id = $1 AND producer_epoch = $2 AND txn_state = 'pending'",
                    None,
                    &[producer_id.into(), producer_epoch.into()],
                )?;
            }

            // Step 4: Delete pending offsets (aborted ones are simply dropped)
            client.update(
                "DELETE FROM kafka.txn_pending_offsets WHERE transactional_id = $1",
                None,
                &[transactional_id.into()],
            )?;

            // Step 5: Terminal transaction state
            client.update(
                if commit {
                    "UPDATE kafka.transactions SET state = 'CompleteCommit', last_updated_at = NOW()
                     WHERE transactional_id = $1"
                } else {
                    "UPDATE kafka.transactions SET state = 'CompleteAbort', last_updated_at = NOW()
                     WHERE transactional_id = $1"
                },
                None,
                &[transactional_id.into()],
            )?;

            crate::pg_debug!("Transaction {} completed ({})", transactional_id, op);
            Ok(())
        })
        .map_err(|e| match e {
            KafkaError::TransactionalIdNotFound { .. }
            | KafkaError::ProducerFenced { .. }
            | KafkaError::InvalidTxnState { .. } => e,
            _ => KafkaError::Internal(format!("{}_transaction failed: {}", op, e)),
        })
    }

    /// DR-1/DR-2 (DEEP-REVIEW-2026-07): one pass of the storage-lifecycle sweep.
    ///
    /// Before this existed the system only ever grew: `cleanup_aborted_messages` was
    /// implemented but had no production caller, and nothing pruned expired messages,
    /// stale producer ids/sequences, terminal transaction rows, or delivered
    /// shadow-outbox rows. The worker calls this every RETENTION_SWEEP_INTERVAL; the
    /// SQL function `pg_kafka_run_retention_sweep()` exposes the same pass on demand.
    ///
    /// `message_retention_hours` ≤ 0 disables the expired-message delete (messages
    /// kept forever); pending transactional rows are never deleted regardless of age.
    /// `aborted_grace` is the ABORTED_MESSAGE_GRACE window (parameterized so the
    /// on-demand SQL function can shrink it for testing).
    ///
    /// Offset-monotonicity safety: the message delete never touches
    /// `kafka.partition_offsets.next_offset`, and every offset producer/read path
    /// takes `GREATEST(next_offset, MAX+1)` (BUG-3), so removing rows — oldest or
    /// newest — cannot cause offset reuse or HWM regression. It *does* advance
    /// `log_start_offset` for emptied partitions (only ever forward, GREATEST) so
    /// the reported earliest offset tracks retention instead of regressing to 0.
    /// Consumers positioned before a retention cutoff get a standard Kafka
    /// out-of-range reset, exactly as with a real broker's retention.
    pub fn run_retention_sweep(
        &self,
        message_retention_hours: i32,
        aborted_grace: Duration,
    ) -> Result<RetentionSweepStats> {
        let mut stats = RetentionSweepStats {
            aborted_messages: self.cleanup_aborted_messages(aborted_grace)?,
            ..Default::default()
        };

        Spi::connect_mut(|client| {
            // Expired messages. Batched via ctid so one sweep is bounded; never
            // deletes pending transactional rows. Per-topic `retention.ms`
            // (kafka.topics.retention_ms, settable via IncrementalAlterConfigs)
            // overrides the global GUC: >= 0 enforces that window even when the
            // global sweep is off; < 0 pins the topic to infinite retention even
            // when a global window is set; NULL falls back to the GUC.
            let table = client.update(
                "DELETE FROM kafka.messages
                 WHERE ctid IN (
                     SELECT m.ctid FROM kafka.messages m
                     JOIN kafka.topics t ON t.id = m.topic_id
                     WHERE (m.txn_state IS NULL OR m.txn_state <> 'pending')
                       AND (
                            (t.retention_ms IS NOT NULL AND t.retention_ms >= 0
                             AND m.created_at < NOW() - (t.retention_ms || ' milliseconds')::interval)
                         OR (t.retention_ms IS NULL AND $1 > 0
                             AND m.created_at < NOW() - ($1 || ' hours')::interval)
                       )
                     LIMIT $2
                 )
                 RETURNING topic_id, partition_id",
                None,
                &[
                    (message_retention_hours as i64).into(),
                    RETENTION_DELETE_BATCH.into(),
                ],
            )?;
            // Collect the distinct partitions this batch touched so we can
            // advance their durable log start (Codex review, PR #95): like
            // DeleteRecords, retention deletion must advance
            // partition_offsets.log_start_offset — otherwise a partition emptied
            // by retention reports EARLIEST=0 via get_earliest_offset and a
            // consumer resets to offsets retention already removed. Offsets are
            // assigned monotonically with created_at, so retention deletes an
            // offset-contiguous prefix; the new log start is the oldest
            // surviving offset, or next_offset (the HWM) when the partition is
            // now empty.
            let mut affected: std::collections::HashSet<(i32, i32)> = std::collections::HashSet::new();
            let mut expired = 0u64;
            for row in table {
                expired += 1;
                let tid: i32 = row.get_by_name("topic_id")?.unwrap_or(0);
                let pid: i32 = row.get_by_name("partition_id")?.unwrap_or(0);
                affected.insert((tid, pid));
            }
            stats.expired_messages = expired;

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

    /// Shared produce-path insert for plain and transactional batches (DR-24,
    /// DEEP-REVIEW-2026-07): the two paths were ~90% identical copies (advisory
    /// lock -> BUG-3 base-offset -> UNNEST insert -> counter advance) that had
    /// already diverged once (RV-5-style "fix landed on one twin only" bugs).
    /// `txn = Some((producer_id, producer_epoch))` stamps the transaction columns
    /// and txn_state='pending'; None inserts NULLs (plain produce).
    fn insert_records_inner(
        &self,
        topic_id: i32,
        partition_id: i32,
        records: &[Record],
        txn: Option<(i64, i16)>,
    ) -> Result<i64> {
        if records.is_empty() {
            return Ok(0);
        }

        Spi::connect_mut(|client| {
            // Step 1: Lock the partition using advisory lock.
            // DR-13 (DEEP-REVIEW-2026-07): in the shipped topology this lock is
            // uncontended overhead — all extension writes serialize on the single
            // DB thread, so no in-extension writer can race it. It is kept as
            // defense-in-depth against OUT-OF-BAND writers (direct SQL inserts, a
            // future second worker): the offset-assignment read below is a
            // check-then-act that would race such a writer without it. Cost is one
            // fast-path lock acquisition per produce; revisit only with a benchmark
            // showing it matters.
            client.select(
                "SELECT pg_advisory_xact_lock($1, $2)",
                None,
                &[topic_id.into(), partition_id.into()],
            )?;

            // Step 2: Compute base_offset = GREATEST(persisted next_offset, MAX(partition_offset)+1).
            // BUG-3: the persisted per-partition counter (kafka.partition_offsets) is never
            // decremented, so deleting the highest (aborted) rows in cleanup_aborted_messages can't
            // lower the next offset and cause offset reuse. The MAX+1 term seeds the counter from
            // existing data the first time a partition is produced to after this counter existed.
            // (Transactional rows are exactly the ones cleanup later deletes, so both paths MUST
            // advance the same counter — the historical duplication risked them drifting.)
            let table = client.select(
                "SELECT GREATEST(
                          COALESCE((SELECT next_offset FROM kafka.partition_offsets
                                    WHERE topic_id = $1 AND partition_id = $2), 0),
                          COALESCE((SELECT MAX(partition_offset) + 1 FROM kafka.messages
                                    WHERE topic_id = $1 AND partition_id = $2), 0)
                        ) AS base_offset",
                None,
                &[topic_id.into(), partition_id.into()],
            )?;

            let base_offset: i64 = table
                .first()
                .get_by_name::<i64, _>("base_offset")?
                .unwrap_or(0);

            crate::pg_debug!("Assigning base_offset={}", base_offset);

            // Step 3: Build parallel arrays for UNNEST-based bulk insert
            // (type-safe, no SQL built from data, PostgreSQL-optimized).
            let count = records.len();
            let topic_ids: Vec<i32> = vec![topic_id; count];
            let partition_ids: Vec<i32> = vec![partition_id; count];
            let offsets: Vec<i64> = (0..count).map(|i| base_offset + i as i64).collect();
            let keys: Vec<Option<Vec<u8>>> = records.iter().map(|r| r.key.clone()).collect();
            let values: Vec<Option<Vec<u8>>> = records.iter().map(|r| r.value.clone()).collect();
            let headers: Vec<String> = records
                .iter()
                .map(|r| {
                    if r.headers.is_empty() {
                        "{}".to_string()
                    } else {
                        let headers_map: HashMap<String, String> = r
                            .headers
                            .iter()
                            .map(|h| (h.key.clone(), hex_encode(&h.value)))
                            .collect();
                        serde_json::to_string(&headers_map).unwrap_or_else(|_| "{}".to_string())
                    }
                })
                .collect();
            // BUG-7: persist the producer's record timestamp (epoch ms) instead of dropping it; -1
            // marks "no timestamp" so the fetch path falls back to the broker insert time.
            let timestamps: Vec<i64> = records.iter().map(|r| r.timestamp.unwrap_or(-1)).collect();
            // Transaction columns: stamped for transactional batches, NULL otherwise.
            let producer_ids: Vec<Option<i64>> = vec![txn.map(|(pid, _)| pid); count];
            let producer_epochs: Vec<Option<i16>> = vec![txn.map(|(_, epoch)| epoch); count];
            let txn_states: Vec<Option<&str>> = vec![txn.map(|_| "pending"); count];

            client
                .update(
                    "INSERT INTO kafka.messages (topic_id, partition_id, partition_offset, key, value, headers, producer_id, producer_epoch, txn_state, timestamp_ms)
                     SELECT * FROM unnest($1::int[], $2::int[], $3::bigint[], $4::bytea[], $5::bytea[], $6::jsonb[], $7::bigint[], $8::smallint[], $9::text[], $10::bigint[])",
                    None,
                    &[
                        topic_ids.into(),
                        partition_ids.into(),
                        offsets.into(),
                        keys.into(),
                        values.into(),
                        headers.into(),
                        producer_ids.into(),
                        producer_epochs.into(),
                        txn_states.into(),
                        timestamps.into(),
                    ],
                )
                .map_err(|e| KafkaError::Internal(format!("Failed to insert records: {}", e)))?;

            // BUG-3: advance the monotonic per-partition counter so the next produce — even after
            // cleanup_aborted_messages deletes the highest rows — cannot reuse these offsets.
            client
                .update(
                    "INSERT INTO kafka.partition_offsets (topic_id, partition_id, next_offset)
                     VALUES ($1, $2, $3)
                     ON CONFLICT (topic_id, partition_id)
                     DO UPDATE SET next_offset = GREATEST(kafka.partition_offsets.next_offset, EXCLUDED.next_offset)",
                    None,
                    &[
                        topic_id.into(),
                        partition_id.into(),
                        (base_offset + count as i64).into(),
                    ],
                )
                .map_err(|e| {
                    KafkaError::Internal(format!("Failed to advance partition offset: {}", e))
                })?;

            crate::pg_debug!(
                "Successfully inserted {} records (offsets {} to {}, txn={})",
                count,
                base_offset,
                base_offset + count as i64 - 1,
                txn.is_some()
            );

            Ok(base_offset)
        })
        .map_err(|e| match e {
            KafkaError::Internal(_) => e,
            _ => KafkaError::Internal(format!("insert_records failed: {}", e)),
        })
    }

    /// Shared fetch implementation for both isolation levels (DR-8/DR-9/DR-24,
    /// DEEP-REVIEW-2026-07). Previously `fetch_records` and the ReadUncommitted
    /// branch of `fetch_records_with_isolation` were byte-identical copies, and
    /// both capped every fetch at 5,000 rows regardless of the client's
    /// `max_bytes` budget — a 1 MB fetch of small messages returned tens of KB
    /// and forced extra round trips through the single DB thread.
    ///
    /// This version iterates: each query is bounded (MAX_ROWS_PER_QUERY) so one
    /// pass can't hold a huge result set, but the loop continues until the byte
    /// budget is spent or the partition has no more rows. Headers are selected
    /// and decoded (DR-8) so consumers actually receive them.
    fn fetch_records_filtered(
        &self,
        topic_id: i32,
        partition_id: i32,
        fetch_offset: i64,
        max_bytes: i32,
        read_committed: bool,
    ) -> Result<Vec<FetchedMessage>> {
        // ADR-002: Dynamic Fetch Sizing — pg_column_size() tracks actual bytes via a
        // window function; the row LIMIT is only a per-query bound, not the fetch cap.
        const ESTIMATE_BYTES_PER_MESSAGE: i64 = 500;
        const MAX_ROWS_PER_QUERY: i64 = 5_000;

        // RV-4 (ReadCommitted): filter uncommitted rows AND clamp below the Last
        // Stable Offset — a committed record above a still-open lower-offset
        // transaction must not be returned, or the consumer advances past the
        // pending offset and never re-reads it (lost/reordered delivery). With
        // nothing pending the COALESCE default makes the bound a no-op.
        let query = if read_committed {
            "SELECT partition_offset, key, value, headers::text AS headers_json,
                    CASE WHEN timestamp_ms >= 0 THEN timestamp_ms ELSE (EXTRACT(EPOCH FROM created_at) * 1000)::bigint END as timestamp_ms,
                    SUM(COALESCE(pg_column_size(key), 0) + COALESCE(pg_column_size(value), 0) + 64)
                        OVER (ORDER BY partition_offset) as cumulative_bytes
             FROM kafka.messages
             WHERE topic_id = $1 AND partition_id = $2 AND partition_offset >= $3
               AND (txn_state IS NULL)
               AND partition_offset < COALESCE(
                   (SELECT MIN(m2.partition_offset) FROM kafka.messages m2
                    WHERE m2.topic_id = $1 AND m2.partition_id = $2
                      AND m2.txn_state = 'pending'),
                   partition_offset + 1)
             ORDER BY partition_offset
             LIMIT $4"
        } else {
            "SELECT partition_offset, key, value, headers::text AS headers_json,
                    CASE WHEN timestamp_ms >= 0 THEN timestamp_ms ELSE (EXTRACT(EPOCH FROM created_at) * 1000)::bigint END as timestamp_ms,
                    SUM(COALESCE(pg_column_size(key), 0) + COALESCE(pg_column_size(value), 0) + 64)
                        OVER (ORDER BY partition_offset) as cumulative_bytes
             FROM kafka.messages
             WHERE topic_id = $1 AND partition_id = $2 AND partition_offset >= $3
             ORDER BY partition_offset
             LIMIT $4"
        };

        Spi::connect(|client| {
            let mut messages: Vec<FetchedMessage> = Vec::new();
            let max_bytes_i64 = max_bytes.max(0) as i64;
            let mut consumed: i64 = 0;
            let mut next_offset = fetch_offset;

            loop {
                let remaining = max_bytes_i64 - consumed;
                if remaining <= 0 && !messages.is_empty() {
                    break;
                }
                let limit =
                    (remaining / ESTIMATE_BYTES_PER_MESSAGE + 1).clamp(10, MAX_ROWS_PER_QUERY);

                let table = client.select(
                    query,
                    None,
                    &[
                        topic_id.into(),
                        partition_id.into(),
                        next_offset.into(),
                        limit.into(),
                    ],
                )?;

                let row_count = table.len() as i64;
                let batch_base = consumed;
                let mut budget_hit = false;

                for row in table {
                    // cumulative_bytes restarts per query; add the running total.
                    let batch_cumulative: i64 = row.get_by_name("cumulative_bytes")?.unwrap_or(0);
                    let total = batch_base + batch_cumulative;

                    // Always include at least one message even if it exceeds max_bytes
                    // (Kafka protocol behavior: one message is the minimum response).
                    if !messages.is_empty() && total > max_bytes_i64 {
                        budget_hit = true;
                        break;
                    }

                    let partition_offset: i64 = row.get_by_name("partition_offset")?.unwrap_or(0);
                    let key: Option<Vec<u8>> = row.get_by_name("key")?;
                    let value: Option<Vec<u8>> = row.get_by_name("value")?;
                    let timestamp: i64 = row.get_by_name("timestamp_ms")?.unwrap_or(0);
                    let headers_json: Option<String> = row.get_by_name("headers_json")?;
                    let headers = super::decode_headers_json(headers_json.as_deref());

                    messages.push(FetchedMessage {
                        partition_offset,
                        key,
                        value,
                        timestamp,
                        headers,
                    });
                    next_offset = partition_offset + 1;
                    consumed = total;
                }

                // Stop when the budget is spent or the partition is drained
                // (a short batch means no more matching rows past next_offset).
                if budget_hit || row_count < limit {
                    break;
                }
            }

            crate::pg_debug!(
                "Fetched {} messages ({} bytes of {} budget, read_committed={})",
                messages.len(),
                consumed,
                max_bytes_i64,
                read_committed
            );
            Ok(messages)
        })
        .map_err(|e: KafkaError| KafkaError::Internal(format!("fetch_records failed: {}", e)))
    }
}

impl Default for PostgresStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Helper function to encode bytes as hex string for JSONB storage
fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

impl KafkaStore for PostgresStore {
    fn get_or_create_topic(&self, name: &str, default_partitions: i32) -> Result<(i32, i32)> {
        crate::pg_debug!(
            "PostgresStore::get_or_create_topic: '{}' (default_partitions={})",
            name,
            default_partitions
        );

        Spi::connect_mut(|client| {
            let topic_name_string = name.to_string();

            let table = client.update(
                "INSERT INTO kafka.topics (name, partitions)
                 VALUES ($1, $2)
                 ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
                 RETURNING id, partitions",
                None,
                &[topic_name_string.into(), default_partitions.into()],
            )?;

            let row = table.first();
            let topic_id: i32 = row
                .get_by_name("id")?
                .ok_or_else(|| KafkaError::Internal("Failed to get topic ID".into()))?;
            let partition_count: i32 = row
                .get_by_name("partitions")?
                .ok_or_else(|| KafkaError::Internal("Failed to get partition count".into()))?;

            crate::pg_debug!(
                "Topic '{}' has id={}, partitions={}",
                name,
                topic_id,
                partition_count
            );
            Ok((topic_id, partition_count))
        })
        .map_err(|e: KafkaError| KafkaError::Internal(format!("get_or_create_topic failed: {}", e)))
    }

    fn get_topic_metadata(&self, names: Option<&[String]>) -> Result<Vec<TopicMetadata>> {
        crate::pg_debug!("PostgresStore::get_topic_metadata");

        Spi::connect(|client| {
            let mut topics = Vec::new();

            if let Some(topic_names) = names {
                // Fetch specific topics
                for name in topic_names {
                    let mut table = client.select(
                        "SELECT id, partitions FROM kafka.topics WHERE name = $1",
                        None,
                        &[name.clone().into()],
                    )?;

                    if let Some(row) = table.next() {
                        let id: i32 = row.get_by_name("id")?.unwrap_or(0);
                        let partitions: i32 = row.get_by_name("partitions")?.unwrap_or(1);

                        topics.push(TopicMetadata {
                            name: name.clone(),
                            id,
                            partition_count: partitions,
                        });
                    }
                }
            } else {
                // Fetch all topics
                let table =
                    client.select("SELECT id, name, partitions FROM kafka.topics", None, &[])?;

                for row in table {
                    let id: i32 = row.get_by_name("id")?.unwrap_or(0);
                    let name: String = row.get_by_name("name")?.unwrap_or_default();
                    let partitions: i32 = row.get_by_name("partitions")?.unwrap_or(1);

                    topics.push(TopicMetadata {
                        name,
                        id,
                        partition_count: partitions,
                    });
                }
            }

            Ok(topics)
        })
        .map_err(|e: KafkaError| KafkaError::Internal(format!("get_topic_metadata failed: {}", e)))
    }

    fn insert_records(&self, topic_id: i32, partition_id: i32, records: &[Record]) -> Result<i64> {
        crate::pg_debug!(
            "PostgresStore::insert_records: {} records for topic_id={}, partition_id={}",
            records.len(),
            topic_id,
            partition_id
        );
        // DR-24: shared implementation with the transactional path (txn = None).
        self.insert_records_inner(topic_id, partition_id, records, None)
    }

    fn fetch_records(
        &self,
        topic_id: i32,
        partition_id: i32,
        fetch_offset: i64,
        max_bytes: i32,
    ) -> Result<Vec<FetchedMessage>> {
        crate::pg_debug!(
            "PostgresStore::fetch_records: topic_id={}, partition_id={}, fetch_offset={}, max_bytes={}",
            topic_id,
            partition_id,
            fetch_offset,
            max_bytes
        );

        // Kept on the trait for the ShadowStore SH-6 suppression path; the consumer
        // path goes through fetch_records_with_isolation. Both share one
        // implementation (DR-24): this is the ReadUncommitted filter.
        self.fetch_records_filtered(topic_id, partition_id, fetch_offset, max_bytes, false)
    }

    fn get_high_watermark(&self, topic_id: i32, partition_id: i32) -> Result<i64> {
        Spi::connect(|client| {
            // BUG-4: the high watermark must be the log-end offset and must never regress. A plain
            // MAX(partition_offset)+1 drops when cleanup_aborted_messages deletes the highest
            // (aborted) rows, reporting a HWM below offsets already handed out. Use the never-
            // decreasing per-partition counter (kafka.partition_offsets, the same source produce
            // assigns from — see BUG-3), taking GREATEST with MAX+1 as a backstop for partitions
            // that predate the counter. Read isolation (read_committed vs read_uncommitted) is
            // unaffected: that is decided by the fetch row filter and the last-stable-offset, not by
            // the HWM bound reported here.
            let table = client.select(
                "SELECT GREATEST(
                     COALESCE((SELECT next_offset FROM kafka.partition_offsets
                               WHERE topic_id = $1 AND partition_id = $2), 0),
                     COALESCE((SELECT MAX(partition_offset) + 1 FROM kafka.messages
                               WHERE topic_id = $1 AND partition_id = $2), 0)
                 ) AS high_watermark",
                None,
                &[topic_id.into(), partition_id.into()],
            )?;

            let high_watermark: i64 = table.first().get_by_name("high_watermark")?.unwrap_or(0);

            Ok(high_watermark)
        })
        .map_err(|e: KafkaError| KafkaError::Internal(format!("get_high_watermark failed: {}", e)))
    }

    fn get_earliest_offset(&self, topic_id: i32, partition_id: i32) -> Result<i64> {
        Spi::connect(|client| {
            // Earliest = GREATEST(smallest remaining offset, durable log start).
            // The durable log start (kafka.partition_offsets.log_start_offset,
            // advanced by DeleteRecords) dominates only when the partition has
            // been emptied by truncation — otherwise MIN(partition_offset) is at
            // or above it. For a never-truncated partition log_start is 0, so
            // this is unchanged. The LEFT JOIN covers a partition with rows but
            // no counter row (log_start defaults to 0).
            let table = client.select(
                "SELECT GREATEST(
                            COALESCE((SELECT MIN(partition_offset) FROM kafka.messages
                                      WHERE topic_id = $1 AND partition_id = $2), 0),
                            COALESCE((SELECT log_start_offset FROM kafka.partition_offsets
                                      WHERE topic_id = $1 AND partition_id = $2), 0)
                        ) AS earliest_offset",
                None,
                &[topic_id.into(), partition_id.into()],
            )?;

            let earliest_offset: i64 = table.first().get_by_name("earliest_offset")?.unwrap_or(0);

            Ok(earliest_offset)
        })
        .map_err(|e: KafkaError| KafkaError::Internal(format!("get_earliest_offset failed: {}", e)))
    }

    fn get_offset_for_timestamp(
        &self,
        topic_id: i32,
        partition_id: i32,
        timestamp_ms: i64,
    ) -> Result<Option<(i64, i64)>> {
        Spi::connect(|client| {
            // CONF-7: the lowest committed offset whose stored producer timestamp (BUG-7) is at or
            // after the target. Records with no timestamp (-1) are excluded; pending/aborted txn
            // rows are excluded so the lookup matches read-committed visibility.
            let table = client.select(
                "SELECT partition_offset, timestamp_ms
                 FROM kafka.messages
                 WHERE topic_id = $1 AND partition_id = $2
                   AND timestamp_ms >= 0 AND timestamp_ms >= $3
                   AND txn_state IS NULL
                 ORDER BY partition_offset ASC
                 LIMIT 1",
                None,
                &[topic_id.into(), partition_id.into(), timestamp_ms.into()],
            )?;

            let row = table.first();
            match row.get_by_name::<i64, _>("partition_offset")? {
                Some(offset) => {
                    let ts: i64 = row.get_by_name("timestamp_ms")?.unwrap_or(-1);
                    Ok(Some((offset, ts)))
                }
                None => Ok(None),
            }
        })
        .map_err(|e: KafkaError| {
            KafkaError::Internal(format!("get_offset_for_timestamp failed: {}", e))
        })
    }

    fn commit_offset(
        &self,
        group_id: &str,
        topic_id: i32,
        partition_id: i32,
        offset: i64,
        metadata: Option<&str>,
    ) -> Result<()> {
        crate::pg_debug!(
            "PostgresStore::commit_offset: group_id={}, topic_id={}, partition_id={}, offset={}",
            group_id,
            topic_id,
            partition_id,
            offset
        );
        Spi::connect_mut(|client| {
            let query = "INSERT INTO kafka.consumer_offsets
                            (group_id, topic_id, partition_id, committed_offset, metadata)
                        VALUES ($1, $2, $3, $4, $5)
                        ON CONFLICT (group_id, topic_id, partition_id)
                        DO UPDATE SET
                            committed_offset = EXCLUDED.committed_offset,
                            metadata = EXCLUDED.metadata,
                            commit_timestamp = NOW()";

            client.update(
                query,
                None,
                &[
                    group_id.into(),
                    topic_id.into(),
                    partition_id.into(),
                    offset.into(),
                    metadata.into(),
                ],
            )?;
            Ok(())
        })
        .map_err(|e: KafkaError| KafkaError::Internal(format!("commit_offset failed: {}", e)))
    }

    fn fetch_offset(
        &self,
        group_id: &str,
        topic_id: i32,
        partition_id: i32,
    ) -> Result<Option<CommittedOffset>> {
        Spi::connect(|client| {
            let query = "SELECT committed_offset, metadata
                 FROM kafka.consumer_offsets
                 WHERE group_id = $1 AND topic_id = $2 AND partition_id = $3";

            let mut table = client.select(
                query,
                Some(1),
                &[group_id.into(), topic_id.into(), partition_id.into()],
            )?;

            if let Some(row) = table.next() {
                let offset: i64 = row.get_by_name("committed_offset")?.unwrap_or(-1);
                let metadata: Option<String> = row.get_by_name("metadata")?;

                Ok(Some(CommittedOffset { offset, metadata }))
            } else {
                Ok(None)
            }
        })
        .map_err(|e: KafkaError| KafkaError::Internal(format!("fetch_offset failed: {}", e)))
    }

    fn fetch_all_offsets(&self, group_id: &str) -> Result<Vec<(String, i32, CommittedOffset)>> {
        Spi::connect(|client| {
            let query = "SELECT t.name, co.partition_id, co.committed_offset, co.metadata
                 FROM kafka.consumer_offsets co
                 JOIN kafka.topics t ON co.topic_id = t.id
                 WHERE co.group_id = $1
                 ORDER BY t.name, co.partition_id";

            let table = client.select(query, None, &[group_id.into()])?;
            let mut results = Vec::new();

            for row in table {
                let topic_name: String = row.get_by_name("name")?.unwrap_or_default();
                let partition_id: i32 = row.get_by_name("partition_id")?.unwrap_or(0);
                let offset: i64 = row.get_by_name("committed_offset")?.unwrap_or(-1);
                let metadata: Option<String> = row.get_by_name("metadata")?;

                results.push((
                    topic_name,
                    partition_id,
                    CommittedOffset { offset, metadata },
                ));
            }

            Ok(results)
        })
        .map_err(|e: KafkaError| KafkaError::Internal(format!("fetch_all_offsets failed: {}", e)))
    }

    // ===== Admin Topic Operations (Phase 6) =====

    fn topic_exists(&self, name: &str) -> Result<bool> {
        crate::pg_debug!("PostgresStore::topic_exists: '{}'", name);

        Spi::connect(|client| {
            let table = client.select(
                "SELECT 1 FROM kafka.topics WHERE name = $1",
                Some(1),
                &[name.into()],
            )?;

            Ok(!table.is_empty())
        })
        .map_err(|e: KafkaError| KafkaError::Internal(format!("topic_exists failed: {}", e)))
    }

    fn create_topic(&self, name: &str, partition_count: i32) -> Result<i32> {
        crate::pg_debug!(
            "PostgresStore::create_topic: '{}' with {} partitions",
            name,
            partition_count
        );

        Spi::connect_mut(|client| {
            let table = client.update(
                "INSERT INTO kafka.topics (name, partitions)
                 VALUES ($1, $2)
                 RETURNING id",
                None,
                &[name.into(), partition_count.into()],
            )?;

            let topic_id: i32 = table
                .first()
                .get_by_name("id")?
                .ok_or_else(|| KafkaError::Internal("Failed to get topic ID".into()))?;

            crate::pg_debug!("Created topic '{}' with id={}", name, topic_id);
            Ok(topic_id)
        })
        .map_err(|e: KafkaError| KafkaError::Internal(format!("create_topic failed: {}", e)))
    }

    fn get_topic_id(&self, name: &str) -> Result<Option<i32>> {
        crate::pg_debug!("PostgresStore::get_topic_id: '{}'", name);

        Spi::connect(|client| {
            let mut table = client.select(
                "SELECT id FROM kafka.topics WHERE name = $1",
                Some(1),
                &[name.into()],
            )?;

            if let Some(row) = table.next() {
                let id: i32 = row.get_by_name("id")?.unwrap_or(0);
                Ok(Some(id))
            } else {
                Ok(None)
            }
        })
        .map_err(|e: KafkaError| KafkaError::Internal(format!("get_topic_id failed: {}", e)))
    }

    fn delete_topic(&self, topic_id: i32) -> Result<()> {
        crate::pg_debug!("PostgresStore::delete_topic: topic_id={}", topic_id);

        Spi::connect_mut(|client| {
            // Delete messages first (foreign key constraint)
            client.update(
                "DELETE FROM kafka.messages WHERE topic_id = $1",
                None,
                &[topic_id.into()],
            )?;

            // Delete consumer offsets
            client.update(
                "DELETE FROM kafka.consumer_offsets WHERE topic_id = $1",
                None,
                &[topic_id.into()],
            )?;

            // Delete topic
            client.update(
                "DELETE FROM kafka.topics WHERE id = $1",
                None,
                &[topic_id.into()],
            )?;

            crate::pg_debug!("Deleted topic with id={}", topic_id);
            Ok(())
        })
        .map_err(|e: KafkaError| KafkaError::Internal(format!("delete_topic failed: {}", e)))
    }

    fn get_topic_partition_count(&self, name: &str) -> Result<Option<i32>> {
        crate::pg_debug!("PostgresStore::get_topic_partition_count: '{}'", name);

        Spi::connect(|client| {
            let mut table = client.select(
                "SELECT partitions FROM kafka.topics WHERE name = $1",
                Some(1),
                &[name.into()],
            )?;

            if let Some(row) = table.next() {
                let partitions: i32 = row.get_by_name("partitions")?.unwrap_or(1);
                Ok(Some(partitions))
            } else {
                Ok(None)
            }
        })
        .map_err(|e: KafkaError| {
            KafkaError::Internal(format!("get_topic_partition_count failed: {}", e))
        })
    }

    fn set_topic_partition_count(&self, name: &str, partition_count: i32) -> Result<()> {
        crate::pg_debug!(
            "PostgresStore::set_topic_partition_count: '{}' to {}",
            name,
            partition_count
        );

        Spi::connect_mut(|client| {
            client.update(
                "UPDATE kafka.topics SET partitions = $2 WHERE name = $1",
                None,
                &[name.into(), partition_count.into()],
            )?;

            crate::pg_debug!(
                "Updated topic '{}' partition count to {}",
                name,
                partition_count
            );
            Ok(())
        })
        .map_err(|e: KafkaError| {
            KafkaError::Internal(format!("set_topic_partition_count failed: {}", e))
        })
    }

    // ===== Admin Consumer Group Operations (Phase 6) =====

    fn delete_consumer_group_offsets(&self, group_id: &str) -> Result<()> {
        crate::pg_debug!(
            "PostgresStore::delete_consumer_group_offsets: '{}'",
            group_id
        );

        Spi::connect_mut(|client| {
            client.update(
                "DELETE FROM kafka.consumer_offsets WHERE group_id = $1",
                None,
                &[group_id.into()],
            )?;

            crate::pg_debug!("Deleted consumer offsets for group '{}'", group_id);
            Ok(())
        })
        .map_err(|e: KafkaError| {
            KafkaError::Internal(format!("delete_consumer_group_offsets failed: {}", e))
        })
    }

    // ===== Idempotent Producer Operations (Phase 9) =====

    fn allocate_producer_id(
        &self,
        client_id: Option<&str>,
        transactional_id: Option<&str>,
    ) -> Result<(i64, i16)> {
        crate::pg_debug!(
            "PostgresStore::allocate_producer_id: client_id={:?}, transactional_id={:?}",
            client_id,
            transactional_id
        );

        Spi::connect_mut(|client| {
            let table = client.update(
                "INSERT INTO kafka.producer_ids (client_id, transactional_id, epoch)
                 VALUES ($1, $2, 0)
                 RETURNING producer_id, epoch",
                None,
                &[client_id.into(), transactional_id.into()],
            )?;

            let row = table.first();
            let producer_id: i64 = row
                .get_by_name("producer_id")?
                .ok_or_else(|| KafkaError::Internal("Failed to get producer_id".into()))?;
            let epoch: i16 = row.get_by_name("epoch")?.unwrap_or(0);

            crate::pg_debug!("Allocated producer_id={}, epoch={}", producer_id, epoch);
            Ok((producer_id, epoch))
        })
        .map_err(|e: KafkaError| {
            KafkaError::Internal(format!("allocate_producer_id failed: {}", e))
        })
    }

    fn get_producer_epoch(&self, producer_id: i64) -> Result<Option<i16>> {
        crate::pg_debug!(
            "PostgresStore::get_producer_epoch: producer_id={}",
            producer_id
        );

        Spi::connect(|client| {
            let mut table = client.select(
                "SELECT epoch FROM kafka.producer_ids WHERE producer_id = $1",
                Some(1),
                &[producer_id.into()],
            )?;

            if let Some(row) = table.next() {
                let epoch: i16 = row.get_by_name("epoch")?.unwrap_or(0);
                Ok(Some(epoch))
            } else {
                Ok(None)
            }
        })
        .map_err(|e: KafkaError| KafkaError::Internal(format!("get_producer_epoch failed: {}", e)))
    }

    fn increment_producer_epoch(&self, producer_id: i64) -> Result<i16> {
        crate::pg_debug!(
            "PostgresStore::increment_producer_epoch: producer_id={}",
            producer_id
        );

        Spi::connect_mut(|client| {
            let table = client.update(
                "UPDATE kafka.producer_ids
                 SET epoch = epoch + 1, last_active_at = NOW()
                 WHERE producer_id = $1
                 RETURNING epoch",
                None,
                &[producer_id.into()],
            )?;

            if table.is_empty() {
                return Err(KafkaError::unknown_producer_id(producer_id));
            }

            let new_epoch: i16 = table
                .first()
                .get_by_name("epoch")?
                .ok_or_else(|| KafkaError::Internal("Failed to get new epoch".into()))?;

            crate::pg_debug!(
                "Incremented producer_id={} to epoch={}",
                producer_id,
                new_epoch
            );
            Ok(new_epoch)
        })
        .map_err(|e| match e {
            KafkaError::UnknownProducerId { .. } => e,
            _ => KafkaError::Internal(format!("increment_producer_epoch failed: {}", e)),
        })
    }

    fn check_and_update_sequence(
        &self,
        producer_id: i64,
        producer_epoch: i16,
        topic_id: i32,
        partition_id: i32,
        base_sequence: i32,
        record_count: i32,
    ) -> Result<bool> {
        crate::pg_debug!(
            "PostgresStore::check_and_update_sequence: producer_id={}, epoch={}, topic_id={}, partition_id={}, base_seq={}, count={}",
            producer_id,
            producer_epoch,
            topic_id,
            partition_id,
            base_sequence,
            record_count
        );

        Spi::connect_mut(|client| {
            // Step 1: Verify producer epoch (fencing check)
            let epoch_table = client.select(
                "SELECT epoch FROM kafka.producer_ids WHERE producer_id = $1",
                Some(1),
                &[producer_id.into()],
            )?;

            if epoch_table.is_empty() {
                return Err(KafkaError::unknown_producer_id(producer_id));
            }

            let current_epoch: i16 = epoch_table
                .first()
                .get_by_name("epoch")?
                .unwrap_or(0);

            if producer_epoch < current_epoch {
                return Err(KafkaError::producer_fenced(
                    producer_id,
                    producer_epoch,
                    current_epoch,
                ));
            }

            // Step 2: Use advisory lock to serialize sequence checks for this producer+partition
            // Combine producer_id, topic_id, partition_id into a single bigint lock key
            // Using XOR and shifts to create a unique hash that fits in i64
            let lock_key: i64 = producer_id
                ^ ((topic_id as i64) << 20)
                ^ ((partition_id as i64) << 40);
            client.select(
                "SELECT pg_advisory_xact_lock($1)",
                None,
                &[lock_key.into()],
            )?;

            // Step 3: Get current last_sequence for this producer/topic/partition
            let seq_table = client.select(
                "SELECT last_sequence FROM kafka.producer_sequences
                 WHERE producer_id = $1 AND topic_id = $2 AND partition_id = $3",
                Some(1),
                &[producer_id.into(), topic_id.into(), partition_id.into()],
            )?;

            let last_sequence: i32 = if seq_table.is_empty() {
                -1 // First batch for this producer/partition
            } else {
                seq_table
                    .first()
                    .get_by_name("last_sequence")?
                    .unwrap_or(-1)
            };

            // Step 4: Validate the sequence range (pure logic in storage::validate_sequence).
            // A batch is only a duplicate if it is ENTIRELY at or behind last_sequence;
            // a partially overlapping batch must be rejected, not silently skipped.
            //
            // RV-8: the stored last_sequence is NOT advanced here. record_producer_sequence
            // persists it AFTER a successful insert, so a failed insert leaves the sequence
            // un-advanced and a retry of the same batch re-inserts instead of being dropped
            // as a false duplicate.
            match super::validate_sequence(last_sequence, base_sequence, record_count) {
                super::SequenceValidation::Duplicate => {
                    // Kafka spec: exact replay returns success, skip insert.
                    crate::pg_debug!(
                        "Duplicate detected: producer_id={}, partition_id={}, base_sequence={}, last_sequence={}",
                        producer_id,
                        partition_id,
                        base_sequence,
                        last_sequence
                    );
                    Ok(false) // Duplicate - skip insert
                }
                super::SequenceValidation::OutOfOrder { expected_sequence } => {
                    Err(KafkaError::out_of_order_sequence(
                        producer_id,
                        partition_id,
                        base_sequence,
                        expected_sequence,
                    ))
                }
                super::SequenceValidation::Proceed { .. } => Ok(true), // valid - proceed with insert
            }
        })
        .map_err(|e| match e {
            KafkaError::UnknownProducerId { .. }
            | KafkaError::ProducerFenced { .. }
            | KafkaError::DuplicateSequence { .. }
            | KafkaError::OutOfOrderSequence { .. } => e,
            _ => KafkaError::Internal(format!("check_and_update_sequence failed: {}", e)),
        })
    }

    fn record_producer_sequence(
        &self,
        producer_id: i64,
        topic_id: i32,
        partition_id: i32,
        last_sequence: i32,
    ) -> Result<()> {
        // RV-8: persist the advanced last_sequence, called only AFTER the records
        // are durably inserted (runs in the same request subtransaction).
        Spi::connect_mut(|client| {
            client.update(
                "INSERT INTO kafka.producer_sequences (producer_id, topic_id, partition_id, last_sequence)
                 VALUES ($1, $2, $3, $4)
                 ON CONFLICT (producer_id, topic_id, partition_id)
                 DO UPDATE SET last_sequence = EXCLUDED.last_sequence, updated_at = NOW()",
                None,
                &[
                    producer_id.into(),
                    topic_id.into(),
                    partition_id.into(),
                    last_sequence.into(),
                ],
            )?;
            Ok(())
        })
        .map_err(|e: KafkaError| {
            KafkaError::Internal(format!("record_producer_sequence failed: {}", e))
        })
    }

    // ===== Transaction Operations (Phase 10) =====

    fn get_or_create_transactional_producer(
        &self,
        transactional_id: &str,
        transaction_timeout_ms: i32,
        client_id: Option<&str>,
    ) -> Result<(i64, i16)> {
        crate::pg_debug!(
            "PostgresStore::get_or_create_transactional_producer: transactional_id={}, timeout_ms={}",
            transactional_id,
            transaction_timeout_ms
        );

        Spi::connect_mut(|client| {
            // Look up existing producer by transactional_id
            let existing = client.select(
                "SELECT producer_id, epoch FROM kafka.producer_ids WHERE transactional_id = $1",
                Some(1),
                &[transactional_id.into()],
            )?;

            let (producer_id, epoch) = if existing.is_empty() {
                // New transactional producer - allocate producer_id
                let table = client.update(
                    "INSERT INTO kafka.producer_ids (client_id, transactional_id, epoch)
                     VALUES ($1, $2, 0)
                     RETURNING producer_id, epoch",
                    None,
                    &[client_id.into(), transactional_id.into()],
                )?;

                let row = table.first();
                let producer_id: i64 = row
                    .get_by_name("producer_id")?
                    .ok_or_else(|| KafkaError::Internal("Failed to get producer_id".into()))?;
                let epoch: i16 = row.get_by_name("epoch")?.unwrap_or(0);

                crate::pg_debug!(
                    "Allocated new transactional producer_id={}, epoch={}",
                    producer_id,
                    epoch
                );
                (producer_id, epoch)
            } else {
                // Existing transactional producer - bump epoch (fences old producer)
                let row = existing.first();
                let producer_id: i64 = row.get_by_name("producer_id")?.unwrap_or(0);
                let old_epoch: i16 = row.get_by_name("epoch")?.unwrap_or(0);

                // RV-2: InitProducerId must abort any transaction still in flight for this
                // transactional_id (the canonical producer crash-restart path). Otherwise the
                // previous epoch's `pending` records are stranded forever — commit/abort filter
                // on the new epoch and require state='Ongoing', the timeout sweep scans
                // state='Ongoing', and cleanup only touches 'aborted' — so nothing reclaims
                // them and get_last_stable_offset (MIN pending offset) pins the LSO permanently
                // (a hanging transaction). Abort the old-epoch pending records here, before the
                // bump. Harmless (matches nothing) when no transaction was in flight.
                client.update(
                    "UPDATE kafka.messages SET txn_state = 'aborted'
                     WHERE producer_id = $1 AND producer_epoch = $2 AND txn_state = 'pending'",
                    None,
                    &[producer_id.into(), old_epoch.into()],
                )?;
                client.update(
                    "DELETE FROM kafka.txn_pending_offsets WHERE transactional_id = $1",
                    None,
                    &[transactional_id.into()],
                )?;

                let table = if old_epoch == i16::MAX {
                    // RV-13: SMALLINT epoch exhausted. `epoch + 1` would overflow the
                    // column and fail InitProducerId forever for this transactional_id.
                    // Kafka handles exhaustion by allocating a fresh producer_id. Reassign
                    // this row's producer_id from the BIGSERIAL sequence and reset the epoch
                    // to 0. transactional_id is UNIQUE (one row), and the transactions FK is
                    // ON UPDATE CASCADE, so the transaction row's producer_id follows the new
                    // id automatically.
                    client.update(
                        "UPDATE kafka.producer_ids
                         SET producer_id = nextval(pg_get_serial_sequence('kafka.producer_ids', 'producer_id')),
                             epoch = 0, last_active_at = NOW()
                         WHERE producer_id = $1
                         RETURNING producer_id, epoch",
                        None,
                        &[producer_id.into()],
                    )?
                } else {
                    client.update(
                        "UPDATE kafka.producer_ids
                         SET epoch = epoch + 1, last_active_at = NOW()
                         WHERE producer_id = $1
                         RETURNING producer_id, epoch",
                        None,
                        &[producer_id.into()],
                    )?
                };

                let updated = table.first();
                let new_producer_id: i64 =
                    updated.get_by_name("producer_id")?.unwrap_or(producer_id);
                let new_epoch: i16 = updated
                    .get_by_name("epoch")?
                    .ok_or_else(|| KafkaError::Internal("Failed to get new epoch".into()))?;

                crate::pg_debug!(
                    "Existing transactional producer_id={} -> producer_id={}, new_epoch={}",
                    producer_id,
                    new_producer_id,
                    new_epoch
                );
                (new_producer_id, new_epoch)
            };

            // Create or update transaction record with state='Empty'
            client.update(
                "INSERT INTO kafka.transactions (transactional_id, producer_id, producer_epoch, state, timeout_ms)
                 VALUES ($1, $2, $3, 'Empty', $4)
                 ON CONFLICT (transactional_id) DO UPDATE SET
                     producer_id = EXCLUDED.producer_id,
                     producer_epoch = EXCLUDED.producer_epoch,
                     state = 'Empty',
                     timeout_ms = EXCLUDED.timeout_ms,
                     started_at = NULL,
                     last_updated_at = NOW()",
                None,
                &[
                    transactional_id.into(),
                    producer_id.into(),
                    epoch.into(),
                    transaction_timeout_ms.into(),
                ],
            )?;

            Ok((producer_id, epoch))
        })
        .map_err(|e: KafkaError| {
            KafkaError::Internal(format!("get_or_create_transactional_producer failed: {}", e))
        })
    }

    fn begin_transaction(
        &self,
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
    ) -> Result<()> {
        crate::pg_debug!(
            "PostgresStore::begin_transaction: transactional_id={}, producer_id={}, epoch={}",
            transactional_id,
            producer_id,
            producer_epoch
        );

        Spi::connect_mut(|client| {
            // Verify ownership and update state to 'Ongoing'
            let table = client.update(
                "UPDATE kafka.transactions
                 SET state = 'Ongoing', started_at = NOW(), last_updated_at = NOW()
                 WHERE transactional_id = $1 AND producer_id = $2 AND producer_epoch = $3
                   AND state IN ('Empty', 'CompleteCommit', 'CompleteAbort')
                 RETURNING state",
                None,
                &[transactional_id.into(), producer_id.into(), producer_epoch.into()],
            )?;

            if table.is_empty() {
                // Check if transaction exists with different producer/epoch
                let exists = client.select(
                    "SELECT state, producer_id, producer_epoch FROM kafka.transactions WHERE transactional_id = $1",
                    Some(1),
                    &[transactional_id.into()],
                )?;

                if exists.is_empty() {
                    return Err(KafkaError::transactional_id_not_found(transactional_id));
                }

                let row = exists.first();
                let current_producer_id: i64 = row.get_by_name("producer_id")?.unwrap_or(0);
                let current_epoch: i16 = row.get_by_name("producer_epoch")?.unwrap_or(0);
                let state: String = row.get_by_name("state")?.unwrap_or_default();

                if current_producer_id != producer_id || current_epoch != producer_epoch {
                    return Err(KafkaError::producer_fenced(producer_id, producer_epoch, current_epoch));
                }

                // State is not valid for beginning a transaction
                return Err(KafkaError::invalid_txn_state(
                    transactional_id,
                    "Empty, CompleteCommit, or CompleteAbort",
                    &state,
                ));
            }

            crate::pg_debug!("Transaction {} started", transactional_id);
            Ok(())
        })
        .map_err(|e| match e {
            KafkaError::TransactionalIdNotFound { .. }
            | KafkaError::ProducerFenced { .. }
            | KafkaError::InvalidTxnState { .. } => e,
            _ => KafkaError::Internal(format!("begin_transaction failed: {}", e)),
        })
    }

    fn begin_or_continue_transaction(
        &self,
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
    ) -> Result<()> {
        crate::pg_debug!(
            "PostgresStore::begin_or_continue_transaction: transactional_id={}, producer_id={}, epoch={}",
            transactional_id,
            producer_id,
            producer_epoch
        );

        Spi::connect_mut(|client| {
            // Atomic compare-and-swap: transition to 'Ongoing' if in valid starting state
            // OR continue if already 'Ongoing' with matching producer
            let updated = client.update(
                "UPDATE kafka.transactions
                 SET state = 'Ongoing',
                     started_at = COALESCE(started_at, NOW()),
                     last_updated_at = NOW()
                 WHERE transactional_id = $1
                   AND producer_id = $2
                   AND producer_epoch = $3
                   AND state IN ('Empty', 'CompleteCommit', 'CompleteAbort', 'Ongoing')
                 RETURNING state",
                None,
                &[transactional_id.into(), producer_id.into(), producer_epoch.into()],
            )?;

            if !updated.is_empty() {
                crate::pg_debug!("Transaction {} is now Ongoing", transactional_id);
                return Ok(());
            }

            // Update failed - check why
            let exists = client.select(
                "SELECT state, producer_id, producer_epoch FROM kafka.transactions WHERE transactional_id = $1",
                Some(1),
                &[transactional_id.into()],
            )?;

            if exists.is_empty() {
                return Err(KafkaError::transactional_id_not_found(transactional_id));
            }

            let row = exists.first();
            let current_producer_id: i64 =
                row.get_by_name("producer_id")?.ok_or_else(|| KafkaError::Database {
                    message: "Unexpected NULL in transactions.producer_id column".to_string(),
                })?;
            let current_epoch: i16 =
                row.get_by_name("producer_epoch")?.ok_or_else(|| KafkaError::Database {
                    message: "Unexpected NULL in transactions.producer_epoch column".to_string(),
                })?;
            let state: String = row.get_by_name("state")?.ok_or_else(|| KafkaError::Database {
                message: "Unexpected NULL in transactions.state column".to_string(),
            })?;

            if current_producer_id != producer_id || current_epoch != producer_epoch {
                return Err(KafkaError::producer_fenced(
                    producer_id,
                    producer_epoch,
                    current_epoch,
                ));
            }

            // State is not valid for beginning a transaction
            Err(KafkaError::invalid_txn_state(
                transactional_id,
                "Empty, CompleteCommit, or CompleteAbort",
                &state,
            ))
        })
        .map_err(|e| match e {
            KafkaError::TransactionalIdNotFound { .. }
            | KafkaError::ProducerFenced { .. }
            | KafkaError::InvalidTxnState { .. } => e,
            _ => KafkaError::Internal(format!("begin_or_continue_transaction failed: {}", e)),
        })
    }

    fn validate_transaction(
        &self,
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
    ) -> Result<()> {
        crate::pg_debug!(
            "PostgresStore::validate_transaction: transactional_id={}, producer_id={}, epoch={}",
            transactional_id,
            producer_id,
            producer_epoch
        );

        Spi::connect(|client| {
            let table = client.select(
                "SELECT state, producer_id, producer_epoch FROM kafka.transactions WHERE transactional_id = $1",
                Some(1),
                &[transactional_id.into()],
            )?;

            if table.is_empty() {
                return Err(KafkaError::transactional_id_not_found(transactional_id));
            }

            let row = table.first();
            let current_producer_id: i64 = row.get_by_name("producer_id")?.unwrap_or(0);
            let current_epoch: i16 = row.get_by_name("producer_epoch")?.unwrap_or(0);
            let state: String = row.get_by_name("state")?.unwrap_or_default();

            if current_producer_id != producer_id || current_epoch != producer_epoch {
                return Err(KafkaError::producer_fenced(producer_id, producer_epoch, current_epoch));
            }

            // Note: We only validate ownership here, not state.
            // State validation is done by the handlers which know the expected states
            // for their specific operations (e.g., AddPartitionsToTxn allows Empty or Ongoing).
            let _ = state; // Acknowledge we read state but don't check it here

            Ok(())
        })
        .map_err(|e| match e {
            KafkaError::TransactionalIdNotFound { .. }
            | KafkaError::ProducerFenced { .. }
            | KafkaError::InvalidTxnState { .. } => e,
            _ => KafkaError::Internal(format!("validate_transaction failed: {}", e)),
        })
    }

    fn insert_transactional_records(
        &self,
        topic_id: i32,
        partition_id: i32,
        records: &[Record],
        producer_id: i64,
        producer_epoch: i16,
    ) -> Result<i64> {
        crate::pg_debug!(
            "PostgresStore::insert_transactional_records: {} records for topic_id={}, partition_id={}, producer_id={}",
            records.len(),
            topic_id,
            partition_id,
            producer_id
        );
        // DR-24: shared implementation with the plain path; Some(..) stamps the
        // transaction columns and txn_state='pending'.
        self.insert_records_inner(
            topic_id,
            partition_id,
            records,
            Some((producer_id, producer_epoch)),
        )
    }

    fn store_txn_pending_offset(
        &self,
        transactional_id: &str,
        group_id: &str,
        topic_id: i32,
        partition_id: i32,
        offset: i64,
        metadata: Option<&str>,
    ) -> Result<()> {
        crate::pg_debug!(
            "PostgresStore::store_txn_pending_offset: txn_id={}, group_id={}, topic_id={}, partition_id={}, offset={}",
            transactional_id,
            group_id,
            topic_id,
            partition_id,
            offset
        );

        Spi::connect_mut(|client| {
            client.update(
                "INSERT INTO kafka.txn_pending_offsets (transactional_id, group_id, topic_id, partition_id, pending_offset, metadata)
                 VALUES ($1, $2, $3, $4, $5, $6)
                 ON CONFLICT (transactional_id, group_id, topic_id, partition_id)
                 DO UPDATE SET pending_offset = EXCLUDED.pending_offset, metadata = EXCLUDED.metadata",
                None,
                &[
                    transactional_id.into(),
                    group_id.into(),
                    topic_id.into(),
                    partition_id.into(),
                    offset.into(),
                    metadata.into(),
                ],
            )?;
            Ok(())
        })
        .map_err(|e: KafkaError| {
            KafkaError::Internal(format!("store_txn_pending_offset failed: {}", e))
        })
    }

    fn commit_transaction(
        &self,
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
    ) -> Result<()> {
        self.end_transaction(transactional_id, producer_id, producer_epoch, true)
    }

    fn abort_transaction(
        &self,
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
    ) -> Result<()> {
        self.end_transaction(transactional_id, producer_id, producer_epoch, false)
    }

    fn get_transaction_state(&self, transactional_id: &str) -> Result<Option<TransactionState>> {
        crate::pg_debug!(
            "PostgresStore::get_transaction_state: transactional_id={}",
            transactional_id
        );

        Spi::connect(|client| {
            let mut table = client.select(
                "SELECT state FROM kafka.transactions WHERE transactional_id = $1",
                Some(1),
                &[transactional_id.into()],
            )?;

            if let Some(row) = table.next() {
                let state_str: String = row.get_by_name("state")?.unwrap_or_default();
                Ok(TransactionState::parse(&state_str))
            } else {
                Ok(None)
            }
        })
        .map_err(|e: KafkaError| {
            KafkaError::Internal(format!("get_transaction_state failed: {}", e))
        })
    }

    fn fetch_records_with_isolation(
        &self,
        topic_id: i32,
        partition_id: i32,
        fetch_offset: i64,
        max_bytes: i32,
        isolation_level: IsolationLevel,
    ) -> Result<Vec<FetchedMessage>> {
        crate::pg_debug!(
            "PostgresStore::fetch_records_with_isolation: topic_id={}, partition_id={}, offset={}, isolation={:?}",
            topic_id,
            partition_id,
            fetch_offset,
            isolation_level
        );

        // DR-24: both isolation levels share fetch_records_filtered; ReadCommitted
        // adds the txn_state filter + RV-4 LSO clamp inside the shared query.
        let read_committed = matches!(isolation_level, IsolationLevel::ReadCommitted);
        self.fetch_records_filtered(
            topic_id,
            partition_id,
            fetch_offset,
            max_bytes,
            read_committed,
        )
    }

    fn get_last_stable_offset(&self, topic_id: i32, partition_id: i32) -> Result<i64> {
        crate::pg_debug!(
            "PostgresStore::get_last_stable_offset: topic_id={}, partition_id={}",
            topic_id,
            partition_id
        );

        Spi::connect(|client| {
            // Get the minimum offset of pending messages, or the high watermark if
            // none. RV-10/BUG-4: when there are no pending rows the LSO equals the
            // HWM, which must never regress. A plain MAX(partition_offset)+1 drops
            // when cleanup_aborted_messages deletes the highest (aborted) rows,
            // reporting an LSO below offsets already handed out. Use the same
            // GREATEST(monotonic next_offset counter, MAX+1) the HWM uses so the two
            // agree and neither regresses after cleanup.
            let table = client.select(
                "SELECT COALESCE(
                    (SELECT MIN(partition_offset) FROM kafka.messages
                     WHERE topic_id = $1 AND partition_id = $2 AND txn_state = 'pending'),
                    GREATEST(
                        COALESCE((SELECT next_offset FROM kafka.partition_offsets
                                  WHERE topic_id = $1 AND partition_id = $2), 0),
                        COALESCE((SELECT MAX(partition_offset) + 1 FROM kafka.messages
                                  WHERE topic_id = $1 AND partition_id = $2), 0)
                    )
                 ) as lso",
                None,
                &[topic_id.into(), partition_id.into()],
            )?;

            let lso: i64 = table.first().get_by_name("lso")?.unwrap_or(0);
            Ok(lso)
        })
        .map_err(|e: KafkaError| {
            KafkaError::Internal(format!("get_last_stable_offset failed: {}", e))
        })
    }

    fn abort_timed_out_transactions(&self, timeout: Duration) -> Result<Vec<String>> {
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

                // Update transaction state
                client.update(
                    "UPDATE kafka.transactions SET state = 'CompleteAbort', last_updated_at = NOW()
                     WHERE transactional_id = $1",
                    None,
                    &[txn_id.clone().into()],
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

    fn cleanup_aborted_messages(&self, older_than: Duration) -> Result<u64> {
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

    fn get_topic_retention_ms(&self, topic_id: i32) -> Result<Option<i64>> {
        Spi::connect(|client| {
            let table = client.select(
                "SELECT retention_ms FROM kafka.topics WHERE id = $1",
                Some(1),
                &[topic_id.into()],
            )?;
            if table.is_empty() {
                return Err(KafkaError::Internal(format!(
                    "topic id {} not found",
                    topic_id
                )));
            }
            Ok(table.first().get_by_name::<i64, _>("retention_ms")?)
        })
        .map_err(|e: KafkaError| {
            KafkaError::Internal(format!("get_topic_retention_ms failed: {}", e))
        })
    }

    fn set_topic_retention_ms(&self, topic_id: i32, retention_ms: Option<i64>) -> Result<()> {
        crate::pg_debug!(
            "PostgresStore::set_topic_retention_ms: topic_id={}, retention_ms={:?}",
            topic_id,
            retention_ms
        );
        Spi::connect_mut(|client| {
            client.update(
                "UPDATE kafka.topics SET retention_ms = $2 WHERE id = $1",
                None,
                &[topic_id.into(), retention_ms.into()],
            )?;
            Ok(())
        })
        .map_err(|e: KafkaError| {
            KafkaError::Internal(format!("set_topic_retention_ms failed: {}", e))
        })
    }

    fn delete_records_before(
        &self,
        topic_id: i32,
        partition_id: i32,
        before_offset: i64,
    ) -> Result<i64> {
        crate::pg_debug!(
            "PostgresStore::delete_records_before: topic_id={}, partition_id={}, before_offset={}",
            topic_id,
            partition_id,
            before_offset
        );
        Spi::connect_mut(|client| {
            client.update(
                "DELETE FROM kafka.messages
                 WHERE topic_id = $1 AND partition_id = $2 AND partition_offset < $3",
                None,
                &[topic_id.into(), partition_id.into(), before_offset.into()],
            )?;

            // Persist the advanced log start so it survives an emptied partition.
            // Without this, get_earliest_offset would fall back to
            // COALESCE(MIN(partition_offset), 0) = 0 once every row is deleted,
            // regressing the reported log start (ListOffsets EARLIEST / Fetch
            // log_start_offset) to offsets that were explicitly truncated. The
            // per-partition counter row already exists (created on produce);
            // GREATEST keeps log_start monotonic. next_offset is untouched here,
            // so producer monotonicity (GREATEST(next_offset, MAX+1), BUG-3)
            // is preserved.
            client.update(
                "INSERT INTO kafka.partition_offsets (topic_id, partition_id, next_offset, log_start_offset)
                 VALUES ($1, $2, $3, $3)
                 ON CONFLICT (topic_id, partition_id) DO UPDATE SET
                     log_start_offset = GREATEST(kafka.partition_offsets.log_start_offset, EXCLUDED.log_start_offset)",
                None,
                &[topic_id.into(), partition_id.into(), before_offset.into()],
            )?;

            // New log start offset = the persisted log start, or the earliest
            // remaining row if it sits above it (a contiguous log reports MIN,
            // which equals the truncation point). Matches Kafka's DeleteRecords
            // semantics.
            let table = client.select(
                "SELECT GREATEST(
                            po.log_start_offset,
                            COALESCE((SELECT MIN(m.partition_offset)
                                      FROM kafka.messages m
                                      WHERE m.topic_id = po.topic_id AND m.partition_id = po.partition_id),
                                     po.log_start_offset)
                        ) AS low_watermark
                 FROM kafka.partition_offsets po
                 WHERE po.topic_id = $1 AND po.partition_id = $2",
                None,
                &[topic_id.into(), partition_id.into()],
            )?;
            let low_watermark: i64 = table
                .first()
                .get_by_name("low_watermark")?
                .unwrap_or(before_offset);
            Ok(low_watermark)
        })
        .map_err(|e: KafkaError| {
            KafkaError::Internal(format!("delete_records_before failed: {}", e))
        })
    }
}
