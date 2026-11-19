//! Message log SQL: append, fetch (both isolation levels), offset bounds and
//! DeleteRecords.

use super::{select_cached, PostgresStore, INT4OID, INT8OID};
use crate::kafka::error::{KafkaError, Result};
use crate::kafka::messages::Record;
use crate::kafka::storage::{FetchedMessage, IsolationLevel, PartitionBounds};
use pgrx::prelude::*;

impl PostgresStore {
    /// Shared produce-path insert for plain and transactional batches (DR-24,
    /// DEEP-REVIEW-2026-07): the two paths were ~90% identical copies (advisory
    /// lock -> BUG-3 base-offset -> UNNEST insert -> counter advance) that had
    /// already diverged once (RV-5-style "fix landed on one twin only" bugs).
    /// `txn = Some((producer_id, producer_epoch))` stamps the transaction columns
    /// and txn_state='pending'; None inserts NULLs (plain produce).
    pub(super) fn insert_records_inner(
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
            let table = select_cached(
                client,
                "SELECT GREATEST(
                          COALESCE((SELECT next_offset FROM kafka.partition_offsets
                                    WHERE topic_id = $1 AND partition_id = $2), 0),
                          COALESCE((SELECT MAX(partition_offset) + 1 FROM kafka.messages
                                    WHERE topic_id = $1 AND partition_id = $2), 0)
                        ) AS base_offset",
                &[INT4OID, INT4OID],
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
                        // An array of [key, hex(value)] pairs keeps the producer's header
                        // order (a JSONB object would reorder its keys).
                        let pairs: Vec<(&str, String)> = r
                            .headers
                            .iter()
                            .map(|h| (h.key.as_str(), hex_encode(&h.value)))
                            .collect();
                        serde_json::to_string(&pairs).unwrap_or_else(|_| "{}".to_string())
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
    pub(super) fn fetch_records_filtered(
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
                    producer_id, producer_epoch,
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
                    producer_id, producer_epoch,
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

                let table = select_cached(
                    client,
                    query,
                    &[INT4OID, INT4OID, INT8OID, INT8OID],
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
                    let headers = crate::kafka::storage::decode_headers_json(headers_json.as_deref());
                    let producer_id: Option<i64> = row.get_by_name("producer_id")?;
                    let producer_epoch: Option<i16> = row.get_by_name("producer_epoch")?;

                    messages.push(FetchedMessage {
                        partition_offset,
                        key,
                        value,
                        timestamp,
                        headers,
                        producer_id,
                        producer_epoch,
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

    pub(super) fn insert_records(&self, topic_id: i32, partition_id: i32, records: &[Record]) -> Result<i64> {
        crate::pg_debug!(
            "PostgresStore::insert_records: {} records for topic_id={}, partition_id={}",
            records.len(),
            topic_id,
            partition_id
        );
        // DR-24: shared implementation with the transactional path (txn = None).
        self.insert_records_inner(topic_id, partition_id, records, None)
    }

    pub(super) fn fetch_records(
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

    pub(super) fn get_high_watermark(&self, topic_id: i32, partition_id: i32) -> Result<i64> {
        Spi::connect(|client| {
            // BUG-4: the high watermark must be the log-end offset and must never regress. A plain
            // MAX(partition_offset)+1 drops when cleanup_aborted_messages deletes the highest
            // (aborted) rows, reporting a HWM below offsets already handed out. Use the never-
            // decreasing per-partition counter (kafka.partition_offsets, the same source produce
            // assigns from — see BUG-3), taking GREATEST with MAX+1 as a backstop for partitions
            // that predate the counter. Read isolation (read_committed vs read_uncommitted) is
            // unaffected: that is decided by the fetch row filter and the last-stable-offset, not by
            // the HWM bound reported here.
            let table = select_cached(
                client,
                "SELECT GREATEST(
                     COALESCE((SELECT next_offset FROM kafka.partition_offsets
                               WHERE topic_id = $1 AND partition_id = $2), 0),
                     COALESCE((SELECT MAX(partition_offset) + 1 FROM kafka.messages
                               WHERE topic_id = $1 AND partition_id = $2), 0)
                 ) AS high_watermark",
                &[INT4OID, INT4OID],
                &[topic_id.into(), partition_id.into()],
            )?;

            let high_watermark: i64 = table.first().get_by_name("high_watermark")?.unwrap_or(0);

            Ok(high_watermark)
        })
        .map_err(|e: KafkaError| KafkaError::Internal(format!("get_high_watermark failed: {}", e)))
    }

    pub(super) fn get_earliest_offset(&self, topic_id: i32, partition_id: i32) -> Result<i64> {
        Spi::connect(|client| {
            // Earliest = GREATEST(smallest remaining offset, durable log start).
            // The durable log start (kafka.partition_offsets.log_start_offset,
            // advanced by DeleteRecords) dominates only when the partition has
            // been emptied by truncation — otherwise MIN(partition_offset) is at
            // or above it. For a never-truncated partition log_start is 0, so
            // this is unchanged. The LEFT JOIN covers a partition with rows but
            // no counter row (log_start defaults to 0).
            let table = select_cached(
                client,
                "SELECT GREATEST(
                            COALESCE((SELECT MIN(partition_offset) FROM kafka.messages
                                      WHERE topic_id = $1 AND partition_id = $2), 0),
                            COALESCE((SELECT log_start_offset FROM kafka.partition_offsets
                                      WHERE topic_id = $1 AND partition_id = $2), 0)
                        ) AS earliest_offset",
                &[INT4OID, INT4OID],
                &[topic_id.into(), partition_id.into()],
            )?;

            let earliest_offset: i64 = table.first().get_by_name("earliest_offset")?.unwrap_or(0);

            Ok(earliest_offset)
        })
        .map_err(|e: KafkaError| KafkaError::Internal(format!("get_earliest_offset failed: {}", e)))
    }

    pub(super) fn get_log_start_offset(&self, topic_id: i32, partition_id: i32) -> Result<i64> {
        Spi::connect(|client| {
            let table = client.select(
                "SELECT COALESCE((SELECT log_start_offset FROM kafka.partition_offsets
                                  WHERE topic_id = $1 AND partition_id = $2), 0)
                        AS log_start_offset",
                None,
                &[topic_id.into(), partition_id.into()],
            )?;
            Ok(table.first().get_by_name("log_start_offset")?.unwrap_or(0))
        })
        .map_err(|e: KafkaError| {
            KafkaError::Internal(format!("get_log_start_offset failed: {}", e))
        })
    }

    pub(super) fn get_partition_bounds(
        &self,
        topic_id: i32,
        partition_id: i32,
        isolation_level: IsolationLevel,
    ) -> Result<PartitionBounds> {
        // One statement for every bound a Fetch reports (it used to run 4-5: high
        // watermark, LSO, earliest, log start). Each scalar subquery is a
        // primary-key (or idx_messages_txn_pending) probe; PartitionBounds::from_raw
        // combines them exactly as the single-value methods above do.
        Spi::connect(|client| {
            let table = select_cached(
                client,
                "SELECT
                     (SELECT next_offset FROM kafka.partition_offsets
                      WHERE topic_id = $1 AND partition_id = $2) AS next_offset,
                     (SELECT log_start_offset FROM kafka.partition_offsets
                      WHERE topic_id = $1 AND partition_id = $2) AS log_start_offset,
                     (SELECT MIN(partition_offset) FROM kafka.messages
                      WHERE topic_id = $1 AND partition_id = $2) AS min_offset,
                     (SELECT MAX(partition_offset) FROM kafka.messages
                      WHERE topic_id = $1 AND partition_id = $2) AS max_offset,
                     (SELECT MIN(partition_offset) FROM kafka.messages
                      WHERE topic_id = $1 AND partition_id = $2
                        AND txn_state = 'pending') AS min_pending_offset",
                &[INT4OID, INT4OID],
                &[topic_id.into(), partition_id.into()],
            )?;
            let row = table.first();
            Ok(PartitionBounds::from_raw(
                row.get_by_name("next_offset")?,
                row.get_by_name("log_start_offset")?,
                row.get_by_name("min_offset")?,
                row.get_by_name("max_offset")?,
                row.get_by_name("min_pending_offset")?,
                isolation_level,
            ))
        })
        .map_err(|e: KafkaError| {
            KafkaError::Internal(format!("get_partition_bounds failed: {}", e))
        })
    }

    pub(super) fn get_offset_for_timestamp(
        &self,
        topic_id: i32,
        partition_id: i32,
        timestamp_ms: i64,
    ) -> Result<Option<(i64, i64)>> {
        Spi::connect(|client| {
            // CONF-7: the lowest offset whose stored producer timestamp (BUG-7) is at or after
            // the target. Records with no timestamp (-1) and aborted records are excluded;
            // pending records count, and the handler bounds the answer by the HWM or LSO, as
            // Kafka does (wave 3; aborted rows are invisible here and later deleted).
            // MAX_TIMESTAMP (-3, KIP-734): the record with the largest timestamp, the
            // lowest offset among equal ones.
            let table = if timestamp_ms == crate::kafka::storage::MAX_TIMESTAMP {
                client.select(
                    "SELECT partition_offset, timestamp_ms
                     FROM kafka.messages
                     WHERE topic_id = $1 AND partition_id = $2
                       AND timestamp_ms >= 0
                       AND txn_state IS DISTINCT FROM 'aborted'
                     ORDER BY timestamp_ms DESC, partition_offset ASC
                     LIMIT 1",
                    None,
                    &[topic_id.into(), partition_id.into()],
                )?
            } else {
                client.select(
                    "SELECT partition_offset, timestamp_ms
                     FROM kafka.messages
                     WHERE topic_id = $1 AND partition_id = $2
                       AND timestamp_ms >= 0 AND timestamp_ms >= $3
                       AND txn_state IS DISTINCT FROM 'aborted'
                     ORDER BY partition_offset ASC
                     LIMIT 1",
                    None,
                    &[topic_id.into(), partition_id.into(), timestamp_ms.into()],
                )?
            };

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

    pub(super) fn fetch_records_with_isolation(
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

    pub(super) fn get_last_stable_offset(&self, topic_id: i32, partition_id: i32) -> Result<i64> {
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

    pub(super) fn delete_records_before(
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

/// Helper function to encode bytes as hex string for JSONB storage
fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}
