//! Message log SQL: append, fetch (both isolation levels), offset bounds and
//! DeleteRecords.
//!
//! Offsets: every `partition_offset` is assigned from the per-partition counter
//! `kafka.partition_offsets.next_offset`, which is never decremented (BUG-3), so
//! deleting rows (aborted-row cleanup, retention, DeleteRecords) never lets an offset
//! be reused or the high watermark regress (BUG-4). Every log-end computation takes
//! `GREATEST(next_offset, MAX(partition_offset) + 1)`: the second term covers rows
//! written before their partition had a counter row.

use super::{internal, internal_unless, select_cached, PostgresStore, INT4OID, INT8OID};
use crate::kafka::error::{KafkaError, Result};
use crate::kafka::messages::Record;
use crate::kafka::storage::{
    decode_headers_json, encode_headers_json, FetchedMessage, IsolationLevel, PartitionBounds,
    MAX_TIMESTAMP,
};
use pgrx::prelude::*;
use pgrx::spi::SpiClient;

/// The log-end offset of one partition (`$1` topic, `$2` partition): the next offset
/// Produce assigns, which is also the high watermark.
const LOG_END_OFFSET_SQL: &str = "SELECT GREATEST(
         COALESCE((SELECT next_offset FROM kafka.partition_offsets
                   WHERE topic_id = $1 AND partition_id = $2), 0),
         COALESCE((SELECT MAX(partition_offset) + 1 FROM kafka.messages
                   WHERE topic_id = $1 AND partition_id = $2), 0)
     ) AS log_end_offset";

/// Fetch of committed records (ReadCommitted). RV-4: besides filtering uncommitted
/// rows it stops below the last stable offset: a committed record above a still-open
/// lower-offset transaction must not be returned, or the consumer moves past the
/// pending offset and never re-reads it. With nothing pending the COALESCE default
/// makes the bound a no-op.
const FETCH_COMMITTED_SQL: &str =
    "SELECT partition_offset, key, value, headers::text AS headers_json,
        producer_id, producer_epoch,
        CASE WHEN timestamp_ms >= 0 THEN timestamp_ms
             ELSE (EXTRACT(EPOCH FROM created_at) * 1000)::bigint END AS timestamp_ms,
        SUM(COALESCE(pg_column_size(key), 0) + COALESCE(pg_column_size(value), 0) + 64)
            OVER (ORDER BY partition_offset) AS cumulative_bytes
 FROM kafka.messages
 WHERE topic_id = $1 AND partition_id = $2 AND partition_offset >= $3
   AND txn_state IS NULL
   AND partition_offset < COALESCE(
       (SELECT MIN(m2.partition_offset) FROM kafka.messages m2
        WHERE m2.topic_id = $1 AND m2.partition_id = $2
          AND m2.txn_state = 'pending'),
       partition_offset + 1)
 ORDER BY partition_offset
 LIMIT $4";

/// Fetch of every record (ReadUncommitted).
const FETCH_ALL_SQL: &str = "SELECT partition_offset, key, value, headers::text AS headers_json,
        producer_id, producer_epoch,
        CASE WHEN timestamp_ms >= 0 THEN timestamp_ms
             ELSE (EXTRACT(EPOCH FROM created_at) * 1000)::bigint END AS timestamp_ms,
        SUM(COALESCE(pg_column_size(key), 0) + COALESCE(pg_column_size(value), 0) + 64)
            OVER (ORDER BY partition_offset) AS cumulative_bytes
 FROM kafka.messages
 WHERE topic_id = $1 AND partition_id = $2 AND partition_offset >= $3
 ORDER BY partition_offset
 LIMIT $4";

/// Rough size of one stored message, to turn a byte budget into a row LIMIT.
const ESTIMATE_BYTES_PER_MESSAGE: i64 = 500;
/// Row bounds of one fetch query; the fetch loops until its byte budget is spent.
const MIN_ROWS_PER_QUERY: i64 = 10;
const MAX_ROWS_PER_QUERY: i64 = 5_000;

/// `timestamp_ms` of a record that carried no timestamp: the fetch path then reports
/// the broker insert time (BUG-7).
const NO_TIMESTAMP: i64 = -1;

/// The producer a transactional batch is stamped with.
#[derive(Debug, Clone, Copy)]
pub(super) struct TxnStamp {
    pub(super) producer_id: i64,
    pub(super) producer_epoch: i16,
}

/// The per-row columns of one append, as parallel arrays for a single UNNEST insert:
/// type-safe, and no SQL is built from data.
#[derive(Debug, PartialEq)]
struct MessageColumns<'a> {
    offsets: Vec<i64>,
    keys: Vec<Option<Vec<u8>>>,
    values: Vec<Option<Vec<u8>>>,
    headers: Vec<String>,
    timestamps: Vec<i64>,
    producer_ids: Vec<Option<i64>>,
    producer_epochs: Vec<Option<i16>>,
    txn_states: Vec<Option<&'a str>>,
}

impl MessageColumns<'_> {
    /// The columns of `records` at offsets `base_offset..base_offset + count`
    /// (`count` is `records.len()`), stamped with `txn`'s producer and
    /// `txn_state = 'pending'` when it is a transactional batch.
    fn new(records: &[Record], base_offset: i64, count: i64, txn: Option<TxnStamp>) -> Self {
        let n = records.len();
        Self {
            offsets: (base_offset..base_offset + count).collect(),
            keys: records.iter().map(|r| r.key.clone()).collect(),
            values: records.iter().map(|r| r.value.clone()).collect(),
            headers: records
                .iter()
                .map(|r| {
                    encode_headers_json(
                        r.headers
                            .iter()
                            .map(|h| (h.key.as_str(), h.value.as_slice())),
                    )
                })
                .collect(),
            // BUG-7: the producer's record timestamp is stored verbatim.
            timestamps: records
                .iter()
                .map(|r| r.timestamp.unwrap_or(NO_TIMESTAMP))
                .collect(),
            producer_ids: vec![txn.map(|t| t.producer_id); n],
            producer_epochs: vec![txn.map(|t| t.producer_epoch); n],
            txn_states: vec![txn.map(|_| "pending"); n],
        }
    }
}

/// Take the partition's append lock and read its log-end offset, the base offset of
/// the next append.
///
/// DR-13: every extension write runs on the single DB thread, so this lock is
/// uncontended there; it guards the read-then-insert offset assignment against
/// out-of-band writers (direct SQL inserts).
fn lock_and_read_log_end(client: &SpiClient<'_>, topic_id: i32, partition_id: i32) -> Result<i64> {
    client.select(
        "SELECT pg_advisory_xact_lock($1, $2)",
        None,
        &[topic_id.into(), partition_id.into()],
    )?;
    Ok(select_cached(
        client,
        LOG_END_OFFSET_SQL,
        &[INT4OID, INT4OID],
        &[topic_id.into(), partition_id.into()],
    )?
    .first()
    .get_by_name("log_end_offset")?
    .unwrap_or(0))
}

/// Insert one append's rows into `kafka.messages`.
fn insert_message_rows(
    client: &mut SpiClient<'_>,
    topic_id: i32,
    partition_id: i32,
    columns: MessageColumns<'_>,
) -> Result<()> {
    client
        .update(
            "INSERT INTO kafka.messages (topic_id, partition_id, partition_offset, key, value,
                                         headers, producer_id, producer_epoch, txn_state,
                                         timestamp_ms)
             SELECT $1, $2, u.partition_offset, u.key, u.value, u.headers, u.producer_id,
                    u.producer_epoch, u.txn_state, u.timestamp_ms
             FROM unnest($3::bigint[], $4::bytea[], $5::bytea[], $6::jsonb[], $7::bigint[],
                         $8::smallint[], $9::text[], $10::bigint[])
                  AS u(partition_offset, key, value, headers, producer_id, producer_epoch,
                       txn_state, timestamp_ms)",
            None,
            &[
                topic_id.into(),
                partition_id.into(),
                columns.offsets.into(),
                columns.keys.into(),
                columns.values.into(),
                columns.headers.into(),
                columns.producer_ids.into(),
                columns.producer_epochs.into(),
                columns.txn_states.into(),
                columns.timestamps.into(),
            ],
        )
        .map_err(|e| KafkaError::Internal(format!("Failed to insert records: {e}")))?;
    Ok(())
}

/// Advance the partition's offset counter to `next_offset` (it never moves back).
fn advance_next_offset(
    client: &mut SpiClient<'_>,
    topic_id: i32,
    partition_id: i32,
    next_offset: i64,
) -> Result<()> {
    client
        .update(
            "INSERT INTO kafka.partition_offsets (topic_id, partition_id, next_offset)
             VALUES ($1, $2, $3)
             ON CONFLICT (topic_id, partition_id) DO UPDATE
                 SET next_offset = GREATEST(kafka.partition_offsets.next_offset,
                                            EXCLUDED.next_offset)",
            None,
            &[topic_id.into(), partition_id.into(), next_offset.into()],
        )
        .map_err(|e| KafkaError::Internal(format!("Failed to advance partition offset: {e}")))?;
    Ok(())
}

impl PostgresStore {
    /// Append `records` to a partition and return the base offset; shared by plain and
    /// transactional produce (DR-24) so the offset assignment exists once. `txn`
    /// stamps the producer columns and `txn_state = 'pending'`; `None` leaves them NULL.
    pub(super) fn insert_records_inner(
        topic_id: i32,
        partition_id: i32,
        records: &[Record],
        txn: Option<TxnStamp>,
    ) -> Result<i64> {
        if records.is_empty() {
            return Ok(0);
        }
        let count = i64::try_from(records.len())
            .map_err(|_| KafkaError::Internal("record batch too large".into()))?;

        Spi::connect_mut(|client| {
            let base_offset = lock_and_read_log_end(client, topic_id, partition_id)?;
            crate::pg_debug!("Assigning base_offset={base_offset}");

            let columns = MessageColumns::new(records, base_offset, count, txn);
            insert_message_rows(client, topic_id, partition_id, columns)?;
            advance_next_offset(client, topic_id, partition_id, base_offset + count)?;

            crate::pg_debug!(
                "Inserted {count} records (offsets {base_offset} to {}, txn={})",
                base_offset + count - 1,
                txn.is_some()
            );
            Ok(base_offset)
        })
        .map_err(internal_unless("insert_records", |e| {
            matches!(e, KafkaError::Internal(_))
        }))
    }

    /// Records from `fetch_offset` on, up to about `max_bytes` (always at least one
    /// record, as Kafka does), for either isolation level (DR-24). Each query is
    /// bounded to [`MAX_ROWS_PER_QUERY`] rows so a pass never holds a huge result set;
    /// the loop runs until the byte budget is spent or the partition has no more rows
    /// (DR-9). Sizes come from `pg_column_size()` in a window function (ADR-002).
    fn fetch_records_filtered(
        topic_id: i32,
        partition_id: i32,
        fetch_offset: i64,
        max_bytes: i32,
        isolation_level: IsolationLevel,
    ) -> Result<Vec<FetchedMessage>> {
        let query = match isolation_level {
            IsolationLevel::ReadCommitted => FETCH_COMMITTED_SQL,
            IsolationLevel::ReadUncommitted => FETCH_ALL_SQL,
        };

        Spi::connect(|client| {
            let mut messages: Vec<FetchedMessage> = Vec::new();
            let budget = i64::from(max_bytes.max(0));
            let mut consumed: i64 = 0;
            let mut next_offset = fetch_offset;

            loop {
                let remaining = budget - consumed;
                if remaining <= 0 && !messages.is_empty() {
                    break;
                }
                let limit = (remaining / ESTIMATE_BYTES_PER_MESSAGE + 1)
                    .clamp(MIN_ROWS_PER_QUERY, MAX_ROWS_PER_QUERY);

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
                let row_count = i64::try_from(table.len()).unwrap_or(i64::MAX);
                let batch_base = consumed;
                let mut budget_hit = false;

                for row in table {
                    // cumulative_bytes restarts per query; add the running total.
                    let batch_cumulative: i64 = row.get_by_name("cumulative_bytes")?.unwrap_or(0);
                    let total = batch_base + batch_cumulative;
                    if !messages.is_empty() && total > budget {
                        budget_hit = true;
                        break;
                    }

                    let partition_offset: i64 = row.get_by_name("partition_offset")?.unwrap_or(0);
                    let headers_json: Option<String> = row.get_by_name("headers_json")?;
                    messages.push(FetchedMessage {
                        partition_offset,
                        key: row.get_by_name("key")?,
                        value: row.get_by_name("value")?,
                        timestamp: row.get_by_name("timestamp_ms")?.unwrap_or(0),
                        headers: decode_headers_json(headers_json.as_deref()),
                        producer_id: row.get_by_name("producer_id")?,
                        producer_epoch: row.get_by_name("producer_epoch")?,
                    });
                    next_offset = partition_offset + 1;
                    consumed = total;
                }

                // A short batch means no more matching rows past next_offset.
                if budget_hit || row_count < limit {
                    break;
                }
            }

            crate::pg_debug!(
                "Fetched {} messages ({consumed} bytes of {budget} budget, {isolation_level:?})",
                messages.len()
            );
            Ok(messages)
        })
        .map_err(internal("fetch_records"))
    }

    pub(super) fn insert_records(
        topic_id: i32,
        partition_id: i32,
        records: &[Record],
    ) -> Result<i64> {
        crate::pg_debug!(
            "PostgresStore::insert_records: {} records for topic_id={topic_id}, partition_id={partition_id}",
            records.len()
        );
        Self::insert_records_inner(topic_id, partition_id, records, None)
    }

    /// ReadUncommitted fetch; the ShadowStore read-suppression path (SH-6) uses it,
    /// consumers go through [`Self::fetch_records_with_isolation`].
    pub(super) fn fetch_records(
        topic_id: i32,
        partition_id: i32,
        fetch_offset: i64,
        max_bytes: i32,
    ) -> Result<Vec<FetchedMessage>> {
        crate::pg_debug!(
            "PostgresStore::fetch_records: topic_id={topic_id}, partition_id={partition_id}, fetch_offset={fetch_offset}, max_bytes={max_bytes}"
        );
        Self::fetch_records_filtered(
            topic_id,
            partition_id,
            fetch_offset,
            max_bytes,
            IsolationLevel::ReadUncommitted,
        )
    }

    pub(super) fn fetch_records_with_isolation(
        topic_id: i32,
        partition_id: i32,
        fetch_offset: i64,
        max_bytes: i32,
        isolation_level: IsolationLevel,
    ) -> Result<Vec<FetchedMessage>> {
        crate::pg_debug!(
            "PostgresStore::fetch_records_with_isolation: topic_id={topic_id}, partition_id={partition_id}, offset={fetch_offset}, isolation={isolation_level:?}"
        );
        Self::fetch_records_filtered(
            topic_id,
            partition_id,
            fetch_offset,
            max_bytes,
            isolation_level,
        )
    }

    /// The log-end offset; the read isolation level does not change it (that is the
    /// fetch filter's and the last stable offset's job).
    pub(super) fn get_high_watermark(topic_id: i32, partition_id: i32) -> Result<i64> {
        Spi::connect(|client| {
            Ok(select_cached(
                client,
                LOG_END_OFFSET_SQL,
                &[INT4OID, INT4OID],
                &[topic_id.into(), partition_id.into()],
            )?
            .first()
            .get_by_name("log_end_offset")?
            .unwrap_or(0))
        })
        .map_err(internal("get_high_watermark"))
    }

    /// GREATEST(smallest remaining offset, durable log start). The durable log start
    /// dominates only once truncation emptied the partition's front; it is 0 for a
    /// partition without a counter row.
    pub(super) fn get_earliest_offset(topic_id: i32, partition_id: i32) -> Result<i64> {
        Spi::connect(|client| {
            Ok(select_cached(
                client,
                "SELECT GREATEST(
                     COALESCE((SELECT MIN(partition_offset) FROM kafka.messages
                               WHERE topic_id = $1 AND partition_id = $2), 0),
                     COALESCE((SELECT log_start_offset FROM kafka.partition_offsets
                               WHERE topic_id = $1 AND partition_id = $2), 0)
                 ) AS earliest_offset",
                &[INT4OID, INT4OID],
                &[topic_id.into(), partition_id.into()],
            )?
            .first()
            .get_by_name("earliest_offset")?
            .unwrap_or(0))
        })
        .map_err(internal("get_earliest_offset"))
    }

    pub(super) fn get_log_start_offset(topic_id: i32, partition_id: i32) -> Result<i64> {
        Spi::connect(|client| {
            Ok(client
                .select(
                    "SELECT COALESCE((SELECT log_start_offset FROM kafka.partition_offsets
                                      WHERE topic_id = $1 AND partition_id = $2), 0)
                            AS log_start_offset",
                    None,
                    &[topic_id.into(), partition_id.into()],
                )?
                .first()
                .get_by_name("log_start_offset")?
                .unwrap_or(0))
        })
        .map_err(internal("get_log_start_offset"))
    }

    /// Every bound a Fetch reports, in one statement of primary-key (or
    /// idx_messages_txn_pending) probes; [`PartitionBounds::from_raw`] combines them
    /// exactly as the single-value methods do.
    pub(super) fn get_partition_bounds(
        topic_id: i32,
        partition_id: i32,
        isolation_level: IsolationLevel,
    ) -> Result<PartitionBounds> {
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
        .map_err(internal("get_partition_bounds"))
    }

    /// CONF-7: the lowest offset whose stored producer timestamp is at or after
    /// `timestamp_ms`, or for [`MAX_TIMESTAMP`] the record with the largest timestamp
    /// (the lowest offset among equal ones). Records without a timestamp and aborted
    /// records are excluded; pending records count, and the handler bounds the answer
    /// by the high watermark or the last stable offset, as Kafka does.
    pub(super) fn get_offset_for_timestamp(
        topic_id: i32,
        partition_id: i32,
        timestamp_ms: i64,
    ) -> Result<Option<(i64, i64)>> {
        Spi::connect(|client| {
            let table = if timestamp_ms == MAX_TIMESTAMP {
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

            // No matching record (a timestamp after the last one, or an empty
            // partition): reading the first row of an empty result is an error in SPI.
            if table.is_empty() {
                return Ok(None);
            }
            let row = table.first();
            let Some(offset) = row.get_by_name::<i64, _>("partition_offset")? else {
                return Ok(None);
            };
            let timestamp: i64 = row.get_by_name("timestamp_ms")?.unwrap_or(NO_TIMESTAMP);
            Ok(Some((offset, timestamp)))
        })
        .map_err(internal("get_offset_for_timestamp"))
    }

    /// The lowest pending transactional offset, or the log-end offset when nothing is
    /// pending (RV-10: computed like the high watermark, so neither regresses after
    /// aborted-row cleanup and the two agree).
    pub(super) fn get_last_stable_offset(topic_id: i32, partition_id: i32) -> Result<i64> {
        crate::pg_debug!(
            "PostgresStore::get_last_stable_offset: topic_id={topic_id}, partition_id={partition_id}"
        );
        Spi::connect(|client| {
            Ok(client
                .select(
                    "SELECT COALESCE(
                         (SELECT MIN(partition_offset) FROM kafka.messages
                          WHERE topic_id = $1 AND partition_id = $2 AND txn_state = 'pending'),
                         GREATEST(
                             COALESCE((SELECT next_offset FROM kafka.partition_offsets
                                       WHERE topic_id = $1 AND partition_id = $2), 0),
                             COALESCE((SELECT MAX(partition_offset) + 1 FROM kafka.messages
                                       WHERE topic_id = $1 AND partition_id = $2), 0)
                         )
                     ) AS lso",
                    None,
                    &[topic_id.into(), partition_id.into()],
                )?
                .first()
                .get_by_name("lso")?
                .unwrap_or(0))
        })
        .map_err(internal("get_last_stable_offset"))
    }

    /// DeleteRecords: remove every row below `before_offset` (pending ones too, as
    /// Kafka advances the log start unconditionally) and return the new log start.
    pub(super) fn delete_records_before(
        topic_id: i32,
        partition_id: i32,
        before_offset: i64,
    ) -> Result<i64> {
        crate::pg_debug!(
            "PostgresStore::delete_records_before: topic_id={topic_id}, partition_id={partition_id}, before_offset={before_offset}"
        );
        Spi::connect_mut(|client| {
            client.update(
                "DELETE FROM kafka.messages
                 WHERE topic_id = $1 AND partition_id = $2 AND partition_offset < $3",
                None,
                &[topic_id.into(), partition_id.into(), before_offset.into()],
            )?;

            // Persist the advanced log start, so a partition emptied by the delete still
            // reports it (ListOffsets EARLIEST, Fetch log_start_offset) instead of 0.
            // GREATEST keeps it monotonic; next_offset is untouched.
            client.update(
                "INSERT INTO kafka.partition_offsets (topic_id, partition_id, next_offset,
                                                      log_start_offset)
                 VALUES ($1, $2, $3, $3)
                 ON CONFLICT (topic_id, partition_id) DO UPDATE
                     SET log_start_offset = GREATEST(kafka.partition_offsets.log_start_offset,
                                                     EXCLUDED.log_start_offset)",
                None,
                &[topic_id.into(), partition_id.into(), before_offset.into()],
            )?;

            // The new log start: the persisted one, or the earliest remaining row if it
            // sits above it (Kafka's DeleteRecords low watermark).
            Ok(client
                .select(
                    "SELECT GREATEST(
                         po.log_start_offset,
                         COALESCE((SELECT MIN(m.partition_offset)
                                   FROM kafka.messages m
                                   WHERE m.topic_id = po.topic_id
                                     AND m.partition_id = po.partition_id),
                                  po.log_start_offset)
                     ) AS low_watermark
                     FROM kafka.partition_offsets po
                     WHERE po.topic_id = $1 AND po.partition_id = $2",
                    None,
                    &[topic_id.into(), partition_id.into()],
                )?
                .first()
                .get_by_name("low_watermark")?
                .unwrap_or(before_offset))
        })
        .map_err(internal("delete_records_before"))
    }
}

#[cfg(test)]
mod tests {
    use super::{MessageColumns, TxnStamp, NO_TIMESTAMP};
    use crate::kafka::messages::{Record, RecordHeader};
    use crate::kafka::storage::encode_headers_json;

    fn records() -> Vec<Record> {
        vec![
            Record {
                key: Some(b"k".to_vec()),
                value: Some(b"v".to_vec()),
                headers: vec![RecordHeader {
                    key: "h".into(),
                    value: b"x".to_vec(),
                }],
                timestamp: Some(1_700_000_000_000),
            },
            Record {
                key: None,
                value: None,
                headers: Vec::new(),
                timestamp: None,
            },
        ]
    }

    #[test]
    fn message_columns_of_a_plain_batch() {
        let columns = MessageColumns::new(&records(), 40, 2, None);
        assert_eq!(columns.offsets, [40, 41]);
        assert_eq!(columns.keys, [Some(b"k".to_vec()), None]);
        assert_eq!(columns.values, [Some(b"v".to_vec()), None]);
        assert_eq!(
            columns.headers,
            [
                encode_headers_json([("h", b"x".as_slice())].into_iter()),
                encode_headers_json(std::iter::empty())
            ]
        );
        // A record without a timestamp is stored as NO_TIMESTAMP (the fetch path then
        // reports the insert time).
        assert_eq!(columns.timestamps, [1_700_000_000_000, NO_TIMESTAMP]);
        assert_eq!(columns.producer_ids, [None, None]);
        assert_eq!(columns.producer_epochs, [None, None]);
        assert_eq!(columns.txn_states, [None, None]);
    }

    #[test]
    fn message_columns_of_a_transactional_batch_are_stamped_pending() {
        let txn = TxnStamp {
            producer_id: 7,
            producer_epoch: 3,
        };
        let columns = MessageColumns::new(&records(), 0, 2, Some(txn));
        assert_eq!(columns.offsets, [0, 1]);
        assert_eq!(columns.producer_ids, [Some(7), Some(7)]);
        assert_eq!(columns.producer_epochs, [Some(3), Some(3)]);
        assert_eq!(columns.txn_states, [Some("pending"), Some("pending")]);
    }
}
