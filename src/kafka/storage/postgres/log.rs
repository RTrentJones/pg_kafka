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
    decode_headers_json, encode_headers_json, record_fetch_bytes, FetchedMessage, FetchedRecords,
    IsolationLevel, PartitionBounds, MAX_TIMESTAMP,
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

/// A fetch query: the rows of one partition from `$3` in offset order, at most `$4` of
/// them, each with the running total of the bytes it adds to a Fetch response
/// (`cumulative_bytes`: the `record_bytes` Produce stored with the row, see
/// `record_bytes_sql!` for rows without it); of those, only the rows whose running
/// total is within `$5` bytes, and the first row whatever its size. The cut is made here so
/// that the result set, which SPI holds whole, never holds the rows a byte budget leaves
/// out. The running total reads no key, value or headers: a row's are read (from TOAST, when
/// stored there) only when it is returned whole. The first row past the budget carries its
/// key, value and headers only when `$6` is true (`whole`); otherwise they are NULL, so they
/// are never read, and it only tells the caller that a record is there. `$filter` is the isolation level's extra
/// condition.
///
/// `headers` is cast to text in the outer query, for the rows returned whole only.
macro_rules! fetch_sql {
    ($filter:literal) => {
        concat!(
            "SELECT partition_offset,
        CASE WHEN cumulative_bytes <= $5 OR $6 THEN key END AS key,
        CASE WHEN cumulative_bytes <= $5 OR $6 THEN value END AS value,
        CASE WHEN cumulative_bytes <= $5 OR $6 THEN headers::text END AS headers_json,
        cumulative_bytes <= $5 OR $6 AS whole,
        producer_id, producer_epoch, timestamp_ms, cumulative_bytes
 FROM (
   SELECT partition_offset, key, value, headers, producer_id, producer_epoch,
          CASE WHEN timestamp_ms >= 0 THEN timestamp_ms
               ELSE (EXTRACT(EPOCH FROM created_at) * 1000)::bigint END AS timestamp_ms,
          (SUM(COALESCE(record_bytes, ",
            record_bytes_sql!(),
            ")) OVER w)::bigint AS cumulative_bytes,
          ROW_NUMBER() OVER w AS row_number
   FROM kafka.messages
   WHERE topic_id = $1 AND partition_id = $2 AND partition_offset >= $3",
            $filter,
            "
   WINDOW w AS (ORDER BY partition_offset)
   ORDER BY partition_offset
   LIMIT $4
 ) fetched
 WHERE cumulative_bytes <= $5 OR row_number = 1
 ORDER BY partition_offset"
        )
    };
}

/// The bytes one stored row adds to a Fetch response, as SQL over its key, value and
/// headers, for rows stored without `record_bytes` (written by SQL outside the extension;
/// the 0.0.0 -> 0.1.0 upgrade fills it for older rows with this expression): the
/// uncompressed lengths of its key, value and header keys and values (`octet_length`
/// reads a bytea's raw length from its TOAST header without decompressing it; the stored
/// header value is hex, two characters a byte), plus 84 bytes per record and 10 per header
/// for the record's framing in an uncompressed RecordBatch: a header's two varint lengths are
/// at most 10 bytes, and a record alone in its batch (a run of one producer's consecutive
/// offsets is one batch, so read_committed skipping aborted offsets or alternating producers
/// make lone records) takes the whole 61-byte batch header plus at most 23 bytes of its own
/// (length, key, value and header-count varints of at most 5 bytes, attributes and two zero
/// deltas of one); a record that shares its batch takes at most 31 of its own plus half the
/// batch header or less. So the count is never under what the record adds. Stored (compressed) sizes
/// would let a highly compressible record count as a small fraction of what the response
/// and the DB thread hold of it, and headers would not count at all. Rows written before
/// headers were stored as an ordered array hold a `{key: hex}` object; `{}` is a record
/// without headers. [`record_fetch_bytes`], which Produce stores and the Fetch handler's
/// `message_size_estimate` uses, counts the same. COALESCE evaluates it only for a row whose
/// `record_bytes` is NULL; it reads the whole headers value, from TOAST when stored there.
macro_rules! record_bytes_sql {
    () => {
        "COALESCE(octet_length(key), 0) + COALESCE(octet_length(value), 0) + 84
            + CASE WHEN headers IS NULL OR headers = '{}'::jsonb THEN 0
                   WHEN jsonb_typeof(headers) = 'array' THEN
                     (SELECT COALESCE(SUM(COALESCE(octet_length(h ->> 0), 0)
                                          + COALESCE(octet_length(h ->> 1), 0) / 2 + 10), 0)
                      FROM jsonb_array_elements(headers) h)
                   ELSE
                     (SELECT COALESCE(SUM(octet_length(k) + COALESCE(octet_length(v), 0) / 2 + 10), 0)
                      FROM jsonb_each_text(headers) e(k, v))
              END"
    };
}

/// Fetch of committed records (ReadCommitted). RV-4: besides filtering uncommitted
/// rows it stops below the last stable offset: a committed record above a still-open
/// lower-offset transaction must not be returned, or the consumer moves past the
/// pending offset and never re-reads it. With nothing pending the COALESCE default
/// (the largest bigint) makes the bound a no-op. The default is a constant, so the bound
/// is a value computed once (an InitPlan) and ends the index scan at the last stable
/// offset; a default naming `partition_offset` would make it a per-row filter, and the
/// scan would read every committed row past an open transaction, as LIMIT counts only
/// the rows that pass.
const FETCH_COMMITTED_SQL: &str = fetch_sql!(
    "
     AND txn_state IS NULL
     AND partition_offset < COALESCE(
         (SELECT MIN(m2.partition_offset) FROM kafka.messages m2
          WHERE m2.topic_id = $1 AND m2.partition_id = $2
            AND m2.txn_state = 'pending'),
         9223372036854775807)"
);

/// Fetch of every record (ReadUncommitted).
const FETCH_ALL_SQL: &str = fetch_sql!("");

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
    record_bytes: Vec<i32>,
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
            record_bytes: records.iter().map(stored_record_bytes).collect(),
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

/// The `kafka.messages.record_bytes` of `record`: [`record_fetch_bytes`], saturated to the
/// column's INT (no record a Produce frame can carry comes near it).
fn stored_record_bytes(record: &Record) -> i32 {
    let bytes = record_fetch_bytes(
        record.key.as_ref().map_or(0, Vec::len),
        record.value.as_ref().map_or(0, Vec::len),
        record.headers.iter().map(|h| (h.key.len(), h.value.len())),
    );
    i32::try_from(bytes).unwrap_or(i32::MAX)
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
                                         timestamp_ms, record_bytes)
             SELECT $1, $2, u.partition_offset, u.key, u.value, u.headers, u.producer_id,
                    u.producer_epoch, u.txn_state, u.timestamp_ms, u.record_bytes
             FROM unnest($3::bigint[], $4::bytea[], $5::bytea[], $6::jsonb[], $7::bigint[],
                         $8::smallint[], $9::text[], $10::bigint[], $11::int[])
                  AS u(partition_offset, key, value, headers, producer_id, producer_epoch,
                       txn_state, timestamp_ms, record_bytes)",
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
                columns.record_bytes.into(),
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

    /// Records from `fetch_offset` on, up to `max_bytes` of what they add to the response,
    /// for either isolation level (DR-24). With `whole_first_record` the first record is
    /// returned whatever its size, as Kafka returns the first record batch of the first
    /// partition with data (KIP-74); without it a first record larger than `max_bytes` is not
    /// read, only reported (`first_left_out`), so a partition that could not return it costs
    /// the DB thread nothing of its size. Each query is bounded to [`MAX_ROWS_PER_QUERY`] rows
    /// and returns only the rows within what is left of the budget, so neither a pass nor its
    /// result set holds more than the budget (and the one first record); the loop runs until
    /// the budget is spent or the partition has no more rows (DR-9). Sizes are the
    /// uncompressed key, value and header lengths plus their framing, as Produce stored them
    /// (`record_bytes`), summed by a window function (ADR-002).
    fn fetch_records_filtered(
        topic_id: i32,
        partition_id: i32,
        fetch_offset: i64,
        max_bytes: i32,
        whole_first_record: bool,
        isolation_level: IsolationLevel,
    ) -> Result<FetchedRecords> {
        let query = match isolation_level {
            IsolationLevel::ReadCommitted => FETCH_COMMITTED_SQL,
            IsolationLevel::ReadUncommitted => FETCH_ALL_SQL,
        };

        Spi::connect(|client| {
            let mut messages: Vec<FetchedMessage> = Vec::new();
            let budget = i64::from(max_bytes.max(0));
            let mut consumed: i64 = 0;
            let mut next_offset = fetch_offset;
            let mut first_left_out = None;

            'passes: loop {
                let remaining = budget - consumed;
                if remaining <= 0 && !messages.is_empty() {
                    break;
                }
                let limit = (remaining / ESTIMATE_BYTES_PER_MESSAGE + 1)
                    .clamp(MIN_ROWS_PER_QUERY, MAX_ROWS_PER_QUERY);

                let table = select_cached(
                    client,
                    query,
                    &[
                        INT4OID,
                        INT4OID,
                        INT8OID,
                        INT8OID,
                        INT8OID,
                        PgBuiltInOids::BOOLOID,
                    ],
                    &[
                        topic_id.into(),
                        partition_id.into(),
                        next_offset.into(),
                        limit.into(),
                        remaining.into(),
                        (whole_first_record && messages.is_empty()).into(),
                    ],
                )?;
                // The query leaves out every row past the budget: fewer rows than the
                // limit means the budget is spent or the partition has no more rows.
                let row_count = i64::try_from(table.len()).unwrap_or(i64::MAX);

                for row in table {
                    // The first row of a pass past what is left of the budget, without its
                    // key, value and headers: nothing more fits.
                    let partition_offset: i64 = row.get_by_name("partition_offset")?.unwrap_or(0);
                    if !row.get_by_name::<bool, _>("whole")?.unwrap_or(false) {
                        if messages.is_empty() {
                            first_left_out = Some(partition_offset);
                        }
                        break 'passes;
                    }
                    // cumulative_bytes restarts per query; add the running total.
                    let batch_cumulative: i64 = row.get_by_name("cumulative_bytes")?.unwrap_or(0);
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
                    consumed = budget - remaining + batch_cumulative;
                }

                if row_count < limit {
                    break;
                }
            }

            crate::pg_debug!(
                "Fetched {} messages ({consumed} bytes of {budget} budget, {isolation_level:?}, first left out: {first_left_out:?})",
                messages.len()
            );
            Ok(FetchedRecords {
                messages,
                first_left_out,
            })
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
            true,
            IsolationLevel::ReadUncommitted,
        )
        .map(|fetched| fetched.messages)
    }

    pub(super) fn fetch_records_with_isolation(
        topic_id: i32,
        partition_id: i32,
        fetch_offset: i64,
        max_bytes: i32,
        whole_first_record: bool,
        isolation_level: IsolationLevel,
    ) -> Result<FetchedRecords> {
        crate::pg_debug!(
            "PostgresStore::fetch_records_with_isolation: topic_id={topic_id}, partition_id={partition_id}, offset={fetch_offset}, whole_first_record={whole_first_record}, isolation={isolation_level:?}"
        );
        Self::fetch_records_filtered(
            topic_id,
            partition_id,
            fetch_offset,
            max_bytes,
            whole_first_record,
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
        _isolation_level: IsolationLevel,
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
        // Key, value, header key and value, 84 per record and 10 per header.
        assert_eq!(columns.record_bytes, [1 + 1 + 1 + 1 + 84 + 10, 84]);
    }

    /// Every header adds its key, value and 10 bytes of framing.
    #[test]
    fn stored_record_bytes_count_every_header() {
        let mut record = records().remove(1);
        record.headers = (0..3)
            .map(|_| RecordHeader {
                key: "k".into(),
                value: vec![0; 3],
            })
            .collect();
        assert_eq!(super::stored_record_bytes(&record), 84 + 3 * 14);
    }

    /// The upgrade fills `record_bytes` for 0.0.0 rows with the expression fetches use for
    /// rows without it, so both count what Produce stores.
    #[test]
    fn upgrade_fills_record_bytes_with_the_fetch_expression() {
        const UPGRADE_SQL: &str = include_str!("../../../../sql/pg_kafka--0.0.0--0.1.0.sql");
        let words = |s: &str| s.split_whitespace().collect::<Vec<_>>().join(" ");
        assert!(
            words(UPGRADE_SQL).contains(&words(record_bytes_sql!())),
            "the upgrade's record_bytes expression differs from record_bytes_sql!"
        );
        assert!(super::FETCH_ALL_SQL.contains("COALESCE(record_bytes, "));
        assert!(super::FETCH_COMMITTED_SQL.contains("COALESCE(record_bytes, "));
    }

    /// The read_committed bound's default is a constant: one naming `partition_offset` would
    /// make the bound a per-row filter instead of the end of the index scan.
    #[test]
    fn the_last_stable_offset_bound_ends_the_scan() {
        let words = |s: &str| s.split_whitespace().collect::<Vec<_>>().join(" ");
        let sql = words(super::FETCH_COMMITTED_SQL);
        assert!(
            sql.contains("AND m2.txn_state = 'pending'), 9223372036854775807)"),
            "{sql}"
        );
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
