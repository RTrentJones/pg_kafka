//! What a read_committed fetch reads behind an open transaction: only up to the last
//! stable offset.

use super::pending_reads::worker_reads;
use crate::common::TestResult;
use crate::part::MAINTENANCE_LOCK_KEY;
use crate::wire::records::fetched_values;
use crate::wire::Wire;
use crate::worker::{boxed, into_test_result, session, unique, SendResult};
use tokio_postgres::Client;

/// Committed rows below the open transaction's first offset.
const BELOW_LSO: i64 = 1_000;
/// Committed rows above it.
const ABOVE_LSO: i64 = 200_000;
/// Each fetch runs this many times, as a long-polling consumer reruns it.
const FETCHES: usize = 3;
/// The most rows of the test's leaf the fetches may read between them: the rows below
/// the last stable offset, once per fetch, and slack; far below one read of the rows
/// above it.
const MAX_ROWS_READ: i64 = 2 * BELOW_LSO * 3 + 10_000;

/// The leaf the test's rows live in: an hour of 1999 nothing else writes to, of a topic
/// with infinite retention, so its statistics count only what the fetches read.
const LEAF: &str = "messages_p19990601_050000";

/// The read_committed fetch stops below the lowest pending offset. With that bound's
/// default naming the row's own offset, the bound was a per-row filter rather than the
/// end of the index scan, and the scan read every committed row past an open transaction
/// looking for rows to fill its LIMIT; a fetch from the last stable offset itself, which
/// can return nothing, read them all too. Every long-poll attempt reruns the fetch.
///
/// Asserts: with 1 000 committed rows, one pending row at offset 1 000 and 200 000
/// committed rows after it in a leaf of their own, three read_committed fetches from 0
/// each return offsets 0..1 000 and three from 1 000 return nothing (last stable offset
/// 1 000), and together they read fewer than 16 000 rows of that leaf (its
/// `seq_tup_read + idx_tup_fetch`, the worker's statistics flushed by ending it before
/// and after, the maintenance held off by the maintenance lock, the leaf's BRIN index
/// summarized up front).
pub async fn test_read_committed_fetch_reads_only_to_the_last_stable_offset() -> TestResult {
    into_test_result(lso_reads().await)
}

async fn lso_reads() -> SendResult<()> {
    println!("=== Test: a read_committed fetch reads only to the last stable offset ===\n");
    let db = session().await?;
    db.batch_execute("SET statement_timeout = '120s'")
        .await
        .map_err(boxed)?;
    let topic = unique("lso-reads");
    let topic_id: i32 = db
        .query_one(
            "INSERT INTO kafka.topics (name, partitions, retention_ms) VALUES ($1, 1, -1)
             RETURNING id",
            &[&topic],
        )
        .await
        .map_err(boxed)?
        .get(0);
    let leaf = format!("kafka.{LEAF}");
    let result = match db
        .execute("SELECT pg_advisory_lock($1)", &[&MAINTENANCE_LOCK_KEY])
        .await
    {
        Ok(_) => steps(&db, &topic, topic_id, &leaf).await,
        Err(e) => Err(boxed(e)),
    };
    let unlocked = db
        .execute("SELECT pg_advisory_unlock($1)", &[&MAINTENANCE_LOCK_KEY])
        .await
        .map_err(boxed);
    let cleaned = async {
        db.execute(
            "DELETE FROM kafka.messages WHERE topic_id = $1",
            &[&topic_id],
        )
        .await
        .map_err(boxed)?;
        db.query(
            "SELECT dropped FROM kafka.drop_message_partition(to_regclass($1), '{}')",
            &[&leaf],
        )
        .await
        .map_err(boxed)?;
        db.execute("DELETE FROM kafka.topics WHERE id = $1", &[&topic_id])
            .await
            .map_err(boxed)?;
        SendResult::Ok(())
    }
    .await;
    result?;
    unlocked?;
    cleaned?;
    println!("✅ the fetches read only up to the last stable offset");
    Ok(())
}

async fn steps(db: &Client, topic: &str, topic_id: i32, leaf: &str) -> SendResult<()> {
    db.execute(
        "SELECT kafka.create_message_partition(timestamptz '1999-06-01 05:00+00',
                                               timestamptz '1999-06-01 06:00+00')",
        &[],
    )
    .await
    .map_err(boxed)?;
    // A negative producer id: never issued, and no transaction owns it, so the row stays
    // pending.
    db.execute(
        "INSERT INTO kafka.messages (topic_id, partition_id, partition_offset, value,
                                     producer_id, producer_epoch, txn_state, created_at)
         SELECT $1, 0, g, 'v'::bytea,
                CASE WHEN g = $2 THEN -1 END, CASE WHEN g = $2 THEN 0 END,
                CASE WHEN g = $2 THEN 'pending' END,
                timestamptz '1999-06-01 05:00+00' + g * INTERVAL '1 millisecond'
         FROM generate_series(0::bigint, $2 + $3::bigint) AS g",
        &[&topic_id, &BELOW_LSO, &ABOVE_LSO],
    )
    .await
    .map_err(boxed)?;
    db.batch_execute("ANALYZE kafka.messages")
        .await
        .map_err(boxed)?;
    // Autovacuum's summarizing of the new BRIN page ranges reads the heap, inside the
    // window or not by chance; summarized now, they leave it nothing to do.
    db.query(
        "SELECT brin_summarize_new_values(i.indexrelid)
         FROM pg_index i
         JOIN pg_class c ON c.oid = i.indexrelid
         JOIN pg_am a ON a.oid = c.relam
         WHERE i.indrelid = to_regclass($1) AND a.amname = 'brin'",
        &[&leaf],
    )
    .await
    .map_err(boxed)?;
    println!(
        "Step 0: {BELOW_LSO} committed rows, a pending one at offset {BELOW_LSO}, \
         {ABOVE_LSO} committed rows after it"
    );

    let before = worker_reads(db, leaf).await?;
    let mut wire = Wire::connect().await.map_err(boxed)?;
    for _ in 0..FETCHES {
        let p = wire.fetch(topic, 0, 0, 1).await.map_err(boxed)?;
        let records = p.records.unwrap_or_default();
        let offsets: Vec<i64> = fetched_values(&records)
            .map_err(boxed)?
            .into_iter()
            .map(|(o, _)| o)
            .collect();
        if p.error_code != 0
            || p.last_stable_offset != BELOW_LSO
            || offsets != (0..BELOW_LSO).collect::<Vec<_>>()
        {
            return Err(format!(
                "a fetch from 0: error {}, LSO {}, {} records ({:?}..{:?})",
                p.error_code,
                p.last_stable_offset,
                offsets.len(),
                offsets.first(),
                offsets.last()
            )
            .into());
        }
        let p = wire.fetch(topic, 0, BELOW_LSO, 1).await.map_err(boxed)?;
        let records = p.records.unwrap_or_default();
        if p.error_code != 0 || p.last_stable_offset != BELOW_LSO || !records.is_empty() {
            return Err(format!(
                "a fetch from the LSO: error {}, LSO {}, {} bytes of records",
                p.error_code,
                p.last_stable_offset,
                records.len()
            )
            .into());
        }
    }
    drop(wire);
    println!("Step 1: {FETCHES} fetches from 0 and from the LSO answered");
    let after = worker_reads(db, leaf).await?;
    let read = after.rows_read - before.rows_read;
    println!("Step 2: the fetches read {read} rows of the leaf");
    if read >= MAX_ROWS_READ {
        return Err(format!(
            "{} read_committed fetches read {read} rows of the leaf ({ABOVE_LSO} rows past \
             the last stable offset)",
            2 * FETCHES
        )
        .into());
    }
    Ok(())
}
