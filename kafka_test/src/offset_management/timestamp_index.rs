//! What a ListOffsets lookup by timestamp or MAX_TIMESTAMP reads.

use crate::common::TestResult;
use crate::error_codes;
use crate::part::MAINTENANCE_LOCK_KEY;
use crate::transaction::pending_reads::worker_reads;
use crate::wire::Wire;
use crate::worker::{boxed, into_test_result, session, unique, SendResult};
use tokio_postgres::Client;

/// Committed records of the test's partition, offsets 0 to `ROWS - 1`.
const ROWS: i64 = 200_000;
/// The timestamp of offset `g` is `BASE + 10 g`, but for the rows below.
const BASE: i64 = 1_600_000_000_000;
/// A record whose timestamp is far ahead of its offset's: a lookup between the
/// timestamps of [`OUTLIER_SWAPS_WITH`] and the outlier's own answers this offset.
const OUTLIER: i64 = 100;
/// The offset whose timestamp the outlier carries.
const OUTLIER_SWAPS_WITH: i64 = 150_000;
/// An aborted record with the largest timestamp: never a match.
const ABORTED: i64 = 10;
/// A record without a timestamp (-1): never a match.
const NO_TIMESTAMP: i64 = 20;
/// The rows of the test's leaf the five lookups may read between them (by sequential or
/// index scans): far below one read of the partition.
const MAX_ROWS_READ: i64 = ROWS / 20;

/// With no index on the record timestamp, a lookup past the last timestamp (as
/// offsetsForTimes(now) and `kafka-consumer-groups --reset-offsets --to-datetime` send)
/// read every row of the partition in offset order, a lookup within the partition every
/// row before the answer, and MAX_TIMESTAMP (-3) sorted the whole partition, on the
/// single DB thread.
///
/// Asserts: in a partition of 200 000 records (timestamps growing with offsets, one
/// record ahead of its offset's time, one aborted record and one without a timestamp)
/// in a leaf of its own, a lookup past the last timestamp, MAX_TIMESTAMP and lookups
/// near the start, near the end and at the out-of-order record together read fewer than
/// 10 000 rows of that leaf (its `seq_tup_read + idx_tup_fetch`, flushed by ending the
/// worker before and after, the worker's maintenance held off by the maintenance lock,
/// the leaf vacuumed, analyzed and its BRIN index summarized up front); each answer is
/// the lowest offset of a non-aborted record with a timestamp at or after the target
/// (the largest timestamp, lowest offset among equal ones, for -3), as a scan of the
/// whole partition finds it; and the planner reads the timestamp index for the lookups
/// that use it.
pub async fn test_list_offsets_by_timestamp_reads_through_an_index() -> TestResult {
    into_test_result(lookups().await)
}

async fn lookups() -> SendResult<()> {
    println!("=== Test: ListOffsets by timestamp and MAX_TIMESTAMP read through an index ===\n");
    let db = session().await?;
    db.batch_execute("SET statement_timeout = '120s'")
        .await
        .map_err(boxed)?;
    let name = unique("ts-index");
    let topic: i32 = db
        .query_one(
            "INSERT INTO kafka.topics (name, partitions, retention_ms) VALUES ($1, 1, -1)
             RETURNING id",
            &[&name],
        )
        .await
        .map_err(boxed)?
        .get(0);
    // The worker's partition maintenance and retention sweep skip while another session
    // holds the maintenance lock, so none of their probes of the leaf lands in the window.
    let result = match db
        .execute("SELECT pg_advisory_lock($1)", &[&MAINTENANCE_LOCK_KEY])
        .await
    {
        Ok(_) => match leaf(&db).await {
            Ok(leaf) => steps(&db, topic, &name, &leaf).await,
            Err(e) => Err(e),
        },
        Err(e) => Err(boxed(e)),
    };
    let unlocked = db
        .execute("SELECT pg_advisory_unlock($1)", &[&MAINTENANCE_LOCK_KEY])
        .await
        .map_err(boxed);
    let cleaned = async {
        db.execute("DELETE FROM kafka.messages WHERE topic_id = $1", &[&topic])
            .await
            .map_err(boxed)?;
        db.query(
            "SELECT dropped FROM kafka.drop_message_partition(
                 to_regclass('kafka.' || $1::text), '{}')",
            &[&LEAF],
        )
        .await
        .map_err(boxed)?;
        db.execute("DELETE FROM kafka.topics WHERE id = $1", &[&topic])
            .await
            .map_err(boxed)?;
        SendResult::Ok(())
    }
    .await;
    result?;
    unlocked?;
    cleaned?;
    println!("✅ every lookup read through the timestamp index");
    Ok(())
}

/// The leaf of the test's rows: an hour of 1998 that nothing else writes to, of a topic
/// with infinite retention, so no maintenance has a reason to read it and its statistics
/// count only what the lookups read.
const LEAF: &str = "messages_p19980301_030000";

/// Create [`LEAF`] and return its name, schema-qualified.
async fn leaf(db: &Client) -> SendResult<String> {
    db.execute(
        "SELECT kafka.create_message_partition(timestamptz '1998-03-01 03:00+00',
                                               timestamptz '1998-03-01 04:00+00')",
        &[],
    )
    .await
    .map_err(boxed)?;
    let leaf = format!("kafka.{LEAF}");
    let found: bool = db
        .query_one("SELECT to_regclass($1) IS NOT NULL", &[&leaf])
        .await
        .map_err(boxed)?
        .get(0);
    if !found {
        return Err(format!("{leaf} was not created").into());
    }
    Ok(leaf)
}

/// One lookup: its name, the ListOffsets timestamp and whether the planner reads the
/// timestamp index for its second step (`Some(false)`: the offset key; `None`: no check).
struct Lookup {
    name: &'static str,
    timestamp: i64,
    earlier_through_timestamp_index: Option<bool>,
}

async fn steps(db: &Client, topic: i32, name: &str, leaf: &str) -> SendResult<()> {
    let max_ts = BASE + 10 * ROWS + 1_000;
    db.execute(
        "INSERT INTO kafka.messages (topic_id, partition_id, partition_offset, value,
                                     timestamp_ms, txn_state, created_at)
         SELECT $1, 0, g, 'v'::bytea,
                CASE WHEN g = $3::bigint THEN $2::bigint + 10 * $4::bigint
                     WHEN g = $5::bigint THEN $7::bigint + 1000
                     WHEN g = $6::bigint THEN -1
                     WHEN g >= $8::bigint - 2 THEN $7::bigint
                     ELSE $2::bigint + 10 * g END,
                CASE WHEN g = $5::bigint THEN 'aborted' END,
                timestamptz '1998-03-01 03:00+00' + g * INTERVAL '10 milliseconds'
         FROM generate_series(0::bigint, $8::bigint - 1) AS g",
        &[
            &topic,
            &BASE,
            &OUTLIER,
            &OUTLIER_SWAPS_WITH,
            &ABORTED,
            &NO_TIMESTAMP,
            &max_ts,
            &ROWS,
        ],
    )
    .await
    .map_err(boxed)?;
    // All-visible pages (so an index-only scan reads no heap), statistics for the
    // planner, and the BRIN index's page ranges summarized: autovacuum would otherwise
    // summarize them as a work item, reading the heap, inside the window or not by chance.
    db.batch_execute(&format!("VACUUM (ANALYZE) {leaf}"))
        .await
        .map_err(boxed)?;
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
        "Step 0: {ROWS} records, offset {OUTLIER} carrying the timestamp of offset \
         {OUTLIER_SWAPS_WITH}, offset {ABORTED} aborted, offset {NO_TIMESTAMP} without one"
    );

    let lookups = [
        Lookup {
            name: "past the last timestamp",
            timestamp: max_ts + 1,
            earlier_through_timestamp_index: None,
        },
        Lookup {
            name: "MAX_TIMESTAMP",
            timestamp: -3,
            earlier_through_timestamp_index: None,
        },
        Lookup {
            name: "near the start",
            timestamp: BASE + 10 * 50 + 5,
            earlier_through_timestamp_index: Some(false),
        },
        Lookup {
            name: "near the end",
            timestamp: BASE + 10 * 180_000 + 5,
            earlier_through_timestamp_index: Some(true),
        },
        Lookup {
            name: "at the out-of-order record",
            timestamp: BASE + 10 * (OUTLIER_SWAPS_WITH - 1_000),
            earlier_through_timestamp_index: Some(true),
        },
    ];
    let mut expected = Vec::new();
    for l in &lookups {
        expected.push(scanned_answer(db, topic, l.timestamp).await?);
    }
    // Before the connection: ending the worker drops it.
    let before = worker_reads(db, leaf).await?;
    let mut wire = Wire::connect().await.map_err(boxed)?;
    let mut answers = Vec::new();
    for l in &lookups {
        answers.push(
            wire.list_offsets(name, 0, l.timestamp, 0)
                .await
                .map_err(boxed)?,
        );
    }
    drop(wire);
    let after = worker_reads(db, leaf).await?;
    let read = after.rows_read - before.rows_read;
    println!(
        "Step 1: the {} lookups read {read} rows of the leaf",
        lookups.len()
    );

    for ((l, answer), want) in lookups.iter().zip(&answers).zip(&expected) {
        println!("  {}: {answer:?}", l.name);
        if *answer != (error_codes::NONE, want.0, want.1) {
            return Err(format!(
                "lookup {} ({}) answered {answer:?}, a scan of the partition finds {want:?}",
                l.name, l.timestamp
            )
            .into());
        }
    }
    if read >= MAX_ROWS_READ {
        return Err(format!(
            "the lookups read {read} rows of the leaf ({ROWS} rows in the partition)"
        )
        .into());
    }
    plans(db, topic, leaf, &lookups).await
}

/// The answer to a lookup of `timestamp` (or -3) found by reading the whole partition:
/// `(offset, timestamp)`, `(-1, -1)` when nothing matches.
async fn scanned_answer(db: &Client, topic: i32, timestamp: i64) -> SendResult<(i64, i64)> {
    let order = if timestamp == -3 {
        "timestamp_ms DESC, partition_offset"
    } else {
        "partition_offset"
    };
    let rows = db
        .query(
            &format!(
                "SELECT partition_offset, timestamp_ms FROM kafka.messages
                 WHERE topic_id = $1 AND partition_id = 0 AND timestamp_ms >= 0
                   AND txn_state IS DISTINCT FROM 'aborted' AND ($2::bigint = -3 OR timestamp_ms >= $2::bigint)
                 ORDER BY {order} LIMIT 1"
            ),
            &[&topic, &timestamp],
        )
        .await
        .map_err(boxed)?;
    Ok(rows
        .first()
        .map_or((-1, -1), |r| (r.get::<_, i64>(0), r.get::<_, i64>(1))))
}

/// The plans of the lookups' statements (as src/kafka/storage/postgres/log.rs writes
/// them) for each lookup, with its arguments: the record with the largest timestamp and
/// the first record at or after a timestamp are read from the timestamp index, and the
/// second step reads the side of the candidate the lookup names.
async fn plans(db: &Client, topic: i32, leaf: &str, lookups: &[Lookup]) -> SendResult<()> {
    let index: String = db
        .query_one(
            "SELECT c.relname::text FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
             WHERE i.indrelid = to_regclass($1)
               AND pg_get_indexdef(i.indexrelid)
                   LIKE '%(topic_id, partition_id, timestamp_ms, partition_offset)%'",
            &[&leaf],
        )
        .await
        .map_err(boxed)?
        .get(0);
    let rows = "timestamp_ms >= 0 AND txn_state IS DISTINCT FROM 'aborted'";
    let max = format!(
        "SELECT partition_offset, timestamp_ms FROM kafka.messages
         WHERE topic_id = {topic} AND partition_id = 0 AND {rows}
         ORDER BY timestamp_ms DESC, partition_offset ASC LIMIT 1"
    );
    expect_plan(db, &max, &index, true).await?;
    for l in lookups.iter().filter(|l| l.timestamp >= 0) {
        let candidate = format!(
            "SELECT partition_offset, timestamp_ms FROM kafka.messages
             WHERE topic_id = {topic} AND partition_id = 0 AND {rows}
               AND timestamp_ms >= {}
             ORDER BY timestamp_ms, partition_offset LIMIT 1",
            l.timestamp
        );
        expect_plan(db, &candidate, &index, true).await?;
        let Some(through_index) = l.earlier_through_timestamp_index else {
            continue;
        };
        let (candidate_offset, _) = scanned_candidate(db, topic, l.timestamp).await?;
        let earlier = format!(
            "SELECT partition_offset, timestamp_ms
             FROM (SELECT partition_offset, timestamp_ms FROM kafka.messages
                   WHERE topic_id = {topic} AND partition_id = 0 AND {rows}
                     AND timestamp_ms >= {} AND partition_offset < {candidate_offset}
                   ORDER BY timestamp_ms, partition_offset) AS earlier
             ORDER BY partition_offset LIMIT 1",
            l.timestamp
        );
        expect_plan(db, &earlier, &index, through_index).await?;
    }
    println!("Step 2: the lookups' plans read the leaf through the indexes expected");
    Ok(())
}

/// The record with the smallest timestamp at or after `timestamp` (the lookups' first
/// step), found by reading the whole partition.
async fn scanned_candidate(db: &Client, topic: i32, timestamp: i64) -> SendResult<(i64, i64)> {
    let row = db
        .query_one(
            "SELECT partition_offset, timestamp_ms FROM kafka.messages
             WHERE topic_id = $1 AND partition_id = 0 AND timestamp_ms >= $2::bigint
               AND txn_state IS DISTINCT FROM 'aborted'
             ORDER BY timestamp_ms, partition_offset LIMIT 1",
            &[&topic, &timestamp],
        )
        .await
        .map_err(boxed)?;
    Ok((row.get(0), row.get(1)))
}

/// `sql`'s plan reads the test's leaf without a sequential scan, through `index` when
/// `through_index`, else through some other index (the offset key).
async fn expect_plan(db: &Client, sql: &str, index: &str, through_index: bool) -> SendResult<()> {
    let text = db
        .simple_query(&format!("EXPLAIN (FORMAT JSON) {sql}"))
        .await
        .map_err(boxed)?
        .into_iter()
        .find_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => r.get(0).map(str::to_string),
            _ => None,
        })
        .ok_or("EXPLAIN returned no plan")?;
    let plan: serde_json::Value = serde_json::from_str(&text).map_err(boxed)?;
    let mut nodes = Vec::new();
    plan_nodes(&plan, &mut nodes);
    let uses_index = nodes.iter().any(|(_, _, i)| i == index);
    let seq_scan_of_leaf = nodes
        .iter()
        .any(|(node, relation, _)| node == "Seq Scan" && relation == LEAF);
    if uses_index != through_index || seq_scan_of_leaf {
        return Err(format!(
            "expected a plan {} {index} and no sequential scan of {LEAF}: {sql}\n{text}",
            if through_index {
                "through"
            } else {
                "not through"
            }
        )
        .into());
    }
    Ok(())
}

/// Every node of an EXPLAIN (FORMAT JSON) plan: `(Node Type, Relation Name, Index Name)`.
fn plan_nodes(value: &serde_json::Value, out: &mut Vec<(String, String, String)>) {
    match value {
        serde_json::Value::Array(items) => items.iter().for_each(|v| plan_nodes(v, out)),
        serde_json::Value::Object(map) => {
            if let Some(node) = map.get("Node Type").and_then(|v| v.as_str()) {
                let field = |k: &str| {
                    map.get(k)
                        .and_then(|v| v.as_str())
                        .unwrap_or_default()
                        .to_string()
                };
                out.push((
                    node.to_string(),
                    field("Relation Name"),
                    field("Index Name"),
                ));
            }
            map.values().for_each(|v| plan_nodes(v, out));
        }
        _ => {}
    }
}
