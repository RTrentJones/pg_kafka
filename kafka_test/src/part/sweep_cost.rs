//! The retention sweep's cost on the single worker DB thread, and what it reaps
//! besides messages.
//!
//! The worker runs a sweep every 60 s whatever `pg_kafka.message_retention_hours` is
//! (0, keep forever, by default): the aborted-row cleanup, the leaf checks and the
//! shadow-outbox reaping run every pass. Each must be index-driven, so a pass with
//! (almost) nothing to do reads (almost) nothing, however much is stored: a full read
//! of a large leaf there stalls every request (Produce, Fetch, Heartbeats) for as
//! long as it takes. The work is counted with `pg_stat_xact_user_tables`, read in the
//! sweep's own transaction (no stats-flush delay, on every Postgres version).
//!
//! Also: outbox rows whose message retention or DeleteRecords removed can never be
//! forwarded; the sweep deletes them, or a dead letter among them keeps an
//! `external_only` topic served locally for good and a pending one keeps the lag
//! above 0 forever.
//!
//! All sequential: they run the sweep, which reaps other tests' rows too.

use crate::admin::configs::delete_records;
use crate::common::{create_producer, DynResult, TestResult};
use crate::setup::TestContext;
use rdkafka::producer::FutureRecord;
use std::time::Duration;
use tokio_postgres::Client;

/// Committed rows stored in one partition: the backlog the sweep must not read.
const BACKLOG_ROWS: i64 = 300_000;
/// Delivered outbox rows, all recent (not yet due for reaping).
const DELIVERED_ROWS: i64 = 200_000;
/// Aborted rows past the grace, which the sweep must delete.
const ABORTED_ROWS: i64 = 20;
/// Delivered outbox rows older than a day, which the sweep must delete.
const OLD_DELIVERED_ROWS: i64 = 100;
/// Tuples a sweep may read from each table with this backlog: far below the backlog
/// (a full read is BACKLOG_ROWS / DELIVERED_ROWS), well above what the due rows and
/// other tests' leftovers need.
const MAX_TUPLES_READ: i64 = 5_000;

/// Rows a sweep removed in one category of `pg_kafka_run_retention_sweep()`.
fn category(rows: &[tokio_postgres::Row], name: &str) -> Option<i64> {
    rows.iter()
        .find(|r| r.get::<_, String>(0) == name)
        .map(|r| r.get::<_, i64>(1))
}

/// Tuples read (sequentially or through an index) from `tables` in this transaction.
async fn tuples_read(db: &Client, tables: &str) -> DynResult<i64> {
    Ok(db
        .query_one(
            &format!(
                "SELECT COALESCE(SUM(seq_tup_read + COALESCE(idx_tup_fetch, 0)), 0)::bigint
                 FROM pg_stat_xact_user_tables WHERE relid IN ({tables})"
            ),
            &[],
        )
        .await?
        .get(0))
}

/// A sweep over a large committed backlog and a large recent outbox reads only the
/// rows it deletes: the aborted rows through the partial index on aborted rows (not
/// every block of the leaf), the delivered outbox rows due for reaping through the
/// delivered-row index (not the whole outbox). On 5d11bea the aborted-row DELETE read
/// every row of the leaf and the delivered-row DELETE scanned the outbox.
pub async fn test_sweep_reads_only_what_it_deletes() -> TestResult {
    println!("=== Test: The Retention Sweep Reads Only What It Deletes ===\n");
    let ctx = TestContext::new().await?;
    let db = ctx.db();
    super::drain_droppable_leaves(db).await?;
    let topic = ctx.unique_topic("sweep-cost").await;

    println!(
        "Step 1: Storing {BACKLOG_ROWS} committed rows, {ABORTED_ROWS} old aborted rows, \
         {DELIVERED_ROWS} recent and {OLD_DELIVERED_ROWS} day-old delivered outbox rows..."
    );
    let topic_id: i32 = db
        .query_one(
            "INSERT INTO kafka.topics (name, partitions) VALUES ($1, 1) RETURNING id",
            &[&topic],
        )
        .await?
        .get(0);
    db.execute(
        "INSERT INTO kafka.messages (topic_id, partition_id, partition_offset, value)
         SELECT $1, 0, g, 'backlog'::bytea FROM generate_series(0, $2::int8 - 1) g",
        &[&topic_id, &BACKLOG_ROWS],
    )
    .await?;
    // Aborted rows written five minutes ago (past the 60 s grace).
    db.execute(
        "INSERT INTO kafka.messages (topic_id, partition_id, partition_offset, value,
                                     created_at, producer_id, producer_epoch, txn_state)
         SELECT $1, 0, $2::int8 + g, 'aborted'::bytea, NOW() - INTERVAL '5 minutes', 1, 0, 'aborted'
         FROM generate_series(0, $3::int8 - 1) g",
        &[&topic_id, &BACKLOG_ROWS, &ABORTED_ROWS],
    )
    .await?;
    db.execute(
        "INSERT INTO kafka.partition_offsets (topic_id, partition_id, next_offset)
         VALUES ($1, 0, $2::int8)",
        &[&topic_id, &(BACKLOG_ROWS + ABORTED_ROWS)],
    )
    .await?;
    db.execute(
        "INSERT INTO kafka.shadow_tracking
             (topic_id, partition_id, local_offset, external_offset, forwarded_at)
         SELECT $1, 0, g, g,
                CASE WHEN g < $2::int8 THEN NOW() - INTERVAL '2 days' ELSE NOW() END
         FROM generate_series(0, $2::int8 + $3::int8 - 1) g",
        &[&topic_id, &OLD_DELIVERED_ROWS, &DELIVERED_ROWS],
    )
    .await?;
    // Planner statistics, as autovacuum would keep them.
    db.batch_execute("ANALYZE kafka.messages; ANALYZE kafka.shadow_tracking;")
        .await?;
    println!("✅ Stored\n");

    println!("Step 2: One sweep (retention off, grace 60 s), counting the tuples it reads...");
    db.batch_execute("BEGIN").await?;
    let rows = db
        .query("SELECT * FROM pg_kafka_run_retention_sweep(0, 60)", &[])
        .await?;
    let leaves_read = tuples_read(
        db,
        "SELECT inhrelid FROM pg_inherits WHERE inhparent = 'kafka.messages'::regclass",
    )
    .await?;
    let outbox_read = tuples_read(db, "SELECT 'kafka.shadow_tracking'::regclass").await?;
    db.batch_execute("COMMIT").await?;
    let aborted = category(&rows, "aborted_messages").unwrap_or(0);
    let delivered = category(&rows, "shadow_delivered_rows").unwrap_or(0);
    println!(
        "   deleted {aborted} aborted row(s), {delivered} delivered outbox row(s); \
         read {leaves_read} message tuple(s), {outbox_read} outbox tuple(s)"
    );

    println!("\n=== Database Verification ===\n");
    let left: i64 = db
        .query_one(
            "SELECT COUNT(*) FROM kafka.messages WHERE topic_id = $1 AND txn_state = 'aborted'",
            &[&topic_id],
        )
        .await?
        .get(0);
    assert_eq!(
        left, 0,
        "the sweep must delete the aborted rows past the grace"
    );
    assert!(aborted >= ABORTED_ROWS, "aborted_messages = {aborted}");
    let (old_left, recent_left): (i64, i64) = {
        let r = db
            .query_one(
                "SELECT COUNT(*) FILTER (WHERE local_offset < $2::int8),
                        COUNT(*) FILTER (WHERE local_offset >= $2::int8)
                 FROM kafka.shadow_tracking WHERE topic_id = $1",
                &[&topic_id, &OLD_DELIVERED_ROWS],
            )
            .await?;
        (r.get(0), r.get(1))
    };
    assert_eq!(
        old_left, 0,
        "delivered rows older than a day must be reaped"
    );
    assert_eq!(
        recent_left, DELIVERED_ROWS,
        "recent delivered rows must stay"
    );
    assert!(
        leaves_read < MAX_TUPLES_READ,
        "the sweep read {leaves_read} tuples of kafka.messages with {BACKLOG_ROWS} rows \
         stored and {ABORTED_ROWS} to delete: the aborted-row cleanup must use its index"
    );
    assert!(
        outbox_read < MAX_TUPLES_READ,
        "the sweep read {outbox_read} outbox tuples with {DELIVERED_ROWS} recent delivered \
         rows and {OLD_DELIVERED_ROWS} due: the reaping must not scan the outbox"
    );
    println!("✅ Verification passed\n");

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

async fn produce(topic: &str, n: usize) -> TestResult {
    let producer = create_producer()?;
    for i in 0..n {
        producer
            .send(
                FutureRecord::to(topic)
                    .key("k")
                    .payload(&format!("v{i}"))
                    .partition(0),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(e, _)| e)?;
    }
    Ok(())
}

async fn topic_id(db: &Client, topic: &str) -> DynResult<i32> {
    Ok(db
        .query_one("SELECT id FROM kafka.topics WHERE name = $1", &[&topic])
        .await?
        .get(0))
}

/// The outbox offsets left for a topic.
async fn outbox_offsets(db: &Client, topic_id: i32) -> DynResult<Vec<i64>> {
    let rows = db
        .query(
            "SELECT local_offset FROM kafka.shadow_tracking WHERE topic_id = $1 ORDER BY 1",
            &[&topic_id],
        )
        .await?;
    Ok(rows.iter().map(|r| r.get(0)).collect())
}

/// Outbox rows whose message DeleteRecords or retention removed are deleted by the
/// next sweep: a dead letter among them no longer marks the topic as having dead
/// letters (which serves an `external_only` topic locally), and the partition's lag
/// and oldest pending offset reflect the rows that can still be forwarded. On 5d11bea
/// nothing deleted them: the reaper took delivered rows only and replay skips rows
/// without a message.
pub async fn test_sweep_reaps_outbox_rows_of_removed_messages() -> TestResult {
    println!("=== Test: The Sweep Reaps Outbox Rows Of Removed Messages ===\n");
    let ctx = TestContext::new().await?;
    let db = ctx.db();
    super::drain_droppable_leaves(db).await?;
    let truncated = ctx.unique_topic("orphan-delrec").await;
    let expired = ctx.unique_topic("orphan-retention").await;

    println!("Step 1: Producing 5 records to {truncated} and 3 to {expired}...");
    produce(&truncated, 5).await?;
    produce(&expired, 3).await?;
    let t1 = topic_id(db, &truncated).await?;
    let t2 = topic_id(db, &expired).await?;
    // Outbox rows as the shadow forwarder leaves them: offset 2 a dead letter, the
    // others pending.
    db.execute(
        "INSERT INTO kafka.shadow_tracking (topic_id, partition_id, local_offset, retry_count, dead)
         VALUES ($1, 0, 2, 10, true), ($1, 0, 3, 0, false), ($1, 0, 4, 0, false),
                ($2, 0, 0, 0, false), ($2, 0, 1, 0, false), ($2, 0, 2, 10, true)",
        &[&t1, &t2],
    )
    .await?;
    println!("✅ Produced, outbox rows written\n");

    println!(
        "Step 2: DeleteRecords before offset 4 on {truncated}; retention.ms=1 on {expired}..."
    );
    let (code, low) = delete_records(&truncated, 0, 4).await?;
    assert_eq!((code, low), (0, 4), "DeleteRecords");
    db.execute(
        "UPDATE kafka.topics SET retention_ms = 1 WHERE id = $1",
        &[&t2],
    )
    .await?;
    tokio::time::sleep(Duration::from_millis(50)).await;
    println!("✅ Done\n");

    println!("Step 3: One sweep...");
    let rows = db
        .query("SELECT * FROM pg_kafka_run_retention_sweep(0, 3600)", &[])
        .await?;
    let orphaned = category(&rows, "shadow_orphaned_rows");
    println!("   shadow_orphaned_rows = {orphaned:?}");

    println!("\n=== Database Verification ===\n");
    assert_eq!(
        outbox_offsets(db, t1).await?,
        vec![4],
        "the outbox rows below the DeleteRecords log start must be gone"
    );
    assert_eq!(
        outbox_offsets(db, t2).await?,
        Vec::<i64>::new(),
        "the outbox rows of expired messages must be gone"
    );
    assert!(orphaned.is_some_and(|n| n >= 5), "orphaned rows counted");
    let status = db
        .query_one(
            "SELECT lag, dead_lettered, oldest_pending_offset
             FROM kafka.shadow_partition_status WHERE topic_id = $1 AND partition_id = 0",
            &[&t1],
        )
        .await?;
    let lag: i64 = status.get(0);
    let dead: i64 = status.get(1);
    let oldest: Option<i64> = status.get(2);
    println!("   {truncated}: lag {lag}, dead_lettered {dead}, oldest_pending {oldest:?}");
    assert_eq!((lag, dead, oldest), (1, 0, Some(4)));
    println!("✅ Verification passed\n");

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// Topics in the leaf of the drop-cost test, one partition and one row each.
const DROP_TOPICS: i32 = 20_000;
/// The expired set of the larger drop: the leaf's topics after this many times as
/// many ids of other topics (the sweep's expired set holds every wholly expired topic,
/// most of them in no given leaf), 160,000 entries, within the default cap of 200,000
/// partitions a client can create topics for.
const EXPIRED_FACTOR: i32 = 8;
/// Runs of each size; the fastest counts (a loaded host only adds time).
const DROP_RUNS: usize = 3;
/// How much longer the drop with the eightfold expired set may take: the re-check is
/// the only part of the drop that reads that set, and a linear one adds tens of
/// milliseconds to a drop that costs hundreds (the contents probe and the log-end read,
/// a btree descent per pair each, and the DROP), so the ratio stays near 1. The array
/// operator `<@` compared every leaf topic with every expired id before it: eight times
/// the work for an eightfold set, 11 times the whole drop at these sizes (0.5 s and
/// 5.9 s here, against 0.13 s and 0.16 s for the linear re-check).
const MAX_DROP_RATIO: f64 = 2.5;

/// The re-check of `kafka.drop_message_partition` (every topic in the leaf is in the
/// expired set) is linear in the two arrays: one entry per (topic, partition) pair in
/// the leaf and one per expired topic, both client-driven. As `<@` it compared every
/// element of one with every element of the other, under the drop's ACCESS EXCLUSIVE
/// lock on kafka.messages, which stops every Produce and Fetch for as long.
///
/// The check is a ratio on one host, not a wall-clock bound: the same drop of the same
/// leaf, rolled back after each run, with the expired set grown eightfold. Only the
/// re-check reads the expired set, so the ratio is what the re-check's growth adds,
/// however fast the host is.
///
/// Asserts: a past leaf holding one row of each of 20,000 topics (infinite retention,
/// so the worker leaves it alone; the function trusts the expired set it is given)
/// drops through `drop_message_partition` with the 20,000 topics as the expired set and
/// with 140,000 other ids before them, and the fastest of 3 runs of the larger takes at
/// most 2.5 times the fastest of the smaller.
pub async fn test_sweep_drop_recheck_is_linear_in_the_leaf_topics() -> TestResult {
    println!("=== Test: a leaf drop's re-check is linear in its topics ===\n");
    let ctx = TestContext::new().await?;
    let db = ctx.db();
    db.batch_execute("SET statement_timeout = '120s'").await?;
    let prefix = ctx.unique_topic("drop-cost").await;
    let topic_ids: Vec<i32> = db
        .query(
            "INSERT INTO kafka.topics (name, partitions, retention_ms)
             SELECT $1 || '-' || g, 1, -1 FROM generate_series(1, $2) AS g
             RETURNING id",
            &[&prefix, &DROP_TOPICS],
        )
        .await?
        .iter()
        .map(|r| r.get(0))
        .collect();
    // Ids of no topic, first, so `<@` would scan past all of them for every leaf topic.
    let mut larger: Vec<i32> = (1..=DROP_TOPICS * (EXPIRED_FACTOR - 1))
        .map(|i| -i)
        .collect();
    larger.extend_from_slice(&topic_ids);
    // A one-hour leaf in 1987 (no other test's leaves or rows use that year), a row per
    // topic.
    let hour: i32 = db
        .query_one("SELECT (random() * 8000)::int", &[])
        .await?
        .get(0);
    db.execute(
        "SELECT kafka.create_message_partition(lo, lo + INTERVAL '1 hour')
         FROM (SELECT timestamptz '1987-01-01 00:00+00' + $1::int * INTERVAL '1 hour') AS b(lo)",
        &[&hour],
    )
    .await?;
    let leaf: String = db
        .query_one(
            "SELECT partition_oid::regclass::text FROM kafka.message_partitions()
             WHERE lower_bound = timestamptz '1987-01-01 00:00+00' + $1::int * INTERVAL '1 hour'",
            &[&hour],
        )
        .await?
        .get(0);
    db.execute(
        "INSERT INTO kafka.messages (topic_id, partition_id, partition_offset, value, created_at)
         SELECT t, 0, 0, 'v'::bytea,
                timestamptz '1987-01-01 00:00+00' + $2::int * INTERVAL '1 hour'
         FROM unnest($1::int[]) AS t",
        &[&topic_ids, &hour],
    )
    .await?;
    db.batch_execute(&format!("ANALYZE {leaf}")).await?;
    println!(
        "Step 1: {leaf} holds one row of each of {DROP_TOPICS} topics; expired sets of {} and \
         {} ids",
        topic_ids.len(),
        larger.len()
    );

    let result = async {
        let mut best = [Duration::MAX; 2];
        for _ in 0..DROP_RUNS {
            for (slot, expired) in [&topic_ids, &larger].into_iter().enumerate() {
                let took = timed_drop(db, &leaf, expired).await?;
                best[slot] = best[slot].min(took);
            }
        }
        let ratio = best[1].as_secs_f64() / best[0].as_secs_f64();
        println!(
            "Step 2: fastest drop {:?} with {} expired ids, {:?} with {} ({ratio:.2}x)",
            best[0],
            topic_ids.len(),
            best[1],
            larger.len()
        );
        assert!(
            ratio <= MAX_DROP_RATIO,
            "an eightfold expired set made the drop {ratio:.2}x as long (limit \
             {MAX_DROP_RATIO}x): the re-check is not linear"
        );
        // Now for good: the leaf goes, so no empty 1987 leaf is left behind.
        let dropped: bool = db
            .query_one(
                "SELECT dropped FROM kafka.drop_message_partition($1::text::regclass, $2::int[])",
                &[&leaf, &topic_ids],
            )
            .await?
            .get(0);
        assert!(
            dropped,
            "{leaf} holds only listed topics and must be dropped"
        );
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    }
    .await;
    db.execute(
        "DELETE FROM kafka.topics WHERE id = ANY ($1::int[])",
        &[&topic_ids],
    )
    .await?;
    result.map_err(|e| e as Box<dyn std::error::Error>)?;
    println!("✅ the drop's re-check stays linear");
    Ok(())
}

/// One drop of `leaf` with `expired` as the expired set, rolled back; returns how long
/// the call took. A sweep holding the maintenance lock makes the call return at once
/// without dropping: it is retried.
async fn timed_drop(
    db: &Client,
    leaf: &str,
    expired: &[i32],
) -> Result<Duration, Box<dyn std::error::Error + Send + Sync>> {
    for _ in 0..20 {
        db.batch_execute("BEGIN").await?;
        let started = std::time::Instant::now();
        let row = db
            .query_one(
                "SELECT dropped, lock_timed_out
                 FROM kafka.drop_message_partition($1::text::regclass, $2::int[])",
                &[&leaf, &expired],
            )
            .await;
        let took = started.elapsed();
        db.batch_execute("ROLLBACK").await?;
        let row = row?;
        let (dropped, timed_out): (bool, bool) = (row.get(0), row.get(1));
        if dropped {
            return Ok(took);
        }
        if !timed_out {
            return Err(format!("{leaf} holds only listed topics and must be dropped").into());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    Err("the maintenance lock stayed held".into())
}
