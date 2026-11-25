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

/// Topics in the leaf of the drop-cost test, one partition and one row each: a client
/// can create this many (CreateTopics, the default cap is 200,000 partitions) and give
/// each a short `retention.ms`.
const DROP_TOPICS: i32 = 80_000;
/// What the drop of that leaf may take: its contents probe and its log-end read (one
/// btree descent per pair each) and the DROP, with slack for a loaded host. Comparing
/// every topic in the leaf with every expired topic (the array operator `<@`) took
/// about 6 s here, holding ACCESS EXCLUSIVE on kafka.messages.
const DROP_LIMIT: Duration = Duration::from_millis(2_500);

/// The re-check of `kafka.drop_message_partition` (every topic in the leaf is in the
/// expired set) is linear in the two arrays: one entry per (topic, partition) pair in
/// the leaf and one per expired topic, both client-driven. As `<@` it compared every
/// element of one with every element of the other, under the drop's ACCESS EXCLUSIVE
/// lock on kafka.messages, which stops every Produce and Fetch for as long.
///
/// Asserts: a past leaf holding one row of each of 80,000 topics, all wholly expired,
/// drops (through `drop_message_partition` with the 80,000 expired topics, as the
/// sweep calls it) within 2.5 s.
pub async fn test_sweep_drop_recheck_is_linear_in_the_leaf_topics() -> TestResult {
    println!("=== Test: a leaf drop's re-check is linear in its topics ===\n");
    let ctx = TestContext::new().await?;
    let db = ctx.db();
    db.batch_execute("SET statement_timeout = '120s'").await?;
    let prefix = ctx.unique_topic("drop-cost").await;
    let topic_ids: Vec<i32> = db
        .query(
            "INSERT INTO kafka.topics (name, partitions, retention_ms)
             SELECT $1 || '-' || g, 1, 1000 FROM generate_series(1, $2) AS g
             RETURNING id",
            &[&prefix, &DROP_TOPICS],
        )
        .await?
        .iter()
        .map(|r| r.get(0))
        .collect();
    // A one-hour leaf in 2001 (no other test's leaves use that year), a row per topic.
    let hour: i32 = db
        .query_one("SELECT (random() * 8000)::int", &[])
        .await?
        .get(0);
    db.execute(
        "SELECT kafka.create_message_partition(lo, lo + INTERVAL '1 hour')
         FROM (SELECT timestamptz '2001-01-01 00:00+00' + $1::int * INTERVAL '1 hour') AS b(lo)",
        &[&hour],
    )
    .await?;
    let leaf: String = db
        .query_one(
            "SELECT partition_oid::regclass::text FROM kafka.message_partitions()
             WHERE lower_bound = timestamptz '2001-01-01 00:00+00' + $1::int * INTERVAL '1 hour'",
            &[&hour],
        )
        .await?
        .get(0);
    db.execute(
        "INSERT INTO kafka.messages (topic_id, partition_id, partition_offset, value, created_at)
         SELECT t, 0, 0, 'v'::bytea,
                timestamptz '2001-01-01 00:00+00' + $2::int * INTERVAL '1 hour'
         FROM unnest($1::int[]) AS t",
        &[&topic_ids, &hour],
    )
    .await?;
    println!("Step 1: {leaf} holds one row of each of {DROP_TOPICS} expired topics");

    let started = std::time::Instant::now();
    let dropped: bool = db
        .query_one(
            "SELECT dropped FROM kafka.drop_message_partition($1::text::regclass, $2::int[])",
            &[&leaf, &topic_ids],
        )
        .await?
        .get(0);
    let took = started.elapsed();
    println!("Step 2: dropped {dropped} in {took:?}");
    db.execute(
        "DELETE FROM kafka.topics WHERE id = ANY ($1::int[])",
        &[&topic_ids],
    )
    .await?;
    assert!(
        dropped,
        "{leaf} holds only expired topics and must be dropped"
    );
    assert!(
        took <= DROP_LIMIT,
        "dropping a leaf of {DROP_TOPICS} expired topics took {took:?} (limit {DROP_LIMIT:?})"
    );
    println!("✅ the drop's re-check stays linear");
    Ok(())
}
