//! E2E tests for the time-partitioned `kafka.messages` (ADR-001).
//!
//! The tests are width-agnostic: they read `pg_kafka.message_partition_minutes` and
//! backdate rows by whole multiples of it (an UPDATE of created_at moves a row into
//! kafka.messages_default when no leaf covers that time; partition maintenance then
//! gives it a leaf). Retention is set straight in kafka.topics.retention_ms (the
//! IncrementalAlterConfigs path is covered in admin/configs.rs).
//!
//! All `parallel_safe: false`: they create and drop leaves far in the past and run
//! the sweep, which also reaps other topics' rows that have a per-topic window.

use crate::common::{
    create_base_consumer, create_producer, create_read_committed_consumer,
    create_transactional_producer, TestResult,
};
use crate::setup::TestContext;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::{Message, Offset, TopicPartitionList};
use std::time::{Duration, Instant};
use tokio_postgres::Client;

use crate::common::DynResult;

/// `pg_kafka.message_partition_minutes` in milliseconds.
async fn width_ms(db: &Client) -> DynResult<i64> {
    Ok(db
        .query_one(
            "SELECT current_setting('pg_kafka.message_partition_minutes')::bigint * 60000",
            &[],
        )
        .await?
        .get(0))
}

async fn produce(producer: &FutureProducer, topic: &str, values: &[String]) -> DynResult<()> {
    for v in values {
        producer
            .send(
                FutureRecord::to(topic).key("k").payload(v).partition(0),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(e, _)| e)?;
    }
    Ok(())
}

/// Move rows `(topic, first offset, last offset)` to `age_ms` in the past, all in one
/// statement so they share one NOW() and land in the same leaf.
async fn backdate(db: &Client, ranges: &[(&str, i64, i64)], age_ms: i64) -> DynResult<()> {
    let names: Vec<String> = ranges.iter().map(|r| r.0.to_string()).collect();
    let froms: Vec<i64> = ranges.iter().map(|r| r.1).collect();
    let tos: Vec<i64> = ranges.iter().map(|r| r.2).collect();
    db.execute(
        "UPDATE kafka.messages m SET created_at = NOW() - $4::bigint * INTERVAL '1 millisecond'
         FROM unnest($1::text[], $2::bigint[], $3::bigint[]) AS r(name, lo, hi)
         JOIN kafka.topics t ON t.name = r.name
         WHERE m.topic_id = t.id AND m.partition_offset BETWEEN r.lo AND r.hi",
        &[&names, &froms, &tos, &age_ms],
    )
    .await?;
    Ok(())
}

/// The leaf holding one row.
async fn leaf_of(db: &Client, topic: &str, offset: i64) -> DynResult<String> {
    Ok(db
        .query_one(
            "SELECT m.tableoid::regclass::text FROM kafka.messages m
             JOIN kafka.topics t ON t.id = m.topic_id
             WHERE t.name = $1 AND m.partition_offset = $2",
            &[&topic, &offset],
        )
        .await?
        .get(0))
}

/// The leaves holding a topic's rows, by name.
async fn leaves_of(db: &Client, topic: &str) -> DynResult<Vec<String>> {
    let rows = db
        .query(
            "SELECT DISTINCT m.tableoid::regclass::text FROM kafka.messages m
             JOIN kafka.topics t ON t.id = m.topic_id WHERE t.name = $1 ORDER BY 1",
            &[&topic],
        )
        .await?;
    Ok(rows.iter().map(|r| r.get(0)).collect())
}

async fn leaf_exists(db: &Client, leaf: &str) -> DynResult<bool> {
    Ok(db
        .query_one("SELECT to_regclass($1) IS NOT NULL", &[&leaf])
        .await?
        .get(0))
}

async fn set_retention(db: &Client, topic: &str, retention_ms: i64) -> DynResult<()> {
    db.execute(
        "UPDATE kafka.topics SET retention_ms = $2 WHERE name = $1",
        &[&topic, &retention_ms],
    )
    .await?;
    Ok(())
}

/// One sweep (global retention off, aborted grace 1 h so concurrent transaction tests'
/// aborted rows are left alone); returns (expired_messages, dropped_partitions).
async fn sweep(db: &Client) -> DynResult<(i64, i64)> {
    let rows = db
        .query(
            "SELECT category, deleted FROM pg_kafka_run_retention_sweep(0, 3600)",
            &[],
        )
        .await?;
    let get = |c: &str| {
        rows.iter()
            .find(|r| r.get::<_, String>(0) == c)
            .map(|r| r.get::<_, i64>(1))
    };
    Ok((
        get("expired_messages").ok_or("missing expired_messages")?,
        get("dropped_partitions").ok_or("missing dropped_partitions")?,
    ))
}

async fn offsets_of(db: &Client, topic: &str) -> DynResult<Vec<i64>> {
    let rows = db
        .query(
            "SELECT m.partition_offset FROM kafka.messages m JOIN kafka.topics t ON t.id = m.topic_id
             WHERE t.name = $1 ORDER BY 1",
            &[&topic],
        )
        .await?;
    Ok(rows.iter().map(|r| r.get(0)).collect())
}

async fn log_start(db: &Client, topic: &str) -> DynResult<i64> {
    Ok(db
        .query_one(
            "SELECT po.log_start_offset FROM kafka.partition_offsets po
             JOIN kafka.topics t ON t.id = po.topic_id WHERE t.name = $1 AND po.partition_id = 0",
            &[&topic],
        )
        .await?
        .get(0))
}

/// Read from the beginning of partition 0 until `n` messages arrived (or 15 s).
fn consume_from_beginning(
    consumer: &BaseConsumer,
    topic: &str,
    n: usize,
) -> DynResult<Vec<(i64, String)>> {
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(topic, 0, Offset::Beginning)?;
    consumer.assign(&tpl)?;
    let mut out = Vec::new();
    let start = Instant::now();
    while out.len() < n && start.elapsed() < Duration::from_secs(15) {
        if let Some(Ok(m)) = consumer.poll(Duration::from_millis(500)) {
            let v = m
                .payload()
                .map_or_default(|p| String::from_utf8_lossy(p).to_string());
            out.push((m.offset(), v));
        }
    }
    Ok(out)
}

/// Retention drops a time leaf whole once every row in it is expired; a leaf shared
/// with an infinite-retention topic is kept and only the expired topic's rows in it
/// are deleted (the row-delete fallback). Offsets, log_start_offset, ListOffsets and
/// a consumer reading from the beginning all see exactly the surviving suffix.
pub async fn test_time_partition_retention_drops_whole_partitions() -> TestResult {
    println!("=== Test: Time-Partition Retention Drops Whole Partitions ===\n");
    let ctx = TestContext::new().await?;
    let db = ctx.db();
    super::drain_droppable_leaves(db).await?;
    let topic = ctx.unique_topic("part-retention").await;
    let keeper = ctx.unique_topic("part-infinite").await;
    let w = width_ms(db).await?;
    let producer = create_producer()?;

    println!("Step 1: Producing 3 batches of 5 to {topic} and 2 records to {keeper}...");
    let batch = |b: usize| -> Vec<String> { (0..5).map(|i| format!("b{b}-{i}")).collect() };
    produce(&producer, &topic, &batch(0)).await?;
    produce(&producer, &topic, &batch(1)).await?;
    produce(&producer, &topic, &batch(2)).await?;
    produce(&producer, &keeper, &["keep-0".into(), "keep-1".into()]).await?;

    // Batch 0 -> leaf A (40 widths ago), batch 1 + the keeper -> leaf B (39 widths
    // ago), batch 2 stays in the current leaf.
    backdate(db, &[(&topic, 0, 4)], 40 * w).await?;
    backdate(db, &[(&topic, 5, 9), (&keeper, 0, 1)], 39 * w).await?;
    let created: i64 = db
        .query_one("SELECT pg_kafka_run_partition_maintenance()", &[])
        .await?
        .get(0);
    println!("   maintenance created {created} leaf/leaves");

    let leaves = leaves_of(db, &topic).await?;
    assert_eq!(leaves.len(), 3, "batches must sit in 3 leaves: {leaves:?}");
    assert!(
        !leaves.iter().any(|l| l.ends_with("messages_default")),
        "maintenance must move backdated rows out of the DEFAULT leaf: {leaves:?}"
    );
    let leaf_a = leaf_of(db, &topic, 0).await?;
    let leaf_b = leaf_of(db, &topic, 5).await?;
    let leaf_c = leaf_of(db, &topic, 10).await?;
    assert_eq!(leaves_of(db, &keeper).await?, vec![leaf_b.clone()]);
    println!("✅ A={leaf_a} B={leaf_b} (shared with the infinite topic) C={leaf_c}\n");

    println!("Step 2: retention.ms = 10 widths on {topic}, -1 (infinite) on {keeper}; sweep...");
    set_retention(db, &topic, 10 * w).await?;
    set_retention(db, &keeper, -1).await?;
    let (expired, dropped) = sweep(db).await?;
    println!("   sweep: expired_messages={expired} dropped_partitions={dropped}");
    assert!(dropped >= 1, "leaf A must be dropped whole");
    assert!(
        expired >= 5,
        "batch 1 must be row-deleted from the shared leaf B"
    );
    assert!(!leaf_exists(db, &leaf_a).await?, "{leaf_a} must be gone");
    assert!(
        leaf_exists(db, &leaf_b).await?,
        "{leaf_b} holds an infinite topic"
    );
    assert!(leaf_exists(db, &leaf_c).await?, "the current leaf stays");
    println!("✅ A dropped, B kept, C kept\n");

    println!("Step 3: Offsets and log start...");
    assert_eq!(
        offsets_of(db, &topic).await?,
        (10..15).collect::<Vec<i64>>()
    );
    assert_eq!(offsets_of(db, &keeper).await?, vec![0, 1]);
    assert_eq!(
        log_start(db, &topic).await?,
        10,
        "log start = first survivor"
    );
    assert_eq!(log_start(db, &keeper).await?, 0, "infinite topic untouched");
    let consumer = create_base_consumer(&ctx.unique_group("part-reader").await)?;
    let (low, high) = consumer.fetch_watermarks(&topic, 0, Duration::from_secs(5))?;
    assert_eq!((low, high), (10, 15), "ListOffsets earliest/latest");
    println!("✅ offsets 10..14 survive, log_start=10, watermarks (10, 15)\n");

    println!("Step 4: A consumer from the beginning reads exactly the remainder...");
    let got = consume_from_beginning(&consumer, &topic, 5)?;
    let expected: Vec<(i64, String)> = (10..15).zip(batch(2)).collect();
    assert_eq!(got, expected);
    println!("✅ read offsets 10..14 in order\n");

    println!("Step 5: The next produce continues at 15 (no offset reuse)...");
    produce(&producer, &topic, &["after".into()]).await?;
    assert_eq!(offsets_of(db, &topic).await?.last().copied(), Some(15));
    println!("✅ next offset 15\n");

    // Cleanup: deleting the topics empties leaf B; an empty past leaf is dropped by
    // the next sweep.
    ctx.cleanup().await?;
    sweep(db).await?;
    assert!(
        !leaf_exists(db, &leaf_b).await?,
        "an empty past leaf is dropped"
    );
    println!("✅ Test PASSED\n");
    Ok(())
}

/// A pending transactional row keeps its expired leaf (and is never deleted); the
/// commit's txn_state UPDATE reaches the row in that old leaf without moving it;
/// once committed, the leaf is dropped and log_start moves to the high watermark.
pub async fn test_time_partition_retention_waits_for_pending_transaction() -> TestResult {
    println!("=== Test: Time-Partition Retention Waits For a Pending Transaction ===\n");
    let ctx = TestContext::new().await?;
    let db = ctx.db();
    super::drain_droppable_leaves(db).await?;
    let topic = ctx.unique_topic("part-txn").await;
    let txn_id = format!("part-txn-{}", uuid::Uuid::new_v4());
    let w = width_ms(db).await?;

    println!("Step 1: Transactional produce of 3 records, left open...");
    let producer = create_transactional_producer(&txn_id)?;
    producer.init_transactions(Duration::from_secs(10))?;
    producer.begin_transaction()?;
    produce(&producer, &topic, &["t0".into(), "t1".into(), "t2".into()]).await?;
    backdate(db, &[(&topic, 0, 2)], 45 * w).await?;
    db.query_one("SELECT pg_kafka_run_partition_maintenance()", &[])
        .await?;
    let leaves = leaves_of(db, &topic).await?;
    assert_eq!(leaves.len(), 1, "{leaves:?}");
    let leaf = leaves[0].clone();
    assert!(!leaf.ends_with("messages_default"));
    set_retention(db, &topic, 10 * w).await?;
    println!("✅ 3 pending rows in {leaf}, 45 widths old, retention 10 widths\n");

    println!("Step 2: Sweep keeps the leaf and the pending rows...");
    sweep(db).await?;
    assert!(leaf_exists(db, &leaf).await?, "pending rows block the drop");
    let pending: i64 = db
        .query_one(
            "SELECT count(*) FROM kafka.messages m JOIN kafka.topics t ON t.id = m.topic_id
             WHERE t.name = $1 AND m.txn_state = 'pending'",
            &[&topic],
        )
        .await?
        .get(0);
    assert_eq!(pending, 3, "pending rows are never deleted");
    println!("✅ leaf kept, 3 pending rows intact\n");

    println!("Step 3: Commit; a read_committed consumer reads the 3 records...");
    producer.commit_transaction(Duration::from_secs(10))?;
    assert_eq!(
        leaves_of(db, &topic).await?,
        vec![leaf.clone()],
        "no row moved"
    );
    let consumer = create_read_committed_consumer(&ctx.unique_group("part-txn").await)?;
    let got = consume_from_beginning(&consumer, &topic, 3)?;
    let values: Vec<String> = got.into_iter().map(|(_, v)| v).collect();
    assert_eq!(values, vec!["t0", "t1", "t2"]);
    println!("✅ committed rows visible in the old leaf\n");

    println!("Step 4: Sweep now drops the leaf; log start = high watermark...");
    let (_, dropped) = sweep(db).await?;
    assert!(dropped >= 1);
    assert!(!leaf_exists(db, &leaf).await?, "{leaf} must be dropped");
    assert_eq!(offsets_of(db, &topic).await?, Vec::<i64>::new());
    let (low, high) = consumer.fetch_watermarks(&topic, 0, Duration::from_secs(5))?;
    assert!(high >= 3);
    assert_eq!(low, high, "emptied partition: earliest == latest");
    assert_eq!(log_start(db, &topic).await?, high);
    println!("✅ dropped; watermarks ({low}, {high})\n");

    ctx.cleanup().await?;
    db.execute(
        "DELETE FROM kafka.transactions WHERE transactional_id = $1",
        &[&txn_id],
    )
    .await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// Maintenance keeps the current and the next two leaves ready, fresh produces land
/// in a time leaf (not DEFAULT), the metrics view counts the leaves, and the worker
/// role only gets EXECUTE on the partition functions (no DDL of its own, no direct
/// access to time leaves, nothing for PUBLIC).
pub async fn test_time_partition_maintenance_and_least_privilege() -> TestResult {
    println!("=== Test: Time-Partition Maintenance and Least Privilege ===\n");
    let ctx = TestContext::new().await?;
    let db = ctx.db();
    let topic = ctx.unique_topic("part-maint").await;
    let w = width_ms(db).await?;

    println!("Step 1: Maintenance covers now and the next two widths...");
    db.query_one("SELECT pg_kafka_run_partition_maintenance()", &[])
        .await?;
    for k in 0..3i64 {
        let n: i64 = db
            .query_one(
                "SELECT count(*) FROM kafka.message_partitions()
                 WHERE NOT is_default
                   AND lower_bound <= NOW() + $1::bigint * INTERVAL '1 millisecond'
                   AND upper_bound > NOW() + $1::bigint * INTERVAL '1 millisecond'",
                &[&(k * w)],
            )
            .await?
            .get(0);
        assert_eq!(n, 1, "exactly one leaf must cover now + {k} widths");
    }
    let again: i64 = db
        .query_one("SELECT pg_kafka_run_partition_maintenance()", &[])
        .await?
        .get(0);
    assert_eq!(again, 0, "maintenance is idempotent");
    println!("✅ leaves ready; a second pass creates nothing\n");

    println!("Step 2: A produce lands in a time leaf...");
    let producer = create_producer()?;
    produce(&producer, &topic, &["x".into()]).await?;
    let leaves = leaves_of(db, &topic).await?;
    assert_eq!(leaves.len(), 1);
    assert!(!leaves[0].ends_with("messages_default"), "{leaves:?}");
    let metric: i64 = db
        .query_one(
            "SELECT value FROM kafka.metrics WHERE category = 'messages' AND metric = 'time_partitions'",
            &[],
        )
        .await?
        .get(0);
    assert!(metric >= 4, "DEFAULT + 3 time leaves, got {metric}");
    println!(
        "✅ produced into {}; metrics report {metric} leaves\n",
        leaves[0]
    );

    println!("Step 3: Privileges...");
    let leaf = leaves[0].clone();
    let row = db
        .query_one(
            "SELECT
               has_function_privilege('pgkafka_worker', 'kafka.ensure_message_partitions(int, int)', 'EXECUTE'),
               has_function_privilege('pgkafka_worker', 'kafka.drop_message_partition(regclass, int[])', 'EXECUTE'),
               has_function_privilege('pgkafka_worker', 'kafka.create_message_partition(timestamptz, timestamptz)', 'EXECUTE'),
               has_function_privilege('public', 'kafka.drop_message_partition(regclass, int[])', 'EXECUTE'),
               has_table_privilege('pgkafka_worker', $1, 'SELECT'),
               has_schema_privilege('pgkafka_worker', 'kafka', 'CREATE'),
               (SELECT prosecdef FROM pg_proc WHERE oid = 'kafka.drop_message_partition(regclass, int[])'::regprocedure)",
            &[&leaf],
        )
        .await?;
    let flags: Vec<bool> = (0..7).map(|i| row.get(i)).collect();
    assert_eq!(
        flags,
        vec![true, true, false, false, false, false, true],
        "[worker ensure, worker drop, worker create, public drop, worker on leaf, worker CREATE on schema, drop is SECURITY DEFINER]"
    );
    // The definer functions refuse anything that is not one of their leaves.
    for sql in [
        "SELECT * FROM kafka.message_partition_contents('kafka.topics'::regclass)",
        "SELECT * FROM kafka.drop_message_partition('kafka.messages_default'::regclass, '{}')",
        "SELECT * FROM kafka.drop_message_partition('kafka.topics'::regclass, '{}')",
    ] {
        assert!(db.query(sql, &[]).await.is_err(), "must be refused: {sql}");
    }
    // A non-empty current leaf is not dropped even if asked (topic not expired).
    let dropped: bool = db
        .query_one(
            "SELECT dropped FROM kafka.drop_message_partition($1::text::regclass, '{}')",
            &[&leaf],
        )
        .await?
        .get(0);
    assert!(!dropped, "a leaf with unexpired topics must not be dropped");
    assert!(leaf_exists(db, &leaf).await?);
    println!("✅ EXECUTE-only, definer functions validate their argument\n");

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// Retention keeps offsets monotonic for a partition that has no
/// `kafka.partition_offsets` counter row (rows written by direct SQL, or before the
/// counter existed): emptied by a whole-leaf drop or by row deletes, its next produce
/// continues after the removed offsets instead of reusing 0.
pub async fn test_retention_without_offset_counter_keeps_offsets() -> TestResult {
    println!("=== Test: Retention Without an Offset Counter Keeps Offsets ===\n");
    let ctx = TestContext::new().await?;
    let db = ctx.db();
    super::drain_droppable_leaves(db).await?;
    let dropped = ctx.unique_topic("part-nocounter-drop").await;
    let deleted = ctx.unique_topic("part-nocounter-del").await;
    let keeper = ctx.unique_topic("part-nocounter-keep").await;
    let w = width_ms(db).await?;
    let producer = create_producer()?;
    let five: Vec<String> = (0..5).map(|i| format!("v{i}")).collect();

    println!("Step 1: 5 records each, counters removed, rows aged into past leaves...");
    produce(&producer, &dropped, &five).await?;
    produce(&producer, &deleted, &five).await?;
    produce(&producer, &keeper, &["keep".into()]).await?;
    db.execute(
        "DELETE FROM kafka.partition_offsets po USING kafka.topics t
         WHERE t.id = po.topic_id AND t.name = ANY($1)",
        &[&vec![dropped.clone(), deleted.clone()]],
    )
    .await?;
    // `dropped` alone in a leaf (dropped whole); `deleted` shares one with an
    // infinite-retention topic (row deletes).
    backdate(db, &[(&dropped, 0, 4)], 40 * w).await?;
    backdate(db, &[(&deleted, 0, 4), (&keeper, 0, 0)], 38 * w).await?;
    db.query_one("SELECT pg_kafka_run_partition_maintenance()", &[])
        .await?;
    set_retention(db, &dropped, 10 * w).await?;
    set_retention(db, &deleted, 10 * w).await?;
    set_retention(db, &keeper, -1).await?;
    println!("✅ Ready\n");

    println!("Step 2: Sweep...");
    let (expired, dropped_leaves) = sweep(db).await?;
    println!("   expired_messages={expired} dropped_partitions={dropped_leaves}");
    assert!(dropped_leaves >= 1 && expired >= 5);
    assert_eq!(offsets_of(db, &dropped).await?, Vec::<i64>::new());
    assert_eq!(offsets_of(db, &deleted).await?, Vec::<i64>::new());
    for t in [&dropped, &deleted] {
        assert_eq!(
            log_start(db, t).await?,
            5,
            "{t}: log start after the removed offsets"
        );
    }
    println!("✅ Both emptied; log start 5\n");

    println!("Step 3: The next produce continues at 5...");
    for t in [&dropped, &deleted] {
        produce(&producer, t, &["after".into()]).await?;
        assert_eq!(offsets_of(db, t).await?, vec![5], "{t}: no offset reuse");
    }
    println!("✅ Offset 5 on both\n");

    ctx.cleanup().await?;
    sweep(db).await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
