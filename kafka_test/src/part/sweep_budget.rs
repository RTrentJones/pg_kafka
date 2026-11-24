//! Retention-sweep passes that run out of budget: what such passes still reach, and
//! that a pass stays within its budget when its leaf drops are blocked.

use crate::common::{create_producer, TestResult};
use crate::worker::{boxed, into_test_result, send, session, set_guc, unique, SendResult};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_postgres::Client;

/// Empty past leaves a blocked drop step faces: dropped one after another, each
/// waiting its 2 s lock_timeout, they would hold a pass for about 16 s.
const BLOCKED_LEAVES: i32 = 8;
/// What a pass may take with its drops blocked: the 1.5 s budget, one 2 s drop, and
/// slack (including waiting for a worker pass in progress, bounded the same way).
const BLOCKED_PASS_LIMIT: Duration = Duration::from_secs(6);

/// Test-only GUC: the sweep's pass budget in ms (0 = every bounded step runs exactly
/// one statement per pass).
const PASS_BUDGET_GUC: &str = "pg_kafka.test_retention_pass_budget_ms";

/// Create the one-hour leaf starting `hour` hours after 2000-01-01 (a year no other
/// test's leaves use, so these are the oldest leaves) and return its name.
async fn hour_leaf(db: &Client, hour: i32) -> SendResult<String> {
    db.query_one(
        "SELECT kafka.create_message_partition(lo, lo + INTERVAL '1 hour')
         FROM (SELECT timestamptz '2000-01-01 00:00+00' + $1::int * INTERVAL '1 hour') AS b(lo)",
        &[&hour],
    )
    .await
    .map_err(boxed)?;
    Ok(db
        .query_one(
            "SELECT partition_oid::regclass::text FROM kafka.message_partitions()
             WHERE lower_bound = timestamptz '2000-01-01 00:00+00' + $1::int * INTERVAL '1 hour'",
            &[&hour],
        )
        .await
        .map_err(boxed)?
        .get(0))
}

/// `rows` committed rows of `topic_id` in the leaf of `hour`, from `first_offset`.
async fn fill_leaf(
    db: &Client,
    topic_id: i32,
    hour: i32,
    first_offset: i64,
    rows: i64,
) -> SendResult<()> {
    db.execute(
        "INSERT INTO kafka.messages (topic_id, partition_id, partition_offset, value, created_at)
         SELECT $1, 0, $2::bigint + g, 'v'::bytea,
                timestamptz '2000-01-01 00:00+00' + $3::int * INTERVAL '1 hour'
                    + g * INTERVAL '1 second'
         FROM generate_series(0::bigint, $4::bigint - 1) AS g",
        &[&topic_id, &first_offset, &hour, &rows],
    )
    .await
    .map_err(boxed)?;
    Ok(())
}

async fn leaf_exists(db: &Client, leaf: &str) -> SendResult<bool> {
    Ok(db
        .query_one("SELECT to_regclass($1) IS NOT NULL", &[&leaf])
        .await
        .map_err(boxed)?
        .get(0))
}

async fn create_topic(db: &Client, name: &str, retention_ms: i64) -> SendResult<i32> {
    Ok(db
        .query_one(
            "INSERT INTO kafka.topics (name, partitions, retention_ms) VALUES ($1, 1, $2)
             RETURNING id",
            &[&name, &retention_ms],
        )
        .await
        .map_err(boxed)?
        .get(0))
}

/// The contents probes rotate over every candidate leaf whatever the list size, by the
/// probes each pass actually made. A pass whose budget other steps spent makes only its
/// one mandatory probe; if the rotation did not advance by it (lists of at most 64
/// candidates always restarted at the oldest), every such pass probed the same leaf,
/// and a wholly expired leaf further along was never dropped while the oldest stays
/// (here because it also holds an infinite-retention topic).
///
/// Asserts: with the pass budget at 0, the oldest past leaf holding an expired and an
/// infinite-retention topic, and the next one only the expired topic, repeated sweeps
/// drop the second leaf within two rotations over the probe candidates, and keep the
/// first leaf with the infinite-retention topic's rows.
pub async fn test_sweep_probe_rotation_reaches_every_leaf() -> TestResult {
    into_test_result(probe_rotation().await)
}

async fn probe_rotation() -> SendResult<()> {
    println!("=== Test: contents probes rotate over every leaf on a spent budget ===\n");
    let db = session().await?;
    db.batch_execute("SET statement_timeout = '30s'")
        .await
        .map_err(boxed)?;
    let expired = create_topic(&db, &unique("rot-expired"), 1000).await?;
    let forever = create_topic(&db, &unique("rot-forever"), -1).await?;
    let base_hour: i32 = db
        .query_one("SELECT (random() * 8000)::int", &[])
        .await
        .map_err(boxed)?
        .get(0);
    let kept = hour_leaf(&db, base_hour).await?;
    let droppable = hour_leaf(&db, base_hour + 1).await?;
    fill_leaf(&db, expired, base_hour, 0, 20).await?;
    fill_leaf(&db, forever, base_hour, 0, 20).await?;
    fill_leaf(&db, expired, base_hour + 1, 20, 20).await?;
    println!("Step 0: {kept} (expired + infinite topic), {droppable} (expired topic only)");

    if !set_guc(&db, PASS_BUDGET_GUC, Some("0")).await? {
        return Err(format!("{PASS_BUDGET_GUC} is not known to the server").into());
    }
    let result: SendResult<()> = async {
        let candidates: i64 = db
            .query_one(
                "SELECT count(*) FROM kafka.message_partitions()
                 WHERE NOT is_default AND upper_bound <= now()",
                &[],
            )
            .await
            .map_err(boxed)?
            .get(0);
        let passes = 2 * candidates + 2;
        let mut dropped_after = None;
        for pass in 1..=passes {
            db.query("SELECT * FROM pg_kafka_run_retention_sweep(0, 3600)", &[])
                .await
                .map_err(boxed)?;
            if !leaf_exists(&db, &droppable).await? {
                dropped_after = Some(pass);
                break;
            }
        }
        let Some(pass) = dropped_after else {
            return Err(format!(
                "{droppable} still exists after {passes} sweeps over {candidates} past leaves"
            )
            .into());
        };
        println!("Step 1: {droppable} dropped after {pass} sweep(s) ({candidates} past leaves)");
        assert!(leaf_exists(&db, &kept).await?, "{kept} must stay");
        let left: i64 = db
            .query_one(
                &format!("SELECT count(*) FROM {kept} WHERE topic_id = $1"),
                &[&forever],
            )
            .await
            .map_err(boxed)?
            .get(0);
        assert_eq!(left, 20, "the infinite-retention rows must stay");
        Ok(())
    }
    .await;
    set_guc(&db, PASS_BUDGET_GUC, None).await?;
    // The kept leaf holds only this test's topics: drop it as if both were expired.
    db.query(
        "SELECT dropped FROM kafka.drop_message_partition($1::text::regclass, ARRAY[$2::int, $3::int])",
        &[&kept, &expired, &forever],
    )
    .await
    .map_err(boxed)?;
    result?;
    println!("✅ every probe candidate was reached");
    Ok(())
}

/// The whole-leaf drop step is bounded like the others: within the pass budget, and it
/// stops after a drop that ran out of a lock wait. Each `drop_message_partition` call can
/// wait 2 s for a session holding a lock on kafka.messages or a leaf (here a reader idle
/// in transaction after a SELECT on kafka.messages), and without the bound a pass tried
/// every planned leaf in turn, holding the worker's single DB thread for 2 s per leaf.
///
/// Asserts: with a reader holding ACCESS SHARE on kafka.messages and eight empty past
/// leaves, a sweep returns within 6 s having dropped none, a producer running meanwhile
/// sees no ack slower than 2 s; once the reader ends, the next sweep drops all eight.
pub async fn test_sweep_drops_bounded_behind_a_held_reader() -> TestResult {
    into_test_result(blocked_drops().await)
}

async fn blocked_drops() -> SendResult<()> {
    println!("=== Test: a sweep's leaf drops stay within its budget behind a reader ===\n");
    let db = session().await?;
    db.batch_execute("SET statement_timeout = '60s'")
        .await
        .map_err(boxed)?;
    let base_hour: i32 = db
        .query_one("SELECT (random() * 8000)::int", &[])
        .await
        .map_err(boxed)?
        .get(0);
    let mut leaves = Vec::new();
    for k in 0..BLOCKED_LEAVES {
        leaves.push(hour_leaf(&db, base_hour + k).await?);
    }
    println!(
        "Step 0: {} empty past leaves from {}",
        leaves.len(),
        leaves[0]
    );

    let reader = session().await?;
    reader
        .batch_execute("BEGIN; SELECT count(*) FROM kafka.messages")
        .await
        .map_err(boxed)?;

    let topic = unique("blocked-drops");
    let stop = Arc::new(AtomicBool::new(false));
    let producing = {
        let stop = stop.clone();
        let topic = topic.clone();
        tokio::spawn(async move {
            let producer = create_producer().map_err(boxed)?;
            let mut slowest = Duration::ZERO;
            while !stop.load(Ordering::SeqCst) {
                let t = Instant::now();
                send(&producer, &topic, "p").await?;
                slowest = slowest.max(t.elapsed());
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            SendResult::Ok(slowest)
        })
    };
    // Let the producer create its topic before the sweep starts.
    tokio::time::sleep(Duration::from_secs(1)).await;

    let result: SendResult<()> = async {
        let t = Instant::now();
        let dropped = dropped_partitions(&db).await?;
        let took = t.elapsed();
        println!("Step 1: sweep behind the reader took {took:?}, dropped {dropped}");
        assert!(
            took < BLOCKED_PASS_LIMIT,
            "a sweep with blocked drops took {took:?}"
        );
        assert_eq!(
            dropped, 0,
            "no leaf can be dropped while the reader holds its lock"
        );
        Ok(())
    }
    .await;
    stop.store(true, Ordering::SeqCst);
    let slowest = producing.await.map_err(boxed)?;
    reader.batch_execute("ROLLBACK").await.map_err(boxed)?;
    result?;
    let slowest = slowest?;
    println!("Step 2: slowest produce ack during the sweep {slowest:?}");
    assert!(
        slowest < Duration::from_secs(2),
        "a produce ack took {slowest:?} during the sweep"
    );

    let mut left = leaves.len();
    for _ in 0..3 {
        dropped_partitions(&db).await?;
        left = 0;
        for leaf in &leaves {
            if leaf_exists(&db, leaf).await? {
                left += 1;
            }
        }
        if left == 0 {
            break;
        }
    }
    assert_eq!(
        left, 0,
        "the leaves must be dropped once the reader is gone"
    );
    println!("✅ the blocked pass stayed bounded and the next one dropped every leaf");
    Ok(())
}

/// One sweep (global retention off, aborted grace 1 h); returns dropped_partitions.
async fn dropped_partitions(db: &Client) -> SendResult<i64> {
    let rows = db
        .query(
            "SELECT category, deleted FROM pg_kafka_run_retention_sweep(0, 3600)",
            &[],
        )
        .await
        .map_err(boxed)?;
    rows.iter()
        .find(|r| r.get::<_, String>(0) == "dropped_partitions")
        .map(|r| r.get::<_, i64>(1))
        .ok_or_else(|| "missing dropped_partitions".into())
}
