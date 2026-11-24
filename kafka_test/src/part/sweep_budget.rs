//! Retention-sweep passes that run out of budget: what such passes still reach, and
//! that a pass stays within its budget when its leaf drops are blocked or a step has a
//! large backlog.

use crate::common::{create_producer, TestResult};
use crate::setup::SystemGucGuard;
use crate::worker::{boxed, into_test_result, send, session, unique, SendResult};
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

/// The slowest produce ack the blocked-drops test accepts. Its own sweep is not the only
/// one: the worker's (every 60 s) may run just before it and, bounded the same way, stall
/// the DB thread for one 2 s drop wait plus its other steps, so the ceiling sits above
/// one bounded drop wait; dropping every blocked leaf in turn would stall for about 16 s.
const PRODUCE_ACK_CEILING: Duration = Duration::from_secs(5);

/// Test-only GUC: the sweep's pass budget in ms (0 = every bounded step runs exactly
/// one statement per pass).
const PASS_BUDGET_GUC: &str = "pg_kafka.test_retention_pass_budget_ms";

/// Set the pass budget until the returned guard is restored (or dropped: after a
/// failed assert or a timeout).
async fn pass_budget(db: &Client, ms: &str) -> SendResult<SystemGucGuard> {
    SystemGucGuard::set(db, PASS_BUDGET_GUC, ms)
        .await?
        .ok_or_else(|| format!("{PASS_BUDGET_GUC} is not known to the server").into())
}

/// Sets a flag when dropped: stops a background task on every exit path of a test.
struct StopOnDrop(Arc<AtomicBool>);

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

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

    let budget = pass_budget(&db, "0").await?;
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
        if !leaf_exists(&db, &kept).await? {
            return Err(format!("{kept} must stay").into());
        }
        let left: i64 = db
            .query_one(
                &format!("SELECT count(*) FROM {kept} WHERE topic_id = $1"),
                &[&forever],
            )
            .await
            .map_err(boxed)?
            .get(0);
        if left != 20 {
            return Err(format!("the infinite-retention rows must stay: {left} of 20 left").into());
        }
        Ok(())
    }
    .await;
    let restored = budget.restore().await;
    // The kept leaf holds only this test's topics: drop it as if both were expired.
    db.query(
        "SELECT dropped FROM kafka.drop_message_partition($1::text::regclass, ARRAY[$2::int, $3::int])",
        &[&kept, &expired, &forever],
    )
    .await
    .map_err(boxed)?;
    result?;
    restored?;
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
/// sees no ack slower than 5 s; once the reader ends, the next sweep drops all eight.
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
    let stop_producer = StopOnDrop(stop.clone());
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

    let t = Instant::now();
    let swept = dropped_partitions(&db).await;
    let took = t.elapsed();
    drop(stop_producer);
    let slowest = producing.await.map_err(boxed)?;
    reader.batch_execute("ROLLBACK").await.map_err(boxed)?;
    let dropped = swept?;
    println!("Step 1: sweep behind the reader took {took:?}, dropped {dropped}");
    assert!(
        took < BLOCKED_PASS_LIMIT,
        "a sweep with blocked drops took {took:?}"
    );
    assert_eq!(
        dropped, 0,
        "no leaf can be dropped while the reader holds its lock"
    );
    let slowest = slowest?;
    println!("Step 2: slowest produce ack during the sweep {slowest:?}");
    assert!(
        slowest < PRODUCE_ACK_CEILING,
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

/// The aborted-row cleanup rotates over the leaves too. Each leaf it visits costs at
/// least one probe of its partial index, so a pass whose budget runs out (many retained
/// leaves, or earlier steps that spent it) stops part way; starting from the oldest
/// leaf (DEFAULT first) every pass, the aborted rows of newer leaves were never
/// deleted, and with infinite retention nothing else removes them.
///
/// Asserts: with the pass budget at 0 and aborted rows in a past leaf that is kept
/// (it also holds a committed row of an infinite-retention topic), repeated sweeps
/// delete every aborted row within two rotations over the leaves and keep the
/// committed row.
pub async fn test_sweep_aborted_cleanup_reaches_every_leaf() -> TestResult {
    into_test_result(aborted_rotation().await)
}

async fn aborted_rotation() -> SendResult<()> {
    println!("=== Test: the aborted-row cleanup rotates over every leaf on a spent budget ===\n");
    let db = session().await?;
    db.batch_execute("SET statement_timeout = '30s'")
        .await
        .map_err(boxed)?;
    let forever = create_topic(&db, &unique("abort-forever"), -1).await?;
    let hour: i32 = db
        .query_one("SELECT (random() * 8000)::int", &[])
        .await
        .map_err(boxed)?
        .get(0);
    let leaf = hour_leaf(&db, hour).await?;
    fill_leaf(&db, forever, hour, 0, 1).await?;
    db.execute(
        "INSERT INTO kafka.messages (topic_id, partition_id, partition_offset, value,
                                     created_at, txn_state)
         SELECT $1, 0, 1 + g, 'v'::bytea,
                timestamptz '2000-01-01 00:00+00' + $2::int * INTERVAL '1 hour'
                    + (1 + g) * INTERVAL '1 second',
                'aborted'
         FROM generate_series(0::bigint, 19) AS g",
        &[&forever, &hour],
    )
    .await
    .map_err(boxed)?;
    println!("Step 0: {leaf} holds 1 committed and 20 aborted rows");

    let budget = pass_budget(&db, "0").await?;
    let result: SendResult<()> = async {
        let leaves: i64 = db
            .query_one("SELECT count(*) FROM kafka.message_partitions()", &[])
            .await
            .map_err(boxed)?
            .get(0);
        let passes = 2 * leaves + 2;
        let count = |state: &'static str| {
            let db = &db;
            let leaf = leaf.clone();
            async move {
                let sql = format!(
                    "SELECT count(*) FROM {leaf} WHERE topic_id = $1 AND txn_state IS NOT DISTINCT FROM {state}"
                );
                db.query_one(&sql, &[&forever])
                    .await
                    .map(|r| r.get::<_, i64>(0))
                    .map_err(boxed)
            }
        };
        let mut cleaned_after = None;
        for pass in 1..=passes {
            db.query("SELECT * FROM pg_kafka_run_retention_sweep(0, 3600)", &[])
                .await
                .map_err(boxed)?;
            if count("'aborted'").await? == 0 {
                cleaned_after = Some(pass);
                break;
            }
        }
        let Some(pass) = cleaned_after else {
            return Err(format!(
                "aborted rows still in {leaf} after {passes} sweeps over {leaves} leaves"
            )
            .into());
        };
        println!("Step 1: aborted rows of {leaf} deleted after {pass} sweep(s) ({leaves} leaves)");
        let committed = count("NULL").await?;
        if committed != 1 {
            return Err(format!("the committed row must stay: {committed} left").into());
        }
        Ok(())
    }
    .await;
    let restored = budget.restore().await;
    db.query(
        "SELECT dropped FROM kafka.drop_message_partition($1::text::regclass, ARRAY[$2::int])",
        &[&leaf, &forever],
    )
    .await
    .map_err(boxed)?;
    result?;
    restored?;
    println!("✅ the aborted-row cleanup reached every leaf");
    Ok(())
}

/// Rows of `topic_id` whose created_at is `age` before now, in the leaf that holds now
/// (not the DEFAULT leaf); `first_offset` onwards.
async fn fill_current_leaf(
    db: &Client,
    topic_id: i32,
    first_offset: i64,
    rows: i64,
    age: &str,
    txn_state: Option<&str>,
) -> SendResult<()> {
    db.execute(
        "INSERT INTO kafka.messages (topic_id, partition_id, partition_offset, value,
                                     created_at, txn_state)
         SELECT $1, 0, $2::bigint + g, 'v'::bytea, now() - $4::text::interval, $5::text
         FROM generate_series(0::bigint, $3::bigint - 1) AS g",
        &[&topic_id, &first_offset, &rows, &age, &txn_state],
    )
    .await
    .map_err(boxed)?;
    let in_default: bool = db
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM kafka.messages_default WHERE topic_id = $1)",
            &[&topic_id],
        )
        .await
        .map_err(boxed)?
        .get(0);
    if in_default {
        return Err("the rows landed in the DEFAULT leaf: no leaf holds now()".into());
    }
    Ok(())
}

async fn topic_rows(db: &Client, topic_id: i32) -> SendResult<i64> {
    Ok(db
        .query_one(
            "SELECT count(*) FROM kafka.messages WHERE topic_id = $1",
            &[&topic_id],
        )
        .await
        .map_err(boxed)?
        .get(0))
}

/// Row expiry keeps a cursor over its (topic, leaf) work items, so a pass with no budget
/// left for it still moves on. Each topic's items begin with the DEFAULT leaf, which is
/// always in scope; when every pass started from the first item, a pass whose budget an
/// earlier step had spent ran only that DEFAULT delete (usually of nothing), and a
/// short-window topic's rows in the current leaf were never expired.
///
/// Asserts: with the pass budget at 0 and committed rows of two 1 s retention topics in
/// the current leaf (so the first topic's items come before the second's), repeated
/// sweeps expire both within one rotation over the work items (topics with a finite
/// window times leaves, plus two).
pub async fn test_sweep_expiry_reaches_the_current_leaf_on_a_spent_budget() -> TestResult {
    into_test_result(expiry_rotation().await)
}

async fn expiry_rotation() -> SendResult<()> {
    println!("=== Test: row expiry reaches the current leaf on a spent budget ===\n");
    let db = session().await?;
    db.batch_execute("SET statement_timeout = '30s'")
        .await
        .map_err(boxed)?;
    let first = create_topic(&db, &unique("expiry-rot-a"), 1000).await?;
    let topic = create_topic(&db, &unique("expiry-rot-b"), 1000).await?;
    fill_current_leaf(&db, first, 0, 20, "5 seconds", None).await?;
    fill_current_leaf(&db, topic, 0, 20, "5 seconds", None).await?;
    println!("Step 0: 20 rows each of two 1 s retention topics in the current leaf");

    let budget = pass_budget(&db, "0").await?;
    let result: SendResult<()> = async {
        let items: i64 = db
            .query_one(
                "SELECT (SELECT count(*) FROM kafka.topics WHERE retention_ms >= 0)
                        * (SELECT count(*) FROM kafka.message_partitions())",
                &[],
            )
            .await
            .map_err(boxed)?
            .get(0);
        let passes = items + 2;
        for pass in 1..=passes {
            db.query("SELECT * FROM pg_kafka_run_retention_sweep(0, 3600)", &[])
                .await
                .map_err(boxed)?;
            if topic_rows(&db, first).await? + topic_rows(&db, topic).await? == 0 {
                println!("Step 1: expired after {pass} sweep(s) ({items} work items)");
                return Ok(());
            }
        }
        Err(format!(
            "{} + {} rows still there after {passes} sweeps over {items} work items",
            topic_rows(&db, first).await?,
            topic_rows(&db, topic).await?
        )
        .into())
    }
    .await;
    let restored = budget.restore().await;
    db.execute(
        "DELETE FROM kafka.topics WHERE id = ANY($1)",
        &[&vec![first, topic]],
    )
    .await
    .map_err(boxed)?;
    result?;
    restored?;
    println!("✅ the current leaf's expired rows were reached");
    Ok(())
}

/// Aborted rows a pass deletes before the expiry: 80 full batches, more than the whole
/// pass budget clears.
const ABORTED_BACKLOG: i64 = 800_000;

/// A backlog in one step cannot take the whole pass: the aborted-row cleanup (and the
/// contents probes) get at most a third of the budget each, so row expiry, which runs
/// after them, keeps a share. Without the cap a client that aborts enough transactions
/// left row expiry its one mandatory statement per pass.
///
/// Asserts: with 800,000 aborted rows due (grace 0) and committed rows of a 1 s
/// retention topic in the current leaf, the first sweep at the default budget expires
/// them.
pub async fn test_sweep_expiry_keeps_its_share_behind_an_aborted_backlog() -> TestResult {
    into_test_result(expiry_share().await)
}

async fn expiry_share() -> SendResult<()> {
    println!("=== Test: row expiry keeps its share of a pass behind an aborted backlog ===\n");
    let db = session().await?;
    db.batch_execute("SET statement_timeout = '120s'")
        .await
        .map_err(boxed)?;
    let aborted = create_topic(&db, &unique("share-aborted"), -1).await?;
    let short = create_topic(&db, &unique("share-short"), 1000).await?;
    let result: SendResult<()> = async {
        // 10 s old: past this sweep's grace (0), within the worker's (60 s).
        fill_current_leaf(
            &db,
            aborted,
            0,
            ABORTED_BACKLOG,
            "10 seconds",
            Some("aborted"),
        )
        .await?;
        fill_current_leaf(&db, short, 0, 20, "5 seconds", None).await?;
        println!("Step 0: {ABORTED_BACKLOG} aborted rows due, 20 expired rows of a 1 s topic");
        let t = Instant::now();
        let rows = db
            .query(
                "SELECT category, deleted FROM pg_kafka_run_retention_sweep(0, 0)",
                &[],
            )
            .await
            .map_err(boxed)?;
        let took = t.elapsed();
        let stat = |name: &str| {
            rows.iter()
                .find(|r| r.get::<_, String>(0) == name)
                .map_or(0, |r| r.get::<_, i64>(1))
        };
        let left = topic_rows(&db, short).await?;
        println!(
            "Step 1: one sweep in {took:?}: {} aborted, {} expired; {left} of the 20 left",
            stat("aborted_messages"),
            stat("expired_messages")
        );
        if stat("aborted_messages") >= ABORTED_BACKLOG {
            return Err("the pass cleared the whole backlog: it does not test the cap".into());
        }
        if left != 0 {
            return Err(format!("{left} expired rows left after the first sweep").into());
        }
        Ok(())
    }
    .await;
    db.execute(
        "DELETE FROM kafka.topics WHERE id = ANY($1)",
        &[&vec![aborted, short]],
    )
    .await
    .map_err(boxed)?;
    result?;
    println!("✅ row expiry ran behind the aborted backlog");
    Ok(())
}

/// Idle producer ids with a terminal transaction each, seeded for the prune test.
const EXPIRING_PRODUCERS: i64 = 100_000;
/// What one sweep with that backlog may take: the 1.5 s budget, one statement per step
/// and slack.
const PRUNE_PASS_LIMIT: Duration = Duration::from_secs(5);

/// The terminal-transaction and stale-producer prunes are bounded steps: index-driven
/// (every deleted producer id checks the transactions that reference it through an
/// index on transactions.producer_id), in batches within the pass budget, carrying
/// their progress to the next pass. Without the index each deleted id read the whole
/// kafka.transactions table for its foreign key, so the prune of N ids cost N full
/// scans in one statement (40,000 ids: 18.6 s on the DB thread).
///
/// Asserts: with 100,000 producer ids idle for 8 days, each with a CompleteCommit
/// transaction as old, one sweep returns within 5 s (statement_timeout 30 s), and
/// repeated sweeps remove every seeded id and transaction.
pub async fn test_sweep_prunes_producers_within_its_budget() -> TestResult {
    into_test_result(producer_prune().await)
}

async fn producer_prune() -> SendResult<()> {
    println!("=== Test: the producer prune stays within the pass budget ===\n");
    let db = session().await?;
    db.batch_execute("SET statement_timeout = '30s'")
        .await
        .map_err(boxed)?;
    let prefix = unique("prune");
    db.execute(
        "WITH p AS (
             INSERT INTO kafka.producer_ids (producer_id, transactional_id, created_at,
                                             last_active_at)
             SELECT base.id - g, $1 || '-' || g, now() - INTERVAL '8 days',
                    now() - INTERVAL '8 days'
             FROM generate_series(1, $2::bigint) AS g,
                  -- Explicit negative ids, never issued by the sequence: drawing 100,000
                  -- values from it would move the ids later tests are handed far past the
                  -- creation-time range the idempotent tests check them against.
                  (SELECT -(floor(random() * 1000000)::bigint + 1) * 1000000 AS id) AS base
             RETURNING producer_id, transactional_id)
         INSERT INTO kafka.transactions (transactional_id, producer_id, producer_epoch, state,
                                         last_updated_at)
         SELECT transactional_id, producer_id, 0, 'CompleteCommit', now() - INTERVAL '8 days'
         FROM p",
        &[&prefix, &EXPIRING_PRODUCERS],
    )
    .await
    .map_err(boxed)?;
    println!(
        "Step 0: {EXPIRING_PRODUCERS} producer ids idle 8 days, each with a terminal transaction"
    );
    let left = || {
        let db = &db;
        let prefix = &prefix;
        async move {
            db.query_one(
            "SELECT (SELECT count(*) FROM kafka.producer_ids WHERE transactional_id LIKE $1 || '-%'),
                    (SELECT count(*) FROM kafka.transactions WHERE transactional_id LIKE $1 || '-%')",
            &[prefix],
        )
        .await
        .map(|r| (r.get::<_, i64>(0), r.get::<_, i64>(1)))
        .map_err(boxed)
        }
    };

    let t = Instant::now();
    db.query("SELECT * FROM pg_kafka_run_retention_sweep(0, 3600)", &[])
        .await
        .map_err(boxed)?;
    let took = t.elapsed();
    println!(
        "Step 1: first sweep took {took:?}, left {:?}",
        left().await?
    );
    if took >= PRUNE_PASS_LIMIT {
        return Err(format!("a sweep with the prune backlog took {took:?}").into());
    }
    for pass in 2..=60 {
        if left().await? == (0, 0) {
            println!(
                "Step 2: every seeded id and transaction gone after {} sweep(s)",
                pass - 1
            );
            println!("✅ the prune stayed bounded and drained");
            return Ok(());
        }
        let t = Instant::now();
        db.query("SELECT * FROM pg_kafka_run_retention_sweep(0, 3600)", &[])
            .await
            .map_err(boxed)?;
        let took = t.elapsed();
        if took >= PRUNE_PASS_LIMIT {
            return Err(format!("sweep {pass} took {took:?}").into());
        }
    }
    Err(format!("seeded rows left after 60 sweeps: {:?}", left().await?).into())
}

/// The server's pass budget setting, and its `postgresql.auto.conf` entry if any.
/// Read on a new session, which starts with the reloaded configuration.
async fn budget_now() -> SendResult<(String, Option<String>)> {
    let check = session().await?;
    let setting: String = check
        .query_one(
            "SELECT setting FROM pg_settings WHERE name = $1",
            &[&PASS_BUDGET_GUC],
        )
        .await
        .map_err(boxed)?
        .get(0);
    let entry: Option<String> = check
        .query_opt(
            "SELECT setting FROM pg_file_settings
             WHERE name = $1 AND sourcefile LIKE '%postgresql.auto.conf'
             ORDER BY seqno DESC LIMIT 1",
            &[&PASS_BUDGET_GUC],
        )
        .await
        .map_err(boxed)?
        .map(|r| r.get(0));
    Ok((setting, entry))
}

/// The tests above rely on `SystemGucGuard` putting `ALTER SYSTEM` settings back: a
/// pass budget left at 0 would change every later sweep of the run.
///
/// Asserts: a guard restored explicitly, and one dropped by a panicking task, leave the
/// pass budget at its default with no `postgresql.auto.conf` entry; a guard over an
/// earlier `ALTER SYSTEM` value (7) restores that value rather than resetting it.
pub async fn test_system_guc_guard_restores_on_every_path() -> TestResult {
    into_test_result(guard_paths().await)
}

async fn guard_paths() -> SendResult<()> {
    println!("=== Test: SystemGucGuard restores the setting on every path ===\n");
    let db = session().await?;
    let (default, entry) = budget_now().await?;
    if entry.is_some() {
        return Err(
            format!("{PASS_BUDGET_GUC} already has an ALTER SYSTEM entry {entry:?}").into(),
        );
    }

    pass_budget(&db, "0").await?.restore().await?;
    let after = budget_now().await?;
    println!("Step 1: after restore(): {after:?}");
    if after != (default.clone(), None) {
        return Err(format!("restore() left {after:?}, expected ({default:?}, None)").into());
    }

    let guard = pass_budget(&db, "0").await?;
    let panicked = tokio::spawn(async move {
        let _guard = guard;
        panic!("a failed assert with the guard held");
    })
    .await;
    if panicked.is_ok() {
        return Err("the task was expected to panic".into());
    }
    let after = budget_now().await?;
    println!("Step 2: after a panic with the guard held: {after:?}");
    if after != (default.clone(), None) {
        return Err(format!("the drop left {after:?}, expected ({default:?}, None)").into());
    }

    let outer = pass_budget(&db, "7").await?;
    pass_budget(&db, "0").await?.restore().await?;
    let after = budget_now().await?;
    println!("Step 3: a guard over an earlier value 7: {after:?}");
    outer.restore().await?;
    if after != ("7".to_string(), Some("7".to_string())) {
        return Err(format!("the inner guard left {after:?}, expected 7").into());
    }
    let after = budget_now().await?;
    if after != (default.clone(), None) {
        return Err(format!("the outer guard left {after:?}, expected ({default:?}, None)").into());
    }
    println!("✅ every path restored the setting");
    Ok(())
}
