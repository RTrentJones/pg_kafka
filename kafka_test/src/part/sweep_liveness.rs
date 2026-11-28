//! What an on-demand retention sweep and partition DDL make other sessions (the
//! worker's DB thread above all) wait for, and that an on-demand pass does the drops
//! its own probes find.
//!
//! All sequential: they create, fill and drop leaves of kafka.messages and run the
//! retention sweep.

use super::drain_droppable_leaves;
use super::sweep_budget::{create_topic, fill_leaf, hour_leaf};
use crate::common::TestResult;
use crate::worker::{boxed, into_test_result, session, unique, SendResult};
use std::time::{Duration, Instant};
use tokio_postgres::Client;

/// A random base hour in 2000 (leaves no other test uses at the same time).
async fn random_hour(db: &Client) -> SendResult<i32> {
    Ok(db
        .query_one("SELECT (random() * 8000)::int", &[])
        .await
        .map_err(boxed)?
        .get(0))
}

/// `rows` rows of `topic_id` spread over the leaf of `hour` (3 ms apart, so up to
/// 1 200 000 fit), from `first_offset`; `aborted` marks them as aborted transactional rows.
async fn fill_leaf_dense(
    db: &Client,
    topic_id: i32,
    hour: i32,
    first_offset: i64,
    rows: i64,
    aborted: bool,
) -> SendResult<()> {
    db.execute(
        "INSERT INTO kafka.messages (topic_id, partition_id, partition_offset, value, created_at,
                                     producer_id, producer_epoch, txn_state)
         SELECT $1, 0, $2::bigint + g, 'v'::bytea,
                timestamptz '2000-01-01 00:00+00' + $3::int * INTERVAL '1 hour'
                    + g * INTERVAL '3 milliseconds',
                CASE WHEN $5 THEN 1 END, CASE WHEN $5 THEN 0::int2 END,
                CASE WHEN $5 THEN 'aborted' END
         FROM generate_series(0::bigint, $4::bigint - 1) AS g",
        &[&topic_id, &first_offset, &hour, &rows, &aborted],
    )
    .await
    .map_err(boxed)?;
    Ok(())
}

/// `category` of a `pg_kafka_run_retention_sweep()` result.
fn category(rows: &[tokio_postgres::Row], name: &str) -> i64 {
    rows.iter()
        .find(|r| r.get::<_, String>(0) == name)
        .map_or(0, |r| r.get::<_, i64>(1))
}

async fn leaf_exists(db: &Client, leaf: &str) -> SendResult<bool> {
    Ok(db
        .query_one("SELECT to_regclass($1) IS NOT NULL", &[&leaf])
        .await
        .map_err(boxed)?
        .get(0))
}

/// Drop `leaf` whatever it holds (cleanup).
async fn force_drop(db: &Client, leaf: &str) -> SendResult<()> {
    if leaf_exists(db, leaf).await? {
        db.query(
            "SELECT dropped FROM kafka.drop_message_partition($1::text::regclass,
                 ARRAY(SELECT id FROM kafka.topics))",
            &[&leaf],
        )
        .await
        .map_err(boxed)?;
    }
    Ok(())
}

async fn delete_topics(db: &Client, ids: &[i32]) -> SendResult<()> {
    db.execute("DELETE FROM kafka.topics WHERE id = ANY($1)", &[&ids])
        .await
        .map_err(boxed)?;
    Ok(())
}

/// This session's backend pid.
async fn backend_pid(db: &Client) -> SendResult<i32> {
    Ok(db
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .map_err(boxed)?
        .get(0))
}

/// Whether `pid` holds ACCESS EXCLUSIVE on kafka.messages (a pass that dropped a leaf
/// holds it until it commits).
async fn holds_messages_exclusively(db: &Client, pid: i32) -> SendResult<bool> {
    Ok(db
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_locks
                            WHERE pid = $1 AND granted AND mode = 'AccessExclusiveLock'
                              AND locktype = 'relation'
                              AND relation = 'kafka.messages'::regclass)",
            &[&pid],
        )
        .await
        .map_err(boxed)?
        .get(0))
}

/// Whether `waiter` waits for a lock `holder` holds.
async fn waits_for(db: &Client, waiter: i32, holder: i32) -> SendResult<bool> {
    Ok(db
        .query_one(
            "SELECT $2 = ANY (pg_blocking_pids($1))",
            &[&waiter, &holder],
        )
        .await
        .map_err(boxed)?
        .get(0))
}

/// Poll every 2 ms, for at most `within`, until `sweeper` holds ACCESS EXCLUSIVE on
/// kafka.messages (its pass dropped a leaf and has not committed); false if it never did
/// while `sweep` ran.
async fn wait_for_drop_lock(
    db: &Client,
    sweeper: i32,
    sweep: &tokio::task::JoinHandle<SendResult<(Vec<tokio_postgres::Row>, Duration)>>,
    within: Duration,
) -> SendResult<bool> {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline && !sweep.is_finished() {
        if holds_messages_exclusively(db, sweeper).await? {
            return Ok(true);
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    Ok(false)
}

/// One `pg_kafka_run_retention_sweep(0, 0)` from `sweeper`, in a task: its rows and how
/// long it took.
fn spawn_sweep(
    sweeper: Client,
) -> tokio::task::JoinHandle<SendResult<(Vec<tokio_postgres::Row>, Duration)>> {
    tokio::spawn(async move {
        let t = Instant::now();
        let rows = sweeper
            .query("SELECT * FROM pg_kafka_run_retention_sweep(0, 0)", &[])
            .await
            .map_err(boxed)?;
        SendResult::Ok((rows, t.elapsed()))
    })
}

/// An INSERT into kafka.messages from a session of its own, in a task, and whether
/// (polled every 2 ms) it was seen waiting for `holder`: how long it took, and that.
async fn timed_insert(
    db: &Client,
    holder: i32,
    topic_id: i32,
    offset: i64,
) -> SendResult<(Duration, bool)> {
    let writer = session().await?;
    let writer_pid = backend_pid(&writer).await?;
    let t = Instant::now();
    let insert = tokio::spawn(async move {
        writer
            .execute(
                "INSERT INTO kafka.messages (topic_id, partition_id, partition_offset, value)
                 VALUES ($1, 0, $2, 'w')",
                &[&topic_id, &offset],
            )
            .await
            .map_err(boxed)?;
        SendResult::Ok(t.elapsed())
    });
    let mut waited = false;
    while !insert.is_finished() {
        if !waited && waits_for(db, writer_pid, holder).await? {
            waited = true;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    let took = insert.await.map_err(boxed)??;
    Ok((took, waited))
}

/// Aborted rows that keep an on-demand pass busy for its whole budget after its drop.
const BUSY_ABORTED_ROWS: i64 = 1_000_000;
/// The longest a write through kafka.messages may wait for an on-demand pass that
/// dropped a leaf: the drop itself and one more turn (a 10 000-row delete), not the
/// rest of the 1.5 s pass.
const WRITER_WAIT_CEILING: Duration = Duration::from_millis(700);

/// A drop takes ACCESS EXCLUSIVE on kafka.messages, held until the pass commits. An
/// on-demand pass (`pg_kafka_run_retention_sweep()`, its own session) that dropped a
/// leaf first and then had other work kept that lock for the rest of its budget, and
/// every write through kafka.messages (the worker's Produce, so every client of the DB
/// thread) waited for it.
///
/// Asserts: with an expired leaf to drop and 1 000 000 aborted rows to delete in another,
/// a session's INSERT into kafka.messages, issued once the on-demand pass holds its
/// drop's lock and seen waiting for it, finishes within 700 ms (the pass ends at its next
/// turn once someone has waited 100 ms), the pass dropped the leaf, and a later pass
/// deletes the rest of the aborted rows. A run in which the INSERT never waited for the
/// pass fails (it would prove nothing).
pub async fn test_sweep_pass_ends_when_a_writer_waits_for_its_drop() -> TestResult {
    into_test_result(writer_waits_for_drop().await)
}

async fn writer_waits_for_drop() -> SendResult<()> {
    println!("=== Test: an on-demand pass that dropped a leaf ends when a writer waits ===\n");
    let db = session().await?;
    db.batch_execute("SET statement_timeout = '120s'")
        .await
        .map_err(boxed)?;
    drain_droppable_leaves(&db).await?;
    let expired = create_topic(&db, &unique("drop-wait-expired"), 1000).await?;
    let forever = create_topic(&db, &unique("drop-wait-forever"), -1).await?;
    let hour = random_hour(&db).await?;
    let droppable = hour_leaf(&db, hour).await?;
    let busy = hour_leaf(&db, hour + 1).await?;
    let outcome = async {
        fill_leaf(&db, expired, hour, 0, 10).await?;
        fill_leaf_dense(&db, forever, hour + 1, 0, 1, false).await?;
        fill_leaf_dense(&db, forever, hour + 1, 1, BUSY_ABORTED_ROWS, true).await?;
        db.batch_execute("ANALYZE kafka.messages")
            .await
            .map_err(boxed)?;
        println!("Step 1: {droppable} to drop, {BUSY_ABORTED_ROWS} aborted rows in {busy}");

        let sweeper = session().await?;
        let sweeper_pid = backend_pid(&sweeper).await?;
        let sweep = spawn_sweep(sweeper);
        if !wait_for_drop_lock(&db, sweeper_pid, &sweep, Duration::from_secs(30)).await? {
            return Err(
                "the pass never held the lock of a drop: nothing for a writer to wait \
                        for, so the test proves nothing"
                    .into(),
            );
        }
        let (waited, blocked) =
            timed_insert(&db, sweeper_pid, forever, BUSY_ABORTED_ROWS + 10).await?;
        let (rows, took) = sweep.await.map_err(boxed)??;
        let dropped = category(&rows, "dropped_partitions");
        let aborted = category(&rows, "aborted_messages");
        println!(
            "Step 2: issued while the pass held its drop's lock, the INSERT waited {waited:?} \
             (seen waiting for the pass: {blocked}); the pass took {took:?}, dropped \
             {dropped} leaf(s) and deleted {aborted} aborted row(s)"
        );
        if !blocked {
            return Err(
                "the INSERT was never seen waiting for the pass that held its drop's \
                        lock: the test proves nothing"
                    .into(),
            );
        }
        if leaf_exists(&db, &droppable).await? {
            return Err(format!("the pass did not drop {droppable}").into());
        }
        if waited >= WRITER_WAIT_CEILING {
            return Err(format!(
                "an INSERT into kafka.messages waited {waited:?} for an on-demand pass \
                 holding the lock of its drop"
            )
            .into());
        }
        let mut left = BUSY_ABORTED_ROWS;
        for _ in 0..60 {
            db.query("SELECT * FROM pg_kafka_run_retention_sweep(0, 0)", &[])
                .await
                .map_err(boxed)?;
            left = db
                .query_one(
                    "SELECT count(*) FROM kafka.messages
                     WHERE topic_id = $1 AND txn_state = 'aborted'",
                    &[&forever],
                )
                .await
                .map_err(boxed)?
                .get(0);
            if left == 0 {
                break;
            }
        }
        if left != 0 {
            return Err(format!("{left} aborted rows left after the later passes").into());
        }
        println!("Step 3: later passes deleted the remaining aborted rows");
        SendResult::Ok(())
    }
    .await;
    force_drop(&db, &droppable).await?;
    force_drop(&db, &busy).await?;
    delete_topics(&db, &[expired, forever]).await?;
    outcome?;
    println!("✅ the writer waited for one turn, not for the rest of the pass");
    Ok(())
}

/// The longest leaf DDL may take beside a lock on a leaf it does not touch.
const UNRELATED_LOCK_CEILING: Duration = Duration::from_millis(1000);

/// Partition DDL (`kafka.create_message_partition`, `kafka.drop_message_partition`)
/// locks kafka.messages ACCESS EXCLUSIVE first (`kafka.lock_message_tables`). Without
/// ONLY, LOCK TABLE takes that lock on every leaf too, so a SHARE UPDATE EXCLUSIVE
/// holder on any leaf (autovacuum, VACUUM, ANALYZE, here a session standing in for
/// one) made every leaf creation and every drop wait out its 2 s lock timeout and give
/// up: leaves were not made ahead (rows spilled into the DEFAULT leaf) and retention
/// dropped nothing while any leaf was being vacuumed.
///
/// Asserts: while a session holds SHARE UPDATE EXCLUSIVE on an unrelated leaf, a new
/// leaf is created and an empty leaf dropped, each within 1 s; the locks the DDL does
/// need still count: with that lock on the leaf to drop, the drop gives up
/// (lock_timed_out), and with it on kafka.messages_default, a leaf creation gives up.
pub async fn test_partition_ddl_ignores_locks_on_other_leaves() -> TestResult {
    into_test_result(ddl_beside_vacuum().await)
}

async fn create_leaf(db: &Client, hour: i32) -> SendResult<i32> {
    Ok(db
        .query_one(
            "SELECT kafka.create_message_partition(lo, lo + INTERVAL '1 hour')
             FROM (SELECT timestamptz '2000-01-01 00:00+00' + $1::int * INTERVAL '1 hour') AS b(lo)",
            &[&hour],
        )
        .await
        .map_err(boxed)?
        .get(0))
}

async fn leaf_of_hour(db: &Client, hour: i32) -> SendResult<Option<String>> {
    Ok(db
        .query_opt(
            "SELECT partition_oid::regclass::text FROM kafka.message_partitions()
             WHERE lower_bound = timestamptz '2000-01-01 00:00+00' + $1::int * INTERVAL '1 hour'",
            &[&hour],
        )
        .await
        .map_err(boxed)?
        .map(|r| r.get(0)))
}

/// (dropped, lock_timed_out) of a drop of an empty `leaf`.
async fn drop_empty(db: &Client, leaf: &str) -> SendResult<(bool, bool)> {
    let row = db
        .query_one(
            "SELECT dropped, lock_timed_out
             FROM kafka.drop_message_partition($1::text::regclass, ARRAY[]::int4[])",
            &[&leaf],
        )
        .await
        .map_err(boxed)?;
    Ok((row.get(0), row.get(1)))
}

async fn ddl_beside_vacuum() -> SendResult<()> {
    println!("=== Test: partition DDL does not wait for locks on other leaves ===\n");
    let db = session().await?;
    db.batch_execute("SET statement_timeout = '60s'")
        .await
        .map_err(boxed)?;
    let hour = random_hour(&db).await?;
    let vacuumed = hour_leaf(&db, hour).await?;
    let to_drop = hour_leaf(&db, hour + 1).await?;
    let guarded = hour_leaf(&db, hour + 2).await?;
    let holder = session().await?;
    let outcome = async {
        holder
            .batch_execute(&format!(
                "BEGIN; LOCK TABLE {vacuumed} IN SHARE UPDATE EXCLUSIVE MODE"
            ))
            .await
            .map_err(boxed)?;
        println!("Step 1: a session holds SHARE UPDATE EXCLUSIVE on {vacuumed}");

        let t = Instant::now();
        let created = create_leaf(&db, hour + 3).await?;
        let took = t.elapsed();
        println!("Step 2: leaf creation returned {created} in {took:?}");
        if created != 1 || took >= UNRELATED_LOCK_CEILING {
            return Err(format!(
                "creating a leaf beside a lock on another leaf returned {created} after {took:?}"
            )
            .into());
        }
        let t = Instant::now();
        let (dropped, timed_out) = drop_empty(&db, &to_drop).await?;
        let took = t.elapsed();
        println!("Step 3: drop of {to_drop}: dropped={dropped} lock_timed_out={timed_out} in {took:?}");
        if !dropped || took >= UNRELATED_LOCK_CEILING {
            return Err(format!(
                "dropping a leaf beside a lock on another leaf: dropped={dropped}, \
                 lock_timed_out={timed_out}, after {took:?}"
            )
            .into());
        }
        holder.batch_execute("ROLLBACK").await.map_err(boxed)?;

        holder
            .batch_execute(&format!(
                "BEGIN; LOCK TABLE {guarded} IN SHARE UPDATE EXCLUSIVE MODE"
            ))
            .await
            .map_err(boxed)?;
        let (dropped, timed_out) = drop_empty(&db, &guarded).await?;
        holder.batch_execute("ROLLBACK").await.map_err(boxed)?;
        println!("Step 4: with the lock on the leaf itself: dropped={dropped} lock_timed_out={timed_out}");
        if dropped || !timed_out {
            return Err("a drop must wait for (and give up on) a lock on its own leaf".into());
        }
        holder
            .batch_execute("BEGIN; LOCK TABLE kafka.messages_default IN SHARE UPDATE EXCLUSIVE MODE")
            .await
            .map_err(boxed)?;
        let created = create_leaf(&db, hour + 4).await?;
        holder.batch_execute("ROLLBACK").await.map_err(boxed)?;
        println!("Step 5: with the lock on kafka.messages_default: created={created}");
        if created != 0 {
            return Err("a leaf creation must wait for a lock on the DEFAULT leaf".into());
        }
        SendResult::Ok(())
    }
    .await;
    holder.batch_execute("ROLLBACK").await.ok();
    for h in hour..hour + 5 {
        if let Some(leaf) = leaf_of_hour(&db, h).await? {
            force_drop(&db, &leaf).await?;
        }
    }
    outcome?;
    println!("✅ the DDL locked only kafka.messages and the leaves it touches");
    Ok(())
}

/// An on-demand pass runs in a session of its own, usually a fresh one, so nothing
/// it found survives it. A drop runs only while the pass holds no row, and the probes
/// after the pass's first one ran once the row deletes had started: the drops those
/// probes found were gated and forgotten with the session, so calls from fresh
/// sessions dropped only the leaf the first probe found.
///
/// Asserts: with an older leaf to trim (an expired topic beside an infinite-retention
/// one) and a newer, wholly expired leaf, one `pg_kafka_run_retention_sweep()` call from
/// a fresh session trims the first and drops the second.
pub async fn test_fresh_session_sweep_drops_what_its_probes_find() -> TestResult {
    into_test_result(fresh_session_drops().await)
}

async fn fresh_session_drops() -> SendResult<()> {
    println!("=== Test: a fresh-session sweep drops the leaves its probes find ===\n");
    let db = session().await?;
    db.batch_execute("SET statement_timeout = '60s'")
        .await
        .map_err(boxed)?;
    drain_droppable_leaves(&db).await?;
    let expired = create_topic(&db, &unique("fresh-expired"), 1000).await?;
    let forever = create_topic(&db, &unique("fresh-forever"), -1).await?;
    let hour = random_hour(&db).await?;
    let trimmed = hour_leaf(&db, hour).await?;
    let droppable = hour_leaf(&db, hour + 1).await?;
    let outcome = async {
        fill_leaf(&db, expired, hour, 0, 5).await?;
        fill_leaf(&db, forever, hour, 0, 1).await?;
        fill_leaf(&db, expired, hour + 1, 5, 5).await?;
        println!("Step 1: {trimmed} to trim, {droppable} to drop");
        let fresh = session().await?;
        let rows = fresh
            .query("SELECT * FROM pg_kafka_run_retention_sweep(0, 3600)", &[])
            .await
            .map_err(boxed)?;
        println!(
            "Step 2: one call from a fresh session: {} expired row(s), {} leaf(s) dropped",
            category(&rows, "expired_messages"),
            category(&rows, "dropped_partitions")
        );
        let left: i64 = db
            .query_one(
                "SELECT count(*) FROM kafka.messages WHERE topic_id = $1",
                &[&expired],
            )
            .await
            .map_err(boxed)?
            .get(0);
        if leaf_exists(&db, &droppable).await? {
            return Err(format!(
                "a fresh-session pass did not drop {droppable}, which its probes found"
            )
            .into());
        }
        if left != 0 {
            return Err(format!("{left} expired rows left in {trimmed}").into());
        }
        SendResult::Ok(())
    }
    .await;
    force_drop(&db, &trimmed).await?;
    force_drop(&db, &droppable).await?;
    delete_topics(&db, &[expired, forever]).await?;
    outcome?;
    println!("✅ the pass dropped the leaf its own probe found");
    Ok(())
}

/// The longest a write through kafka.messages may wait for an on-demand pass whose second
/// drop finds its leaf held: one 50 ms attempt of that drop, the commit and slack; the
/// drop of a pass that had not deleted a row waited its 2 s lock_timeout.
const SECOND_DROP_WRITER_CEILING: Duration = Duration::from_millis(700);
/// The longest such a pass may take.
const SECOND_DROP_PASS_CEILING: Duration = Duration::from_millis(1000);

/// A drop holds ACCESS EXCLUSIVE on kafka.messages until the pass commits, and the next
/// drop of the pass locks its own leaf too. A pass that had dropped a leaf but deleted no
/// row ran that next drop like the first, with its 2 s lock_timeout: with a session
/// holding the second leaf (a reader that names the leaf, autovacuum, pg_dump), every
/// write and read through kafka.messages (the worker's Produce and Fetch, so every client
/// of the DB thread) waited those 2 s behind the pass.
///
/// Asserts: with two expired leaves and a session holding ACCESS SHARE on the second,
/// one on-demand pass drops the first, gives the second up within 1 s, and an INSERT into
/// kafka.messages issued while the pass holds the first drop's lock finishes within
/// 700 ms; once the holder ends, a sweep drops the second leaf.
pub async fn test_sweep_second_drop_waits_briefly_for_its_leaf() -> TestResult {
    into_test_result(second_drop_behind_leaf_holder().await)
}

/// Run `pg_kafka_run_retention_sweep(0, 0)` until a call removes nothing (at most 10
/// calls): the prunes and reapers then have no due row, so a test's own sweep right after
/// does only what the test set up.
async fn quiesce_sweeps(db: &Client) -> SendResult<()> {
    for _ in 0..10 {
        let removed: i64 = db
            .query("SELECT * FROM pg_kafka_run_retention_sweep(0, 0)", &[])
            .await
            .map_err(boxed)?
            .iter()
            .map(|r| r.get::<_, i64>(1))
            .sum();
        if removed == 0 {
            return Ok(());
        }
    }
    Err("sweeps kept removing rows after 10 calls".into())
}

async fn second_drop_behind_leaf_holder() -> SendResult<()> {
    println!("=== Test: a pass's second drop waits briefly for a held leaf ===\n");
    let db = session().await?;
    db.batch_execute("SET statement_timeout = '60s'")
        .await
        .map_err(boxed)?;
    drain_droppable_leaves(&db).await?;
    // Nothing else for the test's pass to delete: it holds no row when it reaches the drops.
    quiesce_sweeps(&db).await?;
    // A window longer than a leaf: its rows go with their leaves, never row by row.
    let expired = create_topic(&db, &unique("second-drop-expired"), 86_400_000).await?;
    let hour = random_hour(&db).await?;
    let first = hour_leaf(&db, hour).await?;
    let second = hour_leaf(&db, hour + 1).await?;
    let holder = session().await?;
    let outcome = async {
        fill_leaf(&db, expired, hour, 0, 5).await?;
        fill_leaf(&db, expired, hour + 1, 5, 5).await?;
        holder
            .batch_execute(&format!("BEGIN; SELECT count(*) FROM {second}"))
            .await
            .map_err(boxed)?;
        println!("Step 1: {first} and {second} to drop; a session holds ACCESS SHARE on {second}");

        let sweeper = session().await?;
        let sweeper_pid = backend_pid(&sweeper).await?;
        let sweep = spawn_sweep(sweeper);
        let locked = wait_for_drop_lock(&db, sweeper_pid, &sweep, Duration::from_secs(30)).await?;
        let writer = if locked {
            Some(timed_insert(&db, sweeper_pid, expired, 1_000_000).await?)
        } else {
            None
        };
        let (rows, took) = sweep.await.map_err(boxed)??;
        let removed: Vec<(String, i64)> = rows.iter().map(|r| (r.get(0), r.get(1))).collect();
        println!(
            "Step 2: the pass took {took:?} ({removed:?}); an INSERT issued while it held its \
             first drop's lock: {writer:?} (time, seen waiting)"
        );
        if leaf_exists(&db, &first).await? {
            return Err(format!("the pass did not drop {first}").into());
        }
        if !leaf_exists(&db, &second).await? {
            return Err(format!("the pass dropped {second} through a session's lock on it").into());
        }
        if took >= SECOND_DROP_PASS_CEILING {
            return Err(format!(
                "the pass took {took:?}: its second drop waited out the lock on {second}"
            )
            .into());
        }
        if let Some((waited, _)) = writer {
            if waited >= SECOND_DROP_WRITER_CEILING {
                return Err(format!(
                    "an INSERT into kafka.messages waited {waited:?} behind a pass whose \
                     second drop waited for a held leaf"
                )
                .into());
            }
        }
        holder.batch_execute("ROLLBACK").await.map_err(boxed)?;
        db.query("SELECT * FROM pg_kafka_run_retention_sweep(0, 3600)", &[])
            .await
            .map_err(boxed)?;
        if leaf_exists(&db, &second).await? {
            return Err(format!("{second} was not dropped once its holder ended").into());
        }
        println!("Step 3: with the holder gone, a sweep dropped {second}");
        SendResult::Ok(())
    }
    .await;
    holder.batch_execute("ROLLBACK").await.ok();
    db.execute(
        "DELETE FROM kafka.messages WHERE topic_id = $1 AND partition_offset = 1000000",
        &[&expired],
    )
    .await
    .map_err(boxed)?;
    force_drop(&db, &first).await?;
    force_drop(&db, &second).await?;
    delete_topics(&db, &[expired]).await?;
    outcome?;
    println!("✅ the second drop gave up its held leaf after one short attempt");
    Ok(())
}

/// A pass that has trimmed a partition holds its offset counter until it commits, and a
/// Produce to that partition waits for it while holding ROW EXCLUSIVE on kafka.messages:
/// no drop can take its lock then. The drops came after the row deletes, and a drop of a
/// pass that holds rows is refused while a session waits for one of them, so an on-demand
/// pass in a fresh session under steady Produce traffic to a partition it trimmed dropped
/// nothing its probes found, and the session took what they found with it.
///
/// Asserts: with an older leaf to trim (a topic with a 1 s window beside an
/// infinite-retention one) and a newer leaf of only that topic, and a producer writing to
/// the topic all along (its records expire too, row by row), one
/// `pg_kafka_run_retention_sweep()` call from a fresh session drops the newer leaf, and
/// calls from fresh sessions trim the older one.
pub async fn test_fresh_session_sweep_drops_beside_produce_to_a_trimmed_partition() -> TestResult {
    into_test_result(drops_beside_produce().await)
}

async fn drops_beside_produce() -> SendResult<()> {
    use rdkafka::producer::FutureRecord;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::Arc;
    println!("=== Test: a fresh-session sweep drops beside Produce to a trimmed partition ===\n");
    let db = session().await?;
    db.batch_execute("SET statement_timeout = '60s'")
        .await
        .map_err(boxed)?;
    drain_droppable_leaves(&db).await?;
    let topic = unique("produce-trim");
    let short = create_topic(&db, &topic, 1000).await?;
    let forever = create_topic(&db, &unique("produce-trim-forever"), -1).await?;
    let hour = random_hour(&db).await?;
    let trimmed = hour_leaf(&db, hour).await?;
    let droppable = hour_leaf(&db, hour + 1).await?;
    let stop = Arc::new(AtomicBool::new(false));
    let produced = Arc::new(AtomicU64::new(0));
    let producer = crate::worker::quick_producer("20000")?;
    let traffic = {
        let (stop, produced, topic) = (stop.clone(), produced.clone(), topic.clone());
        tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                producer
                    .send(
                        FutureRecord::<(), _>::to(&topic).payload("p").partition(0),
                        Duration::from_secs(20),
                    )
                    .await
                    .map_err(|(e, _)| boxed(e))?;
                produced.fetch_add(1, Ordering::Relaxed);
            }
            SendResult::Ok(())
        })
    };
    let outcome = async {
        fill_leaf(&db, short, hour, 0, 5).await?;
        fill_leaf(&db, forever, hour, 0, 1).await?;
        fill_leaf(&db, short, hour + 1, 5, 5).await?;
        // The producer's records are older than the 1 s window when the sweep runs.
        tokio::time::sleep(Duration::from_millis(1500)).await;
        let before = produced.load(Ordering::Relaxed);
        if before == 0 || traffic.is_finished() {
            return Err("the producer did not produce".into());
        }
        println!("Step 1: {trimmed} to trim, {droppable} to drop; {before} records produced");

        let fresh = session().await?;
        let rows = fresh
            .query("SELECT * FROM pg_kafka_run_retention_sweep(0, 3600)", &[])
            .await
            .map_err(boxed)?;
        let during = produced.load(Ordering::Relaxed) - before;
        println!(
            "Step 2: one call from a fresh session: {} expired row(s), {} leaf(s) dropped; \
             {during} records produced meanwhile",
            category(&rows, "expired_messages"),
            category(&rows, "dropped_partitions")
        );
        if leaf_exists(&db, &droppable).await? {
            return Err(format!(
                "a fresh-session pass under Produce traffic to a trimmed partition did not \
                 drop {droppable}, which its probes found"
            )
            .into());
        }
        let mut left: i64 = -1;
        for _ in 0..10 {
            let fresh = session().await?;
            fresh
                .query("SELECT * FROM pg_kafka_run_retention_sweep(0, 3600)", &[])
                .await
                .map_err(boxed)?;
            left = db
                .query_one(
                    "SELECT count(*) FROM kafka.messages
                     WHERE topic_id = $1 AND created_at < timestamptz '2002-01-01 00:00+00'",
                    &[&short],
                )
                .await
                .map_err(boxed)?
                .get(0);
            if left == 0 {
                break;
            }
        }
        if left != 0 {
            return Err(format!("{left} expired rows left in {trimmed}").into());
        }
        println!("Step 3: calls from fresh sessions trimmed {trimmed}");
        SendResult::Ok(())
    }
    .await;
    stop.store(true, Ordering::Relaxed);
    let traffic = traffic.await.map_err(boxed)?;
    force_drop(&db, &trimmed).await?;
    force_drop(&db, &droppable).await?;
    delete_topics(&db, &[short, forever]).await?;
    outcome?;
    traffic?;
    println!("✅ the pass dropped the leaf beside a Produce waiting for its trimmed partition");
    Ok(())
}

/// Rows the plan test keeps in the leaf beside the expired ones.
const KEPT_ROWS: i64 = 100_000;
/// Expired rows the plan test deletes from the leaf.
const EXPIRED_ROWS: i64 = 10_000;

/// The row deletes of the sweep reach their rows by ctid. Written as one statement,
/// `ctid = ANY (ARRAY(subquery))` left the planner free to read the leaf another way
/// and filter every row against the array: under stale statistics it chose a BRIN
/// bitmap scan of the whole leaf (47 s for one batch on a 990 000-row leaf). The test
/// stands in for such a misjudgement by turning TID scans off in the sweeping session
/// (`enable_tidscan = off`), and counts the tuples the sweep reads from the leaf.
///
/// Asserts: a sweep that deletes 10 000 expired rows from a leaf that keeps 100 000
/// others reads fewer than 3 x 10 000 tuples of it (a scan of the leaf is 110 000 per
/// batch).
pub async fn test_sweep_row_deletes_read_only_their_rows_whatever_the_plan() -> TestResult {
    into_test_result(row_deletes_by_tid().await)
}

async fn row_deletes_by_tid() -> SendResult<()> {
    println!("=== Test: the sweep's row deletes read only their rows, whatever the plan ===\n");
    let db = session().await?;
    db.batch_execute("SET statement_timeout = '120s'")
        .await
        .map_err(boxed)?;
    let expired = create_topic(&db, &unique("tid-expired"), 1000).await?;
    let forever = create_topic(&db, &unique("tid-forever"), -1).await?;
    let hour = random_hour(&db).await?;
    let leaf = hour_leaf(&db, hour).await?;
    let outcome = async {
        fill_leaf_dense(&db, forever, hour, 0, KEPT_ROWS, false).await?;
        fill_leaf_dense(&db, expired, hour, 0, EXPIRED_ROWS, false).await?;
        db.batch_execute(&format!("ANALYZE {leaf}"))
            .await
            .map_err(boxed)?;
        println!("Step 1: {leaf} holds {KEPT_ROWS} kept and {EXPIRED_ROWS} expired rows");
        db.batch_execute("SET enable_tidscan = off; BEGIN")
            .await
            .map_err(boxed)?;
        let rows = db
            .query("SELECT * FROM pg_kafka_run_retention_sweep(0, 3600)", &[])
            .await
            .map_err(boxed)?;
        let read: i64 = db
            .query_one(
                "SELECT COALESCE(SUM(seq_tup_read + COALESCE(idx_tup_fetch, 0)), 0)::bigint
                 FROM pg_stat_xact_user_tables WHERE relid = $1::text::regclass",
                &[&leaf],
            )
            .await
            .map_err(boxed)?
            .get(0);
        db.batch_execute("COMMIT; RESET enable_tidscan")
            .await
            .map_err(boxed)?;
        let deleted = category(&rows, "expired_messages");
        println!("Step 2: the sweep deleted {deleted} row(s) and read {read} tuple(s) of {leaf}");
        if deleted < EXPIRED_ROWS {
            return Err(format!("only {deleted} of {EXPIRED_ROWS} expired rows deleted").into());
        }
        if read >= 3 * EXPIRED_ROWS {
            return Err(format!(
                "deleting {deleted} rows read {read} tuples of the leaf: it was scanned"
            )
            .into());
        }
        SendResult::Ok(())
    }
    .await;
    db.batch_execute("ROLLBACK; RESET enable_tidscan")
        .await
        .ok();
    force_drop(&db, &leaf).await?;
    delete_topics(&db, &[expired, forever]).await?;
    outcome?;
    println!("✅ the deletes went by ctid");
    Ok(())
}
