//! Leaf drops and the transaction isolation level of the sweep.

use super::drop_extension::MAINTENANCE_LOCK_KEY;
use super::sweep_budget::{create_topic, fill_leaf, hour_leaf};
use crate::common::TestResult;
use crate::setup::SystemGucGuard;
use crate::worker::{boxed, into_test_result, session, unique, SendResult};
use std::time::{Duration, Instant};
use tokio_postgres::Client;

/// How long the writer's transaction stays open after the sweep started: well inside
/// the drop's 2 s lock_timeout, so the drop waits for it and then goes on.
const WRITER_HOLD: Duration = Duration::from_millis(1000);
/// When the sweep starts, after the writer's INSERT.
const SWEEP_START: Duration = Duration::from_millis(300);
/// The worker sweeps every 60 s; this leaves room for a pass already in progress.
const WORKER_SWEEP_WAIT: Duration = Duration::from_secs(75);

/// `kafka.drop_message_partition` re-checks the leaf's contents once it holds the
/// table locks, but the check reads the transaction's snapshot: under REPEATABLE READ
/// or SERIALIZABLE that snapshot predates the lock wait, so a row committed while the
/// drop waited for the writer was invisible to it and dropped with the leaf, after the
/// writer had been told it was stored.
///
/// Asserts: with the worker's sweep held off by the maintenance lock, a past one-hour
/// leaf, empty when a REPEATABLE READ sweep starts, and a row of it committed while
/// that sweep waits for its drop locks: the sweep drops nothing and the row is kept.
/// With the row deleted, a REPEATABLE READ sweep still does not drop the empty leaf,
/// and a READ COMMITTED one does.
pub async fn test_sweep_keeps_rows_committed_during_a_repeatable_read_drop() -> TestResult {
    into_test_result(repeatable_read_drop().await)
}

async fn repeatable_read_drop() -> SendResult<()> {
    println!(
        "=== Test: a REPEATABLE READ sweep never drops a row committed during its lock wait ===\n"
    );
    let sweeper = session().await?;
    sweeper
        .batch_execute("SET statement_timeout = '30s'")
        .await
        .map_err(boxed)?;
    // The worker's own sweep skips while this session holds the lock; this session's
    // sweeps re-enter it.
    sweeper
        .execute("SELECT pg_advisory_lock($1)", &[&MAINTENANCE_LOCK_KEY])
        .await
        .map_err(boxed)?;
    let mut topic = None;
    let mut leaf = None;
    let result = run(&sweeper, &mut topic, &mut leaf).await;
    if result.is_err() {
        // Leave no transaction open in the sweeper before cleaning up.
        sweeper.batch_execute("ROLLBACK").await.map_err(boxed)?;
    }
    let cleaned = cleanup(&sweeper, topic, leaf.as_deref()).await;
    let unlocked = sweeper
        .execute("SELECT pg_advisory_unlock($1)", &[&MAINTENANCE_LOCK_KEY])
        .await
        .map_err(boxed);
    result?;
    cleaned?;
    unlocked?;
    println!("✅ no committed row was dropped; only READ COMMITTED dropped the empty leaf");
    Ok(())
}

async fn run(
    sweeper: &Client,
    topic_out: &mut Option<i32>,
    leaf_out: &mut Option<String>,
) -> SendResult<()> {
    // An hour of 1999 (20 to 8020 hours before 2000): the other tests that create leaves
    // there are sequential part tests that drop them, so no other test's leaves are there.
    let hour: i32 = sweeper
        .query_one("SELECT -20 - (random() * 8000)::int", &[])
        .await
        .map_err(boxed)?
        .get(0);
    let topic = create_topic(sweeper, &unique("rr-drop"), -1).await?;
    *topic_out = Some(topic);
    let leaf = hour_leaf(sweeper, hour).await?;
    *leaf_out = Some(leaf.clone());

    let writer = session().await?;
    writer.batch_execute("BEGIN").await.map_err(boxed)?;
    fill_leaf(&writer, topic, hour, 0, 1).await?;
    println!("Step 1: {leaf} is empty to others; a writer holds one uncommitted row of it");

    let commit = async {
        tokio::time::sleep(WRITER_HOLD).await;
        writer.batch_execute("COMMIT").await.map_err(boxed)
    };
    let sweep = async {
        tokio::time::sleep(SWEEP_START).await;
        sweeper
            .batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
            .await
            .map_err(boxed)?;
        let dropped = dropped_partitions(sweeper).await?;
        sweeper.batch_execute("COMMIT").await.map_err(boxed)?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(dropped)
    };
    let (committed, dropped) = tokio::join!(commit, sweep);
    committed?;
    let dropped = dropped?;
    let rows = count(sweeper, topic).await?;
    let exists = leaf_exists(sweeper, &leaf).await?;
    println!(
        "Step 2: the REPEATABLE READ sweep (started before the commit) dropped {dropped} \
         leaves; {rows} row(s) of the topic left, leaf exists: {exists}"
    );
    if rows != 1 || !exists || dropped != 0 {
        return Err(format!(
            "the sweep dropped the leaf holding a row committed during its lock wait \
             (dropped_partitions {dropped}, rows left {rows}, leaf exists {exists})"
        )
        .into());
    }

    sweeper
        .execute("DELETE FROM kafka.messages WHERE topic_id = $1", &[&topic])
        .await
        .map_err(boxed)?;
    sweeper
        .batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
        .await
        .map_err(boxed)?;
    let dropped = dropped_partitions(sweeper).await?;
    sweeper.batch_execute("COMMIT").await.map_err(boxed)?;
    let exists = leaf_exists(sweeper, &leaf).await?;
    println!("Step 3: with the leaf empty, a REPEATABLE READ sweep dropped {dropped}; leaf exists: {exists}");
    if dropped != 0 || !exists {
        return Err("a REPEATABLE READ sweep must not drop leaves".into());
    }

    let dropped = dropped_partitions(sweeper).await?;
    let exists = leaf_exists(sweeper, &leaf).await?;
    println!("Step 4: a READ COMMITTED sweep dropped {dropped}; leaf exists: {exists}");
    if dropped < 1 || exists {
        return Err("a READ COMMITTED sweep must drop the empty past leaf".into());
    }
    Ok(())
}

/// The worker's transactions run at READ COMMITTED whatever the cluster's
/// `default_transaction_isolation`: its sweep drops leaves only at that level.
///
/// Asserts: with `default_transaction_isolation = 'repeatable read'` set by ALTER
/// SYSTEM and reloaded, the worker's own sweep drops an empty past leaf.
pub async fn test_worker_sweep_drops_under_a_repeatable_read_default() -> TestResult {
    into_test_result(worker_default_isolation().await)
}

async fn worker_default_isolation() -> SendResult<()> {
    println!("=== Test: the worker drops leaves under default_transaction_isolation = repeatable read ===\n");
    let db = session().await?;
    let hour: i32 = db
        .query_one("SELECT -20 - (random() * 8000)::int", &[])
        .await
        .map_err(boxed)?
        .get(0);
    let leaf = hour_leaf(&db, hour).await?;
    let guard = SystemGucGuard::set(&db, "default_transaction_isolation", "repeatable read")
        .await?
        .ok_or("default_transaction_isolation could not be set")?;
    println!("Step 1: {leaf} is an empty past leaf; the cluster default is REPEATABLE READ");
    let deadline = Instant::now() + WORKER_SWEEP_WAIT;
    let result: SendResult<()> = async {
        while leaf_exists(&db, &leaf).await? {
            if Instant::now() > deadline {
                return Err(format!(
                    "the worker's sweep did not drop {leaf} within {WORKER_SWEEP_WAIT:?}"
                )
                .into());
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        Ok(())
    }
    .await;
    guard.restore().await?;
    if result.is_err() {
        db.query(
            "SELECT dropped FROM kafka.drop_message_partition(to_regclass($1), '{}')",
            &[&leaf],
        )
        .await
        .map_err(boxed)?;
    }
    result?;
    println!("✅ the worker's sweep dropped {leaf}");
    Ok(())
}

async fn dropped_partitions(db: &Client) -> SendResult<i64> {
    Ok(db
        .query_one(
            "SELECT deleted FROM pg_kafka_run_retention_sweep(0, 60)
             WHERE category = 'dropped_partitions'",
            &[],
        )
        .await
        .map_err(boxed)?
        .get(0))
}

async fn count(db: &Client, topic_id: i32) -> SendResult<i64> {
    Ok(db
        .query_one(
            "SELECT count(*) FROM kafka.messages WHERE topic_id = $1",
            &[&topic_id],
        )
        .await
        .map_err(boxed)?
        .get(0))
}

async fn leaf_exists(db: &Client, leaf: &str) -> SendResult<bool> {
    Ok(db
        .query_one("SELECT to_regclass($1) IS NOT NULL", &[&leaf])
        .await
        .map_err(boxed)?
        .get(0))
}

async fn cleanup(db: &Client, topic: Option<i32>, leaf: Option<&str>) -> SendResult<()> {
    if let Some(topic) = topic {
        db.execute("DELETE FROM kafka.messages WHERE topic_id = $1", &[&topic])
            .await
            .map_err(boxed)?;
    }
    if let Some(leaf) = leaf {
        db.query(
            "SELECT dropped FROM kafka.drop_message_partition(to_regclass($1), '{}')",
            &[&leaf],
        )
        .await
        .map_err(boxed)?;
    }
    if let Some(topic) = topic {
        db.execute("DELETE FROM kafka.topics WHERE id = $1", &[&topic])
            .await
            .map_err(boxed)?;
    }
    Ok(())
}
