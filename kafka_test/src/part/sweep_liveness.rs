//! What an on-demand retention sweep and partition DDL make other sessions (the
//! worker's DB thread above all) wait for, and that an on-demand pass does the drops
//! its own probes find.
//!
//! All sequential: they create, fill and drop leaves of kafka.messages and run the
//! retention sweep.

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

