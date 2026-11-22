//! Lock order of partition maintenance: kafka.messages before any of its leaves.

use crate::common::TestResult;
use crate::worker::{boxed, into_test_result, session, wait_for_serving_listener, SendResult};
use std::time::{Duration, Instant};
use tokio_postgres::Client;

/// Partition maintenance must lock kafka.messages before any of its leaves, the order
/// every statement through the parent (and every DDL on it) takes them in. The pass
/// reads kafka.messages_default first; locking only that leaf, it later waited (in
/// `lock_message_tables`) for kafka.messages behind a session that held the parent
/// ACCESS EXCLUSIVE and waited for the leaf: a lock cycle, broken by "deadlock
/// detected" in that session or by the pass giving the leaf up at its lock_timeout.
///
/// The race, made deterministic: the worker's furthest leaf ahead is dropped so the
/// pass has one to create, and a session holds kafka.topics ROW EXCLUSIVE, so the pass
/// stops in `lock_message_tables` (which takes kafka.topics before kafka.messages)
/// after its reads of the DEFAULT leaf. Meanwhile another session runs
/// `LOCK TABLE kafka.messages IN ACCESS EXCLUSIVE MODE` (which recurses to the leaves);
/// then kafka.topics is released.
///
/// Asserts: the pass, holding the DEFAULT leaf, also holds kafka.messages; the LOCK
/// TABLE waits for kafka.messages itself, not for a leaf; and both then succeed, the
/// pass recreating the leaf.
pub async fn test_partition_maintenance_locks_parent_before_leaves() -> TestResult {
    into_test_result(parent_before_leaves().await)
}

/// The worker's furthest leaf ahead, `[b + 2w, b + 3w)` for the current bucket `b`.
pub(super) async fn furthest_leaf(db: &Client) -> SendResult<Option<String>> {
    Ok(db
        .query_opt(
            "SELECT p.partition_oid::regclass::text
             FROM kafka.message_partitions() p,
                  LATERAL (SELECT current_setting('pg_kafka.message_partition_minutes')::bigint
                                  * 60 AS w) s
             WHERE NOT p.is_default
               AND p.lower_bound = to_timestamp((floor(extract(epoch FROM now()) / s.w) + 2) * s.w)",
            &[],
        )
        .await
        .map_err(boxed)?
        .map(|r| r.get(0)))
}

/// Poll `sql` (one bool column, parameter `$1` = `pid`) until it is true or `limit`
/// passes; returns whether it became true.
pub(super) async fn wait_until(
    db: &Client,
    sql: &str,
    pid: i32,
    limit: Duration,
) -> SendResult<bool> {
    let deadline = Instant::now() + limit;
    loop {
        if db.query_one(sql, &[&pid]).await.map_err(boxed)?.get(0) {
            return Ok(true);
        }
        if Instant::now() >= deadline {
            return Ok(false);
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

pub(super) async fn backend_pid(db: &Client) -> SendResult<i32> {
    Ok(db
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .map_err(boxed)?
        .get(0))
}

/// Step 0 of the tests that race a maintenance pass: give the pass a leaf to create by
/// dropping the worker's furthest leaf ahead. A pass first (right after a bucket
/// boundary the worker may not have made the new furthest leaf yet); the drop returns
/// false while the worker's own maintenance holds the maintenance lock. Returns the
/// dropped leaf.
pub(super) async fn drop_furthest_leaf(db: &Client) -> SendResult<String> {
    for _ in 0..10 {
        db.query("SELECT pg_kafka_run_partition_maintenance()", &[])
            .await
            .map_err(boxed)?;
        let leaf = furthest_leaf(db)
            .await?
            .ok_or("no leaf two widths ahead; did the worker run its partition maintenance?")?;
        let ok: bool = db
            .query_one(
                "SELECT dropped FROM kafka.drop_message_partition($1::text::regclass, '{}')",
                &[&leaf],
            )
            .await
            .map_err(boxed)?
            .get(0);
        if ok {
            println!("Step 0: dropped {leaf}; the next pass recreates it");
            return Ok(leaf);
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    Err("could not drop the furthest leaf ahead".into())
}

async fn parent_before_leaves() -> SendResult<()> {
    println!("=== Test: partition maintenance locks kafka.messages before its leaves ===\n");
    let db = session().await?;
    wait_for_serving_listener(&db, None, Duration::from_secs(30)).await?;

    let leaf = drop_furthest_leaf(&db).await?;

    let result = race(&db).await;

    // Whatever happened, leave the worker's layout as it was.
    db.query("SELECT pg_kafka_run_partition_maintenance()", &[])
        .await
        .map_err(boxed)?;
    let (parent_held, ddl_waited_for, ddl, created) = result?;

    assert!(
        parent_held,
        "the pass held kafka.messages_default without kafka.messages"
    );
    assert_eq!(
        ddl_waited_for.as_deref(),
        Some("kafka.messages"),
        "LOCK TABLE kafka.messages must wait for the parent, not hold it and wait for a leaf"
    );
    ddl.map_err(|e| format!("LOCK TABLE kafka.messages failed: {e}"))?;
    assert_eq!(created, Ok(1), "the pass must recreate {leaf}");
    println!("✅ the pass locked kafka.messages first; the DDL waited for it, and both succeeded");
    Ok(())
}

/// What [`race`] observed: whether the pass held kafka.messages while holding the
/// DEFAULT leaf, the relation the LOCK TABLE waited for, its outcome, and the pass's.
type RaceOutcome = (
    bool,
    Option<String>,
    Result<(), String>,
    Result<i64, String>,
);

async fn race(db: &Client) -> SendResult<RaceOutcome> {
    let blocker = session().await?;
    blocker
        .batch_execute("BEGIN; LOCK TABLE kafka.topics IN ROW EXCLUSIVE MODE")
        .await
        .map_err(boxed)?;
    println!("Step 1: a session holds kafka.topics ROW EXCLUSIVE");

    let maintainer = session().await?;
    maintainer
        .batch_execute("SET statement_timeout = '30s'")
        .await
        .map_err(boxed)?;
    let maintainer_pid = backend_pid(&maintainer).await?;
    let pass = tokio::spawn(async move {
        maintainer
            .query_one("SELECT pg_kafka_run_partition_maintenance()", &[])
            .await
            .map(|r| r.get::<_, i64>(0))
            .map_err(|e| format!("{e:?}"))
    });

    let holds = |rel: &str| {
        format!(
            "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE pid = $1 AND granted
                            AND locktype = 'relation' AND relation = '{rel}'::regclass)"
        )
    };
    let has_default = wait_until(
        db,
        &holds("kafka.messages_default"),
        maintainer_pid,
        Duration::from_secs(5),
    )
    .await?;
    let parent_held = db
        .query_one(&holds("kafka.messages"), &[&maintainer_pid])
        .await
        .map_err(boxed)?
        .get(0);
    println!(
        "Step 2: the pass holds kafka.messages_default: {has_default}, kafka.messages: {parent_held}"
    );

    let ddl = session().await?;
    ddl.batch_execute("SET lock_timeout = '10s'; SET statement_timeout = '30s'")
        .await
        .map_err(boxed)?;
    let ddl_pid = backend_pid(&ddl).await?;
    let lock = tokio::spawn(async move {
        ddl.batch_execute("BEGIN; LOCK TABLE kafka.messages IN ACCESS EXCLUSIVE MODE; COMMIT")
            .await
            .map_err(|e| format!("{e:?}"))
    });
    let waiting = "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE pid = $1 AND NOT granted
                                  AND locktype = 'relation')";
    let ddl_waited_for: Option<String> =
        if wait_until(db, waiting, ddl_pid, Duration::from_secs(5)).await? {
            db.query_opt(
                "SELECT relation::regclass::text FROM pg_locks
                 WHERE pid = $1 AND NOT granted AND locktype = 'relation'",
                &[&ddl_pid],
            )
            .await
            .map_err(boxed)?
            .map(|r| r.get(0))
        } else {
            None
        };
    println!("Step 3: LOCK TABLE kafka.messages waits for {ddl_waited_for:?}");

    blocker.batch_execute("COMMIT").await.map_err(boxed)?;
    println!("Step 4: kafka.topics released");
    let created = pass.await.map_err(boxed)?;
    let ddl_result = lock.await.map_err(boxed)?;
    println!("Step 5: the pass returned {created:?}, the LOCK TABLE {ddl_result:?}");
    if !has_default {
        return Err("the pass never held kafka.messages_default".into());
    }
    Ok((parent_held, ddl_waited_for, ddl_result, created))
}
