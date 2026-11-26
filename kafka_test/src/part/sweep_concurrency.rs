//! Concurrent retention sweeps and partition maintenance against each other.

use crate::common::{create_producer, TestResult};
use crate::worker::{
    boxed, deadlocks, into_test_result, send, session, unique, wait_for_serving_listener,
    SendResult, ServerLog,
};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_postgres::Client;

/// Rounds of concurrent sweeps; each round gives every sweep leaves to drop.
const ROUNDS: i32 = 12;
/// Concurrent `pg_kafka_run_retention_sweep()` calls per round.
const SWEEPERS: usize = 4;
/// Past leaves created per round, each with expired and aborted rows.
const LEAVES_PER_ROUND: i32 = 3;
/// Rows per leaf.
const ROWS_PER_LEAF: i64 = 50;

/// Concurrent retention sweeps (and partition maintenance) must never deadlock. A sweep
/// deletes rows (the aborted-row cleanup, then the expired rows), which leaves it
/// holding ROW EXCLUSIVE on kafka.messages and row locks, and then drops leaves, which
/// needs the maintenance advisory lock and ACCESS EXCLUSIVE on kafka.messages. Two
/// sweeps interleaving those steps (one holding the advisory lock and waiting for the
/// other's ROW EXCLUSIVE, the other waiting for the advisory lock or for the first's
/// deleted rows) form a lock cycle, and Postgres aborts one with "deadlock detected".
///
/// Asserts: over several rounds, each with fresh expired leaves to drop and aborted
/// rows to clean up, four concurrent sweeps and a partition-maintenance call all
/// succeed, the database records no deadlock, a producer running throughout gets every
/// record acked, and the expired leaves end up dropped.
pub async fn test_concurrent_sweeps_and_maintenance_do_not_deadlock() -> TestResult {
    into_test_result(concurrent_sweeps().await)
}

async fn concurrent_sweeps() -> SendResult<()> {
    println!("=== Test: concurrent retention sweeps and maintenance do not deadlock ===\n");
    let db = session().await?;
    super::drain_droppable_leaves(&db).await?;
    let topic = unique("sweep-race");
    let topic_id: i32 = db
        .query_one(
            "INSERT INTO kafka.topics (name, partitions, retention_ms) VALUES ($1, 1, 1000)
             RETURNING id",
            &[&topic],
        )
        .await
        .map_err(boxed)?
        .get(0);
    // A random hour range in 2003-2012, years no other test's leaves or rows use.
    let base_hour: i32 = db
        .query_one("SELECT (random() * 80000)::int", &[])
        .await
        .map_err(boxed)?
        .get(0);

    let mut sweepers = Vec::new();
    for _ in 0..SWEEPERS {
        let c = session().await?;
        c.batch_execute("SET statement_timeout = '60s'")
            .await
            .map_err(boxed)?;
        sweepers.push(Arc::new(c));
    }
    let maintainer = Arc::new(session().await?);

    // The worker keeps writing kafka.messages throughout: produce must stay unblocked.
    let produce_topic = unique("sweep-race-produce");
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let producing = {
        let stop = stop.clone();
        let topic = produce_topic.clone();
        tokio::spawn(async move {
            let producer = create_producer().map_err(boxed)?;
            let mut acked = 0u64;
            let mut errors = Vec::new();
            while !stop.load(std::sync::atomic::Ordering::SeqCst) {
                match send(&producer, &topic, "p").await {
                    Ok(_) => acked += 1,
                    Err(e) => errors.push(e.to_string()),
                }
            }
            SendResult::Ok((acked, errors))
        })
    };

    let deadlocks_before = deadlocks(&db).await?;
    let mut leaves = Vec::new();
    let mut failures = Vec::new();
    let started = Instant::now();
    for round in 0..ROUNDS {
        for k in 0..LEAVES_PER_ROUND {
            let hour = base_hour + round * LEAVES_PER_ROUND + k;
            db.query_one(
                "SELECT kafka.create_message_partition(lo, lo + INTERVAL '1 hour')
                 FROM (SELECT timestamptz '2003-01-01 00:00+00' + $1::int * INTERVAL '1 hour') AS b(lo)",
                &[&hour],
            )
            .await
            .map_err(boxed)?;
            let leaf: String = db
                .query_one(
                    "SELECT partition_oid::regclass::text FROM kafka.message_partitions()
                     WHERE lower_bound = timestamptz '2003-01-01 00:00+00' + $1::int * INTERVAL '1 hour'",
                    &[&hour],
                )
                .await
                .map_err(boxed)?
                .get(0);
            // Expired committed rows (the leaf is dropped whole) and aborted rows older
            // than the sweep's grace (the aborted-row cleanup deletes them first).
            let first_offset = i64::from(hour) * ROWS_PER_LEAF * 2;
            db.execute(
                "INSERT INTO kafka.messages (topic_id, partition_id, partition_offset, value,
                                             created_at, txn_state)
                 SELECT $1, 0, $2::bigint + g, 'v'::bytea,
                        timestamptz '2003-01-01 00:00+00' + $3::int * INTERVAL '1 hour'
                            + g * INTERVAL '1 second',
                        CASE WHEN g % 2 = 0 THEN NULL ELSE 'aborted' END
                 FROM generate_series(0::bigint, $4::bigint - 1) AS g",
                &[&topic_id, &first_offset, &hour, &(ROWS_PER_LEAF * 2)],
            )
            .await
            .map_err(boxed)?;
            leaves.push(leaf);
        }

        let mut tasks = Vec::new();
        for c in &sweepers {
            let c = c.clone();
            tasks.push(tokio::spawn(async move {
                c.query("SELECT * FROM pg_kafka_run_retention_sweep(0, 60)", &[])
                    .await
                    .map(|_| ())
                    .map_err(|e| format!("sweep: {}", db_error(&e)))
            }));
        }
        {
            let c = maintainer.clone();
            tasks.push(tokio::spawn(async move {
                c.query("SELECT pg_kafka_run_partition_maintenance()", &[])
                    .await
                    .map(|_| ())
                    .map_err(|e| format!("maintenance: {}", db_error(&e)))
            }));
        }
        for t in tasks {
            if let Err(e) = t.await.map_err(boxed)? {
                failures.push(format!("round {round}: {e}"));
            }
        }
        println!(
            "Round {round}: {} failure(s) so far, {:?} elapsed",
            failures.len(),
            started.elapsed()
        );
    }

    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    let (acked, produce_errors) = producing.await.map_err(boxed)??;
    failures.extend(produce_errors.into_iter().map(|e| format!("produce: {e}")));
    let deadlocks_after = deadlocks(&db).await?;

    // One more sweep for anything a sweep left to the next pass (a leaf it could not
    // lock in time), then the expired leaves must be gone.
    db.query("SELECT * FROM pg_kafka_run_retention_sweep(0, 60)", &[])
        .await
        .map_err(boxed)?;
    let remaining = remaining_leaves(&db, &leaves).await?;
    db.execute(
        "DELETE FROM kafka.topics WHERE name = ANY($1)",
        &[&vec![topic.clone(), produce_topic.clone()]],
    )
    .await
    .map_err(boxed)?;

    println!(
        "   {} failure(s), {} new deadlock(s), {acked} record(s) produced meanwhile, \
         {} of {} leaves left",
        failures.len(),
        deadlocks_after - deadlocks_before,
        remaining.len(),
        leaves.len()
    );
    if !failures.is_empty() {
        return Err(format!(
            "concurrent sweeps/maintenance failed {} time(s): {}",
            failures.len(),
            failures.join("; ")
        )
        .into());
    }
    assert_eq!(
        deadlocks_after, deadlocks_before,
        "the database recorded deadlocks during the concurrent sweeps"
    );
    assert!(acked > 0, "the producer must keep getting acks");
    assert!(
        remaining.is_empty(),
        "expired leaves were not dropped: {remaining:?}"
    );
    println!("✅ no deadlock; every sweep and maintenance call succeeded");
    Ok(())
}

/// The message and detail of a database error ("deadlock detected" and its cycle).
fn db_error(e: &tokio_postgres::Error) -> String {
    match e.as_db_error() {
        Some(d) => format!(
            "{} ({}){}",
            d.message(),
            d.code().code(),
            d.detail().map_or_else(String::new, |s| format!(": {s}"))
        ),
        None => e.to_string(),
    }
}

/// The leaves in `leaves` that still exist.
async fn remaining_leaves(db: &Client, leaves: &[String]) -> SendResult<Vec<String>> {
    let rows = db
        .query(
            "SELECT l FROM unnest($1::text[]) AS l WHERE to_regclass(l) IS NOT NULL",
            &[&leaves],
        )
        .await
        .map_err(boxed)?;
    Ok(rows.iter().map(|r| r.get(0)).collect())
}

/// The worker's retention sweep never waits for the partition-maintenance lock (its
/// transaction holds the schema lock, which a manual maintenance pass holding that lock
/// may be waiting for): while another session holds it, the worker skips the pass with
/// a WARNING and keeps serving, and the next interval retries.
///
/// Asserts: with the maintenance lock held by another session, the worker's next sweep
/// (at most one sweep interval, 60 s, away) logs that it was skipped, the same listener
/// keeps answering produce requests, and after the lock is released it still serves.
pub async fn test_worker_sweep_skips_while_maintenance_lock_held() -> TestResult {
    into_test_result(worker_sweep_skips().await)
}

async fn worker_sweep_skips() -> SendResult<()> {
    println!("=== Test: the worker's sweep skips while the maintenance lock is held ===\n");
    let db = session().await?;
    let pid = wait_for_serving_listener(&db, None, Duration::from_secs(30)).await?;
    let Some(log) = ServerLog::open(&db).await? else {
        println!("   (server log not readable as a file: skipped)");
        return Ok(());
    };
    let from = log.end();
    let holder = session().await?;
    holder.batch_execute("BEGIN").await.map_err(boxed)?;
    holder
        .query("SELECT pg_advisory_xact_lock(8099547369649367905)", &[])
        .await
        .map_err(boxed)?;
    let result: SendResult<()> = async {
        let deadline = Instant::now() + Duration::from_secs(75);
        while !log.since(from).contains("Retention sweep skipped") {
            if Instant::now() > deadline {
                return Err("the worker's sweep never reported a skip while the \
                            maintenance lock was held"
                    .into());
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        println!("Step 1: the worker logged the skipped sweep");
        let serving = wait_for_serving_listener(&db, None, Duration::from_secs(10)).await?;
        assert_eq!(serving, pid, "the listener must not have restarted");
        Ok(())
    }
    .await;
    holder.batch_execute("ROLLBACK").await.map_err(boxed)?;
    result?;
    let serving = wait_for_serving_listener(&db, None, Duration::from_secs(10)).await?;
    assert_eq!(serving, pid, "the listener must not have restarted");
    println!("✅ the worker skipped the sweep and kept serving (pid {pid})");
    Ok(())
}
