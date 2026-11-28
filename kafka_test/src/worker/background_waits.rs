//! The worker's background statements (the broker-metrics flush here) wait only
//! briefly for rows another session holds: the single DB thread answers every client,
//! so a background statement that waited up to `pg_kafka.worker_lock_timeout_ms`
//! (5 s by default) for a row an operator's transaction holds stopped every client
//! that long, once per run.

use crate::common::TestResult;
use crate::wire::Wire;
use crate::worker::{boxed, into_test_result, session, SendResult};
use kafka_protocol::messages::metadata_request::MetadataRequest;
use std::time::{Duration, Instant};

/// How long the test holds the gauge rows: several flush intervals (1 s each).
const HOLD: Duration = Duration::from_secs(4);
/// The slowest Metadata answer the test accepts while the rows are held: far below
/// the 5 s worker lock timeout a flush used to wait out.
const METADATA_CEILING: Duration = Duration::from_millis(200);

/// The broker-metrics flush (every second, when a counter moved) upserts
/// `kafka.broker_api_stats` and `kafka.broker_gauges`. With another session holding
/// those rows (`SELECT ... FOR UPDATE` in an open transaction, as an operator's script
/// or a dashboard's lock might), it waited the worker lock timeout every interval and
/// every client waited with it.
///
/// Asserts: while a session holds every row of kafka.broker_gauges and
/// kafka.broker_api_stats for 4 s, Metadata requests sent every 100 ms (each one moves
/// a counter, so every interval flushes) are all answered within 200 ms; after the
/// holder ends, the flush catches up (kafka.broker_api_stats shows the Metadata
/// requests sent meanwhile).
pub async fn test_metrics_flush_skips_held_rows() -> TestResult {
    into_test_result(metrics_flush_skips_held_rows().await)
}

async fn metrics_requests(db: &tokio_postgres::Client) -> SendResult<i64> {
    Ok(db
        .query_one(
            "SELECT COALESCE(MAX(requests), 0)::int8 FROM kafka.broker_api_stats WHERE api_key = 3",
            &[],
        )
        .await
        .map_err(boxed)?
        .get(0))
}

async fn metrics_flush_skips_held_rows() -> SendResult<()> {
    println!("=== Test: the metrics flush does not wait for held rows ===\n");
    let db = session().await?;
    let mut wire = Wire::connect().await.map_err(|e| boxed(e.to_string()))?;
    let metadata = MetadataRequest::default();
    // Make sure both tables have rows to hold (the first flush writes them).
    wire.call(9, &metadata)
        .await
        .map_err(|e| boxed(e.to_string()))?;
    let deadline = Instant::now() + Duration::from_secs(10);
    while metrics_requests(&db).await? == 0 {
        if Instant::now() >= deadline {
            return Err("the metrics flush never wrote kafka.broker_api_stats".into());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let holder = session().await?;
    holder
        .batch_execute(
            "BEGIN;
             SELECT * FROM kafka.broker_gauges FOR UPDATE;
             SELECT * FROM kafka.broker_api_stats FOR UPDATE;",
        )
        .await
        .map_err(boxed)?;
    println!("Step 1: a session holds every metrics row");

    let started = Instant::now();
    let mut slowest = Duration::ZERO;
    let mut sent = 0;
    while started.elapsed() < HOLD {
        let t = Instant::now();
        wire.call(9, &metadata)
            .await
            .map_err(|e| boxed(e.to_string()))?;
        slowest = slowest.max(t.elapsed());
        sent += 1;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let before_release = metrics_requests(&db).await?;
    holder.batch_execute("COMMIT").await.map_err(boxed)?;
    println!("Step 2: {sent} Metadata requests, slowest answered in {slowest:?}");
    if slowest >= METADATA_CEILING {
        return Err(format!(
            "a Metadata request waited {slowest:?} while another session held the metrics rows"
        )
        .into());
    }

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let now = metrics_requests(&db).await?;
        if now >= before_release + sent {
            println!("Step 3: the flush caught up ({before_release} -> {now} Metadata requests)");
            break;
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "the metrics flush did not catch up after the rows were released: \
                 {now} Metadata requests published, at least {} sent",
                before_release + sent
            )
            .into());
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    println!("✅ the flush skipped the held rows and caught up afterwards");
    Ok(())
}

/// The slowest worker start the test accepts with two leaves to create behind a reader,
/// from the new worker's appearance to its first Metadata answer: the start itself, two
/// 200 ms lock waits and slack; at 2 s per leaf it took over 4 s.
const START_CEILING: Duration = Duration::from_millis(2500);

/// The worker's partition maintenance (at start, and before every retention sweep) and
/// its sweep run on the DB thread, so a lock they wait for stops every client. They
/// wait only for other sessions (no worker request runs meanwhile): here an operator's
/// transaction that read kafka.messages and stayed open, which holds ACCESS SHARE on it
/// and so blocks the ACCESS EXCLUSIVE lock a new leaf needs. The partition functions
/// waited 2 s per leaf for it (two leaves ahead: 4 s before the worker served anyone).
/// The worker now runs them with a 200 ms lock_timeout, which the functions honour when
/// it is lower than their 2 s.
///
/// Asserts: with the future leaves dropped (each drop asserted) and a reader holding
/// ACCESS SHARE on kafka.messages, a restarted worker answers its first Metadata request
/// (a raw connection, retried from the moment the new worker appears, so no client
/// start-up is timed) within 2.5 s of appearing; once the reader ends, maintenance makes
/// the leaves again.
pub async fn test_worker_maintenance_waits_briefly_behind_a_reader() -> TestResult {
    into_test_result(maintenance_behind_reader().await)
}

/// Retry a raw Metadata request until the listener answers one, for at most `within`:
/// when it answered.
async fn first_metadata_answer(within: Duration) -> SendResult<Instant> {
    let deadline = Instant::now() + within;
    let metadata = MetadataRequest::default();
    loop {
        let wire = Wire::connect().await.ok();
        if let Some(mut wire) = wire {
            let answered = wire.call(9, &metadata).await.is_ok();
            if answered {
                return Ok(Instant::now());
            }
        }
        if Instant::now() >= deadline {
            return Err(
                format!("the new worker answered no Metadata request within {within:?}").into(),
            );
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn maintenance_behind_reader() -> SendResult<()> {
    use crate::worker::listener_pid;
    println!("=== Test: the worker's maintenance waits briefly behind a reader ===\n");
    let db = session().await?;
    let minutes: String = db
        .query_one("SHOW pg_kafka.message_partition_minutes", &[])
        .await
        .map_err(boxed)?
        .get(0);
    let minutes: i32 = minutes.parse().map_err(boxed)?;
    let future: Vec<String> = db
        .query(
            "SELECT partition_oid::regclass::text FROM kafka.message_partitions()
             WHERE NOT is_default AND lower_bound > now()",
            &[],
        )
        .await
        .map_err(boxed)?
        .iter()
        .map(|r| r.get(0))
        .collect();
    if future.is_empty() {
        return Err("no future leaf to drop: the worker would create none at start".into());
    }
    for leaf in &future {
        let row = db
            .query_one(
                "SELECT dropped, lock_timed_out
                 FROM kafka.drop_message_partition($1::text::regclass, ARRAY[]::int4[])",
                &[leaf],
            )
            .await
            .map_err(boxed)?;
        let (dropped, timed_out): (bool, bool) = (row.get(0), row.get(1));
        if !dropped {
            return Err(format!(
                "could not drop the future leaf {leaf} (lock_timed_out={timed_out})"
            )
            .into());
        }
    }
    println!("Step 1: dropped the future leaves {future:?}");
    let reader = session().await?;
    reader
        .batch_execute("BEGIN; SELECT 1 FROM kafka.messages LIMIT 1")
        .await
        .map_err(boxed)?;
    let outcome = async {
        let old = listener_pid(&db).await?;
        db.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity
             WHERE backend_type = 'pg_kafka_listener'",
            &[],
        )
        .await
        .map_err(boxed)?;
        let deadline = Instant::now() + Duration::from_secs(30);
        let started = loop {
            match listener_pid(&db).await? {
                Some(pid) if Some(pid) != old => break Instant::now(),
                _ if Instant::now() >= deadline => {
                    return Err("no new pg_kafka_listener within 30 s".into());
                }
                _ => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        };
        let answered = first_metadata_answer(Duration::from_secs(20)).await?;
        let took = answered.duration_since(started);
        println!(
            "Step 2: the restarted worker answered its first Metadata {took:?} after appearing"
        );
        if took >= START_CEILING {
            return Err(format!(
                "the worker took {took:?} to serve after starting behind an open reader"
            )
            .into());
        }
        SendResult::Ok(())
    }
    .await;
    reader.batch_execute("ROLLBACK").await.map_err(boxed)?;
    db.execute("SELECT pg_kafka_run_partition_maintenance($1)", &[&minutes])
        .await
        .map_err(boxed)?;
    let missing: i64 = db
        .query_one(
            "SELECT count(*) FROM unnest($1::text[]) l WHERE to_regclass(l) IS NULL",
            &[&future],
        )
        .await
        .map_err(boxed)?
        .get(0);
    outcome?;
    if missing != 0 {
        return Err(format!("{missing} future leaves not made again").into());
    }
    println!(
        "✅ the worker gave up its leaf creation quickly and maintenance made the leaves later"
    );
    Ok(())
}
