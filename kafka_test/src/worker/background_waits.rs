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
