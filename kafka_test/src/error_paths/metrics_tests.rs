//! E2E tests for the broker request metrics.
//!
//! The worker counts every request per API in memory and publishes the counters to
//! `kafka.broker_api_stats` / `kafka.broker_gauges` (views `kafka.api_metrics`,
//! `kafka.broker_metrics`) at most once per second.

use crate::common::{create_base_consumer, create_producer, TestResult};
use crate::error_codes;
use crate::fixtures::topic_with_partitions;
use crate::setup::TestContext;
use crate::wire::records::plain_batch;
use crate::wire::Wire;
use bytes::Bytes;
use kafka_protocol::records::Compression;
use rdkafka::consumer::{CommitMode, Consumer};
use rdkafka::producer::FutureRecord;
use rdkafka::TopicPartitionList;
use std::time::{Duration, Instant};

/// `(requests, errors, latency_us_sum)` of one API in `kafka.api_metrics`.
async fn api_counters(
    ctx: &TestContext,
    api: &str,
) -> Result<(i64, i64, i64), Box<dyn std::error::Error>> {
    let row = ctx
        .db()
        .query_opt(
            "SELECT requests, errors, latency_us_sum FROM kafka.api_metrics WHERE api_name = $1",
            &[&api],
        )
        .await?;
    Ok(row.map_or((0, 0, 0), |r| (r.get(0), r.get(1), r.get(2))))
}

/// Produce requests this test sends that succeed, that fail, and its Fetch requests.
const PRODUCES_OK: i64 = 5;
const PRODUCES_FAILED: i64 = 3;
const FETCHES: i64 = 4;

/// The counters of `kafka.api_metrics` count exactly the requests sent: five Produce
/// requests that succeed, three to a partition that does not exist (errors), and four
/// Fetch requests move Produce's requests by 8 and errors by 3, Fetch's requests by 4 and
/// errors by none, with their latency recorded; `kafka.broker_metrics` shows the connection
/// gauges. Sequential, so no other test's requests move the counters meanwhile.
pub async fn test_metrics_count_produce_and_fetch() -> TestResult {
    println!("=== Test: Broker Metrics Count Produce And Fetch ===\n");

    let ctx = TestContext::new().await?;
    let topic = topic_with_partitions(&ctx, "metrics-produce-fetch", 1).await?;
    let mut w = Wire::connect().await?;
    // Whatever earlier requests counted is flushed (at most once per second) before the
    // counters are read.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let produce_before = api_counters(&ctx, "Produce").await?;
    let fetch_before = api_counters(&ctx, "Fetch").await?;
    println!("Before: Produce={produce_before:?}, Fetch={fetch_before:?}");

    println!("Step 1: {PRODUCES_OK} produces, {PRODUCES_FAILED} to a missing partition, {FETCHES} fetches...");
    for i in 0..PRODUCES_OK {
        let (code, _) = w
            .produce(
                &topic,
                0,
                plain_batch(&[Bytes::from(format!("m{i}"))], Compression::None),
                None,
            )
            .await?;
        assert_eq!(code, error_codes::NONE);
    }
    for i in 0..PRODUCES_FAILED {
        let (code, _) = w
            .produce(
                &topic,
                99,
                plain_batch(&[Bytes::from(format!("x{i}"))], Compression::None),
                None,
            )
            .await?;
        assert_eq!(code, error_codes::UNKNOWN_TOPIC_OR_PARTITION);
    }
    for _ in 0..FETCHES {
        let p = w.fetch(&topic, 0, 0, 0).await?;
        assert_eq!(p.error_code, error_codes::NONE);
    }

    println!("Step 2: wait for the worker's metrics flush (<= 1 s interval)...");
    let delta = |before: (i64, i64, i64), after: (i64, i64, i64)| {
        (after.0 - before.0, after.1 - before.1, after.2 - before.2)
    };
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(15) {
        let produce = delta(produce_before, api_counters(&ctx, "Produce").await?);
        let fetch = delta(fetch_before, api_counters(&ctx, "Fetch").await?);
        if produce.0 >= PRODUCES_OK + PRODUCES_FAILED && fetch.0 >= FETCHES {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    // One more flush interval: nothing beyond this test's requests may arrive.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let produce = delta(produce_before, api_counters(&ctx, "Produce").await?);
    let fetch = delta(fetch_before, api_counters(&ctx, "Fetch").await?);
    println!("Deltas (requests, errors, latency_us_sum): Produce={produce:?}, Fetch={fetch:?}");
    assert_eq!(
        (produce.0, produce.1),
        (PRODUCES_OK + PRODUCES_FAILED, PRODUCES_FAILED),
        "Produce requests and errors"
    );
    assert_eq!(
        (fetch.0, fetch.1),
        (FETCHES, 0),
        "Fetch requests and errors"
    );
    assert!(
        produce.2 > 0 && fetch.2 > 0,
        "latency is recorded with the counts"
    );
    let max: i64 = ctx
        .db()
        .query_one(
            "SELECT latency_us_max FROM kafka.api_metrics WHERE api_name = 'Produce'",
            &[],
        )
        .await?
        .get(0);
    assert!(max > 0, "Produce latency max is recorded ({max})");

    // Gauges are published with every flush; this test's connection is open.
    let rows = ctx
        .db()
        .query(
            "SELECT name, value FROM kafka.broker_metrics
             WHERE name IN ('connections_open', 'connections_accepted_total')",
            &[],
        )
        .await?;
    let get = |n: &str| {
        rows.iter()
            .find(|r| r.get::<_, &str>(0) == n)
            .map(|r| r.get::<_, i64>(1))
    };
    let open = get("connections_open").expect("connections_open gauge is published");
    let accepted =
        get("connections_accepted_total").expect("connections_accepted_total is published");
    println!("Gauges: connections_open={open}, connections_accepted_total={accepted}");
    assert!(open >= 1, "at least this test's connections are open");
    assert!(
        accepted >= open,
        "accepted total ({accepted}) >= open ({open})"
    );

    drop(w);
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// `kafka.consumer_lag` reports log-end offset minus committed offset per group/partition.
pub async fn test_consumer_lag_view() -> TestResult {
    println!("=== Test: Consumer Lag View ===\n");

    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("metrics-lag").await;
    let group = ctx.unique_group("metrics-lag").await;

    println!("Step 1: produce 3 records to partition 0...");
    let producer = create_producer()?;
    for i in 0..3 {
        producer
            .send(
                FutureRecord::<(), _>::to(&topic)
                    .payload(&format!("lag-{i}"))
                    .partition(0),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(e, _)| e)?;
    }

    println!("Step 2: commit offset 1 for the group...");
    let consumer = create_base_consumer(&group)?;
    let mut offsets = TopicPartitionList::new();
    offsets.add_partition_offset(&topic, 0, rdkafka::Offset::Offset(1))?;
    consumer.commit(&offsets, CommitMode::Sync)?;

    let row = ctx
        .db()
        .query_one(
            "SELECT committed_offset, high_watermark, lag FROM kafka.consumer_lag
             WHERE group_id = $1 AND topic = $2 AND partition_id = 0",
            &[&group, &topic],
        )
        .await?;
    let (committed, hwm, lag): (i64, i64, Option<i64>) = (row.get(0), row.get(1), row.get(2));
    println!("committed={committed}, high_watermark={hwm}, lag={lag:?}");
    assert_eq!(committed, 1);
    assert_eq!(hwm, 3);
    assert_eq!(lag, Some(2));

    drop(consumer);
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
