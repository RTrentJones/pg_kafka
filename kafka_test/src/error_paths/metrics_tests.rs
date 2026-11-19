//! E2E tests for the broker request metrics.
//!
//! The worker counts every request per API in memory and publishes the counters to
//! `kafka.broker_api_stats` / `kafka.broker_gauges` (views `kafka.api_metrics`,
//! `kafka.broker_metrics`) at most once per second.

use crate::common::{create_base_consumer, create_producer, TestResult, POLL_TIMEOUT};
use crate::setup::TestContext;
use rdkafka::consumer::{CommitMode, Consumer};
use rdkafka::producer::FutureRecord;
use rdkafka::TopicPartitionList;
use std::time::{Duration, Instant};

async fn api_requests(ctx: &TestContext, api: &str) -> Result<i64, Box<dyn std::error::Error>> {
    let row = ctx
        .db()
        .query_opt(
            "SELECT requests FROM kafka.api_metrics WHERE api_name = $1",
            &[&api],
        )
        .await?;
    Ok(row.map(|r| r.get::<_, i64>(0)).unwrap_or(0))
}

/// After a produce and a fetch, `kafka.api_metrics` shows the Produce and Fetch request
/// counters increased, and `kafka.broker_metrics` shows the connection gauges.
pub async fn test_metrics_count_produce_and_fetch() -> TestResult {
    println!("=== Test: Broker Metrics Count Produce And Fetch ===\n");

    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("metrics-produce-fetch").await;

    let produce_before = api_requests(&ctx, "Produce").await?;
    let fetch_before = api_requests(&ctx, "Fetch").await?;
    println!("Before: Produce={}, Fetch={}", produce_before, fetch_before);

    println!("Step 1: produce one record...");
    let producer = create_producer()?;
    let (partition, offset) = producer
        .send(
            FutureRecord::<(), _>::to(&topic).payload("metrics"),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(e, _)| e)?;

    println!("Step 2: fetch it back...");
    let consumer = create_base_consumer(&ctx.unique_group("metrics-fetch").await)?;
    let mut assignment = TopicPartitionList::new();
    assignment.add_partition_offset(&topic, partition, rdkafka::Offset::Offset(offset))?;
    consumer.assign(&assignment)?;
    let start = Instant::now();
    let mut received = false;
    while start.elapsed() < Duration::from_secs(15) {
        if let Some(Ok(_)) = consumer.poll(POLL_TIMEOUT) {
            received = true;
            break;
        }
    }
    assert!(received, "the produced record must be fetched");

    println!("Step 3: wait for the worker's metrics flush (<= 1 s interval)...");
    let start = Instant::now();
    let (mut produce_after, mut fetch_after) = (produce_before, fetch_before);
    while start.elapsed() < Duration::from_secs(15) {
        produce_after = api_requests(&ctx, "Produce").await?;
        fetch_after = api_requests(&ctx, "Fetch").await?;
        if produce_after > produce_before && fetch_after > fetch_before {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    println!("After: Produce={}, Fetch={}", produce_after, fetch_after);
    assert!(
        produce_after > produce_before,
        "Produce request counter must increase ({} -> {})",
        produce_before,
        produce_after
    );
    assert!(
        fetch_after > fetch_before,
        "Fetch request counter must increase ({} -> {})",
        fetch_before,
        fetch_after
    );

    // Latency is recorded alongside the count.
    let row = ctx
        .db()
        .query_one(
            "SELECT latency_us_sum, latency_us_max, errors FROM kafka.api_metrics
             WHERE api_name = 'Produce'",
            &[],
        )
        .await?;
    let (sum, max, errors): (i64, i64, i64) = (row.get(0), row.get(1), row.get(2));
    assert!(
        sum > 0 && max > 0,
        "Produce latency must be recorded (sum={}, max={})",
        sum,
        max
    );
    assert!(
        errors >= 0 && max <= sum,
        "max ({}) cannot exceed sum ({})",
        max,
        sum
    );

    // Gauges are published with every flush; our producer/consumer connections are open.
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
    println!(
        "Gauges: connections_open={}, connections_accepted_total={}",
        open, accepted
    );
    assert!(open >= 1, "at least this test's connections are open");
    assert!(
        accepted >= open,
        "accepted total ({}) >= open ({})",
        accepted,
        open
    );

    drop(consumer);
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
                    .payload(&format!("lag-{}", i))
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
    println!(
        "committed={}, high_watermark={}, lag={:?}",
        committed, hwm, lag
    );
    assert_eq!(committed, 1);
    assert_eq!(hwm, 3);
    assert_eq!(lag, Some(2));

    drop(consumer);
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
