//! Idle long polls cost the DB thread nothing between notifications.
//!
//! A waiting Fetch is re-read only when a partition it watches changes, at its deadline, or
//! within the safety net's shared allowance (`pg_kafka.fetch_poll_interval_ms`), so 64 idle
//! long polls of 1,000 partitions each neither run Fetch attempts on the DB thread while
//! they wait nor slow another client's Produce. A long poll re-read every safety-net interval
//! costs 64,000 partition reads per second, which keeps the DB thread busy and holds every
//! other request behind them.

use crate::common::{create_producer, get_bootstrap_servers, TestResult};
use crate::fixtures::topic_with_partitions;
use crate::setup::TestContext;
use crate::wire::request_frame;
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchRequest, FetchTopic};
use kafka_protocol::messages::{BrokerId, TopicName};
use kafka_protocol::protocol::StrBytes;
use rdkafka::producer::FutureRecord;
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

/// Long polls on one connection (its long-poll cap).
pub(super) const LONG_POLLS: usize = 64;
/// Partitions each long poll names.
pub(super) const PARTITIONS: i32 = 1_000;
/// Long enough that no long poll reaches its deadline while the test measures.
const MAX_WAIT_MS: i32 = 60_000;
/// Produces timed in each phase.
const PRODUCES: usize = 20;
/// How long the idle phase is watched for Fetch attempts.
const WATCH: Duration = Duration::from_secs(3);

/// A Fetch v4 of every partition of `topic` from offset 0, waiting for one byte, at
/// `isolation_level` (0 read_uncommitted, 1 read_committed).
pub(super) fn long_poll_frame(correlation_id: i32, topic: &str, isolation_level: i8) -> Vec<u8> {
    let partitions = (0..PARTITIONS)
        .map(|partition| {
            FetchPartition::default()
                .with_partition(partition)
                .with_fetch_offset(0)
                .with_partition_max_bytes(1024)
        })
        .collect();
    let fetch_topic = FetchTopic::default()
        .with_topic(TopicName(StrBytes::from_string(topic.to_string())))
        .with_partitions(partitions);
    let request = FetchRequest::default()
        .with_replica_id(BrokerId(-1))
        .with_max_wait_ms(MAX_WAIT_MS)
        .with_min_bytes(1)
        .with_max_bytes(1 << 20)
        .with_isolation_level(isolation_level)
        .with_topics(vec![fetch_topic]);
    request_frame(4, correlation_id, &request)
}

/// Fetch requests the worker has executed (`kafka.api_metrics`, published once a second).
pub(super) async fn fetches_executed(ctx: &TestContext) -> Result<i64, Box<dyn std::error::Error>> {
    Ok(ctx
        .db()
        .query_opt(
            "SELECT requests FROM kafka.api_metrics WHERE api_name = 'Fetch'",
            &[],
        )
        .await?
        .map_or(0, |row| row.get(0)))
}

/// The median of `PRODUCES` sequential one-record Produce round trips to `topic`.
pub(super) async fn median_produce_latency(
    producer: &rdkafka::producer::FutureProducer,
    topic: &str,
) -> Result<Duration, Box<dyn std::error::Error>> {
    let mut latencies = Vec::with_capacity(PRODUCES);
    for i in 0..PRODUCES {
        let started = Instant::now();
        producer
            .send(
                FutureRecord::to(topic)
                    .key(&format!("k{i}"))
                    .payload("v")
                    .partition(0),
                Duration::from_secs(30),
            )
            .await
            .map_err(|(e, _)| e)?;
        latencies.push(started.elapsed());
    }
    latencies.sort();
    Ok(latencies[PRODUCES / 2])
}

/// 64 idle long polls of 1,000 partitions run no Fetch attempt while they wait (after their
/// first reads) and leave another client's Produce latency within a small factor of what it
/// is without them. Sequential: it reads the broker's Fetch counter, and other tests' work
/// would move it and the latencies.
pub async fn test_idle_long_polls_leave_the_db_thread_free() -> TestResult {
    println!("=== Test: Idle Long Polls Leave The DB Thread Free ===\n");

    let ctx = TestContext::new().await?;
    let watched = topic_with_partitions(&ctx, "lp-idle-cost", PARTITIONS).await?;
    let other = topic_with_partitions(&ctx, "lp-idle-cost-produce", 1).await?;
    let producer = create_producer()?;
    median_produce_latency(&producer, &other).await?; // warm-up: connection and metadata
    let baseline = median_produce_latency(&producer, &other).await?;
    println!("Step 1: median Produce latency without long polls: {baseline:?}");

    println!("Step 2: {LONG_POLLS} long polls of {PARTITIONS} partitions on one connection...");
    let before = fetches_executed(&ctx).await?;
    let mut stream = TcpStream::connect(get_bootstrap_servers()).await?;
    for i in 0..LONG_POLLS {
        stream
            .write_all(&long_poll_frame(i32::try_from(i)?, &watched, 0))
            .await?;
    }
    stream.flush().await?;

    // Their first reads: one Fetch attempt each, published within a second of running.
    let started = Instant::now();
    let expected = before + i64::try_from(LONG_POLLS)?;
    while fetches_executed(&ctx).await? < expected {
        if started.elapsed() > Duration::from_secs(60) {
            return Err("the long polls' first reads did not run within 60 s".into());
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    // Let the last of them and the counter's publication settle.
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    println!("   first reads done after {:?}", started.elapsed());

    println!("Step 3: watching the idle long polls for {WATCH:?}...");
    let watch_start = fetches_executed(&ctx).await?;
    let watched_since = Instant::now();
    let during = median_produce_latency(&producer, &other).await?;
    let remaining = WATCH.saturating_sub(watched_since.elapsed());
    tokio::time::sleep(remaining).await;
    // The counter is published at most once a second.
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    let attempts = fetches_executed(&ctx).await? - watch_start;
    println!("   median Produce latency while they wait: {during:?}");
    println!(
        "   Fetch attempts while they waited ({:?}): {attempts}",
        watched_since.elapsed()
    );
    drop(stream);

    assert!(
        attempts <= 2,
        "idle long polls ran {attempts} Fetch attempts while nothing changed"
    );
    let bound = (baseline * 3).max(baseline + Duration::from_millis(50));
    assert!(
        during <= bound,
        "idle long polls slowed another client's Produce from {baseline:?} to {during:?} \
         (bound {bound:?})"
    );

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
