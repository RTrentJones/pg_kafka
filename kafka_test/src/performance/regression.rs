//! Performance regression tests
//!
//! Tests for detecting performance regressions including:
//! - Large batch throughput
//! - Latency percentiles
//! - Concurrent connection scaling
//! - Resource usage during idle

use crate::common::{create_producer, create_stream_consumer, get_bootstrap_servers, TestResult};
use crate::fixtures::{generate_messages, TestTopicBuilder};
use crate::setup::TestContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::{Duration, Instant};

// The floors and ceilings of these tests. Like `throughput.rs`'s, they sit some 10-100x
// from what a dev-class machine measures (in brackets), so environment variance passes and
// an order-of-magnitude regression fails the suite.

/// Produce throughput of 1000 concurrent sends, the producer's first metadata request
/// included (1,000 to 50,000 msg/s, depending on how long that request takes).
const MIN_LARGE_BATCH_PRODUCE_THROUGHPUT: f64 = 100.0;
/// Consume throughput of those 1000 records, the group join included (900 to 6,000 msg/s).
const MIN_LARGE_BATCH_CONSUME_THROUGHPUT: f64 = 100.0;
/// Median latency of an awaited single-record produce (some 6 ms).
const MAX_PRODUCE_P50: Duration = Duration::from_secs(1);
/// 99th-percentile latency of an awaited single-record produce (8 to 40 ms).
const MAX_PRODUCE_P99: Duration = Duration::from_secs(5);
/// How long ten producers on their own connections may take to each send one record (the
/// connections' setup included; some 1 s).
const MAX_CONCURRENT_SENDS: Duration = Duration::from_secs(10);
/// The listener process's CPU time while a consumer long-polls an empty topic for 3 s
/// (some 0.02 s; a fetch loop that spins instead of waiting burns the whole 3 s).
const MAX_IDLE_LONG_POLL_CPU: Duration = Duration::from_secs(1);

/// Test large batch throughput (target: process 1000 messages efficiently)
///
/// This test measures the system's ability to handle large batches
/// and ensures throughput remains acceptable.
pub async fn test_large_batch_throughput() -> TestResult {
    println!("=== Test: Large Batch Throughput ===\n");

    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "large-batch-throughput")
        .build()
        .await?;

    let producer = create_producer()?;
    let batch_size = 1000;

    // 1. Measure produce throughput for large batch
    println!("Step 1: Producing {batch_size} messages...");
    let produce_start = Instant::now();

    // Use concurrent sends for better throughput
    let mut handles = Vec::new();
    for i in 0..batch_size {
        let topic_name = topic.name.clone();
        let prod = producer.clone();
        let handle = tokio::spawn(async move {
            let value = format!("large-batch-msg-{i}");
            let key = format!("key-{i}");
            prod.send(
                FutureRecord::to(&topic_name).payload(&value).key(&key),
                Duration::from_secs(30),
            )
            .await
            .is_ok()
        });
        handles.push(handle);
    }

    let mut success_count = 0;
    for handle in handles {
        if let Ok(true) = handle.await {
            success_count += 1;
        }
    }

    let produce_elapsed = produce_start.elapsed();
    let produce_throughput = f64::from(success_count) / produce_elapsed.as_secs_f64();

    println!("   Produced: {success_count} messages");
    println!("   Duration: {:.2}s", produce_elapsed.as_secs_f64());
    println!("   Throughput: {produce_throughput:.2} msgs/sec\n");

    // 2. Consume and measure
    println!("Step 2: Consuming {batch_size} messages...");
    let group = ctx.unique_group("large-batch").await;
    let consumer = create_stream_consumer(&group)?;
    consumer.subscribe(&[&topic.name])?;

    let consume_start = Instant::now();
    let mut consumed = 0;
    let timeout = Duration::from_secs(60);

    while consumed < success_count && consume_start.elapsed() < timeout {
        if let Ok(Ok(_)) = tokio::time::timeout(Duration::from_secs(2), consumer.recv()).await {
            consumed += 1;
        }
    }

    let consume_elapsed = consume_start.elapsed();
    let consume_throughput = f64::from(consumed) / consume_elapsed.as_secs_f64();

    println!("   Consumed: {consumed} messages");
    println!("   Duration: {:.2}s", consume_elapsed.as_secs_f64());
    println!("   Throughput: {consume_throughput:.2} msgs/sec\n");

    // 3. Verify results
    println!("=== Performance Summary ===\n");
    println!("   Produce throughput: {produce_throughput:.2} msgs/sec");
    println!("   Consume throughput: {consume_throughput:.2} msgs/sec");

    assert_eq!(success_count, batch_size, "every record is acknowledged");
    assert_eq!(
        consumed, success_count,
        "every acknowledged record is consumed"
    );
    if produce_throughput < MIN_LARGE_BATCH_PRODUCE_THROUGHPUT {
        return Err(format!(
            "Large batch produce regression: {produce_throughput:.2} msg/s is below the \
             {MIN_LARGE_BATCH_PRODUCE_THROUGHPUT:.0} msg/s floor"
        )
        .into());
    }
    if consume_throughput < MIN_LARGE_BATCH_CONSUME_THROUGHPUT {
        return Err(format!(
            "Large batch consume regression: {consume_throughput:.2} msg/s is below the \
             {MIN_LARGE_BATCH_CONSUME_THROUGHPUT:.0} msg/s floor"
        )
        .into());
    }

    println!("✅ Large batch throughput test PASSED\n");

    ctx.cleanup().await?;
    Ok(())
}

/// Test produce latency percentiles (P50, P95, P99)
///
/// Measures latency distribution to detect tail latency issues.
pub async fn test_produce_latency_percentiles() -> TestResult {
    println!("=== Test: Produce Latency Percentiles ===\n");

    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "latency-percentiles")
        .build()
        .await?;

    let producer = create_producer()?;
    let sample_size = 100;
    let mut latencies: Vec<Duration> = Vec::with_capacity(sample_size);

    // 1. Measure individual message latencies
    println!("Step 1: Measuring latency for {sample_size} messages...");
    for i in 0..sample_size {
        let value = format!("latency-msg-{i}");
        let start = Instant::now();

        let result = producer
            .send(
                FutureRecord::to(&topic.name)
                    .payload(&value)
                    .key(&format!("key-{i}")),
                Duration::from_secs(10),
            )
            .await;

        if result.is_ok() {
            latencies.push(start.elapsed());
        }
    }

    println!("   Measured {} successful messages\n", latencies.len());

    // 2. Calculate percentiles
    println!("Step 2: Calculating percentiles...");
    latencies.sort();

    let p50_idx = latencies.len() / 2;
    let p95_idx = latencies.len() * 95 / 100;
    let p99_idx = latencies.len() * 99 / 100;

    let p50 = latencies.get(p50_idx).copied().unwrap_or_default();
    let p95 = latencies.get(p95_idx).copied().unwrap_or_default();
    let p99 = latencies
        .get(p99_idx.min(latencies.len().saturating_sub(1)))
        .copied()
        .unwrap_or_default();
    let min = latencies.first().copied().unwrap_or_default();
    let max = latencies.last().copied().unwrap_or_default();

    println!("   Min:  {min:?}");
    println!("   P50:  {p50:?}");
    println!("   P95:  {p95:?}");
    println!("   P99:  {p99:?}");
    println!("   Max:  {max:?}\n");

    // 3. Verify latencies are reasonable
    println!("=== Latency Assessment ===\n");

    assert_eq!(latencies.len(), sample_size, "every record is acknowledged");
    if p50 >= MAX_PRODUCE_P50 || p99 >= MAX_PRODUCE_P99 {
        return Err(format!(
            "Produce latency regression: P50 {p50:?} (ceiling {MAX_PRODUCE_P50:?}), \
             P99 {p99:?} (ceiling {MAX_PRODUCE_P99:?})"
        )
        .into());
    }
    println!("   ✓ P50 and P99 under their ceilings ({MAX_PRODUCE_P50:?}, {MAX_PRODUCE_P99:?})");

    println!("\n✅ Test: Produce Latency Percentiles PASSED\n");

    ctx.cleanup().await?;
    Ok(())
}

/// Test memory scaling with concurrent connections
///
/// Creates multiple producer/consumer connections to verify
/// system handles concurrent connections without issues.
pub async fn test_concurrent_connection_scaling() -> TestResult {
    println!("=== Test: Concurrent Connection Scaling ===\n");

    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "connection-scaling")
        .build()
        .await?;

    // Pre-produce some messages
    let messages = generate_messages(50, "scale-test");
    topic.produce(&messages).await?;
    println!("   Pre-produced {} messages\n", messages.len());

    // 1. Create many concurrent producers
    let num_producers = 10;
    println!("Step 1: Creating {num_producers} concurrent producers...");

    let mut producers: Vec<FutureProducer> = Vec::new();
    for _ in 0..num_producers {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", get_bootstrap_servers())
            .set("broker.address.family", "v4")
            .set("message.timeout.ms", "5000")
            .create()?;
        producers.push(producer);
    }
    println!("   ✓ Created {} producers\n", producers.len());

    // 2. Create many concurrent consumers
    let num_consumers = 10;
    println!("Step 2: Creating {num_consumers} concurrent consumers...");

    let mut consumers: Vec<StreamConsumer> = Vec::new();
    for i in 0..num_consumers {
        let group = format!("{}-consumer-{}", ctx.unique_group("scale").await, i);
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", get_bootstrap_servers())
            .set("broker.address.family", "v4")
            .set("group.id", &group)
            .set("auto.offset.reset", "earliest")
            .set("session.timeout.ms", "10000")
            .set("enable.auto.commit", "false")
            .create()?;
        consumers.push(consumer);
    }
    println!("   ✓ Created {} consumers\n", consumers.len());

    // 3. Use connections concurrently
    println!("Step 3: Using connections concurrently...");
    let topic_name = topic.name.clone();

    let sends_started = Instant::now();
    let mut handles = Vec::new();
    for (i, producer) in producers.iter().enumerate() {
        let topic = topic_name.clone();
        let prod = producer.clone();
        let handle = tokio::spawn(async move {
            let value = format!("concurrent-msg-{i}");
            prod.send(
                FutureRecord::to(&topic).payload(&value).key("concurrent"),
                Duration::from_secs(5),
            )
            .await
            .is_ok()
        });
        handles.push(handle);
    }

    let mut success = 0;
    for handle in handles {
        if let Ok(true) = handle.await {
            success += 1;
        }
    }
    let sends_took = sends_started.elapsed();
    println!("   ✓ {success} producers sent messages successfully in {sends_took:?}\n");

    // 4. Verify all connections work
    println!("=== Connection Scaling Summary ===\n");
    println!("   Total producers: {num_producers}");
    println!("   Total consumers: {num_consumers}");
    println!("   Successful sends: {success}");

    assert_eq!(
        success, num_producers,
        "every producer's record is acknowledged"
    );
    if sends_took >= MAX_CONCURRENT_SENDS {
        return Err(format!(
            "Connection scaling regression: {num_producers} concurrent sends took \
             {sends_took:?} (ceiling {MAX_CONCURRENT_SENDS:?})"
        )
        .into());
    }

    println!("\n✅ Test: Concurrent Connection Scaling PASSED\n");

    ctx.cleanup().await?;
    Ok(())
}

/// Test long poll CPU usage during idle
///
/// A consumer long-polls an empty topic (fetch.wait.max.ms 5 s) for 3 s: the listener's
/// CPU time over that time stays under [`MAX_IDLE_LONG_POLL_CPU`], so the broker waits for
/// data instead of re-fetching in a loop.
pub async fn test_long_poll_cpu_efficiency() -> TestResult {
    println!("=== Test: Long Poll CPU Efficiency ===\n");

    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "cpu-efficiency")
        .build()
        .await?;

    let group = ctx.unique_group("cpu-test").await;

    // 1. Create consumer that will long poll
    println!("Step 1: Creating consumer for long poll...");
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", get_bootstrap_servers())
        .set("broker.address.family", "v4")
        .set("group.id", &group)
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "30000")
        .set("enable.auto.commit", "false")
        .set("fetch.wait.max.ms", "5000") // 5 second long poll
        .create()?;

    consumer.subscribe(&[&topic.name])?;
    println!("✅ Consumer created and subscribed\n");

    // Let the consumer join its group and start fetching first.
    let _ = tokio::time::timeout(Duration::from_secs(3), consumer.recv()).await;
    let listener = listener_cpu(&ctx).await?;

    // 2. Long poll an empty topic, measuring the listener's CPU time meanwhile.
    println!("Step 2: Long polling for 3 seconds (empty topic)...");
    let poll_start = Instant::now();
    let poll_duration = Duration::from_secs(3);

    let mut poll_count = 0;
    while poll_start.elapsed() < poll_duration {
        match tokio::time::timeout(Duration::from_millis(500), consumer.recv()).await {
            // A message (unexpected in an empty topic) or a poll error.
            Ok(_) => {}
            Err(_) => poll_count += 1,
        }
    }
    println!("   Poll cycles: {poll_count}");

    // 3. The broker waits for data instead of re-fetching in a loop.
    println!("=== CPU Efficiency Assessment ===\n");
    match listener {
        Some((pid, before)) => {
            let after = listener_cpu_of(pid).ok_or("the listener's CPU time is unreadable")?;
            let used = after.saturating_sub(before);
            println!(
                "   Listener CPU time over {:.2}s of long polling: {used:?}",
                poll_start.elapsed().as_secs_f64()
            );
            if used >= MAX_IDLE_LONG_POLL_CPU {
                return Err(format!(
                    "Long poll CPU regression: the listener used {used:?} of CPU while a \
                     consumer long-polled an empty topic (ceiling {MAX_IDLE_LONG_POLL_CPU:?})"
                )
                .into());
            }
        }
        None => println!("   (the listener's /proc entry is not readable here: CPU not measured)"),
    }
    assert!(poll_count > 0, "Should have at least one poll cycle");

    println!("\n✅ Test: Long Poll CPU Efficiency PASSED\n");

    ctx.cleanup().await?;
    Ok(())
}

/// The listener process and its CPU time so far, or `None` when this process cannot read
/// its `/proc` entry (the database on another host).
async fn listener_cpu(
    ctx: &TestContext,
) -> Result<Option<(i32, Duration)>, Box<dyn std::error::Error>> {
    let pid: i32 = ctx
        .db()
        .query_one(
            "SELECT pid FROM pg_stat_activity WHERE backend_type = 'pg_kafka_listener'",
            &[],
        )
        .await?
        .get(0);
    Ok(listener_cpu_of(pid).map(|cpu| (pid, cpu)))
}

/// The user and system CPU time of process `pid` (`/proc/<pid>/stat`, in clock ticks of
/// 1/100 s, Linux's USER_HZ).
fn listener_cpu_of(pid: i32) -> Option<Duration> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    // The fields after the parenthesised command name, which may contain spaces.
    let rest = &stat[stat.rfind(')')? + 2..];
    let fields: Vec<&str> = rest.split_whitespace().collect();
    // utime and stime are fields 14 and 15 of the line, 12 and 13 after the name.
    let ticks: u64 = fields.get(11)?.parse::<u64>().ok()? + fields.get(12)?.parse::<u64>().ok()?;
    Some(Duration::from_millis(ticks * 10))
}
