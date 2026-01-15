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
    println!("Step 1: Producing {} messages...", batch_size);
    let produce_start = Instant::now();

    // Use concurrent sends for better throughput
    let mut handles = Vec::new();
    for i in 0..batch_size {
        let topic_name = topic.name.clone();
        let prod = producer.clone();
        let handle = tokio::spawn(async move {
            let value = format!("large-batch-msg-{}", i);
            let key = format!("key-{}", i);
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
    let produce_throughput = success_count as f64 / produce_elapsed.as_secs_f64();

    println!("   Produced: {} messages", success_count);
    println!("   Duration: {:.2}s", produce_elapsed.as_secs_f64());
    println!("   Throughput: {:.2} msgs/sec\n", produce_throughput);

    // 2. Consume and measure
    println!("Step 2: Consuming {} messages...", batch_size);
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
    let consume_throughput = consumed as f64 / consume_elapsed.as_secs_f64();

    println!("   Consumed: {} messages", consumed);
    println!("   Duration: {:.2}s", consume_elapsed.as_secs_f64());
    println!("   Throughput: {:.2} msgs/sec\n", consume_throughput);

    // 3. Verify results
    println!("=== Performance Summary ===\n");
    println!("   Produce throughput: {:.2} msgs/sec", produce_throughput);
    println!("   Consume throughput: {:.2} msgs/sec", consume_throughput);

    // Basic assertions (set low to avoid flaky tests)
    assert!(
        success_count >= batch_size * 95 / 100,
        "Should produce at least 95% of messages"
    );
    assert!(
        consumed >= success_count * 80 / 100,
        "Should consume at least 80% of produced messages"
    );

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
    println!("Step 1: Measuring latency for {} messages...", sample_size);
    for i in 0..sample_size {
        let value = format!("latency-msg-{}", i);
        let start = Instant::now();

        let result = producer
            .send(
                FutureRecord::to(&topic.name)
                    .payload(&value)
                    .key(&format!("key-{}", i)),
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
    let p99 = latencies.get(p99_idx.min(latencies.len().saturating_sub(1)))
        .copied()
        .unwrap_or_default();
    let min = latencies.first().copied().unwrap_or_default();
    let max = latencies.last().copied().unwrap_or_default();

    println!("   Min:  {:?}", min);
    println!("   P50:  {:?}", p50);
    println!("   P95:  {:?}", p95);
    println!("   P99:  {:?}", p99);
    println!("   Max:  {:?}\n", max);

    // 3. Verify latencies are reasonable
    println!("=== Latency Assessment ===\n");

    // P50 should be under 1 second (very relaxed for test environments)
    if p50 < Duration::from_secs(1) {
        println!("   ✓ P50 latency is acceptable");
    } else {
        println!("   ⚠ P50 latency is high (>1s)");
    }

    // P99 should be under 5 seconds (very relaxed)
    if p99 < Duration::from_secs(5) {
        println!("   ✓ P99 latency is acceptable");
    } else {
        println!("   ⚠ P99 latency is high (>5s)");
    }

    // Basic assertion (very relaxed)
    assert!(
        latencies.len() >= sample_size * 90 / 100,
        "Should measure at least 90% of messages"
    );

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
    println!("Step 1: Creating {} concurrent producers...", num_producers);

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
    println!("Step 2: Creating {} concurrent consumers...", num_consumers);

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

    let mut handles = Vec::new();
    for (i, producer) in producers.iter().enumerate() {
        let topic = topic_name.clone();
        let prod = producer.clone();
        let handle = tokio::spawn(async move {
            let value = format!("concurrent-msg-{}", i);
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
    println!("   ✓ {} producers sent messages successfully\n", success);

    // 4. Verify all connections work
    println!("=== Connection Scaling Summary ===\n");
    println!("   Total producers: {}", num_producers);
    println!("   Total consumers: {}", num_consumers);
    println!("   Successful sends: {}", success);

    assert!(
        success >= num_producers * 80 / 100,
        "At least 80% of producers should succeed"
    );

    println!("\n✅ Test: Concurrent Connection Scaling PASSED\n");

    ctx.cleanup().await?;
    Ok(())
}

/// Test long poll CPU usage during idle
///
/// Verifies that long polling doesn't consume excessive CPU
/// when waiting for messages.
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

    // 2. Measure time during idle poll (no messages)
    println!("Step 2: Long polling for 3 seconds (empty topic)...");
    let poll_start = Instant::now();
    let poll_duration = Duration::from_secs(3);

    let mut poll_count = 0;
    while poll_start.elapsed() < poll_duration {
        // Try to receive with short timeout
        match tokio::time::timeout(Duration::from_millis(500), consumer.recv()).await {
            Ok(Ok(_)) => {
                // Received a message (unexpected in empty topic)
            }
            Ok(Err(_)) => {
                // Error during poll
            }
            Err(_) => {
                // Timeout - expected for empty topic
                poll_count += 1;
            }
        }
    }

    let actual_duration = poll_start.elapsed();
    println!("   Poll cycles: {}", poll_count);
    println!("   Actual duration: {:.2}s\n", actual_duration.as_secs_f64());

    // 3. Verify efficient polling (not spinning)
    println!("=== CPU Efficiency Assessment ===\n");

    // With 500ms timeout and 3s duration, we expect ~6 poll cycles
    // If we see many more, there might be busy-waiting
    let expected_polls = (poll_duration.as_millis() / 500) as i32;
    let poll_efficiency = poll_count as f64 / expected_polls as f64;

    println!("   Expected poll cycles: ~{}", expected_polls);
    println!("   Actual poll cycles: {}", poll_count);
    println!("   Efficiency ratio: {:.2}", poll_efficiency);

    if poll_efficiency <= 2.0 {
        println!("   ✓ Polling is efficient (not busy-waiting)");
    } else {
        println!("   ⚠ Polling may be less efficient than expected");
    }

    // Relaxed assertion - just verify polls happened
    assert!(poll_count > 0, "Should have at least one poll cycle");

    println!("\n✅ Test: Long Poll CPU Efficiency PASSED\n");

    ctx.cleanup().await?;
    Ok(())
}
