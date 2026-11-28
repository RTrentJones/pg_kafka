//! Throughput and performance baseline tests
//!
//! These tests establish performance baselines and detect regressions.

use crate::common::to_f64;
use crate::common::{create_producer, create_stream_consumer, TestResult};
use crate::fixtures::{generate_messages, TestTopicBuilder};
use crate::setup::TestContext;
use rdkafka::consumer::Consumer;
use rdkafka::producer::FutureRecord;
use std::time::{Duration, Instant};

/// Minimum expected produce throughput (messages per second).
/// DR-18 (DEEP-REVIEW-2026-07): thresholds are now real regression floors instead
/// of 5 msg/s warn-only decoration. Calibrated against a dev-class machine at
/// ~150 msg/s for the awaited-per-send produce loop and ~2,300 msg/s consume;
/// the floors sit ~5-10x below measured so environment variance passes but an
/// order-of-magnitude regression (the kind an accidental O(n) or extra
/// round-trip introduces) fails the suite.
const MIN_PRODUCE_THROUGHPUT: f64 = 25.0;

/// Minimum expected consume throughput (messages per second). See above.
const MIN_CONSUME_THROUGHPUT: f64 = 200.0;

/// Test produce throughput baseline
pub async fn test_produce_throughput_baseline() -> TestResult {
    println!("=== Test: Produce Throughput Baseline ===\n");

    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "throughput-produce")
        .build()
        .await?;

    let producer = create_producer()?;
    let message_count = 500;

    println!("   Producing {message_count} messages...");
    let start = Instant::now();

    let mut success_count = 0;
    for i in 0..message_count {
        let value = format!("throughput-message-{i}");
        let result = producer
            .send(
                FutureRecord::to(&topic.name)
                    .payload(&value)
                    .key(&format!("key-{i}")),
                Duration::from_secs(10),
            )
            .await;

        if result.is_ok() {
            success_count += 1;
        }
    }

    let elapsed = start.elapsed();
    let throughput = f64::from(success_count) / elapsed.as_secs_f64();

    println!("   Produced: {success_count} messages");
    println!("   Duration: {:.2}s", elapsed.as_secs_f64());
    println!("   Throughput: {throughput:.2} msgs/sec");

    assert!(
        success_count == message_count,
        "All messages should be produced successfully"
    );

    // A miss fails the test (DR-18), it does not just print a warning.
    if throughput < MIN_PRODUCE_THROUGHPUT {
        return Err(format!(
            "Produce throughput regression: {throughput:.2} msg/s is below the {MIN_PRODUCE_THROUGHPUT:.0} msg/s floor"
        )
        .into());
    }
    println!("   Throughput meets baseline ({MIN_PRODUCE_THROUGHPUT:.0} msgs/sec)");

    ctx.cleanup().await?;
    println!("\n   Produce throughput baseline test PASSED\n");
    Ok(())
}

/// Test consume throughput baseline
pub async fn test_consume_throughput_baseline() -> TestResult {
    println!("=== Test: Consume Throughput Baseline ===\n");

    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "throughput-consume")
        .build()
        .await?;

    // First produce messages
    let messages = generate_messages(500, "consume-throughput");
    topic.produce(&messages).await?;
    println!("   Pre-produced {} messages", messages.len());

    // Now consume and measure
    let group_id = ctx.unique_group("throughput-group").await;
    let consumer = create_stream_consumer(&group_id)?;
    consumer.subscribe(&[&topic.name])?;

    let message_count = messages.len();
    let mut consumed = 0;

    println!("   Consuming {message_count} messages...");
    let start = Instant::now();
    let timeout = Duration::from_secs(30);

    while consumed < message_count && start.elapsed() < timeout {
        if let Ok(Ok(_)) = tokio::time::timeout(Duration::from_secs(1), consumer.recv()).await {
            consumed += 1;
        }
    }

    let elapsed = start.elapsed();
    let throughput = to_f64(consumed) / elapsed.as_secs_f64();

    println!("   Consumed: {consumed} messages");
    println!("   Duration: {:.2}s", elapsed.as_secs_f64());
    println!("   Throughput: {throughput:.2} msgs/sec");

    assert!(
        consumed >= message_count * 80 / 100,
        "Should consume at least 80% of messages"
    );

    // A miss fails the test (DR-18), it does not just print a warning.
    if throughput < MIN_CONSUME_THROUGHPUT {
        return Err(format!(
            "Consume throughput regression: {throughput:.2} msg/s is below the {MIN_CONSUME_THROUGHPUT:.0} msg/s floor"
        )
        .into());
    }
    println!("   Throughput meets baseline ({MIN_CONSUME_THROUGHPUT:.0} msgs/sec)");

    ctx.cleanup().await?;
    println!("\n   Consume throughput baseline test PASSED\n");
    Ok(())
}

/// Test batch vs single message performance comparison
pub async fn test_batch_vs_single_performance() -> TestResult {
    println!("=== Test: Batch vs Single Performance ===\n");

    let ctx = TestContext::new().await?;

    // Test 1: Single message production
    let single_topic = TestTopicBuilder::new(&ctx, "perf-single").build().await?;

    let producer = create_producer()?;
    let message_count = 100;

    println!("   Single-message mode ({message_count} messages)...");
    let start_single = Instant::now();

    for i in 0..message_count {
        let value = format!("single-{i}");
        producer
            .send(
                FutureRecord::to(&single_topic.name)
                    .payload(&value)
                    .key(&format!("key-{i}")),
                Duration::from_secs(10),
            )
            .await
            .map_err(|(e, _)| e)?;
    }

    let single_elapsed = start_single.elapsed();
    let single_throughput = f64::from(message_count) / single_elapsed.as_secs_f64();
    println!(
        "   Single: {:.2}s ({:.2} msgs/sec)",
        single_elapsed.as_secs_f64(),
        single_throughput
    );

    // Test 2: Batch production (using concurrent spawned tasks)
    let batch_topic = TestTopicBuilder::new(&ctx, "perf-batch").build().await?;
    let batch_topic_name = batch_topic.name.clone();

    println!("   Batch mode ({message_count} messages)...");
    let start_batch = Instant::now();

    // Spawn concurrent tasks for true parallelism
    let mut handles = Vec::new();
    for i in 0..message_count {
        let topic_name = batch_topic_name.clone();
        let producer_clone = producer.clone();
        let handle = tokio::spawn(async move {
            let value = format!("batch-{i}");
            let key = format!("key-{i}");
            producer_clone
                .send(
                    FutureRecord::to(&topic_name).payload(&value).key(&key),
                    Duration::from_secs(10),
                )
                .await
                .is_ok()
        });
        handles.push(handle);
    }

    // Wait for all tasks and count successes
    let mut batch_success = 0;
    for handle in handles {
        if let Ok(true) = handle.await {
            batch_success += 1;
        }
    }

    let batch_elapsed = start_batch.elapsed();
    let batch_throughput = f64::from(batch_success) / batch_elapsed.as_secs_f64();
    println!(
        "   Batch:  {:.2}s ({:.2} msgs/sec, {} succeeded)",
        batch_elapsed.as_secs_f64(),
        batch_throughput,
        batch_success
    );

    assert_eq!(
        batch_success, message_count,
        "every batch-mode record is acknowledged"
    );

    // Calculate speedup
    let speedup = batch_throughput / single_throughput;
    println!("   Batch speedup: {speedup:.2}x");

    // DR-18: batch must not be grossly slower than sequential sends — a real
    // inversion here means pipelining/parallel dispatch broke. The 0.5x floor
    // (vs the informational 0.9x) leaves room for scheduler noise while still
    // failing on a genuine serialization regression.
    if speedup < 0.5 {
        return Err(format!(
            "Batch produce regression: {speedup:.2}x vs single (floor 0.5x) — parallel dispatch appears serialized"
        )
        .into());
    }
    println!("   Batch performance is acceptable ({speedup:.2}x)");

    ctx.cleanup().await?;
    println!("\n   Batch vs single performance test PASSED\n");
    Ok(())
}
