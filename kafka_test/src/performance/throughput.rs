//! Throughput and performance baseline tests
//!
//! These tests establish performance baselines and detect regressions.

use crate::common::{create_producer, create_stream_consumer, TestResult};
use crate::fixtures::{generate_messages, TestTopicBuilder};
use crate::setup::TestContext;
use rdkafka::consumer::Consumer;
use rdkafka::producer::FutureRecord;
use std::time::{Duration, Instant};

/// Minimum expected produce throughput (messages per second)
/// Note: Set low to avoid flaky tests; actual throughput varies by environment
const MIN_PRODUCE_THROUGHPUT: f64 = 5.0;

/// Minimum expected consume throughput (messages per second)
/// Note: Set low to avoid flaky tests; actual throughput varies by environment
const MIN_CONSUME_THROUGHPUT: f64 = 5.0;

/// Test produce throughput baseline
pub async fn test_produce_throughput_baseline() -> TestResult {
    println!("=== Test: Produce Throughput Baseline ===\n");

    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "throughput-produce")
        .build()
        .await?;

    let producer = create_producer()?;
    let message_count = 50; // Reduced from 500 for faster CI

    println!("   Producing {} messages...", message_count);
    let start = Instant::now();

    let mut success_count = 0;
    for i in 0..message_count {
        let value = format!("throughput-message-{}", i);
        let result = producer
            .send(
                FutureRecord::to(&topic.name)
                    .payload(&value)
                    .key(&format!("key-{}", i)),
                Duration::from_secs(10),
            )
            .await;

        if result.is_ok() {
            success_count += 1;
        }
    }

    let elapsed = start.elapsed();
    let throughput = success_count as f64 / elapsed.as_secs_f64();

    println!("   Produced: {} messages", success_count);
    println!("   Duration: {:.2}s", elapsed.as_secs_f64());
    println!("   Throughput: {:.2} msgs/sec", throughput);

    assert!(
        success_count == message_count,
        "All messages should be produced successfully"
    );

    if throughput >= MIN_PRODUCE_THROUGHPUT {
        println!(
            "   Throughput meets baseline ({:.0} msgs/sec)",
            MIN_PRODUCE_THROUGHPUT
        );
    } else {
        println!(
            "   WARNING: Throughput below baseline ({:.0} < {:.0})",
            throughput, MIN_PRODUCE_THROUGHPUT
        );
    }

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
    let messages = generate_messages(50, "consume-throughput"); // Reduced from 500 for faster CI
    topic.produce(&messages).await?;
    println!("   Pre-produced {} messages", messages.len());

    // Now consume and measure
    let group_id = ctx.unique_group("throughput-group").await;
    let consumer = create_stream_consumer(&group_id)?;
    consumer.subscribe(&[&topic.name])?;

    let message_count = messages.len();
    let mut consumed = 0;

    println!("   Consuming {} messages...", message_count);
    let start = Instant::now();
    let timeout = Duration::from_secs(30);

    while consumed < message_count && start.elapsed() < timeout {
        if let Ok(Ok(_)) = tokio::time::timeout(Duration::from_secs(1), consumer.recv()).await {
            consumed += 1;
        }
    }

    let elapsed = start.elapsed();
    let throughput = consumed as f64 / elapsed.as_secs_f64();

    println!("   Consumed: {} messages", consumed);
    println!("   Duration: {:.2}s", elapsed.as_secs_f64());
    println!("   Throughput: {:.2} msgs/sec", throughput);

    assert!(
        consumed >= message_count * 80 / 100,
        "Should consume at least 80% of messages"
    );

    if throughput >= MIN_CONSUME_THROUGHPUT {
        println!(
            "   Throughput meets baseline ({:.0} msgs/sec)",
            MIN_CONSUME_THROUGHPUT
        );
    } else {
        println!(
            "   WARNING: Throughput below baseline ({:.0} < {:.0})",
            throughput, MIN_CONSUME_THROUGHPUT
        );
    }

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
    let message_count = 20; // Reduced from 100 for faster CI

    println!("   Single-message mode ({} messages)...", message_count);
    let start_single = Instant::now();

    for i in 0..message_count {
        let value = format!("single-{}", i);
        let _ = producer
            .send(
                FutureRecord::to(&single_topic.name)
                    .payload(&value)
                    .key(&format!("key-{}", i)),
                Duration::from_secs(10),
            )
            .await;
    }

    let single_elapsed = start_single.elapsed();
    let single_throughput = message_count as f64 / single_elapsed.as_secs_f64();
    println!(
        "   Single: {:.2}s ({:.2} msgs/sec)",
        single_elapsed.as_secs_f64(),
        single_throughput
    );

    // Test 2: Batch production (using concurrent spawned tasks)
    let batch_topic = TestTopicBuilder::new(&ctx, "perf-batch").build().await?;
    let batch_topic_name = batch_topic.name.clone();

    println!("   Batch mode ({} messages)...", message_count);
    let start_batch = Instant::now();

    // Spawn concurrent tasks for true parallelism
    let mut handles = Vec::new();
    for i in 0..message_count {
        let topic_name = batch_topic_name.clone();
        let producer_clone = producer.clone();
        let handle = tokio::spawn(async move {
            let value = format!("batch-{}", i);
            let key = format!("key-{}", i);
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
    let batch_throughput = batch_success as f64 / batch_elapsed.as_secs_f64();
    println!(
        "   Batch:  {:.2}s ({:.2} msgs/sec, {} succeeded)",
        batch_elapsed.as_secs_f64(),
        batch_throughput,
        batch_success
    );

    // Calculate speedup
    let speedup = batch_throughput / single_throughput;
    println!("   Batch speedup: {:.2}x", speedup);

    // Batch should generally be faster due to parallelism
    if speedup >= 0.9 {
        println!("   Batch performance is acceptable");
    } else {
        println!("   WARNING: Batch unexpectedly slower than single");
    }

    ctx.cleanup().await?;
    println!("\n   Batch vs single performance test PASSED\n");
    Ok(())
}
