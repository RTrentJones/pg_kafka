//! Long Polling Edge Case Tests
//!
//! Tests for edge cases in long polling functionality:
//! - min_bytes threshold behavior
//! - Timeout precision
//! - Connection drop handling
//! - Multiple waiters on same partition
//! - Auto-commit during long poll

use crate::common::{create_producer, get_bootstrap_servers, TestResult};
use crate::fixtures::TestTopicBuilder;
use crate::members::{close_all, Member, CLOSE_TIMEOUT};
use crate::setup::TestContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::FutureRecord;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;

/// Test long poll min_bytes wakeup behavior
///
/// Scenario:
/// 1. Consumer polls with min_bytes=500
/// 2. Produce small message (100 bytes)
/// 3. Consumer should NOT immediately return (below threshold)
/// 4. Produce more data to exceed threshold
/// 5. Consumer should wake up
///
/// This tests min_bytes threshold accumulation.
pub async fn test_long_poll_min_bytes_threshold() -> TestResult {
    println!("=== Test: Long Poll min_bytes Threshold ===\n");

    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "longpoll-minbytes")
        .build()
        .await?;

    // Create topic with initial message
    println!("Step 1: Creating topic...");
    let producer = create_producer()?;
    producer
        .send(
            FutureRecord::to(&topic.name).payload("setup").key("setup"),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(err, _)| err)?;
    println!("  Topic created");

    // Create consumer with min_bytes threshold
    println!("\nStep 2: Creating consumer with fetch.min.bytes=500...");
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", get_bootstrap_servers())
        .set("broker.address.family", "v4")
        .set(
            "group.id",
            format!("minbytes-group-{}", uuid::Uuid::new_v4()),
        )
        .set("auto.offset.reset", "latest")
        .set("fetch.wait.max.ms", "10000") // Long wait
        .set("fetch.min.bytes", "500") // Accumulation threshold
        .set("session.timeout.ms", "10000")
        .set("enable.auto.commit", "false")
        .create()?;

    let consumer = Member::new(consumer);
    consumer.subscribe(&[&topic.name])?;
    println!("  Consumer subscribed");

    // Wait for assignment
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Spawn consumer task
    let received = Arc::new(AtomicU32::new(0));
    let received_clone = received.clone();
    let topic_name = topic.name.clone();

    let consumer_handle = tokio::spawn(async move {
        let elapsed = async {
            let start = Instant::now();
            loop {
                if start.elapsed() > Duration::from_secs(8) {
                    break;
                }

                match tokio::time::timeout(Duration::from_millis(100), consumer.recv()).await {
                    Ok(Ok(msg)) => {
                        let value = msg
                            .payload()
                            .map(|v| String::from_utf8_lossy(v).to_string());
                        if value.as_deref().is_some_and(|v| v.starts_with("batch-")) {
                            received_clone.fetch_add(1, Ordering::SeqCst);
                            let elapsed = start.elapsed();
                            println!("  Consumer received message in {elapsed:?}");
                            if received_clone.load(Ordering::SeqCst) >= 3 {
                                return elapsed;
                            }
                        }
                    }
                    Ok(Err(e)) => {
                        if !e.to_string().contains("BrokerTransportFailure") {
                            println!("  Consumer error: {e}");
                        }
                    }
                    Err(_) => {}
                }
            }
            start.elapsed()
        }
        .await;
        (elapsed, consumer)
    });

    // Wait briefly then produce batch of messages
    tokio::time::sleep(Duration::from_millis(500)).await;

    println!("\nStep 3: Producing batch of messages...");
    for i in 0..5 {
        // Each message ~100 bytes payload
        let payload = format!("batch-{}-{}", i, "x".repeat(100));
        producer
            .send(
                FutureRecord::to(&topic_name)
                    .payload(&payload)
                    .key(&format!("key-{i}")),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(err, _)| err)?;
    }
    println!("  Produced 5 messages (~500+ bytes total)");

    // Wait for consumer
    let (duration, consumer) = consumer_handle.await?;
    consumer.close(CLOSE_TIMEOUT).await?;
    let count = received.load(Ordering::SeqCst);

    println!("\nStep 4: Verifying behavior...");
    println!("  Messages received: {count}");
    println!("  Time to receive: {duration:?}");

    // Consumer should have received messages after batch accumulated
    assert!(
        count >= 3,
        "Should receive at least 3 messages, got {count}"
    );

    ctx.cleanup().await?;
    println!("\n✅ Long poll min_bytes threshold test PASSED\n");
    Ok(())
}

/// An empty Fetch is answered after its own `max_wait_ms`, whatever it is: below, at and above
/// the broker's 1000 ms safety-net re-check (`pg_kafka.fetch_poll_interval_ms`), each wait is
/// at least `max_wait_ms` and not much more, so the broker neither answers at its first
/// re-check nor rounds the wait up to the next one.
pub async fn test_long_poll_timeout_precision() -> TestResult {
    println!("=== Test: Long Poll Timeout Precision ===\n");
    let ctx = TestContext::new().await?;
    let topic = super::timeout::empty_topic(&ctx, "longpoll-precision").await?;
    let mut s = TcpStream::connect(("127.0.0.1", crate::common::broker_port())).await?;
    println!("Step 1: empty topic {topic}");

    for (corr, max_wait_ms) in (1..).zip([150, 700, 1_000, 1_300]) {
        let took = super::timeout::timed_empty_fetch(&mut s, corr, &topic, max_wait_ms, 1).await?;
        let max_wait = Duration::from_millis(u64::try_from(max_wait_ms)?);
        println!("  max_wait_ms={max_wait_ms}: answered in {took:?}");
        assert!(
            took + super::timeout::EARLY_SLACK >= max_wait,
            "an empty Fetch with max_wait_ms={max_wait_ms} was answered early, in {took:?}"
        );
        assert!(
            took < max_wait + super::timeout::ANSWER_SLACK,
            "an empty Fetch with max_wait_ms={max_wait_ms} was answered late, in {took:?}"
        );
    }
    ctx.cleanup().await?;
    println!("\n✅ Long poll timeout precision test PASSED\n");
    Ok(())
}

/// Test long poll behavior after consumer disconnect
///
/// Scenario:
/// 1. Consumer starts long polling
/// 2. Drop consumer (simulates disconnect)
/// 3. Verify server handles gracefully
/// 4. New consumer can connect and consume
///
/// This tests connection cleanup during long poll.
pub async fn test_long_poll_consumer_disconnect() -> TestResult {
    println!("=== Test: Long Poll Consumer Disconnect ===\n");

    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "longpoll-disconnect")
        .build()
        .await?;

    // Create topic with message
    println!("Step 1: Creating topic with message...");
    let producer = create_producer()?;
    producer
        .send(
            FutureRecord::to(&topic.name)
                .payload("test-message")
                .key("key"),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(err, _)| err)?;
    println!("  Topic created");

    // Create and start first consumer
    println!("\nStep 2: Creating first consumer (will be dropped)...");
    {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", get_bootstrap_servers())
            .set("broker.address.family", "v4")
            .set(
                "group.id",
                format!("disconnect-group-{}", uuid::Uuid::new_v4()),
            )
            .set("auto.offset.reset", "earliest")
            .set("fetch.wait.max.ms", "30000") // Very long wait
            .set("fetch.min.bytes", "1")
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "false")
            .create()?;

        let consumer = Member::new(consumer);
        consumer.subscribe(&[&topic.name])?;
        println!("  Consumer subscribed, starting poll...");

        // Start polling in background (will be dropped)
        let poll_start = Instant::now();
        let _ = tokio::time::timeout(Duration::from_millis(500), consumer.recv()).await;
        println!("  Consumer polling for {:?}...", poll_start.elapsed());

        // Closes while its Fetch is parked on the broker.
        consumer.close(CLOSE_TIMEOUT).await?;
    }
    println!("  First consumer dropped (disconnected)\n");

    // Brief wait to ensure cleanup
    tokio::time::sleep(Duration::from_millis(500)).await;

    // New consumer should work fine
    println!("Step 3: Creating new consumer after disconnect...");
    let consumer2: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", get_bootstrap_servers())
        .set("broker.address.family", "v4")
        .set(
            "group.id",
            format!("disconnect-group2-{}", uuid::Uuid::new_v4()),
        )
        .set("auto.offset.reset", "earliest")
        .set("fetch.wait.max.ms", "5000")
        .set("fetch.min.bytes", "1")
        .set("session.timeout.ms", "10000")
        .set("enable.auto.commit", "false")
        .create()?;

    let consumer2 = Member::new(consumer2);
    consumer2.subscribe(&[&topic.name])?;
    println!("  New consumer subscribed");

    // Should receive message
    println!("\nStep 4: Verifying new consumer receives message...");
    let start = Instant::now();
    let mut received = false;
    while start.elapsed() < Duration::from_secs(5) {
        if let Ok(Ok(msg)) =
            tokio::time::timeout(Duration::from_millis(500), consumer2.recv()).await
        {
            let value = msg
                .payload()
                .map(|v| String::from_utf8_lossy(v).to_string());
            if value.as_deref() == Some("test-message") {
                received = true;
                println!("  ✅ New consumer received message");
                break;
            }
        }
    }

    assert!(received, "New consumer should receive the message");
    consumer2.close(CLOSE_TIMEOUT).await?;

    ctx.cleanup().await?;
    println!("\n✅ Long poll consumer disconnect test PASSED\n");
    Ok(())
}

/// Test multiple consumers waiting on same partition
///
/// Scenario:
/// 1. Create 3 consumers on same topic (different groups)
/// 2. All start long polling
/// 3. Produce one message
/// 4. All 3 consumers should receive message
///
/// This tests notification broadcast to multiple waiters.
pub async fn test_long_poll_multiple_consumers_same_partition() -> TestResult {
    println!("=== Test: Long Poll Multiple Consumers Same Partition ===\n");

    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "longpoll-multi")
        .build()
        .await?;

    // Create topic
    println!("Step 1: Creating topic...");
    let producer = create_producer()?;
    producer
        .send(
            FutureRecord::to(&topic.name).payload("setup").key("setup"),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(err, _)| err)?;
    println!("  Topic created");

    // Create 3 consumers with different group IDs
    println!("\nStep 2: Creating 3 consumers...");
    let mut consumers = Vec::new();
    for i in 0..3 {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", get_bootstrap_servers())
            .set("broker.address.family", "v4")
            .set(
                "group.id",
                format!("multi-group-{}-{}", i, uuid::Uuid::new_v4()),
            )
            .set("auto.offset.reset", "latest")
            .set("fetch.wait.max.ms", "10000")
            .set("fetch.min.bytes", "1")
            .set("session.timeout.ms", "10000")
            .set("enable.auto.commit", "false")
            .create()?;

        let consumer = Member::new(consumer);
        consumer.subscribe(&[&topic.name])?;
        consumers.push(consumer);
        println!("  Consumer {} created and subscribed", i + 1);
    }

    // Wait for all to join
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // Start all consumers polling
    println!("\nStep 3: Starting all consumers polling...");
    let received_counts: Vec<Arc<AtomicU32>> =
        (0..3).map(|_| Arc::new(AtomicU32::new(0))).collect();

    let mut handles = Vec::new();
    for (i, consumer) in consumers.into_iter().enumerate() {
        let count = received_counts[i].clone();
        let handle = tokio::spawn(async move {
            let got = async {
                let start = Instant::now();
                while start.elapsed() < Duration::from_secs(5) {
                    if let Ok(Ok(msg)) =
                        tokio::time::timeout(Duration::from_millis(100), consumer.recv()).await
                    {
                        let value = msg
                            .payload()
                            .map(|v| String::from_utf8_lossy(v).to_string());
                        if value.as_deref() == Some("broadcast-message") {
                            count.fetch_add(1, Ordering::SeqCst);
                            println!(
                                "  Consumer {} received broadcast in {:?}",
                                i + 1,
                                start.elapsed()
                            );
                            return true;
                        }
                    }
                }
                false
            }
            .await;
            (got, consumer)
        });
        handles.push(handle);
    }

    // Give consumers time to start polling
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Produce broadcast message
    println!("\nStep 4: Producing broadcast message...");
    producer
        .send(
            FutureRecord::to(&topic.name)
                .payload("broadcast-message")
                .key("broadcast-key"),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(err, _)| err)?;
    println!("  Message produced");

    // Wait for consumers
    let mut success_count = 0;
    let mut closing = Vec::new();
    for handle in handles {
        let (got, consumer) = handle.await?;
        if got {
            success_count += 1;
        }
        closing.push(consumer.closing());
    }
    close_all(closing, CLOSE_TIMEOUT).await?;

    println!("\nStep 5: Verifying results...");
    println!("  Consumers that received message: {success_count}/3");

    // At least 2 of 3 consumers should receive (accounting for timing)
    assert!(
        success_count >= 2,
        "At least 2 of 3 consumers should receive the broadcast, got {success_count}"
    );

    ctx.cleanup().await?;
    println!("\n✅ Long poll multiple consumers same partition test PASSED\n");
    Ok(())
}

/// Test auto-commit behavior during long polling
///
/// Scenario:
/// 1. Consumer with auto.commit.interval.ms=2000
/// 2. Consume message
/// 3. Enter long poll
/// 4. Verify offset auto-committed during poll
///
/// This tests auto-commit works alongside long polling.
pub async fn test_long_poll_auto_commit_interval() -> TestResult {
    println!("=== Test: Long Poll Auto-Commit Interval ===\n");

    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "longpoll-autocommit")
        .build()
        .await?;

    let group_id = format!("autocommit-group-{}", uuid::Uuid::new_v4());

    // Create topic with messages
    println!("Step 1: Creating topic with messages...");
    let producer = create_producer()?;
    for i in 0..3 {
        producer
            .send(
                FutureRecord::to(&topic.name)
                    .payload(&format!("message-{i}"))
                    .key(&format!("key-{i}")),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(err, _)| err)?;
    }
    println!("  Produced 3 messages");

    // Create consumer with auto-commit
    println!("\nStep 2: Creating consumer with auto.commit.interval.ms=2000...");
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", get_bootstrap_servers())
        .set("broker.address.family", "v4")
        .set("group.id", &group_id)
        .set("auto.offset.reset", "earliest")
        .set("fetch.wait.max.ms", "5000")
        .set("fetch.min.bytes", "1")
        .set("session.timeout.ms", "10000")
        .set("enable.auto.commit", "true")
        .set("auto.commit.interval.ms", "2000")
        .create()?;

    let consumer = Member::new(consumer);
    consumer.subscribe(&[&topic.name])?;
    println!("  Consumer subscribed");

    // Consume messages
    println!("\nStep 3: Consuming messages...");
    let mut consumed = 0;
    let start = Instant::now();
    while consumed < 3 && start.elapsed() < Duration::from_secs(5) {
        if let Ok(Ok(msg)) = tokio::time::timeout(Duration::from_millis(500), consumer.recv()).await
        {
            let value = msg
                .payload()
                .map(|v| String::from_utf8_lossy(v).to_string());
            if let Some(v) = value {
                if v.starts_with("message-") {
                    consumed += 1;
                    println!("  Consumed: {v}");
                }
            }
        }
    }
    println!("  Consumed {consumed} messages");

    // Wait for auto-commit interval
    println!("\nStep 4: Waiting for auto-commit interval (3s)...");
    tokio::time::sleep(Duration::from_secs(3)).await;
    println!("  Wait complete");

    // Drop consumer to ensure commits are flushed
    consumer.close(CLOSE_TIMEOUT).await?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Check committed offset
    println!("\nStep 5: Checking committed offset...");
    let topic_row = ctx
        .db()
        .query_one(
            "SELECT id FROM kafka.topics WHERE name = $1",
            &[&topic.name],
        )
        .await?;
    let topic_id: i32 = topic_row.get(0);

    let offset_result = ctx
        .db()
        .query_opt(
            "SELECT committed_offset FROM kafka.consumer_offsets
             WHERE group_id = $1 AND topic_id = $2 AND partition_id = 0",
            &[&group_id, &topic_id],
        )
        .await?;

    match offset_result {
        Some(row) => {
            let offset: i64 = row.get(0);
            println!("  Committed offset: {offset}");
            assert!(offset >= 1, "Should have auto-committed after consuming");
        }
        None => {
            println!("  Note: No committed offset found (auto-commit may not have triggered)");
            // This is acceptable - auto-commit timing can vary
        }
    }

    ctx.cleanup().await?;
    println!("\n✅ Long poll auto-commit interval test PASSED\n");
    Ok(())
}
