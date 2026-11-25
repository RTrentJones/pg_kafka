//! Long polling wakeup tests
//!
//! Tests for producer waking up waiting consumers.

use crate::common::{create_producer, get_bootstrap_servers, TestResult};
use crate::members::{Member, CLOSE_TIMEOUT};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::FutureRecord;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;

/// An rdkafka consumer long polling with fetch.wait.max.ms 30000 receives a record
/// produced while it waits, long before that wait ends.
///
/// Asserts: the record produced 0.5 s after the consumer starts waiting arrives within 5 s.
/// The safety-net re-check (`pg_kafka.fetch_poll_interval_ms`, 1 s) alone would deliver it
/// within that bound too: that a Produce wakes a waiting Fetch at once, not at the next
/// re-check, is asserted at the wire level by
/// `error_paths::listener_tests::test_listener_produce_wakes_long_poll_promptly` (and for
/// several waiters by `edge_cases::test_long_poll_multiple_consumers_same_partition`).
pub async fn test_long_poll_consumer_receives_before_max_wait() -> TestResult {
    println!("=== Test: Long Poll Consumer Receives Before max_wait ===\n");

    let topic = format!("longpoll-wakeup-{}", Uuid::new_v4());
    let group_id = format!("longpoll-wakeup-group-{}", Uuid::new_v4());

    // Create topic first
    println!("Step 1: Creating topic...");
    let producer = create_producer()?;
    producer
        .send(
            FutureRecord::to(&topic).payload("setup").key("setup"),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(err, _)| err)?;
    println!("   Topic created with initial message");

    // Create consumer with long max_wait_ms
    println!("\nStep 2: Creating consumer with fetch.wait.max.ms=30000...");
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", get_bootstrap_servers())
        .set("broker.address.family", "v4")
        .set("group.id", &group_id)
        .set("auto.offset.reset", "latest") // Start from end
        .set("fetch.wait.max.ms", "30000") // Wait up to 30 seconds
        .set("fetch.min.bytes", "1") // Accept any data
        .set("session.timeout.ms", "10000")
        .set("enable.auto.commit", "false")
        .create()?;

    let consumer = Member::new(consumer);
    consumer.subscribe(&[&topic])?;
    println!("   Consumer subscribed");

    // Wait for consumer to join and start waiting
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Flag to track if message was received
    let received = Arc::new(AtomicBool::new(false));
    let received_clone = received.clone();
    let consumer_start = Instant::now();

    // Spawn consumer task
    let consumer_handle = tokio::spawn(async move {
        let elapsed = async {
            println!("\nStep 3: Consumer waiting for message...");
            let start = Instant::now();

            // Poll with long timeout (will be interrupted by producer)
            loop {
                if start.elapsed() > Duration::from_secs(10) {
                    println!("   Consumer timeout after 10s");
                    break;
                }

                match tokio::time::timeout(Duration::from_millis(100), consumer.recv()).await {
                    Ok(Ok(msg)) => {
                        let elapsed = start.elapsed();
                        let value = msg
                            .payload()
                            .map(|v| String::from_utf8_lossy(v).to_string());
                        if value.as_deref() == Some("wakeup-message") {
                            println!("   ✅ Consumer received wakeup message in {elapsed:?}");
                            received_clone.store(true, Ordering::SeqCst);
                            return elapsed;
                        }
                    }
                    Ok(Err(e)) => {
                        println!("   Consumer error: {e}");
                    }
                    Err(_) => {} // Timeout, keep polling
                }
            }
            start.elapsed()
        }
        .await;
        (elapsed, consumer)
    });

    // Wait a bit then produce
    tokio::time::sleep(Duration::from_millis(500)).await;
    println!("\nStep 4: Producing wakeup message...");
    producer
        .send(
            FutureRecord::to(&topic)
                .payload("wakeup-message")
                .key("wakeup-key"),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(err, _)| err)?;
    println!("   Message produced");

    // Wait for consumer
    let (consumer_duration, consumer) = consumer_handle.await?;
    consumer.close(CLOSE_TIMEOUT).await?;
    let total_duration = consumer_start.elapsed();

    println!("\nStep 5: Verifying timing...");
    println!("   Consumer received message in: {consumer_duration:?}");
    println!("   Total test duration: {total_duration:?}");

    // Consumer should have received the message
    assert!(
        received.load(Ordering::SeqCst),
        "Consumer should have received the wakeup message"
    );

    // Well before the 30 s fetch.wait.max.ms.
    assert!(
        consumer_duration < Duration::from_secs(5),
        "the record should arrive well before the long poll's max wait, took {consumer_duration:?}"
    );

    println!("\n✅ Long poll consumer receives before max_wait test PASSED\n");
    Ok(())
}

/// One long-polling consumer (fetch.wait.max.ms 5000) reads a topic's first record, then
/// three records produced after it, in order. Several waiters on one partition are
/// `edge_cases::test_long_poll_multiple_consumers_same_partition`.
///
/// Asserts: the consumer receives "setup", then message-1, message-2 and message-3, in
/// that order, within 10 s.
pub async fn test_long_poll_sequential_produce_consume() -> TestResult {
    println!("=== Test: Long Poll Sequential Produce and Consume ===\n");

    let topic = format!("longpoll-seq-{}", Uuid::new_v4());
    let group_id = format!("longpoll-seq-group-{}", Uuid::new_v4());

    // Create topic first
    println!("Step 1: Creating topic...");
    let producer = create_producer()?;
    producer
        .send(
            FutureRecord::to(&topic).payload("setup").key("setup"),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(err, _)| err)?;
    println!("   Topic created");

    // Create consumer
    println!("\nStep 2: Creating consumer...");
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", get_bootstrap_servers())
        .set("broker.address.family", "v4")
        .set("group.id", &group_id)
        .set("auto.offset.reset", "earliest")
        .set("fetch.wait.max.ms", "5000")
        .set("fetch.min.bytes", "1")
        .set("session.timeout.ms", "10000")
        .set("enable.auto.commit", "false")
        .create()?;

    let consumer = Member::new(consumer);
    consumer.subscribe(&[&topic])?;
    println!("   Consumer subscribed");

    // Wait for consumer setup
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Receive first message (setup)
    println!("\nStep 3: Receiving setup message...");
    let msg = tokio::time::timeout(Duration::from_secs(5), consumer.recv()).await;
    let setup = match msg {
        Ok(Ok(m)) => m.payload().map(|v| String::from_utf8_lossy(v).to_string()),
        _ => None,
    };
    println!("   Received: {setup:?}");
    assert_eq!(setup.as_deref(), Some("setup"), "the topic's first record");

    // Now produce multiple messages and verify consumer receives them
    println!("\nStep 4: Producing 3 messages...");
    for i in 1..=3 {
        producer
            .send(
                FutureRecord::to(&topic)
                    .payload(&format!("message-{i}"))
                    .key(&format!("key-{i}")),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(err, _)| err)?;
        println!("   Produced message-{i}");
    }

    // Receive all 3 messages
    println!("\nStep 5: Receiving messages...");
    let mut received = Vec::new();
    let start = Instant::now();
    while received.len() < 3 && start.elapsed() < Duration::from_secs(10) {
        match tokio::time::timeout(Duration::from_secs(2), consumer.recv()).await {
            Ok(Ok(msg)) => {
                let value = msg
                    .payload()
                    .map(|v| String::from_utf8_lossy(v).to_string());
                if let Some(v) = value {
                    println!("   Received: {} (in {:?})", v, start.elapsed());
                    received.push(v);
                }
            }
            Ok(Err(e)) => println!("   Error: {e}"),
            Err(_) => {}
        }
    }

    println!("\nStep 6: Verifying results...");
    println!("   Received {received:?}");
    consumer.close(CLOSE_TIMEOUT).await?;
    assert_eq!(
        received,
        ["message-1", "message-2", "message-3"],
        "every record produced, in order"
    );

    println!("\n✅ Long poll sequential operations test PASSED\n");
    Ok(())
}
