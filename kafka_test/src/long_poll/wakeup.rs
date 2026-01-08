//! Long polling wakeup tests
//!
//! Tests for producer waking up waiting consumers.

use crate::common::{create_producer, get_bootstrap_servers, TestResult};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::FutureRecord;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;

/// Test that producer wakes up a waiting consumer
///
/// A consumer waiting with max_wait_ms should be woken up
/// when a producer sends a message, without waiting the full timeout.
pub async fn test_long_poll_producer_wakeup() -> TestResult {
    println!("=== Test: Long Poll Producer Wakeup ===\n");

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
                        println!("   ✅ Consumer received wakeup message in {:?}", elapsed);
                        received_clone.store(true, Ordering::SeqCst);
                        return elapsed;
                    }
                }
                Ok(Err(e)) => {
                    println!("   Consumer error: {}", e);
                }
                Err(_) => continue, // Timeout, keep polling
            }
        }
        start.elapsed()
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
    let consumer_duration = consumer_handle.await?;
    let total_duration = consumer_start.elapsed();

    println!("\nStep 5: Verifying timing...");
    println!("   Consumer received message in: {:?}", consumer_duration);
    println!("   Total test duration: {:?}", total_duration);

    // Consumer should have received the message
    assert!(
        received.load(Ordering::SeqCst),
        "Consumer should have received the wakeup message"
    );

    // The consumer should have woken up well before the 30s max_wait_ms
    // With long polling, we expect < 5 seconds after produce
    assert!(
        consumer_duration < Duration::from_secs(5),
        "Consumer should wake up quickly after produce, took {:?}",
        consumer_duration
    );

    println!("\n✅ Long poll producer wakeup test PASSED\n");
    Ok(())
}

/// Test sequential consumers receiving messages via long poll
///
/// This test verifies that sequential produce/consume operations
/// work correctly with long polling enabled.
pub async fn test_long_poll_multiple_waiters() -> TestResult {
    println!("=== Test: Long Poll Sequential Operations ===\n");

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

    consumer.subscribe(&[&topic])?;
    println!("   Consumer subscribed");

    // Wait for consumer setup
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Receive first message (setup)
    println!("\nStep 3: Receiving setup message...");
    let msg = tokio::time::timeout(Duration::from_secs(5), consumer.recv()).await;
    match msg {
        Ok(Ok(m)) => {
            let val = m.payload().map(|v| String::from_utf8_lossy(v).to_string());
            println!("   Received: {:?}", val);
        }
        _ => println!("   No message or error"),
    }

    // Now produce multiple messages and verify consumer receives them
    println!("\nStep 4: Producing 3 messages...");
    for i in 1..=3 {
        producer
            .send(
                FutureRecord::to(&topic)
                    .payload(&format!("message-{}", i))
                    .key(&format!("key-{}", i)),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(err, _)| err)?;
        println!("   Produced message-{}", i);
    }

    // Receive all 3 messages
    println!("\nStep 5: Receiving messages...");
    let mut received = 0;
    let start = Instant::now();
    while received < 3 && start.elapsed() < Duration::from_secs(10) {
        match tokio::time::timeout(Duration::from_secs(2), consumer.recv()).await {
            Ok(Ok(msg)) => {
                let value = msg
                    .payload()
                    .map(|v| String::from_utf8_lossy(v).to_string());
                if let Some(v) = &value {
                    if v.starts_with("message-") {
                        received += 1;
                        println!("   Received: {} (in {:?})", v, start.elapsed());
                    }
                }
            }
            Ok(Err(e)) => println!("   Error: {}", e),
            Err(_) => continue,
        }
    }

    println!("\nStep 6: Verifying results...");
    assert!(
        received >= 2,
        "Should receive at least 2 of 3 messages, got {}",
        received
    );
    println!("   Received {} messages", received);

    println!("\n✅ Long poll sequential operations test PASSED\n");
    Ok(())
}
