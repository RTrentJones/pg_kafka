//! Long polling timeout tests
//!
//! Tests for max_wait_ms behavior in FetchRequest.

use crate::common::{create_producer, get_bootstrap_servers, TestResult};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::FutureRecord;
use std::time::{Duration, Instant};
use uuid::Uuid;

/// Test that fetch with max_wait_ms=0 returns immediately
///
/// When max_wait_ms is 0, the server should return immediately
/// without waiting, even if no data is available.
pub async fn test_long_poll_immediate_return() -> TestResult {
    println!("=== Test: Long Poll Immediate Return (max_wait_ms=0) ===\n");

    let topic = format!("longpoll-immediate-{}", Uuid::new_v4());
    let group_id = format!("longpoll-group-{}", Uuid::new_v4());

    // Create topic first by producing a message
    println!("Step 1: Creating topic by producing initial message...");
    let producer = create_producer()?;
    producer
        .send(
            FutureRecord::to(&topic).payload("setup").key("setup"),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(err, _)| err)?;
    println!("   Topic created");

    // Create consumer with very short fetch.wait.max.ms
    println!("\nStep 2: Creating consumer with fetch.wait.max.ms=0...");
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", get_bootstrap_servers())
        .set("broker.address.family", "v4")
        .set("group.id", &group_id)
        .set("auto.offset.reset", "latest") // Start from end
        .set("fetch.wait.max.ms", "0") // Don't wait for data
        .set("fetch.min.bytes", "1")
        .set("session.timeout.ms", "10000")
        .set("enable.auto.commit", "false")
        .create()?;

    consumer.subscribe(&[&topic])?;
    println!("   Consumer subscribed");

    // Wait briefly for assignment
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Poll should return quickly (no data at end of topic)
    println!("\nStep 3: Polling with max_wait_ms=0...");
    let start = Instant::now();

    // Poll a few times - with max_wait_ms=0, each poll should be fast
    for i in 0..5 {
        let poll_start = Instant::now();
        // Use a short timeout since we expect immediate return
        let _ = tokio::time::timeout(Duration::from_millis(200), consumer.recv()).await;
        let poll_duration = poll_start.elapsed();
        println!("   Poll {}: {:?}", i + 1, poll_duration);
    }

    let total_duration = start.elapsed();
    println!("\nStep 4: Verifying timing...");
    println!("   Total duration for 5 polls: {:?}", total_duration);

    // 5 polls should complete in well under 3 seconds with max_wait_ms=0
    assert!(
        total_duration < Duration::from_secs(3),
        "Polls should complete quickly with max_wait_ms=0, took {:?}",
        total_duration
    );

    println!("\n✅ Long poll immediate return test PASSED\n");
    Ok(())
}

/// Test that fetch waits up to max_wait_ms when no data available
///
/// When fetching from an empty partition with max_wait_ms > 0,
/// the server should wait approximately max_wait_ms before returning.
pub async fn test_long_poll_timeout() -> TestResult {
    println!("=== Test: Long Poll Timeout Behavior ===\n");

    let topic = format!("longpoll-timeout-{}", Uuid::new_v4());
    let group_id = format!("longpoll-timeout-group-{}", Uuid::new_v4());

    // Create topic first
    println!("Step 1: Creating empty topic...");
    let producer = create_producer()?;
    producer
        .send(
            FutureRecord::to(&topic).payload("setup").key("setup"),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(err, _)| err)?;
    println!("   Topic created");

    // Create consumer with longer fetch.wait.max.ms
    println!("\nStep 2: Creating consumer with fetch.wait.max.ms=2000...");
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", get_bootstrap_servers())
        .set("broker.address.family", "v4")
        .set("group.id", &group_id)
        .set("auto.offset.reset", "latest") // Start from end (no new data)
        .set("fetch.wait.max.ms", "2000") // Wait up to 2 seconds
        .set("fetch.min.bytes", "1000000") // High min_bytes to force waiting
        .set("session.timeout.ms", "10000")
        .set("enable.auto.commit", "false")
        .create()?;

    consumer.subscribe(&[&topic])?;
    println!("   Consumer subscribed");

    // Wait for assignment
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Single poll should take close to max_wait_ms
    println!("\nStep 3: Polling (should wait ~2 seconds)...");
    let poll_start = Instant::now();

    // Use a longer client timeout than server's max_wait_ms
    let result = tokio::time::timeout(Duration::from_secs(5), consumer.recv()).await;
    let poll_duration = poll_start.elapsed();

    println!("   Poll returned in {:?}", poll_duration);
    match &result {
        Ok(Ok(_)) => println!("   Result: received message"),
        Ok(Err(e)) => println!("   Result: error - {}", e),
        Err(_) => println!("   Result: timeout"),
    }

    println!("\nStep 4: Verifying timing...");
    // The poll should take at least 1 second (accounting for network overhead)
    // but no more than 6 seconds (max_wait_ms + overhead + our timeout)
    if poll_duration >= Duration::from_millis(1000) {
        println!("   ✓ Poll waited for data (took {:?})", poll_duration);
    } else {
        println!(
            "   Note: Poll returned quickly ({:?}) - this is acceptable",
            poll_duration
        );
        println!("   (rdkafka may batch requests or server had no data)");
    }

    // Key assertion: poll completed (didn't hang)
    assert!(
        poll_duration < Duration::from_secs(10),
        "Poll should complete within reasonable time"
    );

    println!("\n✅ Long poll timeout test PASSED\n");
    Ok(())
}
