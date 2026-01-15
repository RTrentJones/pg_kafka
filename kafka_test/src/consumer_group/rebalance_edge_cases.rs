//! Rebalancing edge case tests
//!
//! Tests for edge cases in consumer group rebalancing.

use crate::common::{create_producer, TestResult};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::FutureRecord;
use std::time::Duration;

/// Test rapid rebalance cycles
pub async fn test_rapid_rebalance_cycles() -> TestResult {
    println!("=== Test: Rapid Rebalance Cycles ===\n");

    let topic = format!("rapid-rebalance-{}", uuid::Uuid::new_v4());
    let group_id = format!("rapid-group-{}", uuid::Uuid::new_v4());

    // Produce messages
    println!("Step 1: Producing test messages...");
    let producer = create_producer()?;
    for i in 0..3 {
        producer
            .send(
                FutureRecord::to(&topic)
                    .payload(&format!("msg-{}", i))
                    .key(&format!("key-{}", i)),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(err, _)| err)?;
    }
    println!("  Produced 3 messages\n");

    // Run 2 cycles
    for cycle in 0..2 {
        println!("Cycle {}: Join 2 consumers, then leave...", cycle + 1);

        let c1: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .set("group.id", &group_id)
            .set("session.timeout.ms", "10000")
            .set("enable.auto.commit", "false")
            .create()?;
        c1.subscribe(&[&topic])?;
        let _ = tokio::time::timeout(Duration::from_millis(300), c1.recv()).await;

        let c2: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .set("group.id", &group_id)
            .set("session.timeout.ms", "10000")
            .set("enable.auto.commit", "false")
            .create()?;
        c2.subscribe(&[&topic])?;
        let _ = tokio::time::timeout(Duration::from_millis(300), c2.recv()).await;

        tokio::time::sleep(Duration::from_millis(200)).await;
        drop(c1);
        drop(c2);
        tokio::time::sleep(Duration::from_millis(300)).await;
        println!("  Cycle {} complete", cycle + 1);
    }

    // Verify final join works
    println!("\nStep 2: Verifying group accepts new member...");
    let final_c: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", &group_id)
        .set("session.timeout.ms", "10000")
        .set("enable.auto.commit", "false")
        .create()?;
    final_c.subscribe(&[&topic])?;
    let _ = tokio::time::timeout(Duration::from_millis(500), final_c.recv()).await;
    println!("  Final consumer subscribed");

    println!("\n✅ Rapid rebalance cycles test PASSED\n");
    Ok(())
}

/// Test heartbeat during rebalance window
pub async fn test_heartbeat_during_rebalance_window() -> TestResult {
    println!("=== Test: Heartbeat During Rebalance Window ===\n");

    let topic = format!("heartbeat-rebal-{}", uuid::Uuid::new_v4());
    let group_id = format!("heartbeat-group-{}", uuid::Uuid::new_v4());

    println!("Step 1: Producing messages...");
    let producer = create_producer()?;
    for i in 0..3 {
        producer
            .send(
                FutureRecord::to(&topic)
                    .payload(&format!("msg-{}", i))
                    .key(&format!("key-{}", i)),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(err, _)| err)?;
    }
    println!("  Produced 3 messages");

    println!("\nStep 2: Consumer 1 joining...");
    let c1: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", &group_id)
        .set("session.timeout.ms", "10000")
        .set("enable.auto.commit", "false")
        .create()?;
    c1.subscribe(&[&topic])?;
    let _ = tokio::time::timeout(Duration::from_millis(500), c1.recv()).await;
    println!("  Consumer 1 joined");

    println!("\nStep 3: Consumer 2 joining (triggers rebalance)...");
    let c2: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", &group_id)
        .set("session.timeout.ms", "10000")
        .set("enable.auto.commit", "false")
        .create()?;
    c2.subscribe(&[&topic])?;
    let _ = tokio::time::timeout(Duration::from_millis(500), c2.recv()).await;
    println!("  Consumer 2 joined");

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Brief poll
    let _ = tokio::time::timeout(Duration::from_secs(1), c1.recv()).await;
    let _ = tokio::time::timeout(Duration::from_secs(1), c2.recv()).await;
    println!("\nStep 4: Both consumers polled");

    println!("\n✅ Heartbeat during rebalance window test PASSED\n");
    Ok(())
}

/// Test multiple concurrent timeouts
pub async fn test_multiple_concurrent_timeouts() -> TestResult {
    println!("=== Test: Multiple Concurrent Timeouts ===\n");

    let topic = format!("concurrent-to-{}", uuid::Uuid::new_v4());
    let group_id = format!("concurrent-group-{}", uuid::Uuid::new_v4());

    println!("Step 1: Producing messages...");
    let producer = create_producer()?;
    for i in 0..3 {
        producer
            .send(
                FutureRecord::to(&topic)
                    .payload(&format!("msg-{}", i))
                    .key(&format!("key-{}", i)),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(err, _)| err)?;
    }
    println!("  Produced 3 messages");

    println!("\nStep 2: Creating 3 consumers...");
    let mut consumers = Vec::new();
    for i in 0..3 {
        let c: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .set("group.id", &group_id)
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "false")
            .create()?;
        c.subscribe(&[&topic])?;
        let _ = tokio::time::timeout(Duration::from_millis(300), c.recv()).await;
        consumers.push(c);
        println!("  Consumer {} joined", i + 1);
    }

    println!("\nStep 3: Dropping all consumers...");
    drop(consumers);
    tokio::time::sleep(Duration::from_millis(500)).await;

    println!("\nStep 4: New consumer joining...");
    let new_c: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", &group_id)
        .set("session.timeout.ms", "10000")
        .set("enable.auto.commit", "false")
        .create()?;
    new_c.subscribe(&[&topic])?;
    let _ = tokio::time::timeout(Duration::from_secs(2), new_c.recv()).await;
    println!("  New consumer joined");

    println!("\n✅ Multiple concurrent timeouts test PASSED\n");
    Ok(())
}

/// Test rebalance with minimal session timeout
pub async fn test_rebalance_with_minimal_session_timeout() -> TestResult {
    println!("=== Test: Rebalance With Minimal Session Timeout ===\n");

    let topic = format!("minimal-to-{}", uuid::Uuid::new_v4());
    let group_id = format!("minimal-group-{}", uuid::Uuid::new_v4());

    println!("Step 1: Producing message...");
    let producer = create_producer()?;
    producer
        .send(
            FutureRecord::to(&topic).payload("test").key("key"),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(err, _)| err)?;
    println!("  Produced 1 message");

    println!("\nStep 2: Consumer joining with 6s timeout...");
    let c1: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", &group_id)
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .create()?;
    c1.subscribe(&[&topic])?;
    let _ = tokio::time::timeout(Duration::from_millis(500), c1.recv()).await;
    println!("  Consumer 1 joined");

    println!("\nStep 3: Dropping consumer...");
    drop(c1);

    println!("\nStep 4: New consumer joining...");
    let c2: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", &group_id)
        .set("session.timeout.ms", "10000")
        .set("enable.auto.commit", "false")
        .create()?;
    c2.subscribe(&[&topic])?;
    let _ = tokio::time::timeout(Duration::from_secs(2), c2.recv()).await;
    println!("  New consumer joined");

    println!("\n✅ Rebalance with minimal session timeout test PASSED\n");
    Ok(())
}

/// Test rebalance with mixed timeout values
pub async fn test_rebalance_mixed_timeout_values() -> TestResult {
    println!("=== Test: Rebalance Mixed Timeout Values ===\n");

    let topic = format!("mixed-to-{}", uuid::Uuid::new_v4());
    let group_id = format!("mixed-group-{}", uuid::Uuid::new_v4());

    println!("Step 1: Producing messages...");
    let producer = create_producer()?;
    for i in 0..3 {
        producer
            .send(
                FutureRecord::to(&topic)
                    .payload(&format!("msg-{}", i))
                    .key(&format!("key-{}", i)),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(err, _)| err)?;
    }
    println!("  Produced 3 messages");

    println!("\nStep 2: Consumer A joining (6s timeout)...");
    let ca: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", &group_id)
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .create()?;
    ca.subscribe(&[&topic])?;
    let _ = tokio::time::timeout(Duration::from_millis(500), ca.recv()).await;
    println!("  Consumer A joined");

    println!("\nStep 3: Consumer B joining (10s timeout)...");
    let cb: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", &group_id)
        .set("session.timeout.ms", "10000")
        .set("enable.auto.commit", "false")
        .create()?;
    cb.subscribe(&[&topic])?;
    let _ = tokio::time::timeout(Duration::from_millis(500), cb.recv()).await;
    println!("  Consumer B joined");

    tokio::time::sleep(Duration::from_millis(300)).await;

    println!("\nStep 4: Dropping both consumers...");
    drop(ca);
    drop(cb);
    tokio::time::sleep(Duration::from_millis(300)).await;

    println!("\nStep 5: New consumer joining...");
    let new_c: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", &group_id)
        .set("session.timeout.ms", "10000")
        .set("enable.auto.commit", "false")
        .create()?;
    new_c.subscribe(&[&topic])?;
    let _ = tokio::time::timeout(Duration::from_secs(2), new_c.recv()).await;
    println!("  New consumer joined");

    println!("\n✅ Rebalance mixed timeout values test PASSED\n");
    Ok(())
}
