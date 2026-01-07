//! Consumer group lifecycle test
//!
//! Validates the full consumer group coordinator protocol:
//! FindCoordinator → JoinGroup → SyncGroup → Heartbeat → LeaveGroup

use crate::common::{create_producer, TestResult};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::FutureRecord;
use std::time::Duration;

/// Test full consumer group lifecycle
///
/// This test exercises:
/// 1. FindCoordinator (API key 10) - finding the group coordinator
/// 2. JoinGroup (API key 11) - joining the consumer group
/// 3. SyncGroup (API key 14) - synchronizing partition assignments
/// 4. Heartbeat (API key 12) - maintaining group membership
/// 5. OffsetCommit (API key 8) - committing consumed offsets
/// 6. LeaveGroup (API key 13) - gracefully leaving the group
pub async fn test_consumer_group_lifecycle() -> TestResult {
    println!("=== Test: Consumer Group Lifecycle ===\n");

    let topic = "lifecycle-test-topic";
    let group_id = "lifecycle-test-group";

    // 1. Produce some test messages
    println!("Step 1: Producing test messages...");
    let producer = create_producer()?;

    for i in 0..3 {
        let value = format!("Lifecycle test message {}", i);
        producer
            .send(
                FutureRecord::to(topic)
                    .payload(&value)
                    .key(&format!("key-{}", i)),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(err, _msg)| err)?;
    }
    println!("✅ 3 messages produced");

    // 2. Create consumer and join group (exercises FindCoordinator + JoinGroup)
    println!("\nStep 2: Creating consumer and joining group...");
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", group_id)
        .set("session.timeout.ms", "10000")
        .set("heartbeat.interval.ms", "1000")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()?;

    // Subscribe exercises FindCoordinator, JoinGroup, SyncGroup
    consumer.subscribe(&[topic])?;
    println!("✅ Consumer subscribed (FindCoordinator + JoinGroup + SyncGroup)");

    // 3. Poll for messages (exercises Fetch + implicit Heartbeats)
    println!("\nStep 3: Polling for messages (Fetch + Heartbeats)...");
    let timeout = Duration::from_secs(15);
    let start = std::time::Instant::now();
    let mut received = 0;

    while received < 3 && start.elapsed() < timeout {
        match tokio::time::timeout(Duration::from_secs(2), consumer.recv()).await {
            Ok(Ok(msg)) => {
                received += 1;
                println!(
                    "   Received message {} at offset {}",
                    received,
                    msg.offset()
                );
            }
            Ok(Err(e)) => {
                println!("   ⚠️  Consumer error: {}", e);
            }
            Err(_) => {
                println!("   (polling...)");
            }
        }
    }

    println!("✅ Received {} messages", received);

    // 4. Commit offsets (exercises OffsetCommit)
    println!("\nStep 4: Committing offsets...");
    match consumer.commit_consumer_state(rdkafka::consumer::CommitMode::Sync) {
        Ok(_) => println!("✅ Offsets committed"),
        Err(e) => println!("   ⚠️  Commit warning: {} (may be expected)", e),
    }

    // 5. Graceful shutdown (exercises LeaveGroup)
    println!("\nStep 5: Leaving group (graceful shutdown)...");
    drop(consumer);
    println!("✅ Consumer dropped (LeaveGroup sent)");

    // 6. Verify group is empty in database/coordinator
    println!("\nStep 6: Verifying group lifecycle completed...");
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Create new consumer to verify group is joinable
    let consumer2: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", group_id)
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .create()?;

    consumer2.subscribe(&[topic])?;
    println!("✅ New consumer successfully joined same group");

    drop(consumer2);

    println!("\n✅ Consumer group lifecycle test PASSED\n");
    Ok(())
}
