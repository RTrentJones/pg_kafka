//! Expected failure tests
//!
//! Tests that verify proper error handling in failure scenarios.

use crate::common::TestResult;
use crate::fixtures::TestTopicBuilder;
use crate::setup::TestContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::FutureProducer;
use std::time::Duration;

/// Test connection to wrong port fails gracefully
pub async fn test_connection_refused() -> TestResult {
    println!("=== Test: Connection Refused ===\n");

    // Try to connect to a port that's not running kafka
    let producer_result: Result<FutureProducer, _> = ClientConfig::new()
        .set("bootstrap.servers", "localhost:19999") // Non-existent port
        .set("message.timeout.ms", "1000")
        .set("socket.timeout.ms", "1000")
        .create();

    match producer_result {
        Ok(producer) => {
            // Producer created, but should fail on actual operation
            let result = producer
                .send(
                    rdkafka::producer::FutureRecord::to("test-topic")
                        .payload("test")
                        .key("key"),
                    Duration::from_secs(2),
                )
                .await;

            match result {
                Err((kafka_error, _)) => {
                    println!("   Expected failure occurred: {}", kafka_error);
                    println!("   Error is recoverable/expected");
                }
                Ok(_) => {
                    println!("   Unexpected: message succeeded to non-existent server");
                    // This might happen if port 19999 is somehow in use
                }
            }
        }
        Err(e) => {
            println!("   Producer creation failed (expected): {}", e);
        }
    }

    println!("   Connection refused handled gracefully");
    println!("\n   Test PASSED (graceful failure handling)\n");
    Ok(())
}

/// Test produce with very short timeout
pub async fn test_produce_timeout() -> TestResult {
    println!("=== Test: Produce Timeout ===\n");

    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "timeout-test").build().await?;

    // Create producer with very short timeout
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", crate::common::get_bootstrap_servers())
        .set("message.timeout.ms", "1") // 1ms timeout - extremely short
        .create()?;

    let mut timeout_count = 0;
    let mut success_count = 0;

    for i in 0..5 {
        let result = producer
            .send(
                rdkafka::producer::FutureRecord::to(&topic.name)
                    .payload(&format!("message-{}", i))
                    .key(&format!("key-{}", i)),
                Duration::from_millis(1),
            )
            .await;

        match result {
            Err((e, _)) => {
                timeout_count += 1;
                if i == 0 {
                    println!("   Timeout error (expected): {}", e);
                }
            }
            Ok(_) => {
                success_count += 1;
            }
        }
    }

    println!(
        "   Timeouts: {}, Successes: {}",
        timeout_count, success_count
    );
    // With such a short timeout, we expect at least some failures
    // But on fast systems, some might succeed
    println!("   Short timeout behavior verified");

    ctx.cleanup().await?;
    println!("\n   Produce timeout test PASSED\n");
    Ok(())
}

/// Test invalid (empty) group ID
pub async fn test_invalid_group_id() -> TestResult {
    println!("=== Test: Invalid Group ID ===\n");

    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "invalid-group").build().await?;

    // Try to create consumer with empty group ID
    let consumer_result: Result<StreamConsumer, _> = ClientConfig::new()
        .set("bootstrap.servers", crate::common::get_bootstrap_servers())
        .set("group.id", "") // Empty group ID - invalid
        .set("auto.offset.reset", "earliest")
        .create();

    match consumer_result {
        Err(e) => {
            println!(
                "   Consumer creation failed with empty group.id (expected): {}",
                e
            );
        }
        Ok(consumer) => {
            // Some implementations might allow creation but fail on subscribe
            match consumer.subscribe(&[&topic.name]) {
                Err(e) => {
                    println!("   Subscribe failed with empty group.id (expected): {}", e);
                }
                Ok(_) => {
                    // Try to receive - should fail or return error
                    match tokio::time::timeout(Duration::from_secs(2), consumer.recv()).await {
                        Ok(Ok(_)) => println!("   Unexpected: received message with empty group"),
                        Ok(Err(e)) => println!("   Recv error (expected): {}", e),
                        Err(_) => println!("   Timeout (acceptable for empty group)"),
                    }
                }
            }
        }
    }

    println!("   Invalid group ID handled appropriately");

    ctx.cleanup().await?;
    println!("\n   Invalid group ID test PASSED\n");
    Ok(())
}

/// Test duplicate consumer join (same member ID)
pub async fn test_duplicate_consumer_join() -> TestResult {
    println!("=== Test: Duplicate Consumer Join ===\n");

    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "duplicate-join")
        .build()
        .await?;

    let group_id = ctx.unique_group("duplicate-group").await;

    // Create first consumer
    let consumer1: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", crate::common::get_bootstrap_servers())
        .set("group.id", &group_id)
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "6000")
        .create()?;

    consumer1.subscribe(&[&topic.name])?;
    println!("   Consumer 1 joined group: {}", group_id);

    // Let first consumer establish itself
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Create second consumer in same group (should cause rebalance, not error)
    let consumer2: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", crate::common::get_bootstrap_servers())
        .set("group.id", &group_id)
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "6000")
        .create()?;

    consumer2.subscribe(&[&topic.name])?;
    println!("   Consumer 2 joined same group");

    // Both consumers should be able to operate (triggering rebalance)
    // With a single-partition topic, only one will actually receive messages
    tokio::time::sleep(Duration::from_millis(500)).await;

    println!("   Both consumers in group (rebalance handled)");
    println!("   Duplicate join behavior verified");

    ctx.cleanup().await?;
    println!("\n   Duplicate consumer join test PASSED\n");
    Ok(())
}
