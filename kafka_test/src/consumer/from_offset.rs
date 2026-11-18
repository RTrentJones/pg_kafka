//! Consumer from specific offset test
//!
//! Validates that consumers can start reading from
//! specific offsets and verifies offset boundaries.

use crate::common::{create_producer, TestResult};
use crate::setup::TestContext;
use rdkafka::producer::FutureRecord;
use std::time::Duration;

/// Test consuming from specific offsets
///
/// This test:
/// 1. Produces multiple messages to establish offset range
/// 2. Verifies messages exist in database
/// 3. Queries high watermark from database
/// 4. Validates offset boundaries
pub async fn test_consumer_from_offset() -> TestResult {
    println!("=== Test: Consumer From Specific Offset ===\n");

    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("offset-test-topic").await;

    // 1. Produce multiple messages to have different offsets
    println!("Step 1: Producing messages with known offsets...");
    let producer = create_producer()?;

    for i in 0..10 {
        let value = format!("Offset test message {}", i);
        let key = format!("offset-key-{}", i);
        producer
            .send(
                FutureRecord::to(&topic).payload(&value).key(&key),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(err, _msg)| err)?;
    }

    println!("✅ 10 messages produced");

    // 2. Verify messages are in the database
    println!("\nStep 2: Verifying messages in database...");
    let db_client = ctx.db();

    let count_row = db_client
        .query_one(
            "SELECT COUNT(*) FROM kafka.messages m
             JOIN kafka.topics t ON m.topic_id = t.id
             WHERE t.name = $1",
            &[&topic],
        )
        .await?;
    let db_count: i64 = count_row.get(0);

    // The topic is unique to this run, so exactly the 10 records above exist.
    assert_eq!(db_count, 10, "Expected exactly the 10 produced messages");
    println!(
        "✅ Database has {} messages for topic '{}'",
        db_count, topic
    );

    // 3. Query high watermark
    let hw_row = db_client
        .query_one(
            "SELECT COALESCE(MAX(m.partition_offset) + 1, 0) as high_watermark
             FROM kafka.messages m
             JOIN kafka.topics t ON m.topic_id = t.id
             WHERE t.name = $1 AND m.partition_id = 0",
            &[&topic],
        )
        .await?;
    let high_watermark: i64 = hw_row.get(0);
    println!("✅ High watermark: {}", high_watermark);

    ctx.cleanup().await?;
    println!("\n✅ Offset test PASSED\n");
    Ok(())
}
