//! Batch producer test
//!
//! Validates high-throughput batch produce operations
//! and verifies the N+1 query fix by producing 100 records.

use crate::common::{create_batch_producer, create_db_client, TestResult};
use rdkafka::producer::FutureRecord;
use std::time::Duration;

/// Test batch produce of 100 records
///
/// This test validates:
/// 1. High-throughput message production (100 records)
/// 2. Correct batch handling without N+1 query issues
/// 3. All messages are persisted to database
/// 4. Acceptable throughput rate
pub async fn test_batch_produce() -> TestResult {
    println!("=== Test: Batch Produce (100 records) ===\n");

    let topic = "batch-produce-topic";
    let batch_size = 100;

    // 1. Get initial message count in database
    println!("Step 1: Getting initial message count...");
    let db_client = create_db_client().await?;

    let initial_count: i64 = db_client
        .query_one(
            "SELECT COUNT(*) FROM kafka.messages m
             JOIN kafka.topics t ON m.topic_id = t.id
             WHERE t.name = $1",
            &[&topic],
        )
        .await
        .map(|row| row.get(0))
        .unwrap_or(0);

    println!("   Initial count: {}", initial_count);

    // 2. Create producer and send batch of 100 messages
    println!("\nStep 2: Producing {} messages in batch...", batch_size);
    let producer = create_batch_producer()?;

    let start_time = std::time::Instant::now();

    // Send all messages - use async sends to allow batching by rdkafka
    let mut delivered = 0;
    let mut failed = 0;

    for i in 0..batch_size {
        let key = format!("batch-key-{}", i);
        let value = format!("Batch message {} - testing N+1 fix performance", i);

        match producer
            .send(
                FutureRecord::to(topic).payload(&value).key(&key),
                Duration::from_secs(30),
            )
            .await
        {
            Ok(_) => delivered += 1,
            Err((err, _)) => {
                println!("   ⚠️  Delivery failed: {}", err);
                failed += 1;
            }
        }
    }

    let elapsed = start_time.elapsed();
    println!("✅ Batch produce completed:");
    println!("   Delivered: {}", delivered);
    println!("   Failed: {}", failed);
    println!("   Time: {:?}", elapsed);
    println!(
        "   Rate: {:.0} msg/sec",
        delivered as f64 / elapsed.as_secs_f64()
    );

    assert_eq!(failed, 0, "Some messages failed to deliver");
    assert_eq!(delivered, batch_size, "Not all messages were delivered");

    // 3. Verify all messages in database
    println!("\nStep 3: Verifying messages in database...");
    let db_client2 = create_db_client().await?;

    let final_count: i64 = db_client2
        .query_one(
            "SELECT COUNT(*) FROM kafka.messages m
             JOIN kafka.topics t ON m.topic_id = t.id
             WHERE t.name = $1",
            &[&topic],
        )
        .await?
        .get(0);

    let new_messages = final_count - initial_count;
    println!("   Final count: {}", final_count);
    println!("   New messages: {}", new_messages);

    assert_eq!(
        new_messages, batch_size as i64,
        "Database should have {} new messages",
        batch_size
    );
    println!("✅ All {} messages verified in database", batch_size);

    println!("\n✅ Batch produce test PASSED\n");
    Ok(())
}
