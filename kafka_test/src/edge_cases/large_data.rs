//! Large data tests

use crate::common::TestResult;
use crate::fixtures::{generate_messages, TestMessage, TestTopicBuilder};
use crate::setup::TestContext;

/// Test large message key (1KB)
pub async fn test_large_message_key() -> TestResult {
    println!("=== Test: Large Message Key (1KB) ===\n");

    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "large-key").build().await?;

    let large_key = "K".repeat(1024);
    let msg = TestMessage::with_key("value", &large_key);
    let offsets = topic.produce(&[msg]).await?;

    // Verify key size in database
    let row = ctx
        .db()
        .query_one(
            "SELECT LENGTH(key) FROM kafka.messages m
         JOIN kafka.topics t ON m.topic_id = t.id
         WHERE t.name = $1 AND partition_offset = $2",
            &[&topic.name, &offsets[0]],
        )
        .await?;

    let key_len: i32 = row.get(0);
    assert_eq!(key_len, 1024);
    println!("✅ 1KB key stored correctly");

    ctx.cleanup().await?;
    println!("\n✅ Large message key test PASSED\n");
    Ok(())
}

/// Test large message value (100KB)
pub async fn test_large_message_value() -> TestResult {
    println!("=== Test: Large Message Value (100KB) ===\n");

    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "large-value").build().await?;

    let large_value = "V".repeat(100 * 1024);
    let msg = TestMessage::with_key(&large_value, "key");
    let offsets = topic.produce(&[msg]).await?;

    // Verify value size in database
    let row = ctx
        .db()
        .query_one(
            "SELECT LENGTH(value) FROM kafka.messages m
         JOIN kafka.topics t ON m.topic_id = t.id
         WHERE t.name = $1 AND partition_offset = $2",
            &[&topic.name, &offsets[0]],
        )
        .await?;

    let value_len: i32 = row.get(0);
    assert_eq!(value_len, 100 * 1024);
    println!("✅ 100KB value stored correctly");

    ctx.cleanup().await?;
    println!("\n✅ Large message value test PASSED\n");
    Ok(())
}

/// Test batch of 1000 messages
pub async fn test_batch_1000_messages() -> TestResult {
    println!("=== Test: Batch 1000 Messages ===\n");

    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "batch-1000").build().await?;

    let start = std::time::Instant::now();
    let messages = generate_messages(1000, "batch");
    let offsets = topic.produce(&messages).await?;
    let elapsed = start.elapsed();

    assert_eq!(offsets.len(), 1000);
    println!("✅ Produced 1000 messages in {:?}", elapsed);
    println!("   Rate: {:.0} msg/sec", 1000.0 / elapsed.as_secs_f64());

    // Verify in database
    let row = ctx
        .db()
        .query_one(
            "SELECT COUNT(*) FROM kafka.messages m
         JOIN kafka.topics t ON m.topic_id = t.id
         WHERE t.name = $1",
            &[&topic.name],
        )
        .await?;

    let count: i64 = row.get(0);
    assert_eq!(count, 1000);
    println!("✅ All 1000 messages verified in database");

    ctx.cleanup().await?;
    println!("\n✅ Batch 1000 messages test PASSED\n");
    Ok(())
}
