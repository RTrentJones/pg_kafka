//! Boundary condition tests

use crate::common::TestResult;
use crate::fixtures::{generate_messages, TestTopicBuilder};
use crate::setup::TestContext;

/// Test first message at offset 0
pub async fn test_offset_zero_boundary() -> TestResult {
    println!("=== Test: Offset Zero Boundary ===\n");

    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "offset-zero").build().await?;

    let messages = generate_messages(1, "test");
    let offsets = topic.produce(&messages).await?;

    assert_eq!(offsets[0], 0, "First message should be at offset 0");
    println!("✅ First message correctly at offset 0");

    ctx.cleanup().await?;
    println!("\n✅ Offset zero boundary test PASSED\n");
    Ok(())
}

/// Test partition 0 explicit targeting
pub async fn test_partition_zero() -> TestResult {
    println!("=== Test: Partition Zero Targeting ===\n");

    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "partition-zero")
        .with_partitions(3)
        .build()
        .await?;

    // Produce explicitly to partition 0
    use crate::fixtures::TestMessage;
    let msg = TestMessage {
        key: Some("key".to_string()),
        value: "value".to_string(),
        partition: Some(0),
    };

    let offsets = topic.produce(&[msg]).await?;
    println!(
        "   Message delivered to partition 0 at offset {}",
        offsets[0]
    );

    // Verify in database
    let row = ctx
        .db()
        .query_one(
            "SELECT partition_id FROM kafka.messages m
         JOIN kafka.topics t ON m.topic_id = t.id
         WHERE t.name = $1 AND partition_offset = $2",
            &[&topic.name, &offsets[0]],
        )
        .await?;

    let partition: i32 = row.get(0);
    assert_eq!(partition, 0, "Message should be in partition 0");
    println!("✅ Message correctly in partition 0");

    ctx.cleanup().await?;
    println!("\n✅ Partition zero test PASSED\n");
    Ok(())
}

/// Test single partition topic
pub async fn test_single_partition_topic() -> TestResult {
    println!("=== Test: Single Partition Topic ===\n");

    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "single-partition")
        .with_partitions(1)
        .build()
        .await?;

    let messages = generate_messages(5, "test");
    let offsets = topic.produce(&messages).await?;

    // All should go to partition 0
    for (i, offset) in offsets.iter().enumerate() {
        assert_eq!(*offset, i as i64, "Offset {} should be {}", offset, i);
    }
    println!("✅ All messages in single partition with sequential offsets");

    ctx.cleanup().await?;
    println!("\n✅ Single partition topic test PASSED\n");
    Ok(())
}

/// Test high offset values
pub async fn test_high_offset_values() -> TestResult {
    println!("=== Test: High Offset Values ===\n");

    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "high-offset").build().await?;

    // Produce many messages to get high offsets
    let messages = generate_messages(100, "test");
    let offsets = topic.produce(&messages).await?;

    let last_offset = offsets.last().unwrap();
    assert_eq!(*last_offset, 99, "Last offset should be 99");
    println!("✅ Produced 100 messages, last offset: {}", last_offset);

    ctx.cleanup().await?;
    println!("\n✅ High offset values test PASSED\n");
    Ok(())
}
