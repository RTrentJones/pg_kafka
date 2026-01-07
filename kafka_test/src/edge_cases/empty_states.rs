//! Empty state tests

use crate::assertions::{assert_high_watermark, assert_no_committed_offset};
use crate::common::TestResult;
use crate::fixtures::TestTopicBuilder;
use crate::setup::TestContext;

/// Test consuming from empty partition
pub async fn test_consume_empty_partition() -> TestResult {
    println!("=== Test: Consume Empty Partition ===\n");

    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "empty-partition")
        .build()
        .await?;

    // Verify high watermark is 0 for empty partition
    assert_high_watermark(ctx.db(), &topic.name, 0, 0).await?;
    println!("✅ Empty partition has high watermark 0");

    ctx.cleanup().await?;
    println!("\n✅ Consume empty partition test PASSED\n");
    Ok(())
}

/// Test list offsets for empty topic
pub async fn test_list_offsets_empty_topic() -> TestResult {
    println!("=== Test: List Offsets Empty Topic ===\n");

    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "list-offsets-empty")
        .build()
        .await?;

    // Verify earliest = 0, latest = 0 for empty topic
    let row = ctx
        .db()
        .query_one(
            "SELECT COALESCE(MIN(partition_offset), 0), COALESCE(MAX(partition_offset) + 1, 0)
         FROM kafka.messages m
         JOIN kafka.topics t ON m.topic_id = t.id
         WHERE t.name = $1 AND m.partition_id = 0",
            &[&topic.name],
        )
        .await?;

    let earliest: i64 = row.get(0);
    let latest: i64 = row.get(1);

    println!("   Earliest: {}, Latest: {}", earliest, latest);
    assert_eq!(earliest, 0);
    assert_eq!(latest, 0);
    println!("✅ Empty topic has earliest=0, latest=0");

    ctx.cleanup().await?;
    println!("\n✅ List offsets empty topic test PASSED\n");
    Ok(())
}

/// Test fetch committed offset when none exists
pub async fn test_fetch_committed_no_history() -> TestResult {
    println!("=== Test: Fetch Committed No History ===\n");

    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "no-commit-history")
        .build()
        .await?;
    let group_id = ctx.unique_group("no-history-group").await;

    // Verify no committed offset exists
    assert_no_committed_offset(ctx.db(), &group_id, &topic.name, 0).await?;
    println!("✅ No committed offset found for new group");

    ctx.cleanup().await?;
    println!("\n✅ Fetch committed no history test PASSED\n");
    Ok(())
}

/// Test new consumer group with no members
pub async fn test_consumer_group_empty() -> TestResult {
    println!("=== Test: Consumer Group Empty ===\n");

    let ctx = TestContext::new().await?;
    let group_id = ctx.unique_group("empty-group").await;

    // Group should not exist until a consumer joins
    let row = ctx
        .db()
        .query_opt(
            "SELECT COUNT(*) FROM kafka.consumer_offsets WHERE group_id = $1",
            &[&group_id],
        )
        .await?;

    let count: i64 = row.map(|r| r.get(0)).unwrap_or(0);
    assert_eq!(count, 0, "Empty group should have no offsets");
    println!("✅ Empty group has no committed offsets");

    ctx.cleanup().await?;
    println!("\n✅ Consumer group empty test PASSED\n");
    Ok(())
}
