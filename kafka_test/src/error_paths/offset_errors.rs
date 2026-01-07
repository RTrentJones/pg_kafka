//! Offset error path tests
//!
//! Tests for offset management error conditions including
//! negative offsets, missing committed offsets, and boundary conditions.

use crate::assertions::{assert_no_committed_offset, assert_offset_committed};
use crate::common::{create_manual_commit_consumer, TestResult, POLL_TIMEOUT};
use crate::fixtures::{generate_messages, TestTopicBuilder};
use crate::setup::TestContext;
use rdkafka::consumer::Consumer;
use rdkafka::TopicPartitionList;
use std::time::Duration;

/// Test committing offset 0 (valid, earliest position)
///
/// Should succeed as 0 is a valid offset.
pub async fn test_commit_offset_zero() -> TestResult {
    println!("=== Test: Commit Offset Zero ===\n");

    let ctx = TestContext::new().await?;

    let topic = TestTopicBuilder::new(&ctx, "commit-zero").build().await?;

    let messages = generate_messages(3, "test");
    topic.produce(&messages).await?;

    let group_id = ctx.unique_group("commit-zero-group").await;
    let consumer = create_manual_commit_consumer(&group_id)?;

    let mut assignment = TopicPartitionList::new();
    assignment.add_partition_offset(&topic.name, 0, rdkafka::Offset::Beginning)?;
    consumer.assign(&assignment)?;

    // Commit offset 0
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(&topic.name, 0, rdkafka::Offset::Offset(0))?;

    println!("Committing offset 0...");
    consumer.commit(&tpl, rdkafka::consumer::CommitMode::Sync)?;

    // Verify in database
    assert_offset_committed(ctx.db(), &group_id, &topic.name, 0, 0).await?;
    println!("✅ Offset 0 committed successfully");

    ctx.cleanup().await?;
    println!("\n✅ Commit offset zero test PASSED\n");
    Ok(())
}

/// Test fetching committed offset before any commits
///
/// Should return -1 or appropriate "no offset" indicator.
pub async fn test_fetch_uncommitted_offset() -> TestResult {
    println!("=== Test: Fetch Uncommitted Offset ===\n");

    let ctx = TestContext::new().await?;

    let topic = TestTopicBuilder::new(&ctx, "fetch-uncommitted")
        .build()
        .await?;

    let messages = generate_messages(3, "test");
    topic.produce(&messages).await?;

    let group_id = ctx.unique_group("fetch-uncommitted-group").await;

    // Verify no committed offset exists
    assert_no_committed_offset(ctx.db(), &group_id, &topic.name, 0).await?;
    println!("✅ No committed offset found (correct)");

    ctx.cleanup().await?;
    println!("\n✅ Fetch uncommitted offset test PASSED\n");
    Ok(())
}

/// Test committing and then fetching offset
///
/// Basic offset lifecycle verification.
pub async fn test_commit_then_fetch_offset() -> TestResult {
    println!("=== Test: Commit Then Fetch Offset ===\n");

    let ctx = TestContext::new().await?;

    let topic = TestTopicBuilder::new(&ctx, "commit-fetch").build().await?;

    let messages = generate_messages(5, "test");
    topic.produce(&messages).await?;

    let group_id = ctx.unique_group("commit-fetch-group").await;
    let consumer = create_manual_commit_consumer(&group_id)?;

    let mut assignment = TopicPartitionList::new();
    assignment.add_partition_offset(&topic.name, 0, rdkafka::Offset::Beginning)?;
    consumer.assign(&assignment)?;

    // Consume 3 messages
    let mut consumed = 0;
    let timeout = Duration::from_secs(10);
    let start = std::time::Instant::now();

    while consumed < 3 && start.elapsed() < timeout {
        if let Some(Ok(_)) = consumer.poll(POLL_TIMEOUT) {
            consumed += 1;
        }
    }

    // Commit offset 3 (next offset to consume)
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(&topic.name, 0, rdkafka::Offset::Offset(3))?;
    consumer.commit(&tpl, rdkafka::consumer::CommitMode::Sync)?;

    println!("Committed offset 3 after consuming 3 messages");

    // Verify committed offset
    assert_offset_committed(ctx.db(), &group_id, &topic.name, 0, 3).await?;
    println!("✅ Offset correctly committed and verified");

    ctx.cleanup().await?;
    println!("\n✅ Commit then fetch offset test PASSED\n");
    Ok(())
}
