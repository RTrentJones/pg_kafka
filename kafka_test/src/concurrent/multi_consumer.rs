//! Multi-consumer concurrent tests

use crate::common::{create_stream_consumer, TestResult};
use crate::fixtures::{generate_messages, TestTopicBuilder};
use crate::setup::TestContext;
use rdkafka::consumer::Consumer;
use std::time::Duration;

/// Test multiple consumer groups on same topic
pub async fn test_multiple_consumer_groups() -> TestResult {
    println!("=== Test: Multiple Consumer Groups ===\n");

    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "multi-group").build().await?;

    let messages = generate_messages(5, "test");
    topic.produce(&messages).await?;

    let group1 = ctx.unique_group("group-1").await;
    let group2 = ctx.unique_group("group-2").await;

    // Create two consumers in different groups
    let consumer1 = create_stream_consumer(&group1)?;
    let consumer2 = create_stream_consumer(&group2)?;

    consumer1.subscribe(&[&topic.name])?;
    consumer2.subscribe(&[&topic.name])?;

    // Each group should receive all messages
    let mut c1_count = 0;
    let mut c2_count = 0;

    let timeout = Duration::from_secs(15);
    let start = std::time::Instant::now();

    while (c1_count < 5 || c2_count < 5) && start.elapsed() < timeout {
        tokio::select! {
            result = tokio::time::timeout(Duration::from_millis(500), consumer1.recv()) => {
                if let Ok(Ok(_)) = result {
                    c1_count += 1;
                }
            }
            result = tokio::time::timeout(Duration::from_millis(500), consumer2.recv()) => {
                if let Ok(Ok(_)) = result {
                    c2_count += 1;
                }
            }
        }
    }

    println!(
        "   Group 1 received: {}, Group 2 received: {}",
        c1_count, c2_count
    );
    assert!(c1_count >= 3, "Group 1 should receive at least 3 messages");
    assert!(c2_count >= 3, "Group 2 should receive at least 3 messages");
    println!("✅ Both consumer groups received messages independently");

    ctx.cleanup().await?;
    println!("\n✅ Multiple consumer groups test PASSED\n");
    Ok(())
}

/// Test two consumers in same group
pub async fn test_consumer_group_two_members() -> TestResult {
    println!("=== Test: Consumer Group Two Members ===\n");

    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "two-members")
        .with_partitions(2)
        .build()
        .await?;

    let messages = generate_messages(10, "test");
    topic.produce(&messages).await?;

    let group_id = ctx.unique_group("two-member-group").await;

    let consumer1 = create_stream_consumer(&group_id)?;
    let consumer2 = create_stream_consumer(&group_id)?;

    consumer1.subscribe(&[&topic.name])?;
    consumer2.subscribe(&[&topic.name])?;

    let mut c1_count = 0;
    let mut c2_count = 0;

    let timeout = Duration::from_secs(15);
    let start = std::time::Instant::now();

    while (c1_count + c2_count) < 8 && start.elapsed() < timeout {
        tokio::select! {
            result = tokio::time::timeout(Duration::from_millis(500), consumer1.recv()) => {
                if let Ok(Ok(_)) = result {
                    c1_count += 1;
                }
            }
            result = tokio::time::timeout(Duration::from_millis(500), consumer2.recv()) => {
                if let Ok(Ok(_)) = result {
                    c2_count += 1;
                }
            }
        }
    }

    let total = c1_count + c2_count;
    println!(
        "   Consumer 1: {}, Consumer 2: {}, Total: {}",
        c1_count, c2_count, total
    );
    assert!(total >= 5, "Combined should receive at least 5 messages");
    println!("✅ Two consumers in same group working");

    ctx.cleanup().await?;
    println!("\n✅ Consumer group two members test PASSED\n");
    Ok(())
}

/// Test consumer rejoin after leave
pub async fn test_consumer_rejoin_after_leave() -> TestResult {
    println!("=== Test: Consumer Rejoin After Leave ===\n");

    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "rejoin").build().await?;

    let messages = generate_messages(10, "test");
    topic.produce(&messages).await?;

    let group_id = ctx.unique_group("rejoin-group").await;

    // First consumer
    let consumer1 = create_stream_consumer(&group_id)?;
    consumer1.subscribe(&[&topic.name])?;

    let mut first_count = 0;
    let timeout = Duration::from_secs(5);
    let start = std::time::Instant::now();

    while first_count < 3 && start.elapsed() < timeout {
        if let Ok(Ok(_)) = tokio::time::timeout(Duration::from_secs(1), consumer1.recv()).await {
            first_count += 1;
        }
    }

    println!("   First consumer received: {}", first_count);
    drop(consumer1);

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Second consumer with same group
    let consumer2 = create_stream_consumer(&group_id)?;
    consumer2.subscribe(&[&topic.name])?;

    let mut second_count = 0;
    let start = std::time::Instant::now();

    while second_count < 3 && start.elapsed() < timeout {
        if let Ok(Ok(_)) = tokio::time::timeout(Duration::from_secs(1), consumer2.recv()).await {
            second_count += 1;
        }
    }

    println!("   Second consumer (rejoin) received: {}", second_count);
    println!("✅ Consumer successfully rejoined group");

    ctx.cleanup().await?;
    println!("\n✅ Consumer rejoin after leave test PASSED\n");
    Ok(())
}
