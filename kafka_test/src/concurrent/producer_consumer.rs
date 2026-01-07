//! Producer-consumer concurrent tests

use crate::common::{create_stream_consumer, TestResult};
use crate::fixtures::TestTopicBuilder;
use crate::setup::TestContext;
use rdkafka::consumer::Consumer;
use rdkafka::message::Message;
use rdkafka::producer::FutureRecord;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Test simultaneous produce and consume
pub async fn test_produce_while_consuming() -> TestResult {
    println!("=== Test: Produce While Consuming ===\n");

    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "simultaneous").build().await?;

    let topic_name = Arc::new(topic.name.clone());
    let produced = Arc::new(AtomicUsize::new(0));
    let consumed = Arc::new(AtomicUsize::new(0));

    let group_id = ctx.unique_group("simultaneous-group").await;

    // Start consumer
    let consumer = create_stream_consumer(&group_id)?;
    consumer.subscribe(&[&topic.name])?;

    let consumed_clone = consumed.clone();
    let consumer_handle = tokio::spawn(async move {
        let timeout = Duration::from_secs(10);
        let start = std::time::Instant::now();

        while start.elapsed() < timeout {
            if let Ok(Ok(_)) =
                tokio::time::timeout(Duration::from_millis(200), consumer.recv()).await
            {
                consumed_clone.fetch_add(1, Ordering::SeqCst);
            }
        }
    });

    // Start producer (slight delay to let consumer set up)
    tokio::time::sleep(Duration::from_millis(500)).await;

    let topic_name_clone = topic_name.clone();
    let produced_clone = produced.clone();
    let producer_handle = tokio::spawn(async move {
        let producer = crate::common::create_producer().unwrap();

        for i in 0..20 {
            let value = format!("message-{}", i);
            if producer
                .send(
                    FutureRecord::to(&topic_name_clone)
                        .payload(&value)
                        .key(&format!("key-{}", i)),
                    Duration::from_secs(5),
                )
                .await
                .is_ok()
            {
                produced_clone.fetch_add(1, Ordering::SeqCst);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    });

    // Wait for both
    let _ = producer_handle.await;
    let _ = consumer_handle.await;

    let produced_count = produced.load(Ordering::SeqCst);
    let consumed_count = consumed.load(Ordering::SeqCst);

    println!(
        "   Produced: {}, Consumed: {}",
        produced_count, consumed_count
    );
    assert!(produced_count >= 15, "Should produce at least 15 messages");
    assert!(consumed_count >= 5, "Should consume at least 5 messages");
    println!("✅ Simultaneous produce/consume working");

    ctx.cleanup().await?;
    println!("\n✅ Produce while consuming test PASSED\n");
    Ok(())
}

/// Test consumer catching up to producer
pub async fn test_consumer_catches_up() -> TestResult {
    println!("=== Test: Consumer Catches Up ===\n");

    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "catch-up").build().await?;

    // Produce messages first
    let producer = crate::common::create_producer()?;
    for i in 0..10 {
        producer
            .send(
                FutureRecord::to(&topic.name)
                    .payload(&format!("message-{}", i))
                    .key(&format!("key-{}", i)),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(e, _)| e)?;
    }
    println!("   Produced 10 messages");

    // Now start consumer
    let group_id = ctx.unique_group("catch-up-group").await;
    let consumer = create_stream_consumer(&group_id)?;
    consumer.subscribe(&[&topic.name])?;

    let mut consumed = 0;
    let timeout = Duration::from_secs(10);
    let start = std::time::Instant::now();

    while consumed < 10 && start.elapsed() < timeout {
        if let Ok(Ok(msg)) = tokio::time::timeout(Duration::from_secs(1), consumer.recv()).await {
            consumed += 1;
            if consumed == 1 {
                println!("   First message at offset: {}", msg.offset());
            }
        }
    }

    println!("   Consumer caught up, received: {}", consumed);
    assert!(consumed >= 8, "Should consume at least 8 of 10 messages");
    println!("✅ Consumer successfully caught up");

    ctx.cleanup().await?;
    println!("\n✅ Consumer catches up test PASSED\n");
    Ok(())
}
