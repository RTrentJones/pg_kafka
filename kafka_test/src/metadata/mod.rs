//! Metadata API edge case tests

use crate::common::{create_producer, TestResult};
use crate::setup::TestContext;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::FutureRecord;
use rdkafka::ClientConfig;
use std::time::Duration;

/// Test: Fetch metadata for all topics
/// Verifies that MetadataRequest with empty topic list returns all topics
pub async fn test_metadata_all_topics() -> TestResult {
    println!("=== Test: Metadata All Topics ===\n");

    let ctx = TestContext::new().await?;

    // Create multiple topics
    let topic1 = ctx.unique_topic("meta1").await;
    let topic2 = ctx.unique_topic("meta2").await;
    let topic3 = ctx.unique_topic("meta3").await;

    // Produce to each topic to ensure they exist
    let producer = create_producer()?;
    for topic in [&topic1, &topic2, &topic3] {
        producer
            .send(
                FutureRecord::to(topic).payload("test").key("k"),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(e, _)| e)?;
    }

    println!("Step 1: Created 3 topics\n");

    // Create consumer and fetch metadata for all topics
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", &ctx.unique_group("meta").await)
        .create()?;

    let metadata = consumer.fetch_metadata(None, Duration::from_secs(5))?;

    println!("Step 2: Fetched metadata for all topics\n");

    // Verify our topics are in the list
    let topic_names: Vec<&str> = metadata.topics().iter().map(|t| t.name()).collect();

    assert!(
        topic_names.contains(&topic1.as_str()),
        "Topic 1 should be in metadata"
    );
    assert!(
        topic_names.contains(&topic2.as_str()),
        "Topic 2 should be in metadata"
    );
    assert!(
        topic_names.contains(&topic3.as_str()),
        "Topic 3 should be in metadata"
    );

    println!("✅ All 3 topics found in metadata response\n");
    println!("✅ Test PASSED\n");
    Ok(())
}

/// Test: Metadata request for non-existent topic
/// Verifies proper handling when topic doesn't exist (may auto-create or error)
pub async fn test_metadata_nonexistent_topic() -> TestResult {
    println!("=== Test: Metadata Non-existent Topic ===\n");

    let ctx = TestContext::new().await?;
    let nonexistent_topic = format!("nonexistent-{}", uuid::Uuid::new_v4());

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", &ctx.unique_group("meta").await)
        .create()?;

    println!("Step 1: Requesting metadata for non-existent topic\n");

    // This should either auto-create the topic or return it with an error code
    let metadata = consumer.fetch_metadata(Some(&nonexistent_topic), Duration::from_secs(5))?;

    println!("Step 2: Received metadata response\n");

    // Find our topic in the response
    let topic_meta = metadata
        .topics()
        .iter()
        .find(|t| t.name() == nonexistent_topic);

    match topic_meta {
        Some(t) => {
            // Topic was auto-created or has error
            if t.error().is_some() {
                println!(
                    "✅ Topic returned with error code: {:?}\n",
                    t.error().unwrap()
                );
            } else {
                println!(
                    "✅ Topic was auto-created with {} partition(s)\n",
                    t.partitions().len()
                );
            }
        }
        None => {
            println!("✅ Topic not in response (broker may have filtered it)\n");
        }
    }

    println!("✅ Test PASSED\n");
    Ok(())
}

/// Test: Metadata refresh after topic creation
/// Verifies that metadata is updated after a new topic is created
pub async fn test_metadata_refresh_after_create() -> TestResult {
    println!("=== Test: Metadata Refresh After Create ===\n");

    let ctx = TestContext::new().await?;

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", &ctx.unique_group("meta").await)
        .create()?;

    // Get initial metadata
    let initial_metadata = consumer.fetch_metadata(None, Duration::from_secs(5))?;
    let initial_count = initial_metadata.topics().len();

    println!("Step 1: Initial metadata has {} topics\n", initial_count);

    // Create a new topic
    let new_topic = ctx.unique_topic("newmeta").await;
    let producer = create_producer()?;
    producer
        .send(
            FutureRecord::to(&new_topic).payload("test").key("k"),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(e, _)| e)?;

    println!("Step 2: Created new topic: {}\n", new_topic);

    // Fetch metadata again
    let refreshed_metadata = consumer.fetch_metadata(None, Duration::from_secs(5))?;
    let refreshed_count = refreshed_metadata.topics().len();

    println!(
        "Step 3: Refreshed metadata has {} topics\n",
        refreshed_count
    );

    // Verify new topic appears
    let has_new_topic = refreshed_metadata
        .topics()
        .iter()
        .any(|t| t.name() == new_topic);

    assert!(
        has_new_topic,
        "New topic should appear in refreshed metadata"
    );
    assert!(
        refreshed_count >= initial_count,
        "Topic count should not decrease"
    );

    println!("✅ New topic found in refreshed metadata\n");
    println!("✅ Test PASSED\n");
    Ok(())
}
