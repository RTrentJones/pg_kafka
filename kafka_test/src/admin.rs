//! Admin API E2E tests
//!
//! Tests for Kafka admin operations:
//! - CreateTopics (API 19)
//! - DeleteTopics (API 20)
//! - CreatePartitions (API 37)
//! - DeleteGroups (API 42)

use crate::common::{create_db_client, get_bootstrap_servers, TestResult};
use rdkafka::admin::{AdminClient, AdminOptions, NewPartitions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use std::time::Duration;
use uuid::Uuid;

/// Create an admin client for testing
fn create_admin_client() -> Result<AdminClient<DefaultClientContext>, Box<dyn std::error::Error>> {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", get_bootstrap_servers())
        .set("broker.address.family", "v4")
        .create()?;
    Ok(admin)
}

/// Test creating a new topic via CreateTopics API
pub async fn test_create_topic() -> TestResult {
    let admin = create_admin_client()?;
    let topic_name = format!("test-create-topic-{}", Uuid::new_v4());

    // Create a new topic with 3 partitions
    let new_topic = NewTopic::new(&topic_name, 3, TopicReplication::Fixed(1));
    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(5)));

    let results = admin.create_topics(&[new_topic], &opts).await?;
    assert_eq!(results.len(), 1);

    // Check that the topic was created successfully
    let result = &results[0];
    assert!(
        result.is_ok(),
        "Failed to create topic: {:?}",
        result.as_ref().err()
    );

    // Verify in database
    let db = create_db_client().await?;
    let row = db
        .query_one(
            "SELECT partitions FROM kafka.topics WHERE name = $1",
            &[&topic_name],
        )
        .await?;

    let partitions: i32 = row.get(0);
    assert_eq!(partitions, 3, "Topic should have 3 partitions");

    // Cleanup - delete the topic
    let _ = admin
        .delete_topics(&[&topic_name], &AdminOptions::new())
        .await;

    println!("    Created topic '{}' with 3 partitions", topic_name);
    Ok(())
}

/// Test creating a topic that already exists
pub async fn test_create_topic_already_exists() -> TestResult {
    let admin = create_admin_client()?;
    let topic_name = format!("test-create-exists-{}", Uuid::new_v4());

    // Create the topic first
    let new_topic = NewTopic::new(&topic_name, 1, TopicReplication::Fixed(1));
    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(5)));

    let results = admin.create_topics(&[new_topic], &opts).await?;
    assert!(results[0].is_ok(), "First creation should succeed");

    // Try to create the same topic again
    let new_topic = NewTopic::new(&topic_name, 1, TopicReplication::Fixed(1));
    let results = admin.create_topics(&[new_topic], &opts).await?;

    // Should fail with TOPIC_ALREADY_EXISTS
    let result = &results[0];
    assert!(result.is_err(), "Second creation should fail");
    let err = result.as_ref().unwrap_err();
    // rdkafka error code 36 is TOPIC_ALREADY_EXISTS
    assert!(
        format!("{:?}", err).contains("TopicAlreadyExists")
            || format!("{:?}", err).contains("36")
            || format!("{:?}", err).contains("already exists"),
        "Expected TOPIC_ALREADY_EXISTS error, got: {:?}",
        err
    );

    // Cleanup
    let _ = admin
        .delete_topics(&[&topic_name], &AdminOptions::new())
        .await;

    println!(
        "    Correctly rejected duplicate topic creation for '{}'",
        topic_name
    );
    Ok(())
}

/// Test deleting an existing topic
pub async fn test_delete_topic() -> TestResult {
    let admin = create_admin_client()?;
    let topic_name = format!("test-delete-topic-{}", Uuid::new_v4());

    // First create the topic
    let new_topic = NewTopic::new(&topic_name, 1, TopicReplication::Fixed(1));
    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(5)));

    let results = admin.create_topics(&[new_topic], &opts).await?;
    assert!(results[0].is_ok(), "Topic creation should succeed");

    // Now delete it
    let results = admin.delete_topics(&[&topic_name], &opts).await?;
    assert_eq!(results.len(), 1);

    let result = &results[0];
    assert!(
        result.is_ok(),
        "Failed to delete topic: {:?}",
        result.as_ref().err()
    );

    // Verify in database that topic is gone
    let db = create_db_client().await?;
    let count: i64 = db
        .query_one(
            "SELECT COUNT(*) FROM kafka.topics WHERE name = $1",
            &[&topic_name],
        )
        .await?
        .get(0);

    assert_eq!(count, 0, "Topic should be deleted from database");

    println!("    Deleted topic '{}'", topic_name);
    Ok(())
}

/// Test deleting a non-existent topic
pub async fn test_delete_topic_not_found() -> TestResult {
    let admin = create_admin_client()?;
    let topic_name = format!("nonexistent-topic-{}", Uuid::new_v4());

    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(5)));

    let results = admin.delete_topics(&[&topic_name], &opts).await?;
    assert_eq!(results.len(), 1);

    // Should fail with UNKNOWN_TOPIC_OR_PARTITION
    let result = &results[0];
    assert!(result.is_err(), "Deleting non-existent topic should fail");

    let err = result.as_ref().unwrap_err();
    assert!(
        format!("{:?}", err).contains("UnknownTopicOrPartition")
            || format!("{:?}", err).contains("3")
            || format!("{:?}", err).contains("does not exist"),
        "Expected UNKNOWN_TOPIC_OR_PARTITION error, got: {:?}",
        err
    );

    println!(
        "    Correctly rejected deletion of non-existent topic '{}'",
        topic_name
    );
    Ok(())
}

/// Test increasing partition count for a topic
pub async fn test_create_partitions() -> TestResult {
    let admin = create_admin_client()?;
    let topic_name = format!("test-create-partitions-{}", Uuid::new_v4());

    // Create topic with 2 partitions
    let new_topic = NewTopic::new(&topic_name, 2, TopicReplication::Fixed(1));
    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(5)));

    let results = admin.create_topics(&[new_topic], &opts).await?;
    assert!(results[0].is_ok(), "Topic creation should succeed");

    // Increase to 5 partitions
    let new_partitions = NewPartitions::new(&topic_name, 5);
    let results = admin.create_partitions(&[new_partitions], &opts).await?;
    assert_eq!(results.len(), 1);

    let result = &results[0];
    assert!(
        result.is_ok(),
        "Failed to increase partitions: {:?}",
        result.as_ref().err()
    );

    // Verify in database
    let db = create_db_client().await?;
    let row = db
        .query_one(
            "SELECT partitions FROM kafka.topics WHERE name = $1",
            &[&topic_name],
        )
        .await?;

    let partitions: i32 = row.get(0);
    assert_eq!(partitions, 5, "Topic should have 5 partitions now");

    // Cleanup
    let _ = admin
        .delete_topics(&[&topic_name], &AdminOptions::new())
        .await;

    println!("    Increased partitions for '{}' from 2 to 5", topic_name);
    Ok(())
}

/// Test that partition count cannot be decreased
pub async fn test_create_partitions_cannot_decrease() -> TestResult {
    let admin = create_admin_client()?;
    let topic_name = format!("test-partitions-decrease-{}", Uuid::new_v4());

    // Create topic with 5 partitions
    let new_topic = NewTopic::new(&topic_name, 5, TopicReplication::Fixed(1));
    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(5)));

    let results = admin.create_topics(&[new_topic], &opts).await?;
    assert!(results[0].is_ok(), "Topic creation should succeed");

    // Try to decrease to 3 partitions
    let new_partitions = NewPartitions::new(&topic_name, 3);
    let results = admin.create_partitions(&[new_partitions], &opts).await?;
    assert_eq!(results.len(), 1);

    // Should fail with INVALID_PARTITIONS
    let result = &results[0];
    assert!(result.is_err(), "Decreasing partitions should fail");

    let err = result.as_ref().unwrap_err();
    assert!(
        format!("{:?}", err).contains("InvalidPartitions")
            || format!("{:?}", err).contains("37")
            || format!("{:?}", err).contains("reduce")
            || format!("{:?}", err).contains("Cannot"),
        "Expected INVALID_PARTITIONS error, got: {:?}",
        err
    );

    // Cleanup
    let _ = admin
        .delete_topics(&[&topic_name], &AdminOptions::new())
        .await;

    println!(
        "    Correctly rejected partition decrease for '{}'",
        topic_name
    );
    Ok(())
}

/// Test deleting an empty consumer group
pub async fn test_delete_group_empty() -> TestResult {
    let admin = create_admin_client()?;
    // Use a group that doesn't exist - should succeed (no-op)
    let group_id = format!("test-delete-group-{}", Uuid::new_v4());

    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(5)));

    let results = admin.delete_groups(&[&group_id], &opts).await?;
    assert_eq!(results.len(), 1);

    // Deleting a non-existent group should succeed (it's empty)
    let result = &results[0];
    assert!(
        result.is_ok(),
        "Deleting empty/non-existent group should succeed: {:?}",
        result.as_ref().err()
    );

    println!("    Successfully deleted empty group '{}'", group_id);
    Ok(())
}

/// Test that deleting a group with active members fails
pub async fn test_delete_group_non_empty() -> TestResult {
    use crate::common::create_stream_consumer;
    use rdkafka::consumer::Consumer;

    let admin = create_admin_client()?;
    let topic_name = format!("test-delete-group-nonempty-{}", Uuid::new_v4());
    let group_id = format!("test-group-nonempty-{}", Uuid::new_v4());

    // Create a topic first
    let new_topic = NewTopic::new(&topic_name, 1, TopicReplication::Fixed(1));
    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(5)));
    let _ = admin.create_topics(&[new_topic], &opts).await?;

    // Create a consumer that joins the group
    let consumer = create_stream_consumer(&group_id)?;
    consumer.subscribe(&[&topic_name])?;

    // Wait for the consumer to join the group
    // StreamConsumer handles the join protocol in the background when subscribed
    // We need to give it enough time to complete the JoinGroup/SyncGroup exchange
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Try to delete the group while consumer is active
    let results = admin.delete_groups(&[&group_id], &opts).await?;
    assert_eq!(results.len(), 1);

    // Should fail with NON_EMPTY_GROUP
    let result = &results[0];
    assert!(
        result.is_err(),
        "Deleting non-empty group should fail: {:?}",
        result.as_ref().ok()
    );

    let err = result.as_ref().unwrap_err();
    assert!(
        format!("{:?}", err).contains("NonEmptyGroup")
            || format!("{:?}", err).contains("68")
            || format!("{:?}", err).contains("active"),
        "Expected NON_EMPTY_GROUP error, got: {:?}",
        err
    );

    // Consumer will be dropped here, leaving the group
    drop(consumer);

    // Cleanup
    let _ = admin
        .delete_topics(&[&topic_name], &AdminOptions::new())
        .await;

    println!(
        "    Correctly rejected deletion of non-empty group '{}'",
        group_id
    );
    Ok(())
}

/// Test creating multiple topics in a single request
pub async fn test_create_multiple_topics() -> TestResult {
    let admin = create_admin_client()?;
    let topic1 = format!("test-multi-topic-1-{}", Uuid::new_v4());
    let topic2 = format!("test-multi-topic-2-{}", Uuid::new_v4());
    let topic3 = format!("test-multi-topic-3-{}", Uuid::new_v4());

    let topics = vec![
        NewTopic::new(&topic1, 1, TopicReplication::Fixed(1)),
        NewTopic::new(&topic2, 2, TopicReplication::Fixed(1)),
        NewTopic::new(&topic3, 3, TopicReplication::Fixed(1)),
    ];

    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(5)));

    let results = admin.create_topics(&topics, &opts).await?;
    assert_eq!(results.len(), 3);

    for result in &results {
        assert!(result.is_ok(), "Topic creation failed: {:?}", result);
    }

    // Verify in database
    let db = create_db_client().await?;
    let count: i64 = db
        .query_one(
            "SELECT COUNT(*) FROM kafka.topics WHERE name IN ($1, $2, $3)",
            &[&topic1, &topic2, &topic3],
        )
        .await?
        .get(0);

    assert_eq!(count, 3, "All 3 topics should exist in database");

    // Cleanup
    let _ = admin
        .delete_topics(&[&topic1, &topic2, &topic3], &AdminOptions::new())
        .await;

    println!("    Created 3 topics in single request");
    Ok(())
}
