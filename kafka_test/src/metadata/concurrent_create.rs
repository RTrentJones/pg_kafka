//! Metadata auto-creation racing another session's insert of the same topic.

use crate::common::{create_db_client, TestResult};
use crate::setup::TestContext;
use crate::wire::Wire;
use kafka_protocol::messages::metadata_request::{MetadataRequest, MetadataRequestTopic};
use kafka_protocol::messages::{MetadataResponse, TopicName};
use kafka_protocol::protocol::StrBytes;
use std::time::Duration;

/// Metadata with auto-creation of a name another session inserts and commits while the
/// broker's insert waits on it reports the topic that session created. The insert skips
/// the name on the conflict and the rest of its statement reads a snapshot from before
/// that commit, so the topic was answered UNKNOWN_TOPIC_OR_PARTITION although it existed.
pub async fn test_metadata_auto_create_sees_a_concurrently_created_topic() -> TestResult {
    println!("=== Test: Metadata auto-create racing another session's insert ===\n");
    let ctx = TestContext::new().await?;
    let raced = ctx.unique_topic("race").await;
    let fresh = ctx.unique_topic("race-fresh").await;

    println!("Step 1: Another session inserts {raced} (3 partitions) and holds it open...");
    let other = create_db_client().await?;
    other.batch_execute("BEGIN").await?;
    other
        .execute(
            "INSERT INTO kafka.topics (name, partitions) VALUES ($1, 3)",
            &[&raced],
        )
        .await?;
    let commit = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(800)).await;
        other.batch_execute("COMMIT").await?;
        Ok::<_, tokio_postgres::Error>(())
    });

    println!("Step 2: Metadata v4 (auto-create) of it and of a missing topic...");
    let mut req = MetadataRequest::default();
    req.topics = Some(
        [&raced, &fresh]
            .iter()
            .map(|n| {
                let mut t = MetadataRequestTopic::default();
                t.name = Some(TopicName(StrBytes::from_string((*n).clone())));
                t
            })
            .collect(),
    );
    req.allow_auto_topic_creation = true;
    let mut w = Wire::connect().await?;
    let resp: MetadataResponse = w.call(4, &req).await?;
    commit.await??;
    let answers: Vec<(String, i16, usize)> = resp
        .topics
        .iter()
        .map(|t| {
            let name = t
                .name
                .as_ref()
                .map_or_else(String::new, |n| n.0.to_string());
            (name, t.error_code, t.partitions.len())
        })
        .collect();
    println!("  answered {answers:?}");
    let raced_answer = answers.iter().find(|a| a.0 == raced).ok_or("no entry")?;
    assert_eq!(
        (raced_answer.1, raced_answer.2),
        (0, 3),
        "the topic the other session created, with its 3 partitions"
    );
    let fresh_answer = answers.iter().find(|a| a.0 == fresh).ok_or("no entry")?;
    assert_eq!(fresh_answer.1, 0, "the missing topic is auto-created");

    println!("=== Database Verification ===\n");
    let rows = ctx
        .db()
        .query(
            "SELECT partitions FROM kafka.topics WHERE name = $1",
            &[&raced],
        )
        .await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 3);
    println!("✅ One {raced} row, the other session's\n");
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
