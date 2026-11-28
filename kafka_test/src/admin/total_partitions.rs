//! `pg_kafka.max_total_partitions`: the partitions of all topics together.

use crate::common::{DynResult, TestResult};
use crate::error_codes;
use crate::setup::{GucError, SystemGucGuard, TestContext};
use crate::wire::records::plain_batch;
use crate::wire::requests::{conn, creatable, produce_request, s};
use crate::wire::roundtrip;
use bytes::Bytes;
use kafka_protocol::messages::create_partitions_request::{
    CreatePartitionsRequest, CreatePartitionsTopic,
};
use kafka_protocol::messages::create_topics_request::CreateTopicsRequest;
use kafka_protocol::messages::metadata_request::{MetadataRequest, MetadataRequestTopic};
use kafka_protocol::messages::TopicName;
use kafka_protocol::records::Compression;
use tokio::net::TcpStream;
use tokio_postgres::Client;

const GUC: &str = "pg_kafka.max_total_partitions";

/// The guard's errors are `Send + Sync`; this file's results are not.
fn widen(e: GucError) -> Box<dyn std::error::Error> {
    e
}

/// The partitions of all topics.
async fn total(db: &Client) -> DynResult<i64> {
    Ok(db
        .query_one(
            "SELECT coalesce(sum(partitions), 0)::int8 FROM kafka.topics",
            &[],
        )
        .await?
        .get(0))
}

async fn exists(db: &Client, topic: &str) -> DynResult<bool> {
    Ok(db
        .query_opt("SELECT 1 FROM kafka.topics WHERE name = $1", &[&topic])
        .await?
        .is_some())
}

/// CreateTopics v5 of `(name, partitions)`: each topic's error code.
async fn create(
    c: &mut TcpStream,
    topics: &[(&str, i32)],
    validate_only: bool,
) -> DynResult<Vec<i16>> {
    let mut req = CreateTopicsRequest::default();
    req.topics = topics
        .iter()
        .map(|(name, partitions)| creatable(name, *partitions, 1))
        .collect();
    req.timeout_ms = 10_000;
    req.validate_only = validate_only;
    let resp = roundtrip(c, 5, 1, &req).await?;
    Ok(resp.topics.iter().map(|t| t.error_code).collect())
}

/// CreatePartitions v1 of one topic to `count`: its error code.
async fn grow(c: &mut TcpStream, topic: &str, count: i32) -> DynResult<i16> {
    let mut t = CreatePartitionsTopic::default();
    t.name = TopicName(s(topic));
    t.count = count;
    let mut req = CreatePartitionsRequest::default();
    req.topics = vec![t];
    req.timeout_ms = 10_000;
    Ok(roundtrip(c, 1, 2, &req).await?.results[0].error_code)
}

/// Metadata v9 of one topic with auto-creation: its error code.
async fn metadata_auto(c: &mut TcpStream, topic: &str) -> DynResult<i16> {
    let mut m = MetadataRequestTopic::default();
    m.name = Some(TopicName(s(topic)));
    let mut req = MetadataRequest::default();
    req.topics = Some(vec![m]);
    req.allow_auto_topic_creation = true;
    Ok(roundtrip(c, 9, 3, &req).await?.topics[0].error_code)
}

/// The review's reproduction, scaled down: partitions cost nothing to create and only the
/// per-topic count was capped, so one CreateTopics of 10,000 topics of 10,000 partitions
/// made 1e8 partitions, and every all-topics Metadata then allocated one entry per partition
/// (about 17 GB) on the DB thread: an allocation failure aborts the worker, sends Postgres
/// into crash recovery, and does again after every restart. With the cap set to what exists
/// plus 10, CreateTopics, a validate_only CreateTopics, CreatePartitions and auto-creation by
/// Metadata and Produce each go up to the cap and no further, answering POLICY_VIOLATION and
/// creating nothing past it.
pub async fn test_total_partitions_are_capped() -> TestResult {
    println!("=== Test: pg_kafka.max_total_partitions caps all topics' partitions ===\n");
    let ctx = TestContext::new().await?;
    let base = total(ctx.db()).await?;
    let cap = base + 10;
    let guard = SystemGucGuard::set(ctx.db(), GUC, &cap.to_string())
        .await
        .map_err(widen)?
        .ok_or_else(|| format!("the server refused {GUC} = {cap}"))?;
    run(&ctx, cap).await?;
    guard.restore().await.map_err(widen)?;
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

async fn run(ctx: &TestContext, cap: i64) -> DynResult<()> {
    let db = ctx.db();
    let topic_a = ctx.unique_topic("cap-a").await;
    let topic_b = ctx.unique_topic("cap-b").await;
    let topic_c = ctx.unique_topic("cap-c").await;
    let topic_d = ctx.unique_topic("cap-d").await;
    let topic_e = ctx.unique_topic("cap-e").await;
    let mut client = conn().await?;

    println!("Step 1: two topics of 6 partitions with room for 10: the first fits...");
    let codes = create(&mut client, &[(&topic_a, 6), (&topic_b, 6)], false).await?;
    assert_eq!(codes, [error_codes::NONE, error_codes::POLICY_VIOLATION]);
    assert!(exists(db, &topic_a).await? && !exists(db, &topic_b).await?);

    println!("Step 2: validate_only is held to the same cap...");
    assert_eq!(
        create(&mut client, &[(&topic_c, 4)], true).await?,
        [error_codes::NONE]
    );
    assert_eq!(
        create(&mut client, &[(&topic_c, 5)], true).await?,
        [error_codes::POLICY_VIOLATION]
    );
    assert!(!exists(db, &topic_c).await?);

    println!("Step 3: CreatePartitions grows only within the cap...");
    assert_eq!(
        grow(&mut client, &topic_a, 11).await?,
        error_codes::POLICY_VIOLATION
    );
    assert_eq!(grow(&mut client, &topic_a, 10).await?, error_codes::NONE);
    assert_eq!(total(db).await?, cap);

    println!("Step 4: at the cap, Metadata does not auto-create...");
    assert_eq!(
        metadata_auto(&mut client, &topic_d).await?,
        error_codes::POLICY_VIOLATION
    );
    assert!(!exists(db, &topic_d).await?);

    println!("Step 5: nor does Produce...");
    let req = produce_request(
        &topic_e,
        vec![(
            0,
            plain_batch(&[Bytes::from_static(b"v")], Compression::None),
        )],
        1,
    );
    let resp = roundtrip(&mut client, 7, 4, &req).await?;
    assert_eq!(
        resp.responses[0].partition_responses[0].error_code,
        error_codes::POLICY_VIOLATION
    );
    assert!(!exists(db, &topic_e).await?);
    assert_eq!(total(db).await?, cap);
    Ok(())
}
