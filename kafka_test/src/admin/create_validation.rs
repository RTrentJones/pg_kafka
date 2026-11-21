//! CreateTopics / CreatePartitions validation.

use crate::common::create_admin_client;
use crate::common::TestResult;
use crate::error_codes;
use crate::setup::TestContext;
use crate::wire::requests::{conn, creatable, create_topics_raw, metadata_raw, s};
use crate::wire::roundtrip;
use kafka_protocol::messages::create_partitions_request::{
    CreatePartitionsRequest, CreatePartitionsTopic,
};
use kafka_protocol::messages::TopicName;
use rdkafka::admin::{AdminOptions, NewPartitions, NewTopic, TopicReplication};
use rdkafka::types::RDKafkaErrorCode;
use std::time::Duration;

/// CreateTopics / CreatePartitions refuse partition counts above
/// `pg_kafka.max_partitions_per_topic` (default 10000) with INVALID_PARTITIONS, through the
/// real rdkafka admin client and raw requests, and nothing is stored; the schema's CHECK
/// constraint backs it up. A stored count of 2,000,000,000 would make every Metadata
/// request try to allocate ~224 GB and abort the worker.
pub async fn test_partition_count_above_cap_is_rejected() -> TestResult {
    println!("=== Test: partition counts are capped ===\n");
    let ctx = TestContext::new().await?;
    let huge = ctx.unique_topic("huge").await;
    let over = ctx.unique_topic("over").await;
    let at_cap = ctx.unique_topic("atcap").await;

    // librdkafka refuses counts above 100000 client-side, so 2e9 goes over a raw request
    // (any other client, or a hostile one, can send it).
    let r = create_topics_raw(vec![creatable(&huge, 2_000_000_000, 1)]).await?;
    println!("raw CreateTopics 2e9: {r:?}");
    assert_eq!(r[0].error_code, error_codes::INVALID_PARTITIONS);

    let admin = create_admin_client()?;
    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(10)));
    let results = admin
        .create_topics(
            &[
                NewTopic::new(&over, 10_001, TopicReplication::Fixed(1)),
                NewTopic::new(&at_cap, 1, TopicReplication::Fixed(1)),
            ],
            &opts,
        )
        .await?;
    match &results[0] {
        Err((_, RDKafkaErrorCode::InvalidPartitions)) => {}
        other => return Err(format!("over: expected InvalidPartitions, got {other:?}").into()),
    }
    assert!(results[1].is_ok(), "{:?}", results[1]);
    println!("✅ CreateTopics with 2e9 (raw) / 10001 (rdkafka) partitions -> INVALID_PARTITIONS\n");

    // CreatePartitions growth is capped too.
    let grow = admin
        .create_partitions(&[NewPartitions::new(&at_cap, 10_001)], &opts)
        .await?;
    match &grow[0] {
        Err((_, RDKafkaErrorCode::InvalidPartitions)) => {}
        other => return Err(format!("grow: expected InvalidPartitions, got {other:?}").into()),
    }
    let mut raw = CreatePartitionsRequest::default();
    let mut t = CreatePartitionsTopic::default();
    t.name = TopicName(s(&at_cap));
    t.count = 2_000_000_000;
    raw.topics = vec![t];
    raw.timeout_ms = 5_000;
    let mut c = conn().await?;
    let resp = roundtrip(&mut c, 1, 1, &raw).await?;
    assert_eq!(resp.results[0].error_code, error_codes::INVALID_PARTITIONS);
    println!("✅ CreatePartitions to 10001 (rdkafka) / 2e9 (raw) -> INVALID_PARTITIONS\n");

    let row = ctx
        .db()
        .query_one(
            "SELECT count(*) FILTER (WHERE name IN ($1, $2)), max(partitions) FILTER (WHERE name = $3)
             FROM kafka.topics",
            &[&huge, &over, &at_cap],
        )
        .await?;
    assert_eq!(row.get::<_, i64>(0), 0, "refused topics must not be stored");
    assert_eq!(row.get::<_, Option<i32>>(1), Some(1));

    // The schema backstop.
    let err = ctx
        .db()
        .execute(
            "INSERT INTO kafka.topics (name, partitions) VALUES ($1, 2000000000)",
            &[&huge],
        )
        .await
        .expect_err("CHECK constraint must refuse 2e9 partitions");
    assert!(
        err.to_string().contains("topics_partitions_range")
            || format!("{err:?}").contains("topics_partitions_range"),
        "{err:?}"
    );
    println!("✅ CHECK (partitions BETWEEN 1 AND 100000) refuses a direct insert\n");

    // The broker is alive and serves metadata.
    let mut c = conn().await?;
    let md = metadata_raw(&mut c, &[&at_cap]).await?;
    assert_eq!(md[0].error_code, error_codes::NONE);
    assert_eq!(md[0].partitions, 1);
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// CreateTopics validation as Kafka 3.7 answers it (checked against a real broker):
/// a name repeated in one request -> one INVALID_REQUEST "Duplicate topic name." result;
/// `x.N` after `x_N` (or the reverse) -> INVALID_TOPIC_EXCEPTION "collides with existing topic";
/// replication factor 3 -> INVALID_REPLICATION_FACTOR, 0 / -2 -> INVALID_REPLICATION_FACTOR,
/// -1 -> created.
pub async fn test_create_topics_rejects_duplicates_collisions_and_bad_replication() -> TestResult {
    println!("=== Test: CreateTopics validation ===\n");
    let ctx = TestContext::new().await?;
    let base = ctx.unique_topic("val").await.replace('-', "");
    let dup = format!("{base}-dup");
    let other = format!("{base}-other");
    let dotted = format!("{base}.col");
    let underscored = format!("{base}_col");

    let r = create_topics_raw(vec![
        creatable(&dup, 1, 1),
        creatable(&other, 1, 1),
        creatable(&dup, 1, 1),
    ])
    .await?;
    println!("duplicates: {r:?}");
    assert_eq!(r.len(), 2, "one result per distinct name");
    let d = r.iter().find(|t| t.name == dup).unwrap();
    assert_eq!(d.error_code, error_codes::INVALID_REQUEST);
    assert_eq!(d.error_message.as_deref(), Some("Duplicate topic name."));
    assert_eq!(
        r.iter().find(|t| t.name == other).unwrap().error_code,
        error_codes::NONE
    );

    let r = create_topics_raw(vec![creatable(&dotted, 1, 1)]).await?;
    assert_eq!(r[0].error_code, error_codes::NONE);
    let r = create_topics_raw(vec![creatable(&underscored, 1, 1)]).await?;
    println!("collision: {r:?}");
    assert_eq!(r[0].error_code, error_codes::INVALID_TOPIC_EXCEPTION);
    assert_eq!(
        r[0].error_message.as_deref(),
        Some(format!("Topic '{underscored}' collides with existing topic: {dotted}").as_str())
    );

    let rf = |n: &str| format!("{base}-rf{n}");
    let r = create_topics_raw(vec![
        creatable(&rf("3"), 1, 3),
        creatable(&rf("0"), 1, 0),
        creatable(&rf("m2"), 1, -2),
        creatable(&rf("m1"), 1, -1),
        creatable(&rf("p0"), 0, 1),
    ])
    .await?;
    println!("replication factors: {r:?}");
    let code = |n: &str| r.iter().find(|t| t.name == rf(n)).unwrap().error_code;
    assert_eq!(code("3"), error_codes::INVALID_REPLICATION_FACTOR);
    assert_eq!(code("0"), error_codes::INVALID_REPLICATION_FACTOR);
    assert_eq!(code("m2"), error_codes::INVALID_REPLICATION_FACTOR);
    assert_eq!(code("m1"), error_codes::NONE);
    assert_eq!(code("p0"), error_codes::INVALID_PARTITIONS);

    // The real admin client sees the same.
    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(10)));
    let admin = create_admin_client()?;
    let res = admin
        .create_topics(
            &[NewTopic::new(&rf("3b"), 1, TopicReplication::Fixed(3))],
            &opts,
        )
        .await?;
    match &res[0] {
        Err((_, RDKafkaErrorCode::InvalidReplicationFactor)) => {}
        other => return Err(format!("expected InvalidReplicationFactor, got {other:?}").into()),
    }

    let names = vec![
        dup.clone(),
        underscored.clone(),
        rf("3"),
        rf("0"),
        rf("m2"),
        rf("p0"),
        rf("3b"),
    ];
    let stored: i64 = ctx
        .db()
        .query_one(
            "SELECT count(*) FROM kafka.topics WHERE name = ANY($1)",
            &[&names],
        )
        .await?
        .get(0);
    assert_eq!(stored, 0, "no refused topic is stored");
    ctx.db()
        .execute(
            "DELETE FROM kafka.topics WHERE name = ANY($1)",
            &[&vec![other, dotted, rf("m1")]],
        )
        .await?;
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
