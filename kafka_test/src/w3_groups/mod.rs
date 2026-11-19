//! Wave 3, stream `groups`: consumer groups, admin validation and resource bounds.
//!
//! Each test checks a Kafka 3.7 behaviour (verified against a real Kafka 3.7 broker) that
//! pg_kafka used to get wrong:
//! - partition counts are capped (a 2,000,000,000-partition topic used to abort the worker on
//!   every Metadata request and send Postgres into crash recovery), and a corrupt stored count
//!   no longer takes the server down;
//! - CreateTopics validation (duplicates, '.'/'_' collisions, replication factor);
//! - groups known only by their committed offsets are Empty groups for ListGroups /
//!   DescribeGroups, also after a worker restart, and DeleteGroups answers GROUP_ID_NOT_FOUND
//!   for a group that does not exist;
//! - INCONSISTENT_GROUP_PROTOCOL, UNKNOWN_MEMBER_ID for unknown member ids, OffsetCommit rules.

use crate::common::{create_db_client, create_producer, get_bootstrap_servers, TestResult};
use crate::error_paths::bug_hunt::roundtrip;
use crate::setup::TestContext;
use kafka_protocol::messages::create_partitions_request::{
    CreatePartitionsRequest, CreatePartitionsTopic,
};
use kafka_protocol::messages::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use kafka_protocol::messages::delete_groups_request::DeleteGroupsRequest;
use kafka_protocol::messages::describe_groups_request::DescribeGroupsRequest;
use kafka_protocol::messages::join_group_request::{JoinGroupRequest, JoinGroupRequestProtocol};
use kafka_protocol::messages::leave_group_request::LeaveGroupRequest;
use kafka_protocol::messages::list_groups_request::ListGroupsRequest;
use kafka_protocol::messages::metadata_request::{MetadataRequest, MetadataRequestTopic};
use kafka_protocol::messages::offset_commit_request::{
    OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
use kafka_protocol::messages::sync_group_request::{SyncGroupRequest, SyncGroupRequestAssignment};
use kafka_protocol::messages::{GroupId, TopicName};
use kafka_protocol::protocol::StrBytes;
use rdkafka::admin::{AdminClient, AdminOptions, NewPartitions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::producer::FutureRecord;
use rdkafka::types::RDKafkaErrorCode;
use rdkafka::TopicPartitionList;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;

const BROKER: &str = "127.0.0.1:9092";
const ERROR_NONE: i16 = 0;
const ERROR_UNKNOWN_SERVER_ERROR: i16 = -1;
const ERROR_ILLEGAL_GENERATION: i16 = 22;
const ERROR_INCONSISTENT_GROUP_PROTOCOL: i16 = 23;
const ERROR_UNKNOWN_MEMBER_ID: i16 = 25;
const ERROR_INVALID_PARTITIONS: i16 = 37;
const ERROR_INVALID_REPLICATION_FACTOR: i16 = 38;
const ERROR_INVALID_REQUEST: i16 = 42;
const ERROR_INVALID_TOPIC_EXCEPTION: i16 = 17;
const ERROR_GROUP_ID_NOT_FOUND: i16 = 69;

type DynResult<T> = Result<T, Box<dyn std::error::Error>>;

fn admin() -> DynResult<AdminClient<DefaultClientContext>> {
    Ok(ClientConfig::new()
        .set("bootstrap.servers", get_bootstrap_servers())
        .set("broker.address.family", "v4")
        .create()?)
}

fn s(v: &str) -> StrBytes {
    StrBytes::from_string(v.to_string())
}

async fn conn() -> DynResult<TcpStream> {
    Ok(TcpStream::connect(BROKER).await?)
}

fn creatable(name: &str, partitions: i32, rf: i16) -> CreatableTopic {
    let mut t = CreatableTopic::default();
    t.name = TopicName(s(name));
    t.num_partitions = partitions;
    t.replication_factor = rf;
    t
}

/// CreateTopics v5 over a raw connection; `(name, error_code, message)` per result.
async fn create_topics_raw(
    topics: Vec<CreatableTopic>,
) -> DynResult<Vec<(String, i16, Option<String>)>> {
    let mut c = conn().await?;
    let mut req = CreateTopicsRequest::default();
    req.topics = topics;
    req.timeout_ms = 10_000;
    let resp = roundtrip(&mut c, 5, 1, &req).await?;
    Ok(resp
        .topics
        .into_iter()
        .map(|t| {
            (
                t.name.0.to_string(),
                t.error_code,
                t.error_message.map(|m| m.to_string()),
            )
        })
        .collect())
}

async fn metadata_raw(c: &mut TcpStream, topics: &[&str]) -> DynResult<Vec<(String, i16, usize)>> {
    let mut req = MetadataRequest::default();
    req.topics = Some(
        topics
            .iter()
            .map(|t| {
                let mut m = MetadataRequestTopic::default();
                m.name = Some(TopicName(s(t)));
                m
            })
            .collect(),
    );
    req.allow_auto_topic_creation = false;
    let resp = roundtrip(c, 9, 2, &req).await?;
    Ok(resp
        .topics
        .into_iter()
        .map(|t| {
            (
                t.name.map(|n| n.0.to_string()).unwrap_or_default(),
                t.error_code,
                t.partitions.len(),
            )
        })
        .collect())
}

fn protocol(name: &str) -> JoinGroupRequestProtocol {
    let mut p = JoinGroupRequestProtocol::default();
    p.name = s(name);
    p.metadata = bytes::Bytes::from_static(b"\x00\x01");
    p
}

fn join_req(
    group: &str,
    member: &str,
    protocol_type: &str,
    protocols: &[&str],
) -> JoinGroupRequest {
    let mut r = JoinGroupRequest::default();
    r.group_id = GroupId(s(group));
    r.session_timeout_ms = 10_000;
    r.rebalance_timeout_ms = 10_000;
    r.member_id = s(member);
    r.protocol_type = s(protocol_type);
    r.protocols = protocols.iter().map(|p| protocol(p)).collect();
    r
}

/// JoinGroup v5, answered at once (a lone member, or an error). `(error, generation, member,
/// protocol_name)`.
async fn join_now(
    c: &mut TcpStream,
    req: &JoinGroupRequest,
) -> DynResult<(i16, i32, String, Option<String>)> {
    let r = tokio::time::timeout(Duration::from_secs(15), roundtrip(c, 5, 3, req)).await??;
    Ok((
        r.error_code,
        r.generation_id,
        r.member_id.to_string(),
        r.protocol_name.map(|p| p.to_string()),
    ))
}

async fn sync_leader(
    c: &mut TcpStream,
    group: &str,
    generation: i32,
    member: &str,
) -> DynResult<i16> {
    let mut a = SyncGroupRequestAssignment::default();
    a.member_id = s(member);
    a.assignment = bytes::Bytes::from_static(b"");
    let mut r = SyncGroupRequest::default();
    r.group_id = GroupId(s(group));
    r.generation_id = generation;
    r.member_id = s(member);
    r.assignments = vec![a];
    Ok(roundtrip(c, 3, 4, &r).await?.error_code)
}

async fn leave(c: &mut TcpStream, group: &str, member: &str) -> DynResult<i16> {
    let mut r = LeaveGroupRequest::default();
    r.group_id = GroupId(s(group));
    r.member_id = s(member);
    Ok(roundtrip(c, 1, 5, &r).await?.error_code)
}

/// OffsetCommit v2 of one offset; the partition's error code.
async fn commit(
    group: &str,
    generation: i32,
    member: &str,
    topic: &str,
    offset: i64,
) -> DynResult<i16> {
    let mut c = conn().await?;
    let mut p = OffsetCommitRequestPartition::default();
    p.partition_index = 0;
    p.committed_offset = offset;
    let mut t = OffsetCommitRequestTopic::default();
    t.name = TopicName(s(topic));
    t.partitions = vec![p];
    let mut r = OffsetCommitRequest::default();
    r.group_id = GroupId(s(group));
    r.generation_id_or_member_epoch = generation;
    r.member_id = s(member);
    r.retention_time_ms = -1;
    r.topics = vec![t];
    let resp = roundtrip(&mut c, 2, 6, &r).await?;
    Ok(resp.topics[0].partitions[0].error_code)
}

/// DescribeGroups v5: `(error, state, protocol_type, members)`.
async fn describe(group: &str) -> DynResult<(i16, String, String, usize)> {
    let mut c = conn().await?;
    let mut r = DescribeGroupsRequest::default();
    r.groups = vec![GroupId(s(group))];
    let resp = roundtrip(&mut c, 5, 7, &r).await?;
    let g = &resp.groups[0];
    Ok((
        g.error_code,
        g.group_state.to_string(),
        g.protocol_type.to_string(),
        g.members.len(),
    ))
}

/// ListGroups v4: `(group, protocol_type, state)` for every group.
async fn list_groups() -> DynResult<Vec<(String, String, String)>> {
    let mut c = conn().await?;
    let resp = roundtrip(&mut c, 4, 8, &ListGroupsRequest::default()).await?;
    Ok(resp
        .groups
        .into_iter()
        .map(|g| {
            (
                g.group_id.0.to_string(),
                g.protocol_type.to_string(),
                g.group_state.to_string(),
            )
        })
        .collect())
}

async fn delete_group(group: &str) -> DynResult<i16> {
    let mut c = conn().await?;
    let mut r = DeleteGroupsRequest::default();
    r.groups_names = vec![GroupId(s(group))];
    Ok(roundtrip(&mut c, 1, 9, &r).await?.results[0].error_code)
}

async fn produce_one(topic: &str) -> TestResult {
    let producer = create_producer()?;
    producer
        .send(
            FutureRecord::to(topic).payload("v").key("k"),
            Duration::from_secs(10),
        )
        .await
        .map_err(|(e, _)| e)?;
    Ok(())
}

/// Item 1: CreateTopics / CreatePartitions refuse partition counts above
/// `pg_kafka.max_partitions_per_topic` (default 10000) with INVALID_PARTITIONS, through the real
/// rdkafka admin client and raw requests, and nothing is stored. The schema's CHECK constraint
/// backs it up. Before: 2,000,000,000 was accepted and stored, and the next Metadata request
/// aborted the worker (memory allocation of 224000000000 bytes failed, signal 6, crash
/// recovery, repeated on every Metadata as long as the row existed).
pub async fn test_w3g_partition_cap() -> TestResult {
    println!("=== Test: partition counts are capped ===\n");
    let ctx = TestContext::new().await?;
    let huge = ctx.unique_topic("w3g-huge").await;
    let over = ctx.unique_topic("w3g-over").await;
    let at_cap = ctx.unique_topic("w3g-atcap").await;

    // librdkafka refuses counts above 100000 client-side, so 2e9 goes over a raw request
    // (any other client, or a hostile one, can send it).
    let r = create_topics_raw(vec![creatable(&huge, 2_000_000_000, 1)]).await?;
    println!("raw CreateTopics 2e9: {:?}", r);
    assert_eq!(r[0].1, ERROR_INVALID_PARTITIONS);

    let admin = admin()?;
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
        other => return Err(format!("over: expected InvalidPartitions, got {:?}", other).into()),
    }
    assert!(results[1].is_ok(), "{:?}", results[1]);
    println!("✅ CreateTopics with 2e9 (raw) / 10001 (rdkafka) partitions -> INVALID_PARTITIONS\n");

    // CreatePartitions growth is capped too.
    let grow = admin
        .create_partitions(&[NewPartitions::new(&at_cap, 10_001)], &opts)
        .await?;
    match &grow[0] {
        Err((_, RDKafkaErrorCode::InvalidPartitions)) => {}
        other => return Err(format!("grow: expected InvalidPartitions, got {:?}", other).into()),
    }
    let mut raw = CreatePartitionsRequest::default();
    let mut t = CreatePartitionsTopic::default();
    t.name = TopicName(s(&at_cap));
    t.count = 2_000_000_000;
    raw.topics = vec![t];
    raw.timeout_ms = 5_000;
    let mut c = conn().await?;
    let resp = roundtrip(&mut c, 1, 1, &raw).await?;
    assert_eq!(resp.results[0].error_code, ERROR_INVALID_PARTITIONS);
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
            || format!("{:?}", err).contains("topics_partitions_range"),
        "{:?}",
        err
    );
    println!("✅ CHECK (partitions BETWEEN 1 AND 100000) refuses a direct insert\n");

    // The broker is alive and serves metadata.
    let mut c = conn().await?;
    let md = metadata_raw(&mut c, &[&at_cap]).await?;
    assert_eq!(md[0].1, ERROR_NONE);
    assert_eq!(md[0].2, 1);
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// Item 1: a corrupt stored partition count (only possible when the CHECK constraint is
/// bypassed) is reported as an error for that topic instead of being allocated; the worker and
/// the server stay up and other topics keep working. Sequential: drops and restores the
/// constraint for the duration of the test.
pub async fn test_w3g_metadata_survives_corrupt_partition_count() -> TestResult {
    println!("=== Test: Metadata survives a corrupt stored partition count ===\n");
    let ctx = TestContext::new().await?;
    let bad = ctx.unique_topic("w3g-corrupt").await;
    let good = ctx.unique_topic("w3g-good").await;
    produce_one(&good).await?;

    let db = create_db_client().await?;
    db.batch_execute("ALTER TABLE kafka.topics DROP CONSTRAINT topics_partitions_range")
        .await?;
    let outcome = async {
        db.execute(
            "INSERT INTO kafka.topics (name, partitions) VALUES ($1, 2000000000)",
            &[&bad],
        )
        .await?;
        let mut c = conn().await?;
        let md = metadata_raw(&mut c, &[&bad, &good]).await?;
        println!("Metadata: {:?}", md);
        assert_eq!(
            md[0].1, ERROR_UNKNOWN_SERVER_ERROR,
            "corrupt topic is an error"
        );
        assert_eq!(md[0].2, 0, "no partition entries for a corrupt count");
        assert_eq!(md[1].1, ERROR_NONE);
        assert_eq!(md[1].2, 1);
        // An all-topics Metadata (what every client sends on startup) too.
        let mut req = MetadataRequest::default();
        req.topics = None;
        let all = roundtrip(&mut c, 9, 3, &req).await?;
        assert!(all
            .topics
            .iter()
            .any(|t| t.name.as_ref().map(|n| n.0.as_str()) == Some(good.as_str())));
        // The same connection (and so the same worker) still answers.
        produce_one(&good).await?;
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await
    .map_err(|e| e.to_string());
    db.execute("DELETE FROM kafka.topics WHERE name = $1", &[&bad])
        .await?;
    db.batch_execute(
        "ALTER TABLE kafka.topics ADD CONSTRAINT topics_partitions_range
         CHECK (partitions >= 1 AND partitions <= 100000)",
    )
    .await?;
    outcome.map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// Item 5: CreateTopics validation as Kafka 3.7 answers it (checked against a real broker):
/// a name repeated in one request -> one INVALID_REQUEST "Duplicate topic name." result;
/// `x.N` after `x_N` (or the reverse) -> INVALID_TOPIC_EXCEPTION "collides with existing topic";
/// replication factor 3 -> INVALID_REPLICATION_FACTOR, 0 / -2 -> INVALID_REPLICATION_FACTOR,
/// -1 -> created. Before: duplicates were created then TOPIC_ALREADY_EXISTS, collisions and any
/// replication factor were accepted.
pub async fn test_w3g_create_topics_validation() -> TestResult {
    println!("=== Test: CreateTopics validation ===\n");
    let ctx = TestContext::new().await?;
    let base = ctx.unique_topic("w3g-val").await.replace('-', "");
    let dup = format!("{}-dup", base);
    let other = format!("{}-other", base);
    let dotted = format!("{}.col", base);
    let underscored = format!("{}_col", base);

    let r = create_topics_raw(vec![
        creatable(&dup, 1, 1),
        creatable(&other, 1, 1),
        creatable(&dup, 1, 1),
    ])
    .await?;
    println!("duplicates: {:?}", r);
    assert_eq!(r.len(), 2, "one result per distinct name");
    let d = r.iter().find(|t| t.0 == dup).unwrap();
    assert_eq!(d.1, ERROR_INVALID_REQUEST);
    assert_eq!(d.2.as_deref(), Some("Duplicate topic name."));
    assert_eq!(r.iter().find(|t| t.0 == other).unwrap().1, ERROR_NONE);

    let r = create_topics_raw(vec![creatable(&dotted, 1, 1)]).await?;
    assert_eq!(r[0].1, ERROR_NONE);
    let r = create_topics_raw(vec![creatable(&underscored, 1, 1)]).await?;
    println!("collision: {:?}", r);
    assert_eq!(r[0].1, ERROR_INVALID_TOPIC_EXCEPTION);
    assert_eq!(
        r[0].2.as_deref(),
        Some(
            format!(
                "Topic '{}' collides with existing topic: {}",
                underscored, dotted
            )
            .as_str()
        )
    );

    let rf = |n: &str| format!("{}-rf{}", base, n);
    let r = create_topics_raw(vec![
        creatable(&rf("3"), 1, 3),
        creatable(&rf("0"), 1, 0),
        creatable(&rf("m2"), 1, -2),
        creatable(&rf("m1"), 1, -1),
        creatable(&rf("p0"), 0, 1),
    ])
    .await?;
    println!("replication factors: {:?}", r);
    let code = |n: &str| r.iter().find(|t| t.0 == rf(n)).unwrap().1;
    assert_eq!(code("3"), ERROR_INVALID_REPLICATION_FACTOR);
    assert_eq!(code("0"), ERROR_INVALID_REPLICATION_FACTOR);
    assert_eq!(code("m2"), ERROR_INVALID_REPLICATION_FACTOR);
    assert_eq!(code("m1"), ERROR_NONE);
    assert_eq!(code("p0"), ERROR_INVALID_PARTITIONS);

    // The real admin client sees the same.
    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(10)));
    let admin = admin()?;
    let res = admin
        .create_topics(
            &[NewTopic::new(&rf("3b"), 1, TopicReplication::Fixed(3))],
            &opts,
        )
        .await?;
    match &res[0] {
        Err((_, RDKafkaErrorCode::InvalidReplicationFactor)) => {}
        other => return Err(format!("expected InvalidReplicationFactor, got {:?}", other).into()),
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

/// Items 3 and 4 (raw wire, Kafka 3.7's answers): INCONSISTENT_GROUP_PROTOCOL for a different
/// protocol type, no common protocol, or an empty protocol list; UNKNOWN_MEMBER_ID for a JoinGroup
/// with a member id the group does not know (no group is created for it); and a v5 error
/// response decodes (protocol_name "" rather than null before v7). The group is unaffected.
pub async fn test_w3g_join_group_protocol_rules() -> TestResult {
    println!("=== Test: JoinGroup protocol rules ===\n");
    let ctx = TestContext::new().await?;
    let group = ctx.unique_group("w3g-proto").await;
    let fresh = ctx.unique_group("w3g-unknown-member").await;

    let mut a = conn().await?;
    let (e, generation, member, proto) = join_now(
        &mut a,
        &join_req(&group, "", "consumer", &["range", "roundrobin"]),
    )
    .await?;
    assert_eq!((e, proto.as_deref()), (ERROR_NONE, Some("range")));
    assert_eq!(
        sync_leader(&mut a, &group, generation, &member).await?,
        ERROR_NONE
    );

    for (what, req) in [
        (
            "protocol type connect",
            join_req(&group, "", "connect", &["range"]),
        ),
        (
            "no common protocol",
            join_req(&group, "", "consumer", &["cooperative-sticky"]),
        ),
        ("empty protocol list", join_req(&group, "", "consumer", &[])),
    ] {
        let mut c = conn().await?;
        let (e, _, _, proto) = join_now(&mut c, &req).await?;
        println!("{}: error {} protocol_name {:?}", what, e, proto);
        assert_eq!(e, ERROR_INCONSISTENT_GROUP_PROTOCOL, "{}", what);
        assert_eq!(
            proto.as_deref(),
            Some(""),
            "v5 error carries an empty, non-null name"
        );
    }
    let mut c = conn().await?;
    let (e, ..) = join_now(&mut c, &join_req(&fresh, "", "consumer", &[])).await?;
    assert_eq!(
        e, ERROR_INCONSISTENT_GROUP_PROTOCOL,
        "empty protocols, new group"
    );

    let (e, ..) = join_now(
        &mut c,
        &join_req(&group, "no-such-member", "consumer", &["range"]),
    )
    .await?;
    assert_eq!(e, ERROR_UNKNOWN_MEMBER_ID);
    let (e, ..) = join_now(
        &mut c,
        &join_req(&fresh, "no-such-member", "consumer", &["range"]),
    )
    .await?;
    assert_eq!(e, ERROR_UNKNOWN_MEMBER_ID);
    let (_, state, _, _) = describe(&fresh).await?;
    assert_eq!(state, "Dead", "refused joins create no group");

    let (e, state, ptype, members) = describe(&group).await?;
    assert_eq!(
        (e, state.as_str(), ptype.as_str(), members),
        (ERROR_NONE, "Stable", "consumer", 1)
    );
    assert_eq!(leave(&mut a, &group, &member).await?, ERROR_NONE);
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// Item 4: OffsetCommit validation, Kafka 3.7's answers (each checked against a real broker):
/// unknown group: gen -1 accepted (any member id), gen >= 0 ILLEGAL_GENERATION; group with
/// members: gen -1 / "" UNKNOWN_MEMBER_ID, gen -1 with the member's id ILLEGAL_GENERATION,
/// the right member and generation accepted; Empty group: gen -1 accepted, gen >= 0
/// UNKNOWN_MEMBER_ID. Before: gen -1 was accepted for a group with members, and an unknown group
/// at gen >= 0 got UNKNOWN_MEMBER_ID.
pub async fn test_w3g_offset_commit_rules() -> TestResult {
    println!("=== Test: OffsetCommit rules ===\n");
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("w3g-commit").await;
    produce_one(&topic).await?;
    let unknown = ctx.unique_group("w3g-commit-unknown").await;
    let group = ctx.unique_group("w3g-commit-live").await;

    assert_eq!(
        commit(&unknown, 5, "x", &topic, 1).await?,
        ERROR_ILLEGAL_GENERATION
    );
    assert_eq!(commit(&unknown, -1, "x", &topic, 1).await?, ERROR_NONE);
    assert_eq!(commit(&unknown, -1, "", &topic, 1).await?, ERROR_NONE);

    let mut a = conn().await?;
    let (e, generation, member, _) =
        join_now(&mut a, &join_req(&group, "", "consumer", &["range"])).await?;
    assert_eq!(e, ERROR_NONE);
    assert_eq!(
        sync_leader(&mut a, &group, generation, &member).await?,
        ERROR_NONE
    );
    assert_eq!(
        commit(&group, -1, "", &topic, 2).await?,
        ERROR_UNKNOWN_MEMBER_ID
    );
    assert_eq!(
        commit(&group, -1, "x", &topic, 2).await?,
        ERROR_UNKNOWN_MEMBER_ID
    );
    assert_eq!(
        commit(&group, -1, &member, &topic, 2).await?,
        ERROR_ILLEGAL_GENERATION
    );
    assert_eq!(
        commit(&group, generation + 3, &member, &topic, 2).await?,
        ERROR_ILLEGAL_GENERATION
    );
    assert_eq!(
        commit(&group, generation, &member, &topic, 3).await?,
        ERROR_NONE
    );

    assert_eq!(leave(&mut a, &group, &member).await?, ERROR_NONE);
    assert_eq!(commit(&group, -1, "", &topic, 4).await?, ERROR_NONE);
    assert_eq!(commit(&group, -1, &member, &topic, 4).await?, ERROR_NONE);
    assert_eq!(
        commit(&group, 5, "x", &topic, 4).await?,
        ERROR_UNKNOWN_MEMBER_ID
    );

    let committed: i64 = ctx
        .db()
        .query_one(
            "SELECT committed_offset FROM kafka.consumer_offsets WHERE group_id = $1",
            &[&group],
        )
        .await?
        .get(0);
    assert_eq!(committed, 4, "only accepted commits are stored");
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// Item 2: a group with only committed offsets is an Empty "consumer" group for ListGroups and
/// DescribeGroups, including after a worker restart (which empties the in-memory coordinator);
/// DeleteGroups deletes it and then answers GROUP_ID_NOT_FOUND. Uses a real rdkafka consumer
/// group to commit. Before: after a restart the group was missing from ListGroups and "Dead" in
/// DescribeGroups (`kafka-consumer-groups --describe` said it does not exist), and DeleteGroups of
/// a group that never existed answered NONE. Sequential: restarts the listener worker.
pub async fn test_w3g_offsets_only_group_survives_restart() -> TestResult {
    println!("=== Test: offsets-only groups are Empty groups after a restart ===\n");
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("w3g-oo").await;
    let group = ctx.unique_group("w3g-oo").await;
    produce_one(&topic).await?;

    {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", get_bootstrap_servers())
            .set("broker.address.family", "v4")
            .set("group.id", &group)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .set("session.timeout.ms", "10000")
            .create()?;
        consumer.subscribe(&[&topic])?;
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut got = false;
        while !got && Instant::now() < deadline {
            if let Some(Ok(_)) = consumer.poll(Duration::from_millis(500)) {
                got = true;
            }
        }
        assert!(got, "consumer read the record");
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(&topic, 0, rdkafka::Offset::Offset(1))?;
        consumer.commit(&tpl, rdkafka::consumer::CommitMode::Sync)?;
        consumer.unsubscribe();
    } // dropped: leaves the group

    let db = create_db_client().await?;
    crate::auth::restart_listener(&db, &[], &[]).await?;
    println!("✅ listener worker restarted (coordinator state gone)\n");

    let listed = list_groups().await?;
    let entry = listed.iter().find(|g| g.0 == group);
    assert_eq!(
        entry.map(|g| (g.1.as_str(), g.2.as_str())),
        Some(("consumer", "Empty")),
        "ListGroups: {:?}",
        listed
            .iter()
            .filter(|g| g.0.starts_with("w3g"))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        describe(&group).await?,
        (ERROR_NONE, "Empty".into(), "consumer".into(), 0)
    );
    println!("✅ ListGroups / DescribeGroups report the group as Empty \"consumer\"\n");

    // rdkafka's group listing (ListGroups + DescribeGroups) agrees.
    let (state, protocol_type) = {
        let probe: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", get_bootstrap_servers())
            .set("broker.address.family", "v4")
            .create()?;
        let groups = probe.fetch_group_list(Some(&group), Duration::from_secs(10))?;
        let g = groups
            .groups()
            .iter()
            .find(|g| g.name() == group)
            .ok_or("rdkafka group list misses the group")?;
        (g.state().to_string(), g.protocol_type().to_string())
    };
    assert_eq!(
        (state.as_str(), protocol_type.as_str()),
        ("Empty", "consumer")
    );

    assert_eq!(delete_group(&group).await?, ERROR_NONE);
    assert_eq!(delete_group(&group).await?, ERROR_GROUP_ID_NOT_FOUND);
    assert_eq!(describe(&group).await?.1, "Dead");
    let left: i64 = ctx
        .db()
        .query_one(
            "SELECT count(*) FROM kafka.consumer_offsets WHERE group_id = $1",
            &[&group],
        )
        .await?
        .get(0);
    assert_eq!(left, 0);
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// Items 3 + 7 with real clients: an rdkafka consumer using the cooperative-sticky assignor
/// joining a group whose member uses range gets INCONSISTENT_GROUP_PROTOCOL, and the existing
/// member keeps its assignment and keeps consuming (no rebalance is started by the refused join).
pub async fn test_w3g_different_assignor_is_refused_group_unaffected() -> TestResult {
    println!("=== Test: a consumer with no common assignor is refused ===\n");
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("w3g-assignor").await;
    let group = ctx.unique_group("w3g-assignor").await;
    produce_one(&topic).await?;

    let make = |strategy: &str| -> DynResult<BaseConsumer> {
        Ok(ClientConfig::new()
            .set("bootstrap.servers", get_bootstrap_servers())
            .set("broker.address.family", "v4")
            .set("group.id", &group)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .set("session.timeout.ms", "10000")
            .set("partition.assignment.strategy", strategy)
            .create()?)
    };
    let a = make("range")?;
    a.subscribe(&[&topic])?;
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut got = false;
    while !got && Instant::now() < deadline {
        got = matches!(a.poll(Duration::from_millis(300)), Some(Ok(_)));
    }
    assert!(got, "range consumer consumes");
    let (_, state, _, members) = describe(&group).await?;
    assert_eq!((state.as_str(), members), ("Stable", 1));

    let b = make("cooperative-sticky")?;
    b.subscribe(&[&topic])?;
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut refused = false;
    while !refused && Instant::now() < deadline {
        // keep A alive
        let _ = a.poll(Duration::from_millis(100));
        if let Some(Err(e)) = b.poll(Duration::from_millis(200)) {
            println!("B: {}", e);
            if let KafkaError::Global(RDKafkaErrorCode::InconsistentGroupProtocol)
            | KafkaError::MessageConsumption(RDKafkaErrorCode::InconsistentGroupProtocol) = e
            {
                refused = true;
            } else if e.to_string().contains("nconsistent") {
                refused = true;
            }
        }
    }
    assert!(refused, "B must be told INCONSISTENT_GROUP_PROTOCOL");

    // A is still the only member, Stable, with its partition, and still consumes new records.
    let (_, state, _, members) = describe(&group).await?;
    assert_eq!((state.as_str(), members), ("Stable", 1), "group unaffected");
    assert_eq!(a.assignment()?.count(), 1);
    produce_one(&topic).await?;
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut again = false;
    while !again && Instant::now() < deadline {
        again = matches!(a.poll(Duration::from_millis(300)), Some(Ok(_)));
    }
    assert!(again, "A keeps consuming");
    drop(b);
    drop(a);
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// The `w3_groups` tests, in run order.
pub fn tests() -> Vec<crate::registry::TestDef> {
    crate::test_defs!["w3_groups";
        test_w3g_partition_cap,
        test_w3g_create_topics_validation,
        test_w3g_join_group_protocol_rules,
        test_w3g_offset_commit_rules,
        test_w3g_different_assignor_is_refused_group_unaffected,
        test_w3g_metadata_survives_corrupt_partition_count: sequential, // drops and restores the CHECK constraint on kafka.topics
        test_w3g_offsets_only_group_survives_restart: sequential, // restarts the listener worker
    ]
}
