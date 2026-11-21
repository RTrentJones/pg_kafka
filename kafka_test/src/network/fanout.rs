//! One small request must not hold the DB thread for long, however many entries it lists:
//! the DB thread serves every client, so a request whose topic, partition, resource or group
//! entries each cost a database round trip stalls the whole broker for as long as it runs.
//! Each test sends a ~100-200 KB request of repeated (or many distinct) entries on its own
//! connection while another connection produces one record every few milliseconds, and
//! checks the slowest of those produces as well as the answer, which follows Kafka 3.7's.

use crate::common::{DynResult, TestResult};
use crate::setup::TestContext;
use crate::wire::records::plain_batch;
use crate::wire::requests::{creatable, create_topics_raw};
use crate::wire::{decode_response, read_frame_within, request_frame, Wire};
use bytes::Bytes;
use kafka_protocol::messages::{
    add_partitions_to_txn_request::{AddPartitionsToTxnRequest, AddPartitionsToTxnTopic},
    create_partitions_request::{CreatePartitionsRequest, CreatePartitionsTopic},
    create_topics_request::CreateTopicsRequest,
    delete_groups_request::DeleteGroupsRequest,
    delete_records_request::{DeleteRecordsPartition, DeleteRecordsRequest, DeleteRecordsTopic},
    delete_topics_request::DeleteTopicsRequest,
    describe_configs_request::{DescribeConfigsRequest, DescribeConfigsResource},
    describe_groups_request::DescribeGroupsRequest,
    fetch_request::{FetchPartition, FetchRequest, FetchTopic},
    incremental_alter_configs_request::{
        AlterConfigsResource, AlterableConfig, IncrementalAlterConfigsRequest,
    },
    list_offsets_request::{ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic},
    metadata_request::{MetadataRequest, MetadataRequestTopic},
    offset_commit_request::{
        OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
    },
    offset_fetch_request::{OffsetFetchRequest, OffsetFetchRequestTopic},
    txn_offset_commit_request::{
        TxnOffsetCommitRequest, TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic,
    },
    BrokerId, GroupId, ProducerId, TopicName, TransactionalId,
};
use kafka_protocol::protocol::{Request, StrBytes};
use kafka_protocol::records::Compression;
use std::collections::BTreeMap;
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

/// The slowest a one-record produce on another connection may take while a hostile request
/// runs. Each request below costs the DB thread a few milliseconds once its entries share
/// their lookups and a repeated entry is acted on once; one database round trip per entry
/// costs it 70 ms to 11 s.
const STALL_BOUND: Duration = Duration::from_millis(50);

fn s(v: &str) -> StrBytes {
    StrBytes::from_string(v.to_string())
}

fn topic_name(v: &str) -> TopicName {
    TopicName(s(v))
}

/// A short name, `tag` and five hex digits: short names make the most entries per byte.
fn short_name(tag: &str) -> String {
    format!("{tag}{}", &uuid::Uuid::new_v4().simple().to_string()[..5])
}

/// `n` distinct names, `prefix` and a five-digit counter.
fn numbered(prefix: &str, n: usize) -> Vec<String> {
    (0..n).map(|i| format!("{prefix}{i:05}")).collect()
}

/// A hostile request's answer, how long it took, and what a produce alongside saw.
struct Measured<T> {
    response: T,
    took: Duration,
    worst: Duration,
    probes: usize,
}

/// Send `request` at `version` on its own connection while another connection produces one
/// record to partition 0 of `probe_topic` every 10 ms; the answer and the slowest produce.
async fn measure<R: Request>(
    probe_topic: &str,
    version: i16,
    request: &R,
) -> DynResult<Measured<R::Response>> {
    let frame = request_frame(version, 1, request);
    let size = frame.len();
    let mut probe = Wire::connect().await?;
    let value = plain_batch(&[Bytes::from_static(b"p")], Compression::None);
    let (code, _) = probe.produce(probe_topic, 0, value.clone(), None).await?;
    assert_eq!(code, 0, "the probe produce works before the request");
    let hostile = tokio::spawn(async move {
        let started = Instant::now();
        let mut stream = TcpStream::connect(crate::common::broker_addr())
            .await
            .map_err(|e| e.to_string())?;
        stream.write_all(&frame).await.map_err(|e| e.to_string())?;
        let answer = read_frame_within(&mut stream, Duration::from_secs(120))
            .await
            .map_err(|e| e.to_string())?
            .ok_or("connection closed instead of an answer")?;
        Ok::<_, String>((answer, started.elapsed()))
    });
    let mut worst = Duration::ZERO;
    let mut probes = 0;
    while !hostile.is_finished() {
        let started = Instant::now();
        let (code, _) = probe.produce(probe_topic, 0, value.clone(), None).await?;
        assert_eq!(code, 0, "a probe produce alongside the request failed");
        worst = worst.max(started.elapsed());
        probes += 1;
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let (answer, took) = hostile.await??;
    let (correlation, response) = decode_response::<R>(version, &answer)?;
    assert_eq!(correlation, 1);
    println!(
        "  {size}-byte request answered after {took:?}; {probes} produces alongside, the \
         slowest {worst:?}"
    );
    Ok(Measured {
        response,
        took,
        worst,
        probes,
    })
}

/// The produce alongside was never held up for long.
fn assert_not_stalled<T>(m: &Measured<T>) {
    assert!(
        m.worst < STALL_BOUND,
        "a produce alongside took {:?} (request answered after {:?}, {} probes)",
        m.worst,
        m.took,
        m.probes
    );
}

/// How many entries carry each error code.
fn tally(codes: impl IntoIterator<Item = i16>) -> BTreeMap<i16, usize> {
    let mut t = BTreeMap::new();
    for c in codes {
        *t.entry(c).or_default() += 1;
    }
    t
}

/// A test context and a one-partition probe topic.
async fn setup(tag: &str) -> DynResult<(TestContext, String)> {
    let ctx = TestContext::new().await?;
    let probe = ctx.unique_topic(&format!("net-fan-{tag}")).await;
    let created = create_topics_raw(vec![creatable(&probe, 1, 1)]).await?;
    assert_eq!(created[0].error_code, 0, "{created:?}");
    writable(&probe).await?;
    Ok((ctx, probe))
}

/// Wait until partition 0 of `topic` takes a produce (at once here; a reference Kafka
/// broker elects the leader of a new topic shortly after creating it).
async fn writable(topic: &str) -> DynResult<()> {
    let mut w = Wire::connect().await?;
    let value = plain_batch(&[Bytes::from_static(b"w")], Compression::None);
    for _ in 0..50 {
        if w.produce(topic, 0, value.clone(), None).await?.0 == 0 {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(format!("{topic} never took a produce").into())
}

/// A topic with a short name and `partitions` partitions, created for one test.
async fn short_topic(tag: &str, partitions: i32) -> DynResult<String> {
    let name = short_name(tag);
    let created = create_topics_raw(vec![creatable(&name, partitions, 1)]).await?;
    assert_eq!(created[0].error_code, 0, "{created:?}");
    writable(&name).await?;
    Ok(name)
}

/// Drop every topic whose name starts with `prefix` (topics the context does not know).
async fn drop_topics(ctx: &TestContext, prefix: &str) -> DynResult<()> {
    ctx.db()
        .execute(
            "DELETE FROM kafka.topics WHERE starts_with(name, $1)",
            &[&prefix],
        )
        .await?;
    Ok(())
}

/// How many topics' names start with `prefix`.
async fn topics_named(ctx: &TestContext, prefix: &str) -> DynResult<i64> {
    Ok(ctx
        .db()
        .query_one(
            "SELECT count(*) FROM kafka.topics WHERE starts_with(name, $1)",
            &[&prefix],
        )
        .await?
        .get(0))
}

fn metadata_request(names: &[String], auto_create: bool) -> MetadataRequest {
    let mut req = MetadataRequest::default();
    req.topics = Some(
        names
            .iter()
            .map(|n| {
                let mut t = MetadataRequestTopic::default();
                t.name = Some(topic_name(n));
                t
            })
            .collect(),
    );
    req.allow_auto_topic_creation = auto_create;
    req
}

/// Metadata v1 naming one short topic 18,000 times (144 KB): Kafka answers each distinct
/// name once, and the missing topic is auto-created once.
pub async fn test_metadata_of_a_repeated_name_is_answered_once() -> TestResult {
    println!("=== Test: Metadata of one name repeated 18,000 times ===\n");
    let (ctx, probe) = setup("md").await?;
    let name = short_name("fm");
    let req = metadata_request(&vec![name.clone(); 18_000], true);
    for round in ["missing (auto-created)", "existing"] {
        println!("Step: the topic {round}");
        let m = measure(&probe, 1, &req).await?;
        let names: Vec<String> = m
            .response
            .topics
            .iter()
            .map(|t| {
                t.name
                    .as_ref()
                    .map_or_else(String::new, |n| n.0.to_string())
            })
            .collect();
        println!(
            "  {} topic entries, codes {:?}",
            names.len(),
            tally(m.response.topics.iter().map(|t| t.error_code))
        );
        assert_not_stalled(&m);
        assert_eq!(names.len(), 1, "one entry for the one name");
        assert_eq!(names[0], name);
        assert_eq!(m.response.topics[0].error_code, 0);
    }
    assert_eq!(topics_named(&ctx, &name).await?, 1);
    drop_topics(&ctx, &name).await?;
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// Metadata v4 without auto-creation naming 13,000 distinct missing topics (182 KB): each
/// is UNKNOWN_TOPIC_OR_PARTITION, looked up together.
pub async fn test_metadata_of_many_missing_names_is_answered_quickly() -> TestResult {
    println!("=== Test: Metadata of 13,000 distinct missing names ===\n");
    let (ctx, probe) = setup("mdm").await?;
    let prefix = short_name("mm");
    let req = metadata_request(&numbered(&prefix, 13_000), false);
    let m = measure(&probe, 4, &req).await?;
    let codes = tally(m.response.topics.iter().map(|t| t.error_code));
    println!("  codes {codes:?}");
    assert_eq!(codes, BTreeMap::from([(3, 13_000)]));
    assert_not_stalled(&m);
    assert_eq!(topics_named(&ctx, &prefix).await?, 0);
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// Metadata v1 with auto-creation naming 3,000 distinct missing topics (42 KB): each is
/// created, all in one statement.
pub async fn test_metadata_auto_creates_many_names_together() -> TestResult {
    println!("=== Test: Metadata auto-creating 3,000 distinct names ===\n");
    let (ctx, probe) = setup("mda").await?;
    let prefix = short_name("ma");
    let req = metadata_request(&numbered(&prefix, 3_000), true);
    let m = measure(&probe, 1, &req).await?;
    let codes = tally(m.response.topics.iter().map(|t| t.error_code));
    let with_partitions = m
        .response
        .topics
        .iter()
        .filter(|t| !t.partitions.is_empty())
        .count();
    println!("  codes {codes:?}, {with_partitions} with partitions");
    let created = topics_named(&ctx, &prefix).await?;
    drop_topics(&ctx, &prefix).await?;
    assert_not_stalled(&m);
    assert_eq!(codes, BTreeMap::from([(0, 3_000)]));
    assert_eq!(with_partitions, 3_000);
    assert_eq!(created, 3_000);
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// ListOffsets v1 listing partition 0 of one topic 15,000 times (180 KB): Kafka answers
/// every entry of a repeated partition INVALID_REQUEST and reads nothing for it.
pub async fn test_list_offsets_of_a_repeated_partition_is_refused_quickly() -> TestResult {
    println!("=== Test: ListOffsets of one partition repeated 15,000 times ===\n");
    let (ctx, probe) = setup("lo").await?;
    let mut t = ListOffsetsTopic::default();
    t.name = topic_name(&probe);
    t.partitions = (0..15_000)
        .map(|_| {
            let mut p = ListOffsetsPartition::default();
            p.partition_index = 0;
            p.timestamp = -1;
            p
        })
        .collect();
    let mut req = ListOffsetsRequest::default();
    req.replica_id = BrokerId(-1);
    req.topics = vec![t];
    let m = measure(&probe, 1, &req).await?;
    let codes = tally(
        m.response
            .topics
            .iter()
            .flat_map(|t| t.partitions.iter().map(|p| p.error_code)),
    );
    println!(
        "  {} topic entries, codes {codes:?}",
        m.response.topics.len()
    );
    assert_eq!(codes, BTreeMap::from([(42, 15_000)]));
    assert_not_stalled(&m);
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// ListOffsets v1 of 7,000 distinct missing topics (210 KB): each UNKNOWN_TOPIC_OR_PARTITION.
pub async fn test_list_offsets_of_many_missing_topics_is_answered_quickly() -> TestResult {
    println!("=== Test: ListOffsets of 7,000 distinct missing topics ===\n");
    let (ctx, probe) = setup("lom").await?;
    let prefix = short_name("lm");
    let mut req = ListOffsetsRequest::default();
    req.replica_id = BrokerId(-1);
    req.topics = numbered(&prefix, 7_000)
        .iter()
        .map(|n| {
            let mut p = ListOffsetsPartition::default();
            p.timestamp = -1;
            let mut t = ListOffsetsTopic::default();
            t.name = topic_name(n);
            t.partitions = vec![p];
            t
        })
        .collect();
    let m = measure(&probe, 1, &req).await?;
    let codes = tally(
        m.response
            .topics
            .iter()
            .flat_map(|t| t.partitions.iter().map(|p| p.error_code)),
    );
    println!("  codes {codes:?}");
    assert_eq!(codes, BTreeMap::from([(3, 7_000)]));
    assert_not_stalled(&m);
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// OffsetCommit v2 (simple-consumer style) of partition 0 of one topic 12,000 times
/// (168 KB): Kafka stores the request's last offset for the partition and answers it once.
pub async fn test_offset_commit_of_a_repeated_partition_is_answered_quickly() -> TestResult {
    println!("=== Test: OffsetCommit of one partition repeated 12,000 times ===\n");
    let (ctx, probe) = setup("oc").await?;
    let group = ctx.unique_group("net-fan-oc").await;
    let mut t = OffsetCommitRequestTopic::default();
    t.name = topic_name(&probe);
    t.partitions = (0..12_000)
        .map(|i| {
            let mut p = OffsetCommitRequestPartition::default();
            p.partition_index = 0;
            p.committed_offset = i;
            p
        })
        .collect();
    let mut req = OffsetCommitRequest::default();
    req.group_id = GroupId(s(&group));
    req.generation_id_or_member_epoch = -1;
    req.retention_time_ms = -1;
    req.topics = vec![t];
    let m = measure(&probe, 2, &req).await?;
    let entries: usize = m.response.topics.iter().map(|t| t.partitions.len()).sum();
    let codes = tally(
        m.response
            .topics
            .iter()
            .flat_map(|t| t.partitions.iter().map(|p| p.error_code)),
    );
    println!("  {entries} partition entries, codes {codes:?}");
    assert_not_stalled(&m);
    assert_eq!(entries, 1, "Kafka answers the repeated partition once");
    assert_eq!(codes, BTreeMap::from([(0, 1)]));
    let stored: i64 = ctx
        .db()
        .query_one(
            "SELECT committed_offset FROM kafka.consumer_offsets WHERE group_id = $1",
            &[&group],
        )
        .await?
        .get(0);
    println!("  stored offset {stored}");
    assert_eq!(stored, 11_999, "the last entry for the partition is stored");
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// OffsetFetch v1 of partition 0 of one topic 40,000 times (160 KB): Kafka answers the
/// partition once.
pub async fn test_offset_fetch_of_a_repeated_partition_is_answered_quickly() -> TestResult {
    println!("=== Test: OffsetFetch of one partition repeated 40,000 times ===\n");
    let (ctx, probe) = setup("of").await?;
    let group = ctx.unique_group("net-fan-of").await;
    let mut t = OffsetFetchRequestTopic::default();
    t.name = topic_name(&probe);
    t.partition_indexes = vec![0; 40_000];
    let mut req = OffsetFetchRequest::default();
    req.group_id = GroupId(s(&group));
    req.topics = Some(vec![t]);
    let m = measure(&probe, 1, &req).await?;
    let entries: usize = m.response.topics.iter().map(|t| t.partitions.len()).sum();
    let codes = tally(
        m.response
            .topics
            .iter()
            .flat_map(|t| t.partitions.iter().map(|p| p.error_code)),
    );
    println!("  {entries} partition entries, codes {codes:?}");
    assert_not_stalled(&m);
    assert_eq!(entries, 1, "Kafka answers the repeated partition once");
    assert_eq!(codes, BTreeMap::from([(0, 1)]));
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// DeleteRecords v0 of partition 0 of one topic 15,000 times (180 KB): Kafka truncates the
/// partition once, to its last entry's offset, and answers it once.
pub async fn test_delete_records_of_a_repeated_partition_is_answered_quickly() -> TestResult {
    println!("=== Test: DeleteRecords of one partition repeated 15,000 times ===\n");
    let (ctx, probe) = setup("dr").await?;
    let target = short_topic("dr", 1).await?;
    let mut t = DeleteRecordsTopic::default();
    t.name = topic_name(&target);
    t.partitions = (0..15_000)
        .map(|_| {
            let mut p = DeleteRecordsPartition::default();
            p.offset = -1;
            p
        })
        .collect();
    let mut req = DeleteRecordsRequest::default();
    req.timeout_ms = 30_000;
    req.topics = vec![t];
    let m = measure(&probe, 0, &req).await?;
    let entries: usize = m.response.topics.iter().map(|t| t.partitions.len()).sum();
    let codes = tally(
        m.response
            .topics
            .iter()
            .flat_map(|t| t.partitions.iter().map(|p| p.error_code)),
    );
    println!("  {entries} partition entries, codes {codes:?}");
    assert_not_stalled(&m);
    assert_eq!(entries, 1, "Kafka answers the repeated partition once");
    assert_eq!(codes, BTreeMap::from([(0, 1)]));
    drop_topics(&ctx, &target).await?;
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// CreateTopics v2 (validate_only) of 4,000 distinct new topics (112 KB): each would be
/// created; nothing is.
pub async fn test_create_topics_of_many_names_is_answered_quickly() -> TestResult {
    println!("=== Test: CreateTopics (validate_only) of 4,000 distinct names ===\n");
    let (ctx, probe) = setup("ct").await?;
    let prefix = short_name("ct");
    let mut req = CreateTopicsRequest::default();
    req.timeout_ms = 30_000;
    req.validate_only = true;
    req.topics = numbered(&prefix, 4_000)
        .iter()
        .map(|n| creatable(n, 1, 1))
        .collect();
    let m = measure(&probe, 2, &req).await?;
    let codes = tally(m.response.topics.iter().map(|t| t.error_code));
    println!("  codes {codes:?}");
    assert_eq!(codes, BTreeMap::from([(0, 4_000)]));
    assert_not_stalled(&m);
    assert_eq!(topics_named(&ctx, &prefix).await?, 0);
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// DeleteTopics v1 of 13,000 distinct missing topics (182 KB): each UNKNOWN_TOPIC_OR_PARTITION.
pub async fn test_delete_topics_of_many_missing_names_is_answered_quickly() -> TestResult {
    println!("=== Test: DeleteTopics of 13,000 distinct missing names ===\n");
    let (ctx, probe) = setup("dt").await?;
    let prefix = short_name("dt");
    let mut req = DeleteTopicsRequest::default();
    req.timeout_ms = 30_000;
    req.topic_names = numbered(&prefix, 13_000)
        .iter()
        .map(|n| topic_name(n))
        .collect();
    let m = measure(&probe, 1, &req).await?;
    let codes = tally(m.response.responses.iter().map(|t| t.error_code));
    println!("  codes {codes:?}");
    assert_eq!(codes, BTreeMap::from([(3, 13_000)]));
    assert_not_stalled(&m);
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// CreatePartitions v0 (validate_only) of 9,000 distinct missing topics (198 KB): each
/// UNKNOWN_TOPIC_OR_PARTITION.
pub async fn test_create_partitions_of_many_missing_names_is_answered_quickly() -> TestResult {
    println!("=== Test: CreatePartitions of 9,000 distinct missing names ===\n");
    let (ctx, probe) = setup("cp").await?;
    let prefix = short_name("cp");
    let mut req = CreatePartitionsRequest::default();
    req.timeout_ms = 30_000;
    req.validate_only = true;
    req.topics = numbered(&prefix, 9_000)
        .iter()
        .map(|n| {
            let mut t = CreatePartitionsTopic::default();
            t.name = topic_name(n);
            t.count = 2;
            t
        })
        .collect();
    let m = measure(&probe, 0, &req).await?;
    let codes = tally(m.response.results.iter().map(|t| t.error_code));
    println!("  codes {codes:?}");
    assert_eq!(codes, BTreeMap::from([(3, 9_000)]));
    assert_not_stalled(&m);
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// DescribeConfigs v1 of one existing topic 7,000 times (196 KB): Kafka answers every
/// entry.
pub async fn test_describe_configs_of_a_repeated_topic_is_answered_quickly() -> TestResult {
    println!("=== Test: DescribeConfigs of one topic repeated 7,000 times ===\n");
    let (ctx, probe) = setup("dc").await?;
    let target = short_topic("dc", 1).await?;
    let mut req = DescribeConfigsRequest::default();
    req.resources = (0..7_000)
        .map(|_| {
            let mut r = DescribeConfigsResource::default();
            r.resource_type = 2;
            r.resource_name = s(&target);
            r.configuration_keys = Some(vec![s("retention.ms")]);
            r
        })
        .collect();
    let m = measure(&probe, 1, &req).await?;
    let codes = tally(m.response.results.iter().map(|r| r.error_code));
    println!("  {} results, codes {codes:?}", m.response.results.len());
    assert_not_stalled(&m);
    assert_eq!(codes, BTreeMap::from([(0, 7_000)]));
    drop_topics(&ctx, &target).await?;
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// IncrementalAlterConfigs v0 of one existing topic 5,000 times (190 KB): Kafka refuses
/// every entry of a repeated resource INVALID_REQUEST and alters nothing.
pub async fn test_incremental_alter_configs_of_a_repeated_topic_is_answered_quickly() -> TestResult
{
    println!("=== Test: IncrementalAlterConfigs of one topic repeated 5,000 times ===\n");
    let (ctx, probe) = setup("ia").await?;
    let target = short_topic("ia", 1).await?;
    let mut req = IncrementalAlterConfigsRequest::default();
    req.resources = (0..5_000)
        .map(|i| {
            let mut c = AlterableConfig::default();
            c.name = s("retention.ms");
            c.config_operation = 0;
            c.value = Some(s(&format!("{}", 3_600_000 + i)));
            let mut r = AlterConfigsResource::default();
            r.resource_type = 2;
            r.resource_name = s(&target);
            r.configs = vec![c];
            r
        })
        .collect();
    let m = measure(&probe, 0, &req).await?;
    let codes = tally(m.response.responses.iter().map(|r| r.error_code));
    println!("  {} results, codes {codes:?}", m.response.responses.len());
    assert_not_stalled(&m);
    assert_eq!(codes, BTreeMap::from([(42, 5_000)]));
    let retention: Option<i64> = ctx
        .db()
        .query_one(
            "SELECT retention_ms FROM kafka.topics WHERE name = $1",
            &[&target],
        )
        .await?
        .get(0);
    assert_eq!(retention, None, "nothing is altered");
    drop_topics(&ctx, &target).await?;
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// A transactional producer's id and epoch for a fresh transactional id.
async fn transactional_producer(ctx: &TestContext) -> DynResult<(String, i64, i16)> {
    let txn = format!("{}-txn", ctx.unique_group("net-fan").await);
    let mut w = Wire::connect().await?;
    let (code, pid, epoch) = w.init_producer_id(Some(&txn), 60_000).await?;
    assert_eq!(code, 0);
    Ok((txn, pid, epoch))
}

/// AddPartitionsToTxn v1 naming partition 0 of one short-named topic in 10,000 topic
/// entries (150 KB): Kafka answers every entry; the partition joins the transaction once.
pub async fn test_add_partitions_to_txn_of_a_repeated_topic_is_answered_quickly() -> TestResult {
    println!("=== Test: AddPartitionsToTxn of one topic repeated 10,000 times ===\n");
    let (ctx, probe) = setup("ap").await?;
    let (txn, pid, epoch) = transactional_producer(&ctx).await?;
    let name = short_topic("fa", 1).await?;
    let mut req = AddPartitionsToTxnRequest::default();
    req.v3_and_below_transactional_id = TransactionalId(s(&txn));
    req.v3_and_below_producer_id = ProducerId(pid);
    req.v3_and_below_producer_epoch = epoch;
    req.v3_and_below_topics = (0..10_000)
        .map(|_| {
            let mut t = AddPartitionsToTxnTopic::default();
            t.name = topic_name(&name);
            t.partitions = vec![0];
            t
        })
        .collect();
    let m = measure(&probe, 1, &req).await?;
    let topics = m.response.results_by_topic_v3_and_below.len();
    let codes = tally(
        m.response
            .results_by_topic_v3_and_below
            .iter()
            .flat_map(|t| {
                t.results_by_partition
                    .iter()
                    .map(|p| p.partition_error_code)
            }),
    );
    println!("  {topics} topic entries, codes {codes:?}");
    assert_not_stalled(&m);
    assert_eq!(topics, 10_000, "Kafka answers every entry");
    assert_eq!(codes, BTreeMap::from([(0, 10_000)]));
    let added: i64 = ctx
        .db()
        .query_one(
            "SELECT count(*) FROM kafka.txn_partitions WHERE transactional_id = $1",
            &[&txn],
        )
        .await?
        .get(0);
    assert_eq!(added, 1);
    let mut w = Wire::connect().await?;
    assert_eq!(w.end_txn(&txn, pid, epoch, false).await?, 0);
    drop_topics(&ctx, &name).await?;
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// TxnOffsetCommit v1 of partition 0 of one topic 12,000 times (168 KB): Kafka stores the
/// last entry's offset for the partition and answers it once.
pub async fn test_txn_offset_commit_of_a_repeated_partition_is_answered_quickly() -> TestResult {
    println!("=== Test: TxnOffsetCommit of one partition repeated 12,000 times ===\n");
    let (ctx, probe) = setup("to").await?;
    let group = ctx.unique_group("net-fan-to").await;
    let (txn, pid, epoch) = transactional_producer(&ctx).await?;
    let mut w = Wire::connect().await?;
    assert_eq!(w.add_offsets(&txn, pid, epoch, &group).await?, 0);
    let mut t = TxnOffsetCommitRequestTopic::default();
    t.name = topic_name(&probe);
    t.partitions = (0..12_000)
        .map(|i| {
            let mut p = TxnOffsetCommitRequestPartition::default();
            p.committed_offset = i;
            p
        })
        .collect();
    let mut req = TxnOffsetCommitRequest::default();
    req.transactional_id = TransactionalId(s(&txn));
    req.group_id = GroupId(s(&group));
    req.producer_id = ProducerId(pid);
    req.producer_epoch = epoch;
    req.generation_id = -1;
    req.topics = vec![t];
    let m = measure(&probe, 1, &req).await?;
    let entries: usize = m.response.topics.iter().map(|t| t.partitions.len()).sum();
    let codes = tally(
        m.response
            .topics
            .iter()
            .flat_map(|t| t.partitions.iter().map(|p| p.error_code)),
    );
    println!("  {entries} partition entries, codes {codes:?}");
    assert_not_stalled(&m);
    assert_eq!(entries, 1, "Kafka answers the repeated partition once");
    assert_eq!(codes, BTreeMap::from([(0, 1)]));
    let pending: i64 = ctx
        .db()
        .query_one(
            "SELECT pending_offset FROM kafka.txn_pending_offsets WHERE group_id = $1",
            &[&group],
        )
        .await?
        .get(0);
    assert_eq!(
        pending, 11_999,
        "the last entry for the partition is stored"
    );
    assert_eq!(w.end_txn(&txn, pid, epoch, false).await?, 0);
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// DeleteGroups v0 of 13,000 distinct unknown groups (182 KB): each GROUP_ID_NOT_FOUND.
pub async fn test_delete_groups_of_many_groups_is_answered_quickly() -> TestResult {
    println!("=== Test: DeleteGroups of 13,000 distinct unknown groups ===\n");
    let (ctx, probe) = setup("dg").await?;
    let prefix = short_name("dg");
    let mut req = DeleteGroupsRequest::default();
    req.groups_names = numbered(&prefix, 13_000)
        .iter()
        .map(|g| GroupId(s(g)))
        .collect();
    let m = measure(&probe, 0, &req).await?;
    let codes = tally(m.response.results.iter().map(|r| r.error_code));
    println!("  codes {codes:?}");
    assert_not_stalled(&m);
    assert_eq!(codes, BTreeMap::from([(69, 13_000)]));
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// DescribeGroups v0 of 13,000 distinct unknown groups (182 KB): each Dead, no error.
pub async fn test_describe_groups_of_many_groups_is_answered_quickly() -> TestResult {
    println!("=== Test: DescribeGroups of 13,000 distinct unknown groups ===\n");
    let (ctx, probe) = setup("dsg").await?;
    let prefix = short_name("ds");
    let mut req = DescribeGroupsRequest::default();
    req.groups = numbered(&prefix, 13_000)
        .iter()
        .map(|g| GroupId(s(g)))
        .collect();
    let m = measure(&probe, 0, &req).await?;
    let codes = tally(m.response.groups.iter().map(|g| g.error_code));
    let states = m
        .response
        .groups
        .iter()
        .map(|g| g.group_state.to_string())
        .collect::<std::collections::BTreeSet<_>>();
    println!("  codes {codes:?}, states {states:?}");
    assert_not_stalled(&m);
    assert_eq!(codes, BTreeMap::from([(0, 13_000)]));
    assert_eq!(
        states,
        std::collections::BTreeSet::from(["Dead".to_string()])
    );
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// Fetch v4 of partition 0 of one topic 10,000 times (160 KB), the partition holding one
/// record: Kafka reads the partition once and answers it once.
pub async fn test_fetch_of_a_repeated_partition_is_answered_quickly() -> TestResult {
    println!("=== Test: Fetch of one partition repeated 10,000 times ===\n");
    let (ctx, probe) = setup("fe").await?;
    let target = short_topic("fe", 1).await?;
    let mut w = Wire::connect().await?;
    let value = plain_batch(&[Bytes::from_static(b"r")], Compression::None);
    assert_eq!(w.produce(&target, 0, value, None).await?.0, 0);
    let mut t = FetchTopic::default();
    t.topic = topic_name(&target);
    t.partitions = (0..10_000)
        .map(|_| {
            let mut p = FetchPartition::default();
            p.partition_max_bytes = 1_048_576;
            p
        })
        .collect();
    let mut req = FetchRequest::default();
    req.replica_id = BrokerId(-1);
    req.max_wait_ms = 0;
    req.min_bytes = 1;
    req.max_bytes = 52_428_800;
    req.topics = vec![t];
    let m = measure(&probe, 4, &req).await?;
    let entries: usize = m
        .response
        .responses
        .iter()
        .map(|t| t.partitions.len())
        .sum();
    let with_records = m
        .response
        .responses
        .iter()
        .flat_map(|t| t.partitions.iter())
        .filter(|p| p.records.as_ref().is_some_and(|r| !r.is_empty()))
        .count();
    let codes = tally(
        m.response
            .responses
            .iter()
            .flat_map(|t| t.partitions.iter().map(|p| p.error_code)),
    );
    println!("  {entries} partition entries ({with_records} with records), codes {codes:?}");
    assert_not_stalled(&m);
    assert_eq!(
        (entries, with_records),
        (1, 1),
        "Kafka answers the partition once"
    );
    assert_eq!(codes, BTreeMap::from([(0, 1)]));
    drop_topics(&ctx, &target).await?;
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
