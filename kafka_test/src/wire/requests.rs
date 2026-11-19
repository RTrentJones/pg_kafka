//! One-call raw requests for the admin and consumer-group APIs.

use crate::common::{create_producer, get_bootstrap_servers, TestResult};
use crate::error_paths::bug_hunt::roundtrip;
use crate::fixtures::TestTopicBuilder;
use crate::setup::TestContext;
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
use rdkafka::admin::AdminClient;
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::producer::FutureRecord;
use std::time::Duration;
use tokio::net::TcpStream;

pub(crate) const BROKER: &str = "127.0.0.1:9092";

pub(crate) const ERROR_NONE: i16 = 0;

pub(crate) const ERROR_UNKNOWN_SERVER_ERROR: i16 = -1;

pub(crate) const ERROR_ILLEGAL_GENERATION: i16 = 22;

pub(crate) const ERROR_INCONSISTENT_GROUP_PROTOCOL: i16 = 23;

pub(crate) const ERROR_UNKNOWN_MEMBER_ID: i16 = 25;

pub(crate) const ERROR_INVALID_PARTITIONS: i16 = 37;

pub(crate) const ERROR_INVALID_REPLICATION_FACTOR: i16 = 38;

pub(crate) const ERROR_INVALID_REQUEST: i16 = 42;

pub(crate) const ERROR_INVALID_TOPIC_EXCEPTION: i16 = 17;

pub(crate) const ERROR_GROUP_ID_NOT_FOUND: i16 = 69;

pub(crate) type DynResult<T> = Result<T, Box<dyn std::error::Error>>;

pub(crate) fn admin() -> DynResult<AdminClient<DefaultClientContext>> {
    Ok(ClientConfig::new()
        .set("bootstrap.servers", get_bootstrap_servers())
        .set("broker.address.family", "v4")
        .create()?)
}

pub(crate) fn s(v: &str) -> StrBytes {
    StrBytes::from_string(v.to_string())
}

pub(crate) async fn conn() -> DynResult<TcpStream> {
    Ok(TcpStream::connect(BROKER).await?)
}

pub(crate) fn creatable(name: &str, partitions: i32, rf: i16) -> CreatableTopic {
    let mut t = CreatableTopic::default();
    t.name = TopicName(s(name));
    t.num_partitions = partitions;
    t.replication_factor = rf;
    t
}

/// CreateTopics v5 over a raw connection; `(name, error_code, message)` per result.
pub(crate) async fn create_topics_raw(
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

pub(crate) async fn metadata_raw(
    c: &mut TcpStream,
    topics: &[&str],
) -> DynResult<Vec<(String, i16, usize)>> {
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

pub(crate) fn protocol(name: &str) -> JoinGroupRequestProtocol {
    let mut p = JoinGroupRequestProtocol::default();
    p.name = s(name);
    p.metadata = bytes::Bytes::from_static(b"\x00\x01");
    p
}

pub(crate) fn join_req(
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
pub(crate) async fn join_now(
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

pub(crate) async fn sync_leader(
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

pub(crate) async fn leave(c: &mut TcpStream, group: &str, member: &str) -> DynResult<i16> {
    let mut r = LeaveGroupRequest::default();
    r.group_id = GroupId(s(group));
    r.member_id = s(member);
    Ok(roundtrip(c, 1, 5, &r).await?.error_code)
}

/// OffsetCommit v2 of one offset; the partition's error code.
pub(crate) async fn commit(
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
pub(crate) async fn describe(group: &str) -> DynResult<(i16, String, String, usize)> {
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
pub(crate) async fn list_groups() -> DynResult<Vec<(String, String, String)>> {
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

pub(crate) async fn delete_group(group: &str) -> DynResult<i16> {
    let mut c = conn().await?;
    let mut r = DeleteGroupsRequest::default();
    r.groups_names = vec![GroupId(s(group))];
    Ok(roundtrip(&mut c, 1, 9, &r).await?.results[0].error_code)
}

pub(crate) async fn produce_one(topic: &str) -> TestResult {
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

pub(crate) const NONE: i16 = 0;

pub(crate) const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;

pub(crate) const OUT_OF_ORDER_SEQUENCE_NUMBER: i16 = 45;

pub(crate) const INVALID_PRODUCER_EPOCH: i16 = 47;

pub(crate) const INVALID_TXN_STATE: i16 = 48;

pub(crate) const INVALID_TRANSACTION_TIMEOUT: i16 = 50;

pub(crate) const OPERATION_NOT_ATTEMPTED: i16 = 55;

pub(crate) const UNSTABLE_OFFSET_COMMIT: i16 = 88;

pub(crate) async fn topic(
    ctx: &TestContext,
    base: &str,
    partitions: i32,
) -> Result<String, Box<dyn std::error::Error>> {
    Ok(TestTopicBuilder::new(ctx, base)
        .with_partitions(partitions)
        .build()
        .await?
        .name)
}
