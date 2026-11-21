//! One-call raw requests for the admin and consumer-group APIs.

use crate::common::broker_addr;
use crate::common::DynResult;
use crate::wire::records::batch;
use crate::wire::roundtrip;
use bytes::Bytes;
use kafka_protocol::messages::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use kafka_protocol::messages::delete_groups_request::DeleteGroupsRequest;
use kafka_protocol::messages::describe_groups_request::DescribeGroupsRequest;
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchRequest, FetchTopic};
use kafka_protocol::messages::init_producer_id_request::InitProducerIdRequest;
use kafka_protocol::messages::join_group_request::{JoinGroupRequest, JoinGroupRequestProtocol};
use kafka_protocol::messages::leave_group_request::LeaveGroupRequest;
use kafka_protocol::messages::list_groups_request::ListGroupsRequest;
use kafka_protocol::messages::metadata_request::{MetadataRequest, MetadataRequestTopic};
use kafka_protocol::messages::offset_commit_request::{
    OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
use kafka_protocol::messages::produce_request::{
    PartitionProduceData, ProduceRequest, TopicProduceData,
};
use kafka_protocol::messages::sync_group_request::{SyncGroupRequest, SyncGroupRequestAssignment};
use kafka_protocol::messages::{GroupId, ProducerId, TopicName, TransactionalId};
use kafka_protocol::protocol::StrBytes;
use std::time::Duration;
use tokio::net::TcpStream;

pub(crate) fn s(v: &str) -> StrBytes {
    StrBytes::from_string(v.to_string())
}

pub(crate) async fn conn() -> DynResult<TcpStream> {
    Ok(TcpStream::connect(broker_addr()).await?)
}

pub(crate) fn creatable(name: &str, partitions: i32, rf: i16) -> CreatableTopic {
    let mut t = CreatableTopic::default();
    t.name = TopicName(s(name));
    t.num_partitions = partitions;
    t.replication_factor = rf;
    t
}

/// One topic's result in a CreateTopics response.
#[derive(Debug)]
pub(crate) struct CreatedTopic {
    pub name: String,
    pub error_code: i16,
    pub error_message: Option<String>,
}

/// One topic's entry in a Metadata response.
#[derive(Debug)]
pub(crate) struct TopicMetadata {
    pub error_code: i16,
    pub partitions: usize,
}

/// CreateTopics v5 over a raw connection; one result per distinct topic.
pub(crate) async fn create_topics_raw(topics: Vec<CreatableTopic>) -> DynResult<Vec<CreatedTopic>> {
    let mut c = conn().await?;
    let mut req = CreateTopicsRequest::default();
    req.topics = topics;
    req.timeout_ms = 10_000;
    let resp = roundtrip(&mut c, 5, 1, &req).await?;
    Ok(resp
        .topics
        .into_iter()
        .map(|t| CreatedTopic {
            name: t.name.0.to_string(),
            error_code: t.error_code,
            error_message: t.error_message.map(|m| m.to_string()),
        })
        .collect())
}

/// Metadata v9 of `topics` (no auto-creation), in request order.
pub(crate) async fn metadata_raw(
    c: &mut TcpStream,
    topics: &[&str],
) -> DynResult<Vec<TopicMetadata>> {
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
        .map(|t| TopicMetadata {
            error_code: t.error_code,
            partitions: t.partitions.len(),
        })
        .collect())
}

pub(crate) fn protocol(name: &str) -> JoinGroupRequestProtocol {
    let mut p = JoinGroupRequestProtocol::default();
    p.name = s(name);
    p.metadata = Bytes::from_static(b"\x00\x01");
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
    a.assignment = Bytes::from_static(b"");
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

/// A Produce request: one topic, `(partition, batch)` pairs.
pub(crate) fn produce_request(topic: &str, parts: Vec<(i32, Bytes)>, acks: i16) -> ProduceRequest {
    let mut t = TopicProduceData::default();
    t.name = TopicName(StrBytes::from_string(topic.to_string()));
    for (index, batch) in parts {
        let mut p = PartitionProduceData::default();
        p.index = index;
        p.records = Some(batch);
        t.partition_data.push(p);
    }
    let mut r = ProduceRequest::default();
    r.acks = acks;
    r.timeout_ms = 5000;
    r.topic_data = vec![t];
    r
}

pub(crate) fn fetch_request(
    topic: &str,
    partition: i32,
    offset: i64,
    max_wait_ms: i32,
) -> FetchRequest {
    let mut p = FetchPartition::default();
    p.partition = partition;
    p.fetch_offset = offset;
    p.partition_max_bytes = 1024 * 1024;
    let mut t = FetchTopic::default();
    t.topic = TopicName(StrBytes::from_string(topic.to_string()));
    t.partitions = vec![p];
    let mut req = FetchRequest::default();
    req.replica_id = kafka_protocol::messages::BrokerId(-1);
    req.max_wait_ms = max_wait_ms;
    req.min_bytes = 1;
    req.max_bytes = 1024 * 1024;
    req.topics = vec![t];
    req
}

/// An InitProducerId request for a new producer id (`transactional_id` None: an
/// idempotent producer).
pub(crate) fn init_producer_id_request(
    transactional_id: Option<&str>,
    transaction_timeout_ms: i32,
) -> InitProducerIdRequest {
    let mut request = InitProducerIdRequest::default();
    request.transactional_id =
        transactional_id.map(|id| TransactionalId(StrBytes::from_string(id.to_string())));
    request.transaction_timeout_ms = transaction_timeout_ms;
    request.producer_id = ProducerId(-1);
    request.producer_epoch = -1;
    request
}

/// An acks=1 Produce of `values` to one partition, written by `producer_id`/`epoch`
/// from `base_sequence` (all -1 for a plain producer).
pub(crate) fn sequenced_produce_request(
    topic: &str,
    partition: i32,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    values: &[&str],
) -> ProduceRequest {
    let records = batch(
        values,
        producer_id,
        producer_epoch,
        base_sequence,
        false,
        None,
    );
    produce_request(topic, vec![(partition, records)], 1)
}
