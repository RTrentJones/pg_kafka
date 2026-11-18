//! Stream `api`: every request that fails after a valid header gets a well-formed response of
//! its own API and version, carrying an error code and the request's correlation id.
//!
//! These tests speak the wire protocol directly (kafka-protocol encodes the requests and
//! decodes the responses) so they can send what real clients never do: versions outside the
//! advertised range, bodies cut short, and requests whose handler fails on the DB thread.
//! Each response must decode completely in the request's version, which is what a strict
//! client does with it.

use crate::common::TestResult;
use crate::setup::TestContext;
use bytes::{Buf, BytesMut};
use kafka_protocol::messages::{
    fetch_request, list_offsets_request, metadata_request, offset_fetch_request, produce_request,
    ApiKey, FetchRequest, FetchResponse, FindCoordinatorRequest, FindCoordinatorResponse, GroupId,
    HeartbeatRequest, HeartbeatResponse, InitProducerIdRequest, InitProducerIdResponse,
    JoinGroupRequest, JoinGroupResponse, ListOffsetsRequest, ListOffsetsResponse, MetadataRequest,
    MetadataResponse, OffsetFetchRequest, OffsetFetchResponse, ProduceRequest, ProduceResponse,
    RequestHeader, ResponseHeader, TopicName,
};
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, StrBytes};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const BROKER: &str = "localhost:9092";
const UNKNOWN_SERVER_ERROR: i16 = -1;
const UNSUPPORTED_VERSION: i16 = 35;
const INVALID_REQUEST: i16 = 42;

type BoxError = Box<dyn std::error::Error>;

/// Encode `request` (header + body) for API `key` at `version`, size-prefixed.
fn frame<R: Encodable>(key: ApiKey, version: i16, correlation_id: i32, request: &R) -> BytesMut {
    let mut payload = BytesMut::new();
    RequestHeader::default()
        .with_request_api_key(key as i16)
        .with_request_api_version(version)
        .with_correlation_id(correlation_id)
        .with_client_id(Some(StrBytes::from_static_str("api-errors")))
        .encode(&mut payload, key.request_header_version(version))
        .unwrap();
    request.encode(&mut payload, version).unwrap();
    let mut framed = BytesMut::new();
    framed.extend_from_slice(&(payload.len() as i32).to_be_bytes());
    framed.extend_from_slice(&payload);
    framed
}

/// Read one response frame (`None` if the broker closed the connection).
async fn read_frame(stream: &mut TcpStream) -> Result<Option<BytesMut>, BoxError> {
    let mut size = [0u8; 4];
    match tokio::time::timeout(Duration::from_secs(10), stream.read_exact(&mut size)).await? {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    let mut body = vec![0u8; i32::from_be_bytes(size) as usize];
    stream.read_exact(&mut body).await?;
    Ok(Some(BytesMut::from(&body[..])))
}

/// Send `framed`, read the reply and decode it completely as `R` at `version`, checking the
/// correlation id. A connection close or trailing bytes fail the test.
async fn exchange<R: Decodable + HeaderVersion>(
    stream: &mut TcpStream,
    framed: &[u8],
    version: i16,
    correlation_id: i32,
) -> Result<R, BoxError> {
    stream.write_all(framed).await?;
    let mut reply = read_frame(stream)
        .await?
        .ok_or("the broker closed the connection instead of answering")?;
    let header = ResponseHeader::decode(&mut reply, R::header_version(version))?;
    assert_eq!(header.correlation_id, correlation_id, "correlation id");
    let response = R::decode(&mut reply, version)?;
    assert!(
        !reply.has_remaining(),
        "{} bytes left after decoding the response",
        reply.remaining()
    );
    Ok(response)
}

/// The connection still serves requests: a FindCoordinator v2 round trip succeeds.
async fn connection_still_usable(stream: &mut TcpStream) -> Result<(), BoxError> {
    let mut request = FindCoordinatorRequest::default();
    request.key = StrBytes::from_static_str("api-errors-probe");
    let framed = frame(ApiKey::FindCoordinator, 2, 9_999, &request);
    let response: FindCoordinatorResponse = exchange(stream, &framed, 2, 9_999).await?;
    assert_eq!(response.error_code, 0, "the connection keeps working");
    Ok(())
}

fn topic(name: &str) -> TopicName {
    TopicName(StrBytes::from_string(name.to_string()))
}

/// A version above the advertised range is answered with UNSUPPORTED_VERSION in that
/// version, for APIs with a top-level error code and for APIs whose errors live on the
/// echoed topics/partitions/groups.
pub async fn test_unsupported_versions_get_typed_error_responses() -> TestResult {
    println!("=== Test: Unsupported Versions Get Typed Error Responses ===\n");
    let mut stream = TcpStream::connect(BROKER).await?;

    // Metadata v11 (max 10): no top-level code; the requested topic carries it.
    let mut request = MetadataRequest::default();
    let mut t = metadata_request::MetadataRequestTopic::default();
    t.name = Some(topic("api-errors-meta"));
    request.topics = Some(vec![t]);
    let response: MetadataResponse = exchange(
        &mut stream,
        &frame(ApiKey::Metadata, 11, 101, &request),
        11,
        101,
    )
    .await?;
    assert_eq!(response.topics.len(), 1);
    assert_eq!(response.topics[0].error_code, UNSUPPORTED_VERSION);
    println!(
        "✅ Metadata v11: topic error {}",
        response.topics[0].error_code
    );

    // Fetch v12 (max 11): top-level code plus the echoed partition.
    let mut request = FetchRequest::default();
    let mut t = fetch_request::FetchTopic::default();
    t.topic = topic("api-errors-fetch");
    let mut p = fetch_request::FetchPartition::default();
    p.partition = 2;
    t.partitions = vec![p];
    request.topics = vec![t];
    let response: FetchResponse = exchange(
        &mut stream,
        &frame(ApiKey::Fetch, 12, 102, &request),
        12,
        102,
    )
    .await?;
    assert_eq!(response.error_code, UNSUPPORTED_VERSION);
    assert_eq!(response.responses[0].partitions[0].partition_index, 2);
    assert_eq!(
        response.responses[0].partitions[0].error_code,
        UNSUPPORTED_VERSION
    );
    println!("✅ Fetch v12: top-level and partition error");

    // Produce v10 (max 9): no top-level code; every partition carries it.
    let mut request = ProduceRequest::default();
    request.acks = 1;
    request.timeout_ms = 1000;
    let mut t = produce_request::TopicProduceData::default();
    t.name = topic("api-errors-produce");
    let mut p = produce_request::PartitionProduceData::default();
    p.index = 0;
    t.partition_data = vec![p];
    request.topic_data = vec![t];
    let response: ProduceResponse = exchange(
        &mut stream,
        &frame(ApiKey::Produce, 10, 103, &request),
        10,
        103,
    )
    .await?;
    assert_eq!(
        response.responses[0].partition_responses[0].error_code,
        UNSUPPORTED_VERSION
    );
    println!("✅ Produce v10: partition error");

    // OffsetFetch v8 (max 7): the error moves into the per-group entry.
    let mut request = OffsetFetchRequest::default();
    let mut g = offset_fetch_request::OffsetFetchRequestGroup::default();
    g.group_id = GroupId(StrBytes::from_static_str("api-errors-group"));
    request.groups = vec![g];
    let response: OffsetFetchResponse = exchange(
        &mut stream,
        &frame(ApiKey::OffsetFetch, 8, 104, &request),
        8,
        104,
    )
    .await?;
    assert_eq!(response.groups.len(), 1);
    assert_eq!(response.groups[0].group_id.as_str(), "api-errors-group");
    assert_eq!(response.groups[0].error_code, UNSUPPORTED_VERSION);
    println!("✅ OffsetFetch v8: group error");

    // JoinGroup v8 (max 7): top-level code.
    let mut request = JoinGroupRequest::default();
    request.group_id = GroupId(StrBytes::from_static_str("api-errors-group"));
    request.protocol_type = StrBytes::from_static_str("consumer");
    let response: JoinGroupResponse = exchange(
        &mut stream,
        &frame(ApiKey::JoinGroup, 8, 105, &request),
        8,
        105,
    )
    .await?;
    assert_eq!(response.error_code, UNSUPPORTED_VERSION);
    println!("✅ JoinGroup v8: top-level error");

    connection_still_usable(&mut stream).await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// A body cut short after a valid header (the kafka-protocol decoder runs out of input) is
/// answered with INVALID_REQUEST in the API's own schema, and the connection stays usable.
pub async fn test_truncated_body_gets_invalid_request() -> TestResult {
    println!("=== Test: Truncated Body Gets INVALID_REQUEST ===\n");
    let mut stream = TcpStream::connect(BROKER).await?;

    /// `framed` without its last `cut` bytes, with the size prefix adjusted.
    fn truncate(framed: &BytesMut, cut: usize) -> BytesMut {
        let payload = &framed[4..framed.len() - cut];
        let mut out = BytesMut::new();
        out.extend_from_slice(&(payload.len() as i32).to_be_bytes());
        out.extend_from_slice(payload);
        out
    }

    let mut heartbeat = HeartbeatRequest::default();
    heartbeat.group_id = GroupId(StrBytes::from_static_str("api-errors-group"));
    heartbeat.generation_id = 1;
    heartbeat.member_id = StrBytes::from_static_str("member-1");
    let framed = truncate(&frame(ApiKey::Heartbeat, 3, 201, &heartbeat), 6);
    let response: HeartbeatResponse = exchange(&mut stream, &framed, 3, 201).await?;
    assert_eq!(response.error_code, INVALID_REQUEST);
    println!("✅ Heartbeat v3: error {}", response.error_code);

    let mut init = InitProducerIdRequest::default();
    init.transaction_timeout_ms = 60_000;
    let framed = truncate(&frame(ApiKey::InitProducerId, 1, 202, &init), 3);
    let response: InitProducerIdResponse = exchange(&mut stream, &framed, 1, 202).await?;
    assert_eq!(response.error_code, INVALID_REQUEST);
    println!("✅ InitProducerId v1: error {}", response.error_code);

    // Produce has no top-level code; a body that cannot be decoded has no partitions to
    // echo, but the (empty) response is still a well-formed Produce response.
    let mut produce = ProduceRequest::default();
    produce.acks = 1;
    produce.timeout_ms = 1000;
    let mut t = produce_request::TopicProduceData::default();
    t.name = topic("api-errors-produce");
    produce.topic_data = vec![t];
    let framed = truncate(&frame(ApiKey::Produce, 7, 203, &produce), 4);
    let response: ProduceResponse = exchange(&mut stream, &framed, 7, 203).await?;
    assert!(response.responses.is_empty());
    println!("✅ Produce v7: well-formed response");

    connection_still_usable(&mut stream).await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// A handler that fails on the DB thread (here: a Postgres ERROR because the worker role
/// lost SELECT on kafka.partition_offsets, which pgrx raises as a panic) is answered with
/// UNKNOWN_SERVER_ERROR on every requested partition. ListOffsets has no top-level error
/// code, so before the API registry this was an empty response the client read as "no
/// partitions".
pub async fn test_handler_failure_gets_typed_error_response() -> TestResult {
    println!("=== Test: Handler Failure Gets Typed Error Response ===\n");
    let ctx = TestContext::new().await?;
    let topic_name = ctx.unique_topic("api-errors-lo").await;
    ctx.db()
        .execute(
            "INSERT INTO kafka.topics (name, partitions) VALUES ($1, 2)",
            &[&topic_name],
        )
        .await?;

    let role: String = ctx
        .db()
        .query_one("SHOW pg_kafka.run_as_role", &[])
        .await?
        .get(0);
    if role.is_empty() {
        return Err("pg_kafka.run_as_role is empty: the worker is superuser and cannot be made to fail by a REVOKE".into());
    }

    ctx.db()
        .execute(
            &format!("REVOKE SELECT ON kafka.partition_offsets FROM {}", role),
            &[],
        )
        .await?;
    // Keep the error as a String: a boxed error is not Send across the GRANT below.
    let result = list_offsets_while_revoked(&topic_name)
        .await
        .map_err(|e| e.to_string());
    ctx.db()
        .execute(
            &format!("GRANT SELECT ON kafka.partition_offsets TO {}", role),
            &[],
        )
        .await?;
    let response = result?;

    let partitions = &response.topics[0].partitions;
    assert_eq!(partitions.len(), 2, "both requested partitions are echoed");
    for p in partitions {
        assert_eq!(
            p.error_code, UNKNOWN_SERVER_ERROR,
            "partition {}",
            p.partition_index
        );
        assert_eq!(p.offset, -1);
    }
    println!("✅ ListOffsets: UNKNOWN_SERVER_ERROR on both partitions");

    // After the GRANT the same request succeeds again.
    let mut stream = TcpStream::connect(BROKER).await?;
    let response: ListOffsetsResponse = exchange(
        &mut stream,
        &frame(ApiKey::ListOffsets, 5, 302, &list_offsets(&topic_name)),
        5,
        302,
    )
    .await?;
    assert!(response.topics[0]
        .partitions
        .iter()
        .all(|p| p.error_code == 0));
    println!("✅ ListOffsets succeeds once the privilege is back");

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

fn list_offsets(topic_name: &str) -> ListOffsetsRequest {
    let mut request = ListOffsetsRequest::default();
    request.replica_id = kafka_protocol::messages::BrokerId(-1);
    let mut t = list_offsets_request::ListOffsetsTopic::default();
    t.name = topic(topic_name);
    t.partitions = (0..2)
        .map(|index| {
            let mut p = list_offsets_request::ListOffsetsPartition::default();
            p.partition_index = index;
            p.timestamp = -1; // latest: reads kafka.partition_offsets
            p
        })
        .collect();
    request.topics = vec![t];
    request
}

async fn list_offsets_while_revoked(topic_name: &str) -> Result<ListOffsetsResponse, BoxError> {
    let mut stream = TcpStream::connect(BROKER).await?;
    let response = exchange(
        &mut stream,
        &frame(ApiKey::ListOffsets, 5, 301, &list_offsets(topic_name)),
        5,
        301,
    )
    .await?;
    connection_still_usable(&mut stream).await?;
    Ok(response)
}
