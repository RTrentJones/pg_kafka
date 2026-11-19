//! Hostile clients against the listener in its default configuration.

use super::{
    answers, api_versions_frame, client_config, connect_from, rdkafka_roundtrip, stored_rows,
    time_until_closed, PORT,
};
use crate::common::to_i32;
use crate::common::{DynResult, TestResult};
use crate::error_codes;
use crate::setup::TestContext;
use crate::wire::records::plain_batch;
use crate::wire::requests::produce_request;
use crate::wire::{correlation_of, decode_response, read_frame_within, request_frame};
use bytes::Bytes;
use kafka_protocol::messages::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchRequest, FetchTopic};
use kafka_protocol::messages::{
    ApiVersionsRequest, BrokerId, CreateTopicsResponse, FetchResponse, ProduceResponse,
};
use kafka_protocol::messages::{ProduceRequest, TopicName};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::Compression;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::net::Ipv4Addr;
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

/// A frame the broker cannot parse (header cut short): it closes the connection.
const BAD_FRAME: [u8; 7] = [0, 0, 0, 3, 0, 0x12, 0];

fn create_topic_request(topic: &str, partitions: i32) -> CreateTopicsRequest {
    let mut t = CreatableTopic::default();
    t.name = TopicName(StrBytes::from_string(topic.to_string()));
    t.num_partitions = partitions;
    t.replication_factor = 1;
    let mut r = CreateTopicsRequest::default();
    r.topics = vec![t];
    r.timeout_ms = 5000;
    r
}

fn fetch(
    topic: &str,
    partitions: &[i32],
    offset: i64,
    max_wait: i32,
    max_bytes: i32,
) -> FetchRequest {
    let mut t = FetchTopic::default();
    t.topic = TopicName(StrBytes::from_string(topic.to_string()));
    for &p in partitions {
        let mut fp = FetchPartition::default();
        fp.partition = p;
        fp.fetch_offset = offset;
        fp.partition_max_bytes = max_bytes;
        t.partitions.push(fp);
    }
    let mut r = FetchRequest::default();
    r.replica_id = BrokerId(-1);
    r.max_wait_ms = max_wait;
    r.min_bytes = 1;
    r.max_bytes = max_bytes
        .saturating_mul(to_i32(partitions.len()))
        .max(max_bytes);
    r.topics = vec![t];
    r
}

async fn produce_ok(
    stream: &mut TcpStream,
    corr: i32,
    topic: &str,
    value: &[u8],
) -> DynResult<i64> {
    let req = produce_request(
        topic,
        vec![(
            0,
            plain_batch(&[Bytes::copy_from_slice(value)], Compression::None),
        )],
        1,
    );
    stream.write_all(&request_frame(3, corr, &req)).await?;
    let frame = read_frame_within(stream, Duration::from_secs(10))
        .await?
        .ok_or("connection closed instead of a produce response")?;
    let (c, resp) = decode_response::<ProduceRequest>(3, &frame)?;
    assert_eq!(c, corr);
    let p = &resp.responses[0].partition_responses[0];
    if p.error_code != 0 {
        return Err(format!("produce error {}", p.error_code).into());
    }
    Ok(p.base_offset)
}

/// Six connections that each send only a 4-byte length prefix (99 MB, 37 MB) must not
/// hold the whole 512 MiB frame budget and lock every other client out: only bytes
/// actually received are charged, and small frames never are.
pub async fn test_partial_frames_cannot_exhaust_the_frame_budget() -> TestResult {
    println!("=== Test: frame-budget lockout ===\n");
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("net-budget").await;
    let attacker = Ipv4Addr::new(127, 0, 0, 2);

    let mut hogs = Vec::new();
    for len in [
        99_999_999i32,
        99_999_999,
        99_999_999,
        99_999_999,
        99_999_999,
        36_870_907,
    ] {
        let mut h = connect_from(attacker, PORT).await?;
        h.write_all(&len.to_be_bytes()).await?;
        hogs.push(h);
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    println!("Step 1: 6 hogs hold declared frames of ~536 MB");

    let mut victim = TcpStream::connect(("127.0.0.1", PORT)).await?;
    assert!(
        answers(&mut victim, 1).await,
        "a small ApiVersions must be served while the hogs hold their prefixes"
    );
    println!("✅ ApiVersions served");

    // A 2 MiB record (above the unmetered size: charged as it arrives).
    let base = produce_ok(&mut victim, 2, &topic, &vec![b'z'; 2 * 1024 * 1024]).await?;
    assert_eq!(base, 0);
    println!("✅ 2 MiB produce accepted");

    rdkafka_roundtrip(client_config(PORT), &topic, 10).await?;
    println!("✅ rdkafka produce/consume works during the attack");

    for h in &mut hogs {
        // The hogs are only waiting (their frames are within the read deadline).
        assert!(time_until_closed(h, Duration::from_millis(50))
            .await
            .is_none());
    }
    drop(hogs);
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// A response above the 100 MB inbound request limit is delivered (the outbound codec
/// does not apply the inbound cap; a refused encode would hang the connection). A
/// response is bounded by the request's max_bytes, as in Kafka.
pub async fn test_response_larger_than_the_request_limit_is_delivered() -> TestResult {
    println!("=== Test: >100 MB fetch response ===\n");
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("net-big").await;
    let mut s = TcpStream::connect(("127.0.0.1", PORT)).await?;
    let created: CreateTopicsResponse =
        crate::wire::roundtrip(&mut s, 2, 1, &create_topic_request(&topic, 2)).await?;
    assert_eq!(created.topics[0].error_code, 0);

    // 2 x 51 MB of zeros, gzip-compressed on the wire (a few hundred KB each).
    let value = Bytes::from(vec![0u8; 51_000_000]);
    for p in 0..2 {
        let batch = plain_batch(std::slice::from_ref(&value), Compression::Gzip);
        let req = produce_request(&topic, vec![(p, batch)], 1);
        s.write_all(&request_frame(3, 10 + p, &req)).await?;
        let f = read_frame_within(&mut s, Duration::from_secs(60))
            .await?
            .ok_or("closed on produce")?;
        let (_, resp) = decode_response::<ProduceRequest>(3, &f)?;
        assert_eq!(resp.responses[0].partition_responses[0].error_code, 0);
    }
    drop(value);
    println!("Step 1: produced 2 x 51 MB");

    let req = fetch(&topic, &[0, 1], 0, 0, 60_000_000);
    let started = Instant::now();
    s.write_all(&request_frame(4, 20, &req)).await?;
    let frame = read_frame_within(&mut s, Duration::from_secs(120))
        .await?
        .ok_or("the connection was closed instead of answering the fetch")?;
    println!(
        "Step 2: fetch response of {} bytes in {:?}",
        frame.len(),
        started.elapsed()
    );
    assert!(frame.len() > 100_000_000, "response must exceed 100 MB");
    let (corr, resp) = decode_response::<FetchRequest>(4, &frame)?;
    let resp: FetchResponse = resp;
    assert_eq!(corr, 20);
    assert_eq!(resp.responses[0].partitions.len(), 2);
    drop(frame);
    assert!(answers(&mut s, 21).await, "the connection keeps working");
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// Without TCP_NODELAY the second of two pipelined responses waits for the
/// client's delayed ACK (~40 ms on Linux).
pub async fn test_pipelined_requests_are_not_delayed_by_nagle() -> TestResult {
    println!("=== Test: TCP_NODELAY ===\n");
    let mut s = TcpStream::connect(("127.0.0.1", PORT)).await?;
    s.set_nodelay(true)?;
    let mut times = Vec::new();
    for round in 0..20 {
        let mut bytes = Vec::new();
        for i in 0..3 {
            bytes.extend_from_slice(&api_versions_frame(round * 10 + i));
        }
        let t0 = Instant::now();
        s.write_all(&bytes).await?;
        for _ in 0..3 {
            read_frame_within(&mut s, Duration::from_secs(3))
                .await?
                .ok_or("closed")?;
        }
        times.push(t0.elapsed());
    }
    times.sort();
    let median = times[times.len() / 2];
    println!("   3 pipelined ApiVersions: median {median:?}");
    assert!(
        median < Duration::from_millis(20),
        "median {median:?}: responses are held back by Nagle"
    );

    // A real client: 50 sequential acks=1 produces.
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("net-nodelay").await;
    rdkafka_roundtrip(client_config(PORT), &topic, 50).await?;
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// Kafka closes the connection when an acks=0 produce hits any partition error
/// (after the good partitions are written), so the client refreshes its metadata.
pub async fn test_acks0_produce_error_closes_connection() -> TestResult {
    println!("=== Test: acks=0 error closes the connection ===\n");
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("net-acks0").await;
    let mut s = TcpStream::connect(("127.0.0.1", PORT)).await?;
    produce_ok(&mut s, 1, &topic, b"seed").await?;

    // A good acks=0 produce: no response, connection stays open.
    let good = produce_request(
        &topic,
        vec![(
            0,
            plain_batch(&[Bytes::from_static(b"ok")], Compression::None),
        )],
        0,
    );
    s.write_all(&request_frame(3, 2, &good)).await?;
    assert!(
        answers(&mut s, 3).await,
        "a good acks=0 produce keeps the connection"
    );
    println!("✅ good acks=0 produce: connection open");

    // Partition 7 does not exist.
    let bad = produce_request(
        &topic,
        vec![(
            7,
            plain_batch(&[Bytes::from_static(b"x")], Compression::None),
        )],
        0,
    );
    s.write_all(&request_frame(3, 4, &bad)).await?;
    let closed = time_until_closed(&mut s, Duration::from_secs(5)).await;
    assert!(
        closed.is_some(),
        "acks=0 to a missing partition must close the connection"
    );
    println!(
        "✅ acks=0 to a missing partition: closed after {:?}",
        closed.unwrap()
    );

    // Mixed: partition 0 is written, then the connection closes.
    let mut s = TcpStream::connect(("127.0.0.1", PORT)).await?;
    let mixed = produce_request(
        &topic,
        vec![
            (
                0,
                plain_batch(&[Bytes::from_static(b"kept")], Compression::None),
            ),
            (
                7,
                plain_batch(&[Bytes::from_static(b"y")], Compression::None),
            ),
        ],
        0,
    );
    s.write_all(&request_frame(3, 5, &mixed)).await?;
    assert!(
        time_until_closed(&mut s, Duration::from_secs(5))
            .await
            .is_some(),
        "a mixed acks=0 produce with an error must close the connection"
    );
    assert_eq!(stored_rows(ctx.db(), &topic).await?, 3, "seed, ok, kept");
    println!("✅ mixed acks=0: good partition committed, connection closed");

    // A real acks=0 client keeps producing normally.
    let mut cfg = client_config(PORT);
    cfg.set("acks", "0");
    let producer: FutureProducer = cfg.create()?;
    for i in 0..5 {
        producer
            .send(
                FutureRecord::<(), _>::to(&topic)
                    .payload(&format!("r{i}"))
                    .partition(0),
                Duration::from_secs(10),
            )
            .await
            .map_err(|(e, _)| e)?;
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    while stored_rows(ctx.db(), &topic).await? < 8 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(stored_rows(ctx.db(), &topic).await?, 8);
    println!("✅ rdkafka acks=0 producer unaffected");
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// Acks other than -1, 0, 1 are answered INVALID_REQUIRED_ACKS and nothing is
/// written.
pub async fn test_invalid_acks_rejected_and_nothing_written() -> TestResult {
    println!("=== Test: invalid acks ===\n");
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("net-acks").await;
    let mut s = TcpStream::connect(("127.0.0.1", PORT)).await?;
    for (i, acks) in [2i16, -2, 5, i16::MAX].into_iter().enumerate() {
        let req = produce_request(
            &topic,
            vec![(
                0,
                plain_batch(&[Bytes::from_static(b"no")], Compression::None),
            )],
            acks,
        );
        let resp: ProduceResponse = crate::wire::roundtrip(&mut s, 7, to_i32(i), &req).await?;
        let p = &resp.responses[0].partition_responses[0];
        assert_eq!(
            p.error_code,
            error_codes::INVALID_REQUIRED_ACKS,
            "acks={acks}"
        );
        assert_eq!(p.base_offset, -1);
    }
    assert_eq!(stored_rows(ctx.db(), &topic).await?, 0, "nothing written");
    for acks in [-1i16, 1] {
        let req = produce_request(
            &topic,
            vec![(
                0,
                plain_batch(&[Bytes::from_static(b"yes")], Compression::None),
            )],
            acks,
        );
        let resp: ProduceResponse = crate::wire::roundtrip(&mut s, 7, 100, &req).await?;
        assert_eq!(
            resp.responses[0].partition_responses[0].error_code, 0,
            "acks={acks}"
        );
    }
    assert_eq!(stored_rows(ctx.db(), &topic).await?, 2);
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// Kafka reads a connection serially, so a request before the frame that closes the
/// connection is always answered: the writer is not cut off after a grace period, which
/// would drop e.g. a long-poll fetch's response.
pub async fn test_request_before_closing_frame_is_answered() -> TestResult {
    println!("=== Test: responses before close ===\n");
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("net-close").await;
    let mut s = TcpStream::connect(("127.0.0.1", PORT)).await?;
    produce_ok(&mut s, 1, &topic, b"seed").await?;

    // (a) A long poll at the end of the partition (500 ms), then a bad frame.
    let mut s = TcpStream::connect(("127.0.0.1", PORT)).await?;
    let mut bytes = request_frame(4, 7, &fetch(&topic, &[0], 1, 500, 1024 * 1024));
    bytes.extend_from_slice(&BAD_FRAME);
    let t0 = Instant::now();
    s.write_all(&bytes).await?;
    let frame = read_frame_within(&mut s, Duration::from_secs(5))
        .await?
        .ok_or("the long-poll response was dropped when the connection closed")?;
    assert_eq!(correlation_of(&frame), 7);
    println!("✅ long-poll fetch answered after {:?}", t0.elapsed());
    assert!(time_until_closed(&mut s, Duration::from_secs(5))
        .await
        .is_some());

    // (b) An acks=1 produce followed at once by a bad frame: committed and answered.
    let mut s = TcpStream::connect(("127.0.0.1", PORT)).await?;
    let req = produce_request(
        &topic,
        vec![(
            0,
            plain_batch(&[Bytes::from_static(b"durable")], Compression::None),
        )],
        1,
    );
    let mut bytes = request_frame(3, 8, &req);
    bytes.extend_from_slice(&BAD_FRAME);
    s.write_all(&bytes).await?;
    let frame = read_frame_within(&mut s, Duration::from_secs(5))
        .await?
        .ok_or("the produce response was dropped when the connection closed")?;
    let (corr, resp) = decode_response::<ProduceRequest>(3, &frame)?;
    assert_eq!(corr, 8);
    assert_eq!(resp.responses[0].partition_responses[0].error_code, 0);
    assert_eq!(resp.responses[0].partition_responses[0].base_offset, 1);
    assert!(time_until_closed(&mut s, Duration::from_secs(5))
        .await
        .is_some());
    assert_eq!(stored_rows(ctx.db(), &topic).await?, 2);
    println!("✅ acks=1 produce answered and committed");

    // (c) Pipelined ApiVersions, then a bad frame: all answered, in order.
    let mut s = TcpStream::connect(("127.0.0.1", PORT)).await?;
    let mut bytes = Vec::new();
    for i in 0..20 {
        bytes.extend_from_slice(&api_versions_frame(100 + i));
    }
    bytes.extend_from_slice(&BAD_FRAME);
    s.write_all(&bytes).await?;
    for i in 0..20 {
        let f = read_frame_within(&mut s, Duration::from_secs(5))
            .await?
            .ok_or("an ApiVersions response was dropped")?;
        assert_eq!(correlation_of(&f), 100 + i);
        let (_, r) = decode_response::<ApiVersionsRequest>(0, &f)?;
        assert_eq!(r.error_code, 0);
    }
    assert!(time_until_closed(&mut s, Duration::from_secs(5))
        .await
        .is_some());
    println!("✅ 20 pipelined responses delivered before the close");
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
