//! Regression tests for the follow-up review after #98.
//!
//! Each test drives the broker the way a real client does (rdkafka where it can,
//! raw wire requests where a specific request shape is needed) and checks both the
//! protocol response and the stored state.

use crate::common::{create_transactional_producer, TestResult};
use crate::setup::TestContext;
use rdkafka::producer::{FutureRecord, Producer};
use std::time::Duration;

async fn count_messages(ctx: &TestContext, topic: &str) -> Result<i64, Box<dyn std::error::Error>> {
    Ok(ctx
        .db()
        .query_one(
            "SELECT COUNT(*) FROM kafka.messages m JOIN kafka.topics t ON m.topic_id = t.id
             WHERE t.name = $1 AND m.txn_state IS NULL",
            &[&topic],
        )
        .await?
        .get(0))
}

async fn produce_txn(
    txn_id: &str,
    topic: &str,
    count: usize,
    label: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let producer = create_transactional_producer(txn_id)?;
    producer.init_transactions(Duration::from_secs(10))?;
    producer.begin_transaction()?;
    for i in 0..count {
        let value = format!("{}-{}", label, i);
        producer
            .send(
                FutureRecord::<(), _>::to(topic)
                    .payload(&value)
                    .partition(0),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(e, _)| e)?;
    }
    producer.commit_transaction(Duration::from_secs(10))?;
    Ok(())
}

/// A transactional producer that restarts with the same transactional.id gets a new
/// epoch and restarts its sequence numbers at 0. Its records must be stored, not
/// dropped as duplicates of the previous epoch's sequence range.
pub async fn test_producer_restart_new_epoch_records_are_stored() -> TestResult {
    println!("=== Test: Producer Restart (New Epoch) Records Are Stored ===\n");

    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("review-epoch-restart").await;
    let txn_id = format!("review-epoch-restart-{}", uuid::Uuid::new_v4());

    println!("Step 1: first producer instance commits 10 records...");
    produce_txn(&txn_id, &topic, 10, "first").await?;
    assert_eq!(count_messages(&ctx, &topic).await?, 10);

    println!("Step 2: restarted instance (same transactional.id) commits 3 records...");
    produce_txn(&txn_id, &topic, 3, "second").await?;

    let stored = count_messages(&ctx, &topic).await?;
    println!("   committed records stored: {}", stored);
    if stored != 13 {
        return Err(format!(
            "expected 13 committed records after the restart, found {} \
             (the new epoch's batches were dropped as duplicates)",
            stored
        )
        .into());
    }

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// last_active_at only moves on InitProducerId, so an idempotent producer that keeps
/// producing for longer than the 7-day producer retention looked idle and was pruned
/// (with its sequences); its next produce then failed with UNKNOWN_PRODUCER_ID. The
/// sweep must treat recent sequence activity as liveness.
pub async fn test_active_idempotent_producer_survives_retention_sweep() -> TestResult {
    println!("=== Test: Active Idempotent Producer Survives Retention Sweep ===\n");

    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("review-producer-prune").await;
    let producer = crate::common::create_idempotent_producer()?;
    let send = |v: &'static str| {
        let producer = producer.clone();
        let topic = topic.clone();
        async move {
            producer
                .send(
                    FutureRecord::<(), _>::to(&topic).payload(v).partition(0),
                    Duration::from_secs(5),
                )
                .await
                .map_err(|(e, _)| e)
        }
    };
    send("before").await?;

    let producer_id: i64 = ctx
        .db()
        .query_one(
            "SELECT s.producer_id FROM kafka.producer_sequences s
             JOIN kafka.topics t ON s.topic_id = t.id WHERE t.name = $1",
            &[&topic],
        )
        .await?
        .get(0);
    // Pretend InitProducerId ran 8 days ago; the producer has been producing since.
    ctx.db()
        .execute(
            "UPDATE kafka.producer_ids SET last_active_at = NOW() - INTERVAL '8 days'
             WHERE producer_id = $1",
            &[&producer_id],
        )
        .await?;
    ctx.db()
        .query("SELECT * FROM pg_kafka_run_retention_sweep(0, 60)", &[])
        .await?;

    let still_there: i64 = ctx
        .db()
        .query_one(
            "SELECT COUNT(*) FROM kafka.producer_ids WHERE producer_id = $1",
            &[&producer_id],
        )
        .await?
        .get(0);
    println!(
        "   producer {} rows after sweep: {}",
        producer_id, still_there
    );
    if still_there != 1 {
        return Err("the sweep pruned a producer that is still producing".into());
    }

    send("after").await?;
    assert_eq!(count_messages(&ctx, &topic).await?, 2);

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// A producer fenced by a newer instance (same transactional.id) must get an error
/// when it tries to write again, and nothing it sends may become visible. The fenced
/// AddPartitionsToTxn used to come back as an empty response (its top-level error
/// field only exists from v4, and the broker advertises v0-3), which reads as success.
pub async fn test_fenced_producer_cannot_write() -> TestResult {
    println!("=== Test: Fenced Producer Cannot Write ===\n");

    let ctx = TestContext::new().await?;
    let topic = crate::fixtures::TestTopicBuilder::new(&ctx, "review-fenced-write")
        .with_partitions(2)
        .build()
        .await?;
    let txn_id = format!("review-fenced-{}", uuid::Uuid::new_v4());

    let zombie = create_transactional_producer(&txn_id)?;
    zombie.init_transactions(Duration::from_secs(10))?;
    let newer = create_transactional_producer(&txn_id)?;
    newer.init_transactions(Duration::from_secs(10))?;
    println!("Step 1: second instance initialised; the first is now fenced");

    // The zombie writes to a partition it never added, so the broker sees
    // AddPartitionsToTxn (then Produce) carrying the stale epoch.
    let outcome: Result<(), String> = async {
        zombie.begin_transaction().map_err(|e| e.to_string())?;
        zombie
            .send(
                FutureRecord::<(), _>::to(&topic.name)
                    .payload("zombie")
                    .partition(1),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(e, _)| e.to_string())?;
        zombie
            .commit_transaction(Duration::from_secs(10))
            .map_err(|e| e.to_string())
    }
    .await;
    println!("   zombie outcome: {:?}", outcome);

    let visible = count_messages(&ctx, &topic.name).await?;
    let fatal = zombie.client().fatal_error();
    println!(
        "   committed records: {}, fatal error: {:?}",
        visible, fatal
    );
    if outcome.is_ok() {
        return Err("the fenced producer's transaction was reported as committed".into());
    }
    // The broker must tell the client it was fenced (a fatal ProducerFenced error),
    // not leave it to time out.
    match fatal {
        Some((rdkafka::types::RDKafkaErrorCode::Fenced, _))
        | Some((rdkafka::types::RDKafkaErrorCode::ProducerFenced, _)) => {}
        other => {
            return Err(format!(
                "expected rdkafka to record a fatal ProducerFenced error, got {:?}",
                other
            )
            .into())
        }
    }
    assert_eq!(
        visible, 0,
        "a fenced producer's record must not be committed"
    );

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

// ----- wire-level protocol regressions -----

use bytes::{Buf, BufMut, BytesMut};
use kafka_protocol::messages::produce_request::{
    PartitionProduceData, ProduceRequest, TopicProduceData,
};
use kafka_protocol::messages::TopicName;
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{
    Compression, Record as PRecord, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const BROKER: &str = "localhost:9092";

/// Write a raw request frame with a v1 (non-flexible) header and an empty client id.
async fn send_raw(
    stream: &mut TcpStream,
    api_key: i16,
    version: i16,
    correlation_id: i32,
    body: &[u8],
) -> Result<(), Box<dyn std::error::Error>> {
    let mut frame = BytesMut::new();
    frame.put_i32((2 + 2 + 4 + 2 + body.len()) as i32);
    frame.put_i16(api_key);
    frame.put_i16(version);
    frame.put_i32(correlation_id);
    frame.put_i16(-1);
    frame.extend_from_slice(body);
    stream.write_all(&frame).await?;
    Ok(())
}

/// Read one response frame, or None if the broker closed the connection.
async fn read_raw(stream: &mut TcpStream) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
    let mut size = [0u8; 4];
    match tokio::time::timeout(Duration::from_secs(5), stream.read_exact(&mut size)).await? {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    let mut body = vec![0u8; i32::from_be_bytes(size) as usize];
    stream.read_exact(&mut body).await?;
    Ok(Some(body))
}

/// ApiVersions above the supported range is answered in the v0 format with
/// UNSUPPORTED_VERSION and the key list (KIP-511), instead of an encode failure
/// that drops the connection.
pub async fn test_api_versions_above_range_negotiates_down() -> TestResult {
    println!("=== Test: ApiVersions Above Range Negotiates Down ===\n");
    let mut stream = TcpStream::connect(BROKER).await?;
    // ApiVersions v3+ uses the flexible request header (v2), which ends with an empty
    // tagged-field section after the client id; the body is likewise just tagged fields.
    let mut frame = BytesMut::new();
    frame.put_i16(18);
    frame.put_i16(5);
    frame.put_i32(501);
    frame.put_i16(-1); // null client id
    frame.put_u8(0); // header tagged fields
    frame.put_u8(0); // body tagged fields
    let mut sized = BytesMut::new();
    sized.put_i32(frame.len() as i32);
    sized.extend_from_slice(&frame);
    stream.write_all(&sized).await?;
    let body = read_raw(&mut stream)
        .await?
        .ok_or("the broker closed the connection instead of answering ApiVersions v5")?;
    let mut b = &body[..];
    assert_eq!(b.get_i32(), 501, "correlation id");
    let error_code = b.get_i16();
    let key_count = b.get_i32();
    println!("   error_code={} api_keys={}", error_code, key_count);
    assert_eq!(error_code, 35, "UNSUPPORTED_VERSION");
    assert!(key_count > 0, "the key list lets the client pick a version");
    println!("✅ Test PASSED\n");
    Ok(())
}

/// Fetch v0-3 used to be advertised although the protocol crate cannot encode
/// them. The advertised range now starts at v4, and a v4 fetch works.
pub async fn test_fetch_advertised_minimum_is_usable() -> TestResult {
    println!("=== Test: Fetch Advertised Minimum Is Usable ===\n");
    let mut stream = TcpStream::connect(BROKER).await?;
    let response = crate::error_paths::bug_hunt::roundtrip(
        &mut stream,
        0,
        502,
        &kafka_protocol::messages::api_versions_request::ApiVersionsRequest::default(),
    )
    .await?;
    let fetch = response
        .api_keys
        .iter()
        .find(|k| k.api_key == 1)
        .ok_or("Fetch not advertised")?;
    println!(
        "   Fetch advertised v{}..=v{}",
        fetch.min_version, fetch.max_version
    );
    assert_eq!(fetch.min_version, 4);

    let ctx = TestContext::new().await?;
    let topic = crate::fixtures::TestTopicBuilder::new(&ctx, "review-fetch-min")
        .with_partitions(1)
        .build()
        .await?;
    let fetched = crate::error_paths::bug_hunt::roundtrip(
        &mut stream,
        fetch.min_version,
        503,
        &crate::error_paths::bug_hunt::fetch_request(&topic.name, 0, 0, 0),
    )
    .await?;
    assert_eq!(fetched.responses[0].partitions[0].error_code, 0);
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

fn record_batch(value: &str) -> BytesMut {
    let record = PRecord {
        transactional: false,
        control: false,
        partition_leader_epoch: 0,
        producer_id: -1,
        producer_epoch: -1,
        timestamp_type: TimestampType::Creation,
        offset: 0,
        sequence: -1,
        timestamp: 0,
        key: None,
        value: Some(bytes::Bytes::from(value.to_string())),
        headers: Default::default(),
    };
    let mut buf = BytesMut::new();
    RecordBatchEncoder::encode(
        &mut buf,
        [record].iter(),
        &RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        },
    )
    .expect("encode record batch");
    buf
}

fn produce_request(topic: &str, records: bytes::Bytes, acks: i16) -> ProduceRequest {
    let mut p = PartitionProduceData::default();
    p.index = 0;
    p.records = Some(records);
    let mut t = TopicProduceData::default();
    t.name = TopicName(StrBytes::from_string(topic.to_string()));
    t.partition_data = vec![p];
    let mut req = ProduceRequest::default();
    req.acks = acks;
    req.timeout_ms = 5000;
    req.topic_data = vec![t];
    req
}

/// Produce v3+ allows one record batch per partition. A second batch used to be
/// dropped while the request was acked; it must now be rejected (INVALID_RECORD)
/// with nothing stored.
pub async fn test_produce_two_batches_in_one_partition_is_rejected() -> TestResult {
    println!("=== Test: Two Record Batches In One Partition Are Rejected ===\n");
    let ctx = TestContext::new().await?;
    let topic = crate::fixtures::TestTopicBuilder::new(&ctx, "review-two-batches")
        .with_partitions(1)
        .build()
        .await?;
    let mut two = record_batch("first");
    two.extend_from_slice(&record_batch("second"));

    let mut stream = TcpStream::connect(BROKER).await?;
    let response = crate::error_paths::bug_hunt::roundtrip(
        &mut stream,
        7,
        504,
        &produce_request(&topic.name, two.freeze(), 1),
    )
    .await?;
    let p = &response.responses[0].partition_responses[0];
    let stored = count_messages(&ctx, &topic.name).await?;
    println!("   error_code={} stored={}", p.error_code, stored);
    assert_eq!(p.error_code, 87, "INVALID_RECORD");
    assert_eq!(stored, 0, "neither batch may be stored");

    // A single batch is still accepted.
    let ok = crate::error_paths::bug_hunt::roundtrip(
        &mut stream,
        7,
        505,
        &produce_request(&topic.name, record_batch("only").freeze(), 1),
    )
    .await?;
    assert_eq!(ok.responses[0].partition_responses[0].error_code, 0);
    assert_eq!(count_messages(&ctx, &topic.name).await?, 1);
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// An acks=0 produce never gets a reply, even when its batch is corrupt. An
/// unsolicited error reply would be read as the answer to the next request.
pub async fn test_corrupt_acks0_produce_sends_no_reply() -> TestResult {
    println!("=== Test: Corrupt acks=0 Produce Sends No Reply ===\n");
    let ctx = TestContext::new().await?;
    let topic = crate::fixtures::TestTopicBuilder::new(&ctx, "review-acks0-corrupt")
        .with_partitions(1)
        .build()
        .await?;
    let mut stream = TcpStream::connect(BROKER).await?;

    // Fire-and-forget produce with garbage in place of a record batch...
    let req = produce_request(
        &topic.name,
        bytes::Bytes::from_static(b"not a record batch at all"),
        0,
    );
    let mut header = kafka_protocol::messages::RequestHeader::default();
    header.request_api_key = 0;
    header.request_api_version = 7;
    header.correlation_id = 506;
    let mut body = BytesMut::new();
    use kafka_protocol::protocol::Encodable;
    header.encode(&mut body, 1)?;
    req.encode(&mut body, 7)?;
    let mut frame = BytesMut::new();
    frame.put_i32(body.len() as i32);
    frame.extend_from_slice(&body);
    stream.write_all(&frame).await?;

    // ...then an ordinary request: its reply must be the first frame we read.
    send_raw(&mut stream, 18, 0, 507, &[]).await?;
    let first = read_raw(&mut stream).await?.ok_or("connection closed")?;
    let correlation = (&first[..]).get_i32();
    println!("   first reply correlation id: {}", correlation);
    assert_eq!(
        correlation, 507,
        "the acks=0 produce must not have been answered"
    );

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
