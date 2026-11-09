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

// ----- storage / transaction regressions -----

/// A topic with a huge retention.ms (i64::MAX is a common "forever") used to
/// overflow the sweep's interval arithmetic, rolling back the whole sweep and
/// stopping retention for every topic.
pub async fn test_huge_retention_ms_does_not_break_sweep() -> TestResult {
    println!("=== Test: Huge retention.ms Does Not Break The Sweep ===\n");
    let ctx = TestContext::new().await?;
    let forever = crate::fixtures::TestTopicBuilder::new(&ctx, "review-retention-forever")
        .with_partitions(1)
        .build()
        .await?;
    let expiring = crate::fixtures::TestTopicBuilder::new(&ctx, "review-retention-zero")
        .with_partitions(1)
        .build()
        .await?;
    forever
        .produce(&crate::fixtures::generate_messages(1, "keep"))
        .await?;
    expiring
        .produce(&crate::fixtures::generate_messages(1, "drop"))
        .await?;
    ctx.db()
        .execute(
            "UPDATE kafka.topics SET retention_ms = 9223372036854775807 WHERE name = $1",
            &[&forever.name],
        )
        .await?;
    ctx.db()
        .execute(
            "UPDATE kafka.topics SET retention_ms = 0 WHERE name = $1",
            &[&expiring.name],
        )
        .await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let swept = ctx
        .db()
        .query("SELECT * FROM pg_kafka_run_retention_sweep(0, 60)", &[])
        .await;
    println!("   sweep result: {:?}", swept.as_ref().map(|r| r.len()));
    swept.map_err(|e| format!("the retention sweep failed: {}", e))?;

    let kept = count_messages(&ctx, &forever.name).await?;
    let dropped = count_messages(&ctx, &expiring.name).await?;
    println!(
        "   forever topic: {} row(s), retention 0 topic: {} row(s)",
        kept, dropped
    );
    assert_eq!(kept, 1, "retention.ms = i64::MAX keeps the record");
    assert_eq!(
        dropped, 0,
        "the other topic's expired record is still swept"
    );
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// When the broker aborts a transaction for exceeding its timeout, the stalled
/// producer must be fenced. Before, it kept its epoch: adding a new partition moved
/// the aborted transaction back to Ongoing and the producer committed only the
/// records written after the abort.
pub async fn test_timed_out_transaction_cannot_be_resumed() -> TestResult {
    println!("=== Test: Timed-Out Transaction Cannot Be Resumed ===\n");
    let ctx = TestContext::new().await?;
    let topic = crate::fixtures::TestTopicBuilder::new(&ctx, "review-txn-timeout")
        .with_partitions(2)
        .build()
        .await?;
    let txn_id = format!("review-txn-timeout-{}", uuid::Uuid::new_v4());
    let producer = create_transactional_producer(&txn_id)?;
    producer.init_transactions(Duration::from_secs(10))?;
    producer.begin_transaction()?;
    producer
        .send(
            FutureRecord::<(), _>::to(&topic.name)
                .payload("before-timeout")
                .partition(0),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(e, _)| e)?;

    // Make the transaction look expired and wait for the worker's timeout sweep
    // (every 10 s) to abort it.
    ctx.db()
        .execute(
            "UPDATE kafka.transactions SET started_at = NOW() - INTERVAL '1 hour', timeout_ms = 1000
             WHERE transactional_id = $1",
            &[&txn_id],
        )
        .await?;
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let state: String = ctx
            .db()
            .query_one(
                "SELECT state FROM kafka.transactions WHERE transactional_id = $1",
                &[&txn_id],
            )
            .await?
            .get(0);
        if state == "CompleteAbort" {
            break;
        }
        if std::time::Instant::now() > deadline {
            return Err(format!(
                "the timeout sweep never aborted the transaction (state {})",
                state
            )
            .into());
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    println!("Step 1: the broker aborted the expired transaction");

    // The stalled producer carries on: a new partition, then commit.
    let outcome: Result<(), String> = async {
        producer
            .send(
                FutureRecord::<(), _>::to(&topic.name)
                    .payload("after-timeout")
                    .partition(1),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(e, _)| e.to_string())?;
        producer
            .commit_transaction(Duration::from_secs(10))
            .map_err(|e| e.to_string())
    }
    .await;
    let committed = count_messages(&ctx, &topic.name).await?;
    println!(
        "   resumed commit: {:?}; committed rows: {}",
        outcome, committed
    );
    assert!(outcome.is_err(), "a timed-out transaction must not commit");
    assert_eq!(
        committed, 0,
        "no part of the timed-out transaction may commit"
    );
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// EndTxn retried after a lost response finds the transaction already finished
/// with the requested outcome; Kafka answers NONE, not INVALID_TXN_STATE.
pub async fn test_end_txn_retry_is_idempotent() -> TestResult {
    use crate::error_paths::bug_hunt::roundtrip;
    use kafka_protocol::messages::add_partitions_to_txn_request::{
        AddPartitionsToTxnRequest, AddPartitionsToTxnTopic,
    };
    use kafka_protocol::messages::end_txn_request::EndTxnRequest;
    use kafka_protocol::messages::init_producer_id_request::InitProducerIdRequest;
    use kafka_protocol::messages::{ProducerId, TransactionalId};
    println!("=== Test: EndTxn Retry Is Idempotent ===\n");
    let ctx = TestContext::new().await?;
    let topic = crate::fixtures::TestTopicBuilder::new(&ctx, "review-endtxn-retry")
        .with_partitions(1)
        .build()
        .await?;
    let txn_id = format!("review-endtxn-{}", uuid::Uuid::new_v4());
    let mut stream = TcpStream::connect(BROKER).await?;

    let mut init = InitProducerIdRequest::default();
    init.transactional_id = Some(TransactionalId(StrBytes::from_string(txn_id.clone())));
    init.transaction_timeout_ms = 60000;
    let ids = roundtrip(&mut stream, 2, 601, &init).await?;
    assert_eq!(ids.error_code, 0);

    let mut add = AddPartitionsToTxnRequest::default();
    add.v3_and_below_transactional_id = TransactionalId(StrBytes::from_string(txn_id.clone()));
    add.v3_and_below_producer_id = ids.producer_id;
    add.v3_and_below_producer_epoch = ids.producer_epoch;
    let mut t = AddPartitionsToTxnTopic::default();
    t.name = TopicName(StrBytes::from_string(topic.name.clone()));
    t.partitions = vec![0];
    add.v3_and_below_topics = vec![t];
    let added = roundtrip(&mut stream, 1, 602, &add).await?;
    assert_eq!(
        added.results_by_topic_v3_and_below[0].results_by_partition[0].partition_error_code,
        0
    );

    let mut end = EndTxnRequest::default();
    end.transactional_id = TransactionalId(StrBytes::from_string(txn_id.clone()));
    end.producer_id = ProducerId(ids.producer_id.0);
    end.producer_epoch = ids.producer_epoch;
    end.committed = true;
    let first = roundtrip(&mut stream, 1, 603, &end).await?;
    let retry = roundtrip(&mut stream, 1, 604, &end).await?;
    println!(
        "   EndTxn(commit): first={} retry={}",
        first.error_code, retry.error_code
    );
    assert_eq!(first.error_code, 0);
    assert_eq!(retry.error_code, 0, "a matching retry must succeed");

    // The opposite outcome is still refused.
    end.committed = false;
    let opposite = roundtrip(&mut stream, 1, 605, &end).await?;
    assert_eq!(opposite.error_code, 48, "INVALID_TXN_STATE");
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// Metadata (and Produce) auto-creation used to skip the topic-name rules
/// CreateTopics enforces, creating topics such as "bad name/x".
pub async fn test_auto_create_rejects_invalid_topic_name() -> TestResult {
    use kafka_protocol::messages::metadata_request::{MetadataRequest, MetadataRequestTopic};
    println!("=== Test: Auto-Create Rejects Invalid Topic Name ===\n");
    let ctx = TestContext::new().await?;
    let bad = format!("bad name/{}", uuid::Uuid::new_v4());
    let mut t = MetadataRequestTopic::default();
    t.name = Some(TopicName(StrBytes::from_string(bad.clone())));
    let mut req = MetadataRequest::default();
    req.topics = Some(vec![t]);
    req.allow_auto_topic_creation = true;
    let mut stream = TcpStream::connect(BROKER).await?;
    let response = crate::error_paths::bug_hunt::roundtrip(&mut stream, 4, 701, &req).await?;
    let code = response.topics[0].error_code;
    let rows = ctx
        .db()
        .query("SELECT 1 FROM kafka.topics WHERE name = $1", &[&bad])
        .await?;
    println!("   metadata error_code={} topic rows={}", code, rows.len());
    assert_eq!(code, 17, "INVALID_TOPIC_EXCEPTION");
    assert!(rows.is_empty(), "the invalid topic must not be created");
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// Headers come back in the order the producer sent them (they used to come back
/// sorted by key).
pub async fn test_headers_keep_their_order() -> TestResult {
    use rdkafka::consumer::{BaseConsumer, Consumer};
    use rdkafka::message::{Header, Headers, Message, OwnedHeaders};
    println!("=== Test: Headers Keep Their Order ===\n");
    let ctx = TestContext::new().await?;
    let topic = crate::fixtures::TestTopicBuilder::new(&ctx, "review-header-order")
        .with_partitions(1)
        .build()
        .await?;
    let producer = crate::common::create_producer()?;
    let headers = OwnedHeaders::new()
        .insert(Header {
            key: "zeta",
            value: Some("1"),
        })
        .insert(Header {
            key: "alpha",
            value: Some("2"),
        })
        .insert(Header {
            key: "mu",
            value: Some("3"),
        });
    producer
        .send(
            FutureRecord::<(), _>::to(&topic.name)
                .payload("v")
                .partition(0)
                .headers(headers),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(e, _)| e)?;

    let group = ctx.unique_group("review-header-order").await;
    let consumer: BaseConsumer = crate::common::create_base_consumer(&group)?;
    let mut tpl = rdkafka::TopicPartitionList::new();
    tpl.add_partition_offset(&topic.name, 0, rdkafka::Offset::Beginning)?;
    consumer.assign(&tpl)?;
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let keys: Vec<String> = loop {
        if let Some(Ok(msg)) = consumer.poll(Duration::from_millis(500)) {
            break msg
                .headers()
                .map(|hs| hs.iter().map(|h| h.key.to_string()).collect())
                .unwrap_or_default();
        }
        if std::time::Instant::now() > deadline {
            return Err("no record consumed".into());
        }
    };
    println!("   header keys: {:?}", keys);
    assert_eq!(keys, vec!["zeta", "alpha", "mu"]);
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

// ----- consumer-group coordinator regressions -----

mod group_wire {
    use super::*;
    use crate::error_paths::bug_hunt::roundtrip;
    use kafka_protocol::messages::heartbeat_request::HeartbeatRequest;
    use kafka_protocol::messages::join_group_request::{
        JoinGroupRequest, JoinGroupRequestProtocol,
    };
    use kafka_protocol::messages::offset_commit_request::{
        OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
    };
    use kafka_protocol::messages::sync_group_request::{
        SyncGroupRequest, SyncGroupRequestAssignment,
    };
    use kafka_protocol::messages::GroupId;

    pub struct Joined {
        pub member_id: String,
        pub generation: i32,
        pub leader: String,
    }

    pub async fn join(
        s: &mut TcpStream,
        group: &str,
        member_id: &str,
        metadata: &[u8],
        corr: i32,
    ) -> Result<Joined, Box<dyn std::error::Error>> {
        let mut p = JoinGroupRequestProtocol::default();
        p.name = StrBytes::from_static_str("range");
        p.metadata = bytes::Bytes::copy_from_slice(metadata);
        let mut req = JoinGroupRequest::default();
        req.group_id = GroupId(StrBytes::from_string(group.to_string()));
        req.session_timeout_ms = 30000;
        req.rebalance_timeout_ms = 60000;
        req.member_id = StrBytes::from_string(member_id.to_string());
        req.protocol_type = StrBytes::from_static_str("consumer");
        req.protocols = vec![p];
        let r = roundtrip(s, 5, corr, &req).await?;
        if r.error_code != 0 {
            return Err(format!("JoinGroup error {}", r.error_code).into());
        }
        Ok(Joined {
            member_id: r.member_id.to_string(),
            generation: r.generation_id,
            leader: r.leader.to_string(),
        })
    }

    pub async fn sync(
        s: &mut TcpStream,
        group: &str,
        member_id: &str,
        generation: i32,
        assignments: &[(&str, &[u8])],
        corr: i32,
    ) -> Result<(i16, Vec<u8>), Box<dyn std::error::Error>> {
        let mut req = SyncGroupRequest::default();
        req.group_id = GroupId(StrBytes::from_string(group.to_string()));
        req.generation_id = generation;
        req.member_id = StrBytes::from_string(member_id.to_string());
        req.assignments = assignments
            .iter()
            .map(|(m, a)| {
                let mut x = SyncGroupRequestAssignment::default();
                x.member_id = StrBytes::from_string(m.to_string());
                x.assignment = bytes::Bytes::copy_from_slice(a);
                x
            })
            .collect();
        let r = roundtrip(s, 3, corr, &req).await?;
        Ok((r.error_code, r.assignment.to_vec()))
    }

    pub async fn heartbeat(
        s: &mut TcpStream,
        group: &str,
        member_id: &str,
        generation: i32,
        corr: i32,
    ) -> Result<i16, Box<dyn std::error::Error>> {
        let mut req = HeartbeatRequest::default();
        req.group_id = GroupId(StrBytes::from_string(group.to_string()));
        req.generation_id = generation;
        req.member_id = StrBytes::from_string(member_id.to_string());
        Ok(roundtrip(s, 3, corr, &req).await?.error_code)
    }

    pub async fn commit(
        s: &mut TcpStream,
        group: &str,
        member_id: &str,
        generation: i32,
        topic: &str,
        offset: i64,
        corr: i32,
    ) -> Result<i16, Box<dyn std::error::Error>> {
        let mut p = OffsetCommitRequestPartition::default();
        p.partition_index = 0;
        p.committed_offset = offset;
        let mut t = OffsetCommitRequestTopic::default();
        t.name = TopicName(StrBytes::from_string(topic.to_string()));
        t.partitions = vec![p];
        let mut req = OffsetCommitRequest::default();
        req.group_id = GroupId(StrBytes::from_string(group.to_string()));
        req.generation_id_or_member_epoch = generation;
        req.member_id = StrBytes::from_string(member_id.to_string());
        req.topics = vec![t];
        let r = roundtrip(s, 7, corr, &req).await?;
        Ok(r.topics[0].partitions[0].error_code)
    }
}

/// The coordinator's rebalance behaviour as seen by a client:
/// - when a new member starts a rebalance, members still on the old generation get
///   REBALANCE_IN_PROGRESS from Heartbeat (not ILLEGAL_GENERATION, which clients
///   treat as "partitions lost") and can still commit offsets for their revoked
///   partitions;
/// - a follower that rejoins a stable group with unchanged metadata keeps its
///   assignment and generation (it used to lose the assignment and force another
///   rebalance);
/// - Heartbeat for a group the coordinator does not know returns UNKNOWN_MEMBER_ID
///   so the client rejoins (it used to get COORDINATOR_NOT_AVAILABLE).
pub async fn test_group_rebalance_keeps_old_generation_usable() -> TestResult {
    use group_wire::*;
    println!("=== Test: Rebalance Keeps The Old Generation Usable ===\n");
    let ctx = TestContext::new().await?;
    let topic = crate::fixtures::TestTopicBuilder::new(&ctx, "review-group")
        .with_partitions(1)
        .build()
        .await?;
    let group = ctx.unique_group("review-group").await;
    let mut s = TcpStream::connect(BROKER).await?;

    // Stable group {a (leader), b}.
    let a = join(&mut s, &group, "", b"sub", 800).await?;
    let b = join(&mut s, &group, "", b"sub", 801).await?;
    let a = join(&mut s, &group, &a.member_id, b"sub", 802).await?;
    assert_eq!(a.leader, a.member_id);
    let gen = a.generation;
    let (code, _) = sync(
        &mut s,
        &group,
        &a.member_id,
        gen,
        &[(&a.member_id, b"assign-a"), (&b.member_id, b"assign-b")],
        803,
    )
    .await?;
    assert_eq!(code, 0, "leader sync");
    let (code, got) = sync(&mut s, &group, &b.member_id, gen, &[], 804).await?;
    assert_eq!((code, got.as_slice()), (0, b"assign-b".as_slice()));
    println!("Step 1: stable at generation {}", gen);

    // A follower rejoining with the same metadata keeps generation and assignment.
    let b2 = join(&mut s, &group, &b.member_id, b"sub", 805).await?;
    let (code, got) = sync(&mut s, &group, &b.member_id, b2.generation, &[], 806).await?;
    println!(
        "   follower rejoin: generation {} -> {}, sync code {} assignment {:?}",
        gen,
        b2.generation,
        code,
        String::from_utf8_lossy(&got)
    );
    assert_eq!(
        b2.generation, gen,
        "a plain follower rejoin must not rebalance"
    );
    assert_eq!((code, got.as_slice()), (0, b"assign-b".as_slice()));

    // A new member starts a rebalance; a and b still hold `gen`.
    let c = join(&mut s, &group, "", b"sub", 807).await?;
    assert!(c.generation > gen);
    let hb = heartbeat(&mut s, &group, &a.member_id, gen, 808).await?;
    let commit_code = commit(&mut s, &group, &b.member_id, gen, &topic.name, 5, 809).await?;
    println!(
        "   during rebalance: old-generation heartbeat={} commit={}",
        hb, commit_code
    );
    assert_eq!(hb, 27, "REBALANCE_IN_PROGRESS, so the member rejoins");
    assert_eq!(commit_code, 0, "the revoke-time commit must be accepted");
    let stored: i64 = ctx
        .db()
        .query_one(
            "SELECT committed_offset FROM kafka.consumer_offsets WHERE group_id = $1",
            &[&group],
        )
        .await?
        .get(0);
    assert_eq!(stored, 5);

    // Unknown group.
    let unknown = heartbeat(&mut s, "review-no-such-group", "m", 1, 810).await?;
    println!("   heartbeat for an unknown group: {}", unknown);
    assert_eq!(unknown, 25, "UNKNOWN_MEMBER_ID");

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
