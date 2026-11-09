//! Regression tests for the bug hunt at commit 6c982be.
//!
//! These cover the fixes whose code lives in the SPI/async files (`worker.rs`,
//! `listener.rs`) that unit coverage cannot see, plus the broker-visible behaviour of
//! the handler fixes. They speak the wire protocol directly (via the kafka-protocol
//! crate) so each request is exactly the one the bug report describes.

use crate::common::TestResult;
use crate::fixtures::{generate_messages, TestTopicBuilder};
use crate::setup::TestContext;
use bytes::{Buf, BufMut, BytesMut};
use kafka_protocol::messages::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchRequest, FetchTopic};
use kafka_protocol::messages::txn_offset_commit_request::{
    TxnOffsetCommitRequest, TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic,
};
use kafka_protocol::messages::{
    GroupId, ProducerId, RequestHeader, ResponseHeader, TopicName, TransactionalId,
};
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, Request, StrBytes};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const BROKER: &str = "localhost:9092";

/// Send one request at `version` and decode its response.
async fn roundtrip<R: Request>(
    stream: &mut TcpStream,
    version: i16,
    correlation_id: i32,
    request: &R,
) -> Result<R::Response, Box<dyn std::error::Error>> {
    let mut header = RequestHeader::default();
    header.request_api_key = R::KEY;
    header.request_api_version = version;
    header.correlation_id = correlation_id;
    header.client_id = Some(StrBytes::from_static_str("bug-hunt-e2e"));

    let mut body = BytesMut::new();
    header.encode(&mut body, R::header_version(version))?;
    request.encode(&mut body, version)?;

    let mut frame = BytesMut::with_capacity(4 + body.len());
    frame.put_i32(body.len() as i32);
    frame.extend_from_slice(&body);
    stream.write_all(&frame).await?;
    stream.flush().await?;

    let mut size = [0u8; 4];
    stream.read_exact(&mut size).await?;
    let mut buf = vec![0u8; i32::from_be_bytes(size) as usize];
    stream.read_exact(&mut buf).await?;
    let mut buf = BytesMut::from(&buf[..]);

    let response_header = ResponseHeader::decode(
        &mut buf,
        <R::Response as HeaderVersion>::header_version(version),
    )?;
    if response_header.correlation_id != correlation_id {
        return Err(format!(
            "correlation id mismatch: sent {}, got {}",
            correlation_id, response_header.correlation_id
        )
        .into());
    }
    Ok(R::Response::decode(&mut buf, version)?)
}

fn fetch_request(topic: &str, partition: i32, offset: i64, max_wait_ms: i32) -> FetchRequest {
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

/// Bug 6 (+ the long-poll path in listener.rs): fetching a partition that does not
/// exist, or an offset past the high watermark, returns the matching error code, and
/// does so straight away instead of after max_wait_ms.
pub async fn test_fetch_bad_partition_or_offset_returns_error() -> TestResult {
    println!("=== Test: Fetch Bad Partition / Offset Returns Error ===\n");

    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "bughunt-fetch")
        .with_partitions(1)
        .build()
        .await?;
    topic.produce(&generate_messages(3, "m")).await?;

    let mut stream = TcpStream::connect(BROKER).await?;
    stream.set_nodelay(true)?;

    // (partition, offset, expected error code)
    let cases = [(99, 0, 3i16), (0, 1000, 1i16), (0, 0, 0i16)];
    for (i, (partition, offset, expected)) in cases.into_iter().enumerate() {
        // max_wait_ms = 5000 takes the long-poll path for any response with no data.
        let started = Instant::now();
        let response = roundtrip(
            &mut stream,
            4,
            100 + i as i32,
            &fetch_request(&topic.name, partition, offset, 5000),
        )
        .await?;
        let elapsed = started.elapsed();
        let p = &response.responses[0].partitions[0];
        println!(
            "   partition={} offset={} -> error_code={} in {:?}",
            partition, offset, p.error_code, elapsed
        );
        if p.error_code != expected {
            return Err(format!(
                "partition {} offset {}: expected error {}, got {}",
                partition, offset, expected, p.error_code
            )
            .into());
        }
        if expected != 0 && elapsed >= Duration::from_secs(3) {
            return Err(format!(
                "error for partition {} offset {} took {:?}; it should not wait out max_wait_ms",
                partition, offset, elapsed
            )
            .into());
        }
    }

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// Fetch lower bound (PR #98 review): aborted-message cleanup deletes rows without
/// retiring their offsets, so after it removes a partition's leading rows a fetch from
/// offset 0 must still succeed and return the next committed record. Validating against
/// the first surviving row instead reported OFFSET_OUT_OF_RANGE, and a consumer with
/// auto.offset.reset=latest would skip the committed record.
pub async fn test_fetch_into_cleaned_up_aborted_rows_is_in_range() -> TestResult {
    println!("=== Test: Fetch Into Cleaned-Up Aborted Rows Is In Range ===\n");

    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "bughunt-aborted-hole")
        .with_partitions(1)
        .build()
        .await?;
    topic.produce(&generate_messages(2, "m")).await?;

    // Turn offset 0 into a cleaned-up aborted row: mark it aborted, then run the
    // sweep with no grace period so cleanup_aborted_messages deletes it.
    ctx.db()
        .execute(
            "UPDATE kafka.messages m SET txn_state = 'aborted'
             FROM kafka.topics t
             WHERE m.topic_id = t.id AND t.name = $1 AND m.partition_offset = 0",
            &[&topic.name],
        )
        .await?;
    ctx.db()
        .query("SELECT * FROM pg_kafka_run_retention_sweep(0, 0)", &[])
        .await?;
    let surviving: i64 = ctx
        .db()
        .query_one(
            "SELECT MIN(m.partition_offset) FROM kafka.messages m
             JOIN kafka.topics t ON m.topic_id = t.id WHERE t.name = $1",
            &[&topic.name],
        )
        .await?
        .get(0);
    assert_eq!(surviving, 1, "offset 0 should have been cleaned up");

    let mut stream = TcpStream::connect(BROKER).await?;
    stream.set_nodelay(true)?;
    let response = roundtrip(&mut stream, 4, 150, &fetch_request(&topic.name, 0, 0, 0)).await?;
    let p = &response.responses[0].partitions[0];
    let bytes = p.records.as_ref().map(|r| r.len()).unwrap_or(0);
    println!(
        "   fetch from 0 -> error_code={} hwm={} record bytes={}",
        p.error_code, p.high_watermark, bytes
    );
    if p.error_code != 0 {
        return Err(format!(
            "fetch from a cleaned-up aborted offset returned error {}",
            p.error_code
        )
        .into());
    }
    assert!(
        bytes > 0,
        "the committed record at offset 1 should be returned"
    );

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// Bug 4 (worker.rs): a TxnOffsetCommit whose handler fails (here: a transactional id
/// that was never initialised) must report the error on every requested partition. It
/// used to come back as an empty response, which the client reads as success.
pub async fn test_txn_offset_commit_failure_reports_partition_errors() -> TestResult {
    println!("=== Test: Failed TxnOffsetCommit Reports Partition Errors ===\n");

    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "bughunt-txn-offset")
        .with_partitions(2)
        .build()
        .await?;
    let group = ctx.unique_group("bughunt-txn-offset").await;

    let mut t = TxnOffsetCommitRequestTopic::default();
    t.name = TopicName(StrBytes::from_string(topic.name.clone()));
    t.partitions = (0..2)
        .map(|partition| {
            let mut p = TxnOffsetCommitRequestPartition::default();
            p.partition_index = partition;
            p.committed_offset = 1;
            p
        })
        .collect();
    let mut req = TxnOffsetCommitRequest::default();
    req.transactional_id = TransactionalId(StrBytes::from_string(format!(
        "bughunt-never-initialised-{}",
        uuid::Uuid::new_v4()
    )));
    req.group_id = GroupId(StrBytes::from_string(group.clone()));
    req.producer_id = ProducerId(999_999_999);
    req.producer_epoch = 0;
    req.topics = vec![t];

    let mut stream = TcpStream::connect(BROKER).await?;
    stream.set_nodelay(true)?;
    let response = roundtrip(&mut stream, 0, 200, &req).await?;

    let codes: Vec<(i32, i16)> = response
        .topics
        .iter()
        .flat_map(|t| t.partitions.iter())
        .map(|p| (p.partition_index, p.error_code))
        .collect();
    println!("   per-partition results: {:?}", codes);
    if codes.len() != 2 || codes.iter().any(|(_, code)| *code == 0) {
        return Err(format!(
            "expected an error on both requested partitions, got {:?}",
            codes
        )
        .into());
    }

    // Nothing may have been stored as a pending offset.
    let row = ctx
        .db()
        .query_one(
            "SELECT COUNT(*) FROM kafka.txn_pending_offsets WHERE group_id = $1",
            &[&group],
        )
        .await?;
    let pending: i64 = row.get(0);
    assert_eq!(pending, 0, "a failed TxnOffsetCommit must store nothing");

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// Bug 2: CreateTopics with num_partitions = -1 uses pg_kafka.default_partitions, and 0
/// is rejected with INVALID_PARTITIONS (37) instead of silently creating 1 partition.
pub async fn test_create_topics_default_and_zero_partitions() -> TestResult {
    println!("=== Test: CreateTopics -1 / 0 Partitions ===\n");

    let ctx = TestContext::new().await?;
    let defaulted = ctx.unique_topic("bughunt-default-partitions").await;
    let zero = ctx.unique_topic("bughunt-zero-partitions").await;

    let default_partitions: i32 = ctx
        .db()
        .query_one(
            "SELECT current_setting('pg_kafka.default_partitions')::int",
            &[],
        )
        .await?
        .get(0);

    let topic = |name: &str, n: i32| {
        let mut t = CreatableTopic::default();
        t.name = TopicName(StrBytes::from_string(name.to_string()));
        t.num_partitions = n;
        t.replication_factor = -1;
        t
    };
    let mut req = CreateTopicsRequest::default();
    req.topics = vec![topic(&defaulted, -1), topic(&zero, 0)];
    req.timeout_ms = 5000;

    let mut stream = TcpStream::connect(BROKER).await?;
    stream.set_nodelay(true)?;
    // v5: CreatableTopicResult.num_partitions only exists from v5 on.
    let response = roundtrip(&mut stream, 5, 300, &req).await?;
    let result = |name: &str| {
        response
            .topics
            .iter()
            .find(|t| t.name.0.as_str() == name)
            .map(|t| (t.error_code, t.num_partitions))
    };
    println!(
        "   -1 -> {:?} (default_partitions={}), 0 -> {:?}",
        result(&defaulted),
        default_partitions,
        result(&zero)
    );
    assert_eq!(result(&defaulted), Some((0, default_partitions)));
    assert_eq!(result(&zero).map(|r| r.0), Some(37));

    let stored: i32 = ctx
        .db()
        .query_one(
            "SELECT partitions FROM kafka.topics WHERE name = $1",
            &[&defaulted],
        )
        .await?
        .get(0);
    assert_eq!(stored, default_partitions);
    let zero_rows = ctx
        .db()
        .query("SELECT 1 FROM kafka.topics WHERE name = $1", &[&zero])
        .await?;
    assert!(
        zero_rows.is_empty(),
        "a 0-partition topic must not be created"
    );

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// Bug 3 (listener.rs): a client that sends a length prefix and then disconnects must
/// not keep its frame charge against the global 512 MiB reservation budget. Before the
/// fix, six abandoned 100 MB prefixes exhausted the budget and every later large frame
/// was refused until a restart.
///
/// Not parallel-safe: it deliberately reserves most of the shared budget.
pub async fn test_frame_budget_released_on_mid_frame_disconnect() -> TestResult {
    println!("=== Test: Frame Budget Released on Mid-Frame Disconnect ===\n");

    const DECLARED: i32 = 100_000_000; // == MAX_REQUEST_SIZE, the largest charged frame

    // Abandon more frames than the budget can hold (8 x 100 MB > 512 MiB).
    for i in 0..8 {
        let mut stream = TcpStream::connect(BROKER).await?;
        stream.write_all(&DECLARED.to_be_bytes()).await?;
        stream.flush().await?;
        // Give the listener time to read the prefix and charge it.
        tokio::time::sleep(Duration::from_millis(100)).await;
        drop(stream);
        println!("   abandoned 100 MB frame #{}", i + 1);
    }
    // Let the listener notice the disconnects and drop the connection tasks.
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // A fresh large frame must still be admitted. When the budget is exhausted the
    // listener errors the decode and closes the connection at once (read returns EOF);
    // when it is admitted the connection stays open waiting for the payload.
    let mut stream = TcpStream::connect(BROKER).await?;
    stream.write_all(&DECLARED.to_be_bytes()).await?;
    stream.flush().await?;
    let mut probe = [0u8; 1];
    match tokio::time::timeout(Duration::from_millis(1500), stream.read(&mut probe)).await {
        Err(_) => println!("   large frame admitted (connection held open)"),
        Ok(Ok(0)) | Ok(Err(_)) => {
            return Err(
                "a 100 MB frame was refused: abandoned frames leaked the reservation budget".into(),
            )
        }
        Ok(Ok(_)) => return Err("unexpected data from the broker".into()),
    }
    drop(stream);

    // The broker still serves ordinary requests.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let mut stream = TcpStream::connect(BROKER).await?;
    let mut req = BytesMut::new();
    req.put_i32(10);
    req.put_i16(18); // ApiVersions
    req.put_i16(0);
    req.put_i32(4242);
    req.put_i16(-1); // null client id
    stream.write_all(&req).await?;
    let mut size = [0u8; 4];
    tokio::time::timeout(Duration::from_secs(5), stream.read_exact(&mut size)).await??;
    let mut body = vec![0u8; i32::from_be_bytes(size) as usize];
    stream.read_exact(&mut body).await?;
    let mut body = &body[..];
    assert_eq!(body.get_i32(), 4242, "ApiVersions correlation id");

    println!("✅ Test PASSED\n");
    Ok(())
}
