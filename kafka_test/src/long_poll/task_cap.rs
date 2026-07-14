//! RV-10 follow-up E2E: per-connection long-poll task cap.
//!
//! Each long-poll fetch spawns a detached task on the network thread. The cap
//! (`MAX_LONG_POLLS_PER_CONNECTION` = 64 in listener.rs) bounds how many one
//! connection can hold; a fetch over the cap degrades to the immediate
//! (non-waiting) fetch path and returns a valid — possibly empty — response.
//!
//! Detection is by response CONTENT, not timing (responses are delivered in
//! request order, so per-response timing is uninformative): we pipeline 80
//! long-poll fetches at an offset past the end of the log, produce one record
//! mid-wait, and decode every response.
//!   - Long-polled fetches (the first 64) are still waiting when the record
//!     arrives, wake, and return it.
//!   - Over-cap fetches (the last 16) were served immediately at send time —
//!     before the record existed — and come back empty.
//! Pre-cap builds long-poll all 80, so all 80 would contain the record; the
//! capped build yields exactly 64 with records / 16 empty.

use crate::common::{create_producer, get_bootstrap_servers, TestResult};
use crate::setup::TestContext;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchRequest, FetchTopic};
use kafka_protocol::messages::fetch_response::FetchResponse;
use kafka_protocol::messages::{RequestHeader, TopicName};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use rdkafka::producer::FutureRecord;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Must exceed listener.rs MAX_LONG_POLLS_PER_CONNECTION (64).
const PIPELINED_FETCHES: usize = 80;
const EXPECTED_LONG_POLLED: usize = 64;
const FETCH_MAX_WAIT_MS: i32 = 8_000;

/// Encode a Fetch v4 request ([size][header v1][body v4]) for one partition.
fn encode_fetch_v4(correlation_id: i32, topic: &str, fetch_offset: i64) -> Bytes {
    let partition = FetchPartition::default()
        .with_partition(0)
        .with_fetch_offset(fetch_offset)
        .with_partition_max_bytes(1024 * 1024);
    let fetch_topic = FetchTopic::default()
        .with_topic(TopicName(StrBytes::from_string(topic.to_string())))
        .with_partitions(vec![partition]);
    let request = FetchRequest::default()
        .with_replica_id(kafka_protocol::messages::BrokerId(-1))
        .with_max_wait_ms(FETCH_MAX_WAIT_MS)
        .with_min_bytes(1)
        .with_max_bytes(4 * 1024 * 1024)
        .with_isolation_level(0)
        .with_topics(vec![fetch_topic]);

    let header = RequestHeader::default()
        .with_request_api_key(1) // Fetch
        .with_request_api_version(4)
        .with_correlation_id(correlation_id)
        .with_client_id(Some(StrBytes::from_static_str("task-cap-test")));

    let mut body = BytesMut::new();
    header.encode(&mut body, 1).unwrap(); // header v1 for fetch v4
    request.encode(&mut body, 4).unwrap();

    let mut framed = BytesMut::with_capacity(body.len() + 4);
    framed.put_i32(body.len() as i32);
    framed.put(body);
    framed.freeze()
}

/// Read one [size][correlation_id][FetchResponse v4] frame; return
/// (correlation_id, has_records).
async fn read_fetch_response(stream: &mut TcpStream) -> Result<(i32, bool), String> {
    let mut size_buf = [0u8; 4];
    stream
        .read_exact(&mut size_buf)
        .await
        .map_err(|e| format!("read size failed: {e}"))?;
    let size = i32::from_be_bytes(size_buf) as usize;
    let mut frame = vec![0u8; size];
    stream
        .read_exact(&mut frame)
        .await
        .map_err(|e| format!("read frame failed: {e}"))?;

    let mut buf = Bytes::from(frame);
    let correlation_id = buf.get_i32(); // response header v0
    let response = FetchResponse::decode(&mut buf, 4)
        .map_err(|e| format!("decode fetch response failed: {e}"))?;
    let has_records = response.responses.iter().any(|t| {
        t.partitions
            .iter()
            .any(|p| p.records.as_ref().is_some_and(|r| !r.is_empty()))
    });
    Ok((correlation_id, has_records))
}

/// RV-10 follow-up: fetches beyond the per-connection long-poll cap must be
/// served immediately (valid empty response), while capped fetches still
/// long-poll and deliver.
pub async fn test_long_poll_per_connection_task_cap() -> TestResult {
    println!("=== Test: Long-Poll Per-Connection Task Cap (RV-10 follow-up) ===\n");

    // 1. SETUP: topic with one seed record; fetches at offset 1 see an empty
    // log tail and long-poll.
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("lp-task-cap").await;
    let producer = create_producer()?;
    producer
        .send(
            FutureRecord::to(&topic)
                .key("seed")
                .payload("seed")
                .partition(0),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(e, _)| e)?;
    println!("Step 1: Seed record produced (topic auto-created)\n");

    // 2. ACTION: pipeline 80 long-poll fetches on ONE raw connection.
    println!(
        "Step 2: Pipelining {} long-poll fetches (cap is {})...",
        PIPELINED_FETCHES, EXPECTED_LONG_POLLED
    );
    let mut stream = TcpStream::connect(get_bootstrap_servers())
        .await
        .map_err(|e| format!("connect failed: {e}"))?;
    for i in 0..PIPELINED_FETCHES {
        let frame = encode_fetch_v4(i as i32, &topic, 1);
        stream
            .write_all(&frame)
            .await
            .map_err(|e| format!("send fetch {i} failed: {e}"))?;
    }
    stream
        .flush()
        .await
        .map_err(|e| format!("flush failed: {e}"))?;
    println!("✅ All fetches sent\n");

    // Give the listener a moment to admit all 80 (long-poll or immediate),
    // then produce the wake-up record the long-pollers will return.
    tokio::time::sleep(Duration::from_secs(2)).await;
    println!("Step 3: Producing wake-up record mid-wait...");
    producer
        .send(
            FutureRecord::to(&topic)
                .key("wake")
                .payload("wake")
                .partition(0),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(e, _)| e)?;
    println!("✅ Wake-up record produced\n");

    // 3. VERIFY: read all 80 responses; count which carry the record.
    println!("Step 4: Reading {} responses...", PIPELINED_FETCHES);
    let mut with_records = 0usize;
    let mut empty = 0usize;
    for i in 0..PIPELINED_FETCHES {
        let read = tokio::time::timeout(
            Duration::from_millis(FETCH_MAX_WAIT_MS as u64 + 10_000),
            read_fetch_response(&mut stream),
        )
        .await
        .map_err(|_| format!("timed out waiting for response {i}"))??;
        let (corr, has_records) = read;
        if corr != i as i32 {
            return Err(format!("response order violated: expected corr {i}, got {corr}").into());
        }
        if has_records {
            with_records += 1;
        } else {
            empty += 1;
        }
    }
    println!("   responses with the record: {with_records}, empty: {empty}\n");

    // The capped 64 long-poll and deliver the record; the 16 over the cap were
    // answered immediately, before the record existed. A pre-cap build
    // long-polls all 80 (with_records == 80).
    assert_eq!(
        with_records, EXPECTED_LONG_POLLED,
        "expected exactly the capped number of fetches to long-poll and deliver the record"
    );
    assert_eq!(
        empty,
        PIPELINED_FETCHES - EXPECTED_LONG_POLLED,
        "expected the over-cap fetches to be served immediately (empty)"
    );

    // 4. CLEANUP
    ctx.cleanup().await?;
    println!("✅ Test PASSED: Long-Poll Per-Connection Task Cap\n");
    Ok(())
}
