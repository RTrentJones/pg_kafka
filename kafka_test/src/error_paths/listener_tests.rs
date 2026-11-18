//! Regression tests for the `listener` workstream (src/kafka/listener.rs and friends).
//!
//! - Idle long polls are no longer re-run every 100 ms: a waiting fetch re-runs only on a
//!   new-data notification, its `max_wait_ms` deadline, or the slow safety-net interval
//!   (`pg_kafka.fetch_poll_interval_ms`, default 1000 ms).
//! - A produce still wakes a waiting fetch at once.
//! - EndTxn wakes `read_committed` long polls, so a commit is seen promptly.
//! - Per-connection in-flight requests are bounded with read-side backpressure; a client
//!   that pipelines far beyond the bound still gets every response, in order.
//!
//! The idle test is log-free: it inserts a record directly with SQL, which produces no
//! long-poll notification. The only way the waiting fetch can return that record is by
//! re-running the fetch, so the time it arrives measures how often an idle long poll
//! re-fetches.

use crate::common::{
    create_producer, create_transactional_producer, get_bootstrap_servers, TestResult,
};
use crate::error_paths::bug_hunt::{fetch_request, roundtrip};
use crate::setup::TestContext;
use rdkafka::producer::{FutureRecord, Producer};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// The shipped default of `pg_kafka.fetch_poll_interval_ms` (the long-poll safety net).
const DEFAULT_SAFETY_NET_MS: u64 = 1000;

async fn produce_seed(topic: &str) -> Result<(), Box<dyn std::error::Error>> {
    let producer = create_producer()?;
    producer
        .send(
            FutureRecord::to(topic)
                .key("seed")
                .payload("seed")
                .partition(0),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(e, _)| e)?;
    Ok(())
}

async fn high_watermark(ctx: &TestContext, topic: &str) -> Result<i64, Box<dyn std::error::Error>> {
    Ok(ctx
        .db()
        .query_one(
            "SELECT po.next_offset FROM kafka.partition_offsets po
             JOIN kafka.topics t ON t.id = po.topic_id
             WHERE t.name = $1 AND po.partition_id = 0",
            &[&topic],
        )
        .await?
        .get(0))
}

/// Start one raw long-poll Fetch (v4) on its own connection. Resolves to the time from
/// just before the request was sent until its response arrived, and whether the response
/// carried records.
fn spawn_long_poll(
    topic: String,
    offset: i64,
    max_wait_ms: i32,
    isolation_level: i8,
) -> tokio::task::JoinHandle<Result<(Duration, bool), String>> {
    tokio::spawn(async move {
        let mut stream = TcpStream::connect(get_bootstrap_servers())
            .await
            .map_err(|e| format!("connect failed: {e}"))?;
        let mut request = fetch_request(&topic, 0, offset, max_wait_ms);
        request.isolation_level = isolation_level;
        let sent = Instant::now();
        let response = roundtrip(&mut stream, 4, 7, &request)
            .await
            .map_err(|e| format!("fetch failed: {e}"))?;
        let elapsed = sent.elapsed();
        let has_records = response.responses.iter().any(|t| {
            t.partitions
                .iter()
                .any(|p| p.records.as_ref().is_some_and(|r| !r.is_empty()))
        });
        Ok((elapsed, has_records))
    })
}

/// An idle long poll must not re-run the fetch every 100 ms.
///
/// A fetch waits at the log end with max_wait_ms = 6 s. 200 ms later a record is inserted
/// with plain SQL (no Produce, so no notification). Old behaviour: the fetch re-ran every
/// 100 ms and returned the record ~300 ms after it was sent. New behaviour: nothing re-runs
/// it until the safety-net interval (1 s by default), so the record arrives at ~1 s.
pub async fn test_listener_idle_long_poll_does_not_refetch() -> TestResult {
    println!("=== Test: Idle Long Poll Does Not Re-Fetch (listener) ===\n");

    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("listener-idle-lp").await;
    produce_seed(&topic).await?;
    let hwm = high_watermark(&ctx, &topic).await?;

    let interval_ms: u64 = ctx
        .db()
        .query_one("SHOW pg_kafka.fetch_poll_interval_ms", &[])
        .await?
        .get::<_, String>(0)
        .trim()
        .trim_end_matches("ms")
        .parse()?;
    println!("Step 1: pg_kafka.fetch_poll_interval_ms = {interval_ms}");
    assert!(
        interval_ms >= DEFAULT_SAFETY_NET_MS,
        "this test assumes the shipped safety-net default (>= {DEFAULT_SAFETY_NET_MS} ms), got {interval_ms} ms"
    );

    const MAX_WAIT_MS: i32 = 6_000;
    println!("Step 2: long poll at offset {hwm} (max_wait_ms={MAX_WAIT_MS})...");
    let fetch = spawn_long_poll(topic.clone(), hwm, MAX_WAIT_MS, 0);

    tokio::time::sleep(Duration::from_millis(200)).await;
    println!("Step 3: inserting a record with SQL (no long-poll notification)...");
    ctx.db()
        .execute(
            "WITH t AS (SELECT id FROM kafka.topics WHERE name = $1),
                  po AS (UPDATE kafka.partition_offsets
                         SET next_offset = next_offset + 1
                         WHERE topic_id = (SELECT id FROM t) AND partition_id = 0
                         RETURNING next_offset - 1 AS off)
             INSERT INTO kafka.messages (topic_id, partition_id, partition_offset, key, value)
             SELECT (SELECT id FROM t), 0, po.off, 'k'::bytea, 'injected'::bytea FROM po",
            &[&topic],
        )
        .await?;

    let (elapsed, has_records) = fetch.await??;
    println!("   response after {elapsed:?}, has_records={has_records}");

    assert!(
        has_records,
        "the injected record must be returned (by the safety net or the deadline)"
    );
    assert!(
        elapsed >= Duration::from_millis(800),
        "an idle long poll re-ran the fetch before the safety-net interval: the SQL-inserted \
         record came back after {elapsed:?}"
    );
    let bound = interval_ms.min(MAX_WAIT_MS as u64) + 1_500;
    assert!(
        elapsed <= Duration::from_millis(bound),
        "the safety net (or deadline) should deliver within {bound} ms, took {elapsed:?}"
    );

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// A produce must wake a waiting long poll at once, not at the next safety-net tick.
///
/// The fetch waits at the log end (max_wait_ms = 10 s); the produce is sent 2.2 s later,
/// i.e. 0.2 s after a safety-net tick (ticks at ~1 s, ~2 s, ~3 s). Notification delivery is
/// well under 400 ms; the safety net alone would take ~800 ms.
pub async fn test_listener_produce_wakes_long_poll_promptly() -> TestResult {
    println!("=== Test: Produce Wakes Long Poll Promptly (listener) ===\n");

    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("listener-wake-lp").await;
    produce_seed(&topic).await?;
    let hwm = high_watermark(&ctx, &topic).await?;
    let producer = create_producer()?;

    println!("Step 1: long poll at offset {hwm} (max_wait_ms=10000)...");
    let started = Instant::now();
    let fetch = spawn_long_poll(topic.clone(), hwm, 10_000, 0);

    tokio::time::sleep(Duration::from_millis(2_200)).await;
    println!("Step 2: producing the wake-up record...");
    let produce_started = started.elapsed();
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

    let (elapsed, has_records) = fetch.await??;
    let after_produce = elapsed.saturating_sub(produce_started);
    println!("   response {after_produce:?} after the produce started (total {elapsed:?})");

    assert!(
        has_records,
        "the woken fetch must return the produced record"
    );
    assert!(
        elapsed >= produce_started,
        "the fetch must have waited for the produce"
    );
    assert!(
        after_produce < Duration::from_millis(400),
        "a produce must wake the waiting fetch at once; took {after_produce:?}"
    );

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// A read_committed long poll must return promptly after the transaction commits.
///
/// A transactional record is produced (pending), then a read_committed fetch waits at its
/// offset (nothing is visible below the LSO). 2.2 s later the transaction commits. EndTxn
/// now emits a notification that wakes read_committed waiters, so the record arrives well
/// under 500 ms after the commit starts; relying on the safety net would take ~800 ms.
pub async fn test_listener_read_committed_long_poll_wakes_on_commit() -> TestResult {
    println!("=== Test: read_committed Long Poll Wakes On Commit (listener) ===\n");

    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("listener-rc-lp").await;
    produce_seed(&topic).await?;
    let hwm = high_watermark(&ctx, &topic).await?;

    println!("Step 1: producing one transactional record (left pending)...");
    let txn_id = format!("listener-rc-lp-{}", uuid::Uuid::new_v4());
    let producer = create_transactional_producer(&txn_id)?;
    producer.init_transactions(Duration::from_secs(10))?;
    producer.begin_transaction()?;
    producer
        .send(
            FutureRecord::<(), _>::to(&topic)
                .payload("txn-record")
                .partition(0),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(e, _)| e)?;

    println!("Step 2: read_committed long poll at offset {hwm} (max_wait_ms=10000)...");
    let started = Instant::now();
    let fetch = spawn_long_poll(topic.clone(), hwm, 10_000, 1);

    tokio::time::sleep(Duration::from_millis(2_200)).await;
    println!("Step 3: committing...");
    let commit_started = started.elapsed();
    producer.commit_transaction(Duration::from_secs(10))?;

    let (elapsed, has_records) = fetch.await??;
    let after_commit = elapsed.saturating_sub(commit_started);
    println!("   response {after_commit:?} after the commit started (total {elapsed:?})");

    assert!(has_records, "the committed record must be returned");
    assert!(
        elapsed >= commit_started,
        "read_committed must not return the record before the commit"
    );
    assert!(
        after_commit < Duration::from_millis(500),
        "a commit must wake a read_committed long poll at once; took {after_commit:?}"
    );

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// ApiVersions v0 request frame: [size][api_key=18][api_version=0][correlation_id][client_id=null].
fn api_versions_v0_frame(correlation_id: i32) -> Vec<u8> {
    let mut body = Vec::with_capacity(10);
    body.extend_from_slice(&18i16.to_be_bytes());
    body.extend_from_slice(&0i16.to_be_bytes());
    body.extend_from_slice(&correlation_id.to_be_bytes());
    body.extend_from_slice(&(-1i16).to_be_bytes());
    let mut frame = (body.len() as i32).to_be_bytes().to_vec();
    frame.extend_from_slice(&body);
    frame
}

/// Pipelining far beyond the per-connection in-flight bound (128) without reading must
/// neither deadlock nor drop or reorder responses: once the client reads, every response
/// arrives, in request order.
pub async fn test_listener_pipelined_requests_beyond_in_flight_cap() -> TestResult {
    println!("=== Test: Pipelining Beyond The In-Flight Cap (listener) ===\n");

    const REQUESTS: i32 = 400;
    let mut stream = TcpStream::connect(get_bootstrap_servers()).await?;
    let mut bytes = Vec::new();
    for i in 0..REQUESTS {
        bytes.extend_from_slice(&api_versions_v0_frame(i));
    }
    println!("Step 1: writing {REQUESTS} pipelined ApiVersions requests without reading...");
    stream.write_all(&bytes).await?;
    stream.flush().await?;

    // Let the broker run into the bound while nothing is read.
    tokio::time::sleep(Duration::from_millis(500)).await;

    println!("Step 2: reading all responses...");
    for expected in 0..REQUESTS {
        let read = async {
            let mut size = [0u8; 4];
            stream.read_exact(&mut size).await?;
            let mut frame = vec![0u8; i32::from_be_bytes(size) as usize];
            stream.read_exact(&mut frame).await?;
            Ok::<_, std::io::Error>(i32::from_be_bytes([frame[0], frame[1], frame[2], frame[3]]))
        };
        let correlation_id = tokio::time::timeout(Duration::from_secs(10), read)
            .await
            .map_err(|_| format!("timed out waiting for response {expected}"))??;
        assert_eq!(
            correlation_id, expected,
            "responses must arrive in request order"
        );
    }

    println!("✅ Test PASSED: {REQUESTS} responses, in order\n");
    Ok(())
}
