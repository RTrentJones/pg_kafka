//! Listener behaviour (`src/kafka/listener/`):
//!
//! - Idle long polls are not re-run on a short timer: a waiting fetch re-runs only on a
//!   new-data notification, its `max_wait_ms` deadline, or the slow safety-net interval
//!   (`pg_kafka.fetch_poll_interval_ms`, default 1000 ms).
//! - A produce still wakes a waiting fetch at once.
//! - EndTxn wakes `read_committed` long polls, so a commit is seen promptly.
//! - DeleteRecords and DeleteTopics wake the long polls they make an error of, so the
//!   consumer learns OFFSET_OUT_OF_RANGE or UNKNOWN_TOPIC_OR_PARTITION promptly.
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
use crate::error_codes;
use crate::setup::TestContext;
use crate::wire::requests::fetch_request;
use crate::wire::{raw_frame, read_raw_response, roundtrip, Wire};
use rdkafka::producer::{FutureRecord, Producer};
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

/// The shipped default of `pg_kafka.fetch_poll_interval_ms` (the long-poll safety net).
const DEFAULT_SAFETY_NET_MS: u64 = 1000;

/// When the wake-up tests produce or commit, after the fetch was sent: half-way to its first
/// safety-net re-run, which is at least `DEFAULT_SAFETY_NET_MS` after the fetch was sent.
const PRODUCE_AT: Duration = Duration::from_millis(500);
/// How soon after the produce's ack (or the commit's return) a woken fetch must answer.
const PROMPT: Duration = Duration::from_millis(300);
/// Wake-up attempts before an inconclusive test fails.
const ATTEMPTS: usize = 3;

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
/// just before the request was sent until its response arrived, whether the response
/// carried records, and the instant the response arrived.
fn spawn_long_poll(
    topic: String,
    offset: i64,
    max_wait_ms: i32,
    isolation_level: i8,
) -> tokio::task::JoinHandle<Result<(Duration, bool, Instant), String>> {
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
        let received = Instant::now();
        let elapsed = received.duration_since(sent);
        let has_records = response.responses.iter().any(|t| {
            t.partitions
                .iter()
                .any(|p| p.records.as_ref().is_some_and(|r| !r.is_empty()))
        });
        Ok((elapsed, has_records, received))
    })
}

/// An idle long poll must not re-run the fetch every 100 ms.
///
/// A fetch waits at the log end with max_wait_ms = 6 s. 200 ms later a record is inserted
/// with plain SQL (no Produce, so no notification). Old behaviour: the fetch re-ran every
/// 100 ms and returned the record ~300 ms after it was sent. New behaviour: nothing re-runs
/// it until the safety-net interval (1 s by default), so the record arrives at ~1 s.
pub async fn test_listener_idle_long_poll_does_not_refetch() -> TestResult {
    const MAX_WAIT_MS: i32 = 6_000;

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

    let (elapsed, has_records, _) = fetch.await??;
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
/// The fetch waits at the log end (max_wait_ms = 10 s); the produce is sent 0.5 s later.
/// The fetch's first safety-net re-run is at least 1 s after it was sent (the interval
/// starts when the long poll does, and again each time it elapses), so a
/// response that arrives within 1 s of sending the fetch was woken by the produce; one that
/// arrives later cannot be told from the safety net (a slow runner, or no wake-up), and the
/// attempt is repeated (3 attempts: a produce that wakes nothing fails them all). The
/// response must come within 300 ms of the produce's ack (both follow the same commit, so a
/// slow fsync delays them alike).
pub async fn test_listener_produce_wakes_long_poll_promptly() -> TestResult {
    // Metadata for the test topic too.
    use rdkafka::producer::Producer;

    println!("=== Test: Produce Wakes Long Poll Promptly (listener) ===\n");

    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("listener-wake-lp").await;
    produce_seed(&topic).await?;
    let producer = create_producer()?;
    // Warm the producer up (connection + metadata) on another topic, so the timing
    // below measures the broker's wake-up and not a fresh client's first send.
    let warmup_topic = ctx.unique_topic("listener-wake-warmup").await;
    producer
        .send(
            FutureRecord::to(&warmup_topic).key("w").payload("w"),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(e, _)| e)?;
    producer
        .client()
        .fetch_metadata(Some(&topic), Duration::from_secs(5))?;

    // The fetch is woken by the produce's commit, so its response is measured from the
    // produce's ack (sent after that same commit), which leaves the produce's own commit
    // and fsync time out. A response later than the fetch's first possible safety-net
    // re-run cannot be told from it: the attempt is inconclusive, and the test tries again.
    let first_safety_net = Duration::from_millis(DEFAULT_SAFETY_NET_MS);
    for attempt in 1..=ATTEMPTS {
        let hwm = high_watermark(&ctx, &topic).await?;
        println!("Step 1 (attempt {attempt}/{ATTEMPTS}): long poll at offset {hwm} (max_wait_ms=10000)...");
        let fetch = spawn_long_poll(topic.clone(), hwm, 10_000, 0);

        tokio::time::sleep(PRODUCE_AT).await;
        println!("Step 2: producing the wake-up record...");
        let produce_started = Instant::now();
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
        let acked = Instant::now();

        let (elapsed, has_records, received) = fetch.await??;
        let after_ack = received.saturating_duration_since(acked);
        println!(
            "   produce acked {:?} after it started; response {after_ack:?} after the ack (fetch waited {elapsed:?})",
            acked.duration_since(produce_started)
        );

        assert!(
            has_records,
            "the woken fetch must return the produced record"
        );
        assert!(
            received >= produce_started,
            "the fetch must have waited for the produce"
        );
        if elapsed >= first_safety_net {
            println!(
                "   answered {elapsed:?} after the fetch was sent, not before its first possible \
                 safety-net re-run; retrying"
            );
            continue;
        }
        assert!(
            after_ack < PROMPT,
            "a produce must wake the waiting fetch at once; the response came {after_ack:?} after the ack"
        );
        ctx.cleanup().await?;
        println!("✅ Test PASSED\n");
        return Ok(());
    }
    Err(format!(
        "no attempt's fetch was answered within {first_safety_net:?} of being sent, before its \
         first possible safety-net re-run: the produce woke no waiting fetch (or every \
         attempt was stalled that long)"
    )
    .into())
}

/// A read_committed long poll must return promptly after the transaction commits.
///
/// Each attempt produces one transactional record (left pending), then a read_committed
/// fetch waits at its offset (max_wait_ms = 10 s; nothing is visible below the LSO), and
/// the transaction commits 0.5 s after the fetch was sent. EndTxn wakes read_committed
/// waiters, so the record must arrive within 300 ms of the commit's return. As in
/// `test_listener_produce_wakes_long_poll_promptly`, a response that arrives 1 s or more
/// after the fetch was sent cannot be told from the fetch's first safety-net re-run (the
/// interval starts when the long poll does, so it is never earlier), and the attempt
/// is repeated (3 attempts: a commit that wakes nothing fails them all).
pub async fn test_listener_read_committed_long_poll_wakes_on_commit() -> TestResult {
    println!("=== Test: read_committed Long Poll Wakes On Commit (listener) ===\n");

    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("listener-rc-lp").await;
    produce_seed(&topic).await?;
    let txn_id = format!("listener-rc-lp-{}", uuid::Uuid::new_v4());
    let producer = create_transactional_producer(&txn_id)?;
    producer.init_transactions(Duration::from_secs(10))?;

    let first_safety_net = Duration::from_millis(DEFAULT_SAFETY_NET_MS);
    for attempt in 1..=ATTEMPTS {
        let hwm = high_watermark(&ctx, &topic).await?;
        println!("Step 1 (attempt {attempt}/{ATTEMPTS}): one transactional record at offset {hwm}, left pending...");
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
        let fetch = spawn_long_poll(topic.clone(), hwm, 10_000, 1);

        tokio::time::sleep(PRODUCE_AT).await;
        println!("Step 3: committing...");
        let commit_started = Instant::now();
        producer.commit_transaction(Duration::from_secs(10))?;
        let committed = Instant::now();

        let (elapsed, has_records, received) = fetch.await??;
        let after_commit = received.saturating_duration_since(committed);
        println!(
            "   commit took {:?}; response {after_commit:?} after it returned (fetch waited {elapsed:?})",
            committed.duration_since(commit_started)
        );

        assert!(has_records, "the committed record must be returned");
        assert!(
            received >= commit_started,
            "read_committed must not return the record before the commit"
        );
        if elapsed >= first_safety_net {
            println!(
                "   answered {elapsed:?} after the fetch was sent, not before its first possible \
                 safety-net re-run; retrying"
            );
            continue;
        }
        assert!(
            after_commit < PROMPT,
            "a commit must wake a read_committed long poll at once; the response came \
             {after_commit:?} after the commit returned"
        );
        ctx.cleanup().await?;
        println!("✅ Test PASSED\n");
        return Ok(());
    }
    Err(format!(
        "no attempt's fetch was answered within {first_safety_net:?} of being sent, before its \
         first possible safety-net re-run: the commit woke no waiting read_committed fetch (or \
         every attempt was stalled that long)"
    )
    .into())
}

/// Start one raw long-poll Fetch (v4) of partition 0 of `topic` from `offset`, waiting for
/// `min_bytes`, on its own connection. Resolves to the time from just before the request was
/// sent until its response arrived, the partition's error code, and the instant the response
/// arrived.
fn spawn_waiting_fetch(
    topic: String,
    offset: i64,
    min_bytes: i32,
) -> tokio::task::JoinHandle<Result<(Duration, i16, Instant), String>> {
    tokio::spawn(async move {
        let mut stream = TcpStream::connect(get_bootstrap_servers())
            .await
            .map_err(|e| format!("connect failed: {e}"))?;
        let mut request = fetch_request(&topic, 0, offset, 10_000);
        request.min_bytes = min_bytes;
        let sent = Instant::now();
        let response = roundtrip(&mut stream, 4, 7, &request)
            .await
            .map_err(|e| format!("fetch failed: {e}"))?;
        let received = Instant::now();
        let code = response
            .responses
            .first()
            .and_then(|t| t.partitions.first())
            .map_or(-1, |p| p.error_code);
        Ok((received.duration_since(sent), code, received))
    })
}

/// Whether a waiting fetch answered `expected` within `PROMPT` of `changed` (the change's
/// return): `None` when it answered at or after its first possible safety-net re-run, which
/// cannot be told from a wake-up (the attempt is inconclusive).
fn woken_promptly(
    answer: (Duration, i16, Instant),
    changed: Instant,
    expected: i16,
) -> Option<Duration> {
    let (elapsed, code, received) = answer;
    let after = received.saturating_duration_since(changed);
    println!("   answered {code} {after:?} after the change (fetch waited {elapsed:?})");
    assert_eq!(code, expected, "the woken fetch's error code");
    if elapsed >= Duration::from_millis(DEFAULT_SAFETY_NET_MS) {
        println!("   not before its first possible safety-net re-run; retrying");
        return None;
    }
    Some(after)
}

/// DeleteRecords past a waiting fetch's offset must answer it OFFSET_OUT_OF_RANGE at once.
///
/// Each attempt produces three records and starts a fetch from the first of them waiting for
/// 1 MiB (max_wait_ms = 10 s), so it waits although they are there; 0.5 s later DeleteRecords
/// moves the log start past them. The fetch must be answered within 300 ms of the
/// DeleteRecords' return; one answered 1 s or more after it was sent cannot be told from its
/// safety-net re-run, and the attempt is repeated.
pub async fn test_listener_delete_records_wakes_long_poll() -> TestResult {
    println!("=== Test: DeleteRecords Wakes A Long Poll (listener) ===\n");

    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("listener-dr-lp").await;
    produce_seed(&topic).await?;
    let producer = create_producer()?;
    let mut wire = Wire::connect().await?;
    for attempt in 1..=ATTEMPTS {
        let start = high_watermark(&ctx, &topic).await?;
        for i in 0..3 {
            producer
                .send(
                    FutureRecord::to(&topic)
                        .key(&format!("k{i}"))
                        .payload("v")
                        .partition(0),
                    Duration::from_secs(5),
                )
                .await
                .map_err(|(e, _)| e)?;
        }
        println!("Step 1 (attempt {attempt}/{ATTEMPTS}): fetch from {start}, waiting for 1 MiB...");
        let fetch = spawn_waiting_fetch(topic.clone(), start, 1 << 20);
        tokio::time::sleep(PRODUCE_AT).await;
        println!("Step 2: DeleteRecords before offset {}...", start + 3);
        let (code, low_watermark) = wire.delete_records(&topic, 0, start + 3).await?;
        let deleted = Instant::now();
        assert_eq!(code, error_codes::NONE, "DeleteRecords");
        assert_eq!(low_watermark, start + 3);
        if let Some(after) =
            woken_promptly(fetch.await??, deleted, error_codes::OFFSET_OUT_OF_RANGE)
        {
            assert!(
                after < PROMPT,
                "DeleteRecords must wake the waiting fetch at once; it answered {after:?} after"
            );
            ctx.cleanup().await?;
            println!("✅ Test PASSED\n");
            return Ok(());
        }
    }
    Err(
        "no attempt's fetch was answered before its first possible safety-net re-run: \
         DeleteRecords woke no waiting fetch (or every attempt was stalled that long)"
            .into(),
    )
}

/// DeleteTopics must answer a fetch waiting on the topic UNKNOWN_TOPIC_OR_PARTITION at once.
///
/// Each attempt starts a fetch at the log end of a new topic (max_wait_ms = 10 s) and
/// deletes the topic 0.5 s later; the fetch must be answered within 300 ms of the
/// DeleteTopics' return (retried as in `test_listener_delete_records_wakes_long_poll`).
pub async fn test_listener_delete_topics_wakes_long_poll() -> TestResult {
    use kafka_protocol::messages::delete_topics_request::DeleteTopicsRequest;
    use kafka_protocol::messages::TopicName;
    use kafka_protocol::protocol::StrBytes;

    println!("=== Test: DeleteTopics Wakes A Long Poll (listener) ===\n");

    let ctx = TestContext::new().await?;
    let mut wire = Wire::connect().await?;
    for attempt in 1..=ATTEMPTS {
        let topic = ctx.unique_topic("listener-dt-lp").await;
        produce_seed(&topic).await?;
        let hwm = high_watermark(&ctx, &topic).await?;
        println!(
            "Step 1 (attempt {attempt}/{ATTEMPTS}): fetch at the log end ({hwm}) of {topic}..."
        );
        let fetch = spawn_waiting_fetch(topic.clone(), hwm, 1);
        tokio::time::sleep(PRODUCE_AT).await;
        println!("Step 2: DeleteTopics...");
        let mut request = DeleteTopicsRequest::default();
        request.topic_names = vec![TopicName(StrBytes::from_string(topic.clone()))];
        request.timeout_ms = 10_000;
        let response = wire.call(1, &request).await?;
        let deleted = Instant::now();
        assert_eq!(
            response.responses.first().map(|t| t.error_code),
            Some(error_codes::NONE),
            "DeleteTopics"
        );
        if let Some(after) = woken_promptly(
            fetch.await??,
            deleted,
            error_codes::UNKNOWN_TOPIC_OR_PARTITION,
        ) {
            assert!(
                after < PROMPT,
                "DeleteTopics must wake the waiting fetch at once; it answered {after:?} after"
            );
            ctx.cleanup().await?;
            println!("✅ Test PASSED\n");
            return Ok(());
        }
    }
    Err(
        "no attempt's fetch was answered before its first possible safety-net re-run: \
         DeleteTopics woke no waiting fetch (or every attempt was stalled that long)"
            .into(),
    )
}

/// Pipelining far beyond the per-connection in-flight bound (128) without reading must
/// neither deadlock nor drop or reorder responses: once the client reads, every response
/// arrives, in request order.
pub async fn test_listener_pipelined_requests_beyond_in_flight_cap() -> TestResult {
    const REQUESTS: i32 = 400;

    println!("=== Test: Pipelining Beyond The In-Flight Cap (listener) ===\n");

    let mut stream = TcpStream::connect(get_bootstrap_servers()).await?;
    let mut bytes = Vec::new();
    for i in 0..REQUESTS {
        bytes.extend_from_slice(&raw_frame(18, 0, i, &[]));
    }
    println!("Step 1: writing {REQUESTS} pipelined ApiVersions requests without reading...");
    stream.write_all(&bytes).await?;
    stream.flush().await?;

    // Let the broker run into the bound while nothing is read.
    tokio::time::sleep(Duration::from_millis(500)).await;

    println!("Step 2: reading all responses...");
    for expected in 0..REQUESTS {
        let (correlation_id, _) =
            tokio::time::timeout(Duration::from_secs(10), read_raw_response(&mut stream))
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
