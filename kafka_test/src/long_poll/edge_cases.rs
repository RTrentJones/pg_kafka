//! Long Polling Edge Case Tests
//!
//! Tests for edge cases in long polling functionality:
//! - min_bytes threshold behavior
//! - Timeout precision
//! - Connection drop handling
//! - Multiple waiters on same partition
//! - Auto-commit during long poll

use crate::common::{create_producer, get_bootstrap_servers, TestResult};
use crate::fixtures::TestTopicBuilder;
use crate::members::{Member, CLOSE_TIMEOUT};
use crate::setup::TestContext;
use crate::wire::records::{fetched_values, plain_batch};
use crate::wire::requests::fetch_request;
use crate::wire::{decode_response, read_frame_within, request_frame, roundtrip, Wire};
use bytes::Bytes;
use kafka_protocol::messages::FetchRequest;
use kafka_protocol::records::Compression;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::FutureRecord;
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

/// A long poll below its `min_bytes` keeps waiting as records arrive, and is answered once
/// they reach it, well before `max_wait_ms`. Measured at the wire level: an rdkafka consumer
/// receiving its records says nothing about how many bytes the response that carried them
/// waited for.
///
/// Asserts: a Fetch (v4) at the log end with min_bytes 500 and max_wait_ms 10 000 is not
/// answered within 1.5 s (past a safety-net re-check) of the first and of the second
/// record of 150 bytes (219 and 377 bytes in the response), and is answered within 2 s of
/// the third (535 bytes) with offsets 0, 1 and 2.
pub async fn test_long_poll_min_bytes_threshold() -> TestResult {
    println!("=== Test: Long Poll min_bytes Threshold ===\n");
    let ctx = TestContext::new().await?;
    let topic = super::timeout::empty_topic(&ctx, "longpoll-minbytes").await?;
    let mut fetcher = TcpStream::connect(get_bootstrap_servers()).await?;
    let mut req = fetch_request(&topic, 0, 0, 10_000);
    req.min_bytes = MIN_BYTES;
    fetcher.write_all(&request_frame(4, 1, &req)).await?;
    println!("Step 1: a Fetch parked at offset 0 with min_bytes {MIN_BYTES}, max_wait_ms 10000");

    let mut producer = Wire::connect().await?;
    let value = "v".repeat(150);
    for i in 0..3 {
        let (code, offset) = producer
            .produce(
                &topic,
                0,
                plain_batch(&[Bytes::from(value.clone())], Compression::None),
                None,
            )
            .await?;
        assert_eq!((code, offset), (0, i), "record {i} stored");
        if i < 2 {
            let early = read_frame_within(&mut fetcher, Duration::from_millis(1500)).await;
            assert!(
                early.is_err(),
                "answered with {} record(s) produced, below min_bytes {MIN_BYTES}",
                i + 1
            );
            println!("  record {i} produced: still waiting 1.5 s later");
        }
    }
    let produced = Instant::now();
    let frame = read_frame_within(&mut fetcher, Duration::from_secs(2))
        .await?
        .ok_or("closed on fetch")?;
    let took = produced.elapsed();
    let (_, resp) = decode_response::<FetchRequest>(4, &frame)?;
    let p = &resp.responses[0].partitions[0];
    assert_eq!(p.error_code, 0);
    let offsets: Vec<i64> = fetched_values(&p.records.clone().unwrap_or_default())?
        .into_iter()
        .map(|(o, _)| o)
        .collect();
    assert_eq!(
        offsets,
        vec![0, 1, 2],
        "the response carries all three records"
    );
    println!("Step 2: answered {took:?} after the third record, with offsets {offsets:?}");
    ctx.cleanup().await?;
    println!("\n✅ Long poll min_bytes threshold test PASSED\n");
    Ok(())
}

/// The `min_bytes` of [`test_long_poll_min_bytes_threshold`]: between what two and three
/// records of 150 bytes take in a response (377 and 535 bytes: a 61-byte batch header and 158
/// bytes a record).
const MIN_BYTES: i32 = 500;

/// An empty Fetch is answered after its own `max_wait_ms`, whatever it is: below, at and above
/// the broker's 1000 ms safety-net re-check (`pg_kafka.fetch_poll_interval_ms`), each wait is
/// at least `max_wait_ms` and not much more, so the broker neither answers at its first
/// re-check nor rounds the wait up to the next one.
pub async fn test_long_poll_timeout_precision() -> TestResult {
    println!("=== Test: Long Poll Timeout Precision ===\n");
    let ctx = TestContext::new().await?;
    let topic = super::timeout::empty_topic(&ctx, "longpoll-precision").await?;
    let mut s = TcpStream::connect(("127.0.0.1", crate::common::broker_port())).await?;
    println!("Step 1: empty topic {topic}");

    for (corr, max_wait_ms) in (1..).zip([150, 700, 1_000, 1_300]) {
        let took = super::timeout::timed_empty_fetch(&mut s, corr, &topic, max_wait_ms, 1).await?;
        let max_wait = Duration::from_millis(u64::try_from(max_wait_ms)?);
        println!("  max_wait_ms={max_wait_ms}: answered in {took:?}");
        assert!(
            took + super::timeout::EARLY_SLACK >= max_wait,
            "an empty Fetch with max_wait_ms={max_wait_ms} was answered early, in {took:?}"
        );
        assert!(
            took < max_wait + super::timeout::ANSWER_SLACK,
            "an empty Fetch with max_wait_ms={max_wait_ms} was answered late, in {took:?}"
        );
    }
    ctx.cleanup().await?;
    println!("\n✅ Long poll timeout precision test PASSED\n");
    Ok(())
}

/// Test long poll behavior after consumer disconnect
///
/// Scenario:
/// 1. Consumer starts long polling
/// 2. Drop consumer (simulates disconnect)
/// 3. Verify server handles gracefully
/// 4. New consumer can connect and consume
///
/// This tests connection cleanup during long poll.
pub async fn test_long_poll_consumer_disconnect() -> TestResult {
    println!("=== Test: Long Poll Consumer Disconnect ===\n");

    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "longpoll-disconnect")
        .build()
        .await?;

    // Create topic with message
    println!("Step 1: Creating topic with message...");
    let producer = create_producer()?;
    producer
        .send(
            FutureRecord::to(&topic.name)
                .payload("test-message")
                .key("key"),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(err, _)| err)?;
    println!("  Topic created");

    // Create and start first consumer
    println!("\nStep 2: Creating first consumer (will be dropped)...");
    {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", get_bootstrap_servers())
            .set("broker.address.family", "v4")
            .set(
                "group.id",
                format!("disconnect-group-{}", uuid::Uuid::new_v4()),
            )
            .set("auto.offset.reset", "earliest")
            .set("fetch.wait.max.ms", "30000") // Very long wait
            .set("fetch.min.bytes", "1")
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "false")
            .create()?;

        let consumer = Member::new(consumer);
        consumer.subscribe(&[&topic.name])?;
        println!("  Consumer subscribed, starting poll...");

        // Start polling in background (will be dropped)
        let poll_start = Instant::now();
        let _ = tokio::time::timeout(Duration::from_millis(500), consumer.recv()).await;
        println!("  Consumer polling for {:?}...", poll_start.elapsed());

        // Closes while its Fetch is parked on the broker.
        consumer.close(CLOSE_TIMEOUT).await?;
    }
    println!("  First consumer dropped (disconnected)\n");

    // Brief wait to ensure cleanup
    tokio::time::sleep(Duration::from_millis(500)).await;

    // New consumer should work fine
    println!("Step 3: Creating new consumer after disconnect...");
    let consumer2: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", get_bootstrap_servers())
        .set("broker.address.family", "v4")
        .set(
            "group.id",
            format!("disconnect-group2-{}", uuid::Uuid::new_v4()),
        )
        .set("auto.offset.reset", "earliest")
        .set("fetch.wait.max.ms", "5000")
        .set("fetch.min.bytes", "1")
        .set("session.timeout.ms", "10000")
        .set("enable.auto.commit", "false")
        .create()?;

    let consumer2 = Member::new(consumer2);
    consumer2.subscribe(&[&topic.name])?;
    println!("  New consumer subscribed");

    // Should receive message
    println!("\nStep 4: Verifying new consumer receives message...");
    let start = Instant::now();
    let mut received = false;
    while start.elapsed() < Duration::from_secs(5) {
        if let Ok(Ok(msg)) =
            tokio::time::timeout(Duration::from_millis(500), consumer2.recv()).await
        {
            let value = msg
                .payload()
                .map(|v| String::from_utf8_lossy(v).to_string());
            if value.as_deref() == Some("test-message") {
                received = true;
                println!("  ✅ New consumer received message");
                break;
            }
        }
    }

    assert!(received, "New consumer should receive the message");
    consumer2.close(CLOSE_TIMEOUT).await?;

    ctx.cleanup().await?;
    println!("\n✅ Long poll consumer disconnect test PASSED\n");
    Ok(())
}

/// Long polls of the same partition from several connections are all woken by one
/// Produce, at once, not one of them with the others left to the safety-net re-check
/// (`pg_kafka.fetch_poll_interval_ms`, 1 s). Measured at the wire level, as
/// `error_paths::listener_tests::test_listener_produce_wakes_long_poll_promptly` measures
/// one waiter: the three Fetches (max_wait_ms 10 000) are sent together and the record is
/// produced 2.2 s later, 0.2 s after their second re-check; an ack that lands within
/// 300 ms of the third re-check cannot be told apart from it, and the attempt is repeated
/// (3 attempts).
///
/// Asserts: each of the three Fetches is answered with the produced record within 300 ms
/// of the Produce's ack.
pub async fn test_long_poll_multiple_consumers_same_partition() -> TestResult {
    println!("=== Test: Long Poll Multiple Consumers Same Partition ===\n");
    let ctx = TestContext::new().await?;
    let topic = super::timeout::empty_topic(&ctx, "longpoll-multi").await?;
    let mut producer = Wire::connect().await?;
    for attempt in 1..=WAKE_ATTEMPTS {
        let offset = i64::try_from(attempt - 1)?;
        let started = Instant::now();
        let waiters: Vec<_> = (0..WAITERS)
            .map(|_| park_fetch(topic.clone(), offset))
            .collect();
        tokio::time::sleep(PRODUCE_AT).await;
        let (code, stored) = producer
            .produce(
                &topic,
                0,
                plain_batch(&[Bytes::from_static(b"broadcast")], Compression::None),
                None,
            )
            .await?;
        let acked = Instant::now();
        assert_eq!((code, stored), (0, offset));
        let mut after_ack = Vec::new();
        for waiter in waiters {
            let (received, offsets) = waiter.await??;
            assert_eq!(offsets, vec![offset], "each waiter gets the record");
            after_ack.push(received.saturating_duration_since(acked));
        }
        println!("  attempt {attempt}: answered {after_ack:?} after the ack");
        // Their third re-check is at least 3 s after `started`.
        if acked.duration_since(started) + PROMPT >= 3 * SAFETY_NET {
            println!("  the ack landed too close to the next safety-net re-check; retrying");
            continue;
        }
        assert!(
            after_ack.iter().all(|&d| d < PROMPT),
            "one Produce must wake every waiting Fetch at once: answered {after_ack:?} after the ack"
        );
        ctx.cleanup().await?;
        println!("\n✅ Long poll multiple consumers same partition test PASSED\n");
        return Ok(());
    }
    Err(format!(
        "every attempt's ack landed within {PROMPT:?} of a safety-net re-check; the wake-up \
         could not be measured"
    )
    .into())
}

/// The waiters of [`test_long_poll_multiple_consumers_same_partition`].
const WAITERS: usize = 3;
/// Its attempts before an inconclusive test fails.
const WAKE_ATTEMPTS: usize = 3;
/// When it produces after sending the Fetches: 0.2 s past their second safety-net re-check.
const PRODUCE_AT: Duration = Duration::from_millis(2_200);
/// The long poll's safety-net interval (`pg_kafka.fetch_poll_interval_ms`, default).
const SAFETY_NET: Duration = Duration::from_millis(1_000);
/// How soon after the Produce's ack a woken Fetch must be answered.
const PROMPT: Duration = Duration::from_millis(300);

/// A Fetch (v4, max_wait_ms 10 000, min_bytes 1) of partition 0 of `topic` from `offset` on
/// a connection of its own: when its answer arrived and the offsets it holds.
fn park_fetch(
    topic: String,
    offset: i64,
) -> tokio::task::JoinHandle<Result<(Instant, Vec<i64>), String>> {
    tokio::spawn(async move {
        let mut s = TcpStream::connect(get_bootstrap_servers())
            .await
            .map_err(|e| format!("connect: {e}"))?;
        let resp = roundtrip(&mut s, 4, 1, &fetch_request(&topic, 0, offset, 10_000))
            .await
            .map_err(|e| format!("fetch: {e}"))?;
        let received = Instant::now();
        let p = &resp.responses[0].partitions[0];
        let offsets = fetched_values(&p.records.clone().unwrap_or_default())
            .map_err(|e| format!("decode: {e}"))?
            .into_iter()
            .map(|(o, _)| o)
            .collect();
        Ok((received, offsets))
    })
}

/// Auto-commit keeps committing while the consumer sits in a long poll: its commit timer
/// is not held up by a Fetch parked on the broker.
///
/// Asserts: a consumer (enable.auto.commit, auto.commit.interval.ms 2000, fetch.wait.max.ms
/// 5000) consumes the topic's 3 records, and 3 s later, while it is still open and long
/// polling at the log end, the group's committed offset is 3 (read before the consumer
/// closes, which commits too).
pub async fn test_long_poll_auto_commit_interval() -> TestResult {
    println!("=== Test: Long Poll Auto-Commit Interval ===\n");

    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "longpoll-autocommit")
        .build()
        .await?;

    let group_id = format!("autocommit-group-{}", uuid::Uuid::new_v4());

    // Create topic with messages
    println!("Step 1: Creating topic with messages...");
    let producer = create_producer()?;
    for i in 0..3 {
        producer
            .send(
                FutureRecord::to(&topic.name)
                    .payload(&format!("message-{i}"))
                    .key(&format!("key-{i}")),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(err, _)| err)?;
    }
    println!("  Produced 3 messages");

    // Create consumer with auto-commit
    println!("\nStep 2: Creating consumer with auto.commit.interval.ms=2000...");
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", get_bootstrap_servers())
        .set("broker.address.family", "v4")
        .set("group.id", &group_id)
        .set("auto.offset.reset", "earliest")
        .set("fetch.wait.max.ms", "5000")
        .set("fetch.min.bytes", "1")
        .set("session.timeout.ms", "10000")
        .set("enable.auto.commit", "true")
        .set("auto.commit.interval.ms", "2000")
        .create()?;

    let consumer = Member::new(consumer);
    consumer.subscribe(&[&topic.name])?;
    println!("  Consumer subscribed");

    // Consume messages
    println!("\nStep 3: Consuming messages...");
    let mut consumed = 0;
    let start = Instant::now();
    while consumed < 3 && start.elapsed() < Duration::from_secs(5) {
        if let Ok(Ok(msg)) = tokio::time::timeout(Duration::from_millis(500), consumer.recv()).await
        {
            let value = msg
                .payload()
                .map(|v| String::from_utf8_lossy(v).to_string());
            if let Some(v) = value {
                if v.starts_with("message-") {
                    consumed += 1;
                    println!("  Consumed: {v}");
                }
            }
        }
    }
    println!("  Consumed {consumed} messages");
    assert_eq!(consumed, 3, "the consumer reads the topic's 3 records");

    // Wait for auto-commit interval
    println!("\nStep 4: Waiting for auto-commit interval (3s)...");
    tokio::time::sleep(Duration::from_secs(3)).await;
    println!("  Wait complete");

    // Check committed offset
    println!("\nStep 5: Checking committed offset...");
    let topic_row = ctx
        .db()
        .query_one(
            "SELECT id FROM kafka.topics WHERE name = $1",
            &[&topic.name],
        )
        .await?;
    let topic_id: i32 = topic_row.get(0);

    let offset_result = ctx
        .db()
        .query_opt(
            "SELECT committed_offset FROM kafka.consumer_offsets
             WHERE group_id = $1 AND topic_id = $2 AND partition_id = 0",
            &[&group_id, &topic_id],
        )
        .await?;

    let committed: Option<i64> = offset_result.map(|row| row.get(0));
    println!("  Committed offset while long polling: {committed:?}");
    consumer.close(CLOSE_TIMEOUT).await?;
    assert_eq!(
        committed,
        Some(3),
        "auto-commit must commit the consumed offsets during the long poll"
    );

    ctx.cleanup().await?;
    println!("\n✅ Long poll auto-commit interval test PASSED\n");
    Ok(())
}
