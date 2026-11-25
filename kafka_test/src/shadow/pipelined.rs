//! Shadow tests of the pipelined, order-preserving forwarder.
//!
//! Every test here talks to the external Kafka (category `shadow`):
//! - throughput floor of the forwarder;
//! - record headers and timestamps reach the external topic;
//! - per-partition order survives a failing record that is retried, and a
//!   delivery timeout during an external outage;
//! - a sync-mode produce against an unreachable cluster holds only its own
//!   response, not the broker (the DB thread);
//! - every producer GUC rebuilds the producer on reload, Sighup GUCs refuse a
//!   session SET, `shadow_metrics_enabled` and `messages_skipped` are real.

use crate::common::{create_db_client, create_producer, TestResult};
use crate::common::{to_f64, to_i64};
use crate::setup::TestContext;
use rdkafka::admin::{
    AdminClient, AdminOptions, AlterConfig, NewTopic, ResourceSpecifier, TopicReplication,
};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::Consumer;
use rdkafka::message::{Header, Headers, Message, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::Timestamp;
use std::time::{Duration, Instant};
use tokio_postgres::Client;

use super::external_client::{
    create_external_consumer, get_external_bootstrap_servers, get_internal_bootstrap_servers,
    verify_external_kafka_ready,
};
use super::helpers::{enable_shadow_mode, ShadowMode, ShadowTopicConfig, SyncMode, WriteMode};

fn async_config(external: Option<String>) -> ShadowTopicConfig {
    ShadowTopicConfig {
        mode: ShadowMode::Shadow,
        forward_percentage: 100,
        external_topic_name: external,
        sync_mode: SyncMode::Async,
        write_mode: WriteMode::DualWrite,
    }
}

/// A fast pg_kafka producer (batched, acks=1).
fn bulk_producer() -> Result<FutureProducer, Box<dyn std::error::Error>> {
    Ok(ClientConfig::new()
        .set("bootstrap.servers", crate::common::get_bootstrap_servers())
        .set("broker.address.family", "v4")
        .set("linger.ms", "5")
        .set("batch.num.messages", "1000")
        .set("message.timeout.ms", "60000")
        .set("queue.buffering.max.messages", "200000")
        .create()?)
}

/// An admin client of the external broker.
pub(super) fn external_admin(
) -> Result<AdminClient<DefaultClientContext>, Box<dyn std::error::Error>> {
    Ok(ClientConfig::new()
        .set("bootstrap.servers", get_external_bootstrap_servers())
        .set("broker.address.family", "v4")
        .create()?)
}

async fn alter_system(db: &Client, sql: &str) -> TestResult {
    db.execute(sql, &[]).await?;
    db.execute("SELECT pg_reload_conf()", &[]).await?;
    // The worker reloads on SIGHUP within ~100 ms; the forwarder reads the
    // reloaded config on its next record.
    tokio::time::sleep(Duration::from_millis(700)).await;
    Ok(())
}

/// (done, total) outbox rows of a topic.
async fn outbox_progress(
    db: &Client,
    topic: &str,
) -> Result<(i64, i64), Box<dyn std::error::Error>> {
    let row = db
        .query_one(
            "SELECT COUNT(*) FILTER (WHERE st.external_offset IS NOT NULL), COUNT(*) \
             FROM kafka.shadow_tracking st JOIN kafka.topics t ON t.id = st.topic_id \
             WHERE t.name = $1",
            &[&topic],
        )
        .await?;
    Ok((row.get(0), row.get(1)))
}

async fn wait_all_forwarded(
    db: &Client,
    topic: &str,
    expected: i64,
    timeout: Duration,
) -> Result<Duration, Box<dyn std::error::Error>> {
    let start = Instant::now();
    loop {
        let (done, total) = outbox_progress(db, topic).await?;
        if total == expected && done == expected {
            return Ok(start.elapsed());
        }
        if start.elapsed() > timeout {
            return Err(format!(
                "only {done}/{total} of {expected} outbox rows delivered after {timeout:?}"
            )
            .into());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Every message of an external topic, in partition order.
fn read_external(
    topic: &str,
    expected: usize,
    timeout: Duration,
) -> Result<Vec<rdkafka::message::OwnedMessage>, Box<dyn std::error::Error>> {
    let consumer = create_external_consumer(&format!("shadow-{}", uuid::Uuid::new_v4()))?;
    consumer.subscribe(&[topic])?;
    let mut out = Vec::new();
    let start = Instant::now();
    while start.elapsed() < timeout {
        match consumer.poll(Duration::from_millis(200)) {
            Some(Ok(m)) => out.push(m.detach()),
            Some(Err(e)) => eprintln!("external consume error: {e}"),
            None if expected > 0 && out.len() >= expected => break,
            None => {}
        }
    }
    Ok(out)
}

/// Throughput floor: the pipelined forwarder moves thousands of records per
/// second to a healthy external broker (a forwarder that awaited every delivery
/// report before sending the next record manages < ~100 msg/s here).
///
/// The floor applies to the forward phase, from the last produce ack until every
/// outbox row is delivered, and to the rows still undelivered at that ack: the
/// produce time (0.3-4 s on CI runners) says nothing about the forwarder, and the
/// rows it delivered while the produce ran are not counted against the phase. The
/// end-to-end rate is still printed.
pub async fn test_shadow_forward_throughput() -> TestResult {
    const N: usize = 20_000;
    const FLOOR_MSG_PER_SEC: f64 = 2_000.0;
    println!("=== Test: shadow forwarding throughput ===\n");

    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("shadow-throughput").await;
    verify_external_kafka_ready().await?;
    enable_shadow_mode(ctx.db(), &topic, &async_config(None)).await?;

    let producer = bulk_producer()?;
    let value = vec![b'x'; 100];
    let start = Instant::now();
    let mut pending = Vec::with_capacity(N);
    for i in 0..N {
        let key = format!("k{}", i % 97);
        pending.push(
            producer
                .send_result(FutureRecord::to(&topic).key(key.as_bytes()).payload(&value))
                .map_err(|(e, _)| e)?,
        );
    }
    for f in pending {
        f.await?.map_err(|(e, _)| e)?;
    }
    let produced_in = start.elapsed();
    let last_ack = Instant::now();
    // Rows delivered while the produce ran are not the forward phase's work.
    let (done_at_ack, _) = outbox_progress(ctx.db(), &topic).await?;
    println!(
        "Produced {N} records to pg_kafka in {produced_in:?}; {done_at_ack} already forwarded"
    );

    wait_all_forwarded(ctx.db(), &topic, to_i64(N), Duration::from_secs(240)).await?;
    let forwarded_in = last_ack.elapsed();
    let total = start.elapsed();
    let rate = to_f64(N) / total.as_secs_f64();
    let remaining = N - usize::try_from(done_at_ack)?;
    let forward_rate = to_f64(remaining) / forwarded_in.as_secs_f64();
    println!(
        "The other {remaining} forwarded {forwarded_in:?} after the last produce ack = \
         {forward_rate:.0} msg/s; end-to-end {total:?} = {rate:.0} msg/s"
    );
    println!("SHADOW_THROUGHPUT_MSG_PER_SEC={rate:.0}");
    println!("SHADOW_FORWARD_PHASE_MSG_PER_SEC={forward_rate:.0}");

    let failures: i64 = ctx
        .db()
        .query_one(
            "SELECT COALESCE(SUM(retry_count), 0)::bigint FROM kafka.shadow_tracking st \
             JOIN kafka.topics t ON t.id = st.topic_id WHERE t.name = $1",
            &[&topic],
        )
        .await?
        .get(0);
    assert_eq!(
        failures, 0,
        "a healthy broker must not record failed attempts"
    );
    let external = read_external(&topic, N, Duration::from_secs(60))?;
    assert!(
        external.len() >= N,
        "external topic has {} of {N} records",
        external.len()
    );
    assert!(
        forward_rate >= FLOOR_MSG_PER_SEC,
        "forwarding throughput {forward_rate:.0} msg/s after the last produce ack is below \
         the {FLOOR_MSG_PER_SEC} msg/s floor"
    );
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// Headers and timestamps reach the external topic, not only key and value stamped
/// with the forward time.
pub async fn test_shadow_forwards_headers_and_timestamps() -> TestResult {
    println!("=== Test: shadow forwards headers and timestamps ===\n");
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("shadow-headers").await;
    verify_external_kafka_ready().await?;
    // The record's CreateTime is years old, so on a topic with the broker's default
    // retention (7 days, by timestamp) the next retention check (every 5 minutes)
    // deletes it, possibly between the forward and the read below. The external
    // topic keeps its records forever instead.
    let admin = external_admin()?;
    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(10)));
    let new_topic = NewTopic::new(&topic, 1, TopicReplication::Fixed(1)).set("retention.ms", "-1");
    for r in admin.create_topics(&[new_topic], &opts).await? {
        r.map_err(|(t, e)| format!("create {t}: {e}"))?;
    }
    enable_shadow_mode(ctx.db(), &topic, &async_config(None)).await?;

    let producer = create_producer()?;
    let ts: i64 = 1_600_000_000_123; // 2020, far from "now"
    let headers = OwnedHeaders::new()
        .insert(Header {
            key: "trace-id",
            value: Some(&b"abc-123"[..]),
        })
        .insert(Header {
            key: "bin",
            value: Some(&[0u8, 255, 7][..]),
        });
    producer
        .send(
            FutureRecord::to(&topic)
                .key("hk")
                .payload("hv")
                .timestamp(ts)
                .headers(headers),
            Duration::from_secs(10),
        )
        .await
        .map_err(|(e, _)| e)?;
    // And one record without a producer timestamp-of-interest (rdkafka sets now).
    wait_all_forwarded(ctx.db(), &topic, 1, Duration::from_secs(30)).await?;

    let msgs = read_external(&topic, 1, Duration::from_secs(20))?;
    assert_eq!(msgs.len(), 1, "one external record");
    let m = &msgs[0];
    assert_eq!(m.key(), Some(&b"hk"[..]));
    assert_eq!(m.payload(), Some(&b"hv"[..]));
    assert_eq!(
        m.timestamp(),
        Timestamp::CreateTime(ts),
        "the producer's CreateTime must be forwarded"
    );
    let h = m.headers().ok_or("forwarded record has no headers")?;
    let got: Vec<(String, Vec<u8>)> = h
        .iter()
        .map(|x| (x.key.to_string(), x.value.unwrap_or_default().to_vec()))
        .collect();
    assert_eq!(
        got,
        vec![
            ("trace-id".to_string(), b"abc-123".to_vec()),
            ("bin".to_string(), vec![0u8, 255, 7]),
        ],
        "headers must be forwarded in order"
    );
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// The value and the headers of each fetched record.
type Fetched = Vec<(Vec<u8>, Vec<(String, Vec<u8>)>)>;

/// A header key holding U+0000 is an ordinary key to Kafka 3.7 (pg_kafka stores it, see
/// `consumer::headers`). The forwarder handed header keys to librdkafka as C strings, and
/// rdkafka's `OwnedHeaders::insert` panics on an interior NUL: the panic ended the
/// forwarder task, and with it the forwarding of every shadow topic until the worker
/// restarted (where the same row panicked again).
///
/// Asserts: after a record with header keys "a\0b", "k" and "\0" on topic A, a plain
/// record on A and one on another shadow topic B are delivered (every outbox row of
/// both topics gets an external offset), and a raw Fetch of the external topic A
/// returns the first record with its three headers byte for byte, then the second.
pub async fn test_shadow_forwards_a_header_key_holding_nul() -> TestResult {
    use crate::wire::records::{encode, plain_record};
    use crate::wire::Wire;
    use bytes::Bytes;
    use kafka_protocol::protocol::StrBytes;
    use kafka_protocol::records::{Compression, RecordBatchDecoder};
    println!("=== Test: shadow forwards a header key holding U+0000 ===\n");
    let ctx = TestContext::new().await?;
    let topic_a = ctx.unique_topic("shadow-nul-key").await;
    let topic_b = ctx.unique_topic("shadow-after-nul").await;
    verify_external_kafka_ready().await?;
    let admin = external_admin()?;
    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(10)));
    let new_topics = [
        NewTopic::new(&topic_a, 1, TopicReplication::Fixed(1)),
        NewTopic::new(&topic_b, 1, TopicReplication::Fixed(1)),
    ];
    for r in admin.create_topics(&new_topics, &opts).await? {
        r.map_err(|(t, e)| format!("create {t}: {e}"))?;
    }
    enable_shadow_mode(ctx.db(), &topic_a, &async_config(None)).await?;
    enable_shadow_mode(ctx.db(), &topic_b, &async_config(None)).await?;

    let headers: [(&str, &[u8]); 3] = [("a\0b", b"\0v\0"), ("k", b"plain"), ("\0", b"")];
    let mut record = plain_record(0, None, b"with-nul-key");
    for (k, v) in headers {
        record.headers.insert(
            StrBytes::from_string(k.to_string()),
            Some(Bytes::copy_from_slice(v)),
        );
    }
    let mut wire = Wire::connect().await?;
    let produced = wire
        .produce(&topic_a, 0, encode(&[record], Compression::None), None)
        .await?;
    assert_eq!(produced, (0, 0), "the record is stored at offset 0");
    println!("Step 1: produced a record with header keys \"a\\0b\", \"k\" and \"\\0\" to A");
    // The forwarder handles the first record before the next ones arrive.
    tokio::time::sleep(Duration::from_secs(1)).await;

    let plain = || encode(&[plain_record(0, None, b"plain")], Compression::None);
    assert_eq!(wire.produce(&topic_a, 0, plain(), None).await?, (0, 1));
    assert_eq!(wire.produce(&topic_b, 0, plain(), None).await?, (0, 0));
    println!("Step 2: produced a plain record to A and one to B");

    wait_all_forwarded(ctx.db(), &topic_a, 2, Duration::from_secs(30)).await?;
    wait_all_forwarded(ctx.db(), &topic_b, 1, Duration::from_secs(30)).await?;
    println!("Step 3: every outbox row of A and B is delivered");

    let mut external = Wire::connect_to(&get_external_bootstrap_servers()).await?;
    let p = external.fetch(&topic_a, 0, 0, 0).await?;
    assert_eq!(p.error_code, 0);
    let mut records = p.records.unwrap_or_default();
    let fetched: Fetched = RecordBatchDecoder::decode_all(&mut records)?
        .iter()
        .flat_map(|b| b.records.iter())
        .map(|r| {
            let headers = r
                .headers
                .iter()
                .map(|(k, v)| {
                    (
                        k.to_string(),
                        v.as_ref().map_or_else(Vec::new, |v| v.to_vec()),
                    )
                })
                .collect();
            (
                r.value.as_ref().map_or_else(Vec::new, |v| v.to_vec()),
                headers,
            )
        })
        .collect();
    let expected_headers: Vec<(String, Vec<u8>)> = headers
        .iter()
        .map(|(k, v)| ((*k).to_string(), v.to_vec()))
        .collect();
    assert_eq!(
        fetched,
        vec![
            (b"with-nul-key".to_vec(), expected_headers),
            (b"plain".to_vec(), Vec::new()),
        ],
        "the external topic holds both records, the headers byte for byte"
    );
    println!("Step 4: the external topic holds the record with its headers unchanged");
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// A record that fails is retried BEFORE any later record of its partition is
/// delivered, so the external topic keeps the local order. Failure injection:
/// the external topic's max.message.bytes is below one (large) record until the
/// test raises it; the later records must not overtake the retried one.
pub async fn test_shadow_order_preserved_across_a_retried_failure() -> TestResult {
    const N: usize = 40;
    const BIG: usize = 5;
    println!("=== Test: shadow order preserved across a retried failure ===\n");
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("shadow-order").await;
    let external = format!("{topic}-ext");
    verify_external_kafka_ready().await?;

    let admin = external_admin()?;
    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(10)));
    let result = async {
        let new_topic = NewTopic::new(&external, 1, TopicReplication::Fixed(1))
            .set("max.message.bytes", "3000");
        for r in admin.create_topics(&[new_topic], &opts).await? {
            r.map_err(|(t, e)| format!("create {t}: {e}"))?;
        }
        enable_shadow_mode(ctx.db(), &topic, &async_config(Some(external.clone()))).await?;

        let producer = bulk_producer()?;
        let big = vec![b'B'; 8_000];
        let mut pending = Vec::new();
        for i in 0..N {
            let small = format!("{i:04}");
            let mut payload = small.clone().into_bytes();
            if i == BIG {
                payload.extend_from_slice(&big);
            }
            pending.push(
                producer
                    .send_result(FutureRecord::to(&topic).key("same-key").payload(&payload))
                    .map_err(|(e, _)| e)?,
            );
        }
        for f in pending {
            f.await?.map_err(|(e, _)| e)?;
        }

        // Wait until a failed attempt is recorded (the batch holding the big
        // record is rejected; with enable.gapless.guarantee the producer goes
        // fatal and the first failed record of the batch counts as the failure).
        // The broker's MESSAGE_TOO_LARGE is retried by the idempotent producer
        // before it is reported, so this can take up to the delivery timeout.
        let deadline = Instant::now() + Duration::from_secs(170);
        loop {
            let failed: i64 = ctx
                .db()
                .query_one(
                    "SELECT COALESCE(MAX(st.retry_count), 0)::bigint FROM kafka.shadow_tracking st \
                     JOIN kafka.topics t ON t.id = st.topic_id WHERE t.name = $1",
                    &[&topic],
                )
                .await?
                .get(0);
            if failed >= 1 {
                break;
            }
            if Instant::now() > deadline {
                return Err("the oversized record never failed".into());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        println!("Oversized record failed; nothing after it may be delivered yet");
        let early = read_external(&external, 0, Duration::from_secs(3))?;
        let early_idx: Vec<usize> = early
            .iter()
            .map(|m| {
                String::from_utf8_lossy(&m.payload().unwrap_or_default()[..4])
                    .parse()
                    .unwrap()
            })
            .collect();
        assert!(
            early_idx.iter().all(|&i| i < BIG),
            "records after the failed one were delivered before its retry: {early_idx:?}"
        );

        // Heal the external topic: the retry now succeeds.
        let alter = AlterConfig::new(ResourceSpecifier::Topic(&external))
            .set("max.message.bytes", "1048576");
        for r in admin.alter_configs(&[alter], &opts).await? {
            r.map_err(|(t, e)| format!("alter {t:?}: {e}"))?;
        }
        wait_all_forwarded(ctx.db(), &topic, to_i64(N), Duration::from_secs(120)).await?;

        let msgs = read_external(&external, N, Duration::from_secs(30))?;
        let idx: Vec<usize> = msgs
            .iter()
            .map(|m| {
                String::from_utf8_lossy(&m.payload().unwrap_or_default()[..4])
                    .parse()
                    .unwrap()
            })
            .collect();
        // At-least-once: a re-send across a producer rebuild may duplicate, but the
        // first occurrence of every record must be in local order.
        let mut seen = std::collections::HashSet::new();
        let first: Vec<usize> = idx.iter().copied().filter(|i| seen.insert(*i)).collect();
        println!("external order (first occurrences): {first:?}");
        assert_eq!(
            first,
            (0..N).collect::<Vec<_>>(),
            "external order must match local order"
        );
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await
    .map_err(|e| e.to_string());
    ctx.cleanup().await?;
    let _ = admin.delete_topics(&[&external], &opts).await;
    result?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// Shortened shadow delivery timeout for the delivery-timeout order test.
const SHORT_DELIVERY_TIMEOUT_MS: u64 = 6_000;

/// A delivery timeout must not let a later record overtake an earlier one.
///
/// The external broker is paused (its process stopped: connections are
/// accepted by the kernel but never answered) and the shadow producer is
/// rebuilt while it is, so every record waits in librdkafka's queue. The
/// first records are enqueued at t0, the rest half a delivery timeout later,
/// so their deadlines are staggered. When the first records time out (t0 +
/// timeout) the later ones still have half a timeout to go, and the test
/// resumes the broker in between. A producer that failed the first records
/// alone (librdkafka's per-message `message.timeout.ms`) delivers the later
/// records, still queued, before the outbox re-sends the first ones; the
/// forwarder must fail everything outstanding together instead, so the
/// partition restarts at its first undelivered record.
///
/// The pause and resume are shell commands: `EXTERNAL_KAFKA_PAUSE_CMD` /
/// `EXTERNAL_KAFKA_RESUME_CMD`, by default `docker pause` / `docker unpause` of
/// docker-compose.yml's `external_kafka` container.
pub async fn test_shadow_order_preserved_across_a_delivery_timeout() -> TestResult {
    const EARLY: usize = 5;
    const N: usize = 10;
    println!("=== Test: shadow order preserved across a delivery timeout ===\n");
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("shadow-timeout-order").await;
    let external = format!("{topic}-ext");
    verify_external_kafka_ready().await?;
    let db = create_db_client().await?;

    let admin = external_admin()?;
    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(10)));
    let mut pause = None;
    let result = async {
        let new_topic = NewTopic::new(&external, 1, TopicReplication::Fixed(1));
        for r in admin.create_topics(&[new_topic], &opts).await? {
            r.map_err(|(t, e)| format!("create {t}: {e}"))?;
        }
        alter_system(
            &db,
            &format!(
                "ALTER SYSTEM SET pg_kafka.test_shadow_delivery_timeout_ms = {SHORT_DELIVERY_TIMEOUT_MS}"
            ),
        )
        .await?;
        enable_shadow_mode(ctx.db(), &topic, &async_config(Some(external.clone()))).await?;

        println!("Step 1: pause the external broker and rebuild the shadow producer");
        pause = Some(BrokerPause::new()?);
        // A producer setting change rebuilds the producer, which then cannot
        // finish connecting: nothing it is handed leaves its queue.
        alter_system(&db, "ALTER SYSTEM SET pg_kafka.shadow_linger_ms = 7").await?;

        let producer = create_producer()?;
        let send = |i: usize| {
            let payload = format!("{i:04}");
            let f = producer
                .send_result(FutureRecord::to(&topic).key("same-key").payload(&payload))
                .map_err(|(e, _)| e.to_string());
            async move {
                f?.await
                    .map_err(|e| e.to_string())?
                    .map_err(|(e, _)| e.to_string())?;
                Ok::<(), String>(())
            }
        };
        println!("Step 2: records 0..{EARLY} at t0, {EARLY}..{N} half a timeout later");
        for i in 0..EARLY {
            send(i).await?;
        }
        let t0 = Instant::now();
        tokio::time::sleep(Duration::from_millis(SHORT_DELIVERY_TIMEOUT_MS / 2)).await;
        for i in EARLY..N {
            send(i).await?;
        }

        println!("Step 3: wait for the first failed attempt, then resume the broker");
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let failed: i64 = ctx
                .db()
                .query_one(
                    "SELECT COALESCE(MAX(st.retry_count), 0)::bigint FROM kafka.shadow_tracking st \
                     JOIN kafka.topics t ON t.id = st.topic_id WHERE t.name = $1",
                    &[&topic],
                )
                .await?
                .get(0);
            if failed >= 1 {
                break;
            }
            if Instant::now() > deadline {
                return Err("no delivery attempt ever timed out".into());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let failed_after = t0.elapsed();
        if let Some(p) = pause.take() {
            p.resume()?;
        }
        let resumed_after = t0.elapsed();
        println!("First failure {failed_after:?} after t0; broker resumed {resumed_after:?} after t0");
        // The scenario needs the broker back before the later records' own
        // deadline (t0 + 1.5 timeouts).
        assert!(
            resumed_after < Duration::from_millis(SHORT_DELIVERY_TIMEOUT_MS * 3 / 2),
            "resumed too late ({resumed_after:?}) to tell the orders apart"
        );

        wait_all_forwarded(ctx.db(), &topic, to_i64(N), Duration::from_secs(120)).await?;
        let msgs = read_external(&external, N, Duration::from_secs(30))?;
        let idx: Vec<usize> = msgs
            .iter()
            .map(|m| {
                String::from_utf8_lossy(m.payload().unwrap_or_default())
                    .parse()
                    .unwrap_or(usize::MAX)
            })
            .collect();
        println!("external order: {idx:?}");
        // At-least-once: a re-send may duplicate, but the first occurrence of
        // every record must be in local order, and none may be missing.
        let mut seen = std::collections::HashSet::new();
        let first: Vec<usize> = idx.iter().copied().filter(|i| seen.insert(*i)).collect();
        assert_eq!(
            first,
            (0..N).collect::<Vec<_>>(),
            "external order (first occurrences) must match the local order"
        );
        // Only the record the partition waited on counts a failed attempt; the
        // records behind it keep their retry budget.
        let rows = ctx
            .db()
            .query(
                "SELECT st.local_offset, st.retry_count FROM kafka.shadow_tracking st \
                 JOIN kafka.topics t ON t.id = st.topic_id WHERE t.name = $1 \
                 ORDER BY st.local_offset",
                &[&topic],
            )
            .await?;
        let retries: Vec<(i64, i32)> = rows.iter().map(|r| (r.get(0), r.get(1))).collect();
        println!("retry counts (local offset, retries): {retries:?}");
        assert!(
            retries.first().is_some_and(|&(_, n)| n >= 1)
                && retries.iter().skip(1).all(|&(_, n)| n == 0),
            "only the partition's first record may count failed attempts: {retries:?}"
        );
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await
    .map_err(|e| e.to_string());
    if let Some(p) = pause.take() {
        let _ = p.resume();
    }
    let _ = alter_system(&db, "ALTER SYSTEM RESET pg_kafka.shadow_linger_ms").await;
    let _ = alter_system(
        &db,
        "ALTER SYSTEM RESET pg_kafka.test_shadow_delivery_timeout_ms",
    )
    .await;
    ctx.cleanup().await?;
    let _ = admin.delete_topics(&[&external], &opts).await;
    result?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// The external broker, paused until [`BrokerPause::resume`] (or the drop,
/// so a failing test never leaves it stopped).
pub(super) struct BrokerPause {
    resumed: bool,
}

impl BrokerPause {
    pub(super) fn new() -> Result<Self, String> {
        run_shell("EXTERNAL_KAFKA_PAUSE_CMD", "docker pause external_kafka")?;
        Ok(Self { resumed: false })
    }

    pub(super) fn resume(mut self) -> Result<(), String> {
        self.resumed = true;
        run_shell("EXTERNAL_KAFKA_RESUME_CMD", "docker unpause external_kafka")
    }
}

impl Drop for BrokerPause {
    fn drop(&mut self) {
        if !self.resumed {
            let _ = run_shell("EXTERNAL_KAFKA_RESUME_CMD", "docker unpause external_kafka");
        }
    }
}

/// Run the shell command in `var` (or `default`).
fn run_shell(var: &str, default: &str) -> Result<(), String> {
    let cmd = std::env::var(var).unwrap_or_else(|_| default.to_string());
    let status = std::process::Command::new("sh")
        .arg("-c")
        .arg(&cmd)
        .status()
        .map_err(|e| format!("{var} `{cmd}`: {e}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("{var} `{cmd}` failed: {status}"))
    }
}

/// Sync mode with the external cluster unreachable: the sync produce waits at
/// most pg_kafka.shadow_sync_timeout_ms, and a concurrent produce to an
/// unrelated topic is not delayed (waiting on the DB thread would stall every client,
/// up to 5 s per batch).
pub async fn test_shadow_sync_stall_is_bounded_to_its_own_response() -> TestResult {
    println!("=== Test: sync-mode stall is bounded to its own response ===\n");
    let ctx = TestContext::new().await?;
    let sync_topic = ctx.unique_topic("shadow-sync-down").await;
    let other_topic = ctx.unique_topic("shadow-unrelated").await;
    verify_external_kafka_ready().await?;
    enable_shadow_mode(
        ctx.db(),
        &sync_topic,
        &ShadowTopicConfig {
            sync_mode: SyncMode::Sync,
            ..async_config(None)
        },
    )
    .await?;
    let db = create_db_client().await?;
    // Unreachable external cluster (connection refused), 3 s sync hold.
    alter_system(
        &db,
        "ALTER SYSTEM SET pg_kafka.shadow_bootstrap_servers = '127.0.0.1:1'",
    )
    .await?;
    // Best effort: a build without this GUC rejects it.
    let _ = alter_system(
        &db,
        "ALTER SYSTEM SET pg_kafka.shadow_sync_timeout_ms = 3000",
    )
    .await;

    let result = async {
        let producer = create_producer()?;
        // Warm up the unrelated topic (metadata, topic creation).
        producer
            .send(
                FutureRecord::<(), _>::to(&other_topic).payload("warm"),
                Duration::from_secs(10),
            )
            .await
            .map_err(|(e, _)| e)?;

        let p2 = create_producer()?;
        let t = sync_topic.clone();
        let sync_send = tokio::spawn(async move {
            let start = Instant::now();
            let r = p2
                .send(
                    FutureRecord::to(&t).key("k").payload("held"),
                    Duration::from_secs(30),
                )
                .await;
            (start.elapsed(), r.is_ok())
        });
        tokio::time::sleep(Duration::from_millis(200)).await;

        let mut worst = Duration::ZERO;
        for i in 0..15 {
            let start = Instant::now();
            producer
                .send(
                    FutureRecord::<(), _>::to(&other_topic).payload(&format!("u{i}")),
                    Duration::from_secs(30),
                )
                .await
                .map_err(|(e, _)| e)?;
            worst = worst.max(start.elapsed());
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let (held_for, ok) = sync_send.await?;
        println!("unrelated produce worst latency {worst:?}; sync produce held {held_for:?}");
        println!("SHADOW_SYNC_UNRELATED_WORST_MS={}", worst.as_millis());
        assert!(
            ok,
            "the sync produce itself succeeds (the record is stored locally)"
        );
        assert!(
            worst < Duration::from_millis(1000),
            "an unrelated produce waited {worst:?}: the sync wait stalled the broker"
        );
        assert!(
            held_for >= Duration::from_millis(2500) && held_for < Duration::from_millis(6000),
            "the sync response must be held ~shadow_sync_timeout_ms (3 s), was {held_for:?}"
        );
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await
    .map_err(|e| e.to_string());

    let _ = alter_system(&db, "ALTER SYSTEM RESET pg_kafka.shadow_sync_timeout_ms").await;
    alter_system(
        &db,
        &format!(
            "ALTER SYSTEM SET pg_kafka.shadow_bootstrap_servers = '{}'",
            get_internal_bootstrap_servers()
        ),
    )
    .await?;
    ctx.cleanup().await?;
    result?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// GUCs do what they say: a producer setting other than the bootstrap servers
/// (here the security protocol) rebuilds the producer on reload; the shadow
/// GUCs are Sighup (a session SET is refused instead of silently ignored);
/// shadow_metrics_enabled gates kafka.shadow_metrics; sampled-out records are
/// counted in messages_skipped.
pub async fn test_shadow_gucs_take_effect_on_reload() -> TestResult {
    println!("=== Test: shadow GUCs take effect on reload ===\n");
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("shadow-guc").await;
    let sampled = ctx.unique_topic("shadow-sampled").await;
    verify_external_kafka_ready().await?;
    enable_shadow_mode(ctx.db(), &topic, &async_config(None)).await?;
    enable_shadow_mode(
        ctx.db(),
        &sampled,
        &ShadowTopicConfig {
            forward_percentage: 50,
            ..async_config(None)
        },
    )
    .await?;
    let db = create_db_client().await?;

    // 1. Session SET of a shadow GUC is refused (it would have no effect on the
    //    worker).
    for guc in [
        "pg_kafka.shadow_max_retries = 1",
        "pg_kafka.shadow_batch_size = 10",
        "pg_kafka.shadow_metrics_enabled = off",
        "pg_kafka.shadow_retry_backoff_ms = 20",
        "pg_kafka.shadow_linger_ms = 1",
    ] {
        let r = db.execute(&format!("SET {guc}"), &[]).await;
        assert!(r.is_err(), "SET {guc} must be refused (Sighup GUC)");
    }

    let result = async {
        let producer = create_producer()?;
        // 2. Wrong security protocol: the producer is rebuilt (SSL against a
        //    PLAINTEXT broker cannot deliver).
        alter_system(&db, "ALTER SYSTEM SET pg_kafka.shadow_security_protocol = 'SSL'").await?;
        producer
            .send(FutureRecord::to(&topic).key("a").payload("1"), Duration::from_secs(10))
            .await
            .map_err(|(e, _)| e)?;
        tokio::time::sleep(Duration::from_secs(4)).await;
        let (done, total) = outbox_progress(ctx.db(), &topic).await?;
        assert_eq!(total, 1);
        assert_eq!(
            done, 0,
            "with security_protocol=SSL the record must not be delivered: the producer \
             was not rebuilt on reload"
        );
        // 3. Back to PLAINTEXT and metrics off: delivered, not counted.
        alter_system(&db, "ALTER SYSTEM SET pg_kafka.shadow_metrics_enabled = off").await?;
        alter_system(&db, "ALTER SYSTEM SET pg_kafka.shadow_security_protocol = 'PLAINTEXT'")
            .await?;
        wait_all_forwarded(ctx.db(), &topic, 1, Duration::from_secs(150)).await?;
        let counted: i64 = ctx
            .db()
            .query_one(
                "SELECT COALESCE(SUM(sm.messages_forwarded), 0)::bigint FROM kafka.shadow_metrics sm \
                 JOIN kafka.topics t ON t.id = sm.topic_id WHERE t.name = $1",
                &[&topic],
            )
            .await?
            .get(0);
        assert_eq!(counted, 0, "shadow_metrics_enabled=off must not write kafka.shadow_metrics");
        alter_system(&db, "ALTER SYSTEM RESET pg_kafka.shadow_metrics_enabled").await?;

        // 4. messages_skipped counts the sampled-out records.
        for i in 0..40 {
            producer
                .send(
                    FutureRecord::to(&sampled).key(&format!("s{i}")).payload("x"),
                    Duration::from_secs(10),
                )
                .await
                .map_err(|(e, _)| e)?;
        }
        let row = ctx
            .db()
            .query_one(
                "SELECT total_skipped, (SELECT COUNT(*) FROM kafka.shadow_tracking st \
                 WHERE st.topic_id = t.id)::bigint \
                 FROM kafka.shadow_status s JOIN kafka.topics t ON t.name = s.topic_name \
                 WHERE s.topic_name = $1",
                &[&sampled],
            )
            .await?;
        let skipped: i64 = row.get(0);
        let queued: i64 = row.get(1);
        println!("sampled topic: {queued} queued, {skipped} skipped");
        assert!(skipped > 0, "50% routing must skip some records");
        assert_eq!(skipped + queued, 40, "every record is either queued or counted skipped");
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await
    .map_err(|e| e.to_string());

    db.execute(
        "ALTER SYSTEM SET pg_kafka.shadow_security_protocol = 'PLAINTEXT'",
        &[],
    )
    .await?;
    alter_system(&db, "ALTER SYSTEM RESET pg_kafka.shadow_metrics_enabled").await?;
    ctx.cleanup().await?;
    result?;
    println!("✅ Test PASSED\n");
    Ok(())
}
