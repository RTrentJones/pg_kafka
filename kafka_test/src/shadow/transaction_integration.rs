//! Transaction integration tests for shadow mode
//!
//! Tests for transaction commit/abort behavior with shadow mode forwarding.

use crate::common::{create_transactional_producer, TestResult};
use crate::setup::TestContext;
use rdkafka::producer::{FutureRecord, Producer};
use std::time::Duration;

use super::assertions::{assert_external_kafka_empty, assert_message_in_external_kafka};
use super::external_client::verify_external_kafka_ready;
use super::helpers::{enable_shadow_mode, ShadowMode, ShadowTopicConfig, SyncMode, WriteMode};

/// Test that committed transactions are forwarded
///
/// Produces a message within a transaction, commits, and verifies
/// it appears in external Kafka.
pub async fn test_committed_transaction_forwarded() -> TestResult {
    println!("=== Test: Committed Transaction Forwarded ===\n");

    // 1. SETUP
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("txn-commit").await;
    let txn_id = format!("shadow-txn-{}", ctx.test_id);

    println!("Step 1: Verifying external Kafka is ready...");
    verify_external_kafka_ready().await?;
    println!("✅ External Kafka ready\n");

    println!("Step 2: Enabling shadow mode...");
    enable_shadow_mode(
        ctx.db(),
        &topic,
        &ShadowTopicConfig {
            mode: ShadowMode::Shadow,
            forward_percentage: 100,
            external_topic_name: None,
            sync_mode: SyncMode::Sync,
            write_mode: WriteMode::DualWrite,
        },
    )
    .await?;
    println!("✅ Shadow mode enabled\n");

    // 2. ACTION - Produce within transaction and commit
    println!("Step 3: Creating transactional producer...");
    let producer = create_transactional_producer(&txn_id)?;
    producer.init_transactions(Duration::from_secs(10))?;
    println!("✅ Transactional producer initialized\n");

    println!("Step 4: Beginning transaction...");
    producer.begin_transaction()?;
    println!("✅ Transaction started\n");

    println!("Step 5: Producing message within transaction...");
    let key = b"txn-commit-key";
    let value = b"txn-commit-value";

    producer
        .send(
            FutureRecord::to(&topic).key(&key[..]).payload(&value[..]),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(e, _)| e)?;
    println!("✅ Message produced\n");

    println!("Step 6: Committing transaction...");
    producer.commit_transaction(Duration::from_secs(10))?;
    println!("✅ Transaction committed\n");

    // 3. VERIFY
    println!("=== Verification ===\n");
    tokio::time::sleep(Duration::from_millis(1000)).await;

    println!("Checking external Kafka for committed message...");
    assert_message_in_external_kafka(&topic, Some(key), Some(value), Duration::from_secs(10))
        .await?;
    println!("✅ Committed message found in external Kafka\n");

    // 4. CLEANUP
    ctx.cleanup().await?;
    println!("✅ Test PASSED: Committed Transaction Forwarded\n");
    Ok(())
}

/// Test that aborted transactions are NOT forwarded
///
/// Produces a message within a transaction, aborts, and verifies
/// it does NOT appear in external Kafka.
pub async fn test_aborted_transaction_not_forwarded() -> TestResult {
    println!("=== Test: Aborted Transaction Not Forwarded ===\n");

    // 1. SETUP
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("txn-abort").await;
    let txn_id = format!("shadow-abort-{}", ctx.test_id);

    println!("Step 1: Verifying external Kafka is ready...");
    verify_external_kafka_ready().await?;
    println!("✅ External Kafka ready\n");

    println!("Step 2: Enabling shadow mode...");
    enable_shadow_mode(
        ctx.db(),
        &topic,
        &ShadowTopicConfig {
            mode: ShadowMode::Shadow,
            forward_percentage: 100,
            external_topic_name: None,
            sync_mode: SyncMode::Sync,
            write_mode: WriteMode::DualWrite,
        },
    )
    .await?;
    println!("✅ Shadow mode enabled\n");

    // 2. ACTION - Produce within transaction and abort
    println!("Step 3: Creating transactional producer...");
    let producer = create_transactional_producer(&txn_id)?;
    producer.init_transactions(Duration::from_secs(10))?;
    println!("✅ Transactional producer initialized\n");

    println!("Step 4: Beginning transaction...");
    producer.begin_transaction()?;
    println!("✅ Transaction started\n");

    println!("Step 5: Producing message within transaction...");
    let key = b"txn-abort-key";
    let value = b"txn-abort-value";

    producer
        .send(
            FutureRecord::to(&topic).key(&key[..]).payload(&value[..]),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(e, _)| e)?;
    println!("✅ Message produced\n");

    println!("Step 6: Aborting transaction...");
    producer.abort_transaction(Duration::from_secs(10))?;
    println!("✅ Transaction aborted\n");

    // 3. VERIFY
    println!("=== Verification ===\n");
    tokio::time::sleep(Duration::from_millis(1000)).await;

    println!("Checking external Kafka (should be empty)...");
    assert_external_kafka_empty(&topic, Duration::from_secs(3)).await?;
    println!("✅ No message in external Kafka (aborted transaction not forwarded)\n");

    // 4. CLEANUP
    ctx.cleanup().await?;
    println!("✅ Test PASSED: Aborted Transaction Not Forwarded\n");
    Ok(())
}

/// Test that a committed transaction forwards via the DURABLE OUTBOX (RA-1/RA-2)
///
/// Uses async mode so forwarding can only happen via the background poll (never
/// an inline `block_on` on the commit path — the RA-1 fix). Asserts both that
/// the message reaches external Kafka AND that the `kafka.shadow_tracking` outbox
/// row is written and finalized (`external_offset` non-NULL) — proving the txn
/// commit now routes through the same at-least-once outbox as a non-txn produce.
pub async fn test_committed_transaction_uses_durable_outbox() -> TestResult {
    println!("=== Test: Committed Transaction Uses Durable Outbox (RA-1/RA-2) ===\n");

    // 1. SETUP
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("txn-outbox").await;
    let txn_id = format!("shadow-txn-outbox-{}", ctx.test_id);

    verify_external_kafka_ready().await?;
    enable_shadow_mode(
        ctx.db(),
        &topic,
        &ShadowTopicConfig {
            mode: ShadowMode::Shadow,
            forward_percentage: 100,
            external_topic_name: None,
            sync_mode: SyncMode::Async, // async → only the poll can forward
            write_mode: WriteMode::DualWrite,
        },
    )
    .await?;

    // 2. ACTION — produce within a transaction and commit.
    let producer = create_transactional_producer(&txn_id)?;
    producer.init_transactions(Duration::from_secs(10))?;
    producer.begin_transaction()?;
    let key = b"txn-outbox-key";
    let value = b"txn-outbox-value";
    producer
        .send(
            FutureRecord::to(&topic).key(&key[..]).payload(&value[..]),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(e, _)| e)?;
    producer.commit_transaction(Duration::from_secs(10))?;
    println!("✅ Transaction committed\n");

    // 3. VERIFY
    // 3a. Reaches external Kafka — via the poll, not an inline commit-path send.
    assert_message_in_external_kafka(&topic, Some(key), Some(value), Duration::from_secs(15))
        .await?;
    println!("✅ Forwarded to external Kafka (via the outbox poll)\n");

    // 3b. The outbox row exists and is finalized — proving the durable path.
    let mut finalized = false;
    for _ in 0..20 {
        let row = ctx
            .db()
            .query_one(
                r"
                SELECT COUNT(*) FILTER (WHERE st.external_offset IS NOT NULL) AS done,
                       COUNT(*) AS total
                FROM kafka.shadow_tracking st
                JOIN kafka.topics t ON t.id = st.topic_id
                WHERE t.name = $1
                ",
                &[&topic],
            )
            .await?;
        let done: i64 = row.get("done");
        let total: i64 = row.get("total");
        if total >= 1 && done >= 1 {
            finalized = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    if !finalized {
        return Err("committed txn did not produce a finalized shadow_tracking outbox row".into());
    }
    println!("✅ Outbox row written and finalized for the committed txn\n");

    // 4. CLEANUP
    ctx.cleanup().await?;
    println!("✅ Test PASSED: Committed Transaction Uses Durable Outbox\n");
    Ok(())
}

/// B1: a transactional commit must not enqueue outbox pointers for message rows
/// that do not exist any more. The pending message row is deleted after it is written
/// but before commit. The commit enqueues the transaction's records from the
/// producer's pending rows in kafka.messages, so a deleted row gets no outbox row; an
/// orphan `kafka.shadow_tracking` row would never be claimable by the poll's EXISTS
/// guard and never cleaned (a slow leak).
pub async fn test_committed_txn_leaves_no_orphan_outbox_rows() -> TestResult {
    println!("=== Test: Committed Txn Leaves No Orphan Outbox Rows (B1) ===\n");

    // 1. SETUP
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("txn-orphan").await;
    let txn_id = format!("shadow-txn-orphan-{}", ctx.test_id);

    verify_external_kafka_ready().await?;
    enable_shadow_mode(
        ctx.db(),
        &topic,
        &ShadowTopicConfig {
            mode: ShadowMode::Shadow,
            forward_percentage: 100,
            external_topic_name: None,
            sync_mode: SyncMode::Async,
            write_mode: WriteMode::DualWrite,
        },
    )
    .await?;

    // 2. ACTION — produce in a txn (the message row lands with txn_state='pending';
    // nothing is enqueued until the commit), then delete the pending row before
    // committing.
    let producer = create_transactional_producer(&txn_id)?;
    producer.init_transactions(Duration::from_secs(10))?;
    producer.begin_transaction()?;
    producer
        .send(
            FutureRecord::to(&topic).key(&b"k"[..]).payload(&b"v"[..]),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(e, _)| e)?;

    let deleted = ctx
        .db()
        .execute(
            r"
            DELETE FROM kafka.messages m
            USING kafka.topics t
            WHERE m.topic_id = t.id AND t.name = $1 AND m.txn_state = 'pending'
            ",
            &[&topic],
        )
        .await?;
    assert!(deleted >= 1, "expected a pending message row to delete");

    producer.commit_transaction(Duration::from_secs(10))?;
    println!("✅ Transaction committed after deleting the pending message row\n");

    // 3. VERIFY — the commit's outbox enqueue must not have written a pointer for
    // the now-missing message row.
    let row = ctx
        .db()
        .query_one(
            r"
            SELECT COUNT(*) AS orphans
            FROM kafka.shadow_tracking st
            JOIN kafka.topics t ON t.id = st.topic_id
            WHERE t.name = $1
              AND NOT EXISTS (
                SELECT 1 FROM kafka.messages m
                WHERE m.topic_id = st.topic_id
                  AND m.partition_id = st.partition_id
                  AND m.partition_offset = st.local_offset
              )
            ",
            &[&topic],
        )
        .await?;
    let orphans: i64 = row.get("orphans");
    assert_eq!(
        orphans, 0,
        "commit enqueued {orphans} orphan shadow_tracking row(s) with no backing message"
    );
    println!("✅ No orphan outbox rows after commit\n");

    // 4. CLEANUP
    ctx.cleanup().await?;
    println!("✅ Test PASSED: Committed Txn Leaves No Orphan Outbox Rows\n");
    Ok(())
}

/// The records a committed transaction forwards are found in kafka.messages at
/// commit time, not in worker memory filled at produce time (which a worker restart
/// would lose, so the transaction would commit locally and never reach the external
/// cluster). The records are produced while the topic is not yet shadowed, then
/// shadowing is enabled and the transaction committed: forwarding is decided at
/// commit time, so the records must arrive.
pub async fn test_committed_transaction_forwards_without_ram_buffer() -> TestResult {
    println!("=== Test: Committed Transaction Forwards Without The RAM Buffer ===\n");

    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("txn-no-buffer").await;
    let txn_id = format!("shadow-nobuf-{}", ctx.test_id);
    verify_external_kafka_ready().await?;

    let producer = create_transactional_producer(&txn_id)?;
    producer.init_transactions(Duration::from_secs(10))?;
    producer.begin_transaction()?;
    let key = b"txn-nobuf-key";
    let value = b"txn-nobuf-value";
    producer
        .send(
            FutureRecord::to(&topic).key(&key[..]).payload(&value[..]),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(e, _)| e)?;
    println!("Step 1: record produced in an open transaction (topic not shadowed yet)");

    enable_shadow_mode(
        ctx.db(),
        &topic,
        &ShadowTopicConfig {
            mode: ShadowMode::Shadow,
            forward_percentage: 100,
            external_topic_name: None,
            sync_mode: SyncMode::Async,
            write_mode: WriteMode::DualWrite,
        },
    )
    .await?;
    println!("Step 2: shadowing enabled for the topic");

    producer.commit_transaction(Duration::from_secs(10))?;
    println!("Step 3: transaction committed");

    assert_message_in_external_kafka(&topic, Some(key), Some(value), Duration::from_secs(20))
        .await?;
    println!("✅ Committed record reached external Kafka\n");

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// RssAnon of a process in bytes (the worker's private heap, not the shared buffers it
/// touches), if `/proc/<pid>/status` is readable.
fn rss_anon_bytes(pid: i32) -> Option<u64> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    let line = status.lines().find(|l| l.starts_with("RssAnon:"))?;
    let kb: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kb * 1024)
}

/// Records of the large transaction, on each of its two topics: a first part that warms
/// the worker up (plan and relation caches, allocator arenas), then the measured rest.
const LARGE_TXN_WARMUP_RECORDS: usize = 50_000;
const LARGE_TXN_RECORDS: usize = 250_000;
/// Records per topic between two samples of the worker's memory, each taken once every
/// record sent so far is acknowledged.
const LARGE_TXN_SAMPLE_EVERY: usize = 12_500;

/// The median sample of the measured records' second half may exceed the median of their
/// first half by at most this much. One sample against another is noise: what the
/// allocator keeps of the bursts of pipelined produce requests it has served moves a
/// sample by up to ~12 MB either way from one sample to the next, with no trend. Holding
/// a per-record forward buffer in RAM (about 50 MB over the measured 200,000 records of
/// each topic) moves the median of the second half (taken ~112,000 records after that of
/// the first) up by about 28 MB.
const LARGE_TXN_GROWTH_BOUND: u64 = 12 * 1024 * 1024;

/// How much the median of the last half of `samples` exceeds the median of the first half
/// (0 if it does not); the middle sample of an odd count is in neither.
fn median_growth(samples: &[u64]) -> Option<u64> {
    let half = samples.len() / 2;
    let median = |part: &[u64]| {
        let mut part = part.to_vec();
        part.sort_unstable();
        part.get(part.len() / 2).copied()
    };
    let first = median(&samples[..half])?;
    let last = median(&samples[samples.len() - half..])?;
    Some(last.saturating_sub(first))
}

/// A large committed transaction on two shadowed topics, one forwarding everything and
/// one half of its records (keyless records route by `offset % 100`, so the expected half
/// is known): every record the routing selects gets exactly one outbox row at the commit,
/// none other does, and the worker holds nothing per record while the transaction is
/// open (its records are read from kafka.messages at the commit).
pub async fn test_large_committed_transaction_is_enqueued_exactly_once() -> TestResult {
    println!("=== Test: a large committed transaction is enqueued exactly once ===\n");
    let ctx = TestContext::new().await?;
    let full = ctx.unique_topic("txn-large-full").await;
    let half = ctx.unique_topic("txn-large-half").await;
    let txn_id = format!("shadow-large-{}", ctx.test_id);
    verify_external_kafka_ready().await?;
    for (topic, forward_percentage) in [(&full, 100), (&half, 50)] {
        enable_shadow_mode(
            ctx.db(),
            topic,
            &ShadowTopicConfig {
                mode: ShadowMode::Shadow,
                forward_percentage,
                external_topic_name: None,
                sync_mode: SyncMode::Async,
                write_mode: WriteMode::DualWrite,
            },
        )
        .await?;
    }
    let pid = crate::worker::listener_pid(ctx.db())
        .await
        .map_err(|e| e.to_string())?
        .ok_or("no pg_kafka_listener")?;

    let producer: rdkafka::producer::FutureProducer = rdkafka::config::ClientConfig::new()
        .set("bootstrap.servers", crate::common::get_bootstrap_servers())
        .set("broker.address.family", "v4")
        .set("transactional.id", &txn_id)
        .set("linger.ms", "20")
        .set("queue.buffering.max.messages", "1000000")
        .set("message.timeout.ms", "120000")
        .set("transaction.timeout.ms", "300000")
        .create()?;
    producer.init_transactions(Duration::from_secs(10))?;
    producer.begin_transaction()?;
    println!("Step 1: {LARGE_TXN_RECORDS} keyed records to {full}, as many keyless to {half}...");
    let value = vec![b'v'; 32];
    let mut samples: Option<Vec<u64>> = Some(Vec::new());
    let mut sent: Vec<rdkafka::producer::DeliveryFuture> =
        Vec::with_capacity(2 * LARGE_TXN_SAMPLE_EVERY);
    for i in 0..=LARGE_TXN_RECORDS {
        if i >= LARGE_TXN_WARMUP_RECORDS && i % LARGE_TXN_SAMPLE_EVERY == 0 {
            for delivery in sent.drain(..) {
                delivery.await?.map_err(|(e, _)| e)?;
            }
            samples = samples.and_then(|mut s| {
                s.push(rss_anon_bytes(pid)?);
                Some(s)
            });
        }
        if i == LARGE_TXN_RECORDS {
            break;
        }
        let key = format!("{i:064}");
        sent.push(
            producer
                .send_result(FutureRecord::to(&full).key(&key).payload(&value))
                .map_err(|(e, _)| e)?,
        );
        sent.push(
            producer
                .send_result(FutureRecord::<(), _>::to(&half).payload(&value))
                .map_err(|(e, _)| e)?,
        );
    }
    println!("Step 2: committing...");
    producer.commit_transaction(Duration::from_secs(120))?;

    println!("=== Database Verification ===\n");
    for topic in [&full, &half] {
        // A record is selected when its topic forwards everything, or for the half topic
        // when `partition_offset % 100 < 50` (a keyless record routes by its offset).
        let row = ctx
            .db()
            .query_one(
                "WITH t AS (SELECT id, name = $2 AS sampled FROM kafka.topics WHERE name = $1),
                 sel AS (SELECT m.partition_id, m.partition_offset FROM kafka.messages m, t
                         WHERE m.topic_id = t.id AND m.txn_state IS NULL
                           AND (NOT t.sampled OR m.partition_offset % 100 < 50)),
                 enq AS (SELECT s.partition_id, s.local_offset FROM kafka.shadow_tracking s, t
                         WHERE s.topic_id = t.id)
                 SELECT (SELECT COUNT(*) FROM kafka.messages m, t
                           WHERE m.topic_id = t.id AND m.txn_state IS NULL),
                        (SELECT COUNT(*) FROM sel),
                        (SELECT COUNT(*) FROM enq),
                        (SELECT COUNT(*) FROM (SELECT * FROM sel EXCEPT SELECT * FROM enq) x),
                        (SELECT COUNT(*) FROM (SELECT * FROM enq EXCEPT SELECT * FROM sel) x)",
                &[topic, &half],
            )
            .await?;
        let (committed, selected, enqueued, missing, unexpected): (i64, i64, i64, i64, i64) =
            (row.get(0), row.get(1), row.get(2), row.get(3), row.get(4));
        println!(
            "{topic}: {committed} committed, {selected} selected, {enqueued} enqueued \
             ({missing} missing, {unexpected} unexpected)"
        );
        assert_eq!(committed, i64::try_from(LARGE_TXN_RECORDS)?);
        assert!(selected > 0);
        assert_eq!(
            (enqueued, missing, unexpected),
            (selected, 0, 0),
            "{topic}: exactly the records routing selects are enqueued, once each"
        );
    }
    match samples
        .as_deref()
        .and_then(|s| Some((s, median_growth(s)?)))
    {
        Some((samples, growth)) => {
            println!(
                "Worker RssAnon while the transaction was open, every \
                 {LARGE_TXN_SAMPLE_EVERY} records: {samples:?} (median +{growth})"
            );
            assert!(
                growth <= LARGE_TXN_GROWTH_BOUND,
                "the worker's median grew by {growth} bytes while the transaction was open \
                 ({samples:?})"
            );
        }
        None => println!("/proc/{pid}/status is not readable here; memory not checked"),
    }
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{median_growth, LARGE_TXN_GROWTH_BOUND, LARGE_TXN_SAMPLE_EVERY};

    const MB: u64 = 1_000_000;

    /// The 17 samples of the measured records with `per_sample` bytes held per sample
    /// and, at every other sample, `spike` bytes the allocator still keeps.
    fn samples(per_sample: u64, spike: u64) -> Vec<u64> {
        (0..17u64)
            .map(|i| 40 * MB + i * per_sample + if i % 2 == 0 { spike } else { 0 })
            .collect()
    }

    /// Noise of 12 MB at every other sample, with no trend, passes; a per-record buffer of
    /// 50 MB over the measured 200,000 records fails, with or without that noise.
    #[test]
    fn the_median_growth_tells_a_per_record_buffer_from_noise() {
        let leak_per_sample = 50 * MB * u64::try_from(LARGE_TXN_SAMPLE_EVERY).unwrap() / 200_000;
        for spike in [0, 12 * MB] {
            assert!(median_growth(&samples(0, spike)).unwrap() <= LARGE_TXN_GROWTH_BOUND);
            assert!(
                median_growth(&samples(leak_per_sample, spike)).unwrap() > LARGE_TXN_GROWTH_BOUND
            );
        }
        // Noise that lowers the first half's samples instead.
        let mut dips = samples(0, 0);
        for s in dips.iter_mut().take(8).step_by(2) {
            *s -= 12 * MB;
        }
        assert!(median_growth(&dips).unwrap() <= LARGE_TXN_GROWTH_BOUND);
    }
}
