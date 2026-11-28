//! Replay engine tests for shadow mode
//!
//! Tests for replaying historical messages to external Kafka.

use crate::common::{create_db_client, create_producer, create_transactional_producer, TestResult};
use crate::setup::TestContext;
use rdkafka::producer::{FutureRecord, Producer};
use std::time::{Duration, Instant};

use super::assertions::{assert_external_message_count, assert_message_in_external_kafka};
use super::external_client::verify_external_kafka_ready;
use super::helpers::{enable_shadow_mode, ShadowMode, ShadowTopicConfig, SyncMode, WriteMode};

/// Test replaying historical messages to external Kafka
///
/// 1. Produces messages with shadow mode disabled (LocalOnly)
/// 2. Enables shadow mode
/// 3. Triggers replay of historical messages
/// 4. Verifies messages appear in external Kafka
pub async fn test_replay_historical_messages() -> TestResult {
    println!("=== Test: Replay Historical Messages ===\n");

    // 1. SETUP
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("replay-test").await;
    let message_count = 5;

    println!("Step 1: Producing messages with shadow mode DISABLED (LocalOnly)...");

    // First, enable LocalOnly mode (no forwarding)
    enable_shadow_mode(
        ctx.db(),
        &topic,
        &ShadowTopicConfig {
            mode: ShadowMode::LocalOnly,
            forward_percentage: 100,
            external_topic_name: None,
            sync_mode: SyncMode::Sync,
            write_mode: WriteMode::DualWrite,
        },
    )
    .await?;

    let producer = create_producer()?;
    for i in 0..message_count {
        let key = format!("historical-key-{i}");
        let value = format!("historical-value-{i}");
        producer
            .send(
                FutureRecord::to(&topic)
                    .key(key.as_bytes())
                    .payload(value.as_bytes()),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(e, _)| e)?;
    }
    println!("✅ {message_count} historical messages produced (not forwarded)\n");

    // 2. Enable shadow mode
    println!("Step 2: Verifying external Kafka is ready...");
    verify_external_kafka_ready().await?;
    println!("✅ External Kafka ready\n");

    println!("Step 3: Enabling shadow mode for future messages...");
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

    // 3. Trigger replay
    println!("Step 4: Triggering replay of historical messages...");

    // Get topic_id
    let row = ctx
        .db()
        .query_one("SELECT id FROM kafka.topics WHERE name = $1", &[&topic])
        .await?;
    let topic_id: i32 = row.get(0);

    let queued: i64 = ctx
        .db()
        .query_one(
            "SELECT kafka.replay_shadow_messages($1, 0, NULL)",
            &[&topic_id],
        )
        .await?
        .get(0);
    assert_eq!(
        queued,
        i64::try_from(message_count)?,
        "replay re-queues every historical record"
    );
    println!("✅ Replay queued {queued} records\n");

    // 4. VERIFY
    println!("=== Verification ===\n");
    tokio::time::sleep(Duration::from_secs(2)).await;

    println!("Checking external Kafka for replayed messages...");
    assert_external_message_count(&topic, message_count, Duration::from_secs(15)).await?;
    println!("✅ All {message_count} historical messages replayed to external Kafka\n");

    // 5. CLEANUP
    ctx.cleanup().await?;
    println!("✅ Test PASSED: Replay Historical Messages\n");
    Ok(())
}

/// Test that replay skips aborted (and uncommitted) records (RA-6)
///
/// `kafka.replay_shadow_messages` filters on `txn_state IS NULL` so a replay over
/// a range that includes an aborted transactional record does NOT forward that
/// record to the external broker — only the committed/non-transactional ones.
pub async fn test_replay_skips_aborted_records() -> TestResult {
    println!("=== Test: Replay Skips Aborted Records (RA-6) ===\n");

    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("replay-aborted").await;
    let txn_id = format!("replay-abort-{}", ctx.test_id);

    verify_external_kafka_ready().await?;

    // Produce with shadow OFF (LocalOnly) so nothing forwards until we replay.
    enable_shadow_mode(
        ctx.db(),
        &topic,
        &ShadowTopicConfig {
            mode: ShadowMode::LocalOnly,
            forward_percentage: 100,
            external_topic_name: None,
            sync_mode: SyncMode::Sync,
            write_mode: WriteMode::DualWrite,
        },
    )
    .await?;

    // 1. A committed, non-transactional record (txn_state NULL).
    let producer = create_producer()?;
    let committed_key = b"committed-key";
    let committed_value = b"committed-value";
    producer
        .send(
            FutureRecord::to(&topic)
                .key(&committed_key[..])
                .payload(&committed_value[..]),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(e, _)| e)?;

    // 2. An ABORTED transactional record (persists with txn_state='aborted').
    let txn_producer = create_transactional_producer(&txn_id)?;
    txn_producer.init_transactions(Duration::from_secs(10))?;
    txn_producer.begin_transaction()?;
    txn_producer
        .send(
            FutureRecord::to(&topic)
                .key(&b"aborted-key"[..])
                .payload(&b"aborted-value"[..]),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(e, _)| e)?;
    txn_producer.abort_transaction(Duration::from_secs(10))?;
    println!("✅ Produced 1 committed + 1 aborted record\n");

    // 3. Enable shadow + replay the whole range.
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

    let row = ctx
        .db()
        .query_one("SELECT id FROM kafka.topics WHERE name = $1", &[&topic])
        .await?;
    let topic_id: i32 = row.get(0);
    ctx.db()
        .execute(
            "SELECT kafka.replay_shadow_messages($1, 0, NULL)",
            &[&topic_id],
        )
        .await?;
    println!("✅ Replay invoked\n");

    // 4. VERIFY: only the committed record reaches external Kafka.
    assert_message_in_external_kafka(
        &topic,
        Some(committed_key),
        Some(committed_value),
        Duration::from_secs(15),
    )
    .await?;
    assert_external_message_count(&topic, 1, Duration::from_secs(5)).await?;
    println!("✅ Only the committed record was replayed (aborted record skipped)\n");

    ctx.cleanup().await?;
    println!("✅ Test PASSED: Replay Skips Aborted Records\n");
    Ok(())
}

/// `kafka.replay_shadow_messages` re-queues its rows with one INSERT .. ON CONFLICT DO
/// UPDATE and holds them until its caller commits. The worker's outbox tick (the
/// forward acks of rows in flight, then the claim of due rows) waited for such a row up
/// to `pg_kafka.worker_lock_timeout_ms` (5 s) per statement, at every tick while the
/// replay stayed open, and the whole DB thread with it: every client's requests stood.
///
/// The claim and the acks also went over every partition in one statement each, so a
/// held row failed the whole tick: no topic was forwarded while the replay stayed open,
/// and the failed ticks backed off exponentially (up to 30 s), so forwarding resumed up
/// to 30 s after the replay ended. A held row now stops only its own partition at that
/// row, and a tick that found rows held does not back off.
///
/// Asserts: a shadow topic with 200 forwarded records; one replay of all of them
/// committed (the rows due again) and a second held open in another session (holding
/// them, as the claim or the acks reach them); meanwhile five Produce requests to
/// another shadow topic, 300 ms apart, are each answered within 1 s, and that topic's
/// records are forwarded within 5 s while the replay is still open (it stays open
/// about 8 s); after the replay rolls back, the first topic's rows are forwarded again
/// within 3 s.
pub async fn test_replay_held_open_does_not_stall_the_db_thread() -> TestResult {
    println!("=== Test: an open replay does not stall the DB thread ===\n");
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("replay-held").await;
    let other = ctx.unique_topic("replay-held-other").await;
    let records = 200;
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
    enable_shadow_mode(
        ctx.db(),
        &other,
        &ShadowTopicConfig {
            mode: ShadowMode::Shadow,
            forward_percentage: 100,
            external_topic_name: None,
            sync_mode: SyncMode::Async,
            write_mode: WriteMode::DualWrite,
        },
    )
    .await?;
    let topic_id: i32 = ctx
        .db()
        .query_one("SELECT id FROM kafka.topics WHERE name = $1", &[&topic])
        .await?
        .get(0);
    let other_id: i32 = ctx
        .db()
        .query_one("SELECT id FROM kafka.topics WHERE name = $1", &[&other])
        .await?
        .get(0);

    let producer = create_producer()?;
    for i in 0..records {
        producer
            .send(
                FutureRecord::to(&topic)
                    .key(format!("k{i}").as_bytes())
                    .payload(format!("v{i}").as_bytes()),
                Duration::from_secs(10),
            )
            .await
            .map_err(|(e, _)| e)?;
    }
    // Warm the other topic up (creation, metadata) before the replay holds anything.
    producer
        .send(
            FutureRecord::to(&other).key("warm").payload("up"),
            Duration::from_secs(10),
        )
        .await
        .map_err(|(e, _)| e)?;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let forwarded: i64 = ctx
            .db()
            .query_one(
                "SELECT count(*) FROM kafka.shadow_tracking
                 WHERE topic_id = $1 AND external_offset IS NOT NULL",
                &[&topic_id],
            )
            .await?
            .get(0);
        if forwarded >= records {
            break;
        }
        if Instant::now() >= deadline {
            return Err(format!("only {forwarded} of {records} records forwarded").into());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    println!("Step 1: {records} records produced to {topic} and forwarded");

    let holder = create_db_client().await?;
    holder
        .batch_execute("SET statement_timeout = '30s'")
        .await?;
    ctx.db()
        .query_one(
            "SELECT kafka.replay_shadow_messages($1, 0, NULL)",
            &[&topic_id],
        )
        .await?;
    holder.batch_execute("BEGIN").await?;
    let held: i64 = holder
        .query_one(
            "SELECT kafka.replay_shadow_messages($1, 0, NULL)",
            &[&topic_id],
        )
        .await?
        .get(0);
    println!("Step 2: one replay committed; a second re-queued {held} rows and holds them");

    let mut slowest = Duration::ZERO;
    for i in 0..5 {
        tokio::time::sleep(Duration::from_millis(300)).await;
        let started = Instant::now();
        producer
            .send(
                FutureRecord::to(&other)
                    .key(format!("probe{i}").as_bytes())
                    .payload("x"),
                Duration::from_secs(30),
            )
            .await
            .map_err(|(e, _)| e)?;
        let took = started.elapsed();
        println!("   Produce {i} to {other} answered in {took:?}");
        slowest = slowest.max(took);
    }
    // The other topic keeps forwarding while the replay holds the first topic's rows.
    let forwarding = async {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let pending: i64 = ctx
                .db()
                .query_one(
                    "SELECT count(*) FROM kafka.shadow_tracking
                     WHERE topic_id = $1 AND external_offset IS NULL",
                    &[&other_id],
                )
                .await?
                .get(0);
            if pending == 0 {
                return Ok::<bool, Box<dyn std::error::Error>>(true);
            }
            if Instant::now() >= deadline {
                println!("   {pending} record(s) of {other} still not forwarded");
                return Ok(false);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    .await?;
    // Keep the replay open long enough for an exponential backoff to grow well past
    // the 3 s the resumption below is given.
    tokio::time::sleep(Duration::from_secs(3)).await;
    holder.batch_execute("ROLLBACK").await?;
    let released = Instant::now();
    println!("Step 3: the replay rolled back; slowest Produce {slowest:?}");
    if slowest >= Duration::from_secs(1) {
        return Err(format!(
            "a Produce to another topic waited {slowest:?} while a replay held the outbox rows"
        )
        .into());
    }
    if !forwarding {
        return Err(
            format!("{other} was not forwarded while a replay held rows of {topic} only").into(),
        );
    }
    println!("   {other} was forwarded while the replay was open");

    let deadline = released + Duration::from_secs(3);
    loop {
        let pending: i64 = ctx
            .db()
            .query_one(
                "SELECT count(*) FROM kafka.shadow_tracking
                 WHERE topic_id = $1 AND external_offset IS NULL",
                &[&topic_id],
            )
            .await?
            .get(0);
        if pending == 0 {
            break;
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "{pending} replayed rows not forwarded within 3 s of the replay's end"
            )
            .into());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    println!(
        "Step 4: the committed replay's rows were forwarded again {:?} after its end",
        released.elapsed()
    );
    ctx.cleanup().await?;
    println!("✅ the DB thread kept answering while the replay held its rows");
    Ok(())
}
