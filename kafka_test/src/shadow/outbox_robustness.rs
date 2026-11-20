//! Shadow tests of the outbox under stress and failure:
//! - `pg_kafka.shadow_max_retries` refuses 0 (every record gets an attempt);
//! - one topic's backlog does not starve another topic's records;
//! - a full librdkafka queue holds back only the records behind it, not the
//!   sync-mode response timeouts.

use crate::common::{create_db_client, create_producer, TestResult};
use crate::setup::TestContext;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::{Duration, Instant};
use tokio_postgres::Client;

use super::external_client::{get_internal_bootstrap_servers, verify_external_kafka_ready};
use super::helpers::{enable_shadow_mode, ShadowMode, ShadowTopicConfig, SyncMode, WriteMode};

fn config(mode: ShadowMode, sync_mode: SyncMode) -> ShadowTopicConfig {
    ShadowTopicConfig {
        mode,
        forward_percentage: 100,
        external_topic_name: None,
        sync_mode,
        write_mode: WriteMode::DualWrite,
    }
}

async fn alter_system(db: &Client, sql: &str) -> TestResult {
    db.execute(sql, &[]).await?;
    db.execute("SELECT pg_reload_conf()", &[]).await?;
    // The worker reloads on SIGHUP within ~100 ms; the forwarder reads the
    // reloaded config on its next record or settings check (~0.5 s).
    tokio::time::sleep(Duration::from_millis(700)).await;
    Ok(())
}

async fn restore_bootstrap_servers(db: &Client) -> TestResult {
    alter_system(
        db,
        &format!(
            "ALTER SYSTEM SET pg_kafka.shadow_bootstrap_servers = '{}'",
            get_internal_bootstrap_servers()
        ),
    )
    .await
}

/// A pg_kafka producer for large or many records.
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

async fn topic_id(db: &Client, topic: &str) -> Result<i32, Box<dyn std::error::Error>> {
    Ok(db
        .query_one("SELECT id FROM kafka.topics WHERE name = $1", &[&topic])
        .await?
        .get(0))
}

/// Outbox rows of a topic not yet delivered.
async fn undelivered(db: &Client, topic_id: i32) -> Result<i64, Box<dyn std::error::Error>> {
    Ok(db
        .query_one(
            "SELECT COUNT(*) FROM kafka.shadow_tracking \
             WHERE topic_id = $1 AND external_offset IS NULL",
            &[&topic_id],
        )
        .await?
        .get(0))
}

/// `shadow_max_retries = 0` would leave every record un-forwarded (a row is
/// claimed only while its retry count is below the cap): the GUC refuses it.
pub async fn test_shadow_max_retries_refuses_zero() -> TestResult {
    println!("=== Test: shadow_max_retries refuses 0 ===\n");
    let db = create_db_client().await?;
    let zero = db
        .execute("ALTER SYSTEM SET pg_kafka.shadow_max_retries = 0", &[])
        .await;
    let one = db
        .execute("ALTER SYSTEM SET pg_kafka.shadow_max_retries = 1", &[])
        .await;
    db.execute("ALTER SYSTEM RESET pg_kafka.shadow_max_retries", &[])
        .await?;
    db.execute("SELECT pg_reload_conf()", &[]).await?;
    match zero {
        Err(e) => println!("  0 refused: {e}"),
        Ok(_) => {
            return Err("ALTER SYSTEM SET pg_kafka.shadow_max_retries = 0 was accepted".into())
        }
    }
    one.map_err(|e| format!("shadow_max_retries = 1 must be accepted: {e}"))?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// A topic with a large outbox backlog (here a replay) shares each claim with
/// the other partitions: another topic's new record is forwarded while most of
/// the backlog is still waiting, not after it.
pub async fn test_shadow_backlog_does_not_starve_other_topics() -> TestResult {
    const BACKLOG: usize = 30_000;
    println!("=== Test: a backlog does not starve other topics ===\n");
    let ctx = TestContext::new().await?;
    // Created first: the lower topic_id, which a claim in row-key order serves first.
    let backlog_topic = ctx.unique_topic("shadow-backlog").await;
    let other_topic = ctx.unique_topic("shadow-not-starved").await;
    verify_external_kafka_ready().await?;
    enable_shadow_mode(
        ctx.db(),
        &backlog_topic,
        &config(ShadowMode::LocalOnly, SyncMode::Async),
    )
    .await?;

    println!("Step 1: {BACKLOG} records stored locally, not forwarded...");
    let producer = bulk_producer()?;
    let payload = [b'b'; 100];
    let mut sends = Vec::with_capacity(BACKLOG);
    for i in 0..BACKLOG {
        let key = format!("k{i}");
        let record = FutureRecord::to(&backlog_topic)
            .key(key.as_bytes())
            .payload(&payload[..])
            .partition(0);
        sends.push(producer.send_result(record).map_err(|(e, _)| e)?);
    }
    for send in sends {
        send.await?.map_err(|(e, _)| e)?;
    }

    enable_shadow_mode(
        ctx.db(),
        &backlog_topic,
        &config(ShadowMode::Shadow, SyncMode::Async),
    )
    .await?;
    enable_shadow_mode(
        ctx.db(),
        &other_topic,
        &config(ShadowMode::Shadow, SyncMode::Async),
    )
    .await?;
    let backlog_id = topic_id(ctx.db(), &backlog_topic).await?;
    let other_id = topic_id(ctx.db(), &other_topic).await?;
    assert!(backlog_id < other_id);

    println!("Step 2: replay the backlog, then produce one record to the other topic...");
    let queued: i64 = ctx
        .db()
        .query_one(
            "SELECT kafka.replay_shadow_messages($1, 0, NULL)",
            &[&backlog_id],
        )
        .await?
        .get(0);
    assert_eq!(queued, i64::try_from(BACKLOG)?);
    let single = create_producer()?;
    single
        .send(
            FutureRecord::to(&other_topic)
                .key("k")
                .payload("not starved")
                .partition(0),
            Duration::from_secs(10),
        )
        .await
        .map_err(|(e, _)| e)?;

    let start = Instant::now();
    let backlog_left = loop {
        if undelivered(ctx.db(), other_id).await? == 0 {
            break undelivered(ctx.db(), backlog_id).await?;
        }
        if start.elapsed() > Duration::from_secs(120) {
            return Err("the other topic's record was not forwarded within 120 s".into());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    println!(
        "  other topic forwarded after {:?} with {backlog_left}/{BACKLOG} backlog rows still pending",
        start.elapsed()
    );
    assert!(
        backlog_left >= i64::try_from(BACKLOG / 2)?,
        "the other topic's record waited behind the backlog: only {backlog_left} of \
         {BACKLOG} backlog rows were still pending when it was forwarded"
    );

    // The backlog drains, every record once.
    let start = Instant::now();
    while undelivered(ctx.db(), backlog_id).await? > 0 {
        if start.elapsed() > Duration::from_secs(180) {
            return Err("the backlog did not drain within 180 s".into());
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// With the external cluster unreachable, librdkafka's local queue
/// (`queue.buffering.max.kbytes`, 64 MiB) fills up. The records behind it wait
/// in the forwarder's backlog while the forwarder keeps releasing held
/// sync-mode responses at their timeout.
pub async fn test_shadow_full_producer_queue_does_not_block_sync_timeouts() -> TestResult {
    const BIG: usize = 80;
    const RECORD_BYTES: usize = 900_000;
    println!("=== Test: a full producer queue does not block sync timeouts ===\n");
    let ctx = TestContext::new().await?;
    let big_topic = ctx.unique_topic("shadow-queue-full").await;
    let sync_topic = ctx.unique_topic("shadow-sync-behind").await;
    verify_external_kafka_ready().await?;
    enable_shadow_mode(
        ctx.db(),
        &big_topic,
        &config(ShadowMode::Shadow, SyncMode::Async),
    )
    .await?;
    enable_shadow_mode(
        ctx.db(),
        &sync_topic,
        &config(ShadowMode::Shadow, SyncMode::Sync),
    )
    .await?;
    let db = create_db_client().await?;
    alter_system(
        &db,
        "ALTER SYSTEM SET pg_kafka.shadow_bootstrap_servers = '127.0.0.1:1'",
    )
    .await?;
    alter_system(
        &db,
        "ALTER SYSTEM SET pg_kafka.shadow_sync_timeout_ms = 3000",
    )
    .await?;

    let result = async {
        println!("Step 1: {BIG} records of {RECORD_BYTES} bytes (more than the queue holds)...");
        let producer = bulk_producer()?;
        let payload = vec![b'q'; RECORD_BYTES];
        for i in 0..BIG {
            producer
                .send(
                    FutureRecord::<(), _>::to(&big_topic)
                        .payload(&payload[..])
                        .partition(0),
                    Duration::from_secs(30),
                )
                .await
                .map_err(|(e, _)| format!("big record {i}: {e}"))?;
        }
        // Let the outbox dispatch them all to the forwarder.
        tokio::time::sleep(Duration::from_secs(2)).await;

        println!("Step 2: a sync-mode produce behind them...");
        let sync_producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", crate::common::get_bootstrap_servers())
            .set("broker.address.family", "v4")
            .set("message.timeout.ms", "30000")
            .create()?;
        let start = Instant::now();
        let sent = sync_producer
            .send(
                FutureRecord::to(&sync_topic).key("k").payload("held"),
                Duration::from_secs(30),
            )
            .await;
        let held_for = start.elapsed();
        println!("  sync produce answered after {held_for:?}");
        sent.map_err(|(e, _)| format!("sync produce failed after {held_for:?}: {e}"))?;
        assert!(
            held_for < Duration::from_secs(8),
            "the sync response must be released at shadow_sync_timeout_ms (3 s), \
             was held {held_for:?}"
        );
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await
    .map_err(|e| e.to_string());

    // Delete the topics (and their outbox rows) before the real cluster is back.
    ctx.cleanup().await?;
    alter_system(&db, "ALTER SYSTEM RESET pg_kafka.shadow_sync_timeout_ms").await?;
    restore_bootstrap_servers(&db).await?;
    result?;
    println!("✅ Test PASSED\n");
    Ok(())
}
