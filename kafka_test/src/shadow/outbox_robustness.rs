//! Shadow tests of the outbox under stress and failure:
//! - `pg_kafka.shadow_max_retries` refuses 0 (every record gets an attempt);
//! - one topic's backlog does not starve another topic's records;
//! - a full librdkafka queue holds back only the records behind it, not the
//!   sync-mode response timeouts;
//! - the transaction-timeout sweep drops the forward buffers of the
//!   transactions it aborts;
//! - an ERROR while applying forward acks loses no ack (no lost-attempt count,
//!   no duplicate delivery);
//! - a failed COMMIT of the transaction that applied forward acks loses no
//!   ack either;
//! - partitions with nothing due take no share of a claim, and a backlog
//!   drains claim after claim.

use crate::common::{create_db_client, create_producer, TestResult};
use crate::setup::TestContext;
use crate::worker::ServerLog;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use std::time::{Duration, Instant};
use tokio_postgres::Client;

use super::external_client::{
    count_external_messages, get_internal_bootstrap_servers, verify_external_kafka_ready,
};
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

/// A transaction left open by a producer that went away is aborted by the
/// timeout sweep; the sweep also drops the records the shadow store buffered
/// for it (they can never be committed), instead of keeping them for the life
/// of the worker. Needs the server log (the listener's stderr as a file).
pub async fn test_shadow_timed_out_transaction_buffer_is_dropped() -> TestResult {
    println!("=== Test: the timeout sweep drops an abandoned transaction's buffer ===\n");
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("shadow-txn-abandoned").await;
    verify_external_kafka_ready().await?;
    enable_shadow_mode(
        ctx.db(),
        &topic,
        &config(ShadowMode::Shadow, SyncMode::Async),
    )
    .await?;
    let log = ServerLog::open(ctx.db())
        .await
        .map_err(|e| e.to_string())?
        .ok_or("the server log is not readable (listener stderr is not a regular file)")?;
    let log_from = log.end();

    println!("Step 1: a transactional produce, never committed (transaction.timeout.ms=2000)...");
    let txn_id = format!("shadow-abandoned-{}", uuid::Uuid::new_v4());
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", crate::common::get_bootstrap_servers())
        .set("broker.address.family", "v4")
        .set("message.timeout.ms", "2000")
        .set("transaction.timeout.ms", "2000")
        .set("transactional.id", &txn_id)
        .create()?;
    producer.init_transactions(Duration::from_secs(10))?;
    producer.begin_transaction()?;
    producer
        .send(
            FutureRecord::to(&topic).key("k").payload("abandoned"),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(e, _)| e)?;

    println!("Step 2: wait for the sweep (every 10 s) to abort it...");
    let needle = "forward buffer(s) of transactions that are no longer open";
    let start = Instant::now();
    loop {
        if log.since(log_from).contains(needle) {
            break;
        }
        if start.elapsed() > Duration::from_secs(40) {
            drop(producer);
            ctx.cleanup().await?;
            return Err(format!(
                "no \"{needle}\" in the server log within 40 s: the sweep kept the buffer"
            )
            .into());
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let state: String = ctx
        .db()
        .query_one(
            "SELECT state FROM kafka.transactions WHERE transactional_id = $1",
            &[&txn_id],
        )
        .await?
        .get(0);
    assert_eq!(state, "CompleteAbort");
    println!("  buffer dropped after {:?}", start.elapsed());

    drop(producer);
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// An ERROR while the worker applies a delivery ack (here a trigger that
/// refuses to mark the row delivered) rolls the application back. The ack is
/// applied again on a later tick: the row is delivered without a lost attempt
/// counted against it and without being forwarded twice.
pub async fn test_shadow_ack_application_error_loses_no_ack() -> TestResult {
    println!("=== Test: an ERROR applying acks loses no ack ===\n");
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("shadow-ack-error").await;
    verify_external_kafka_ready().await?;
    enable_shadow_mode(
        ctx.db(),
        &topic,
        &config(ShadowMode::Shadow, SyncMode::Async),
    )
    .await?;
    let id = topic_id(ctx.db(), &topic).await?;
    let trigger = format!("kt_refuse_ack_{id}");
    ctx.db()
        .batch_execute(&format!(
            "CREATE FUNCTION public.{trigger}() RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN
                 IF NEW.topic_id = {id} AND NEW.external_offset IS NOT NULL
                    AND OLD.external_offset IS NULL THEN
                     RAISE EXCEPTION 'kafka_test: delivery ack refused';
                 END IF;
                 RETURN NEW;
             END $$;
             CREATE TRIGGER {trigger} BEFORE UPDATE ON kafka.shadow_tracking
                 FOR EACH ROW EXECUTE FUNCTION public.{trigger}();"
        ))
        .await?;

    let result = async {
        println!("Step 1: produce one record; its delivery ack fails to apply...");
        let producer = create_producer()?;
        producer
            .send(
                FutureRecord::to(&topic).key("k").payload("once"),
                Duration::from_secs(10),
            )
            .await
            .map_err(|(e, _)| e)?;
        tokio::time::sleep(Duration::from_secs(4)).await;
        let (delivered, retries): (bool, i32) = {
            let row = ctx
                .db()
                .query_one(
                    "SELECT external_offset IS NOT NULL, retry_count \
                     FROM kafka.shadow_tracking WHERE topic_id = $1",
                    &[&id],
                )
                .await?;
            (row.get(0), row.get(1))
        };
        assert!(!delivered, "the trigger refuses the delivered mark");
        println!("  while refused: retry_count={retries}");

        println!("Step 2: drop the trigger; the kept ack is applied...");
        ctx.db()
            .batch_execute(&format!("DROP TRIGGER {trigger} ON kafka.shadow_tracking"))
            .await?;
        let start = Instant::now();
        while undelivered(ctx.db(), id).await? > 0 {
            if start.elapsed() > Duration::from_secs(60) {
                return Err("the row was not delivered within 60 s".into());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let retry_count: i32 = ctx
            .db()
            .query_one(
                "SELECT retry_count FROM kafka.shadow_tracking WHERE topic_id = $1",
                &[&id],
            )
            .await?
            .get(0);
        assert_eq!(
            retry_count, 0,
            "the ack was lost: its attempt was counted as failed and re-sent"
        );
        let external = count_external_messages(&topic, Duration::from_secs(5)).await?;
        assert_eq!(external, 1, "forwarded {external} times");
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await
    .map_err(|e| e.to_string());

    if let Err(e) = ctx
        .db()
        .batch_execute(&format!(
            "DROP TRIGGER IF EXISTS {trigger} ON kafka.shadow_tracking; \
             DROP FUNCTION IF EXISTS public.{trigger}()"
        ))
        .await
    {
        eprintln!("⚠️  cleanup: failed to drop trigger {trigger}: {e}");
    }
    ctx.cleanup().await?;
    result?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// The COMMIT of the outbox tick that applied a delivery ack fails (here a deferred
/// constraint trigger refuses the delivered mark at commit): the ack application is
/// rolled back after it ran. The ack is applied again on a later tick, and meanwhile
/// its row stays in flight: no lost attempt is counted and the record is forwarded once.
pub async fn test_shadow_failed_tick_commit_loses_no_ack() -> TestResult {
    const RECORDS: usize = 5;
    println!("=== Test: a failed COMMIT of the ack application loses no ack ===\n");
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("shadow-ack-commit").await;
    verify_external_kafka_ready().await?;
    enable_shadow_mode(
        ctx.db(),
        &topic,
        &config(ShadowMode::Shadow, SyncMode::Async),
    )
    .await?;
    let id = topic_id(ctx.db(), &topic).await?;
    let trigger = format!("kt_refuse_ack_commit_{id}");
    ctx.db()
        .batch_execute(&format!(
            "CREATE FUNCTION public.{trigger}() RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN
                 IF NEW.topic_id = {id} AND NEW.external_offset IS NOT NULL
                    AND OLD.external_offset IS NULL THEN
                     RAISE EXCEPTION 'kafka_test: delivery ack refused at commit';
                 END IF;
                 RETURN NULL;
             END $$;
             CREATE CONSTRAINT TRIGGER {trigger} AFTER UPDATE ON kafka.shadow_tracking
                 DEFERRABLE INITIALLY DEFERRED
                 FOR EACH ROW EXECUTE FUNCTION public.{trigger}();"
        ))
        .await?;

    let result = async {
        println!(
            "Step 1: produce {RECORDS} records; every tick applying their acks fails at COMMIT..."
        );
        let producer = create_producer()?;
        for i in 0..RECORDS {
            producer
                .send(
                    FutureRecord::to(&topic)
                        .key(format!("k{i}").as_str())
                        .payload("once")
                        .partition(0),
                    Duration::from_secs(10),
                )
                .await
                .map_err(|(e, _)| e)?;
        }
        tokio::time::sleep(Duration::from_secs(4)).await;
        let row = ctx
            .db()
            .query_one(
                "SELECT COUNT(*) FILTER (WHERE external_offset IS NOT NULL), \
                        COALESCE(MAX(retry_count), 0) \
                 FROM kafka.shadow_tracking WHERE topic_id = $1",
                &[&id],
            )
            .await?;
        let (delivered, retries): (i64, i32) = (row.get(0), row.get(1));
        assert_eq!(
            delivered, 0,
            "the trigger refuses the delivered mark at commit"
        );
        println!("  while refused: max retry_count={retries}");

        println!("Step 2: drop the trigger; the kept acks are applied...");
        ctx.db()
            .batch_execute(&format!("DROP TRIGGER {trigger} ON kafka.shadow_tracking"))
            .await?;
        let start = Instant::now();
        while undelivered(ctx.db(), id).await? > 0 {
            if start.elapsed() > Duration::from_secs(60) {
                return Err("the rows were not delivered within 60 s".into());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let max_retries: i32 = ctx
            .db()
            .query_one(
                "SELECT MAX(retry_count) FROM kafka.shadow_tracking WHERE topic_id = $1",
                &[&id],
            )
            .await?
            .get(0);
        assert_eq!(
            max_retries, 0,
            "an ack was lost: its attempt was counted as failed and re-sent"
        );
        let external = count_external_messages(&topic, Duration::from_secs(5)).await?;
        assert_eq!(
            external, RECORDS,
            "forwarded {external} records for {RECORDS}"
        );
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await
    .map_err(|e| e.to_string());

    if let Err(e) = ctx
        .db()
        .batch_execute(&format!(
            "DROP TRIGGER IF EXISTS {trigger} ON kafka.shadow_tracking; \
             DROP FUNCTION IF EXISTS public.{trigger}()"
        ))
        .await
    {
        eprintln!("⚠️  cleanup: failed to drop trigger {trigger}: {e}");
    }
    ctx.cleanup().await?;
    result?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// Wait until no outbox row of `topic_id` is undelivered; the time it took.
async fn drained_within(
    db: &Client,
    topic_id: i32,
    limit: Duration,
) -> Result<Duration, Box<dyn std::error::Error>> {
    let start = Instant::now();
    loop {
        let left = undelivered(db, topic_id).await?;
        if left == 0 {
            return Ok(start.elapsed());
        }
        if start.elapsed() > limit {
            return Err(format!("{left} row(s) still undelivered after {limit:?}").into());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Partitions whose pending rows cannot be claimed (here 500 orphan pointers whose
/// messages are gone; dead letters and rows in flight or backing off are alike) take
/// no share of a claim, so a replayed backlog on another topic still drains at full
/// claims, not a few rows per claim. And a backlog drains claim after claim: with
/// `pg_kafka.shadow_batch_size = 10` the worker does not wait for requests between
/// claims that leave rows due.
pub async fn test_shadow_stuck_partitions_do_not_throttle_a_backlog() -> TestResult {
    const BACKLOG: usize = 2_000;
    const STUCK_PARTITIONS: i32 = 500;
    println!("=== Test: stuck partitions do not throttle a backlog ===\n");
    let ctx = TestContext::new().await?;
    let stuck_topic = ctx.unique_topic("shadow-stuck").await;
    let backlog_topic = ctx.unique_topic("shadow-unthrottled").await;
    verify_external_kafka_ready().await?;
    enable_shadow_mode(
        ctx.db(),
        &stuck_topic,
        &config(ShadowMode::LocalOnly, SyncMode::Async),
    )
    .await?;
    enable_shadow_mode(
        ctx.db(),
        &backlog_topic,
        &config(ShadowMode::LocalOnly, SyncMode::Async),
    )
    .await?;
    let stuck_id = topic_id(ctx.db(), &stuck_topic).await?;
    let backlog_id = topic_id(ctx.db(), &backlog_topic).await?;
    let db = create_db_client().await?;

    let result = async {
        println!("Step 1: {BACKLOG} records stored locally, not forwarded...");
        let producer = bulk_producer()?;
        let mut sends = Vec::with_capacity(BACKLOG);
        for i in 0..BACKLOG {
            let key = format!("k{i}");
            let record = FutureRecord::to(&backlog_topic)
                .key(key.as_bytes())
                .payload("backlog")
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

        println!(
            "Step 2: {STUCK_PARTITIONS} partitions with only orphan outbox rows, then replay..."
        );
        ctx.db()
            .execute(
                "INSERT INTO kafka.shadow_tracking (topic_id, partition_id, local_offset) \
                 SELECT $1, g, 1000000 FROM generate_series(0, $2 - 1) AS g",
                &[&stuck_id, &STUCK_PARTITIONS],
            )
            .await?;
        ctx.db()
            .query_one(
                "SELECT kafka.replay_shadow_messages($1, 0, NULL)",
                &[&backlog_id],
            )
            .await?;
        let took = drained_within(ctx.db(), backlog_id, Duration::from_secs(20)).await?;
        println!("  drained next to {STUCK_PARTITIONS} stuck partitions in {took:?}");
        ctx.db()
            .execute(
                "DELETE FROM kafka.shadow_tracking WHERE topic_id = $1",
                &[&stuck_id],
            )
            .await?;

        println!("Step 3: shadow_batch_size = 10, replay again...");
        alter_system(&db, "ALTER SYSTEM SET pg_kafka.shadow_batch_size = 10").await?;
        ctx.db()
            .query_one(
                "SELECT kafka.replay_shadow_messages($1, 0, NULL)",
                &[&backlog_id],
            )
            .await?;
        // 200 claims: at one claim per 100 ms receive wait this takes 20 s.
        let took = drained_within(ctx.db(), backlog_id, Duration::from_secs(10)).await?;
        println!("  {BACKLOG} rows in claims of 10 drained in {took:?}");
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await
    .map_err(|e| e.to_string());

    if let Err(e) = alter_system(&db, "ALTER SYSTEM RESET pg_kafka.shadow_batch_size").await {
        eprintln!("⚠️  cleanup: failed to reset pg_kafka.shadow_batch_size: {e}");
    }
    if let Err(e) = ctx
        .db()
        .execute(
            "DELETE FROM kafka.shadow_tracking WHERE topic_id = $1",
            &[&stuck_id],
        )
        .await
    {
        eprintln!("⚠️  cleanup: failed to delete the orphan rows: {e}");
    }
    ctx.cleanup().await?;
    result?;
    println!("✅ Test PASSED\n");
    Ok(())
}
