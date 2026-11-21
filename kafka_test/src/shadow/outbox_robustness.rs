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
//!   drains claim after claim;
//! - with no external broker configured the outbox keeps its rows without
//!   claiming them over and over (the worker stays idle) and forwards them once
//!   one is;
//! - a claim does not slow down with the number of rows in flight (the
//!   in-flight exclusion is a hash lookup, not a scan per pending row).

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

    // The log line does not name the transaction (a sweep can drop the buffer of
    // another test's abandoned transaction too), so this transaction's abort is
    // awaited as well: once the sweep that aborted it has run, its buffer is gone,
    // and the line is in the log.
    println!("Step 2: wait for the sweep (every 10 s) to abort it and drop its buffer...");
    let needle = "forward buffer(s) of transactions that are no longer open";
    let start = Instant::now();
    loop {
        let state: Option<String> = ctx
            .db()
            .query_opt(
                "SELECT state FROM kafka.transactions WHERE transactional_id = $1",
                &[&txn_id],
            )
            .await?
            .map(|row| row.get(0));
        let aborted = state.as_deref() == Some("CompleteAbort");
        if aborted && log.since(log_from).contains(needle) {
            break;
        }
        if start.elapsed() > Duration::from_secs(40) {
            drop(producer);
            ctx.cleanup().await?;
            return Err(format!(
                "within 40 s the transaction is {state:?} (expected CompleteAbort) and \
                 \"{needle}\" is {}in the server log",
                if log.since(log_from).contains(needle) {
                    ""
                } else {
                    "not "
                }
            )
            .into());
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
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

/// CPU time (user + system, in clock ticks of 1/100 s) the process has used, or
/// `None` when this process cannot read its /proc entry (the DB runs elsewhere).
fn cpu_ticks(pid: i32) -> Option<u64> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    // Fields after the parenthesised command name: state is field 3, utime 14, stime 15.
    let rest = stat.rsplit_once(')')?.1;
    let fields: Vec<&str> = rest.split_whitespace().collect();
    Some(fields.get(11)?.parse::<u64>().ok()? + fields.get(12)?.parse::<u64>().ok()?)
}

/// How often the outbox rows of a topic have been claimed, summed: each claim
/// stamps `forwarded_at` and opens an attempt, so this is the latest stamp and
/// the open attempts.
async fn claim_marks(
    db: &Client,
    topic_id: i32,
) -> Result<(Option<String>, i64), Box<dyn std::error::Error>> {
    let row = db
        .query_one(
            "SELECT max(forwarded_at)::text, count(*) FILTER (WHERE attempt_open) \
             FROM kafka.shadow_tracking WHERE topic_id = $1",
            &[&topic_id],
        )
        .await?;
    Ok((row.get(0), row.get(1)))
}

/// Shadow mode on with `pg_kafka.shadow_bootstrap_servers` empty: the produce
/// writes its outbox rows, but the worker does not claim them (the forwarder
/// would answer every one NotAttempted and leave it due again), so with ten claims
/// worth of rows the worker stays idle instead of re-claiming them
/// without pause. Once a broker is configured the rows are forwarded.
pub async fn test_shadow_unconfigured_broker_does_not_spin_the_worker() -> TestResult {
    const BATCH: i32 = 10;
    const RECORDS: i32 = 10 * BATCH;
    const WINDOW: Duration = Duration::from_secs(5);
    println!("=== Test: no external broker configured, the outbox stays idle ===\n");
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("shadow-unconfigured").await;
    verify_external_kafka_ready().await?;
    enable_shadow_mode(
        ctx.db(),
        &topic,
        &config(ShadowMode::Shadow, SyncMode::Async),
    )
    .await?;
    let topic_id = topic_id(ctx.db(), &topic).await?;
    let db = create_db_client().await?;

    let result = async {
        println!("Step 1: shadow_bootstrap_servers = '', shadow_batch_size = {BATCH}...");
        alter_system(
            &db,
            &format!("ALTER SYSTEM SET pg_kafka.shadow_batch_size = {BATCH}"),
        )
        .await?;
        alter_system(&db, "ALTER SYSTEM SET pg_kafka.shadow_bootstrap_servers = ''").await?;

        println!("Step 2: produce {RECORDS} records...");
        let producer = create_producer()?;
        for i in 0..RECORDS {
            let key = format!("k{i}");
            producer
                .send(
                    FutureRecord::to(&topic).key(key.as_bytes()).payload("v"),
                    Duration::from_secs(10),
                )
                .await
                .map_err(|(e, _)| e)?;
        }
        let rows = undelivered(ctx.db(), topic_id).await?;
        if rows != i64::from(RECORDS) {
            return Err(format!("{rows} pending outbox rows, expected one per record").into());
        }
        // Let the ticks of the produce settle.
        tokio::time::sleep(Duration::from_secs(1)).await;

        println!("Step 3: watch the worker for {WINDOW:?}...");
        let pid: i32 = ctx
            .db()
            .query_one(
                "SELECT pid FROM pg_stat_activity WHERE backend_type = 'pg_kafka_listener'",
                &[],
            )
            .await?
            .get(0);
        let marks_before = claim_marks(ctx.db(), topic_id).await?;
        let cpu_before = cpu_ticks(pid);
        let start = Instant::now();
        tokio::time::sleep(WINDOW).await;
        let cpu_after = cpu_ticks(pid);
        let elapsed = start.elapsed();
        let marks_after = claim_marks(ctx.db(), topic_id).await?;
        println!(
            "  claim marks (latest forwarded_at, open attempts): {marks_before:?} -> {marks_after:?}"
        );
        // /proc counts in USER_HZ, 100 per second on Linux.
        let cpu_share = cpu_before.zip(cpu_after).map(|(before, after)| {
            let used = Duration::from_millis(after.saturating_sub(before) * 10);
            println!("  worker CPU: {used:?} in {elapsed:?}");
            used.as_secs_f64() / elapsed.as_secs_f64()
        });
        // Errors, not panics: the cleanup below must still restore the GUCs.
        if marks_before != marks_after {
            return Err("the outbox rows were claimed again while no broker is configured".into());
        }
        match cpu_share {
            Some(share) if share >= 0.2 => {
                return Err(format!(
                    "the worker used {:.0}% of a CPU with nothing to forward",
                    share * 100.0
                )
                .into());
            }
            Some(_) => {}
            None => println!("  (the worker's /proc entry is not readable here: CPU not checked)"),
        }

        println!("Step 4: configure the broker again; the rows go out...");
        restore_bootstrap_servers(&db).await?;
        let took = drained_within(ctx.db(), topic_id, Duration::from_secs(30)).await?;
        println!("  {RECORDS} rows forwarded in {took:?}");
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await
    .map_err(|e| e.to_string());

    if let Err(e) = restore_bootstrap_servers(&db).await {
        eprintln!("⚠️  cleanup: failed to restore pg_kafka.shadow_bootstrap_servers: {e}");
    }
    if let Err(e) = alter_system(&db, "ALTER SYSTEM RESET pg_kafka.shadow_batch_size").await {
        eprintln!("⚠️  cleanup: failed to reset pg_kafka.shadow_batch_size: {e}");
    }
    ctx.cleanup().await?;
    result?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// Rows of a topic some claim has taken (`forwarded_at` is set by the claim).
async fn claimed(db: &Client, topic_id: i32) -> Result<i64, Box<dyn std::error::Error>> {
    Ok(db
        .query_one(
            "SELECT COUNT(*) FROM kafka.shadow_tracking \
             WHERE topic_id = $1 AND forwarded_at IS NOT NULL",
            &[&topic_id],
        )
        .await?
        .get(0))
}

/// A claim skips the rows already in flight, and its cost must not grow with
/// their number: with the in-flight exclusion a correlated scan of the in-flight
/// list per pending row passed, claim k of a backlog cost O((k x batch)^2), and
/// filling the worker's 8 192-row in-flight window took ~15-20 s (each claim
/// stalling every request on the DB thread); with a hashed lookup it takes well
/// under a second.
///
/// The backlog is written while no broker is configured (nothing is claimed),
/// and acks are held back (`pg_kafka.test_forward_ack_delay_ms`), so every claimed
/// row stays in flight; the test times how long the claims take to fill the window,
/// from the first claimed row.
pub async fn test_shadow_claims_stay_fast_with_a_full_in_flight_window() -> TestResult {
    const RECORDS: usize = 10_000;
    // The worker's MAX_INFLIGHT: claims stop once this many rows are in flight.
    const WINDOW: i64 = 8_192;
    // From the first claimed row to a full window, measured on a dev machine:
    // 0.3-0.4 s with the hashed exclusion, 10-15 s with the old scan; CI runners
    // are ~2x slower, so this leaves >= 3x either way.
    const BOUND: Duration = Duration::from_secs(3);
    const ACK_DELAY_MS: i32 = 15_000;
    println!("=== Test: claims stay fast with a full in-flight window ===\n");
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("shadow-inflight-window").await;
    verify_external_kafka_ready().await?;
    enable_shadow_mode(
        ctx.db(),
        &topic,
        &config(ShadowMode::Shadow, SyncMode::Async),
    )
    .await?;
    let topic_id = topic_id(ctx.db(), &topic).await?;
    let db = create_db_client().await?;

    let result = async {
        println!("Step 1: no broker configured; produce {RECORDS} records...");
        alter_system(
            &db,
            "ALTER SYSTEM SET pg_kafka.shadow_bootstrap_servers = ''",
        )
        .await?;
        let producer = bulk_producer()?;
        let mut pending = Vec::with_capacity(RECORDS);
        for i in 0..RECORDS {
            let key = format!("k{}", i % 97);
            pending.push(
                producer
                    .send_result(FutureRecord::to(&topic).key(key.as_bytes()).payload("v"))
                    .map_err(|(e, _)| e)?,
            );
        }
        for f in pending {
            f.await?.map_err(|(e, _)| e)?;
        }
        let rows = undelivered(ctx.db(), topic_id).await?;
        let taken = claimed(ctx.db(), topic_id).await?;
        if rows != i64::try_from(RECORDS)? || taken != 0 {
            return Err(format!(
                "{rows} pending outbox rows ({taken} claimed), expected {RECORDS} unclaimed"
            )
            .into());
        }

        println!("Step 2: hold acks {ACK_DELAY_MS} ms, configure the broker, time the claims...");
        alter_system(
            &db,
            &format!("ALTER SYSTEM SET pg_kafka.test_forward_ack_delay_ms = {ACK_DELAY_MS}"),
        )
        .await?;
        // Timed from the first claimed row, so the reload's latency is not counted.
        db.execute(
            &format!(
                "ALTER SYSTEM SET pg_kafka.shadow_bootstrap_servers = '{}'",
                get_internal_bootstrap_servers()
            ),
            &[],
        )
        .await?;
        db.execute("SELECT pg_reload_conf()", &[]).await?;
        let reloaded = Instant::now();
        let mut first_claim: Option<Instant> = None;
        let filled = loop {
            let taken = claimed(ctx.db(), topic_id).await?;
            if taken > 0 && first_claim.is_none() {
                first_claim = Some(Instant::now());
            }
            if taken >= WINDOW {
                break first_claim.map_or(Duration::ZERO, |t| t.elapsed());
            }
            if reloaded.elapsed() > Duration::from_secs(60) {
                return Err(format!("only {taken} of {WINDOW} rows claimed after 60 s").into());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        println!("  the in-flight window ({WINDOW} rows) filled in {filled:?} (bound {BOUND:?})");
        if filled > BOUND {
            return Err(format!(
                "claiming {WINDOW} rows behind a growing in-flight window took {filled:?}, \
                 over {BOUND:?}: a claim's cost grows with the rows in flight"
            )
            .into());
        }

        println!("Step 3: release the acks; the backlog drains...");
        alter_system(&db, "ALTER SYSTEM RESET pg_kafka.test_forward_ack_delay_ms").await?;
        let took = drained_within(ctx.db(), topic_id, Duration::from_secs(90)).await?;
        println!("  drained {took:?} later");
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await
    .map_err(|e| e.to_string());

    if let Err(e) = alter_system(&db, "ALTER SYSTEM RESET pg_kafka.test_forward_ack_delay_ms").await
    {
        eprintln!("⚠️  cleanup: failed to reset pg_kafka.test_forward_ack_delay_ms: {e}");
    }
    if let Err(e) = restore_bootstrap_servers(&db).await {
        eprintln!("⚠️  cleanup: failed to restore pg_kafka.shadow_bootstrap_servers: {e}");
    }
    ctx.cleanup().await?;
    result?;
    println!("✅ Test PASSED\n");
    Ok(())
}
