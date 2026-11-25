//! Shadow tests of the outbox under stress and failure:
//! - `pg_kafka.shadow_max_retries` refuses 0 (every record gets an attempt);
//! - one topic's backlog does not starve another topic's records;
//! - a full librdkafka queue holds back only the records behind it, not the
//!   sync-mode response timeouts;
//! - a record that waits behind a full queue is answered within the delivery
//!   timeout of its dispatch, so its in-flight entry never expires while it is
//!   still held (no lost attempt counted, no second copy sent);
//! - a transaction the timeout sweep aborts is never forwarded (a commit
//!   enqueues from kafka.messages, so an open transaction has no outbox rows);
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
//!   in-flight exclusion is a hash lookup, not a scan per pending row), with
//!   the size of the backlog, with a tiny `work_mem`, or with JIT;
//! - a partition behind a backing-off head, or holding dead letters, does not
//!   make every claim read its pending rows (the worker stays idle beside it);
//! - dead letters follow `pg_kafka.shadow_max_retries` as it changes, whether
//!   they reached the cap by failure acks or by a lowered cap.

use crate::common::{create_db_client, create_producer, TestResult};
use crate::setup::TestContext;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use std::time::{Duration, Instant};
use tokio_postgres::Client;

use super::external_client::{
    count_external_messages, get_internal_bootstrap_servers, verify_external_kafka_ready,
};
use super::helpers::{enable_shadow_mode, ShadowMode, ShadowTopicConfig, SyncMode, WriteMode};
use super::pipelined::external_admin;
use rdkafka::admin::{AdminOptions, AlterConfig, NewTopic, ResourceSpecifier, TopicReplication};

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
/// the other partitions: another topic's record, queued with the backlog, is
/// forwarded while most of the backlog is still waiting, not after it. Both are
/// queued by the two replays of one statement, so the backlog has no head start: a
/// record produced only after the replay would let the forwarder drain rounds of
/// the backlog before the other topic had anything to send.
pub async fn test_shadow_backlog_does_not_starve_other_topics() -> TestResult {
    const BACKLOG: usize = 30_000;
    println!("=== Test: a backlog does not starve other topics ===\n");
    let ctx = TestContext::new().await?;
    // Created first: the lower topic_id, which a claim in row-key order serves first.
    let backlog_topic = ctx.unique_topic("shadow-backlog").await;
    let other_topic = ctx.unique_topic("shadow-not-starved").await;
    verify_external_kafka_ready().await?;
    for topic in [&backlog_topic, &other_topic] {
        enable_shadow_mode(
            ctx.db(),
            topic,
            &config(ShadowMode::LocalOnly, SyncMode::Async),
        )
        .await?;
    }

    println!("Step 1: {BACKLOG} records and one other record stored locally, not forwarded...");
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

    println!("Step 2: replay the backlog and the other record in one statement...");
    let (queued_backlog, queued_other): (i64, i64) = {
        let row = ctx
            .db()
            .query_one(
                "SELECT kafka.replay_shadow_messages($1, 0, NULL), \
                        kafka.replay_shadow_messages($2, 0, NULL)",
                &[&backlog_id, &other_id],
            )
            .await?;
        (row.get(0), row.get(1))
    };
    assert_eq!(queued_backlog, i64::try_from(BACKLOG)?);
    assert_eq!(queued_other, 1);

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
        // Errors, not panics: the cleanup below must still restore the GUCs.
        if held_for >= Duration::from_secs(8) {
            return Err(format!(
                "the sync response must be released at shadow_sync_timeout_ms (3 s), \
                 was held {held_for:?}"
            )
            .into());
        }
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await
    .map_err(|e| e.to_string());

    // Delete the topics (and their outbox rows) before the real cluster is back.
    if let Err(e) = ctx.cleanup().await {
        eprintln!("⚠️  cleanup: failed to delete the topics: {e}");
    }
    if let Err(e) = alter_system(&db, "ALTER SYSTEM RESET pg_kafka.shadow_sync_timeout_ms").await {
        eprintln!("⚠️  cleanup: failed to reset pg_kafka.shadow_sync_timeout_ms: {e}");
    }
    restore_bootstrap_servers(&db).await?;
    result?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// Shortened shadow delivery timeout for the backlog test: long enough that a
/// record timed from its enqueue (up to two timeouts after its dispatch) would
/// outlive the DB thread's in-flight eviction (150 s), which is fixed.
const BACKLOG_DELIVERY_TIMEOUT_MS: u64 = 100_000;

/// With the external cluster unreachable, one topic's records fill
/// librdkafka's queue and another topic's records wait in the forwarder's
/// backlog. Their delivery timeout must run from their dispatch, backlog
/// included: timed from the enqueue, they would reach the replacement
/// producer when the first producer times out and stay outstanding for
/// another whole timeout, past the DB thread's in-flight eviction (150 s).
/// The claim then takes them again while they are still held, counts a lost
/// attempt for every one of them (not only the partition's head) and sends
/// a second copy beside the first.
///
/// The small records must really wait in the backlog (not on the producer),
/// or the test would pass without exercising it. The backlog is in arrival
/// order, so they do if the large topic's last record was still there when
/// they were dispatched: it is, if it was unanswered then and is answered
/// (and claimed again) before the producer's generation times out (before
/// the large topic's head counts a failed attempt), which only the backlog
/// does, once less than a quarter of its delivery timeout is left.
///
/// The small topic's head is answered from the backlog too, but NotAttempted:
/// it found no room only because the large topic's older records filled the
/// queue, so it counts no failed attempt there. For that the "small" records
/// carry as many bytes as the large ones: the queue holds a whole number of
/// large records, and the bytes left over (under one large record) would take
/// a record of a few bytes as soon as the large records ahead of it in the
/// backlog are answered there. Such a record goes out on the first
/// generation, a few seconds before that generation times out, and fails with
/// it (a real attempt), not from the backlog. It is claimed again, goes out
/// on the replacement producer once the first generation times out, and fails
/// one delivery timeout after that second dispatch.
pub async fn test_shadow_backlogged_records_are_answered_before_their_in_flight_entry_expires(
) -> TestResult {
    const BIG: usize = 80;
    const RECORD_BYTES: usize = 900_000;
    const SMALL: usize = 3;
    println!(
        "=== Test: backlogged records are answered before their in-flight entry expires ===\n"
    );
    let ctx = TestContext::new().await?;
    let big_topic = ctx.unique_topic("shadow-fill-queue").await;
    let small_topic = ctx.unique_topic("shadow-behind-queue").await;
    verify_external_kafka_ready().await?;
    enable_shadow_mode(
        ctx.db(),
        &big_topic,
        &config(ShadowMode::Shadow, SyncMode::Async),
    )
    .await?;
    enable_shadow_mode(
        ctx.db(),
        &small_topic,
        &config(ShadowMode::Shadow, SyncMode::Async),
    )
    .await?;
    let small_id = topic_id(ctx.db(), &small_topic).await?;
    let big_id = topic_id(ctx.db(), &big_topic).await?;
    let db = create_db_client().await?;
    alter_system(
        &db,
        "ALTER SYSTEM SET pg_kafka.shadow_bootstrap_servers = '127.0.0.1:1'",
    )
    .await?;
    alter_system(
        &db,
        &format!(
            "ALTER SYSTEM SET pg_kafka.test_shadow_delivery_timeout_ms = {BACKLOG_DELIVERY_TIMEOUT_MS}"
        ),
    )
    .await?;

    let result = async {
        println!("Step 1: {BIG} records of {RECORD_BYTES} bytes fill the producer queue...");
        let producer = bulk_producer()?;
        let payload = vec![b'q'; RECORD_BYTES];
        // No large record reaches the forwarder before this, so the generation
        // holding them cannot time out before `big_started` + the timeout.
        let big_started = Instant::now();
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
        tokio::time::sleep(Duration::from_secs(2)).await;
        println!("Step 2: {SMALL} records of another topic, as large, wait behind them...");
        for i in 0..SMALL {
            producer
                .send(
                    FutureRecord::<(), _>::to(&small_topic)
                        .payload(&payload[..])
                        .partition(0),
                    Duration::from_secs(30),
                )
                .await
                .map_err(|(e, _)| format!("small record {i}: {e}"))?;
        }
        // The topics are async: the produce ack comes before the claim, which
        // the next outbox tick (every 250 ms) makes. Wait for it, so the first
        // claim is not taken for the re-claim after a NotAttempted answer, and
        // time the head's first attempt from it.
        let claim_wait = Instant::now();
        let small_head_claimed: Option<String> = loop {
            let row = ctx
                .db()
                .query_one(
                    "SELECT forwarded_at::text, attempt_open, retry_count \
                     FROM kafka.shadow_tracking WHERE topic_id = $1 AND local_offset = 0",
                    &[&small_id],
                )
                .await?;
            let (claimed_at, open, retries): (Option<String>, bool, i32) =
                (row.get(0), row.get(1), row.get(2));
            if retries != 0 {
                return Err(format!(
                    "the small topic's head counted {retries} failed attempt(s) before its \
                     first claim was seen"
                )
                .into());
            }
            if claimed_at.is_some() && open {
                break claimed_at;
            }
            if claim_wait.elapsed() > Duration::from_secs(10) {
                return Err(format!(
                    "the small topic's head was not claimed within 10 s of its produce \
                     (forwarded_at {claimed_at:?}, attempt open {open})"
                )
                .into());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        let dispatched = Instant::now();
        println!(
            "  the small topic's head was claimed {:?} after its produce was acked",
            claim_wait.elapsed()
        );
        // The large topic's last record and head: (retry_count, attempt_open,
        // when it was last claimed).
        let big_state = |offset: i64| {
            let db = ctx.db();
            async move {
                let row = db
                    .query_one(
                        "SELECT retry_count, attempt_open, forwarded_at::text \
                         FROM kafka.shadow_tracking WHERE topic_id = $1 AND local_offset = $2",
                        &[&big_id, &offset],
                    )
                    .await?;
                Ok::<(i32, bool, Option<String>), Box<dyn std::error::Error>>((
                    row.get(0),
                    row.get(1),
                    row.get(2),
                ))
            }
        };
        let big_last = i64::try_from(BIG)? - 1;
        let (tail_retries, tail_open, tail_claimed) = big_state(big_last).await?;
        if tail_retries != 0 || !tail_open {
            return Err(format!(
                "the large topic's last record must still be held when the small records \
                 are dispatched (retries {tail_retries}, attempt open {tail_open})"
            )
            .into());
        }
        let mut tail_answered_after = None;

        // The in-flight entries expire 150 s after each dispatch: watch a
        // little past that, and until the small topic's head failed its
        // second attempt (one timeout after its second dispatch). Only the
        // partition's head may count failed attempts; a lost attempt counted
        // by a claim of a still-held record shows on the records behind it
        // too.
        let watch = Duration::from_secs(165);
        let watch_max =
            Duration::from_millis(2 * BACKLOG_DELIVERY_TIMEOUT_MS) + Duration::from_secs(20);
        println!("Step 3: watch the retry counts for {watch:?} (at most {watch_max:?})...");
        let mut head_failed_after = None;
        let mut head_reclaimed_after = None;
        while dispatched.elapsed() < watch
            || (head_failed_after.is_none() && dispatched.elapsed() < watch_max)
        {
            let rows = ctx
                .db()
                .query(
                    "SELECT local_offset, retry_count, forwarded_at::text \
                     FROM kafka.shadow_tracking WHERE topic_id = $1 ORDER BY local_offset",
                    &[&small_id],
                )
                .await?;
            let retries: Vec<(i64, i32)> = rows.iter().map(|r| (r.get(0), r.get(1))).collect();
            if retries.len() != SMALL {
                return Err(format!("expected {SMALL} outbox rows, got {retries:?}").into());
            }
            let head_claimed: Option<String> = rows[0].get(2);
            if head_reclaimed_after.is_none() && head_claimed != small_head_claimed {
                if retries[0].1 > 0 {
                    return Err(format!(
                        "the small topic's head counted a failed attempt after {:?} on its \
                         first dispatch, while the large topic's older records filled the \
                         queue: {retries:?}",
                        dispatched.elapsed()
                    )
                    .into());
                }
                head_reclaimed_after = Some(dispatched.elapsed());
                println!(
                    "  head answered NotAttempted from the backlog and claimed again after {:?}",
                    dispatched.elapsed()
                );
            }
            if head_failed_after.is_none() && retries[0].1 > 0 {
                if head_reclaimed_after.is_none() {
                    return Err(format!(
                        "the small topic's head counted a failed attempt after {:?} on its \
                         first dispatch, while the large topic's older records filled the \
                         queue",
                        dispatched.elapsed()
                    )
                    .into());
                }
                head_failed_after = Some(dispatched.elapsed());
                println!("  head failed an attempt after {:?}", dispatched.elapsed());
            }
            if tail_answered_after.is_none() && big_state(big_last).await?.2 != tail_claimed {
                let (head_retries, _, _) = big_state(0).await?;
                if head_retries != 0 {
                    return Err(format!(
                        "the large topic's last record was answered only with the producer's \
                         generation (its head has {head_retries} failed attempt(s)): it was \
                         not in the forwarder's backlog, so neither were the small records"
                    )
                    .into());
                }
                let since_big = big_started.elapsed();
                let generation_timeout = Duration::from_millis(BACKLOG_DELIVERY_TIMEOUT_MS);
                if since_big + Duration::from_secs(5) >= generation_timeout {
                    return Err(format!(
                        "the large topic's last record was answered {since_big:?} after the \
                         first large record was sent, too late to tell the backlog's answer \
                         from the generation's timeout ({generation_timeout:?})"
                    )
                    .into());
                }
                tail_answered_after = Some(dispatched.elapsed());
                println!(
                    "  the large topic's last record was answered from the backlog after {:?}",
                    dispatched.elapsed()
                );
            }
            if retries[1..].iter().any(|&(_, n)| n > 0) {
                return Err(format!(
                    "a record behind the partition's head counted a failed attempt after \
                     {:?} (its in-flight entry expired while the forwarder still held it): \
                     {retries:?}",
                    dispatched.elapsed()
                )
                .into());
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        let head_reclaimed_after = head_reclaimed_after
            .ok_or("the small topic's head was never answered: the queue did not hold it back")?;
        let head_failed_after = head_failed_after
            .ok_or("the small topic's head never timed out on the replacement producer")?;
        tail_answered_after.ok_or(
            "the large topic's last record was never answered from the backlog: the small \
             records did not wait behind it",
        )?;
        // Each attempt is answered within one delivery timeout of its dispatch:
        // the first from the backlog, the second by the replacement
        // producer's timeout, timed from that dispatch.
        // Errors, not panics: the cleanup below must still restore the GUCs.
        let timeout = Duration::from_millis(BACKLOG_DELIVERY_TIMEOUT_MS);
        if head_reclaimed_after >= timeout {
            return Err(format!(
                "the head's first attempt was answered {head_reclaimed_after:?} after its \
                 dispatch, more than the delivery timeout ({BACKLOG_DELIVERY_TIMEOUT_MS} ms)"
            )
            .into());
        }
        let second = head_failed_after.saturating_sub(head_reclaimed_after);
        if second >= timeout + Duration::from_secs(10) {
            return Err(format!(
                "the head failed {second:?} after its second dispatch, more than the \
                 delivery timeout ({BACKLOG_DELIVERY_TIMEOUT_MS} ms)"
            )
            .into());
        }
        println!(
            "  first attempt answered after {head_reclaimed_after:?}, second failed {second:?} \
             after its dispatch"
        );
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await
    .map_err(|e| e.to_string());

    // Delete the topics (and their outbox rows) before the real cluster is back.
    if let Err(e) = ctx.cleanup().await {
        eprintln!("⚠️  cleanup: failed to delete the topics: {e}");
    }
    if let Err(e) = alter_system(
        &db,
        "ALTER SYSTEM RESET pg_kafka.test_shadow_delivery_timeout_ms",
    )
    .await
    {
        eprintln!("⚠️  cleanup: failed to reset pg_kafka.test_shadow_delivery_timeout_ms: {e}");
    }
    restore_bootstrap_servers(&db).await?;
    result?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// A transaction left open by a producer that went away is aborted by the
/// timeout sweep, and nothing of it is ever forwarded: the shadow store keeps
/// nothing for an open transaction (its records are enqueued at commit, from
/// kafka.messages), so the abort leaves no outbox row and nothing to drop.
pub async fn test_shadow_timed_out_transaction_is_not_forwarded() -> TestResult {
    println!("=== Test: an abandoned transaction's records are never forwarded ===\n");
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("shadow-txn-abandoned").await;
    verify_external_kafka_ready().await?;
    enable_shadow_mode(
        ctx.db(),
        &topic,
        &config(ShadowMode::Shadow, SyncMode::Async),
    )
    .await?;

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
        if state.as_deref() == Some("CompleteAbort") {
            break;
        }
        if start.elapsed() > Duration::from_secs(40) {
            drop(producer);
            ctx.cleanup().await?;
            return Err(
                format!("within 40 s the transaction is {state:?}, not CompleteAbort").into(),
            );
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    println!("  aborted after {:?}", start.elapsed());

    let row = ctx
        .db()
        .query_one(
            "SELECT
               (SELECT COUNT(*) FROM kafka.messages m WHERE m.topic_id = t.id
                  AND m.txn_state = 'aborted'),
               (SELECT COUNT(*) FROM kafka.shadow_tracking s WHERE s.topic_id = t.id)
             FROM kafka.topics t WHERE t.name = $1",
            &[&topic],
        )
        .await?;
    let (aborted, enqueued): (i64, i64) = (row.get(0), row.get(1));
    assert_eq!(aborted, 1, "the record is aborted");
    assert_eq!(
        enqueued, 0,
        "nothing of the aborted transaction is enqueued"
    );
    super::assertions::assert_external_kafka_empty(&topic, Duration::from_secs(3)).await?;

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
        // Errors, not panics: the cleanup below must still drop the trigger.
        if delivered {
            return Err("the trigger did not refuse the delivered mark".into());
        }
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
        if retry_count != 0 {
            return Err(format!(
                "the ack was lost: its attempt was counted as failed and re-sent \
                 (retry_count {retry_count})"
            )
            .into());
        }
        let external = count_external_messages(&topic, Duration::from_secs(5)).await?;
        if external != 1 {
            return Err(format!("forwarded {external} times").into());
        }
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
        // Errors, not panics: the cleanup below must still drop the trigger.
        if delivered != 0 {
            return Err(format!(
                "the trigger did not refuse the delivered mark at commit ({delivered} delivered)"
            )
            .into());
        }
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
        if max_retries != 0 {
            return Err(format!(
                "an ack was lost: its attempt was counted as failed and re-sent \
                 (max retry_count {max_retries})"
            )
            .into());
        }
        let external = count_external_messages(&topic, Duration::from_secs(5)).await?;
        if external != RECORDS {
            return Err(format!("forwarded {external} records for {RECORDS}").into());
        }
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

/// How [`time_claims`] sets up and times the claims of a backlog.
struct ClaimRun {
    /// Records written while no broker is configured: the backlog.
    records: usize,
    /// Claimed rows to time, from the first claimed row.
    rows: i64,
    /// Fails the test if claiming `rows` takes longer.
    bound: Duration,
    /// `ANALYZE kafka.shadow_tracking` once the backlog is written, so the planner
    /// sees its size (autovacuum would, a little later).
    analyze: bool,
    /// Server settings (`name`, `value`) set for the claims, then reset.
    settings: &'static [(&'static str, &'static str)],
    /// What a run over the bound means.
    failure: &'static str,
}

/// Write a backlog while no broker is configured (nothing is claimed), hold acks
/// back (`pg_kafka.test_forward_ack_delay_ms`) so every claimed row stays in
/// flight, configure the broker and time the claims from the first claimed row
/// until `run.rows` are claimed; then release the acks and let the backlog drain.
async fn time_claims(name: &str, run: &ClaimRun) -> TestResult {
    const ACK_DELAY_MS: i32 = 15_000;
    println!("=== Test: {name} ===\n");
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("shadow-claim-time").await;
    verify_external_kafka_ready().await?;
    enable_shadow_mode(
        ctx.db(),
        &topic,
        &config(ShadowMode::Shadow, SyncMode::Async),
    )
    .await?;
    let topic_id = topic_id(ctx.db(), &topic).await?;
    let db = create_db_client().await?;
    let records = run.records;

    let result = async {
        println!("Step 1: no broker configured; produce {records} records...");
        alter_system(
            &db,
            "ALTER SYSTEM SET pg_kafka.shadow_bootstrap_servers = ''",
        )
        .await?;
        let producer = bulk_producer()?;
        let mut pending = Vec::with_capacity(records);
        for i in 0..records {
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
        if rows != i64::try_from(records)? || taken != 0 {
            return Err(format!(
                "{rows} pending outbox rows ({taken} claimed), expected {records} unclaimed"
            )
            .into());
        }
        if run.analyze {
            db.execute("ANALYZE kafka.shadow_tracking", &[]).await?;
        }

        println!("Step 2: hold acks {ACK_DELAY_MS} ms, set {:?}, configure the broker, time the claims...", run.settings);
        db.execute(
            &format!("ALTER SYSTEM SET pg_kafka.test_forward_ack_delay_ms = {ACK_DELAY_MS}"),
            &[],
        )
        .await?;
        for (setting, value) in run.settings {
            db.execute(&format!("ALTER SYSTEM SET {setting} = '{value}'"), &[])
                .await?;
        }
        db.execute("SELECT pg_reload_conf()", &[]).await?;
        tokio::time::sleep(Duration::from_millis(700)).await;
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
        let took = loop {
            let taken = claimed(ctx.db(), topic_id).await?;
            if taken > 0 && first_claim.is_none() {
                first_claim = Some(Instant::now());
            }
            if taken >= run.rows {
                break first_claim.map_or(Duration::ZERO, |t| t.elapsed());
            }
            if reloaded.elapsed() > Duration::from_secs(60) {
                return Err(format!(
                    "only {taken} of {} rows claimed after 60 s: {}",
                    run.rows, run.failure
                )
                .into());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        println!(
            "  {} rows claimed in {took:?} (bound {:?})",
            run.rows, run.bound
        );
        println!("SHADOW_CLAIM_TIME_MS={}", took.as_millis());
        if took > run.bound {
            return Err(format!(
                "claiming {} rows took {took:?}, over {:?}: {}",
                run.rows, run.bound, run.failure
            )
            .into());
        }

        println!("Step 3: reset the settings, release the acks; the backlog drains...");
        for (setting, _) in run.settings {
            db.execute(&format!("ALTER SYSTEM RESET {setting}"), &[])
                .await?;
        }
        alter_system(&db, "ALTER SYSTEM RESET pg_kafka.test_forward_ack_delay_ms").await?;
        let took = drained_within(ctx.db(), topic_id, Duration::from_secs(120)).await?;
        println!("  drained {took:?} later");
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await
    .map_err(|e| e.to_string());

    for (setting, _) in run.settings {
        if let Err(e) = db
            .execute(&format!("ALTER SYSTEM RESET {setting}"), &[])
            .await
        {
            eprintln!("⚠️  cleanup: failed to reset {setting}: {e}");
        }
    }
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

/// A claim skips the rows already in flight, and its cost must not grow with
/// their number: with the in-flight exclusion a correlated scan of the in-flight
/// list per pending row passed, claim k of a backlog cost O((k x batch)^2), and
/// filling the worker's 8 192-row in-flight window took ~15-20 s (each claim
/// stalling every request on the DB thread); with a hashed lookup it takes well
/// under a second. Timed from the first claimed row until the window is full.
pub async fn test_shadow_claims_stay_fast_with_a_full_in_flight_window() -> TestResult {
    time_claims(
        "claims stay fast with a full in-flight window",
        &ClaimRun {
            records: 10_000,
            // The worker's MAX_INFLIGHT: claims stop once this many rows are in flight.
            rows: 8_192,
            // Measured on a dev machine: 0.3-0.4 s with the hashed exclusion, 10-15 s
            // with the old scan; CI runners are ~2x slower, so this leaves >= 3x
            // either way.
            bound: Duration::from_secs(3),
            analyze: false,
            settings: &[],
            failure: "a claim's cost grows with the rows in flight",
        },
    )
    .await
}

/// A claim's cost must not grow with the backlog. Its probe for the partitions
/// with a due row was a `WHERE EXISTS`, which the planner turned into a hash
/// semi join over every pending row (a seq scan of the outbox and a message
/// probe per row) for single-partition backlogs of 100 000-200 000 rows: ~200 ms
/// per claim however small. A backlog of 150 000 rows is written and analyzed
/// (as autovacuum would), then claims of 50 rows are timed until 2 000 rows are
/// in flight (40 claims). JIT is off so only the plan is measured. On PG17 here:
/// 0.45-0.55 s with the ordered LATERAL probe, 10.6 s with the semi join; the
/// 3 s bound leaves >= 3x either way allowing for CI runners ~2x slower.
pub async fn test_shadow_claims_stay_fast_behind_a_large_backlog() -> TestResult {
    time_claims(
        "claims stay fast behind a large backlog",
        &ClaimRun {
            records: 150_000,
            rows: 2_000,
            bound: Duration::from_secs(3),
            analyze: true,
            settings: &[("jit", "off"), ("pg_kafka.shadow_batch_size", "50")],
            failure: "a claim's cost grows with the backlog",
        },
    )
    .await
}

/// The claim's in-flight exclusions are hashed only when the in-flight list fits
/// in the hash memory limit; with a tiny `work_mem` Postgres rescans the list for
/// every pending row it passes (10 s per claim with 8 000 rows in flight at
/// 64 kB). The claim raises `work_mem` for itself, so the in-flight window fills
/// as fast as with the default: on PG17 here 0.23 s, 22 s without the floor.
pub async fn test_shadow_claims_stay_fast_with_a_tiny_work_mem() -> TestResult {
    time_claims(
        "claims stay fast with a tiny work_mem",
        &ClaimRun {
            records: 10_000,
            rows: 8_192,
            bound: Duration::from_secs(3),
            analyze: false,
            settings: &[("work_mem", "64kB"), ("jit", "off")],
            failure: "the claim ran with the server's tiny work_mem",
        },
    )
    .await
}

/// The claim's cost estimate grows with the backlog and crosses `jit_above_cost`;
/// compiling it costs far more than running it. The claim turns JIT off for
/// itself: with the JIT thresholds at 1 000 (every claim, and no trivial
/// statement, crosses them) the in-flight window fills as fast as without JIT:
/// on PG17 here 0.27-0.32 s, 11.2 s with each claim compiled. Skipped where the
/// server has no JIT provider (llvmjit).
pub async fn test_shadow_claims_are_not_jit_compiled() -> TestResult {
    let db = create_db_client().await?;
    let available: bool = db.query_one("SELECT pg_jit_available()", &[]).await?.get(0);
    if !available {
        println!("=== Test: claims are not JIT-compiled ===\n");
        println!("⚠️  SKIPPED: the server has no JIT provider (pg_jit_available() is false)\n");
        return Ok(());
    }
    time_claims(
        "claims are not JIT-compiled",
        &ClaimRun {
            records: 10_000,
            rows: 8_192,
            bound: Duration::from_secs(3),
            analyze: false,
            settings: &[
                ("jit", "on"),
                ("jit_above_cost", "1000"),
                ("jit_inline_above_cost", "1000"),
                ("jit_optimize_above_cost", "1000"),
            ],
            failure: "the claim was JIT-compiled",
        },
    )
    .await
}

/// The share of a CPU the process `pid` uses over `window`; `None` if its `/proc`
/// entry is not readable.
async fn cpu_share(pid: i32, window: Duration) -> Option<f64> {
    let before = cpu_ticks(pid)?;
    let start = Instant::now();
    tokio::time::sleep(window).await;
    let after = cpu_ticks(pid)?;
    // /proc counts in USER_HZ, 100 per second on Linux.
    let used = Duration::from_millis(after.saturating_sub(before) * 10);
    Some(used.as_secs_f64() / start.elapsed().as_secs_f64())
}

/// How a test leaves its stalled rows not due.
#[derive(Clone, Copy)]
enum Stall {
    /// This SQL (`$1` the topic id).
    Sql(&'static str),
    /// Every row of the topic gets `retry_count = cap`, then `pg_kafka.shadow_max_retries`
    /// is set to `cap` (3, or 4 if it already is 3) with a reload: dead letters, as the
    /// worker learns of them when the cap changes.
    DeadAtCap,
}

/// Pending rows of a partition with nothing due must cost a claim nothing: every
/// outbox tick (every 250 ms on the DB thread, whether anything is due or not)
/// probes each partition with pending rows for its first due row. A topic of
/// `records` pending rows is written while no broker is configured, `stall` leaves
/// none of them due, the broker is configured, and the worker's CPU over 10 s, with
/// nothing to forward, is compared with its CPU over 10 s before, with an empty
/// outbox. Needs the DB host's `/proc` (skipped without it).
async fn idle_beside_a_stall(
    name: &str,
    records: usize,
    stall: Stall,
    failure: &str,
) -> TestResult {
    const WINDOW: Duration = Duration::from_secs(10);
    /// The share of a CPU the stalled rows may add (measured numbers at the callers).
    const MAX_EXTRA_CPU_SHARE: f64 = 0.03;
    println!("=== Test: {name} ===\n");
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("shadow-stalled").await;
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
        let pid: i32 = ctx
            .db()
            .query_one(
                "SELECT pid FROM pg_stat_activity WHERE backend_type = 'pg_kafka_listener'",
                &[],
            )
            .await?
            .get(0);
        println!("Step 0: the worker's CPU over {WINDOW:?} with an empty outbox...");
        let Some(idle) = cpu_share(pid, WINDOW).await else {
            println!("⚠️  SKIPPED: the worker's /proc entry is not readable here");
            return Ok(());
        };
        println!("  {:.1}% of a CPU", idle * 100.0);

        println!("Step 1: no broker configured; produce {records} records...");
        alter_system(
            &db,
            "ALTER SYSTEM SET pg_kafka.shadow_bootstrap_servers = ''",
        )
        .await?;
        let producer = bulk_producer()?;
        // In rounds the producer's queue (200 000 messages) holds.
        for round in (0..records).collect::<Vec<_>>().chunks(100_000) {
            let mut pending = Vec::with_capacity(round.len());
            for i in round {
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
        }
        let rows = undelivered(ctx.db(), topic_id).await?;
        if rows != i64::try_from(records)? {
            return Err(format!("{rows} pending outbox rows, expected {records}").into());
        }

        let stalled = match stall {
            Stall::Sql(sql) => {
                println!("Step 2: stall them: {sql}");
                db.execute(sql, &[&topic_id]).await?
            }
            Stall::DeadAtCap => {
                let current: i32 = db
                    .query_one(
                        "SELECT current_setting('pg_kafka.shadow_max_retries')::int",
                        &[],
                    )
                    .await?
                    .get(0);
                let cap: i32 = if current == 3 { 4 } else { 3 };
                println!("Step 2: retry_count = {cap}, then the cap to {cap}...");
                let n = db
                    .execute(
                        "UPDATE kafka.shadow_tracking SET retry_count = $2 WHERE topic_id = $1",
                        &[&topic_id, &cap],
                    )
                    .await?;
                alter_system(
                    &db,
                    &format!("ALTER SYSTEM SET pg_kafka.shadow_max_retries = {cap}"),
                )
                .await?;
                n
            }
        };
        println!("  {stalled} rows changed");
        if stalled == 0 {
            return Err("the stall changed no outbox row".into());
        }

        println!("Step 3: configure the broker; let the first ticks run, then VACUUM ANALYZE...");
        restore_bootstrap_servers(&db).await?;
        tokio::time::sleep(Duration::from_secs(3)).await;
        // As autovacuum would after the stall's mass update (and what the first
        // claims changed).
        db.execute("VACUUM ANALYZE kafka.shadow_tracking", &[])
            .await?;
        tokio::time::sleep(Duration::from_secs(1)).await;
        let rows = undelivered(ctx.db(), topic_id).await?;
        if rows != i64::try_from(records)? {
            return Err(format!(
                "{rows} of {records} stalled rows left pending: some were forwarded"
            )
            .into());
        }

        println!("Step 4: the worker's CPU over {WINDOW:?} beside the stalled rows...");
        let share = cpu_share(pid, WINDOW)
            .await
            .ok_or("the worker's /proc entry is no longer readable")?;
        let extra = share - idle;
        println!(
            "  {:.1}% of a CPU, {:.1} points over the empty outbox",
            share * 100.0,
            extra * 100.0
        );
        println!("SHADOW_STALLED_EXTRA_CPU_POINTS={:.1}", extra * 100.0);
        let rows = undelivered(ctx.db(), topic_id).await?;
        if rows != i64::try_from(records)? {
            return Err(format!(
                "{rows} of {records} stalled rows left pending: some were forwarded"
            )
            .into());
        }
        if extra >= MAX_EXTRA_CPU_SHARE {
            return Err(format!(
                "the stalled rows added {:.1}% of a CPU (bound {:.0}%) with nothing due: {failure}",
                extra * 100.0,
                MAX_EXTRA_CPU_SHARE * 100.0
            )
            .into());
        }
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await
    .map_err(|e| e.to_string());

    if matches!(stall, Stall::DeadAtCap) {
        if let Err(e) = alter_system(&db, "ALTER SYSTEM RESET pg_kafka.shadow_max_retries").await {
            eprintln!("⚠️  cleanup: failed to reset pg_kafka.shadow_max_retries: {e}");
        }
    }
    if let Err(e) = restore_bootstrap_servers(&db).await {
        eprintln!("⚠️  cleanup: failed to restore pg_kafka.shadow_bootstrap_servers: {e}");
    }
    ctx.cleanup().await?;
    result?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// The stalled rows of the idle tests. The extra CPU of reading them on every tick is
/// about linear in their number; on PG17 here, before the fixes (d08e2d8), it was
/// 8.8 / 10.6 points at 300 000 rows (blocked partition / dead letters), 5.4 / 6.5 at
/// 200 000, 4.8 / 4.8 at 150 000 and 3.7 / 3.6 at 100 000, against the 3-point bound;
/// 200 000 still fails by about 2x, and costs 2-3 s less per test than 300 000.
const STALLED_ROWS: usize = 200_000;

/// Behind a backing-off head (a failed row whose backoff has not passed) nothing of
/// its partition is due, and the claim's probe stopped there only by a filter: it
/// read the partition's whole backlog on every tick. Here the head of a
/// 200 000-row backlog backs off (its last attempt stamped an hour ahead, so it
/// stays blocked) and the idle worker is measured. On PG17 here the stalled rows
/// added 5.4 points of a CPU with the filter (d08e2d8), at most 0.0 with the index
/// condition; the bound is 3 points.
pub async fn test_shadow_worker_stays_idle_beside_a_blocked_partition() -> TestResult {
    idle_beside_a_stall(
        "the worker stays idle beside a blocked partition",
        STALLED_ROWS,
        Stall::Sql(
            "UPDATE kafka.shadow_tracking \
         SET retry_count = 1, attempt_open = false, forwarded_at = NOW() + interval '1 hour' \
         WHERE (topic_id, partition_id, local_offset) IN ( \
             SELECT topic_id, partition_id, MIN(local_offset) \
             FROM kafka.shadow_tracking WHERE topic_id = $1 \
             GROUP BY topic_id, partition_id)",
        ),
        "a claim reads the backlog behind a backing-off head",
    )
    .await
}

/// Dead letters (rows at `pg_kafka.shadow_max_retries`) stay pending until replayed
/// or the cap is raised, and every claim read past all of them: in its probe of
/// their partition and in its search for backing-off rows. Here 200 000 rows become
/// dead letters by a lowered cap (the worker re-flags its dead letters when the cap
/// changes) and the idle worker is measured. On PG17 here they added 6.5 points of a
/// CPU read on every tick (d08e2d8), at most -0.4 skipped by the dead flag.
pub async fn test_shadow_worker_stays_idle_beside_dead_letters() -> TestResult {
    idle_beside_a_stall(
        "the worker stays idle beside dead letters",
        STALLED_ROWS,
        Stall::DeadAtCap,
        "a claim reads the dead letters",
    )
    .await
}

/// Dead letters follow `pg_kafka.shadow_max_retries` as it changes: rows at the cap
/// are not forwarded; raising the cap above their retry count forwards them; lowering
/// it to the retry count of due rows stops them; a replay forwards them again. (The
/// claim keeps a per-row dead flag in step with the cap, so each change must reach it:
/// step 1 checks the flags were set by the sync a cap change makes, and step 2 that
/// the sync clears them again.)
pub async fn test_shadow_dead_letters_follow_the_retry_cap() -> TestResult {
    const RECORDS: usize = 200;
    println!("=== Test: dead letters follow pg_kafka.shadow_max_retries ===\n");
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("shadow-dead-cap").await;
    verify_external_kafka_ready().await?;
    enable_shadow_mode(
        ctx.db(),
        &topic,
        &config(ShadowMode::Shadow, SyncMode::Async),
    )
    .await?;
    let topic_id = topic_id(ctx.db(), &topic).await?;
    let db = create_db_client().await?;
    let delivered = |n: i64| i64::try_from(RECORDS).map(|r| r - n);

    let result = async {
        let producer = bulk_producer()?;
        let produce = || async {
            let mut pending = Vec::with_capacity(RECORDS);
            for i in 0..RECORDS {
                let key = format!("k{i}");
                pending.push(
                    producer
                        .send_result(FutureRecord::to(&topic).key(key.as_bytes()).payload("v"))
                        .map_err(|(e, _)| e)?,
                );
            }
            for f in pending {
                f.await?.map_err(|(e, _)| e)?;
            }
            Ok::<(), Box<dyn std::error::Error>>(())
        };
        let current: i32 = db
            .query_one(
                "SELECT current_setting('pg_kafka.shadow_max_retries')::int",
                &[],
            )
            .await?
            .get(0);
        // A cap other than the current one: the reload that sets it makes the worker
        // sync the flags, so the dead letters below are flagged and step 2 needs the
        // sync to revive them (a hand edit alone is not synced).
        let cap: i32 = if current == 5 { 6 } else { 5 };

        println!("Step 1: no broker; {RECORDS} records dead-lettered at a new cap ({cap})...");
        alter_system(
            &db,
            "ALTER SYSTEM SET pg_kafka.shadow_bootstrap_servers = ''",
        )
        .await?;
        produce().await?;
        db.execute(
            "UPDATE kafka.shadow_tracking \
             SET retry_count = $2, forwarded_at = NOW() - interval '1 hour' \
             WHERE topic_id = $1",
            &[&topic_id, &cap],
        )
        .await?;
        alter_system(
            &db,
            &format!("ALTER SYSTEM SET pg_kafka.shadow_max_retries = {cap}"),
        )
        .await?;
        restore_bootstrap_servers(&db).await?;
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let unflagged: i64 = db
                .query_one(
                    "SELECT COUNT(*) FROM kafka.shadow_tracking \
                     WHERE topic_id = $1 AND external_offset IS NULL AND NOT dead",
                    &[&topic_id],
                )
                .await?
                .get(0);
            if unflagged == 0 {
                break;
            }
            if Instant::now() > deadline {
                return Err(format!(
                    "{unflagged} of {RECORDS} rows at cap {cap} have no dead flag after 30 s"
                )
                .into());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let left = undelivered(ctx.db(), topic_id).await?;
        if delivered(left)? != 0 {
            return Err(format!("{} dead letters were forwarded", delivered(left)?).into());
        }

        println!(
            "Step 2: raise the cap to {}; they are forwarded...",
            cap + 1
        );
        alter_system(
            &db,
            &format!("ALTER SYSTEM SET pg_kafka.shadow_max_retries = {}", cap + 1),
        )
        .await?;
        let took = drained_within(ctx.db(), topic_id, Duration::from_secs(30)).await?;
        println!("  forwarded in {took:?}");

        println!("Step 3: no broker; {RECORDS} more records with 3 failures each...");
        alter_system(
            &db,
            "ALTER SYSTEM SET pg_kafka.shadow_bootstrap_servers = ''",
        )
        .await?;
        produce().await?;
        db.execute(
            "UPDATE kafka.shadow_tracking \
             SET retry_count = 3, forwarded_at = NOW() - interval '1 hour' \
             WHERE topic_id = $1 AND external_offset IS NULL",
            &[&topic_id],
        )
        .await?;
        println!("Step 4: lower the cap to 3 and configure the broker; nothing is forwarded...");
        alter_system(&db, "ALTER SYSTEM SET pg_kafka.shadow_max_retries = 3").await?;
        restore_bootstrap_servers(&db).await?;
        tokio::time::sleep(Duration::from_secs(2)).await;
        let left = undelivered(ctx.db(), topic_id).await?;
        if left != i64::try_from(RECORDS)? {
            return Err(format!("{left} of {RECORDS} rows at the lowered cap left pending").into());
        }

        println!("Step 5: replay them; they are forwarded...");
        db.execute(
            "SELECT kafka.replay_shadow_messages($1, 0, NULL)",
            &[&topic_id],
        )
        .await?;
        let took = drained_within(ctx.db(), topic_id, Duration::from_secs(30)).await?;
        println!("  forwarded in {took:?}");
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await
    .map_err(|e| e.to_string());

    if let Err(e) = alter_system(&db, "ALTER SYSTEM RESET pg_kafka.shadow_max_retries").await {
        eprintln!("⚠️  cleanup: failed to reset pg_kafka.shadow_max_retries: {e}");
    }
    if let Err(e) = restore_bootstrap_servers(&db).await {
        eprintln!("⚠️  cleanup: failed to restore pg_kafka.shadow_bootstrap_servers: {e}");
    }
    ctx.cleanup().await?;
    result?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// The outbox rows of `topic_id` that are not yet dead letters at `cap` or still have an
/// attempt open.
async fn not_dead_at(
    db: &Client,
    topic_id: i32,
    cap: i32,
) -> Result<i64, Box<dyn std::error::Error>> {
    Ok(db
        .query_one(
            "SELECT COUNT(*) FROM kafka.shadow_tracking \
             WHERE topic_id = $1 AND external_offset IS NULL \
               AND (retry_count < $2 OR attempt_open)",
            &[&topic_id, &cap],
        )
        .await?
        .get(0))
}

/// Wait until every outbox row of `topic_id` is a dead letter at `cap` (its failures
/// acked, no attempt open), then check that it stays one: `retry_count` exactly `cap`,
/// not delivered, and its `dead` flag set (the claim skips it by that flag).
async fn dead_letters_at(db: &Client, topic_id: i32, rows: usize, cap: i32) -> TestResult {
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let left = not_dead_at(db, topic_id, cap).await?;
        if left == 0 {
            break;
        }
        if Instant::now() > deadline {
            return Err(format!("{left} row(s) not dead letters at cap {cap} after 120 s").into());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // Long enough for several backoffs (100 ms base): a row still retried would show.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let row = db
        .query_one(
            "SELECT COUNT(*), \
                    COUNT(*) FILTER (WHERE retry_count = $2 AND NOT attempt_open \
                                     AND external_offset IS NULL), \
                    COUNT(*) FILTER (WHERE dead) \
             FROM kafka.shadow_tracking WHERE topic_id = $1",
            &[&topic_id, &cap],
        )
        .await?;
    let (all, at_cap, dead): (i64, i64, i64) = (row.get(0), row.get(1), row.get(2));
    println!("  {all} row(s): {at_cap} at retry_count {cap}, {dead} flagged dead");
    let rows = i64::try_from(rows)?;
    if all != rows || at_cap != rows {
        return Err(
            format!("{at_cap} of {all} rows ({rows} expected) are dead letters at {cap}").into(),
        );
    }
    if dead != rows {
        return Err(
            format!("{dead} of {rows} dead letters at cap {cap} have their dead flag").into(),
        );
    }
    Ok(())
}

/// Dead letters made by failure acks, while the worker runs: each failure ack that
/// reaches `pg_kafka.shadow_max_retries` flags its row dead in the statement that
/// counts it (the claim skips dead letters by that flag, and nothing else recomputes
/// it while the cap stays put). A reload that raises the cap revives them: they are
/// retried, fail again and are dead letters at the new cap; once the external topic
/// takes them and the cap is raised again they are forwarded. The records are larger
/// than the external topic's `max.message.bytes`, so every attempt fails.
pub async fn test_shadow_failure_acks_make_dead_letters() -> TestResult {
    const RECORDS: usize = 2;
    println!("=== Test: failure acks make dead letters ===\n");
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("shadow-dead-acks").await;
    let external = format!("{topic}-ext");
    verify_external_kafka_ready().await?;
    let admin = external_admin()?;
    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(10)));
    let db = create_db_client().await?;

    let result = async {
        let new_topic = NewTopic::new(&external, 1, TopicReplication::Fixed(1))
            .set("max.message.bytes", "3000");
        for r in admin.create_topics(&[new_topic], &opts).await? {
            r.map_err(|(t, e)| format!("create {t}: {e}"))?;
        }
        println!("Step 1: cap 2, backoff 100 ms; {RECORDS} records the external topic refuses...");
        alter_system(&db, "ALTER SYSTEM SET pg_kafka.shadow_max_retries = 2").await?;
        alter_system(
            &db,
            "ALTER SYSTEM SET pg_kafka.shadow_retry_backoff_ms = 100",
        )
        .await?;
        enable_shadow_mode(
            ctx.db(),
            &topic,
            &ShadowTopicConfig {
                external_topic_name: Some(external.clone()),
                ..config(ShadowMode::Shadow, SyncMode::Async)
            },
        )
        .await?;
        let topic_id = topic_id(ctx.db(), &topic).await?;
        let producer = bulk_producer()?;
        let big = vec![b'B'; 8_000];
        for i in 0..RECORDS {
            let key = format!("k{i}");
            producer
                .send(
                    FutureRecord::to(&topic).key(key.as_bytes()).payload(&big),
                    Duration::from_secs(10),
                )
                .await
                .map_err(|(e, _)| e)?;
        }

        println!("Step 2: every attempt fails; at 2 failures they are dead letters...");
        dead_letters_at(ctx.db(), topic_id, RECORDS, 2).await?;

        println!("Step 3: a reload raises the cap to 3: each is retried once more...");
        alter_system(&db, "ALTER SYSTEM SET pg_kafka.shadow_max_retries = 3").await?;
        dead_letters_at(ctx.db(), topic_id, RECORDS, 3).await?;

        println!("Step 4: the external topic takes them; the cap to 4: they are forwarded...");
        let alter = AlterConfig::new(ResourceSpecifier::Topic(&external))
            .set("max.message.bytes", "1048576");
        for r in admin.alter_configs(&[alter], &opts).await? {
            r.map_err(|(t, e)| format!("alter {t:?}: {e}"))?;
        }
        alter_system(&db, "ALTER SYSTEM SET pg_kafka.shadow_max_retries = 4").await?;
        let took = drained_within(ctx.db(), topic_id, Duration::from_secs(120)).await?;
        println!("  forwarded in {took:?}");
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await
    .map_err(|e| e.to_string());

    for reset in [
        "ALTER SYSTEM RESET pg_kafka.shadow_max_retries",
        "ALTER SYSTEM RESET pg_kafka.shadow_retry_backoff_ms",
    ] {
        if let Err(e) = alter_system(&db, reset).await {
            eprintln!("⚠️  cleanup: {reset} failed: {e}");
        }
    }
    ctx.cleanup().await?;
    let _ = admin.delete_topics(&[&external], &opts).await;
    result?;
    println!("✅ Test PASSED\n");
    Ok(())
}
