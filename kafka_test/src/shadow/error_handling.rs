//! Error handling tests for shadow mode:
//! - DualWrite with the external broker down: the produce succeeds at once and
//!   is stored locally, and the record is forwarded once the broker is back;
//! - ExternalOnly with a forward that keeps failing: the record is
//!   dead-lettered and then served by local reads (the fallback);
//! - settings no producer can be built from fail each partition's head
//!   (counted, dead-lettered), not the records behind it, also after a working
//!   producer was retired by the change;
//! - a dead-lettered external-primary forward is served locally (RV-6).

use crate::common::{
    create_batch_producer, create_db_client, create_producer, create_read_committed_consumer,
    TestResult,
};
use crate::setup::TestContext;
use rdkafka::admin::{AdminOptions, NewTopic, TopicReplication};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use rdkafka::producer::FutureRecord;
use rdkafka::TopicPartitionList;
use std::time::{Duration, Instant};
use tokio_postgres::Client;

use super::assertions::assert_local_message_count;
use super::external_client::{
    count_external_messages, get_internal_bootstrap_servers, verify_external_kafka_ready,
};
use super::helpers::{enable_shadow_mode, ShadowMode, ShadowTopicConfig, SyncMode, WriteMode};
use super::pipelined::{external_admin, BrokerPause};

async fn alter_system(db: &Client, sql: &str) -> TestResult {
    db.execute(sql, &[]).await?;
    db.execute("SELECT pg_reload_conf()", &[]).await?;
    // The worker reloads on SIGHUP within ~100 ms; the forwarder reads the
    // reloaded config on its next record or settings check (~0.5 s).
    tokio::time::sleep(Duration::from_millis(700)).await;
    Ok(())
}

/// Runs `ALTER SYSTEM` statements that undo a test's settings when it drops, also when an
/// assert panics or the runner aborts the test at its timeout. They run on a thread of their
/// own with its own runtime and connection, and the drop waits for them, so they are in force
/// (reloaded) before the next test starts whatever state the test's runtime is in.
struct SystemReset(Vec<String>);

impl Drop for SystemReset {
    fn drop(&mut self) {
        let statements = std::mem::take(&mut self.0);
        let reset = std::thread::spawn(move || -> Result<(), String> {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| e.to_string())?;
            runtime.block_on(async {
                let db = create_db_client().await.map_err(|e| e.to_string())?;
                let mut failed = Vec::new();
                for sql in &statements {
                    if let Err(e) = db.batch_execute(sql).await {
                        failed.push(format!("{sql}: {e}"));
                    }
                }
                db.batch_execute("SELECT pg_reload_conf()")
                    .await
                    .map_err(|e| e.to_string())?;
                // As in alter_system: the worker and the forwarder pick the reload up.
                tokio::time::sleep(Duration::from_millis(700)).await;
                if failed.is_empty() {
                    Ok(())
                } else {
                    Err(failed.join("; "))
                }
            })
        });
        match reset.join() {
            Ok(Ok(())) => {}
            Ok(Err(e)) => eprintln!("⚠️  cleanup: {e}"),
            Err(_) => eprintln!("⚠️  cleanup: the reset panicked"),
        }
    }
}

async fn topic_id(db: &Client, topic: &str) -> Result<i32, Box<dyn std::error::Error>> {
    Ok(db
        .query_one("SELECT id FROM kafka.topics WHERE name = $1", &[&topic])
        .await?
        .get(0))
}

/// (rows, delivered, dead letters at `cap` with no attempt open) of a topic's outbox.
async fn outbox_state(
    db: &Client,
    topic_id: i32,
    cap: i32,
) -> Result<(i64, i64, i64), Box<dyn std::error::Error>> {
    let row = db
        .query_one(
            "SELECT COUNT(*), \
                    COUNT(*) FILTER (WHERE external_offset IS NOT NULL), \
                    COUNT(*) FILTER (WHERE external_offset IS NULL AND retry_count >= $2 \
                                     AND dead AND NOT attempt_open) \
             FROM kafka.shadow_tracking WHERE topic_id = $1",
            &[&topic_id, &cap],
        )
        .await?;
    Ok((row.get(0), row.get(1), row.get(2)))
}

/// DualWrite with the external broker down (paused): the produce is acked at
/// once and stored locally, its outbox row stays undelivered while the broker
/// is down, and the record reaches the external topic once it is back.
///
/// The pause and resume are `EXTERNAL_KAFKA_PAUSE_CMD` / `EXTERNAL_KAFKA_RESUME_CMD`
/// (by default `docker pause` / `docker unpause external_kafka`).
pub async fn test_dual_write_external_down() -> TestResult {
    println!("=== Test: DualWrite with External Kafka Down ===\n");
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("dual-write-down").await;
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
    let topic_id = topic_id(ctx.db(), &topic).await?;

    let mut pause = None;
    let result = async {
        println!("Step 1: pause the external broker");
        pause = Some(BrokerPause::new()?);

        println!("Step 2: produce while it is down");
        let producer = create_producer()?;
        let started = Instant::now();
        producer
            .send(
                FutureRecord::to(&topic)
                    .key("dual-write-down-key")
                    .payload("dual-write-down-value"),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(e, _)| e)?;
        let took = started.elapsed();
        println!("  acked in {took:?}");
        assert!(
            took < Duration::from_secs(2),
            "a DualWrite produce must not wait for the external broker ({took:?})"
        );
        assert_local_message_count(ctx.db(), &topic, 1).await?;

        println!("Step 3: the outbox row stays undelivered while the broker is down");
        tokio::time::sleep(Duration::from_secs(3)).await;
        let (rows, delivered, _) = outbox_state(ctx.db(), topic_id, i32::MAX).await?;
        println!("  {rows} outbox row(s), {delivered} delivered");
        assert_eq!(rows, 1, "the produce must write one outbox row");
        assert_eq!(delivered, 0, "nothing can be delivered to a paused broker");

        println!("Step 4: resume the broker; the record is forwarded");
        if let Some(p) = pause.take() {
            p.resume()?;
        }
        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            let (_, delivered, _) = outbox_state(ctx.db(), topic_id, i32::MAX).await?;
            if delivered == 1 {
                break;
            }
            if Instant::now() > deadline {
                return Err("the record was not forwarded within 90 s of the resume".into());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let external = count_external_messages(&topic, Duration::from_secs(10)).await?;
        println!("  {external} record(s) on the external topic");
        assert!(external >= 1, "the record must reach the external topic");
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await
    .map_err(|e| e.to_string());
    if let Some(p) = pause.take() {
        let _ = p.resume();
    }
    ctx.cleanup().await?;
    result?;
    println!("✅ Test PASSED: DualWrite succeeds locally when external is down\n");
    Ok(())
}

/// ExternalOnly (external-primary) with a forward that keeps failing: the
/// external topic refuses the record (larger than its `max.message.bytes`), so
/// every attempt fails; at `pg_kafka.shadow_max_retries` it is a dead letter,
/// and from then on the topic is served by local reads, so the record is not
/// readable nowhere.
pub async fn test_external_only_fallback() -> TestResult {
    const CAP: i32 = 2;
    println!("=== Test: ExternalOnly Fallback to Local ===\n");
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("external-only-fallback").await;
    let external = format!("{topic}-ext");
    verify_external_kafka_ready().await?;
    let admin = external_admin()?;
    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(10)));
    let db = create_db_client().await?;
    let reset = SystemReset(vec![
        "ALTER SYSTEM RESET pg_kafka.shadow_max_retries".to_string(),
        "ALTER SYSTEM RESET pg_kafka.shadow_retry_backoff_ms".to_string(),
    ]);

    let result = async {
        let new_topic = NewTopic::new(&external, 1, TopicReplication::Fixed(1))
            .set("max.message.bytes", "3000");
        for r in admin.create_topics(&[new_topic], &opts).await? {
            r.map_err(|(t, e)| format!("create {t}: {e}"))?;
        }
        println!("Step 1: cap {CAP}, backoff 10 ms; an ExternalOnly topic");
        alter_system(
            &db,
            &format!("ALTER SYSTEM SET pg_kafka.shadow_max_retries = {CAP}"),
        )
        .await?;
        alter_system(
            &db,
            "ALTER SYSTEM SET pg_kafka.shadow_retry_backoff_ms = 10",
        )
        .await?;
        enable_shadow_mode(
            ctx.db(),
            &topic,
            &ShadowTopicConfig {
                mode: ShadowMode::Shadow,
                forward_percentage: 100,
                external_topic_name: Some(external.clone()),
                sync_mode: SyncMode::Async,
                write_mode: WriteMode::ExternalOnly,
            },
        )
        .await?;
        let topic_id = topic_id(ctx.db(), &topic).await?;

        println!("Step 2: produce a record the external topic refuses");
        let payload = vec![b'F'; 8_000];
        let producer = create_producer()?;
        producer
            .send(
                FutureRecord::to(&topic)
                    .key("fallback-key")
                    .payload(&payload),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(e, _)| e)?;
        assert_local_message_count(ctx.db(), &topic, 1).await?;

        println!("Step 3: every attempt fails; it becomes a dead letter");
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let (rows, delivered, dead) = outbox_state(ctx.db(), topic_id, CAP).await?;
            if rows == 1 && dead == 1 {
                break;
            }
            assert_eq!(delivered, 0, "the external topic must refuse the record");
            if Instant::now() > deadline {
                return Err(
                    format!("no dead letter within 60 s ({rows} row(s), {dead} dead)").into(),
                );
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let error: Option<String> = ctx
            .db()
            .query_one(
                "SELECT error_message FROM kafka.shadow_tracking WHERE topic_id = $1",
                &[&topic_id],
            )
            .await?
            .get(0);
        println!("  dead letter, last error: {error:?}");
        assert!(error.is_some(), "a failed attempt records its error");

        println!("Step 4: a pg_kafka consumer reads the record locally");
        let consumer: BaseConsumer =
            create_read_committed_consumer(&format!("g-fallback-{topic}"))?;
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(&topic, 0, rdkafka::Offset::Beginning)?;
        consumer.assign(&tpl)?;
        let deadline = Instant::now() + Duration::from_secs(15);
        let got = loop {
            if let Some(m) = consumer.poll(Duration::from_millis(300)) {
                break m?.payload().map(<[u8]>::to_vec);
            }
            if Instant::now() > deadline {
                return Err("the dead-lettered record was not served locally".into());
            }
        };
        assert_eq!(
            got.as_deref(),
            Some(&payload[..]),
            "the local copy must be the produced record"
        );
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await
    .map_err(|e| e.to_string());

    drop(reset);
    ctx.cleanup().await?;
    let _ = admin.delete_topics(&[&external], &opts).await;
    result?;
    println!("✅ Test PASSED: ExternalOnly Fallback\n");
    Ok(())
}

/// Settings no producer can be built from, set while a working producer holds
/// the records: the change retires that producer (its records come back
/// uncounted, as they should), and from then on the record each partition waits
/// on (its head) is a failed attempt, so it reaches the retry cap and is
/// dead-lettered with the error recorded, and the next record takes its place.
/// The records behind a head are not charged: they were all answered as failed
/// attempts, so with the default backoff every claimed row was a dead letter
/// within a minute of one bad setting, and the rest of the backlog followed
/// claim by claim.
pub async fn test_shadow_unbuildable_settings_dead_letter_after_a_working_producer() -> TestResult {
    const RECORDS: usize = 500;
    const CAP: i32 = 3;
    /// Dead letters to wait for: several heads in turn reach the cap.
    const DEAD: i64 = 4;
    println!("=== Test: unbuildable shadow settings after a working producer ===\n");
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("shadow-unbuildable").await;
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
    let topic_id = topic_id(ctx.db(), &topic).await?;
    let db = create_db_client().await?;
    let reset = SystemReset(vec![
        "ALTER SYSTEM SET pg_kafka.shadow_security_protocol = 'PLAINTEXT'".to_string(),
        format!(
            "ALTER SYSTEM SET pg_kafka.shadow_bootstrap_servers = '{}'",
            get_internal_bootstrap_servers()
        ),
        "ALTER SYSTEM RESET pg_kafka.shadow_max_retries".to_string(),
        "ALTER SYSTEM RESET pg_kafka.shadow_retry_backoff_ms".to_string(),
    ]);

    let result = async {
        println!("Step 1: a valid producer for an unreachable broker; {RECORDS} records");
        alter_system(
            &db,
            "ALTER SYSTEM SET pg_kafka.shadow_bootstrap_servers = '127.0.0.1:1'",
        )
        .await?;
        let producer = create_batch_producer()?;
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
        // Every row claimed and handed to the producer (attempt open).
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let open: i64 = ctx
                .db()
                .query_one(
                    "SELECT COUNT(*) FROM kafka.shadow_tracking \
                     WHERE topic_id = $1 AND attempt_open",
                    &[&topic_id],
                )
                .await?
                .get(0);
            if open == i64::try_from(RECORDS)? {
                break;
            }
            if Instant::now() > deadline {
                return Err(format!("only {open} of {RECORDS} rows in flight after 60 s").into());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        println!("Step 2: cap {CAP}, backoff 10 ms, security protocol 'BOGUS'");
        alter_system(
            &db,
            &format!("ALTER SYSTEM SET pg_kafka.shadow_max_retries = {CAP}"),
        )
        .await?;
        alter_system(
            &db,
            "ALTER SYSTEM SET pg_kafka.shadow_retry_backoff_ms = 10",
        )
        .await?;
        alter_system(
            &db,
            "ALTER SYSTEM SET pg_kafka.shadow_security_protocol = 'BOGUS'",
        )
        .await?;

        println!("Step 3: each partition's head fails until it is a dead letter at {CAP}");
        let rows = i64::try_from(RECORDS)?;
        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            let (all, _, dead) = outbox_state(ctx.db(), topic_id, CAP).await?;
            assert_eq!(all, rows);
            if dead >= DEAD {
                break;
            }
            if Instant::now() > deadline {
                return Err(
                    format!("after 90 s only {dead} of {all} rows are dead letters").into(),
                );
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        let row = ctx
            .db()
            .query_one(
                "SELECT COUNT(*) FILTER (WHERE dead), \
                        COUNT(*) FILTER (WHERE dead \
                          AND error_message LIKE '%invalid shadow configuration%'), \
                        COUNT(*) FILTER (WHERE NOT dead AND retry_count > 0), \
                        COUNT(*) FILTER (WHERE external_offset IS NOT NULL), \
                        COUNT(*) FILTER (WHERE dead AND EXISTS ( \
                            SELECT 1 FROM kafka.shadow_tracking lo \
                            WHERE lo.topic_id = st.topic_id \
                              AND lo.partition_id = st.partition_id \
                              AND lo.local_offset < st.local_offset AND NOT lo.dead)), \
                        (SELECT COUNT(DISTINCT partition_id) FROM kafka.shadow_tracking \
                         WHERE topic_id = $1) \
                 FROM kafka.shadow_tracking st WHERE topic_id = $1",
                &[&topic_id],
            )
            .await?;
        let (dead, with_error, charged, delivered, out_of_order, partitions): (
            i64,
            i64,
            i64,
            i64,
            i64,
            i64,
        ) = (
            row.get(0),
            row.get(1),
            row.get(2),
            row.get(3),
            row.get(4),
            row.get(5),
        );
        println!(
            "  {dead} dead letters ({with_error} record the configuration error), \
             {charged} live rows charged an attempt, {partitions} partition(s)"
        );
        assert_eq!(with_error, dead, "every dead letter records why it failed");
        assert_eq!(
            out_of_order, 0,
            "the dead letters are their partitions' heads"
        );
        assert!(
            charged <= partitions,
            "only each partition's head is charged, not the rows behind it"
        );
        assert!(
            dead < rows / 2,
            "the backlog is not dead-lettered wholesale"
        );
        assert_eq!(delivered, 0, "nothing can have been delivered");
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await
    .map_err(|e| e.to_string());

    drop(reset);
    ctx.cleanup().await?;
    result?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// RV-6 regression: a dead-lettered external-primary forward must fall back to local
/// reads (default), so a record produced to an external-primary topic is never
/// "readable nowhere". A healthy external-primary topic suppresses local reads; once
/// a forward exhausts the retry cap (never delivered externally) the record is served
/// locally instead.
pub async fn test_external_primary_dead_letter_serves_local() -> TestResult {
    println!("=== Test: External-Primary Dead-Letter Serves Local (RV-6) ===\n");

    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("ext-primary-deadletter").await;

    enable_shadow_mode(
        ctx.db(),
        &topic,
        &ShadowTopicConfig {
            mode: ShadowMode::Shadow,
            forward_percentage: 100,
            external_topic_name: None,
            sync_mode: SyncMode::Async,
            write_mode: WriteMode::ExternalOnly,
        },
    )
    .await?;

    // Produce a record. External-primary always writes locally (only reads suppress).
    let producer = create_producer()?;
    producer
        .send(
            FutureRecord::to(&topic).key("k").payload("deadletter"),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(e, _)| e)?;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_local_message_count(ctx.db(), &topic, 1).await?;

    let topic_id: i32 = ctx
        .db()
        .query_one("SELECT id FROM kafka.topics WHERE name = $1", &[&topic])
        .await?
        .get(0);

    // Control: while the forward is not dead-lettered, an external-primary topic
    // suppresses local reads (the consumer sees nothing).
    let consumer: BaseConsumer = create_read_committed_consumer(&format!("g-deadletter-{topic}"))?;
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(&topic, 0, rdkafka::Offset::Beginning)?;
    consumer.assign(&tpl)?;
    let mut suppressed = true;
    for _ in 0..4 {
        if let Some(Ok(_)) = consumer.poll(Duration::from_millis(250)) {
            suppressed = false;
            break;
        }
    }
    assert!(
        suppressed,
        "a healthy external-primary topic must suppress local reads"
    );

    // Let the first forward's ack land before forcing the dead letter: an ack applied
    // after the UPDATE would set external_offset again and keep the topic suppressed.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let acked: i64 = ctx
            .db()
            .query_one(
                "SELECT count(*) FROM kafka.shadow_tracking \
                 WHERE topic_id = $1 AND external_offset IS NOT NULL",
                &[&topic_id],
            )
            .await?
            .get(0);
        if acked == 1 {
            break;
        }
        if Instant::now() >= deadline {
            return Err("the external forward was not acknowledged within 10 s".into());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Force the forward into the dead-letter state (retry cap exhausted, external
    // delivery abandoned): retry_count >= pg_kafka.shadow_max_retries, still un-forwarded.
    ctx.db()
        .execute(
            "UPDATE kafka.shadow_tracking SET external_offset = NULL, \
             retry_count = current_setting('pg_kafka.shadow_max_retries')::int WHERE topic_id = $1",
            &[&topic_id],
        )
        .await?;

    // Now the record must be served locally (reads un-suppressed).
    let mut received = false;
    for _ in 0..20 {
        if let Some(Ok(_)) = consumer.poll(Duration::from_millis(300)) {
            received = true;
            break;
        }
    }
    assert!(
        received,
        "a dead-lettered external-primary record must be served locally (RV-6)"
    );

    ctx.cleanup().await?;
    println!("✅ External-primary dead-letter serves local test PASSED\n");
    Ok(())
}
