//! Worker lifecycle under extension DDL, old schemas, SIGTERM, lock waits and failed commits.

use crate::common::{get_bootstrap_servers, TestResult};
use crate::error_codes::KAFKA_STORAGE_ERROR;
use crate::members::Member;
use crate::wire::records::batch;
use crate::wire::Wire;
use crate::worker::{
    boxed, deadlocks, ensure_old_version_available, hold_partition_counter, into_test_result,
    listener_pid, quick_producer, read_values, send, session, set_guc, unique,
    wait_for_serving_listener, SendResult, ServerLog, WORKER_LOCK_TIMEOUT_GUC,
};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// DROP EXTENSION locks the extension's members in pg_depend order; a worker that locked
/// shadow_tracking, shadow_config, transactions, ... in its own order would deadlock with
/// it. Every worker transaction therefore first takes the extension object lock (which
/// DROP takes before any member) and the pg_kafka schema advisory lock (which the upgrade
/// script takes first), and a worker that finds the extension dropped goes back to
/// waiting for the schema instead of failing every request.
///
/// Asserts, over 8 rounds (alternating `CREATE EXTENSION` and `CREATE ... VERSION
/// '0.0.0'` + `ALTER EXTENSION UPDATE`) while producers (plain + idempotent), a group
/// consumer and shadow-outbox polling run: every DDL statement succeeds, neither the server
/// log nor pg_stat_database records a deadlock, the listener process survives (same pid), and afterwards a new
/// topic round-trips through rdkafka.
pub async fn test_extension_ddl_loop_under_load() -> TestResult {
    into_test_result(extension_ddl_loop().await)
}

async fn extension_ddl_loop() -> SendResult<()> {
    println!("=== Test: DROP/CREATE/ALTER EXTENSION loop under load ===\n");
    let db = session().await?;
    ensure_old_version_available(&db).await?;
    let pid_before = wait_for_serving_listener(&db, None, Duration::from_secs(30)).await?;
    let deadlocks_before = deadlocks(&db).await?;
    // The worker's own statistics are not flushed promptly (a background worker does
    // not report them between transactions), so the log is the reliable witness.
    let log = ServerLog::open(&db).await?;
    let log_from = log.as_ref().map_or(0, ServerLog::end);

    // Shadow mode on (no shadow topics, unreachable broker): the worker polls the
    // outbox every 250 ms, which is one of the lock orders that deadlocked.
    set_guc(&db, "pg_kafka.shadow_mode_enabled", Some("on")).await?;
    set_guc(
        &db,
        "pg_kafka.shadow_bootstrap_servers",
        Some("127.0.0.1:1"),
    )
    .await?;
    set_guc(&db, "pg_kafka.shadow_security_protocol", Some("PLAINTEXT")).await?;
    // Frequent config-reload transactions (shadow_config, shadow_metrics).
    set_guc(&db, "pg_kafka.config_reload_interval_ms", Some("100")).await?;

    let stop = Arc::new(AtomicBool::new(false));
    let acked = Arc::new(AtomicU64::new(0));
    let topic = unique("ddl-load");
    let mut tasks = Vec::new();
    for idempotent in [false, true] {
        let stop = stop.clone();
        let acked = acked.clone();
        let topic = topic.clone();
        tasks.push(tokio::spawn(async move {
            let producer: FutureProducer = ClientConfig::new()
                .set("bootstrap.servers", get_bootstrap_servers())
                .set("broker.address.family", "v4")
                .set("message.timeout.ms", "10000")
                .set(
                    "enable.idempotence",
                    if idempotent { "true" } else { "false" },
                )
                .create()
                .expect("producer");
            let mut i = 0u64;
            while !stop.load(Ordering::Relaxed) {
                i += 1;
                if producer
                    .send(
                        FutureRecord::to(&topic).key("k").payload(&format!("v{i}")),
                        Duration::from_secs(10),
                    )
                    .await
                    .is_ok()
                {
                    acked.fetch_add(1, Ordering::Relaxed);
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }));
    }
    {
        let stop = stop.clone();
        let topic = topic.clone();
        tasks.push(tokio::spawn(async move {
            let consumer: StreamConsumer = ClientConfig::new()
                .set("bootstrap.servers", get_bootstrap_servers())
                .set("broker.address.family", "v4")
                .set("group.id", unique("ddl-group"))
                .set("auto.offset.reset", "earliest")
                .set("enable.auto.commit", "true")
                .set("auto.commit.interval.ms", "200")
                .set("allow.auto.create.topics", "true")
                .create()
                .expect("consumer");
            // Closed off the runtime's threads when the task ends: the close may meet a
            // dropped extension, and the loop's checks do not depend on it.
            let consumer = Member::new(consumer);
            let _ = consumer.subscribe(&[&topic]);
            while !stop.load(Ordering::Relaxed) {
                let _ = tokio::time::timeout(Duration::from_millis(200), consumer.recv()).await;
            }
        }));
    }
    tokio::time::sleep(Duration::from_secs(2)).await;

    let result: SendResult<()> = async {
        for round in 0..8 {
            let sql = if round % 2 == 0 {
                "DROP EXTENSION pg_kafka CASCADE; CREATE EXTENSION pg_kafka;"
            } else {
                "DROP EXTENSION pg_kafka CASCADE; CREATE EXTENSION pg_kafka VERSION '0.0.0'; \
                 ALTER EXTENSION pg_kafka UPDATE;"
            };
            let start = Instant::now();
            db.batch_execute("SET statement_timeout = '60s'")
                .await
                .map_err(boxed)?;
            db.batch_execute(sql)
                .await
                .map_err(|e| format!("round {round}: {sql}: {e:?}"))?;
            println!("  round {}: DDL done in {:?}", round, start.elapsed());
            tokio::time::sleep(Duration::from_millis(1500)).await;
        }
        Ok(())
    }
    .await;

    stop.store(true, Ordering::Relaxed);
    for t in tasks {
        let _ = tokio::time::timeout(Duration::from_secs(15), t).await;
    }
    set_guc(&db, "pg_kafka.shadow_mode_enabled", None).await?;
    set_guc(&db, "pg_kafka.shadow_bootstrap_servers", None).await?;
    set_guc(&db, "pg_kafka.shadow_security_protocol", None).await?;
    set_guc(&db, "pg_kafka.config_reload_interval_ms", None).await?;
    result?;

    if let Some(log) = &log {
        let n = log.since(log_from).matches("deadlock detected").count();
        assert_eq!(
            n, 0,
            "the server log reports {n} deadlock(s) during the loop"
        );
        println!("✅ server log: no deadlock detected");
    }
    let deadlocks_after = deadlocks(&db).await?;
    assert_eq!(
        deadlocks_after, deadlocks_before,
        "DDL against the running worker produced deadlocks"
    );
    println!("✅ no deadlocks (pg_stat_database.deadlocks unchanged)");

    let pid_after = wait_for_serving_listener(&db, None, Duration::from_secs(30)).await?;
    assert_eq!(
        pid_after, pid_before,
        "the listener worker must survive DROP/CREATE EXTENSION in-process"
    );
    let fresh = unique("ddl-after");
    let producer = quick_producer("10000")?;
    send(&producer, &fresh, "after-loop").await?;
    let got = read_values(&fresh, false, 1, Duration::from_secs(15)).await?;
    assert_eq!(got, vec!["after-loop".to_string()]);
    println!(
        "✅ listener {} survived; {} records acked during the loop; new topic round-trips",
        pid_after,
        acked.load(Ordering::Relaxed)
    );
    Ok(())
}

/// A library newer than its schema (the 0.0.0 schema has no time partitions and no
/// kafka.broker_api_stats) must not fail its partition maintenance, retention sweep and
/// metrics flush on every cycle. The worker detects the older schema, logs one WARNING
/// naming `ALTER EXTENSION pg_kafka UPDATE`, runs no maintenance and serves no client
/// until the update (test_old_schema_refuses_clients_until_update covers the clients),
/// then turns everything on without a restart.
///
/// Asserts: on 0.0.0, `pg_kafka_run_retention_sweep()` succeeds from SQL; the worker,
/// also after a restart on the old schema, logs none of the per-cycle failures and the
/// old-schema WARNING at most once per start; after the update, the listener serves,
/// broker metrics are published and a time partition covering now exists.
pub async fn test_old_schema_degrades_gracefully() -> TestResult {
    into_test_result(old_schema().await)
}

async fn old_schema() -> SendResult<()> {
    println!("=== Test: new binary on the 0.0.0 schema ===\n");
    let db = session().await?;
    ensure_old_version_available(&db).await?;
    wait_for_serving_listener(&db, None, Duration::from_secs(30)).await?;
    let log = ServerLog::open(&db).await?;
    let log_from = log.as_ref().map_or(0, ServerLog::end);

    let result: SendResult<()> = async {
        db.batch_execute(
            "DROP EXTENSION pg_kafka CASCADE; CREATE EXTENSION pg_kafka VERSION '0.0.0';",
        )
        .await
        .map_err(boxed)?;
        let pid = listener_pid(&db)
            .await?
            .ok_or("no pg_kafka_listener on the 0.0.0 schema")?;
        // Requests keep arriving (and are refused) while the worker waits.
        let topic = unique("old-schema");
        let producer = quick_producer("3000")?;
        let _ = send(&producer, &topic, "refused").await;

        // The on-demand sweep runs on the unpartitioned table.
        db.query("SELECT * FROM pg_kafka_run_retention_sweep(0, 3600)", &[])
            .await
            .map_err(|e| format!("retention sweep on 0.0.0 failed: {e:?}"))?;

        // A worker started on the old schema (startup path).
        db.query("SELECT pg_terminate_backend($1)", &[&pid])
            .await
            .map_err(boxed)?;
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            match listener_pid(&db).await? {
                Some(p) if p != pid => break,
                _ if Instant::now() > deadline => {
                    return Err("no new pg_kafka_listener after pg_terminate_backend".into())
                }
                _ => tokio::time::sleep(Duration::from_millis(250)).await,
            }
        }
        for _ in 0..3 {
            let _ = send(&producer, &topic, "refused").await;
        }
        tokio::time::sleep(Duration::from_millis(1500)).await;

        if let Some(log) = &log {
            let text = log.since(log_from);
            for bad in [
                "Partition maintenance failed",
                "Partition maintenance panicked",
                "Retention sweep failed",
                "Retention-sweep maintenance panicked",
                "Failed to publish broker metrics",
            ] {
                assert!(
                    !text.contains(bad),
                    "the worker on the 0.0.0 schema logged '{bad}'"
                );
            }
            let degrade = text.matches("older pg_kafka schema").count();
            assert!(
                (1..=3).contains(&degrade),
                "expected the old-schema WARNING once per worker start, found {degrade}"
            );
            assert!(
                text.contains("ALTER EXTENSION pg_kafka UPDATE"),
                "the old-schema WARNING must name ALTER EXTENSION pg_kafka UPDATE"
            );
            println!("✅ server log: one old-schema WARNING per start, no per-cycle failures");
        } else {
            println!("  (server log not readable; skipped the log assertions)");
        }

        // Upgrade in place; the worker sees it within 500 ms and serves again.
        db.batch_execute("ALTER EXTENSION pg_kafka UPDATE")
            .await
            .map_err(boxed)?;
        db.batch_execute("SELECT pg_reload_conf()")
            .await
            .map_err(boxed)?;
        wait_for_serving_listener(&db, None, Duration::from_secs(20)).await?;
        let producer = quick_producer("10000")?;
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            send(&producer, &topic, "after-update").await?;
            let metrics: i64 = db
                .query_one("SELECT count(*) FROM kafka.broker_api_stats", &[])
                .await
                .map_err(boxed)?
                .get(0);
            let leaf_now: bool = db
                .query_one(
                    "SELECT EXISTS (SELECT 1 FROM kafka.message_partitions()
                                    WHERE NOT is_default AND lower_bound <= now()
                                      AND upper_bound > now())",
                    &[],
                )
                .await
                .map_err(boxed)?
                .get(0);
            if metrics > 0 && leaf_now {
                break;
            }
            if Instant::now() > deadline {
                return Err(format!(
                    "after ALTER EXTENSION UPDATE: broker_api_stats rows={metrics}, leaf for now={leaf_now}"
                )
                .into());
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        println!("✅ features enabled after ALTER EXTENSION UPDATE + reload");
        Ok(())
    }
    .await;

    // Leave a fresh current-version schema behind whatever happened.
    db.batch_execute("DROP EXTENSION IF EXISTS pg_kafka CASCADE; CREATE EXTENSION pg_kafka;")
        .await
        .map_err(boxed)?;
    wait_for_serving_listener(&db, None, Duration::from_secs(30)).await?;
    result
}

/// The 0.0.0 schema lacks tables the library's request paths use (kafka.producer_batches
/// for every idempotent Produce, kafka.txn_partitions and kafka.txn_groups for transactions,
/// kafka.sasl_credentials for SASL, shadow_tracking.attempt_open for the outbox), so a
/// library newer than its schema does not serve clients: it answers every request with
/// the retriable KAFKA_STORAGE_ERROR until `ALTER EXTENSION pg_kafka UPDATE`, and serves
/// once it sees the updated schema, without a restart.
///
/// Asserts: on 0.0.0, InitProducerId and an idempotent Produce (raw protocol) are answered
/// KAFKA_STORAGE_ERROR and nothing is stored; an rdkafka idempotent producer's record stays
/// unacknowledged (rdkafka retries); the server log names `ALTER EXTENSION pg_kafka
/// UPDATE`; after the update and a reload, that same record is acked at offset 0 and stored
/// once, a new idempotent producer produces, and the listener process is the same.
pub async fn test_old_schema_refuses_clients_until_update() -> TestResult {
    into_test_result(old_schema_refuses_clients().await)
}

async fn old_schema_refuses_clients() -> SendResult<()> {
    println!("=== Test: the 0.0.0 schema is refused until ALTER EXTENSION UPDATE ===\n");
    let db = session().await?;
    ensure_old_version_available(&db).await?;
    wait_for_serving_listener(&db, None, Duration::from_secs(30)).await?;
    let log = ServerLog::open(&db).await?;
    let log_from = log.as_ref().map_or(0, ServerLog::end);

    let result: SendResult<()> = async {
        db.batch_execute(
            "DROP EXTENSION pg_kafka CASCADE; CREATE EXTENSION pg_kafka VERSION '0.0.0';",
        )
        .await
        .map_err(boxed)?;
        let topic = unique("old-idem");
        db.execute(
            "INSERT INTO kafka.topics (name, partitions) VALUES ($1, 1)",
            &[&topic],
        )
        .await
        .map_err(boxed)?;
        let pid = listener_pid(&db)
            .await?
            .ok_or("no pg_kafka_listener on the 0.0.0 schema")?;
        tokio::time::sleep(Duration::from_secs(1)).await;

        println!("Step 1: raw InitProducerId and idempotent Produce on 0.0.0...");
        let mut wire = Wire::connect().await.map_err(|e| e.to_string())?;
        for _ in 0..3 {
            let (init_code, producer_id, epoch) = wire
                .init_producer_id(None, 60_000)
                .await
                .map_err(|e| e.to_string())?;
            // On a schema that served InitProducerId, produce as that producer.
            let (producer_id, epoch) = if init_code == 0 {
                (producer_id, epoch)
            } else {
                (1, 0)
            };
            let records = batch(&["raw"], producer_id, epoch, 0, false, None);
            let (produce_code, _) = wire
                .produce(&topic, 0, records, None)
                .await
                .map_err(|e| e.to_string())?;
            assert_eq!(
                (init_code, produce_code),
                (KAFKA_STORAGE_ERROR, KAFKA_STORAGE_ERROR),
                "(InitProducerId, idempotent Produce) on the 0.0.0 schema: \
                 expected KAFKA_STORAGE_ERROR for both"
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        println!("✅ InitProducerId and Produce answered KAFKA_STORAGE_ERROR\n");

        println!("Step 2: an rdkafka idempotent producer on 0.0.0...");
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", get_bootstrap_servers())
            .set("broker.address.family", "v4")
            .set("enable.idempotence", "true")
            .set("message.timeout.ms", "60000")
            .set("linger.ms", "0")
            .create()
            .map_err(boxed)?;
        let pending = {
            let producer = producer.clone();
            let topic = topic.clone();
            tokio::spawn(async move {
                producer
                    .send(
                        FutureRecord::to(&topic)
                            .key("k")
                            .payload("during-window")
                            .partition(0),
                        Duration::from_secs(60),
                    )
                    .await
                    .map(|(_, offset)| offset)
                    .map_err(|(e, _)| format!("{e:?}"))
            })
        };
        tokio::time::sleep(Duration::from_secs(4)).await;
        if pending.is_finished() {
            let outcome = pending.await.map_err(boxed)?;
            return Err(format!(
                "the idempotent send on the 0.0.0 schema finished before the update: {outcome:?}"
            )
            .into());
        }
        let stored: i64 = db
            .query_one(
                "SELECT count(*) FROM kafka.messages m JOIN kafka.topics t ON t.id = m.topic_id
                 WHERE t.name = $1",
                &[&topic],
            )
            .await
            .map_err(boxed)?
            .get(0);
        assert_eq!(stored, 0, "nothing may be stored on the 0.0.0 schema");
        println!("✅ the record is still pending (rdkafka retries), nothing stored\n");

        if let Some(log) = &log {
            let text = log.since(log_from);
            assert!(
                text.contains("older pg_kafka schema")
                    && text.contains("ALTER EXTENSION pg_kafka UPDATE")
                    && text.contains("KAFKA_STORAGE_ERROR"),
                "expected a WARNING that the old schema is refused until ALTER EXTENSION pg_kafka UPDATE"
            );
            println!("✅ server log names ALTER EXTENSION pg_kafka UPDATE\n");
        }

        println!("Step 3: ALTER EXTENSION pg_kafka UPDATE + reload...");
        db.batch_execute("ALTER EXTENSION pg_kafka UPDATE")
            .await
            .map_err(boxed)?;
        db.batch_execute("SELECT pg_reload_conf()")
            .await
            .map_err(boxed)?;
        let offset = tokio::time::timeout(Duration::from_secs(50), pending)
            .await
            .map_err(|_| "the pending idempotent send was not acked after the update")?
            .map_err(boxed)??;
        assert_eq!(offset, 0, "the retried record is the partition's first");
        let values: Vec<String> = db
            .query(
                "SELECT convert_from(m.value, 'UTF8') FROM kafka.messages m
                 JOIN kafka.topics t ON t.id = m.topic_id WHERE t.name = $1
                 ORDER BY m.partition_offset",
                &[&topic],
            )
            .await
            .map_err(boxed)?
            .iter()
            .map(|r| r.get(0))
            .collect();
        assert_eq!(values, vec!["during-window".to_string()], "stored once");
        println!("✅ the pending record was acked at offset 0 and stored once\n");

        let fresh: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", get_bootstrap_servers())
            .set("broker.address.family", "v4")
            .set("enable.idempotence", "true")
            .set("message.timeout.ms", "15000")
            .create()
            .map_err(boxed)?;
        let offset = send(&fresh, &topic, "after-update").await?;
        assert_eq!(offset, 1, "a new idempotent producer appends after the update");
        let pid_after = listener_pid(&db).await?;
        assert_eq!(pid_after, Some(pid), "served without a worker restart");
        println!("✅ a new idempotent producer produces; listener {pid} was not restarted");
        Ok(())
    }
    .await;

    db.batch_execute("DROP EXTENSION IF EXISTS pg_kafka CASCADE; CREATE EXTENSION pg_kafka;")
        .await
        .map_err(boxed)?;
    wait_for_serving_listener(&db, None, Duration::from_secs(30)).await?;
    result
}

/// pgrx's SIGTERM handler only sets a flag the main loop checks between requests, which
/// cannot end a worker waiting on a lock (here: a row another session holds, with no
/// lock_timeout). SIGTERM during database work therefore also sets ProcDiePending, so the
/// lock wait is interrupted and the worker exits (and is restarted).
///
/// Asserts: with the worker lock timeout disabled and a Produce blocked on a held row
/// lock (pg_stat_activity shows a Lock wait), `pg_terminate_backend` ends the worker
/// within 3 s while the lock is still held; a new worker then serves.
pub async fn test_sigterm_interrupts_a_worker_lock_wait() -> TestResult {
    into_test_result(sigterm_lock_wait().await)
}

async fn sigterm_lock_wait() -> SendResult<()> {
    println!("=== Test: SIGTERM interrupts a worker blocked on a lock ===\n");
    let db = session().await?;
    let holder = session().await?;
    let pid = wait_for_serving_listener(&db, None, Duration::from_secs(30)).await?;
    set_guc(&db, WORKER_LOCK_TIMEOUT_GUC, Some("0")).await?;
    let topic = unique("sigterm");
    let producer = quick_producer("30000")?;
    send(&producer, &topic, "create").await?;

    hold_partition_counter(&holder, &topic).await?;
    let blocked = {
        let producer = producer.clone();
        let topic = topic.clone();
        tokio::spawn(async move { send(&producer, &topic, "blocked").await.is_ok() })
    };
    let result: SendResult<()> = async {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let waiting: bool = db
                .query_one(
                    "SELECT COALESCE(bool_or(wait_event_type = 'Lock'), false)
                     FROM pg_stat_activity WHERE pid = $1",
                    &[&pid],
                )
                .await
                .map_err(boxed)?
                .get(0);
            if waiting {
                break;
            }
            if Instant::now() > deadline {
                return Err("the worker never waited on the held row lock".into());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        println!("Step 1: worker {pid} waits on the held lock");
        db.query("SELECT pg_terminate_backend($1)", &[&pid])
            .await
            .map_err(boxed)?;
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(3) {
            if listener_pid(&db).await? != Some(pid) {
                println!("Step 2: worker exited {:?} after SIGTERM", start.elapsed());
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Err("worker still alive 3 s after pg_terminate_backend while blocked on a lock".into())
    }
    .await;
    holder.batch_execute("ROLLBACK").await.map_err(boxed)?;
    set_guc(&db, WORKER_LOCK_TIMEOUT_GUC, None).await?;
    let _ = tokio::time::timeout(Duration::from_secs(40), blocked).await;
    let new_pid = wait_for_serving_listener(&db, Some(pid), Duration::from_secs(30)).await?;
    result?;
    println!("✅ new worker {new_pid} serves");
    Ok(())
}

/// One stuck lock (an idle-in-transaction session holding a row the worker needs) must
/// not block the single DB thread and with it every request of every client:
/// `pg_kafka.worker_lock_timeout_ms` bounds each lock wait, so the stuck request fails and
/// the rest of the broker keeps serving.
///
/// Asserts: with the timeout at 1 s and a Produce blocked on a held row lock, a
/// ListOffsets for another topic is answered within 4 s, and after the lock is released
/// the blocked partition accepts produces again.
pub async fn test_worker_lock_timeout_bounds_a_stuck_lock() -> TestResult {
    into_test_result(lock_timeout_bounds().await)
}

async fn lock_timeout_bounds() -> SendResult<()> {
    println!("=== Test: worker lock_timeout bounds a stuck lock ===\n");
    let db = session().await?;
    let holder = session().await?;
    wait_for_serving_listener(&db, None, Duration::from_secs(30)).await?;
    set_guc(&db, WORKER_LOCK_TIMEOUT_GUC, Some("1000")).await?;
    let stuck = unique("stuck");
    let other = unique("other");
    let producer = quick_producer("30000")?;
    send(&producer, &stuck, "create").await?;
    send(&producer, &other, "create").await?;

    hold_partition_counter(&holder, &stuck).await?;
    let blocked = {
        let producer = producer.clone();
        let stuck = stuck.clone();
        tokio::spawn(async move { send(&producer, &stuck, "blocked").await })
    };
    tokio::time::sleep(Duration::from_millis(500)).await;
    let other_c = other.clone();
    let watermarks = tokio::task::spawn_blocking(move || {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", get_bootstrap_servers())
            .set("broker.address.family", "v4")
            .set("group.id", unique("wm"))
            .create()
            .map_err(boxed)?;
        let start = Instant::now();
        let r = consumer.fetch_watermarks(&other_c, 0, Duration::from_secs(4));
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>((r.map_err(boxed), start.elapsed()))
    })
    .await
    .map_err(boxed)?;
    // Keep the lock a while longer, then release it.
    tokio::time::sleep(Duration::from_secs(2)).await;
    holder.batch_execute("ROLLBACK").await.map_err(boxed)?;
    let blocked_result = tokio::time::timeout(Duration::from_secs(40), blocked).await;
    set_guc(&db, WORKER_LOCK_TIMEOUT_GUC, None).await?;

    let (wm, took) = watermarks?;
    let (low, high) = wm.map_err(|e| {
        format!("ListOffsets for another topic failed while one row was locked: {e}")
    })?;
    println!("Step 1: ListOffsets on {other} answered in {took:?} (low {low}, high {high})");
    assert_eq!(high, 1);
    println!(
        "Step 2: blocked produce finished: {:?}",
        blocked_result.is_ok()
    );
    send(&producer, &stuck, "after-release").await?;
    println!("✅ broker kept serving; the partition works after the lock is released");
    Ok(())
}

/// A batch whose COMMIT fails must be answered: the worker aborts the failed
/// transaction, answers every request of the batch with an error and goes on, instead of
/// exiting before the network thread sends the responses (which the client would see as
/// a dropped connection, followed by a listener restart).
///
/// Asserts: with a deferred constraint trigger that fails at commit for one topic, a
/// produce (no retries) to that topic gets an error response within 5 s, the listener
/// pid is unchanged, and another produce succeeds right after.
pub async fn test_commit_failure_answered_without_restart() -> TestResult {
    into_test_result(commit_failure().await)
}

async fn commit_failure() -> SendResult<()> {
    println!("=== Test: commit failure is answered, worker survives ===\n");
    let db = session().await?;
    let pid = wait_for_serving_listener(&db, None, Duration::from_secs(30)).await?;
    let topic = unique("commitfail");
    let producer = quick_producer("10000")?;
    send(&producer, &topic, "create").await?;
    db.batch_execute(
        "CREATE TABLE IF NOT EXISTS public.e2e_commit_fail (topic_id INT PRIMARY KEY);
         CREATE OR REPLACE FUNCTION public.e2e_commit_fail_fn() RETURNS trigger
         LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
         BEGIN
             IF EXISTS (SELECT 1 FROM public.e2e_commit_fail WHERE topic_id = NEW.topic_id) THEN
                 RAISE EXCEPTION 'injected commit failure';
             END IF;
             RETURN NULL;
         END $$;
         DROP TRIGGER IF EXISTS e2e_commit_fail ON kafka.partition_offsets;
         CREATE CONSTRAINT TRIGGER e2e_commit_fail AFTER INSERT OR UPDATE ON kafka.partition_offsets
         DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.e2e_commit_fail_fn();",
    )
    .await
    .map_err(boxed)?;
    db.execute(
        "INSERT INTO public.e2e_commit_fail SELECT id FROM kafka.topics WHERE name = $1",
        &[&topic],
    )
    .await
    .map_err(boxed)?;

    let result: SendResult<()> = async {
        let no_retry: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", get_bootstrap_servers())
            .set("broker.address.family", "v4")
            .set("message.timeout.ms", "8000")
            .set("message.send.max.retries", "0")
            .set("enable.idempotence", "false")
            .create()
            .map_err(boxed)?;
        let start = Instant::now();
        let r = send(&no_retry, &topic, "doomed").await;
        let took = start.elapsed();
        println!("Step 1: produce into the failing commit -> {r:?} after {took:?}");
        assert!(r.is_err(), "the produce must not be acknowledged");
        assert!(
            took < Duration::from_secs(5),
            "the error must be an answer, not a timeout ({took:?})"
        );
        let other = unique("commitfail-other");
        send(&producer, &other, "fine").await?;
        let now = listener_pid(&db).await?;
        assert_eq!(now, Some(pid), "the listener must not restart");
        println!("✅ error answered in {took:?}; listener {pid} still serving");
        Ok(())
    }
    .await;
    db.batch_execute(
        "DROP TRIGGER IF EXISTS e2e_commit_fail ON kafka.partition_offsets;
         DROP FUNCTION IF EXISTS public.e2e_commit_fail_fn();
         DROP TABLE IF EXISTS public.e2e_commit_fail;",
    )
    .await
    .map_err(boxed)?;
    result
}
