//! Worker lifecycle under extension DDL, old schemas, SIGTERM, lock waits and failed commits.

use crate::common::{get_bootstrap_servers, TestResult};
use crate::worker::*;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

// -------------------------------------------------------------------------------------
// Item 1 (+5): DROP / CREATE / ALTER EXTENSION UPDATE in a loop under load.
// -------------------------------------------------------------------------------------

/// Before: the worker locked shadow_tracking/shadow_config/transactions/... in its own
/// order while DROP EXTENSION locked the members in pg_depend order, and the two
/// deadlocked ("deadlock detected", seen in the server logs); after a DROP the worker
/// failed every request and every poll. Now every worker transaction first takes the
/// extension object lock (which DROP takes before any member) and the pg_kafka schema
/// advisory lock (which the upgrade script takes first), finds a dropped extension and
/// goes back to waiting for the schema.
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
    let log_from = log.as_ref().map(|l| l.end()).unwrap_or(0);

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
                        FutureRecord::to(&topic)
                            .key("k")
                            .payload(&format!("v{}", i)),
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
                .map_err(|e| format!("round {}: {}: {:?}", round, sql, e))?;
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
            "the server log reports {} deadlock(s) during the loop",
            n
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

// -------------------------------------------------------------------------------------
// Item 5: a new binary on the 0.0.0 schema.
// -------------------------------------------------------------------------------------

/// Before: on a 0.0.0 schema the worker's partition maintenance failed at start and
/// every sweep ("Partition maintenance failed ... message_partitions"), the retention
/// sweep failed every cycle (and `pg_kafka_run_retention_sweep()` failed outright), and
/// the metrics flush failed (no kafka.broker_api_stats). Now the worker detects the
/// schema version, logs one WARNING naming what is unavailable, runs the retention
/// sweep row by row on the unpartitioned table, and turns the features on once
/// `ALTER EXTENSION pg_kafka UPDATE` has run (at the next reload).
///
/// Asserts: on 0.0.0, produce/consume work, `pg_kafka_run_retention_sweep()` succeeds,
/// a restarted worker logs none of the per-cycle failures and the degrade WARNING at
/// most once per start; after the update and a reload, broker metrics are published
/// and a time partition covering now exists.
pub async fn test_old_schema_degrades_gracefully() -> TestResult {
    into_test_result(old_schema().await)
}

async fn old_schema() -> SendResult<()> {
    println!("=== Test: new binary on the 0.0.0 schema ===\n");
    let db = session().await?;
    ensure_old_version_available(&db).await?;
    wait_for_serving_listener(&db, None, Duration::from_secs(30)).await?;
    let log = ServerLog::open(&db).await?;
    let log_from = log.as_ref().map(|l| l.end()).unwrap_or(0);

    let result: SendResult<()> = async {
        db.batch_execute(
            "DROP EXTENSION pg_kafka CASCADE; CREATE EXTENSION pg_kafka VERSION '0.0.0';",
        )
        .await
        .map_err(boxed)?;
        let pid = wait_for_serving_listener(&db, None, Duration::from_secs(30)).await?;
        let topic = unique("old-schema");
        let producer = quick_producer("10000")?;
        for i in 0..20 {
            send(&producer, &topic, &format!("v{}", i)).await?;
        }
        let got = read_values(&topic, false, 20, Duration::from_secs(15)).await?;
        assert_eq!(got.len(), 20, "consume on the 0.0.0 schema");

        // The on-demand sweep runs on the unpartitioned table.
        db.query("SELECT * FROM pg_kafka_run_retention_sweep(0, 3600)", &[])
            .await
            .map_err(|e| format!("retention sweep on 0.0.0 failed: {:?}", e))?;

        // A worker started on the old schema (startup maintenance path).
        db.query("SELECT pg_terminate_backend($1)", &[&pid])
            .await
            .map_err(boxed)?;
        wait_for_serving_listener(&db, Some(pid), Duration::from_secs(30)).await?;
        for i in 0..20 {
            send(&producer, &topic, &format!("w{}", i)).await?;
            tokio::time::sleep(Duration::from_millis(100)).await;
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
                    "the worker on the 0.0.0 schema logged '{}'",
                    bad
                );
            }
            let degrade = text.matches("older pg_kafka schema").count();
            assert!(
                (1..=3).contains(&degrade),
                "expected the old-schema WARNING once per worker start, found {}",
                degrade
            );
            println!("✅ server log: one old-schema WARNING per start, no per-cycle failures");
        } else {
            println!("  (server log not readable; skipped the log assertions)");
        }

        // Upgrade in place; the next reload turns the features on.
        db.batch_execute("ALTER EXTENSION pg_kafka UPDATE")
            .await
            .map_err(boxed)?;
        db.batch_execute("SELECT pg_reload_conf()")
            .await
            .map_err(boxed)?;
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
                    "after ALTER EXTENSION UPDATE: broker_api_stats rows={}, leaf for now={}",
                    metrics, leaf_now
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

/// Before: pgrx's SIGTERM handler only set a flag the main loop checks between
/// requests, so a worker waiting on a lock (here: a row another session holds, with no
/// lock_timeout) could not be terminated: the broker stayed wedged until the lock was
/// released. Now SIGTERM during database work sets ProcDiePending, so the lock wait is
/// interrupted and the worker exits (and is restarted).
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
        println!("Step 1: worker {} waits on the held lock", pid);
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
    println!("✅ new worker {} serves", new_pid);
    Ok(())
}

/// Before: no lock_timeout was set for the worker, so one stuck lock (an idle-in-
/// transaction session holding a row the worker needs) blocked the single DB thread and
/// with it every request of every client. Now `pg_kafka.worker_lock_timeout_ms` bounds
/// each lock wait: the stuck request fails, the rest of the broker keeps serving.
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
        format!(
            "ListOffsets for another topic failed while one row was locked: {}",
            e
        )
    })?;
    println!(
        "Step 1: ListOffsets on {} answered in {:?} (low {}, high {})",
        other, took, low, high
    );
    assert_eq!(high, 1);
    println!(
        "Step 2: blocked produce finished: {:?}",
        blocked_result.is_ok()
    );
    send(&producer, &stuck, "after-release").await?;
    println!("✅ broker kept serving; the partition works after the lock is released");
    Ok(())
}

// -------------------------------------------------------------------------------------
// Item 6: a failing commit is answered, and the worker survives.
// -------------------------------------------------------------------------------------

/// Before: when a batch's COMMIT failed, the worker queued error responses and then
/// re-raised, so the process exited before the network thread could send them: the
/// client saw a dropped connection and the listener restarted. Now the worker aborts
/// the failed transaction, answers every request of the batch with an error and goes on.
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
        "CREATE TABLE IF NOT EXISTS public.w3_commit_fail (topic_id INT PRIMARY KEY);
         CREATE OR REPLACE FUNCTION public.w3_commit_fail_fn() RETURNS trigger
         LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
         BEGIN
             IF EXISTS (SELECT 1 FROM public.w3_commit_fail WHERE topic_id = NEW.topic_id) THEN
                 RAISE EXCEPTION 'w3 injected commit failure';
             END IF;
             RETURN NULL;
         END $$;
         DROP TRIGGER IF EXISTS w3_commit_fail ON kafka.partition_offsets;
         CREATE CONSTRAINT TRIGGER w3_commit_fail AFTER INSERT OR UPDATE ON kafka.partition_offsets
         DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.w3_commit_fail_fn();",
    )
    .await
    .map_err(boxed)?;
    db.execute(
        "INSERT INTO public.w3_commit_fail SELECT id FROM kafka.topics WHERE name = $1",
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
        println!(
            "Step 1: produce into the failing commit -> {:?} after {:?}",
            r, took
        );
        assert!(r.is_err(), "the produce must not be acknowledged");
        assert!(
            took < Duration::from_secs(5),
            "the error must be an answer, not a timeout ({:?})",
            took
        );
        let other = unique("commitfail-other");
        send(&producer, &other, "fine").await?;
        let now = listener_pid(&db).await?;
        assert_eq!(now, Some(pid), "the listener must not restart");
        println!(
            "✅ error answered in {:?}; listener {} still serving",
            took, pid
        );
        Ok(())
    }
    .await;
    db.batch_execute(
        "DROP TRIGGER IF EXISTS w3_commit_fail ON kafka.partition_offsets;
         DROP FUNCTION IF EXISTS public.w3_commit_fail_fn();
         DROP TABLE IF EXISTS public.w3_commit_fail;",
    )
    .await
    .map_err(boxed)?;
    result
}
