//! The `w3_worker` E2E tests (see the module docs in `mod.rs`).

use super::*;
use crate::common::create_transactional_producer;
use rdkafka::consumer::{BaseConsumer, Consumer, StreamConsumer};
use rdkafka::producer::Producer;
use rdkafka::{Message, Offset, TopicPartitionList};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

const WORKER_LOCK_TIMEOUT_GUC: &str = "pg_kafka.worker_lock_timeout_ms";

fn unique(base: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("w3-worker-{}-{}", base, nanos % 1_000_000_000_000)
}

async fn send(producer: &FutureProducer, topic: &str, value: &str) -> DynResult<i64> {
    producer
        .send(
            FutureRecord::to(topic).key("k").payload(value).partition(0),
            Duration::from_secs(30),
        )
        .await
        .map(|(_, offset)| offset)
        .map_err(|(e, _)| boxed(e))
}

fn assign_consumer(
    topic: &str,
    read_committed: bool,
    extra: &[(&str, &str)],
) -> DynResult<StreamConsumer> {
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", get_bootstrap_servers())
        .set("broker.address.family", "v4")
        .set("group.id", unique("assign"))
        .set("enable.auto.commit", "false")
        .set(
            "isolation.level",
            if read_committed {
                "read_committed"
            } else {
                "read_uncommitted"
            },
        );
    for (k, v) in extra {
        cfg.set(*k, *v);
    }
    let consumer: StreamConsumer = cfg.create().map_err(boxed)?;
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(topic, 0, Offset::Beginning)
        .map_err(boxed)?;
    consumer.assign(&tpl).map_err(boxed)?;
    Ok(consumer)
}

/// Read `n` values from partition 0 of `topic` (read_committed or not), within `within`.
async fn read_values(
    topic: &str,
    read_committed: bool,
    n: usize,
    within: Duration,
) -> DynResult<Vec<String>> {
    let consumer = assign_consumer(topic, read_committed, &[])?;
    let deadline = Instant::now() + within;
    let mut out = Vec::new();
    while out.len() < n && Instant::now() < deadline {
        if let Ok(Ok(m)) = tokio::time::timeout(Duration::from_millis(500), consumer.recv()).await {
            out.push(String::from_utf8_lossy(m.payload().unwrap_or_default()).into_owned());
        }
    }
    Ok(out)
}

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
pub async fn test_w3_extension_ddl_loop_under_load() -> TestResult {
    into_test_result(extension_ddl_loop().await)
}

async fn extension_ddl_loop() -> DynResult<()> {
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

    let result: DynResult<()> = async {
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
pub async fn test_w3_old_schema_degrades_gracefully() -> TestResult {
    into_test_result(old_schema().await)
}

async fn old_schema() -> DynResult<()> {
    println!("=== Test: new binary on the 0.0.0 schema ===\n");
    let db = session().await?;
    ensure_old_version_available(&db).await?;
    wait_for_serving_listener(&db, None, Duration::from_secs(30)).await?;
    let log = ServerLog::open(&db).await?;
    let log_from = log.as_ref().map(|l| l.end()).unwrap_or(0);

    let result: DynResult<()> = async {
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

// -------------------------------------------------------------------------------------
// Item 3: SIGTERM interrupts a lock wait; lock_timeout bounds a stuck lock.
// -------------------------------------------------------------------------------------

/// Hold the counter row of partition 0 of `topic` (FOR UPDATE) in an open transaction
/// on `holder`: a Produce to that partition then waits on the row lock.
async fn hold_partition_counter(holder: &Client, topic: &str) -> DynResult<()> {
    holder.batch_execute("BEGIN").await.map_err(boxed)?;
    let n = holder
        .query(
            "SELECT 1 FROM kafka.partition_offsets po JOIN kafka.topics t ON t.id = po.topic_id
             WHERE t.name = $1 AND po.partition_id = 0 FOR UPDATE OF po",
            &[&topic],
        )
        .await
        .map_err(boxed)?
        .len();
    if n != 1 {
        return Err(format!(
            "expected one partition_offsets row for {}, got {}",
            topic, n
        )
        .into());
    }
    Ok(())
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
pub async fn test_w3_sigterm_interrupts_lock_wait() -> TestResult {
    into_test_result(sigterm_lock_wait().await)
}

async fn sigterm_lock_wait() -> DynResult<()> {
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
    let result: DynResult<()> = async {
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
pub async fn test_w3_lock_timeout_bounds_stuck_lock() -> TestResult {
    into_test_result(lock_timeout_bounds().await)
}

async fn lock_timeout_bounds() -> DynResult<()> {
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
// Item 4: partition-maintenance races.
// -------------------------------------------------------------------------------------

/// (a) Before: the maintenance move (DELETE from kafka.messages_default + INSERT into
/// the new leaf) ran concurrently with EndTxn's `UPDATE kafka.messages SET txn_state`;
/// the UPDATE waited on the deleted rows, skipped them once the move committed and never
/// saw their copies, so a committed transaction's rows stayed `pending` forever (and the
/// LSO stuck). Now the move takes the pg_kafka schema lock exclusively and every worker
/// transaction takes it shared first, so they never overlap.
///
/// Asserts: while a session holds the schema lock in SHARE mode, maintenance does not
/// move rows out of DEFAULT (before: it moved them regardless of open worker
/// transactions); and with pending rows backdated into the DEFAULT leaf, an open session
/// that has just moved them (maintenance, uncommitted), a commit_transaction that arrives meanwhile:
/// after the session commits, the commit succeeds, no row of the transaction is left
/// pending and a read_committed consumer reads all of them.
pub async fn test_w3_end_txn_during_default_leaf_move() -> TestResult {
    into_test_result(end_txn_during_move().await)
}

async fn end_txn_during_move() -> DynResult<()> {
    println!("=== Test: EndTxn while maintenance moves rows out of DEFAULT ===\n");
    let db = session().await?;
    let mover = session().await?;
    wait_for_serving_listener(&db, None, Duration::from_secs(30)).await?;
    let topic = unique("endtxn-move");
    let txn_id = unique("endtxn-move-txn");
    let producer = create_transactional_producer(&txn_id).map_err(boxed)?;
    {
        let p = producer.clone();
        tokio::task::spawn_blocking(move || p.init_transactions(Duration::from_secs(10)))
            .await
            .map_err(boxed)?
            .map_err(boxed)?;
    }
    producer.begin_transaction().map_err(boxed)?;
    for i in 0..5 {
        send(&producer, &topic, &format!("t{}", i)).await?;
    }
    // Backdate the pending rows 100 partition widths: no leaf covers that time, so they
    // land in kafka.messages_default.
    db.execute(
        "UPDATE kafka.messages m
         SET created_at = m.created_at - 100 * current_setting('pg_kafka.message_partition_minutes')::int
                                         * INTERVAL '1 minute'
         FROM kafka.topics t WHERE t.id = m.topic_id AND t.name = $1",
        &[&topic],
    )
    .await
    .map_err(boxed)?;
    let in_default: i64 = db
        .query_one(
            "SELECT count(*) FROM kafka.messages_default d JOIN kafka.topics t ON t.id = d.topic_id
             WHERE t.name = $1 AND d.txn_state = 'pending'",
            &[&topic],
        )
        .await
        .map_err(boxed)?
        .get(0);
    assert_eq!(
        in_default, 5,
        "the pending rows must sit in the DEFAULT leaf"
    );

    // The move respects the schema lock: while a session holds it in SHARE mode (as
    // every worker transaction does), maintenance leaves the rows where they are.
    let share = session().await?;
    share.batch_execute("BEGIN").await.map_err(boxed)?;
    share
        .query(
            "SELECT pg_advisory_xact_lock_shared(8099547369649367906)",
            &[],
        )
        .await
        .map_err(boxed)?;
    let t = Instant::now();
    let blocked_move = mover
        .query_one("SELECT pg_kafka_run_partition_maintenance()", &[])
        .await;
    share.batch_execute("ROLLBACK").await.map_err(boxed)?;
    blocked_move.map_err(boxed)?;
    let still_default: i64 = db
        .query_one(
            "SELECT count(*) FROM kafka.messages_default d JOIN kafka.topics t ON t.id = d.topic_id
             WHERE t.name = $1",
            &[&topic],
        )
        .await
        .map_err(boxed)?
        .get(0);
    assert_eq!(
        still_default, 5,
        "maintenance moved rows out of DEFAULT while a worker transaction could be open"
    );
    println!(
        "Step 0: with the schema lock held, maintenance returned in {:?} and moved nothing",
        t.elapsed()
    );

    mover.batch_execute("BEGIN").await.map_err(boxed)?;
    let created: i64 = mover
        .query_one("SELECT pg_kafka_run_partition_maintenance()", &[])
        .await
        .map_err(boxed)?
        .get(0);
    println!(
        "Step 1: maintenance (uncommitted) created {} leaf/leaves",
        created
    );

    let committing = {
        let p = producer.clone();
        tokio::task::spawn_blocking(move || p.commit_transaction(Duration::from_secs(30)))
    };
    tokio::time::sleep(Duration::from_millis(1500)).await;
    mover.batch_execute("COMMIT").await.map_err(boxed)?;
    committing.await.map_err(boxed)?.map_err(boxed)?;
    println!("Step 2: commit_transaction returned OK");

    let pending: i64 = db
        .query_one(
            "SELECT count(*) FROM kafka.messages m JOIN kafka.topics t ON t.id = m.topic_id
             WHERE t.name = $1 AND m.txn_state = 'pending'",
            &[&topic],
        )
        .await
        .map_err(boxed)?
        .get(0);
    let leaf: Option<String> = db
        .query_opt(
            "SELECT DISTINCT m.tableoid::regclass::text FROM kafka.messages m
             JOIN kafka.topics t ON t.id = m.topic_id WHERE t.name = $1",
            &[&topic],
        )
        .await
        .map_err(boxed)?
        .map(|r| r.get(0));
    let got = read_values(&topic, true, 5, Duration::from_secs(10)).await?;

    // Clean up: the topic and the ancient leaf it now has to itself.
    db.execute("DELETE FROM kafka.topics WHERE name = $1", &[&topic])
        .await
        .map_err(boxed)?;
    if let Some(leaf) = &leaf {
        if leaf != "kafka.messages_default" {
            let _ = db
                .query(
                    "SELECT * FROM kafka.drop_message_partition($1::text::regclass, '{}')",
                    &[leaf],
                )
                .await;
        }
    }

    assert_eq!(
        pending, 0,
        "committed transaction left {} row(s) pending (moved by concurrent maintenance)",
        pending
    );
    assert_eq!(
        got.len(),
        5,
        "read_committed consumer sees the committed rows"
    );
    println!(
        "✅ no pending rows; read_committed reads all 5 (leaf {:?})",
        leaf
    );
    Ok(())
}

/// (b) ensure_message_partitions took its advisory lock with no timeout, so a session
/// that had called maintenance and stayed open stalled the next pass (the worker's
/// included) indefinitely. (c) drop_message_partition's advisory lock was outside its
/// lock_not_available handler: a 2 s timeout ERRORed and rolled back the whole sweep.
/// (d) when a concurrent sweep dropped the same leaf first, the waiter errored on the
/// vanished OID. Now all lock waits are bounded and handled, and a vanished leaf is
/// "not dropped", not an error.
///
/// Asserts: with the maintenance advisory lock held by another session, manual
/// maintenance returns within 6 s, `drop_message_partition` returns dropped=false
/// without an error and a retention sweep succeeds; and a drop that waits behind
/// another session's drop of the same leaf returns dropped=false after it commits.
pub async fn test_w3_partition_maintenance_lock_waits_are_bounded() -> TestResult {
    into_test_result(maintenance_lock_waits().await)
}

async fn maintenance_lock_waits() -> DynResult<()> {
    println!("=== Test: bounded lock waits in partition maintenance ===\n");
    let db = session().await?;
    let other = session().await?;
    db.batch_execute("SET statement_timeout = '20s'")
        .await
        .map_err(boxed)?;
    // An empty, long-past leaf for the drop paths.
    let leaf: String = {
        db.query_one(
            "SELECT kafka.create_message_partition('2001-01-01 00:00+00', '2001-01-02 00:00+00')",
            &[],
        )
        .await
        .map_err(boxed)?;
        db.query_one(
            "SELECT partition_oid::regclass::text FROM kafka.message_partitions()
             WHERE lower_bound = '2001-01-01 00:00+00'",
            &[],
        )
        .await
        .map_err(boxed)?
        .get(0)
    };
    println!("Step 0: test leaf {}", leaf);

    other.batch_execute("BEGIN").await.map_err(boxed)?;
    other
        .query("SELECT pg_advisory_xact_lock(8099547369649367905)", &[])
        .await
        .map_err(boxed)?;
    let result: DynResult<()> = async {
        let t = Instant::now();
        db.query("SELECT pg_kafka_run_partition_maintenance()", &[])
            .await
            .map_err(|e| format!("(b) maintenance behind a held lock: {:?}", e))?;
        assert!(
            t.elapsed() < Duration::from_secs(6),
            "(b) took {:?}",
            t.elapsed()
        );
        println!("Step 1 (b): maintenance returned in {:?}", t.elapsed());

        let row = db
            .query_one(
                "SELECT dropped FROM kafka.drop_message_partition($1::text::regclass, '{}')",
                &[&leaf],
            )
            .await
            .map_err(|e| format!("(c) drop behind a held lock: {:?}", e))?;
        assert!(!row.get::<_, bool>(0), "(c) must not drop without the lock");
        db.query("SELECT * FROM pg_kafka_run_retention_sweep(0, 3600)", &[])
            .await
            .map_err(|e| format!("(c) sweep behind a held lock: {:?}", e))?;
        println!("Step 2 (c): drop returned not-dropped, sweep succeeded");
        Ok(())
    }
    .await;
    other.batch_execute("ROLLBACK").await.map_err(boxed)?;
    result?;

    // (d) another session drops the leaf; ours waits, then finds it gone.
    other.batch_execute("BEGIN").await.map_err(boxed)?;
    let first: bool = other
        .query_one(
            "SELECT dropped FROM kafka.drop_message_partition($1::text::regclass, '{}')",
            &[&leaf],
        )
        .await
        .map_err(boxed)?
        .get(0);
    assert!(first, "the first session drops the empty leaf");
    let oid: i64 = db
        .query_one("SELECT $1::text::regclass::oid::bigint", &[&leaf])
        .await
        .map_err(boxed)?
        .get(0);
    let waiter = tokio::spawn(async move {
        db.query_one(
            "SELECT dropped FROM kafka.drop_message_partition($1::bigint::oid::regclass, '{}')",
            &[&oid],
        )
        .await
        .map(|r| r.get::<_, bool>(0))
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    other.batch_execute("COMMIT").await.map_err(boxed)?;
    let second = waiter
        .await
        .map_err(boxed)?
        .map_err(|e| format!("(d) drop of a leaf dropped concurrently: {:?}", e))?;
    assert!(!second, "(d) the waiter must report not-dropped");
    println!("Step 3 (d): the waiting drop returned not-dropped");
    println!("✅ every maintenance lock wait is bounded and handled");
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
pub async fn test_w3_commit_failure_answered_without_restart() -> TestResult {
    into_test_result(commit_failure().await)
}

async fn commit_failure() -> DynResult<()> {
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

    let result: DynResult<()> = async {
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

// -------------------------------------------------------------------------------------
// Item 7: heartbeats no longer split group-commit batches.
// -------------------------------------------------------------------------------------

/// Before: a batch stopped taking requests as soon as a Heartbeat waited on the
/// liveness lane, so with many consumers heartbeating, produce batches collapsed to one
/// request each (one commit, one fsync each). Now heartbeats waiting during a batch are
/// answered between its requests and the batch keeps taking queued work.
///
/// Construction: four raw connections keep the liveness lane busy with pipelined
/// Heartbeats (written continuously, read by a separate task), and every produce batch is delayed 200 ms before
/// its commit (`pg_kafka.test_pre_commit_delay_ms`), so the number of batches shows in
/// the elapsed time. 12 producers send 5 records each, one at a time. Before, most of
/// the 60 produces committed in batches of their own (measured 5-6 s); now concurrent
/// produces share batches (a few rounds of ~200 ms).
///
/// Asserts: the 60 produces complete within 5 s while the heartbeat flood is served
/// (every flood connection keeps getting responses).
pub async fn test_w3_heartbeats_do_not_split_batches() -> TestResult {
    into_test_result(heartbeats_batches().await)
}

/// One connection that keeps 32 pipelined Heartbeat (v0) requests in flight until
/// `stop`; returns how many responses it read.
async fn heartbeat_flood(stop: Arc<AtomicBool>) -> DynResult<u64> {
    use bytes::{BufMut, BytesMut};
    use kafka_protocol::messages::{GroupId, HeartbeatRequest, RequestHeader};
    use kafka_protocol::protocol::{Encodable, HeaderVersion, Request, StrBytes};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut header = RequestHeader::default();
    header.request_api_key = HeartbeatRequest::KEY;
    header.request_api_version = 0;
    header.correlation_id = 7;
    header.client_id = Some(StrBytes::from_static_str("w3-heartbeat-flood"));
    let mut request = HeartbeatRequest::default();
    request.group_id = GroupId(StrBytes::from_static_str("w3-worker-flood-group"));
    request.generation_id = 1;
    request.member_id = StrBytes::from_static_str("w3-flood-member");
    let mut body = BytesMut::new();
    header
        .encode(&mut body, HeartbeatRequest::header_version(0))
        .map_err(boxed)?;
    request.encode(&mut body, 0).map_err(boxed)?;
    let mut frame = BytesMut::new();
    frame.put_i32(body.len() as i32);
    frame.extend_from_slice(&body);
    let burst: Vec<u8> = frame.repeat(32);

    let stream = tokio::net::TcpStream::connect("127.0.0.1:9092")
        .await
        .map_err(boxed)?;
    let (mut reader, mut writer) = stream.into_split();
    // Writer and reader are decoupled: the socket always holds unread Heartbeats, so
    // the broker re-fills the liveness lane the moment it has room.
    let writer_stop = stop.clone();
    let writing = tokio::spawn(async move {
        while !writer_stop.load(Ordering::Relaxed) {
            if writer.write_all(&burst).await.is_err() {
                break;
            }
        }
    });
    let mut responses = 0u64;
    while !stop.load(Ordering::Relaxed) {
        let mut size = [0u8; 4];
        match tokio::time::timeout(Duration::from_millis(500), reader.read_exact(&mut size)).await {
            Ok(r) => {
                r.map_err(boxed)?;
            }
            Err(_) => continue,
        }
        let mut buf = vec![0u8; i32::from_be_bytes(size) as usize];
        reader.read_exact(&mut buf).await.map_err(boxed)?;
        responses += 1;
    }
    writing.abort();
    Ok(responses)
}

async fn heartbeats_batches() -> DynResult<()> {
    println!("=== Test: heartbeats do not split group-commit batches ===\n");
    let db = session().await?;
    wait_for_serving_listener(&db, None, Duration::from_secs(30)).await?;
    let topic = unique("hb-batch");
    let producers: Vec<FutureProducer> = (0..12)
        .map(|_| quick_producer("30000"))
        .collect::<DynResult<_>>()?;
    for p in &producers {
        send(p, &topic, "warm").await?;
    }

    let stop = Arc::new(AtomicBool::new(false));
    let floods: Vec<_> = (0..4)
        .map(|_| tokio::spawn(heartbeat_flood(stop.clone())))
        .collect();
    tokio::time::sleep(Duration::from_millis(500)).await;
    set_guc(&db, "pg_kafka.test_pre_commit_delay_ms", Some("200")).await?;
    let result: DynResult<Duration> = async {
        let start = Instant::now();
        let mut tasks = Vec::new();
        for p in &producers {
            let p = p.clone();
            let t = topic.clone();
            tasks.push(tokio::spawn(async move {
                for i in 0..5 {
                    send(&p, &t, &format!("r{}", i)).await?;
                }
                Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
            }));
        }
        for t in tasks {
            t.await.map_err(boxed)??;
        }
        Ok(start.elapsed())
    }
    .await;
    set_guc(&db, "pg_kafka.test_pre_commit_delay_ms", None).await?;
    stop.store(true, Ordering::Relaxed);
    let mut served = Vec::new();
    for f in floods {
        served.push(f.await.map_err(boxed)??);
    }
    let took = result?;
    println!(
        "Step 1: 60 produces in {:?} while the flood got {:?} heartbeat responses",
        took, served
    );
    assert!(
        served.iter().all(|n| *n > 0),
        "every heartbeat connection must be served"
    );
    assert!(
        took < Duration::from_millis(5000),
        "produces were committed one batch each under heartbeat traffic ({:?})",
        took
    );
    println!("✅ produce batches stay large under heartbeat traffic");
    Ok(())
}

// -------------------------------------------------------------------------------------
// Item 8: every transaction end wakes read_committed long polls.
// -------------------------------------------------------------------------------------

/// Before: only EndTxn sent `TransactionEnded`; a transaction aborted by fencing
/// (InitProducerId of a new producer instance) or by its timeout woke a parked
/// read_committed fetch only through the long-poll safety net (1 s). Now the worker
/// notifies after every transaction end.
///
/// Asserts, with a read_committed consumer parked (fetch.wait.max.ms 5000) behind a
/// pending transaction and a committed record after it: in 5 fencing rounds the
/// record arrives within 300 ms of the fencing InitProducerId, and after a
/// transaction-timeout abort within 500 ms of the abort.
pub async fn test_w3_transaction_end_wakes_read_committed() -> TestResult {
    into_test_result(txn_end_wakes().await)
}

async fn txn_end_wakes() -> DynResult<()> {
    println!("=== Test: fencing / timeout aborts wake read_committed long polls ===\n");
    let db = session().await?;
    wait_for_serving_listener(&db, None, Duration::from_secs(30)).await?;
    let topic = unique("txn-wake");
    let txn_id = unique("txn-wake-id");
    let plain = quick_producer("10000")?;
    send(&plain, &topic, "start").await?;

    let (tx, mut rx) =
        tokio::sync::mpsc::unbounded_channel::<(String, Instant, std::time::SystemTime)>();
    let stop = Arc::new(AtomicBool::new(false));
    let reader = {
        let consumer = assign_consumer(&topic, true, &[("fetch.wait.max.ms", "5000")])?;
        let stop = stop.clone();
        tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                if let Ok(Ok(m)) =
                    tokio::time::timeout(Duration::from_millis(200), consumer.recv()).await
                {
                    let v = String::from_utf8_lossy(m.payload().unwrap_or_default()).into_owned();
                    let _ = tx.send((v, Instant::now(), std::time::SystemTime::now()));
                }
            }
        })
    };
    async fn wait_for(
        rx: &mut tokio::sync::mpsc::UnboundedReceiver<(String, Instant, std::time::SystemTime)>,
        want: &str,
    ) -> DynResult<(Instant, std::time::SystemTime)> {
        let deadline = Instant::now() + Duration::from_secs(20);
        while Instant::now() < deadline {
            if let Ok(Some((v, at, wall))) =
                tokio::time::timeout(Duration::from_millis(200), rx.recv()).await
            {
                if v == want {
                    return Ok((at, wall));
                }
            }
        }
        Err(format!("never received {}", want).into())
    }
    wait_for(&mut rx, "start").await?;

    let result: DynResult<()> = async {
        let init = |id: String, timeout_ms: Option<&'static str>| async move {
            let mut cfg = ClientConfig::new();
            cfg.set("bootstrap.servers", get_bootstrap_servers())
                .set("broker.address.family", "v4")
                .set("message.timeout.ms", "10000")
                .set("transactional.id", &id);
            if let Some(t) = timeout_ms {
                cfg.set("transaction.timeout.ms", t)
                    .set("message.timeout.ms", t);
            }
            let p: FutureProducer = cfg.create().map_err(boxed)?;
            let q = p.clone();
            tokio::task::spawn_blocking(move || q.init_transactions(Duration::from_secs(10)))
                .await
                .map_err(boxed)?
                .map_err(boxed)?;
            Ok::<FutureProducer, Box<dyn std::error::Error + Send + Sync>>(p)
        };
        let mut keep = Vec::new();
        let mut current = init(txn_id.clone(), None).await?;
        for round in 0..5 {
            current.begin_transaction().map_err(boxed)?;
            send(&current, &topic, &format!("pending-{}", round)).await?;
            let visible = format!("visible-{}", round);
            send(&plain, &topic, &visible).await?;
            tokio::time::sleep(Duration::from_millis(400)).await;
            let fenced_by = init(txn_id.clone(), None).await?;
            let after_init = Instant::now();
            let (got, _) = wait_for(&mut rx, &visible).await?;
            let latency = got.saturating_duration_since(after_init);
            println!(
                "  round {}: committed record after fencing in {:?}",
                round, latency
            );
            assert!(
                latency < Duration::from_millis(300),
                "round {}: read_committed fetch woke {:?} after the fencing abort",
                round,
                latency
            );
            keep.push(std::mem::replace(&mut current, fenced_by));
        }

        // Transaction-timeout abort (the worker checks every 10 s).
        let timeout_id = unique("txn-wake-timeout");
        let p = init(timeout_id.clone(), Some("2000")).await?;
        p.begin_transaction().map_err(boxed)?;
        send(&p, &topic, "pending-timeout").await?;
        send(&plain, &topic, "visible-timeout").await?;
        let (_, wall) = wait_for(&mut rx, "visible-timeout").await?;
        let aborted_at: f64 = db
            .query_one(
                "SELECT EXTRACT(EPOCH FROM last_updated_at::timestamptz)::float8
                 FROM kafka.transactions WHERE transactional_id = $1 AND state = 'CompleteAbort'",
                &[&timeout_id],
            )
            .await
            .map_err(boxed)?
            .get(0);
        let received = wall
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(boxed)?
            .as_secs_f64();
        let latency = received - aborted_at;
        println!(
            "  timeout abort: committed record {:.3} s after the abort",
            latency
        );
        assert!(
            latency < 0.5,
            "read_committed fetch woke {:.3} s after the timeout abort",
            latency
        );
        keep.push(p);
        drop(keep);
        Ok(())
    }
    .await;
    stop.store(true, Ordering::Relaxed);
    let _ = reader.await;
    result?;
    println!("✅ every transaction end wakes the parked read_committed fetch");
    Ok(())
}
