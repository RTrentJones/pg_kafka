//! Partition maintenance against concurrent transactions and lock holders.

use crate::common::create_transactional_producer;
use crate::common::TestResult;
use crate::worker::{
    boxed, deadlocks, into_test_result, quick_producer, read_values, send, session, unique,
    wait_for_serving_listener, SendResult, ServerLog,
};
use rdkafka::producer::Producer;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_postgres::Client;

/// The maintenance move (DELETE from kafka.messages_default + INSERT into the new leaf)
/// must not run concurrently with EndTxn's `UPDATE kafka.messages SET txn_state`: the
/// UPDATE would wait on the deleted rows, skip them once the move commits and never see
/// their copies, leaving a committed transaction's rows `pending` forever (and the LSO
/// stuck). The move takes the pg_kafka schema lock exclusively and every worker
/// transaction takes it shared first, so they never overlap.
///
/// Asserts: while a session holds the schema lock in SHARE mode, maintenance does not
/// move rows out of DEFAULT; and with pending rows backdated into the DEFAULT leaf, an open session
/// that has just moved them (maintenance, uncommitted), a commit_transaction that arrives meanwhile:
/// after the session commits, the commit succeeds, no row of the transaction is left
/// pending and a read_committed consumer reads all of them.
pub async fn test_end_txn_during_default_leaf_move() -> TestResult {
    into_test_result(end_txn_during_move().await)
}

async fn end_txn_during_move() -> SendResult<()> {
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
        send(&producer, &topic, &format!("t{i}")).await?;
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
    println!("Step 1: maintenance (uncommitted) created {created} leaf/leaves");

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
        "committed transaction left {pending} row(s) pending (moved by concurrent maintenance)"
    );
    assert_eq!(
        got.len(),
        5,
        "read_committed consumer sees the committed rows"
    );
    println!("✅ no pending rows; read_committed reads all 5 (leaf {leaf:?})");
    Ok(())
}

/// Every lock wait of the partition functions is bounded and handled: a session that
/// called maintenance and stays open must not stall the next pass (the worker's
/// included); a lock timeout in drop_message_partition must not roll back the whole
/// sweep; and a leaf a concurrent sweep dropped first is "not dropped", not an error on
/// its vanished OID.
///
/// Asserts: with the maintenance advisory lock held by another session, manual
/// maintenance returns within 6 s, `drop_message_partition` returns dropped=false
/// without an error and a retention sweep (which queues for the lock) ends at its
/// session's lock_timeout with lock_not_available, then succeeds once the lock is
/// free; and a drop that waits behind
/// another session's drop of the same leaf returns dropped=false after it commits.
pub async fn test_partition_maintenance_lock_waits_are_bounded() -> TestResult {
    into_test_result(maintenance_lock_waits().await)
}

async fn maintenance_lock_waits() -> SendResult<()> {
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
    println!("Step 0: test leaf {leaf}");

    other.batch_execute("BEGIN").await.map_err(boxed)?;
    other
        .query("SELECT pg_advisory_xact_lock(8099547369649367905)", &[])
        .await
        .map_err(boxed)?;
    let result: SendResult<()> = async {
        let t = Instant::now();
        db.query("SELECT pg_kafka_run_partition_maintenance()", &[])
            .await
            .map_err(|e| format!("(b) maintenance behind a held lock: {e:?}"))?;
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
            .map_err(|e| format!("(c) drop behind a held lock: {e:?}"))?;
        assert!(!row.get::<_, bool>(0), "(c) must not drop without the lock");
        // A sweep queues for the maintenance lock, bounded by the session's
        // lock_timeout: it fails cleanly (55P03, nothing done) instead of stalling.
        db.batch_execute("SET lock_timeout = '1s'")
            .await
            .map_err(boxed)?;
        let t = Instant::now();
        let swept = db
            .query("SELECT * FROM pg_kafka_run_retention_sweep(0, 3600)", &[])
            .await;
        db.batch_execute("RESET lock_timeout")
            .await
            .map_err(boxed)?;
        match swept {
            Err(e) if e.code() == Some(&tokio_postgres::error::SqlState::LOCK_NOT_AVAILABLE) => {}
            other => return Err(format!("(c) sweep behind a held lock: {other:?}").into()),
        }
        assert!(
            t.elapsed() < Duration::from_secs(6),
            "(c) sweep took {:?}",
            t.elapsed()
        );
        println!("Step 2 (c): drop returned not-dropped, sweep hit its lock_timeout");
        Ok(())
    }
    .await;
    other.batch_execute("ROLLBACK").await.map_err(boxed)?;
    result?;

    // Another session drops the leaf; ours waits, then finds it gone.
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
        .map_err(|e| format!("(d) drop of a leaf dropped concurrently: {e:?}"))?;
    assert!(!second, "(d) the waiter must report not-dropped");
    println!("Step 3 (d): the waiting drop returned not-dropped");
    other
        .query("SELECT * FROM pg_kafka_run_retention_sweep(0, 3600)", &[])
        .await
        .map_err(|e| format!("(c) sweep once the lock is free: {e:?}"))?;
    println!("Step 4 (c): with the lock free, the sweep succeeds");
    println!("✅ every maintenance lock wait is bounded and handled");
    Ok(())
}

/// Maintenance passes over this many partition widths of the worker's setting.
const WIDE_FACTOR: i32 = 7;
/// Maintenance passes run against the busy worker, each over a fresh layout.
const WIDE_ROUNDS: i32 = 6;
/// Concurrent producers keeping the worker's transactions on kafka.messages.
const WIDE_PRODUCERS: usize = 3;

/// One maintenance pass that creates several leaves, the later of which moves rows out
/// of the DEFAULT leaf, must not deadlock with a worker transaction. Moving rows needs
/// the schema lock EXCLUSIVE, which every worker transaction holds SHARE from its start;
/// an earlier, row-less leaf of the same pass already holds ACCESS EXCLUSIVE on
/// kafka.messages to commit. A pass that only then queued for the schema lock waits for
/// a worker transaction that waits for kafka.messages: "deadlock detected".
///
/// The layout: a pass `WIDE_FACTOR` times wider than the worker's width (an operator
/// raising pg_kafka.message_partition_minutes), so the current wide bucket is partly
/// covered by the worker's leaves and the next is a row-less gap, and rows in the DEFAULT
/// leaf two wide buckets ahead. Producers keep the worker writing kafka.messages
/// throughout.
///
/// Asserts: every round's `pg_kafka_run_partition_maintenance(width)` succeeds and moves
/// the DEFAULT rows into a leaf, the producers get every record acked (a worker chosen
/// as the deadlock victim fails a produce), and neither pg_stat_database nor the server
/// log (when readable) records a deadlock.
pub async fn test_wide_maintenance_moving_default_rows_under_load() -> TestResult {
    into_test_result(wide_maintenance_under_load().await)
}

async fn wide_maintenance_under_load() -> SendResult<()> {
    println!("=== Test: multi-leaf maintenance moving DEFAULT rows under load ===\n");
    let db = session().await?;
    wait_for_serving_listener(&db, None, Duration::from_secs(30)).await?;
    let minutes: i32 = db
        .query_one(
            "SELECT current_setting('pg_kafka.message_partition_minutes')::int",
            &[],
        )
        .await
        .map_err(boxed)?
        .get(0);
    let width = (minutes * WIDE_FACTOR).min(43_200);
    let topic = unique("wide-move");
    let topic_id: i32 = db
        .query_one(
            "INSERT INTO kafka.topics (name, partitions) VALUES ($1, 1) RETURNING id",
            &[&topic],
        )
        .await
        .map_err(boxed)?
        .get(0);
    let maintainer = session().await?;
    maintainer
        .batch_execute("SET statement_timeout = '30s'")
        .await
        .map_err(boxed)?;
    println!("Step 0: worker width {minutes} min, maintenance width {width} min");

    let stop = Arc::new(AtomicBool::new(false));
    let produce_topics: Vec<String> = (0..WIDE_PRODUCERS)
        .map(|i| unique(&format!("wide-move-produce-{i}")))
        .collect();
    let producing: Vec<_> = produce_topics
        .iter()
        .map(|t| {
            let stop = stop.clone();
            let t = t.clone();
            tokio::spawn(async move {
                let producer = quick_producer("10000")?;
                let (mut acked, mut errors) = (0u64, Vec::new());
                while !stop.load(Ordering::SeqCst) {
                    match send(&producer, &t, "p").await {
                        Ok(_) => acked += 1,
                        Err(e) => errors.push(e.to_string()),
                    }
                }
                SendResult::Ok((acked, errors))
            })
        })
        .collect();

    let log = ServerLog::open(&db).await?;
    let log_from = log.as_ref().map(ServerLog::end);
    let deadlocks_before = deadlocks(&db).await?;
    let mut failures = Vec::new();
    let mut result: SendResult<()> = Ok(());
    for round in 0..WIDE_ROUNDS {
        let round_result: SendResult<()> = async {
            let before = leaf_names(&db).await?;
            // Rows two wide buckets ahead: no leaf covers them, so they sit in DEFAULT,
            // and the pass reaches them only after creating the row-less leaves before.
            db.execute(
                "INSERT INTO kafka.messages (topic_id, partition_id, partition_offset, value,
                                             created_at)
                 SELECT $1, 0, $2::bigint * 10 + g, 'v'::bytea,
                        to_timestamp((floor(extract(epoch FROM now()) / ($3::int * 60)) + 2)
                                     * ($3::int * 60)) + $4::int * INTERVAL '1 minute'
                 FROM generate_series(0::bigint, 9) AS g",
                &[&topic_id, &i64::from(round), &width, &minutes],
            )
            .await
            .map_err(boxed)?;
            match maintainer
                .query_one("SELECT pg_kafka_run_partition_maintenance($1)", &[&width])
                .await
            {
                Ok(row) => {
                    let created: i64 = row.get(0);
                    let in_default: i64 = db
                        .query_one(
                            "SELECT count(*) FROM kafka.messages_default WHERE topic_id = $1",
                            &[&topic_id],
                        )
                        .await
                        .map_err(boxed)?
                        .get(0);
                    println!("Round {round}: created {created} leaves, {in_default} row(s) left in DEFAULT");
                    if in_default != 0 {
                        failures.push(format!("round {round}: {in_default} row(s) not moved"));
                    }
                }
                Err(e) => {
                    println!("Round {round}: maintenance failed: {}", db_error(&e));
                    failures.push(format!("round {round}: {}", db_error(&e)));
                }
            }
            // Back to the starting layout: the rows and the leaves this round created.
            db.execute("DELETE FROM kafka.messages WHERE topic_id = $1", &[&topic_id])
                .await
                .map_err(boxed)?;
            drop_new_leaves(&db, &before).await
        }
        .await;
        if let Err(e) = round_result {
            result = Err(e);
            break;
        }
    }

    stop.store(true, Ordering::SeqCst);
    let mut acked = 0;
    for p in producing {
        let (n, errors) = p.await.map_err(boxed)??;
        acked += n;
        failures.extend(errors.into_iter().map(|e| format!("produce: {e}")));
    }
    let deadlocks_after = deadlocks(&db).await?;
    if let (Some(log), Some(from)) = (&log, log_from) {
        let logged = log.since(from).matches("deadlock detected").count();
        if logged > 0 {
            failures.push(format!("the server log reports {logged} deadlock(s)"));
        }
    }
    let mut names = produce_topics;
    names.push(topic);
    db.execute("DELETE FROM kafka.topics WHERE name = ANY($1)", &[&names])
        .await
        .map_err(boxed)?;
    result?;
    println!(
        "   {} failure(s), {} new deadlock(s), {acked} record(s) produced meanwhile",
        failures.len(),
        deadlocks_after - deadlocks_before
    );
    if !failures.is_empty() {
        return Err(format!(
            "maintenance under load failed {} time(s): {}",
            failures.len(),
            failures.join("; ")
        )
        .into());
    }
    assert_eq!(
        deadlocks_after, deadlocks_before,
        "the database recorded deadlocks during multi-leaf maintenance"
    );
    assert!(acked > 0, "the producers must keep getting acks");
    println!("✅ no deadlock; every pass moved the DEFAULT rows");
    Ok(())
}

/// The names of the leaves of kafka.messages.
async fn leaf_names(db: &Client) -> SendResult<Vec<String>> {
    let rows = db
        .query(
            "SELECT partition_oid::regclass::text FROM kafka.message_partitions()",
            &[],
        )
        .await
        .map_err(boxed)?;
    Ok(rows.iter().map(|r| r.get(0)).collect())
}

/// Drop every leaf not in `before`, retrying a drop that timed out on its locks.
async fn drop_new_leaves(db: &Client, before: &[String]) -> SendResult<()> {
    for leaf in leaf_names(db).await? {
        if before.contains(&leaf) {
            continue;
        }
        let mut dropped = false;
        for _ in 0..5 {
            dropped = db
                .query_one(
                    "SELECT dropped FROM kafka.drop_message_partition($1::text::regclass, '{}')",
                    &[&leaf],
                )
                .await
                .map_err(boxed)?
                .get(0);
            if dropped {
                break;
            }
        }
        if !dropped {
            return Err(format!("could not drop the test's leaf {leaf}").into());
        }
    }
    Ok(())
}

/// The message and code of a database error ("deadlock detected" and its cycle).
fn db_error(e: &tokio_postgres::Error) -> String {
    match e.as_db_error() {
        Some(d) => format!(
            "{} ({}){}",
            d.message(),
            d.code().code(),
            d.detail().map_or_else(String::new, |s| format!(": {s}"))
        ),
        None => e.to_string(),
    }
}
