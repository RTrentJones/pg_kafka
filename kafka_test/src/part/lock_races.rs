//! Partition maintenance against concurrent transactions and lock holders.

use crate::common::create_transactional_producer;
use crate::common::TestResult;
use crate::worker::{
    boxed, into_test_result, read_values, send, session, unique, wait_for_serving_listener,
    SendResult,
};
use rdkafka::producer::Producer;
use std::time::{Duration, Instant};

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
/// without an error and a retention sweep succeeds; and a drop that waits behind
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
        db.query("SELECT * FROM pg_kafka_run_retention_sweep(0, 3600)", &[])
            .await
            .map_err(|e| format!("(c) sweep behind a held lock: {e:?}"))?;
        println!("Step 2 (c): drop returned not-dropped, sweep succeeded");
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
    println!("✅ every maintenance lock wait is bounded and handled");
    Ok(())
}
