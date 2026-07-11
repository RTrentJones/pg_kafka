//! E2E tests for the storage-lifecycle retention sweep (DR-1/DR-2, DEEP-REVIEW-2026-07).
//!
//! Fail-before/pass-after: before the fix, `pg_kafka_run_retention_sweep()` did not
//! exist (and `cleanup_aborted_messages` had no production caller), so these tests
//! fail on the pre-fix extension. The SQL function runs the exact sweep the worker
//! runs every RETENTION_SWEEP_INTERVAL, so exercising it exercises the production
//! path without waiting out the interval.
//!
//! Marked `parallel_safe: false` in main.rs: the aborted-row pass (grace 0) reclaims
//! *any* aborted row, which would race concurrent transaction tests that assert
//! aborted rows exist.

use crate::common::{create_db_client, create_transactional_producer, TestResult};
use rdkafka::producer::{FutureRecord, Producer};
use std::time::Duration;
use uuid::Uuid;

/// Aborted transactional messages are physically reclaimed by the sweep (DR-1).
pub async fn test_retention_sweep_reclaims_aborted_messages() -> TestResult {
    println!("=== Test: Retention Sweep Reclaims Aborted Messages ===\n");

    let txn_id = format!("txn-sweep-{}", Uuid::new_v4());
    let topic = format!("sweep-aborted-{}", Uuid::new_v4());

    println!("Connecting to PostgreSQL...");
    let client = create_db_client().await?;

    // 1. Produce inside a transaction, then abort it.
    println!("Step 1: Producing and aborting a transaction...");
    let producer = create_transactional_producer(&txn_id)?;
    producer.init_transactions(Duration::from_secs(10))?;
    producer.begin_transaction()?;
    producer
        .send(
            FutureRecord::to(&topic).payload("doomed").key("k"),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(err, _)| err)?;
    producer.abort_transaction(Duration::from_secs(10))?;
    println!("  ✅ Transaction aborted\n");

    // 2. The aborted row must exist before the sweep (fail-before guard: if the
    //    abort path ever stopped marking rows, this test would vacuously pass).
    println!("Step 2: Verifying aborted row exists before sweep...");
    let before: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM kafka.messages m
             JOIN kafka.topics t ON m.topic_id = t.id
             WHERE t.name = $1 AND m.txn_state = 'aborted'",
            &[&topic],
        )
        .await?
        .get(0);
    assert_eq!(before, 1, "Expected exactly 1 aborted row before sweep");
    println!("  ✅ 1 aborted row present\n");

    // 3. Run the sweep with grace 0 (retention pass disabled for this run).
    println!("Step 3: Running pg_kafka_run_retention_sweep(0, 0)...");
    let rows = client
        .query(
            "SELECT category, deleted FROM pg_kafka_run_retention_sweep(0, 0)",
            &[],
        )
        .await?;
    let aborted_deleted: i64 = rows
        .iter()
        .find(|r| r.get::<_, String>(0) == "aborted_messages")
        .map(|r| r.get(1))
        .ok_or("sweep result missing aborted_messages row")?;
    println!(
        "  Sweep reported {} aborted message(s) deleted",
        aborted_deleted
    );
    assert!(
        aborted_deleted >= 1,
        "Sweep should report at least our 1 aborted row deleted"
    );

    // 4. The aborted row is physically gone.
    println!("\nStep 4: Verifying aborted row is gone...");
    let after: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM kafka.messages m
             JOIN kafka.topics t ON m.topic_id = t.id
             WHERE t.name = $1",
            &[&topic],
        )
        .await?
        .get(0);
    assert_eq!(after, 0, "Aborted row should be physically deleted");
    println!("  ✅ Aborted row reclaimed\n");

    // Cleanup
    client
        .execute("DELETE FROM kafka.topics WHERE name = $1", &[&topic])
        .await?;
    client
        .execute(
            "DELETE FROM kafka.transactions WHERE transactional_id = $1",
            &[&txn_id],
        )
        .await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// Message retention deletes only expired, non-pending rows, and stays disabled
/// by default (DR-2).
pub async fn test_retention_sweep_expires_old_messages() -> TestResult {
    println!("=== Test: Retention Sweep Expires Old Messages ===\n");

    let txn_id = format!("txn-pending-{}", Uuid::new_v4());
    let topic = format!("sweep-expiry-{}", Uuid::new_v4());
    let client = create_db_client().await?;

    // 1. Produce 3 plain messages.
    println!("Step 1: Producing 3 messages...");
    let producer = crate::common::create_producer()?;
    for i in 0..3 {
        producer
            .send(
                FutureRecord::to(&topic)
                    .payload(&format!("msg-{}", i))
                    .key("k"),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(err, _)| err)?;
    }
    println!("  ✅ 3 messages produced\n");

    // 2. Default sweep (GUC message_retention_hours = 0) must delete nothing.
    println!("Step 2: Sweep with retention disabled (default) keeps everything...");
    client
        .query("SELECT * FROM pg_kafka_run_retention_sweep(0, 3600)", &[])
        .await?;
    let count: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM kafka.messages m
             JOIN kafka.topics t ON m.topic_id = t.id WHERE t.name = $1",
            &[&topic],
        )
        .await?
        .get(0);
    assert_eq!(count, 3, "Retention disabled: all messages must survive");
    println!("  ✅ All 3 messages survive\n");

    // 3. Age the two oldest rows past a 1-hour retention window, plus add an aged
    //    *pending* transactional row that must never be deleted.
    println!("Step 3: Aging 2 rows to 3h old + creating an aged pending txn row...");
    let txn_producer = create_transactional_producer(&txn_id)?;
    txn_producer.init_transactions(Duration::from_secs(10))?;
    txn_producer.begin_transaction()?;
    txn_producer
        .send(
            FutureRecord::to(&topic).payload("pending").key("p"),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(err, _)| err)?;
    // Transaction stays open — the row is txn_state='pending'.
    client
        .execute(
            "UPDATE kafka.messages m SET created_at = NOW() - INTERVAL '3 hours'
             FROM kafka.topics t
             WHERE m.topic_id = t.id AND t.name = $1
               AND (m.partition_offset < 2 OR m.txn_state = 'pending')",
            &[&topic],
        )
        .await?;
    println!("  ✅ Rows aged\n");

    // 4. Sweep with 1-hour retention: the 2 aged committed rows go, the fresh row
    //    and the aged pending row stay.
    println!("Step 4: Sweep with message_retention_hours=1...");
    client
        .query("SELECT * FROM pg_kafka_run_retention_sweep(1, 3600)", &[])
        .await?;
    let rows = client
        .query(
            "SELECT m.partition_offset, m.txn_state FROM kafka.messages m
             JOIN kafka.topics t ON m.topic_id = t.id
             WHERE t.name = $1 ORDER BY m.partition_offset",
            &[&topic],
        )
        .await?;
    let survivors: Vec<(i64, Option<String>)> = rows.iter().map(|r| (r.get(0), r.get(1))).collect();
    println!("  Survivors: {:?}", survivors);
    assert_eq!(
        survivors.len(),
        2,
        "Expected 2 survivors (fresh committed + aged pending), got {:?}",
        survivors
    );
    assert!(
        survivors
            .iter()
            .any(|(_, s)| s.as_deref() == Some("pending")),
        "Aged pending transactional row must never be deleted by retention"
    );
    assert!(
        survivors.iter().any(|(o, s)| *o == 2 && s.is_none()),
        "Fresh committed row (offset 2) must survive a 1h retention sweep"
    );
    println!("  ✅ Retention deleted only expired committed rows\n");

    // Cleanup: abort the open transaction, then drop test rows.
    txn_producer.abort_transaction(Duration::from_secs(10))?;
    client
        .execute("DELETE FROM kafka.topics WHERE name = $1", &[&topic])
        .await?;
    client
        .execute(
            "DELETE FROM kafka.transactions WHERE transactional_id = $1",
            &[&txn_id],
        )
        .await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
