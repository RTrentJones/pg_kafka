//! The transaction timeout sweep: what it measures and how it fences.

use crate::common::{create_db_client, create_transactional_producer, TestResult};
use rdkafka::producer::{FutureRecord, Producer};
use std::time::{Duration, Instant};
use tokio_postgres::Client;
use uuid::Uuid;

/// Start a transaction with `transaction.timeout.ms` = 1 s whose clock is already 90 s
/// old, and wait until the sweep has aborted it: proof that one full sweep cycle ran
/// after the caller's setup.
async fn wait_for_a_sweep(db: &Client, topic: &str) -> TestResult {
    let txn_id = format!("txn-sweep-sentinel-{}", Uuid::new_v4());
    let producer = create_transactional_producer(&txn_id)?;
    producer.init_transactions(Duration::from_secs(10))?;
    producer.begin_transaction()?;
    producer
        .send(
            FutureRecord::to(topic).payload("sentinel").key("s"),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(e, _)| e)?;
    db.execute(
        "UPDATE kafka.transactions
         SET started_at = NOW() - INTERVAL '90 seconds', timeout_ms = 1000
         WHERE transactional_id = $1",
        &[&txn_id],
    )
    .await?;
    let deadline = Instant::now() + Duration::from_secs(25);
    loop {
        let state: Option<String> = db
            .query_one(
                "SELECT state FROM kafka.transactions WHERE transactional_id = $1",
                &[&txn_id],
            )
            .await?
            .get(0);
        if state.as_deref() == Some("CompleteAbort") {
            return Ok(());
        }
        if Instant::now() > deadline {
            return Err("the timeout sweep never aborted the expired sentinel".into());
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// The timeout clock starts with each transaction (Kafka's txnStartTimestamp on
/// Empty/Complete* -> Ongoing), not with the transactional id's first one: a second
/// transaction that begins long after the first committed is not aborted by the sweep
/// and commits.
pub async fn test_second_transaction_gets_its_own_timeout() -> TestResult {
    println!("=== Test: Each Transaction Gets Its Own Timeout ===\n");
    let db = create_db_client().await?;
    let txn_id = format!("txn-own-timeout-{}", Uuid::new_v4());
    let topic = format!("txn-own-timeout-{}", Uuid::new_v4());
    let producer = create_transactional_producer(&txn_id)?;
    producer.init_transactions(Duration::from_secs(10))?;

    println!("Step 1: Transaction 1 commits...");
    producer.begin_transaction()?;
    producer
        .send(
            FutureRecord::to(&topic).payload("first").key("k"),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(e, _)| e)?;
    producer.commit_transaction(Duration::from_secs(10))?;

    // Transaction 1 began 90 s ago, longer than the 60 s timeout of the id.
    db.execute(
        "UPDATE kafka.transactions SET started_at = NOW() - INTERVAL '90 seconds',
                timeout_ms = 60000
         WHERE transactional_id = $1",
        &[&txn_id],
    )
    .await?;
    println!("✅ Committed; its start backdated to 90 s ago (timeout 60 s)\n");

    println!("Step 2: Transaction 2 begins now and outlives one sweep...");
    producer.begin_transaction()?;
    producer
        .send(
            FutureRecord::to(&topic).payload("second").key("k"),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(e, _)| e)?;
    let age_s: f64 = db
        .query_one(
            "SELECT EXTRACT(EPOCH FROM NOW() - started_at)::float8 FROM kafka.transactions
             WHERE transactional_id = $1",
            &[&txn_id],
        )
        .await?
        .get(0);
    println!("   started_at is {age_s:.1} s old");
    assert!(
        age_s < 30.0,
        "transaction 2's clock must start when it begins, not at transaction 1 ({age_s:.1} s)"
    );
    wait_for_a_sweep(&db, &topic).await?;
    let state: Option<String> = db
        .query_one(
            "SELECT state FROM kafka.transactions WHERE transactional_id = $1",
            &[&txn_id],
        )
        .await?
        .get(0);
    assert_eq!(state.as_deref(), Some("Ongoing"), "the sweep must leave it");
    println!("✅ A sweep ran; transaction 2 is still Ongoing\n");

    println!("Step 3: Transaction 2 commits...");
    producer.commit_transaction(Duration::from_secs(10))?;
    let visible: i64 = db
        .query_one(
            "SELECT COUNT(*) FROM kafka.messages m JOIN kafka.topics t ON t.id = m.topic_id
             WHERE t.name = $1 AND m.txn_state IS NULL",
            &[&topic],
        )
        .await?
        .get(0);
    assert_eq!(visible, 2, "both transactions' records are committed");
    println!("✅ Test PASSED\n");
    Ok(())
}
