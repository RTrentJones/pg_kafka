//! Transaction integration tests for shadow mode
//!
//! Tests for transaction commit/abort behavior with shadow mode forwarding.

use crate::common::{create_transactional_producer, TestResult};
use crate::setup::TestContext;
use rdkafka::producer::{FutureRecord, Producer};
use std::time::Duration;

use super::assertions::{assert_external_kafka_empty, assert_message_in_external_kafka};
use super::external_client::verify_external_kafka_ready;
use super::helpers::{enable_shadow_mode, ShadowMode, ShadowTopicConfig, SyncMode, WriteMode};

/// Test that committed transactions are forwarded
///
/// Produces a message within a transaction, commits, and verifies
/// it appears in external Kafka.
pub async fn test_committed_transaction_forwarded() -> TestResult {
    println!("=== Test: Committed Transaction Forwarded ===\n");

    // 1. SETUP
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("txn-commit").await;
    let txn_id = format!("shadow-txn-{}", ctx.test_id);

    println!("Step 1: Verifying external Kafka is ready...");
    verify_external_kafka_ready().await?;
    println!("✅ External Kafka ready\n");

    println!("Step 2: Enabling shadow mode...");
    enable_shadow_mode(
        ctx.db(),
        &topic,
        &ShadowTopicConfig {
            mode: ShadowMode::Shadow,
            forward_percentage: 100,
            external_topic_name: None,
            sync_mode: SyncMode::Sync,
            write_mode: WriteMode::DualWrite,
        },
    )
    .await?;
    println!("✅ Shadow mode enabled\n");

    // 2. ACTION - Produce within transaction and commit
    println!("Step 3: Creating transactional producer...");
    let producer = create_transactional_producer(&txn_id)?;
    producer.init_transactions(Duration::from_secs(10))?;
    println!("✅ Transactional producer initialized\n");

    println!("Step 4: Beginning transaction...");
    producer.begin_transaction()?;
    println!("✅ Transaction started\n");

    println!("Step 5: Producing message within transaction...");
    let key = b"txn-commit-key";
    let value = b"txn-commit-value";

    producer
        .send(
            FutureRecord::to(&topic).key(&key[..]).payload(&value[..]),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(e, _)| e)?;
    println!("✅ Message produced\n");

    println!("Step 6: Committing transaction...");
    producer.commit_transaction(Duration::from_secs(10))?;
    println!("✅ Transaction committed\n");

    // 3. VERIFY
    println!("=== Verification ===\n");
    tokio::time::sleep(Duration::from_millis(1000)).await;

    println!("Checking external Kafka for committed message...");
    assert_message_in_external_kafka(&topic, Some(key), Some(value), Duration::from_secs(10))
        .await?;
    println!("✅ Committed message found in external Kafka\n");

    // 4. CLEANUP
    ctx.cleanup().await?;
    println!("✅ Test PASSED: Committed Transaction Forwarded\n");
    Ok(())
}

/// Test that aborted transactions are NOT forwarded
///
/// Produces a message within a transaction, aborts, and verifies
/// it does NOT appear in external Kafka.
pub async fn test_aborted_transaction_not_forwarded() -> TestResult {
    println!("=== Test: Aborted Transaction Not Forwarded ===\n");

    // 1. SETUP
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("txn-abort").await;
    let txn_id = format!("shadow-abort-{}", ctx.test_id);

    println!("Step 1: Verifying external Kafka is ready...");
    verify_external_kafka_ready().await?;
    println!("✅ External Kafka ready\n");

    println!("Step 2: Enabling shadow mode...");
    enable_shadow_mode(
        ctx.db(),
        &topic,
        &ShadowTopicConfig {
            mode: ShadowMode::Shadow,
            forward_percentage: 100,
            external_topic_name: None,
            sync_mode: SyncMode::Sync,
            write_mode: WriteMode::DualWrite,
        },
    )
    .await?;
    println!("✅ Shadow mode enabled\n");

    // 2. ACTION - Produce within transaction and abort
    println!("Step 3: Creating transactional producer...");
    let producer = create_transactional_producer(&txn_id)?;
    producer.init_transactions(Duration::from_secs(10))?;
    println!("✅ Transactional producer initialized\n");

    println!("Step 4: Beginning transaction...");
    producer.begin_transaction()?;
    println!("✅ Transaction started\n");

    println!("Step 5: Producing message within transaction...");
    let key = b"txn-abort-key";
    let value = b"txn-abort-value";

    producer
        .send(
            FutureRecord::to(&topic).key(&key[..]).payload(&value[..]),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(e, _)| e)?;
    println!("✅ Message produced\n");

    println!("Step 6: Aborting transaction...");
    producer.abort_transaction(Duration::from_secs(10))?;
    println!("✅ Transaction aborted\n");

    // 3. VERIFY
    println!("=== Verification ===\n");
    tokio::time::sleep(Duration::from_millis(1000)).await;

    println!("Checking external Kafka (should be empty)...");
    assert_external_kafka_empty(&topic, Duration::from_secs(3)).await?;
    println!("✅ No message in external Kafka (aborted transaction not forwarded)\n");

    // 4. CLEANUP
    ctx.cleanup().await?;
    println!("✅ Test PASSED: Aborted Transaction Not Forwarded\n");
    Ok(())
}

/// Test that a committed transaction forwards via the DURABLE OUTBOX (RA-1/RA-2)
///
/// Uses async mode so forwarding can only happen via the background poll (never
/// an inline `block_on` on the commit path — the RA-1 fix). Asserts both that
/// the message reaches external Kafka AND that the `kafka.shadow_tracking` outbox
/// row is written and finalized (`external_offset` non-NULL) — proving the txn
/// commit now routes through the same at-least-once outbox as a non-txn produce.
pub async fn test_committed_transaction_uses_durable_outbox() -> TestResult {
    println!("=== Test: Committed Transaction Uses Durable Outbox (RA-1/RA-2) ===\n");

    // 1. SETUP
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("txn-outbox").await;
    let txn_id = format!("shadow-txn-outbox-{}", ctx.test_id);

    verify_external_kafka_ready().await?;
    enable_shadow_mode(
        ctx.db(),
        &topic,
        &ShadowTopicConfig {
            mode: ShadowMode::Shadow,
            forward_percentage: 100,
            external_topic_name: None,
            sync_mode: SyncMode::Async, // async → only the poll can forward
            write_mode: WriteMode::DualWrite,
        },
    )
    .await?;

    // 2. ACTION — produce within a transaction and commit.
    let producer = create_transactional_producer(&txn_id)?;
    producer.init_transactions(Duration::from_secs(10))?;
    producer.begin_transaction()?;
    let key = b"txn-outbox-key";
    let value = b"txn-outbox-value";
    producer
        .send(
            FutureRecord::to(&topic).key(&key[..]).payload(&value[..]),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(e, _)| e)?;
    producer.commit_transaction(Duration::from_secs(10))?;
    println!("✅ Transaction committed\n");

    // 3. VERIFY
    // 3a. Reaches external Kafka — via the poll, not an inline commit-path send.
    assert_message_in_external_kafka(&topic, Some(key), Some(value), Duration::from_secs(15))
        .await?;
    println!("✅ Forwarded to external Kafka (via the outbox poll)\n");

    // 3b. The outbox row exists and is finalized — proving the durable path.
    let mut finalized = false;
    for _ in 0..20 {
        let row = ctx
            .db()
            .query_one(
                r#"
                SELECT COUNT(*) FILTER (WHERE st.external_offset IS NOT NULL) AS done,
                       COUNT(*) AS total
                FROM kafka.shadow_tracking st
                JOIN kafka.topics t ON t.id = st.topic_id
                WHERE t.name = $1
                "#,
                &[&topic],
            )
            .await?;
        let done: i64 = row.get("done");
        let total: i64 = row.get("total");
        if total >= 1 && done >= 1 {
            finalized = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    if !finalized {
        return Err(
            "committed txn did not produce a finalized shadow_tracking outbox row".into(),
        );
    }
    println!("✅ Outbox row written and finalized for the committed txn\n");

    // 4. CLEANUP
    ctx.cleanup().await?;
    println!("✅ Test PASSED: Committed Transaction Uses Durable Outbox\n");
    Ok(())
}

/// B1: a transactional commit must not enqueue outbox pointers for message rows
/// that no longer exist. This reproduces the mid-txn rollback leak black-box: the
/// pending message row is deleted after it is written but before commit, while the
/// worker's in-RAM forward buffer still holds the offset. The old code enqueued an
/// orphan `kafka.shadow_tracking` row (never claimable by the poll's EXISTS guard,
/// never cleaned → slow leak). The existence-checked enqueue must drop it.
/// Fails before the fix; passes after.
pub async fn test_committed_txn_leaves_no_orphan_outbox_rows() -> TestResult {
    println!("=== Test: Committed Txn Leaves No Orphan Outbox Rows (B1) ===\n");

    // 1. SETUP
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("txn-orphan").await;
    let txn_id = format!("shadow-txn-orphan-{}", ctx.test_id);

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

    // 2. ACTION — produce in a txn (message row lands with txn_state='pending' and
    // the shadow forward buffer is populated in the worker's RAM), then delete the
    // pending row before committing to simulate the mid-txn rollback.
    let producer = create_transactional_producer(&txn_id)?;
    producer.init_transactions(Duration::from_secs(10))?;
    producer.begin_transaction()?;
    producer
        .send(
            FutureRecord::to(&topic).key(&b"k"[..]).payload(&b"v"[..]),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(e, _)| e)?;

    let deleted = ctx
        .db()
        .execute(
            r#"
            DELETE FROM kafka.messages m
            USING kafka.topics t
            WHERE m.topic_id = t.id AND t.name = $1 AND m.txn_state = 'pending'
            "#,
            &[&topic],
        )
        .await?;
    assert!(deleted >= 1, "expected a pending message row to delete");

    producer.commit_transaction(Duration::from_secs(10))?;
    println!("✅ Transaction committed after deleting the pending message row\n");

    // 3. VERIFY — the commit's outbox enqueue must not have written a pointer for
    // the now-missing message row.
    let row = ctx
        .db()
        .query_one(
            r#"
            SELECT COUNT(*) AS orphans
            FROM kafka.shadow_tracking st
            JOIN kafka.topics t ON t.id = st.topic_id
            WHERE t.name = $1
              AND NOT EXISTS (
                SELECT 1 FROM kafka.messages m
                WHERE m.topic_id = st.topic_id
                  AND m.partition_id = st.partition_id
                  AND m.partition_offset = st.local_offset
              )
            "#,
            &[&topic],
        )
        .await?;
    let orphans: i64 = row.get("orphans");
    assert_eq!(
        orphans, 0,
        "commit enqueued {} orphan shadow_tracking row(s) with no backing message",
        orphans
    );
    println!("✅ No orphan outbox rows after commit\n");

    // 4. CLEANUP
    ctx.cleanup().await?;
    println!("✅ Test PASSED: Committed Txn Leaves No Orphan Outbox Rows\n");
    Ok(())
}
