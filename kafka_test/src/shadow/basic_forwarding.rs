//! Basic shadow mode forwarding tests
//!
//! Tests for DualWrite/ExternalOnly modes with sync/async forwarding.

use crate::common::{create_producer, TestResult};
use crate::setup::TestContext;
use rdkafka::producer::FutureRecord;
use std::time::Duration;

use super::assertions::{
    assert_external_kafka_empty, assert_external_message_count, assert_local_message_count,
    assert_message_in_external_kafka,
};
use super::external_client::verify_external_kafka_ready;
use super::helpers::{enable_shadow_mode, ShadowMode, ShadowTopicConfig, SyncMode, WriteMode};

/// Test DualWrite mode with synchronous forwarding
///
/// Produces a message and verifies it appears in both local DB and external Kafka.
pub async fn test_dual_write_sync() -> TestResult {
    println!("=== Test: DualWrite Sync Mode ===\n");

    // 1. SETUP
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("dual-write-sync").await;

    println!("Step 1: Verifying external Kafka is ready...");
    verify_external_kafka_ready().await?;
    println!("✅ External Kafka ready\n");

    println!("Step 2: Enabling shadow mode (DualWrite + Sync)...");
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

    // 2. ACTION
    println!("Step 3: Producing message to pg_kafka...");
    let producer = create_producer()?;
    let key = b"dual-write-sync-key";
    let value = b"dual-write-sync-value";

    producer
        .send(
            FutureRecord::to(&topic).key(&key[..]).payload(&value[..]),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(e, _)| e)?;
    println!("✅ Message produced\n");

    // 3. VERIFY
    println!("=== Verification ===\n");

    // Give some time for sync forwarding to complete
    tokio::time::sleep(Duration::from_millis(500)).await;

    // 3a. Local DB verification
    println!("Checking local database...");
    assert_local_message_count(ctx.db(), &topic, 1).await?;
    println!("✅ Message found in local database\n");

    // 3b. External Kafka verification
    println!("Checking external Kafka...");
    assert_message_in_external_kafka(&topic, Some(key), Some(value), Duration::from_secs(10))
        .await?;
    println!("✅ Message found in external Kafka\n");

    // 4. CLEANUP
    ctx.cleanup().await?;
    println!("✅ Test PASSED: DualWrite Sync Mode\n");
    Ok(())
}

/// Test DualWrite mode with asynchronous forwarding
///
/// Produces a message and verifies it appears in both local DB and external Kafka
/// (with some delay for async delivery).
pub async fn test_dual_write_async() -> TestResult {
    println!("=== Test: DualWrite Async Mode ===\n");

    // 1. SETUP
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("dual-write-async").await;

    println!("Step 1: Verifying external Kafka is ready...");
    verify_external_kafka_ready().await?;
    println!("✅ External Kafka ready\n");

    println!("Step 2: Enabling shadow mode (DualWrite + Async)...");
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
    println!("✅ Shadow mode enabled\n");

    // 2. ACTION
    println!("Step 3: Producing message to pg_kafka...");
    let producer = create_producer()?;
    let key = b"dual-write-async-key";
    let value = b"dual-write-async-value";

    producer
        .send(
            FutureRecord::to(&topic).key(&key[..]).payload(&value[..]),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(e, _)| e)?;
    println!("✅ Message produced\n");

    // 3. VERIFY
    println!("=== Verification ===\n");

    // 3a. Local DB verification (should be immediate)
    println!("Checking local database...");
    assert_local_message_count(ctx.db(), &topic, 1).await?;
    println!("✅ Message found in local database\n");

    // 3b. External Kafka verification (may need to wait for async delivery)
    println!("Checking external Kafka (waiting for async forwarding)...");
    // Async mode may take longer, use extended timeout
    assert_message_in_external_kafka(&topic, Some(key), Some(value), Duration::from_secs(15))
        .await?;
    println!("✅ Message found in external Kafka\n");

    // 4. CLEANUP
    ctx.cleanup().await?;
    println!("✅ Test PASSED: DualWrite Async Mode\n");
    Ok(())
}

/// Test ExternalOnly mode
///
/// Produces a message and verifies it only appears in external Kafka,
/// NOT in the local database (when external succeeds).
pub async fn test_external_only_mode() -> TestResult {
    println!("=== Test: ExternalOnly Mode ===\n");

    // 1. SETUP
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("external-only").await;

    println!("Step 1: Verifying external Kafka is ready...");
    verify_external_kafka_ready().await?;
    println!("✅ External Kafka ready\n");

    println!("Step 2: Enabling shadow mode (ExternalOnly + Sync)...");
    enable_shadow_mode(
        ctx.db(),
        &topic,
        &ShadowTopicConfig {
            mode: ShadowMode::Shadow,
            forward_percentage: 100,
            external_topic_name: None,
            sync_mode: SyncMode::Sync,
            write_mode: WriteMode::ExternalOnly,
        },
    )
    .await?;
    println!("✅ Shadow mode enabled\n");

    // 2. ACTION
    println!("Step 3: Producing message to pg_kafka...");
    let producer = create_producer()?;
    let key = b"external-only-key";
    let value = b"external-only-value";

    producer
        .send(
            FutureRecord::to(&topic).key(&key[..]).payload(&value[..]),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(e, _)| e)?;
    println!("✅ Message produced\n");

    // 3. VERIFY
    println!("=== Verification ===\n");

    // Give some time for processing
    tokio::time::sleep(Duration::from_millis(500)).await;

    // 3a. External Kafka verification - should have the message
    println!("Checking external Kafka...");
    assert_message_in_external_kafka(&topic, Some(key), Some(value), Duration::from_secs(10))
        .await?;
    println!("✅ Message found in external Kafka\n");

    // 3b. Local DB verification - should NOT have the message (ExternalOnly skips local on success)
    println!("Checking local database (should be empty)...");
    assert_local_message_count(ctx.db(), &topic, 0).await?;
    println!("✅ No message in local database (as expected for ExternalOnly)\n");

    // 4. CLEANUP
    ctx.cleanup().await?;
    println!("✅ Test PASSED: ExternalOnly Mode\n");
    Ok(())
}

/// Test LocalOnly mode (shadow disabled)
///
/// Produces a message and verifies it only appears in local database,
/// NOT forwarded to external Kafka.
pub async fn test_local_only_mode() -> TestResult {
    println!("=== Test: LocalOnly Mode ===\n");

    // 1. SETUP
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("local-only").await;

    println!("Step 1: Enabling shadow mode with LocalOnly (shadow disabled)...");
    enable_shadow_mode(
        ctx.db(),
        &topic,
        &ShadowTopicConfig {
            mode: ShadowMode::LocalOnly,
            forward_percentage: 100,
            external_topic_name: None,
            sync_mode: SyncMode::Sync,
            write_mode: WriteMode::DualWrite,
        },
    )
    .await?;
    println!("✅ LocalOnly mode enabled\n");

    // 2. ACTION
    println!("Step 2: Producing message to pg_kafka...");
    let producer = create_producer()?;
    let key = b"local-only-key";
    let value = b"local-only-value";

    producer
        .send(
            FutureRecord::to(&topic).key(&key[..]).payload(&value[..]),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(e, _)| e)?;
    println!("✅ Message produced\n");

    // 3. VERIFY
    println!("=== Verification ===\n");

    // Give some time for processing
    tokio::time::sleep(Duration::from_millis(500)).await;

    // 3a. Local DB verification - should have the message
    println!("Checking local database...");
    assert_local_message_count(ctx.db(), &topic, 1).await?;
    println!("✅ Message found in local database\n");

    // 3b. External Kafka verification - should NOT have the message
    println!("Checking external Kafka (should be empty)...");
    // Use shorter timeout since we expect nothing
    assert_external_kafka_empty(&topic, Duration::from_secs(3)).await?;
    println!("✅ No message in external Kafka (as expected for LocalOnly)\n");

    // 4. CLEANUP
    ctx.cleanup().await?;
    println!("✅ Test PASSED: LocalOnly Mode\n");
    Ok(())
}

/// Test that a reload drops a DELETED topic config from the cache (SH-3)
///
/// Regression: the in-memory topic config cache was only ever *updated* on
/// reload, never cleared, so a `kafka.shadow_config` row that was DELETED kept
/// forwarding forever. After the fix the cache is replaced on each successful
/// reload, so a deleted topic stops forwarding (but is still stored locally).
pub async fn test_reload_clears_deleted_topic_config() -> TestResult {
    println!("=== Test: Reload Clears Deleted Topic Config (SH-3) ===\n");

    // 1. SETUP
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("reload-clears-cache").await;

    println!("Step 1: Verifying external Kafka is ready...");
    verify_external_kafka_ready().await?;
    println!("✅ External Kafka ready\n");

    println!("Step 2: Enabling shadow mode (DualWrite + Sync, 100%)...");
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

    let producer = create_producer()?;

    // 2a. Produce before delete — must be forwarded.
    println!("Step 3: Producing a message (expect forwarded)...");
    let key1 = b"before-delete-key";
    let value1 = b"before-delete-value";
    producer
        .send(
            FutureRecord::to(&topic).key(&key1[..]).payload(&value1[..]),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(e, _)| e)?;
    assert_message_in_external_kafka(&topic, Some(key1), Some(value1), Duration::from_secs(15))
        .await?;
    println!("✅ First message forwarded\n");

    // 2b. DELETE the shadow_config row (not just set local_only) and reload.
    println!("Step 4: Deleting shadow_config row and reloading...");
    ctx.db()
        .execute(
            r#"
            DELETE FROM kafka.shadow_config sc
            USING kafka.topics t
            WHERE sc.topic_id = t.id AND t.name = $1
            "#,
            &[&topic],
        )
        .await?;
    ctx.db().execute("SELECT pg_reload_conf()", &[]).await?;
    // Wait out the SIGHUP + the 2s reload interval so the worker reloads.
    tokio::time::sleep(Duration::from_millis(3000)).await;
    println!("✅ Config deleted and reload window elapsed\n");

    // 2c. Produce after delete — must NOT be forwarded, but stored locally.
    println!("Step 5: Producing a message after delete (expect NOT forwarded)...");
    let key2 = b"after-delete-key";
    let value2 = b"after-delete-value";
    producer
        .send(
            FutureRecord::to(&topic).key(&key2[..]).payload(&value2[..]),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(e, _)| e)?;
    tokio::time::sleep(Duration::from_millis(500)).await;
    println!("✅ Second message produced\n");

    // 3. VERIFY
    println!("=== Verification ===\n");
    // Local: both messages persisted (deleted config still stores locally).
    assert_local_message_count(ctx.db(), &topic, 2).await?;
    println!("✅ Both messages stored locally\n");
    // External: only the first message — the second was NOT forwarded because
    // the deleted row was evicted from the cache on reload.
    assert_external_message_count(&topic, 1, Duration::from_secs(5)).await?;
    println!("✅ Only the pre-delete message was forwarded (cache cleared)\n");

    // 4. CLEANUP
    ctx.cleanup().await?;
    println!("✅ Test PASSED: Reload Clears Deleted Topic Config\n");
    Ok(())
}

/// Test the durable-outbox lifecycle end to end (SH-9)
///
/// Produces in async mode and asserts both that the message reaches external
/// Kafka (forwarded by the periodic poll, not inline) AND that the
/// `kafka.shadow_tracking` outbox row is written and then *finalized* — its
/// `external_offset` becomes non-NULL once the forward ack is applied. This is
/// what proves the durable path: a row is the source of truth, forwarded
/// at-least-once and marked done exactly once.
pub async fn test_outbox_row_written_and_finalized() -> TestResult {
    println!("=== Test: Durable Outbox Row Written + Finalized (SH-9) ===\n");

    // 1. SETUP
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("outbox-finalized").await;

    verify_external_kafka_ready().await?;
    enable_shadow_mode(
        ctx.db(),
        &topic,
        &ShadowTopicConfig {
            mode: ShadowMode::Shadow,
            forward_percentage: 100,
            external_topic_name: None,
            sync_mode: SyncMode::Async, // async → the poll does the forwarding
            write_mode: WriteMode::DualWrite,
        },
    )
    .await?;

    // 2. ACTION
    let producer = create_producer()?;
    let key = b"outbox-key";
    let value = b"outbox-value";
    producer
        .send(
            FutureRecord::to(&topic).key(&key[..]).payload(&value[..]),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(e, _)| e)?;
    println!("✅ Message produced (async)\n");

    // 3. VERIFY
    // 3a. Reaches external Kafka via the poll.
    assert_message_in_external_kafka(&topic, Some(key), Some(value), Duration::from_secs(15))
        .await?;
    println!("✅ Forwarded to external Kafka\n");

    // 3b. The outbox row exists and is finalized (external_offset NOT NULL).
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
            "shadow_tracking outbox row was not written and finalized (external_offset NULL)"
                .into(),
        );
    }
    println!("✅ Outbox row finalized (external_offset set)\n");

    // 4. CLEANUP
    ctx.cleanup().await?;
    println!("✅ Test PASSED: Durable Outbox Row Written + Finalized\n");
    Ok(())
}
