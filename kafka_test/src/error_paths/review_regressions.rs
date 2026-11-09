//! Regression tests for the follow-up review after #98.
//!
//! Each test drives the broker the way a real client does (rdkafka where it can,
//! raw wire requests where a specific request shape is needed) and checks both the
//! protocol response and the stored state.

use crate::common::{create_transactional_producer, TestResult};
use crate::setup::TestContext;
use rdkafka::producer::{FutureRecord, Producer};
use std::time::Duration;

async fn count_messages(ctx: &TestContext, topic: &str) -> Result<i64, Box<dyn std::error::Error>> {
    Ok(ctx
        .db()
        .query_one(
            "SELECT COUNT(*) FROM kafka.messages m JOIN kafka.topics t ON m.topic_id = t.id
             WHERE t.name = $1 AND m.txn_state IS NULL",
            &[&topic],
        )
        .await?
        .get(0))
}

async fn produce_txn(
    txn_id: &str,
    topic: &str,
    count: usize,
    label: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let producer = create_transactional_producer(txn_id)?;
    producer.init_transactions(Duration::from_secs(10))?;
    producer.begin_transaction()?;
    for i in 0..count {
        let value = format!("{}-{}", label, i);
        producer
            .send(
                FutureRecord::<(), _>::to(topic)
                    .payload(&value)
                    .partition(0),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(e, _)| e)?;
    }
    producer.commit_transaction(Duration::from_secs(10))?;
    Ok(())
}

/// A transactional producer that restarts with the same transactional.id gets a new
/// epoch and restarts its sequence numbers at 0. Its records must be stored, not
/// dropped as duplicates of the previous epoch's sequence range.
pub async fn test_producer_restart_new_epoch_records_are_stored() -> TestResult {
    println!("=== Test: Producer Restart (New Epoch) Records Are Stored ===\n");

    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("review-epoch-restart").await;
    let txn_id = format!("review-epoch-restart-{}", uuid::Uuid::new_v4());

    println!("Step 1: first producer instance commits 10 records...");
    produce_txn(&txn_id, &topic, 10, "first").await?;
    assert_eq!(count_messages(&ctx, &topic).await?, 10);

    println!("Step 2: restarted instance (same transactional.id) commits 3 records...");
    produce_txn(&txn_id, &topic, 3, "second").await?;

    let stored = count_messages(&ctx, &topic).await?;
    println!("   committed records stored: {}", stored);
    if stored != 13 {
        return Err(format!(
            "expected 13 committed records after the restart, found {} \
             (the new epoch's batches were dropped as duplicates)",
            stored
        )
        .into());
    }

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// last_active_at only moves on InitProducerId, so an idempotent producer that keeps
/// producing for longer than the 7-day producer retention looked idle and was pruned
/// (with its sequences); its next produce then failed with UNKNOWN_PRODUCER_ID. The
/// sweep must treat recent sequence activity as liveness.
pub async fn test_active_idempotent_producer_survives_retention_sweep() -> TestResult {
    println!("=== Test: Active Idempotent Producer Survives Retention Sweep ===\n");

    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("review-producer-prune").await;
    let producer = crate::common::create_idempotent_producer()?;
    let send = |v: &'static str| {
        let producer = producer.clone();
        let topic = topic.clone();
        async move {
            producer
                .send(
                    FutureRecord::<(), _>::to(&topic).payload(v).partition(0),
                    Duration::from_secs(5),
                )
                .await
                .map_err(|(e, _)| e)
        }
    };
    send("before").await?;

    let producer_id: i64 = ctx
        .db()
        .query_one(
            "SELECT s.producer_id FROM kafka.producer_sequences s
             JOIN kafka.topics t ON s.topic_id = t.id WHERE t.name = $1",
            &[&topic],
        )
        .await?
        .get(0);
    // Pretend InitProducerId ran 8 days ago; the producer has been producing since.
    ctx.db()
        .execute(
            "UPDATE kafka.producer_ids SET last_active_at = NOW() - INTERVAL '8 days'
             WHERE producer_id = $1",
            &[&producer_id],
        )
        .await?;
    ctx.db()
        .query("SELECT * FROM pg_kafka_run_retention_sweep(0, 60)", &[])
        .await?;

    let still_there: i64 = ctx
        .db()
        .query_one(
            "SELECT COUNT(*) FROM kafka.producer_ids WHERE producer_id = $1",
            &[&producer_id],
        )
        .await?
        .get(0);
    println!(
        "   producer {} rows after sweep: {}",
        producer_id, still_there
    );
    if still_there != 1 {
        return Err("the sweep pruned a producer that is still producing".into());
    }

    send("after").await?;
    assert_eq!(count_messages(&ctx, &topic).await?, 2);

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// A producer fenced by a newer instance (same transactional.id) must get an error
/// when it tries to write again, and nothing it sends may become visible. The fenced
/// AddPartitionsToTxn used to come back as an empty response (its top-level error
/// field only exists from v4, and the broker advertises v0-3), which reads as success.
pub async fn test_fenced_producer_cannot_write() -> TestResult {
    println!("=== Test: Fenced Producer Cannot Write ===\n");

    let ctx = TestContext::new().await?;
    let topic = crate::fixtures::TestTopicBuilder::new(&ctx, "review-fenced-write")
        .with_partitions(2)
        .build()
        .await?;
    let txn_id = format!("review-fenced-{}", uuid::Uuid::new_v4());

    let zombie = create_transactional_producer(&txn_id)?;
    zombie.init_transactions(Duration::from_secs(10))?;
    let newer = create_transactional_producer(&txn_id)?;
    newer.init_transactions(Duration::from_secs(10))?;
    println!("Step 1: second instance initialised; the first is now fenced");

    // The zombie writes to a partition it never added, so the broker sees
    // AddPartitionsToTxn (then Produce) carrying the stale epoch.
    let outcome: Result<(), String> = async {
        zombie.begin_transaction().map_err(|e| e.to_string())?;
        zombie
            .send(
                FutureRecord::<(), _>::to(&topic.name)
                    .payload("zombie")
                    .partition(1),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(e, _)| e.to_string())?;
        zombie
            .commit_transaction(Duration::from_secs(10))
            .map_err(|e| e.to_string())
    }
    .await;
    println!("   zombie outcome: {:?}", outcome);

    let visible = count_messages(&ctx, &topic.name).await?;
    let fatal = zombie.client().fatal_error();
    println!(
        "   committed records: {}, fatal error: {:?}",
        visible, fatal
    );
    if outcome.is_ok() {
        return Err("the fenced producer's transaction was reported as committed".into());
    }
    // The broker must tell the client it was fenced (a fatal ProducerFenced error),
    // not leave it to time out.
    match fatal {
        Some((rdkafka::types::RDKafkaErrorCode::Fenced, _))
        | Some((rdkafka::types::RDKafkaErrorCode::ProducerFenced, _)) => {}
        other => {
            return Err(format!(
                "expected rdkafka to record a fatal ProducerFenced error, got {:?}",
                other
            )
            .into())
        }
    }
    assert_eq!(
        visible, 0,
        "a fenced producer's record must not be committed"
    );

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
