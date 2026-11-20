//! An idempotent producer the broker holds no state for (its row expired or was never
//! issued here): Kafka (KIP-360) accepts its batch at any sequence and tracks it from
//! there.

use crate::common::{create_idempotent_producer, TestResult};
use crate::error_codes;
use crate::fixtures::topic_with_partitions as topic;
use crate::setup::TestContext;
use crate::wire::records::batch;
use crate::wire::Wire;
use rdkafka::producer::FutureRecord;
use std::time::Duration;
use tokio_postgres::Client;

/// Forget a producer the way the stale-producer prune does: its row and its sequences.
async fn forget_producer(db: &Client, producer_id: i64) -> TestResult {
    db.execute(
        "DELETE FROM kafka.producer_sequences WHERE producer_id = $1",
        &[&producer_id],
    )
    .await?;
    db.execute(
        "DELETE FROM kafka.producer_ids WHERE producer_id = $1",
        &[&producer_id],
    )
    .await?;
    Ok(())
}

async fn row_count(db: &Client, topic: &str) -> Result<i64, Box<dyn std::error::Error>> {
    Ok(db
        .query_one(
            "SELECT COUNT(*) FROM kafka.messages m JOIN kafka.topics t ON t.id = m.topic_id
             WHERE t.name = $1",
            &[&topic],
        )
        .await?
        .get(0))
}

/// Over the raw protocol, as Kafka 3.7 answers: a forgotten producer's next batch is
/// accepted at its sequence, and from then on retries are duplicates, gaps are
/// OUT_OF_ORDER_SEQUENCE_NUMBER and a lower epoch is INVALID_PRODUCER_EPOCH. A producer
/// id this broker never issued is accepted too, and a later InitProducerId does not hand
/// that id out again.
pub async fn test_unknown_producer_is_accepted_as_new() -> TestResult {
    println!("=== Test: Unknown Idempotent Producer Is Accepted As New ===\n");
    let ctx = TestContext::new().await?;
    let db = ctx.db();
    let t = topic(&ctx, "idem-unknown", 1).await?;
    let mut w = Wire::connect().await?;

    println!("Step 1: A producer writes sequence 0, then its state is pruned...");
    let (err, pid, ep) = w.init_producer_id(None, 60_000).await?;
    assert_eq!(err, error_codes::NONE);
    let b = |values: &[&str], seq: i32, pid: i64, epoch: i16| {
        batch(values, pid, epoch, seq, false, None)
    };
    assert_eq!(
        w.produce(&t, 0, b(&["a"], 0, pid, ep), None).await?,
        (error_codes::NONE, 0)
    );
    forget_producer(db, pid).await?;
    println!("✅ Row and sequences deleted\n");

    println!("Step 2: Its next batches...");
    assert_eq!(
        w.produce(&t, 0, b(&["b", "c"], 1, pid, ep), None).await?,
        (error_codes::NONE, 1),
        "a producer without state is accepted at any sequence"
    );
    assert_eq!(
        w.produce(&t, 0, b(&["b", "c"], 1, pid, ep), None).await?,
        (error_codes::NONE, 1),
        "the retry is a duplicate with its original offset"
    );
    assert_eq!(
        w.produce(&t, 0, b(&["x"], 9, pid, ep), None).await?.0,
        error_codes::OUT_OF_ORDER_SEQUENCE_NUMBER,
        "its sequence is tracked again"
    );
    assert_eq!(
        w.produce(&t, 0, b(&["d"], 3, pid, ep), None).await?,
        (error_codes::NONE, 3)
    );
    let stored: i16 = db
        .query_one(
            "SELECT epoch FROM kafka.producer_ids WHERE producer_id = $1 AND transactional_id IS NULL",
            &[&pid],
        )
        .await?
        .get(0);
    assert_eq!(
        stored, ep,
        "the producer is recorded at its presented epoch"
    );
    println!("✅ Accepted, deduplicated and sequence-checked again\n");

    println!("Step 3: A never-issued producer id at a high epoch and sequence...");
    let next_id: i64 = db
        .query_one(
            "SELECT last_value + 1 FROM kafka.producer_ids_producer_id_seq",
            &[],
        )
        .await?
        .get(0);
    assert_eq!(
        w.produce(&t, 0, b(&["e"], 5, next_id, 7), None).await?,
        (error_codes::NONE, 4)
    );
    assert_eq!(
        w.produce(&t, 0, b(&["f"], 6, next_id, 6), None).await?.0,
        error_codes::INVALID_PRODUCER_EPOCH,
        "a lower epoch than the one it was recorded at"
    );
    let (err, fresh, _) = w.init_producer_id(None, 60_000).await?;
    assert_eq!(err, error_codes::NONE, "InitProducerId still allocates");
    assert_ne!(fresh, next_id, "an id in use is not handed out again");
    println!("✅ Accepted; InitProducerId skipped id {next_id} and gave {fresh}\n");

    assert_eq!(row_count(db, &t).await?, 5, "a, b, c, d, e: nothing twice");
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// A live librdkafka idempotent producer whose broker state is pruned keeps producing
/// (it used to get UNKNOWN_PRODUCER_ID on every batch).
pub async fn test_idempotent_producer_survives_pruned_state() -> TestResult {
    println!("=== Test: Idempotent Producer Survives Pruned State ===\n");
    let ctx = TestContext::new().await?;
    let db = ctx.db();
    let t = topic(&ctx, "idem-pruned", 1).await?;
    let producer = create_idempotent_producer()?;
    let send = |v: String| {
        let producer = producer.clone();
        let t = t.clone();
        async move {
            producer
                .send(
                    FutureRecord::to(&t).payload(&v).key("k").partition(0),
                    Duration::from_secs(10),
                )
                .await
                .map_err(|(e, _)| e)
        }
    };

    send("before".into()).await?;
    let pid: i64 = db
        .query_one(
            "SELECT producer_id FROM kafka.producer_sequences s JOIN kafka.topics t ON t.id = s.topic_id
             WHERE t.name = $1",
            &[&t],
        )
        .await?
        .get(0);
    forget_producer(db, pid).await?;
    println!("Pruned producer {pid}; producing on...");
    for i in 0..3 {
        send(format!("after-{i}")).await?;
    }
    assert_eq!(row_count(db, &t).await?, 4);
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
