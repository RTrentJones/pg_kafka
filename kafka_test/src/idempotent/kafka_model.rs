//! Kafka's idempotent-producer model over the raw protocol.

use crate::common::TestResult;
use crate::error_codes;
use crate::fixtures::topic_with_partitions as topic;
use crate::setup::TestContext;
use crate::wire::records::batch;
use crate::wire::Wire;
use std::time::Duration;

/// Kafka's idempotent producer model: a retry of any of the last 5 batches is acked with
/// its ORIGINAL base offset; a never-sent range at or below the last sequence, an
/// overlap, a gap or a retry older than the cache is OUT_OF_ORDER_SEQUENCE_NUMBER; a
/// client-bumped epoch must restart at 0 and fences the old epoch with
/// INVALID_PRODUCER_EPOCH.
pub async fn test_idempotent_duplicates_and_epochs_follow_kafka() -> TestResult {
    println!("=== Test: idempotent producer follows Kafka's model ===\n");
    let ctx = TestContext::new().await?;
    let t = topic(&ctx, "txn-idem", 1).await?;
    let mut w = Wire::connect().await?;
    let (err, pid, ep) = w.init_producer_id(None, 60_000).await?;
    assert_eq!(err, error_codes::NONE);
    let b = |seq: i32, n: usize, epoch: i16| {
        let values: Vec<&str> = (0..n).map(|_| "v").collect();
        batch(&values, pid, epoch, seq, false, None)
    };
    assert_eq!(
        w.produce(&t, 0, b(0, 2, ep), None).await?,
        (error_codes::NONE, 0)
    );
    assert_eq!(
        w.produce(&t, 0, b(2, 2, ep), None).await?,
        (error_codes::NONE, 2)
    );
    assert_eq!(
        w.produce(&t, 0, b(0, 2, ep), None).await?,
        (error_codes::NONE, 0),
        "retry of batch 1"
    );
    assert_eq!(
        w.produce(&t, 0, b(2, 2, ep), None).await?,
        (error_codes::NONE, 2),
        "retry of batch 2"
    );
    assert_eq!(
        w.produce(&t, 0, b(1, 1, ep), None).await?.0,
        error_codes::OUT_OF_ORDER_SEQUENCE_NUMBER,
        "never-sent range"
    );
    assert_eq!(
        w.produce(&t, 0, b(0, 3, ep), None).await?.0,
        error_codes::OUT_OF_ORDER_SEQUENCE_NUMBER,
        "overlap"
    );
    assert_eq!(
        w.produce(&t, 0, b(9, 1, ep), None).await?.0,
        error_codes::OUT_OF_ORDER_SEQUENCE_NUMBER,
        "gap"
    );
    for seq in 4..10 {
        assert_eq!(
            w.produce(&t, 0, b(seq, 1, ep), None).await?.0,
            error_codes::NONE
        );
    }
    // Offsets now 0..=9; the cache holds the batches at sequences 5..=9.
    assert_eq!(
        w.produce(&t, 0, b(0, 2, ep), None).await?.0,
        error_codes::OUT_OF_ORDER_SEQUENCE_NUMBER,
        "older than the cache"
    );
    assert_eq!(
        w.produce(&t, 0, b(4, 1, ep), None).await?.0,
        error_codes::OUT_OF_ORDER_SEQUENCE_NUMBER,
        "6th newest"
    );
    assert_eq!(
        w.produce(&t, 0, b(5, 1, ep), None).await?,
        (error_codes::NONE, 5),
        "5th newest"
    );
    let cached: i64 = ctx
        .db()
        .query_one(
            "SELECT COUNT(*) FROM kafka.producer_batches WHERE producer_id = $1",
            &[&pid],
        )
        .await?
        .get(0);
    assert_eq!(cached, 5);

    // The duplicate survives a worker restart (the cache is a table).
    ctx.db()
        .execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity
             WHERE backend_type = 'pg_kafka_listener'",
            &[],
        )
        .await?;
    let mut w = reconnect().await?;
    assert_eq!(
        w.produce(&t, 0, b(9, 1, ep), None).await?,
        (error_codes::NONE, 9),
        "retry after restart"
    );

    // Client-side epoch bump (KIP-360, the Java client does it): must restart at 0.
    assert_eq!(
        w.produce(&t, 0, b(3, 1, ep + 1), None).await?.0,
        error_codes::OUT_OF_ORDER_SEQUENCE_NUMBER
    );
    assert_eq!(
        w.produce(&t, 0, b(0, 1, ep + 1), None).await?,
        (error_codes::NONE, 10)
    );
    assert_eq!(
        w.produce(&t, 0, b(10, 1, ep), None).await?.0,
        error_codes::INVALID_PRODUCER_EPOCH,
        "old epoch"
    );
    assert_eq!(
        w.produce(&t, 0, b(1, 1, ep + 1), None).await?,
        (error_codes::NONE, 11)
    );
    assert_eq!(
        w.list_offsets(&t, 0, -1, 0).await?,
        (error_codes::NONE, 12, -1)
    );
    let rows: i64 = ctx
        .db()
        .query_one(
            "SELECT COUNT(*) FROM kafka.messages m JOIN kafka.topics t ON t.id = m.topic_id WHERE t.name = $1",
            &[&t],
        )
        .await?
        .get(0);
    assert_eq!(rows, 12, "nothing written twice, nothing dropped");
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

async fn reconnect() -> Result<Wire, Box<dyn std::error::Error>> {
    for _ in 0..60 {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let mut w = match Wire::connect().await {
            Ok(w) => w,
            Err(_) => continue,
        };
        let ready = w.init_producer_id(None, 60_000).await.is_ok();
        if ready {
            return Ok(w);
        }
    }
    Err("listener did not come back".into())
}

/// Sequence numbers wrap at i32::MAX, as in Kafka.
pub async fn test_idempotent_sequence_wraps_at_i32_max() -> TestResult {
    println!("=== Test: idempotent sequence wraps at i32::MAX ===\n");
    let ctx = TestContext::new().await?;
    let t = topic(&ctx, "txn-wrap", 1).await?;
    let mut w = Wire::connect().await?;
    let (err, pid, ep) = w.init_producer_id(None, 60_000).await?;
    assert_eq!(err, error_codes::NONE);
    assert_eq!(
        w.produce(&t, 0, batch(&["a"], pid, ep, 0, false, None), None)
            .await?,
        (error_codes::NONE, 0)
    );
    // Jump the stored state to just below the wrap (no client sends 2^31 records here).
    ctx.db()
        .execute(
            "UPDATE kafka.producer_sequences SET last_sequence = 2147483645 WHERE producer_id = $1",
            &[&pid],
        )
        .await?;
    // 2147483646, 2147483647, 0: crosses the wrap.
    assert_eq!(
        w.produce(
            &t,
            0,
            batch(&["b", "c", "d"], pid, ep, 2147483646, false, None),
            None
        )
        .await?,
        (error_codes::NONE, 1)
    );
    assert_eq!(
        w.produce(&t, 0, batch(&["e"], pid, ep, 1, false, None), None)
            .await?,
        (error_codes::NONE, 4)
    );
    assert_eq!(
        w.produce(
            &t,
            0,
            batch(&["b", "c", "d"], pid, ep, 2147483646, false, None),
            None
        )
        .await?,
        (error_codes::NONE, 1),
        "retry of the wrapping batch"
    );
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
