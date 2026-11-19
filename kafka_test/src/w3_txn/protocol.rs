//! Wave 3 (txn): raw-protocol checks of ListOffsets, OffsetFetch require_stable,
//! Kafka's idempotent-producer model, transaction validation and fetch batching.
//! The expected codes are what Apache Kafka 3.7 answers to the same requests.

use super::wire::{batch, batch_headers, fetched_values, now_ms, Wire};
use crate::common::TestResult;
use crate::fixtures::TestTopicBuilder;
use crate::setup::TestContext;
use std::time::Duration;

const NONE: i16 = 0;
const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
const OUT_OF_ORDER_SEQUENCE_NUMBER: i16 = 45;
const INVALID_PRODUCER_EPOCH: i16 = 47;
const INVALID_TXN_STATE: i16 = 48;
const INVALID_TRANSACTION_TIMEOUT: i16 = 50;
const OPERATION_NOT_ATTEMPTED: i16 = 55;
const UNSTABLE_OFFSET_COMMIT: i16 = 88;

async fn topic(
    ctx: &TestContext,
    base: &str,
    partitions: i32,
) -> Result<String, Box<dyn std::error::Error>> {
    Ok(TestTopicBuilder::new(ctx, base)
        .with_partitions(partitions)
        .build()
        .await?
        .name)
}

/// ListOffsets: read_committed LATEST is the LSO, -3 is the max-timestamp record, a
/// lookup past the readable end is -1, and a partition the topic lacks is
/// UNKNOWN_TOPIC_OR_PARTITION (were: HWM; the first record; the pending record; NONE
/// and offset 0).
pub async fn test_w3_list_offsets_isolation_max_timestamp_and_partitions() -> TestResult {
    println!("=== Test: ListOffsets isolation, MAX_TIMESTAMP, partition check ===\n");
    let ctx = TestContext::new().await?;
    let t = topic(&ctx, "w3-txn-lo", 1).await?;
    let mut w = Wire::connect().await?;
    let now = now_ms();
    // Offsets 0..=2 with timestamps now, now+50s, now+1s: the max is offset 1.
    let (err, base) = w
        .produce(
            &t,
            0,
            batch(
                &["a", "b", "c"],
                -1,
                -1,
                -1,
                false,
                Some(&[now, now + 50_000, now + 1_000]),
            ),
            None,
        )
        .await?;
    assert_eq!((err, base), (NONE, 0));
    assert_eq!(w.list_offsets(&t, 0, -3, 0).await?, (NONE, 1, now + 50_000));
    assert_eq!(w.list_offsets(&t, 0, -2, 0).await?, (NONE, 0, -1));
    assert_eq!(w.list_offsets(&t, 0, -1, 0).await?, (NONE, 3, -1));
    for ts in [-1, -2, -3, now] {
        assert_eq!(
            w.list_offsets(&t, 7, ts, 0).await?,
            (UNKNOWN_TOPIC_OR_PARTITION, -1, -1),
            "partition 7, timestamp {}",
            ts
        );
    }

    // An open transaction at offset 3 with an even later timestamp.
    let tx = ctx.unique_topic("w3-txn-lo-tx").await;
    let (err, pid, ep) = w.init_producer_id(Some(&tx), 60_000).await?;
    assert_eq!(err, NONE);
    assert_eq!(
        w.add_partitions(&tx, pid, ep, &[(&t, &[0])]).await?[0].2,
        NONE
    );
    let (err, base) = w
        .produce(
            &t,
            0,
            batch(&["t"], pid, ep, 0, true, Some(&[now + 90_000])),
            Some(&tx),
        )
        .await?;
    assert_eq!((err, base), (NONE, 3));
    assert_eq!(w.list_offsets(&t, 0, -1, 0).await?, (NONE, 4, -1), "HWM");
    assert_eq!(w.list_offsets(&t, 0, -1, 1).await?, (NONE, 3, -1), "LSO");
    // Kafka bounds timestamp lookups by the readable end.
    assert_eq!(w.list_offsets(&t, 0, -3, 0).await?, (NONE, 3, now + 90_000));
    assert_eq!(w.list_offsets(&t, 0, -3, 1).await?, (NONE, -1, -1));
    assert_eq!(
        w.list_offsets(&t, 0, now + 60_000, 1).await?,
        (NONE, -1, -1)
    );
    assert_eq!(
        w.list_offsets(&t, 0, now + 60_000, 0).await?,
        (NONE, 3, now + 90_000)
    );
    assert_eq!(w.end_txn(&tx, pid, ep, false).await?, NONE);
    // Aborted rows are not candidates.
    assert_eq!(w.list_offsets(&t, 0, -3, 0).await?, (NONE, 1, now + 50_000));
    assert_eq!(w.list_offsets(&t, 0, -1, 1).await?, (NONE, 4, -1));
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// OffsetFetch v7 require_stable (KIP-447): UNSTABLE_OFFSET_COMMIT while a
/// TxnOffsetCommit is pending, the committed offset once the transaction ends.
pub async fn test_w3_offset_fetch_require_stable() -> TestResult {
    println!("=== Test: OffsetFetch require_stable ===\n");
    let ctx = TestContext::new().await?;
    let t = topic(&ctx, "w3-txn-of", 2).await?;
    let g = ctx.unique_group("w3-txn-of").await;
    let tx = ctx.unique_topic("w3-txn-of-tx").await;
    let mut w = Wire::connect().await?;
    let (err, pid, ep) = w.init_producer_id(Some(&tx), 60_000).await?;
    assert_eq!(err, NONE);
    assert_eq!(w.add_offsets(&tx, pid, ep, &g).await?, NONE);
    assert_eq!(w.txn_offset_commit(&tx, pid, ep, &g, &t, 0, 7).await?, NONE);

    assert_eq!(
        w.offset_fetch(&g, &t, &[0, 1], true).await?,
        (NONE, vec![(0, -1, UNSTABLE_OFFSET_COMMIT), (1, -1, NONE)])
    );
    // Without require_stable: the (absent) committed offset, no error.
    assert_eq!(
        w.offset_fetch(&g, &t, &[0, 1], false).await?,
        (NONE, vec![(0, -1, NONE), (1, -1, NONE)])
    );
    assert_eq!(w.end_txn(&tx, pid, ep, true).await?, NONE);
    assert_eq!(
        w.offset_fetch(&g, &t, &[0], true).await?,
        (NONE, vec![(0, 7, NONE)])
    );
    // An aborted transactional commit leaves nothing pending either.
    let (err, pid, ep) = w.init_producer_id(Some(&tx), 60_000).await?;
    assert_eq!(err, NONE);
    assert_eq!(w.add_offsets(&tx, pid, ep, &g).await?, NONE);
    assert_eq!(w.txn_offset_commit(&tx, pid, ep, &g, &t, 0, 9).await?, NONE);
    assert_eq!(
        w.offset_fetch(&g, &t, &[0], true).await?.1[0].2,
        UNSTABLE_OFFSET_COMMIT
    );
    assert_eq!(w.end_txn(&tx, pid, ep, false).await?, NONE);
    assert_eq!(
        w.offset_fetch(&g, &t, &[0], true).await?,
        (NONE, vec![(0, 7, NONE)])
    );
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// Kafka's idempotent producer model: a retry of any of the last 5 batches is acked with
/// its ORIGINAL base offset; a never-sent range at or below the last sequence, an
/// overlap, a gap or a retry older than the cache is OUT_OF_ORDER_SEQUENCE_NUMBER; a
/// client-bumped epoch must restart at 0 and fences the old epoch with
/// INVALID_PRODUCER_EPOCH. Before wave 3 every range at or below the last sequence was
/// acked with offset -1 and dropped, and any other epoch was PRODUCER_FENCED.
pub async fn test_w3_idempotent_producer_follows_kafka() -> TestResult {
    println!("=== Test: idempotent producer follows Kafka's model ===\n");
    let ctx = TestContext::new().await?;
    let t = topic(&ctx, "w3-txn-idem", 1).await?;
    let mut w = Wire::connect().await?;
    let (err, pid, ep) = w.init_producer_id(None, 60_000).await?;
    assert_eq!(err, NONE);
    let b = |seq: i32, n: usize, epoch: i16| {
        let values: Vec<&str> = (0..n).map(|_| "v").collect();
        batch(&values, pid, epoch, seq, false, None)
    };
    assert_eq!(w.produce(&t, 0, b(0, 2, ep), None).await?, (NONE, 0));
    assert_eq!(w.produce(&t, 0, b(2, 2, ep), None).await?, (NONE, 2));
    assert_eq!(
        w.produce(&t, 0, b(0, 2, ep), None).await?,
        (NONE, 0),
        "retry of batch 1"
    );
    assert_eq!(
        w.produce(&t, 0, b(2, 2, ep), None).await?,
        (NONE, 2),
        "retry of batch 2"
    );
    assert_eq!(
        w.produce(&t, 0, b(1, 1, ep), None).await?.0,
        OUT_OF_ORDER_SEQUENCE_NUMBER,
        "never-sent range"
    );
    assert_eq!(
        w.produce(&t, 0, b(0, 3, ep), None).await?.0,
        OUT_OF_ORDER_SEQUENCE_NUMBER,
        "overlap"
    );
    assert_eq!(
        w.produce(&t, 0, b(9, 1, ep), None).await?.0,
        OUT_OF_ORDER_SEQUENCE_NUMBER,
        "gap"
    );
    for seq in 4..10 {
        assert_eq!(w.produce(&t, 0, b(seq, 1, ep), None).await?.0, NONE);
    }
    // Offsets now 0..=9; the cache holds the batches at sequences 5..=9.
    assert_eq!(
        w.produce(&t, 0, b(0, 2, ep), None).await?.0,
        OUT_OF_ORDER_SEQUENCE_NUMBER,
        "older than the cache"
    );
    assert_eq!(
        w.produce(&t, 0, b(4, 1, ep), None).await?.0,
        OUT_OF_ORDER_SEQUENCE_NUMBER,
        "6th newest"
    );
    assert_eq!(
        w.produce(&t, 0, b(5, 1, ep), None).await?,
        (NONE, 5),
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
        (NONE, 9),
        "retry after restart"
    );

    // Client-side epoch bump (KIP-360, the Java client does it): must restart at 0.
    assert_eq!(
        w.produce(&t, 0, b(3, 1, ep + 1), None).await?.0,
        OUT_OF_ORDER_SEQUENCE_NUMBER
    );
    assert_eq!(w.produce(&t, 0, b(0, 1, ep + 1), None).await?, (NONE, 10));
    assert_eq!(
        w.produce(&t, 0, b(10, 1, ep), None).await?.0,
        INVALID_PRODUCER_EPOCH,
        "old epoch"
    );
    assert_eq!(w.produce(&t, 0, b(1, 1, ep + 1), None).await?, (NONE, 11));
    assert_eq!(w.list_offsets(&t, 0, -1, 0).await?, (NONE, 12, -1));
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
pub async fn test_w3_idempotent_sequence_wraps() -> TestResult {
    println!("=== Test: idempotent sequence wraps at i32::MAX ===\n");
    let ctx = TestContext::new().await?;
    let t = topic(&ctx, "w3-txn-wrap", 1).await?;
    let mut w = Wire::connect().await?;
    let (err, pid, ep) = w.init_producer_id(None, 60_000).await?;
    assert_eq!(err, NONE);
    assert_eq!(
        w.produce(&t, 0, batch(&["a"], pid, ep, 0, false, None), None)
            .await?,
        (NONE, 0)
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
        (NONE, 1)
    );
    assert_eq!(
        w.produce(&t, 0, batch(&["e"], pid, ep, 1, false, None), None)
            .await?,
        (NONE, 4)
    );
    assert_eq!(
        w.produce(
            &t,
            0,
            batch(&["b", "c", "d"], pid, ep, 2147483646, false, None),
            None
        )
        .await?,
        (NONE, 1),
        "retry of the wrapping batch"
    );
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// Transaction validation: InitProducerId timeouts, AddPartitionsToTxn on an unknown
/// topic (not created any more), and a transactional Produce to a partition the
/// transaction did not add.
pub async fn test_w3_transaction_validation() -> TestResult {
    println!("=== Test: transaction validation ===\n");
    let ctx = TestContext::new().await?;
    let t = topic(&ctx, "w3-txn-val", 2).await?;
    let tx = ctx.unique_topic("w3-txn-val-tx").await;
    let mut w = Wire::connect().await?;
    for timeout in [-5, 0, 900_001, i32::MAX] {
        assert_eq!(
            w.init_producer_id(Some(&tx), timeout).await?,
            (INVALID_TRANSACTION_TIMEOUT, -1, -1),
            "timeout {}",
            timeout
        );
    }
    // Idempotent producers ignore the timeout.
    assert_eq!(w.init_producer_id(None, -5).await?.0, NONE);
    let (err, pid, ep) = w.init_producer_id(Some(&tx), 900_000).await?;
    assert_eq!(err, NONE);

    let missing = ctx.unique_topic("w3-txn-val-missing").await;
    assert_eq!(
        w.add_partitions(&tx, pid, ep, &[(&missing, &[0]), (&t, &[0])])
            .await?,
        vec![
            (missing.clone(), 0, UNKNOWN_TOPIC_OR_PARTITION),
            (t.clone(), 0, OPERATION_NOT_ATTEMPTED)
        ]
    );
    let created: i64 = ctx
        .db()
        .query_one(
            "SELECT COUNT(*) FROM kafka.topics WHERE name = $1",
            &[&missing],
        )
        .await?
        .get(0);
    assert_eq!(created, 0, "AddPartitionsToTxn must not create topics");
    assert_eq!(
        w.add_partitions(&tx, pid, ep, &[(&t, &[0, 9])]).await?,
        vec![
            (t.clone(), 0, OPERATION_NOT_ATTEMPTED),
            (t.clone(), 9, UNKNOWN_TOPIC_OR_PARTITION)
        ]
    );
    assert_eq!(
        w.add_partitions(&tx, pid, ep, &[(&t, &[0])]).await?[0].2,
        NONE
    );

    let tb = |seq: i32| batch(&["t"], pid, ep, seq, true, None);
    assert_eq!(w.produce(&t, 0, tb(0), Some(&tx)).await?, (NONE, 0));
    assert_eq!(
        w.produce(&t, 1, tb(0), Some(&tx)).await?.0,
        INVALID_TXN_STATE,
        "partition 1 not added"
    );
    assert_eq!(
        w.produce(&t, 0, batch(&["t"], pid, ep - 1, 1, true, None), Some(&tx))
            .await?
            .0,
        INVALID_PRODUCER_EPOCH
    );
    assert_eq!(w.end_txn(&tx, pid, ep, true).await?, NONE);
    // The next transaction starts with an empty registry.
    assert_eq!(
        w.add_partitions(&tx, pid, ep, &[(&t, &[1])]).await?[0].2,
        NONE
    );
    assert_eq!(
        w.produce(&t, 0, tb(1), Some(&tx)).await?.0,
        INVALID_TXN_STATE,
        "added in the previous transaction only"
    );
    assert_eq!(w.produce(&t, 1, tb(0), Some(&tx)).await?.0, NONE);
    assert_eq!(w.end_txn(&tx, pid, ep, false).await?, NONE);
    let p1_rows: i64 = ctx
        .db()
        .query_one(
            "SELECT COUNT(*) FROM kafka.messages m JOIN kafka.topics t ON t.id = m.topic_id
             WHERE t.name = $1 AND m.partition_id = 1 AND m.txn_state IS DISTINCT FROM 'aborted'",
            &[&t],
        )
        .await?
        .get(0);
    assert_eq!(p1_rows, 0);
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// Fetch groups records into batches (was: one RecordBatch per record).
pub async fn test_w3_fetch_groups_records_into_batches() -> TestResult {
    println!("=== Test: fetch returns records grouped into batches ===\n");
    let ctx = TestContext::new().await?;
    let t = topic(&ctx, "w3-txn-batch", 1).await?;
    let mut w = Wire::connect().await?;
    for i in 0..10 {
        let values: Vec<String> = (0..10).map(|j| format!("m{}", i * 10 + j)).collect();
        let refs: Vec<&str> = values.iter().map(|s| s.as_str()).collect();
        assert_eq!(
            w.produce(&t, 0, batch(&refs, -1, -1, -1, false, None), None)
                .await?
                .0,
            NONE
        );
    }
    let pd = w.fetch(&t, 0, 0, 0).await?;
    let records = pd.records.unwrap_or_default();
    assert_eq!(
        batch_headers(&records),
        vec![(0, 99, 100)],
        "one batch of 100"
    );
    let values = fetched_values(&records)?;
    assert_eq!(values.len(), 100);
    assert!(values
        .iter()
        .enumerate()
        .all(|(i, (o, v))| *o == i as i64 && *v == format!("m{}", i)));
    println!("  100 records in {} bytes (one batch)", records.len());
    // From the middle of the run.
    let pd = w.fetch(&t, 0, 37, 0).await?;
    assert_eq!(
        batch_headers(&pd.records.unwrap_or_default()),
        vec![(37, 62, 63)]
    );
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
