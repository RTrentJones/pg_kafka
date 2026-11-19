//! Validation of transactional requests.

use crate::common::TestResult;
use crate::error_codes;
use crate::fixtures::topic_with_partitions as topic;
use crate::setup::TestContext;
use crate::wire::records::batch;
use crate::wire::Wire;

/// Transaction validation: InitProducerId timeouts, AddPartitionsToTxn on an unknown
/// topic (not created any more), and a transactional Produce to a partition the
/// transaction did not add.
pub async fn test_transactional_requests_are_validated() -> TestResult {
    println!("=== Test: transaction validation ===\n");
    let ctx = TestContext::new().await?;
    let t = topic(&ctx, "w3-txn-val", 2).await?;
    let tx = ctx.unique_topic("w3-txn-val-tx").await;
    let mut w = Wire::connect().await?;
    for timeout in [-5, 0, 900_001, i32::MAX] {
        assert_eq!(
            w.init_producer_id(Some(&tx), timeout).await?,
            (error_codes::INVALID_TRANSACTION_TIMEOUT, -1, -1),
            "timeout {}",
            timeout
        );
    }
    // Idempotent producers ignore the timeout.
    assert_eq!(w.init_producer_id(None, -5).await?.0, error_codes::NONE);
    let (err, pid, ep) = w.init_producer_id(Some(&tx), 900_000).await?;
    assert_eq!(err, error_codes::NONE);

    let missing = ctx.unique_topic("w3-txn-val-missing").await;
    assert_eq!(
        w.add_partitions(&tx, pid, ep, &[(&missing, &[0]), (&t, &[0])])
            .await?,
        vec![
            (missing.clone(), 0, error_codes::UNKNOWN_TOPIC_OR_PARTITION),
            (t.clone(), 0, error_codes::OPERATION_NOT_ATTEMPTED)
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
            (t.clone(), 0, error_codes::OPERATION_NOT_ATTEMPTED),
            (t.clone(), 9, error_codes::UNKNOWN_TOPIC_OR_PARTITION)
        ]
    );
    assert_eq!(
        w.add_partitions(&tx, pid, ep, &[(&t, &[0])]).await?[0].2,
        error_codes::NONE
    );

    let tb = |seq: i32| batch(&["t"], pid, ep, seq, true, None);
    assert_eq!(
        w.produce(&t, 0, tb(0), Some(&tx)).await?,
        (error_codes::NONE, 0)
    );
    assert_eq!(
        w.produce(&t, 1, tb(0), Some(&tx)).await?.0,
        error_codes::INVALID_TXN_STATE,
        "partition 1 not added"
    );
    assert_eq!(
        w.produce(&t, 0, batch(&["t"], pid, ep - 1, 1, true, None), Some(&tx))
            .await?
            .0,
        error_codes::INVALID_PRODUCER_EPOCH
    );
    assert_eq!(w.end_txn(&tx, pid, ep, true).await?, error_codes::NONE);
    // The next transaction starts with an empty registry.
    assert_eq!(
        w.add_partitions(&tx, pid, ep, &[(&t, &[1])]).await?[0].2,
        error_codes::NONE
    );
    assert_eq!(
        w.produce(&t, 0, tb(1), Some(&tx)).await?.0,
        error_codes::INVALID_TXN_STATE,
        "added in the previous transaction only"
    );
    assert_eq!(
        w.produce(&t, 1, tb(0), Some(&tx)).await?.0,
        error_codes::NONE
    );
    assert_eq!(w.end_txn(&tx, pid, ep, false).await?, error_codes::NONE);
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
