//! OffsetFetch require_stable (KIP-447).

use crate::common::TestResult;
use crate::error_codes;
use crate::fixtures::topic_with_partitions as topic;
use crate::setup::TestContext;
use crate::wire::Wire;

/// OffsetFetch v7 require_stable (KIP-447): UNSTABLE_OFFSET_COMMIT while a
/// TxnOffsetCommit is pending, the committed offset once the transaction ends.
pub async fn test_offset_fetch_require_stable() -> TestResult {
    println!("=== Test: OffsetFetch require_stable ===\n");
    let ctx = TestContext::new().await?;
    let t = topic(&ctx, "txn-of", 2).await?;
    let g = ctx.unique_group("txn-of").await;
    let tx = ctx.unique_topic("txn-of-tx").await;
    let mut w = Wire::connect().await?;
    let (err, pid, ep) = w.init_producer_id(Some(&tx), 60_000).await?;
    assert_eq!(err, error_codes::NONE);
    assert_eq!(w.add_offsets(&tx, pid, ep, &g).await?, error_codes::NONE);
    assert_eq!(
        w.txn_offset_commit(&tx, pid, ep, &g, &t, 0, 7).await?,
        error_codes::NONE
    );

    assert_eq!(
        w.offset_fetch(&g, &t, &[0, 1], true).await?,
        (
            error_codes::NONE,
            vec![
                (0, -1, error_codes::UNSTABLE_OFFSET_COMMIT),
                (1, -1, error_codes::NONE)
            ]
        )
    );
    // Without require_stable: the (absent) committed offset, no error.
    assert_eq!(
        w.offset_fetch(&g, &t, &[0, 1], false).await?,
        (
            error_codes::NONE,
            vec![(0, -1, error_codes::NONE), (1, -1, error_codes::NONE)]
        )
    );
    assert_eq!(w.end_txn(&tx, pid, ep, true).await?, error_codes::NONE);
    assert_eq!(
        w.offset_fetch(&g, &t, &[0], true).await?,
        (error_codes::NONE, vec![(0, 7, error_codes::NONE)])
    );
    // An aborted transactional commit leaves nothing pending either.
    let (err, pid, ep) = w.init_producer_id(Some(&tx), 60_000).await?;
    assert_eq!(err, error_codes::NONE);
    assert_eq!(w.add_offsets(&tx, pid, ep, &g).await?, error_codes::NONE);
    assert_eq!(
        w.txn_offset_commit(&tx, pid, ep, &g, &t, 0, 9).await?,
        error_codes::NONE
    );
    assert_eq!(
        w.offset_fetch(&g, &t, &[0], true).await?.1[0].2,
        error_codes::UNSTABLE_OFFSET_COMMIT
    );
    assert_eq!(w.end_txn(&tx, pid, ep, false).await?, error_codes::NONE);
    assert_eq!(
        w.offset_fetch(&g, &t, &[0], true).await?,
        (error_codes::NONE, vec![(0, 7, error_codes::NONE)])
    );
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
