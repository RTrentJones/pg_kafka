//! OffsetFetch require_stable (KIP-447).

use crate::common::TestResult;
use crate::setup::TestContext;
use crate::wire::requests::{topic, NONE, UNSTABLE_OFFSET_COMMIT};
use crate::wire::Wire;

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
