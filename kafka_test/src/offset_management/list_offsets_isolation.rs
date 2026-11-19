//! ListOffsets isolation, MAX_TIMESTAMP and partition checks.

use crate::common::TestResult;
use crate::setup::TestContext;
use crate::wire::requests::{topic, NONE, UNKNOWN_TOPIC_OR_PARTITION};
use crate::wire::{batch, now_ms, Wire};

/// ListOffsets: read_committed LATEST is the LSO, -3 is the max-timestamp record, a
/// lookup past the readable end is -1, and a partition the topic lacks is
/// UNKNOWN_TOPIC_OR_PARTITION (were: HWM; the first record; the pending record; NONE
/// and offset 0).
pub async fn test_list_offsets_isolation_max_timestamp_and_partitions() -> TestResult {
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
