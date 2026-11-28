//! A transaction end wakes only the read_committed long polls of its partitions.
//!
//! InitProducerId of an idempotent producer ends no transaction, and EndTxn of a
//! one-partition transaction changes what a read_committed Fetch of that partition reads,
//! nothing else: neither may re-read the partitions of read_committed long polls watching
//! other partitions. 64 read_committed long polls of 1,000 partitions each, re-read once per
//! InitProducerId or EndTxn, cost the DB thread 64,000 partition reads each time, which holds
//! every other client's requests behind them.

use super::idle_cost::{
    fetches_executed, long_poll_frame, median_produce_latency, LONG_POLLS, PARTITIONS,
};
use crate::common::{create_producer, get_bootstrap_servers, TestResult};
use crate::fixtures::topic_with_partitions;
use crate::setup::TestContext;
use crate::wire::records::batch;
use crate::wire::Wire;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

/// One-partition transactions committed while the long polls wait.
const TRANSACTIONS: usize = 20;

/// Wait until the long polls' first reads (one Fetch attempt each) have run and the
/// broker's counter has published them.
async fn wait_for_first_reads(ctx: &TestContext, before: i64) -> TestResult {
    let started = Instant::now();
    let expected = before + i64::try_from(LONG_POLLS)?;
    while fetches_executed(ctx).await? < expected {
        if started.elapsed() > Duration::from_secs(60) {
            return Err("the long polls' first reads did not run within 60 s".into());
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    // Let the last of them and the counter's publication settle.
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    println!("   first reads done after {:?}", started.elapsed());
    Ok(())
}

/// Commit `TRANSACTIONS` transactions of one record each to partition 0 of `topic`.
async fn commit_one_partition_transactions(topic: &str, txn_id: &str) -> TestResult {
    let mut wire = Wire::connect().await?;
    let (code, producer_id, epoch) = wire.init_producer_id(Some(txn_id), 60_000).await?;
    assert_eq!(code, 0, "InitProducerId");
    for i in 0..TRANSACTIONS {
        let added = wire
            .add_partitions(txn_id, producer_id, epoch, &[(topic, &[0])])
            .await?;
        assert!(added.iter().all(|(_, _, code)| *code == 0), "{added:?}");
        let sequence = i32::try_from(i)?;
        let records = batch(&["v"], producer_id, epoch, sequence, true, None);
        let (code, _) = wire.produce(topic, 0, records, Some(txn_id)).await?;
        assert_eq!(code, 0, "transactional Produce {i}");
        assert_eq!(wire.end_txn(txn_id, producer_id, epoch, true).await?, 0);
    }
    Ok(())
}

/// 64 read_committed long polls of 1,000 partitions run no Fetch attempt while a stream of
/// idempotent InitProducerIds runs and while 20 one-partition transactions of another topic
/// commit, and the InitProducerIds leave another client's Produce latency within a small
/// factor of what it is without them. Sequential: it reads the broker's Fetch counter and
/// times Produce.
pub async fn test_transaction_ends_wake_only_their_partitions() -> TestResult {
    println!("=== Test: Transaction Ends Wake Only Their Partitions ===\n");

    let ctx = TestContext::new().await?;
    let watched = topic_with_partitions(&ctx, "lp-txn-cost", PARTITIONS).await?;
    let txn_topic = topic_with_partitions(&ctx, "lp-txn-cost-txn", 1).await?;
    let other = topic_with_partitions(&ctx, "lp-txn-cost-produce", 1).await?;
    let txn_id = ctx.unique_topic("lp-txn-cost-id").await;
    let producer = create_producer()?;
    median_produce_latency(&producer, &other).await?; // warm-up: connection and metadata
    let baseline = median_produce_latency(&producer, &other).await?;
    println!("Step 1: median Produce latency: {baseline:?}");

    println!("Step 2: {LONG_POLLS} read_committed long polls of {PARTITIONS} partitions...");
    let before = fetches_executed(&ctx).await?;
    let mut stream = TcpStream::connect(get_bootstrap_servers()).await?;
    for i in 0..LONG_POLLS {
        stream
            .write_all(&long_poll_frame(i32::try_from(i)?, &watched, 1))
            .await?;
    }
    stream.flush().await?;
    wait_for_first_reads(&ctx, before).await?;

    println!("Step 3: idempotent InitProducerIds while timing Produce...");
    let watch_start = fetches_executed(&ctx).await?;
    let stop = Arc::new(AtomicBool::new(false));
    let sent = Arc::new(AtomicUsize::new(0));
    let spam = {
        let (stop, sent) = (stop.clone(), sent.clone());
        tokio::spawn(async move {
            let mut wire = Wire::connect().await.map_err(|e| e.to_string())?;
            while !stop.load(Ordering::Relaxed) {
                let (code, _, _) = wire
                    .init_producer_id(None, 60_000)
                    .await
                    .map_err(|e| e.to_string())?;
                if code != 0 {
                    return Err(format!("InitProducerId answered {code}"));
                }
                sent.fetch_add(1, Ordering::Relaxed);
            }
            Ok::<(), String>(())
        })
    };
    let during = median_produce_latency(&producer, &other).await?;
    stop.store(true, Ordering::Relaxed);
    spam.await??;
    println!(
        "   median Produce latency with {} InitProducerIds: {during:?}",
        sent.load(Ordering::Relaxed)
    );

    println!("Step 4: {TRANSACTIONS} one-partition transactions of another topic...");
    commit_one_partition_transactions(&txn_topic, &txn_id).await?;
    // The counter is published at most once a second.
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    let attempts = fetches_executed(&ctx).await? - watch_start;
    println!("   Fetch attempts of the long polls meanwhile: {attempts}");
    drop(stream);

    assert!(
        attempts <= 2,
        "InitProducerIds and transaction ends of other partitions ran {attempts} Fetch \
         attempts of read_committed long polls"
    );
    let bound = (baseline * 3).max(baseline + Duration::from_millis(50));
    assert!(
        during <= bound,
        "idempotent InitProducerIds slowed another client's Produce from {baseline:?} to \
         {during:?} (bound {bound:?})"
    );

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
