//! Transaction ends wake parked read_committed fetches.

use crate::common::{get_bootstrap_servers, TestResult};
use crate::worker::{
    assign_consumer, boxed, into_test_result, quick_producer, send, session, unique,
    wait_for_serving_listener, SendResult,
};
use rdkafka::config::ClientConfig;
use rdkafka::producer::FutureProducer;
use rdkafka::producer::Producer;
use rdkafka::Message;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Every transaction end notifies `TransactionEnded`, not only EndTxn: a transaction
/// aborted by fencing (InitProducerId of a new producer instance) or by its timeout must
/// wake a parked read_committed fetch at once, not through the long-poll safety net (1 s).
///
/// Asserts, with a read_committed consumer parked (fetch.wait.max.ms 5000) behind a
/// pending transaction and a committed record after it: in 5 fencing rounds the
/// record arrives within 300 ms of the fencing InitProducerId, and after a
/// transaction-timeout abort within 500 ms of the abort.
pub async fn test_transaction_end_wakes_parked_read_committed_fetch() -> TestResult {
    into_test_result(txn_end_wakes().await)
}

async fn txn_end_wakes() -> SendResult<()> {
    async fn wait_for(
        rx: &mut tokio::sync::mpsc::UnboundedReceiver<(String, Instant, std::time::SystemTime)>,
        want: &str,
    ) -> SendResult<(Instant, std::time::SystemTime)> {
        let deadline = Instant::now() + Duration::from_secs(20);
        while Instant::now() < deadline {
            if let Ok(Some((v, at, wall))) =
                tokio::time::timeout(Duration::from_millis(200), rx.recv()).await
            {
                if v == want {
                    return Ok((at, wall));
                }
            }
        }
        Err(format!("never received {want}").into())
    }

    println!("=== Test: fencing / timeout aborts wake read_committed long polls ===\n");
    let db = session().await?;
    wait_for_serving_listener(&db, None, Duration::from_secs(30)).await?;
    let topic = unique("txn-wake");
    let txn_id = unique("txn-wake-id");
    let plain = quick_producer("10000")?;
    send(&plain, &topic, "start").await?;

    let (tx, mut rx) =
        tokio::sync::mpsc::unbounded_channel::<(String, Instant, std::time::SystemTime)>();
    let stop = Arc::new(AtomicBool::new(false));
    let reader = {
        let consumer = assign_consumer(&topic, true, &[("fetch.wait.max.ms", "5000")])?;
        let stop = stop.clone();
        tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                if let Ok(Ok(m)) =
                    tokio::time::timeout(Duration::from_millis(200), consumer.recv()).await
                {
                    let v = String::from_utf8_lossy(m.payload().unwrap_or_default()).into_owned();
                    let _ = tx.send((v, Instant::now(), std::time::SystemTime::now()));
                }
            }
        })
    };
    wait_for(&mut rx, "start").await?;

    let result: SendResult<()> = async {
        let init = |id: String, timeout_ms: Option<&'static str>| async move {
            let mut cfg = ClientConfig::new();
            cfg.set("bootstrap.servers", get_bootstrap_servers())
                .set("broker.address.family", "v4")
                .set("message.timeout.ms", "10000")
                .set("transactional.id", &id);
            if let Some(t) = timeout_ms {
                cfg.set("transaction.timeout.ms", t)
                    .set("message.timeout.ms", t);
            }
            let p: FutureProducer = cfg.create().map_err(boxed)?;
            let q = p.clone();
            tokio::task::spawn_blocking(move || q.init_transactions(Duration::from_secs(10)))
                .await
                .map_err(boxed)?
                .map_err(boxed)?;
            Ok::<FutureProducer, Box<dyn std::error::Error + Send + Sync>>(p)
        };
        let mut keep = Vec::new();
        let mut current = init(txn_id.clone(), None).await?;
        for round in 0..5 {
            current.begin_transaction().map_err(boxed)?;
            send(&current, &topic, &format!("pending-{round}")).await?;
            let visible = format!("visible-{round}");
            send(&plain, &topic, &visible).await?;
            tokio::time::sleep(Duration::from_millis(400)).await;
            let fenced_by = init(txn_id.clone(), None).await?;
            let after_init = Instant::now();
            let (got, _) = wait_for(&mut rx, &visible).await?;
            let latency = got.saturating_duration_since(after_init);
            println!("  round {round}: committed record after fencing in {latency:?}");
            assert!(
                latency < Duration::from_millis(300),
                "round {round}: read_committed fetch woke {latency:?} after the fencing abort"
            );
            keep.push(std::mem::replace(&mut current, fenced_by));
        }

        // Transaction-timeout abort (the worker checks every 10 s).
        let timeout_id = unique("txn-wake-timeout");
        let p = init(timeout_id.clone(), Some("2000")).await?;
        p.begin_transaction().map_err(boxed)?;
        send(&p, &topic, "pending-timeout").await?;
        send(&plain, &topic, "visible-timeout").await?;
        let (_, wall) = wait_for(&mut rx, "visible-timeout").await?;
        let aborted_at: f64 = db
            .query_one(
                "SELECT EXTRACT(EPOCH FROM last_updated_at::timestamptz)::float8
                 FROM kafka.transactions WHERE transactional_id = $1 AND state = 'CompleteAbort'",
                &[&timeout_id],
            )
            .await
            .map_err(boxed)?
            .get(0);
        let received = wall
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(boxed)?
            .as_secs_f64();
        let latency = received - aborted_at;
        println!("  timeout abort: committed record {latency:.3} s after the abort");
        assert!(
            latency < 0.5,
            "read_committed fetch woke {latency:.3} s after the timeout abort"
        );
        keep.push(p);
        drop(keep);
        Ok(())
    }
    .await;
    stop.store(true, Ordering::Relaxed);
    let _ = reader.await;
    result?;
    println!("✅ every transaction end wakes the parked read_committed fetch");
    Ok(())
}
