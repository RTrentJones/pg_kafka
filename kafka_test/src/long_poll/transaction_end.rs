//! Transaction ends wake parked read_committed fetches.

use crate::common::{get_bootstrap_servers, TestResult};
use crate::worker::{
    assign_consumer, boxed, into_test_result, quick_producer, send, session, unique,
    wait_for_serving_listener, SendResult,
};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::StreamConsumer;
use rdkafka::producer::FutureProducer;
use rdkafka::producer::Producer;
use rdkafka::Message;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

/// The read_committed consumers parked behind the timed-out transaction.
const READERS: usize = 5;
/// The spacing of their last wake-ups, and so of their safety-net re-fetches.
const STAGGER: Duration = Duration::from_millis(200);
/// How close to the first a reader's record must arrive to count as woken with it.
const TOGETHER: Duration = Duration::from_millis(100);

/// A record a reader received: its value and when it arrived.
type Received = (String, Instant, SystemTime);

/// Every transaction end notifies `TransactionEnded`, not only EndTxn: a transaction
/// aborted by fencing (InitProducerId of a new producer instance) or by its timeout must
/// wake a parked read_committed fetch at once, not through the long-poll safety net
/// (`pg_kafka.fetch_poll_interval_ms`, 1 s).
///
/// Asserts, with a read_committed consumer parked (fetch.wait.max.ms 5000) behind a
/// pending transaction and a committed record after it: in 5 fencing rounds the
/// record arrives within 300 ms of the fencing InitProducerId.
///
/// The timeout abort cannot be timed that way: the worker's sweep picks its moment (every
/// 10 s), and one wake-up latency cannot tell the notification from the safety net, which
/// also delivers within 1 s of the abort; a host stall of a few hundred milliseconds fails
/// such a bound while a lost notification passes it about half the time. So five
/// read_committed consumers are parked on five topics behind one transaction (timeout 4 s),
/// each last woken (by its own topic's produce) 200 ms after the previous one, which spaces
/// their safety-net re-fetches 200 ms apart. Asserts that after the timeout abort at least
/// three of the five records arrive within 100 ms of the first (through the safety net
/// they arrive 200, 400, 600 and 800 ms after it, whatever the abort's moment, while a
/// stall of the whole test process shifts them together), and that the first arrives
/// within 2 s of the abort.
pub async fn test_transaction_end_wakes_parked_read_committed_fetch() -> TestResult {
    into_test_result(txn_end_wakes().await)
}

async fn txn_end_wakes() -> SendResult<()> {
    /// When each of `wants` arrived (other values are skipped).
    async fn wait_for_all(
        rx: &mut UnboundedReceiver<Received>,
        wants: &[String],
    ) -> SendResult<HashMap<String, (Instant, SystemTime)>> {
        let mut got = HashMap::new();
        let deadline = Instant::now() + Duration::from_secs(20);
        while got.len() < wants.len() && Instant::now() < deadline {
            if let Ok(Some((v, at, wall))) =
                tokio::time::timeout(Duration::from_millis(200), rx.recv()).await
            {
                if wants.contains(&v) {
                    got.insert(v, (at, wall));
                }
            }
        }
        match wants.iter().find(|w| !got.contains_key(*w)) {
            Some(missing) => Err(format!("never received {missing}").into()),
            None => Ok(got),
        }
    }
    async fn wait_for(
        rx: &mut UnboundedReceiver<Received>,
        want: &str,
    ) -> SendResult<(Instant, SystemTime)> {
        let want = want.to_string();
        let got = wait_for_all(rx, std::slice::from_ref(&want)).await?;
        got.get(&want)
            .copied()
            .ok_or_else(|| format!("never received {want}").into())
    }
    /// Forward every record `consumer` reads, with its arrival time, until `stop`.
    fn spawn_reader(
        consumer: StreamConsumer,
        tx: UnboundedSender<Received>,
        stop: Arc<AtomicBool>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                if let Ok(Ok(m)) =
                    tokio::time::timeout(Duration::from_millis(200), consumer.recv()).await
                {
                    let v = String::from_utf8_lossy(m.payload().unwrap_or_default()).into_owned();
                    let _ = tx.send((v, Instant::now(), SystemTime::now()));
                }
            }
        })
    }

    println!("=== Test: fencing / timeout aborts wake read_committed long polls ===\n");
    let db = session().await?;
    wait_for_serving_listener(&db, None, Duration::from_secs(30)).await?;
    let topic = unique("txn-wake");
    let txn_id = unique("txn-wake-id");
    let plain = quick_producer("10000")?;
    send(&plain, &topic, "start").await?;

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Received>();
    let stop = Arc::new(AtomicBool::new(false));
    let mut readers = vec![spawn_reader(
        assign_consumer(&topic, true, &[("fetch.wait.max.ms", "5000")])?,
        tx.clone(),
        stop.clone(),
    )];
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

        // Transaction-timeout abort (the worker checks every 10 s). The readers' long
        // polls (fetch.wait.max.ms 30000) outlast the wait for the sweep, so a reader's
        // safety-net phase is set by the last produce to its topic alone.
        let topics: Vec<String> = (0..READERS)
            .map(|i| unique(&format!("txn-wake-timeout-{i}")))
            .collect();
        for t in &topics {
            send(&plain, t, t).await?;
            readers.push(spawn_reader(
                assign_consumer(t, true, &[("fetch.wait.max.ms", "30000")])?,
                tx.clone(),
                stop.clone(),
            ));
        }
        wait_for_all(&mut rx, &topics).await?;
        let timeout_id = unique("txn-wake-timeout");
        let p = init(timeout_id.clone(), Some("4000")).await?;
        p.begin_transaction().map_err(boxed)?;
        futures::future::try_join_all(topics.iter().map(|t| send(&p, t, "pending-timeout")))
            .await?;
        // A reader's last wake-up is the produce of its visible record, so each send has
        // to land in its slot for the safety-net re-fetches to be spaced STAGGER apart.
        let visible: Vec<String> = (0..READERS)
            .map(|i| format!("visible-timeout-{i}"))
            .collect();
        let first_slot = tokio::time::Instant::now();
        for (slot, (t, v)) in (0u32..).zip(topics.iter().zip(&visible)) {
            let at = first_slot + STAGGER * slot;
            tokio::time::sleep_until(at).await;
            send(&plain, t, v).await?;
            let late = at.elapsed();
            if late > STAGGER / 4 {
                return Err(format!("{v} landed {late:?} after its slot").into());
            }
        }
        let state: String = db
            .query_one(
                "SELECT state FROM kafka.transactions WHERE transactional_id = $1",
                &[&timeout_id],
            )
            .await
            .map_err(boxed)?
            .get(0);
        if state != "Ongoing" {
            return Err(format!("the transaction ended ({state}) before the last produce").into());
        }
        let got = wait_for_all(&mut rx, &visible).await?;
        let aborted_at: f64 = db
            .query_one(
                "SELECT EXTRACT(EPOCH FROM last_updated_at::timestamptz)::float8
                 FROM kafka.transactions WHERE transactional_id = $1 AND state = 'CompleteAbort'",
                &[&timeout_id],
            )
            .await
            .map_err(boxed)?
            .get(0);
        let first = got
            .values()
            .min_by_key(|(at, _)| *at)
            .copied()
            .ok_or("no record received")?;
        let after_first: Vec<Duration> = visible
            .iter()
            .filter_map(|v| got.get(v))
            .map(|(at, _)| at.saturating_duration_since(first.0))
            .collect();
        let latency = first
            .1
            .duration_since(UNIX_EPOCH)
            .map_err(boxed)?
            .as_secs_f64()
            - aborted_at;
        println!(
            "  timeout abort: first committed record {latency:.3} s after the abort; \
             each reader after the first: {after_first:?}"
        );
        let together = after_first.iter().filter(|d| **d <= TOGETHER).count();
        assert!(
            together > READERS / 2,
            "only {together} of {READERS} read_committed fetches woke together after the \
             timeout abort ({after_first:?}): the safety net, not the abort, woke them"
        );
        assert!(
            latency < 2.0,
            "the first read_committed fetch woke {latency:.3} s after the timeout abort"
        );
        keep.push(p);
        drop(keep);
        Ok(())
    }
    .await;
    stop.store(true, Ordering::Relaxed);
    for reader in readers {
        let _ = reader.await;
    }
    result?;
    println!("✅ every transaction end wakes the parked read_committed fetch");
    Ok(())
}
