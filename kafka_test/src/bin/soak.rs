//! pg_kafka soak driver; run it through `scripts/soak.sh`.
//!
//! For `--duration-secs` it runs, in epochs of `--epoch-secs`:
//! * an idempotent producer (3 partitions) and a transactional producer (commits, with
//!   every 5th transaction aborted on purpose), both recording what was acknowledged;
//! * two consumer groups of two members each (read_committed on the transactional
//!   topic, read_uncommitted on the idempotent one);
//! * retention: both topics have `retention.ms` = `--retention-ms`, so with a short
//!   partition width (soak.sh sets 5 minutes) the sweep trims rows and drops leaves;
//! * chaos: `pg_terminate_backend()` of the listener every `--terminate-secs`, and
//!   `DROP EXTENSION` + `CREATE EXTENSION` between epochs;
//! * a sampler of the listener's resident memory every 10 s.
//!
//! At the end of every epoch (before the DROP), producers stop, the open transactions
//! time out, and it checks against the database: every acknowledged idempotent record
//! at or above its partition's log start offset is stored exactly once (no loss), no
//! value is stored twice (no duplicates), every committed transactional record is
//! visible, no aborted one is, an indeterminate transaction is all-or-nothing, no row
//! is still `pending`, and the read_committed group never saw an aborted record. At
//! the end it checks that the worker's memory stayed bounded. Exit status 1 on any
//! violation; the last line is a one-line JSON summary.

use clap::Parser;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::Message;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio_postgres::{Client, NoTls};

#[derive(Parser, Debug, Clone)]
struct Args {
    #[arg(long, default_value_t = 1800)]
    duration_secs: u64,
    #[arg(long, default_value_t = 600)]
    epoch_secs: u64,
    #[arg(long, default_value_t = 120)]
    terminate_secs: u64,
    #[arg(long, default_value_t = 180_000)]
    retention_ms: i64,
    #[arg(
        long,
        default_value = "host=127.0.0.1 port=28814 user=postgres dbname=postgres"
    )]
    db: String,
    #[arg(long, default_value = "127.0.0.1:9092")]
    bootstrap: String,
    /// Allowed growth of the listener's RSS over one incarnation, in MiB.
    #[arg(long, default_value_t = 128)]
    max_rss_growth_mib: u64,
}

type Res<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Default)]
struct Epoch {
    /// Idempotent producer: acked (partition, offset) -> value.
    idem_acked: HashMap<(i32, i64), String>,
    idem_failed: u64,
    /// Transactional producer: committed (partition, offset) -> value.
    txn_committed: HashMap<(i32, i64), String>,
    txn_aborted: HashSet<String>,
    /// Transactions whose outcome is unknown: their values, all-or-nothing.
    txn_indeterminate: Vec<Vec<String>>,
    seen_txn: HashSet<String>,
    seen_idem: HashSet<String>,
}

async fn db_connect(url: &str) -> Res<Client> {
    let (client, conn) = tokio_postgres::connect(url, NoTls).await?;
    tokio::spawn(async move {
        let _ = conn.await;
    });
    Ok(client)
}

async fn listener_pid(db: &Client) -> Option<i32> {
    db.query_opt(
        "SELECT pid FROM pg_stat_activity WHERE backend_type = 'pg_kafka_listener'",
        &[],
    )
    .await
    .ok()
    .flatten()
    .map(|r| r.get(0))
}

fn rss_kib(pid: i32) -> Option<u64> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    status
        .lines()
        .find(|l| l.starts_with("VmRSS:"))
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|v| v.parse().ok())
}

fn base(bootstrap: &str) -> ClientConfig {
    let mut c = ClientConfig::new();
    c.set("bootstrap.servers", bootstrap)
        .set("broker.address.family", "v4");
    c
}

async fn create_topics(args: &Args, db: &Client, names: &[String]) -> Res<()> {
    let admin: AdminClient<DefaultClientContext> = base(&args.bootstrap).create()?;
    let topics: Vec<NewTopic> = names
        .iter()
        .map(|n| NewTopic::new(n, 3, TopicReplication::Fixed(1)))
        .collect();
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let r = admin
            .create_topics(
                &topics,
                &AdminOptions::new().operation_timeout(Some(Duration::from_secs(10))),
            )
            .await;
        let ok = r.is_ok_and(|v| v.iter().all(Result::is_ok));
        let n: i64 = db
            .query_one(
                "SELECT count(*) FROM kafka.topics WHERE name = ANY($1)",
                &[&names],
            )
            .await
            .map_or(0, |r| r.get(0));
        if ok || usize::try_from(n).is_ok_and(|n| n == names.len()) {
            break;
        }
        if Instant::now() > deadline {
            return Err("could not create the soak topics".into());
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    db.execute(
        "UPDATE kafka.topics SET retention_ms = $2 WHERE name = ANY($1)",
        &[&names, &args.retention_ms],
    )
    .await?;
    Ok(())
}

fn spawn_consumer(
    args: &Args,
    topic: String,
    group: String,
    read_committed: bool,
    seen: Arc<Mutex<HashSet<String>>>,
    stop: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<()> {
    let bootstrap = args.bootstrap.clone();
    tokio::spawn(async move {
        let consumer: StreamConsumer = base(&bootstrap)
            .set("group.id", &group)
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "true")
            .set("auto.commit.interval.ms", "1000")
            .set("session.timeout.ms", "10000")
            .set("heartbeat.interval.ms", "1000")
            .set(
                "isolation.level",
                if read_committed {
                    "read_committed"
                } else {
                    "read_uncommitted"
                },
            )
            .create()
            .expect("consumer");
        consumer.subscribe(&[&topic]).expect("subscribe");
        while !stop.load(Ordering::Relaxed) {
            if let Ok(Ok(m)) =
                tokio::time::timeout(Duration::from_millis(500), consumer.recv()).await
            {
                if let Some(v) = m.payload() {
                    seen.lock()
                        .unwrap()
                        .insert(String::from_utf8_lossy(v).into_owned());
                }
            }
        }
    })
}

async fn idempotent_producer(
    args: Args,
    topic: String,
    epoch: u32,
    acked: Arc<Mutex<HashMap<(i32, i64), String>>>,
    failed: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
) {
    use futures::StreamExt;

    let producer: FutureProducer = base(&args.bootstrap)
        .set("enable.idempotence", "true")
        .set("message.timeout.ms", "120000")
        .set("linger.ms", "5")
        .create()
        .expect("idempotent producer");
    let mut n = 0u64;
    let mut inflight = futures::stream::FuturesUnordered::new();
    while !stop.load(Ordering::Relaxed) {
        for _ in 0..20 {
            n += 1;
            let value = format!("i{epoch}:{n}");
            let p = producer.clone();
            let t = topic.clone();
            inflight.push(async move {
                let r = p
                    .send(
                        FutureRecord::to(&t).key(&value).payload(&value),
                        Duration::from_secs(120),
                    )
                    .await;
                (value, r.map_err(|(e, _)| e))
            });
        }
        while inflight.len() > 200 {
            if let Some((value, r)) = inflight.next().await {
                record(&acked, &failed, &value, &r);
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    while let Some((value, r)) = inflight.next().await {
        record(&acked, &failed, &value, &r);
    }
}

fn record(
    acked: &Arc<Mutex<HashMap<(i32, i64), String>>>,
    failed: &Arc<AtomicU64>,
    value: &str,
    r: &Result<(i32, i64), rdkafka::error::KafkaError>,
) {
    match r {
        Ok((p, o)) => {
            acked.lock().unwrap().insert((*p, *o), value.to_string());
        }
        Err(_) => {
            failed.fetch_add(1, Ordering::Relaxed);
        }
    }
}

struct TxnLog {
    committed: HashMap<(i32, i64), String>,
    aborted: HashSet<String>,
    indeterminate: Vec<Vec<String>>,
    commits: u64,
    errors: u64,
}

async fn transactional_producer(
    args: Args,
    topic: String,
    epoch: u32,
    stop: Arc<AtomicBool>,
) -> TxnLog {
    let mut log = TxnLog {
        committed: HashMap::new(),
        aborted: HashSet::new(),
        indeterminate: Vec::new(),
        commits: 0,
        errors: 0,
    };
    let txn_id = format!("soak-txn-{epoch}");
    let make = || -> Option<FutureProducer> {
        let p: FutureProducer = base(&args.bootstrap)
            .set("transactional.id", &txn_id)
            .set("transaction.timeout.ms", "10000")
            .set("message.timeout.ms", "9000")
            .create()
            .ok()?;
        p.init_transactions(Duration::from_secs(30)).ok()?;
        Some(p)
    };
    let mut producer: Option<FutureProducer> = None;
    let mut n = 0u64;
    let mut t = 0u64;
    while !stop.load(Ordering::Relaxed) {
        if producer.is_none() {
            producer = tokio::task::block_in_place(make);
            if producer.is_none() {
                log.errors += 1;
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }
        }
        let p = producer.clone().unwrap();
        t += 1;
        let abort_on_purpose = t % 5 == 0;
        if p.begin_transaction().is_err() {
            log.errors += 1;
            producer = None;
            continue;
        }
        let mut sent: Vec<((i32, i64), String)> = Vec::new();
        let mut values: Vec<String> = Vec::new();
        let mut failed = false;
        for _ in 0..5 {
            n += 1;
            let value = format!("t{epoch}:{n}");
            values.push(value.clone());
            if let Ok(po) = p
                .send(
                    FutureRecord::to(&topic).key(&value).payload(&value),
                    Duration::from_secs(10),
                )
                .await
            {
                sent.push((po, value));
            } else {
                failed = true;
                break;
            }
        }
        let outcome = tokio::task::block_in_place(|| {
            if failed || abort_on_purpose {
                p.abort_transaction(Duration::from_secs(30)).map(|()| false)
            } else {
                p.commit_transaction(Duration::from_secs(30)).map(|()| true)
            }
        });
        match outcome {
            Ok(true) => {
                log.commits += 1;
                log.committed.extend(sent);
            }
            Ok(false) => log.aborted.extend(values),
            Err(_) => {
                log.errors += 1;
                // Unknown outcome: try to abort; if that fails too, all-or-nothing.
                let aborted = tokio::task::block_in_place(|| {
                    p.abort_transaction(Duration::from_secs(30)).is_ok()
                });
                if aborted && !(failed || abort_on_purpose) {
                    // A commit that reported an error but then aborted cleanly: the
                    // commit may still have happened before the error. Indeterminate.
                    log.indeterminate.push(values);
                } else if aborted {
                    log.aborted.extend(values);
                } else {
                    log.indeterminate.push(values);
                }
                producer = None;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    log
}

async fn verify_epoch(db: &Client, epoch: u32, idem: &str, txn: &str, e: &Epoch) -> Vec<String> {
    let mut problems = Vec::new();
    // Stored rows (only committed/visible state matters for the idempotent topic).
    let rows = match db
        .query(
            "SELECT m.partition_id, m.partition_offset, convert_from(m.value, 'UTF8'), m.txn_state
             FROM kafka.messages m JOIN kafka.topics t ON t.id = m.topic_id WHERE t.name = $1",
            &[&idem],
        )
        .await
    {
        Ok(r) => r,
        Err(err) => return vec![format!("epoch {}: query failed: {}", epoch, err)],
    };
    let mut stored: HashMap<(i32, i64), String> = HashMap::new();
    let mut counts: HashMap<String, u32> = HashMap::new();
    for r in &rows {
        let v: String = r.get(2);
        *counts.entry(v.clone()).or_default() += 1;
        stored.insert((r.get(0), r.get(1)), v);
    }
    let log_start = |topic: &str| {
        let topic = topic.to_string();
        async move {
            let rows = db
                .query(
                    "SELECT po.partition_id, po.log_start_offset FROM kafka.partition_offsets po
                     JOIN kafka.topics t ON t.id = po.topic_id WHERE t.name = $1",
                    &[&topic],
                )
                .await
                .unwrap_or_default();
            rows.iter()
                .map(|r| (r.get::<_, i32>(0), r.get::<_, i64>(1)))
                .collect::<HashMap<i32, i64>>()
        }
    };
    let idem_start = log_start(idem).await;
    let dups: Vec<&String> = counts
        .iter()
        .filter(|(_, c)| **c > 1)
        .map(|(v, _)| v)
        .collect();
    if !dups.is_empty() {
        problems.push(format!(
            "epoch {}: {} duplicated idempotent value(s), e.g. {:?}",
            epoch,
            dups.len(),
            dups.first()
        ));
    }
    let mut lost = 0;
    let mut retained = 0;
    for ((p, o), v) in &e.idem_acked {
        if *o < idem_start.get(p).copied().unwrap_or(0) {
            continue; // removed by retention
        }
        retained += 1;
        match stored.get(&(*p, *o)) {
            Some(s) if s == v => {}
            other => {
                lost += 1;
                if lost <= 3 {
                    problems.push(format!(
                        "epoch {epoch}: acked {v} at {p}/{o} but stored {other:?}"
                    ));
                }
            }
        }
    }
    if lost > 0 {
        problems.push(format!(
            "epoch {epoch}: {lost} acked idempotent record(s) lost"
        ));
    }
    let idem_unseen = e
        .idem_acked
        .iter()
        .filter(|((p, o), v)| {
            *o >= idem_start.get(p).copied().unwrap_or(0) && !e.seen_idem.contains(*v)
        })
        .count();

    // Transactional topic.
    let rows = db
        .query(
            "SELECT m.partition_id, m.partition_offset, convert_from(m.value, 'UTF8'), m.txn_state
             FROM kafka.messages m JOIN kafka.topics t ON t.id = m.topic_id WHERE t.name = $1",
            &[&txn],
        )
        .await
        .unwrap_or_default();
    let mut visible: HashSet<String> = HashSet::new();
    let mut pending = 0;
    let mut by_pos: HashMap<(i32, i64), (String, Option<String>)> = HashMap::new();
    for r in &rows {
        let v: String = r.get(2);
        let state: Option<String> = r.get(3);
        if state.is_none() {
            visible.insert(v.clone());
        }
        if state.as_deref() == Some("pending") {
            pending += 1;
        }
        by_pos.insert((r.get(0), r.get(1)), (v, state));
    }
    if pending > 0 {
        problems.push(format!(
            "epoch {epoch}: {pending} transactional row(s) still pending"
        ));
    }
    let txn_start = log_start(txn).await;
    let mut missing_commits = 0;
    for ((p, o), v) in &e.txn_committed {
        if *o < txn_start.get(p).copied().unwrap_or(0) {
            continue;
        }
        match by_pos.get(&(*p, *o)) {
            Some((s, None)) if s == v => {}
            _ => missing_commits += 1,
        }
    }
    if missing_commits > 0 {
        problems.push(format!(
            "epoch {epoch}: {missing_commits} committed transactional record(s) not visible"
        ));
    }
    let visible_aborted = e
        .txn_aborted
        .iter()
        .filter(|v| visible.contains(*v))
        .count();
    if visible_aborted > 0 {
        problems.push(format!(
            "epoch {epoch}: {visible_aborted} aborted record(s) visible"
        ));
    }
    let seen_aborted = e
        .txn_aborted
        .iter()
        .filter(|v| e.seen_txn.contains(*v))
        .count();
    if seen_aborted > 0 {
        problems.push(format!(
            "epoch {epoch}: read_committed group saw {seen_aborted} aborted record(s)"
        ));
    }
    let mut torn = 0;
    for values in &e.txn_indeterminate {
        let n = values.iter().filter(|v| visible.contains(*v)).count();
        // Retention may have removed part of an old transaction; only flag a torn
        // transaction whose rows are all still stored.
        let stored_n = values
            .iter()
            .filter(|v| by_pos.values().any(|(s, _)| s == *v))
            .count();
        if n != 0 && n != values.len() && stored_n == values.len() {
            torn += 1;
        }
    }
    if torn > 0 {
        problems.push(format!(
            "epoch {epoch}: {torn} indeterminate transaction(s) partially visible"
        ));
    }
    println!(
        "epoch {}: idempotent acked {} (retained {}, failed {}, unseen by group {}), stored {}; \
         txn committed {} / aborted {} / indeterminate {}, visible {}, pending {}",
        epoch,
        e.idem_acked.len(),
        retained,
        e.idem_failed,
        idem_unseen,
        rows.len(),
        e.txn_committed.len(),
        e.txn_aborted.len(),
        e.txn_indeterminate.len(),
        visible.len(),
        pending
    );
    problems
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let args = Args::parse();
    let db = db_connect(&args.db).await.expect("db");
    let start = Instant::now();
    let samples: Arc<Mutex<Vec<(u64, i32, u64)>>> = Arc::new(Mutex::new(Vec::new()));
    let all_stop = Arc::new(AtomicBool::new(false));

    // Memory sampler.
    let sampler = {
        let samples = samples.clone();
        let stop = all_stop.clone();
        let url = args.db.clone();
        tokio::spawn(async move {
            let db = db_connect(&url).await.expect("db");
            while !stop.load(Ordering::Relaxed) {
                if let Some(pid) = listener_pid(&db).await {
                    if let Some(kib) = rss_kib(pid) {
                        samples
                            .lock()
                            .unwrap()
                            .push((start.elapsed().as_secs(), pid, kib));
                    }
                }
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
        })
    };
    // Worker terminations.
    let terminations = Arc::new(AtomicU64::new(0));
    let terminator = {
        let stop = all_stop.clone();
        let url = args.db.clone();
        let every = args.terminate_secs;
        let terminations = terminations.clone();
        tokio::spawn(async move {
            let db = db_connect(&url).await.expect("db");
            loop {
                for _ in 0..every {
                    if stop.load(Ordering::Relaxed) {
                        return;
                    }
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                if let Some(pid) = listener_pid(&db).await {
                    if db
                        .query("SELECT pg_terminate_backend($1)", &[&pid])
                        .await
                        .is_ok()
                    {
                        terminations.fetch_add(1, Ordering::Relaxed);
                        println!(
                            "[{:>5}s] terminated listener {}",
                            start.elapsed().as_secs(),
                            pid
                        );
                    }
                }
            }
        })
    };

    let mut problems: Vec<String> = Vec::new();
    let mut epoch: u32 = 0;
    let mut ddl_cycles = 0;
    let mut totals = (0usize, 0usize, 0u64);
    while start.elapsed() < Duration::from_secs(args.duration_secs) {
        epoch += 1;
        let idem = format!("soak-idem-{epoch}");
        let txn = format!("soak-txn-{epoch}");
        if let Err(e) = create_topics(&args, &db, &[idem.clone(), txn.clone()]).await {
            problems.push(format!("epoch {epoch}: {e}"));
            break;
        }
        println!(
            "[{:>5}s] epoch {} started",
            start.elapsed().as_secs(),
            epoch
        );
        let stop = Arc::new(AtomicBool::new(false));
        let consumers_stop = Arc::new(AtomicBool::new(false));
        let seen_txn = Arc::new(Mutex::new(HashSet::new()));
        let seen_idem = Arc::new(Mutex::new(HashSet::new()));
        let mut consumers = Vec::new();
        for _ in 0..2 {
            consumers.push(spawn_consumer(
                &args,
                txn.clone(),
                format!("soak-g-txn-{epoch}"),
                true,
                seen_txn.clone(),
                consumers_stop.clone(),
            ));
            consumers.push(spawn_consumer(
                &args,
                idem.clone(),
                format!("soak-g-idem-{epoch}"),
                false,
                seen_idem.clone(),
                consumers_stop.clone(),
            ));
        }
        let acked = Arc::new(Mutex::new(HashMap::new()));
        let failed = Arc::new(AtomicU64::new(0));
        let idem_task = tokio::spawn(idempotent_producer(
            args.clone(),
            idem.clone(),
            epoch,
            acked.clone(),
            failed.clone(),
            stop.clone(),
        ));
        let txn_task = tokio::spawn(transactional_producer(
            args.clone(),
            txn.clone(),
            epoch,
            stop.clone(),
        ));

        let epoch_end = Instant::now() + Duration::from_secs(args.epoch_secs);
        while Instant::now() < epoch_end
            && start.elapsed() < Duration::from_secs(args.duration_secs)
        {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        stop.store(true, Ordering::Relaxed);
        let _ = idem_task.await;
        let txn_log = txn_task.await.expect("txn task");
        // Open transactions of killed producers time out (10 s + the 10 s check).
        tokio::time::sleep(Duration::from_secs(25)).await;
        // Let the groups catch up.
        tokio::time::sleep(Duration::from_secs(10)).await;
        consumers_stop.store(true, Ordering::Relaxed);
        for c in consumers {
            let _ = c.await;
        }
        let e = Epoch {
            idem_acked: std::mem::take(&mut *acked.lock().unwrap()),
            idem_failed: failed.load(Ordering::Relaxed),
            txn_committed: txn_log.committed,
            txn_aborted: txn_log.aborted,
            txn_indeterminate: txn_log.indeterminate,
            seen_txn: std::mem::take(&mut *seen_txn.lock().unwrap()),
            seen_idem: std::mem::take(&mut *seen_idem.lock().unwrap()),
        };
        println!(
            "epoch {}: {} txn commits, {} txn errors",
            epoch, txn_log.commits, txn_log.errors
        );
        totals.0 += e.idem_acked.len();
        totals.1 += e.txn_committed.len();
        totals.2 += txn_log.commits;
        problems.extend(verify_epoch(&db, epoch, &idem, &txn, &e).await);

        if start.elapsed() < Duration::from_secs(args.duration_secs) {
            let t = Instant::now();
            match db
                .batch_execute("DROP EXTENSION pg_kafka CASCADE; CREATE EXTENSION pg_kafka;")
                .await
            {
                Ok(()) => {
                    ddl_cycles += 1;
                    println!(
                        "[{:>5}s] DROP/CREATE EXTENSION in {:?}",
                        start.elapsed().as_secs(),
                        t.elapsed()
                    );
                }
                Err(err) => problems.push(format!("DROP/CREATE EXTENSION failed: {err:?}")),
            }
        }
    }
    all_stop.store(true, Ordering::Relaxed);
    let _ = sampler.await;
    let _ = terminator.await;

    // Memory: per listener incarnation, growth from its first sample after 60 s of
    // life to its last sample.
    let samples = samples.lock().unwrap().clone();
    let mut by_pid: HashMap<i32, Vec<(u64, u64)>> = HashMap::new();
    for (t, pid, kib) in &samples {
        by_pid.entry(*pid).or_default().push((*t, *kib));
    }
    let mut max_rss = 0;
    let mut worst_growth: i64 = 0;
    for v in by_pid.values() {
        let born = v.first().map_or(0, |s| s.0);
        let settled = v.iter().find(|s| s.0 >= born + 60).map(|s| s.1);
        let last = v.last().map(|s| s.1);
        if let (Some(a), Some(b)) = (settled, last) {
            worst_growth = worst_growth.max(kib(b) - kib(a));
        }
        max_rss = max_rss.max(v.iter().map(|s| s.1).max().unwrap_or(0));
    }
    if worst_growth > kib(args.max_rss_growth_mib * 1024) {
        problems.push(format!(
            "listener RSS grew {worst_growth} KiB within one incarnation"
        ));
    }
    for p in &problems {
        println!("PROBLEM: {p}");
    }
    println!(
        "{{\"duration_s\":{},\"epochs\":{},\"ddl_cycles\":{},\"terminations\":{},\"idempotent_acked\":{},\"txn_records_committed\":{},\"txn_commits\":{},\"max_rss_kib\":{},\"worst_rss_growth_kib\":{},\"listener_incarnations\":{},\"problems\":{}}}",
        start.elapsed().as_secs(),
        epoch,
        ddl_cycles,
        terminations.load(Ordering::Relaxed),
        totals.0,
        totals.1,
        totals.2,
        max_rss,
        worst_growth,
        by_pid.len(),
        problems.len()
    );
    std::process::exit(i32::from(!problems.is_empty()));
}

/// A resident-set size in KiB as an `i64`, for growth arithmetic.
fn kib(v: u64) -> i64 {
    i64::try_from(v).expect("a resident-set size in KiB fits in i64")
}
