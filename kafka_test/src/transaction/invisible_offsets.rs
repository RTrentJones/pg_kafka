//! Wave 3 (txn): consumers move past offsets they cannot see.
//!
//! pg_kafka filters aborted rows server-side and writes no transaction markers, so an
//! aborted range at the end of a partition (or rows deleted by the aborted-row cleanup)
//! left a fetch with nothing to return: the consumer re-fetched the same offset forever,
//! its position and committed offset never reached the end, and its lag stayed above 0.
//! A fetch that finds nothing visible below its readable end (the LSO for
//! read_committed, the high watermark otherwise) now returns a control batch covering
//! the hidden offsets. Each test drives a real rdkafka consumer to the end and checks
//! its position and committed offset; the open-transaction test checks that it still
//! stops at the LSO.

use crate::common::TestResult;
use crate::common::{create_producer, create_transactional_producer, get_bootstrap_servers};
use crate::fixtures::TestTopicBuilder;
use crate::setup::TestContext;
use crate::wire::records::batch_headers;
use crate::wire::Wire;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::{Message, Offset, TopicPartitionList};
use std::time::{Duration, Instant};

/// Run one transaction of `values` on partition 0 and commit or abort it.
async fn txn(producer: &FutureProducer, topic: &str, values: &[&str], commit: bool) -> TestResult {
    producer.begin_transaction()?;
    for v in values {
        producer
            .send(
                FutureRecord::<(), _>::to(topic).partition(0).payload(*v),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(e, _)| e)?;
    }
    if commit {
        producer.commit_transaction(Duration::from_secs(10))?;
    } else {
        producer.abort_transaction(Duration::from_secs(10))?;
    }
    Ok(())
}

fn consumer(group: &str, isolation: &str) -> Result<BaseConsumer, Box<dyn std::error::Error>> {
    Ok(ClientConfig::new()
        .set("bootstrap.servers", get_bootstrap_servers())
        .set("broker.address.family", "v4")
        .set("group.id", group)
        .set("isolation.level", isolation)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("fetch.wait.max.ms", "100")
        .create()?)
}

fn position(c: &BaseConsumer, topic: &str) -> i64 {
    c.position()
        .ok()
        .and_then(|tpl| tpl.find_partition(topic, 0).map(|p| p.offset()))
        .and_then(|o| match o {
            Offset::Offset(n) => Some(n),
            _ => None,
        })
        .unwrap_or(-1)
}

/// Consume partition 0 from the beginning until `end` is reached (or `wait` passes).
/// Returns the values read and the final position.
fn consume_to(
    c: &BaseConsumer,
    topic: &str,
    end: i64,
    wait: Duration,
) -> Result<(Vec<String>, i64), Box<dyn std::error::Error>> {
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(topic, 0, Offset::Beginning)?;
    c.assign(&tpl)?;
    let mut values = Vec::new();
    let start = Instant::now();
    while start.elapsed() < wait {
        if let Some(m) = c.poll(Duration::from_millis(100)) {
            let m = m?;
            values.push(String::from_utf8_lossy(m.payload().unwrap_or_default()).into_owned());
        }
        if position(c, topic) >= end {
            break;
        }
    }
    Ok((values, position(c, topic)))
}

/// The committed offset of `group` stored by pg_kafka.
async fn committed(
    ctx: &TestContext,
    group: &str,
    topic: &str,
) -> Result<i64, Box<dyn std::error::Error>> {
    let row = ctx
        .db()
        .query_one(
            "SELECT o.committed_offset FROM kafka.consumer_offsets o
             JOIN kafka.topics t ON t.id = o.topic_id
             WHERE o.group_id = $1 AND t.name = $2 AND o.partition_id = 0",
            &[&group, &topic],
        )
        .await?;
    Ok(row.get(0))
}

/// Consume to the end, commit, and assert position == committed == `end` and the
/// values read.
async fn assert_reaches_end(
    ctx: &TestContext,
    topic: &str,
    isolation: &str,
    end: i64,
    expected: &[&str],
) -> TestResult {
    let group = ctx.unique_group("w3-txn-end").await;
    let c = consumer(&group, isolation)?;
    let (values, pos) = consume_to(&c, topic, end, Duration::from_secs(20))?;
    println!(
        "  {}: values={:?} position={} (end {})",
        isolation, values, pos, end
    );
    assert_eq!(
        values, expected,
        "{} consumer read the wrong records",
        isolation
    );
    assert_eq!(pos, end, "{} consumer stalled before the end", isolation);
    c.commit_consumer_state(CommitMode::Sync)?;
    let committed = committed(ctx, &group, topic).await?;
    assert_eq!(
        committed, end,
        "{} committed offset (lag must be 0)",
        isolation
    );
    Ok(())
}

async fn high_watermark(
    wire: &mut Wire,
    topic: &str,
    isolation: i8,
) -> Result<i64, Box<dyn std::error::Error>> {
    let (err, offset, _) = wire.list_offsets(topic, 0, -1, isolation).await?;
    assert_eq!(err, 0);
    Ok(offset)
}

/// Aborted transaction at the end of the partition.
pub async fn test_read_committed_consumer_passes_aborted_tail() -> TestResult {
    println!("=== Test: read_committed consumer passes an aborted tail ===\n");
    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "w3-txn-tail")
        .build()
        .await?
        .name;
    let txn_id = ctx.unique_topic("w3-txn-tail-tx").await;
    let p = create_transactional_producer(&txn_id)?;
    p.init_transactions(Duration::from_secs(10))?;
    txn(&p, &topic, &["a", "b", "c"], true).await?;
    txn(&p, &topic, &["x", "y"], false).await?;

    let mut wire = Wire::connect().await?;
    let end = high_watermark(&mut wire, &topic, 0).await?;
    assert_eq!(end, 5);
    assert_eq!(
        high_watermark(&mut wire, &topic, 1).await?,
        5,
        "LSO = HWM after the abort"
    );
    // On the wire: one batch with the committed records, then a marker batch covering
    // the aborted offsets 3..=4 (was: an empty response, forever).
    let pd = wire.fetch(&topic, 0, 3, 1).await?;
    let records = pd.records.clone().unwrap_or_default();
    assert_eq!(batch_headers(&records), vec![(3, 1, 1)]);
    let pd = wire.fetch(&topic, 0, 0, 1).await?;
    assert_eq!(
        batch_headers(&pd.records.unwrap_or_default()),
        vec![(0, 2, 3)]
    );

    assert_reaches_end(&ctx, &topic, "read_committed", end, &["a", "b", "c"]).await?;
    assert_reaches_end(
        &ctx,
        &topic,
        "read_uncommitted",
        end,
        &["a", "b", "c", "x", "y"],
    )
    .await?;
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// Aborted transaction between two committed ones.
pub async fn test_read_committed_consumer_passes_aborted_middle() -> TestResult {
    println!("=== Test: read_committed consumer passes an aborted transaction in the middle ===\n");
    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "w3-txn-mid")
        .build()
        .await?
        .name;
    let txn_id = ctx.unique_topic("w3-txn-mid-tx").await;
    let p = create_transactional_producer(&txn_id)?;
    p.init_transactions(Duration::from_secs(10))?;
    txn(&p, &topic, &["a"], true).await?;
    txn(&p, &topic, &["x", "y"], false).await?;
    txn(&p, &topic, &["b"], true).await?;
    let mut wire = Wire::connect().await?;
    let end = high_watermark(&mut wire, &topic, 1).await?;
    assert_eq!(end, 4);
    assert_reaches_end(&ctx, &topic, "read_committed", end, &["a", "b"]).await?;
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// Aborted rows deleted by the cleanup (retention sweep): read_uncommitted consumers
/// see a hole at the tail too.
pub async fn test_consumers_pass_cleaned_up_aborted_rows() -> TestResult {
    println!("=== Test: consumers pass aborted rows deleted by the cleanup ===\n");
    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "w3-txn-clean")
        .build()
        .await?
        .name;
    let txn_id = ctx.unique_topic("w3-txn-clean-tx").await;
    let p = create_transactional_producer(&txn_id)?;
    p.init_transactions(Duration::from_secs(10))?;
    txn(&p, &topic, &["a"], true).await?;
    txn(&p, &topic, &["x", "y"], false).await?;
    // Delete the aborted rows now (grace 0).
    ctx.db()
        .execute("SELECT * FROM pg_kafka_run_retention_sweep(NULL, 0)", &[])
        .await?;
    let left: i64 = ctx
        .db()
        .query_one(
            "SELECT COUNT(*) FROM kafka.messages m JOIN kafka.topics t ON t.id = m.topic_id
             WHERE t.name = $1",
            &[&topic],
        )
        .await?
        .get(0);
    assert_eq!(left, 1, "the cleanup deletes the aborted rows");
    let mut wire = Wire::connect().await?;
    let end = high_watermark(&mut wire, &topic, 0).await?;
    assert_eq!(end, 3);
    assert_reaches_end(&ctx, &topic, "read_uncommitted", end, &["a"]).await?;
    assert_reaches_end(&ctx, &topic, "read_committed", end, &["a"]).await?;
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// DeleteRecords up to the end, then an aborted transaction: a consumer starting from
/// the log start sees nothing and must still reach the end.
pub async fn test_consumers_pass_delete_records_gap() -> TestResult {
    println!("=== Test: consumers pass a DeleteRecords gap followed by an abort ===\n");
    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "w3-txn-del")
        .build()
        .await?
        .name;
    let plain = create_producer()?;
    for v in ["p0", "p1", "p2"] {
        plain
            .send(
                FutureRecord::<(), _>::to(&topic).partition(0).payload(v),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(e, _)| e)?;
    }
    let mut wire = Wire::connect().await?;
    let (err, low) = wire.delete_records(&topic, 0, 3).await?;
    assert_eq!((err, low), (0, 3));
    let txn_id = ctx.unique_topic("w3-txn-del-tx").await;
    let p = create_transactional_producer(&txn_id)?;
    p.init_transactions(Duration::from_secs(10))?;
    txn(&p, &topic, &["x", "y"], false).await?;
    let end = high_watermark(&mut wire, &topic, 1).await?;
    assert_eq!(end, 5);
    assert_reaches_end(&ctx, &topic, "read_committed", end, &[]).await?;
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// An open transaction still blocks read_committed consumers at the LSO, and they
/// continue once it commits.
pub async fn test_read_committed_consumer_stops_at_open_transaction() -> TestResult {
    println!("=== Test: read_committed consumer stops at an open transaction ===\n");
    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "w3-txn-open")
        .build()
        .await?
        .name;
    let txn_id = ctx.unique_topic("w3-txn-open-tx").await;
    let p = create_transactional_producer(&txn_id)?;
    p.init_transactions(Duration::from_secs(10))?;
    txn(&p, &topic, &["a"], true).await?;
    txn(&p, &topic, &["x"], false).await?; // offset 1, aborted
    p.begin_transaction()?;
    p.send(
        FutureRecord::<(), _>::to(&topic)
            .partition(0)
            .payload("open"),
        Duration::from_secs(5),
    )
    .await
    .map_err(|(e, _)| e)?; // offset 2, pending

    let mut wire = Wire::connect().await?;
    assert_eq!(high_watermark(&mut wire, &topic, 1).await?, 2, "LSO");
    assert_eq!(high_watermark(&mut wire, &topic, 0).await?, 3, "HWM");
    // The marker batch covers the aborted offset 1 but stops at the LSO.
    let pd = wire.fetch(&topic, 0, 1, 1).await?;
    assert_eq!(
        batch_headers(&pd.records.unwrap_or_default()),
        vec![(1, 0, 1)]
    );
    let pd = wire.fetch(&topic, 0, 2, 1).await?;
    assert!(
        pd.records.unwrap_or_default().is_empty(),
        "nothing past the LSO"
    );

    let group = ctx.unique_group("w3-txn-open").await;
    let c = consumer(&group, "read_committed")?;
    let (values, pos) = consume_to(&c, &topic, 3, Duration::from_secs(4))?;
    assert_eq!(values, vec!["a"]);
    assert_eq!(
        pos, 2,
        "position stops at the LSO while the transaction is open"
    );

    p.commit_transaction(Duration::from_secs(10))?;
    let start = Instant::now();
    let mut more = Vec::new();
    while start.elapsed() < Duration::from_secs(20) && position(&c, &topic) < 3 {
        if let Some(m) = c.poll(Duration::from_millis(100)) {
            more.push(String::from_utf8_lossy(m?.payload().unwrap_or_default()).into_owned());
        }
    }
    assert_eq!(more, vec!["open"]);
    assert_eq!(position(&c, &topic), 3);
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
