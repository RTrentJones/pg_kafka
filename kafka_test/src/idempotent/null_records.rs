//! A Produce partition whose records are null: Kafka 3.7 refuses that partition with
//! UNKNOWN_SERVER_ERROR (-1) and writes the request's other partitions; with acks=0 the
//! refusal closes the connection. Nothing is remembered for the producer on the refused
//! partition, so its first real batch there is written.

use crate::common::{DynResult, TestResult};
use crate::error_codes;
use crate::fixtures::topic_with_partitions as topic;
use crate::setup::TestContext;
use crate::wire::records::batch;
use crate::wire::{request_frame, Wire};
use bytes::Bytes;
use kafka_protocol::messages::produce_request::{
    PartitionProduceData, ProduceRequest, TopicProduceData,
};
use kafka_protocol::messages::produce_response::ProduceResponse;
use kafka_protocol::messages::{TopicName, TransactionalId};
use kafka_protocol::protocol::StrBytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// A Produce v7 of `parts` to `topic`; `None` sends the partition with null records.
fn request(
    topic: &str,
    parts: Vec<(i32, Option<Bytes>)>,
    acks: i16,
    transactional_id: Option<&str>,
) -> ProduceRequest {
    let mut t = TopicProduceData::default();
    t.name = TopicName(StrBytes::from_string(topic.to_string()));
    for (index, records) in parts {
        let mut p = PartitionProduceData::default();
        p.index = index;
        p.records = records;
        t.partition_data.push(p);
    }
    let mut r = ProduceRequest::default();
    r.acks = acks;
    r.timeout_ms = 10_000;
    r.transactional_id =
        transactional_id.map(|id| TransactionalId(StrBytes::from_string(id.to_string())));
    r.topic_data = vec![t];
    r
}

/// Every partition's `(partition, error_code, base_offset)`, sorted.
async fn produce(
    w: &mut Wire,
    topic: &str,
    parts: Vec<(i32, Option<Bytes>)>,
    transactional_id: Option<&str>,
) -> DynResult<Vec<(i32, i16, i64)>> {
    let resp: ProduceResponse = w
        .call(7, &request(topic, parts, -1, transactional_id))
        .await?;
    let mut out: Vec<_> = resp.responses[0]
        .partition_responses
        .iter()
        .map(|p| (p.index, p.error_code, p.base_offset))
        .collect();
    out.sort_unstable();
    Ok(out)
}

/// Send an acks=0 Produce on a fresh connection: whether the broker closes it within 3 s.
async fn acks0_closes(
    topic: &str,
    parts: Vec<(i32, Option<Bytes>)>,
    transactional_id: Option<&str>,
) -> DynResult<bool> {
    let mut s = tokio::net::TcpStream::connect(crate::common::get_bootstrap_servers()).await?;
    s.write_all(&request_frame(
        7,
        1,
        &request(topic, parts, 0, transactional_id),
    ))
    .await?;
    let mut buf = [0u8; 64];
    loop {
        match tokio::time::timeout(std::time::Duration::from_secs(3), s.read(&mut buf)).await {
            Ok(Ok(0) | Err(_)) => return Ok(true),
            Ok(Ok(_)) => {}
            Err(_) => return Ok(false),
        }
    }
}

/// The values stored on each partition of `topic`, by partition offset.
async fn stored(ctx: &TestContext, topic: &str) -> DynResult<Vec<(i32, i64, String)>> {
    Ok(ctx
        .db()
        .query(
            "SELECT m.partition_id, m.partition_offset, convert_from(m.value, 'UTF8')
             FROM kafka.messages m JOIN kafka.topics t ON t.id = m.topic_id
             WHERE t.name = $1 ORDER BY 1, 2",
            &[&topic],
        )
        .await?
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect())
}

pub async fn test_null_records_partition_is_refused() -> TestResult {
    println!("=== Test: a partition with null records is refused ===\n");
    let ctx = TestContext::new().await?;
    let mut w = Wire::connect().await?;

    println!("Step 1: an idempotent producer, p0 seq 0 and p1 null...");
    let t = topic(&ctx, "null-records", 2).await?;
    let (err, pid, ep) = w.init_producer_id(None, 60_000).await?;
    assert_eq!(err, error_codes::NONE);
    let b = |value: &str, seq| Some(batch(&[value], pid, ep, seq, false, None));
    assert_eq!(
        produce(&mut w, &t, vec![(0, b("a", 0)), (1, None)], None).await?,
        vec![
            (0, error_codes::NONE, 0),
            (1, error_codes::UNKNOWN_SERVER_ERROR, -1)
        ]
    );
    assert_eq!(
        produce(&mut w, &t, vec![(1, b("b", 0))], None).await?,
        vec![(1, error_codes::NONE, 0)],
        "the producer's first batch on p1"
    );
    assert_eq!(
        stored(&ctx, &t).await?,
        vec![(0, 0, "a".to_string()), (1, 0, "b".to_string())],
        "p1's first batch is stored, not taken for a duplicate of the null partition"
    );
    println!("✅ p1 refused -1; the producer's seq-0 batch there is stored\n");

    println!("Step 2: acks=0, transactional, p0 seq 0 and p1 null...");
    let t = topic(&ctx, "null-records-txn", 2).await?;
    let txn = ctx.unique_topic("null-records-tx").await;
    let (err, pid, ep) = w.init_producer_id(Some(&txn), 60_000).await?;
    assert_eq!(err, error_codes::NONE);
    let added = w.add_partitions(&txn, pid, ep, &[(&t, &[0, 1])]).await?;
    assert!(added.iter().all(|(_, _, e)| *e == error_codes::NONE));
    let tb = |value: &str, seq| Some(batch(&[value], pid, ep, seq, true, None));
    assert!(
        acks0_closes(&t, vec![(0, tb("a", 0)), (1, None)], Some(&txn)).await?,
        "the refused partition closes the acks=0 connection"
    );
    assert_eq!(
        produce(&mut w, &t, vec![(1, tb("b", 0))], Some(&txn)).await?,
        vec![(1, error_codes::NONE, 0)]
    );
    assert_eq!(w.end_txn(&txn, pid, ep, true).await?, error_codes::NONE);
    assert_eq!(
        stored(&ctx, &t).await?,
        vec![(0, 0, "a".to_string()), (1, 0, "b".to_string())],
        "p0 written, p1's first transactional batch stored"
    );
    println!("✅ connection closed; p0 written, p1's seq-0 batch stored\n");

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
