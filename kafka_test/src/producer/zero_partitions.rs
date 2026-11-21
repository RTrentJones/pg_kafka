//! Produce topic entries that name no partition.

use crate::common::TestResult;
use crate::setup::TestContext;
use crate::wire::records::plain_batch;
use crate::wire::requests::conn;
use crate::wire::{roundtrip, send, Wire};
use bytes::Bytes;
use kafka_protocol::messages::produce_request::{
    PartitionProduceData, ProduceRequest, TopicProduceData,
};
use kafka_protocol::messages::{MetadataRequest, TopicName, TransactionalId};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::Compression;

fn topic(name: &str, partitions: Vec<PartitionProduceData>) -> TopicProduceData {
    let mut t = TopicProduceData::default();
    t.name = TopicName(StrBytes::from_string(name.to_string()));
    t.partition_data = partitions;
    t
}

fn produce(acks: i16, txn: Option<&str>, topics: Vec<TopicProduceData>) -> ProduceRequest {
    let mut req = ProduceRequest::default();
    req.acks = acks;
    req.timeout_ms = 10_000;
    req.transactional_id = txn.map(|t| TransactionalId(StrBytes::from_string(t.to_string())));
    req.topic_data = topics;
    req
}

/// Kafka 3.7 answers each partition a Produce names and nothing else: topic entries
/// without partitions get no entry in the response and are not auto-created (checked
/// against a 3.7 broker with acks -1, 0 and 5, and with a transactional id). pg_kafka
/// answered each with an empty topic entry and created the topic.
pub async fn test_produce_topic_entries_without_partitions_answer_nothing() -> TestResult {
    println!("=== Test: Produce topic entries without partitions ===\n");
    let ctx = TestContext::new().await?;
    let mut names = Vec::new();
    for i in 0..6 {
        names.push(ctx.unique_topic(&format!("zp{i}")).await);
    }
    let written = ctx.unique_topic("zp-written").await;
    let mut w = Wire::connect().await?;
    let batch = || plain_batch(&[Bytes::from_static(b"x")], Compression::None);

    println!("Step 1: Only topics without partitions (acks=-1)...");
    let resp = w
        .call(
            7,
            &produce(
                -1,
                None,
                vec![topic(&names[0], vec![]), topic(&names[1], vec![])],
            ),
        )
        .await?;
    println!("  {} topic entries", resp.responses.len());
    assert!(resp.responses.is_empty(), "{:?}", resp.responses);

    println!("Step 2: Mixed with a written partition, repeated name...");
    let mut p = PartitionProduceData::default();
    p.index = 0;
    p.records = Some(batch());
    let resp = w
        .call(
            7,
            &produce(
                -1,
                None,
                vec![
                    topic(&written, vec![]),
                    topic(&names[2], vec![]),
                    topic(&written, vec![p]),
                ],
            ),
        )
        .await?;
    let answered: Vec<(String, Vec<(i32, i16)>)> = resp
        .responses
        .iter()
        .map(|t| {
            let parts = t
                .partition_responses
                .iter()
                .map(|p| (p.index, p.error_code))
                .collect();
            (t.name.0.to_string(), parts)
        })
        .collect();
    println!("  answered {answered:?}");
    assert_eq!(answered, vec![(written.clone(), vec![(0, 0)])]);

    println!("Step 3: Invalid acks and a transactional id...");
    let resp = w
        .call(7, &produce(5, None, vec![topic(&names[3], vec![])]))
        .await?;
    assert!(resp.responses.is_empty(), "acks=5: {:?}", resp.responses);
    let resp = w
        .call(
            7,
            &produce(-1, Some("zp-no-such-txn"), vec![topic(&names[4], vec![])]),
        )
        .await?;
    assert!(
        resp.responses.is_empty(),
        "transactional: {:?}",
        resp.responses
    );

    println!("Step 4: acks=0, then a Metadata on the same connection...");
    let mut c = conn().await?;
    send(
        &mut c,
        7,
        1,
        &produce(0, None, vec![topic(&names[5], vec![])]),
    )
    .await?;
    // Responses are written in request order, so the Metadata answer follows the
    // Produce's handling.
    let _ = roundtrip(&mut c, 4, 2, &MetadataRequest::default()).await?;

    println!("=== Database Verification ===\n");
    for name in &names {
        let rows = ctx
            .db()
            .query("SELECT 1 FROM kafka.topics WHERE name = $1", &[name])
            .await?;
        assert!(rows.is_empty(), "{name} must not be created");
    }
    println!("✅ None of the topics without partitions was created\n");
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
