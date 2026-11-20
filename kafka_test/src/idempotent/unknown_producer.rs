//! An idempotent producer the broker holds no state for (its row expired or was never
//! issued here): Kafka (KIP-360) accepts its batch at any sequence and tracks it from
//! there.

use crate::common::{create_idempotent_producer, TestResult};
use crate::error_codes;
use crate::fixtures::topic_with_partitions as topic;
use crate::setup::TestContext;
use crate::wire::records::batch;
use crate::wire::Wire;
use bytes::Bytes;
use kafka_protocol::messages::produce_request::{
    PartitionProduceData, ProduceRequest, TopicProduceData,
};
use kafka_protocol::messages::produce_response::ProduceResponse;
use kafka_protocol::messages::TopicName;
use kafka_protocol::protocol::StrBytes;
use rdkafka::producer::FutureRecord;
use std::time::Duration;
use tokio_postgres::Client;

/// Forget a producer the way the stale-producer prune does: its row and its sequences.
async fn forget_producer(db: &Client, producer_id: i64) -> TestResult {
    db.execute(
        "DELETE FROM kafka.producer_sequences WHERE producer_id = $1",
        &[&producer_id],
    )
    .await?;
    db.execute(
        "DELETE FROM kafka.producer_ids WHERE producer_id = $1",
        &[&producer_id],
    )
    .await?;
    Ok(())
}

async fn row_count(db: &Client, topic: &str) -> Result<i64, Box<dyn std::error::Error>> {
    Ok(db
        .query_one(
            "SELECT COUNT(*) FROM kafka.messages m JOIN kafka.topics t ON t.id = m.topic_id
             WHERE t.name = $1",
            &[&topic],
        )
        .await?
        .get(0))
}

/// Over the raw protocol, as Kafka 3.7 answers: a forgotten producer's next batch is
/// accepted at its sequence, and from then on retries are duplicates, gaps are
/// OUT_OF_ORDER_SEQUENCE_NUMBER and a lower epoch is INVALID_PRODUCER_EPOCH. A producer
/// id this broker never issued is accepted too, and a later InitProducerId does not hand
/// that id out again.
pub async fn test_unknown_producer_is_accepted_as_new() -> TestResult {
    println!("=== Test: Unknown Idempotent Producer Is Accepted As New ===\n");
    let ctx = TestContext::new().await?;
    let db = ctx.db();
    let t = topic(&ctx, "idem-unknown", 1).await?;
    let mut w = Wire::connect().await?;

    println!("Step 1: A producer writes sequence 0, then its state is pruned...");
    let (err, pid, ep) = w.init_producer_id(None, 60_000).await?;
    assert_eq!(err, error_codes::NONE);
    let b = |values: &[&str], seq: i32, pid: i64, epoch: i16| {
        batch(values, pid, epoch, seq, false, None)
    };
    assert_eq!(
        w.produce(&t, 0, b(&["a"], 0, pid, ep), None).await?,
        (error_codes::NONE, 0)
    );
    forget_producer(db, pid).await?;
    println!("✅ Row and sequences deleted\n");

    println!("Step 2: Its next batches...");
    assert_eq!(
        w.produce(&t, 0, b(&["b", "c"], 1, pid, ep), None).await?,
        (error_codes::NONE, 1),
        "a producer without state is accepted at any sequence"
    );
    assert_eq!(
        w.produce(&t, 0, b(&["b", "c"], 1, pid, ep), None).await?,
        (error_codes::NONE, 1),
        "the retry is a duplicate with its original offset"
    );
    assert_eq!(
        w.produce(&t, 0, b(&["x"], 9, pid, ep), None).await?.0,
        error_codes::OUT_OF_ORDER_SEQUENCE_NUMBER,
        "its sequence is tracked again"
    );
    assert_eq!(
        w.produce(&t, 0, b(&["d"], 3, pid, ep), None).await?,
        (error_codes::NONE, 3)
    );
    let stored: i16 = db
        .query_one(
            "SELECT epoch FROM kafka.producer_ids WHERE producer_id = $1 AND transactional_id IS NULL",
            &[&pid],
        )
        .await?
        .get(0);
    assert_eq!(
        stored, ep,
        "the producer is recorded at its presented epoch"
    );
    println!("✅ Accepted, deduplicated and sequence-checked again\n");

    println!("Step 3: A never-issued producer id at a high epoch and sequence...");
    let next_id: i64 = db
        .query_one(
            "SELECT last_value + 1 FROM kafka.producer_ids_producer_id_seq",
            &[],
        )
        .await?
        .get(0);
    assert_eq!(
        w.produce(&t, 0, b(&["e"], 5, next_id, 7), None).await?,
        (error_codes::NONE, 4)
    );
    assert_eq!(
        w.produce(&t, 0, b(&["f"], 6, next_id, 6), None).await?.0,
        error_codes::INVALID_PRODUCER_EPOCH,
        "a lower epoch than the one it was recorded at"
    );
    let (err, fresh, _) = w.init_producer_id(None, 60_000).await?;
    assert_eq!(err, error_codes::NONE, "InitProducerId still allocates");
    assert_ne!(fresh, next_id, "an id in use is not handed out again");
    println!("✅ Accepted; InitProducerId skipped id {next_id} and gave {fresh}\n");

    assert_eq!(row_count(db, &t).await?, 5, "a, b, c, d, e: nothing twice");
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// A live librdkafka idempotent producer whose broker state is pruned keeps producing
/// (it used to get UNKNOWN_PRODUCER_ID on every batch).
pub async fn test_idempotent_producer_survives_pruned_state() -> TestResult {
    println!("=== Test: Idempotent Producer Survives Pruned State ===\n");
    let ctx = TestContext::new().await?;
    let db = ctx.db();
    let t = topic(&ctx, "idem-pruned", 1).await?;
    let producer = create_idempotent_producer()?;
    let send = |v: String| {
        let producer = producer.clone();
        let t = t.clone();
        async move {
            producer
                .send(
                    FutureRecord::to(&t).payload(&v).key("k").partition(0),
                    Duration::from_secs(10),
                )
                .await
                .map_err(|(e, _)| e)
        }
    };

    send("before".into()).await?;
    let pid: i64 = db
        .query_one(
            "SELECT producer_id FROM kafka.producer_sequences s JOIN kafka.topics t ON t.id = s.topic_id
             WHERE t.name = $1",
            &[&t],
        )
        .await?
        .get(0);
    forget_producer(db, pid).await?;
    println!("Pruned producer {pid}; producing on...");
    for i in 0..3 {
        send(format!("after-{i}")).await?;
    }
    assert_eq!(row_count(db, &t).await?, 4);
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// One Produce v7 request carrying a batch for each of `batches` (partition, records):
/// `(partition, error_code, base_offset)` per partition, in partition order.
async fn produce_partitions(
    w: &mut Wire,
    topic: &str,
    batches: Vec<(i32, Bytes)>,
) -> Result<Vec<(i32, i16, i64)>, Box<dyn std::error::Error>> {
    let mut t = TopicProduceData::default();
    t.name = TopicName(StrBytes::from_string(topic.to_string()));
    t.partition_data = batches
        .into_iter()
        .map(|(index, records)| {
            let mut p = PartitionProduceData::default();
            p.index = index;
            p.records = Some(records);
            p
        })
        .collect();
    let mut req = ProduceRequest::default();
    req.acks = -1;
    req.timeout_ms = 10_000;
    req.topic_data = vec![t];
    let resp: ProduceResponse = w.call(7, &req).await?;
    let mut out: Vec<(i32, i16, i64)> = resp
        .responses
        .iter()
        .flat_map(|t| t.partition_responses.iter())
        .map(|p| (p.index, p.error_code, p.base_offset))
        .collect();
    out.sort_unstable();
    Ok(out)
}

/// A producer whose state was pruned resumes on several partitions at once, each at the
/// sequence it had reached there: Kafka keeps producer state per partition and accepts
/// any first sequence on a partition without state, so the second and later partitions
/// of the request must not be judged against an expected sequence of 0.
pub async fn test_pruned_producer_resumes_on_every_partition() -> TestResult {
    println!("=== Test: Pruned Producer Resumes On Every Partition ===\n");
    let ctx = TestContext::new().await?;
    let db = ctx.db();
    let t = topic(&ctx, "idem-pruned-parts", 3).await?;
    let mut w = Wire::connect().await?;
    let (err, pid, ep) = w.init_producer_id(None, 60_000).await?;
    assert_eq!(err, error_codes::NONE);
    let b = |values: &[&str], seq: i32| batch(values, pid, ep, seq, false, None);

    println!("Step 1: Sequences 0.. on three partitions, then the state is pruned...");
    assert_eq!(
        produce_partitions(
            &mut w,
            &t,
            vec![
                (0, b(&["a0", "a1"], 0)),
                (1, b(&["b0", "b1", "b2"], 0)),
                (2, b(&["c0"], 0)),
            ],
        )
        .await?,
        vec![
            (0, error_codes::NONE, 0),
            (1, error_codes::NONE, 0),
            (2, error_codes::NONE, 0)
        ]
    );
    forget_producer(db, pid).await?;
    println!("✅ Pruned\n");

    println!("Step 2: One request continuing every partition at its own sequence...");
    let resume = || {
        vec![
            (0, b(&["a2"], 2)),
            (1, b(&["b3", "b4"], 3)),
            (2, b(&["c1"], 1)),
        ]
    };
    let expected = vec![
        (0, error_codes::NONE, 2),
        (1, error_codes::NONE, 3),
        (2, error_codes::NONE, 1),
    ];
    assert_eq!(
        produce_partitions(&mut w, &t, resume()).await?,
        expected,
        "every partition is accepted at its sequence"
    );
    assert_eq!(
        produce_partitions(&mut w, &t, resume()).await?,
        expected,
        "the retry is a duplicate on every partition, with its original offsets"
    );
    assert_eq!(
        w.produce(&t, 1, b(&["x"], 9), None).await?.0,
        error_codes::OUT_OF_ORDER_SEQUENCE_NUMBER,
        "each partition's sequence is tracked again"
    );
    println!("✅ Accepted, then deduplicated\n");

    assert_eq!(
        row_count(db, &t).await?,
        10,
        "nothing twice, nothing dropped"
    );
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// The id InitProducerId allocates skips ids that producers the broker has no state for
/// recorded (one of them the largest id there is), and moves the id sequence past the
/// run it skipped.
pub async fn test_producer_id_allocation_skips_recorded_ids() -> TestResult {
    println!("=== Test: Producer Id Allocation Skips Recorded Ids ===\n");
    let ctx = TestContext::new().await?;
    let db = ctx.db();
    let t = topic(&ctx, "idem-id-skip", 1).await?;
    let mut w = Wire::connect().await?;

    println!("Step 1: Unknown producers record i64::MAX and the next three ids...");
    let next_id: i64 = db
        .query_one(
            "SELECT last_value + 1 FROM kafka.producer_ids_producer_id_seq",
            &[],
        )
        .await?
        .get(0);
    for pid in [i64::MAX, next_id, next_id + 1, next_id + 2] {
        assert_eq!(
            w.produce(&t, 0, batch(&["v"], pid, 0, 0, false, None), None)
                .await?
                .0,
            error_codes::NONE,
            "producer {pid} is accepted"
        );
    }
    println!("✅ Recorded\n");

    println!("Step 2: InitProducerId...");
    let (err, fresh, _) = w.init_producer_id(None, 60_000).await?;
    assert_eq!(err, error_codes::NONE, "InitProducerId still allocates");
    assert_eq!(fresh, next_id + 3, "the first id after the recorded run");
    let last_value: i64 = db
        .query_one(
            "SELECT last_value FROM kafka.producer_ids_producer_id_seq",
            &[],
        )
        .await?
        .get(0);
    assert!(
        last_value >= next_id + 3,
        "the sequence moved past the skipped run (last_value {last_value})"
    );
    let (err, after, _) = w.init_producer_id(None, 60_000).await?;
    assert_eq!(err, error_codes::NONE);
    assert_eq!(after, next_id + 4, "the next allocation needs no skip");
    println!("✅ Allocated {fresh}, then {after}\n");

    db.execute(
        "DELETE FROM kafka.producer_sequences WHERE producer_id = ANY($1)",
        &[&vec![i64::MAX, next_id, next_id + 1, next_id + 2]],
    )
    .await?;
    db.execute(
        "DELETE FROM kafka.producer_ids WHERE producer_id = ANY($1)",
        &[&vec![i64::MAX, next_id, next_id + 1, next_id + 2]],
    )
    .await?;
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// Producer ids start from the time the extension was created (milliseconds since the
/// Unix epoch), so an extension dropped and created again never hands out an id an
/// earlier installation issued to a producer that may still be running.
pub async fn test_producer_ids_start_from_creation_time() -> TestResult {
    println!("=== Test: Producer Ids Start From Creation Time ===\n");
    let ctx = TestContext::new().await?;
    let mut w = Wire::connect().await?;
    let (err, pid, _) = w.init_producer_id(None, 60_000).await?;
    assert_eq!(err, error_codes::NONE);
    // 2026-01-01T00:00:00Z in milliseconds: every installation is younger than that.
    let floor: i64 = 1_767_225_600_000;
    let now_ms: i64 = ctx
        .db()
        .query_one(
            "SELECT (extract(epoch FROM clock_timestamp()) * 1000)::bigint",
            &[],
        )
        .await?
        .get(0);
    assert!(
        (floor..now_ms + 1_000_000).contains(&pid),
        "producer id {pid} is a creation-time value in [{floor}, now + 10^6)"
    );
    println!("✅ InitProducerId gave {pid}\n");
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
