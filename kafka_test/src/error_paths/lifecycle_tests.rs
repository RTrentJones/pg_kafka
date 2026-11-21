//! Worker lifecycle and group commit: worker restart, the explicit commit/rollback
//! outcome of each request, and group commit (several requests per top-level
//! transaction).
//!
//! The code under test lives in `src/worker/` / `src/lib.rs`, which unit
//! coverage cannot see (QA-1), so these drive the running broker.

use crate::common::broker_addr;
use crate::common::to_i64;
use crate::common::{create_db_client, create_producer, TestResult};
use crate::setup::TestContext;
use crate::wire::records::{encode, plain_record};
use crate::wire::requests::produce_request;
use crate::wire::roundtrip;
use crate::wire::{read_response, request_frame};
use kafka_protocol::messages::produce_request::ProduceRequest;
use kafka_protocol::messages::produce_response::ProduceResponse;
use kafka_protocol::messages::{ApiVersionsRequest, EndTxnRequest, ProducerId, TransactionalId};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::Compression;
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

/// The listener worker's pid, from pg_stat_activity (`backend_type` is the
/// bgworker type, registered as "pg_kafka_listener" in `_PG_init`).
async fn listener_pid(ctx: &TestContext) -> Result<Option<i32>, Box<dyn std::error::Error>> {
    let rows = ctx
        .db()
        .query(
            "SELECT pid FROM pg_stat_activity WHERE backend_type = 'pg_kafka_listener'",
            &[],
        )
        .await?;
    if rows.len() > 1 {
        return Err(format!(
            "expected at most one pg_kafka_listener, found {}",
            rows.len()
        )
        .into());
    }
    Ok(rows.first().map(|r| r.get(0)))
}

/// One ApiVersions round trip on a fresh connection, with a short timeout.
async fn broker_answers_api_versions() -> bool {
    let attempt = async {
        let mut stream = TcpStream::connect(broker_addr()).await?;
        let response = roundtrip(&mut stream, 0, 1, &ApiVersionsRequest::default()).await?;
        Ok::<i16, Box<dyn std::error::Error>>(response.error_code)
    };
    matches!(
        tokio::time::timeout(Duration::from_secs(2), attempt).await,
        Ok(Ok(0))
    )
}

/// Terminating the listener worker must not leave the Kafka port dead: the worker
/// exits with status 1 on SIGTERM and is registered with a restart interval, so the
/// postmaster restarts it after 5 s and the new worker re-binds the port.
///
/// Asserts: (1) exactly one pg_kafka_listener exists and the broker answers;
/// (2) after pg_terminate_backend, the old pid leaves pg_stat_activity; (3)
/// within 20 s a NEW listener pid appears and the port answers ApiVersions
/// (error_code 0); (4) a produce through rdkafka is then acked and stored.
///
/// Not parallel-safe: it takes the broker down for several seconds.
pub async fn test_worker_restarts_after_terminate() -> TestResult {
    println!("=== Test: Listener Worker Restarts After pg_terminate_backend ===\n");
    let ctx = TestContext::new().await?;

    let old_pid = listener_pid(&ctx)
        .await?
        .ok_or("no pg_kafka_listener in pg_stat_activity")?;
    assert!(
        broker_answers_api_versions().await,
        "broker must answer before the test terminates it"
    );
    println!("Step 1: listener pid {old_pid} is serving");

    let terminated: bool = ctx
        .db()
        .query_one("SELECT pg_terminate_backend($1)", &[&old_pid])
        .await?
        .get(0);
    assert!(terminated, "pg_terminate_backend({old_pid}) returned false");
    println!("Step 2: terminated pid {old_pid}");

    let deadline = Instant::now() + Duration::from_secs(20);
    let mut new_pid = None;
    let mut serving = false;
    while Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(250)).await;
        let current = listener_pid(&ctx).await?;
        match current {
            Some(pid) if pid != old_pid => {
                new_pid = Some(pid);
                if broker_answers_api_versions().await {
                    serving = true;
                    break;
                }
            }
            _ => {}
        }
    }
    let new_pid = new_pid.ok_or("no new pg_kafka_listener appeared within 20 s")?;
    assert!(
        serving,
        "new listener pid {new_pid} did not serve ApiVersions on {} within 20 s",
        broker_addr()
    );
    println!("Step 3: new listener pid {new_pid} serves the port");

    // The restarted worker handles real traffic end to end.
    let topic = ctx.unique_topic("lifecycle-restart").await;
    let producer = create_producer()?;
    producer
        .send(
            rdkafka::producer::FutureRecord::<(), _>::to(&topic)
                .payload("after-restart")
                .partition(0),
            Duration::from_secs(10),
        )
        .await
        .map_err(|(e, _)| e)?;
    let count: i64 = ctx
        .db()
        .query_one(
            "SELECT COUNT(*) FROM kafka.messages m JOIN kafka.topics t ON m.topic_id = t.id
             WHERE t.name = $1",
            &[&topic],
        )
        .await?
        .get(0);
    assert_eq!(count, 1, "the produce after restart must be stored");
    println!("Step 4: produce after restart acked and stored");

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

async fn count_messages(ctx: &TestContext, topic: &str) -> Result<i64, Box<dyn std::error::Error>> {
    Ok(ctx
        .db()
        .query_one(
            "SELECT COUNT(*) FROM kafka.messages m JOIN kafka.topics t ON m.topic_id = t.id
             WHERE t.name = $1",
            &[&topic],
        )
        .await?
        .get(0))
}

/// An acks=1 Produce of one plain record holding `value` to partition 0.
fn produce_value(topic: &str, value: &str) -> ProduceRequest {
    let batch = encode(
        &[plain_record(0, None, value.as_bytes())],
        Compression::None,
    );
    produce_request(topic, vec![(0, batch)], 1)
}

const PRODUCE_VERSION: i16 = 7;

/// Check one decoded Produce response: expected correlation id, no error, and
/// return its base offset.
fn produce_ok(
    (correlation_id, response): &(i32, ProduceResponse),
    expected_correlation_id: i32,
) -> Result<i64, Box<dyn std::error::Error>> {
    if *correlation_id != expected_correlation_id {
        return Err(format!(
            "response order: expected correlation id {expected_correlation_id}, got {correlation_id}"
        )
        .into());
    }
    let p = &response.responses[0].partition_responses[0];
    if p.error_code != 0 {
        return Err(format!(
            "produce (correlation id {}) failed with error {}",
            correlation_id, p.error_code
        )
        .into());
    }
    Ok(p.base_offset)
}

/// A failed request inside a group-commit batch rolls back alone.
///
/// Three requests are written to one connection in a single write, so the
/// worker sees them queued together (and, with group commit, runs them in one
/// top-level transaction): Produce "a", an EndTxn for a transactional id that
/// does not exist (the handler returns `Err`, which the worker receives as a
/// reported Rollback), then Produce "b".
///
/// Asserts: the three responses arrive in request order; both produces succeed
/// with consecutive base offsets 0 and 1; the EndTxn response carries a non-zero
/// error code; exactly 2 records are stored; and the worker still serves a new
/// connection afterwards (the handler error did not take down the batch or the
/// worker).
pub async fn test_failed_request_rolls_back_alone() -> TestResult {
    println!("=== Test: A Failed Request Rolls Back Alone ===\n");
    let ctx = TestContext::new().await?;
    let topic = crate::fixtures::TestTopicBuilder::new(&ctx, "lifecycle-rollback-alone")
        .with_partitions(1)
        .build()
        .await?;

    let mut end_txn = EndTxnRequest::default();
    end_txn.transactional_id = TransactionalId(StrBytes::from_string(format!(
        "lifecycle-missing-{}",
        uuid::Uuid::new_v4()
    )));
    end_txn.producer_id = ProducerId(987_654_321);
    end_txn.producer_epoch = 0;
    end_txn.committed = true;

    let mut frames = request_frame(PRODUCE_VERSION, 1, &produce_value(&topic.name, "a"));
    frames.extend_from_slice(&request_frame(0, 2, &end_txn));
    frames.extend_from_slice(&request_frame(
        PRODUCE_VERSION,
        3,
        &produce_value(&topic.name, "b"),
    ));

    let mut stream = TcpStream::connect(broker_addr()).await?;
    stream.write_all(&frames).await?;
    stream.flush().await?;
    println!("Step 1: pipelined Produce, failing EndTxn, Produce");

    let first = read_response::<ProduceRequest>(&mut stream, PRODUCE_VERSION).await?;
    let (end_corr, end_resp) = read_response::<EndTxnRequest>(&mut stream, 0).await?;
    let third = read_response::<ProduceRequest>(&mut stream, PRODUCE_VERSION).await?;

    let off_a = produce_ok(&first, 1)?;
    assert_eq!(end_corr, 2, "EndTxn response out of order");
    assert_ne!(
        end_resp.error_code, 0,
        "EndTxn for an unknown transactional id must fail"
    );
    let off_b = produce_ok(&third, 3)?;
    println!(
        "   produce a@{}, EndTxn error {}, produce b@{}",
        off_a, end_resp.error_code, off_b
    );
    assert_eq!(
        (off_a, off_b),
        (0, 1),
        "the two produces get offsets 0 and 1"
    );
    assert_eq!(count_messages(&ctx, &topic.name).await?, 2);

    let mut fresh = TcpStream::connect(broker_addr()).await?;
    let versions = roundtrip(&mut fresh, 0, 4, &ApiVersionsRequest::default()).await?;
    assert_eq!(
        versions.error_code, 0,
        "worker must still serve after the error"
    );
    println!("✅ Verification passed\n");

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

const GROUP_COMMIT_PRODUCERS: usize = 16;
const GROUP_COMMIT_RECORDS_PER_PRODUCER: usize = 20;

/// Group commit keeps RB-1 (an ack implies a durable write) across a batch.
///
/// 16 producers (separate rdkafka clients, so separate connections) send 20
/// records each, concurrently, to one partition. Concurrent requests queue up
/// together and are committed in shared top-level transactions. After each ack,
/// the producing task immediately reads that exact record (topic, partition,
/// acked offset, unique key) on its OWN Postgres connection, once, with no
/// retry.
///
/// Asserts: every acked record is visible on that first read (no response is
/// released before its batch's commit); all 320 records are stored; and the 320
/// acked offsets are distinct (one per record, none reused across a batch).
pub async fn test_group_commit_concurrent_acks_visible() -> TestResult {
    println!("=== Test: Group Commit — Every Acked Record Is Visible ===\n");
    let ctx = TestContext::new().await?;
    let topic = crate::fixtures::TestTopicBuilder::new(&ctx, "lifecycle-group-commit")
        .with_partitions(1)
        .build()
        .await?;

    let mut tasks = Vec::new();
    for producer_index in 0..GROUP_COMMIT_PRODUCERS {
        let topic = topic.name.clone();
        tasks.push(tokio::spawn(async move {
            let run = async {
                let producer = create_producer()?;
                let db = create_db_client().await?;
                let mut offsets = Vec::new();
                for i in 0..GROUP_COMMIT_RECORDS_PER_PRODUCER {
                    let key = format!("p{producer_index}-r{i}");
                    let (partition, offset) = producer
                        .send(
                            rdkafka::producer::FutureRecord::to(&topic)
                                .key(&key)
                                .payload("group-commit")
                                .partition(0),
                            Duration::from_secs(10),
                        )
                        .await
                        .map_err(|(e, _)| e)?;
                    let visible: i64 = db
                        .query_one(
                            "SELECT COUNT(*) FROM kafka.messages m
                             JOIN kafka.topics t ON m.topic_id = t.id
                             WHERE t.name = $1 AND m.partition_id = $2
                               AND m.partition_offset = $3 AND m.key = $4",
                            &[&topic, &partition, &offset, &key.as_bytes()],
                        )
                        .await?
                        .get(0);
                    if visible != 1 {
                        return Err(format!(
                            "BARRIER VIOLATION: record {key} acked at offset {offset} but the first \
                             read from another connection saw {visible} rows"
                        )
                        .into());
                    }
                    offsets.push(offset);
                }
                Ok::<Vec<i64>, Box<dyn std::error::Error>>(offsets)
            };
            run.await.map_err(|e| e.to_string())
        }));
    }

    let mut all_offsets = Vec::new();
    for task in tasks {
        all_offsets.extend(task.await??);
    }
    let expected = GROUP_COMMIT_PRODUCERS * GROUP_COMMIT_RECORDS_PER_PRODUCER;
    println!(
        "   {} records acked, each visible on first read",
        all_offsets.len()
    );

    all_offsets.sort_unstable();
    all_offsets.dedup();
    assert_eq!(
        all_offsets.len(),
        expected,
        "acked offsets must be distinct"
    );
    assert_eq!(count_messages(&ctx, &topic.name).await?, to_i64(expected));
    println!("✅ Verification passed\n");

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

const PIPELINED_PRODUCES: i32 = 40;

/// Group commit keeps response order and offsets within a batch.
///
/// 40 Produce requests are written to one connection in a single write, so the
/// worker finds them queued together and commits them in one or a few batches.
///
/// Asserts: 40 responses come back in request (correlation id) order, each
/// without error, with base offsets exactly 0..39; and immediately afterwards
/// all 40 records are visible from a separate connection.
pub async fn test_group_commit_pipelined_produces_in_order() -> TestResult {
    println!("=== Test: Group Commit — Pipelined Produces Answer In Order ===\n");
    let ctx = TestContext::new().await?;
    let topic = crate::fixtures::TestTopicBuilder::new(&ctx, "lifecycle-pipelined")
        .with_partitions(1)
        .build()
        .await?;

    let mut frames = Vec::new();
    for i in 0..PIPELINED_PRODUCES {
        frames.extend_from_slice(&request_frame(
            PRODUCE_VERSION,
            100 + i,
            &produce_value(&topic.name, &format!("pipelined-{i}")),
        ));
    }
    let mut stream = TcpStream::connect(broker_addr()).await?;
    stream.write_all(&frames).await?;
    stream.flush().await?;

    for i in 0..PIPELINED_PRODUCES {
        let response = tokio::time::timeout(
            Duration::from_secs(30),
            read_response::<ProduceRequest>(&mut stream, PRODUCE_VERSION),
        )
        .await
        .map_err(|_| format!("timed out waiting for response {i}"))??;
        let offset = produce_ok(&response, 100 + i)?;
        assert_eq!(offset, i64::from(i), "produce {i} got base offset {offset}");
    }
    println!(
        "   {} responses in order, offsets 0..{}",
        PIPELINED_PRODUCES,
        PIPELINED_PRODUCES - 1
    );
    assert_eq!(
        count_messages(&ctx, &topic.name).await?,
        i64::from(PIPELINED_PRODUCES),
        "every acked record must already be visible"
    );
    println!("✅ Verification passed\n");

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
