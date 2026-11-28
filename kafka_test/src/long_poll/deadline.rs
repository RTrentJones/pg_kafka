//! A long poll's deadline answer holds a change no notification names once that change is
//! one safety-net interval old.
//!
//! A long poll of more partitions than the safety net's allowance (256 per interval) is
//! never re-checked while it waits; a change made in another session (an on-demand
//! retention sweep, DROP EXTENSION) reaches it only through its deadline read. Answering its
//! first read at the deadline reports a log start the sweep already moved.

use crate::common::TestResult;
use crate::fixtures::topic_with_partitions;
use crate::setup::TestContext;
use crate::wire::records::plain_batch;
use crate::wire::Wire;
use bytes::Bytes;
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchRequest, FetchTopic};
use kafka_protocol::messages::fetch_response::FetchResponse;
use kafka_protocol::messages::{BrokerId, TopicName};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::Compression;
use std::time::{Duration, Instant};

/// More partitions than the safety net's allowance.
const PARTITIONS: i32 = 300;
/// Longer than two safety-net intervals (`pg_kafka.fetch_poll_interval_ms`, 1000 ms).
const MAX_WAIT_MS: i32 = 3_000;

/// A long poll of 300 partitions, caught up on partition 0 (offset 1) and waiting, answers
/// at its deadline the log start an on-demand retention sweep in another session moved to 1
/// while it waited, not its first read's 0. Sequential: the sweep is the whole broker's,
/// and it times the answer.
pub async fn test_deadline_answer_sees_an_unnotified_change() -> TestResult {
    println!("=== Test: Deadline Answer Sees An Unnotified Change ===\n");
    let ctx = TestContext::new().await?;
    let topic = topic_with_partitions(&ctx, "lp-deadline", PARTITIONS).await?;
    let mut wire = Wire::connect().await?;
    let (code, _) = wire
        .produce(
            &topic,
            0,
            plain_batch(&[Bytes::from_static(b"v")], Compression::None),
            None,
        )
        .await?;
    assert_eq!(code, 0, "Produce");
    // Every record of the topic is expired under a 1 ms retention.
    ctx.db()
        .execute(
            "UPDATE kafka.topics SET retention_ms = 1 WHERE name = $1",
            &[&topic],
        )
        .await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    println!("Step 1: a long poll of {PARTITIONS} partitions, max_wait {MAX_WAIT_MS} ms...");
    let fetch_topic = FetchTopic::default()
        .with_topic(TopicName(StrBytes::from_string(topic.clone())))
        .with_partitions(
            (0..PARTITIONS)
                .map(|partition| {
                    FetchPartition::default()
                        .with_partition(partition)
                        .with_fetch_offset(i64::from(partition == 0))
                        .with_partition_max_bytes(1024)
                        .with_log_start_offset(-1)
                        .with_current_leader_epoch(-1)
                        .with_last_fetched_epoch(-1)
                })
                .collect(),
        );
    let request = FetchRequest::default()
        .with_replica_id(BrokerId(-1))
        .with_max_wait_ms(MAX_WAIT_MS)
        .with_min_bytes(1)
        .with_max_bytes(1 << 20)
        .with_session_epoch(-1)
        .with_topics(vec![fetch_topic]);
    let started = Instant::now();
    let poll = tokio::spawn(async move {
        let response: Result<FetchResponse, String> =
            wire.call(11, &request).await.map_err(|e| e.to_string());
        response
    });

    tokio::time::sleep(Duration::from_millis(500)).await;
    println!("Step 2: an on-demand retention sweep in this session...");
    ctx.db()
        .query("SELECT * FROM pg_kafka_run_retention_sweep(0, 3600)", &[])
        .await?;
    let log_start: i64 = ctx
        .db()
        .query_one(
            "SELECT po.log_start_offset FROM kafka.partition_offsets po
             JOIN kafka.topics t ON t.id = po.topic_id
             WHERE t.name = $1 AND po.partition_id = 0",
            &[&topic],
        )
        .await?
        .get(0);
    assert_eq!(log_start, 1, "the sweep moved the log start");

    let response = poll.await??;
    let waited = started.elapsed();
    let partition = response.responses[0]
        .partitions
        .iter()
        .find(|p| p.partition_index == 0)
        .ok_or("partition 0 missing from the answer")?;
    println!(
        "   answered after {waited:?}: error {}, log start {}",
        partition.error_code, partition.log_start_offset
    );
    assert!(
        waited >= Duration::from_millis(u64::try_from(MAX_WAIT_MS)?),
        "answered before its deadline"
    );
    assert_eq!(partition.error_code, 0);
    assert_eq!(
        partition.log_start_offset, 1,
        "the deadline answer reports its first read's log start, from before the sweep"
    );

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
