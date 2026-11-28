//! OffsetFetch responses are bounded by `pg_kafka.fetch_max_bytes`, however much metadata the
//! group committed.

use super::fetch_parity::commit;
use crate::common::{DynResult, TestResult};
use crate::error_codes;
use crate::fixtures::topic_with_partitions as topic;
use crate::setup::{SystemGucGuard, TestContext};
use crate::wire::Wire;
use kafka_protocol::messages::offset_fetch_request::{OffsetFetchRequest, OffsetFetchRequestTopic};
use kafka_protocol::messages::offset_fetch_response::OffsetFetchResponse;
use kafka_protocol::messages::{GroupId, TopicName};
use kafka_protocol::protocol::StrBytes;

/// `pg_kafka.fetch_max_bytes` for the test, in bytes.
const CAP: usize = 64 * 1024;

/// Partitions committed with the longest metadata Kafka accepts (4096 characters, 12 kB of
/// UTF-8 here): 30 of them, 360 kB of metadata.
const PARTITIONS: i32 = 30;

/// The review's reproduction, scaled down: a group commits 4096-character metadata (12 kB) on
/// 30 partitions, plus one partition without metadata, and OffsetFetch v7 reads them all,
/// by fetching every offset of the group and by naming the partitions. Kafka, and pg_kafka
/// before, answered all 360 kB of metadata (2.4 GB at 200,000 partitions, for a request of a
/// few bytes). Now the metadata answered fits the 64 kB cap plus one partition's, the first
/// partition is answered in full, each later partition with metadata is answered
/// COORDINATOR_NOT_AVAILABLE with offset -1, and the partition without metadata is answered.
pub async fn test_offset_fetch_is_bounded_by_fetch_max_bytes() -> TestResult {
    println!("=== Test: OffsetFetch is bounded by pg_kafka.fetch_max_bytes ===\n");
    let ctx = TestContext::new().await?;
    let cap = SystemGucGuard::set(ctx.db(), "pg_kafka.fetch_max_bytes", &CAP.to_string())
        .await
        .map_err(widen)?
        .ok_or("the server refused pg_kafka.fetch_max_bytes")?;
    // A `String` error, so the future stays `Send` across the restore.
    let result = run(&ctx).await.map_err(|e| e.to_string());
    cap.restore().await.map_err(widen)?;
    result?;
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

async fn run(ctx: &TestContext) -> DynResult<()> {
    let t = topic(ctx, "offset-fetch-bound", PARTITIONS + 1).await?;
    let group = ctx.unique_group("offset-fetch-bound").await;
    let metadata = "€".repeat(4096);
    println!(
        "Step 1: commit {PARTITIONS} partitions with {} bytes of metadata each, and one without...",
        metadata.len()
    );
    let mut w = Wire::connect().await?;
    let mut entries: Vec<(i32, i64, Option<String>)> = (0..PARTITIONS)
        .map(|p| (p, i64::from(p) + 100, Some(metadata.clone())))
        .collect();
    entries.push((PARTITIONS, 7, None));
    let codes = commit(&mut w, &group, &t, &entries).await;
    assert!(codes.iter().all(|&c| c == error_codes::NONE), "{codes:?}");

    for fetch_all in [true, false] {
        println!(
            "Step 2: OffsetFetch v7 ({})...",
            if fetch_all {
                "every offset"
            } else {
                "named partitions"
            }
        );
        let mut req = OffsetFetchRequest::default();
        req.group_id = GroupId(StrBytes::from_string(group.clone()));
        req.topics = if fetch_all {
            None
        } else {
            let mut named = OffsetFetchRequestTopic::default();
            named.name = TopicName(StrBytes::from_string(t.clone()));
            named.partition_indexes = (0..=PARTITIONS).collect();
            Some(vec![named])
        };
        let resp: OffsetFetchResponse = w.call(7, &req).await?;
        assert_eq!(resp.error_code, error_codes::NONE);
        let mut parts: Vec<(i32, i16, i64, usize)> = resp
            .topics
            .iter()
            .filter(|topic| topic.name.as_str() == t)
            .flat_map(|topic| topic.partitions.iter())
            .map(|p| {
                let bytes = p.metadata.as_ref().map_or(0, |m| m.len());
                (p.partition_index, p.error_code, p.committed_offset, bytes)
            })
            .collect();
        parts.sort_unstable();
        let answered_bytes: usize = parts.iter().map(|p| p.3).sum();
        let full = parts
            .iter()
            .filter(|p| p.1 == error_codes::NONE && p.3 > 0)
            .count();
        let left_out = parts
            .iter()
            .filter(|p| p.1 == error_codes::COORDINATOR_NOT_AVAILABLE)
            .count();
        println!("  {} partitions answered: {full} with metadata ({answered_bytes} bytes), {left_out} left out", parts.len());
        assert_eq!(
            parts.len(),
            usize::try_from(PARTITIONS)? + 1,
            "every partition is answered"
        );
        assert!(
            answered_bytes <= CAP + metadata.len(),
            "{answered_bytes} bytes of metadata answered, over pg_kafka.fetch_max_bytes ({CAP}) plus one partition's"
        );
        assert_eq!(
            parts[0],
            (0, error_codes::NONE, 100, metadata.len()),
            "the first partition is answered in full"
        );
        assert!(left_out > 0 && full + left_out == usize::try_from(PARTITIONS)?);
        for &(p, code, offset, bytes) in &parts {
            if code == error_codes::COORDINATOR_NOT_AVAILABLE {
                assert_eq!((offset, bytes), (-1, 0), "partition {p} left out");
            }
        }
        assert_eq!(
            parts[usize::try_from(PARTITIONS)?],
            (PARTITIONS, error_codes::NONE, 7, 0),
            "a partition without metadata is always answered"
        );
    }
    Ok(())
}

/// The guard's errors are `Send + Sync`; this file's results are not.
fn widen(e: crate::setup::GucError) -> Box<dyn std::error::Error> {
    e
}
