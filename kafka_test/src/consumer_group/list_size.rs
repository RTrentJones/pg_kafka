//! ListGroups responses are bounded by `pg_kafka.fetch_max_bytes`, however long the group ids.

use crate::common::{DynResult, TestResult};
use crate::error_codes;
use crate::fixtures::topic_with_partitions as topic;
use crate::setup::{SystemGucGuard, TestContext};
use crate::wire::Wire;
use kafka_protocol::messages::list_groups_request::ListGroupsRequest;
use kafka_protocol::messages::list_groups_response::ListGroupsResponse;
use kafka_protocol::messages::offset_commit_request::{
    OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
use kafka_protocol::messages::offset_commit_response::OffsetCommitResponse;
use kafka_protocol::messages::{GroupId, TopicName};
use kafka_protocol::protocol::StrBytes;

/// `pg_kafka.fetch_max_bytes` for the test, in bytes.
const CAP: usize = 128 * 1024;

/// How many groups the test commits offsets for, each with a 30 kB id: 600 kB of ids.
const GROUPS: usize = 20;

/// The review's reproduction, scaled down: 20 groups with 30 kB ids, known by their committed
/// offsets, listed by ListGroups. Kafka, and pg_kafka before, listed every group (300 such
/// groups made a 9 MB response, and the groups known by their offsets are bounded only by the
/// database). Now the groups are listed in id order within the 128 kB cap plus one group, and
/// the rest are left out.
pub async fn test_list_groups_is_bounded_by_fetch_max_bytes() -> TestResult {
    println!("=== Test: ListGroups is bounded by pg_kafka.fetch_max_bytes ===\n");
    let ctx = TestContext::new().await?;
    let cap = SystemGucGuard::set(ctx.db(), "pg_kafka.fetch_max_bytes", &CAP.to_string())
        .await
        .map_err(widen)?
        .ok_or("the server refused pg_kafka.fetch_max_bytes")?;
    let base = ctx.unique_group("list-size").await;
    // A `String` error, so the future stays `Send` across the restore.
    let result = run(&ctx, &base).await.map_err(|e| e.to_string());
    cap.restore().await.map_err(widen)?;
    ctx.db()
        .execute(
            "DELETE FROM kafka.consumer_offsets WHERE starts_with(group_id, $1)",
            &[&base],
        )
        .await?;
    result?;
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

async fn run(ctx: &TestContext, base: &str) -> DynResult<()> {
    let t = topic(ctx, "list-size", 1).await?;
    let pad = "x".repeat(30 * 1024);
    let groups: Vec<String> = (0..GROUPS)
        .map(|i| format!("{base}-{i:02}-{pad}"))
        .collect();
    println!(
        "Step 1: commit an offset for {GROUPS} groups with {} byte ids...",
        groups[0].len()
    );
    let mut w = Wire::connect().await?;
    for group in &groups {
        let mut p = OffsetCommitRequestPartition::default();
        p.partition_index = 0;
        p.committed_offset = 1;
        let mut topic = OffsetCommitRequestTopic::default();
        topic.name = TopicName(StrBytes::from_string(t.clone()));
        topic.partitions = vec![p];
        let mut req = OffsetCommitRequest::default();
        req.group_id = GroupId(StrBytes::from_string(group.clone()));
        req.generation_id_or_member_epoch = -1;
        req.topics = vec![topic];
        let resp: OffsetCommitResponse = w.call(8, &req).await?;
        assert_eq!(resp.topics[0].partitions[0].error_code, error_codes::NONE);
    }

    println!("Step 2: ListGroups...");
    let resp: ListGroupsResponse = w.call(4, &ListGroupsRequest::default()).await?;
    assert_eq!(resp.error_code, error_codes::NONE);
    let listed_bytes: usize = resp.groups.iter().map(|g| g.group_id.len()).sum();
    let ours = resp
        .groups
        .iter()
        .filter(|g| g.group_id.as_str().starts_with(base))
        .count();
    println!(
        "  {} groups listed ({listed_bytes} bytes of ids), {ours} of the test's {GROUPS}",
        resp.groups.len()
    );
    assert!(
        listed_bytes <= CAP + groups[0].len(),
        "{listed_bytes} bytes of group ids listed, over pg_kafka.fetch_max_bytes ({CAP}) plus one group"
    );
    assert!(ours < GROUPS, "every group was listed");
    let ids: Vec<&str> = resp.groups.iter().map(|g| g.group_id.as_str()).collect();
    assert!(ids.windows(2).all(|w| w[0] < w[1]), "listed in id order");
    Ok(())
}

/// The guard's errors are `Send + Sync`; this file's results are not.
fn widen(e: crate::setup::GucError) -> Box<dyn std::error::Error> {
    e
}
