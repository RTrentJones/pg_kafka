//! DescribeGroups responses are bounded by `pg_kafka.fetch_max_bytes`, however often a
//! request names a group.

use crate::common::{DynResult, TestResult};
use crate::error_codes;
use crate::setup::{SystemGucGuard, TestContext};
use crate::wire::requests::{conn, join_now, join_req, leave, s};
use crate::wire::{decode_response, read_raw_response, roundtrip, send};
use bytes::Bytes;
use kafka_protocol::messages::describe_groups_request::DescribeGroupsRequest;
use kafka_protocol::messages::sync_group_request::{SyncGroupRequest, SyncGroupRequestAssignment};
use kafka_protocol::messages::GroupId;
use tokio_postgres::Client;

/// The member's subscription metadata and its assignment: 200 kB each.
const PAYLOAD: usize = 200 * 1024;

/// `pg_kafka.fetch_max_bytes` for the test, in bytes.
const RESPONSE_CAP: usize = 2 << 20;

/// The review's reproduction, scaled down: a Stable group of one member with 200 kB of
/// metadata and 200 kB of assignment, described by DescribeGroups v0 requests that name it
/// 4, 16 and 64 times (a few hundred bytes each). Each repeat copied the member's metadata
/// and assignment into the response, so the response, and the listener's memory, grew by
/// 400 kB per repeat: a 1.6 kB request answered with 400 MB, and a 100 kB one against a 2 MB
/// group needed about 66 GB. Every entry is still answered, as in Kafka 3.7, but the entries
/// described in full fit `pg_kafka.fetch_max_bytes` (here 2 MB) and the rest are answered
/// COORDINATOR_NOT_AVAILABLE, so the response stays under the cap plus one group whatever
/// the repeat count.
pub async fn test_describe_groups_of_a_repeated_group_is_bounded() -> TestResult {
    println!("=== Test: DescribeGroups of a repeated large group is bounded ===\n");
    let ctx = TestContext::new().await?;
    let cap = SystemGucGuard::set(
        ctx.db(),
        "pg_kafka.fetch_max_bytes",
        &RESPONSE_CAP.to_string(),
    )
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
    let group = ctx.unique_group("describe-size").await;
    println!(
        "Step 1: a Stable group of one member with {PAYLOAD} bytes of metadata and assignment..."
    );
    let mut c = conn().await?;
    let mut join = join_req(&group, "", "consumer", &["range"]);
    join.session_timeout_ms = 60_000;
    join.protocols[0].metadata = Bytes::from(vec![7u8; PAYLOAD]);
    let (e, generation, member, _) = join_now(&mut c, &join).await?;
    assert_eq!(e, error_codes::NONE);
    let mut assignment = SyncGroupRequestAssignment::default();
    assignment.member_id = s(&member);
    assignment.assignment = Bytes::from(vec![9u8; PAYLOAD]);
    let mut sync = SyncGroupRequest::default();
    sync.group_id = GroupId(s(&group));
    sync.generation_id = generation;
    sync.member_id = s(&member);
    sync.assignments = vec![assignment];
    let synced = roundtrip(&mut c, 3, 2, &sync).await?;
    assert_eq!(synced.error_code, error_codes::NONE);

    println!("Step 2: DescribeGroups naming it 4, 16 and 64 times...");
    let pid = listener_pid(ctx.db()).await?;
    let mut d = conn().await?;
    let mut sizes = Vec::new();
    for (i, repeats) in [4_usize, 16, 64].into_iter().enumerate() {
        let mut req = DescribeGroupsRequest::default();
        req.groups = vec![GroupId(s(&group)); repeats];
        let hwm_before = pid.and_then(|p| proc_kb(p, "VmHWM:"));
        let correlation = i32::try_from(10 + i)?;
        send(&mut d, 0, correlation, &req).await?;
        let (got, body) = read_raw_response(&mut d).await?;
        assert_eq!(got, correlation);
        let mut frame = got.to_be_bytes().to_vec();
        frame.extend_from_slice(&body);
        let (_, response) = decode_response::<DescribeGroupsRequest>(0, &frame)?;
        let described = response
            .groups
            .iter()
            .filter(|g| g.error_code == error_codes::NONE)
            .count();
        let refused = response
            .groups
            .iter()
            .filter(|g| g.error_code == error_codes::COORDINATOR_NOT_AVAILABLE)
            .count();
        let hwm_after = pid.and_then(|p| proc_kb(p, "VmHWM:"));
        println!(
            "  {repeats} repeats: {} response bytes, {described} described, {refused} refused; listener VmHWM {hwm_before:?} -> {hwm_after:?} kB",
            body.len()
        );
        assert_eq!(response.groups.len(), repeats, "every entry is answered");
        assert_eq!(described + refused, repeats);
        assert!(described >= 1, "the first entry is always described");
        if let Some(full) = response.groups.iter().find(|g| g.error_code == 0) {
            assert_eq!(full.members.len(), 1);
            assert_eq!(full.members[0].member_metadata.len(), PAYLOAD);
            assert_eq!(full.members[0].member_assignment.len(), PAYLOAD);
        }
        assert!(
            body.len() <= RESPONSE_CAP + 2 * PAYLOAD + 64 * 1024,
            "a {repeats}-repeat request was answered with {} bytes, over pg_kafka.fetch_max_bytes ({RESPONSE_CAP}) plus one group",
            body.len()
        );
        sizes.push(body.len());
    }
    assert!(
        sizes[2] < sizes[0] * 4,
        "the response grows with the repeat count: {sizes:?}"
    );

    assert_eq!(leave(&mut c, &group, &member).await?, error_codes::NONE);
    Ok(())
}

/// The guard's errors are `Send + Sync`; this file's results are not.
fn widen(e: crate::setup::GucError) -> Box<dyn std::error::Error> {
    e
}

/// The pg_kafka_listener backend's pid, if one runs.
async fn listener_pid(db: &Client) -> DynResult<Option<i32>> {
    Ok(db
        .query_opt(
            "SELECT pid FROM pg_stat_activity WHERE backend_type = 'pg_kafka_listener'",
            &[],
        )
        .await?
        .map(|row| row.get(0)))
}

/// A `/proc/<pid>/status` field in kB (`VmHWM:`), where the test runs on the database host.
fn proc_kb(pid: i32, field: &str) -> Option<u64> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    status
        .lines()
        .find(|l| l.starts_with(field))?
        .split_whitespace()
        .nth(1)?
        .parse()
        .ok()
}
