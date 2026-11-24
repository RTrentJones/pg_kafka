//! `pg_kafka.max_group_memory`: the memory all groups' member state may take together.

use crate::common::{DynResult, TestResult};
use crate::error_codes;
use crate::setup::TestContext;
use crate::wire::requests::{conn, join_now, join_req, leave, s};
use crate::wire::roundtrip;
use bytes::Bytes;
use kafka_protocol::messages::sync_group_request::{SyncGroupRequest, SyncGroupRequestAssignment};
use kafka_protocol::messages::GroupId;
use tokio_postgres::Client;

const GUC: &str = "pg_kafka.max_group_memory";

async fn set_budget(db: &Client, value: Option<&str>) -> DynResult<()> {
    let sql = match value {
        Some(v) => format!("ALTER SYSTEM SET {GUC} = '{v}'"),
        None => format!("ALTER SYSTEM RESET {GUC}"),
    };
    db.batch_execute(&sql).await?;
    db.batch_execute("SELECT pg_reload_conf()").await?;
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    Ok(())
}

/// The review's reproduction, scaled to a small budget: JoinGroups to new groups that never
/// sync, each with 700 kB of metadata and the longest session timeout, held that memory in the
/// listener worker for up to 30 minutes, however many arrived (300 of 1 MB grew it by 300 MB;
/// at network rate an allocation failure aborts the backend and Postgres restarts every
/// backend). With `pg_kafka.max_group_memory` = 2MB the first two fit; the next is refused
/// with the retriable COORDINATOR_NOT_AVAILABLE and creates no group, as is a leader SyncGroup
/// whose assignments do not fit; memory a member frees on leaving is available again.
pub async fn test_member_state_beyond_the_memory_budget_is_refused() -> TestResult {
    println!("=== Test: member state beyond pg_kafka.max_group_memory is refused ===\n");
    let ctx = TestContext::new().await?;
    set_budget(ctx.db(), Some("2MB")).await?;
    let outcome = run(&ctx).await.map_err(|e| e.to_string());
    set_budget(ctx.db(), None).await?;
    outcome?;
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

async fn run(ctx: &TestContext) -> DynResult<()> {
    let big = |group: &str| {
        let mut r = join_req(group, "", "consumer", &["range"]);
        r.session_timeout_ms = 1_800_000;
        r.rebalance_timeout_ms = 1_800_000;
        r.protocols[0].metadata = Bytes::from(vec![7u8; 700 * 1024]);
        r
    };
    let mut members = Vec::new();
    println!("Step 1: two groups with 700 kB members fit...");
    for i in 0..2 {
        let group = ctx.unique_group(&format!("mem-{i}")).await;
        let mut c = conn().await?;
        let (e, _, member, _) = join_now(&mut c, &big(&group)).await?;
        assert_eq!(e, error_codes::NONE, "member {i}");
        members.push((c, group, member));
    }
    println!("Step 2: a third does not...");
    let third = ctx.unique_group("mem-2").await;
    let mut third_conn = conn().await?;
    let (code, ..) = join_now(&mut third_conn, &big(&third)).await?;
    assert_eq!(code, error_codes::COORDINATOR_NOT_AVAILABLE);
    let (code, state, _, members_described) = crate::wire::requests::describe(&third).await?;
    assert_eq!(
        (code, state.as_str(), members_described),
        (error_codes::NONE, "Dead", 0),
        "no group is created"
    );

    println!("Step 3: a small member still fits, but not its 700 kB assignment...");
    let small = ctx.unique_group("mem-small").await;
    let mut sc = conn().await?;
    let (e, generation, member, _) =
        join_now(&mut sc, &join_req(&small, "", "consumer", &["range"])).await?;
    assert_eq!(e, error_codes::NONE);
    let mut assignment = SyncGroupRequestAssignment::default();
    assignment.member_id = s(&member);
    assignment.assignment = Bytes::from(vec![1u8; 700 * 1024]);
    let mut sync = SyncGroupRequest::default();
    sync.group_id = GroupId(s(&small));
    sync.generation_id = generation;
    sync.member_id = s(&member);
    sync.assignments = vec![assignment];
    let synced = roundtrip(&mut sc, 3, 30, &sync).await?;
    assert_eq!(synced.error_code, error_codes::COORDINATOR_NOT_AVAILABLE);
    assert_eq!(leave(&mut sc, &small, &member).await?, error_codes::NONE);

    println!("Step 4: once a member leaves, the third fits...");
    let (mut c0, g0, m0) = members.remove(0);
    assert_eq!(leave(&mut c0, &g0, &m0).await?, error_codes::NONE);
    let (code, _, member, _) = join_now(&mut third_conn, &big(&third)).await?;
    assert_eq!(code, error_codes::NONE);
    assert_eq!(
        leave(&mut third_conn, &third, &member).await?,
        error_codes::NONE
    );
    for (mut c, group, member) in members {
        assert_eq!(leave(&mut c, &group, &member).await?, error_codes::NONE);
    }
    Ok(())
}
