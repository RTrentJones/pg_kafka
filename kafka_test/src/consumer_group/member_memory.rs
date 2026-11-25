//! `pg_kafka.max_group_memory`: the memory all groups' member state may take together.

use crate::common::{DynResult, TestResult};
use crate::error_codes;
use crate::setup::{GucError, SystemGucGuard, TestContext};
use crate::wire::requests::{conn, join_now, join_req, leave, s};
use crate::wire::roundtrip;
use bytes::Bytes;
use kafka_protocol::messages::sync_group_request::{SyncGroupRequest, SyncGroupRequestAssignment};
use kafka_protocol::messages::GroupId;
use tokio_postgres::Client;

const GUC: &str = "pg_kafka.max_group_memory";

/// Set `pg_kafka.max_group_memory` for the test. The guard restores the earlier setting when
/// the test calls `restore`, and on drop when an assert panics or the runner aborts the test.
async fn set_budget(db: &Client, value: &str) -> DynResult<SystemGucGuard> {
    SystemGucGuard::set(db, GUC, value)
        .await
        .map_err(widen)?
        .ok_or_else(|| format!("the server refused {GUC} = {value}").into())
}

/// The guard's errors are `Send + Sync`; this file's results are not.
fn widen(e: GucError) -> Box<dyn std::error::Error> {
    e
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
    let budget = set_budget(ctx.db(), "2MB").await?;
    run(&ctx).await?;
    budget.restore().await.map_err(widen)?;
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

/// The review's reproduction, scaled to a small budget: every JoinGroup and LeaveGroup under a
/// long group id leaves an Empty group (no members) that holds the id for 24 hours. With
/// `pg_kafka.max_group_memory` = 2MB about 30 of them, with 30 000-byte ids, filled the budget
/// and every JoinGroup that created a group got COORDINATOR_NOT_AVAILABLE from then on, though
/// nothing else held the memory. The least recently active Empty groups are forgotten to make
/// room (as at `pg_kafka.max_consumer_groups`), so all of them, and a new group after them,
/// are admitted.
pub async fn test_empty_groups_do_not_exhaust_the_memory_budget() -> TestResult {
    println!("=== Test: Empty groups do not exhaust pg_kafka.max_group_memory ===\n");
    let ctx = TestContext::new().await?;
    let budget = set_budget(ctx.db(), "2MB").await?;
    run_empty_groups(&ctx).await?;
    budget.restore().await.map_err(widen)?;
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

async fn run_empty_groups(ctx: &TestContext) -> DynResult<()> {
    const GROUPS: usize = 100;
    let base = ctx.unique_group("mem-empty").await;
    let mut c = conn().await?;
    println!("Step 1: {GROUPS} JoinGroup + LeaveGroup pairs under 30 000-byte group ids...");
    for i in 0..GROUPS {
        let group = format!("{base}-{i}-{}", "x".repeat(30_000));
        let (code, _, member, _) =
            join_now(&mut c, &join_req(&group, "", "consumer", &["range"])).await?;
        assert_eq!(code, error_codes::NONE, "JoinGroup {i} creating its group");
        assert_eq!(leave(&mut c, &group, &member).await?, error_codes::NONE);
    }
    println!("Step 2: a new group is still admitted...");
    let group = ctx.unique_group("mem-after").await;
    let (code, _, member, _) =
        join_now(&mut c, &join_req(&group, "", "consumer", &["range"])).await?;
    assert_eq!(code, error_codes::NONE);
    assert_eq!(leave(&mut c, &group, &member).await?, error_codes::NONE);
    Ok(())
}

/// A JoinGroup whose join phase completes at once stores the protocol it selects as the
/// group's protocol name. A lone member on "range" rejoining with one 30 000-byte protocol
/// grows the group by that protocol in its list and by the name: about 60 000 bytes. With
/// about 45 000 bytes of the budget left the rejoin was admitted (it was priced with the
/// group's old name), leaving the coordinator over `pg_kafka.max_group_memory`; it gets
/// COORDINATOR_NOT_AVAILABLE, and is admitted once the room is there.
pub async fn test_a_rejoin_is_priced_with_the_protocol_it_selects() -> TestResult {
    println!("=== Test: a rejoin is priced with the protocol name it selects ===\n");
    let ctx = TestContext::new().await?;
    let budget = set_budget(ctx.db(), "2MB").await?;
    run_rejoin(&ctx).await?;
    budget.restore().await.map_err(widen)?;
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// A JoinGroup of a new member (sending no Heartbeat for the test's length) with
/// `metadata` bytes of metadata.
fn member_with(group: &str, metadata: usize) -> kafka_protocol::messages::JoinGroupRequest {
    let mut r = join_req(group, "", "consumer", &["range"]);
    r.session_timeout_ms = 120_000;
    r.rebalance_timeout_ms = 120_000;
    r.protocols[0].metadata = Bytes::from(vec![7u8; metadata]);
    r
}

/// The most metadata a new member of `probe` gets admitted with now, to within 256 bytes
/// (each probe joins and leaves; at most `hi` bytes).
async fn room(c: &mut tokio::net::TcpStream, probe: &str, hi: usize) -> DynResult<usize> {
    let (mut lo, mut hi) = (0, hi);
    while hi - lo > 256 {
        let mid = lo + (hi - lo) / 2;
        let (code, _, member, _) = join_now(c, &member_with(probe, mid)).await?;
        if code == error_codes::NONE {
            assert_eq!(leave(c, probe, &member).await?, error_codes::NONE);
            lo = mid;
        } else {
            assert_eq!(
                code,
                error_codes::COORDINATOR_NOT_AVAILABLE,
                "probe of {mid} bytes"
            );
            hi = mid;
        }
    }
    Ok(lo)
}

async fn run_rejoin(ctx: &TestContext) -> DynResult<()> {
    let mut c = conn().await?;
    let mut members = Vec::new();
    println!("Step 1: two 700 kB members leave less than one more of the budget...");
    for i in 0..2 {
        let group = ctx.unique_group(&format!("rejoin-fill-{i}")).await;
        let (code, _, member, _) = join_now(&mut c, &member_with(&group, 700 * 1024)).await?;
        assert_eq!(code, error_codes::NONE, "fill member {i}");
        members.push((group, member));
    }
    let lone = ctx.unique_group("rejoin-lone").await;
    let (code, generation, lone_member, name) = join_now(&mut c, &member_with(&lone, 2)).await?;
    assert_eq!((code, name.as_deref()), (error_codes::NONE, Some("range")));
    assert_eq!(generation, 1);

    println!("Step 2: fill the budget to about 45 000 bytes of room...");
    let probe = ctx.unique_group("rejoin-probe").await;
    let free = room(&mut c, &probe, 800 * 1024).await?;
    println!("  {free} bytes of metadata fit");
    let filler = ctx.unique_group("rejoin-fillr").await;
    let fill = free
        .checked_sub(45_000)
        .ok_or_else(|| format!("only {free} bytes of room"))?;
    let (code, _, filler_member, _) = join_now(&mut c, &member_with(&filler, fill)).await?;
    assert_eq!(code, error_codes::NONE);
    let (code, _, member, _) = join_now(&mut c, &member_with(&probe, 40_000)).await?;
    assert_eq!(code, error_codes::NONE, "40 000 bytes of room are left");
    assert_eq!(leave(&mut c, &probe, &member).await?, error_codes::NONE);

    println!("Step 3: the lone member rejoins with one 30 000-byte protocol...");
    let long = "P".repeat(30_000);
    let mut rejoin = member_with(&lone, 2);
    rejoin.member_id = s(&lone_member);
    rejoin.protocols[0].name = s(&long);
    let (code, ..) = join_now(&mut c, &rejoin).await?;
    assert_eq!(
        code,
        error_codes::COORDINATOR_NOT_AVAILABLE,
        "the rejoin needs about 60 000 bytes"
    );

    println!("Step 4: with the filler gone it is admitted and selects the long protocol...");
    assert_eq!(
        leave(&mut c, &filler, &filler_member).await?,
        error_codes::NONE
    );
    let (code, generation, _, name) = join_now(&mut c, &rejoin).await?;
    assert_eq!(code, error_codes::NONE);
    assert_eq!((generation, name.as_deref()), (2, Some(long.as_str())));

    assert_eq!(leave(&mut c, &lone, &lone_member).await?, error_codes::NONE);
    for (group, member) in members {
        assert_eq!(leave(&mut c, &group, &member).await?, error_codes::NONE);
    }
    Ok(())
}
