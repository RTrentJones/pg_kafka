//! A slow sync phase with members that send no Heartbeat while their SyncGroup is
//! outstanding, as librdkafka behaves.

use crate::common::{DynResult, TestResult};
use crate::error_codes;
use crate::setup::TestContext;
use crate::wire::requests::{conn, describe, join_now, join_req, leave, s};
use crate::wire::{recv, roundtrip, send};
use bytes::Bytes;
use kafka_protocol::messages::heartbeat_request::HeartbeatRequest;
use kafka_protocol::messages::join_group_request::JoinGroupRequest;
use kafka_protocol::messages::sync_group_request::{SyncGroupRequest, SyncGroupRequestAssignment};
use kafka_protocol::messages::GroupId;
use std::time::Duration;
use tokio::net::TcpStream;

/// The session timeout both members advertise (the broker's minimum); the leader syncs
/// later than this after the join phase completes.
const SESSION_TIMEOUT_MS: i32 = 6_000;

fn member_join(group: &str, member: &str) -> JoinGroupRequest {
    let mut r = join_req(group, member, "consumer", &["range"]);
    r.session_timeout_ms = SESSION_TIMEOUT_MS;
    r.rebalance_timeout_ms = 60_000;
    r
}

fn sync_request(
    group: &str,
    generation: i32,
    member: &str,
    assignments: &[(&str, &'static [u8])],
) -> SyncGroupRequest {
    let mut r = SyncGroupRequest::default();
    r.group_id = GroupId(s(group));
    r.generation_id = generation;
    r.member_id = s(member);
    r.assignments = assignments
        .iter()
        .map(|(m, bytes)| {
            let mut a = SyncGroupRequestAssignment::default();
            a.member_id = s(m);
            a.assignment = Bytes::from_static(bytes);
            a
        })
        .collect();
    r
}

async fn heartbeat(
    c: &mut TcpStream,
    group: &str,
    generation: i32,
    member: &str,
) -> DynResult<i16> {
    let mut r = HeartbeatRequest::default();
    r.group_id = GroupId(s(group));
    r.generation_id = generation;
    r.member_id = s(member);
    Ok(roundtrip(c, 3, 40, &r).await?.error_code)
}

/// A follower parks its SyncGroup and sends nothing else; the leader heartbeats (it has
/// its JoinGroup answer) and syncs only after more than the session timeout. Both SyncGroups are
/// answered with the assignments, and both members are still in the group after the
/// coordinator's sweep has run (as in Kafka, a SyncGroup answer restarts the member's
/// session clock).
pub async fn test_slow_leader_sync_keeps_silent_follower() -> TestResult {
    println!("=== Test: a slow leader SyncGroup keeps a non-heartbeating follower ===\n");
    let ctx = TestContext::new().await?;
    let group = ctx.unique_group("slow-sync").await;

    println!("Step 1: leader A forms the group...");
    let mut a = conn().await?;
    let (e, _, leader, _) = join_now(&mut a, &member_join(&group, "")).await?;
    assert_eq!(e, error_codes::NONE);

    println!("Step 2: follower B joins; A rejoins to complete the join phase...");
    let mut b = conn().await?;
    send(&mut b, 5, 10, &member_join(&group, "")).await?;
    // B's JoinGroup is held until A rejoins: give it time to reach the coordinator.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let (e, generation, _, _) = join_now(&mut a, &member_join(&group, &leader)).await?;
    assert_eq!(e, error_codes::NONE);
    let b_join = tokio::time::timeout(
        Duration::from_secs(15),
        recv::<JoinGroupRequest>(&mut b, 5, 10),
    )
    .await??;
    assert_eq!(b_join.error_code, error_codes::NONE);
    assert_eq!(b_join.generation_id, generation);
    let follower = b_join.member_id.to_string();
    println!("✅ generation {generation}, leader {leader}, follower {follower}\n");

    println!("Step 3: B's SyncGroup parks; B sends nothing for longer than the session timeout...");
    send(
        &mut b,
        3,
        11,
        &sync_request(&group, generation, &follower, &[]),
    )
    .await?;
    let wait_until = tokio::time::Instant::now()
        + Duration::from_millis(u64::try_from(SESSION_TIMEOUT_MS)? + 3_000);
    while tokio::time::Instant::now() < wait_until {
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_eq!(
            heartbeat(&mut a, &group, generation, &leader).await?,
            error_codes::NONE,
            "the leader heartbeats during the sync phase"
        );
    }

    println!("Step 4: A syncs late; both SyncGroups are answered...");
    let a_sync = roundtrip(
        &mut a,
        3,
        12,
        &sync_request(
            &group,
            generation,
            &leader,
            &[(leader.as_str(), b"A"), (follower.as_str(), b"B")],
        ),
    )
    .await?;
    assert_eq!(a_sync.error_code, error_codes::NONE);
    assert_eq!(a_sync.assignment.as_ref(), b"A");
    let b_sync = tokio::time::timeout(
        Duration::from_secs(15),
        recv::<SyncGroupRequest>(&mut b, 3, 11),
    )
    .await??;
    assert_eq!(b_sync.error_code, error_codes::NONE);
    assert_eq!(b_sync.assignment.as_ref(), b"B");
    println!("✅ assignments delivered\n");

    println!("Step 5: after the sweep has run, both members are still in the group...");
    tokio::time::sleep(Duration::from_millis(2_500)).await;
    assert_eq!(
        heartbeat(&mut b, &group, generation, &follower).await?,
        error_codes::NONE,
        "the follower kept its membership"
    );
    assert_eq!(
        heartbeat(&mut a, &group, generation, &leader).await?,
        error_codes::NONE,
        "the leader kept its membership"
    );
    let (e, state, _, members) = describe(&group).await?;
    assert_eq!(
        (e, state.as_str(), members),
        (error_codes::NONE, "Stable", 2)
    );
    println!("✅ Stable with 2 members\n");

    assert_eq!(leave(&mut b, &group, &follower).await?, error_codes::NONE);
    assert_eq!(leave(&mut a, &group, &leader).await?, error_codes::NONE);
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
