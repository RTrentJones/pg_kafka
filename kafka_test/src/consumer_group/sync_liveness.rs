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

/// As in Kafka 3.0+ (checked against a 3.7 broker): a member of a new generation that sends
/// no SyncGroup within the rebalance timeout of the join's completion is removed, though it
/// heartbeats and the leader's SyncGroup already made the group Stable; the leader is then
/// told to rejoin. pg_kafka removed only a leader that did not sync, so a follower that
/// never synced held its (unassigned) place, and blocked nothing but stayed forever.
pub async fn test_member_that_never_syncs_is_removed() -> TestResult {
    println!("=== Test: a member that never sends SyncGroup is removed ===\n");
    let ctx = TestContext::new().await?;
    let group = ctx.unique_group("never-sync").await;
    let join = |member: &str| {
        let mut r = member_join(&group, member);
        r.rebalance_timeout_ms = 3_000;
        r
    };

    println!("Step 1: A forms the group, B joins, A rejoins...");
    let mut a = conn().await?;
    let (e, generation, leader, _) = join_now(&mut a, &join("")).await?;
    assert_eq!(e, error_codes::NONE);
    let synced = roundtrip(
        &mut a,
        3,
        20,
        &sync_request(&group, generation, &leader, &[]),
    )
    .await?;
    assert_eq!(synced.error_code, error_codes::NONE);
    let mut b = conn().await?;
    send(&mut b, 5, 10, &join("")).await?;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let (e, generation, _, _) = join_now(&mut a, &join(&leader)).await?;
    assert_eq!(e, error_codes::NONE);
    let b_join = tokio::time::timeout(
        Duration::from_secs(15),
        recv::<JoinGroupRequest>(&mut b, 5, 10),
    )
    .await??;
    assert_eq!(b_join.error_code, error_codes::NONE);
    let follower = b_join.member_id.to_string();

    println!("Step 2: A syncs; B heartbeats but never syncs...");
    let a_sync = roundtrip(
        &mut a,
        3,
        21,
        &sync_request(&group, generation, &leader, &[(leader.as_str(), b"A")]),
    )
    .await?;
    assert_eq!(a_sync.error_code, error_codes::NONE);
    let mut b_answers = Vec::new();
    let mut a_answers = Vec::new();
    for _ in 0..10 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        a_answers.push(heartbeat(&mut a, &group, generation, &leader).await?);
        b_answers.push(heartbeat(&mut b, &group, generation, &follower).await?);
        if b_answers.last() == Some(&error_codes::UNKNOWN_MEMBER_ID) {
            break;
        }
    }
    println!("  A: {a_answers:?}\n  B: {b_answers:?}");
    assert_eq!(
        b_answers.last(),
        Some(&error_codes::UNKNOWN_MEMBER_ID),
        "B is removed once the rebalance timeout has passed"
    );
    assert!(
        b_answers.len() >= 2,
        "not before the rebalance timeout: {b_answers:?}"
    );
    // A's heartbeat of the last round went out before B's, so it may have been answered
    // before B was removed; the next one must be told to rejoin.
    let a_after = heartbeat(&mut a, &group, generation, &leader).await?;
    println!("  A after B's removal: {a_after}");
    assert_eq!(
        a_after,
        error_codes::REBALANCE_IN_PROGRESS,
        "the leader is told to rejoin once B is removed"
    );
    assert_eq!(leave(&mut a, &group, &leader).await?, error_codes::NONE);
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
