//! JoinGroup responses are held until the group's join phase completes, as Kafka does.
//!
//! The worker answers JoinGroup on the client's own sender, possibly long after the
//! request's transaction, which is only visible end to end, so these
//! drive raw wire requests over separate connections: a held JoinGroup blocks its
//! connection's response stream, exactly as it blocks a real client's coordinator
//! connection.

use crate::common::TestResult;
use crate::error_codes;
use crate::setup::TestContext;
use crate::wire::roundtrip;
use crate::wire::{recv, send};
use kafka_protocol::messages::heartbeat_request::HeartbeatRequest;
use kafka_protocol::messages::join_group_request::{JoinGroupRequest, JoinGroupRequestProtocol};
use kafka_protocol::messages::join_group_response::JoinGroupResponse;
use kafka_protocol::messages::leave_group_request::LeaveGroupRequest;
use kafka_protocol::messages::sync_group_request::{SyncGroupRequest, SyncGroupRequestAssignment};
use kafka_protocol::messages::GroupId;
use kafka_protocol::protocol::StrBytes;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;

const BROKER: &str = "localhost:9092";
const JOIN_VERSION: i16 = 5;

fn join_request(group: &str, member_id: &str, session_timeout_ms: i32) -> JoinGroupRequest {
    let mut protocol = JoinGroupRequestProtocol::default();
    protocol.name = StrBytes::from_static_str("range");
    protocol.metadata = bytes::Bytes::from_static(b"sub");
    let mut req = JoinGroupRequest::default();
    req.group_id = GroupId(StrBytes::from_string(group.to_string()));
    req.session_timeout_ms = session_timeout_ms;
    req.rebalance_timeout_ms = 60_000;
    req.member_id = StrBytes::from_string(member_id.to_string());
    req.protocol_type = StrBytes::from_static_str("consumer");
    req.protocols = vec![protocol];
    req
}

async fn send_join(
    stream: &mut TcpStream,
    group: &str,
    member_id: &str,
    session_timeout_ms: i32,
    corr: i32,
) -> Result<(), Box<dyn std::error::Error>> {
    send(
        stream,
        JOIN_VERSION,
        corr,
        &join_request(group, member_id, session_timeout_ms),
    )
    .await
}

async fn recv_join(
    stream: &mut TcpStream,
    corr: i32,
) -> Result<JoinGroupResponse, Box<dyn std::error::Error>> {
    let response = recv::<JoinGroupRequest>(stream, JOIN_VERSION, corr).await?;
    if response.error_code != 0 {
        return Err(format!("JoinGroup error {}", response.error_code).into());
    }
    Ok(response)
}

/// Join and wait for the response (for a join that is expected to complete at once).
async fn join_now(
    stream: &mut TcpStream,
    group: &str,
    member_id: &str,
    corr: i32,
) -> Result<JoinGroupResponse, Box<dyn std::error::Error>> {
    send_join(stream, group, member_id, 30_000, corr).await?;
    tokio::time::timeout(Duration::from_secs(10), recv_join(stream, corr))
        .await
        .map_err(|_| "JoinGroup expected to complete at once was held")?
}

/// Whether nothing at all arrives on `stream` within `wait` (cancel-safe: peeks).
async fn nothing_arrives(stream: &TcpStream, wait: Duration) -> bool {
    let mut probe = [0u8; 1];
    tokio::time::timeout(wait, stream.peek(&mut probe))
        .await
        .is_err()
}

async fn heartbeat(
    stream: &mut TcpStream,
    group: &str,
    member_id: &str,
    generation: i32,
    corr: i32,
) -> Result<i16, Box<dyn std::error::Error>> {
    let mut req = HeartbeatRequest::default();
    req.group_id = GroupId(StrBytes::from_string(group.to_string()));
    req.generation_id = generation;
    req.member_id = StrBytes::from_string(member_id.to_string());
    Ok(roundtrip(stream, 3, corr, &req).await?.error_code)
}

fn sync_request(
    group: &str,
    member_id: &str,
    generation: i32,
    assignments: &[(&str, &[u8])],
) -> SyncGroupRequest {
    let mut req = SyncGroupRequest::default();
    req.group_id = GroupId(StrBytes::from_string(group.to_string()));
    req.generation_id = generation;
    req.member_id = StrBytes::from_string(member_id.to_string());
    req.assignments = assignments
        .iter()
        .map(|(m, a)| {
            let mut x = SyncGroupRequestAssignment::default();
            x.member_id = StrBytes::from_string(m.to_string());
            x.assignment = bytes::Bytes::copy_from_slice(a);
            x
        })
        .collect();
    req
}

async fn sync(
    stream: &mut TcpStream,
    group: &str,
    member_id: &str,
    generation: i32,
    assignments: &[(&str, &[u8])],
    corr: i32,
) -> Result<(i16, Vec<u8>), Box<dyn std::error::Error>> {
    let req = sync_request(group, member_id, generation, assignments);
    let r = roundtrip(stream, 3, corr, &req).await?;
    Ok((r.error_code, r.assignment.to_vec()))
}

/// Heartbeat until the group reports REBALANCE_IN_PROGRESS, or give up after 5 s and
/// return the last code. Heartbeat travels on the worker's liveness lane and can
/// overtake a JoinGroup still queued on the main lane.
async fn heartbeat_until_rebalance(
    stream: &mut TcpStream,
    group: &str,
    member_id: &str,
    generation: i32,
    first_corr: i32,
) -> Result<i16, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut corr = first_corr;
    loop {
        let hb = heartbeat(stream, group, member_id, generation, corr).await?;
        if hb == error_codes::REBALANCE_IN_PROGRESS || Instant::now() > deadline {
            return Ok(hb);
        }
        corr += 1;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn leave(
    stream: &mut TcpStream,
    group: &str,
    member_id: &str,
    corr: i32,
) -> Result<i16, Box<dyn std::error::Error>> {
    let mut req = LeaveGroupRequest::default();
    req.group_id = GroupId(StrBytes::from_string(group.to_string()));
    req.member_id = StrBytes::from_string(member_id.to_string());
    Ok(roundtrip(stream, 1, corr, &req).await?.error_code)
}

fn member_ids(response: &JoinGroupResponse) -> Vec<String> {
    let mut ids: Vec<String> = response
        .members
        .iter()
        .map(|m| m.member_id.to_string())
        .collect();
    ids.sort();
    ids
}

/// Two members joining a group: the second member's JoinGroup is held (nothing
/// arrives on its connection) until the first member, told by Heartbeat that a
/// rebalance is in progress, rejoins. Both then receive the SAME generation, one more
/// than the first member's, and the leader's member list contains both. The new
/// generation is then usable: SyncGroup distributes the leader's assignments and both
/// members heartbeat successfully.
pub async fn test_join_group_held_until_all_members_rejoin() -> TestResult {
    println!("=== Test: JoinGroup Held Until All Members Rejoin ===\n");
    let ctx = TestContext::new().await?;
    let group = ctx.unique_group("coord-hold").await;
    let mut conn_a = TcpStream::connect(BROKER).await?;
    let mut conn_b = TcpStream::connect(BROKER).await?;

    println!("Step 1: first member joins an empty group (completes at once)...");
    let a = join_now(&mut conn_a, &group, "", 1).await?;
    let a_id = a.member_id.to_string();
    assert_eq!(a.leader.as_str(), a_id.as_str(), "first member leads");
    assert_eq!(a.members.len(), 1);
    let gen1 = a.generation_id;

    println!("Step 2: second member joins; its response must be held...");
    send_join(&mut conn_b, &group, "", 30_000, 2).await?;
    assert!(
        nothing_arrives(&conn_b, Duration::from_millis(1500)).await,
        "the second JoinGroup must wait for the first member to rejoin"
    );

    println!("Step 3: first member's heartbeat reports the rebalance...");
    // Heartbeat travels on the worker's liveness lane and can overtake the JoinGroup
    // still queued on the main lane, so poll briefly for the rebalance.
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut corr = 100;
    let hb = loop {
        let hb = heartbeat(&mut conn_a, &group, &a_id, gen1, corr).await?;
        if hb == error_codes::REBALANCE_IN_PROGRESS || Instant::now() > deadline {
            break hb;
        }
        corr += 1;
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    assert_eq!(hb, error_codes::REBALANCE_IN_PROGRESS);

    println!("Step 4: first member rejoins; both joins complete together...");
    let a2 = join_now(&mut conn_a, &group, &a_id, 4).await?;
    let b = tokio::time::timeout(Duration::from_secs(5), recv_join(&mut conn_b, 2))
        .await
        .map_err(|_| "the held JoinGroup was not answered when the phase completed")??;
    let b_id = b.member_id.to_string();
    println!(
        "   generations: a={} b={} (was {}); leader member list: {:?}",
        a2.generation_id,
        b.generation_id,
        gen1,
        member_ids(&a2)
    );
    assert_eq!(
        a2.generation_id, b.generation_id,
        "same generation for both"
    );
    assert_eq!(a2.generation_id, gen1 + 1);
    assert_eq!(a2.leader.as_str(), a_id.as_str());
    assert_eq!(b.leader.as_str(), a_id.as_str());
    let mut expected = vec![a_id.clone(), b_id.clone()];
    expected.sort();
    assert_eq!(
        member_ids(&a2),
        expected,
        "leader's list holds both members"
    );
    assert!(b.members.is_empty(), "a follower gets no member list");

    println!("Step 5: the new generation syncs and heartbeats...");
    let gen2 = a2.generation_id;
    let (code, own) = sync(
        &mut conn_a,
        &group,
        &a_id,
        gen2,
        &[(&a_id, b"assign-a"), (&b_id, b"assign-b")],
        5,
    )
    .await?;
    assert_eq!((code, own.as_slice()), (0, b"assign-a".as_slice()));
    let (code, got) = sync(&mut conn_b, &group, &b_id, gen2, &[], 6).await?;
    assert_eq!((code, got.as_slice()), (0, b"assign-b".as_slice()));
    assert_eq!(heartbeat(&mut conn_a, &group, &a_id, gen2, 7).await?, 0);
    assert_eq!(heartbeat(&mut conn_b, &group, &b_id, gen2, 8).await?, 0);

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// A held JoinGroup is answered by the coordinator's periodic sweep when the member
/// that has not rejoined stops heartbeating: no further request arrives, yet once the
/// first member's 6 s session timeout passes the sweep evicts it and completes the
/// join phase. The waiting member gets its response no earlier than ~4 s and within
/// 15 s, as the sole member and leader of the next generation; the evicted member's
/// heartbeat then gets UNKNOWN_MEMBER_ID.
pub async fn test_held_join_completed_by_session_timeout_sweep() -> TestResult {
    println!("=== Test: Held JoinGroup Completed By The Timeout Sweep ===\n");
    let ctx = TestContext::new().await?;
    let group = ctx.unique_group("coord-sweep").await;
    let mut conn_a = TcpStream::connect(BROKER).await?;
    let mut conn_b = TcpStream::connect(BROKER).await?;

    println!("Step 1: first member joins with the minimum 6s session timeout...");
    send_join(&mut conn_a, &group, "", 6_000, 1).await?;
    let a = tokio::time::timeout(Duration::from_secs(10), recv_join(&mut conn_a, 1))
        .await
        .map_err(|_| "first JoinGroup was held")??;
    let a_id = a.member_id.to_string();

    println!("Step 2: second member joins; the first never heartbeats or rejoins...");
    let sent = Instant::now();
    send_join(&mut conn_b, &group, "", 30_000, 2).await?;
    let b = tokio::time::timeout(Duration::from_secs(15), recv_join(&mut conn_b, 2))
        .await
        .map_err(|_| "the sweep did not complete the held JoinGroup within 15s")??;
    let waited = sent.elapsed();
    println!("   held for {waited:?}");
    assert!(
        waited >= Duration::from_secs(4),
        "answered after {waited:?}, before the first member's session could expire"
    );
    assert_eq!(b.generation_id, a.generation_id + 1);
    assert_eq!(b.leader, b.member_id, "the remaining member leads");
    assert_eq!(member_ids(&b), vec![b.member_id.to_string()]);

    let hb = heartbeat(&mut conn_a, &group, &a_id, a.generation_id, 3).await?;
    assert_eq!(
        hb,
        error_codes::UNKNOWN_MEMBER_ID,
        "the evicted member is fenced"
    );

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// A held JoinGroup is answered promptly (within 3 s, well under any timeout) when
/// the member that has not rejoined leaves the group: the waiting member becomes the
/// sole member and leader of the next generation.
pub async fn test_held_join_completed_by_leave_group() -> TestResult {
    println!("=== Test: Held JoinGroup Completed By LeaveGroup ===\n");
    let ctx = TestContext::new().await?;
    let group = ctx.unique_group("coord-leave").await;
    let mut conn_a = TcpStream::connect(BROKER).await?;
    let mut conn_b = TcpStream::connect(BROKER).await?;

    let a = join_now(&mut conn_a, &group, "", 1).await?;
    let a_id = a.member_id.to_string();
    send_join(&mut conn_b, &group, "", 30_000, 2).await?;
    assert!(
        nothing_arrives(&conn_b, Duration::from_millis(1000)).await,
        "the second JoinGroup must be held"
    );

    println!("Step 1: the first member leaves...");
    assert_eq!(leave(&mut conn_a, &group, &a_id, 3).await?, 0);
    let b = tokio::time::timeout(Duration::from_secs(3), recv_join(&mut conn_b, 2))
        .await
        .map_err(|_| "LeaveGroup did not release the held JoinGroup")??;
    assert_eq!(b.generation_id, a.generation_id + 1);
    assert_eq!(b.leader, b.member_id);
    assert_eq!(member_ids(&b), vec![b.member_id.to_string()]);

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// A follower's SyncGroup that arrives before the leader's is held, as Kafka does:
/// after a two-member join completes, the follower syncs first and gets no response
/// (nothing arrives on its connection for 1.5 s); the leader then syncs, and the
/// follower's response arrives within 5 s carrying exactly the assignment the leader
/// gave it. The generation does not move: both members then heartbeat NONE at the
/// generation of the join. (Answering the early follower REBALANCE_IN_PROGRESS made
/// it rejoin, which with Java clients restarted the rebalance every round.)
pub async fn test_follower_sync_held_until_leader_syncs() -> TestResult {
    println!("=== Test: Follower SyncGroup Held Until The Leader Syncs ===\n");
    let ctx = TestContext::new().await?;
    let group = ctx.unique_group("coord-sync-hold").await;
    let mut conn_a = TcpStream::connect(BROKER).await?;
    let mut conn_b = TcpStream::connect(BROKER).await?;

    println!("Step 1: form a two-member generation...");
    let a = join_now(&mut conn_a, &group, "", 1).await?;
    let a_id = a.member_id.to_string();
    send_join(&mut conn_b, &group, "", 30_000, 2).await?;
    let hb = heartbeat_until_rebalance(&mut conn_a, &group, &a_id, a.generation_id, 100).await?;
    assert_eq!(hb, error_codes::REBALANCE_IN_PROGRESS);
    let a2 = join_now(&mut conn_a, &group, &a_id, 3).await?;
    let b = tokio::time::timeout(Duration::from_secs(5), recv_join(&mut conn_b, 2))
        .await
        .map_err(|_| "the held JoinGroup was not answered when the phase completed")??;
    let b_id = b.member_id.to_string();
    let gen = a2.generation_id;
    assert_eq!(b.generation_id, gen);
    assert_eq!(a2.leader.as_str(), a_id.as_str());

    println!("Step 2: the follower syncs first; its response must be held...");
    send(&mut conn_b, 3, 4, &sync_request(&group, &b_id, gen, &[])).await?;
    assert!(
        nothing_arrives(&conn_b, Duration::from_millis(1500)).await,
        "the follower's SyncGroup must wait for the leader's"
    );

    println!("Step 3: the leader syncs; the follower gets its assignment...");
    let (code, own) = sync(
        &mut conn_a,
        &group,
        &a_id,
        gen,
        &[(&a_id, b"assign-a"), (&b_id, b"assign-b")],
        5,
    )
    .await?;
    assert_eq!((code, own.as_slice()), (0, b"assign-a".as_slice()));
    let follower = tokio::time::timeout(
        Duration::from_secs(5),
        recv::<SyncGroupRequest>(&mut conn_b, 3, 4),
    )
    .await
    .map_err(|_| "the held SyncGroup was not answered when the leader synced")??;
    println!(
        "   follower sync: code {} assignment {:?}",
        follower.error_code,
        String::from_utf8_lossy(&follower.assignment)
    );
    assert_eq!(follower.error_code, 0);
    assert_eq!(follower.assignment.as_ref(), b"assign-b");

    println!("Step 4: the generation did not move...");
    assert_eq!(heartbeat(&mut conn_a, &group, &a_id, gen, 6).await?, 0);
    assert_eq!(heartbeat(&mut conn_b, &group, &b_id, gen, 7).await?, 0);

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
