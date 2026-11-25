//! Server-side assignment (a leader SyncGroup with empty assignments) on the single DB
//! thread: computed only for the SyncGroup the coordinator takes assignments from, and in
//! time about linear in the subscriptions plus the partitions assigned.

use crate::common::{DynResult, TestResult};
use crate::error_codes;
use crate::setup::TestContext;
use crate::wire::requests::{conn, creatable, create_topics_raw, join_now, join_req, leave, s};
use crate::wire::{recv, send};
use bytes::{BufMut, Bytes, BytesMut};
use kafka_protocol::messages::sync_group_request::{SyncGroupRequest, SyncGroupRequestAssignment};
use kafka_protocol::messages::GroupId;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;

/// Partitions of the subscribed topic.
const PARTITIONS: i32 = 10_000;
/// Repeats of a one-letter padding name in the subscription (about 1 MB of metadata, the
/// JoinGroup limit is 1 MiB).
const PADDING: usize = 330_000;
/// SyncGroups pipelined per step.
const PIPELINED: i32 = 8;

/// Consumer protocol subscription metadata (v0) listing `topics`.
fn subscription(topics: impl Iterator<Item = String>) -> Bytes {
    let topics: Vec<String> = topics.collect();
    let mut buf = BytesMut::new();
    buf.put_i16(0);
    buf.put_i32(i32::try_from(topics.len()).expect("fits"));
    for topic in &topics {
        buf.put_i16(i16::try_from(topic.len()).expect("fits"));
        buf.put_slice(topic.as_bytes());
    }
    buf.put_i32(-1); // no user data
    buf.freeze()
}

/// A SyncGroup from `member` with an empty assignment: it asks the server to assign.
fn empty_sync(group: &str, generation: i32, member: &str) -> SyncGroupRequest {
    let mut a = SyncGroupRequestAssignment::default();
    a.member_id = s(member);
    a.assignment = Bytes::new();
    let mut r = SyncGroupRequest::default();
    r.group_id = GroupId(s(group));
    r.generation_id = generation;
    r.member_id = s(member);
    r.assignments = vec![a];
    r
}

/// Send `count` SyncGroups back to back and wait for all their answers: their error codes,
/// the last assignment and the time from the first send to the last answer.
async fn pipelined(
    c: &mut TcpStream,
    request: &SyncGroupRequest,
    count: i32,
) -> DynResult<(Vec<i16>, Bytes, Duration)> {
    let started = Instant::now();
    for i in 0..count {
        send(c, 3, 100 + i, request).await?;
    }
    let mut codes = Vec::new();
    let mut assignment = Bytes::new();
    for i in 0..count {
        let r = tokio::time::timeout(
            Duration::from_secs(300),
            recv::<SyncGroupRequest>(c, 3, 100 + i),
        )
        .await??;
        codes.push(r.error_code);
        assignment = r.assignment;
    }
    Ok((codes, assignment, started.elapsed()))
}

/// The partitions of `topic` in an encoded MemberAssignment (v0).
fn assigned_partitions(assignment: &[u8], topic: &str) -> usize {
    let mut buf = assignment;
    let take = |buf: &mut &[u8], n: usize| -> Vec<u8> {
        let (head, tail) = buf.split_at(n);
        *buf = tail;
        head.to_vec()
    };
    let i16_of = |b: Vec<u8>| i16::from_be_bytes([b[0], b[1]]);
    let i32_of = |b: Vec<u8>| i32::from_be_bytes([b[0], b[1], b[2], b[3]]);
    let _version = i16_of(take(&mut buf, 2));
    let topics = i32_of(take(&mut buf, 4));
    let mut found = 0;
    for _ in 0..topics {
        let len = usize::try_from(i16_of(take(&mut buf, 2))).expect("non-null name");
        let name = String::from_utf8(take(&mut buf, len)).expect("utf-8");
        let count = usize::try_from(i32_of(take(&mut buf, 4))).expect("count");
        let _ = take(&mut buf, 4 * count);
        if name == topic {
            found += count;
        }
    }
    found
}

/// The review's reproduction: one member on "roundrobin" whose subscription lists a padding
/// name 330 000 times and then a topic of 10 000 partitions. Each server-side assignment
/// compared every partition's topic against the whole subscription (3.3e9 comparisons,
/// seconds on the DB thread, which serves every client), and it ran for every leader
/// SyncGroup with empty assignments, also for a stale generation or a Stable group, where
/// the coordinator ignores the result. Now a stale-generation SyncGroup and a Stable
/// group's are answered without computing, and the one computation is about linear.
pub async fn test_server_side_assignment_is_bounded() -> TestResult {
    println!("=== Test: server-side assignment runs once and about linearly ===\n");
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("assign-big").await;
    let group = ctx.unique_group("assign-big").await;

    println!("Step 1: a topic of {PARTITIONS} partitions...");
    let created = create_topics_raw(vec![creatable(&topic, PARTITIONS, 1)]).await?;
    assert_eq!(created[0].error_code, error_codes::NONE, "{created:?}");

    println!("Step 2: the leader joins with a padded roundrobin subscription...");
    let mut c = conn().await?;
    let mut join = join_req(&group, "", "consumer", &["roundrobin"]);
    join.session_timeout_ms = 60_000;
    join.rebalance_timeout_ms = 60_000;
    join.protocols[0].metadata = subscription(
        std::iter::repeat_n("p".to_string(), PADDING).chain(std::iter::once(topic.clone())),
    );
    let (e, generation, member, _) = join_now(&mut c, &join).await?;
    assert_eq!(e, error_codes::NONE);

    println!("Step 3: {PIPELINED} SyncGroups at a stale generation compute nothing...");
    let stale = empty_sync(&group, generation - 1, &member);
    let (codes, _, took) = pipelined(&mut c, &stale, PIPELINED).await?;
    println!("  answered in {took:?}");
    assert!(
        codes.iter().all(|&e| e == error_codes::ILLEGAL_GENERATION),
        "{codes:?}"
    );
    assert!(
        took < Duration::from_secs(3),
        "stale SyncGroups took {took:?}"
    );

    println!("Step 4: the leader's SyncGroup is assigned at once...");
    let current = empty_sync(&group, generation, &member);
    let (codes, assignment, took) = pipelined(&mut c, &current, 1).await?;
    println!("  answered in {took:?}");
    assert_eq!(codes, [error_codes::NONE]);
    assert_eq!(
        assigned_partitions(&assignment, &topic),
        usize::try_from(PARTITIONS)?
    );
    assert!(
        took < Duration::from_secs(2),
        "the assignment took {took:?}"
    );

    println!("Step 5: {PIPELINED} SyncGroups of the Stable group compute nothing...");
    let (codes, again, took) = pipelined(&mut c, &current, PIPELINED).await?;
    println!("  answered in {took:?}");
    assert!(codes.iter().all(|&e| e == error_codes::NONE), "{codes:?}");
    assert_eq!(again, assignment, "the stored assignment");
    assert!(
        took < Duration::from_secs(3),
        "Stable SyncGroups took {took:?}"
    );

    assert_eq!(leave(&mut c, &group, &member).await?, error_codes::NONE);
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
