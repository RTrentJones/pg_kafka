//! Server-side assignment (a leader SyncGroup with empty assignments) on the single DB
//! thread: computed only for the SyncGroup the coordinator takes assignments from, and in
//! time about linear in the subscriptions plus the partitions assigned, also for a sticky
//! rebalance whose donor holds many differently subscribed topics.

use crate::common::{DynResult, TestResult};
use crate::error_codes;
use crate::setup::TestContext;
use crate::wire::requests::{
    conn, creatable, create_topics_raw, describe, join_now, join_req, leave, s,
};
use crate::wire::{recv, send};
use bytes::{BufMut, Bytes, BytesMut};
use kafka_protocol::messages::delete_topics_request::DeleteTopicsRequest;
use kafka_protocol::messages::join_group_request::JoinGroupRequest;
use kafka_protocol::messages::leave_group_request::{LeaveGroupRequest, MemberIdentity};
use kafka_protocol::messages::sync_group_request::{SyncGroupRequest, SyncGroupRequestAssignment};
use kafka_protocol::messages::{GroupId, TopicName};
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

/// Topics (and members) of the nested-subscription group.
const NESTED_TOPICS: usize = 999;
/// Partitions of each of its topics.
const NESTED_PARTITIONS: i32 = 8;
/// JoinGroups pipelined per connection: the listener reads no further request while 8
/// requests other than Fetch and Heartbeat wait for their responses, and a JoinGroup waits
/// until the join phase completes. 143 connections stay within `max_connections_per_ip`.
const JOINS_PER_CONNECTION: usize = 7;

/// A JoinGroup of `group` on "sticky" subscribing to `topics`.
fn sticky_join(group: &str, member: &str, topics: &[String]) -> JoinGroupRequest {
    let mut join = join_req(group, member, "consumer", &["sticky"]);
    join.session_timeout_ms = 120_000;
    join.rebalance_timeout_ms = 120_000;
    join.protocols[0].metadata = subscription(topics.iter().cloned());
    join
}

/// The review's reproduction of a sticky rebalance that held the DB thread for minutes (at
/// more partitions per topic), within the default `group_max_size`: 999 topics of 8
/// partitions, all owned by a lone first member subscribed to all of them; then member i (for
/// i in 1..999) joins subscribed to the topics from the i-th on, and the leader asks the
/// server to assign. Every topic is subscribed to by a different set of members, and the
/// member that gives up partitions holds all of them: each of its 7,984 moves compared every
/// topic's subscribers (seconds in all, about cubic in the members). Now a move costs a few
/// ordered-set operations, and every step counts against a budget.
pub async fn test_sticky_rebalance_of_nested_subscriptions_is_bounded() -> TestResult {
    println!("=== Test: sticky rebalance of nested subscriptions is bounded ===\n");
    let ctx = TestContext::new().await?;
    let group = ctx.unique_group("sticky-nested").await;
    let mut topics = Vec::with_capacity(NESTED_TOPICS);
    for _ in 0..NESTED_TOPICS {
        topics.push(ctx.unique_topic("nested").await);
    }

    println!("Step 1: {NESTED_TOPICS} topics of {NESTED_PARTITIONS} partitions...");
    for chunk in topics.chunks(100) {
        let created = create_topics_raw(
            chunk
                .iter()
                .map(|t| creatable(t, NESTED_PARTITIONS, 1))
                .collect(),
        )
        .await?;
        assert!(
            created.iter().all(|t| t.error_code == error_codes::NONE),
            "{created:?}"
        );
    }

    println!("Step 2: the first member joins alone and is assigned everything...");
    let mut first = conn().await?;
    let (e, generation, owner, _) = join_now(&mut first, &sticky_join(&group, "", &topics)).await?;
    assert_eq!(e, error_codes::NONE);
    let (codes, assignment, _) =
        pipelined(&mut first, &empty_sync(&group, generation, &owner), 1).await?;
    assert_eq!(codes, [error_codes::NONE]);
    let owned: usize = topics
        .iter()
        .map(|t| assigned_partitions(&assignment, t))
        .sum();
    assert_eq!(owned, NESTED_TOPICS * usize::try_from(NESTED_PARTITIONS)?);

    println!(
        "Step 3: {} more members join, member i subscribed to the topics from the i-th on...",
        NESTED_TOPICS - 1
    );
    let mut joiners = Vec::new();
    let others: Vec<usize> = (1..NESTED_TOPICS).collect();
    for chunk in others.chunks(JOINS_PER_CONNECTION) {
        let mut c = conn().await?;
        for (n, &i) in chunk.iter().enumerate() {
            send(
                &mut c,
                5,
                i32::try_from(n)?,
                &sticky_join(&group, "", &topics[i..]),
            )
            .await?;
        }
        joiners.push((c, chunk.len()));
    }
    // Every joiner is a member (its JoinGroup is held) before the first member rejoins, so
    // the rejoin completes the join phase with all of them.
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let (e, _, _, members) = describe(&group).await?;
        assert_eq!(e, error_codes::NONE);
        if members == NESTED_TOPICS {
            break;
        }
        assert!(Instant::now() < deadline, "{members} members after 60 s");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let rejoin = tokio::time::timeout(
        Duration::from_secs(60),
        crate::wire::roundtrip(&mut first, 5, 50, &sticky_join(&group, &owner, &topics)),
    )
    .await??;
    assert_eq!(rejoin.error_code, error_codes::NONE);
    let generation = rejoin.generation_id;
    let mut members = vec![owner.clone()];
    let mut leader = rejoin.leader.to_string();
    for (c, count) in &mut joiners {
        for n in 0..*count {
            let r = tokio::time::timeout(
                Duration::from_secs(60),
                recv::<JoinGroupRequest>(c, 5, i32::try_from(n)?),
            )
            .await??;
            assert_eq!(r.error_code, error_codes::NONE);
            assert_eq!(r.generation_id, generation);
            leader = r.leader.to_string();
            members.push(r.member_id.to_string());
        }
    }
    println!("  generation {generation}, leader {leader}");

    println!("Step 4: the leader's empty SyncGroup is assigned within the time bound...");
    // A SyncGroup names its member, so it may travel on any connection.
    let mut leader_conn = first;
    let (codes, assignment, took) = pipelined(
        &mut leader_conn,
        &empty_sync(&group, generation, &leader),
        1,
    )
    .await?;
    println!("  answered in {took:?}");
    assert_eq!(codes, [error_codes::NONE]);
    let kept: usize = topics
        .iter()
        .map(|t| assigned_partitions(&assignment, t))
        .sum();
    // 7,992 partitions over 999 members (how many the leader gets depends on the random
    // member ids, which break the assignor's ties).
    println!("  the leader holds {kept} partitions");
    assert!(kept > 0, "the leader holds nothing");
    assert!(
        took < Duration::from_secs(2),
        "the sticky assignment took {took:?}"
    );

    println!("Step 5: every member leaves...");
    let mut leave_all = LeaveGroupRequest::default();
    leave_all.group_id = GroupId(s(&group));
    leave_all.members = members
        .iter()
        .map(|m| {
            let mut member = MemberIdentity::default();
            member.member_id = s(m);
            member
        })
        .collect();
    let left = crate::wire::roundtrip(&mut leader_conn, 3, 60, &leave_all).await?;
    assert_eq!(left.error_code, error_codes::NONE);

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// Null-name entries in the leader's explicit assignment: 2 bytes each. With the
/// [`REFUSED_TOPICS`] topics' partitions they take a server-side assignment just over the
/// 2,000,000 entries it may take; without the partitions, within it.
const PADDED_ENTRIES: usize = 1_960_000;

/// Topics the leader subscribes to, of [`REFUSED_PARTITIONS`] partitions each.
const REFUSED_TOPICS: usize = 5;

/// The default `pg_kafka.max_partitions_per_topic`.
const REFUSED_PARTITIONS: i32 = 10_000;

/// The leader's SyncGroup at `generation` assigning itself `assignment`.
fn explicit_sync(
    group: &str,
    generation: i32,
    member: &str,
    assignment: Bytes,
) -> SyncGroupRequest {
    let mut r = empty_sync(group, generation, member);
    r.assignments[0].assignment = assignment;
    r
}

/// A leader on sticky that stores 1,960,000 empty previous-assignment entries (2 bytes
/// each, so within `max_group_memory`) and subscribes to five topics of 10,000 partitions
/// has its next server-side assignment refused before anything is parsed (the entries and
/// partitions together are over the work limit), and its retries in that generation get the
/// refusal without computing again: after the topics are deleted, a recomputation would
/// succeed (the entries alone are within the limit, and there is nothing to assign), yet a
/// retry is still refused. Without the refusal being remembered, pipelined retries in a sync
/// phase that lasts as long as the client's rebalance timeout each read the subscribed
/// topics and walked the stored entries' lengths (about two million steps) on the DB thread.
pub async fn test_refused_sticky_assignment_is_not_recomputed() -> TestResult {
    println!("=== Test: a refused sticky assignment is not recomputed ===\n");
    let ctx = TestContext::new().await?;
    let group = ctx.unique_group("refused").await;
    let mut topics = Vec::new();
    for i in 0..REFUSED_TOPICS {
        topics.push(ctx.unique_topic(&format!("refused-{i}")).await);
    }
    let created = create_topics_raw(
        topics
            .iter()
            .map(|t| creatable(t, REFUSED_PARTITIONS, 1))
            .collect(),
    )
    .await?;
    assert!(
        created.iter().all(|t| t.error_code == error_codes::NONE),
        "{created:?}"
    );

    println!(
        "Step 1: the leader joins on sticky and assigns itself {PADDED_ENTRIES} empty entries..."
    );
    let mut c = conn().await?;
    let mut join = join_req(&group, "", "consumer", &["sticky"]);
    join.session_timeout_ms = 60_000;
    join.rebalance_timeout_ms = 60_000;
    join.protocols[0].metadata = subscription(topics.iter().cloned());
    let (e, generation, member, _) = join_now(&mut c, &join).await?;
    assert_eq!(e, error_codes::NONE);
    let mut padded = BytesMut::new();
    padded.put_i16(0);
    padded.put_i32(i32::try_from(PADDED_ENTRIES)?);
    for _ in 0..PADDED_ENTRIES {
        padded.put_i16(-1); // a null topic name
    }
    padded.put_i32(-1); // no user data
    let explicit = explicit_sync(&group, generation, &member, padded.freeze());
    let (codes, _, _) = pipelined(&mut c, &explicit, 1).await?;
    assert_eq!(codes, [error_codes::NONE]);

    println!("Step 2: the leader rejoins; its empty SyncGroup is refused...");
    join.member_id = s(&member);
    let (e, generation, _, _) = join_now(&mut c, &join).await?;
    assert_eq!(e, error_codes::NONE);
    let current = empty_sync(&group, generation, &member);
    let (codes, _, _) = pipelined(&mut c, &current, 1).await?;
    assert_eq!(codes, [error_codes::POLICY_VIOLATION]);

    println!("Step 3: the topics are deleted; {PIPELINED} retries are still refused...");
    let mut delete = DeleteTopicsRequest::default();
    delete.timeout_ms = 30_000;
    delete.topic_names = topics.iter().map(|t| TopicName(s(t))).collect();
    let deleted = crate::wire::roundtrip(&mut c, 1, 70, &delete).await?;
    assert!(
        deleted
            .responses
            .iter()
            .all(|r| r.error_code == error_codes::NONE),
        "{:?}",
        deleted.responses
    );
    let (codes, _, took) = pipelined(&mut c, &current, PIPELINED).await?;
    println!("  answered in {took:?}");
    assert!(
        codes.iter().all(|&e| e == error_codes::POLICY_VIOLATION),
        "a retry computed the assignment again (it succeeds once the topics are gone): {codes:?}"
    );

    assert_eq!(leave(&mut c, &group, &member).await?, error_codes::NONE);
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
