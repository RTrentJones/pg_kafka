//! Coordinator state-machine tests: joins, syncs, timeouts and limits.

use super::group::{DEFAULT_REBALANCE_TIMEOUT_MS, EMPTY_GROUP_TTL_SECS};
use super::*;
use std::time::Duration;

// ----- helpers -----

type Slot = Arc<Mutex<Option<JoinOutcome>>>;

/// A responder that records the outcome, and the slot it records into.
fn recorder() -> (JoinResponder, Slot) {
    let slot: Slot = Arc::new(Mutex::new(None));
    let writer = slot.clone();
    let responder: JoinResponder = Box::new(move |outcome| {
        let mut guard = writer.lock().unwrap();
        assert!(guard.is_none(), "a responder must be called exactly once");
        *guard = Some(outcome);
    });
    (responder, slot)
}

type SyncSlot = Arc<Mutex<Option<SyncOutcome>>>;

/// A SyncGroup responder that records the outcome, and its slot.
fn sync_recorder() -> (SyncResponder, SyncSlot) {
    let slot: SyncSlot = Arc::new(Mutex::new(None));
    let writer = slot.clone();
    let responder: SyncResponder = Box::new(move |outcome| {
        let mut guard = writer.lock().unwrap();
        assert!(guard.is_none(), "a responder must be called exactly once");
        *guard = Some(outcome);
    });
    (responder, slot)
}

/// Test helpers over the responder-based SyncGroup API.
trait SyncExt {
    /// Send a SyncGroup and return the slot its answer lands in (maybe parked).
    fn sync_park(
        &self,
        group: String,
        member: String,
        generation: i32,
        assignments: Vec<(String, Vec<u8>)>,
    ) -> SyncSlot;
    /// Send a SyncGroup that must be answered at once, and return the answer.
    fn sync_now(
        &self,
        group: String,
        member: String,
        generation: i32,
        assignments: Vec<(String, Vec<u8>)>,
    ) -> SyncOutcome;
}

impl SyncExt for GroupCoordinator {
    fn sync_park(
        &self,
        group: String,
        member: String,
        generation: i32,
        assignments: Vec<(String, Vec<u8>)>,
    ) -> SyncSlot {
        let (responder, slot) = sync_recorder();
        self.sync_group(&group, &member, generation, assignments, responder);
        slot
    }

    fn sync_now(
        &self,
        group: String,
        member: String,
        generation: i32,
        assignments: Vec<(String, Vec<u8>)>,
    ) -> SyncOutcome {
        self.sync_park(group, member, generation, assignments)
            .lock()
            .unwrap()
            .take()
            .expect("SyncGroup expected to be answered at once was parked")
    }
}

fn sync_pending(slot: &SyncSlot) -> bool {
    slot.lock().unwrap().is_none()
}

/// The outcome recorded in a SyncGroup slot (panics if still parked).
fn synced(slot: &SyncSlot) -> SyncOutcome {
    slot.lock().unwrap().take().expect("SyncGroup still parked")
}

fn params(group: &str, member: Option<&str>, protocols: &[(&str, &[u8])]) -> JoinGroupParams {
    JoinGroupParams {
        group_id: group.to_string(),
        member_id: member.map(str::to_string),
        client_id: "client".to_string(),
        client_host: "localhost".to_string(),
        session_timeout_ms: 30_000,
        rebalance_timeout_ms: 60_000,
        protocol_type: "consumer".to_string(),
        protocols: protocols
            .iter()
            .map(|(n, m)| (n.to_string(), m.to_vec()))
            .collect(),
        group_instance_id: None,
    }
}

fn join_with(c: &GroupCoordinator, p: &JoinGroupParams) -> Slot {
    let (responder, slot) = recorder();
    c.join_group(p, responder);
    slot
}

/// Join with default parameters and the "range" protocol.
fn join(c: &GroupCoordinator, group: &str, member: Option<&str>) -> Slot {
    join_with(c, &params(group, member, &[("range", b"sub")]))
}

fn is_pending(slot: &Slot) -> bool {
    slot.lock().unwrap().is_none()
}

/// The successful outcome recorded in `slot` (panics if pending or an error).
fn joined(slot: &Slot) -> JoinResult {
    match slot.lock().unwrap().take() {
        Some(Ok(r)) => r,
        Some(Err(e)) => panic!("join failed: {e}"),
        None => panic!("join still pending"),
    }
}

/// The error code recorded in `slot` (panics if pending or a success).
fn join_error_code(slot: &Slot) -> i16 {
    match slot.lock().unwrap().take() {
        Some(Err(e)) => e.to_kafka_error_code(),
        Some(Ok(r)) => panic!("expected an error, got {r:?}"),
        None => panic!("join still pending"),
    }
}

fn state(c: &GroupCoordinator, group: &str) -> GroupState {
    c.get_group_state(group).unwrap().state
}

fn generation(c: &GroupCoordinator, group: &str) -> i32 {
    c.get_group_state(group).unwrap().generation_id
}

fn ago(d: Duration) -> Instant {
    Instant::now()
        .checked_sub(d)
        .expect("monotonic clock far enough from boot")
}

/// Pretend `member` last heartbeated `d` ago.
fn age_member(c: &GroupCoordinator, group: &str, member: &str, d: Duration) {
    let mut groups = c.groups.write().unwrap();
    groups
        .get_mut(group)
        .unwrap()
        .members
        .get_mut(member)
        .unwrap()
        .last_heartbeat = ago(d);
}

/// Pretend the group's current rebalance phase started `d` ago.
fn age_rebalance(c: &GroupCoordinator, group: &str, d: Duration) {
    let mut groups = c.groups.write().unwrap();
    groups.get_mut(group).unwrap().rebalance_started_at = Some(ago(d));
}

/// A single-member group, Stable at generation 1. Returns the member id.
fn stable_single(c: &GroupCoordinator, group: &str) -> String {
    let a = joined(&join(c, group, None));
    assert_eq!(a.generation_id, 1);
    c.sync_now(
        group.to_string(),
        a.member_id.clone(),
        1,
        vec![(a.member_id.clone(), b"A".to_vec())],
    )
    .unwrap();
    assert_eq!(state(c, group), GroupState::Stable);
    a.member_id
}

/// A Stable two-member group {leader, follower} with assignments "A"/"B".
/// Returns (leader, follower, generation).
fn stable_pair(c: &GroupCoordinator, group: &str) -> (String, String, i32) {
    let leader = stable_single(c, group);
    let follower_slot = join(c, group, None);
    assert!(is_pending(&follower_slot));
    let leader_slot = join(c, group, Some(&leader));
    let l = joined(&leader_slot);
    let f = joined(&follower_slot);
    assert_eq!(l.generation_id, f.generation_id);
    c.sync_now(
        group.to_string(),
        leader.clone(),
        l.generation_id,
        vec![
            (leader.clone(), b"A".to_vec()),
            (f.member_id.clone(), b"B".to_vec()),
        ],
    )
    .unwrap();
    assert_eq!(state(c, group), GroupState::Stable);
    (leader, f.member_id, l.generation_id)
}

// ----- basics -----

#[test]
fn test_generate_member_id() {
    let member_id1 = generate_member_id("test-client");
    let member_id2 = generate_member_id("test-client");
    assert_ne!(member_id1, member_id2);
    assert!(member_id1.starts_with("test-client-"));
}

#[test]
fn test_consumer_group_new() {
    let group = ConsumerGroup::new("test-group".to_string());
    assert_eq!(group.group_id, "test-group");
    assert_eq!(group.generation_id, 0);
    assert_eq!(group.state, GroupState::Empty);
    assert!(group.members.is_empty());
    assert!(group.leader.is_none());
}

#[test]
fn test_group_state_as_str_wire_strings() {
    // These are echoed on the wire (DescribeGroups/ListGroups); pin them.
    assert_eq!(GroupState::Empty.as_str(), "Empty");
    assert_eq!(
        GroupState::PreparingRebalance.as_str(),
        "PreparingRebalance"
    );
    assert_eq!(
        GroupState::CompletingRebalance.as_str(),
        "CompletingRebalance"
    );
    assert_eq!(GroupState::Stable.as_str(), "Stable");
}

#[test]
fn test_invalid_session_timeout_member_is_timed_out() {
    // CG-4: a non-positive session_timeout_ms must not make a member immortal.
    let member = GroupMember {
        member_id: "m".to_string(),
        client_id: "c".to_string(),
        client_host: "localhost".to_string(),
        session_timeout_ms: -1,
        rebalance_timeout_ms: 60000,
        protocol_type: "consumer".to_string(),
        protocols: vec![],
        assignment: None,
        last_heartbeat: Instant::now(),
        awaiting_join: false,
        awaiting_sync: false,
        group_instance_id: None,
    };
    assert!(member.is_timed_out());
    let valid = GroupMember {
        session_timeout_ms: 30_000,
        last_heartbeat: Instant::now(),
        ..member
    };
    assert!(!valid.is_timed_out());
}

#[test]
fn test_effective_rebalance_timeout_falls_back_to_session_timeout() {
    let mut member = GroupMember {
        member_id: "m".to_string(),
        client_id: "c".to_string(),
        client_host: "localhost".to_string(),
        session_timeout_ms: 10_000,
        rebalance_timeout_ms: 45_000,
        protocol_type: "consumer".to_string(),
        protocols: vec![],
        assignment: None,
        last_heartbeat: Instant::now(),
        awaiting_join: false,
        awaiting_sync: false,
        group_instance_id: None,
    };
    assert_eq!(member.effective_rebalance_timeout_ms(), 45_000);
    // v0 JoinGroup: no rebalance timeout, Kafka uses the session timeout.
    member.rebalance_timeout_ms = -1;
    assert_eq!(member.effective_rebalance_timeout_ms(), 10_000);
    member.session_timeout_ms = 0;
    assert_eq!(
        member.effective_rebalance_timeout_ms(),
        u128::from(DEFAULT_REBALANCE_TIMEOUT_MS)
    );
}

// ----- Empty -> PreparingRebalance -> CompletingRebalance -> Stable -----

#[test]
fn test_single_member_join_completes_immediately() {
    let c = GroupCoordinator::new();
    let a = joined(&join(&c, "g", None));
    assert_eq!(
        a.generation_id, 1,
        "the join completion moves the generation"
    );
    assert!(a.is_leader());
    assert_eq!(a.protocol_name.as_deref(), Some("range"));
    assert_eq!(a.members, vec![(a.member_id.clone(), b"sub".to_vec())]);
    assert_eq!(state(&c, "g"), GroupState::CompletingRebalance);
    assert_eq!(c.pending_join_count(), 0);
}

#[test]
fn test_leader_sync_moves_group_to_stable() {
    let c = GroupCoordinator::new();
    let a = joined(&join(&c, "g", None));
    let assignment = c
        .sync_now(
            "g".to_string(),
            a.member_id.clone(),
            1,
            vec![(a.member_id.clone(), b"A".to_vec())],
        )
        .unwrap();
    assert_eq!(assignment, b"A".to_vec());
    assert_eq!(state(&c, "g"), GroupState::Stable);
    assert!(c.heartbeat("g", &a.member_id, 1).is_ok());
}

#[test]
fn test_new_member_join_on_stable_group_is_held_until_existing_member_rejoins() {
    let c = GroupCoordinator::new();
    let a = stable_single(&c, "g");

    let b_slot = join(&c, "g", None);
    assert!(is_pending(&b_slot), "the new member must wait for `a`");
    assert_eq!(state(&c, "g"), GroupState::PreparingRebalance);
    assert_eq!(
        generation(&c, "g"),
        1,
        "generation moves only at completion"
    );
    assert_eq!(c.pending_join_count(), 1);

    // `a` learns of the rebalance from Heartbeat at its (still current) generation.
    assert!(matches!(
        c.heartbeat("g", &a, 1),
        Err(KafkaError::RebalanceInProgress { .. })
    ));

    // `a` rejoins: both held responses are answered with the same generation.
    let a_slot = join(&c, "g", Some(&a));
    let a2 = joined(&a_slot);
    let b = joined(&b_slot);
    assert_eq!(a2.generation_id, 2);
    assert_eq!(b.generation_id, 2);
    assert!(a2.is_leader());
    assert_eq!(b.leader_id, a);
    assert!(b.members.is_empty(), "followers get no member list");
    let mut expected = vec![
        (a.clone(), b"sub".to_vec()),
        (b.member_id.clone(), b"sub".to_vec()),
    ];
    expected.sort();
    assert_eq!(a2.members, expected, "the leader's list holds both members");
    assert_eq!(state(&c, "g"), GroupState::CompletingRebalance);
    assert_eq!(c.pending_join_count(), 0);
}

#[test]
fn test_join_phase_waits_for_every_member() {
    // Three members; the join phase completes only after the last one rejoins.
    let c = GroupCoordinator::new();
    let (leader, follower, gen) = stable_pair(&c, "g");
    let third = join(&c, "g", None);
    let leader_slot = join(&c, "g", Some(&leader));
    assert!(is_pending(&third) && is_pending(&leader_slot));
    assert_eq!(generation(&c, "g"), gen);
    let follower_slot = join(&c, "g", Some(&follower));
    let results = [joined(&third), joined(&leader_slot), joined(&follower_slot)];
    for r in &results {
        assert_eq!(r.generation_id, gen + 1);
        assert_eq!(r.leader_id, leader);
    }
    assert_eq!(results[1].members.len(), 3);
}

#[test]
fn test_heartbeat_by_state() {
    let c = GroupCoordinator::new();
    let a = joined(&join(&c, "g", None)).member_id;
    // CompletingRebalance, current generation: NONE (Kafka; clients heartbeat
    // between the JoinGroup response and SyncGroup).
    assert!(c.heartbeat("g", &a, 1).is_ok());
    // Stale generation.
    assert!(matches!(
        c.heartbeat("g", &a, 0),
        Err(KafkaError::IllegalGeneration { .. })
    ));
    // Unknown member / unknown group. An unknown member is reported as such even
    // with a stale generation (Kafka checks the member first), so an evicted
    // client rejoins as a new member.
    assert!(matches!(
        c.heartbeat("g", "ghost", 1),
        Err(KafkaError::UnknownMemberId { .. })
    ));
    assert!(matches!(
        c.heartbeat("g", "ghost", 0),
        Err(KafkaError::UnknownMemberId { .. })
    ));
    assert!(matches!(
        c.validate_commit("g", "ghost", 0),
        Err(KafkaError::UnknownMemberId { .. })
    ));
    assert!(matches!(
        c.sync_now("g".to_string(), "ghost".to_string(), 0, vec![]),
        Err(KafkaError::UnknownMemberId { .. })
    ));
    assert!(matches!(
        c.heartbeat("nope", &a, 1),
        Err(KafkaError::UnknownMemberId { .. })
    ));
    c.sync_now("g".to_string(), a.clone(), 1, vec![(a.clone(), vec![1])])
        .unwrap();
    assert!(c.heartbeat("g", &a, 1).is_ok());
    // PreparingRebalance: REBALANCE_IN_PROGRESS, and the heartbeat counts.
    let _b = join(&c, "g", None);
    age_member(&c, "g", &a, Duration::from_secs(20));
    assert!(matches!(
        c.heartbeat("g", &a, 1),
        Err(KafkaError::RebalanceInProgress { .. })
    ));
    let g = c.get_group_state("g").unwrap();
    assert!(g.members[&a].last_heartbeat.elapsed() < Duration::from_secs(5));
}

#[test]
fn test_validate_commit_by_state() {
    let c = GroupCoordinator::new();
    let a = joined(&join(&c, "g", None)).member_id;

    // CompletingRebalance: fenced (RV-10), the member has not synced yet.
    assert!(matches!(
        c.validate_commit("g", &a, 1),
        Err(KafkaError::RebalanceInProgress { .. })
    ));
    c.sync_now("g".to_string(), a.clone(), 1, vec![(a.clone(), vec![1])])
        .unwrap();
    // Stable: accepted.
    assert!(c.validate_commit("g", &a, 1).is_ok());

    // PreparingRebalance: the revoke-time commit of a member that has not
    // rejoined yet is accepted under the (unchanged) current generation.
    let _b = join(&c, "g", None);
    assert_eq!(state(&c, "g"), GroupState::PreparingRebalance);
    assert!(c.validate_commit("g", &a, 1).is_ok());

    // Zombie fencing (CONF-1 / RA-5).
    assert!(matches!(
        c.validate_commit("g", &a, 0),
        Err(KafkaError::IllegalGeneration { .. })
    ));
    assert!(matches!(
        c.validate_commit("g", "ghost", 1),
        Err(KafkaError::UnknownMemberId { .. })
    ));
    assert!(matches!(
        c.validate_commit("unknown-group", "m", 1),
        Err(KafkaError::UnknownMemberId { .. })
    ));
    // Simple consumers (generation < 0) are not validated.
    assert!(c.validate_commit("g", "", -1).is_ok());
    assert!(c.validate_commit("unknown-group", "", -1).is_ok());
}

#[test]
fn test_sync_group_by_state() {
    let c = GroupCoordinator::new();
    let (leader, follower, gen) = stable_pair(&c, "g");

    // PreparingRebalance: REBALANCE_IN_PROGRESS.
    let third = join(&c, "g", None);
    assert!(matches!(
        c.sync_now("g".to_string(), leader.clone(), gen, vec![]),
        Err(KafkaError::RebalanceInProgress { .. })
    ));

    let l = join(&c, "g", Some(&leader));
    let f = join(&c, "g", Some(&follower));
    let gen2 = joined(&l).generation_id;
    let _ = joined(&f);
    let third = joined(&third).member_id;

    // CompletingRebalance, followers before the leader: parked (Kafka holds them
    // until the leader syncs), the generation does not move.
    let follower_sync = c.sync_park("g".to_string(), follower.clone(), gen2, vec![]);
    let third_sync = c.sync_park("g".to_string(), third.clone(), gen2, vec![]);
    assert!(sync_pending(&follower_sync) && sync_pending(&third_sync));
    assert_eq!(c.pending_sync_count(), 2);
    assert_eq!(state(&c, "g"), GroupState::CompletingRebalance);
    assert_eq!(generation(&c, "g"), gen2);
    // A parked member keeps its session alive (and may heartbeat meanwhile).
    assert!(c.heartbeat("g", &follower, gen2).is_ok());

    // Wrong generation / unknown member / unknown group.
    assert!(matches!(
        c.sync_now("g".to_string(), leader.clone(), gen, vec![]),
        Err(KafkaError::IllegalGeneration { .. })
    ));
    assert!(matches!(
        c.sync_now("g".to_string(), "ghost".to_string(), gen2, vec![]),
        Err(KafkaError::UnknownMemberId { .. })
    ));
    assert!(matches!(
        c.sync_now("nope".to_string(), leader.clone(), gen2, vec![]),
        Err(KafkaError::UnknownMemberId { .. })
    ));

    // The leader assigns the follower but omits `third`, and names a stranger
    // (ignored). `third` gets an empty assignment, as in Kafka.
    let own = c
        .sync_now(
            "g".to_string(),
            leader.clone(),
            gen2,
            vec![
                (leader.clone(), b"L".to_vec()),
                (follower.clone(), b"F".to_vec()),
                ("stranger".to_string(), b"S".to_vec()),
            ],
        )
        .unwrap();
    assert_eq!(own, b"L".to_vec());
    assert_eq!(state(&c, "g"), GroupState::Stable);
    // The leader's SyncGroup answers every parked follower.
    assert_eq!(synced(&follower_sync).unwrap(), b"F".to_vec());
    assert_eq!(synced(&third_sync).unwrap(), Vec::<u8>::new());
    assert_eq!(c.pending_sync_count(), 0);
    assert_eq!(generation(&c, "g"), gen2);
    // Stable: the stored assignment, at once.
    assert_eq!(
        c.sync_now("g".to_string(), follower.clone(), gen2, vec![])
            .unwrap(),
        b"F".to_vec()
    );
    assert_eq!(
        c.sync_now("g".to_string(), third, gen2, vec![]).unwrap(),
        Vec::<u8>::new()
    );
    assert_eq!(state(&c, "g"), GroupState::Stable, "no repair rebalance");
}

#[test]
fn test_follower_rejoin_with_unchanged_metadata_keeps_generation_and_assignment() {
    let c = GroupCoordinator::new();
    let (_leader, follower, gen) = stable_pair(&c, "g");
    let r = joined(&join(&c, "g", Some(&follower)));
    assert_eq!(r.generation_id, gen);
    assert!(!r.is_leader());
    assert_eq!(state(&c, "g"), GroupState::Stable);
    assert_eq!(
        c.sync_now("g".to_string(), follower, gen, vec![]).unwrap(),
        b"B".to_vec()
    );
}

#[test]
fn test_follower_rejoin_with_changed_metadata_starts_rebalance() {
    let c = GroupCoordinator::new();
    let (_leader, follower, gen) = stable_pair(&c, "g");
    let slot = join_with(&c, &params("g", Some(&follower), &[("range", b"new")]));
    assert!(is_pending(&slot));
    assert_eq!(state(&c, "g"), GroupState::PreparingRebalance);
    assert_eq!(generation(&c, "g"), gen);
}

#[test]
fn test_leader_rejoin_on_stable_group_starts_rebalance_and_stays_leader() {
    // Kafka's rule: a leader rejoining makes the group pick up metadata changes.
    // BUG-9: it must still be the leader afterwards.
    let c = GroupCoordinator::new();
    let (leader, follower, gen) = stable_pair(&c, "g");
    let l = join(&c, "g", Some(&leader));
    assert!(is_pending(&l));
    assert_eq!(state(&c, "g"), GroupState::PreparingRebalance);
    let f = join(&c, "g", Some(&follower));
    let l = joined(&l);
    assert!(l.is_leader(), "a rejoining leader must remain the leader");
    assert_eq!(l.generation_id, gen + 1);
    assert_eq!(joined(&f).leader_id, leader);
}

#[test]
fn test_new_member_during_sync_phase_restarts_join_phase() {
    let c = GroupCoordinator::new();
    let a = joined(&join(&c, "g", None)).member_id;
    assert_eq!(state(&c, "g"), GroupState::CompletingRebalance);
    let b = join(&c, "g", None);
    assert!(is_pending(&b));
    assert_eq!(state(&c, "g"), GroupState::PreparingRebalance);
    // The leader's SyncGroup for the replaced join is refused.
    assert!(matches!(
        c.sync_now("g".to_string(), a.clone(), 1, vec![(a.clone(), vec![])]),
        Err(KafkaError::RebalanceInProgress { .. })
    ));
    let a2 = joined(&join(&c, "g", Some(&a)));
    assert_eq!(a2.generation_id, 2);
    assert_eq!(joined(&b).generation_id, 2);
    assert_eq!(a2.members.len(), 2);
}

#[test]
fn test_known_member_retry_during_sync_phase_is_answered_at_once() {
    let c = GroupCoordinator::new();
    let first = joined(&join(&c, "g", None));
    let retry = joined(&join(&c, "g", Some(&first.member_id)));
    assert_eq!(retry, first, "same generation, leader and member list");
    assert_eq!(state(&c, "g"), GroupState::CompletingRebalance);
}

// ----- wave3 groups: Kafka 3.7 join / commit rules and limits -----

#[test]
fn test_unknown_member_id_gets_unknown_member_id() {
    // Kafka: a JoinGroup with a non-empty member id the group does not know is answered
    // UNKNOWN_MEMBER_ID (the client then rejoins with an empty id); it used to be given a
    // fresh id silently. No group is created for it either.
    let c = GroupCoordinator::new();
    assert_eq!(
        join_error_code(&join(&c, "g", Some("evicted-long-ago"))),
        crate::kafka::constants::ERROR_UNKNOWN_MEMBER_ID
    );
    assert!(c.get_group_state("g").is_none());
    stable_single(&c, "g");
    assert_eq!(
        join_error_code(&join(&c, "g", Some("evicted-long-ago"))),
        crate::kafka::constants::ERROR_UNKNOWN_MEMBER_ID
    );
    assert_eq!(state(&c, "g"), GroupState::Stable, "group unaffected");
}

#[test]
fn test_inconsistent_group_protocol() {
    use crate::kafka::constants::ERROR_INCONSISTENT_GROUP_PROTOCOL as E;
    let c = GroupCoordinator::new();
    // Empty protocol list or empty protocol type on a new group: refused, no group left.
    assert_eq!(join_error_code(&join_with(&c, &params("n", None, &[]))), E);
    let mut p = params("n", None, &[("range", b"")]);
    p.protocol_type = String::new();
    assert_eq!(join_error_code(&join_with(&c, &p)), E);
    assert!(c.get_group_state("n").is_none());

    let leader = stable_single(&c, "g");
    // Different protocol type, same protocol.
    let mut p = params("g", None, &[("range", b"sub")]);
    p.protocol_type = "connect".to_string();
    assert_eq!(join_error_code(&join_with(&c, &p)), E);
    // Same type, no protocol in common.
    assert_eq!(
        join_error_code(&join_with(&c, &params("g", None, &[("roundrobin", b"")]))),
        E
    );
    // Empty protocol list on an existing group.
    assert_eq!(join_error_code(&join_with(&c, &params("g", None, &[]))), E);
    // None of these disturbed the group.
    assert_eq!(state(&c, "g"), GroupState::Stable);
    assert_eq!(c.get_group_state("g").unwrap().members.len(), 1);
    // The only member keeps the group's protocol type...
    let mut p = params("g", Some(&leader), &[("range", b"sub")]);
    p.protocol_type = "connect".to_string();
    assert_eq!(join_error_code(&join_with(&c, &p)), E);
    // ...but may switch protocols: there is no one else to agree with.
    let r = joined(&join_with(
        &c,
        &params("g", Some(&leader), &[("roundrobin", b"x")]),
    ));
    assert_eq!(r.protocol_name.as_deref(), Some("roundrobin"));
}

#[test]
fn test_protocol_type_is_kept_when_group_empties() {
    let c = GroupCoordinator::new();
    let m = stable_single(&c, "g");
    c.leave_group("g", &m).unwrap();
    let g = c.get_group_state("g").unwrap();
    assert_eq!(g.state, GroupState::Empty);
    assert_eq!(g.protocol_type.as_deref(), Some("consumer"));
    // An Empty group accepts any protocol type again (Kafka's rule).
    let mut p = params("g", None, &[("x", b"")]);
    p.protocol_type = "connect".to_string();
    joined(&join_with(&c, &p));
    assert_eq!(
        c.get_group_state("g").unwrap().protocol_type.as_deref(),
        Some("connect")
    );
}

#[test]
fn test_group_protocol_is_chosen_by_member_vote() {
    // Kafka: candidates = protocols every member supports; each member votes for its
    // first candidate. Leader prefers range, both followers prefer roundrobin.
    let c = GroupCoordinator::new();
    let both_rr: &[(&str, &[u8])] = &[("roundrobin", b""), ("range", b"")];
    let leader = joined(&join_with(
        &c,
        &params("g", None, &[("range", b""), ("roundrobin", b"")]),
    ));
    assert_eq!(leader.protocol_name.as_deref(), Some("range"));
    let f1 = join_with(&c, &params("g", None, both_rr));
    let f2 = join_with(&c, &params("g", None, both_rr));
    let l = joined(&join_with(
        &c,
        &params(
            "g",
            Some(&leader.member_id),
            &[("range", b""), ("roundrobin", b"")],
        ),
    ));
    assert_eq!(l.protocol_name.as_deref(), Some("roundrobin"));
    assert_eq!(joined(&f1).protocol_name.as_deref(), Some("roundrobin"));
    assert_eq!(joined(&f2).protocol_name.as_deref(), Some("roundrobin"));
}

#[test]
fn test_vote_tie_goes_to_leader_preference() {
    let mut g = ConsumerGroup::new("g".to_string());
    g.upsert_member("a", &params("g", None, &[("range", b""), ("rr", b"")]));
    g.upsert_member("b", &params("g", None, &[("rr", b""), ("range", b"")]));
    assert_eq!(g.leader.as_deref(), Some("a"));
    assert_eq!(g.select_group_protocol().as_deref(), Some("range"));
}

#[test]
fn test_group_max_size_is_configurable() {
    let c = GroupCoordinator::new();
    c.set_limits(100, 2);
    let a = joined(&join(&c, "g", None));
    let _b = join(&c, "g", None);
    assert_eq!(
        join_error_code(&join(&c, "g", None)),
        crate::kafka::constants::ERROR_GROUP_MAX_SIZE_REACHED
    );
    // A member already in the group may always rejoin.
    let _ = join(&c, "g", Some(&a.member_id));
    assert_eq!(c.get_group_state("g").unwrap().members.len(), 2);
}

#[test]
fn test_max_groups_evicts_oldest_empty_group_then_refuses() {
    let c = GroupCoordinator::new();
    c.set_limits(2, 10);
    let a = stable_single(&c, "a");
    c.leave_group("a", &a).unwrap(); // "a" is Empty now
    stable_single(&c, "b");
    // Full (2): the Empty "a" is forgotten to make room for "c".
    joined(&join(&c, "c", None));
    assert!(c.get_group_state("a").is_none());
    assert!(c.get_group_state("c").is_some());
    // Full of non-empty groups: a new group is refused (retriable), existing ones work.
    assert_eq!(
        join_error_code(&join(&c, "d", None)),
        crate::kafka::constants::ERROR_COORDINATOR_NOT_AVAILABLE
    );
    assert!(c.get_group_state("d").is_none());
    assert_eq!(c.groups.read().unwrap().len(), 2);
}

#[test]
fn test_validate_offset_commit_matches_kafka() {
    use crate::kafka::constants::{
        ERROR_ILLEGAL_GENERATION as IG, ERROR_REBALANCE_IN_PROGRESS as RIP,
        ERROR_UNKNOWN_MEMBER_ID as UM,
    };
    let c = GroupCoordinator::new();
    let code = |g: &str, m: &str, gen: i32| {
        c.validate_offset_commit(g, m, gen)
            .err()
            .map_or(0, |e| e.to_kafka_error_code())
    };
    // Unknown group: generation -1 is a simple consumer (any member id); >= 0 is stale.
    assert_eq!(code("nope", "", -1), 0);
    assert_eq!(code("nope", "x", -1), 0);
    assert_eq!(code("nope", "x", 5), IG);

    let m = stable_single(&c, "g");
    // Group with members: generation -1 / "" is not allowed, members only.
    assert_eq!(code("g", "", -1), UM);
    assert_eq!(code("g", "x", -1), UM);
    assert_eq!(code("g", "x", 1), UM);
    assert_eq!(code("g", &m, -1), IG);
    assert_eq!(code("g", &m, 7), IG);
    assert_eq!(code("g", &m, 1), 0);

    // CompletingRebalance: REBALANCE_IN_PROGRESS for the current generation.
    let follower = join(&c, "g", None);
    let _ = joined(&join(&c, "g", Some(&m)));
    let f = joined(&follower);
    assert_eq!(state(&c, "g"), GroupState::CompletingRebalance);
    assert_eq!(code("g", &f.member_id, f.generation_id), RIP);

    // Empty group: anything at generation -1 goes; a generation needs membership.
    let e = stable_single(&c, "e");
    c.leave_group("e", &e).unwrap();
    assert_eq!(code("e", "", -1), 0);
    assert_eq!(code("e", &e, -1), 0);
    assert_eq!(code("e", "x", 5), UM);
    assert_eq!(code("e", "", 1), UM);
}

#[test]
fn test_duplicate_join_replaces_held_response() {
    let c = GroupCoordinator::new();
    let a = stable_single(&c, "g");
    let b = joined_id_pending(&c);
    // `b` resends its JoinGroup (same id) while the first is still held.
    let first = b.1;
    let second = join(&c, "g", Some(&b.0));
    assert_eq!(
        join_error_code(&first),
        crate::kafka::constants::ERROR_REBALANCE_IN_PROGRESS
    );
    assert!(is_pending(&second));
    assert_eq!(c.pending_join_count(), 1);
    let _ = joined(&join(&c, "g", Some(&a)));
    assert_eq!(joined(&second).generation_id, 2);

    /// Join a new member to the Stable group "g" and return its id (read from the
    /// group) and its held slot.
    fn joined_id_pending(c: &GroupCoordinator) -> (String, Slot) {
        let before: Vec<String> = c
            .get_group_state("g")
            .unwrap()
            .members
            .into_keys()
            .collect();
        let slot = join(c, "g", None);
        let id = c
            .get_group_state("g")
            .unwrap()
            .members
            .into_keys()
            .find(|id| !before.contains(id))
            .unwrap();
        (id, slot)
    }
}

// ----- protocol selection -----

#[test]
fn test_join_selects_strategy_common_to_all_members() {
    // CG-1: the group protocol must be a strategy supported by EVERY member.
    let c = GroupCoordinator::new();
    let leader = joined(&join_with(
        &c,
        &params("g", None, &[("sticky", b"s"), ("range", b"r")]),
    ))
    .member_id;
    let follower = join_with(&c, &params("g", None, &[("range", b"r2")]));
    let l = joined(&join_with(
        &c,
        &params("g", Some(&leader), &[("sticky", b"s"), ("range", b"r")]),
    ));
    let f = joined(&follower);
    assert_eq!(l.protocol_name.as_deref(), Some("range"));
    assert_eq!(f.protocol_name.as_deref(), Some("range"));
    assert_eq!(c.group_protocol_name("g").as_deref(), Some("range"));
    assert!(c.group_protocol_name("nonexistent").is_none());
}

#[test]
fn test_join_leader_gets_selected_protocol_metadata() {
    // The leader's member list carries each member's metadata for the
    // group-SELECTED protocol, not the member's first-listed one.
    let c = GroupCoordinator::new();
    let leader = joined(&join_with(
        &c,
        &params("g", None, &[("roundrobin", b"rr-leader")]),
    ))
    .member_id;
    let follower = join_with(
        &c,
        &params(
            "g",
            None,
            &[("range", b"range-follower"), ("roundrobin", b"rr-follower")],
        ),
    );
    let l = joined(&join_with(
        &c,
        &params("g", Some(&leader), &[("roundrobin", b"rr-leader")]),
    ));
    let f = joined(&follower);
    assert_eq!(l.protocol_name.as_deref(), Some("roundrobin"));
    let meta: HashMap<String, Vec<u8>> = l.members.into_iter().collect();
    assert_eq!(meta[&f.member_id], b"rr-follower".to_vec());
    assert_eq!(meta[&leader], b"rr-leader".to_vec());
}

// ----- LeaveGroup -----

#[test]
fn test_leave_of_member_that_had_not_rejoined_completes_held_join() {
    let c = GroupCoordinator::new();
    let a = stable_single(&c, "g");
    let b = join(&c, "g", None);
    assert!(is_pending(&b));
    c.leave_group("g", &a).unwrap();
    let b = joined(&b);
    assert_eq!(b.generation_id, 2);
    assert!(b.is_leader(), "leadership passes to the remaining member");
    assert_eq!(b.members, vec![(b.member_id.clone(), b"sub".to_vec())]);
    assert_eq!(state(&c, "g"), GroupState::CompletingRebalance);
}

#[test]
fn test_leave_of_waiting_member_answers_its_join_unknown_member() {
    let c = GroupCoordinator::new();
    let (leader, follower, gen) = stable_pair(&c, "g");
    let l = join(&c, "g", Some(&leader));
    assert!(is_pending(&l));
    c.leave_group("g", &leader).unwrap();
    assert_eq!(
        join_error_code(&l),
        crate::kafka::constants::ERROR_UNKNOWN_MEMBER_ID
    );
    assert_eq!(state(&c, "g"), GroupState::PreparingRebalance);
    let f = joined(&join(&c, "g", Some(&follower)));
    assert!(f.is_leader());
    assert_eq!(f.generation_id, gen + 1);
    assert_eq!(c.pending_join_count(), 0);
}

#[test]
fn test_leave_triggers_rebalance_from_stable_and_sync_phase() {
    let c = GroupCoordinator::new();
    let (leader, _follower, _) = stable_pair(&c, "g");
    c.leave_group("g", &leader).unwrap();
    assert_eq!(state(&c, "g"), GroupState::PreparingRebalance);

    let c = GroupCoordinator::new();
    let a = stable_single(&c, "g2");
    let b = join(&c, "g2", None);
    let _ = joined(&join(&c, "g2", Some(&a)));
    let b = joined(&b).member_id;
    assert_eq!(state(&c, "g2"), GroupState::CompletingRebalance);
    c.leave_group("g2", &b).unwrap();
    assert_eq!(state(&c, "g2"), GroupState::PreparingRebalance);
}

#[test]
fn test_last_leave_empties_group_and_keeps_generation() {
    let c = GroupCoordinator::new();
    let a = stable_single(&c, "g");
    c.leave_group("g", &a).unwrap();
    let g = c.get_group_state("g").unwrap();
    assert_eq!(g.state, GroupState::Empty);
    assert!(g.leader.is_none());
    assert_eq!(g.generation_id, 1);
    assert!(matches!(
        c.leave_group("g", &a),
        Err(KafkaError::UnknownMemberId { .. })
    ));
    assert!(matches!(
        c.leave_group("nope", "m"),
        Err(KafkaError::UnknownMemberId { .. })
    ));
    // Rejoining continues the generation (zombie fencing).
    c.check_and_remove_timed_out_members();
    assert_eq!(joined(&join(&c, "g", None)).generation_id, 2);
}

// ----- the periodic sweep -----

#[test]
fn test_sweep_session_timeout_of_straggler_completes_held_join() {
    // No further request arrives: the sweep alone must answer the held join.
    let c = GroupCoordinator::new();
    let a = stable_single(&c, "g");
    let b = join(&c, "g", None);
    age_member(&c, "g", &a, Duration::from_secs(31));
    let removed = c.check_and_remove_timed_out_members();
    assert_eq!(removed, vec![("g".to_string(), a)]);
    let b = joined(&b);
    assert!(b.is_leader());
    assert_eq!(b.generation_id, 2);
}

#[test]
fn test_sweep_does_not_expire_session_of_member_waiting_on_its_join() {
    let c = GroupCoordinator::new();
    let a = stable_single(&c, "g");
    let b_slot = join(&c, "g", None);
    let b_id = c
        .get_group_state("g")
        .unwrap()
        .members
        .into_keys()
        .find(|id| *id != a)
        .unwrap();
    // `b` cannot heartbeat while blocked on its JoinGroup.
    age_member(&c, "g", &b_id, Duration::from_secs(31));
    assert_eq!(
        c.check_and_remove_timed_out_members(),
        [] as [(String, String); 0]
    );
    assert!(is_pending(&b_slot));
    assert_eq!(c.pending_join_count(), 1);
    // When the phase completes, the session clock restarts.
    let _ = joined(&join(&c, "g", Some(&a)));
    assert_eq!(joined(&b_slot).generation_id, 2);
    assert_eq!(
        c.check_and_remove_timed_out_members(),
        [] as [(String, String); 0]
    );
}

#[test]
fn test_sweep_rebalance_timeout_evicts_stragglers_and_completes_join() {
    let c = GroupCoordinator::new();
    let (leader, follower, gen) = stable_pair(&c, "g");
    let third = join(&c, "g", None);
    let l = join(&c, "g", Some(&leader));
    // The follower keeps heartbeating but never rejoins.
    assert!(matches!(
        c.heartbeat("g", &follower, gen),
        Err(KafkaError::RebalanceInProgress { .. })
    ));
    // Within the 60s rebalance timeout nothing happens.
    age_rebalance(&c, "g", Duration::from_secs(59));
    assert_eq!(
        c.check_and_remove_timed_out_members(),
        [] as [(String, String); 0]
    );
    assert!(is_pending(&l));

    age_rebalance(&c, "g", Duration::from_secs(61));
    let removed = c.check_and_remove_timed_out_members();
    assert_eq!(removed, vec![("g".to_string(), follower.clone())]);
    let l = joined(&l);
    let third = joined(&third);
    assert_eq!(l.generation_id, gen + 1);
    assert_eq!(third.generation_id, gen + 1);
    assert_eq!(l.members.len(), 2);
    assert_eq!(state(&c, "g"), GroupState::CompletingRebalance);
    // The evicted follower is fenced and must rejoin as a new member.
    assert!(matches!(
        c.heartbeat("g", &follower, gen + 1),
        Err(KafkaError::UnknownMemberId { .. })
    ));
    // The sync phase starts a fresh window: the leader is not evicted next sweep.
    assert_eq!(
        c.check_and_remove_timed_out_members(),
        [] as [(String, String); 0]
    );
}

#[test]
fn test_rebalance_timeout_honors_member_rebalance_timeout_ms() {
    // CG-3: the timeout is the largest rebalance_timeout_ms the members advertise,
    // not a hardcoded 5 minutes. join_group also runs it (over every group).
    let c = GroupCoordinator::new();
    let mut p = params("g", None, &[("range", b"sub")]);
    p.rebalance_timeout_ms = 20;
    p.session_timeout_ms = 300_000;
    let a = joined(&join_with(&c, &p)).member_id;
    c.sync_now("g".to_string(), a.clone(), 1, vec![(a.clone(), vec![])])
        .unwrap();
    let b = join(&c, "g", None);
    // `b` advertises 60s, so the group's timeout is 60s, not `a`'s 20ms.
    age_rebalance(&c, "g", Duration::from_millis(100));
    let _ = join(&c, "other", None);
    assert!(is_pending(&b));
    age_rebalance(&c, "g", Duration::from_secs(61));
    let _ = join(&c, "other2", None);
    let b = joined(&b);
    assert!(b.is_leader());
    assert!(!c.get_group_state("g").unwrap().members.contains_key(&a));
}

#[test]
fn test_sweep_rebalance_timeout_with_nobody_rejoined_empties_group() {
    let c = GroupCoordinator::new();
    let (leader, _follower, gen) = stable_pair(&c, "g");
    {
        // Start a join phase nobody has rejoined yet (as after a LeaveGroup).
        let mut groups = c.groups.write().unwrap();
        groups
            .get_mut("g")
            .unwrap()
            .prepare_rebalance(&mut Vec::new());
    }
    age_rebalance(&c, "g", Duration::from_secs(61));
    let removed = c.check_and_remove_timed_out_members();
    assert_eq!(removed.len(), 2);
    let g = c.get_group_state("g").unwrap();
    assert_eq!(g.state, GroupState::Empty);
    assert_eq!(g.generation_id, gen);
    assert!(!g.members.contains_key(&leader));
}

#[test]
fn test_sweep_sync_phase_timeout_evicts_leader() {
    let c = GroupCoordinator::new();
    let a = stable_single(&c, "g");
    let b = join(&c, "g", None);
    let _ = joined(&join(&c, "g", Some(&a)));
    let b = joined(&b).member_id;
    assert_eq!(state(&c, "g"), GroupState::CompletingRebalance);
    age_rebalance(&c, "g", Duration::from_secs(61));
    let removed = c.check_and_remove_timed_out_members();
    assert_eq!(removed, vec![("g".to_string(), a)]);
    let g = c.get_group_state("g").unwrap();
    assert_eq!(g.state, GroupState::PreparingRebalance);
    assert_eq!(g.leader.as_deref(), Some(b.as_str()));
}

#[test]
fn test_sweep_session_timeout_in_stable_group_starts_rebalance() {
    let c = GroupCoordinator::new();
    let (leader, follower, gen) = stable_pair(&c, "g");
    age_member(&c, "g", &follower, Duration::from_secs(31));
    let removed = c.check_and_remove_timed_out_members();
    assert_eq!(removed, vec![("g".to_string(), follower)]);
    assert_eq!(state(&c, "g"), GroupState::PreparingRebalance);
    assert_eq!(generation(&c, "g"), gen);
    let l = joined(&join(&c, "g", Some(&leader)));
    assert_eq!(l.generation_id, gen + 1);
    assert_eq!(l.members.len(), 1);
}

#[test]
fn test_empty_group_reclaimed_by_sweep_after_ttl() {
    // CG-5: an empty group is kept until its TTL, then dropped by the sweep.
    let c = GroupCoordinator::new();
    let a = joined(&join(&c, "g", None)).member_id;
    age_member(&c, "g", &a, Duration::from_secs(31));
    c.check_and_remove_timed_out_members();
    assert_eq!(state(&c, "g"), GroupState::Empty);
    {
        let mut groups = c.groups.write().unwrap();
        groups.get_mut("g").unwrap().last_activity =
            ago(Duration::from_secs(EMPTY_GROUP_TTL_SECS + 1));
    }
    c.check_and_remove_timed_out_members();
    assert!(c.get_group_state("g").is_none());
}

// ----- limits and removal -----

#[test]
fn test_join_group_enforces_member_cap() {
    // CG-4: a group is capped at pg_kafka.group_max_size (default) new members.
    let c = GroupCoordinator::new();
    {
        let mut groups = c.groups.write().unwrap();
        let mut g = ConsumerGroup::new("g".to_string());
        for i in 0..crate::kafka::limits::DEFAULT_GROUP_MAX_SIZE {
            g.upsert_member(&format!("m{i}"), &params("g", None, &[("range", b"")]));
        }
        g.state = GroupState::Stable;
        groups.insert("g".to_string(), g);
    }
    assert_eq!(
        join_error_code(&join(&c, "g", None)),
        crate::kafka::constants::ERROR_GROUP_MAX_SIZE_REACHED
    );
    // A known member may still rejoin.
    let rejoin = joined(&join_with(&c, &params("g", Some("m1"), &[("range", b"")])));
    assert_eq!(rejoin.member_id, "m1");
    assert_eq!(
        state(&c, "g"),
        GroupState::Stable,
        "m1 is a follower, unchanged"
    );
}

#[test]
fn test_remove_group_answers_held_joins() {
    let c = GroupCoordinator::new();
    let _a = stable_single(&c, "g");
    let b = join(&c, "g", None);
    c.remove_group("g");
    assert_eq!(
        join_error_code(&b),
        crate::kafka::constants::ERROR_UNKNOWN_MEMBER_ID
    );
    assert_eq!(c.pending_join_count(), 0);
    assert!(c.get_group_state("g").is_none());
}

#[test]
fn test_poisoned_lock_answers_join_with_error() {
    let c = GroupCoordinator::new();
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = c.groups.write().unwrap();
        panic!("poison");
    }));
    let slot = join(&c, "g", None);
    assert_eq!(
        join_error_code(&slot),
        crate::kafka::constants::ERROR_UNKNOWN_SERVER_ERROR
    );
    assert_eq!(
        c.check_and_remove_timed_out_members(),
        [] as [(String, String); 0]
    );
}

// ----- parked follower SyncGroup -----

/// A pair whose join phase just completed (CompletingRebalance). Returns
/// (leader, follower, generation).
fn completing_pair(c: &GroupCoordinator, group: &str) -> (String, String, i32) {
    let (leader, follower, _) = stable_pair(c, group);
    let l = join(c, group, Some(&leader));
    let f = join(c, group, Some(&follower));
    let gen = joined(&l).generation_id;
    assert_eq!(joined(&f).generation_id, gen);
    assert_eq!(state(c, group), GroupState::CompletingRebalance);
    (leader, follower, gen)
}

#[test]
fn test_parked_sync_answered_when_leader_syncs() {
    let c = GroupCoordinator::new();
    let (leader, follower, gen) = completing_pair(&c, "g");
    let parked = c.sync_park("g".to_string(), follower.clone(), gen, vec![]);
    assert!(sync_pending(&parked));
    let own = c
        .sync_now(
            "g".to_string(),
            leader.clone(),
            gen,
            vec![(leader, b"L".to_vec()), (follower, b"F".to_vec())],
        )
        .unwrap();
    assert_eq!(own, b"L".to_vec());
    assert_eq!(synced(&parked).unwrap(), b"F".to_vec());
    assert_eq!(generation(&c, "g"), gen);
}

#[test]
fn test_parked_sync_fails_when_new_member_starts_rebalance() {
    let c = GroupCoordinator::new();
    let (leader, follower, gen) = completing_pair(&c, "g");
    let parked = c.sync_park("g".to_string(), follower, gen, vec![]);
    let newcomer = join(&c, "g", None);
    assert!(is_pending(&newcomer));
    assert_eq!(
        synced(&parked).unwrap_err().to_kafka_error_code(),
        crate::kafka::constants::ERROR_REBALANCE_IN_PROGRESS
    );
    assert_eq!(c.pending_sync_count(), 0);
    // The leader's late SyncGroup is refused too: the group is rebalancing.
    assert!(matches!(
        c.sync_now("g".to_string(), leader, gen, vec![]),
        Err(KafkaError::RebalanceInProgress { .. })
    ));
}

#[test]
fn test_parked_sync_fails_when_another_member_leaves() {
    let c = GroupCoordinator::new();
    let a = stable_single(&c, "g");
    let b = join(&c, "g", None);
    let x = join(&c, "g", None);
    let _ = joined(&join(&c, "g", Some(&a)));
    let (b, x) = (joined(&b), joined(&x));
    let parked = c.sync_park(
        "g".to_string(),
        b.member_id.clone(),
        b.generation_id,
        vec![],
    );
    c.leave_group("g", &x.member_id).unwrap();
    assert_eq!(
        synced(&parked).unwrap_err().to_kafka_error_code(),
        crate::kafka::constants::ERROR_REBALANCE_IN_PROGRESS
    );
    assert_eq!(state(&c, "g"), GroupState::PreparingRebalance);
}

#[test]
fn test_parked_sync_of_leaving_member_answered_unknown_member() {
    let c = GroupCoordinator::new();
    let (_leader, follower, gen) = completing_pair(&c, "g");
    let parked = c.sync_park("g".to_string(), follower.clone(), gen, vec![]);
    c.leave_group("g", &follower).unwrap();
    assert_eq!(
        synced(&parked).unwrap_err().to_kafka_error_code(),
        crate::kafka::constants::ERROR_UNKNOWN_MEMBER_ID
    );
    assert_eq!(c.pending_sync_count(), 0);
}

#[test]
fn test_parked_sync_times_out_when_leader_never_syncs() {
    // The sweep's sync-phase rebalance timeout evicts the leader; the parked
    // follower is told to rejoin, with no further request arriving.
    let c = GroupCoordinator::new();
    let (leader, follower, gen) = completing_pair(&c, "g");
    let parked = c.sync_park("g".to_string(), follower.clone(), gen, vec![]);
    // A parked member is exempt from the session timeout (as a waiting joiner).
    age_member(&c, "g", &follower, Duration::from_secs(31));
    assert_eq!(
        c.check_and_remove_timed_out_members(),
        [] as [(String, String); 0]
    );
    assert!(sync_pending(&parked));
    // (Clients heartbeat while their SyncGroup waits; Heartbeat answers NONE.)
    assert!(c.heartbeat("g", &follower, gen).is_ok());

    age_rebalance(&c, "g", Duration::from_secs(61));
    let removed = c.check_and_remove_timed_out_members();
    assert_eq!(removed, vec![("g".to_string(), leader)]);
    assert_eq!(
        synced(&parked).unwrap_err().to_kafka_error_code(),
        crate::kafka::constants::ERROR_REBALANCE_IN_PROGRESS
    );
    assert_eq!(state(&c, "g"), GroupState::PreparingRebalance);
    // The follower rejoins and, now alone, leads the next generation.
    let f = joined(&join(&c, "g", Some(&follower)));
    assert!(f.is_leader());
    assert_eq!(f.generation_id, gen + 1);
}

#[test]
fn test_parked_sync_resend_replaces_earlier_one() {
    let c = GroupCoordinator::new();
    let (leader, follower, gen) = completing_pair(&c, "g");
    let first = c.sync_park("g".to_string(), follower.clone(), gen, vec![]);
    let second = c.sync_park("g".to_string(), follower.clone(), gen, vec![]);
    assert_eq!(
        synced(&first).unwrap_err().to_kafka_error_code(),
        crate::kafka::constants::ERROR_REBALANCE_IN_PROGRESS
    );
    assert!(sync_pending(&second));
    assert_eq!(c.pending_sync_count(), 1);
    c.sync_now(
        "g".to_string(),
        leader,
        gen,
        vec![(follower, b"F".to_vec())],
    )
    .unwrap();
    assert_eq!(synced(&second).unwrap(), b"F".to_vec());
}

#[test]
fn test_parked_sync_survives_unchanged_rejoin_retry() {
    // A follower that gives up on its SyncGroup and rejoins with unchanged
    // metadata gets the current generation (no rebalance); its parked SyncGroup
    // is still answered by the leader's sync.
    let c = GroupCoordinator::new();
    let (leader, follower, gen) = completing_pair(&c, "g");
    let parked = c.sync_park("g".to_string(), follower.clone(), gen, vec![]);
    assert_eq!(joined(&join(&c, "g", Some(&follower))).generation_id, gen);
    assert_eq!(state(&c, "g"), GroupState::CompletingRebalance);
    c.sync_now(
        "g".to_string(),
        leader,
        gen,
        vec![(follower, b"F".to_vec())],
    )
    .unwrap();
    assert_eq!(synced(&parked).unwrap(), b"F".to_vec());
}

#[test]
fn test_remove_group_answers_parked_syncs() {
    let c = GroupCoordinator::new();
    let (_leader, follower, gen) = completing_pair(&c, "g");
    let parked = c.sync_park("g".to_string(), follower, gen, vec![]);
    c.remove_group("g");
    assert_eq!(
        synced(&parked).unwrap_err().to_kafka_error_code(),
        crate::kafka::constants::ERROR_UNKNOWN_MEMBER_ID
    );
    assert_eq!(c.pending_sync_count(), 0);
}

#[test]
fn test_is_group_leader() {
    let c = GroupCoordinator::new();
    let (leader, follower, _) = stable_pair(&c, "g");
    assert!(c.is_group_leader("g", &leader));
    assert!(!c.is_group_leader("g", &follower));
    assert!(!c.is_group_leader("nope", &leader));
}
