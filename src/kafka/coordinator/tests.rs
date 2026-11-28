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
    let mut group = groups.get_mut(group).unwrap();
    group.rebalance_started_at = Some(ago(d));
    // The sync deadline runs from the same join completion.
    if group.sync_started_at.is_some() {
        group.sync_started_at = Some(ago(d));
    }
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
    // A non-positive session_timeout_ms must not make a member immortal.
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
        pending_sync: false,
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
        pending_sync: false,
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

    // CompletingRebalance: accepted, as Kafka 3.7 accepts a transactional commit there
    // (a plain OffsetCommit gets REBALANCE_IN_PROGRESS).
    assert!(c.validate_commit("g", &a, 1).is_ok());
    assert!(matches!(
        c.validate_offset_commit("g", &a, 1),
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
    assert!(c.validate_offset_commit("g", &a, 1).is_ok());

    // Zombie fencing.
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
    // Generation -1 with an empty member id is not validated.
    assert!(c.validate_commit("g", "", -1).is_ok());
    assert!(c.validate_commit("unknown-group", "", -1).is_ok());
}

/// Kafka 3.7 (checked against a 3.7 broker) fences a transactional commit of generation -1
/// that names a member: a group with members answers UNKNOWN_MEMBER_ID to a member id it
/// does not know and ILLEGAL_GENERATION to one of its members; a group without members
/// (unknown, or Empty) accepts it whatever the member id.
#[test]
fn test_validate_commit_generation_minus_one_with_a_member_id() {
    let c = GroupCoordinator::new();
    let a = joined(&join(&c, "g", None)).member_id;
    c.sync_now("g".to_string(), a.clone(), 1, vec![(a.clone(), vec![1])])
        .unwrap();
    assert!(matches!(
        c.validate_commit("g", "zz", -1),
        Err(KafkaError::UnknownMemberId { .. })
    ));
    assert!(matches!(
        c.validate_commit("g", &a, -1),
        Err(KafkaError::IllegalGeneration { .. })
    ));
    assert!(c.validate_commit("g", "", -1).is_ok());
    assert!(c.validate_commit("unknown-group", "zz", -1).is_ok());

    c.leave_group("g", &a).unwrap();
    assert_eq!(state(&c, "g"), GroupState::Empty);
    assert!(c.validate_commit("g", "zz", -1).is_ok());
    assert!(c.validate_commit("g", &a, -1).is_ok());
    assert!(matches!(
        c.validate_commit("g", "", 1),
        Err(KafkaError::UnknownMemberId { .. })
    ));
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
    // It must still be the leader afterwards.
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

// ----- Kafka 3.7 join / commit rules and limits -----

#[test]
fn test_unknown_member_id_gets_unknown_member_id() {
    // Kafka: a JoinGroup with a non-empty member id the group does not know is answered
    // UNKNOWN_MEMBER_ID (the client then rejoins with an empty id), not given a fresh id
    // silently. No group is created for it either.
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
fn test_lowered_max_groups_evicts_empty_groups_down_to_the_cap() {
    // pg_kafka.max_consumer_groups lowered below the number of groups in memory: a new
    // group still gets in by forgetting as many least recently active Empty groups as
    // it takes, and is refused only once no Empty group is left.
    let c = GroupCoordinator::new();
    for g in ["a", "b", "c"] {
        let m = stable_single(&c, g);
        c.leave_group(g, &m).unwrap();
    }
    stable_single(&c, "busy");
    c.set_limits(2, 10);
    joined(&join(&c, "new", None));
    assert!(c.get_group_state("new").is_some());
    let groups = c.groups.read().unwrap();
    assert_eq!(
        groups.len(),
        2,
        "evicted down to the cap: {:?}",
        groups.keys()
    );
    assert!(groups.contains_key("busy"));
    drop(groups);
    // Only non-empty groups remain: the next new group is refused.
    assert_eq!(
        join_error_code(&join(&c, "newer", None)),
        crate::kafka::constants::ERROR_COORDINATOR_NOT_AVAILABLE
    );
}

#[test]
fn test_refused_new_group_evicts_no_empty_group() {
    // Two non-empty groups fill a lowered cap of 2: forgetting the Empty group could not make
    // room, so the refused join leaves it in memory.
    let c = GroupCoordinator::new();
    let m = stable_single(&c, "idle");
    c.leave_group("idle", &m).unwrap();
    stable_single(&c, "b");
    stable_single(&c, "c");
    c.set_limits(2, 10);
    assert_eq!(
        join_error_code(&join(&c, "new", None)),
        crate::kafka::constants::ERROR_COORDINATOR_NOT_AVAILABLE
    );
    assert_eq!(state(&c, "idle"), GroupState::Empty);
    assert_eq!(c.groups.read().unwrap().len(), 3);
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
}

// ----- protocol selection -----

#[test]
fn test_join_selects_strategy_common_to_all_members() {
    // The group protocol must be a strategy supported by EVERY member.
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
    // The timeout is the largest rebalance_timeout_ms the members advertise,
    // not a hardcoded 5 minutes. The sweep runs it; a JoinGroup only for the group it names.
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
    c.check_and_remove_timed_out_members();
    assert!(is_pending(&b));
    age_rebalance(&c, "g", Duration::from_secs(61));
    // Another group's JoinGroup leaves this group's timeout to the sweep.
    let _ = join(&c, "other", None);
    assert!(is_pending(&b));
    c.check_and_remove_timed_out_members();
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
    let leader = stable_single(&c, "g");
    let follower = join(&c, "g", None);
    let rejoined = joined(&join(&c, "g", Some(&leader)));
    let follower = joined(&follower).member_id;
    assert_eq!(state(&c, "g"), GroupState::CompletingRebalance);
    // The follower synced (parked); the leader never does.
    let parked = c.sync_park(
        "g".to_string(),
        follower.clone(),
        rejoined.generation_id,
        vec![],
    );
    age_rebalance(&c, "g", Duration::from_secs(61));
    let removed = c.check_and_remove_timed_out_members();
    assert_eq!(removed, vec![("g".to_string(), leader)]);
    let g = c.get_group_state("g").unwrap();
    assert_eq!(g.state, GroupState::PreparingRebalance);
    assert_eq!(g.leader.as_deref(), Some(follower.as_str()));
    assert_eq!(
        synced(&parked).unwrap_err().to_kafka_error_code(),
        crate::kafka::constants::ERROR_REBALANCE_IN_PROGRESS
    );
}

#[test]
fn test_members_that_never_sync_are_evicted_after_the_rebalance_timeout() {
    // Kafka 3.0+ (pendingSyncMembers / DelayedSync, checked against a 3.7 broker): every
    // member of a new generation must send its SyncGroup within the rebalance timeout of
    // the join's completion; one that does not is removed even though it heartbeats, also
    // after the leader's SyncGroup made the group Stable.
    let c = GroupCoordinator::new();
    let (leader, follower, gen) = completing_pair(&c, "g");
    c.sync_now("g".to_string(), leader.clone(), gen, vec![])
        .unwrap();
    assert_eq!(state(&c, "g"), GroupState::Stable);
    c.heartbeat("g", &follower, gen).unwrap();
    // Within the timeout nothing happens.
    assert_eq!(c.check_and_remove_timed_out_members(), []);
    age_rebalance(&c, "g", Duration::from_secs(61));
    c.heartbeat("g", &follower, gen).unwrap();
    assert_eq!(
        c.check_and_remove_timed_out_members(),
        vec![("g".to_string(), follower.clone())]
    );
    assert_eq!(state(&c, "g"), GroupState::PreparingRebalance);
    assert_eq!(
        c.heartbeat("g", &follower, gen)
            .unwrap_err()
            .to_kafka_error_code(),
        crate::kafka::constants::ERROR_UNKNOWN_MEMBER_ID
    );
    // A generation whose members all synced keeps them however long it lasts.
    let l = joined(&join(&c, "g", Some(&leader)));
    c.sync_now("g".to_string(), leader.clone(), l.generation_id, vec![])
        .unwrap();
    {
        let mut groups = c.groups.write().unwrap();
        let mut g = groups.get_mut("g").unwrap();
        assert!(g.sync_started_at.is_none());
        g.rebalance_started_at = Some(ago(Duration::from_secs(600)));
    }
    assert_eq!(c.check_and_remove_timed_out_members(), []);
    assert_eq!(state(&c, "g"), GroupState::Stable);
}

#[test]
fn test_no_member_syncing_empties_the_group() {
    // Nobody syncs: every member of the generation is removed, the leader and the
    // followers alike (Kafka 3.7), and the group is Empty.
    let c = GroupCoordinator::new();
    let (_, _, _) = completing_pair(&c, "g");
    age_rebalance(&c, "g", Duration::from_secs(61));
    assert_eq!(c.check_and_remove_timed_out_members().len(), 2);
    assert_eq!(state(&c, "g"), GroupState::Empty);
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
    // An empty group is kept until its TTL, then dropped by the sweep.
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
    // A group is capped at pg_kafka.group_max_size (default) new members.
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

/// Server-side assignments are computed only for the SyncGroup `sync_group` takes
/// assignments from: the leader's, at the current generation, in the sync phase.
#[test]
fn test_awaits_leader_sync_only_in_the_sync_phase() {
    let c = GroupCoordinator::new();
    let (leader, follower, gen) = completing_pair(&c, "g");
    assert!(c.awaits_leader_sync("g", &leader, gen));
    assert!(
        !c.awaits_leader_sync("g", &leader, gen - 1),
        "stale generation"
    );
    assert!(
        !c.awaits_leader_sync("g", &leader, gen + 1),
        "future generation"
    );
    assert!(!c.awaits_leader_sync("g", &follower, gen), "follower");
    assert!(!c.awaits_leader_sync("nope", &leader, gen), "unknown group");
    assert!(
        !c.awaits_leader_sync("g", "stranger", gen),
        "unknown member"
    );
    c.sync_now(
        "g".to_string(),
        leader.clone(),
        gen,
        vec![(leader.clone(), b"A".to_vec())],
    )
    .unwrap();
    assert_eq!(state(&c, "g"), GroupState::Stable);
    assert!(!c.awaits_leader_sync("g", &leader, gen), "Stable group");
    // A rejoin by the leader starts a join phase: still no SyncGroup is awaited.
    let _ = join(&c, "g", Some(&leader));
    assert_eq!(state(&c, "g"), GroupState::PreparingRebalance);
    assert!(!c.awaits_leader_sync("g", &leader, gen));
}

/// Encoded subscription metadata of `topics`.
fn subscription(topics: &[&str]) -> Vec<u8> {
    MemberSubscription::new(topics.iter().map(|t| (*t).to_string()).collect())
        .encode()
        .unwrap()
}

/// A server-side assignment over `limits::MAX_ASSIGNMENT_WORK` entries is refused with
/// POLICY_VIOLATION before it is computed: subscriptions are counted from the array length
/// the metadata claims, before parsing, and the partitions to assign before assigning.
#[test]
fn test_assignment_over_the_work_limit_is_refused() {
    let max = crate::kafka::limits::MAX_ASSIGNMENT_WORK;
    let policy = crate::kafka::constants::ERROR_POLICY_VIOLATION;

    // Metadata claiming more topics than the limit.
    let c = GroupCoordinator::new();
    let mut claimed = vec![0, 0];
    claimed.extend_from_slice(&i32::try_from(max + 1).unwrap().to_be_bytes());
    joined(&join_with(&c, &params("g", None, &[("range", &claimed)])));
    assert_eq!(
        c.get_subscribed_topics("g")
            .unwrap_err()
            .to_kafka_error_code(),
        policy
    );

    // A small subscription over more partitions than the limit.
    let c = GroupCoordinator::new();
    let names: Vec<String> = (0..=max / 100_000).map(|i| format!("t{i}")).collect();
    let refs: Vec<&str> = names.iter().map(String::as_str).collect();
    joined(&join_with(
        &c,
        &params("g", None, &[("range", &subscription(&refs))]),
    ));
    assert_eq!(c.get_subscribed_topics("g").unwrap().len(), names.len());
    let partitions: HashMap<String, i32> = names.iter().map(|n| (n.clone(), 100_000)).collect();
    assert_eq!(
        c.compute_assignments("g", &partitions)
            .unwrap_err()
            .to_kafka_error_code(),
        policy
    );
    // Within the limit it is computed.
    let partitions: HashMap<String, i32> = names.iter().map(|n| (n.clone(), 10)).collect();
    assert_eq!(c.compute_assignments("g", &partitions).unwrap().len(), 1);
}

/// A sticky assignment that spends its step budget is refused with POLICY_VIOLATION;
/// range and round-robin have no step budget.
#[test]
fn test_sticky_assignment_over_its_step_budget_is_refused() {
    let input = AssignmentInput::new(
        HashMap::from([
            (
                "a".to_string(),
                MemberSubscription::new(vec!["t".to_string()]),
            ),
            (
                "b".to_string(),
                MemberSubscription::new(vec!["t".to_string()]),
            ),
        ]),
        HashMap::from([("t".to_string(), 100)]),
    );
    let owned = HashMap::from([(
        "a".to_string(),
        MemberAssignment::with_partitions(HashMap::from([("t".to_string(), (0..100).collect())])),
    )]);
    let refused = run_assignor("g", owned.clone(), &input, "sticky", 100).unwrap_err();
    assert_eq!(
        refused.to_kafka_error_code(),
        crate::kafka::constants::ERROR_POLICY_VIOLATION
    );
    let computed = run_assignor(
        "g",
        owned,
        &input,
        "sticky",
        crate::kafka::limits::MAX_ASSIGNMENT_STEPS,
    )
    .unwrap();
    assert_eq!(computed["a"].partition_count(), 50);
    assert_eq!(computed["a"].partitions("t"), (0..50).collect::<Vec<_>>());
    for strategy in ["range", "roundrobin"] {
        let computed = run_assignor("g", HashMap::new(), &input, strategy, 0).unwrap();
        assert_eq!(computed["b"].partition_count(), 50);
    }
}

#[test]
fn test_late_leader_sync_answers_count_as_liveness() {
    // Neither member heartbeats while the sync phase lasts longer than their session
    // timeout (librdkafka sends no Heartbeat while its SyncGroup is outstanding). The
    // SyncGroup answers restart both session clocks, as Kafka's propagateAssignment
    // does, so the next sweep evicts nobody.
    let c = GroupCoordinator::new();
    let (leader, follower, gen) = completing_pair(&c, "g");
    let parked = c.sync_park("g".to_string(), follower.clone(), gen, vec![]);
    age_member(&c, "g", &follower, Duration::from_secs(31));
    age_member(&c, "g", &leader, Duration::from_secs(31));
    c.sync_now(
        "g".to_string(),
        leader.clone(),
        gen,
        vec![
            (leader.clone(), b"L".to_vec()),
            (follower.clone(), b"F".to_vec()),
        ],
    )
    .unwrap();
    assert_eq!(synced(&parked).unwrap(), b"F".to_vec());
    assert_eq!(
        c.check_and_remove_timed_out_members(),
        [] as [(String, String); 0]
    );
    assert_eq!(state(&c, "g"), GroupState::Stable);
    assert_eq!(c.get_group_state("g").unwrap().members.len(), 2);
}

#[test]
fn test_follower_released_by_sync_phase_timeout_is_not_session_expired() {
    // The leader never syncs: the sweep evicts it and tells the parked follower to
    // rejoin. That answer restarts the follower's session clock, so the same sweep
    // does not also evict the follower, which had not heartbeated while parked.
    let c = GroupCoordinator::new();
    let (leader, follower, gen) = completing_pair(&c, "g");
    let parked = c.sync_park("g".to_string(), follower.clone(), gen, vec![]);
    age_member(&c, "g", &follower, Duration::from_secs(31));
    age_rebalance(&c, "g", Duration::from_secs(61));
    assert_eq!(
        c.check_and_remove_timed_out_members(),
        vec![("g".to_string(), leader)]
    );
    assert_eq!(
        synced(&parked).unwrap_err().to_kafka_error_code(),
        crate::kafka::constants::ERROR_REBALANCE_IN_PROGRESS
    );
    let f = joined(&join(&c, "g", Some(&follower)));
    assert!(f.is_leader());
    assert_eq!(f.generation_id, gen + 1);
}

// ----- memory budget (pg_kafka.max_group_memory) -----

/// What every group holds, counted afresh: the cached counts must always agree with it.
fn counted_afresh(c: &GroupCoordinator) -> usize {
    c.groups
        .read()
        .unwrap()
        .values()
        .map(ConsumerGroup::count_held_bytes)
        .sum()
}

fn big_params(group: &str, member: Option<&str>, metadata_bytes: usize) -> JoinGroupParams {
    let mut p = params(group, member, &[("range", b"")]);
    p.protocols[0].1 = vec![7; metadata_bytes];
    p
}

#[test]
fn test_held_bytes_follow_every_operation() {
    let c = GroupCoordinator::new();
    assert_eq!(c.held_bytes(), 0);
    let (leader, follower, generation) = stable_pair(&c, "g");
    assert_eq!(c.held_bytes(), counted_afresh(&c));
    let before = c.held_bytes();
    // A member's metadata is counted while it is stored.
    let slot = join_with(&c, &big_params("g", Some(&follower), 100_000));
    assert!(is_pending(&slot));
    assert_eq!(c.held_bytes(), counted_afresh(&c));
    assert!(
        c.held_bytes() >= before + 99_000,
        "{} {before}",
        c.held_bytes()
    );
    let l = joined(&join(&c, "g", Some(&leader)));
    assert_eq!(l.generation_id, generation + 1);
    joined(&slot);
    // The leader's assignments too (and their copy for the sticky assignor).
    c.sync_now(
        "g".to_string(),
        leader.clone(),
        l.generation_id,
        vec![
            (leader.clone(), vec![1; 50_000]),
            (follower.clone(), vec![2; 50_000]),
        ],
    )
    .unwrap();
    assert_eq!(c.held_bytes(), counted_afresh(&c));
    assert!(c.held_bytes() >= before + 290_000);
    // Released on removal: a leave, a session timeout, the group's deletion.
    c.leave_group("g", &follower).unwrap();
    assert_eq!(c.held_bytes(), counted_afresh(&c));
    assert!(c.held_bytes() < before + 200_000);
    age_member(&c, "g", &leader, Duration::from_secs(120));
    c.check_and_remove_timed_out_members();
    assert_eq!(state(&c, "g"), GroupState::Empty);
    assert_eq!(c.held_bytes(), counted_afresh(&c));
    assert!(c.held_bytes() < 2_000, "{} bytes", c.held_bytes());
    c.remove_group("g");
    assert_eq!(c.held_bytes(), 0);
}

#[test]
fn test_join_over_the_memory_budget_is_refused() {
    let c = GroupCoordinator::new();
    c.set_memory_limit(1_000_000);
    let a = joined(&join_with(&c, &big_params("a", None, 400_000)));
    let b = joined(&join_with(&c, &big_params("b", None, 400_000)));
    // A third member's metadata does not fit: refused, retriable, and no group is left.
    let refused = join_with(&c, &big_params("c", None, 400_000));
    assert_eq!(
        join_error_code(&refused),
        crate::kafka::constants::ERROR_COORDINATOR_NOT_AVAILABLE
    );
    assert!(c.get_group_state("c").is_none());
    // Nor into an existing group.
    assert_eq!(
        join_error_code(&join_with(&c, &big_params("a", None, 400_000))),
        crate::kafka::constants::ERROR_COORDINATOR_NOT_AVAILABLE
    );
    assert_eq!(c.get_group_state("a").unwrap().members.len(), 1);
    // A small member still fits; a rejoin holding no more than before always does.
    joined(&join(&c, "small", None));
    c.set_memory_limit(10_000);
    let rejoin = join_with(&c, &big_params("a", Some(&a.member_id), 400_000));
    assert_eq!(joined(&rejoin).generation_id, a.generation_id);
    // ... but not one that grows.
    assert_eq!(
        join_error_code(&join_with(
            &c,
            &big_params("a", Some(&a.member_id), 400_001)
        )),
        crate::kafka::constants::ERROR_COORDINATOR_NOT_AVAILABLE
    );
    // Freed memory is available again.
    c.set_memory_limit(1_000_000);
    c.leave_group("b", &b.member_id).unwrap();
    joined(&join_with(&c, &big_params("c", None, 400_000)));
    assert_eq!(c.held_bytes(), counted_afresh(&c));
    assert!(c.held_bytes() <= 1_000_000);
}

#[test]
fn test_sync_over_the_memory_budget_is_refused() {
    let c = GroupCoordinator::new();
    c.set_memory_limit(500_000);
    let a = joined(&join(&c, "g", None));
    let held = c.held_bytes();
    // The assignments, and their copy for the sticky assignor, must fit.
    let refused = c.sync_now(
        "g".to_string(),
        a.member_id.clone(),
        a.generation_id,
        vec![(a.member_id.clone(), vec![0; 300_000])],
    );
    assert_eq!(
        refused.unwrap_err().to_kafka_error_code(),
        crate::kafka::constants::ERROR_COORDINATOR_NOT_AVAILABLE
    );
    assert_eq!(state(&c, "g"), GroupState::CompletingRebalance);
    assert_eq!(c.held_bytes(), held, "nothing stored");
    c.sync_now(
        "g".to_string(),
        a.member_id.clone(),
        a.generation_id,
        vec![(a.member_id.clone(), vec![0; 200_000])],
    )
    .unwrap();
    assert_eq!(state(&c, "g"), GroupState::Stable);
    assert_eq!(c.held_bytes(), counted_afresh(&c));
}

#[test]
fn test_held_bytes_after_sync_predicts_the_sync() {
    // The SyncGroup budget check predicts what the group holds after the leader's sync;
    // it must agree with a recount, whatever the assignments name (repeats, strangers,
    // members left out) and whatever the previous generation left behind.
    let c = GroupCoordinator::new();
    let (leader, follower, _) = stable_pair(&c, "g");
    let third = join(&c, "g", None);
    let f = join(&c, "g", Some(&follower));
    let l = joined(&join(&c, "g", Some(&leader)));
    joined(&third);
    joined(&f);
    let assignments = vec![
        (leader.clone(), vec![1; 10]),
        (leader.clone(), vec![1; 1_000]),
        ("stranger".to_string(), vec![9; 5_000]),
        (follower.clone(), Vec::new()),
    ];
    let predicted = c
        .get_group_state("g")
        .unwrap()
        .held_bytes_after_sync(&assignments);
    c.sync_now("g".to_string(), leader, l.generation_id, assignments)
        .unwrap();
    let group = c.get_group_state("g").unwrap();
    assert_eq!(group.state, GroupState::Stable);
    assert_eq!(predicted, group.count_held_bytes());
    assert_eq!(predicted, group.held_bytes());
}

#[test]
fn test_memory_budget_bounds_never_synced_joins() {
    // The review's reproduction, scaled down: JoinGroups to new groups that never sync,
    // each with large metadata and the longest session timeout. The coordinator's member
    // state stays within the budget however many arrive.
    let c = GroupCoordinator::new();
    c.set_memory_limit(5_000_000);
    let mut admitted = 0;
    for i in 0..30 {
        let mut p = big_params(&format!("g{i}"), None, 1_000_000);
        p.session_timeout_ms = MAX_SESSION_TIMEOUT_MS;
        let slot = join_with(&c, &p);
        if slot.lock().unwrap().as_ref().is_some_and(Result::is_ok) {
            admitted += 1;
        }
        assert!(c.held_bytes() <= 5_000_000);
    }
    assert_eq!(admitted, 4);
    assert_eq!(c.held_bytes(), counted_afresh(&c));
}

/// Join a new group under a long id and leave it, leaving an Empty group behind.
fn join_and_leave(c: &GroupCoordinator, group: &str) -> Result<()> {
    let slot = join_with(c, &params(group, None, &[("range", b"sub")]));
    let outcome = slot.lock().unwrap().take().expect("join answered at once");
    let member = outcome?.member_id;
    c.leave_group(group, &member)
}

#[test]
fn test_empty_groups_are_forgotten_when_memory_is_short() {
    // The review's reproduction, scaled down: each JoinGroup and LeaveGroup under a long
    // group id leaves an Empty group holding that id. Those groups must not keep their bytes
    // in the budget: the least recently active are forgotten to make room, as at the group
    // count cap.
    let c = GroupCoordinator::new();
    c.set_memory_limit(300_000);
    let long = |i: usize| format!("{i}-{}", "x".repeat(30_000));
    for i in 0..40 {
        join_and_leave(&c, &long(i))
            .unwrap_or_else(|e| panic!("group {i}: error {}", e.to_kafka_error_code()));
        assert!(c.held_bytes() <= 300_000, "{} bytes", c.held_bytes());
    }
    // A new group, and a new member of a group in use, still fit.
    let a = joined(&join(&c, "orders", None));
    let slot = join_with(&c, &big_params("orders", None, 100_000));
    assert!(is_pending(&slot), "joins the rebalance");
    assert_eq!(c.held_bytes(), counted_afresh(&c));
    // The least recently active Empty groups went first; the latest is still there.
    assert!(c.get_group_state(&long(0)).is_none());
    assert_eq!(
        c.get_group_state(&long(39)).map(|g| g.state),
        Some(GroupState::Empty)
    );
    // A leader's assignments make room the same way.
    for i in 40..44 {
        join_and_leave(&c, &long(i)).unwrap();
    }
    let l = joined(&join(&c, "orders", Some(&a.member_id)));
    let follower = joined(&slot);
    c.sync_now(
        "orders".to_string(),
        l.member_id.clone(),
        l.generation_id,
        vec![
            (l.member_id.clone(), vec![1; 30_000]),
            (follower.member_id.clone(), vec![2; 30_000]),
        ],
    )
    .unwrap();
    assert_eq!(state(&c, "orders"), GroupState::Stable);
    assert_eq!(c.held_bytes(), counted_afresh(&c));
    assert!(c.held_bytes() <= 300_000, "{} bytes", c.held_bytes());
}

#[test]
fn test_empty_groups_stay_when_forgetting_them_would_not_make_room() {
    let c = GroupCoordinator::new();
    c.set_memory_limit(1_000_000);
    joined(&join_with(&c, &big_params("busy", None, 800_000)));
    join_and_leave(&c, &"e".repeat(30_000)).unwrap();
    // 800 kB in use plus a 300 kB member cannot fit even without the Empty group.
    assert_eq!(
        join_error_code(&join_with(&c, &big_params("new", None, 300_000))),
        crate::kafka::constants::ERROR_COORDINATOR_NOT_AVAILABLE
    );
    assert_eq!(
        c.get_group_state(&"e".repeat(30_000)).map(|g| g.state),
        Some(GroupState::Empty)
    );
    // The Empty group a JoinGroup targets is not forgotten to make room for it.
    let target = "t".repeat(30_000);
    join_and_leave(&c, &target).unwrap();
    joined(&join_with(&c, &big_params(&target, None, 100_000)));
    assert!(c.get_group_state(&"e".repeat(30_000)).is_none());
    assert_eq!(c.get_group_state(&target).map(|g| g.members.len()), Some(1));
}

#[test]
fn test_new_group_is_priced_with_what_the_join_stores() {
    // A group-creating JoinGroup stores the group's own copies of the protocol type, the
    // leader's id and the protocol name besides the member: the admission must price them.
    let mut p = params("g", None, &[]);
    p.protocol_type = "t".repeat(30_000);
    p.protocols = vec![("p".repeat(30_000), Vec::new())];
    let unbounded = GroupCoordinator::new();
    joined(&join_with(&unbounded, &p));
    let held = unbounded.held_bytes();
    assert_eq!(held, counted_afresh(&unbounded));
    let c = GroupCoordinator::new();
    c.set_memory_limit(held - 1);
    assert_eq!(
        join_error_code(&join_with(&c, &p)),
        crate::kafka::constants::ERROR_COORDINATOR_NOT_AVAILABLE
    );
    c.set_memory_limit(held);
    joined(&join_with(&c, &p));
    assert_eq!(c.held_bytes(), held);
}

/// Admission of `p` (run by `setup` first on a fresh coordinator) must price exactly what
/// the join leaves counted: it is refused one byte short of that and admitted at it.
fn assert_join_priced_exactly(
    setup: impl Fn(&GroupCoordinator) -> JoinGroupParams,
    answered_at_once: bool,
) {
    let unbounded = GroupCoordinator::new();
    let p = setup(&unbounded);
    let slot = join_with(&unbounded, &p);
    assert_eq!(!is_pending(&slot), answered_at_once);
    let held = unbounded.held_bytes();
    assert_eq!(held, counted_afresh(&unbounded));

    let c = GroupCoordinator::new();
    let p = setup(&c);
    c.set_memory_limit(held - 1);
    assert_eq!(
        join_error_code(&join_with(&c, &p)),
        crate::kafka::constants::ERROR_COORDINATOR_NOT_AVAILABLE,
        "admitted beyond the {held} bytes it leaves counted"
    );
    c.set_memory_limit(held);
    let slot = join_with(&c, &p);
    assert_eq!(!is_pending(&slot), answered_at_once);
    if answered_at_once {
        joined(&slot);
    }
    assert_eq!(c.held_bytes(), held);
}

/// A Stable group of one member: joined, synced, and the rejoin params for its member id
/// with `protocols`.
fn lone_stable_member(c: &GroupCoordinator, protocols: &[(&str, &[u8])]) -> JoinGroupParams {
    let a = joined(&join(c, "g", None));
    c.sync_now(
        "g".into(),
        a.member_id.clone(),
        a.generation_id,
        vec![(a.member_id.clone(), vec![])],
    )
    .unwrap();
    params("g", Some(&a.member_id), protocols)
}

#[test]
fn test_a_join_that_completes_at_once_is_priced_with_the_protocol_it_selects() {
    // A lone Stable member on "range" rejoins with one long protocol: the join phase
    // completes at once and the group's protocol name becomes the long one. Pricing the
    // group's current name admitted it 29 000 bytes over the budget.
    let long = "P".repeat(30_000);
    assert_join_priced_exactly(|c| lone_stable_member(c, &[(long.as_str(), b"sub")]), true);
}

#[test]
fn test_a_new_group_is_priced_with_what_its_count_holds() {
    // A lone member with a short and a long protocol: the group is priced and counted at the
    // longest name it could select, so the join is admitted exactly at its count (pricing
    // the longest but counting the selected short one refused a join that fit).
    let long = "P".repeat(30_000);
    assert_join_priced_exactly(
        |_| params("g", None, &[("a", b"s"), (long.as_str(), b"s")]),
        true,
    );
}

#[test]
fn test_a_held_join_is_priced_with_the_protocol_a_later_completion_selects() {
    // Two Stable members on "range"; the follower rejoins with a long protocol it prefers.
    // The join is held; if the leader then leaves, the phase completes with the follower
    // alone and its long protocol, with no JoinGroup to refuse: the held join is priced so.
    let long = "P".repeat(30_000);
    let two_stable = |c: &GroupCoordinator| {
        let a = joined(&join(c, "g", None));
        let sb = join(c, "g", None);
        let a = joined(&join(c, "g", Some(&a.member_id)));
        let b = joined(&sb);
        let assignments = vec![(a.member_id.clone(), vec![]), (b.member_id.clone(), vec![])];
        c.sync_now(
            "g".into(),
            a.member_id.clone(),
            a.generation_id,
            assignments,
        )
        .unwrap();
        c.sync_now("g".into(), b.member_id.clone(), b.generation_id, vec![])
            .unwrap();
        assert_eq!(state(c, "g"), GroupState::Stable);
        (a, b)
    };
    assert_join_priced_exactly(
        |c| {
            let (_, b) = two_stable(c);
            params(
                "g",
                Some(&b.member_id),
                &[(long.as_str(), b"sub"), ("range", b"sub")],
            )
        },
        false,
    );

    let c = GroupCoordinator::new();
    let (a, b) = two_stable(&c);
    let held_join = join_with(
        &c,
        &params(
            "g",
            Some(&b.member_id),
            &[(long.as_str(), b"sub"), ("range", b"sub")],
        ),
    );
    let before = c.held_bytes();
    c.leave_group("g", &a.member_id).unwrap();
    assert_eq!(
        joined(&held_join).protocol_name.as_deref(),
        Some(long.as_str())
    );
    assert_eq!(c.held_bytes(), counted_afresh(&c));
    assert!(
        c.held_bytes() < before,
        "a LeaveGroup grew the count from {before} to {}",
        c.held_bytes()
    );
}

#[test]
fn test_a_new_leader_is_not_counted_beyond_the_departed_one() {
    // The leader (short member id) leaves; the follower, whose member id carries a 30 000-byte
    // client id, becomes the leader and the group copies its id. The count must not grow on
    // a LeaveGroup ("the memory frees as members leave").
    let c = GroupCoordinator::new();
    let a = joined(&join(&c, "g", None));
    let mut pb = params("g", None, &[("range", b"sub")]);
    pb.client_id = "c".repeat(30_000);
    let sb = join_with(&c, &pb);
    let a = joined(&join(&c, "g", Some(&a.member_id)));
    let b = joined(&sb);
    assert_eq!(b.leader_id, a.member_id);
    let before = c.held_bytes();
    c.leave_group("g", &a.member_id).unwrap();
    let group = c.get_group_state("g").unwrap();
    assert_eq!(group.leader.as_deref(), Some(b.member_id.as_str()));
    assert_eq!(c.held_bytes(), counted_afresh(&c));
    assert!(
        c.held_bytes() < before,
        "a LeaveGroup grew the count from {before} to {}",
        c.held_bytes()
    );
}

#[test]
fn test_a_stale_generation_sync_forgets_no_empty_group() {
    // A leader SyncGroup with assignments that need room, at a generation that is not the
    // group's: it is answered ILLEGAL_GENERATION and must change nothing, not even forget
    // the Empty groups the room would have come from.
    let c = GroupCoordinator::new();
    let e = "e".repeat(60_000);
    join_and_leave(&c, &e).unwrap();
    let a = joined(&join(&c, "g", None));
    let sb = join(&c, "g", None);
    let a = joined(&join(&c, "g", Some(&a.member_id)));
    joined(&sb);
    assert_eq!(state(&c, "g"), GroupState::CompletingRebalance);
    c.set_memory_limit(c.held_bytes() + 50_000);
    let stale = c.sync_now(
        "g".into(),
        a.member_id.clone(),
        a.generation_id - 1,
        vec![(a.member_id.clone(), vec![1; 80_000])],
    );
    assert_eq!(
        stale.unwrap_err().to_kafka_error_code(),
        crate::kafka::constants::ERROR_ILLEGAL_GENERATION
    );
    assert!(
        c.get_group_state(&e).is_some(),
        "the Empty group was forgotten"
    );
    // At its generation the same SyncGroup makes room by forgetting it.
    c.sync_now(
        "g".into(),
        a.member_id.clone(),
        a.generation_id,
        vec![(a.member_id.clone(), vec![1; 80_000])],
    )
    .unwrap();
    assert!(c.get_group_state(&e).is_none());
}

/// A one-member "sticky" group "g" subscribed to topic "t", whose member's stored previous
/// assignment is `previous`; returns the member id.
fn sticky_member_with_previous(c: &GroupCoordinator, previous: Vec<u8>) -> String {
    let member = joined(&join_with(
        c,
        &params("g", None, &[("sticky", &subscription(&["t"]))]),
    ))
    .member_id;
    let mut groups = c.groups.write().unwrap();
    groups
        .get_mut("g")
        .unwrap()
        .previous_assignments
        .insert(member.clone(), previous);
    member
}

/// An encoded MemberAssignment (v0) of `entries` entries named by `name` with an empty
/// partition list, after one entry of topic "t" holding partition 0.
fn padded_previous(entries: usize, name: impl Fn(usize) -> Option<String>) -> Vec<u8> {
    let mut b = vec![0, 0];
    b.extend_from_slice(&i32::try_from(entries + 1).unwrap().to_be_bytes());
    b.extend_from_slice(&1_i16.to_be_bytes());
    b.push(b't');
    b.extend_from_slice(&1_i32.to_be_bytes());
    b.extend_from_slice(&0_i32.to_be_bytes());
    for i in 0..entries {
        match name(i) {
            Some(name) => {
                b.extend_from_slice(&i16::try_from(name.len()).unwrap().to_be_bytes());
                b.extend_from_slice(name.as_bytes());
                b.extend_from_slice(&0_i32.to_be_bytes());
            }
            None => b.extend_from_slice(&(-1_i16).to_be_bytes()),
        }
    }
    b.extend_from_slice(&(-1_i32).to_be_bytes());
    b
}

/// Previous-assignment entries count toward the work limit whatever they hold: an entry
/// with a null name, a name of no existing topic or no partitions costs a parse and a
/// lookup, so a leader storing millions of them (2 bytes each, within max_group_memory)
/// must not make the sticky assignor parse them all.
#[test]
fn test_previous_assignment_entries_count_toward_the_work_limit() {
    let max = crate::kafka::limits::MAX_ASSIGNMENT_WORK;
    let topics = HashMap::from([("t".to_string(), 2)]);
    let c = GroupCoordinator::new();
    sticky_member_with_previous(&c, padded_previous(max, |_| None));
    assert_eq!(
        c.compute_assignments("g", &topics)
            .unwrap_err()
            .to_kafka_error_code(),
        crate::kafka::constants::ERROR_POLICY_VIOLATION
    );

    // Within the limit it is computed, and the member keeps its previous partition.
    let c = GroupCoordinator::new();
    let member = sticky_member_with_previous(&c, padded_previous(1000, |_| None));
    let computed = c.compute_assignments("g", &topics).unwrap();
    let assignment = MemberAssignment::parse(&computed[&member]).unwrap();
    assert_eq!(assignment.partitions("t"), vec![0, 1]);
}

/// The previous assignment is parsed keeping only entries for existing topics with
/// partitions: 200,000 entries naming topics that do not exist (about 2 MB stored) take
/// no memory beyond the kept entry, where parsing them all held a name and a map slot each
/// (about 15 MB here, 584 MB for the 6,000,000 entries max_group_memory allows).
#[test]
fn test_previous_assignment_parse_keeps_only_existing_topics() {
    let topics = HashMap::from([("t".to_string(), 1)]);
    let c = GroupCoordinator::new();
    let member =
        sticky_member_with_previous(&c, padded_previous(200_000, |i| Some(format!("{i:x}"))));
    let (computed, peak) = crate::testing::heap_peak(|| c.compute_assignments("g", &topics));
    let assignment = MemberAssignment::parse(&computed.unwrap()[&member]).unwrap();
    assert_eq!(assignment.partitions("t"), vec![0]);
    assert!(peak < 1 << 20, "computing the assignment held {peak} bytes");
}

/// A refused server-side assignment is remembered for its generation, so the leader's
/// retries are answered without computing; the next generation computes again.
#[test]
fn test_refused_assignment_is_remembered_for_its_generation() {
    let c = GroupCoordinator::new();
    let (leader, _, gen) = completing_pair(&c, "g");
    assert!(c.assignment_refused("g", gen).is_none());
    c.refuse_assignment("g", gen);
    assert_eq!(
        c.assignment_refused("g", gen),
        Some(AssignmentRefusal::TooLarge)
    );
    assert!(
        c.assignment_refused("g", gen + 1).is_none(),
        "another generation"
    );
    assert!(
        c.assignment_refused("other", gen).is_none(),
        "another group"
    );
    // A refusal for a generation the group is no longer at is not recorded.
    c.refuse_assignment("g", gen - 1);
    assert!(c.assignment_refused("g", gen).is_some());
    assert!(
        c.awaits_leader_sync("g", &leader, gen),
        "the group stays in its sync phase"
    );
}

/// The subscriptions a server-side assignment reads keep no member's user data: no assignor
/// reads it, and each member's metadata may carry up to about a megabyte of it, which was
/// copied into the assignor's input (and once more for the subscribed topics) on every
/// computation.
#[test]
fn test_assignment_input_keeps_no_user_data() {
    let c = GroupCoordinator::new();
    let metadata = MemberSubscription {
        version: 0,
        topics: vec!["t".to_string()],
        user_data: Some(vec![7; 512 * 1024]),
    }
    .encode()
    .unwrap();
    joined(&join_with(&c, &params("g", None, &[("range", &metadata)])));
    let groups = c.groups.read().unwrap();
    let subscriptions = member_subscriptions(&groups["g"]);
    assert_eq!(subscriptions.len(), 1);
    for subscription in subscriptions.values() {
        assert_eq!(subscription.topics, ["t"]);
        assert!(subscription.user_data.is_none(), "user data copied");
    }
}

// ----- request cost independent of the number of groups -----

/// The time of `ops` JoinGroups naming an unknown member of an existing group (answered
/// UNKNOWN_MEMBER_ID) and `ops` that create a group and leave it, on a coordinator that holds
/// `groups` Stable groups besides.
fn join_cost(groups: usize, ops: usize) -> Duration {
    let c = GroupCoordinator::new();
    c.set_limits(groups + ops + 10, 10);
    for i in 0..groups {
        stable_single(&c, &format!("g{i}"));
    }
    let started = Instant::now();
    for i in 0..ops {
        assert_eq!(
            join_error_code(&join(&c, "g0", Some("bogus"))),
            crate::kafka::constants::ERROR_UNKNOWN_MEMBER_ID
        );
        let group = format!("new{i}");
        let member = joined(&join(&c, &group, None)).member_id;
        c.leave_group(&group, &member).unwrap();
    }
    started.elapsed()
}

#[test]
fn test_join_cost_is_independent_of_the_number_of_groups() {
    // The review's reproduction: every JoinGroup, also one answered UNKNOWN_MEMBER_ID, ran the
    // TTL and rebalance timeouts of every group and summed what all of them hold (27 ms per
    // JoinGroup at 85 000 groups). Only the named group's state is visited now, so 500 times
    // more groups cost about the same.
    let small = join_cost(100, 500);
    let large = join_cost(50_000, 500);
    println!("100 groups: {small:?}; 50 000 groups: {large:?}");
    assert!(
        large < small * 5 + Duration::from_millis(20),
        "1000 JoinGroups took {large:?} beside 50 000 groups and {small:?} beside 100"
    );
}

/// A small deterministic generator for the randomized sequences below.
struct Rng(u64);

impl Rng {
    fn below(&mut self, n: usize) -> usize {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        usize::try_from(self.0 % u64::try_from(n.max(1)).unwrap()).unwrap()
    }
}

/// The running totals, the Empty-group index, the id index and every group's supported-protocol
/// counts agree with a recount from every group.
fn assert_totals_exact(c: &GroupCoordinator, step: usize, op: usize) {
    let groups = c.groups.read().unwrap();
    let (running, running_empty) = groups.running();
    let (counted, counted_empty) = groups.recounted();
    assert_eq!(running, counted, "totals after step {step} (op {op})");
    assert_eq!(
        running_empty, counted_empty,
        "Empty index after step {step} (op {op})"
    );
    assert!(groups.ids_exact(), "id index after step {step} (op {op})");
    for (id, group) in groups.iter() {
        assert!(
            group.supported_protocols_exact(),
            "supported-protocol counts of {id} after step {step} (op {op})"
        );
    }
}

#[test]
fn test_running_totals_stay_exact_under_random_operations() {
    for seed in 1..=6_u64 {
        let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15));
        let c = GroupCoordinator::new();
        let ids: Vec<String> = (0..8)
            .map(|i| format!("g{i}-{}", "x".repeat(i * 7)))
            .collect();
        let mut slots: Vec<Slot> = Vec::new();
        for step in 0..1_500 {
            let group = ids[rng.below(ids.len())].clone();
            let snapshot = c.get_group_state(&group);
            let members: Vec<String> = snapshot
                .as_ref()
                .map_or_else(Vec::new, |g| g.members.keys().cloned().collect());
            let member = (!members.is_empty()).then(|| members[rng.below(members.len())].clone());
            let op = rng.below(11);
            match op {
                0 | 1 => {
                    let metadata = vec![7_u8; rng.below(300)];
                    let protocol = ["range", "roundrobin"][rng.below(2)];
                    slots.push(join_with(
                        &c,
                        &params(&group, None, &[(protocol, &metadata)]),
                    ));
                }
                2 => {
                    if let Some(m) = &member {
                        let metadata = vec![1_u8; rng.below(50)];
                        slots.push(join_with(
                            &c,
                            &params(&group, Some(m), &[("range", &metadata)]),
                        ));
                    }
                }
                3 => {
                    if let Some(m) = &member {
                        let _ = c.leave_group(&group, m);
                    }
                }
                4 => {
                    if let Some(g) = &snapshot {
                        if let Some(leader) = g.leader.clone() {
                            let assignments = g
                                .members
                                .keys()
                                .map(|m| (m.clone(), vec![3_u8; rng.below(400)]))
                                .collect();
                            c.sync_park(group.clone(), leader, g.generation_id, assignments);
                        }
                    }
                }
                5 => {
                    if let (Some(m), Some(g)) = (&member, &snapshot) {
                        let _ = c.heartbeat(&group, m, g.generation_id);
                        c.sync_park(group.clone(), m.clone(), g.generation_id, Vec::new());
                    }
                }
                6 => {
                    if let Some(m) = &member {
                        age_member(&c, &group, m, Duration::from_secs(31));
                    }
                    c.check_and_remove_timed_out_members();
                }
                7 => {
                    if snapshot
                        .as_ref()
                        .is_some_and(|g| g.state == GroupState::Empty)
                    {
                        c.groups
                            .write()
                            .unwrap()
                            .get_mut(&group)
                            .unwrap()
                            .last_activity = ago(Duration::from_secs(EMPTY_GROUP_TTL_SECS + 1));
                        if rng.below(2) == 0 {
                            c.check_and_remove_timed_out_members();
                        } else {
                            join(&c, &group, None);
                        }
                    }
                }
                8 => {
                    // A tight memory budget, so the join forgets Empty groups to fit.
                    let held = c.held_bytes();
                    c.set_memory_limit(held.saturating_sub(rng.below(3_000)) + 1_500);
                    slots.push(join(&c, &format!("mem{step}"), None));
                    c.set_memory_limit(crate::kafka::limits::DEFAULT_MAX_GROUP_MEMORY_BYTES);
                }
                9 => {
                    // A lowered group cap, so a new group forgets Empty groups.
                    c.set_limits(1 + rng.below(6), 10);
                    slots.push(join(&c, &format!("cap{step}"), None));
                    c.set_limits(100, 10);
                }
                _ => {
                    if snapshot
                        .as_ref()
                        .is_some_and(|g| g.rebalance_started_at.is_some())
                    {
                        age_rebalance(&c, &group, Duration::from_secs(61));
                        c.check_and_remove_timed_out_members();
                    } else if rng.below(4) == 0 {
                        c.remove_group(&group);
                    }
                }
            }
            assert_totals_exact(&c, step, op);
        }
        drop(slots);
    }
}

#[test]
fn test_logged_ids_are_bounded() {
    assert_eq!(shown("g").to_string(), "g");
    let exact = "x".repeat(SHOWN_ID_BYTES);
    assert_eq!(shown(&exact).to_string(), exact);
    let long = "y".repeat(32 * 1024);
    let logged = shown(&long).to_string();
    assert_eq!(logged, format!("{}...(32768 bytes)", "y".repeat(64)));
    // Cut at a character boundary: 'é' is two bytes, so 64 bytes would split the 33rd.
    let accented = format!("a{}", "é".repeat(100));
    let logged = shown(&accented).to_string();
    assert!(
        logged.starts_with(&format!("a{}...", "é".repeat(31))),
        "{logged}"
    );
    assert!(logged.ends_with("(201 bytes)"));
}

/// `len`-byte protocol names that differ only in their last bytes, so every comparison reads
/// them whole.
fn long_names(indexes: impl Iterator<Item = usize>, len: usize) -> Vec<(String, Vec<u8>)> {
    indexes
        .map(|i| (format!("{}{i:04}", "a".repeat(len - 4)), Vec::new()))
        .collect()
}

#[test]
fn test_protocol_checks_cost_what_the_joiner_sends() {
    // The review's shape: members listing many long names (each member lacks a different
    // one, and lists the rest in reverse), and a joiner whose every name is lacked by some
    // member. Matching each joiner name against every member's list cost O(p_joiner x sum p x
    // len) per JoinGroup (100-270 ms per 1 MiB JoinGroup), also for a refused one, and the same
    // again when a join phase completed.
    let (members, p, len) = (40, 16, 1_000);
    let mut g = ConsumerGroup::new("g".to_string());
    for k in 0..members {
        let mut m = params("g", None, &[]);
        m.protocols = long_names((0..p).rev().filter(|i| *i != k % p), len);
        g.upsert_member(&format!("m{k:03}"), &m);
    }
    let joiner = long_names(0..p, len);
    take_work();
    assert!(!g.supports_protocols(None, "consumer", &joiner));
    let refused = take_work();
    // A known member's rejoin with its own list: its old list is looked at once.
    assert!(!g.supports_protocols(Some("m000"), "consumer", &joiner));
    let rejoined = take_work();
    let joiner_bytes = p * len;
    println!("refused join: {refused} units, rejoin: {rejoined} units");
    assert!(
        refused <= 2 * joiner_bytes,
        "{refused} units for a {joiner_bytes}-byte joiner"
    );
    assert!(
        rejoined <= 4 * joiner_bytes,
        "{rejoined} units for a rejoin"
    );

    // Completion: every member supports the names but the last of each; the vote reads each
    // member's list once.
    let mut g = ConsumerGroup::new("g".to_string());
    for k in 0..members {
        let mut m = params("g", None, &[]);
        m.protocols = long_names((0..p).rev(), len);
        g.upsert_member(&format!("m{k:03}"), &m);
    }
    take_work();
    let chosen = g.select_group_protocol();
    let selected = take_work();
    assert_eq!(
        chosen,
        Some(long_names(std::iter::once(p - 1), len)[0].0.clone())
    );
    let group_bytes = members * p * len;
    println!("completion: {selected} units for {group_bytes} bytes of names");
    assert!(
        selected <= 3 * group_bytes,
        "{selected} units at completion"
    );
}
