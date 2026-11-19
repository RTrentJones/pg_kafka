//! Consumer Group Coordinator
//!
//! This module implements Kafka's consumer group coordination protocol.
//! It manages group membership, partition assignments, and rebalancing.
//!
//! # Design
//!
//! The coordinator uses in-memory state (not persisted to PostgreSQL) matching Kafka's design:
//! - Group membership is ephemeral (rebuilt on restart from JoinGroup requests)
//! - Committed offsets ARE persisted (handled by PostgresStore trait)
//!
//! # State Machine
//!
//! ```text
//!            join            every member rejoined           leader SyncGroup
//! Empty ──────────▶ PreparingRebalance ─────────────▶ CompletingRebalance ─────────▶ Stable
//!   ▲                 ▲   (join phase)   (generation+1)   │  (sync phase)              │
//!   │                 │                                    │ member join/leave/timeout  │
//!   │                 └────────────────────────────────────┴────────────────────────────┘
//!   └──── last member leaves or times out (from any state)
//! ```
//!
//! - **Empty**: No members in the group.
//! - **PreparingRebalance** (the join phase): waiting for every known member to send
//!   JoinGroup. Each JoinGroup response is *held* (see below); members that have not
//!   rejoined learn about the rebalance from Heartbeat (`REBALANCE_IN_PROGRESS`) and may
//!   still commit offsets under the current generation (the commit a consumer makes
//!   when its partitions are revoked).
//! - **CompletingRebalance** (the sync phase): the join completed, the generation was
//!   incremented, and every held JoinGroup was answered with it; waiting for the
//!   leader's SyncGroup.
//! - **Stable**: the leader delivered assignments.
//!
//! The generation therefore only moves when a join phase completes, as in Kafka: a
//! member that has not rejoined yet still holds the current generation.
//!
//! # Held JoinGroup responses
//!
//! As in Kafka, a JoinGroup that starts or joins a join phase is not answered until the
//! phase completes, i.e. until every known member has rejoined (a group whose only
//! member is the joiner completes at once). The phase also completes when the members
//! that have not rejoined are removed: by LeaveGroup, by their session timeout, or when
//! the rebalance timeout (the largest `rebalance_timeout_ms` the members advertised)
//! expires and evicts them. The periodic sweep
//! ([`GroupCoordinator::check_and_remove_timed_out_members`]) runs those timeouts, so
//! a held join completes even when no further request arrives.
//!
//! The caller passes a [`JoinResponder`] with each JoinGroup; the coordinator calls it
//! exactly once, either before `join_group` returns or later from whichever call
//! completes the phase (another member's JoinGroup, a LeaveGroup, or the sweep). A
//! member waiting for its JoinGroup response cannot heartbeat, so its session timeout
//! is suspended while it waits; the rebalance timeout bounds the wait instead.
//!
//! # Parked SyncGroup responses
//!
//! Likewise, a follower's SyncGroup that arrives in CompletingRebalance before the
//! leader's is parked (a [`SyncResponder`]) and answered with the member's assignment
//! when the leader's SyncGroup lands. It is answered REBALANCE_IN_PROGRESS if a new
//! join phase starts first (a member joins or leaves, or the sweep evicts a leader
//! that did not sync within the rebalance timeout), and UNKNOWN_MEMBER_ID if the
//! member itself is removed. Answering it REBALANCE_IN_PROGRESS at once instead made
//! clients rejoin, and a client whose subscription metadata differs on each rejoin
//! (the Java consumer's carries its owned partitions and generation) then started a
//! new rebalance every time. A parked member's session timeout is suspended too.
//!
//! All of this runs on the single background-worker thread (see the single-DB-thread
//! invariant in CLAUDE.md); responders are called after the coordinator's locks are
//! released.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

use super::error::{KafkaError, Result};
use crate::pg_log;

mod group;
#[cfg(test)]
mod tests;

pub use group::{ConsumerGroup, GroupMember, GroupState};
use group::{Delivery, DeliveryOutcome};

// Group-count and group-size caps: `pg_kafka.max_consumer_groups` / `pg_kafka.group_max_size`
// (crate::kafka::limits), applied through `GroupCoordinator::set_limits`.

/// Accepted range for a JoinGroup `session_timeout_ms`, matching Kafka's
/// broker defaults (`group.min.session.timeout.ms` = 6s, `group.max.session.timeout.ms`
/// = 30 min). A request outside this range is rejected with `INVALID_SESSION_TIMEOUT`
/// so a hostile or misconfigured client cannot pin a member alive for weeks (blocking
/// rebalances) or set a sub-second timeout that thrashes the group. Enforced at the
/// request boundary (the JoinGroup handler), not in the group-state core, so unit tests
/// can still drive expiry with tiny timeouts.
pub const MIN_SESSION_TIMEOUT_MS: i32 = 6_000;
pub const MAX_SESSION_TIMEOUT_MS: i32 = 1_800_000;

/// The JoinGroup request fields the coordinator uses.
#[derive(Debug, Clone)]
pub struct JoinGroupParams {
    /// Consumer group ID
    pub group_id: String,
    /// The member's id, or `None` for a first join. An id the coordinator does not
    /// know (e.g. a member evicted earlier) is treated as a new member and given a
    /// fresh id.
    pub member_id: Option<String>,
    /// Client ID from the request header
    pub client_id: String,
    /// Client host (informational, echoed by DescribeGroups)
    pub client_host: String,
    /// Session timeout in milliseconds
    pub session_timeout_ms: i32,
    /// Rebalance timeout in milliseconds (v1+; non-positive for v0)
    pub rebalance_timeout_ms: i32,
    /// Protocol type (e.g. "consumer")
    pub protocol_type: String,
    /// Supported assignment strategies with metadata, in preference order
    pub protocols: Vec<(String, Vec<u8>)>,
    /// Static group instance ID (KIP-345), informational only
    pub group_instance_id: Option<String>,
}

/// The successful answer to one JoinGroup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinResult {
    /// The member id the coordinator assigned (or confirmed)
    pub member_id: String,
    /// The generation the member is now part of
    pub generation_id: i32,
    /// The group leader's member id
    pub leader_id: String,
    /// The protocol the group selected (common to all members)
    pub protocol_name: Option<String>,
    /// For the leader: `(member_id, metadata for the selected protocol)` of every
    /// member, sorted by member id. Empty for followers.
    pub members: Vec<(String, Vec<u8>)>,
}

impl JoinResult {
    /// Whether the receiving member is the group leader.
    pub fn is_leader(&self) -> bool {
        self.leader_id == self.member_id
    }
}

/// What a JoinGroup is eventually answered with.
pub type JoinOutcome = Result<JoinResult>;

/// Receives a JoinGroup's outcome. Called exactly once by the coordinator, possibly
/// after `join_group` has returned (see the module docs).
pub type JoinResponder = Box<dyn FnOnce(JoinOutcome) + Send>;

/// What a SyncGroup is eventually answered with: the member's assignment bytes.
pub type SyncOutcome = Result<Vec<u8>>;

/// Receives a SyncGroup's outcome. Called exactly once by the coordinator, possibly
/// after `sync_group` has returned: a follower's SyncGroup that arrives before the
/// leader's is parked until the leader syncs (see the module docs).
pub type SyncResponder = Box<dyn FnOnce(SyncOutcome) + Send>;

/// How `join_locked` disposed of a JoinGroup.
enum JoinDisposition {
    /// Answer now.
    Respond(JoinOutcome),
    /// The member rejoined a join phase: hold its responder until the phase completes
    /// (which may already have happened, in which case its answer is in the
    /// deliveries).
    Hold { group_id: String, member_id: String },
}

/// Thread-safe consumer group coordinator
///
/// Manages multiple consumer groups with concurrent access from multiple Kafka requests.
/// Uses RwLock for thread-safe access (readers can proceed in parallel, writers get exclusive access).
pub struct GroupCoordinator {
    /// Consumer groups by group_id
    pub groups: Arc<RwLock<HashMap<String, ConsumerGroup>>>,

    /// Responders of held JoinGroups, by (group_id, member_id). An entry exists
    /// exactly while that member's `awaiting_join` is true. Always locked after
    /// `groups` (never the other way round), and never while a responder runs.
    pending_joins: Mutex<HashMap<(String, String), JoinResponder>>,

    /// Responders of parked follower SyncGroups, by (group_id, member_id). An entry
    /// exists exactly while that member's `awaiting_sync` is true. Same locking rules
    /// as `pending_joins` (never both at once).
    pending_syncs: Mutex<HashMap<(String, String), SyncResponder>>,

    /// `pg_kafka.max_consumer_groups`: the most groups held in memory.
    max_groups: AtomicUsize,

    /// `pg_kafka.group_max_size`: the most members one group may have.
    max_group_size: AtomicUsize,
}

impl GroupCoordinator {
    /// Create a new group coordinator
    pub fn new() -> Self {
        Self {
            groups: Arc::new(RwLock::new(HashMap::new())),
            pending_joins: Mutex::new(HashMap::new()),
            pending_syncs: Mutex::new(HashMap::new()),
            max_groups: AtomicUsize::new(
                crate::kafka::limits::DEFAULT_MAX_CONSUMER_GROUPS as usize,
            ),
            max_group_size: AtomicUsize::new(crate::kafka::limits::DEFAULT_GROUP_MAX_SIZE as usize),
        }
    }

    /// Set the group-count and group-size caps (the JoinGroup handler applies the current
    /// `pg_kafka.max_consumer_groups` / `pg_kafka.group_max_size` before every join, so a
    /// reload takes effect at the next JoinGroup). Values below 1 are treated as 1.
    pub fn set_limits(&self, max_groups: usize, max_group_size: usize) {
        self.max_groups.store(max_groups.max(1), Ordering::Relaxed);
        self.max_group_size
            .store(max_group_size.max(1), Ordering::Relaxed);
    }

    fn lock_pending(&self) -> std::sync::MutexGuard<'_, HashMap<(String, String), JoinResponder>> {
        // A poisoned lock only means a writer panicked; the map is still usable.
        self.pending_joins
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn lock_pending_syncs(
        &self,
    ) -> std::sync::MutexGuard<'_, HashMap<(String, String), SyncResponder>> {
        self.pending_syncs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Route queued answers to their held responders and call them. Must be called
    /// with the `groups` lock released.
    fn deliver(&self, deliveries: Vec<Delivery>) {
        if deliveries.is_empty() {
            return;
        }
        let mut joins: Vec<(JoinResponder, JoinOutcome)> = Vec::new();
        let mut syncs: Vec<(SyncResponder, SyncOutcome)> = Vec::new();
        for d in deliveries {
            let key = (d.group_id, d.member_id);
            match d.outcome {
                DeliveryOutcome::Join(outcome) => {
                    if let Some(responder) = self.lock_pending().remove(&key) {
                        joins.push((responder, outcome));
                    }
                }
                DeliveryOutcome::Sync(outcome) => {
                    if let Some(responder) = self.lock_pending_syncs().remove(&key) {
                        syncs.push((responder, outcome));
                    }
                }
            }
        }
        for (responder, outcome) in joins {
            responder(outcome);
        }
        for (responder, outcome) in syncs {
            responder(outcome);
        }
    }

    /// Number of JoinGroup responses currently held (for tests and diagnostics).
    pub fn pending_join_count(&self) -> usize {
        self.lock_pending().len()
    }

    /// Number of SyncGroup responses currently parked (for tests and diagnostics).
    pub fn pending_sync_count(&self) -> usize {
        self.lock_pending_syncs().len()
    }

    /// Whether `member_id` is the leader of `group_id` (false for an unknown group).
    /// The SyncGroup handler computes server-side assignments only for the leader.
    pub fn is_group_leader(&self, group_id: &str, member_id: &str) -> bool {
        self.groups
            .read()
            .ok()
            .and_then(|groups| groups.get(group_id).and_then(|g| g.leader.clone()))
            .is_some_and(|leader| leader == member_id)
    }

    /// Handle a JoinGroup request.
    ///
    /// `responder` is called exactly once with the outcome: before this returns when
    /// the request can be answered at once (an error, a group whose join phase the
    /// joiner completes, or a follower rejoin that does not start a rebalance), or
    /// later when the join phase it took part in completes (see the module docs).
    ///
    /// Rules, by group state (a "known" member is one presenting an id the group has):
    /// - Empty: the member joins and a join phase starts (and, the member being the
    ///   only one, completes at once).
    /// - Stable: a known follower with unchanged protocol metadata is answered with the
    ///   current generation and keeps its assignment; anything else (a new member,
    ///   changed metadata, or the leader rejoining, which is how a leader makes the
    ///   group pick up metadata changes) starts a join phase.
    /// - CompletingRebalance: a known member with unchanged metadata (a retry, e.g. a
    ///   follower whose SyncGroup preceded the leader's) is answered with the current
    ///   generation; anything else starts a new join phase.
    /// - PreparingRebalance: the member joins the phase in progress.
    pub fn join_group(&self, params: &JoinGroupParams, responder: JoinResponder) {
        let mut deliveries = Vec::new();
        let mut replaced = None;
        let mut responder = Some(responder);

        // The outcome to answer now, if this JoinGroup is not held.
        let immediate: Option<JoinOutcome> = match self.groups.write() {
            Ok(mut groups) => match Self::join_locked(
                &mut groups,
                params,
                self.max_groups.load(Ordering::Relaxed),
                self.max_group_size.load(Ordering::Relaxed),
                &mut deliveries,
            ) {
                Ok(JoinDisposition::Respond(outcome)) => Some(outcome),
                Ok(JoinDisposition::Hold {
                    group_id,
                    member_id,
                }) => {
                    // Store the responder before any delivery is routed (the phase may
                    // already be complete, with this member's answer in `deliveries`).
                    if let Some(responder) = responder.take() {
                        replaced = self
                            .lock_pending()
                            .insert((group_id.clone(), member_id), responder)
                            .map(|old| (group_id, old));
                    }
                    None
                }
                Err(e) => Some(Err(e)),
            },
            Err(e) => Some(Err(KafkaError::Internal(format!(
                "Failed to acquire groups write lock: {e}"
            )))),
        };

        // A second JoinGroup from a member whose earlier one is still held replaces
        // it; the earlier request (which the client has given up on) is told to rejoin.
        if let Some((group_id, old)) = replaced {
            old(Err(KafkaError::RebalanceInProgress { group_id }));
        }

        if let (Some(outcome), Some(responder)) = (immediate, responder) {
            responder(outcome);
        }

        self.deliver(deliveries);
    }

    /// The JoinGroup state transition, under the groups write lock. Answers for held
    /// JoinGroups (this one's included) are appended to `out`.
    fn join_locked(
        groups: &mut HashMap<String, ConsumerGroup>,
        params: &JoinGroupParams,
        max_groups: usize,
        max_group_size: usize,
        out: &mut Vec<Delivery>,
    ) -> Result<JoinDisposition> {
        // Clean up stale empty groups (TTL-based)
        groups.retain(|_, g| !g.is_expired_empty());

        // Run the rebalance timeouts (also run by the periodic sweep).
        for group in groups.values_mut() {
            group.expire_stuck_rebalance(out);
        }

        let group_id = params.group_id.clone();

        // As in Kafka: a JoinGroup presenting a member id is a rejoin. If the group or the
        // member is unknown (evicted, or lost in a worker restart) it gets UNKNOWN_MEMBER_ID and
        // the client rejoins with an empty id; it is not silently given a new identity, and no
        // group is created for it.
        if let Some(id) = params.member_id.as_deref() {
            if !groups
                .get(&group_id)
                .is_some_and(|g| g.members.contains_key(id))
            {
                return Err(KafkaError::unknown_member(&group_id, id));
            }
        }

        if !groups.contains_key(&group_id) {
            // A new group must also be a valid one (checked here so a refused join leaves no
            // group behind).
            if params.protocol_type.is_empty() || params.protocols.is_empty() {
                return Err(inconsistent_protocol(&group_id));
            }
            // Bound the coordinator's memory (pg_kafka.max_consumer_groups): make room by
            // forgetting the least recently active Empty group (its committed offsets stay in
            // the database, so it remains visible as an Empty group), else refuse with the
            // retriable COORDINATOR_NOT_AVAILABLE.
            if groups.len() >= max_groups {
                let victim = groups
                    .values()
                    .filter(|g| g.state == GroupState::Empty)
                    .min_by_key(|g| g.last_activity)
                    .map(|g| g.group_id.clone());
                match victim {
                    Some(v) if groups.len() == max_groups => {
                        groups.remove(&v);
                    }
                    _ => {
                        crate::pg_warning!(
                            "JoinGroup for new group '{}' refused: {} groups in memory (pg_kafka.max_consumer_groups = {})",
                            group_id,
                            groups.len(),
                            max_groups
                        );
                        return Err(KafkaError::CoordinatorNotAvailable { group_id });
                    }
                }
            }
        }

        let group = groups
            .entry(group_id.clone())
            .or_insert_with(|| ConsumerGroup::new(group_id.clone()));

        let known_id = params.member_id.clone();

        // Cap members per group (DoS protection; Kafka's group.max.size). A rejoining
        // member (already present) is always allowed; only a new member is rejected.
        if known_id.is_none() && group.members.len() >= max_group_size {
            return Err(KafkaError::Protocol {
                code: crate::kafka::constants::ERROR_GROUP_MAX_SIZE_REACHED,
                message: format!(
                    "group '{group_id}' has reached the maximum of {max_group_size} members"
                ),
            });
        }

        // Kafka's protocol check: the member's protocol type must be the group's and it must
        // share at least one protocol with every other member.
        if !group.supports_protocols(
            known_id.as_deref(),
            &params.protocol_type,
            &params.protocols,
        ) {
            return Err(inconsistent_protocol(&group_id));
        }

        let unchanged = known_id
            .as_ref()
            .and_then(|id| group.members.get(id))
            .is_some_and(|m| m.protocols == params.protocols);
        let member_id = known_id.unwrap_or_else(|| generate_member_id(&params.client_id));
        let is_leader = group.leader.as_deref() == Some(member_id.as_str());

        pg_log!(
            "JoinGroup: group_id={}, member_id={}, state={:?}",
            group.group_id,
            member_id,
            group.state
        );

        match group.state {
            GroupState::Stable if unchanged && !is_leader => {
                group.upsert_member(&member_id, params);
                return Ok(JoinDisposition::Respond(Ok(
                    group.join_result_for(&member_id)
                )));
            }
            GroupState::CompletingRebalance if unchanged => {
                group.upsert_member(&member_id, params);
                return Ok(JoinDisposition::Respond(Ok(
                    group.join_result_for(&member_id)
                )));
            }
            GroupState::Empty | GroupState::Stable | GroupState::CompletingRebalance => {
                group.upsert_member(&member_id, params);
                group.prepare_rebalance(out);
            }
            GroupState::PreparingRebalance => {
                group.upsert_member(&member_id, params);
            }
        }

        if let Some(member) = group.members.get_mut(&member_id) {
            member.awaiting_join = true;
        }
        group.maybe_complete_join(out);

        Ok(JoinDisposition::Hold {
            group_id,
            member_id,
        })
    }

    /// Handle a SyncGroup request.
    ///
    /// `responder` is called exactly once with the member's assignment or an error,
    /// as in Kafka:
    /// - unknown group or member: UNKNOWN_MEMBER_ID; generation other than the
    ///   current one: ILLEGAL_GENERATION; PreparingRebalance: REBALANCE_IN_PROGRESS
    ///   (the member must rejoin);
    /// - CompletingRebalance, leader: its assignments are stored, the group becomes
    ///   Stable, and the leader and every parked follower are answered with their
    ///   assignments (empty for members the leader left out);
    /// - CompletingRebalance, follower: parked until the leader's SyncGroup arrives.
    ///   It is answered REBALANCE_IN_PROGRESS if a new rebalance starts first (a
    ///   member joins or leaves, or the leader misses the rebalance timeout and is
    ///   evicted by the sweep), and UNKNOWN_MEMBER_ID if the member is removed;
    /// - Stable: the stored assignment.
    pub fn sync_group(
        &self,
        group_id: &str,
        member_id: &str,
        generation_id: i32,
        assignments: Vec<(String, Vec<u8>)>, // (member_id, assignment_bytes)
        responder: SyncResponder,
    ) {
        let mut deliveries = Vec::new();
        let mut replaced = None;
        let mut responder = Some(responder);

        let immediate: Option<SyncOutcome> = match self.groups.write() {
            Ok(mut groups) => match Self::sync_locked(
                &mut groups,
                group_id,
                member_id,
                generation_id,
                assignments,
                &mut deliveries,
            ) {
                Ok(Some(assignment)) => Some(Ok(assignment)),
                Ok(None) => {
                    // Parked. A resend from a member whose SyncGroup is already parked
                    // replaces it; the earlier request is told to rejoin.
                    if let Some(responder) = responder.take() {
                        replaced = self
                            .lock_pending_syncs()
                            .insert((group_id.to_string(), member_id.to_string()), responder);
                    }
                    None
                }
                Err(e) => Some(Err(e)),
            },
            Err(e) => Some(Err(KafkaError::Internal(format!(
                "Failed to acquire groups write lock: {e}"
            )))),
        };

        if let Some(old) = replaced {
            old(Err(KafkaError::RebalanceInProgress {
                group_id: group_id.to_string(),
            }));
        }
        if let (Some(outcome), Some(responder)) = (immediate, responder) {
            responder(outcome);
        }
        self.deliver(deliveries);
    }

    /// The SyncGroup state transition, under the groups write lock. `Ok(None)` means
    /// the member's SyncGroup is parked (its `awaiting_sync` is now set).
    fn sync_locked(
        groups: &mut HashMap<String, ConsumerGroup>,
        group_id: &str,
        member_id: &str,
        generation_id: i32,
        assignments: Vec<(String, Vec<u8>)>,
        out: &mut Vec<Delivery>,
    ) -> Result<Option<Vec<u8>>> {
        // A group this coordinator has no record of (e.g. lost in a worker restart):
        // UNKNOWN_MEMBER_ID, as Kafka answers, makes the client rejoin.
        // COORDINATOR_NOT_AVAILABLE made it look up the same coordinator and retry.
        let group = groups
            .get_mut(group_id)
            .ok_or_else(|| KafkaError::unknown_member(group_id, member_id))?;

        // As in Kafka: an unknown member first (it must rejoin as a new member), then
        // a generation other than the current one.
        if !group.members.contains_key(member_id) {
            return Err(KafkaError::unknown_member(group_id, member_id));
        }
        if group.generation_id != generation_id {
            return Err(KafkaError::illegal_generation(
                group_id,
                generation_id,
                group.generation_id,
            ));
        }

        let is_leader = group.leader.as_deref() == Some(member_id);
        pg_log!(
            "SyncGroup: group_id={}, member_id={}, is_leader={}, assignments={}, state={:?}",
            group_id,
            member_id,
            is_leader,
            assignments.len(),
            group.state
        );

        match group.state {
            // SyncGroup is only valid once the join phase has completed. During
            // PreparingRebalance the membership is still settling, so the member must
            // rejoin (clients react to REBALANCE_IN_PROGRESS by rejoining).
            GroupState::PreparingRebalance | GroupState::Empty => {
                Err(KafkaError::RebalanceInProgress {
                    group_id: group_id.to_string(),
                })
            }
            GroupState::CompletingRebalance if is_leader => {
                // Membership cannot have changed since the leader got its member
                // list: any join, leave or eviction after the join phase completed
                // moved the group back to PreparingRebalance. Assignments for ids
                // that are not members are ignored.
                for (target_member_id, assignment_bytes) in assignments {
                    if let Some(member) = group.members.get_mut(&target_member_id) {
                        member.assignment = Some(assignment_bytes);
                    }
                }
                group.complete_sync(out);
                Ok(Some(
                    group
                        .members
                        .get(member_id)
                        .and_then(|m| m.assignment.clone())
                        .unwrap_or_default(),
                ))
            }
            GroupState::CompletingRebalance => {
                // A follower ahead of the leader waits for the leader's assignments.
                if let Some(member) = group.members.get_mut(member_id) {
                    member.awaiting_sync = true;
                }
                Ok(None)
            }
            // Stable always carries an assignment (complete_sync fills in empty ones).
            GroupState::Stable => Ok(Some(
                group
                    .members
                    .get(member_id)
                    .and_then(|m| m.assignment.clone())
                    .unwrap_or_default(),
            )),
        }
    }

    /// Validate an OffsetCommit against the live group state to reject zombie commits.
    ///
    /// A simple consumer with no active group membership commits with `generation_id < 0` and is not
    /// validated. Otherwise the commit's member id and generation must match the current ones — an
    /// unknown member yields `UnknownMemberId` (checked first, as Kafka does) and a stale
    /// generation `IllegalGeneration`.
    ///
    /// A generation-bearing commit for a group this coordinator has *no record of* is fenced
    /// with `UnknownMemberId` (a zombie whose group was reaped by the empty-group sweep, or whose
    /// coordinator restarted), forcing it to rejoin and get a fresh generation. Because coordinator
    /// state is in-memory, this means that after a bgworker restart every consumer holding a
    /// generation must rejoin — a brief rejoin/rebalance storm on restart. That is deliberate and
    /// correct (the old generations are gone), not a regression; the earlier "unknown group is
    /// allowed" behavior would have let a zombie commit through.
    ///
    /// As in Kafka, a current-generation commit is accepted while the group is
    /// PreparingRebalance (the commit a member makes when its partitions are revoked,
    /// before it rejoins; the generation has not moved yet) and fenced with
    /// `RebalanceInProgress` while it is CompletingRebalance (the member's
    /// assignment is about to change and it has not synced yet).
    pub fn validate_commit(
        &self,
        group_id: &str,
        member_id: &str,
        generation_id: i32,
    ) -> Result<()> {
        if generation_id < 0 {
            return Ok(());
        }

        let groups = self.groups.read().map_err(|e| {
            KafkaError::Internal(format!("Failed to acquire groups read lock: {e}"))
        })?;

        let Some(group) = groups.get(group_id) else {
            return Err(KafkaError::unknown_member(group_id, member_id));
        };

        if !group.members.contains_key(member_id) {
            return Err(KafkaError::unknown_member(group_id, member_id));
        }

        if group.generation_id != generation_id {
            return Err(KafkaError::illegal_generation(
                group_id,
                generation_id,
                group.generation_id,
            ));
        }

        if group.state == GroupState::CompletingRebalance {
            return Err(KafkaError::RebalanceInProgress {
                group_id: group_id.to_string(),
            });
        }

        Ok(())
    }

    /// Validate a (non-transactional) OffsetCommit as Kafka 3.7 does:
    /// - generation < 0 for an unknown or Empty group: a simple consumer (or an admin tool such
    ///   as `kafka-consumer-groups --reset-offsets`) committing outside group management; accepted
    ///   whatever the member id;
    /// - generation >= 0 for a group the coordinator does not know: ILLEGAL_GENERATION (Kafka's
    ///   answer; the consumer rejoins);
    /// - otherwise the member must be in the group (UNKNOWN_MEMBER_ID, which also covers
    ///   generation -1 / empty member id against a group that has members) and the generation
    ///   must be the current one (ILLEGAL_GENERATION); CompletingRebalance gets
    ///   REBALANCE_IN_PROGRESS, and PreparingRebalance accepts the commit of a member that has not
    ///   rejoined yet (see [`Self::validate_commit`]).
    ///
    /// [`Self::validate_commit`] keeps the older rules for TxnOffsetCommit, where Kafka exempts
    /// transactional commits from the Empty-group requirement.
    pub fn validate_offset_commit(
        &self,
        group_id: &str,
        member_id: &str,
        generation_id: i32,
    ) -> Result<()> {
        let groups = self.groups.read().map_err(|e| {
            KafkaError::Internal(format!("Failed to acquire groups read lock: {e}"))
        })?;
        let Some(group) = groups.get(group_id) else {
            return if generation_id < 0 {
                Ok(())
            } else {
                Err(KafkaError::illegal_generation(group_id, generation_id, -1))
            };
        };
        if generation_id < 0 && group.state == GroupState::Empty {
            return Ok(());
        }
        if !group.members.contains_key(member_id) {
            return Err(KafkaError::unknown_member(group_id, member_id));
        }
        if group.generation_id != generation_id {
            return Err(KafkaError::illegal_generation(
                group_id,
                generation_id,
                group.generation_id,
            ));
        }
        if group.state == GroupState::CompletingRebalance {
            return Err(KafkaError::RebalanceInProgress {
                group_id: group_id.to_string(),
            });
        }
        Ok(())
    }

    /// The group's selected common protocol, set once a rebalance completes
    /// (`None` if the group is unknown or hasn't completed a join).
    pub fn group_protocol_name(&self, group_id: &str) -> Option<String> {
        let groups = self.groups.read().ok()?;
        groups.get(group_id).and_then(|g| g.protocol_name.clone())
    }

    /// Handle Heartbeat request
    ///
    /// Updates the member's last_heartbeat timestamp. As in Kafka: an unknown group or
    /// member gets UNKNOWN_MEMBER_ID, then a generation other than the current one
    /// ILLEGAL_GENERATION; during the join phase a member gets REBALANCE_IN_PROGRESS
    /// (its cue to rejoin), and during the sync phase and when Stable it gets NONE.
    pub fn heartbeat(&self, group_id: &str, member_id: &str, generation_id: i32) -> Result<()> {
        let mut groups = self.groups.write().map_err(|e| {
            KafkaError::Internal(format!("Failed to acquire groups write lock: {e}"))
        })?;

        // Unknown group (e.g. lost in a worker restart): UNKNOWN_MEMBER_ID so the
        // client rejoins, as Kafka answers.
        let group = groups
            .get_mut(group_id)
            .ok_or_else(|| KafkaError::unknown_member(group_id, member_id))?;

        if !group.members.contains_key(member_id) {
            return Err(KafkaError::unknown_member(group_id, member_id));
        }
        if group.generation_id != generation_id {
            return Err(KafkaError::illegal_generation(
                group_id,
                generation_id,
                group.generation_id,
            ));
        }

        let Some(member) = group.members.get_mut(member_id) else {
            return Err(KafkaError::unknown_member(group_id, member_id));
        };
        member.last_heartbeat = Instant::now();

        if group.state == GroupState::PreparingRebalance {
            return Err(KafkaError::RebalanceInProgress {
                group_id: group_id.to_string(),
            });
        }

        Ok(())
    }

    /// Handle LeaveGroup request
    ///
    /// Removes the member from the group. Remaining members of a Stable or
    /// CompletingRebalance group rebalance; a join phase in progress completes if the
    /// leaver was the last member that had not rejoined.
    pub fn leave_group(&self, group_id: &str, member_id: &str) -> Result<()> {
        let mut deliveries = Vec::new();
        let result = {
            let mut groups = self.groups.write().map_err(|e| {
                KafkaError::Internal(format!("Failed to acquire groups write lock: {e}"))
            })?;

            match groups.get_mut(group_id) {
                None => Err(KafkaError::unknown_member(group_id, member_id)),
                Some(group) => {
                    pg_log!("LeaveGroup: group_id={}, member_id={}", group_id, member_id);
                    if group.remove_member(member_id, &mut deliveries) {
                        group.on_members_removed(&mut deliveries);
                        Ok(())
                    } else {
                        Err(KafkaError::unknown_member(group_id, member_id))
                    }
                }
            }
        };
        self.deliver(deliveries);
        result
    }

    /// Run the periodic group timeouts: rebalance timeouts (evicting members that did
    /// not rejoin a join phase, or a leader that did not sync), then session timeouts
    /// (members not heartbeating; a member whose JoinGroup is held or whose SyncGroup
    /// is parked is exempt, the rebalance timeout applies instead). Completes any join
    /// phase these removals make complete, answering the held JoinGroups, and fails
    /// parked SyncGroups whose sync phase is abandoned.
    ///
    /// Called every ~1 second from the background worker main loop, whether or not
    /// requests arrive, so a held JoinGroup is always answered within about a second
    /// of its phase completing or timing out.
    ///
    /// # Returns
    /// List of (group_id, member_id) pairs that were removed
    pub fn check_and_remove_timed_out_members(&self) -> Vec<(String, String)> {
        let mut removed = Vec::new();
        let mut deliveries = Vec::new();

        {
            let Ok(mut groups) = self.groups.write() else {
                // Lock poisoned, skip this check
                return removed;
            };

            for (group_id, group) in groups.iter_mut() {
                if group.state == GroupState::Empty {
                    continue;
                }

                for member_id in group.expire_stuck_rebalance(&mut deliveries) {
                    removed.push((group_id.clone(), member_id));
                }
                if group.state == GroupState::Empty {
                    continue;
                }

                let mut timed_out: Vec<String> = group
                    .members
                    .values()
                    .filter(|m| !m.awaiting_join && !m.awaiting_sync && m.is_timed_out())
                    .map(|m| m.member_id.clone())
                    .collect();
                timed_out.sort();
                if timed_out.is_empty() {
                    continue;
                }

                for member_id in &timed_out {
                    pg_log!(
                        "Member {} timed out in group {} (session_timeout exceeded)",
                        member_id,
                        group_id
                    );
                    group.remove_member(member_id, &mut deliveries);
                    removed.push((group_id.clone(), member_id.clone()));
                }
                group.on_members_removed(&mut deliveries);
            }

            // Reclaim expired empty groups here (every sweep), not just on the next
            // join_group's pass, so coordinator memory stays bounded. An Empty group is kept until
            // EMPTY_GROUP_TTL_SECS has passed (the same rule join_group applies): dropping it
            // immediately would restart a rejoining group at generation 1, defeating the
            // monotonic generation that fences zombie consumers (see remove_member).
            groups.retain(|_, g| !g.is_expired_empty());
        }

        self.deliver(deliveries);
        removed
    }

    /// A snapshot of a group's state (used by DeleteGroups, and by tests)
    pub fn get_group_state(&self, group_id: &str) -> Option<ConsumerGroup> {
        self.groups
            .read()
            .ok()
            .and_then(|groups| groups.get(group_id).cloned())
    }

    /// Compute partition assignments for all members in a group
    ///
    /// This is called when the leader sends an empty SyncGroup request,
    /// indicating the server should compute assignments.
    ///
    /// # Arguments
    /// * `group_id` - Consumer group ID
    /// * `topic_partitions` - Map of topic_name -> partition_count
    ///
    /// # Returns
    /// Map of member_id -> encoded MemberAssignment bytes
    pub fn compute_assignments(
        &self,
        group_id: &str,
        topic_partitions: &HashMap<String, i32>,
    ) -> Result<HashMap<String, Vec<u8>>> {
        use crate::kafka::assignment::strategies::StickyStrategy;
        use crate::kafka::assignment::{
            create_strategy, AssignmentInput, AssignmentStrategy, MemberAssignment,
            MemberSubscription,
        };

        let groups = self.groups.read().map_err(|e| {
            KafkaError::Internal(format!("Failed to acquire groups read lock: {e}"))
        })?;

        let group = groups
            .get(group_id)
            .ok_or_else(|| KafkaError::CoordinatorNotAvailable {
                group_id: group_id.to_string(),
            })?;

        // Parse subscriptions from member protocol metadata
        let mut subscriptions = HashMap::new();
        for (member_id, member) in &group.members {
            // Find the protocol matching the chosen strategy
            if let Some((_, metadata)) = member
                .protocols
                .iter()
                .find(|(name, _)| Some(name) == group.protocol_name.as_ref())
            {
                match MemberSubscription::parse(metadata) {
                    Ok(sub) => {
                        subscriptions.insert(member_id.clone(), sub);
                    }
                    Err(e) => {
                        pg_log!(
                            "Failed to parse subscription for member {}: {}",
                            member_id,
                            e
                        );
                        // Use empty subscription as fallback
                        subscriptions.insert(member_id.clone(), MemberSubscription::default());
                    }
                }
            } else {
                // No matching protocol, use empty subscription
                subscriptions.insert(member_id.clone(), MemberSubscription::default());
            }
        }

        // Create assignment input
        let input = AssignmentInput::new(subscriptions, topic_partitions.clone());

        // Get strategy name (default to "range")
        let strategy_name = group.protocol_name.as_deref().unwrap_or("range");

        // Create strategy - for sticky, include previous assignments
        let assignments = if strategy_name.contains("sticky") {
            // Parse previous assignments for sticky strategy
            let mut previous = HashMap::new();
            for (member_id, bytes) in &group.previous_assignments {
                if let Ok(assign) = MemberAssignment::parse(bytes) {
                    previous.insert(member_id.clone(), assign);
                }
            }
            let strategy = StickyStrategy::with_previous(previous);
            strategy.assign(&input)
        } else {
            // Use standard strategy
            let strategy = create_strategy(strategy_name).ok_or_else(|| {
                KafkaError::Internal(format!("Unknown assignment strategy: {strategy_name}"))
            })?;
            strategy.assign(&input)
        };

        // Encode assignments to bytes
        let encoded = assignments
            .into_iter()
            .map(|(id, assignment)| Ok((id, assignment.encode()?)))
            .collect::<Result<HashMap<String, Vec<u8>>>>()?;

        pg_log!(
            "Computed assignments for group {} using strategy {}: {} members",
            group_id,
            strategy_name,
            encoded.len()
        );

        Ok(encoded)
    }

    /// Get topics subscribed by members in a group
    ///
    /// Parses subscription metadata from all members to determine
    /// which topics need partition information.
    pub fn get_subscribed_topics(&self, group_id: &str) -> Result<Vec<String>> {
        use crate::kafka::assignment::MemberSubscription;

        let groups = self.groups.read().map_err(|e| {
            KafkaError::Internal(format!("Failed to acquire groups read lock: {e}"))
        })?;

        let group = groups
            .get(group_id)
            .ok_or_else(|| KafkaError::CoordinatorNotAvailable {
                group_id: group_id.to_string(),
            })?;

        let mut topics = std::collections::HashSet::new();

        for member in group.members.values() {
            // Try to parse subscription from the chosen protocol
            if let Some((_, metadata)) = member
                .protocols
                .iter()
                .find(|(name, _)| Some(name) == group.protocol_name.as_ref())
            {
                if let Ok(sub) = MemberSubscription::parse(metadata) {
                    for topic in sub.topics {
                        topics.insert(topic);
                    }
                }
            }
        }

        Ok(topics.into_iter().collect())
    }

    /// Remove a consumer group from the coordinator
    ///
    /// This is used by DeleteGroups to remove empty groups.
    /// Should only be called after confirming the group has no active members; any
    /// JoinGroup still held for the group is answered UNKNOWN_MEMBER_ID.
    pub fn remove_group(&self, group_id: &str) {
        let mut orphaned_joins: Vec<(String, JoinResponder)> = Vec::new();
        let mut orphaned_syncs: Vec<(String, SyncResponder)> = Vec::new();
        if let Ok(mut groups) = self.groups.write() {
            groups.remove(group_id);
            pg_log!("Removed group '{}' from coordinator", group_id);
            let mut pending = self.lock_pending();
            let keys: Vec<(String, String)> = pending
                .keys()
                .filter(|(g, _)| g == group_id)
                .cloned()
                .collect();
            for key in keys {
                if let Some(r) = pending.remove(&key) {
                    orphaned_joins.push((key.1, r));
                }
            }
            drop(pending);
            let mut pending = self.lock_pending_syncs();
            let keys: Vec<(String, String)> = pending
                .keys()
                .filter(|(g, _)| g == group_id)
                .cloned()
                .collect();
            for key in keys {
                if let Some(r) = pending.remove(&key) {
                    orphaned_syncs.push((key.1, r));
                }
            }
        }
        for (member_id, responder) in orphaned_joins {
            responder(Err(KafkaError::unknown_member(group_id, &member_id)));
        }
        for (member_id, responder) in orphaned_syncs {
            responder(Err(KafkaError::unknown_member(group_id, &member_id)));
        }
    }
}

impl Default for GroupCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

/// INCONSISTENT_GROUP_PROTOCOL for a JoinGroup whose protocol type or protocols do not fit the
/// group.
fn inconsistent_protocol(group_id: &str) -> KafkaError {
    KafkaError::Protocol {
        code: crate::kafka::constants::ERROR_INCONSISTENT_GROUP_PROTOCOL,
        message: format!(
            "the member's protocol type or protocols are incompatible with group '{group_id}'"
        ),
    }
}

/// Generate a unique member ID for a consumer
///
/// Format: {client_id}-{uuid}
fn generate_member_id(client_id: &str) -> String {
    use uuid::Uuid;
    let uuid = Uuid::new_v4();
    format!("{client_id}-{uuid}")
}
