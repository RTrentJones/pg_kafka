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
//! join phase starts first (a member joins or leaves, or the sweep evicts the members
//! that did not sync within the rebalance timeout), and UNKNOWN_MEMBER_ID if the
//! member itself is removed. Answering it REBALANCE_IN_PROGRESS at once instead made
//! clients rejoin, and a client whose subscription metadata differs on each rejoin
//! (the Java consumer's carries its owned partitions and generation) then started a
//! new rebalance every time. A parked member's session timeout is suspended too.
//!
//! # Members that never sync
//!
//! As in Kafka 3.0+ (`pendingSyncMembers`), every member of a new generation must send its
//! SyncGroup within the group's rebalance timeout of the join phase's completion. The sweep
//! removes those that have not, whether or not they heartbeat and whether the group is still
//! in its sync phase or already Stable, and the rest rebalance.
//!
//! All of this runs on the single background-worker thread (see the single-DB-thread
//! invariant in CLAUDE.md); responders are called after the coordinator's locks are
//! released.

use crate::kafka::assignment::strategies::StickyStrategy;
use crate::kafka::assignment::{
    create_strategy, AssignmentInput, AssignmentOutput, MemberAssignment, MemberSubscription,
};
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
// (crate::kafka::limits), applied through `GroupCoordinator::set_limits`; the memory budget
// for member state, `pg_kafka.max_group_memory`, through `GroupCoordinator::set_memory_limit`.

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

    /// `pg_kafka.max_group_memory`, in bytes: the most member state (ids, protocols and
    /// their metadata, assignments) all groups together may hold.
    max_group_memory: AtomicUsize,
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
            max_group_memory: AtomicUsize::new(
                crate::kafka::limits::DEFAULT_MAX_GROUP_MEMORY_BYTES,
            ),
        }
    }

    /// Set the memory budget for member state, in bytes (`pg_kafka.max_group_memory`,
    /// applied by the JoinGroup and SyncGroup handlers before every request).
    pub fn set_memory_limit(&self, max_bytes: usize) {
        self.max_group_memory.store(max_bytes, Ordering::Relaxed);
    }

    /// What all groups hold in coordinator memory, as counted against
    /// `pg_kafka.max_group_memory` (0 if the lock is poisoned).
    pub fn held_bytes(&self) -> usize {
        self.groups
            .read()
            .map_or(0, |groups| total_held_bytes(&groups))
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

    /// Whether a SyncGroup from `member_id` at `generation_id` is the one [`Self::sync_group`]
    /// takes the group's assignments from: the leader's, at the current generation, while
    /// the group is in its sync phase (CompletingRebalance). The SyncGroup handler computes
    /// server-side assignments only for it; any other SyncGroup ignores its assignments, so
    /// computing them for a stale generation or a Stable group would be work for nothing.
    pub fn awaits_leader_sync(&self, group_id: &str, member_id: &str, generation_id: i32) -> bool {
        self.groups.read().is_ok_and(|groups| {
            groups.get(group_id).is_some_and(|g| {
                g.state == GroupState::CompletingRebalance
                    && g.generation_id == generation_id
                    && g.members.contains_key(member_id)
                    && g.leader.as_deref() == Some(member_id)
            })
        })
    }

    /// Remember that the server-side assignment of `group_id` at `generation_id` was refused
    /// (POLICY_VIOLATION: over `limits::MAX_ASSIGNMENT_WORK` or `MAX_ASSIGNMENT_STEPS`), so
    /// that [`Self::assignment_refused`] answers the leader's retries in that generation.
    ///
    /// A refusal leaves the group in its sync phase with the same leader and generation, and
    /// the rebalance timeout that ends the phase is the client's (up to `i32::MAX` ms): without
    /// this, one connection pipelining empty SyncGroups would recompute a refused assignment
    /// (reading the subscribed topics and walking every member's subscription and previous
    /// assignment, up to the work limit, each time) for as long as it liked. The refusal holds
    /// until the generation moves (a member joins, leaves or is evicted, or the leader rejoins
    /// with changed metadata, as a client does when its subscribed topics change); a change of
    /// the topics' partitions within the generation does not lift it. A computed assignment
    /// refused by the memory budget is remembered the same way ([`Self::sync_group_computed`]).
    pub fn refuse_assignment(&self, group_id: &str, generation_id: i32) {
        self.remember_refusal(group_id, generation_id, AssignmentRefusal::TooLarge);
    }

    fn remember_refusal(&self, group_id: &str, generation_id: i32, why: AssignmentRefusal) {
        if let Ok(mut groups) = self.groups.write() {
            if let Some(group) = groups
                .get_mut(group_id)
                .filter(|g| g.generation_id == generation_id)
            {
                group.refused_assignment = Some((generation_id, why));
            }
        }
    }

    /// Why the server-side assignment of `group_id` at `generation_id`, its current
    /// generation, was refused ([`Self::refuse_assignment`]), if it was.
    pub fn assignment_refused(
        &self,
        group_id: &str,
        generation_id: i32,
    ) -> Option<AssignmentRefusal> {
        let groups = self.groups.read().ok()?;
        let group = groups.get(group_id)?;
        match group.refused_assignment {
            Some((generation, why))
                if generation == generation_id && group.generation_id == generation_id =>
            {
                Some(why)
            }
            _ => None,
        }
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
                JoinLimits {
                    groups: self.max_groups.load(Ordering::Relaxed),
                    group_size: self.max_group_size.load(Ordering::Relaxed),
                    memory: self.max_group_memory.load(Ordering::Relaxed),
                },
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
        limits: JoinLimits,
        out: &mut Vec<Delivery>,
    ) -> Result<JoinDisposition> {
        // Clean up stale empty groups (TTL-based)
        groups.retain(|_, g| !g.is_expired_empty());

        // Run the rebalance timeouts (also run by the periodic sweep).
        for group in groups.values_mut() {
            expire_and_recount(group, out);
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

        match groups.get(&group_id) {
            Some(group) => Self::admit_member(group, params, limits.group_size)?,
            None => Self::admit_new_group(groups, params, limits.groups)?,
        }

        let known_id = params.member_id.clone();
        let member_id = known_id
            .clone()
            .unwrap_or_else(|| generate_member_id(&params.client_id));
        // Checked before the group is created, so a refused join leaves no group behind.
        Self::admit_memory(groups, params, &member_id, limits.memory)?;

        let group = groups
            .entry(group_id.clone())
            .or_insert_with(|| ConsumerGroup::new(group_id.clone()));
        let unchanged = known_id
            .as_ref()
            .and_then(|id| group.members.get(id))
            .is_some_and(|m| m.protocols == params.protocols);

        pg_log!(
            "JoinGroup: group_id={}, member_id={}, state={:?}",
            group.group_id,
            member_id,
            group.state
        );
        let disposition = Self::apply_join(group, member_id, params, unchanged, out);
        group.recount();
        Ok(disposition)
    }

    /// Whether the member state a JoinGroup stores fits the coordinator's memory budget
    /// (`pg_kafka.max_group_memory`): what all groups hold, less what this member and the
    /// group's own fields (protocol type, leader, protocol name) count before, plus what they
    /// will count after the join (and, for a new group, the group's own record; see
    /// [`ConsumerGroup::count_held_bytes`] for why the leader and protocol name count the
    /// most any member could put there). A rejoin that counts no more than before is always
    /// admitted. When the state does not fit, the
    /// least recently active Empty groups are forgotten until it does (see
    /// [`make_room`]). Kafka has no such bound (its member metadata is bounded by the heap);
    /// the refusal is the retriable COORDINATOR_NOT_AVAILABLE, as for a full
    /// `pg_kafka.max_consumer_groups`, since memory frees as other members leave or time
    /// out (GROUP_MAX_SIZE_REACHED would be fatal to the Java consumer, and names the
    /// group's size, which is not what is exhausted).
    fn admit_memory(
        groups: &mut HashMap<String, ConsumerGroup>,
        params: &JoinGroupParams,
        member_id: &str,
        max_memory: usize,
    ) -> Result<()> {
        let (before, assignment, group_after) = if let Some(group) = groups.get(&params.group_id) {
            let member = group.members.get(member_id);
            let (group_now, group_after, completes) = group.join_held_bytes(member_id, params);
            (
                member.map_or(0, GroupMember::held_bytes) + group_now,
                // A join that completes the phase drops the member's assignment too.
                member
                    .and_then(|m| m.assignment.as_deref())
                    .filter(|_| !completes),
                group_after,
            )
        } else {
            let group = ConsumerGroup::new(params.group_id.clone());
            let (_, group_after, _) = group.join_held_bytes(member_id, params);
            (0, None, group.held_bytes() + group_after)
        };
        let after = group::member_held_bytes(member_id, params, assignment) + group_after;
        if after <= before {
            return Ok(());
        }
        if make_room(groups, &params.group_id, after - before, max_memory) {
            return Ok(());
        }
        let held = total_held_bytes(groups);
        crate::pg_warning!(
            "JoinGroup for group '{}' refused: its member state ({} bytes) does not fit the {} bytes groups hold of pg_kafka.max_group_memory ({} bytes)",
            params.group_id,
            after,
            held,
            max_memory
        );
        Err(KafkaError::CoordinatorNotAvailable {
            group_id: params.group_id.clone(),
        })
    }

    /// Whether a JoinGroup may create the group it names: a new group must be a valid one
    /// (checked first, so a refused join leaves no group behind), and the coordinator's
    /// memory is bounded (pg_kafka.max_consumer_groups). Room is made by forgetting the
    /// least recently active Empty groups (their committed offsets stay in the database,
    /// so they remain visible as Empty groups) until the count is below the cap; when the
    /// non-Empty groups alone reach the cap, nothing is forgotten and the join gets the
    /// retriable COORDINATOR_NOT_AVAILABLE.
    fn admit_new_group(
        groups: &mut HashMap<String, ConsumerGroup>,
        params: &JoinGroupParams,
        max_groups: usize,
    ) -> Result<()> {
        let group_id = &params.group_id;
        if params.protocol_type.is_empty() || params.protocols.is_empty() {
            return Err(inconsistent_protocol(group_id));
        }
        // Forgetting Empty groups only helps if it can get below the cap; when the groups
        // in use alone fill it, the join is refused and the Empty groups stay.
        let in_use = groups
            .values()
            .filter(|g| g.state != GroupState::Empty)
            .count();
        // A reload can lower the cap below the number of groups in memory, so as many
        // Empty groups are forgotten as it takes.
        while in_use < max_groups && groups.len() >= max_groups {
            let victim = groups
                .values()
                .filter(|g| g.state == GroupState::Empty)
                .min_by_key(|g| g.last_activity)
                .map(|g| g.group_id.clone());
            let Some(victim) = victim else { break };
            groups.remove(&victim);
        }
        if groups.len() < max_groups {
            return Ok(());
        }
        crate::pg_warning!(
            "JoinGroup for new group '{}' refused: {} groups in memory (pg_kafka.max_consumer_groups = {})",
            group_id,
            groups.len(),
            max_groups
        );
        Err(KafkaError::CoordinatorNotAvailable {
            group_id: group_id.clone(),
        })
    }

    /// Whether the group admits this member: a new member is refused once the group has
    /// pg_kafka.group_max_size members (Kafka's group.max.size; a rejoining member is
    /// always allowed), and by Kafka's protocol check the member's protocol type must be
    /// the group's and it must share at least one protocol with every other member.
    fn admit_member(
        group: &ConsumerGroup,
        params: &JoinGroupParams,
        max_group_size: usize,
    ) -> Result<()> {
        let known_id = params.member_id.as_deref();
        if known_id.is_none() && group.members.len() >= max_group_size {
            return Err(KafkaError::Protocol {
                code: crate::kafka::constants::ERROR_GROUP_MAX_SIZE_REACHED,
                message: format!(
                    "group '{}' has reached the maximum of {max_group_size} members",
                    params.group_id
                ),
            });
        }
        if !group.supports_protocols(known_id, &params.protocol_type, &params.protocols) {
            return Err(inconsistent_protocol(&params.group_id));
        }
        Ok(())
    }

    /// Add or update an admitted member and decide its JoinGroup: an unchanged rejoin of a
    /// Stable group's follower, or of a CompletingRebalance group's member, is answered with
    /// the current generation; anything else joins (or starts) a join phase and is held.
    fn apply_join(
        group: &mut ConsumerGroup,
        member_id: String,
        params: &JoinGroupParams,
        unchanged: bool,
        out: &mut Vec<Delivery>,
    ) -> JoinDisposition {
        let is_leader = group.leader.as_deref() == Some(member_id.as_str());
        match group.state {
            GroupState::Stable if unchanged && !is_leader => {
                group.upsert_member(&member_id, params);
                return JoinDisposition::Respond(Ok(group.join_result_for(&member_id)));
            }
            GroupState::CompletingRebalance if unchanged => {
                group.upsert_member(&member_id, params);
                return JoinDisposition::Respond(Ok(group.join_result_for(&member_id)));
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

        JoinDisposition::Hold {
            group_id: group.group_id.clone(),
            member_id,
        }
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
        self.sync(
            group_id,
            member_id,
            generation_id,
            assignments,
            false,
            responder,
        );
    }

    /// [`Self::sync_group`] for the leader's SyncGroup whose `assignments` the server computed:
    /// as there, and a refusal by the memory budget (COORDINATOR_NOT_AVAILABLE) is remembered
    /// for the generation ([`Self::refuse_assignment`]), so the leader's retries do not compute
    /// the assignment again only to be refused once more.
    pub fn sync_group_computed(
        &self,
        group_id: &str,
        member_id: &str,
        generation_id: i32,
        assignments: Vec<(String, Vec<u8>)>,
        responder: SyncResponder,
    ) {
        self.sync(
            group_id,
            member_id,
            generation_id,
            assignments,
            true,
            responder,
        );
    }

    fn sync(
        &self,
        group_id: &str,
        member_id: &str,
        generation_id: i32,
        assignments: Vec<(String, Vec<u8>)>,
        computed: bool,
        responder: SyncResponder,
    ) {
        let mut deliveries = Vec::new();
        let mut replaced = None;
        let mut responder = Some(responder);

        let immediate: Option<SyncOutcome> = match self.groups.write() {
            Ok(mut groups) => match Self::sync_locked(
                &mut groups,
                SyncRequest {
                    group_id,
                    member_id,
                    generation_id,
                    assignments,
                    computed,
                },
                self.max_group_memory.load(Ordering::Relaxed),
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
        request: SyncRequest<'_>,
        max_memory: usize,
        out: &mut Vec<Delivery>,
    ) -> Result<Option<Vec<u8>>> {
        let SyncRequest {
            group_id,
            member_id,
            generation_id,
            assignments,
            computed,
        } = request;
        // The member and its generation are checked before anything else: a SyncGroup
        // answered UNKNOWN_MEMBER_ID or ILLEGAL_GENERATION changes nothing, not even the
        // Empty groups the memory budget would forget for its assignments.
        let group = current_member_group(groups, group_id, member_id, generation_id)?;
        // What every other group holds, for the memory budget of a leader's assignments
        // (only a leader's SyncGroup in the sync phase stores any).
        let stores_assignments = group.state == GroupState::CompletingRebalance
            && group.leader.as_deref() == Some(member_id);
        // Room for the assignments is made first (forgetting least recently active Empty
        // groups), so the check below sees what is left.
        if stores_assignments {
            let growth = groups.get(group_id).map_or(0, |g| {
                g.held_bytes_after_sync(&assignments)
                    .saturating_sub(g.held_bytes)
            });
            if growth > 0 {
                make_room(groups, group_id, growth, max_memory);
            }
        }
        let held_elsewhere = if stores_assignments {
            groups
                .iter()
                .filter(|(id, _)| id.as_str() != group_id)
                .map(|(_, g)| g.held_bytes)
                .sum::<usize>()
        } else {
            0
        };
        // Still there: make_room keeps the group it makes room for.
        let group = current_member_group(groups, group_id, member_id, generation_id)?;
        let is_leader = group.leader.as_deref() == Some(member_id);
        if matches!(
            group.state,
            GroupState::CompletingRebalance | GroupState::Stable
        ) && !is_leader
        {
            // A follower's SyncGroup for its generation (parked or answered) is its sync. The
            // leader's counts once its assignments are accepted (below).
            group.record_sync(member_id);
        }
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
                // The assignments are kept in memory (and copied for the sticky assignor):
                // they must fit `pg_kafka.max_group_memory` like the members' metadata. A
                // refused SyncGroup stores nothing; the group stays in its sync phase, and the
                // rebalance timeout evicts a leader that cannot sync.
                let after = group.held_bytes_after_sync(&assignments);
                if after > group.held_bytes && held_elsewhere.saturating_add(after) > max_memory {
                    crate::pg_warning!(
                        "SyncGroup for group '{}' refused: its assignments bring the group to {} bytes, which with the {} bytes other groups hold exceeds pg_kafka.max_group_memory ({} bytes)",
                        group_id,
                        after,
                        held_elsewhere,
                        max_memory
                    );
                    if computed {
                        group.refused_assignment =
                            Some((generation_id, AssignmentRefusal::OverMemory));
                    }
                    return Err(KafkaError::CoordinatorNotAvailable {
                        group_id: group_id.to_string(),
                    });
                }
                // The leader's SyncGroup counts as liveness, as in Kafka: a leader that
                // took longer than its session timeout to compute the assignments (it
                // sends no Heartbeat meanwhile) is not evicted by the next sweep.
                if let Some(member) = group.members.get_mut(member_id) {
                    member.last_heartbeat = Instant::now();
                }
                // Membership cannot have changed since the leader got its member
                // list: any join, leave or eviction after the join phase completed
                // moved the group back to PreparingRebalance. Assignments for ids
                // that are not members are ignored.
                for (target_member_id, assignment_bytes) in assignments {
                    if let Some(member) = group.members.get_mut(&target_member_id) {
                        member.assignment = Some(assignment_bytes);
                    }
                }
                group.record_sync(member_id);
                group.complete_sync(out);
                group.recount();
                Ok(Some(group.assignment_of(member_id)))
            }
            GroupState::CompletingRebalance => {
                // A follower ahead of the leader waits for the leader's assignments.
                if let Some(member) = group.members.get_mut(member_id) {
                    member.awaiting_sync = true;
                }
                Ok(None)
            }
            // Stable always carries an assignment (complete_sync fills in empty ones).
            // Kafka counts this SyncGroup as liveness too.
            GroupState::Stable => {
                group.record_sync(member_id);
                if let Some(member) = group.members.get_mut(member_id) {
                    member.last_heartbeat = Instant::now();
                }
                Ok(Some(group.assignment_of(member_id)))
            }
        }
    }

    /// Validate a TxnOffsetCommit's member and generation against the live group state, as
    /// Kafka 3.7 does (checked against a 3.7 broker), to reject zombie commits:
    /// - a group with no members (unknown here, or Empty) takes generation < 0 whatever the
    ///   member id (a pre-v3 client, or a producer committing outside group management);
    /// - a group with members takes generation < 0 only with an empty member id; a member
    ///   id the group does not know gets `UnknownMemberId`, and one of its members
    ///   `IllegalGeneration` (generation -1 is not its generation);
    /// - otherwise the member must be in the group (`UnknownMemberId`, checked first, as
    ///   Kafka does; a group this coordinator has no record of has no members) and the
    ///   generation must be the current one (`IllegalGeneration`).
    ///
    /// A generation-bearing commit for a group this coordinator has *no record of* is fenced
    /// with `UnknownMemberId` (a zombie whose group was reaped by the empty-group sweep, or whose
    /// coordinator restarted), forcing it to rejoin and get a fresh generation. Because coordinator
    /// state is in-memory, this means that after a bgworker restart every consumer holding a
    /// generation must rejoin — a brief rejoin/rebalance storm on restart. That is deliberate and
    /// correct (the old generations are gone), not a regression; the earlier "unknown group is
    /// allowed" behavior would have let a zombie commit through.
    ///
    /// As in Kafka 3.7, the group's state is not checked: a current-generation commit of a
    /// member is accepted in every state, CompletingRebalance included (where a plain
    /// OffsetCommit gets REBALANCE_IN_PROGRESS, see [`Self::validate_offset_commit`]).
    pub fn validate_commit(
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
                Err(KafkaError::unknown_member(group_id, member_id))
            };
        };

        if generation_id < 0 && (group.members.is_empty() || member_id.is_empty()) {
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
    ///   rejoined yet (the commit a member makes when its partitions are revoked, before it
    ///   rejoins; the generation has not moved yet).
    ///
    /// [`Self::validate_commit`] has TxnOffsetCommit's rules, where Kafka exempts transactional
    /// commits from the Empty-group requirement.
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

        let group = current_member_group(&mut groups, group_id, member_id, generation_id)?;
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

    /// Handle a LeaveGroup v0-2 request: [`Self::leave_members`] of the one member,
    /// UNKNOWN_MEMBER_ID if the group does not have it.
    pub fn leave_group(&self, group_id: &str, member_id: &str) -> Result<()> {
        match self.leave_members(group_id, [member_id])?.first() {
            Some(&code) if code == crate::kafka::constants::ERROR_NONE => Ok(()),
            _ => Err(KafkaError::unknown_member(group_id, member_id)),
        }
    }

    /// Handle the members of a LeaveGroup request: remove each named member of `group_id`
    /// and return each entry's error code, in order, as Kafka 3.7 answers every entry: NONE
    /// for a member removed, UNKNOWN_MEMBER_ID for one the group does not have (a group the
    /// coordinator does not hold, or a repeat of an entry already removed). Remaining
    /// members of a Stable or CompletingRebalance group rebalance; a join phase in progress
    /// completes if the leavers were the last members that had not rejoined.
    ///
    /// The members are removed together and the group reacts once, after the last: a new
    /// leader is chosen, the state moves and the group is recounted once per request, not
    /// once per entry. (Kafka reacts after each entry, so a join phase that the first
    /// leavers complete is completed and then restarted by the next; here it completes
    /// once, with the members that remain.)
    ///
    /// Cost, for `n` entries naming members of a group of `m`: one groups lock and one
    /// lookup; time O(n) plus O(m) once for the reaction; memory O(n) for the codes, plus
    /// the answers of the removed members' held requests. One LOG line per request that
    /// removes a member (none otherwise), never a copy of the group id per entry.
    pub fn leave_members<'a>(
        &self,
        group_id: &str,
        member_ids: impl IntoIterator<Item = &'a str>,
    ) -> Result<Vec<i16>> {
        use crate::kafka::constants::{ERROR_NONE, ERROR_UNKNOWN_MEMBER_ID};
        let mut deliveries = Vec::new();
        let codes = {
            let mut groups = self.groups.write().map_err(|e| {
                KafkaError::Internal(format!("Failed to acquire groups write lock: {e}"))
            })?;
            let member_ids = member_ids.into_iter();
            match groups.get_mut(group_id) {
                None => member_ids.map(|_| ERROR_UNKNOWN_MEMBER_ID).collect(),
                Some(group) => {
                    let codes: Vec<i16> = member_ids
                        .map(|member_id| {
                            if group.take_member(member_id, &mut deliveries) {
                                ERROR_NONE
                            } else {
                                ERROR_UNKNOWN_MEMBER_ID
                            }
                        })
                        .collect();
                    let removed = codes.iter().filter(|&&c| c == ERROR_NONE).count();
                    if removed > 0 {
                        pg_log!(
                            "LeaveGroup: {} of {} member(s) left group {}",
                            removed,
                            codes.len(),
                            group_id
                        );
                        group.settle_removals();
                        group.on_members_removed(&mut deliveries);
                        group.recount();
                    }
                    codes
                }
            }
        };
        self.deliver(deliveries);
        Ok(codes)
    }

    /// Run the periodic group timeouts: rebalance timeouts (evicting members that did
    /// not rejoin a join phase, or that did not sync a completed one), then session timeouts
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

                for member_id in expire_and_recount(group, &mut deliveries) {
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
                group.recount();
            }

            // Reclaim expired empty groups here (every sweep), not just on the next
            // join_group's pass, so an idle Empty group does not outlive EMPTY_GROUP_TTL_SECS.
            // Forgetting one earlier is safe too (make_room and pg_kafka.max_consumer_groups do,
            // at any age): see remove_member for why a recreated group needs no old generation.
            groups.retain(|_, g| !g.is_expired_empty());
        }

        self.deliver(deliveries);
        removed
    }

    /// Whether the coordinator holds `group_id`, without copying it (a group may hold a
    /// thousand members of up to 1 MiB of metadata each). A poisoned lock reads as not held.
    pub fn has_group(&self, group_id: &str) -> bool {
        self.groups
            .read()
            .is_ok_and(|groups| groups.contains_key(group_id))
    }

    /// Whether the held group `group_id` has no members, without copying it; `None` when
    /// the coordinator does not hold it (or its lock is poisoned).
    pub fn group_is_empty(&self, group_id: &str) -> Option<bool> {
        self.groups
            .read()
            .ok()
            .and_then(|groups| groups.get(group_id).map(|g| g.members.is_empty()))
    }

    /// A snapshot of a group's state, for tests: it copies the whole group.
    #[cfg(test)]
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
        let groups = self.groups.read().map_err(|e| {
            KafkaError::Internal(format!("Failed to acquire groups read lock: {e}"))
        })?;

        let group = groups
            .get(group_id)
            .ok_or_else(|| KafkaError::CoordinatorNotAvailable {
                group_id: group_id.to_string(),
            })?;

        let subscriptions = member_subscriptions(group);
        // The group's selected assignor, "range" if none was selected.
        let strategy_name = group.protocol_name.as_deref().unwrap_or("range");
        let work = subscriptions
            .values()
            .map(|s| s.topics.len())
            .sum::<usize>()
            + topic_partitions
                .values()
                .filter_map(|&count| crate::kafka::limits::sane_partition_count(count))
                .map(|count| usize::try_from(count).unwrap_or(0))
                .sum::<usize>();
        check_assignment_work(
            group_id,
            work + previous_assignments_work(group, strategy_name),
        )?;
        let previous = previous_assignments(group, strategy_name, topic_partitions);
        let input = AssignmentInput::new(subscriptions, topic_partitions.clone());
        let assignments = run_assignor(
            group_id,
            previous,
            &input,
            strategy_name,
            crate::kafka::limits::MAX_ASSIGNMENT_STEPS,
        )?;

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
        let groups = self.groups.read().map_err(|e| {
            KafkaError::Internal(format!("Failed to acquire groups read lock: {e}"))
        })?;

        let group = groups
            .get(group_id)
            .ok_or_else(|| KafkaError::CoordinatorNotAvailable {
                group_id: group_id.to_string(),
            })?;

        // Counted from the metadata's array lengths before anything is parsed: a member's
        // metadata may list up to about half a million names.
        check_assignment_work(
            group_id,
            group
                .members
                .values()
                .filter_map(|member| {
                    member
                        .protocols
                        .iter()
                        .find(|(name, _)| Some(name) == group.protocol_name.as_ref())
                })
                .map(|(_, metadata)| MemberSubscription::topic_count_hint(metadata))
                .sum(),
        )?;

        let mut topics = std::collections::HashSet::new();

        for member in group.members.values() {
            // Try to parse subscription from the chosen protocol
            if let Some((_, metadata)) = member
                .protocols
                .iter()
                .find(|(name, _)| Some(name) == group.protocol_name.as_ref())
            {
                if let Ok(sub) = MemberSubscription::parse_topics(metadata) {
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

/// Every member's subscription, parsed from its metadata for the group's selected
/// protocol. A member without that protocol, or whose metadata does not parse, is
/// assigned as if it subscribed to nothing. Only the topics are kept
/// ([`MemberSubscription::parse_topics`]): no assignor reads the user data.
fn member_subscriptions(group: &ConsumerGroup) -> HashMap<String, MemberSubscription> {
    group
        .members
        .iter()
        .map(|(member_id, member)| {
            let subscription = member
                .protocols
                .iter()
                .find(|(name, _)| Some(name) == group.protocol_name.as_ref())
                .map_or_else(MemberSubscription::default, |(_, metadata)| {
                    MemberSubscription::parse_topics(metadata).unwrap_or_else(|e| {
                        pg_log!(
                            "Failed to parse subscription for member {}: {}",
                            member_id,
                            e
                        );
                        MemberSubscription::default()
                    })
                });
            (member_id.clone(), subscription)
        })
        .collect()
}

/// The group's previous assignments, for the sticky assignor (none for the others): those
/// of its current members (a departed member keeps nothing), each with only the entries the
/// assignor can keep, for topics in `topic_partitions` ([`MemberAssignment::parse_kept`]).
///
/// Cost: time linear in the stored bytes of the current members' previous assignments
/// (bounded beforehand by [`previous_assignments_work`]); memory: the kept entries only.
fn previous_assignments(
    group: &ConsumerGroup,
    strategy_name: &str,
    topic_partitions: &HashMap<String, i32>,
) -> HashMap<String, MemberAssignment> {
    if !strategy_name.contains("sticky") {
        return HashMap::new();
    }
    group
        .previous_assignments
        .iter()
        .filter(|(member_id, _)| group.members.contains_key(*member_id))
        .filter_map(|(member_id, bytes)| {
            MemberAssignment::parse_kept(bytes, topic_partitions)
                .ok()
                .map(|assignment| (member_id.clone(), assignment))
        })
        .collect()
}

/// The entries [`previous_assignments`] parses: every topic entry of the current members'
/// previous assignments (also those naming no existing topic or listing no partitions,
/// which cost a parse and a lookup each), plus the partitions they list, counted from the
/// stored lengths ([`MemberAssignment::work_hint`]) and only up to just over
/// `limits::MAX_ASSIGNMENT_WORK`. Zero for an assignor other than sticky.
fn previous_assignments_work(group: &ConsumerGroup, strategy_name: &str) -> usize {
    if !strategy_name.contains("sticky") {
        return 0;
    }
    let limit = crate::kafka::limits::MAX_ASSIGNMENT_WORK;
    let mut work = 0_usize;
    for (member_id, bytes) in &group.previous_assignments {
        if work > limit {
            break;
        }
        if group.members.contains_key(member_id) {
            work = work.saturating_add(MemberAssignment::work_hint(bytes, limit - work));
        }
    }
    work
}

/// Refuse a server-side assignment over [`crate::kafka::limits::MAX_ASSIGNMENT_WORK`]
/// entries (subscribed topics per member, previous assignments' entries and partitions,
/// partitions to assign)
/// with POLICY_VIOLATION: it would hold the single DB thread, and every other client
/// with it, for as long as it ran.
fn check_assignment_work(group_id: &str, entries: usize) -> Result<()> {
    let max = crate::kafka::limits::MAX_ASSIGNMENT_WORK;
    if entries > max {
        crate::pg_warning!(
            "SyncGroup for group '{}' refused: computing its assignments takes {} entries (subscriptions, previous assignments and partitions), above the limit of {}",
            group_id,
            entries,
            max
        );
        return Err(KafkaError::PolicyViolation {
            message: format!(
                "the server-side assignment of group '{group_id}' takes {entries} entries, above the limit of {max}"
            ),
        });
    }
    Ok(())
}

/// Run the assignor `strategy_name` over `input` for `group_id`. The sticky assignor starts
/// from `previous`, the group's previous assignments, and is refused with POLICY_VIOLATION
/// once it has spent `sticky_steps` steps: how long its balancing runs depends on how the
/// subscriptions overlap, not only on the entries `check_assignment_work` counts. Range and
/// round-robin take about one step per entry.
fn run_assignor(
    group_id: &str,
    previous: HashMap<String, MemberAssignment>,
    input: &AssignmentInput,
    strategy_name: &str,
    sticky_steps: usize,
) -> Result<AssignmentOutput> {
    if strategy_name.contains("sticky") {
        return StickyStrategy::with_previous(previous)
            .assign_within(input, sticky_steps)
            .map_err(|exceeded| {
                crate::pg_warning!(
                    "SyncGroup for group '{}' refused: its sticky assignment takes more than {} steps",
                    group_id,
                    exceeded.budget
                );
                KafkaError::PolicyViolation {
                    message: format!(
                        "the server-side sticky assignment of group '{group_id}' takes more than {} steps",
                        exceeded.budget
                    ),
                }
            });
    }
    let strategy = create_strategy(strategy_name).ok_or_else(|| {
        KafkaError::Internal(format!("Unknown assignment strategy: {strategy_name}"))
    })?;
    Ok(strategy.assign(input))
}

/// The limits a JoinGroup is admitted under (see [`GroupCoordinator::set_limits`] and
/// [`GroupCoordinator::set_memory_limit`]).
#[derive(Debug, Clone, Copy)]
struct JoinLimits {
    groups: usize,
    group_size: usize,
    memory: usize,
}

/// The SyncGroup request fields the coordinator uses.
struct SyncRequest<'a> {
    group_id: &'a str,
    member_id: &'a str,
    generation_id: i32,
    assignments: Vec<(String, Vec<u8>)>,
    /// Whether the server computed `assignments` (see [`GroupCoordinator::sync_group_computed`]).
    computed: bool,
}

/// Why a server-side assignment was refused ([`GroupCoordinator::refuse_assignment`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssignmentRefusal {
    /// Over `limits::MAX_ASSIGNMENT_WORK` or `MAX_ASSIGNMENT_STEPS`: POLICY_VIOLATION.
    TooLarge,
    /// The computed assignments do not fit `pg_kafka.max_group_memory`:
    /// COORDINATOR_NOT_AVAILABLE.
    OverMemory,
}

/// What all groups hold in coordinator memory, as last counted.
fn total_held_bytes(groups: &HashMap<String, ConsumerGroup>) -> usize {
    groups.values().map(|g| g.held_bytes).sum()
}

/// Make `extra` more bytes fit `max_memory` by forgetting the least recently active Empty
/// groups other than `keep`, as `admit_new_group` does at the group-count cap: an Empty group
/// holds no members, its committed offsets stay in the database (it remains visible as an
/// Empty group), and without this the Empty groups a JoinGroup and LeaveGroup leave behind
/// would keep their bytes for `EMPTY_GROUP_TTL_SECS`. Nothing is forgotten when forgetting
/// every Empty group would still not make room. Returns whether `extra` fits.
fn make_room(
    groups: &mut HashMap<String, ConsumerGroup>,
    keep: &str,
    extra: usize,
    max_memory: usize,
) -> bool {
    let mut held = total_held_bytes(groups);
    if held.saturating_add(extra) <= max_memory {
        return true;
    }
    let forgettable = |g: &&ConsumerGroup| g.state == GroupState::Empty && g.group_id != keep;
    // Whether forgetting helps is decided before anything is collected: a refusal costs no
    // copy of the (up to 32 KB) group ids.
    let reclaimable: usize = groups
        .values()
        .filter(forgettable)
        .map(|g| g.held_bytes)
        .sum();
    if held.saturating_sub(reclaimable).saturating_add(extra) > max_memory {
        return false;
    }
    let mut candidates: Vec<(Instant, &str, usize)> = groups
        .values()
        .filter(forgettable)
        .map(|g| (g.last_activity, g.group_id.as_str(), g.held_bytes))
        .collect();
    candidates.sort_unstable();
    // Only the ids of the groups actually forgotten are copied.
    let mut victims = Vec::new();
    for (_, group_id, bytes) in candidates {
        if held.saturating_add(extra) <= max_memory {
            break;
        }
        victims.push(group_id.to_string());
        held = held.saturating_sub(bytes);
    }
    for group_id in victims {
        groups.remove(&group_id);
    }
    true
}

/// Run a group's rebalance timeout and recount what it holds if that changed it (an
/// eviction, or a join phase that completed and dropped the old assignments).
fn expire_and_recount(group: &mut ConsumerGroup, out: &mut Vec<Delivery>) -> Vec<String> {
    let before = (group.state.clone(), group.members.len());
    let evicted = group.expire_stuck_rebalance(out);
    if !evicted.is_empty() || before != (group.state.clone(), group.members.len()) {
        group.recount();
    }
    evicted
}

/// The group of a member presenting `generation_id`, fenced as Kafka fences SyncGroup and
/// Heartbeat: a group this coordinator has no record of (e.g. lost in a worker restart)
/// or an unknown member gets UNKNOWN_MEMBER_ID, which makes the client rejoin as a new
/// member; then a generation other than the current one gets ILLEGAL_GENERATION.
fn current_member_group<'a>(
    groups: &'a mut HashMap<String, ConsumerGroup>,
    group_id: &str,
    member_id: &str,
    generation_id: i32,
) -> Result<&'a mut ConsumerGroup> {
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
    Ok(group)
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
