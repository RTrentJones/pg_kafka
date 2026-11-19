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

/// TTL for empty consumer groups before cleanup (24 hours)
const EMPTY_GROUP_TTL_SECS: u64 = 86400;

// Group-count and group-size caps: `pg_kafka.max_consumer_groups` / `pg_kafka.group_max_size`
// (crate::kafka::limits), applied through `GroupCoordinator::set_limits` (wave3 groups).

/// Default rebalance timeout (5 minutes), used when a member advertises neither a
/// positive rebalance timeout nor a positive session timeout.
const DEFAULT_REBALANCE_TIMEOUT_MS: u64 = 300_000;

/// RV-10: accepted range for a JoinGroup `session_timeout_ms`, matching Kafka's
/// broker defaults (`group.min.session.timeout.ms` = 6s, `group.max.session.timeout.ms`
/// = 30 min). A request outside this range is rejected with `INVALID_SESSION_TIMEOUT`
/// so a hostile or misconfigured client cannot pin a member alive for weeks (blocking
/// rebalances) or set a sub-second timeout that thrashes the group. Enforced at the
/// request boundary (the JoinGroup handler), not in the group-state core, so unit tests
/// can still drive expiry with tiny timeouts.
pub const MIN_SESSION_TIMEOUT_MS: i32 = 6_000;
pub const MAX_SESSION_TIMEOUT_MS: i32 = 1_800_000;

// Import conditional logging macros for test isolation
use crate::pg_log;

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

/// The answer carried by a [`Delivery`].
enum DeliveryOutcome {
    Join(JoinOutcome),
    Sync(SyncOutcome),
}

/// An answer for a held JoinGroup or parked SyncGroup, produced by the group state
/// machine and routed to the member's responder by the coordinator once its locks are
/// released.
struct Delivery {
    group_id: String,
    member_id: String,
    outcome: DeliveryOutcome,
}

impl Delivery {
    fn join(group_id: &str, member_id: &str, outcome: JoinOutcome) -> Self {
        Self {
            group_id: group_id.to_string(),
            member_id: member_id.to_string(),
            outcome: DeliveryOutcome::Join(outcome),
        }
    }

    fn sync(group_id: &str, member_id: &str, outcome: SyncOutcome) -> Self {
        Self {
            group_id: group_id.to_string(),
            member_id: member_id.to_string(),
            outcome: DeliveryOutcome::Sync(outcome),
        }
    }
}

/// Consumer group member information
#[derive(Debug, Clone)]
pub struct GroupMember {
    /// Unique member ID (UUID assigned by coordinator)
    pub member_id: String,

    /// Client ID from request (e.g., "rdkafka")
    pub client_id: String,

    /// Client host address
    pub client_host: String,

    /// Session timeout in milliseconds
    /// If no heartbeat received within this time, member is considered dead
    pub session_timeout_ms: i32,

    /// Rebalance timeout in milliseconds (v1+)
    /// Maximum time allowed for rebalance to complete
    pub rebalance_timeout_ms: i32,

    /// Protocol type (e.g., "consumer")
    pub protocol_type: String,

    /// Supported assignment strategies with metadata
    /// Vec<(strategy_name, subscription_metadata)>
    pub protocols: Vec<(String, Vec<u8>)>,

    /// Current partition assignment (from leader's SyncGroup request)
    pub assignment: Option<Vec<u8>>,

    /// Last heartbeat timestamp (monotonic for reliable timeout detection)
    pub last_heartbeat: Instant,

    /// True while this member's JoinGroup response is held for the join phase in
    /// progress, i.e. the member has rejoined it. Only ever true while the group is
    /// PreparingRebalance.
    pub awaiting_join: bool,

    /// True while this member's SyncGroup response is parked waiting for the leader's
    /// SyncGroup. Only ever true while the group is CompletingRebalance.
    pub awaiting_sync: bool,

    /// Static group instance ID (KIP-345) for static membership
    pub group_instance_id: Option<String>,
}

impl GroupMember {
    /// Check if member's session has timed out
    pub fn is_timed_out(&self) -> bool {
        // CG-4: a non-positive session timeout is invalid. `self.session_timeout_ms as u128`
        // would wrap a negative value to ~1.8e19 ms, making the member effectively immortal and
        // never reaped. Treat an invalid timeout as already expired so the sweep removes it.
        if self.session_timeout_ms <= 0 {
            return true;
        }
        let elapsed = self.last_heartbeat.elapsed();
        elapsed.as_millis() > self.session_timeout_ms as u128
    }

    /// How long this member allows a rebalance to take: its rebalance timeout, or for a
    /// v0 JoinGroup (which carries none) its session timeout, as Kafka does.
    fn effective_rebalance_timeout_ms(&self) -> u128 {
        if self.rebalance_timeout_ms > 0 {
            self.rebalance_timeout_ms as u128
        } else if self.session_timeout_ms > 0 {
            self.session_timeout_ms as u128
        } else {
            DEFAULT_REBALANCE_TIMEOUT_MS as u128
        }
    }
}

/// Consumer group state machine states
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupState {
    /// No members in the group
    Empty,

    /// Join phase: waiting for every known member to rejoin
    PreparingRebalance,

    /// Sync phase: waiting for the leader to send partition assignments
    CompletingRebalance,

    /// All members have assignments and are consuming
    Stable,
}

impl GroupState {
    /// The Kafka wire-protocol group-state string, echoed in DescribeGroups /
    /// ListGroups. Mirrors `TransactionState::as_str`; centralizes the mapping
    /// that was previously duplicated across the two handlers. (A group the
    /// coordinator does not know is reported as "Dead" by DescribeGroups.)
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Empty => "Empty",
            Self::PreparingRebalance => "PreparingRebalance",
            Self::CompletingRebalance => "CompletingRebalance",
            Self::Stable => "Stable",
        }
    }
}

/// Consumer group metadata and state
#[derive(Debug, Clone)]
pub struct ConsumerGroup {
    /// Consumer group ID
    pub group_id: String,

    /// Generation ID: incremented each time a join phase completes. Used to fence
    /// stale members. Never reset, not even when the group empties (see
    /// `remove_member`).
    pub generation_id: i32,

    /// Chosen partition assignment strategy, selected when a join phase completes
    pub protocol_name: Option<String>,

    /// Leader member ID (first to join becomes leader)
    /// Leader is responsible for computing partition assignments
    pub leader: Option<String>,

    /// Group members by member_id
    pub members: HashMap<String, GroupMember>,

    /// Current group state
    pub state: GroupState,

    /// Previous partition assignments (for sticky strategy)
    /// Stored after successful SyncGroup completion
    /// Maps member_id -> encoded MemberAssignment bytes
    pub previous_assignments: HashMap<String, Vec<u8>>,

    /// Last activity timestamp for TTL cleanup of empty groups
    pub last_activity: Instant,

    /// When the current join or sync phase started (for the rebalance timeout)
    pub rebalance_started_at: Option<Instant>,

    /// The group's protocol type ("consumer", "connect", ...), set by the first member to join
    /// and kept when the group empties, as in Kafka (wave3 groups). Every later member must use
    /// the same one (INCONSISTENT_GROUP_PROTOCOL otherwise) while the group has members.
    pub protocol_type: Option<String>,
}

impl ConsumerGroup {
    /// Create a new empty consumer group
    fn new(group_id: String) -> Self {
        Self {
            group_id,
            generation_id: 0,
            protocol_name: None,
            leader: None,
            members: HashMap::new(),
            state: GroupState::Empty,
            previous_assignments: HashMap::new(),
            last_activity: Instant::now(),
            rebalance_started_at: None,
            protocol_type: None,
        }
    }

    /// Kafka's `supportsProtocols` rule for a JoinGroup (wave3 groups): joining an Empty group
    /// needs a non-empty protocol type and at least one protocol; joining a group with members
    /// needs the group's protocol type and at least one protocol every *other* member supports
    /// (a known member's own previous list does not count, it is being replaced).
    fn supports_protocols(
        &self,
        member_id: Option<&str>,
        protocol_type: &str,
        protocols: &[(String, Vec<u8>)],
    ) -> bool {
        if protocols.is_empty() || protocol_type.is_empty() {
            return false;
        }
        let others: Vec<&GroupMember> = self
            .members
            .values()
            .filter(|m| Some(m.member_id.as_str()) != member_id)
            .collect();
        if others.is_empty() {
            return true;
        }
        if self.protocol_type.as_deref() != Some(protocol_type) {
            return false;
        }
        protocols.iter().any(|(name, _)| {
            others
                .iter()
                .all(|m| m.protocols.iter().any(|(n, _)| n == name))
        })
    }

    /// Insert or update a member from its JoinGroup. An existing member keeps its
    /// assignment and its `awaiting_join` flag; the first member of a leaderless group
    /// becomes the leader.
    fn upsert_member(&mut self, member_id: &str, params: &JoinGroupParams) {
        let existing = self.members.get(member_id);
        let assignment = existing.and_then(|m| m.assignment.clone());
        let awaiting_join = existing.is_some_and(|m| m.awaiting_join);
        let awaiting_sync = existing.is_some_and(|m| m.awaiting_sync);
        let member = GroupMember {
            member_id: member_id.to_string(),
            client_id: params.client_id.clone(),
            client_host: params.client_host.clone(),
            session_timeout_ms: params.session_timeout_ms,
            rebalance_timeout_ms: params.rebalance_timeout_ms,
            protocol_type: params.protocol_type.clone(),
            protocols: params.protocols.clone(),
            assignment,
            last_heartbeat: Instant::now(),
            awaiting_join,
            awaiting_sync,
            group_instance_id: params.group_instance_id.clone(),
        };
        self.members.insert(member_id.to_string(), member);
        // The group adopts the protocol type of the member that fills it; joins that do not
        // match it are refused before this point (supports_protocols).
        if self.members.len() == 1 {
            self.protocol_type = Some(params.protocol_type.clone());
        }

        // The first member to join a leaderless group becomes the leader; an existing
        // leader rejoining stays the leader (BUG-9).
        if self.leader.is_none() {
            self.leader = Some(member_id.to_string());
        }
    }

    /// `(member_id, metadata for the group-selected protocol)` for every member,
    /// sorted by member id: the member list the leader computes assignments from.
    /// Falls back to a member's first protocol only if it does not advertise the
    /// selected one.
    fn member_metadata(&self) -> Vec<(String, Vec<u8>)> {
        let mut members: Vec<(String, Vec<u8>)> = self
            .members
            .values()
            .filter_map(|m| {
                m.protocols
                    .iter()
                    .find(|(name, _)| Some(name) == self.protocol_name.as_ref())
                    .or_else(|| m.protocols.first())
                    .map(|(_, metadata)| (m.member_id.clone(), metadata.clone()))
            })
            .collect();
        members.sort_by(|a, b| a.0.cmp(&b.0));
        members
    }

    /// The JoinGroup answer for `member_id` at the current generation.
    fn join_result_for(&self, member_id: &str) -> JoinResult {
        let leader_id = self.leader.clone().unwrap_or_default();
        let members = if leader_id == member_id {
            self.member_metadata()
        } else {
            Vec::new()
        };
        JoinResult {
            member_id: member_id.to_string(),
            generation_id: self.generation_id,
            leader_id,
            protocol_name: self.protocol_name.clone(),
            members,
        }
    }

    /// Start a join phase. The generation does not move until the phase completes;
    /// members learn of the rebalance from Heartbeat and rejoin. SyncGroups parked in
    /// the sync phase being abandoned are answered REBALANCE_IN_PROGRESS (Kafka's
    /// answer too), so those members rejoin.
    fn prepare_rebalance(&mut self, out: &mut Vec<Delivery>) {
        let mut parked: Vec<String> = self
            .members
            .values_mut()
            .filter(|m| m.awaiting_sync)
            .map(|m| {
                m.awaiting_sync = false;
                m.member_id.clone()
            })
            .collect();
        parked.sort();
        for member_id in parked {
            out.push(Delivery::sync(
                &self.group_id,
                &member_id,
                Err(KafkaError::RebalanceInProgress {
                    group_id: self.group_id.clone(),
                }),
            ));
        }
        self.state = GroupState::PreparingRebalance;
        self.rebalance_started_at = Some(Instant::now());
        pg_log!(
            "Group {} preparing rebalance (current generation {})",
            self.group_id,
            self.generation_id
        );
    }

    /// Complete the join phase if every member has rejoined it: increment the
    /// generation, select the group protocol, move to CompletingRebalance and queue
    /// the JoinGroup answer for every member (the leader's carries the member list).
    /// Called after a join and after member removals, since a phase also becomes
    /// completable when the last member that had not rejoined leaves or is evicted.
    fn maybe_complete_join(&mut self, out: &mut Vec<Delivery>) {
        if self.state != GroupState::PreparingRebalance || self.members.is_empty() {
            return;
        }
        if !self.members.values().all(|m| m.awaiting_join) {
            return;
        }

        self.generation_id += 1;
        if !self
            .leader
            .as_ref()
            .is_some_and(|l| self.members.contains_key(l))
        {
            self.leader = self.members.keys().min().cloned();
        }
        self.protocol_name = self.select_group_protocol();
        self.state = GroupState::CompletingRebalance;
        // The sync phase gets its own rebalance-timeout window.
        self.rebalance_started_at = Some(Instant::now());

        let now = Instant::now();
        for member in self.members.values_mut() {
            member.awaiting_join = false;
            // Assignments belong to the generation just replaced; the leader's
            // SyncGroup hands out new ones.
            member.assignment = None;
            // The held JoinGroup counted as liveness; the session clock restarts now.
            member.last_heartbeat = now;
        }

        let mut member_ids: Vec<String> = self.members.keys().cloned().collect();
        member_ids.sort();
        for member_id in member_ids {
            out.push(Delivery::join(
                &self.group_id,
                &member_id,
                Ok(self.join_result_for(&member_id)),
            ));
        }
        pg_log!(
            "Group {} completed join phase (generation {}, {} member(s))",
            self.group_id,
            self.generation_id,
            self.members.len()
        );
    }

    /// Choose the group protocol by Kafka's member vote (wave3 groups): the candidates are the
    /// protocols every member supports; each member votes for the first candidate in its own
    /// preference order and the candidate with the most votes wins. Ties go to the candidate
    /// the leader prefers (Kafka leaves ties to hash order; this keeps the choice
    /// deterministic). `supports_protocols` guarantees a common protocol at every join; should
    /// there be none anyway, the leader's first protocol is used.
    fn select_group_protocol(&self) -> Option<String> {
        let leader = self
            .leader
            .as_ref()
            .and_then(|l| self.members.get(l))
            .or_else(|| self.members.values().next())?;
        let candidates: Vec<&String> = leader
            .protocols
            .iter()
            .map(|(name, _)| name)
            .filter(|name| {
                self.members
                    .values()
                    .all(|m| m.protocols.iter().any(|(n, _)| n == *name))
            })
            .collect();
        if candidates.is_empty() {
            return leader.protocols.first().map(|(name, _)| name.clone());
        }
        let mut votes: HashMap<&String, usize> = HashMap::new();
        for m in self.members.values() {
            if let Some((name, _)) = m.protocols.iter().find(|(n, _)| candidates.contains(&n)) {
                *votes.entry(name).or_default() += 1;
            }
        }
        // Highest vote count; among equals, the earliest in the leader's preference.
        let mut best: Option<(&String, usize)> = None;
        for name in &candidates {
            let v = votes.get(name).copied().unwrap_or(0);
            if best.is_none_or(|(_, bv)| v > bv) {
                best = Some((name, v));
            }
        }
        best.map(|(name, _)| name.clone())
    }

    /// Remove a member from the group. A member whose JoinGroup is held gets
    /// UNKNOWN_MEMBER_ID for it, as does a member whose SyncGroup is parked. Does not
    /// start a rebalance: callers follow up with
    /// `on_members_removed`.
    fn remove_member(&mut self, member_id: &str, out: &mut Vec<Delivery>) -> bool {
        let Some(removed) = self.members.remove(member_id) else {
            return false;
        };
        // Clean up previous assignments for this member (memory leak fix)
        self.previous_assignments.remove(member_id);

        if removed.awaiting_join {
            out.push(Delivery::join(
                &self.group_id,
                member_id,
                Err(KafkaError::unknown_member(&self.group_id, member_id)),
            ));
        }
        if removed.awaiting_sync {
            out.push(Delivery::sync(
                &self.group_id,
                member_id,
                Err(KafkaError::unknown_member(&self.group_id, member_id)),
            ));
        }

        // If the leader left, reassign leadership
        if self.leader.as_deref() == Some(member_id) {
            self.leader = self.members.keys().min().cloned();
        }

        // If no members are left, return to Empty.
        // NOTE: generation_id is intentionally NOT reset. The generation is the
        // zombie-fencing token: if it restarted at 0, a stale client from a previous
        // incarnation of the group could pass the generation checks. Kafka keeps it
        // monotonic for the lifetime of the group; so do we.
        if self.members.is_empty() {
            self.state = GroupState::Empty;
            self.protocol_name = None;
            self.leader = None;
            self.rebalance_started_at = None;
            self.last_activity = Instant::now();
        }
        true
    }

    /// React to members having been removed: a Stable or CompletingRebalance group
    /// rebalances (the assignments, settled or in flight, covered the departed
    /// members); a join phase may now be complete.
    fn on_members_removed(&mut self, out: &mut Vec<Delivery>) {
        match self.state {
            GroupState::Stable | GroupState::CompletingRebalance => self.prepare_rebalance(out),
            GroupState::PreparingRebalance => self.maybe_complete_join(out),
            GroupState::Empty => {}
        }
    }

    /// Handle a rebalance phase that exceeded its rebalance timeout (the largest
    /// rebalance timeout the members advertised, as in Kafka):
    /// - join phase (PreparingRebalance): evict the members that did not rejoin, then
    ///   complete the join with the ones that did (answering their held JoinGroups);
    /// - sync phase (CompletingRebalance): the leader never delivered assignments, so
    ///   evict it and start a new join phase with the remaining members.
    ///
    /// Returns the evicted member ids.
    fn expire_stuck_rebalance(&mut self, out: &mut Vec<Delivery>) -> Vec<String> {
        if !matches!(
            self.state,
            GroupState::PreparingRebalance | GroupState::CompletingRebalance
        ) {
            return Vec::new();
        }
        let Some(started) = self.rebalance_started_at else {
            return Vec::new();
        };
        // CG-3: honour the members' advertised rebalance_timeout_ms (the max across the
        // group, matching Kafka) rather than a hardcoded value.
        let effective_timeout_ms = self
            .members
            .values()
            .map(GroupMember::effective_rebalance_timeout_ms)
            .max()
            .unwrap_or(DEFAULT_REBALANCE_TIMEOUT_MS as u128);
        if started.elapsed().as_millis() <= effective_timeout_ms {
            return Vec::new();
        }

        if self.state == GroupState::CompletingRebalance {
            let Some(leader) = self.leader.clone() else {
                return Vec::new();
            };
            pg_log!(
                "Group {} leader {} did not sync within the rebalance timeout; evicting it",
                self.group_id,
                leader
            );
            self.remove_member(&leader, out);
            self.on_members_removed(out);
            return vec![leader];
        }

        let mut stragglers: Vec<String> = self
            .members
            .values()
            .filter(|m| !m.awaiting_join)
            .map(|m| m.member_id.clone())
            .collect();
        stragglers.sort();
        pg_log!(
            "Group {} join phase timed out after {}ms; evicting {} member(s) that did not rejoin",
            self.group_id,
            started.elapsed().as_millis(),
            stragglers.len()
        );
        for member_id in &stragglers {
            self.remove_member(member_id, out);
        }
        self.maybe_complete_join(out);
        stragglers
    }

    /// Whether this group is Empty and has been idle past `EMPTY_GROUP_TTL_SECS`,
    /// so the coordinator may drop it.
    fn is_expired_empty(&self) -> bool {
        self.state == GroupState::Empty
            && self.last_activity.elapsed().as_secs() >= EMPTY_GROUP_TTL_SECS
    }

    /// Complete the sync phase and move to Stable. Members the leader did not assign
    /// anything get an empty assignment, as in Kafka. Every parked follower SyncGroup
    /// is answered with the member's assignment.
    fn complete_sync(&mut self, out: &mut Vec<Delivery>) {
        // Save current assignments for sticky strategy
        self.previous_assignments.clear();
        let mut parked = Vec::new();
        for (member_id, member) in self.members.iter_mut() {
            let assignment = member.assignment.get_or_insert_with(Vec::new);
            if !assignment.is_empty() {
                self.previous_assignments
                    .insert(member_id.clone(), assignment.clone());
            }
            if member.awaiting_sync {
                member.awaiting_sync = false;
                parked.push((member_id.clone(), assignment.clone()));
            }
        }
        parked.sort();
        for (member_id, assignment) in parked {
            out.push(Delivery::sync(&self.group_id, &member_id, Ok(assignment)));
        }

        self.state = GroupState::Stable;
        self.rebalance_started_at = None; // Clear rebalance timeout tracking
        self.last_activity = Instant::now();
        pg_log!(
            "Group {} completed sync phase (generation {})",
            self.group_id,
            self.generation_id
        );
    }
}

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

    /// `pg_kafka.max_consumer_groups`: the most groups held in memory (wave3 groups).
    max_groups: AtomicUsize,

    /// `pg_kafka.group_max_size`: the most members one group may have (wave3 groups).
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
        self.pending_joins.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn lock_pending_syncs(
        &self,
    ) -> std::sync::MutexGuard<'_, HashMap<(String, String), SyncResponder>> {
        self.pending_syncs.lock().unwrap_or_else(|e| e.into_inner())
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
    pub fn join_group(&self, params: JoinGroupParams, responder: JoinResponder) {
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
                "Failed to acquire groups write lock: {}",
                e
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
        params: JoinGroupParams,
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
        // group is created for it (wave3 groups).
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

        // CG-4: cap members per group (DoS protection; Kafka's group.max.size). A rejoining
        // member (already present) is always allowed; only a new member is rejected.
        if known_id.is_none() && group.members.len() >= max_group_size {
            return Err(KafkaError::Protocol {
                code: crate::kafka::constants::ERROR_GROUP_MAX_SIZE_REACHED,
                message: format!(
                    "group '{}' has reached the maximum of {} members",
                    group_id, max_group_size
                ),
            });
        }

        // Kafka's protocol check: the member's protocol type must be the group's and it must
        // share at least one protocol with every other member (wave3 groups).
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
                group.upsert_member(&member_id, &params);
                return Ok(JoinDisposition::Respond(Ok(
                    group.join_result_for(&member_id)
                )));
            }
            GroupState::CompletingRebalance if unchanged => {
                group.upsert_member(&member_id, &params);
                return Ok(JoinDisposition::Respond(Ok(
                    group.join_result_for(&member_id)
                )));
            }
            GroupState::Empty | GroupState::Stable | GroupState::CompletingRebalance => {
                group.upsert_member(&member_id, &params);
                group.prepare_rebalance(out);
            }
            GroupState::PreparingRebalance => {
                group.upsert_member(&member_id, &params);
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
        group_id: String,
        member_id: String,
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
                &group_id,
                &member_id,
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
                            .insert((group_id.clone(), member_id.clone()), responder);
                    }
                    None
                }
                Err(e) => Some(Err(e)),
            },
            Err(e) => Some(Err(KafkaError::Internal(format!(
                "Failed to acquire groups write lock: {}",
                e
            )))),
        };

        if let Some(old) = replaced {
            old(Err(KafkaError::RebalanceInProgress {
                group_id: group_id.clone(),
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

    /// CONF-1: validate an OffsetCommit against the live group state to reject zombie commits.
    ///
    /// A simple consumer with no active group membership commits with `generation_id < 0` and is not
    /// validated. Otherwise the commit's member id and generation must match the current ones — an
    /// unknown member yields `UnknownMemberId` (checked first, as Kafka does) and a stale
    /// generation `IllegalGeneration`.
    ///
    /// RA-5: a generation-bearing commit for a group this coordinator has *no record of* is fenced
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
    /// `RebalanceInProgress` while it is CompletingRebalance (RV-10: the member's
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
            KafkaError::Internal(format!("Failed to acquire groups read lock: {}", e))
        })?;

        let group = match groups.get(group_id) {
            Some(g) => g,
            // RA-5: a consumer presenting a generation (>= 0, checked above) for a
            // group this coordinator has no record of is stale — the group was
            // almost certainly reaped by the empty-group sweep (CG-5) while the
            // consumer was partitioned or lagging. Fence it with UNKNOWN_MEMBER_ID
            // so it rejoins and gets a fresh generation, instead of accepting a
            // zombie offset commit. (A simple consumer, generation < 0, already
            // returned Ok above and is unaffected.)
            None => return Err(KafkaError::unknown_member(group_id, member_id)),
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

    /// Validate a (non-transactional) OffsetCommit as Kafka 3.7 does (wave3 groups):
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
            KafkaError::Internal(format!("Failed to acquire groups read lock: {}", e))
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
    pub fn heartbeat(&self, group_id: String, member_id: String, generation_id: i32) -> Result<()> {
        let mut groups = self.groups.write().map_err(|e| {
            KafkaError::Internal(format!("Failed to acquire groups write lock: {}", e))
        })?;

        // Unknown group (e.g. lost in a worker restart): UNKNOWN_MEMBER_ID so the
        // client rejoins, as Kafka answers.
        let group = groups
            .get_mut(&group_id)
            .ok_or_else(|| KafkaError::unknown_member(&group_id, &member_id))?;

        if !group.members.contains_key(&member_id) {
            return Err(KafkaError::unknown_member(&group_id, &member_id));
        }
        if group.generation_id != generation_id {
            return Err(KafkaError::illegal_generation(
                &group_id,
                generation_id,
                group.generation_id,
            ));
        }

        let Some(member) = group.members.get_mut(&member_id) else {
            return Err(KafkaError::unknown_member(&group_id, &member_id));
        };
        member.last_heartbeat = Instant::now();

        if group.state == GroupState::PreparingRebalance {
            return Err(KafkaError::RebalanceInProgress {
                group_id: group_id.clone(),
            });
        }

        Ok(())
    }

    /// Handle LeaveGroup request
    ///
    /// Removes the member from the group. Remaining members of a Stable or
    /// CompletingRebalance group rebalance; a join phase in progress completes if the
    /// leaver was the last member that had not rejoined.
    pub fn leave_group(&self, group_id: String, member_id: String) -> Result<()> {
        let mut deliveries = Vec::new();
        let result = {
            let mut groups = self.groups.write().map_err(|e| {
                KafkaError::Internal(format!("Failed to acquire groups write lock: {}", e))
            })?;

            match groups.get_mut(&group_id) {
                None => Err(KafkaError::unknown_member(&group_id, &member_id)),
                Some(group) => {
                    pg_log!("LeaveGroup: group_id={}, member_id={}", group_id, member_id);
                    if group.remove_member(&member_id, &mut deliveries) {
                        group.on_members_removed(&mut deliveries);
                        Ok(())
                    } else {
                        Err(KafkaError::unknown_member(&group_id, &member_id))
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
            let mut groups = match self.groups.write() {
                Ok(g) => g,
                Err(_) => {
                    // Lock poisoned, skip this check
                    return removed;
                }
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

            // CG-5: reclaim expired empty groups here (every sweep), not just on the next
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
            KafkaError::Internal(format!("Failed to acquire groups read lock: {}", e))
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
                KafkaError::Internal(format!("Unknown assignment strategy: {}", strategy_name))
            })?;
            strategy.assign(&input)
        };

        // Encode assignments to bytes
        let encoded: HashMap<String, Vec<u8>> = assignments
            .into_iter()
            .map(|(id, assignment)| (id, assignment.encode()))
            .collect();

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
            KafkaError::Internal(format!("Failed to acquire groups read lock: {}", e))
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
/// group (wave3 groups).
fn inconsistent_protocol(group_id: &str) -> KafkaError {
    KafkaError::Protocol {
        code: crate::kafka::constants::ERROR_INCONSISTENT_GROUP_PROTOCOL,
        message: format!(
            "the member's protocol type or protocols are incompatible with group '{}'",
            group_id
        ),
    }
}

/// Generate a unique member ID for a consumer
///
/// Format: {client_id}-{uuid}
fn generate_member_id(client_id: &str) -> String {
    use uuid::Uuid;
    let uuid = Uuid::new_v4();
    format!("{}-{}", client_id, uuid)
}

#[cfg(test)]
mod tests {
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
            self.sync_group(group, member, generation, assignments, responder);
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

    fn join_with(c: &GroupCoordinator, p: JoinGroupParams) -> Slot {
        let (responder, slot) = recorder();
        c.join_group(p, responder);
        slot
    }

    /// Join with default parameters and the "range" protocol.
    fn join(c: &GroupCoordinator, group: &str, member: Option<&str>) -> Slot {
        join_with(c, params(group, member, &[("range", b"sub")]))
    }

    fn is_pending(slot: &Slot) -> bool {
        slot.lock().unwrap().is_none()
    }

    /// The successful outcome recorded in `slot` (panics if pending or an error).
    fn joined(slot: &Slot) -> JoinResult {
        match slot.lock().unwrap().take() {
            Some(Ok(r)) => r,
            Some(Err(e)) => panic!("join failed: {}", e),
            None => panic!("join still pending"),
        }
    }

    /// The error code recorded in `slot` (panics if pending or a success).
    fn join_error_code(slot: &Slot) -> i16 {
        match slot.lock().unwrap().take() {
            Some(Err(e)) => e.to_kafka_error_code(),
            Some(Ok(r)) => panic!("expected an error, got {:?}", r),
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
            DEFAULT_REBALANCE_TIMEOUT_MS as u128
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
        assert!(c.heartbeat("g".to_string(), a.member_id, 1).is_ok());
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
            c.heartbeat("g".to_string(), a.clone(), 1),
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
        assert!(c.heartbeat("g".to_string(), a.clone(), 1).is_ok());
        // Stale generation.
        assert!(matches!(
            c.heartbeat("g".to_string(), a.clone(), 0),
            Err(KafkaError::IllegalGeneration { .. })
        ));
        // Unknown member / unknown group. An unknown member is reported as such even
        // with a stale generation (Kafka checks the member first), so an evicted
        // client rejoins as a new member.
        assert!(matches!(
            c.heartbeat("g".to_string(), "ghost".to_string(), 1),
            Err(KafkaError::UnknownMemberId { .. })
        ));
        assert!(matches!(
            c.heartbeat("g".to_string(), "ghost".to_string(), 0),
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
            c.heartbeat("nope".to_string(), a.clone(), 1),
            Err(KafkaError::UnknownMemberId { .. })
        ));
        c.sync_now("g".to_string(), a.clone(), 1, vec![(a.clone(), vec![1])])
            .unwrap();
        assert!(c.heartbeat("g".to_string(), a.clone(), 1).is_ok());
        // PreparingRebalance: REBALANCE_IN_PROGRESS, and the heartbeat counts.
        let _b = join(&c, "g", None);
        age_member(&c, "g", &a, Duration::from_secs(20));
        assert!(matches!(
            c.heartbeat("g".to_string(), a.clone(), 1),
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
        assert!(c.heartbeat("g".to_string(), follower.clone(), gen2).is_ok());

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
        let slot = join_with(&c, params("g", Some(&follower), &[("range", b"new")]));
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
        assert_eq!(join_error_code(&join_with(&c, params("n", None, &[]))), E);
        let mut p = params("n", None, &[("range", b"")]);
        p.protocol_type = String::new();
        assert_eq!(join_error_code(&join_with(&c, p)), E);
        assert!(c.get_group_state("n").is_none());

        let leader = stable_single(&c, "g");
        // Different protocol type, same protocol.
        let mut p = params("g", None, &[("range", b"sub")]);
        p.protocol_type = "connect".to_string();
        assert_eq!(join_error_code(&join_with(&c, p)), E);
        // Same type, no protocol in common.
        assert_eq!(
            join_error_code(&join_with(&c, params("g", None, &[("roundrobin", b"")]))),
            E
        );
        // Empty protocol list on an existing group.
        assert_eq!(join_error_code(&join_with(&c, params("g", None, &[]))), E);
        // None of these disturbed the group.
        assert_eq!(state(&c, "g"), GroupState::Stable);
        assert_eq!(c.get_group_state("g").unwrap().members.len(), 1);
        // The only member may switch to anything: there is no one else to agree with.
        let r = joined(&join_with(
            &c,
            params("g", Some(&leader), &[("roundrobin", b"x")]),
        ));
        assert_eq!(r.protocol_name.as_deref(), Some("roundrobin"));
    }

    #[test]
    fn test_protocol_type_is_kept_when_group_empties() {
        let c = GroupCoordinator::new();
        let m = stable_single(&c, "g");
        c.leave_group("g".to_string(), m).unwrap();
        let g = c.get_group_state("g").unwrap();
        assert_eq!(g.state, GroupState::Empty);
        assert_eq!(g.protocol_type.as_deref(), Some("consumer"));
        // An Empty group accepts any protocol type again (Kafka's rule).
        let mut p = params("g", None, &[("x", b"")]);
        p.protocol_type = "connect".to_string();
        joined(&join_with(&c, p));
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
            params("g", None, &[("range", b""), ("roundrobin", b"")]),
        ));
        assert_eq!(leader.protocol_name.as_deref(), Some("range"));
        let f1 = join_with(&c, params("g", None, both_rr));
        let f2 = join_with(&c, params("g", None, both_rr));
        let l = joined(&join_with(
            &c,
            params(
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
        c.leave_group("a".to_string(), a).unwrap(); // "a" is Empty now
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
                .map(|e| e.to_kafka_error_code())
                .unwrap_or(0)
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
        c.leave_group("e".to_string(), e.clone()).unwrap();
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
            params("g", None, &[("sticky", b"s"), ("range", b"r")]),
        ))
        .member_id;
        let follower = join_with(&c, params("g", None, &[("range", b"r2")]));
        let l = joined(&join_with(
            &c,
            params("g", Some(&leader), &[("sticky", b"s"), ("range", b"r")]),
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
            params("g", None, &[("roundrobin", b"rr-leader")]),
        ))
        .member_id;
        let follower = join_with(
            &c,
            params(
                "g",
                None,
                &[("range", b"range-follower"), ("roundrobin", b"rr-follower")],
            ),
        );
        let l = joined(&join_with(
            &c,
            params("g", Some(&leader), &[("roundrobin", b"rr-leader")]),
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
        c.leave_group("g".to_string(), a.clone()).unwrap();
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
        c.leave_group("g".to_string(), leader.clone()).unwrap();
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
        c.leave_group("g".to_string(), leader).unwrap();
        assert_eq!(state(&c, "g"), GroupState::PreparingRebalance);

        let c = GroupCoordinator::new();
        let a = stable_single(&c, "g2");
        let b = join(&c, "g2", None);
        let _ = joined(&join(&c, "g2", Some(&a)));
        let b = joined(&b).member_id;
        assert_eq!(state(&c, "g2"), GroupState::CompletingRebalance);
        c.leave_group("g2".to_string(), b).unwrap();
        assert_eq!(state(&c, "g2"), GroupState::PreparingRebalance);
    }

    #[test]
    fn test_last_leave_empties_group_and_keeps_generation() {
        let c = GroupCoordinator::new();
        let a = stable_single(&c, "g");
        c.leave_group("g".to_string(), a.clone()).unwrap();
        let g = c.get_group_state("g").unwrap();
        assert_eq!(g.state, GroupState::Empty);
        assert!(g.leader.is_none());
        assert_eq!(g.generation_id, 1);
        assert!(matches!(
            c.leave_group("g".to_string(), a),
            Err(KafkaError::UnknownMemberId { .. })
        ));
        assert!(matches!(
            c.leave_group("nope".to_string(), "m".to_string()),
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
        assert!(c.check_and_remove_timed_out_members().is_empty());
        assert!(is_pending(&b_slot));
        assert_eq!(c.pending_join_count(), 1);
        // When the phase completes, the session clock restarts.
        let _ = joined(&join(&c, "g", Some(&a)));
        assert!(joined(&b_slot).generation_id == 2);
        assert!(c.check_and_remove_timed_out_members().is_empty());
    }

    #[test]
    fn test_sweep_rebalance_timeout_evicts_stragglers_and_completes_join() {
        let c = GroupCoordinator::new();
        let (leader, follower, gen) = stable_pair(&c, "g");
        let third = join(&c, "g", None);
        let l = join(&c, "g", Some(&leader));
        // The follower keeps heartbeating but never rejoins.
        assert!(matches!(
            c.heartbeat("g".to_string(), follower.clone(), gen),
            Err(KafkaError::RebalanceInProgress { .. })
        ));
        // Within the 60s rebalance timeout nothing happens.
        age_rebalance(&c, "g", Duration::from_secs(59));
        assert!(c.check_and_remove_timed_out_members().is_empty());
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
            c.heartbeat("g".to_string(), follower, gen + 1),
            Err(KafkaError::UnknownMemberId { .. })
        ));
        // The sync phase starts a fresh window: the leader is not evicted next sweep.
        assert!(c.check_and_remove_timed_out_members().is_empty());
    }

    #[test]
    fn test_rebalance_timeout_honors_member_rebalance_timeout_ms() {
        // CG-3: the timeout is the largest rebalance_timeout_ms the members advertise,
        // not a hardcoded 5 minutes. join_group also runs it (over every group).
        let c = GroupCoordinator::new();
        let mut p = params("g", None, &[("range", b"sub")]);
        p.rebalance_timeout_ms = 20;
        p.session_timeout_ms = 300_000;
        let a = joined(&join_with(&c, p)).member_id;
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
                g.upsert_member(&format!("m{}", i), &params("g", None, &[("range", b"")]));
            }
            g.state = GroupState::Stable;
            groups.insert("g".to_string(), g);
        }
        assert_eq!(
            join_error_code(&join(&c, "g", None)),
            crate::kafka::constants::ERROR_GROUP_MAX_SIZE_REACHED
        );
        // A known member may still rejoin.
        let rejoin = joined(&join_with(&c, params("g", Some("m1"), &[("range", b"")])));
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
        assert!(c.check_and_remove_timed_out_members().is_empty());
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
        c.leave_group("g".to_string(), x.member_id).unwrap();
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
        c.leave_group("g".to_string(), follower).unwrap();
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
        assert!(c.check_and_remove_timed_out_members().is_empty());
        assert!(sync_pending(&parked));
        // (Clients heartbeat while their SyncGroup waits; Heartbeat answers NONE.)
        assert!(c.heartbeat("g".to_string(), follower.clone(), gen).is_ok());

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
}
