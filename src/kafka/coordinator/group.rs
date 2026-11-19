//! One consumer group's state machine: its members, the join and sync phases, and
//! the answers for held JoinGroups and parked SyncGroups it produces.

use std::collections::HashMap;
use std::time::Instant;

use super::{JoinGroupParams, JoinOutcome, JoinResult, SyncOutcome};
use crate::kafka::error::KafkaError;
use crate::pg_log;

/// TTL for empty consumer groups before cleanup (24 hours)
pub(super) const EMPTY_GROUP_TTL_SECS: u64 = 86400;

/// Default rebalance timeout (5 minutes), used when a member advertises neither a
/// positive rebalance timeout nor a positive session timeout.
pub(super) const DEFAULT_REBALANCE_TIMEOUT_MS: u64 = 300_000;

/// The answer carried by a [`Delivery`].
pub(super) enum DeliveryOutcome {
    Join(JoinOutcome),
    Sync(SyncOutcome),
}

/// An answer for a held JoinGroup or parked SyncGroup, produced by the group state
/// machine and routed to the member's responder by the coordinator once its locks are
/// released.
pub(super) struct Delivery {
    pub(super) group_id: String,
    pub(super) member_id: String,
    pub(super) outcome: DeliveryOutcome,
}

impl Delivery {
    pub(super) fn join(group_id: &str, member_id: &str, outcome: JoinOutcome) -> Self {
        Self {
            group_id: group_id.to_string(),
            member_id: member_id.to_string(),
            outcome: DeliveryOutcome::Join(outcome),
        }
    }

    pub(super) fn sync(group_id: &str, member_id: &str, outcome: SyncOutcome) -> Self {
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
    pub(super) fn effective_rebalance_timeout_ms(&self) -> u128 {
        if self.rebalance_timeout_ms > 0 {
            self.rebalance_timeout_ms as u128
        } else if self.session_timeout_ms > 0 {
            self.session_timeout_ms as u128
        } else {
            u128::from(DEFAULT_REBALANCE_TIMEOUT_MS)
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
    pub(super) fn new(group_id: String) -> Self {
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
    /// (a known member's own previous list does not count, it is being replaced; Kafka counts
    /// it, so a lone member there cannot switch to a disjoint protocol list without leaving).
    pub(super) fn supports_protocols(
        &self,
        member_id: Option<&str>,
        protocol_type: &str,
        protocols: &[(String, Vec<u8>)],
    ) -> bool {
        if protocols.is_empty() || protocol_type.is_empty() {
            return false;
        }
        // A group with members keeps its protocol type, even for its only member's rejoin.
        if !self.members.is_empty() && self.protocol_type.as_deref() != Some(protocol_type) {
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
        protocols.iter().any(|(name, _)| {
            others
                .iter()
                .all(|m| m.protocols.iter().any(|(n, _)| n == name))
        })
    }

    /// Insert or update a member from its JoinGroup. An existing member keeps its
    /// assignment and its `awaiting_join` flag; the first member of a leaderless group
    /// becomes the leader.
    pub(super) fn upsert_member(&mut self, member_id: &str, params: &JoinGroupParams) {
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
    pub(super) fn member_metadata(&self) -> Vec<(String, Vec<u8>)> {
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
    pub(super) fn join_result_for(&self, member_id: &str) -> JoinResult {
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
    pub(super) fn prepare_rebalance(&mut self, out: &mut Vec<Delivery>) {
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
    pub(super) fn maybe_complete_join(&mut self, out: &mut Vec<Delivery>) {
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
    pub(super) fn select_group_protocol(&self) -> Option<String> {
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
    pub(super) fn remove_member(&mut self, member_id: &str, out: &mut Vec<Delivery>) -> bool {
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
    pub(super) fn on_members_removed(&mut self, out: &mut Vec<Delivery>) {
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
    pub(super) fn expire_stuck_rebalance(&mut self, out: &mut Vec<Delivery>) -> Vec<String> {
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
            .unwrap_or(u128::from(DEFAULT_REBALANCE_TIMEOUT_MS));
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
    pub(super) fn is_expired_empty(&self) -> bool {
        self.state == GroupState::Empty
            && self.last_activity.elapsed().as_secs() >= EMPTY_GROUP_TTL_SECS
    }

    /// Complete the sync phase and move to Stable. Members the leader did not assign
    /// anything get an empty assignment, as in Kafka. Every parked follower SyncGroup
    /// is answered with the member's assignment.
    pub(super) fn complete_sync(&mut self, out: &mut Vec<Delivery>) {
        // Save current assignments for sticky strategy
        self.previous_assignments.clear();
        let mut parked = Vec::new();
        for (member_id, member) in &mut self.members {
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
