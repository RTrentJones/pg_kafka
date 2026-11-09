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
//! Consumer groups transition through these states:
//! ```text
//! Empty → PreparingRebalance → CompletingRebalance → Stable → Empty
//!   ↑                                                          ↓
//!   └──────────────────────────────────────────────────────────┘
//! ```
//!
//! - **Empty**: No members in the group
//! - **PreparingRebalance**: Waiting for all members to rejoin (triggered by member join/leave)
//! - **CompletingRebalance**: Waiting for leader to provide assignments via SyncGroup
//! - **Stable**: All members have assignments and are consuming
//! - **Dead**: defined for protocol completeness but never entered — an emptied
//!   group transitions to `Empty` and is reaped by the empty-group TTL sweep
//!   rather than being marked `Dead`. It is treated like `Empty` where matched.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Instant;

use super::error::{KafkaError, Result};

/// TTL for empty consumer groups before cleanup (24 hours)
const EMPTY_GROUP_TTL_SECS: u64 = 86400;

/// Maximum number of consumer groups to prevent DoS
const MAX_CONSUMER_GROUPS: usize = 100_000;

/// CG-4: maximum members in a single consumer group (DoS protection). A genuinely new member is
/// rejected with GROUP_MAX_SIZE_REACHED once a group is full; rejoining members are always allowed.
const MAX_MEMBERS_PER_GROUP: usize = 1000;

/// Default rebalance timeout (5 minutes)
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

    /// Generation this member last (re)joined. The join phase of a rebalance
    /// completes once every member has joined the current generation.
    pub last_join_generation: i32,

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
}

/// Consumer group state machine states
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupState {
    /// No members in the group
    Empty,

    /// Waiting for all members to rejoin (rebalance triggered)
    PreparingRebalance,

    /// Waiting for leader to send partition assignments
    CompletingRebalance,

    /// All members have assignments and are consuming
    Stable,

    /// Group is being deleted (future feature)
    Dead,
}

impl GroupState {
    /// The Kafka wire-protocol group-state string, echoed in DescribeGroups /
    /// ListGroups. Mirrors `TransactionState::as_str`; centralizes the mapping
    /// that was previously duplicated across the two handlers.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Empty => "Empty",
            Self::PreparingRebalance => "PreparingRebalance",
            Self::CompletingRebalance => "CompletingRebalance",
            Self::Stable => "Stable",
            Self::Dead => "Dead",
        }
    }
}

/// Consumer group metadata and state
#[derive(Debug, Clone)]
pub struct ConsumerGroup {
    /// Consumer group ID
    pub group_id: String,

    /// Generation ID (increments on each rebalance)
    /// Used to detect stale member state
    pub generation_id: i32,

    /// Chosen partition assignment strategy
    /// Selected from members' protocols during JoinGroup
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

    /// Timestamp when rebalance started (for timeout detection)
    pub rebalance_started_at: Option<Instant>,

    /// The generation of the last completed SyncGroup: the generation members were
    /// actually assigned partitions under. pg_kafka answers JoinGroup immediately, so
    /// `generation_id` moves forward as soon as a rebalance starts; members that have
    /// not rejoined yet still hold this one. Kafka keeps the old generation until the
    /// join completes, so such a member gets REBALANCE_IN_PROGRESS from Heartbeat (not
    /// ILLEGAL_GENERATION) and may still commit offsets while the group is
    /// PreparingRebalance (the commit it makes when its partitions are revoked).
    /// 0 until the first sync completes.
    pub stable_generation: i32,
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
            stable_generation: 0,
        }
    }

    /// Add a member to the group
    /// Returns (member_id, is_leader)
    #[allow(clippy::too_many_arguments)]
    fn add_member(
        &mut self,
        member_id: String,
        client_id: String,
        client_host: String,
        session_timeout_ms: i32,
        rebalance_timeout_ms: i32,
        protocol_type: String,
        protocols: Vec<(String, Vec<u8>)>,
        group_instance_id: Option<String>,
    ) -> (String, bool) {
        // A member rejoining without starting a rebalance (e.g. a follower that synced
        // before its leader and retried) keeps its assignment; start_rebalance clears
        // assignments when membership is about to change.
        let assignment = self
            .members
            .get(&member_id)
            .and_then(|m| m.assignment.clone());
        let member = GroupMember {
            member_id: member_id.clone(),
            client_id,
            client_host,
            session_timeout_ms,
            rebalance_timeout_ms,
            protocol_type,
            protocols,
            assignment,
            last_heartbeat: Instant::now(),
            last_join_generation: self.generation_id,
            group_instance_id,
        };

        // The first member to join an empty group becomes the leader; an existing leader
        // rejoining (e.g. during a rebalance) is still the leader. BUG-9: the old check
        // `self.members.is_empty()` reported a rejoining leader as a non-leader with an empty
        // member list — it only "worked" because SyncGroup recomputes assignments server-side,
        // so a client doing real client-side assignment would mis-assign / rebalance-loop.
        let is_leader = if self.leader.is_none() {
            self.leader = Some(member_id.clone());
            true
        } else {
            self.leader.as_deref() == Some(member_id.as_str())
        };

        self.members.insert(member_id.clone(), member);

        (member_id, is_leader)
    }

    /// Complete the join phase if every member has (re)joined the current
    /// generation. Called after joins and after member removals: a rebalance
    /// can also become completable when the last straggler leaves or times out.
    ///
    /// The chosen protocol is the most-preferred strategy supported by every member
    /// (see `select_group_protocol`).
    fn maybe_complete_join(&mut self) {
        if self.state != GroupState::PreparingRebalance || self.members.is_empty() {
            return;
        }

        let all_joined = self
            .members
            .values()
            .all(|m| m.last_join_generation == self.generation_id);
        if !all_joined {
            return;
        }

        if let Some(protocol) = self.select_group_protocol() {
            self.complete_join(protocol);
        }
    }

    /// CG-1: choose the group protocol as the most-preferred strategy supported by EVERY member
    /// (Kafka's common-protocol rule), walking the leader's preference order. The previous code took
    /// the leader's first advertised strategy outright, so a follower that didn't support it would be
    /// handed a server-computed assignment for a strategy it never asked for. Falls back to the
    /// leader's first advertised strategy only when the members share none (non-conformant clients).
    fn select_group_protocol(&self) -> Option<String> {
        let leader = self
            .leader
            .as_ref()
            .and_then(|l| self.members.get(l))
            .or_else(|| self.members.values().next())?;

        for (name, _) in &leader.protocols {
            let supported_by_all = self
                .members
                .values()
                .all(|m| m.protocols.iter().any(|(n, _)| n == name));
            if supported_by_all {
                return Some(name.clone());
            }
        }

        leader.protocols.first().map(|(name, _)| name.clone())
    }

    /// Remove a member from the group
    fn remove_member(&mut self, member_id: &str) -> bool {
        if self.members.remove(member_id).is_some() {
            // Clean up previous assignments for this member (memory leak fix)
            self.previous_assignments.remove(member_id);

            // If leader left, reassign leadership
            if self.leader.as_deref() == Some(member_id) {
                self.leader = self.members.keys().next().cloned();
            }

            // If no members left, return to Empty state.
            // NOTE: generation_id is intentionally NOT reset. The generation is
            // the zombie-fencing token: if it restarted at 0, a stale client
            // from a previous incarnation of the group could pass the
            // generation checks. Kafka keeps it monotonic for the lifetime of
            // the group; so do we.
            if self.members.is_empty() {
                self.state = GroupState::Empty;
                self.protocol_name = None;
                self.leader = None;
                // Update last_activity for TTL tracking
                self.last_activity = Instant::now();
            }

            true
        } else {
            false
        }
    }

    /// Whether `member_id`, presenting `generation_id`, is a member still on the last
    /// stable generation that has not rejoined the rebalance now in progress.
    fn is_awaiting_rejoin(&self, member_id: &str, generation_id: i32) -> bool {
        matches!(
            self.state,
            GroupState::PreparingRebalance | GroupState::CompletingRebalance
        ) && self.stable_generation > 0
            && generation_id == self.stable_generation
            && generation_id != self.generation_id
            && self
                .members
                .get(member_id)
                .is_some_and(|m| m.last_join_generation < self.generation_id)
    }

    /// Handle a rebalance that exceeded its rebalance timeout (the largest
    /// rebalance_timeout_ms the members advertised, as in Kafka):
    /// - join phase (PreparingRebalance): evict only the members that did not rejoin,
    ///   then complete the join with the ones that did;
    /// - sync phase (CompletingRebalance): the leader never delivered assignments, so
    ///   evict it and start a new rebalance with the remaining members.
    ///
    /// Returns the evicted member ids.
    fn expire_stuck_rebalance(&mut self) -> Vec<String> {
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
        // group, matching Kafka) rather than a hardcoded value; fall back to the default
        // when no member advertises a positive one (e.g. a v0 JoinGroup).
        let effective_timeout_ms = self
            .members
            .values()
            .map(|m| m.rebalance_timeout_ms)
            .filter(|&t| t > 0)
            .max()
            .map(|t| t as u128)
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
            self.remove_member(&leader);
            if !self.members.is_empty() {
                self.start_rebalance();
            } else {
                self.rebalance_started_at = None;
            }
            return vec![leader];
        }
        let stragglers: Vec<String> = self
            .members
            .values()
            .filter(|m| m.last_join_generation != self.generation_id)
            .map(|m| m.member_id.clone())
            .collect();
        pg_log!(
            "Group {} rebalance timed out after {}ms; evicting {} member(s) that did not rejoin",
            self.group_id,
            started.elapsed().as_millis(),
            stragglers.len()
        );
        for member_id in &stragglers {
            self.remove_member(member_id);
        }
        self.maybe_complete_join();
        stragglers
    }

    /// Whether this group is Empty and has been idle past `EMPTY_GROUP_TTL_SECS`,
    /// so the coordinator may drop it.
    fn is_expired_empty(&self) -> bool {
        self.state == GroupState::Empty
            && self.last_activity.elapsed().as_secs() >= EMPTY_GROUP_TTL_SECS
    }

    /// Start a rebalance (increment generation, move to PreparingRebalance)
    fn start_rebalance(&mut self) {
        // Assignments belong to the generation being replaced; the leader's next
        // SyncGroup hands out new ones.
        for member in self.members.values_mut() {
            member.assignment = None;
        }
        self.generation_id += 1;
        self.state = GroupState::PreparingRebalance;
        self.rebalance_started_at = Some(Instant::now());
        pg_log!(
            "Group {} starting rebalance (generation {})",
            self.group_id,
            self.generation_id
        );
    }

    /// Complete join phase and move to CompletingRebalance
    /// This happens when all expected members have joined
    fn complete_join(&mut self, protocol_name: String) {
        self.protocol_name = Some(protocol_name);
        self.state = GroupState::CompletingRebalance;
        pg_log!(
            "Group {} completed join phase (generation {})",
            self.group_id,
            self.generation_id
        );
    }

    /// Complete sync phase and move to Stable
    /// This happens when leader provides assignments
    fn complete_sync(&mut self) {
        // Save current assignments for sticky strategy
        self.previous_assignments.clear();
        for (member_id, member) in &self.members {
            if let Some(assignment) = &member.assignment {
                self.previous_assignments
                    .insert(member_id.clone(), assignment.clone());
            }
        }

        self.state = GroupState::Stable;
        self.stable_generation = self.generation_id;
        self.rebalance_started_at = None; // Clear rebalance timeout tracking
        self.last_activity = Instant::now();
        pg_log!(
            "Group {} completed sync phase (generation {})",
            self.group_id,
            self.generation_id
        );
    }
}

/// Thread-safe consumer group coordinator
///
/// Manages multiple consumer groups with concurrent access from multiple Kafka requests.
/// Uses RwLock for thread-safe access (readers can proceed in parallel, writers get exclusive access).
pub struct GroupCoordinator {
    /// Consumer groups by group_id
    pub groups: Arc<RwLock<HashMap<String, ConsumerGroup>>>,
}

impl GroupCoordinator {
    /// Create a new group coordinator
    pub fn new() -> Self {
        Self {
            groups: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Handle JoinGroup request
    ///
    /// Returns (member_id, generation_id, is_leader, members_for_leader)
    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::type_complexity)]
    pub fn join_group(
        &self,
        group_id: String,
        existing_member_id: Option<String>, // None for first join, Some(id) for rejoin
        client_id: String,
        client_host: String,
        session_timeout_ms: i32,
        rebalance_timeout_ms: i32,
        protocol_type: String,
        protocols: Vec<(String, Vec<u8>)>,
        group_instance_id: Option<String>,
    ) -> Result<(String, i32, bool, String, Vec<(String, Vec<u8>)>)> {
        let mut groups = self.groups.write().map_err(|e| {
            KafkaError::Internal(format!("Failed to acquire groups write lock: {}", e))
        })?;

        // Clean up stale empty groups (TTL-based)
        groups.retain(|_, g| !g.is_expired_empty());

        // Check for rebalance timeouts: evict the members that never rejoined and
        // complete the join with the rest (also run by the periodic sweep).
        for group in groups.values_mut() {
            group.expire_stuck_rebalance();
        }

        // Check MAX_CONSUMER_GROUPS limit (DoS protection)
        if !groups.contains_key(&group_id) && groups.len() >= MAX_CONSUMER_GROUPS {
            return Err(KafkaError::Internal(format!(
                "Maximum consumer groups limit ({}) reached",
                MAX_CONSUMER_GROUPS
            )));
        }

        // Get or create consumer group
        let group = groups
            .entry(group_id.clone())
            .or_insert_with(|| ConsumerGroup::new(group_id.clone()));

        // CG-4: cap members per group (DoS protection). A rejoining member (already present) is
        // always allowed; only a genuinely new member is rejected when the group is at capacity.
        let is_rejoin = existing_member_id
            .as_ref()
            .is_some_and(|id| group.members.contains_key(id));
        if !is_rejoin && group.members.len() >= MAX_MEMBERS_PER_GROUP {
            return Err(KafkaError::Protocol {
                code: crate::kafka::constants::ERROR_GROUP_MAX_SIZE_REACHED,
                message: format!(
                    "group '{}' has reached the maximum of {} members",
                    group_id, MAX_MEMBERS_PER_GROUP
                ),
            });
        }

        // Generate member ID if this is a new member
        let member_id = if let Some(id) = existing_member_id {
            // Rejoining with existing ID
            if !group.members.contains_key(&id) {
                // Member ID not found - treat as new member
                generate_member_id(&client_id)
            } else {
                id
            }
        } else {
            // New member
            generate_member_id(&client_id)
        };

        pg_log!(
            "JoinGroup: group_id={}, member_id={}, state={:?}",
            group.group_id,
            member_id,
            group.state
        );

        // Determine if we need to rebalance
        let needs_rebalance = match group.state {
            GroupState::Empty => true, // First member joining
            // A stable group rebalances for a new member, a member whose subscription
            // (protocol metadata) changed, or a rejoining leader (Kafka's rule: the
            // leader rejoins to make the group pick up new topics or partitions). A
            // follower rejoining with unchanged metadata keeps the current generation
            // and its assignment.
            GroupState::Stable => match group.members.get(&member_id) {
                None => true,
                Some(existing) => {
                    existing.protocols != protocols
                        || group.leader.as_deref() == Some(member_id.as_str())
                }
            },
            // New member joining while the leader may already be computing
            // assignments: restart the join phase so the new generation's
            // member list (and thus the leader's assignments) includes it
            GroupState::CompletingRebalance => !group.members.contains_key(&member_id),
            _ => false, // Already in the join phase
        };

        if needs_rebalance {
            group.start_rebalance();
        }

        // Add or update member
        let (assigned_member_id, is_leader) = group.add_member(
            member_id.clone(),
            client_id,
            client_host,
            session_timeout_ms,
            rebalance_timeout_ms,
            protocol_type.clone(),
            protocols.clone(),
            group_instance_id,
        );

        // Complete the join phase once every member has rejoined the current
        // generation. Gating on full membership (rather than completing on the
        // first join) means the leader's member list — and therefore its
        // assignments — covers everyone; stragglers are evicted by the
        // session/rebalance timeout sweeps.
        group.maybe_complete_join();

        // If this is the leader, return member list
        let members_for_leader = if is_leader {
            group
                .members
                .values()
                .flat_map(|m| {
                    // Hand the leader the metadata for the group-SELECTED protocol
                    // (what it computes assignments from), not just the member's
                    // first-listed one — mirrors compute_assignments' selection.
                    // Fall back to the first protocol only when the member didn't
                    // advertise the selected one (transient pre-join window).
                    m.protocols
                        .iter()
                        .find(|(name, _)| Some(name) == group.protocol_name.as_ref())
                        .or_else(|| m.protocols.first())
                        .map(|(_, metadata)| (m.member_id.clone(), metadata.clone()))
                })
                .collect()
        } else {
            Vec::new()
        };

        // Get the leader ID (should always exist after add_member)
        let leader_id = group.leader.clone().unwrap_or_default();

        Ok((
            assigned_member_id,
            group.generation_id,
            is_leader,
            leader_id,
            members_for_leader,
        ))
    }

    /// Handle SyncGroup request
    ///
    /// Leader provides assignments for all members.
    /// Followers send empty assignments and wait for their assignment.
    ///
    /// Returns the assignment for the requesting member.
    pub fn sync_group(
        &self,
        group_id: String,
        member_id: String,
        generation_id: i32,
        assignments: Vec<(String, Vec<u8>)>, // (member_id, assignment_bytes)
    ) -> Result<Vec<u8>> {
        let mut groups = self.groups.write().map_err(|e| {
            KafkaError::Internal(format!("Failed to acquire groups write lock: {}", e))
        })?;

        // A group this coordinator has no record of (e.g. lost in a worker restart):
        // UNKNOWN_MEMBER_ID, as Kafka answers, makes the client rejoin.
        // COORDINATOR_NOT_AVAILABLE made it look up the same coordinator and retry.
        let group = groups
            .get_mut(&group_id)
            .ok_or_else(|| KafkaError::unknown_member(&group_id, &member_id))?;

        // Validate generation ID
        if group.generation_id != generation_id {
            return Err(KafkaError::illegal_generation(
                &group_id,
                generation_id,
                group.generation_id,
            ));
        }

        // Validate member ID
        if !group.members.contains_key(&member_id) {
            return Err(KafkaError::unknown_member(&group_id, &member_id));
        }

        pg_log!(
            "SyncGroup: group_id={}, member_id={}, is_leader={}, assignments={}",
            group_id,
            member_id,
            group.leader.as_deref() == Some(&member_id),
            assignments.len()
        );

        // SyncGroup is only valid once the join phase has completed. During
        // PreparingRebalance the membership is still settling, so the member
        // must rejoin (clients react to REBALANCE_IN_PROGRESS by rejoining).
        if group.state == GroupState::PreparingRebalance {
            return Err(KafkaError::RebalanceInProgress {
                group_id: group_id.clone(),
            });
        }

        // If this is the leader, store assignments for all members
        if group.leader.as_deref() == Some(&member_id) {
            // The leader computed these from the member list in its JoinGroup response.
            // If a member it assigned to has left since (LeaveGroup or session timeout
            // during the join phase, which does not move the generation), that member's
            // partitions would go unconsumed. Restart the rebalance instead.
            if let Some(departed) = assignments
                .iter()
                .map(|(id, _)| id)
                .find(|id| !group.members.contains_key(*id))
            {
                pg_log!(
                    "SyncGroup: leader {} assigned to departed member {} in group {}; restarting rebalance",
                    member_id,
                    departed,
                    group_id
                );
                group.start_rebalance();
                return Err(KafkaError::RebalanceInProgress {
                    group_id: group_id.clone(),
                });
            }
            for (target_member_id, assignment_bytes) in assignments.iter() {
                if let Some(member) = group.members.get_mut(target_member_id) {
                    member.assignment = Some(assignment_bytes.clone());
                }
            }

            // Move group to Stable state
            group.complete_sync();
        }

        // Return assignment for this member
        match group
            .members
            .get(&member_id)
            .and_then(|m| m.assignment.clone())
        {
            Some(assignment) => Ok(assignment),
            None => {
                if group.state == GroupState::Stable {
                    // The leader has already synced this generation but had no
                    // assignment for us: we joined after the leader received
                    // its member list, so we are invisible to the current
                    // assignment. Returning an empty assignment here would
                    // leave this member silently consuming nothing forever.
                    // Force a full rebalance so the leader recomputes with
                    // complete membership.
                    pg_log!(
                        "SyncGroup: member {} has no assignment in stable group {}, forcing rebalance",
                        member_id,
                        group_id
                    );
                    group.start_rebalance();
                }
                // CompletingRebalance: the leader simply hasn't provided
                // assignments yet; the member rejoins and retries.
                Err(KafkaError::RebalanceInProgress {
                    group_id: group_id.clone(),
                })
            }
        }
    }

    /// CONF-1: validate an OffsetCommit against the live group state to reject zombie commits.
    ///
    /// A simple consumer with no active group membership commits with `generation_id < 0` and is not
    /// validated. Otherwise the commit's generation and member id must match the current ones — a
    /// stale generation yields `IllegalGeneration` and an unknown member yields `UnknownMemberId`.
    ///
    /// RA-5: a generation-bearing commit for a group this coordinator has *no record of* is fenced
    /// with `UnknownMemberId` (a zombie whose group was reaped by the empty-group sweep, or whose
    /// coordinator restarted), forcing it to rejoin and get a fresh generation. Because coordinator
    /// state is in-memory, this means that after a bgworker restart every consumer holding a
    /// generation must rejoin — a brief rejoin/rebalance storm on restart. That is deliberate and
    /// correct (the old generations are gone), not a regression; the earlier "unknown group is
    /// allowed" behavior would have let a zombie commit through.
    ///
    /// RV-10: even for a known group at the current generation, a commit that arrives while the group
    /// is `PreparingRebalance`/`CompletingRebalance` is fenced with `RebalanceInProgress` so the
    /// member rejoins before committing.
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

        if group.generation_id != generation_id {
            // A member that has not rejoined yet, committing under the generation it
            // was assigned in, while the join phase runs: Kafka accepts this (it is
            // the commit a consumer makes when its partitions are revoked).
            if group.state == GroupState::PreparingRebalance
                && group.is_awaiting_rejoin(member_id, generation_id)
            {
                return Ok(());
            }
            return Err(KafkaError::illegal_generation(
                group_id,
                generation_id,
                group.generation_id,
            ));
        }

        if !group.members.contains_key(member_id) {
            return Err(KafkaError::unknown_member(group_id, member_id));
        }

        // RV-10: fence an OffsetCommit that arrives while the group is rebalancing
        // with REBALANCE_IN_PROGRESS, matching Heartbeat and Kafka's coordinator.
        // A commit accepted mid-rebalance could persist an offset for a partition
        // the member is about to lose, so the client must rejoin and re-fetch its
        // assignment before committing. Checked after the generation/member fences
        // so a stale member still gets the more specific ILLEGAL_GENERATION /
        // UNKNOWN_MEMBER_ID.
        if group.state == GroupState::PreparingRebalance
            || group.state == GroupState::CompletingRebalance
        {
            return Err(KafkaError::RebalanceInProgress {
                group_id: group_id.to_string(),
            });
        }

        Ok(())
    }

    /// The group's selected common protocol, set once a rebalance completes
    /// (`None` if the group is unknown or hasn't completed a join).
    ///
    /// RA-6: the JoinGroup response should advertise the protocol the GROUP
    /// agreed on (the leader's preference that every member supports), not just
    /// the requesting member's first advertised preference — heterogeneous
    /// clients advertising protocols in different orders would otherwise each get
    /// a different `protocol_name` back.
    pub fn group_protocol_name(&self, group_id: &str) -> Option<String> {
        let groups = self.groups.read().ok()?;
        groups.get(group_id).and_then(|g| g.protocol_name.clone())
    }

    /// Handle Heartbeat request
    ///
    /// Updates member's last_heartbeat timestamp.
    /// Returns error if member is unknown or generation mismatches.
    pub fn heartbeat(&self, group_id: String, member_id: String, generation_id: i32) -> Result<()> {
        let mut groups = self.groups.write().map_err(|e| {
            KafkaError::Internal(format!("Failed to acquire groups write lock: {}", e))
        })?;

        // Unknown group (e.g. lost in a worker restart): UNKNOWN_MEMBER_ID so the
        // client rejoins, as Kafka answers.
        let group = groups
            .get_mut(&group_id)
            .ok_or_else(|| KafkaError::unknown_member(&group_id, &member_id))?;

        // A member still on the last stable generation while a rebalance it has not
        // rejoined is running: tell it to rejoin, and keep its session alive meanwhile.
        if group.is_awaiting_rejoin(&member_id, generation_id) {
            if let Some(member) = group.members.get_mut(&member_id) {
                member.last_heartbeat = Instant::now();
            }
            return Err(KafkaError::RebalanceInProgress {
                group_id: group_id.clone(),
            });
        }

        // Validate generation ID
        if group.generation_id != generation_id {
            return Err(KafkaError::illegal_generation(
                &group_id,
                generation_id,
                group.generation_id,
            ));
        }

        // Validate member ID and update heartbeat
        let member = group
            .members
            .get_mut(&member_id)
            .ok_or_else(|| KafkaError::unknown_member(&group_id, &member_id))?;

        member.last_heartbeat = Instant::now();

        // Check if group is rebalancing - force client to rejoin
        if group.state == GroupState::PreparingRebalance
            || group.state == GroupState::CompletingRebalance
        {
            return Err(KafkaError::RebalanceInProgress {
                group_id: group_id.clone(),
            });
        }

        Ok(())
    }

    /// Handle LeaveGroup request
    ///
    /// Removes member from the group.
    /// If members remain and group was Stable, triggers rebalance.
    pub fn leave_group(&self, group_id: String, member_id: String) -> Result<()> {
        let mut groups = self.groups.write().map_err(|e| {
            KafkaError::Internal(format!("Failed to acquire groups write lock: {}", e))
        })?;

        let group = groups
            .get_mut(&group_id)
            .ok_or_else(|| KafkaError::unknown_member(&group_id, &member_id))?;

        pg_log!("LeaveGroup: group_id={}, member_id={}", group_id, member_id);

        if group.remove_member(&member_id) {
            // Member removed successfully
            // Trigger rebalance for remaining members if group is still active.
            // CompletingRebalance counts too: the in-flight assignments may
            // reference the departed member, stranding its partitions.
            if !group.members.is_empty()
                && (group.state == GroupState::Stable
                    || group.state == GroupState::CompletingRebalance)
            {
                group.start_rebalance();
                pg_log!(
                    "LeaveGroup triggered rebalance for {} remaining members in group {}",
                    group.members.len(),
                    group_id
                );
            }
            // If the leaver was the last straggler of an in-flight rebalance,
            // the join phase may now be complete for the remaining members
            group.maybe_complete_join();
            Ok(())
        } else {
            Err(KafkaError::unknown_member(&group_id, &member_id))
        }
    }

    /// Check all groups for timed-out members and trigger rebalance
    ///
    /// This method should be called periodically (e.g., every ~1 second) from the
    /// background worker main loop to detect and remove members that have stopped
    /// sending heartbeats.
    ///
    /// # Returns
    /// List of (group_id, member_id) pairs that were removed due to timeout
    pub fn check_and_remove_timed_out_members(&self) -> Vec<(String, String)> {
        let mut removed = Vec::new();

        let mut groups = match self.groups.write() {
            Ok(g) => g,
            Err(_) => {
                // Lock poisoned, skip this check
                return removed;
            }
        };

        for (group_id, group) in groups.iter_mut() {
            // Skip empty or dead groups
            if group.state == GroupState::Empty || group.state == GroupState::Dead {
                continue;
            }

            // A join phase that exceeded its rebalance timeout evicts the members that
            // never rejoined (previously only checked when some JoinGroup arrived).
            for member_id in group.expire_stuck_rebalance() {
                removed.push((group_id.clone(), member_id));
            }
            if group.state == GroupState::Empty {
                continue;
            }

            // Find timed-out members
            let timed_out: Vec<String> = group
                .members
                .iter()
                .filter(|(_, member)| member.is_timed_out())
                .map(|(id, _)| id.clone())
                .collect();

            // Remove timed-out members
            for member_id in &timed_out {
                pg_log!(
                    "Member {} timed out in group {} (session_timeout exceeded)",
                    member_id,
                    group_id
                );
                group.remove_member(member_id);
                removed.push((group_id.clone(), member_id.clone()));
            }

            // Trigger rebalance if members were removed and group still has members
            if !timed_out.is_empty() && !group.members.is_empty() {
                // During PreparingRebalance the generation already advanced
                // when the rebalance started; bumping it again would force
                // members that already rejoined to rejoin once more
                if group.state != GroupState::PreparingRebalance {
                    group.start_rebalance();
                }
                pg_log!(
                    "Timeout triggered rebalance for {} remaining members in group {}",
                    group.members.len(),
                    group_id
                );
                // If the evicted member was the last straggler of an
                // in-flight rebalance, the join phase is now complete
                group.maybe_complete_join();
            }
        }

        // CG-5: reclaim expired empty groups here (every sweep), not just on the next
        // join_group's pass, so coordinator memory stays bounded. An Empty group is kept until
        // EMPTY_GROUP_TTL_SECS has passed (the same rule join_group applies): dropping it
        // immediately would restart a rejoining group at generation 1, defeating the
        // monotonic generation that fences zombie consumers (see remove_member).
        groups.retain(|_, g| !g.is_expired_empty());

        removed
    }

    /// Get group state for debugging/testing
    #[allow(dead_code)]
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
    /// Should only be called after confirming the group has no active members.
    pub fn remove_group(&self, group_id: &str) {
        if let Ok(mut groups) = self.groups.write() {
            groups.remove(group_id);
            pg_log!("Removed group '{}' from coordinator", group_id);
        }
    }
}

impl Default for GroupCoordinator {
    fn default() -> Self {
        Self::new()
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

    #[test]
    fn test_generate_member_id() {
        let member_id1 = generate_member_id("test-client");
        let member_id2 = generate_member_id("test-client");

        // Should be unique
        assert_ne!(member_id1, member_id2);

        // Should contain client_id prefix
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
    fn test_consumer_group_add_first_member_becomes_leader() {
        let mut group = ConsumerGroup::new("test-group".to_string());

        let (member_id, is_leader) = group.add_member(
            "member-1".to_string(),
            "client-1".to_string(),
            "localhost".to_string(),
            30000,
            60000,
            "consumer".to_string(),
            vec![("range".to_string(), vec![1, 2, 3])],
            None,
        );

        assert_eq!(member_id, "member-1");
        assert!(is_leader);
        assert_eq!(group.leader, Some("member-1".to_string()));
        assert_eq!(group.members.len(), 1);
    }

    #[test]
    fn test_consumer_group_add_second_member_not_leader() {
        let mut group = ConsumerGroup::new("test-group".to_string());

        group.add_member(
            "member-1".to_string(),
            "client-1".to_string(),
            "localhost".to_string(),
            30000,
            60000,
            "consumer".to_string(),
            vec![],
            None,
        );

        let (member_id, is_leader) = group.add_member(
            "member-2".to_string(),
            "client-2".to_string(),
            "localhost".to_string(),
            30000,
            60000,
            "consumer".to_string(),
            vec![],
            None,
        );

        assert_eq!(member_id, "member-2");
        assert!(!is_leader);
        assert_eq!(group.leader, Some("member-1".to_string()));
        assert_eq!(group.members.len(), 2);
    }

    #[test]
    fn test_consumer_group_remove_member() {
        let mut group = ConsumerGroup::new("test-group".to_string());

        group.add_member(
            "member-1".to_string(),
            "client-1".to_string(),
            "localhost".to_string(),
            30000,
            60000,
            "consumer".to_string(),
            vec![],
            None,
        );

        assert!(group.remove_member("member-1"));
        assert_eq!(group.members.len(), 0);
        assert_eq!(group.state, GroupState::Empty);
    }

    #[test]
    fn test_consumer_group_remove_leader_reassigns() {
        let mut group = ConsumerGroup::new("test-group".to_string());

        group.add_member(
            "member-1".to_string(),
            "client-1".to_string(),
            "localhost".to_string(),
            30000,
            60000,
            "consumer".to_string(),
            vec![],
            None,
        );

        group.add_member(
            "member-2".to_string(),
            "client-2".to_string(),
            "localhost".to_string(),
            30000,
            60000,
            "consumer".to_string(),
            vec![],
            None,
        );

        assert_eq!(group.leader, Some("member-1".to_string()));

        group.remove_member("member-1");

        assert_eq!(group.members.len(), 1);
        assert_eq!(group.leader, Some("member-2".to_string()));
    }

    // ===== Phase 5: Automatic Rebalancing Tests =====

    #[test]
    fn test_rejoining_leader_remains_leader() {
        // BUG-9: a leader rejoining the group (e.g. during a rebalance) must still report
        // is_leader=true, not be demoted to a non-leader with an empty member list.
        let coordinator = GroupCoordinator::new();
        let (leader, _, leader_is_leader, ..) = coordinator
            .join_group(
                "bug9-group".to_string(),
                None,
                "client-1".to_string(),
                "localhost".to_string(),
                30000,
                60000,
                "consumer".to_string(),
                vec![("range".to_string(), vec![])],
                None,
            )
            .unwrap();
        assert!(leader_is_leader, "first member should be the leader");

        let (_m2, _, m2_is_leader, ..) = coordinator
            .join_group(
                "bug9-group".to_string(),
                None,
                "client-2".to_string(),
                "localhost".to_string(),
                30000,
                60000,
                "consumer".to_string(),
                vec![("range".to_string(), vec![])],
                None,
            )
            .unwrap();
        assert!(!m2_is_leader, "second member should not be the leader");

        // The leader rejoins the generation started by member 2's join.
        let (_, _, rejoined_is_leader, ..) = coordinator
            .join_group(
                "bug9-group".to_string(),
                Some(leader.clone()),
                "client-1".to_string(),
                "localhost".to_string(),
                30000,
                60000,
                "consumer".to_string(),
                vec![("range".to_string(), vec![])],
                None,
            )
            .unwrap();
        assert!(
            rejoined_is_leader,
            "a rejoining leader must remain the leader (BUG-9)"
        );
    }

    #[test]
    fn test_invalid_session_timeout_member_is_timed_out() {
        // CG-4: a non-positive session_timeout_ms must not make a member immortal. The old
        // `as u128` cast wrapped a negative value to ~1.8e19 ms; a freshly-heartbeated member
        // with such a timeout is now reported timed-out immediately (deterministic, no waiting).
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
            last_join_generation: 0,
            group_instance_id: None,
        };
        assert!(
            member.is_timed_out(),
            "a negative session timeout must not make a member immortal"
        );

        // A valid, freshly-heartbeated member is not timed out.
        let valid = GroupMember {
            session_timeout_ms: 30_000,
            last_heartbeat: Instant::now(),
            ..member
        };
        assert!(!valid.is_timed_out());
    }

    #[test]
    fn test_leave_group_triggers_rebalance_with_remaining_members() {
        let coordinator = GroupCoordinator::new();

        // Add two members to the group
        let (member1, ..) = coordinator
            .join_group(
                "test-group".to_string(),
                None,
                "client-1".to_string(),
                "localhost".to_string(),
                30000,
                60000,
                "consumer".to_string(),
                vec![("range".to_string(), vec![])],
                None,
            )
            .unwrap();

        let (member2, ..) = coordinator
            .join_group(
                "test-group".to_string(),
                None,
                "client-2".to_string(),
                "localhost".to_string(),
                30000,
                60000,
                "consumer".to_string(),
                vec![("range".to_string(), vec![])],
                None,
            )
            .unwrap();

        // member1 rejoins the generation started by member2's join,
        // completing the join phase
        let (_, gen) = rejoin(&coordinator, "test-group", &member1, "client-1");

        // Complete the sync to get to Stable state
        coordinator
            .sync_group(
                "test-group".to_string(),
                member1.clone(),
                gen,
                vec![
                    (member1.clone(), b"a1".to_vec()),
                    (member2.clone(), b"a2".to_vec()),
                ],
            )
            .unwrap();
        coordinator
            .sync_group("test-group".to_string(), member2.clone(), gen, vec![])
            .unwrap();

        // Verify group is now Stable
        {
            let groups = coordinator.groups.read().unwrap();
            let group = groups.get("test-group").unwrap();
            assert_eq!(group.state, GroupState::Stable);
            assert_eq!(group.members.len(), 2);
        }

        // Leave with one member - should trigger rebalance
        coordinator
            .leave_group("test-group".to_string(), member1)
            .unwrap();

        // Verify state is PreparingRebalance and one member remains
        {
            let groups = coordinator.groups.read().unwrap();
            let group = groups.get("test-group").unwrap();
            assert_eq!(group.state, GroupState::PreparingRebalance);
            assert_eq!(group.members.len(), 1);
        }
    }

    #[test]
    fn test_heartbeat_returns_rebalance_in_progress() {
        let coordinator = GroupCoordinator::new();

        // Add a member
        let (member_id, generation_id, ..) = coordinator
            .join_group(
                "test-group".to_string(),
                None,
                "client-1".to_string(),
                "localhost".to_string(),
                30000,
                60000,
                "consumer".to_string(),
                vec![("range".to_string(), vec![])],
                None,
            )
            .unwrap();

        // Group is in PreparingRebalance state after join
        // Heartbeat should return RebalanceInProgress
        let result = coordinator.heartbeat("test-group".to_string(), member_id, generation_id);

        assert!(matches!(
            result,
            Err(KafkaError::RebalanceInProgress { .. })
        ));
    }

    #[test]
    fn test_heartbeat_succeeds_in_stable_state() {
        let coordinator = GroupCoordinator::new();

        // Add a member
        let (member_id, generation_id, ..) = coordinator
            .join_group(
                "test-group".to_string(),
                None,
                "client-1".to_string(),
                "localhost".to_string(),
                30000,
                60000,
                "consumer".to_string(),
                vec![("range".to_string(), vec![])],
                None,
            )
            .unwrap();

        // Complete sync to get to Stable state
        coordinator
            .sync_group(
                "test-group".to_string(),
                member_id.clone(),
                generation_id,
                vec![(member_id.clone(), vec![1, 2, 3])],
            )
            .unwrap();

        // Verify group is Stable
        {
            let groups = coordinator.groups.read().unwrap();
            let group = groups.get("test-group").unwrap();
            assert_eq!(group.state, GroupState::Stable);
        }

        // Heartbeat should succeed in Stable state
        let result = coordinator.heartbeat("test-group".to_string(), member_id, generation_id);
        assert!(result.is_ok());
    }

    #[test]
    fn test_member_timeout_detection() {
        let coordinator = GroupCoordinator::new();

        // Add member with very short timeout (1ms)
        coordinator
            .join_group(
                "test-group".to_string(),
                None,
                "client-1".to_string(),
                "localhost".to_string(),
                1, // 1ms session timeout
                1000,
                "consumer".to_string(),
                vec![("range".to_string(), vec![])],
                None,
            )
            .unwrap();

        // Complete sync to get to stable state
        {
            let groups = coordinator.groups.read().unwrap();
            let group = groups.get("test-group").unwrap();
            assert_eq!(group.members.len(), 1);
        }

        // Wait for timeout to expire
        std::thread::sleep(std::time::Duration::from_millis(10));

        // Check and remove timed-out members
        let removed = coordinator.check_and_remove_timed_out_members();

        // Member should have been removed
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0].0, "test-group");

        // With its only member gone the group is Empty. It is kept (not reclaimed) until the
        // empty-group TTL passes, so its generation stays monotonic for a rejoin.
        let group = coordinator.get_group_state("test-group").unwrap();
        assert_eq!(group.state, GroupState::Empty);
    }

    #[test]
    fn test_rebalance_timeout_honors_member_rebalance_timeout_ms() {
        // CG-3: a stuck rebalance must be aborted after the members' advertised rebalance_timeout_ms,
        // not a hardcoded 5 minutes.
        let coordinator = GroupCoordinator::new();

        // group-1: one member with a very short rebalance budget (20ms) that never syncs, so the
        // group stays mid-rebalance with rebalance_started_at set. The session timeout is long so the
        // member itself is not reaped — we want the *rebalance* to time out.
        coordinator
            .join_group(
                "group-1".to_string(),
                None,
                "c1".to_string(),
                "localhost".to_string(),
                300_000, // session timeout (won't expire)
                20,      // rebalance_timeout_ms (short)
                "consumer".to_string(),
                vec![("range".to_string(), vec![])],
                None,
            )
            .unwrap();

        // Elapse past the 20ms rebalance budget — but far below the old hardcoded 5-minute default.
        std::thread::sleep(std::time::Duration::from_millis(50));

        // join_group runs the rebalance-timeout sweep over all groups up-front; a dummy second group
        // drives it without otherwise touching group-1.
        coordinator
            .join_group(
                "group-2".to_string(),
                None,
                "c2".to_string(),
                "localhost".to_string(),
                300_000,
                300_000,
                "consumer".to_string(),
                vec![("range".to_string(), vec![])],
                None,
            )
            .unwrap();

        // group-1's stuck rebalance must have been aborted (reset to Empty). With the old hardcoded
        // 5-minute timeout, 50ms is nowhere near expiry and the group would still be rebalancing.
        let g1 = coordinator
            .get_group_state("group-1")
            .expect("group-1 should exist");
        assert_eq!(g1.state, GroupState::Empty);
        assert!(g1.members.is_empty());
    }

    #[test]
    fn test_join_selects_strategy_common_to_all_members() {
        // CG-1: the group protocol must be a strategy supported by EVERY member, not just the
        // leader's first advertised preference.
        let coordinator = GroupCoordinator::new();

        // Leader c1 prefers "sticky" then "range"; c2 supports only "range".
        let (leader, ..) = coordinator
            .join_group(
                "g".to_string(),
                None,
                "c1".to_string(),
                "h".to_string(),
                300_000,
                300_000,
                "consumer".to_string(),
                vec![
                    ("sticky".to_string(), vec![]),
                    ("range".to_string(), vec![]),
                ],
                None,
            )
            .unwrap();
        coordinator
            .join_group(
                "g".to_string(),
                None,
                "c2".to_string(),
                "h".to_string(),
                300_000,
                300_000,
                "consumer".to_string(),
                vec![("range".to_string(), vec![])],
                None,
            )
            .unwrap();
        // Leader rejoins the new generation so the join phase completes and the protocol is chosen.
        coordinator
            .join_group(
                "g".to_string(),
                Some(leader),
                "c1".to_string(),
                "h".to_string(),
                300_000,
                300_000,
                "consumer".to_string(),
                vec![
                    ("sticky".to_string(), vec![]),
                    ("range".to_string(), vec![]),
                ],
                None,
            )
            .unwrap();

        // "sticky" is the leader's first preference, but c2 doesn't support it; the only strategy
        // common to both is "range", so that must be selected.
        let g = coordinator.get_group_state("g").expect("group exists");
        assert_eq!(g.protocol_name.as_deref(), Some("range"));
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
        assert_eq!(GroupState::Dead.as_str(), "Dead");
    }

    #[test]
    fn test_join_leader_gets_selected_protocol_metadata() {
        // The leader's returned member metadata must be for the group-SELECTED
        // protocol, not each member's first-listed one. The leader prefers
        // "roundrobin" (supported by all → selected); the follower lists "range"
        // first but also supports "roundrobin", so the leader must receive the
        // follower's "roundrobin" metadata, not its first ("range") metadata.
        let coordinator = GroupCoordinator::new();

        let (leader, ..) = coordinator
            .join_group(
                "g".to_string(),
                None,
                "c1".to_string(),
                "h".to_string(),
                300_000,
                300_000,
                "consumer".to_string(),
                vec![
                    ("roundrobin".to_string(), b"rr-leader".to_vec()),
                    ("range".to_string(), b"rg-leader".to_vec()),
                ],
                None,
            )
            .unwrap();
        // member_id is assigned by the coordinator (we pass None), so capture c2's.
        let (c2_member, ..) = coordinator
            .join_group(
                "g".to_string(),
                None,
                "c2".to_string(),
                "h".to_string(),
                300_000,
                300_000,
                "consumer".to_string(),
                vec![
                    ("range".to_string(), b"rg-follower".to_vec()),
                    ("roundrobin".to_string(), b"rr-follower".to_vec()),
                ],
                None,
            )
            .unwrap();
        // Leader rejoins to complete the join phase and receive the member list.
        let (_, _, is_leader, _, members) = coordinator
            .join_group(
                "g".to_string(),
                Some(leader),
                "c1".to_string(),
                "h".to_string(),
                300_000,
                300_000,
                "consumer".to_string(),
                vec![
                    ("roundrobin".to_string(), b"rr-leader".to_vec()),
                    ("range".to_string(), b"rg-leader".to_vec()),
                ],
                None,
            )
            .unwrap();

        assert!(is_leader);
        let g = coordinator.get_group_state("g").expect("group exists");
        assert_eq!(g.protocol_name.as_deref(), Some("roundrobin"));

        let c2_meta = members
            .iter()
            .find(|(id, _)| *id == c2_member)
            .map(|(_, meta)| meta.clone())
            .expect("c2 in leader member list");
        assert_eq!(c2_meta, b"rr-follower".to_vec());
    }

    #[test]
    fn test_validate_commit_rejects_zombie_commits() {
        // CONF-1: a stale generation or unknown member must be rejected. A simple consumer
        // (generation -1) is allowed through. RA-5: a generation-bearing commit to a group the
        // coordinator doesn't know (e.g. reaped by the empty-group sweep) is fenced as a zombie.
        let coordinator = GroupCoordinator::new();
        let (member, generation, ..) = coordinator
            .join_group(
                "g".to_string(),
                None,
                "c1".to_string(),
                "h".to_string(),
                300_000,
                300_000,
                "consumer".to_string(),
                vec![("range".to_string(), vec![])],
                None,
            )
            .unwrap();

        // Drive the group to Stable (the single leader member assigns itself) so
        // the current-generation commit below isn't fenced by the RV-10
        // mid-rebalance guard — this test is about generation/member fencing.
        coordinator
            .sync_group(
                "g".to_string(),
                member.clone(),
                generation,
                vec![(member.clone(), b"a".to_vec())],
            )
            .unwrap();

        // Current generation + known member → accepted.
        let current = coordinator.validate_commit("g", &member, generation);
        assert!(current.is_ok());

        // Stale generation → IllegalGeneration.
        let stale = coordinator.validate_commit("g", &member, generation - 1);
        assert!(matches!(stale, Err(KafkaError::IllegalGeneration { .. })));

        // Unknown member at the current generation → UnknownMemberId.
        let ghost = coordinator.validate_commit("g", "ghost", generation);
        assert!(matches!(ghost, Err(KafkaError::UnknownMemberId { .. })));

        // Simple consumer (generation -1) → not validated.
        assert!(coordinator.validate_commit("g", "", -1).is_ok());

        // RA-5: a generation-bearing commit to a group this coordinator doesn't
        // know (e.g. reaped by the empty-group sweep) is a zombie → UnknownMemberId.
        let unknown = coordinator.validate_commit("other", &member, generation);
        assert!(matches!(unknown, Err(KafkaError::UnknownMemberId { .. })));

        // ...but a simple consumer (generation -1) on an unknown group is still fine.
        assert!(coordinator.validate_commit("other", "", -1).is_ok());
    }

    #[test]
    fn test_validate_commit_fenced_during_rebalance() {
        // RV-10: between JoinGroup and SyncGroup the group is mid-rebalance
        // (CompletingRebalance). An OffsetCommit at the current generation from a
        // known member must be fenced with REBALANCE_IN_PROGRESS so the client
        // rejoins and re-fetches its assignment before committing, rather than
        // persisting an offset for a partition it may be about to lose.
        let coordinator = GroupCoordinator::new();
        let (member, generation, ..) = coordinator
            .join_group(
                "g".to_string(),
                None,
                "c1".to_string(),
                "h".to_string(),
                300_000,
                300_000,
                "consumer".to_string(),
                vec![("range".to_string(), vec![])],
                None,
            )
            .unwrap();

        // Awaiting SyncGroup → fenced (this is the fail-before assertion: without
        // the guard the commit was accepted mid-rebalance).
        let during = coordinator.validate_commit("g", &member, generation);
        assert!(matches!(
            during,
            Err(KafkaError::RebalanceInProgress { .. })
        ));

        // Once SyncGroup completes and the group is Stable, the same commit is
        // accepted.
        coordinator
            .sync_group(
                "g".to_string(),
                member.clone(),
                generation,
                vec![(member.clone(), b"a".to_vec())],
            )
            .unwrap();
        assert!(coordinator
            .validate_commit("g", &member, generation)
            .is_ok());
    }

    #[test]
    fn test_group_protocol_name_reports_selected_protocol() {
        // RA-6: the accessor the JoinGroup handler uses returns the group's
        // selected common protocol once a join completes, and None for a group
        // the coordinator doesn't know.
        let coordinator = GroupCoordinator::new();
        coordinator
            .join_group(
                "g".to_string(),
                None,
                "c1".to_string(),
                "h".to_string(),
                300_000,
                300_000,
                "consumer".to_string(),
                vec![("range".to_string(), vec![])],
                None,
            )
            .unwrap();

        assert_eq!(
            coordinator.group_protocol_name("g").as_deref(),
            Some("range")
        );
        assert!(coordinator.group_protocol_name("nonexistent").is_none());
    }

    #[test]
    fn test_join_group_enforces_member_cap() {
        // CG-4: a group is capped at MAX_MEMBERS_PER_GROUP; one more new member is rejected with
        // GROUP_MAX_SIZE_REACHED.
        let coordinator = GroupCoordinator::new();
        for i in 0..MAX_MEMBERS_PER_GROUP {
            coordinator
                .join_group(
                    "g".to_string(),
                    None,
                    format!("c{}", i),
                    "h".to_string(),
                    300_000,
                    300_000,
                    "consumer".to_string(),
                    vec![("range".to_string(), vec![])],
                    None,
                )
                .unwrap();
        }

        let overflow = coordinator.join_group(
            "g".to_string(),
            None,
            "overflow".to_string(),
            "h".to_string(),
            300_000,
            300_000,
            "consumer".to_string(),
            vec![("range".to_string(), vec![])],
            None,
        );

        match overflow {
            Err(KafkaError::Protocol { code, .. }) => {
                assert_eq!(code, crate::kafka::constants::ERROR_GROUP_MAX_SIZE_REACHED);
            }
            other => panic!("expected GROUP_MAX_SIZE_REACHED, got {:?}", other),
        }
    }

    #[test]
    fn test_empty_group_reclaimed_by_sweep() {
        // CG-5: a group whose only member times out is removed by the periodic sweep, not left to
        // linger until the next join_group.
        let coordinator = GroupCoordinator::new();
        coordinator
            .join_group(
                "g".to_string(),
                None,
                "c1".to_string(),
                "h".to_string(),
                1, // 1ms session timeout — will expire
                300_000,
                "consumer".to_string(),
                vec![("range".to_string(), vec![])],
                None,
            )
            .unwrap();

        std::thread::sleep(std::time::Duration::from_millis(10));

        // The sweep removes the timed-out member; the now-empty group survives until its TTL.
        coordinator.check_and_remove_timed_out_members();
        assert_eq!(
            coordinator.get_group_state("g").unwrap().state,
            GroupState::Empty,
            "a fresh empty group must be kept until the TTL"
        );

        // Age the group past the TTL: the next sweep reclaims it.
        {
            let mut groups = coordinator.groups.write().unwrap();
            let g = groups.get_mut("g").unwrap();
            g.last_activity = Instant::now()
                .checked_sub(std::time::Duration::from_secs(EMPTY_GROUP_TTL_SECS + 1))
                .expect("monotonic clock far enough from boot");
        }
        coordinator.check_and_remove_timed_out_members();
        assert!(
            coordinator.get_group_state("g").is_none(),
            "expired empty group should be reclaimed by the sweep"
        );
    }

    #[test]
    fn test_generation_monotonic_across_leave_sweep_rejoin() {
        // A group that empties, is swept, and is rejoined must not restart at
        // generation 1: the generation is the zombie-fencing token.
        let coordinator = GroupCoordinator::new();
        let (member1, gen1) = join(&coordinator, "g", "client-1");
        coordinator.leave_group("g".to_string(), member1).unwrap();
        coordinator.check_and_remove_timed_out_members();
        let (_member2, gen2) = join(&coordinator, "g", "client-2");
        assert!(gen2 > gen1, "generation went {} -> {}", gen1, gen2);
    }

    #[test]
    fn test_timeout_triggers_rebalance_for_remaining_members() {
        let coordinator = GroupCoordinator::new();

        // Add two members - first with short timeout, second with long timeout
        let (member1, ..) = coordinator
            .join_group(
                "test-group".to_string(),
                None,
                "client-1".to_string(),
                "localhost".to_string(),
                1, // 1ms session timeout - will expire
                1000,
                "consumer".to_string(),
                vec![("range".to_string(), vec![])],
                None,
            )
            .unwrap();

        let (member2, ..) = coordinator
            .join_group(
                "test-group".to_string(),
                None,
                "client-2".to_string(),
                "localhost".to_string(),
                300000, // 5 minute timeout - won't expire
                1000,
                "consumer".to_string(),
                vec![("range".to_string(), vec![])],
                None,
            )
            .unwrap();

        // member1 rejoins the generation started by member2's join (keeping
        // its short session timeout), completing the join phase
        let (_, gen, ..) = coordinator
            .join_group(
                "test-group".to_string(),
                Some(member1.clone()),
                "client-1".to_string(),
                "localhost".to_string(),
                1, // 1ms session timeout - will expire
                1000,
                "consumer".to_string(),
                vec![("range".to_string(), vec![])],
                None,
            )
            .unwrap();

        // Complete sync to get to Stable state
        coordinator
            .sync_group(
                "test-group".to_string(),
                member1.clone(),
                gen,
                vec![
                    (member1.clone(), b"a1".to_vec()),
                    (member2.clone(), b"a2".to_vec()),
                ],
            )
            .unwrap();
        coordinator
            .sync_group("test-group".to_string(), member2.clone(), gen, vec![])
            .unwrap();

        // Verify stable state with 2 members
        {
            let groups = coordinator.groups.read().unwrap();
            let group = groups.get("test-group").unwrap();
            assert_eq!(group.state, GroupState::Stable);
            assert_eq!(group.members.len(), 2);
        }

        // Keep member2 alive by sending heartbeat
        coordinator
            .heartbeat("test-group".to_string(), member2.clone(), gen)
            .unwrap();

        // Wait for member1's timeout to expire
        std::thread::sleep(std::time::Duration::from_millis(10));

        // Check and remove timed-out members
        let removed = coordinator.check_and_remove_timed_out_members();

        // Member1 should have been removed
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0].1, member1);

        // Verify group is now in PreparingRebalance with 1 member
        {
            let groups = coordinator.groups.read().unwrap();
            let group = groups.get("test-group").unwrap();
            assert_eq!(group.state, GroupState::PreparingRebalance);
            assert_eq!(group.members.len(), 1);
            assert!(group.members.contains_key(&member2));
        }
    }

    /// Join a group with default test parameters, returning (member_id, generation_id)
    /// Form a stable two-member group {leader, follower} and return
    /// (leader, follower, generation).
    fn stable_pair(c: &GroupCoordinator, group: &str) -> (String, String, i32) {
        let (leader, _) = join(c, group, "client-a");
        let (follower, _) = join(c, group, "client-b");
        let (_, generation) = rejoin(c, group, &leader, "client-a");
        let assignments = vec![
            (leader.clone(), b"A".to_vec()),
            (follower.clone(), b"B".to_vec()),
        ];
        c.sync_group(group.to_string(), leader.clone(), generation, assignments)
            .unwrap();
        assert_eq!(c.get_group_state(group).unwrap().state, GroupState::Stable);
        (leader, follower, generation)
    }

    #[test]
    fn follower_rejoin_in_stable_keeps_assignment_and_generation() {
        // A follower that rejoins with unchanged metadata must not lose its
        // assignment or force a new rebalance.
        let c = GroupCoordinator::new();
        let (_leader, follower, generation) = stable_pair(&c, "g");
        let (_, rejoined_gen) = rejoin(&c, "g", &follower, "client-b");
        assert_eq!(rejoined_gen, generation);
        let assignment = c
            .sync_group("g".to_string(), follower, generation, vec![])
            .unwrap();
        assert_eq!(assignment, b"B".to_vec());
        assert_eq!(c.get_group_state("g").unwrap().generation_id, generation);
    }

    #[test]
    fn old_generation_member_is_told_to_rejoin_and_may_commit() {
        // When a new member starts a rebalance, existing members still hold the old
        // generation. Heartbeat must say REBALANCE_IN_PROGRESS (not ILLEGAL_GENERATION)
        // and their revoke-time OffsetCommit must be accepted.
        let c = GroupCoordinator::new();
        let (leader, follower, generation) = stable_pair(&c, "g");
        join(&c, "g", "client-c");
        for member in [&leader, &follower] {
            let hb = c.heartbeat("g".to_string(), member.clone(), generation);
            assert!(
                matches!(hb, Err(KafkaError::RebalanceInProgress { .. })),
                "{:?}",
                hb
            );
            c.validate_commit("g", member, generation)
                .expect("revoke-time commit under the old generation");
        }
        // A generation older than the last stable one is still fenced.
        assert!(matches!(
            c.heartbeat("g".to_string(), leader.clone(), generation - 1),
            Err(KafkaError::IllegalGeneration { .. })
        ));
        // Once a member has rejoined, the old generation no longer works for it.
        rejoin(&c, "g", &follower, "client-b");
        assert!(matches!(
            c.validate_commit("g", &follower, generation),
            Err(KafkaError::IllegalGeneration { .. })
        ));
    }

    #[test]
    fn leader_sync_with_departed_member_restarts_rebalance() {
        // The leader's member list went stale: a member left after the leader's
        // JoinGroup response. Its assignment must not be accepted (the departed
        // member's partitions would go unconsumed).
        let c = GroupCoordinator::new();
        let (leader, follower, _) = stable_pair(&c, "g");
        let (third, _) = join(&c, "g", "client-c");
        let (_, generation) = rejoin(&c, "g", &leader, "client-a");
        rejoin(&c, "g", &follower, "client-b");
        c.leave_group("g".to_string(), follower.clone()).unwrap();
        let generation = c
            .get_group_state("g")
            .unwrap()
            .generation_id
            .max(generation);
        let stale = vec![
            (leader.clone(), b"A".to_vec()),
            (follower, b"B".to_vec()),
            (third, b"C".to_vec()),
        ];
        let result = c.sync_group("g".to_string(), leader, generation, stale);
        assert!(
            matches!(result, Err(KafkaError::RebalanceInProgress { .. })),
            "{:?}",
            result
        );
        assert_ne!(c.get_group_state("g").unwrap().state, GroupState::Stable);
    }

    #[test]
    fn rebalance_timeout_evicts_only_stragglers() {
        let c = GroupCoordinator::new();
        let (leader, follower, _) = stable_pair(&c, "g");
        let (third, _) = join(&c, "g", "client-c");
        rejoin(&c, "g", &leader, "client-a");
        // `follower` never rejoins. Age the rebalance past every member's timeout.
        {
            let mut groups = c.groups.write().unwrap();
            let g = groups.get_mut("g").unwrap();
            g.rebalance_started_at =
                Instant::now().checked_sub(std::time::Duration::from_secs(3600));
        }
        let removed = c.check_and_remove_timed_out_members();
        assert_eq!(removed, vec![("g".to_string(), follower.clone())]);
        let g = c.get_group_state("g").unwrap();
        assert!(g.members.contains_key(&leader) && g.members.contains_key(&third));
        assert!(!g.members.contains_key(&follower));
        assert_eq!(g.state, GroupState::CompletingRebalance);
    }

    #[test]
    fn changed_subscription_in_stable_triggers_rebalance() {
        let c = GroupCoordinator::new();
        let (leader, _follower, generation) = stable_pair(&c, "g");
        let (_, new_gen, ..) = c
            .join_group(
                "g".to_string(),
                Some(leader),
                "client-a".to_string(),
                "localhost".to_string(),
                30000,
                60000,
                "consumer".to_string(),
                vec![("range".to_string(), b"new-subscription".to_vec())],
                None,
            )
            .unwrap();
        assert!(new_gen > generation, "{} -> {}", generation, new_gen);
    }

    #[test]
    fn unknown_group_is_unknown_member() {
        let c = GroupCoordinator::new();
        assert!(matches!(
            c.heartbeat("nope".to_string(), "m".to_string(), 1),
            Err(KafkaError::UnknownMemberId { .. })
        ));
        assert!(matches!(
            c.sync_group("nope".to_string(), "m".to_string(), 1, vec![]),
            Err(KafkaError::UnknownMemberId { .. })
        ));
        assert!(matches!(
            c.leave_group("nope".to_string(), "m".to_string()),
            Err(KafkaError::UnknownMemberId { .. })
        ));
    }

    fn join(coordinator: &GroupCoordinator, group: &str, client: &str) -> (String, i32) {
        join_inner(coordinator, group, None, client)
    }

    /// Rejoin a group with an existing member id, returning (member_id, generation_id)
    fn rejoin(
        coordinator: &GroupCoordinator,
        group: &str,
        member_id: &str,
        client: &str,
    ) -> (String, i32) {
        join_inner(coordinator, group, Some(member_id.to_string()), client)
    }

    fn join_inner(
        coordinator: &GroupCoordinator,
        group: &str,
        existing_member_id: Option<String>,
        client: &str,
    ) -> (String, i32) {
        let (member_id, generation_id, ..) = coordinator
            .join_group(
                group.to_string(),
                existing_member_id,
                client.to_string(),
                "localhost".to_string(),
                30000,
                60000,
                "consumer".to_string(),
                vec![("range".to_string(), vec![])],
                None,
            )
            .unwrap();
        (member_id, generation_id)
    }

    #[test]
    fn test_generation_id_survives_group_becoming_empty() {
        let coordinator = GroupCoordinator::new();

        let (member1, gen1) = join(&coordinator, "test-group", "client-1");
        assert_eq!(gen1, 1);

        // Last member leaves: group becomes Empty
        coordinator
            .leave_group("test-group".to_string(), member1)
            .unwrap();

        // A new incarnation of the group must NOT reuse old generation ids:
        // the generation is the zombie-fencing token, so a stale client from
        // the previous incarnation must fail generation validation.
        let (_member2, gen2) = join(&coordinator, "test-group", "client-2");
        assert!(
            gen2 > gen1,
            "generation must stay monotonic across empty: {} -> {}",
            gen1,
            gen2
        );
    }

    #[test]
    fn test_sync_group_follower_before_leader_gets_rebalance_in_progress() {
        let coordinator = GroupCoordinator::new();

        let (leader, _) = join(&coordinator, "test-group", "client-1");
        let (follower, _) = join(&coordinator, "test-group", "client-2");
        // Leader rejoins the new generation -> join phase completes
        let (_, gen) = rejoin(&coordinator, "test-group", &leader, "client-1");

        // Follower syncs before the leader has provided assignments.
        // It must be told to retry (REBALANCE_IN_PROGRESS), not handed an
        // empty assignment with a success code.
        let result = coordinator.sync_group("test-group".to_string(), follower, gen, vec![]);
        assert!(matches!(
            result,
            Err(KafkaError::RebalanceInProgress { .. })
        ));
    }

    #[test]
    fn test_sync_group_unassigned_member_in_stable_group_forces_rebalance() {
        let coordinator = GroupCoordinator::new();

        // Single member joins and syncs -> Stable
        let (leader, gen) = join(&coordinator, "test-group", "client-1");
        coordinator
            .sync_group(
                "test-group".to_string(),
                leader.clone(),
                gen,
                vec![(leader.clone(), b"leader-assignment".to_vec())],
            )
            .unwrap();

        // The member rejoins (e.g., changed subscription): add_member resets
        // its assignment, the group stays Stable
        let (_, gen) = rejoin(&coordinator, "test-group", &leader, "client-1");

        // Syncing with no stored assignment in a Stable group must force a
        // rebalance instead of handing back an empty assignment forever
        let result = coordinator.sync_group("test-group".to_string(), leader.clone(), gen, vec![]);
        assert!(matches!(
            result,
            Err(KafkaError::RebalanceInProgress { .. })
        ));

        let groups = coordinator.groups.read().unwrap();
        let group = groups.get("test-group").unwrap();
        assert_eq!(group.state, GroupState::PreparingRebalance);
        assert!(group.generation_id > gen);
    }

    #[test]
    fn test_sync_group_during_preparing_rebalance_gets_rebalance_in_progress() {
        let coordinator = GroupCoordinator::new();

        let (member1, _) = join(&coordinator, "test-group", "client-1");
        let (member2, _) = join(&coordinator, "test-group", "client-2");
        // member1 rejoins the new generation -> join phase completes
        let (_, gen) = rejoin(&coordinator, "test-group", &member1, "client-1");

        // Complete a full sync so the group is Stable
        coordinator
            .sync_group(
                "test-group".to_string(),
                member1.clone(),
                gen,
                vec![
                    (member1.clone(), b"a1".to_vec()),
                    (member2.clone(), b"a2".to_vec()),
                ],
            )
            .unwrap();
        coordinator
            .sync_group("test-group".to_string(), member2.clone(), gen, vec![])
            .unwrap();

        // member2 leaves -> PreparingRebalance (member1 must rejoin)
        coordinator
            .leave_group("test-group".to_string(), member2)
            .unwrap();

        // Syncing during PreparingRebalance must be rejected: the join phase
        // for the new generation hasn't completed
        let new_gen = {
            let groups = coordinator.groups.read().unwrap();
            groups.get("test-group").unwrap().generation_id
        };
        let result = coordinator.sync_group("test-group".to_string(), member1, new_gen, vec![]);
        assert!(matches!(
            result,
            Err(KafkaError::RebalanceInProgress { .. })
        ));
    }

    #[test]
    fn test_join_phase_completes_when_all_members_rejoin() {
        let coordinator = GroupCoordinator::new();

        // Single member: join phase completes immediately
        let (member1, _) = join(&coordinator, "test-group", "client-1");
        {
            let groups = coordinator.groups.read().unwrap();
            assert_eq!(
                groups.get("test-group").unwrap().state,
                GroupState::CompletingRebalance
            );
        }

        // A second member joining starts a new generation; the join phase is
        // incomplete until member1 rejoins it
        let (_member2, _) = join(&coordinator, "test-group", "client-2");
        {
            let groups = coordinator.groups.read().unwrap();
            assert_eq!(
                groups.get("test-group").unwrap().state,
                GroupState::PreparingRebalance
            );
        }

        // member1 rejoins -> all members present in the current generation
        rejoin(&coordinator, "test-group", &member1, "client-1");
        {
            let groups = coordinator.groups.read().unwrap();
            assert_eq!(
                groups.get("test-group").unwrap().state,
                GroupState::CompletingRebalance
            );
        }
    }

    #[test]
    fn test_straggler_eviction_completes_join_phase() {
        let coordinator = GroupCoordinator::new();

        // Two-member stable group
        let (member1, _) = join(&coordinator, "test-group", "client-1");
        let (member2, _) = join(&coordinator, "test-group", "client-2");
        let (_, gen) = rejoin(&coordinator, "test-group", &member1, "client-1");
        coordinator
            .sync_group(
                "test-group".to_string(),
                member1.clone(),
                gen,
                vec![
                    (member1.clone(), b"a1".to_vec()),
                    (member2.clone(), b"a2".to_vec()),
                ],
            )
            .unwrap();

        // member2 leaves during Stable -> rebalance; member1 rejoins, but the
        // join phase completes immediately because member1 is the only member
        coordinator
            .leave_group("test-group".to_string(), member2)
            .unwrap();
        rejoin(&coordinator, "test-group", &member1, "client-1");
        {
            let groups = coordinator.groups.read().unwrap();
            assert_eq!(
                groups.get("test-group").unwrap().state,
                GroupState::CompletingRebalance
            );
        }
    }
}
