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
//! - **Dead**: Group is being deleted (future feature)

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

use super::error::{KafkaError, Result};

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

    /// Last heartbeat timestamp
    pub last_heartbeat: SystemTime,

    /// Static group instance ID (KIP-345) for static membership
    pub group_instance_id: Option<String>,
}

impl GroupMember {
    /// Check if member's session has timed out
    pub fn is_timed_out(&self) -> bool {
        let now = SystemTime::now();
        let elapsed = now
            .duration_since(self.last_heartbeat)
            .unwrap_or(Duration::from_secs(0));

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
        let member = GroupMember {
            member_id: member_id.clone(),
            client_id,
            client_host,
            session_timeout_ms,
            rebalance_timeout_ms,
            protocol_type,
            protocols,
            assignment: None,
            last_heartbeat: SystemTime::now(),
            group_instance_id,
        };

        // First member becomes leader
        let is_leader = self.members.is_empty();
        if is_leader {
            self.leader = Some(member_id.clone());
        }

        self.members.insert(member_id.clone(), member);

        (member_id, is_leader)
    }

    /// Remove a member from the group
    fn remove_member(&mut self, member_id: &str) -> bool {
        if self.members.remove(member_id).is_some() {
            // If leader left, reassign leadership
            if self.leader.as_deref() == Some(member_id) {
                self.leader = self.members.keys().next().cloned();
            }

            // If no members left, return to Empty state
            if self.members.is_empty() {
                self.state = GroupState::Empty;
                self.generation_id = 0;
                self.protocol_name = None;
                self.leader = None;
            }

            true
        } else {
            false
        }
    }

    /// Start a rebalance (increment generation, move to PreparingRebalance)
    fn start_rebalance(&mut self) {
        self.generation_id += 1;
        self.state = GroupState::PreparingRebalance;
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

        // Get or create consumer group
        let group = groups
            .entry(group_id.clone())
            .or_insert_with(|| ConsumerGroup::new(group_id.clone()));

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
            GroupState::Stable => {
                // New member joining stable group
                !group.members.contains_key(&member_id)
            }
            _ => false, // Already rebalancing
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

        // Choose assignment strategy (for now, just pick first one from first member)
        if group.protocol_name.is_none() && !protocols.is_empty() {
            let strategy = protocols[0].0.clone();
            group.complete_join(strategy);
        }

        // If this is the leader, return member list
        let members_for_leader = if is_leader {
            group
                .members
                .values()
                .flat_map(|m| {
                    m.protocols
                        .first()
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

        let group =
            groups
                .get_mut(&group_id)
                .ok_or_else(|| KafkaError::CoordinatorNotAvailable {
                    group_id: group_id.clone(),
                })?;

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

        // If this is the leader, store assignments for all members
        if group.leader.as_deref() == Some(&member_id) {
            for (target_member_id, assignment_bytes) in assignments.iter() {
                if let Some(member) = group.members.get_mut(target_member_id) {
                    member.assignment = Some(assignment_bytes.clone());
                }
            }

            // Move group to Stable state
            group.complete_sync();
        }

        // Return assignment for this member
        let assignment = group
            .members
            .get(&member_id)
            .and_then(|m| m.assignment.clone())
            .unwrap_or_default();

        Ok(assignment)
    }

    /// Handle Heartbeat request
    ///
    /// Updates member's last_heartbeat timestamp.
    /// Returns error if member is unknown or generation mismatches.
    pub fn heartbeat(&self, group_id: String, member_id: String, generation_id: i32) -> Result<()> {
        let mut groups = self.groups.write().map_err(|e| {
            KafkaError::Internal(format!("Failed to acquire groups write lock: {}", e))
        })?;

        let group =
            groups
                .get_mut(&group_id)
                .ok_or_else(|| KafkaError::CoordinatorNotAvailable {
                    group_id: group_id.clone(),
                })?;

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

        member.last_heartbeat = SystemTime::now();

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

        let group =
            groups
                .get_mut(&group_id)
                .ok_or_else(|| KafkaError::CoordinatorNotAvailable {
                    group_id: group_id.clone(),
                })?;

        pg_log!("LeaveGroup: group_id={}, member_id={}", group_id, member_id);

        if group.remove_member(&member_id) {
            // Member removed successfully
            // Trigger rebalance for remaining members if group is still active
            if !group.members.is_empty() && group.state == GroupState::Stable {
                group.start_rebalance();
                pg_log!(
                    "LeaveGroup triggered rebalance for {} remaining members in group {}",
                    group.members.len(),
                    group_id
                );
            }
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
                group.start_rebalance();
                pg_log!(
                    "Timeout triggered rebalance for {} remaining members in group {}",
                    group.members.len(),
                    group_id
                );
            }
        }

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

        // Complete the sync to get to Stable state
        coordinator
            .sync_group(
                "test-group".to_string(),
                member1.clone(),
                1,
                vec![(member1.clone(), vec![]), (member2.clone(), vec![])],
            )
            .unwrap();
        coordinator
            .sync_group("test-group".to_string(), member2.clone(), 1, vec![])
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

        // Verify member is gone
        {
            let groups = coordinator.groups.read().unwrap();
            let group = groups.get("test-group").unwrap();
            assert_eq!(group.members.len(), 0);
        }
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

        // Complete sync to get to Stable state
        coordinator
            .sync_group(
                "test-group".to_string(),
                member1.clone(),
                1,
                vec![(member1.clone(), vec![]), (member2.clone(), vec![])],
            )
            .unwrap();
        coordinator
            .sync_group("test-group".to_string(), member2.clone(), 1, vec![])
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
            .heartbeat("test-group".to_string(), member2.clone(), 1)
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
}
