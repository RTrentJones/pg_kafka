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
//! ```
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
        }
    }

    /// Add a member to the group
    /// Returns (member_id, is_leader)
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
    groups: Arc<RwLock<HashMap<String, ConsumerGroup>>>,
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
    ) -> Result<(String, i32, bool, Vec<(String, Vec<u8>)>)> {
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

        Ok((
            assigned_member_id,
            group.generation_id,
            is_leader,
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

        let group = groups.get_mut(&group_id).ok_or_else(|| {
            KafkaError::Internal(format!("Unknown group_id: {}", group_id))
        })?;

        // Validate generation ID
        if group.generation_id != generation_id {
            return Err(KafkaError::Internal(format!(
                "Illegal generation: expected {}, got {}",
                group.generation_id, generation_id
            )));
        }

        // Validate member ID
        if !group.members.contains_key(&member_id) {
            return Err(KafkaError::Internal(format!(
                "Unknown member_id: {}",
                member_id
            )));
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
    pub fn heartbeat(
        &self,
        group_id: String,
        member_id: String,
        generation_id: i32,
    ) -> Result<()> {
        let mut groups = self.groups.write().map_err(|e| {
            KafkaError::Internal(format!("Failed to acquire groups write lock: {}", e))
        })?;

        let group = groups.get_mut(&group_id).ok_or_else(|| {
            KafkaError::Internal(format!("Unknown group_id: {}", group_id))
        })?;

        // Validate generation ID
        if group.generation_id != generation_id {
            return Err(KafkaError::Internal(format!(
                "Illegal generation: expected {}, got {}",
                group.generation_id, generation_id
            )));
        }

        // Validate member ID and update heartbeat
        let member = group.members.get_mut(&member_id).ok_or_else(|| {
            KafkaError::Internal(format!("Unknown member_id: {}", member_id))
        })?;

        member.last_heartbeat = SystemTime::now();

        Ok(())
    }

    /// Handle LeaveGroup request
    ///
    /// Removes member from the group.
    /// Triggers rebalance for remaining members (future optimization).
    pub fn leave_group(&self, group_id: String, member_id: String) -> Result<()> {
        let mut groups = self.groups.write().map_err(|e| {
            KafkaError::Internal(format!("Failed to acquire groups write lock: {}", e))
        })?;

        let group = groups.get_mut(&group_id).ok_or_else(|| {
            KafkaError::Internal(format!("Unknown group_id: {}", group_id))
        })?;

        pg_log!(
            "LeaveGroup: group_id={}, member_id={}",
            group_id,
            member_id
        );

        if group.remove_member(&member_id) {
            // Member removed successfully
            // Future: Trigger rebalance for remaining members
            Ok(())
        } else {
            Err(KafkaError::Internal(format!(
                "Unknown member_id: {}",
                member_id
            )))
        }
    }

    /// Get group state for debugging/testing
    #[allow(dead_code)]
    pub fn get_group_state(&self, group_id: &str) -> Option<ConsumerGroup> {
        self.groups
            .read()
            .ok()
            .and_then(|groups| groups.get(group_id).cloned())
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
}
