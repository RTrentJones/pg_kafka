// Consumer group coordinator handlers
//
// Handlers for consumer group management APIs:
// FindCoordinator, JoinGroup, SyncGroup, Heartbeat, LeaveGroup, DescribeGroups, ListGroups

use crate::kafka::constants::{DEFAULT_BROKER_ID, ERROR_NONE, ERROR_UNKNOWN_SERVER_ERROR};
use crate::kafka::error::Result;

/// Handle FindCoordinator request
///
/// Returns the coordinator broker for a consumer group.
/// In our single-node setup, we always return ourselves as the coordinator.
pub fn handle_find_coordinator(
    broker_host: String,
    broker_port: i32,
    key: String,
    key_type: i8,
) -> Result<kafka_protocol::messages::find_coordinator_response::FindCoordinatorResponse> {
    use kafka_protocol::messages::find_coordinator_response::FindCoordinatorResponse;

    crate::pg_debug!("FindCoordinator: key={}, key_type={}", key, key_type);

    // Build response - we are always the coordinator for all groups
    let mut response = FindCoordinatorResponse::default();
    response.throttle_time_ms = 0;
    response.error_code = ERROR_NONE;
    response.error_message = None;
    response.node_id = DEFAULT_BROKER_ID.into();
    response.host = broker_host.into();
    response.port = broker_port;

    Ok(response)
}

/// Handle JoinGroup request
///
/// Consumer joins a consumer group and receives:
/// - Assigned member ID
/// - Generation ID
/// - Leader status
/// - Member list (if leader)
#[allow(clippy::too_many_arguments)]
pub fn handle_join_group(
    coordinator: &crate::kafka::GroupCoordinator,
    group_id: String,
    member_id: String,
    client_id: String,
    session_timeout_ms: i32,
    rebalance_timeout_ms: i32,
    protocol_type: String,
    protocols: Vec<crate::kafka::messages::JoinGroupProtocol>,
) -> Result<kafka_protocol::messages::join_group_response::JoinGroupResponse> {
    use kafka_protocol::messages::join_group_response::{
        JoinGroupResponse, JoinGroupResponseMember,
    };

    crate::pg_debug!(
        "JoinGroup: group_id={}, member_id={}, client_id={}",
        group_id,
        member_id,
        client_id
    );

    // Convert protocols to coordinator format
    let coord_protocols: Vec<(String, Vec<u8>)> = protocols
        .into_iter()
        .map(|p| (p.name, p.metadata))
        .collect();

    // Get client host (use localhost for now)
    let client_host = "localhost".to_string();

    // Join group via coordinator
    let existing_member_id = if member_id.is_empty() {
        None
    } else {
        Some(member_id)
    };

    // Coordinator returns typed errors (UnknownMemberId, IllegalGeneration, etc.)
    // which map directly to Kafka protocol error codes via to_kafka_error_code()
    let (assigned_member_id, generation_id, is_leader, leader_id, members_metadata) = coordinator
        .join_group(
        group_id.clone(),
        existing_member_id,
        client_id,
        client_host,
        session_timeout_ms,
        rebalance_timeout_ms,
        protocol_type.clone(),
        coord_protocols.clone(),
        None, // group_instance_id
    )?;

    // Build response
    let mut response = JoinGroupResponse::default();
    response.throttle_time_ms = 0;
    response.error_code = ERROR_NONE;
    response.generation_id = generation_id;
    response.protocol_type = Some(protocol_type.into());
    response.protocol_name = coord_protocols.first().map(|(name, _)| name.clone().into());
    response.leader = leader_id.into();
    response.member_id = assigned_member_id.into();

    // If this is the leader, include member list
    if is_leader {
        response.members = members_metadata
            .into_iter()
            .map(|(mid, metadata)| {
                let mut member = JoinGroupResponseMember::default();
                member.member_id = mid.into();
                member.metadata = metadata.into();
                member
            })
            .collect();
    }

    Ok(response)
}

/// Handle SyncGroup request
///
/// Leader provides partition assignments, followers receive their assignment.
/// If leader sends empty assignments, server computes assignments automatically.
pub fn handle_sync_group(
    coordinator: &crate::kafka::GroupCoordinator,
    store: &dyn crate::kafka::KafkaStore,
    group_id: String,
    member_id: String,
    generation_id: i32,
    assignments: Vec<crate::kafka::messages::SyncGroupAssignment>,
) -> Result<kafka_protocol::messages::sync_group_response::SyncGroupResponse> {
    use kafka_protocol::messages::sync_group_response::SyncGroupResponse;
    use std::collections::HashMap;

    crate::pg_debug!(
        "SyncGroup: group_id={}, member_id={}, generation_id={}, assignments={}",
        group_id,
        member_id,
        generation_id,
        assignments.len()
    );

    // Convert assignments to coordinator format
    let mut coord_assignments: Vec<(String, Vec<u8>)> = assignments
        .into_iter()
        .map(|a| (a.member_id, a.assignment))
        .collect();

    // Check if leader sent empty assignments - if so, compute server-side
    let assignments_empty =
        coord_assignments.is_empty() || coord_assignments.iter().all(|(_, bytes)| bytes.is_empty());

    if assignments_empty {
        // Get subscribed topics from group members
        if let Ok(topics) = coordinator.get_subscribed_topics(&group_id) {
            if !topics.is_empty() {
                // Get partition counts for each topic
                let mut topic_partitions: HashMap<String, i32> = HashMap::new();
                for topic in &topics {
                    if let Ok(metadata) =
                        store.get_topic_metadata(Some(std::slice::from_ref(topic)))
                    {
                        if let Some(tm) = metadata.first() {
                            topic_partitions.insert(topic.clone(), tm.partition_count);
                        }
                    }
                }

                // Compute assignments server-side
                if !topic_partitions.is_empty() {
                    if let Ok(computed) =
                        coordinator.compute_assignments(&group_id, &topic_partitions)
                    {
                        crate::pg_debug!(
                            "SyncGroup: computed {} assignments server-side for group {}",
                            computed.len(),
                            group_id
                        );
                        coord_assignments = computed.into_iter().collect();
                    }
                }
            }
        }
    }

    // Coordinator returns typed errors (UnknownMemberId, IllegalGeneration, CoordinatorNotAvailable)
    // which map directly to Kafka protocol error codes via to_kafka_error_code()
    let assignment =
        coordinator.sync_group(group_id, member_id, generation_id, coord_assignments)?;

    // Build response
    let mut response = SyncGroupResponse::default();
    response.throttle_time_ms = 0;
    response.error_code = ERROR_NONE;
    response.assignment = assignment.into();

    Ok(response)
}

/// Handle Heartbeat request
///
/// Updates member's last heartbeat timestamp.
pub fn handle_heartbeat(
    coordinator: &crate::kafka::GroupCoordinator,
    group_id: String,
    member_id: String,
    generation_id: i32,
) -> Result<kafka_protocol::messages::heartbeat_response::HeartbeatResponse> {
    use kafka_protocol::messages::heartbeat_response::HeartbeatResponse;

    crate::pg_debug!(
        "Heartbeat: group_id={}, member_id={}, generation_id={}",
        group_id,
        member_id,
        generation_id
    );

    // Coordinator returns typed errors (UnknownMemberId, IllegalGeneration, CoordinatorNotAvailable)
    // which map directly to Kafka protocol error codes via to_kafka_error_code()
    coordinator.heartbeat(group_id, member_id, generation_id)?;

    // Build response
    let mut response = HeartbeatResponse::default();
    response.throttle_time_ms = 0;
    response.error_code = ERROR_NONE;

    Ok(response)
}

/// Handle LeaveGroup request
///
/// Consumer gracefully leaves the consumer group.
pub fn handle_leave_group(
    coordinator: &crate::kafka::GroupCoordinator,
    group_id: String,
    member_id: String,
) -> Result<kafka_protocol::messages::leave_group_response::LeaveGroupResponse> {
    use kafka_protocol::messages::leave_group_response::LeaveGroupResponse;

    crate::pg_debug!("LeaveGroup: group_id={}, member_id={}", group_id, member_id);

    // Coordinator returns typed errors (UnknownMemberId, CoordinatorNotAvailable)
    // which map directly to Kafka protocol error codes via to_kafka_error_code()
    coordinator.leave_group(group_id, member_id)?;

    // Build response
    let mut response = LeaveGroupResponse::default();
    response.throttle_time_ms = 0;
    response.error_code = ERROR_NONE;

    Ok(response)
}

/// Handle DescribeGroups request
///
/// Returns detailed information about consumer groups including members and state.
/// This is critical for debugging consumer group issues and monitoring group health.
pub fn handle_describe_groups(
    coordinator: &crate::kafka::GroupCoordinator,
    groups: Vec<String>,
) -> Result<kafka_protocol::messages::describe_groups_response::DescribeGroupsResponse> {
    use crate::kafka::GroupState;
    use kafka_protocol::messages::describe_groups_response::{
        DescribeGroupsResponse, DescribedGroup, DescribedGroupMember,
    };
    use kafka_protocol::messages::GroupId;
    use kafka_protocol::protocol::StrBytes;

    let mut response = DescribeGroupsResponse::default();
    response.throttle_time_ms = 0;

    // Get coordinator state
    let coordinator_groups = coordinator.groups.read().unwrap();

    for group_id in groups {
        let mut described_group = DescribedGroup::default();
        described_group.group_id = GroupId(StrBytes::from_string(group_id.clone()));

        match coordinator_groups.get(&group_id) {
            Some(group) => {
                // Group exists - return detailed information
                described_group.error_code = ERROR_NONE;
                described_group.group_state = StrBytes::from_string(match group.state {
                    GroupState::Empty => "Empty".to_string(),
                    GroupState::PreparingRebalance => "PreparingRebalance".to_string(),
                    GroupState::CompletingRebalance => "CompletingRebalance".to_string(),
                    GroupState::Stable => "Stable".to_string(),
                    GroupState::Dead => "Dead".to_string(),
                });
                described_group.protocol_type = StrBytes::from_string(
                    group
                        .members
                        .values()
                        .next()
                        .map(|m| m.protocol_type.clone())
                        .unwrap_or_else(|| "consumer".to_string()),
                );
                described_group.protocol_data =
                    StrBytes::from_string(group.protocol_name.clone().unwrap_or_default());

                // Add member information
                for member in group.members.values() {
                    let mut described_member = DescribedGroupMember::default();
                    described_member.member_id = StrBytes::from_string(member.member_id.clone());
                    described_member.client_id = StrBytes::from_string(member.client_id.clone());
                    described_member.client_host =
                        StrBytes::from_string(member.client_host.clone());

                    // Member metadata (protocol subscription information)
                    // For now, we just store the first protocol's metadata
                    if let Some((_, metadata)) = member.protocols.first() {
                        described_member.member_metadata = metadata.clone().into();
                    }

                    // Member assignment
                    if let Some(assignment) = &member.assignment {
                        described_member.member_assignment = assignment.clone().into();
                    }

                    // Static group instance ID (KIP-345)
                    if let Some(instance_id) = &member.group_instance_id {
                        described_member.group_instance_id =
                            Some(StrBytes::from_string(instance_id.clone()));
                    }

                    described_group.members.push(described_member);
                }
            }
            None => {
                // Group doesn't exist - return error
                described_group.error_code = ERROR_UNKNOWN_SERVER_ERROR;
                described_group.group_state = StrBytes::from_string("Dead".to_string());
                described_group.protocol_type = StrBytes::from_string("".to_string());
                described_group.protocol_data = StrBytes::from_string("".to_string());
            }
        }

        response.groups.push(described_group);
    }

    Ok(response)
}

/// Handle ListGroups request
///
/// Returns a list of all consumer groups currently known to the coordinator.
/// This is useful for discovery, administration, and monitoring.
pub fn handle_list_groups(
    coordinator: &crate::kafka::GroupCoordinator,
    states_filter: Vec<String>,
) -> Result<kafka_protocol::messages::list_groups_response::ListGroupsResponse> {
    use crate::kafka::GroupState;
    use kafka_protocol::messages::list_groups_response::{ListGroupsResponse, ListedGroup};
    use kafka_protocol::messages::GroupId;
    use kafka_protocol::protocol::StrBytes;

    let mut response = ListGroupsResponse::default();
    response.error_code = ERROR_NONE;
    response.throttle_time_ms = 0;

    // Get coordinator state
    let coordinator_groups = coordinator.groups.read().unwrap();

    // Filter groups by state if requested (v4+)
    let filter_enabled = !states_filter.is_empty();

    for (group_id, group) in coordinator_groups.iter() {
        // Apply state filter if provided
        if filter_enabled {
            let state_str = match group.state {
                GroupState::Empty => "Empty",
                GroupState::PreparingRebalance => "PreparingRebalance",
                GroupState::CompletingRebalance => "CompletingRebalance",
                GroupState::Stable => "Stable",
                GroupState::Dead => "Dead",
            };

            if !states_filter.contains(&state_str.to_string()) {
                continue;
            }
        }

        let mut listed_group = ListedGroup::default();
        listed_group.group_id = GroupId(StrBytes::from_string(group_id.clone()));
        listed_group.protocol_type = StrBytes::from_string(
            group
                .members
                .values()
                .next()
                .map(|m| m.protocol_type.clone())
                .unwrap_or_else(|| "consumer".to_string()),
        );

        // Add group state (v4+)
        listed_group.group_state = StrBytes::from_string(match group.state {
            GroupState::Empty => "Empty".to_string(),
            GroupState::PreparingRebalance => "PreparingRebalance".to_string(),
            GroupState::CompletingRebalance => "CompletingRebalance".to_string(),
            GroupState::Stable => "Stable".to_string(),
            GroupState::Dead => "Dead".to_string(),
        });

        response.groups.push(listed_group);
    }

    Ok(response)
}
