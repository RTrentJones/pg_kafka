// Consumer group coordinator handlers
//
// Handlers for consumer group management APIs:
// FindCoordinator, JoinGroup, SyncGroup, Heartbeat, LeaveGroup, DescribeGroups, ListGroups

use crate::kafka::constants::{DEFAULT_BROKER_ID, ERROR_NONE};
use crate::kafka::error::Result;
use crate::kafka::handler_context::HandlerContext;
use kafka_protocol::messages::join_group_response::JoinGroupResponse;

/// Placeholder for a JoinGroup member's `client_host`. This field is informational
/// only — echoed back in DescribeGroups member listings, never used for routing or
/// authorization. The real peer address is known at the TCP accept site
/// (`listener.rs`) but is not plumbed through `KafkaRequest` to this DB-thread
/// handler, so a stable placeholder is reported. Threading the real peer host
/// through the request enum is a non-functional enhancement left as follow-up.
const PLACEHOLDER_CLIENT_HOST: &str = "localhost";

/// Handle FindCoordinator request
///
/// Returns the coordinator broker for a consumer group.
/// In our single-node setup, we always return ourselves as the coordinator.
pub fn handle_find_coordinator(
    ctx: &HandlerContext,
    key: String,
    key_type: i8,
) -> Result<kafka_protocol::messages::find_coordinator_response::FindCoordinatorResponse> {
    let broker_host = ctx.broker.host();
    let broker_port = ctx.broker.port();
    use kafka_protocol::messages::find_coordinator_response::FindCoordinatorResponse;

    crate::pg_debug!("FindCoordinator: key={}, key_type={}", key, key_type);

    // Build response - we are always the coordinator for all groups
    let mut response = FindCoordinatorResponse::default();
    response.throttle_time_ms = 0;
    response.error_code = ERROR_NONE;
    response.error_message = None;
    response.node_id = DEFAULT_BROKER_ID.into();
    response.host = broker_host.to_string().into();
    response.port = broker_port;

    Ok(response)
}

/// The JoinGroup request fields the handler uses.
#[derive(Debug, Clone)]
pub struct JoinGroupRequestData {
    /// Consumer group ID
    pub group_id: String,
    /// "" for a first join, the assigned id for a rejoin
    pub member_id: String,
    /// Client ID from the request header
    pub client_id: String,
    /// Static group instance ID (v5+), informational only
    pub group_instance_id: Option<String>,
    /// Session timeout in milliseconds
    pub session_timeout_ms: i32,
    /// Rebalance timeout in milliseconds (v1+)
    pub rebalance_timeout_ms: i32,
    /// Protocol type (e.g. "consumer")
    pub protocol_type: String,
    /// Supported assignment strategies with metadata
    pub protocols: Vec<crate::kafka::messages::JoinGroupProtocol>,
}

/// The client's side of one JoinGroup: sends the (possibly deferred) response to the
/// connection that asked. If it is dropped without having sent anything (a bug or a
/// panic between accepting the request and answering it), it answers
/// UNKNOWN_SERVER_ERROR instead, so the client is never left waiting for a response
/// that cannot come.
pub struct JoinGroupReply {
    correlation_id: i32,
    api_version: i16,
    response_tx: Option<tokio::sync::mpsc::UnboundedSender<crate::kafka::KafkaResponse>>,
}

impl JoinGroupReply {
    /// A reply for the JoinGroup `correlation_id` (at `api_version`) whose response
    /// goes to `response_tx`, the client connection's own sender.
    pub fn new(
        correlation_id: i32,
        api_version: i16,
        response_tx: tokio::sync::mpsc::UnboundedSender<crate::kafka::KafkaResponse>,
    ) -> Self {
        Self {
            correlation_id,
            api_version,
            response_tx: Some(response_tx),
        }
    }

    /// Send the response. A client that has disconnected meanwhile is ignored.
    pub fn send(mut self, response: JoinGroupResponse) {
        self.send_inner(response);
    }

    fn send_inner(&mut self, response: JoinGroupResponse) {
        if let Some(tx) = self.response_tx.take() {
            if tx
                .send(crate::kafka::KafkaResponse::JoinGroup {
                    correlation_id: self.correlation_id,
                    api_version: self.api_version,
                    response,
                })
                .is_err()
            {
                crate::pg_debug!(
                    "JoinGroup client disconnected before its response (correlation_id={})",
                    self.correlation_id
                );
            }
        }
    }
}

impl Drop for JoinGroupReply {
    fn drop(&mut self) {
        if self.response_tx.is_some() {
            self.send_inner(
                crate::kafka::response_builders::build_join_group_error_response(
                    crate::kafka::constants::ERROR_UNKNOWN_SERVER_ERROR,
                ),
            );
        }
    }
}

/// Handle JoinGroup request
///
/// The consumer joins a consumer group. `respond` receives the response exactly once:
/// at once for an invalid request, an error, or a join that does not wait; or later,
/// when the join phase the member took part in completes (another member's JoinGroup,
/// a LeaveGroup or the coordinator's periodic sweep), as Kafka holds JoinGroup
/// responses until every known member has rejoined (see `GroupCoordinator::join_group`).
/// The response carries the assigned member id, the generation, the leader, the
/// group's selected protocol, and for the leader the member list.
pub fn handle_join_group<F>(ctx: &HandlerContext, request: JoinGroupRequestData, respond: F)
where
    F: FnOnce(JoinGroupResponse) + Send + 'static,
{
    crate::pg_debug!(
        "JoinGroup: group_id={}, member_id={}, client_id={}",
        request.group_id,
        request.member_id,
        request.client_id
    );

    // RV-10: reject a session_timeout_ms outside the broker's accepted range with
    // INVALID_SESSION_TIMEOUT before touching group state, matching Kafka. This
    // prevents a hostile client from pinning a member alive far beyond any real
    // heartbeat cadence (blocking rebalances for the whole group) or requesting a
    // sub-second timeout that thrashes the group. Enforced here at the request
    // boundary so the coordinator core stays free to drive expiry with tiny
    // timeouts in unit tests.
    let session_timeout_range = crate::kafka::coordinator::MIN_SESSION_TIMEOUT_MS
        ..=crate::kafka::coordinator::MAX_SESSION_TIMEOUT_MS;
    if !session_timeout_range.contains(&request.session_timeout_ms) {
        crate::pg_warning!(
            "JoinGroup rejected: session_timeout_ms {} outside accepted range [{}, {}]",
            request.session_timeout_ms,
            crate::kafka::coordinator::MIN_SESSION_TIMEOUT_MS,
            crate::kafka::coordinator::MAX_SESSION_TIMEOUT_MS
        );
        respond(
            crate::kafka::response_builders::build_join_group_error_response(
                crate::kafka::constants::ERROR_INVALID_SESSION_TIMEOUT,
            ),
        );
        return;
    }

    let protocols: Vec<(String, Vec<u8>)> = request
        .protocols
        .into_iter()
        .map(|p| (p.name, p.metadata))
        .collect();
    // Only used if the group has not selected a protocol (never after a completed
    // join), so the response is never without one.
    let fallback_protocol = protocols.first().map(|(name, _)| name.clone());
    let protocol_type = request.protocol_type.clone();

    let params = crate::kafka::coordinator::JoinGroupParams {
        group_id: request.group_id,
        member_id: Some(request.member_id).filter(|id| !id.is_empty()),
        client_id: request.client_id,
        // client_host is informational only (see PLACEHOLDER_CLIENT_HOST); the real
        // peer address is not plumbed to this DB-thread handler.
        client_host: PLACEHOLDER_CLIENT_HOST.to_string(),
        session_timeout_ms: request.session_timeout_ms,
        rebalance_timeout_ms: request.rebalance_timeout_ms,
        protocol_type: request.protocol_type,
        protocols,
        group_instance_id: request.group_instance_id,
    };

    ctx.coordinator.join_group(
        params,
        Box::new(move |outcome| {
            respond(build_join_group_response(
                outcome,
                protocol_type,
                fallback_protocol,
            ))
        }),
    );
}

/// Build the JoinGroup response for a coordinator outcome. Errors map to their Kafka
/// error code (UnknownMemberId, RebalanceInProgress, GroupMaxSizeReached, ...).
fn build_join_group_response(
    outcome: crate::kafka::coordinator::JoinOutcome,
    protocol_type: String,
    fallback_protocol: Option<String>,
) -> JoinGroupResponse {
    use kafka_protocol::messages::join_group_response::JoinGroupResponseMember;

    let result = match outcome {
        Ok(result) => result,
        Err(e) => {
            crate::pg_warning!("Failed to handle JoinGroup request: {}", e);
            return crate::kafka::response_builders::build_join_group_error_response(
                e.to_kafka_error_code(),
            );
        }
    };

    let mut response = JoinGroupResponse::default();
    response.throttle_time_ms = 0;
    response.error_code = ERROR_NONE;
    response.generation_id = result.generation_id;
    response.protocol_type = Some(protocol_type.into());
    // RA-6: advertise the protocol the GROUP selected (common to all members).
    response.protocol_name = result.protocol_name.or(fallback_protocol).map(Into::into);
    response.leader = result.leader_id.into();
    response.member_id = result.member_id.into();
    response.members = result
        .members
        .into_iter()
        .map(|(mid, metadata)| {
            let mut member = JoinGroupResponseMember::default();
            member.member_id = mid.into();
            member.metadata = metadata.into();
            member
        })
        .collect();
    response
}

/// Handle SyncGroup request
///
/// Leader provides partition assignments, followers receive their assignment.
/// If leader sends empty assignments, server computes assignments automatically.
pub fn handle_sync_group(
    ctx: &HandlerContext,
    group_id: String,
    member_id: String,
    generation_id: i32,
    assignments: Vec<crate::kafka::messages::SyncGroupAssignment>,
) -> Result<kafka_protocol::messages::sync_group_response::SyncGroupResponse> {
    let coordinator = ctx.coordinator;
    let store = ctx.store;
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
                // Get partition counts for all topics in a single query (avoid N+1)
                let mut topic_partitions: HashMap<String, i32> = HashMap::new();
                if let Ok(metadata) = store.get_topic_metadata(Some(&topics)) {
                    for tm in metadata {
                        topic_partitions.insert(tm.name.clone(), tm.partition_count);
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
    ctx: &HandlerContext,
    group_id: String,
    member_id: String,
    generation_id: i32,
) -> Result<kafka_protocol::messages::heartbeat_response::HeartbeatResponse> {
    let coordinator = ctx.coordinator;
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
    ctx: &HandlerContext,
    group_id: String,
    member_id: String,
    members: Vec<crate::kafka::messages::MemberIdentity>,
) -> Result<kafka_protocol::messages::leave_group_response::LeaveGroupResponse> {
    let coordinator = ctx.coordinator;
    use kafka_protocol::messages::leave_group_response::{LeaveGroupResponse, MemberResponse};
    use kafka_protocol::protocol::StrBytes;

    let mut response = LeaveGroupResponse::default();
    response.throttle_time_ms = 0;
    response.error_code = ERROR_NONE;

    if members.is_empty() {
        // LeaveGroup v0-2: a single top-level member_id.
        crate::pg_debug!("LeaveGroup: group_id={}, member_id={}", group_id, member_id);
        // Coordinator returns typed errors (UnknownMemberId, CoordinatorNotAvailable) that map to
        // Kafka error codes via to_kafka_error_code().
        coordinator.leave_group(group_id, member_id)?;
    } else {
        // LeaveGroup v3+ (the framing kafkajs / Sarama / librdkafka send): the top-level member_id
        // is empty and the leaving members are batched in `members`. Remove each and report a
        // per-member result. Without this the member is never removed and the group stays
        // non-empty, so DeleteGroups keeps returning NON_EMPTY_GROUP after a clean leave.
        for m in members {
            crate::pg_debug!(
                "LeaveGroup(v3+): group_id={}, member_id={}",
                group_id,
                m.member_id
            );
            let error_code = match coordinator.leave_group(group_id.clone(), m.member_id.clone()) {
                Ok(()) => ERROR_NONE,
                Err(e) => e.to_kafka_error_code(),
            };
            let mut member_response = MemberResponse::default();
            member_response.member_id = StrBytes::from_string(m.member_id);
            member_response.group_instance_id = m.group_instance_id.map(StrBytes::from_string);
            member_response.error_code = error_code;
            response.members.push(member_response);
        }
    }

    Ok(response)
}

/// Handle DescribeGroups request
///
/// Returns detailed information about consumer groups including members and state.
/// This is critical for debugging consumer group issues and monitoring group health.
pub fn handle_describe_groups(
    ctx: &HandlerContext,
    groups: Vec<String>,
) -> Result<kafka_protocol::messages::describe_groups_response::DescribeGroupsResponse> {
    let coordinator = ctx.coordinator;
    use kafka_protocol::messages::describe_groups_response::{
        DescribeGroupsResponse, DescribedGroup, DescribedGroupMember,
    };
    use kafka_protocol::messages::GroupId;
    use kafka_protocol::protocol::StrBytes;

    let mut response = DescribeGroupsResponse::default();
    response.throttle_time_ms = 0;

    // Get coordinator state
    // CG-7: recover from a poisoned lock instead of panicking (which would crash the worker). A
    // poisoned lock only means some writer panicked; the group map itself is still readable.
    let coordinator_groups = coordinator.groups.read().unwrap_or_else(|e| e.into_inner());

    for group_id in groups {
        let mut described_group = DescribedGroup::default();
        described_group.group_id = GroupId(StrBytes::from_string(group_id.clone()));

        match coordinator_groups.get(&group_id) {
            Some(group) => {
                // Group exists - return detailed information
                described_group.error_code = ERROR_NONE;
                described_group.group_state = StrBytes::from_static_str(group.state.as_str());
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
                // Group doesn't exist. For the versions we advertise (v0-5) Kafka reports
                // this as success with state Dead; GROUP_ID_NOT_FOUND only applies from v6.
                described_group.error_code = ERROR_NONE;
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
    ctx: &HandlerContext,
    states_filter: Vec<String>,
) -> Result<kafka_protocol::messages::list_groups_response::ListGroupsResponse> {
    let coordinator = ctx.coordinator;
    use kafka_protocol::messages::list_groups_response::{ListGroupsResponse, ListedGroup};
    use kafka_protocol::messages::GroupId;
    use kafka_protocol::protocol::StrBytes;

    let mut response = ListGroupsResponse::default();
    response.error_code = ERROR_NONE;
    response.throttle_time_ms = 0;

    // Get coordinator state
    // CG-7: recover from a poisoned lock instead of panicking (which would crash the worker). A
    // poisoned lock only means some writer panicked; the group map itself is still readable.
    let coordinator_groups = coordinator.groups.read().unwrap_or_else(|e| e.into_inner());

    // Filter groups by state if requested (v4+)
    let filter_enabled = !states_filter.is_empty();

    for (group_id, group) in coordinator_groups.iter() {
        // Apply state filter if provided
        if filter_enabled {
            let state_str = group.state.as_str();

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
        listed_group.group_state = StrBytes::from_static_str(group.state.as_str());

        response.groups.push(listed_group);
    }

    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::constants::{ERROR_INVALID_SESSION_TIMEOUT, ERROR_REBALANCE_IN_PROGRESS};
    use crate::kafka::error::KafkaError;
    use crate::kafka::messages::{JoinGroupProtocol, SyncGroupAssignment};
    use crate::kafka::{BrokerMetadata, GroupCoordinator};
    use crate::testing::mocks::MockKafkaStore;
    use std::sync::{Arc, Mutex};

    type ResponseSlot = Arc<Mutex<Option<JoinGroupResponse>>>;

    struct Fixture {
        store: MockKafkaStore,
        coordinator: GroupCoordinator,
        broker: BrokerMetadata,
    }

    impl Fixture {
        fn new() -> Self {
            Self {
                store: MockKafkaStore::new(),
                coordinator: GroupCoordinator::new(),
                broker: BrokerMetadata::new("localhost".to_string(), 9092),
            }
        }

        fn ctx(&self) -> HandlerContext<'_> {
            HandlerContext::new(
                &self.store,
                &self.coordinator,
                &self.broker,
                1,
                kafka_protocol::records::Compression::None,
            )
        }

        /// Send a JoinGroup and return the slot its response lands in.
        fn join(&self, group: &str, member_id: &str, session_timeout_ms: i32) -> ResponseSlot {
            let slot: ResponseSlot = Arc::new(Mutex::new(None));
            let writer = slot.clone();
            handle_join_group(
                &self.ctx(),
                JoinGroupRequestData {
                    group_id: group.to_string(),
                    member_id: member_id.to_string(),
                    client_id: "test-client".to_string(),
                    group_instance_id: None,
                    session_timeout_ms,
                    rebalance_timeout_ms: 60_000,
                    protocol_type: "consumer".to_string(),
                    protocols: vec![JoinGroupProtocol {
                        name: "range".to_string(),
                        metadata: vec![0, 1, 2, 3],
                    }],
                },
                move |response| {
                    let mut guard = writer.lock().unwrap();
                    assert!(guard.is_none(), "one response per JoinGroup");
                    *guard = Some(response);
                },
            );
            slot
        }

        /// Join and expect an immediate response.
        fn join_now(&self, group: &str, member_id: &str) -> JoinGroupResponse {
            self.join(group, member_id, 30_000)
                .lock()
                .unwrap()
                .take()
                .expect("JoinGroup answered immediately")
        }
    }

    fn take(slot: &ResponseSlot) -> Option<JoinGroupResponse> {
        slot.lock().unwrap().take()
    }

    #[test]
    fn test_handle_find_coordinator_success() {
        let f = Fixture::new();
        let response = handle_find_coordinator(&f.ctx(), "test-group".to_string(), 0).unwrap();
        assert_eq!(response.error_code, ERROR_NONE);
        assert_eq!(response.node_id, DEFAULT_BROKER_ID);
        assert_eq!(response.host.as_str(), "localhost");
        assert_eq!(response.port, 9092);
    }

    #[test]
    fn test_handle_join_group_new_member_becomes_leader() {
        let f = Fixture::new();
        let response = f.join_now("test-group", "");
        assert_eq!(response.error_code, ERROR_NONE);
        assert!(!response.member_id.is_empty());
        assert_eq!(response.generation_id, 1);
        assert_eq!(response.leader.as_str(), response.member_id.as_str());
        assert_eq!(response.members.len(), 1, "the leader gets the member list");
        assert_eq!(response.protocol_name.as_deref(), Some("range"));
        assert_eq!(response.protocol_type.as_deref(), Some("consumer"));
    }

    #[test]
    fn test_handle_join_group_rejects_out_of_bounds_session_timeout() {
        // RV-10: a session_timeout_ms outside the accepted range is rejected with
        // INVALID_SESSION_TIMEOUT before any group state is created.
        let f = Fixture::new();
        for bad in [100, i32::MAX] {
            let response = take(&f.join("g", "", bad)).expect("answered immediately");
            assert_eq!(response.error_code, ERROR_INVALID_SESSION_TIMEOUT);
        }
        assert!(f.coordinator.get_group_state("g").is_none());
        // The Kafka minimum (6s) is admitted.
        let ok = take(&f.join("g2", "", 6000)).expect("answered immediately");
        assert_eq!(ok.error_code, ERROR_NONE);
    }

    #[test]
    fn test_handle_join_group_second_member_is_held_until_leader_rejoins() {
        let f = Fixture::new();
        let leader = f.join_now("test-group", "");
        let leader_id = leader.member_id.to_string();

        let second = f.join("test-group", "", 30_000);
        assert!(take(&second).is_none(), "held until the leader rejoins");

        let leader2 = f.join_now("test-group", &leader_id);
        let second = take(&second).expect("answered when the join phase completed");
        assert_eq!(second.error_code, ERROR_NONE);
        assert_eq!(second.generation_id, 2);
        assert_eq!(leader2.generation_id, 2);
        assert_eq!(second.leader.as_str(), leader_id.as_str());
        assert!(second.members.is_empty(), "followers get no member list");
        assert_eq!(leader2.members.len(), 2);
    }

    #[test]
    fn test_handle_join_group_rejoin_with_existing_id() {
        let f = Fixture::new();
        let first = f.join_now("test-group", "");
        let rejoin = f.join_now("test-group", first.member_id.as_str());
        assert_eq!(rejoin.error_code, ERROR_NONE);
        assert_eq!(rejoin.member_id, first.member_id);
        assert_eq!(rejoin.generation_id, first.generation_id);
    }

    #[test]
    fn test_handle_join_group_deferred_error_maps_to_error_response() {
        // A held JoinGroup replaced by a resend is answered REBALANCE_IN_PROGRESS.
        let f = Fixture::new();
        let leader = f.join_now("g", "").member_id.to_string();
        let follower = f.join("g", "", 30_000);
        let gen = f.join_now("g", &leader).generation_id;
        let follower = take(&follower).unwrap().member_id.to_string();
        f.coordinator
            .sync_group("g".to_string(), leader.clone(), gen, vec![])
            .unwrap();

        // The leader rejoins (held: the follower has not rejoined), then resends.
        let first = f.join("g", &leader, 30_000);
        let second = f.join("g", &leader, 30_000);
        let first = take(&first).expect("replaced join answered");
        assert_eq!(first.error_code, ERROR_REBALANCE_IN_PROGRESS);
        assert_eq!(first.generation_id, -1);
        assert!(take(&second).is_none());
        // The follower's rejoin completes the phase and answers the resend.
        let _ = f.join_now("g", &follower);
        assert_eq!(take(&second).unwrap().generation_id, gen + 1);
    }

    #[test]
    fn test_join_group_reply_sends_once_and_answers_when_dropped_unsent() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        JoinGroupReply::new(7, 5, tx.clone()).send(JoinGroupResponse::default());
        match rx.try_recv().unwrap() {
            crate::kafka::KafkaResponse::JoinGroup {
                correlation_id,
                api_version,
                response,
            } => {
                assert_eq!((correlation_id, api_version), (7, 5));
                assert_eq!(response.error_code, ERROR_NONE);
            }
            other => panic!("unexpected response {:?}", other),
        }
        assert!(rx.try_recv().is_err(), "exactly one response");

        drop(JoinGroupReply::new(8, 5, tx.clone()));
        match rx.try_recv().unwrap() {
            crate::kafka::KafkaResponse::JoinGroup {
                correlation_id,
                response,
                ..
            } => {
                assert_eq!(correlation_id, 8);
                assert_eq!(
                    response.error_code,
                    crate::kafka::constants::ERROR_UNKNOWN_SERVER_ERROR
                );
            }
            other => panic!("unexpected response {:?}", other),
        }

        // A disconnected client is ignored.
        drop(rx);
        JoinGroupReply::new(9, 5, tx).send(JoinGroupResponse::default());
    }

    #[test]
    fn test_handle_sync_group_leader_and_follower() {
        let f = Fixture::new();
        let leader = f.join_now("test-group", "");
        let leader_id = leader.member_id.to_string();
        let follower = f.join("test-group", "", 30_000);
        let gen = f.join_now("test-group", &leader_id).generation_id;
        let follower_id = take(&follower).unwrap().member_id.to_string();

        let leader_sync = handle_sync_group(
            &f.ctx(),
            "test-group".to_string(),
            leader_id.clone(),
            gen,
            vec![
                SyncGroupAssignment {
                    member_id: leader_id,
                    assignment: vec![1, 2, 3],
                },
                SyncGroupAssignment {
                    member_id: follower_id.clone(),
                    assignment: vec![4, 5, 6],
                },
            ],
        )
        .unwrap();
        assert_eq!(leader_sync.assignment.as_ref(), &[1, 2, 3]);

        let follower_sync =
            handle_sync_group(&f.ctx(), "test-group".to_string(), follower_id, gen, vec![])
                .unwrap();
        assert_eq!(follower_sync.error_code, ERROR_NONE);
        assert_eq!(follower_sync.assignment.as_ref(), &[4, 5, 6]);
    }

    #[test]
    fn test_handle_heartbeat_success_and_unknown_member() {
        let f = Fixture::new();
        let join = f.join_now("test-group", "");
        let member_id = join.member_id.to_string();
        let ok = handle_heartbeat(
            &f.ctx(),
            "test-group".to_string(),
            member_id,
            join.generation_id,
        )
        .unwrap();
        assert_eq!(ok.error_code, ERROR_NONE);
        let unknown = handle_heartbeat(
            &f.ctx(),
            "test-group".to_string(),
            "unknown-member-id".to_string(),
            join.generation_id,
        );
        assert!(matches!(unknown, Err(KafkaError::UnknownMemberId { .. })));
    }

    #[test]
    fn test_handle_leave_group_v0_and_unknown_member() {
        let f = Fixture::new();
        let member_id = f.join_now("test-group", "").member_id.to_string();
        let unknown = handle_leave_group(
            &f.ctx(),
            "test-group".to_string(),
            "unknown-member-id".to_string(),
            vec![],
        );
        assert!(matches!(unknown, Err(KafkaError::UnknownMemberId { .. })));
        let leave = handle_leave_group(
            &f.ctx(),
            "test-group".to_string(),
            member_id.clone(),
            vec![],
        )
        .unwrap();
        assert_eq!(leave.error_code, ERROR_NONE);
        assert!(handle_heartbeat(&f.ctx(), "test-group".to_string(), member_id, 1).is_err());
    }

    #[test]
    fn test_handle_leave_group_v3_members_array() {
        // LeaveGroup v3+ batches the leaving members in `members` with an empty top-level
        // member_id; each is removed and reported per member.
        let f = Fixture::new();
        let member_id = f.join_now("test-group", "").member_id.to_string();
        let leave = handle_leave_group(
            &f.ctx(),
            "test-group".to_string(),
            String::new(),
            vec![
                crate::kafka::messages::MemberIdentity {
                    member_id: member_id.clone(),
                    group_instance_id: None,
                },
                crate::kafka::messages::MemberIdentity {
                    member_id: "unknown-member".to_string(),
                    group_instance_id: None,
                },
            ],
        )
        .unwrap();
        assert_eq!(leave.error_code, ERROR_NONE);
        assert_eq!(leave.members.len(), 2);
        assert_eq!(leave.members[0].error_code, ERROR_NONE);
        assert_ne!(leave.members[1].error_code, ERROR_NONE);
        assert!(handle_heartbeat(&f.ctx(), "test-group".to_string(), member_id, 1).is_err());
    }

    #[test]
    fn test_handle_leave_group_releases_held_join() {
        let f = Fixture::new();
        let leader = f.join_now("test-group", "").member_id.to_string();
        let second = f.join("test-group", "", 30_000);
        assert!(take(&second).is_none());
        handle_leave_group(&f.ctx(), "test-group".to_string(), leader, vec![]).unwrap();
        let second = take(&second).expect("answered by the LeaveGroup");
        assert_eq!(second.error_code, ERROR_NONE);
        assert_eq!(second.leader, second.member_id);
    }

    #[test]
    fn test_handle_describe_and_list_groups() {
        let f = Fixture::new();
        let join = f.join_now("test-group", "");
        let described = handle_describe_groups(
            &f.ctx(),
            vec!["test-group".to_string(), "missing".to_string()],
        )
        .unwrap();
        assert_eq!(described.groups.len(), 2);
        let group = &described.groups[0];
        assert_eq!(group.error_code, ERROR_NONE);
        assert_eq!(group.group_state.as_str(), "CompletingRebalance");
        assert_eq!(group.members.len(), 1);
        assert_eq!(group.members[0].member_id.as_str(), join.member_id.as_str());
        assert_eq!(described.groups[1].group_state.as_str(), "Dead");

        let all = handle_list_groups(&f.ctx(), vec![]).unwrap();
        assert_eq!(all.groups.len(), 1);
        let completing =
            handle_list_groups(&f.ctx(), vec!["CompletingRebalance".to_string()]).unwrap();
        assert_eq!(completing.groups.len(), 1);
        let empty = handle_list_groups(&f.ctx(), vec!["Empty".to_string()]).unwrap();
        assert!(empty.groups.is_empty());
    }

    #[test]
    fn test_describe_and_list_groups_recover_from_poisoned_lock() {
        // CG-7: a poisoned groups lock (a writer panicked while holding it) must NOT crash
        // DescribeGroups / ListGroups. They previously did `groups.read().unwrap()`, which panics
        // on a poisoned lock; now they recover via into_inner(). This test poisons the lock and
        // asserts both handlers still return Ok (it also covers the recovery closures).
        let coordinator = crate::kafka::GroupCoordinator::new();
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = coordinator.groups.write().unwrap();
            panic!("intentionally poison the lock");
        }));
        assert!(coordinator.groups.is_poisoned(), "lock should be poisoned");

        let store = MockKafkaStore::new();
        let broker = crate::kafka::BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(
            &store,
            &coordinator,
            &broker,
            1,
            kafka_protocol::records::Compression::None,
        );

        assert!(handle_describe_groups(&ctx, vec!["g".to_string()]).is_ok());
        assert!(handle_list_groups(&ctx, vec![]).is_ok());
    }
}
