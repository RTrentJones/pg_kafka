//! Consumer group handlers: FindCoordinator, JoinGroup, SyncGroup, Heartbeat, LeaveGroup,
//! DescribeGroups and ListGroups, over the in-memory `GroupCoordinator`.

use crate::kafka::constants::{DEFAULT_BROKER_ID, ERROR_NONE};
use crate::kafka::coordinator::{ConsumerGroup, GroupMember};
use crate::kafka::error::Result;
use crate::kafka::handler_context::HandlerContext;
use kafka_protocol::messages::describe_groups_response::{
    DescribeGroupsResponse, DescribedGroup, DescribedGroupMember,
};
use kafka_protocol::messages::find_coordinator_response::FindCoordinatorResponse;
use kafka_protocol::messages::heartbeat_response::HeartbeatResponse;
use kafka_protocol::messages::join_group_response::JoinGroupResponse;
use kafka_protocol::messages::join_group_response::JoinGroupResponseMember;
use kafka_protocol::messages::leave_group_response::{LeaveGroupResponse, MemberResponse};
use kafka_protocol::messages::list_groups_response::{ListGroupsResponse, ListedGroup};
use kafka_protocol::messages::sync_group_response::SyncGroupResponse;
use kafka_protocol::messages::GroupId;
use kafka_protocol::protocol::StrBytes;
use std::collections::HashMap;

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
    key: &str,
    key_type: i8,
) -> Result<FindCoordinatorResponse> {
    let broker_host = ctx.broker.host();
    let broker_port = ctx.broker.port();

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
        let response = normalize_join_group_response(response, self.api_version);
        if let Some(tx) = self.response_tx.take() {
            if tx
                .send(crate::kafka::KafkaResponse::new(
                    self.correlation_id,
                    self.api_version,
                    crate::kafka::api::ResponseBody::JoinGroup(response),
                ))
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

/// Make a JoinGroup response encodable as Kafka encodes it at `api_version`:
/// `protocol_name` is nullable only from v7, so an absent name (an error response) is sent as ""
/// to older versions (Kafka's `GroupCoordinator.NoProtocol`); a null there made the Java client
/// fail to parse the response.
pub fn normalize_join_group_response(
    mut response: JoinGroupResponse,
    api_version: i16,
) -> JoinGroupResponse {
    if api_version < 7 && response.protocol_name.is_none() {
        response.protocol_name = Some(StrBytes::from_static_str(""));
    }
    response
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

    if let Some(error_code) = session_timeout_refusal(request.session_timeout_ms) {
        respond(crate::kafka::response_builders::build_join_group_error_response(error_code));
        return;
    }
    let protocols: Vec<(String, Vec<u8>)> = request
        .protocols
        .into_iter()
        .map(|p| (p.name, p.metadata))
        .collect();
    if let Some(error_code) = protocols_refusal(&protocols) {
        respond(crate::kafka::response_builders::build_join_group_error_response(error_code));
        return;
    }
    ctx.coordinator.set_limits(
        crate::kafka::limits::max_consumer_groups(),
        crate::kafka::limits::group_max_size(),
    );
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
        &params,
        Box::new(move |outcome| {
            respond(build_join_group_response(
                outcome,
                protocol_type,
                fallback_protocol,
            ));
        }),
    );
}

/// INVALID_SESSION_TIMEOUT for a session_timeout_ms outside the broker's accepted range,
/// checked before touching group state, as in Kafka. This prevents a hostile client from
/// pinning a member alive far beyond any real heartbeat cadence (blocking rebalances for
/// the whole group) or requesting a sub-second timeout that thrashes the group. It is
/// enforced at the request boundary so the coordinator core stays free to drive expiry
/// with tiny timeouts in unit tests.
fn session_timeout_refusal(session_timeout_ms: i32) -> Option<i16> {
    let range = crate::kafka::coordinator::MIN_SESSION_TIMEOUT_MS
        ..=crate::kafka::coordinator::MAX_SESSION_TIMEOUT_MS;
    if range.contains(&session_timeout_ms) {
        return None;
    }
    crate::pg_warning!(
        "JoinGroup rejected: session_timeout_ms {} outside accepted range [{}, {}]",
        session_timeout_ms,
        crate::kafka::coordinator::MIN_SESSION_TIMEOUT_MS,
        crate::kafka::coordinator::MAX_SESSION_TIMEOUT_MS
    );
    Some(crate::kafka::constants::ERROR_INVALID_SESSION_TIMEOUT)
}

/// INVALID_REQUEST for a protocol list beyond what one member may park in coordinator
/// memory. Kafka keeps a group's member metadata in one __consumer_offsets record
/// (message.max.bytes, 1 MiB by default).
fn protocols_refusal(protocols: &[(String, Vec<u8>)]) -> Option<i16> {
    let protocol_bytes: usize = protocols.iter().map(|(n, m)| n.len() + m.len()).sum();
    if protocols.len() <= crate::kafka::limits::MAX_MEMBER_PROTOCOLS
        && protocol_bytes <= crate::kafka::limits::MAX_MEMBER_PROTOCOL_BYTES
    {
        return None;
    }
    crate::pg_warning!(
        "JoinGroup rejected: {} protocols / {} bytes of protocol metadata exceed the limit ({} / {})",
        protocols.len(),
        protocol_bytes,
        crate::kafka::limits::MAX_MEMBER_PROTOCOLS,
        crate::kafka::limits::MAX_MEMBER_PROTOCOL_BYTES
    );
    Some(crate::kafka::constants::ERROR_INVALID_REQUEST)
}

/// Build the JoinGroup response for a coordinator outcome. Errors map to their Kafka
/// error code (UnknownMemberId, RebalanceInProgress, GroupMaxSizeReached, ...).
fn build_join_group_response(
    outcome: crate::kafka::coordinator::JoinOutcome,
    protocol_type: String,
    fallback_protocol: Option<String>,
) -> JoinGroupResponse {
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
    // Advertise the protocol the GROUP selected (common to all members).
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

/// The client's side of one SyncGroup: sends the (possibly deferred) response to the
/// connection that asked, exactly once. Like [`JoinGroupReply`], if it is dropped
/// without having sent anything it answers UNKNOWN_SERVER_ERROR.
pub struct SyncGroupReply {
    correlation_id: i32,
    api_version: i16,
    response_tx: Option<tokio::sync::mpsc::UnboundedSender<crate::kafka::KafkaResponse>>,
}

impl SyncGroupReply {
    /// A reply for the SyncGroup `correlation_id` (at `api_version`) whose response
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
    pub fn send(mut self, response: SyncGroupResponse) {
        self.send_inner(response);
    }

    fn send_inner(&mut self, response: SyncGroupResponse) {
        if let Some(tx) = self.response_tx.take() {
            if tx
                .send(crate::kafka::KafkaResponse::new(
                    self.correlation_id,
                    self.api_version,
                    crate::kafka::api::ResponseBody::SyncGroup(response),
                ))
                .is_err()
            {
                crate::pg_debug!(
                    "SyncGroup client disconnected before its response (correlation_id={})",
                    self.correlation_id
                );
            }
        }
    }
}

impl Drop for SyncGroupReply {
    fn drop(&mut self) {
        if self.response_tx.is_some() {
            self.send_inner(
                crate::kafka::response_builders::build_sync_group_error_response(
                    crate::kafka::constants::ERROR_UNKNOWN_SERVER_ERROR,
                ),
            );
        }
    }
}

/// Handle SyncGroup request
///
/// The leader provides partition assignments (or none, in which case they are
/// computed server-side); every member receives its own. `respond` receives the
/// response exactly once: at once for the leader, a Stable group or an error, or,
/// for a follower whose SyncGroup arrives before the leader's, when the leader syncs
/// (or a new rebalance / the member's removal fails it), as Kafka holds it (see
/// `GroupCoordinator::sync_group`).
pub fn handle_sync_group<F>(
    ctx: &HandlerContext,
    group_id: &str,
    member_id: &str,
    generation_id: i32,
    assignments: Vec<crate::kafka::messages::SyncGroupAssignment>,
    respond: F,
) where
    F: FnOnce(SyncGroupResponse) + Send + 'static,
{
    let coordinator = ctx.coordinator;
    let store = ctx.store;

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

    // A leader that sent empty assignments asks the server to compute them. (Only the
    // leader's assignments are used, so a follower's SyncGroup computes nothing.)
    let assignments_empty =
        coord_assignments.is_empty() || coord_assignments.iter().all(|(_, bytes)| bytes.is_empty());

    if assignments_empty && coordinator.is_group_leader(group_id, member_id) {
        // Get subscribed topics from group members
        if let Ok(topics) = coordinator.get_subscribed_topics(group_id) {
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
                        coordinator.compute_assignments(group_id, &topic_partitions)
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

    // Coordinator outcomes are typed errors (UnknownMemberId, IllegalGeneration,
    // RebalanceInProgress) which map to Kafka error codes via to_kafka_error_code().
    coordinator.sync_group(
        group_id,
        member_id,
        generation_id,
        coord_assignments,
        Box::new(move |outcome| respond(build_sync_group_response(outcome))),
    );
}

/// Build the SyncGroup response for a coordinator outcome.
fn build_sync_group_response(outcome: crate::kafka::coordinator::SyncOutcome) -> SyncGroupResponse {
    match outcome {
        Ok(assignment) => {
            let mut response = SyncGroupResponse::default();
            response.throttle_time_ms = 0;
            response.error_code = ERROR_NONE;
            response.assignment = assignment.into();
            response
        }
        Err(e) => {
            crate::pg_warning!("Failed to handle SyncGroup request: {}", e);
            crate::kafka::response_builders::build_sync_group_error_response(
                e.to_kafka_error_code(),
            )
        }
    }
}

/// Handle Heartbeat request
///
/// Updates member's last heartbeat timestamp.
pub fn handle_heartbeat(
    ctx: &HandlerContext,
    group_id: &str,
    member_id: &str,
    generation_id: i32,
) -> Result<HeartbeatResponse> {
    let coordinator = ctx.coordinator;

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
    group_id: &str,
    member_id: &str,
    members: Vec<crate::kafka::messages::MemberIdentity>,
) -> Result<LeaveGroupResponse> {
    let coordinator = ctx.coordinator;

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
            let error_code = match coordinator.leave_group(group_id, &m.member_id) {
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
) -> Result<DescribeGroupsResponse> {
    let coordinator = ctx.coordinator;

    let mut response = DescribeGroupsResponse::default();
    response.throttle_time_ms = 0;

    // Get coordinator state
    // Recover from a poisoned lock instead of panicking (which would crash the worker). A
    // poisoned lock only means some writer panicked; the group map itself is still readable.
    let coordinator_groups = coordinator
        .groups
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner);

    // Whether each group the coordinator does not hold has committed offsets, in one
    // lookup however many groups the request names.
    let unheld: Vec<String> = groups
        .iter()
        .filter(|g| !coordinator_groups.contains_key(*g))
        .cloned()
        .collect();
    let with_offsets = if unheld.is_empty() {
        Ok(std::collections::HashSet::new())
    } else {
        ctx.store
            .groups_with_offsets(&unheld)
            .map_err(|e| e.to_kafka_error_code())
    };
    for group_id in groups {
        let described_group = match coordinator_groups.get(&group_id) {
            Some(group) => describe_held_group(&group_id, group),
            None => describe_unheld_group(
                &group_id,
                with_offsets.as_ref().map(|g| g.contains(&group_id)),
            ),
        };
        response.groups.push(described_group);
    }

    Ok(response)
}

/// DescribeGroups for a group the coordinator holds: its state, protocol and members.
fn describe_held_group(group_id: &str, group: &ConsumerGroup) -> DescribedGroup {
    let mut described_group = DescribedGroup::default();
    described_group.group_id = GroupId(StrBytes::from_string(group_id.to_string()));
    described_group.error_code = ERROR_NONE;
    described_group.group_state = StrBytes::from_static_str(group.state.as_str());
    described_group.protocol_type = StrBytes::from_string(
        group
            .protocol_type
            .clone()
            .unwrap_or_else(|| "consumer".to_string()),
    );
    described_group.protocol_data =
        StrBytes::from_string(group.protocol_name.clone().unwrap_or_default());
    described_group.members = group
        .members
        .values()
        .map(|member| describe_member(member, group.protocol_name.as_ref()))
        .collect();
    described_group
}

/// One member's DescribeGroups entry.
fn describe_member(member: &GroupMember, protocol_name: Option<&String>) -> DescribedGroupMember {
    let mut described_member = DescribedGroupMember::default();
    described_member.member_id = StrBytes::from_string(member.member_id.clone());
    described_member.client_id = StrBytes::from_string(member.client_id.clone());
    described_member.client_host = StrBytes::from_string(member.client_host.clone());

    // Member metadata for the group's selected protocol, as Kafka reports it (the first
    // protocol before a protocol has been selected).
    if let Some((_, metadata)) = member
        .protocols
        .iter()
        .find(|(n, _)| Some(n) == protocol_name)
        .or_else(|| member.protocols.first())
    {
        described_member.member_metadata = metadata.clone().into();
    }
    if let Some(assignment) = &member.assignment {
        described_member.member_assignment = assignment.clone().into();
    }
    // Static group instance ID (KIP-345)
    if let Some(instance_id) = &member.group_instance_id {
        described_member.group_instance_id = Some(StrBytes::from_string(instance_id.clone()));
    }
    described_member
}

/// DescribeGroups for a group the coordinator does not hold (none of its members joined
/// since the worker started, or it was reclaimed). It still exists while it has committed
/// offsets: Kafka reports it as an Empty "consumer" group, which is what lets
/// `kafka-consumer-groups --describe` show its offsets and lag. Without offsets it does not
/// exist: for the versions we advertise (v0-5) Kafka reports that as success with state
/// Dead (GROUP_ID_NOT_FOUND only applies from v6).
/// `has_offsets` is whether it has any, or the error code of a failed lookup.
fn describe_unheld_group(
    group_id: &str,
    has_offsets: std::result::Result<bool, &i16>,
) -> DescribedGroup {
    let mut described_group = DescribedGroup::default();
    described_group.group_id = GroupId(StrBytes::from_string(group_id.to_string()));
    described_group.protocol_data = StrBytes::from_static_str("");
    let (error_code, state, protocol_type) = match has_offsets {
        Ok(true) => (ERROR_NONE, "Empty", "consumer"),
        Ok(false) => (ERROR_NONE, "Dead", ""),
        Err(&code) => (code, "", ""),
    };
    described_group.error_code = error_code;
    described_group.group_state = StrBytes::from_static_str(state);
    described_group.protocol_type = StrBytes::from_static_str(protocol_type);
    described_group
}

/// Handle ListGroups request
///
/// Returns a list of all consumer groups currently known to the coordinator.
/// This is useful for discovery, administration, and monitoring.
pub fn handle_list_groups(
    ctx: &HandlerContext,
    states_filter: &[String],
) -> Result<ListGroupsResponse> {
    let coordinator = ctx.coordinator;

    let mut response = ListGroupsResponse::default();
    response.error_code = ERROR_NONE;
    response.throttle_time_ms = 0;

    // Get coordinator state
    // Recover from a poisoned lock instead of panicking (which would crash the worker). A
    // poisoned lock only means some writer panicked; the group map itself is still readable.
    let coordinator_groups = coordinator
        .groups
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner);

    // Filter groups by state if requested (v4+), case-insensitively. The filter is a set,
    // as in Kafka, so a long filter list costs once, not once per group.
    let filter_enabled = !states_filter.is_empty();
    let wanted_states: std::collections::HashSet<String> = states_filter
        .iter()
        .map(|f| f.to_ascii_lowercase())
        .collect();

    let mut listed: Vec<(String, String, &'static str)> = coordinator_groups
        .iter()
        .map(|(group_id, group)| {
            (
                group_id.clone(),
                group
                    .protocol_type
                    .clone()
                    .unwrap_or_else(|| "consumer".to_string()),
                group.state.as_str(),
            )
        })
        .collect();
    // Groups known only by their committed offsets are Empty "consumer" groups
    // (see handle_describe_groups), so `kafka-consumer-groups --list` keeps showing them after a
    // worker restart.
    for group_id in ctx.store.list_offset_groups()? {
        if !coordinator_groups.contains_key(&group_id) {
            listed.push((group_id, "consumer".to_string(), "Empty"));
        }
    }
    listed.sort();

    for (group_id, protocol_type, state) in listed {
        // Apply state filter if provided (v4+)
        if filter_enabled && !wanted_states.contains(&state.to_ascii_lowercase()) {
            continue;
        }

        let mut listed_group = ListedGroup::default();
        listed_group.group_id = GroupId(StrBytes::from_string(group_id));
        listed_group.protocol_type = StrBytes::from_string(protocol_type);
        // Add group state (v4+)
        listed_group.group_state = StrBytes::from_static_str(state);
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
    use crate::testing::TestEnv;
    use std::sync::{Arc, Mutex};

    type ResponseSlot = Arc<Mutex<Option<JoinGroupResponse>>>;

    struct Fixture {
        store: MockKafkaStore,
        coordinator: GroupCoordinator,
        broker: BrokerMetadata,
    }

    impl Fixture {
        fn new() -> Self {
            // Groups unknown to the coordinator are looked up by their offsets.
            let mut store = MockKafkaStore::new();
            store
                .expect_groups_with_offsets()
                .returning(|_| Ok(std::collections::HashSet::new()));
            store.expect_list_offset_groups().returning(|| Ok(vec![]));
            Self {
                store,
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

    type SyncSlot = Arc<Mutex<Option<SyncGroupResponse>>>;

    fn take_sync(slot: &SyncSlot) -> Option<SyncGroupResponse> {
        slot.lock().unwrap().take()
    }

    impl Fixture {
        /// Send a SyncGroup and return the slot its response lands in.
        fn sync(
            &self,
            group: &str,
            member_id: &str,
            generation: i32,
            assignments: Vec<SyncGroupAssignment>,
        ) -> SyncSlot {
            let slot: SyncSlot = Arc::new(Mutex::new(None));
            let writer = slot.clone();
            handle_sync_group(
                &self.ctx(),
                group,
                member_id,
                generation,
                assignments,
                move |response| {
                    let mut guard = writer.lock().unwrap();
                    assert!(guard.is_none(), "one response per SyncGroup");
                    *guard = Some(response);
                },
            );
            slot
        }
    }

    #[test]
    fn test_handle_find_coordinator_success() {
        let f = Fixture::new();
        let response = handle_find_coordinator(&f.ctx(), "test-group", 0).unwrap();
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
        // A session_timeout_ms outside the accepted range is rejected with
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
        let leader_sync = f.sync("g", &leader, gen, vec![]);
        assert_eq!(take_sync(&leader_sync).unwrap().error_code, ERROR_NONE);

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
            crate::kafka::KafkaResponse {
                correlation_id,
                api_version,
                body: crate::kafka::api::ResponseBody::JoinGroup(response),
            } => {
                assert_eq!((correlation_id, api_version), (7, 5));
                assert_eq!(response.error_code, ERROR_NONE);
            }
            other => panic!("unexpected response {other:?}"),
        }
        assert!(rx.try_recv().is_err(), "exactly one response");

        drop(JoinGroupReply::new(8, 5, tx.clone()));
        match rx.try_recv().unwrap() {
            crate::kafka::KafkaResponse {
                correlation_id,
                body: crate::kafka::api::ResponseBody::JoinGroup(response),
                ..
            } => {
                assert_eq!(correlation_id, 8);
                assert_eq!(
                    response.error_code,
                    crate::kafka::constants::ERROR_UNKNOWN_SERVER_ERROR
                );
            }
            other => panic!("unexpected response {other:?}"),
        }

        // A disconnected client is ignored.
        drop(rx);
        JoinGroupReply::new(9, 5, tx).send(JoinGroupResponse::default());
    }

    #[test]
    fn test_handle_sync_group_follower_parked_until_leader_syncs() {
        let f = Fixture::new();
        let leader = f.join_now("test-group", "");
        let leader_id = leader.member_id.to_string();
        let follower = f.join("test-group", "", 30_000);
        let gen = f.join_now("test-group", &leader_id).generation_id;
        let follower_id = take(&follower).unwrap().member_id.to_string();

        // The follower syncs first: no response yet.
        let follower_sync = f.sync("test-group", &follower_id, gen, vec![]);
        assert!(
            take_sync(&follower_sync).is_none(),
            "parked until the leader syncs"
        );

        let leader_sync = f.sync(
            "test-group",
            &leader_id,
            gen,
            vec![
                SyncGroupAssignment {
                    member_id: leader_id.clone(),
                    assignment: vec![1, 2, 3],
                },
                SyncGroupAssignment {
                    member_id: follower_id.clone(),
                    assignment: vec![4, 5, 6],
                },
            ],
        );
        assert_eq!(
            take_sync(&leader_sync).unwrap().assignment.as_ref(),
            &[1, 2, 3]
        );
        let follower_sync = take_sync(&follower_sync).expect("answered by the leader's sync");
        assert_eq!(follower_sync.error_code, ERROR_NONE);
        assert_eq!(follower_sync.assignment.as_ref(), &[4, 5, 6]);

        // Stable: answered at once; a stale generation maps to ILLEGAL_GENERATION.
        let again = take_sync(&f.sync("test-group", &follower_id, gen, vec![])).unwrap();
        assert_eq!(again.assignment.as_ref(), &[4, 5, 6]);
        let stale = take_sync(&f.sync("test-group", &follower_id, gen - 1, vec![])).unwrap();
        assert_eq!(
            stale.error_code,
            crate::kafka::constants::ERROR_ILLEGAL_GENERATION
        );
    }

    #[test]
    fn test_sync_group_reply_sends_once_and_answers_when_dropped_unsent() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        SyncGroupReply::new(7, 3, tx.clone()).send(SyncGroupResponse::default());
        match rx.try_recv().unwrap() {
            crate::kafka::KafkaResponse {
                correlation_id,
                api_version,
                body: crate::kafka::api::ResponseBody::SyncGroup(response),
            } => {
                assert_eq!((correlation_id, api_version), (7, 3));
                assert_eq!(response.error_code, ERROR_NONE);
            }
            other => panic!("unexpected response {other:?}"),
        }
        assert!(rx.try_recv().is_err(), "exactly one response");

        drop(SyncGroupReply::new(8, 3, tx.clone()));
        match rx.try_recv().unwrap() {
            crate::kafka::KafkaResponse {
                correlation_id,
                body: crate::kafka::api::ResponseBody::SyncGroup(response),
                ..
            } => {
                assert_eq!(correlation_id, 8);
                assert_eq!(
                    response.error_code,
                    crate::kafka::constants::ERROR_UNKNOWN_SERVER_ERROR
                );
            }
            other => panic!("unexpected response {other:?}"),
        }
        drop(rx);
        SyncGroupReply::new(9, 3, tx).send(SyncGroupResponse::default());
    }

    #[test]
    fn test_handle_heartbeat_success_and_unknown_member() {
        let f = Fixture::new();
        let join = f.join_now("test-group", "");
        let member_id = join.member_id.to_string();
        let ok = handle_heartbeat(&f.ctx(), "test-group", &member_id, join.generation_id).unwrap();
        assert_eq!(ok.error_code, ERROR_NONE);
        let unknown = handle_heartbeat(
            &f.ctx(),
            "test-group",
            "unknown-member-id",
            join.generation_id,
        );
        assert!(matches!(unknown, Err(KafkaError::UnknownMemberId { .. })));
    }

    #[test]
    fn test_handle_leave_group_v0_and_unknown_member() {
        let f = Fixture::new();
        let member_id = f.join_now("test-group", "").member_id.to_string();
        let unknown = handle_leave_group(&f.ctx(), "test-group", "unknown-member-id", vec![]);
        assert!(matches!(unknown, Err(KafkaError::UnknownMemberId { .. })));
        let leave = handle_leave_group(&f.ctx(), "test-group", &member_id, vec![]).unwrap();
        assert_eq!(leave.error_code, ERROR_NONE);
        assert!(handle_heartbeat(&f.ctx(), "test-group", &member_id, 1).is_err());
    }

    #[test]
    fn test_handle_leave_group_v3_members_array() {
        // LeaveGroup v3+ batches the leaving members in `members` with an empty top-level
        // member_id; each is removed and reported per member.
        let f = Fixture::new();
        let member_id = f.join_now("test-group", "").member_id.to_string();
        let leave = handle_leave_group(
            &f.ctx(),
            "test-group",
            "",
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
        assert!(handle_heartbeat(&f.ctx(), "test-group", &member_id, 1).is_err());
    }

    #[test]
    fn test_handle_leave_group_releases_held_join() {
        let f = Fixture::new();
        let leader = f.join_now("test-group", "").member_id.to_string();
        let second = f.join("test-group", "", 30_000);
        assert!(take(&second).is_none());
        handle_leave_group(&f.ctx(), "test-group", &leader, vec![]).unwrap();
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

        let all = handle_list_groups(&f.ctx(), &[]).unwrap();
        assert_eq!(all.groups.len(), 1);
        let completing =
            handle_list_groups(&f.ctx(), &["CompletingRebalance".to_string()]).unwrap();
        assert_eq!(completing.groups.len(), 1);
        let empty = handle_list_groups(&f.ctx(), &["Empty".to_string()]).unwrap();
        assert_eq!(empty.groups, [] as [ListedGroup; 0]);
    }

    #[test]
    fn test_describe_and_list_groups_recover_from_poisoned_lock() {
        // A poisoned groups lock (a writer panicked while holding it) must not crash
        // DescribeGroups / ListGroups: they recover the guard with into_inner(). This test
        // poisons the lock and asserts both handlers still return Ok.
        let coordinator = GroupCoordinator::new();
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = coordinator.groups.write().unwrap();
            panic!("intentionally poison the lock");
        }));
        assert!(coordinator.groups.is_poisoned(), "lock should be poisoned");

        let mut store = MockKafkaStore::new();
        store
            .expect_groups_with_offsets()
            .returning(|_| Ok(std::collections::HashSet::new()));
        store.expect_list_offset_groups().returning(|| Ok(vec![]));
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(
            &store,
            &coordinator,
            &broker,
            1,
            kafka_protocol::records::Compression::None,
        );

        assert!(handle_describe_groups(&ctx, vec!["g".to_string()]).is_ok());
        assert!(handle_list_groups(&ctx, &[]).is_ok());
    }

    // ----- Groups known only by their committed offsets -----

    fn offsets_store(groups: &'static [&'static str]) -> MockKafkaStore {
        let mut store = MockKafkaStore::new();
        store.expect_groups_with_offsets().returning(move |asked| {
            Ok(asked
                .iter()
                .filter(|g| groups.contains(&g.as_str()))
                .cloned()
                .collect())
        });
        store
            .expect_list_offset_groups()
            .returning(move || Ok(groups.iter().map(ToString::to_string).collect()));
        store
    }

    #[test]
    fn test_offsets_only_group_is_an_empty_consumer_group() {
        // A group the coordinator does not hold (e.g. after a worker restart) but that
        // has committed offsets is reported as an Empty "consumer" group, as Kafka does;
        // it was "Dead" in DescribeGroups and missing from ListGroups.
        let store = offsets_store(&["committed-only", "live"]);
        let env = TestEnv::new();
        let ctx = env.ctx(&store);
        // "live" is also in memory: it must be listed once, with its real state.
        let (responder, slot) = {
            let slot: ResponseSlot = Arc::new(Mutex::new(None));
            let w = slot.clone();
            (
                move |r: JoinGroupResponse| *w.lock().unwrap() = Some(r),
                slot,
            )
        };
        handle_join_group(
            &ctx,
            JoinGroupRequestData {
                group_id: "live".to_string(),
                member_id: String::new(),
                client_id: "c".to_string(),
                group_instance_id: None,
                session_timeout_ms: 30_000,
                rebalance_timeout_ms: 60_000,
                protocol_type: "consumer".to_string(),
                protocols: vec![JoinGroupProtocol {
                    name: "range".to_string(),
                    metadata: vec![],
                }],
            },
            responder,
        );
        assert_eq!(take(&slot).unwrap().error_code, ERROR_NONE);

        let d = handle_describe_groups(
            &ctx,
            vec!["committed-only".to_string(), "nothing".to_string()],
        )
        .unwrap();
        assert_eq!(d.groups[0].error_code, ERROR_NONE);
        assert_eq!(d.groups[0].group_state.as_str(), "Empty");
        assert_eq!(d.groups[0].protocol_type.as_str(), "consumer");
        assert_eq!(d.groups[0].members, [] as [DescribedGroupMember; 0]);
        assert_eq!(d.groups[1].group_state.as_str(), "Dead");

        let l = handle_list_groups(&ctx, &[]).unwrap();
        let listed: Vec<(&str, &str, &str)> = l
            .groups
            .iter()
            .map(|g| {
                (
                    g.group_id.as_str(),
                    g.protocol_type.as_str(),
                    g.group_state.as_str(),
                )
            })
            .collect();
        assert_eq!(
            listed,
            vec![
                ("committed-only", "consumer", "Empty"),
                ("live", "consumer", "CompletingRebalance")
            ]
        );
        let empty_only = handle_list_groups(&ctx, &["empty".to_string()]).unwrap();
        assert_eq!(
            empty_only.groups.len(),
            1,
            "state filter is case-insensitive"
        );
    }

    /// ListGroups takes time linear in its groups and in its states filter together: a
    /// filter of many entries is checked as a set, not once per group.
    #[test]
    fn list_groups_filters_in_linear_time() {
        let growth = crate::testing::growth(3_000, |n| {
            let groups: Vec<String> = (0..n).map(|i| format!("g{i:07}")).collect();
            let mut store = MockKafkaStore::new();
            store
                .expect_list_offset_groups()
                .returning(move || Ok(groups.clone()));
            let env = TestEnv::new();
            let filter: Vec<String> = (0..n)
                .map(|i| format!("NotAState{i}"))
                .chain(["EMPTY".to_string()])
                .collect();
            let listed = handle_list_groups(&env.ctx(&store), &filter).unwrap();
            assert_eq!(listed.groups.len(), n);
        });
        assert!(
            growth < 8.0,
            "4x the groups and filter took {growth:.1}x the time"
        );
    }

    #[test]
    fn test_join_group_error_response_protocol_name_by_version() {
        // JoinGroupResponse.protocol_name is nullable only from v7: older versions get "".
        let e = crate::kafka::response_builders::build_join_group_error_response(23);
        assert_eq!(
            normalize_join_group_response(e.clone(), 5)
                .protocol_name
                .as_deref(),
            Some("")
        );
        assert_eq!(normalize_join_group_response(e, 7).protocol_name, None);
    }

    #[test]
    fn test_join_group_rejects_oversized_protocol_metadata() {
        let f = Fixture::new();
        let slot: ResponseSlot = Arc::new(Mutex::new(None));
        let w = slot.clone();
        handle_join_group(
            &f.ctx(),
            JoinGroupRequestData {
                group_id: "big".to_string(),
                member_id: String::new(),
                client_id: "c".to_string(),
                group_instance_id: None,
                session_timeout_ms: 30_000,
                rebalance_timeout_ms: 60_000,
                protocol_type: "consumer".to_string(),
                protocols: vec![JoinGroupProtocol {
                    name: "range".to_string(),
                    metadata: vec![0; crate::kafka::limits::MAX_MEMBER_PROTOCOL_BYTES + 1],
                }],
            },
            move |r| *w.lock().unwrap() = Some(r),
        );
        assert_eq!(
            take(&slot).unwrap().error_code,
            crate::kafka::constants::ERROR_INVALID_REQUEST
        );
        assert!(f.coordinator.get_group_state("big").is_none());
    }
}
