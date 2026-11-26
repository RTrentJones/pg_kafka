//! Consumer group handlers: FindCoordinator, JoinGroup, SyncGroup, Heartbeat, LeaveGroup,
//! DescribeGroups and ListGroups, over the in-memory `GroupCoordinator`.

use crate::kafka::constants::{DEFAULT_BROKER_ID, ERROR_NONE, MAX_PREALLOC_ELEMENTS};
use crate::kafka::coordinator::{AssignmentRefusal, ConsumerGroup, GroupMember};
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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

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

    // Kafka 3.7 knows two coordinator types (0 group, 1 transaction) and answers any other
    // INVALID_REQUEST ("Unknown coordinator type received").
    if !(0..=1).contains(&key_type) {
        return Ok(
            crate::kafka::response_builders::build_find_coordinator_error_response(
                crate::kafka::constants::ERROR_INVALID_REQUEST,
            ),
        );
    }

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

/// The error a JoinGroup or SyncGroup reply dropped unsent answers: COORDINATOR_NOT_AVAILABLE.
///
/// A reply is dropped unsent when its request unwinds (a Postgres ERROR, which pgrx raises as a
/// panic: a lock timeout on kafka.topics while the leader's SyncGroup reads its subscribed
/// topics for the server-side assignment, or a bug) or when the worker exits with held
/// JoinGroups and parked SyncGroups. Either way the broker failed, not the request: neither
/// API writes to the database, the coordinator keeps the group as it was before the request
/// (the unwinding request changed nothing, or the coordinator is gone), and what the client
/// should do is look the coordinator up again and rejoin, which COORDINATOR_NOT_AVAILABLE
/// tells the Java consumer and librdkafka (a leader rejoining with unchanged metadata during
/// the sync phase keeps its generation and syncs again). Kafka 3.7 answers a group request
/// whose group state it could not store the same way. UNKNOWN_SERVER_ERROR is fatal to the
/// Java consumer's JoinGroup and SyncGroup.
///
/// A rule for every dropped reply, rather than a subtransaction around the assignment's
/// topic read: the read is not the only way a reply goes unsent, and every way has the same
/// right answer.
const DROPPED_REPLY_ERROR: i16 = crate::kafka::constants::ERROR_COORDINATOR_NOT_AVAILABLE;

/// The client's side of one JoinGroup: sends the (possibly deferred) response to the
/// connection that asked. If it is dropped without having sent anything, it answers
/// [`DROPPED_REPLY_ERROR`] instead, so the client is never left waiting for a response
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
                    DROPPED_REPLY_ERROR,
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

    // Kafka 3.7 answers a JoinGroup error with the request's member id
    // (`JoinGroupResult(memberId, error)`): INVALID_SESSION_TIMEOUT, UNKNOWN_MEMBER_ID,
    // INCONSISTENT_GROUP_PROTOCOL and the rest, checked against a 3.7 broker.
    let member_id = request.member_id.clone();
    if let Some(error_code) = session_timeout_refusal(request.session_timeout_ms) {
        respond(join_group_error_response(error_code, member_id));
        return;
    }
    let protocols: Vec<(String, Vec<u8>)> = request
        .protocols
        .into_iter()
        .map(|p| (p.name, p.metadata))
        .collect();
    if let Some(error_code) = protocols_refusal(&protocols) {
        respond(join_group_error_response(error_code, member_id));
        return;
    }
    ctx.coordinator.set_limits(
        crate::kafka::limits::max_consumer_groups(),
        crate::kafka::limits::group_max_size(),
    );
    ctx.coordinator
        .set_memory_limit(crate::kafka::limits::max_group_memory_bytes());
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

    // Whether `join_group` has returned: an error answered after that is the answer of
    // a held JoinGroup (its member removed by a LeaveGroup or a DeleteGroups while it
    // waited, or the join replaced by a resend), which Kafka 3.7 answers with no member
    // id (`maybeInvokeJoinCallback(member, JoinGroupResult(UNKNOWN_MEMBER_ID, error))`).
    // All of this runs on the DB thread; the atomic only makes the closure `Send`.
    let returned = Arc::new(AtomicBool::new(false));
    let held = returned.clone();
    ctx.coordinator.join_group(
        &params,
        Box::new(move |outcome| {
            let member_id = if held.load(Ordering::Relaxed) {
                String::new()
            } else {
                member_id
            };
            respond(build_join_group_response(
                outcome,
                protocol_type,
                fallback_protocol,
                member_id,
            ));
        }),
    );
    returned.store(true, Ordering::Relaxed);
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
    crate::pg_debug!(
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
    crate::pg_debug!(
        "JoinGroup rejected: {} protocols / {} bytes of protocol metadata exceed the limit ({} / {})",
        protocols.len(),
        protocol_bytes,
        crate::kafka::limits::MAX_MEMBER_PROTOCOLS,
        crate::kafka::limits::MAX_MEMBER_PROTOCOL_BYTES
    );
    Some(crate::kafka::constants::ERROR_INVALID_REQUEST)
}

/// Log the error a JoinGroup or SyncGroup is answered with, at the level
/// [`crate::kafka::dispatch::failure_log_level`] gives it (a client's own error at DEBUG1).
fn log_group_failure(api: &str, e: &crate::kafka::error::KafkaError) {
    match crate::kafka::dispatch::failure_log_level(e) {
        crate::kafka::dispatch::FailureLogLevel::Warning => {
            crate::pg_warning!("Failed to handle {} request: {}", api, e);
        }
        crate::kafka::dispatch::FailureLogLevel::Debug => {
            crate::pg_debug!("{} answered with an error: {}", api, e);
        }
    }
}

/// A JoinGroup error response carrying the request's `member_id`, as Kafka 3.7 answers
/// every JoinGroup error but GROUP_MAX_SIZE_REACHED, which it answers with no member id
/// (only a new member is refused for the group's size; a known member always rejoins).
fn join_group_error_response(error_code: i16, member_id: String) -> JoinGroupResponse {
    let mut response = crate::kafka::response_builders::build_join_group_error_response(error_code);
    if error_code != crate::kafka::constants::ERROR_GROUP_MAX_SIZE_REACHED {
        response.member_id = member_id.into();
    }
    response
}

/// Build the JoinGroup response for a coordinator outcome. Errors map to their Kafka
/// error code (UnknownMemberId, RebalanceInProgress, GroupMaxSizeReached, ...) and carry
/// `member_id` ([`join_group_error_response`]): the request's for an error answered at
/// once, empty for the error answer of a held JoinGroup.
fn build_join_group_response(
    outcome: crate::kafka::coordinator::JoinOutcome,
    protocol_type: String,
    fallback_protocol: Option<String>,
    member_id: String,
) -> JoinGroupResponse {
    let result = match outcome {
        Ok(result) => result,
        Err(e) => {
            log_group_failure("JoinGroup", &e);
            return join_group_error_response(e.to_kafka_error_code(), member_id);
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
/// without having sent anything it answers [`DROPPED_REPLY_ERROR`].
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
                    DROPPED_REPLY_ERROR,
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
    coordinator.set_memory_limit(crate::kafka::limits::max_group_memory_bytes());

    crate::pg_debug!(
        "SyncGroup: group_id={}, member_id={}, generation_id={}, assignments={}",
        group_id,
        member_id,
        generation_id,
        assignments.len()
    );

    // Convert assignments to coordinator format
    let coord_assignments: Vec<(String, Vec<u8>)> = assignments
        .into_iter()
        .map(|a| (a.member_id, a.assignment))
        .collect();

    // A leader that sent empty assignments asks the server to compute them. Only the
    // SyncGroup the coordinator takes assignments from (the leader's, at the current
    // generation, in the sync phase) computes them: every other SyncGroup ignores its
    // assignments, and the computation runs on the single DB thread.
    let assignments_empty =
        coord_assignments.is_empty() || coord_assignments.iter().all(|(_, bytes)| bytes.is_empty());

    if assignments_empty && coordinator.awaits_leader_sync(group_id, member_id, generation_id) {
        // A refused assignment is not computed again while its refusal holds (for size, the
        // generation; for memory, until it would fit): the leader's retries get the refusal
        // at the cost of a lookup (see `GroupCoordinator::assignment_refused`).
        if let Some(why) = coordinator.assignment_refused(group_id, generation_id) {
            crate::pg_debug!(
                "SyncGroup: the server-side assignment of group {} at generation {} was refused before",
                group_id,
                generation_id
            );
            respond(build_sync_group_response(Err(match why {
                AssignmentRefusal::TooLarge => crate::kafka::error::KafkaError::PolicyViolation {
                    message: format!(
                        "the server-side assignment of group '{group_id}' at generation {generation_id} was refused as too large"
                    ),
                },
                AssignmentRefusal::OverMemory { .. } => {
                    crate::kafka::error::KafkaError::CoordinatorNotAvailable {
                        group_id: group_id.to_string(),
                    }
                }
            })));
            return;
        }
        match server_side_assignments(ctx, group_id) {
            Ok(Some(computed)) => {
                // A refusal by the memory budget is remembered until the assignments would
                // fit.
                coordinator.sync_group_computed(
                    group_id,
                    member_id,
                    generation_id,
                    computed,
                    Box::new(move |outcome| respond(build_sync_group_response(outcome))),
                );
                return;
            }
            Ok(None) => {}
            Err(e @ crate::kafka::error::KafkaError::PolicyViolation { .. }) => {
                // Too large to compute: nothing is stored and the group stays in its sync
                // phase (the rebalance timeout evicts a leader that cannot sync).
                coordinator.refuse_assignment(group_id, generation_id);
                respond(build_sync_group_response(Err(e)));
                return;
            }
            Err(e) => {
                // A transient failure (the topics could not be read): the leader is told to
                // retry, nothing is stored or remembered, and the group stays in its sync
                // phase. Going on with no assignments would make the group Stable with every
                // member assigned nothing until the next rebalance. COORDINATOR_NOT_AVAILABLE
                // is what Kafka 3.7 answers a SyncGroup whose assignment it failed to store;
                // the Java client and librdkafka look the coordinator up again and rejoin,
                // and the leader's rejoin with unchanged metadata keeps the generation, so its
                // next SyncGroup computes the assignment again (UNKNOWN_SERVER_ERROR would be
                // fatal to the Java client's SyncGroup). A Postgres ERROR in the read unwinds
                // instead of returning here; the dropped reply answers the same code
                // (DROPPED_REPLY_ERROR). Unlike Kafka, the parked followers keep waiting and
                // the group is not sent back to its join phase
                // (docs/PROTOCOL_DEVIATIONS.md).
                crate::pg_warning!(
                    "SyncGroup: the server-side assignment of group {} at generation {} failed: {}",
                    group_id,
                    generation_id,
                    e
                );
                respond(build_sync_group_response(Err(
                    crate::kafka::error::KafkaError::CoordinatorNotAvailable {
                        group_id: group_id.to_string(),
                    },
                )));
                return;
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

/// A member id and its encoded MemberAssignment.
type MemberAssignmentBytes = (String, Vec<u8>);

/// The assignments the server computes for `group_id` with the group's assignor over the
/// topics its members subscribe to: `None` when there is nothing to assign (no member
/// subscribes to a topic, or no subscribed topic exists); a `PolicyViolation` when the
/// computation would exceed [`crate::kafka::limits::MAX_ASSIGNMENT_WORK`]; any other `Err`
/// when it failed (the topics could not be read, the assignments not encoded), which the
/// caller answers as retriable.
fn server_side_assignments(
    ctx: &HandlerContext,
    group_id: &str,
) -> Result<Option<Vec<MemberAssignmentBytes>>> {
    let coordinator = ctx.coordinator;
    let topics = coordinator.get_subscribed_topics(group_id)?;
    if topics.is_empty() {
        return Ok(None);
    }
    // Partition counts for all topics in a single query (avoid N+1)
    let metadata = ctx.store.get_topic_metadata(Some(&topics))?;
    let topic_partitions: HashMap<String, i32> = metadata
        .into_iter()
        .map(|tm| (tm.name, tm.partition_count))
        .collect();
    if topic_partitions.is_empty() {
        return Ok(None);
    }
    let computed = coordinator.compute_assignments(group_id, &topic_partitions)?;
    crate::pg_debug!(
        "SyncGroup: computed {} assignments server-side for group {}",
        computed.len(),
        group_id
    );
    Ok(Some(computed.into_iter().collect()))
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
            log_group_failure("SyncGroup", &e);
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
    leaving: crate::kafka::messages::LeavingMembers,
) -> Result<LeaveGroupResponse> {
    use crate::kafka::messages::LeavingMembers;
    let coordinator = ctx.coordinator;

    let mut response = LeaveGroupResponse::default();
    response.throttle_time_ms = 0;
    response.error_code = ERROR_NONE;

    match leaving {
        LeavingMembers::Single(member_id) => {
            crate::pg_debug!("LeaveGroup: group_id={}, member_id={}", group_id, member_id);
            // Coordinator returns typed errors (UnknownMemberId, CoordinatorNotAvailable)
            // that map to Kafka error codes via to_kafka_error_code().
            coordinator.leave_group(group_id, &member_id)?;
        }
        LeavingMembers::Batch(members) => {
            // LeaveGroup v3+ (the framing kafkajs / Sarama / librdkafka send): the leaving
            // members are batched in `members`. Remove each and report a per-member result;
            // an empty batch removes nothing and is NONE, known group or not, as in Kafka
            // 3.7. Without this the member is never removed and the group stays non-empty,
            // so DeleteGroups keeps returning NON_EMPTY_GROUP after a clean leave. One
            // coordinator call answers every entry (see `GroupCoordinator::leave_members`
            // for its cost): nothing here is logged or copied per entry.
            let codes = coordinator
                .leave_members(group_id, members.iter().map(|m| m.member_id.as_str()))?;
            response.members = members
                .into_iter()
                .zip(codes)
                .map(|(m, error_code)| {
                    let mut member_response = MemberResponse::default();
                    member_response.member_id = StrBytes::from_string(m.member_id);
                    member_response.group_instance_id =
                        m.group_instance_id.map(StrBytes::from_string);
                    member_response.error_code = error_code;
                    member_response
                })
                .collect();
        }
    }

    Ok(response)
}

/// Handle DescribeGroups request
///
/// Returns detailed information about consumer groups including members and state, within
/// [`crate::config::response_max_bytes`] (see [`describe_groups`]).
pub fn handle_describe_groups(
    ctx: &HandlerContext,
    groups: Vec<String>,
) -> Result<DescribeGroupsResponse> {
    Ok(describe_groups(
        ctx,
        groups,
        crate::config::response_max_bytes(),
    ))
}

/// DescribeGroups of `groups` in a response of at most about `max_bytes`.
///
/// Every entry is answered, a repeated group as often as it is named, as Kafka 3.7 does. A
/// held group's entry carries its members' ids, hosts, metadata and assignments, up to
/// `pg_kafka.max_group_memory` (128 MB by default) for one group, so the entries described
/// in full are bounded together: an entry whose description ([`described_group_bytes`])
/// would take the response past `max_bytes` is answered COORDINATOR_NOT_AVAILABLE
/// (retriable: a client describes it again, in a smaller request), except the first held
/// group, which is always described so that any one group can be. Each distinct group is
/// described once; its repeats share that entry's bytes.
///
/// Cost, for `n` entries naming `d` distinct held groups of `m` members in all: time
/// O(n + m) to size and build them plus O(members) per entry described in full, which the
/// bound limits to O(`max_bytes` / [`DESCRIBED_MEMBER_OVERHEAD`]); memory: one copy of
/// what each distinct group's entry reports ([`described_group_bytes`]: for a group that is
/// not Stable, its members' ids, client ids and hosts, not their metadata or assignments),
/// which the repeats share, plus O(request) for the other entries; so the response holds at
/// most the larger of `max_bytes` and the first held group's description.
pub fn describe_groups(
    ctx: &HandlerContext,
    groups: Vec<String>,
    max_bytes: usize,
) -> DescribeGroupsResponse {
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

    // Whether each group the coordinator does not hold has offsets (committed, or pending in
    // an open transaction), in one lookup however many groups the request names.
    let unheld: Vec<String> = groups
        .iter()
        .filter(|g| !coordinator_groups.contains_key(g.as_str()))
        .cloned()
        .collect();
    let with_offsets = if unheld.is_empty() {
        Ok(std::collections::HashSet::new())
    } else {
        ctx.store
            .groups_with_offsets(&unheld)
            .map_err(|e| e.to_kafka_error_code())
    };
    // Each distinct held group's size, and its entry once built.
    let mut sized: HashMap<String, (usize, Option<DescribedGroup>)> = HashMap::new();
    let mut described_bytes = 0_usize;
    response
        .groups
        .reserve(groups.len().min(MAX_PREALLOC_ELEMENTS));
    for group_id in groups {
        let described_group = match coordinator_groups.get(group_id.as_str()) {
            Some(group) => {
                let (bytes, entry) = sized
                    .entry(group_id)
                    .or_insert_with(|| (described_group_bytes(group), None));
                if described_bytes > 0 && described_bytes.saturating_add(*bytes) > max_bytes {
                    refused_described_group(&group.group_id)
                } else {
                    described_bytes = described_bytes.saturating_add(*bytes);
                    entry
                        .get_or_insert_with(|| describe_held_group(&group.group_id, group))
                        .clone()
                }
            }
            None => describe_unheld_group(
                &group_id,
                with_offsets.as_ref().map(|g| g.contains(&group_id)),
            ),
        };
        response.groups.push(described_group);
    }

    response
}

/// What one member adds to a DescribeGroups entry besides its strings and bytes: their
/// length prefixes, the entry's framing and tagged fields, rounded up.
const DESCRIBED_MEMBER_OVERHEAD: usize = 32;

/// What a held group's DescribeGroups entry takes when encoded, at most: its id, state,
/// protocol type and name, and each member's ids, client id, host and (Stable only, as
/// [`describe_held_group`] reports them) metadata and assignment, with their framing.
///
/// Cost: O(members) time, no memory.
fn described_group_bytes(group: &ConsumerGroup) -> usize {
    let stable = group.state == crate::kafka::coordinator::GroupState::Stable;
    let members: usize = group
        .members
        .values()
        .map(|member| {
            let payload = if stable {
                member
                    .protocols
                    .iter()
                    .find(|(n, _)| Some(n) == group.protocol_name.as_ref())
                    .or_else(|| member.protocols.first())
                    .map_or(0, |(_, metadata)| metadata.len())
                    + member.assignment.as_ref().map_or(0, Vec::len)
            } else {
                0
            };
            DESCRIBED_MEMBER_OVERHEAD
                + member.member_id.len()
                + member.client_id.len()
                + member.client_host.len()
                + member.group_instance_id.as_ref().map_or(0, String::len)
                + payload
        })
        .sum();
    DESCRIBED_MEMBER_OVERHEAD
        + group.group_id.len()
        + group.protocol_type.as_ref().map_or(0, String::len)
        + group.protocol_name.as_ref().map_or(0, String::len)
        + members
}

/// The entry of a held group whose description does not fit the response
/// ([`describe_groups`]): COORDINATOR_NOT_AVAILABLE, retriable.
fn refused_described_group(group_id: &str) -> DescribedGroup {
    let mut described_group = DescribedGroup::default();
    described_group.group_id = GroupId(StrBytes::from_string(group_id.to_string()));
    described_group.error_code = crate::kafka::constants::ERROR_COORDINATOR_NOT_AVAILABLE;
    described_group
}

/// DescribeGroups for a group the coordinator holds: its state, protocol and members. As in
/// Kafka 3.7 (`GroupMetadata.summary`), only a Stable group reports its protocol and each
/// member's metadata and assignment; in any other state they are empty (the protocol
/// chosen by a join phase in progress, and the metadata members sent for it, are not settled).
fn describe_held_group(group_id: &str, group: &ConsumerGroup) -> DescribedGroup {
    let stable = group.state == crate::kafka::coordinator::GroupState::Stable;
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
    described_group.protocol_data = if stable {
        StrBytes::from_string(group.protocol_name.clone().unwrap_or_default())
    } else {
        StrBytes::default()
    };
    // Only a Stable group's members carry their metadata and assignment: in any other
    // state they are not copied at all.
    described_group.members = group
        .members
        .values()
        .map(|member| describe_member(member, stable, group.protocol_name.as_ref()))
        .collect();
    described_group
}

/// One member's DescribeGroups entry: with its metadata for `protocol_name` and its
/// assignment when `payload`, without them otherwise. O(what the entry holds) time and
/// memory: the metadata and assignment are copied only when reported.
fn describe_member(
    member: &GroupMember,
    payload: bool,
    protocol_name: Option<&String>,
) -> DescribedGroupMember {
    let mut described_member = DescribedGroupMember::default();
    described_member.member_id = StrBytes::from_string(member.member_id.clone());
    described_member.client_id = StrBytes::from_string(member.client_id.clone());
    described_member.client_host = StrBytes::from_string(member.client_host.clone());

    if payload {
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
/// Returns the consumer groups the coordinator holds and those known by their offsets, within
/// [`crate::config::response_max_bytes`] (see [`list_groups`]).
pub fn handle_list_groups(
    ctx: &HandlerContext,
    states_filter: &[String],
) -> Result<ListGroupsResponse> {
    list_groups(ctx, states_filter, crate::config::response_max_bytes())
}

/// ListGroups of the groups in `states_filter` (all if empty) in a response of at most about
/// `max_bytes`.
///
/// The groups are listed in the order of their ids: the groups the coordinator holds, and the
/// groups known only by their offsets (committed, or pending in an open transaction) as Empty
/// "consumer" groups (see [`describe_unheld_group`]), so `kafka-consumer-groups --list` keeps
/// showing them after a worker restart. Group ids are client strings of up to 32 KB and the
/// offset-only groups are bounded only by the database, so the list is cut where its entries
/// ([`listed_group_bytes`]) would take the response past `max_bytes`; the first group is always
/// listed. ListGroups has no per-group error, and an error code would make the Java admin
/// client drop the whole list, so the cut list is answered NONE and the server logs a WARNING
/// (docs/PROTOCOL_DEVIATIONS.md). With a state filter the offset-only groups read are counted
/// as if none were filtered out, so the list may be cut a little earlier than `max_bytes`.
///
/// Cost, for `h` held groups and `o` offset-only groups read: the offset-only ids past
/// `max_bytes` are never read ([`crate::kafka::storage::OffsetStore::list_offset_groups`]);
/// time O((h + o) log(h + o)) to sort references to them; memory O(h + o) references, the ids
/// read (at most about `max_bytes`) and a response of at most about `max_bytes`.
pub fn list_groups(
    ctx: &HandlerContext,
    states_filter: &[String],
    max_bytes: usize,
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

    // Filter groups by state if requested (v4+), case-sensitively as Kafka 3.7 does
    // (`kafka-consumer-groups --state` sends the canonical names). The filter is a set, as in
    // Kafka, so a long filter list costs once, not once per group.
    let wanted_states: std::collections::HashSet<&str> =
        states_filter.iter().map(String::as_str).collect();
    let wanted = |state: &str| wanted_states.is_empty() || wanted_states.contains(state);

    // Groups known only by their offsets are Empty: read only when Empty groups are wanted.
    let offset_groups = if wanted("Empty") {
        ctx.store.list_offset_groups(max_bytes)?
    } else {
        Vec::new()
    };
    let mut listed: Vec<(&str, &str, &'static str)> = coordinator_groups
        .iter()
        .filter(|(_, group)| wanted(group.state.as_str()))
        .map(|(group_id, group)| {
            (
                &**group_id,
                group.protocol_type.as_deref().unwrap_or("consumer"),
                group.state.as_str(),
            )
        })
        .collect();
    listed.extend(
        offset_groups
            .iter()
            .filter(|group_id| !coordinator_groups.contains_key(group_id.as_str()))
            .map(|group_id| (group_id.as_str(), "consumer", "Empty")),
    );
    listed.sort_unstable();

    let mut listed_bytes = 0_usize;
    let mut cut = false;
    for (group_id, protocol_type, state) in listed {
        let bytes = listed_group_bytes(group_id, protocol_type, state);
        if listed_bytes > 0 && listed_bytes.saturating_add(bytes) > max_bytes {
            cut = true;
            break;
        }
        listed_bytes = listed_bytes.saturating_add(bytes);
        let mut listed_group = ListedGroup::default();
        listed_group.group_id = GroupId(StrBytes::from_string(group_id.to_string()));
        listed_group.protocol_type = StrBytes::from_string(protocol_type.to_string());
        // Add group state (v4+)
        listed_group.group_state = StrBytes::from_static_str(state);
        response.groups.push(listed_group);
    }
    if cut {
        crate::pg_warning!(
            "ListGroups: {} group(s) listed; the groups after them are left out, as listing them would take the response past pg_kafka.fetch_max_bytes ({} bytes)",
            response.groups.len(),
            max_bytes
        );
    }

    Ok(response)
}

/// What a ListGroups entry takes when encoded: its id, protocol type and state, and
/// [`crate::kafka::storage::LISTED_GROUP_OVERHEAD`] for their framing. O(1).
fn listed_group_bytes(group_id: &str, protocol_type: &str, state: &str) -> usize {
    group_id.len()
        + protocol_type.len()
        + state.len()
        + crate::kafka::storage::LISTED_GROUP_OVERHEAD
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::constants::{
        ERROR_INCONSISTENT_GROUP_PROTOCOL, ERROR_INVALID_SESSION_TIMEOUT,
        ERROR_REBALANCE_IN_PROGRESS, ERROR_UNKNOWN_MEMBER_ID,
    };
    use crate::kafka::error::KafkaError;
    use crate::kafka::messages::{JoinGroupProtocol, LeavingMembers, SyncGroupAssignment};
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
            store.expect_list_offset_groups().returning(|_| Ok(vec![]));
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
            self.join_protocol(
                group,
                member_id,
                session_timeout_ms,
                "range",
                vec![0, 1, 2, 3],
            )
        }

        /// Send a JoinGroup with one protocol and return the slot its response lands in.
        fn join_protocol(
            &self,
            group: &str,
            member_id: &str,
            session_timeout_ms: i32,
            protocol: &str,
            metadata: Vec<u8>,
        ) -> ResponseSlot {
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
                        name: protocol.to_string(),
                        metadata,
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

    /// A Stable group of one member with `payload` bytes of metadata and of assignment.
    fn stable_group(f: &Fixture, group: &str, payload: usize) {
        let join = take(&f.join_protocol(group, "", 30_000, "range", vec![7; payload])).unwrap();
        let member = join.member_id.to_string();
        let synced = take_sync(&f.sync(
            group,
            &member,
            join.generation_id,
            vec![SyncGroupAssignment {
                member_id: member.clone(),
                assignment: vec![9; payload],
            }],
        ))
        .unwrap();
        assert_eq!(synced.error_code, ERROR_NONE);
    }

    /// A DescribeGroups naming a large group many times copied the group's member state
    /// into the response once per repeat (a 1.6 kB request repeating a group of 1 MB of
    /// member state was answered with 400 MB). Every entry is answered, but those described
    /// in full fit `pg_kafka.fetch_max_bytes` (55 MiB) and share one copy of the group's
    /// bytes; the rest are COORDINATOR_NOT_AVAILABLE.
    #[test]
    fn test_describe_groups_of_a_repeated_group_is_bounded() {
        const PAYLOAD: usize = 512 << 10;
        const REPEATS: usize = 80;
        let f = Fixture::new();
        stable_group(&f, "g", PAYLOAD);
        let response = handle_describe_groups(&f.ctx(), vec!["g".to_string(); REPEATS]).unwrap();
        assert_eq!(response.groups.len(), REPEATS, "every entry is answered");
        let described: Vec<_> = response
            .groups
            .iter()
            .filter(|g| g.error_code == ERROR_NONE)
            .collect();
        let refused = response
            .groups
            .iter()
            .filter(|g| g.error_code == crate::kafka::constants::ERROR_COORDINATOR_NOT_AVAILABLE)
            .count();
        assert_eq!(described.len() + refused, REPEATS);
        let payload: usize = described
            .iter()
            .flat_map(|g| &g.members)
            .map(|m| m.member_metadata.len() + m.member_assignment.len())
            .sum();
        assert!(
            payload <= crate::config::response_max_bytes(),
            "{} entries described in full: {payload} bytes of member state",
            described.len()
        );
        let first = described[0].members[0].member_metadata.as_ptr();
        assert!(
            described
                .iter()
                .all(|g| g.members[0].member_metadata.as_ptr() == first),
            "each repeat copied the member state"
        );
    }

    /// The bound covers distinct groups too, and the first held group is always described
    /// whatever its size; groups the coordinator does not hold (a small entry each) are
    /// always answered, and a group outside its Stable state reports no metadata or
    /// assignment, so they do not count.
    #[test]
    fn test_describe_groups_bounds_the_groups_described_in_full() {
        let f = Fixture::new();
        stable_group(&f, "a", 1000);
        stable_group(&f, "b", 1000);
        // One member joined, not yet synced: CompletingRebalance.
        take(&f.join_protocol("c", "", 30_000, "range", vec![7; 100_000])).unwrap();
        let names = ["a", "missing", "c", "b", "a"].map(String::from).to_vec();
        let codes = |max_bytes| -> Vec<i16> {
            describe_groups(&f.ctx(), names.clone(), max_bytes)
                .groups
                .iter()
                .map(|g| g.error_code)
                .collect()
        };
        let busy = crate::kafka::constants::ERROR_COORDINATOR_NOT_AVAILABLE;
        // Smaller than one group: only the first is described.
        assert_eq!(codes(100), [ERROR_NONE, ERROR_NONE, busy, busy, busy]);
        // Room for a, c (no payload) and b, but not a again.
        assert_eq!(
            codes(5000),
            [ERROR_NONE, ERROR_NONE, ERROR_NONE, ERROR_NONE, busy]
        );
        assert_eq!(codes(1 << 20), [ERROR_NONE; 5]);
        let refused = &describe_groups(&f.ctx(), names.clone(), 100).groups[3];
        assert_eq!(refused.group_id.as_str(), "b");
        assert_eq!(refused.members.len(), 0);
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

    /// Kafka 3.7 knows coordinator types 0 (group) and 1 (transaction) and answers any
    /// other INVALID_REQUEST.
    #[test]
    fn test_handle_find_coordinator_unknown_key_type() {
        let f = Fixture::new();
        assert_eq!(
            handle_find_coordinator(&f.ctx(), "txn", 1)
                .unwrap()
                .error_code,
            ERROR_NONE
        );
        for key_type in [2, 5, 7, -1] {
            let response = handle_find_coordinator(&f.ctx(), "g", key_type).unwrap();
            assert_eq!(
                response.error_code,
                crate::kafka::constants::ERROR_INVALID_REQUEST,
                "key_type {key_type}"
            );
            assert_eq!(response.node_id, -1);
        }
    }

    /// As Kafka 3.7's `GroupMetadata.summary`: outside Stable, DescribeGroups reports no
    /// protocol and no member metadata or assignment; Stable reports all three.
    #[test]
    fn test_describe_groups_reports_metadata_only_when_stable() {
        let f = Fixture::new();
        let join = f.join_now("g", "");
        let describe = || {
            handle_describe_groups(&f.ctx(), vec!["g".to_string()])
                .unwrap()
                .groups
                .remove(0)
        };
        let group = describe();
        assert_eq!(group.group_state.as_str(), "CompletingRebalance");
        assert_eq!(group.protocol_data.as_str(), "");
        assert!(group.members[0].member_metadata.is_empty());
        assert!(group.members[0].member_assignment.is_empty());
        let member = join.member_id.to_string();
        let assignment = SyncGroupAssignment {
            member_id: member.clone(),
            assignment: vec![9, 9],
        };
        let synced = f.sync("g", &member, join.generation_id, vec![assignment]);
        assert_eq!(take_sync(&synced).unwrap().error_code, ERROR_NONE);
        let group = describe();
        assert_eq!(group.group_state.as_str(), "Stable");
        assert_eq!(group.protocol_data.as_str(), "range");
        assert_eq!(group.members[0].member_metadata.as_ref(), [0, 1, 2, 3]);
        assert_eq!(group.members[0].member_assignment.as_ref(), [9, 9]);
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

    /// Kafka 3.7 answers JoinGroup errors with the request's member id (checked against a
    /// 3.7 broker): INVALID_SESSION_TIMEOUT, UNKNOWN_MEMBER_ID on a new or an existing
    /// group, and INCONSISTENT_GROUP_PROTOCOL of a known member; a new member's has none.
    #[test]
    fn test_handle_join_group_errors_echo_the_member_id() {
        let f = Fixture::new();
        let answer = |slot: &ResponseSlot| {
            let r = take(slot).expect("answered immediately");
            (r.error_code, r.generation_id, r.member_id.to_string())
        };
        let error = |code: i16, member: &str| (code, -1, member.to_string());
        assert_eq!(
            answer(&f.join("g", "zz", 100)),
            error(ERROR_INVALID_SESSION_TIMEOUT, "zz")
        );
        assert_eq!(
            answer(&f.join("g", "", 100)),
            error(ERROR_INVALID_SESSION_TIMEOUT, "")
        );
        assert_eq!(
            answer(&f.join("g", "zz", 30_000)),
            error(ERROR_UNKNOWN_MEMBER_ID, "zz")
        );
        let member = f.join_now("g", "").member_id.to_string();
        assert_eq!(
            answer(&f.join("g", "zz", 30_000)),
            error(ERROR_UNKNOWN_MEMBER_ID, "zz")
        );
        let join_as = |member: &str| {
            let slot: ResponseSlot = Arc::new(Mutex::new(None));
            let writer = slot.clone();
            handle_join_group(
                &f.ctx(),
                JoinGroupRequestData {
                    group_id: "g".to_string(),
                    member_id: member.to_string(),
                    client_id: "test-client".to_string(),
                    group_instance_id: None,
                    session_timeout_ms: 30_000,
                    rebalance_timeout_ms: 60_000,
                    protocol_type: "other".to_string(),
                    protocols: vec![JoinGroupProtocol {
                        name: "range".to_string(),
                        metadata: vec![],
                    }],
                },
                move |response| *writer.lock().unwrap() = Some(response),
            );
            answer(&slot)
        };
        assert_eq!(
            join_as(&member),
            error(ERROR_INCONSISTENT_GROUP_PROTOCOL, &member)
        );
        assert_eq!(join_as(""), error(ERROR_INCONSISTENT_GROUP_PROTOCOL, ""));
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
        assert_eq!(
            first.member_id.as_str(),
            "",
            "a held join's error has no member id"
        );
        assert!(take(&second).is_none());
        // The follower's rejoin completes the phase and answers the resend.
        let _ = f.join_now("g", &follower);
        assert_eq!(take(&second).unwrap().generation_id, gen + 1);
    }

    /// Kafka 3.7 answers a held JoinGroup whose member is removed while it waits
    /// UNKNOWN_MEMBER_ID with member id "" (`maybeInvokeJoinCallback` with
    /// `JoinGroupResult(UNKNOWN_MEMBER_ID, ...)`, checked against a 3.7 broker), not the
    /// request's member id that an error answered at once echoes.
    #[test]
    fn test_handle_join_group_held_join_of_a_removed_member_has_no_member_id() {
        let f = Fixture::new();
        let leader = f.join_now("g", "").member_id.to_string();
        let follower = f.join("g", "", 30_000);
        let gen = f.join_now("g", &leader).generation_id;
        assert_eq!(take(&follower).unwrap().error_code, ERROR_NONE);
        assert_eq!(
            take_sync(&f.sync("g", &leader, gen, vec![]))
                .unwrap()
                .error_code,
            ERROR_NONE
        );

        // The leader rejoins (held: the follower has not rejoined), then an admin
        // LeaveGroup v3+ removes it.
        let held = f.join("g", &leader, 30_000);
        assert!(take(&held).is_none(), "held until the follower rejoins");
        let leave = handle_leave_group(
            &f.ctx(),
            "g",
            LeavingMembers::Batch(vec![crate::kafka::messages::MemberIdentity {
                member_id: leader.clone(),
                group_instance_id: None,
            }]),
        )
        .unwrap();
        assert_eq!(leave.members[0].error_code, ERROR_NONE);
        let held = take(&held).expect("answered when its member was removed");
        assert_eq!(
            (held.error_code, held.generation_id, held.member_id.as_str()),
            (ERROR_UNKNOWN_MEMBER_ID, -1, "")
        );
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
                    crate::kafka::constants::ERROR_COORDINATOR_NOT_AVAILABLE
                );
            }
            other => panic!("unexpected response {other:?}"),
        }

        // A disconnected client is ignored.
        drop(rx);
        JoinGroupReply::new(9, 5, tx).send(JoinGroupResponse::default());
    }

    /// The server computes assignments (reading the subscribed topics' partition counts)
    /// only for the SyncGroup the coordinator takes assignments from: the leader's, at the
    /// current generation, in the sync phase. A leader's SyncGroup with empty assignments
    /// for a stale generation, or once the group is Stable, computes nothing: the
    /// computation runs on the single DB thread and its result would be ignored.
    #[test]
    fn test_sync_group_computes_assignments_only_in_the_sync_phase() {
        let mut f = Fixture::new();
        f.store.expect_get_topic_metadata().times(1).returning(|_| {
            Ok(vec![crate::kafka::storage::TopicMetadata {
                name: "t".to_string(),
                id: 1,
                partition_count: 4,
            }])
        });
        let metadata = crate::kafka::assignment::MemberSubscription::new(vec!["t".to_string()])
            .encode()
            .unwrap();
        let join = take(&f.join_protocol("g", "", 30_000, "roundrobin", metadata))
            .expect("a lone member's JoinGroup is answered at once");
        let member = join.member_id.to_string();
        let generation = join.generation_id;
        let empty = || {
            vec![SyncGroupAssignment {
                member_id: member.clone(),
                assignment: vec![],
            }]
        };

        let stale = f.sync("g", &member, generation - 1, empty());
        assert_eq!(
            take_sync(&stale).unwrap().error_code,
            crate::kafka::constants::ERROR_ILLEGAL_GENERATION
        );

        let synced = take_sync(&f.sync("g", &member, generation, empty())).unwrap();
        assert_eq!(synced.error_code, ERROR_NONE);
        let assignment =
            crate::kafka::assignment::MemberAssignment::parse(&synced.assignment).unwrap();
        assert_eq!(assignment.partitions("t"), vec![0, 1, 2, 3]);

        // Stable: the stored assignment, nothing computed.
        let again = take_sync(&f.sync("g", &member, generation, empty())).unwrap();
        assert_eq!(again.error_code, ERROR_NONE);
        assert_eq!(again.assignment, synced.assignment);
    }

    /// A Postgres ERROR while the leader's SyncGroup reads its subscribed topics (a lock
    /// timeout on kafka.topics) unwinds through the handler as a panic and drops the reply
    /// unsent: it answers the retriable COORDINATOR_NOT_AVAILABLE, not UNKNOWN_SERVER_ERROR
    /// (fatal to the Java consumer). The group stays in its sync phase, its leader and
    /// generation unchanged, so the leader's next SyncGroup computes the assignment.
    #[test]
    fn test_sync_group_unwinding_in_the_topic_read_is_retriable() {
        let mut f = Fixture::new();
        f.store
            .expect_get_topic_metadata()
            .times(1)
            .returning(|_| panic!("canceling statement due to lock timeout"));
        let metadata = crate::kafka::assignment::MemberSubscription::new(vec!["t".to_string()])
            .encode()
            .unwrap();
        let join = take(&f.join_protocol("g", "", 30_000, "range", metadata)).unwrap();
        let member = join.member_id.to_string();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let reply = SyncGroupReply::new(11, 3, tx);
            handle_sync_group(
                &f.ctx(),
                "g",
                &member,
                join.generation_id,
                vec![],
                move |response| reply.send(response),
            );
        }));
        assert!(
            unwound.is_err(),
            "the ERROR unwinds to the request's subtransaction"
        );
        match rx.try_recv().unwrap() {
            crate::kafka::KafkaResponse {
                correlation_id,
                body: crate::kafka::api::ResponseBody::SyncGroup(response),
                ..
            } => {
                assert_eq!(correlation_id, 11);
                assert_eq!(
                    response.error_code,
                    crate::kafka::constants::ERROR_COORDINATOR_NOT_AVAILABLE
                );
            }
            other => panic!("unexpected response {other:?}"),
        }
        let group = f.coordinator.get_group_state("g").unwrap();
        assert_eq!(
            group.state,
            crate::kafka::coordinator::GroupState::CompletingRebalance
        );
        assert_eq!(group.generation_id, join.generation_id);
    }

    /// A server-side assignment over the work limit is refused with POLICY_VIOLATION
    /// before anything is read or computed; the group stays in its sync phase.
    #[test]
    fn test_sync_group_refuses_an_assignment_over_the_work_limit() {
        let mut f = Fixture::new();
        f.store.expect_get_topic_metadata().times(0);
        let mut metadata = vec![0, 0];
        metadata.extend_from_slice(
            &i32::try_from(crate::kafka::limits::MAX_ASSIGNMENT_WORK + 1)
                .unwrap()
                .to_be_bytes(),
        );
        let join = take(&f.join_protocol("g", "", 30_000, "range", metadata)).unwrap();
        let member = join.member_id.to_string();
        let refused = f.sync("g", &member, join.generation_id, vec![]);
        assert_eq!(
            take_sync(&refused).unwrap().error_code,
            crate::kafka::constants::ERROR_POLICY_VIOLATION
        );
        assert_eq!(
            f.coordinator.get_group_state("g").unwrap().state,
            crate::kafka::coordinator::GroupState::CompletingRebalance
        );
    }

    /// A refused server-side assignment is not computed again: the leader's retries in the
    /// same generation (the group stays in its sync phase, possibly for as long as the
    /// client's rebalance timeout) are answered POLICY_VIOLATION without reading the topics
    /// or running the assignor, so pipelined retries cannot hold the DB thread.
    #[test]
    fn test_sync_group_retry_of_a_refused_assignment_computes_nothing() {
        let mut f = Fixture::new();
        // 21 topics of the most partitions a topic may have: 2,100,000 partitions, over
        // the work limit once the topics are read.
        let topics: Vec<String> = (0..21).map(|i| format!("t{i}")).collect();
        f.store
            .expect_get_topic_metadata()
            .times(1)
            .returning(|names| {
                Ok(names
                    .unwrap_or_default()
                    .iter()
                    .zip(1..)
                    .map(|(name, id)| crate::kafka::storage::TopicMetadata {
                        name: name.clone(),
                        id,
                        partition_count: crate::kafka::limits::HARD_MAX_PARTITIONS,
                    })
                    .collect())
            });
        let metadata = crate::kafka::assignment::MemberSubscription::new(topics)
            .encode()
            .unwrap();
        let join = take(&f.join_protocol("g", "", 30_000, "range", metadata)).unwrap();
        let member = join.member_id.to_string();
        for attempt in 0..3 {
            let refused = f.sync("g", &member, join.generation_id, vec![]);
            assert_eq!(
                take_sync(&refused).unwrap().error_code,
                crate::kafka::constants::ERROR_POLICY_VIOLATION,
                "attempt {attempt}"
            );
        }
        assert_eq!(
            f.coordinator.get_group_state("g").unwrap().state,
            crate::kafka::coordinator::GroupState::CompletingRebalance
        );
    }

    /// A computed assignment refused by the memory budget is not computed again either: the
    /// leader's retries in the generation (COORDINATOR_NOT_AVAILABLE, retriable, so clients
    /// retry it at once) are answered without reading the topics or running the assignor.
    /// Each computation here reads a topic of 100,000 partitions and encodes a 400 kB
    /// assignment that a 256 kB budget refuses.
    #[test]
    fn test_sync_group_retry_of_an_assignment_over_the_memory_budget_computes_nothing() {
        crate::kafka::limits::test_max_group_memory::set(256 << 10);
        let mut f = Fixture::new();
        f.store.expect_get_topic_metadata().times(1).returning(|_| {
            Ok(vec![crate::kafka::storage::TopicMetadata {
                name: "t".to_string(),
                id: 1,
                partition_count: crate::kafka::limits::HARD_MAX_PARTITIONS,
            }])
        });
        let metadata = crate::kafka::assignment::MemberSubscription::new(vec!["t".to_string()])
            .encode()
            .unwrap();
        let join = take(&f.join_protocol("g", "", 30_000, "range", metadata)).unwrap();
        let member = join.member_id.to_string();
        for attempt in 0..3 {
            let refused = f.sync("g", &member, join.generation_id, vec![]);
            assert_eq!(
                take_sync(&refused).unwrap().error_code,
                crate::kafka::constants::ERROR_COORDINATOR_NOT_AVAILABLE,
                "attempt {attempt}"
            );
        }
        assert_eq!(
            f.coordinator.get_group_state("g").unwrap().state,
            crate::kafka::coordinator::GroupState::CompletingRebalance
        );
    }

    /// A refusal by the memory budget holds only while the assignments would not fit: once
    /// another group frees what it held, the leader's next retry computes the assignment and
    /// the group goes Stable, instead of being refused until the rebalance timeout evicts
    /// the leader. Another group's member holds 400 kB of metadata against a 1 MiB budget, so
    /// the 400 kB assignment (held twice: as the assignment, and as the previous assignment
    /// the sticky assignor reads) is refused, and fits once that member leaves.
    #[test]
    fn test_sync_group_retry_computes_once_the_memory_budget_frees() {
        crate::kafka::limits::test_max_group_memory::set(1 << 20);
        let mut f = Fixture::new();
        f.store.expect_get_topic_metadata().times(2).returning(|_| {
            Ok(vec![crate::kafka::storage::TopicMetadata {
                name: "t".to_string(),
                id: 1,
                partition_count: crate::kafka::limits::HARD_MAX_PARTITIONS,
            }])
        });
        let other =
            take(&f.join_protocol("other", "", 30_000, "range", vec![1; 400 << 10])).unwrap();
        assert_eq!(other.error_code, ERROR_NONE);
        let metadata = crate::kafka::assignment::MemberSubscription::new(vec!["t".to_string()])
            .encode()
            .unwrap();
        let join = take(&f.join_protocol("g", "", 30_000, "range", metadata)).unwrap();
        let member = join.member_id.to_string();
        for attempt in 0..2 {
            let refused = f.sync("g", &member, join.generation_id, vec![]);
            assert_eq!(
                take_sync(&refused).unwrap().error_code,
                crate::kafka::constants::ERROR_COORDINATOR_NOT_AVAILABLE,
                "attempt {attempt}: the budget is still full"
            );
        }
        handle_leave_group(
            &f.ctx(),
            "other",
            LeavingMembers::Single(other.member_id.to_string()),
        )
        .unwrap();
        let synced = take_sync(&f.sync("g", &member, join.generation_id, vec![])).unwrap();
        assert_eq!(
            synced.error_code, ERROR_NONE,
            "computed once the budget freed"
        );
        assert!(synced.assignment.len() > 100_000);
        assert_eq!(
            f.coordinator.get_group_state("g").unwrap().state,
            crate::kafka::coordinator::GroupState::Stable
        );
    }

    /// A transient storage failure while computing the server-side assignment is answered
    /// COORDINATOR_NOT_AVAILABLE (retriable) to the leader and leaves the group in its sync
    /// phase, not remembered: the retry computes the assignment. It used to be taken as
    /// "nothing to assign", and the group went Stable with every member assigned nothing.
    #[test]
    fn test_sync_group_storage_failure_in_server_side_assignment_is_retriable() {
        let mut f = Fixture::new();
        let mut calls = 0;
        f.store
            .expect_get_topic_metadata()
            .times(2)
            .returning(move |_| {
                calls += 1;
                if calls == 1 {
                    return Err(KafkaError::database("connection lost"));
                }
                Ok(vec![crate::kafka::storage::TopicMetadata {
                    name: "t".to_string(),
                    id: 1,
                    partition_count: 3,
                }])
            });
        let metadata = crate::kafka::assignment::MemberSubscription::new(vec!["t".to_string()])
            .encode()
            .unwrap();
        let join = take(&f.join_protocol("g", "", 30_000, "range", metadata)).unwrap();
        let member = join.member_id.to_string();
        let failed = take_sync(&f.sync("g", &member, join.generation_id, vec![])).unwrap();
        assert_eq!(
            failed.error_code,
            crate::kafka::constants::ERROR_COORDINATOR_NOT_AVAILABLE
        );
        assert_eq!(
            f.coordinator.get_group_state("g").unwrap().state,
            crate::kafka::coordinator::GroupState::CompletingRebalance
        );
        let synced = take_sync(&f.sync("g", &member, join.generation_id, vec![])).unwrap();
        assert_eq!(synced.error_code, ERROR_NONE);
        let assignment =
            crate::kafka::assignment::MemberAssignment::parse(synced.assignment.as_ref()).unwrap();
        assert_eq!(assignment.partitions("t"), vec![0, 1, 2]);
    }

    /// DescribeGroups of a group that is not Stable copies none of its members' metadata or
    /// assignments (it reports them empty, as Kafka 3.7 does): describing a group in its sync
    /// phase whose member holds 512 kB of metadata allocates a few hundred bytes, where it
    /// used to copy the metadata into the entry and then discard it.
    #[test]
    fn test_describe_groups_copies_no_payload_of_a_group_that_is_not_stable() {
        let f = Fixture::new();
        let join = take(&f.join_protocol("g", "", 30_000, "range", vec![1; 512 << 10])).unwrap();
        assert_eq!(join.error_code, ERROR_NONE);
        let (response, allocated) = crate::testing::heap_allocated(|| {
            describe_groups(&f.ctx(), vec!["g".to_string()], usize::MAX)
        });
        let group = &response.groups[0];
        assert_eq!(group.group_state.as_str(), "CompletingRebalance");
        assert_eq!(group.members.len(), 1);
        assert!(group.members[0].member_metadata.is_empty());
        assert!(
            allocated < 16 << 10,
            "describing the group allocated {allocated} bytes"
        );
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
                    crate::kafka::constants::ERROR_COORDINATOR_NOT_AVAILABLE
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
        let unknown = handle_leave_group(
            &f.ctx(),
            "test-group",
            LeavingMembers::Single("unknown-member-id".to_string()),
        );
        assert!(matches!(unknown, Err(KafkaError::UnknownMemberId { .. })));
        let leave = handle_leave_group(
            &f.ctx(),
            "test-group",
            LeavingMembers::Single(member_id.clone()),
        )
        .unwrap();
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
            LeavingMembers::Batch(vec![
                crate::kafka::messages::MemberIdentity {
                    member_id: member_id.clone(),
                    group_instance_id: None,
                },
                crate::kafka::messages::MemberIdentity {
                    member_id: "unknown-member".to_string(),
                    group_instance_id: None,
                },
            ]),
        )
        .unwrap();
        assert_eq!(leave.error_code, ERROR_NONE);
        assert_eq!(leave.members.len(), 2);
        assert_eq!(leave.members[0].error_code, ERROR_NONE);
        assert_ne!(leave.members[1].error_code, ERROR_NONE);
        assert!(handle_heartbeat(&f.ctx(), "test-group", &member_id, 1).is_err());
    }

    #[test]
    fn test_handle_leave_group_v3_without_members_is_none() {
        // An empty v3+ batch is NONE with no member results, for a known group (whose
        // member stays) and an unknown one, as in Kafka 3.7; it is not a v0 leave of the
        // member "".
        let f = Fixture::new();
        let member_id = f.join_now("test-group", "").member_id.to_string();
        for group in ["test-group", "no-such-group"] {
            let leave = handle_leave_group(&f.ctx(), group, LeavingMembers::Batch(vec![]))
                .expect("answered");
            assert_eq!(
                (leave.error_code, leave.members.len()),
                (ERROR_NONE, 0),
                "{group}"
            );
        }
        assert!(handle_heartbeat(&f.ctx(), "test-group", &member_id, 1).is_ok());
    }

    #[test]
    fn test_handle_leave_group_releases_held_join() {
        let f = Fixture::new();
        let leader = f.join_now("test-group", "").member_id.to_string();
        let second = f.join("test-group", "", 30_000);
        assert!(take(&second).is_none());
        handle_leave_group(&f.ctx(), "test-group", LeavingMembers::Single(leader)).unwrap();
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
        store.expect_list_offset_groups().returning(|_| Ok(vec![]));
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
            .returning(move |_| Ok(groups.iter().map(ToString::to_string).collect()));
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
        let empty_only = handle_list_groups(&ctx, &["Empty".to_string()]).unwrap();
        assert_eq!(empty_only.groups.len(), 1);
        // The filter is case-sensitive, as in Kafka 3.7.
        for filter in ["empty", "EMPTY"] {
            let listed = handle_list_groups(&ctx, &[filter.to_string()]).unwrap();
            assert!(listed.groups.is_empty(), "{filter}");
        }
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
                .returning(move |_| Ok(groups.clone()));
            let env = TestEnv::new();
            let filter: Vec<String> = (0..n)
                .map(|i| format!("NotAState{i}"))
                .chain(["Empty".to_string()])
                .collect();
            let listed = handle_list_groups(&env.ctx(&store), &filter).unwrap();
            assert_eq!(listed.groups.len(), n);
        });
        assert!(
            growth < 8.0,
            "4x the groups and filter took {growth:.1}x the time"
        );
    }

    /// ListGroups is bounded by `max_bytes` whatever the group ids weigh: 40 held groups and
    /// 40 groups known by their offsets, each with a 30 KB id (2.4 MB of ids), are listed up
    /// to a 256 kB bound, in the order of their ids, the first always; the store is asked
    /// for the offset-only ids within the same bound. The list was the whole 2.4 MB.
    #[test]
    fn test_list_groups_is_bounded_by_max_bytes() {
        let mut f = Fixture::new();
        let id = |c: char, i: usize| format!("{c}{i:03}{}", "x".repeat(30 * 1024));
        let offset_only: Vec<String> = (0..40).map(|i| id('o', i)).collect();
        let max_bytes = 256 << 10;
        f.store.checkpoint();
        f.store
            .expect_list_offset_groups()
            .withf(move |&bytes| bytes == max_bytes || bytes == 1024)
            .returning(move |max| {
                // As the store does: the ids within `max`, and the first past it.
                let mut total = 0;
                Ok(offset_only
                    .iter()
                    .take_while(|g| {
                        let within = total <= max;
                        total += g.len() + crate::kafka::storage::LISTED_GROUP_OVERHEAD;
                        within
                    })
                    .cloned()
                    .collect())
            });
        for i in 0..40 {
            assert_eq!(f.join_now(&id('h', i), "").error_code, ERROR_NONE);
        }
        let (listed, allocated) =
            crate::testing::heap_allocated(|| list_groups(&f.ctx(), &[], max_bytes).unwrap());
        let ids: Vec<&str> = listed.groups.iter().map(|g| g.group_id.as_str()).collect();
        let bytes: usize = ids.iter().map(|g| g.len()).sum();
        assert!(
            !ids.is_empty() && bytes <= max_bytes,
            "{bytes} bytes listed"
        );
        assert!(ids.len() >= 7, "listed {} groups", ids.len());
        assert!(ids.windows(2).all(|w| w[0] < w[1]), "in id order");
        assert_eq!(ids[0], id('h', 0), "the first id is listed");
        assert!(allocated < 3 * max_bytes, "allocated {allocated} bytes");

        // One group larger than the bound is still listed.
        let one = list_groups(&f.ctx(), &[], 1024).unwrap();
        assert_eq!(one.groups.len(), 1);
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

    /// A LeaveGroup v3+ batch costs about its request, not the group id once per entry:
    /// 500 entries naming a 30 KB group id (a held group, then one the coordinator does not
    /// hold) allocate a few copies of the id plus a few hundred bytes per entry, log lines
    /// and errors included (unit tests format every log line). Each entry used to format the
    /// group id into a DEBUG line, for a held group also into a LOG line, and copy it into
    /// an UNKNOWN_MEMBER_ID error: 45 MB for this request of 34 KB, 3.3 GB of server log for
    /// one of 430 KB.
    #[test]
    fn test_leave_group_batch_does_not_copy_the_group_id_per_entry() {
        use crate::kafka::messages::MemberIdentity;
        let f = Fixture::new();
        let held = "g".repeat(30 * 1024);
        let member = f.join_now(&held, "").member_id.to_string();
        let entries = 500;
        for (group_id, known) in [(held.clone(), Some(member)), ("h".repeat(30 * 1024), None)] {
            let members: Vec<MemberIdentity> = (0..entries)
                .map(|i| MemberIdentity {
                    member_id: match &known {
                        Some(m) if i == entries / 2 => m.clone(),
                        _ => format!("m{i}"),
                    },
                    group_instance_id: None,
                })
                .collect();
            let (leave, allocated) = crate::testing::heap_allocated(|| {
                handle_leave_group(&f.ctx(), &group_id, LeavingMembers::Batch(members))
            });
            let leave = leave.unwrap();
            let codes: Vec<i16> = leave.members.iter().map(|m| m.error_code).collect();
            assert_eq!(codes.len(), entries);
            for (i, &code) in codes.iter().enumerate() {
                let removed = known.is_some() && i == entries / 2;
                let expected = if removed {
                    ERROR_NONE
                } else {
                    ERROR_UNKNOWN_MEMBER_ID
                };
                assert_eq!(code, expected, "entry {i}");
            }
            let bound = 4 * group_id.len() + entries * 256;
            assert!(
                allocated <= bound,
                "a {entries}-entry LeaveGroup of a {} byte group id (held: {}) allocated {allocated} bytes, over {bound}",
                group_id.len(),
                known.is_some()
            );
        }
        assert_eq!(f.coordinator.group_is_empty(&held), Some(true));
    }
}
