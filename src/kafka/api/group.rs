//! Consumer group APIs: FindCoordinator (10), JoinGroup (11), Heartbeat (12),
//! LeaveGroup (13), SyncGroup (14), DescribeGroups (15), ListGroups (16), DeleteGroups (42).

use kafka_protocol::messages::delete_groups_request::DeleteGroupsRequest;
use kafka_protocol::messages::delete_groups_response::{
    DeletableGroupResult, DeleteGroupsResponse,
};
use kafka_protocol::messages::describe_groups_request::DescribeGroupsRequest;
use kafka_protocol::messages::describe_groups_response::{DescribeGroupsResponse, DescribedGroup};
use kafka_protocol::messages::find_coordinator_request::FindCoordinatorRequest;
use kafka_protocol::messages::find_coordinator_response::FindCoordinatorResponse;
use kafka_protocol::messages::heartbeat_request::HeartbeatRequest;
use kafka_protocol::messages::heartbeat_response::HeartbeatResponse;
use kafka_protocol::messages::join_group_request::JoinGroupRequest;
use kafka_protocol::messages::join_group_response::JoinGroupResponse;
use kafka_protocol::messages::leave_group_request::LeaveGroupRequest;
use kafka_protocol::messages::leave_group_response::LeaveGroupResponse;
use kafka_protocol::messages::list_groups_request::ListGroupsRequest;
use kafka_protocol::messages::list_groups_response::ListGroupsResponse;
use kafka_protocol::messages::sync_group_request::SyncGroupRequest;
use kafka_protocol::messages::sync_group_response::SyncGroupResponse;
use kafka_protocol::messages::GroupId;

use super::{ApiEnv, ApiSchema, Call, KafkaApi, Lane, Rejection, RequestHeader};
use crate::kafka::dispatch::RequestOutcome;
pub use crate::kafka::handlers::JoinGroupRequestData;

use crate::kafka::handlers;
use crate::kafka::messages::{JoinGroupProtocol, MemberIdentity, SyncGroupAssignment};
use crate::kafka::response_builders as rb;

/// FindCoordinator: which broker coordinates a group or transactional id.
pub struct FindCoordinatorApi;

/// FindCoordinator request.
#[derive(Debug, Clone)]
pub struct FindCoordinatorRequestData {
    /// Group id or transactional id.
    pub key: String,
    /// 0 = consumer group, 1 = transaction coordinator.
    pub key_type: i8,
}

impl ApiSchema for FindCoordinatorApi {
    type Wire = FindCoordinatorRequest;
    type Echo = ();
    const NAME: &'static str = "FindCoordinator";
    const MIN_VERSION: i16 = 0;
    const MAX_VERSION: i16 = 3;

    fn error_response(
        _echo: Option<&()>,
        _version: i16,
        error_code: i16,
    ) -> FindCoordinatorResponse {
        rb::build_find_coordinator_error_response(error_code)
    }

    fn response_has_error(response: &FindCoordinatorResponse) -> bool {
        response.error_code != 0 || response.coordinators.iter().any(|c| c.error_code != 0)
    }
}

impl KafkaApi for FindCoordinatorApi {
    type Request = FindCoordinatorRequestData;

    fn decode(
        wire: FindCoordinatorRequest,
        _header: &RequestHeader,
    ) -> Result<Self::Request, Rejection<()>> {
        Ok(FindCoordinatorRequestData {
            key: wire.key.to_string(),
            key_type: wire.key_type,
        })
    }

    fn echo(_request: &Self::Request) {}

    fn handle(request: Self::Request, call: Call<Self>, env: &ApiEnv<'_>) -> RequestOutcome {
        call.run(request, |r, _| {
            handlers::handle_find_coordinator(&env.ctx, r.key, r.key_type)
        })
    }
}

/// JoinGroup: join (or rejoin) a consumer group.
pub struct JoinGroupApi;

impl ApiSchema for JoinGroupApi {
    type Wire = JoinGroupRequest;
    type Echo = ();
    const NAME: &'static str = "JoinGroup";
    const MIN_VERSION: i16 = 0;
    const MAX_VERSION: i16 = 7;

    fn error_response(_echo: Option<&()>, _version: i16, error_code: i16) -> JoinGroupResponse {
        rb::build_join_group_error_response(error_code)
    }

    fn response_has_error(response: &JoinGroupResponse) -> bool {
        response.error_code != 0
    }
}

impl KafkaApi for JoinGroupApi {
    type Request = JoinGroupRequestData;

    // The coordinator holds the response until the group's join phase completes.
    const DEFERRED_REPLY: bool = true;

    fn decode(
        wire: JoinGroupRequest,
        header: &RequestHeader,
    ) -> Result<Self::Request, Rejection<()>> {
        Ok(JoinGroupRequestData {
            group_id: wire.group_id.to_string(),
            member_id: wire.member_id.to_string(),
            client_id: header
                .client_id
                .clone()
                .unwrap_or_else(|| "unknown".to_string()),
            group_instance_id: wire.group_instance_id.map(|s| s.to_string()),
            session_timeout_ms: wire.session_timeout_ms,
            rebalance_timeout_ms: wire.rebalance_timeout_ms,
            protocol_type: wire.protocol_type.to_string(),
            protocols: wire
                .protocols
                .into_iter()
                .map(JoinGroupProtocol::from)
                .collect(),
        })
    }

    fn echo(_request: &Self::Request) {}

    /// The response may come from a later JoinGroup, a LeaveGroup or the timeout sweep, so
    /// it answers on the client's own sender through a `JoinGroupReply` (which answers
    /// UNKNOWN_SERVER_ERROR if dropped unsent). JoinGroup writes nothing to the database.
    fn handle(request: Self::Request, call: Call<Self>, env: &ApiEnv<'_>) -> RequestOutcome {
        let (header, response_tx) = call.into_parts();
        let reply =
            handlers::JoinGroupReply::new(header.correlation_id, header.api_version, response_tx);
        handlers::handle_join_group(&env.ctx, request, move |response| reply.send(response));
        RequestOutcome::Commit
    }
}

/// Heartbeat: keep a group membership alive.
pub struct HeartbeatApi;

/// Heartbeat request.
#[derive(Debug, Clone)]
pub struct HeartbeatRequestData {
    /// Consumer group id.
    pub group_id: String,
    /// Generation from JoinGroup.
    pub generation_id: i32,
    /// Member id from JoinGroup.
    pub member_id: String,
    /// Static group instance id (v3+).
    pub group_instance_id: Option<String>,
}

impl ApiSchema for HeartbeatApi {
    type Wire = HeartbeatRequest;
    type Echo = ();
    const NAME: &'static str = "Heartbeat";
    const MIN_VERSION: i16 = 0;
    const MAX_VERSION: i16 = 4;

    fn error_response(_echo: Option<&()>, _version: i16, error_code: i16) -> HeartbeatResponse {
        rb::build_heartbeat_error_response(error_code)
    }

    fn response_has_error(response: &HeartbeatResponse) -> bool {
        response.error_code != 0
    }
}

impl KafkaApi for HeartbeatApi {
    type Request = HeartbeatRequestData;

    // Served from the in-memory coordinator; never queued behind heavy work, so a slow
    // Produce cannot make a member miss its session timeout.
    const LANE: Lane = Lane::Liveness;

    fn decode(
        wire: HeartbeatRequest,
        _header: &RequestHeader,
    ) -> Result<Self::Request, Rejection<()>> {
        Ok(HeartbeatRequestData {
            group_id: wire.group_id.to_string(),
            generation_id: wire.generation_id,
            member_id: wire.member_id.to_string(),
            group_instance_id: wire.group_instance_id.map(|s| s.to_string()),
        })
    }

    fn echo(_request: &Self::Request) {}

    fn handle(request: Self::Request, call: Call<Self>, env: &ApiEnv<'_>) -> RequestOutcome {
        call.run(request, |r, _| {
            handlers::handle_heartbeat(&env.ctx, r.group_id, r.member_id, r.generation_id)
        })
    }
}

/// LeaveGroup: leave a consumer group.
pub struct LeaveGroupApi;

/// LeaveGroup request.
#[derive(Debug, Clone)]
pub struct LeaveGroupRequestData {
    /// Consumer group id.
    pub group_id: String,
    /// Leaving member (v0-2).
    pub member_id: String,
    /// Leaving members (v3+ batch).
    pub members: Vec<MemberIdentity>,
}

impl ApiSchema for LeaveGroupApi {
    type Wire = LeaveGroupRequest;
    type Echo = ();
    const NAME: &'static str = "LeaveGroup";
    const MIN_VERSION: i16 = 0;
    const MAX_VERSION: i16 = 4;

    fn error_response(_echo: Option<&()>, _version: i16, error_code: i16) -> LeaveGroupResponse {
        rb::build_leave_group_error_response(error_code)
    }

    fn response_has_error(response: &LeaveGroupResponse) -> bool {
        response.error_code != 0 || response.members.iter().any(|m| m.error_code != 0)
    }
}

impl KafkaApi for LeaveGroupApi {
    type Request = LeaveGroupRequestData;

    fn decode(
        wire: LeaveGroupRequest,
        _header: &RequestHeader,
    ) -> Result<Self::Request, Rejection<()>> {
        Ok(LeaveGroupRequestData {
            group_id: wire.group_id.to_string(),
            member_id: wire.member_id.to_string(),
            members: wire.members.into_iter().map(MemberIdentity::from).collect(),
        })
    }

    fn echo(_request: &Self::Request) {}

    fn handle(request: Self::Request, call: Call<Self>, env: &ApiEnv<'_>) -> RequestOutcome {
        call.run(request, |r, _| {
            handlers::handle_leave_group(&env.ctx, r.group_id, r.member_id, r.members)
        })
    }
}

/// SyncGroup: distribute (leader) or receive (follower) the partition assignment.
pub struct SyncGroupApi;

/// SyncGroup request.
#[derive(Debug, Clone)]
pub struct SyncGroupRequestData {
    /// Consumer group id.
    pub group_id: String,
    /// Generation from JoinGroup.
    pub generation_id: i32,
    /// Member id from JoinGroup.
    pub member_id: String,
    /// Static group instance id (v3+).
    pub group_instance_id: Option<String>,
    /// Protocol type (v5+).
    pub protocol_type: Option<String>,
    /// Protocol name (v5+).
    pub protocol_name: Option<String>,
    /// Assignments (only the leader sends any).
    pub assignments: Vec<SyncGroupAssignment>,
}

impl ApiSchema for SyncGroupApi {
    type Wire = SyncGroupRequest;
    type Echo = ();
    const NAME: &'static str = "SyncGroup";
    const MIN_VERSION: i16 = 0;
    const MAX_VERSION: i16 = 4;

    fn error_response(_echo: Option<&()>, _version: i16, error_code: i16) -> SyncGroupResponse {
        rb::build_sync_group_error_response(error_code)
    }

    fn response_has_error(response: &SyncGroupResponse) -> bool {
        response.error_code != 0
    }
}

impl KafkaApi for SyncGroupApi {
    type Request = SyncGroupRequestData;

    // A follower's SyncGroup is held until the leader's delivers the assignment.
    const DEFERRED_REPLY: bool = true;

    fn decode(
        wire: SyncGroupRequest,
        _header: &RequestHeader,
    ) -> Result<Self::Request, Rejection<()>> {
        Ok(SyncGroupRequestData {
            group_id: wire.group_id.to_string(),
            generation_id: wire.generation_id,
            member_id: wire.member_id.to_string(),
            group_instance_id: wire.group_instance_id.map(|s| s.to_string()),
            protocol_type: wire.protocol_type.map(|s| s.to_string()),
            protocol_name: wire.protocol_name.map(|s| s.to_string()),
            assignments: wire
                .assignments
                .into_iter()
                .map(SyncGroupAssignment::from)
                .collect(),
        })
    }

    fn echo(_request: &Self::Request) {}

    /// Like JoinGroup, answers on the client's own sender (`SyncGroupReply`); SyncGroup
    /// only reads and writes nothing.
    fn handle(request: Self::Request, call: Call<Self>, env: &ApiEnv<'_>) -> RequestOutcome {
        let (header, response_tx) = call.into_parts();
        let reply =
            handlers::SyncGroupReply::new(header.correlation_id, header.api_version, response_tx);
        handlers::handle_sync_group(
            &env.ctx,
            request.group_id,
            request.member_id,
            request.generation_id,
            request.assignments,
            move |response| reply.send(response),
        );
        RequestOutcome::Commit
    }
}

/// DescribeGroups: state and members of groups.
pub struct DescribeGroupsApi;

/// DescribeGroups request.
#[derive(Debug, Clone)]
pub struct DescribeGroupsRequestData {
    /// Group ids to describe.
    pub groups: Vec<String>,
    /// Include authorized operations (v3+).
    pub include_authorized_operations: bool,
}

impl ApiSchema for DescribeGroupsApi {
    type Wire = DescribeGroupsRequest;

    /// The requested group ids.
    type Echo = Vec<String>;
    const NAME: &'static str = "DescribeGroups";
    const MIN_VERSION: i16 = 0;
    const MAX_VERSION: i16 = 5;

    /// Errors are per group: report the code on every requested group.
    fn error_response(
        echo: Option<&Vec<String>>,
        _version: i16,
        error_code: i16,
    ) -> DescribeGroupsResponse {
        let mut response = DescribeGroupsResponse::default();
        response.throttle_time_ms = 0;
        response.groups = echo
            .into_iter()
            .flatten()
            .map(|group_id| {
                let mut g = DescribedGroup::default();
                g.group_id = GroupId(group_id.clone().into());
                g.error_code = error_code;
                g
            })
            .collect();
        response
    }

    fn response_has_error(response: &DescribeGroupsResponse) -> bool {
        response.groups.iter().any(|g| g.error_code != 0)
    }
}

impl KafkaApi for DescribeGroupsApi {
    type Request = DescribeGroupsRequestData;

    fn decode(
        wire: DescribeGroupsRequest,
        _header: &RequestHeader,
    ) -> Result<Self::Request, Rejection<Self::Echo>> {
        Ok(DescribeGroupsRequestData {
            groups: wire.groups.into_iter().map(|g| g.to_string()).collect(),
            include_authorized_operations: wire.include_authorized_operations,
        })
    }

    fn echo(request: &Self::Request) -> Vec<String> {
        request.groups.clone()
    }

    fn handle(request: Self::Request, call: Call<Self>, env: &ApiEnv<'_>) -> RequestOutcome {
        call.run(request, |r, _| {
            handlers::handle_describe_groups(&env.ctx, r.groups)
        })
    }
}

/// ListGroups: every group the coordinator knows.
pub struct ListGroupsApi;

/// ListGroups request.
#[derive(Debug, Clone)]
pub struct ListGroupsRequestData {
    /// States filter (v4+); empty = all states.
    pub states_filter: Vec<String>,
}

impl ApiSchema for ListGroupsApi {
    type Wire = ListGroupsRequest;
    type Echo = ();
    const NAME: &'static str = "ListGroups";
    const MIN_VERSION: i16 = 0;
    const MAX_VERSION: i16 = 4;

    fn error_response(_echo: Option<&()>, _version: i16, error_code: i16) -> ListGroupsResponse {
        rb::build_list_groups_error_response(error_code)
    }

    fn response_has_error(response: &ListGroupsResponse) -> bool {
        response.error_code != 0
    }
}

impl KafkaApi for ListGroupsApi {
    type Request = ListGroupsRequestData;

    fn decode(
        wire: ListGroupsRequest,
        _header: &RequestHeader,
    ) -> Result<Self::Request, Rejection<()>> {
        Ok(ListGroupsRequestData {
            states_filter: wire
                .states_filter
                .into_iter()
                .map(|s| s.to_string())
                .collect(),
        })
    }

    fn echo(_request: &Self::Request) {}

    fn handle(request: Self::Request, call: Call<Self>, env: &ApiEnv<'_>) -> RequestOutcome {
        call.run(request, |r, _| {
            handlers::handle_list_groups(&env.ctx, r.states_filter)
        })
    }
}

/// DeleteGroups: delete groups and their committed offsets.
pub struct DeleteGroupsApi;

/// DeleteGroups request.
#[derive(Debug, Clone)]
pub struct DeleteGroupsRequestData {
    /// Group ids to delete.
    pub groups_names: Vec<String>,
}

impl ApiSchema for DeleteGroupsApi {
    type Wire = DeleteGroupsRequest;

    /// The requested group ids.
    type Echo = Vec<String>;
    const NAME: &'static str = "DeleteGroups";
    const MIN_VERSION: i16 = 0;
    const MAX_VERSION: i16 = 2;

    /// Errors are per group: report the code on every requested group.
    fn error_response(
        echo: Option<&Vec<String>>,
        _version: i16,
        error_code: i16,
    ) -> DeleteGroupsResponse {
        let mut response = DeleteGroupsResponse::default();
        response.throttle_time_ms = 0;
        response.results = echo
            .into_iter()
            .flatten()
            .map(|group_id| {
                let mut r = DeletableGroupResult::default();
                r.group_id = GroupId(group_id.clone().into());
                r.error_code = error_code;
                r
            })
            .collect();
        response
    }

    fn response_has_error(response: &DeleteGroupsResponse) -> bool {
        response.results.iter().any(|r| r.error_code != 0)
    }
}

impl KafkaApi for DeleteGroupsApi {
    type Request = DeleteGroupsRequestData;

    fn decode(
        wire: DeleteGroupsRequest,
        _header: &RequestHeader,
    ) -> Result<Self::Request, Rejection<Self::Echo>> {
        Ok(DeleteGroupsRequestData {
            groups_names: wire
                .groups_names
                .into_iter()
                .map(|g| g.to_string())
                .collect(),
        })
    }

    fn echo(request: &Self::Request) -> Vec<String> {
        request.groups_names.clone()
    }

    fn handle(request: Self::Request, call: Call<Self>, env: &ApiEnv<'_>) -> RequestOutcome {
        call.run(request, |r, _| {
            handlers::handle_delete_groups(&env.ctx, r.groups_names)
        })
    }
}
