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

use crate::kafka::constants::ERROR_INVALID_GROUP_ID;
use crate::kafka::handlers;
use crate::kafka::messages::{
    JoinGroupProtocol, LeavingMembers, MemberIdentity, SyncGroupAssignment,
};
use crate::kafka::response_builders as rb;

/// Refuse an empty group id with INVALID_GROUP_ID, answered without touching any group.
///
/// Kafka 3.7 (`GroupCoordinator.isValidGroupId`) accepts `""` only on OffsetCommit,
/// OffsetFetch, DescribeGroups and DeleteGroups; JoinGroup, SyncGroup, Heartbeat, LeaveGroup
/// and TxnOffsetCommit need a real group (TxnOffsetCommit checks it in its handler, which
/// also tells unknown partitions apart). FindCoordinator and AddOffsetsToTxn take `""`.
fn require_group_id<E>(group_id: &GroupId, echo: impl FnOnce() -> E) -> Result<(), Rejection<E>> {
    if group_id.is_empty() {
        return Err(Rejection::Reply {
            error_code: ERROR_INVALID_GROUP_ID,
            echo: Some(echo()),
        });
    }
    Ok(())
}

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
            handlers::handle_find_coordinator(&env.ctx, &r.key, r.key_type)
        })
    }
}

/// JoinGroup: join (or rejoin) a consumer group.
pub struct JoinGroupApi;

impl ApiSchema for JoinGroupApi {
    type Wire = JoinGroupRequest;

    /// The member id an error response carries. Kafka 3.7 echoes the request's on the
    /// coordinator's errors (INVALID_GROUP_ID here, refused before the handler; the
    /// handler's own refusals, such as INVALID_SESSION_TIMEOUT and UNKNOWN_MEMBER_ID,
    /// carry it themselves) and answers `""` on a request that fails otherwise
    /// (JoinGroupRequest.getErrorResponse), so only the empty group id refusal carries one.
    type Echo = String;
    const NAME: &'static str = "JoinGroup";
    const MIN_VERSION: i16 = 0;
    const MAX_VERSION: i16 = 7;

    fn error_response(echo: Option<&String>, version: i16, error_code: i16) -> JoinGroupResponse {
        let mut response = rb::build_join_group_error_response(error_code);
        if let Some(member_id) = echo {
            response.member_id = member_id.clone().into();
        }
        handlers::normalize_join_group_response(response, version)
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
    ) -> Result<Self::Request, Rejection<String>> {
        require_group_id(&wire.group_id, || wire.member_id.to_string())?;
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

    fn echo(_request: &Self::Request) -> String {
        String::new()
    }

    /// The response may come from a later JoinGroup, a LeaveGroup or the timeout sweep, so
    /// it answers on the client's own sender through a `JoinGroupReply` (which answers
    /// COORDINATOR_NOT_AVAILABLE if dropped unsent). JoinGroup writes nothing to the database.
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
        require_group_id(&wire.group_id, || ())?;
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
            handlers::handle_heartbeat(&env.ctx, &r.group_id, &r.member_id, r.generation_id)
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
    /// The leaving members: the member id (v0-2) or the `members` batch (v3+).
    pub leaving: LeavingMembers,
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
        header: &RequestHeader,
    ) -> Result<Self::Request, Rejection<()>> {
        require_group_id(&wire.group_id, || ())?;
        // Kafka 3.7 (KafkaApis.handleLeaveGroupRequest) reads the member id below v3 and
        // the members array from v3 on, even an empty one.
        let leaving = if header.api_version < 3 {
            LeavingMembers::Single(wire.member_id.to_string())
        } else {
            LeavingMembers::Batch(wire.members.into_iter().map(MemberIdentity::from).collect())
        };
        Ok(LeaveGroupRequestData {
            group_id: wire.group_id.to_string(),
            leaving,
        })
    }

    fn echo(_request: &Self::Request) {}

    fn handle(request: Self::Request, call: Call<Self>, env: &ApiEnv<'_>) -> RequestOutcome {
        call.run(request, |r, _| {
            handlers::handle_leave_group(&env.ctx, &r.group_id, r.leaving)
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
        require_group_id(&wire.group_id, || ())?;
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
            &request.group_id,
            &request.member_id,
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
            handlers::handle_list_groups(&env.ctx, &r.states_filter)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn header(api_key: i16, api_version: i16) -> RequestHeader {
        RequestHeader {
            api_key,
            api_version,
            correlation_id: 1,
            client_id: None,
        }
    }

    /// The code a decode refused with (`None` when it decoded).
    fn refused<R, E>(decoded: Result<R, Rejection<E>>) -> Option<(i16, Option<E>)> {
        match decoded {
            Ok(_) => None,
            Err(Rejection::Reply { error_code, echo }) => Some((error_code, echo)),
            Err(Rejection::Silent | Rejection::Close(_)) => panic!("refused without a reply"),
        }
    }

    /// Kafka 3.7 answers JoinGroup, SyncGroup, Heartbeat and LeaveGroup for the empty group
    /// id with INVALID_GROUP_ID; JoinGroup echoes the request's member id.
    #[test]
    fn empty_group_id_is_invalid_group_id() {
        let join = |group: &str| {
            let mut wire = JoinGroupRequest::default();
            wire.group_id = GroupId(group.to_string().into());
            wire.member_id = "zz".to_string().into();
            refused(JoinGroupApi::decode(wire, &header(11, 5)))
        };
        let (code, echo) = join("").unwrap();
        assert_eq!(code, ERROR_INVALID_GROUP_ID);
        let response = JoinGroupApi::error_response(echo.as_ref(), 5, code);
        assert_eq!(
            (
                response.error_code,
                response.generation_id,
                response.member_id.as_str()
            ),
            (ERROR_INVALID_GROUP_ID, -1, "zz")
        );
        // protocol_name is nullable only from v7; Kafka sends "" before.
        assert_eq!(response.protocol_name.as_deref(), Some(""));
        let v7 = JoinGroupApi::error_response(None, 7, ERROR_INVALID_GROUP_ID);
        assert_eq!(v7.protocol_name, None);
        assert!(join("g").is_none());
        // Every other JoinGroup error answers with no member id, as Kafka's
        // JoinGroupRequest.getErrorResponse does.
        let echo = JoinGroupApi::echo(&JoinGroupRequestData {
            group_id: "g".to_string(),
            member_id: "zz".to_string(),
            client_id: "c".to_string(),
            group_instance_id: None,
            session_timeout_ms: 10_000,
            rebalance_timeout_ms: 10_000,
            protocol_type: "consumer".to_string(),
            protocols: vec![],
        });
        assert_eq!(
            JoinGroupApi::error_response(Some(&echo), 5, 35)
                .member_id
                .as_str(),
            ""
        );

        let heartbeat = |group: &str| {
            let mut wire = HeartbeatRequest::default();
            wire.group_id = GroupId(group.to_string().into());
            refused(HeartbeatApi::decode(wire, &header(12, 4))).map(|(code, _)| code)
        };
        let sync = |group: &str| {
            let mut wire = SyncGroupRequest::default();
            wire.group_id = GroupId(group.to_string().into());
            refused(SyncGroupApi::decode(wire, &header(14, 4))).map(|(code, _)| code)
        };
        let leave = |group: &str| {
            let mut wire = LeaveGroupRequest::default();
            wire.group_id = GroupId(group.to_string().into());
            refused(LeaveGroupApi::decode(wire, &header(13, 4))).map(|(code, _)| code)
        };
        for decoded in [heartbeat, sync, leave] {
            assert_eq!(decoded(""), Some(ERROR_INVALID_GROUP_ID));
            assert_eq!(decoded("g"), None);
        }
        // LeaveGroup v3+ answers the code at the top level, with no member results.
        let response = LeaveGroupApi::error_response(None, 4, ERROR_INVALID_GROUP_ID);
        assert_eq!(response.error_code, ERROR_INVALID_GROUP_ID);
        assert_eq!(response.members.len(), 0);
    }

    /// The request version, not whether `members` is empty, frames LeaveGroup: v0-2 leave
    /// the top-level member id, v3+ the `members` batch, an empty one included.
    #[test]
    fn leave_group_framing_follows_the_version() {
        let decoded = |version: i16| {
            let mut wire = LeaveGroupRequest::default();
            wire.group_id = GroupId("g".to_string().into());
            wire.member_id = "m".to_string().into();
            let Ok(request) = LeaveGroupApi::decode(wire, &header(13, version)) else {
                panic!("v{version} decodes");
            };
            request.leaving
        };
        for version in 0..=2 {
            assert!(
                matches!(decoded(version), LeavingMembers::Single(ref m) if m == "m"),
                "v{version}"
            );
        }
        for version in 3..=4 {
            assert!(
                matches!(decoded(version), LeavingMembers::Batch(ref ms) if ms.is_empty()),
                "v{version}"
            );
        }
    }

    /// DescribeGroups, DeleteGroups and FindCoordinator decode the empty group id, as Kafka 3.7
    /// serves it there (OffsetCommit and OffsetFetch have no group id check at decode).
    #[test]
    fn empty_group_id_is_accepted_where_kafka_accepts_it() {
        let mut describe = DescribeGroupsRequest::default();
        describe.groups = vec![GroupId(String::new().into())];
        assert!(refused(DescribeGroupsApi::decode(describe, &header(15, 5))).is_none());
        let mut delete = DeleteGroupsRequest::default();
        delete.groups_names = vec![GroupId(String::new().into())];
        assert!(refused(DeleteGroupsApi::decode(delete, &header(42, 2))).is_none());
        let mut find = FindCoordinatorRequest::default();
        find.key = String::new().into();
        assert!(refused(FindCoordinatorApi::decode(find, &header(10, 3))).is_none());
    }
}
