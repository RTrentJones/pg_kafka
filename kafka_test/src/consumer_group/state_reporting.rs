//! DescribeGroups, ListGroups and FindCoordinator answers checked against Kafka 3.7.

use crate::common::{DynResult, TestResult};
use crate::error_codes;
use crate::setup::TestContext;
use crate::wire::requests::{conn, join_now, join_req, leave, s, sync_leader};
use crate::wire::roundtrip;
use kafka_protocol::messages::describe_groups_request::DescribeGroupsRequest;
use kafka_protocol::messages::find_coordinator_request::FindCoordinatorRequest;
use kafka_protocol::messages::list_groups_request::ListGroupsRequest;
use kafka_protocol::messages::GroupId;

/// `(state, protocol_data, [(metadata length, assignment length)])` of one group.
async fn described(group: &str) -> DynResult<(String, String, Vec<(usize, usize)>)> {
    let mut c = conn().await?;
    let mut r = DescribeGroupsRequest::default();
    r.groups = vec![GroupId(s(group))];
    let resp = roundtrip(&mut c, 5, 7, &r).await?;
    let g = &resp.groups[0];
    Ok((
        g.group_state.to_string(),
        g.protocol_data.to_string(),
        g.members
            .iter()
            .map(|m| (m.member_metadata.len(), m.member_assignment.len()))
            .collect(),
    ))
}

/// Kafka 3.7 (`GroupMetadata.summary`) reports the protocol and each member's metadata and
/// assignment only for a Stable group; in CompletingRebalance (and PreparingRebalance) the
/// protocol is "" and the members carry neither. pg_kafka reported the protocol name and
/// the member metadata in every state.
pub async fn test_describe_groups_reports_metadata_only_when_stable() -> TestResult {
    println!("=== Test: DescribeGroups reports member metadata only when Stable ===\n");
    let ctx = TestContext::new().await?;
    let group = ctx.unique_group("describe-states").await;
    let mut c = conn().await?;
    let (e, generation, member, _) =
        join_now(&mut c, &join_req(&group, "", "consumer", &["range"])).await?;
    assert_eq!(e, error_codes::NONE);
    assert_eq!(
        described(&group).await?,
        (
            "CompletingRebalance".to_string(),
            String::new(),
            vec![(0, 0)]
        )
    );
    assert_eq!(
        sync_leader(&mut c, &group, generation, &member).await?,
        error_codes::NONE
    );
    let (state, protocol, members) = described(&group).await?;
    assert_eq!((state.as_str(), protocol.as_str()), ("Stable", "range"));
    assert!(
        members[0].0 > 0,
        "metadata reported when Stable: {members:?}"
    );
    assert_eq!(leave(&mut c, &group, &member).await?, error_codes::NONE);
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// Kafka 3.7 matches a ListGroups states filter case-sensitively (the CLI sends the
/// canonical names); pg_kafka matched "stable" and "STABLE" too.
pub async fn test_list_groups_state_filter_is_case_sensitive() -> TestResult {
    println!("=== Test: ListGroups' states filter is case-sensitive ===\n");
    let ctx = TestContext::new().await?;
    let group = ctx.unique_group("list-case").await;
    let mut c = conn().await?;
    let (e, generation, member, _) =
        join_now(&mut c, &join_req(&group, "", "consumer", &["range"])).await?;
    assert_eq!(e, error_codes::NONE);
    assert_eq!(
        sync_leader(&mut c, &group, generation, &member).await?,
        error_codes::NONE
    );
    let listed = |filter: &'static str| {
        let group = group.clone();
        async move {
            let mut c = conn().await?;
            let mut r = ListGroupsRequest::default();
            r.states_filter = vec![s(filter)];
            let resp = roundtrip(&mut c, 4, 8, &r).await?;
            assert_eq!(resp.error_code, error_codes::NONE);
            DynResult::Ok(resp.groups.iter().any(|g| g.group_id.0.as_str() == group))
        }
    };
    assert!(listed("Stable").await?, "the canonical name matches");
    assert!(!listed("stable").await?, "lower case does not");
    assert!(!listed("STABLE").await?, "upper case does not");
    assert_eq!(leave(&mut c, &group, &member).await?, error_codes::NONE);
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// Kafka 3.7 knows coordinator types 0 (group) and 1 (transaction) and answers any other
/// FindCoordinator key_type INVALID_REQUEST; pg_kafka answered NONE with itself.
pub async fn test_find_coordinator_unknown_key_type_is_invalid_request() -> TestResult {
    println!("=== Test: FindCoordinator with an unknown key_type ===\n");
    let mut c = conn().await?;
    for (version, key_type, expected) in [
        (1, 0, error_codes::NONE),
        (3, 1, error_codes::NONE),
        (1, 2, error_codes::INVALID_REQUEST),
        (2, 5, error_codes::INVALID_REQUEST),
        (3, 7, error_codes::INVALID_REQUEST),
    ] {
        let mut r = FindCoordinatorRequest::default();
        r.key = s("find-coordinator-key-type");
        r.key_type = key_type;
        let resp = roundtrip(&mut c, version, 9, &r).await?;
        assert_eq!(resp.error_code, expected, "v{version} key_type {key_type}");
    }
    println!("✅ Test PASSED\n");
    Ok(())
}
