//! OffsetCommit generation and member-id rules.

use crate::common::TestResult;
use crate::setup::TestContext;
use crate::wire::requests::*;

/// Item 4: OffsetCommit validation, Kafka 3.7's answers (each checked against a real broker):
/// unknown group: gen -1 accepted (any member id), gen >= 0 ILLEGAL_GENERATION; group with
/// members: gen -1 / "" UNKNOWN_MEMBER_ID, gen -1 with the member's id ILLEGAL_GENERATION,
/// the right member and generation accepted; Empty group: gen -1 accepted, gen >= 0
/// UNKNOWN_MEMBER_ID. Before: gen -1 was accepted for a group with members, and an unknown group
/// at gen >= 0 got UNKNOWN_MEMBER_ID.
pub async fn test_w3g_offset_commit_rules() -> TestResult {
    println!("=== Test: OffsetCommit rules ===\n");
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("w3g-commit").await;
    produce_one(&topic).await?;
    let unknown = ctx.unique_group("w3g-commit-unknown").await;
    let group = ctx.unique_group("w3g-commit-live").await;

    assert_eq!(
        commit(&unknown, 5, "x", &topic, 1).await?,
        ERROR_ILLEGAL_GENERATION
    );
    assert_eq!(commit(&unknown, -1, "x", &topic, 1).await?, ERROR_NONE);
    assert_eq!(commit(&unknown, -1, "", &topic, 1).await?, ERROR_NONE);

    let mut a = conn().await?;
    let (e, generation, member, _) =
        join_now(&mut a, &join_req(&group, "", "consumer", &["range"])).await?;
    assert_eq!(e, ERROR_NONE);
    assert_eq!(
        sync_leader(&mut a, &group, generation, &member).await?,
        ERROR_NONE
    );
    assert_eq!(
        commit(&group, -1, "", &topic, 2).await?,
        ERROR_UNKNOWN_MEMBER_ID
    );
    assert_eq!(
        commit(&group, -1, "x", &topic, 2).await?,
        ERROR_UNKNOWN_MEMBER_ID
    );
    assert_eq!(
        commit(&group, -1, &member, &topic, 2).await?,
        ERROR_ILLEGAL_GENERATION
    );
    assert_eq!(
        commit(&group, generation + 3, &member, &topic, 2).await?,
        ERROR_ILLEGAL_GENERATION
    );
    assert_eq!(
        commit(&group, generation, &member, &topic, 3).await?,
        ERROR_NONE
    );

    assert_eq!(leave(&mut a, &group, &member).await?, ERROR_NONE);
    assert_eq!(commit(&group, -1, "", &topic, 4).await?, ERROR_NONE);
    assert_eq!(commit(&group, -1, &member, &topic, 4).await?, ERROR_NONE);
    assert_eq!(
        commit(&group, 5, "x", &topic, 4).await?,
        ERROR_UNKNOWN_MEMBER_ID
    );

    let committed: i64 = ctx
        .db()
        .query_one(
            "SELECT committed_offset FROM kafka.consumer_offsets WHERE group_id = $1",
            &[&group],
        )
        .await?
        .get(0);
    assert_eq!(committed, 4, "only accepted commits are stored");
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
