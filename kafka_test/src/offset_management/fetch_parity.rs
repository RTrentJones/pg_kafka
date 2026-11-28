//! OffsetFetch and OffsetCommit answers checked against Kafka 3.7: a topic that does not
//! exist, commit metadata, and the metadata length limit.

use crate::common::{create_admin_client, TestResult};
use crate::error_codes;
use crate::fixtures::topic_with_partitions as topic;
use crate::setup::TestContext;
use crate::wire::Wire;
use kafka_protocol::messages::offset_commit_request::{
    OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
use kafka_protocol::messages::offset_commit_response::OffsetCommitResponse;
use kafka_protocol::messages::offset_fetch_request::{OffsetFetchRequest, OffsetFetchRequestTopic};
use kafka_protocol::messages::offset_fetch_response::OffsetFetchResponse;
use kafka_protocol::messages::{GroupId, TopicName};
use kafka_protocol::protocol::StrBytes;
use rdkafka::admin::AdminOptions;
use std::time::Duration;

/// `(partition, offset, error, metadata)` of every partition an OffsetFetch answers.
type Fetched = Vec<(i32, i64, i16, Option<String>)>;

async fn offset_fetch(w: &mut Wire, version: i16, group: &str, topic: &str) -> Fetched {
    let mut t = OffsetFetchRequestTopic::default();
    t.name = TopicName(StrBytes::from_string(topic.to_string()));
    t.partition_indexes = vec![0, 1];
    let mut req = OffsetFetchRequest::default();
    req.group_id = GroupId(StrBytes::from_string(group.to_string()));
    req.topics = Some(vec![t]);
    let resp: OffsetFetchResponse = w.call(version, &req).await.expect("OffsetFetch");
    assert_eq!(resp.error_code, error_codes::NONE, "v{version} top level");
    resp.topics
        .iter()
        .flat_map(|t| t.partitions.iter())
        .map(|p| {
            (
                p.partition_index,
                p.committed_offset,
                p.error_code,
                p.metadata.as_ref().map(ToString::to_string),
            )
        })
        .collect()
}

/// OffsetCommit v8 of `(partition, offset, metadata)` with generation -1 (a simple
/// consumer): each partition's error code.
pub(super) async fn commit(
    w: &mut Wire,
    group: &str,
    topic: &str,
    entries: &[(i32, i64, Option<String>)],
) -> Vec<i16> {
    let mut t = OffsetCommitRequestTopic::default();
    t.name = TopicName(StrBytes::from_string(topic.to_string()));
    t.partitions = entries
        .iter()
        .map(|(partition, offset, metadata)| {
            let mut p = OffsetCommitRequestPartition::default();
            p.partition_index = *partition;
            p.committed_offset = *offset;
            p.committed_metadata = metadata.clone().map(StrBytes::from_string);
            p
        })
        .collect();
    let mut req = OffsetCommitRequest::default();
    req.group_id = GroupId(StrBytes::from_string(group.to_string()));
    req.generation_id_or_member_epoch = -1;
    req.topics = vec![t];
    let resp: OffsetCommitResponse = w.call(8, &req).await.expect("OffsetCommit");
    resp.topics[0]
        .partitions
        .iter()
        .map(|p| p.error_code)
        .collect()
}

/// Kafka 3.7 answers an OffsetFetch for a topic that does not exist, never created or
/// deleted since the commit, with offset -1 and NONE on every version: a partition without
/// a committed offset. UNKNOWN_TOPIC_OR_PARTITION, which pg_kafka answered, is fatal to the
/// Java consumer: poll() threw "Topic or Partition ... does not exist" for a consumer that
/// assign()s a topic before it is created or after it is deleted.
pub async fn test_offset_fetch_of_a_missing_topic_is_no_offset() -> TestResult {
    println!("=== Test: OffsetFetch of a missing topic answers offset -1, NONE ===\n");
    let ctx = TestContext::new().await?;
    let group = ctx.unique_group("of-missing").await;
    let never = ctx.unique_topic("of-missing-never").await;
    let deleted = topic(&ctx, "of-missing-deleted", 2).await?;
    let mut w = Wire::connect().await?;
    let no_offset = |partition| (partition, -1, error_codes::NONE, Some(String::new()));

    println!("Step 1: a topic that never existed, OffsetFetch v1 to v7...");
    for version in 1..=7 {
        assert_eq!(
            offset_fetch(&mut w, version, &group, &never).await,
            vec![no_offset(0), no_offset(1)],
            "v{version}"
        );
    }
    println!("✅ offset -1, NONE\n");

    println!("Step 2: a topic deleted after the group committed to it...");
    assert_eq!(
        commit(
            &mut w,
            &group,
            &deleted,
            &[(0, 5, None), (1, 6, Some("m".into()))]
        )
        .await,
        [error_codes::NONE, error_codes::NONE]
    );
    // Metadata committed as null is answered "", as Kafka stores it.
    assert_eq!(
        offset_fetch(&mut w, 7, &group, &deleted).await,
        vec![
            (0, 5, error_codes::NONE, Some(String::new())),
            (1, 6, error_codes::NONE, Some("m".to_string()))
        ]
    );
    let admin = create_admin_client()?;
    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(5)));
    let deleted_result = admin.delete_topics(&[&deleted], &opts).await?;
    assert!(deleted_result[0].is_ok(), "{deleted_result:?}");
    for version in 1..=7 {
        assert_eq!(
            offset_fetch(&mut w, version, &group, &deleted).await,
            vec![no_offset(0), no_offset(1)],
            "v{version} after the delete"
        );
    }
    println!("✅ offset -1, NONE\n");

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// Kafka 3.7 (offset.metadata.max.bytes = 4096, a length in UTF-16 code units) answers an
/// OffsetCommit partition whose metadata is longer OFFSET_METADATA_TOO_LARGE, stores
/// nothing for it and commits the request's other partitions.
pub async fn test_offset_commit_metadata_longer_than_4096_is_refused() -> TestResult {
    println!("=== Test: OffsetCommit metadata over 4096 characters ===\n");
    let ctx = TestContext::new().await?;
    let group = ctx.unique_group("oc-metadata").await;
    let t = topic(&ctx, "oc-metadata", 2).await?;
    let mut w = Wire::connect().await?;
    assert_eq!(
        commit(
            &mut w,
            &group,
            &t,
            &[
                (0, 3, Some("x".repeat(4096))),
                (1, 4, Some("x".repeat(4097)))
            ]
        )
        .await,
        [error_codes::NONE, error_codes::OFFSET_METADATA_TOO_LARGE]
    );
    let fetched = offset_fetch(&mut w, 7, &group, &t).await;
    assert_eq!(
        fetched
            .iter()
            .map(|(p, o, e, m)| (*p, *o, *e, m.as_ref().map(String::len)))
            .collect::<Vec<_>>(),
        vec![
            (0, 3, error_codes::NONE, Some(4096)),
            (1, -1, error_codes::NONE, Some(0))
        ]
    );
    let stored: i64 = ctx
        .db()
        .query_one(
            "SELECT count(*) FROM kafka.consumer_offsets WHERE group_id = $1",
            &[&group],
        )
        .await?
        .get(0);
    assert_eq!(stored, 1, "only the partition that fits is stored");
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
