//! ListOffsets leader epoch.

use crate::common::TestResult;
use crate::error_codes;
use crate::fixtures::topic_with_partitions as topic;
use crate::setup::TestContext;
use crate::wire::records::{batch, now_ms};
use crate::wire::Wire;
use kafka_protocol::messages::list_offsets_request::{
    ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic,
};
use kafka_protocol::messages::{BrokerId, TopicName};
use kafka_protocol::protocol::StrBytes;

/// ListOffsets at `version` of partition 0 of `topic` for each of `timestamps`:
/// `(error_code, offset, leader_epoch)` per timestamp.
async fn list(
    wire: &mut Wire,
    version: i16,
    topic: &str,
    timestamps: &[i64],
) -> crate::common::DynResult<Vec<(i16, i64, i32)>> {
    let mut answers = Vec::new();
    for &timestamp in timestamps {
        let mut p = ListOffsetsPartition::default();
        p.partition_index = 0;
        p.timestamp = timestamp;
        let mut t = ListOffsetsTopic::default();
        t.name = TopicName(StrBytes::from_string(topic.to_string()));
        t.partitions = vec![p];
        let mut request = ListOffsetsRequest::default();
        request.replica_id = BrokerId(-1);
        request.topics = vec![t];
        let response = wire.call(version, &request).await?;
        let p = &response.topics[0].partitions[0];
        answers.push((p.error_code, p.offset, p.leader_epoch));
    }
    Ok(answers)
}

/// ListOffsets v4-v7 reports leader epoch 0 with every offset it finds (EARLIEST, LATEST,
/// MAX_TIMESTAMP, a timestamp) and -1 when it finds none, as Apache Kafka 3.7 answers for a
/// partition whose leader never changed; v1-v3, which have no such field, still answer.
pub async fn test_list_offsets_reports_leader_epoch_zero() -> TestResult {
    println!("=== Test: ListOffsets leader epoch ===\n");
    let ctx = TestContext::new().await?;
    let t = topic(&ctx, "lo-epoch", 1).await?;
    let mut w = Wire::connect().await?;
    let now = now_ms();
    let (err, _) = w
        .produce(
            &t,
            0,
            batch(&["a", "b"], -1, -1, -1, false, Some(&[now, now + 1_000])),
            None,
        )
        .await?;
    assert_eq!(err, error_codes::NONE);

    // EARLIEST, LATEST, MAX_TIMESTAMP, the second record's timestamp, past the last one.
    let timestamps = [-2, -1, -3, now + 1_000, now + 60_000];
    for version in 4..=7 {
        let answers = list(&mut w, version, &t, &timestamps).await?;
        println!("   v{version}: {answers:?}");
        assert_eq!(
            answers,
            vec![
                (error_codes::NONE, 0, 0),
                (error_codes::NONE, 2, 0),
                (error_codes::NONE, 1, 0),
                (error_codes::NONE, 1, 0),
                (error_codes::NONE, -1, -1),
            ],
            "ListOffsets v{version}"
        );
    }
    for version in 1..=3 {
        let answers = list(&mut w, version, &t, &[-2, -1]).await?;
        println!("   v{version}: {answers:?}");
        assert_eq!(
            answers,
            vec![(error_codes::NONE, 0, -1), (error_codes::NONE, 2, -1)],
            "ListOffsets v{version} (no leader epoch field)"
        );
    }

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
