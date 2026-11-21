//! OffsetCommit and OffsetFetch handler tests.

use crate::kafka::constants::*;
use crate::kafka::handlers::consumer;
use crate::kafka::messages::{
    OffsetCommitPartitionData, OffsetCommitTopicData, OffsetFetchTopicData,
};
use crate::kafka::storage::{CommittedOffset, TopicMetadata};
use crate::kafka::{BrokerMetadata, GroupCoordinator, HandlerContext};
use crate::testing::mocks::MockKafkaStore;
use crate::testing::TestEnv;
use kafka_protocol::records::Compression;

// ========== OffsetCommit Handler Tests ==========

#[test]
fn test_handle_offset_commit_success() {
    let mut mock = MockKafkaStore::new();

    mock.expect_get_topic_metadata().returning(|_| {
        Ok(vec![TopicMetadata {
            name: "test-topic".to_string(),
            id: 1,
            partition_count: 1,
        }])
    });

    mock.expect_commit_offset()
        .returning(|_, _, _, _, _| Ok(()));

    let topics = vec![OffsetCommitTopicData {
        name: "test-topic".to_string(),
        partitions: vec![OffsetCommitPartitionData {
            partition_index: 0,
            committed_offset: 50,
            metadata: None,
        }],
    }];

    // Create HandlerContext AFTER setting expectations
    let env = TestEnv::new();
    let ctx = env.ctx(&mock);

    let response = consumer::handle_offset_commit(&ctx, "test-group", -1, "", topics).unwrap();

    let partition_response = &response.topics[0].partitions[0];
    assert_eq!(partition_response.error_code, ERROR_NONE);
}

#[test]
fn test_handle_offset_commit_rejects_stale_generation() {
    // A commit with a stale generation for a live group is rejected per-partition with
    // ILLEGAL_GENERATION before any store write (the mock has no commit_offset expectation, so a
    // write attempt would panic the test).
    let mut mock = MockKafkaStore::new();
    crate::testing::expect_topics(&mut mock, |_| Ok(Some((1, 1))));
    let coordinator = GroupCoordinator::new();
    let joined = std::sync::Arc::new(std::sync::Mutex::new(None));
    let slot = joined.clone();
    coordinator.join_group(
        &crate::kafka::coordinator::JoinGroupParams {
            group_id: "zombie-group".to_string(),
            member_id: None,
            client_id: "c1".to_string(),
            client_host: "h".to_string(),
            session_timeout_ms: 300_000,
            rebalance_timeout_ms: 300_000,
            protocol_type: "consumer".to_string(),
            protocols: vec![("range".to_string(), vec![])],
            group_instance_id: None,
        },
        Box::new(move |outcome| *slot.lock().unwrap() = Some(outcome)),
    );
    let joined = joined.lock().unwrap().take().unwrap().unwrap();
    let (member, generation) = (joined.member_id, joined.generation_id);

    let topics = vec![OffsetCommitTopicData {
        name: "t".to_string(),
        partitions: vec![OffsetCommitPartitionData {
            partition_index: 0,
            committed_offset: 10,
            metadata: None,
        }],
    }];

    let broker = BrokerMetadata::new("localhost".to_string(), 9092);
    let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

    let response =
        consumer::handle_offset_commit(&ctx, "zombie-group", generation - 1, &member, topics)
            .unwrap();

    assert_eq!(
        response.topics[0].partitions[0].error_code,
        ERROR_ILLEGAL_GENERATION
    );
}

#[test]
fn test_handle_offset_commit_unknown_topic() {
    let mut mock = MockKafkaStore::new();

    // Topic not found
    mock.expect_get_topic_metadata().returning(|_| Ok(vec![]));

    let topics = vec![OffsetCommitTopicData {
        name: "unknown-topic".to_string(),
        partitions: vec![OffsetCommitPartitionData {
            partition_index: 0,
            committed_offset: 50,
            metadata: None,
        }],
    }];

    // Create HandlerContext AFTER setting expectations
    let env = TestEnv::new();
    let ctx = env.ctx(&mock);

    let response = consumer::handle_offset_commit(&ctx, "test-group", -1, "", topics).unwrap();

    let partition_response = &response.topics[0].partitions[0];
    assert_eq!(
        partition_response.error_code,
        ERROR_UNKNOWN_TOPIC_OR_PARTITION
    );
}

// ========== OffsetFetch Handler Tests ==========

#[test]
fn test_handle_offset_fetch_found() {
    let mut mock = MockKafkaStore::new();

    mock.expect_get_topic_metadata().returning(|_| {
        Ok(vec![TopicMetadata {
            name: "test-topic".to_string(),
            id: 1,
            partition_count: 1,
        }])
    });

    mock.expect_fetch_all_offsets().returning(|_| {
        Ok(vec![(
            "test-topic".to_string(),
            0,
            CommittedOffset {
                offset: 42,
                metadata: Some("test-metadata".to_string()),
            },
        )])
    });

    let topics = Some(vec![OffsetFetchTopicData {
        name: "test-topic".to_string(),
        partition_indexes: vec![0],
    }]);

    // Create HandlerContext AFTER setting expectations
    let env = TestEnv::new();
    let ctx = env.ctx(&mock);

    let response = consumer::handle_offset_fetch(&ctx, "test-group", topics, false).unwrap();

    let partition_response = &response.topics[0].partitions[0];
    assert_eq!(partition_response.error_code, ERROR_NONE);
    assert_eq!(partition_response.committed_offset, 42);
}

#[test]
fn test_handle_offset_fetch_not_found() {
    let mut mock = MockKafkaStore::new();

    mock.expect_get_topic_metadata().returning(|_| {
        Ok(vec![TopicMetadata {
            name: "test-topic".to_string(),
            id: 1,
            partition_count: 1,
        }])
    });

    // No committed offset
    mock.expect_fetch_all_offsets().returning(|_| Ok(vec![]));

    let topics = Some(vec![OffsetFetchTopicData {
        name: "test-topic".to_string(),
        partition_indexes: vec![0],
    }]);

    // Create HandlerContext AFTER setting expectations
    let env = TestEnv::new();
    let ctx = env.ctx(&mock);

    let response = consumer::handle_offset_fetch(&ctx, "test-group", topics, false).unwrap();

    let partition_response = &response.topics[0].partitions[0];
    assert_eq!(partition_response.error_code, ERROR_NONE);
    assert_eq!(partition_response.committed_offset, -1); // -1 indicates not committed
}

#[test]
fn test_handle_offset_fetch_all_offsets() {
    let mut mock = MockKafkaStore::new();

    mock.expect_fetch_all_offsets().returning(|_| {
        Ok(vec![
            (
                "topic1".to_string(),
                0,
                CommittedOffset {
                    offset: 10,
                    metadata: None,
                },
            ),
            (
                "topic2".to_string(),
                0,
                CommittedOffset {
                    offset: 20,
                    metadata: None,
                },
            ),
        ])
    });

    // Create HandlerContext AFTER setting expectations
    let env = TestEnv::new();
    let ctx = env.ctx(&mock);

    // Request with None = fetch all
    let response = consumer::handle_offset_fetch(&ctx, "test-group", None, false).unwrap();

    // Should have 2 topics
    assert_eq!(response.topics.len(), 2);
}

/// An OffsetCommit with a generation >= 0 for a group the coordinator does not hold: Kafka
/// 3.7 answers UNKNOWN_MEMBER_ID when the group has committed offsets (it is an Empty group
/// there) and ILLEGAL_GENERATION when it has none; nothing is stored either way.
#[test]
fn test_handle_offset_commit_generation_for_offsets_only_group() {
    let commit = |has_offsets: bool, generation: i32, member: &str| {
        let mut mock = MockKafkaStore::new();
        crate::testing::expect_topics(&mut mock, |_| Ok(Some((1, 1))));
        mock.expect_groups_with_offsets()
            .withf(|groups| groups == ["offsets-only".to_string()])
            .times(1)
            .returning(move |groups| {
                Ok(if has_offsets {
                    groups.iter().cloned().collect()
                } else {
                    std::collections::HashSet::new()
                })
            });
        mock.expect_commit_offset().never();
        let env = TestEnv::new();
        let ctx = env.ctx(&mock);
        let topics = vec![OffsetCommitTopicData {
            name: "t".to_string(),
            partitions: vec![OffsetCommitPartitionData {
                partition_index: 0,
                committed_offset: 10,
                metadata: None,
            }],
        }];
        consumer::handle_offset_commit(&ctx, "offsets-only", generation, member, topics)
            .unwrap()
            .topics[0]
            .partitions[0]
            .error_code
    };
    assert_eq!(commit(true, 5, "zz"), ERROR_UNKNOWN_MEMBER_ID);
    assert_eq!(commit(true, 0, ""), ERROR_UNKNOWN_MEMBER_ID);
    assert_eq!(commit(false, 5, "zz"), ERROR_ILLEGAL_GENERATION);
}

/// A refused OffsetCommit answers a partition that does not exist
/// UNKNOWN_TOPIC_OR_PARTITION and the refusal's code only on the partitions that exist, as
/// Kafka 3.7 does (it filters unknown partitions out before it asks the coordinator).
#[test]
fn test_handle_offset_commit_refusal_spares_unknown_partitions() {
    let mut mock = MockKafkaStore::new();
    crate::testing::expect_topics(&mut mock, |name| Ok((name == "t").then_some((1, 2))));
    mock.expect_groups_with_offsets()
        .returning(|_| Ok(std::collections::HashSet::new()));
    mock.expect_commit_offset().never();
    let env = TestEnv::new();
    let ctx = env.ctx(&mock);
    let partition = |partition_index| OffsetCommitPartitionData {
        partition_index,
        committed_offset: 10,
        metadata: None,
    };
    let topics = vec![
        OffsetCommitTopicData {
            name: "t".to_string(),
            partitions: vec![partition(0), partition(7)],
        },
        OffsetCommitTopicData {
            name: "missing".to_string(),
            partitions: vec![partition(0)],
        },
    ];
    let response = consumer::handle_offset_commit(&ctx, "no-such-group", 5, "zz", topics).unwrap();
    let codes: Vec<i16> = response
        .topics
        .iter()
        .flat_map(|t| t.partitions.iter().map(|p| p.error_code))
        .collect();
    assert_eq!(
        codes,
        vec![
            ERROR_ILLEGAL_GENERATION,
            ERROR_UNKNOWN_TOPIC_OR_PARTITION,
            ERROR_UNKNOWN_TOPIC_OR_PARTITION
        ]
    );
}
