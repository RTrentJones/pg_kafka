//! OffsetCommit and OffsetFetch handler tests.

use crate::kafka::constants::*;
use crate::kafka::handlers::consumer;
use crate::kafka::messages::{
    OffsetCommitPartitionData, OffsetCommitTopicData, OffsetFetchTopicData,
};
use crate::kafka::storage::{CommittedOffset, TopicMetadata};
use crate::kafka::{BrokerMetadata, GroupCoordinator, HandlerContext};
use crate::testing::mocks::MockKafkaStore;
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
    let coordinator = GroupCoordinator::new();
    let broker = BrokerMetadata::new("localhost".to_string(), 9092);
    let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

    let response =
        consumer::handle_offset_commit(&ctx, "test-group".to_string(), -1, String::new(), topics)
            .unwrap();

    let partition_response = &response.topics[0].partitions[0];
    assert_eq!(partition_response.error_code, ERROR_NONE);
}

#[test]
fn test_handle_offset_commit_rejects_stale_generation() {
    // CONF-1: a commit with a stale generation for a live group is rejected per-partition with
    // ILLEGAL_GENERATION before any store write (the mock has no commit_offset expectation, so a
    // write attempt would panic the test).
    let mock = MockKafkaStore::new();
    let coordinator = GroupCoordinator::new();
    let joined = std::sync::Arc::new(std::sync::Mutex::new(None));
    let slot = joined.clone();
    coordinator.join_group(
        crate::kafka::coordinator::JoinGroupParams {
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

    let response = consumer::handle_offset_commit(
        &ctx,
        "zombie-group".to_string(),
        generation - 1,
        member,
        topics,
    )
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
    let coordinator = GroupCoordinator::new();
    let broker = BrokerMetadata::new("localhost".to_string(), 9092);
    let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

    let response =
        consumer::handle_offset_commit(&ctx, "test-group".to_string(), -1, String::new(), topics)
            .unwrap();

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

    mock.expect_fetch_offset().returning(|_, _, _| {
        Ok(Some(CommittedOffset {
            offset: 42,
            metadata: Some("test-metadata".to_string()),
        }))
    });

    let topics = Some(vec![OffsetFetchTopicData {
        name: "test-topic".to_string(),
        partition_indexes: vec![0],
    }]);

    // Create HandlerContext AFTER setting expectations
    let coordinator = GroupCoordinator::new();
    let broker = BrokerMetadata::new("localhost".to_string(), 9092);
    let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

    let response =
        consumer::handle_offset_fetch(&ctx, "test-group".to_string(), topics, false).unwrap();

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
    mock.expect_fetch_offset().returning(|_, _, _| Ok(None));

    let topics = Some(vec![OffsetFetchTopicData {
        name: "test-topic".to_string(),
        partition_indexes: vec![0],
    }]);

    // Create HandlerContext AFTER setting expectations
    let coordinator = GroupCoordinator::new();
    let broker = BrokerMetadata::new("localhost".to_string(), 9092);
    let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

    let response =
        consumer::handle_offset_fetch(&ctx, "test-group".to_string(), topics, false).unwrap();

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
    let coordinator = GroupCoordinator::new();
    let broker = BrokerMetadata::new("localhost".to_string(), 9092);
    let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

    // Request with None = fetch all
    let response =
        consumer::handle_offset_fetch(&ctx, "test-group".to_string(), None, false).unwrap();

    // Should have 2 topics
    assert_eq!(response.topics.len(), 2);
}
