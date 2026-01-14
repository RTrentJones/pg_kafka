// Handler unit tests
//
// These tests verify handler logic in isolation using MockKafkaStore.
// Each test sets up mock expectations and verifies the handler produces
// correct responses for various scenarios.

#[cfg(test)]
#[allow(clippy::module_inception)]
mod tests {
    use crate::kafka::constants::*;
    use crate::kafka::error::KafkaError;
    use crate::kafka::handlers::{consumer, fetch, metadata, produce};
    use crate::kafka::messages::{
        ListOffsetsPartitionData, ListOffsetsTopicData, OffsetCommitPartitionData,
        OffsetCommitTopicData, OffsetFetchTopicData, PartitionFetchData, PartitionProduceData,
        Record, TopicFetchData, TopicProduceData,
    };
    use crate::kafka::storage::{CommittedOffset, FetchedMessage, TopicMetadata};
    use crate::kafka::{BrokerMetadata, GroupCoordinator, HandlerContext};
    use crate::testing::mocks::MockKafkaStore;
    use kafka_protocol::records::Compression;

    // ========== Produce Handler Tests ==========

    #[test]
    fn test_handle_produce_success() {
        let mut mock = MockKafkaStore::new();

        // Expect topic lookup - now returns (topic_id, partition_count) in single call
        mock.expect_get_or_create_topic()
            .with(
                mockall::predicate::eq("test-topic"),
                mockall::predicate::always(),
            )
            .times(1)
            .returning(|_, _| Ok((1, 1))); // (topic_id=1, partition_count=1)

        mock.expect_insert_records()
            .times(1)
            .returning(|_, _, _| Ok(0)); // Returns base offset 0

        let topic_data = vec![TopicProduceData {
            name: "test-topic".to_string(),
            partitions: vec![PartitionProduceData {
                partition_index: 0,
                records: vec![Record {
                    key: Some(b"key1".to_vec()),
                    value: Some(b"value1".to_vec()),
                    headers: vec![],
                    timestamp: None,
                }],
                producer_metadata: None,
            }],
        }];

        // Create HandlerContext AFTER setting expectations
        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        let response = produce::handle_produce(&ctx, topic_data, None, None).unwrap();

        assert_eq!(response.responses.len(), 1);
        let topic_response = &response.responses[0];
        assert_eq!(topic_response.partition_responses.len(), 1);
        let partition_response = &topic_response.partition_responses[0];
        assert_eq!(partition_response.error_code, ERROR_NONE);
        assert_eq!(partition_response.base_offset, 0);
    }

    #[test]
    fn test_handle_produce_unknown_partition() {
        let mut mock = MockKafkaStore::new();

        // Return (topic_id=1, partition_count=1), so only partition 0 is valid
        mock.expect_get_or_create_topic()
            .returning(|_, _| Ok((1, 1)));

        // Request partition 5 which exceeds partition_count
        let topic_data = vec![TopicProduceData {
            name: "test-topic".to_string(),
            partitions: vec![PartitionProduceData {
                partition_index: 5,
                records: vec![],
                producer_metadata: None,
            }],
        }];

        // Create HandlerContext AFTER setting expectations
        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        let response = produce::handle_produce(&ctx, topic_data, None, None).unwrap();

        let partition_response = &response.responses[0].partition_responses[0];
        assert_eq!(
            partition_response.error_code,
            ERROR_UNKNOWN_TOPIC_OR_PARTITION
        );
        assert_eq!(partition_response.base_offset, -1);
    }

    #[test]
    fn test_handle_produce_insert_error() {
        let mut mock = MockKafkaStore::new();

        // Return (topic_id=1, partition_count=1), so partition 0 is valid
        mock.expect_get_or_create_topic()
            .returning(|_, _| Ok((1, 1)));

        mock.expect_insert_records()
            .returning(|_, _, _| Err(KafkaError::database("insert failed")));

        let topic_data = vec![TopicProduceData {
            name: "test-topic".to_string(),
            partitions: vec![PartitionProduceData {
                partition_index: 0,
                records: vec![Record {
                    key: None,
                    value: Some(b"test".to_vec()),
                    headers: vec![],
                    timestamp: None,
                }],
                producer_metadata: None,
            }],
        }];

        // Create HandlerContext AFTER setting expectations
        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        let response = produce::handle_produce(&ctx, topic_data, None, None).unwrap();

        let partition_response = &response.responses[0].partition_responses[0];
        assert_eq!(partition_response.error_code, ERROR_UNKNOWN_SERVER_ERROR);
    }

    // ========== Fetch Handler Tests ==========

    #[test]
    fn test_handle_fetch_success() {
        let mut mock = MockKafkaStore::new();

        mock.expect_get_topic_metadata().returning(|_| {
            Ok(vec![TopicMetadata {
                name: "test-topic".to_string(),
                id: 1,
                partition_count: 1,
            }])
        });

        mock.expect_fetch_records_with_isolation()
            .returning(|_, _, _, _, _| {
                Ok(vec![FetchedMessage {
                    partition_offset: 0,
                    key: Some(b"key1".to_vec()),
                    value: Some(b"value1".to_vec()),
                    timestamp: 1234567890,
                }])
            });

        mock.expect_get_high_watermark().returning(|_, _| Ok(1));
        mock.expect_get_last_stable_offset().returning(|_, _| Ok(1));

        let topic_data = vec![TopicFetchData {
            name: "test-topic".to_string(),
            partitions: vec![PartitionFetchData {
                partition_index: 0,
                fetch_offset: 0,
                partition_max_bytes: 1024,
            }],
        }];

        // Create HandlerContext AFTER setting expectations
        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        let response = fetch::handle_fetch(
            &ctx,
            topic_data,
            crate::kafka::storage::IsolationLevel::ReadUncommitted,
        )
        .unwrap();

        assert_eq!(response.responses.len(), 1);
        let partition_data = &response.responses[0].partitions[0];
        assert_eq!(partition_data.error_code, ERROR_NONE);
        assert_eq!(partition_data.high_watermark, 1);
    }

    #[test]
    fn test_handle_fetch_unknown_topic() {
        let mut mock = MockKafkaStore::new();

        // Topic not found
        mock.expect_get_topic_metadata().returning(|_| Ok(vec![]));

        let topic_data = vec![TopicFetchData {
            name: "unknown-topic".to_string(),
            partitions: vec![PartitionFetchData {
                partition_index: 0,
                fetch_offset: 0,
                partition_max_bytes: 1024,
            }],
        }];

        // Create HandlerContext AFTER setting expectations
        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        let response = fetch::handle_fetch(
            &ctx,
            topic_data,
            crate::kafka::storage::IsolationLevel::ReadUncommitted,
        )
        .unwrap();

        let partition_data = &response.responses[0].partitions[0];
        assert_eq!(partition_data.error_code, ERROR_UNKNOWN_TOPIC_OR_PARTITION);
    }

    // ========== ListOffsets Handler Tests ==========

    #[test]
    fn test_handle_list_offsets_earliest() {
        let mut mock = MockKafkaStore::new();

        mock.expect_get_topic_metadata().returning(|_| {
            Ok(vec![TopicMetadata {
                name: "test-topic".to_string(),
                id: 1,
                partition_count: 1,
            }])
        });

        mock.expect_get_earliest_offset().returning(|_, _| Ok(0));

        let topics = vec![ListOffsetsTopicData {
            name: "test-topic".to_string(),
            partitions: vec![ListOffsetsPartitionData {
                partition_index: 0,
                current_leader_epoch: -1,
                timestamp: -2, // Earliest
            }],
        }];

        // Create HandlerContext AFTER setting expectations
        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        let response = fetch::handle_list_offsets(&ctx, topics).unwrap();

        let partition_response = &response.topics[0].partitions[0];
        assert_eq!(partition_response.error_code, ERROR_NONE);
        assert_eq!(partition_response.offset, 0);
    }

    #[test]
    fn test_handle_list_offsets_latest() {
        let mut mock = MockKafkaStore::new();

        mock.expect_get_topic_metadata().returning(|_| {
            Ok(vec![TopicMetadata {
                name: "test-topic".to_string(),
                id: 1,
                partition_count: 1,
            }])
        });

        mock.expect_get_high_watermark().returning(|_, _| Ok(100));

        let topics = vec![ListOffsetsTopicData {
            name: "test-topic".to_string(),
            partitions: vec![ListOffsetsPartitionData {
                partition_index: 0,
                current_leader_epoch: -1,
                timestamp: -1, // Latest
            }],
        }];

        // Create HandlerContext AFTER setting expectations
        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        let response = fetch::handle_list_offsets(&ctx, topics).unwrap();

        let partition_response = &response.topics[0].partitions[0];
        assert_eq!(partition_response.error_code, ERROR_NONE);
        assert_eq!(partition_response.offset, 100);
    }

    // ========== Metadata Handler Tests ==========

    #[test]
    fn test_handle_metadata_all_topics() {
        let mut mock = MockKafkaStore::new();

        mock.expect_get_topic_metadata().returning(|_| {
            Ok(vec![
                TopicMetadata {
                    name: "topic1".to_string(),
                    id: 1,
                    partition_count: 1,
                },
                TopicMetadata {
                    name: "topic2".to_string(),
                    id: 2,
                    partition_count: 1,
                },
            ])
        });

        // Create HandlerContext AFTER setting expectations
        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        let response = metadata::handle_metadata(&ctx, None).unwrap();

        assert_eq!(response.topics.len(), 2);
        assert_eq!(response.brokers.len(), 1);
    }

    #[test]
    fn test_handle_metadata_specific_topic() {
        let mut mock = MockKafkaStore::new();

        // get_or_create_topic now returns (topic_id, partition_count) in single call
        mock.expect_get_or_create_topic()
            .with(
                mockall::predicate::eq("my-topic"),
                mockall::predicate::always(),
            )
            .returning(|_, _| Ok((5, 1))); // (topic_id=5, partition_count=1)

        // Create HandlerContext AFTER setting expectations
        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        let response = metadata::handle_metadata(&ctx, Some(vec!["my-topic".to_string()])).unwrap();

        assert_eq!(response.topics.len(), 1);
    }

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
            consumer::handle_offset_commit(&ctx, "test-group".to_string(), topics).unwrap();

        let partition_response = &response.topics[0].partitions[0];
        assert_eq!(partition_response.error_code, ERROR_NONE);
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
            consumer::handle_offset_commit(&ctx, "test-group".to_string(), topics).unwrap();

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
            consumer::handle_offset_fetch(&ctx, "test-group".to_string(), topics).unwrap();

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
            consumer::handle_offset_fetch(&ctx, "test-group".to_string(), topics).unwrap();

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
        let response = consumer::handle_offset_fetch(&ctx, "test-group".to_string(), None).unwrap();

        // Should have 2 topics
        assert_eq!(response.topics.len(), 2);
    }

    // ========== Coordinator Handler Tests ==========
    //
    // These tests use a real GroupCoordinator instance (not a mock) since it's
    // pure in-memory state management with no database dependencies.

    use crate::kafka::handlers::coordinator;
    use crate::kafka::messages::{JoinGroupProtocol, SyncGroupAssignment};
    // Note: MockKafkaStore is already imported in outer scope
    // Note: GroupCoordinator is already imported in outer scope

    fn create_test_coordinator() -> GroupCoordinator {
        GroupCoordinator::new()
    }

    fn create_test_protocol() -> Vec<JoinGroupProtocol> {
        vec![JoinGroupProtocol {
            name: "range".to_string(),
            metadata: vec![0, 1, 2, 3], // Dummy subscription metadata
        }]
    }

    #[test]
    fn test_handle_find_coordinator_success() {
        let mock = MockKafkaStore::new();
        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        let response = coordinator::handle_find_coordinator(
            &ctx,
            "test-group".to_string(),
            0, // GROUP key type
        )
        .unwrap();

        assert_eq!(response.error_code, ERROR_NONE);
        assert_eq!(response.node_id, DEFAULT_BROKER_ID);
        assert_eq!(response.host.as_str(), "localhost");
        assert_eq!(response.port, 9092);
    }

    #[test]
    fn test_handle_join_group_new_member_becomes_leader() {
        let mock = MockKafkaStore::new();
        let coord = create_test_coordinator();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coord, &broker, 1, Compression::None);

        let response = coordinator::handle_join_group(
            &ctx,
            "test-group".to_string(),
            "".to_string(), // Empty = new member
            "test-client".to_string(),
            30000,
            60000,
            "consumer".to_string(),
            create_test_protocol(),
        )
        .unwrap();

        assert_eq!(response.error_code, ERROR_NONE);
        assert!(!response.member_id.is_empty());
        assert_eq!(response.generation_id, 1); // First generation after join
                                               // First member is the leader
        assert_eq!(response.leader.as_str(), response.member_id.as_str());
        // Leader receives member list
        assert!(!response.members.is_empty());
    }

    #[test]
    fn test_handle_join_group_second_member_not_leader() {
        let mock = MockKafkaStore::new();
        let coord = create_test_coordinator();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coord, &broker, 1, Compression::None);

        // First member joins and becomes leader
        let first_response = coordinator::handle_join_group(
            &ctx,
            "test-group".to_string(),
            "".to_string(),
            "client-1".to_string(),
            30000,
            60000,
            "consumer".to_string(),
            create_test_protocol(),
        )
        .unwrap();

        let leader_id = first_response.member_id.to_string();

        // Second member joins
        let second_response = coordinator::handle_join_group(
            &ctx,
            "test-group".to_string(),
            "".to_string(),
            "client-2".to_string(),
            30000,
            60000,
            "consumer".to_string(),
            create_test_protocol(),
        )
        .unwrap();

        assert_eq!(second_response.error_code, ERROR_NONE);
        assert!(!second_response.member_id.is_empty());
        // Second member is not leader (leader should still be first)
        assert_ne!(
            second_response.member_id.as_str(),
            second_response.leader.as_str()
        );
        // Non-leaders don't receive member list
        assert!(second_response.members.is_empty());
        // Leader should still be the first member
        assert_eq!(second_response.leader.as_str(), leader_id.as_str());
    }

    #[test]
    fn test_handle_join_group_rejoin_with_existing_id() {
        let mock = MockKafkaStore::new();
        let coord = create_test_coordinator();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coord, &broker, 1, Compression::None);

        // First join
        let first_response = coordinator::handle_join_group(
            &ctx,
            "test-group".to_string(),
            "".to_string(),
            "test-client".to_string(),
            30000,
            60000,
            "consumer".to_string(),
            create_test_protocol(),
        )
        .unwrap();

        let member_id = first_response.member_id.to_string();

        // Rejoin with same member ID
        let rejoin_response = coordinator::handle_join_group(
            &ctx,
            "test-group".to_string(),
            member_id.clone(),
            "test-client".to_string(),
            30000,
            60000,
            "consumer".to_string(),
            create_test_protocol(),
        )
        .unwrap();

        assert_eq!(rejoin_response.error_code, ERROR_NONE);
        // Should keep the same member ID
        assert_eq!(rejoin_response.member_id.as_str(), member_id.as_str());
    }

    #[test]
    fn test_handle_sync_group_leader_provides_assignments() {
        let mock = MockKafkaStore::new();
        let coord = create_test_coordinator();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coord, &broker, 1, Compression::None);

        // Join group first
        let join_response = coordinator::handle_join_group(
            &ctx,
            "test-group".to_string(),
            "".to_string(),
            "test-client".to_string(),
            30000,
            60000,
            "consumer".to_string(),
            create_test_protocol(),
        )
        .unwrap();

        let member_id = join_response.member_id.to_string();
        let generation_id = join_response.generation_id;

        // Leader sends sync with assignments
        let assignment_data = vec![0, 1, 2, 3, 4]; // Dummy assignment bytes
        let assignments = vec![SyncGroupAssignment {
            member_id: member_id.clone(),
            assignment: assignment_data.clone(),
        }];

        let sync_response = coordinator::handle_sync_group(
            &ctx,
            "test-group".to_string(),
            member_id,
            generation_id,
            assignments,
        )
        .unwrap();

        assert_eq!(sync_response.error_code, ERROR_NONE);
        // Leader should receive their own assignment
        assert_eq!(sync_response.assignment.as_ref(), &assignment_data);
    }

    #[test]
    fn test_handle_sync_group_follower_receives_assignment() {
        let mock = MockKafkaStore::new();
        let coord = create_test_coordinator();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coord, &broker, 1, Compression::None);

        // First member (leader) joins
        let leader_join = coordinator::handle_join_group(
            &ctx,
            "test-group".to_string(),
            "".to_string(),
            "leader-client".to_string(),
            30000,
            60000,
            "consumer".to_string(),
            create_test_protocol(),
        )
        .unwrap();

        let leader_id = leader_join.member_id.to_string();
        let _generation_id = leader_join.generation_id;

        // Second member (follower) joins
        let follower_join = coordinator::handle_join_group(
            &ctx,
            "test-group".to_string(),
            "".to_string(),
            "follower-client".to_string(),
            30000,
            60000,
            "consumer".to_string(),
            create_test_protocol(),
        )
        .unwrap();

        let follower_id = follower_join.member_id.to_string();
        // Generation may have incremented
        let gen = follower_join.generation_id;

        // Leader sends sync with assignments for both members
        let leader_assignment = vec![1, 2, 3];
        let follower_assignment = vec![4, 5, 6];

        let assignments = vec![
            SyncGroupAssignment {
                member_id: leader_id.clone(),
                assignment: leader_assignment.clone(),
            },
            SyncGroupAssignment {
                member_id: follower_id.clone(),
                assignment: follower_assignment.clone(),
            },
        ];

        // Leader syncs first
        let _leader_sync = coordinator::handle_sync_group(
            &ctx,
            "test-group".to_string(),
            leader_id,
            gen,
            assignments,
        )
        .unwrap();

        // Follower syncs (with empty assignments - only leader provides)
        let follower_sync = coordinator::handle_sync_group(
            &ctx,
            "test-group".to_string(),
            follower_id,
            gen,
            vec![], // Follower sends empty
        )
        .unwrap();

        assert_eq!(follower_sync.error_code, ERROR_NONE);
        // Follower should receive their assignment
        assert_eq!(follower_sync.assignment.as_ref(), &follower_assignment);
    }

    #[test]
    fn test_handle_heartbeat_success() {
        let mock = MockKafkaStore::new();
        let coord = create_test_coordinator();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coord, &broker, 1, Compression::None);

        // Join group first
        let join_response = coordinator::handle_join_group(
            &ctx,
            "test-group".to_string(),
            "".to_string(),
            "test-client".to_string(),
            30000,
            60000,
            "consumer".to_string(),
            create_test_protocol(),
        )
        .unwrap();

        let member_id = join_response.member_id.to_string();
        let generation_id = join_response.generation_id;

        // Complete sync to get to Stable state (Phase 5: heartbeat returns REBALANCE_IN_PROGRESS
        // when group is not in Stable state)
        let assignment_data = vec![0, 1, 2, 3, 4];
        let assignments = vec![SyncGroupAssignment {
            member_id: member_id.clone(),
            assignment: assignment_data,
        }];

        coordinator::handle_sync_group(
            &ctx,
            "test-group".to_string(),
            member_id.clone(),
            generation_id,
            assignments,
        )
        .unwrap();

        // Send heartbeat - should succeed now that group is Stable
        let heartbeat_response =
            coordinator::handle_heartbeat(&ctx, "test-group".to_string(), member_id, generation_id)
                .unwrap();

        assert_eq!(heartbeat_response.error_code, ERROR_NONE);
    }

    #[test]
    fn test_handle_heartbeat_unknown_member() {
        let mock = MockKafkaStore::new();
        let coord = create_test_coordinator();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coord, &broker, 1, Compression::None);

        // Create a group with a member
        let _join_response = coordinator::handle_join_group(
            &ctx,
            "test-group".to_string(),
            "".to_string(),
            "test-client".to_string(),
            30000,
            60000,
            "consumer".to_string(),
            create_test_protocol(),
        )
        .unwrap();

        // Heartbeat with unknown member ID
        let result = coordinator::handle_heartbeat(
            &ctx,
            "test-group".to_string(),
            "unknown-member-id".to_string(),
            1,
        );

        assert!(result.is_err());
        // Verify it's an UnknownMemberId error
        match result.unwrap_err() {
            KafkaError::UnknownMemberId { .. } => {}
            e => panic!("Expected UnknownMemberId error, got {:?}", e),
        }
    }

    #[test]
    fn test_handle_leave_group_success() {
        let mock = MockKafkaStore::new();
        let coord = create_test_coordinator();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coord, &broker, 1, Compression::None);

        // Join group first
        let join_response = coordinator::handle_join_group(
            &ctx,
            "test-group".to_string(),
            "".to_string(),
            "test-client".to_string(),
            30000,
            60000,
            "consumer".to_string(),
            create_test_protocol(),
        )
        .unwrap();

        let member_id = join_response.member_id.to_string();

        // Leave group
        let leave_response =
            coordinator::handle_leave_group(&ctx, "test-group".to_string(), member_id.clone())
                .unwrap();

        assert_eq!(leave_response.error_code, ERROR_NONE);

        // Verify member is gone by trying heartbeat
        let heartbeat_result =
            coordinator::handle_heartbeat(&ctx, "test-group".to_string(), member_id, 1);

        assert!(heartbeat_result.is_err());
    }

    #[test]
    fn test_handle_leave_group_unknown_member() {
        let mock = MockKafkaStore::new();
        let coord = create_test_coordinator();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coord, &broker, 1, Compression::None);

        // Create a group with a member
        let _join_response = coordinator::handle_join_group(
            &ctx,
            "test-group".to_string(),
            "".to_string(),
            "test-client".to_string(),
            30000,
            60000,
            "consumer".to_string(),
            create_test_protocol(),
        )
        .unwrap();

        // Leave with unknown member ID
        let result = coordinator::handle_leave_group(
            &ctx,
            "test-group".to_string(),
            "unknown-member-id".to_string(),
        );

        assert!(result.is_err());
        match result.unwrap_err() {
            KafkaError::UnknownMemberId { .. } => {}
            e => panic!("Expected UnknownMemberId error, got {:?}", e),
        }
    }

    #[test]
    fn test_handle_describe_groups_existing_group() {
        let mock = MockKafkaStore::new();
        let coord = create_test_coordinator();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coord, &broker, 1, Compression::None);

        // Join group first
        let join_response = coordinator::handle_join_group(
            &ctx,
            "test-group".to_string(),
            "".to_string(),
            "test-client".to_string(),
            30000,
            60000,
            "consumer".to_string(),
            create_test_protocol(),
        )
        .unwrap();

        // Describe the group
        let describe_response =
            coordinator::handle_describe_groups(&ctx, vec!["test-group".to_string()]).unwrap();

        assert_eq!(describe_response.groups.len(), 1);
        let group = &describe_response.groups[0];
        assert_eq!(group.error_code, ERROR_NONE);
        assert_eq!(group.group_id.0.as_str(), "test-group");
        // Should have one member
        assert_eq!(group.members.len(), 1);
        assert_eq!(
            group.members[0].member_id.as_str(),
            join_response.member_id.as_str()
        );
    }

    #[test]
    fn test_handle_list_groups_with_filter() {
        let mock = MockKafkaStore::new();
        let coord = create_test_coordinator();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coord, &broker, 1, Compression::None);

        // Create a group
        let _join_response = coordinator::handle_join_group(
            &ctx,
            "test-group".to_string(),
            "".to_string(),
            "test-client".to_string(),
            30000,
            60000,
            "consumer".to_string(),
            create_test_protocol(),
        )
        .unwrap();

        // List all groups (no filter)
        let list_response = coordinator::handle_list_groups(&ctx, vec![]).unwrap();

        assert_eq!(list_response.error_code, ERROR_NONE);
        assert_eq!(list_response.groups.len(), 1);
        assert_eq!(list_response.groups[0].group_id.0.as_str(), "test-group");

        // List with state filter that matches
        let list_stable = coordinator::handle_list_groups(
            &ctx,
            vec!["Stable".to_string(), "CompletingRebalance".to_string()],
        )
        .unwrap();

        // Group should be in CompletingRebalance or Stable state
        assert!(list_stable.groups.len() <= 1);

        // List with state filter that doesn't match
        let list_empty = coordinator::handle_list_groups(&ctx, vec!["Empty".to_string()]).unwrap();

        // Group is not empty (it has a member)
        assert_eq!(list_empty.groups.len(), 0);
    }

    // ========== InitProducerId Handler Tests (Phase 9) ==========

    use crate::kafka::handlers::init_producer_id;
    use crate::kafka::messages::ProducerMetadata;

    #[test]
    fn test_handle_init_producer_id_new_producer() {
        let mut mock = MockKafkaStore::new();

        // Expect allocation of new producer ID with epoch 0
        mock.expect_allocate_producer_id()
            .times(1)
            .returning(|_, _| Ok((1000, 0))); // producer_id=1000, epoch=0

        // Create HandlerContext AFTER setting expectations
        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        let response = init_producer_id::handle_init_producer_id(
            &ctx,
            None,  // No transactional ID
            60000, // Transaction timeout (unused for non-transactional)
            -1,    // New producer
            -1,    // New producer
            Some("test-client"),
        )
        .unwrap();

        assert_eq!(response.error_code, ERROR_NONE);
        assert_eq!(response.producer_id.0, 1000);
        assert_eq!(response.producer_epoch, 0);
    }

    #[test]
    fn test_handle_init_producer_id_existing_producer_reconnect() {
        let mut mock = MockKafkaStore::new();

        // First, check the current epoch
        mock.expect_get_producer_epoch()
            .times(1)
            .returning(|_| Ok(Some(0))); // Current epoch is 0

        // Then increment the epoch
        mock.expect_increment_producer_epoch()
            .times(1)
            .returning(|_| Ok(1)); // New epoch is 1

        // Create HandlerContext AFTER setting expectations
        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        let response = init_producer_id::handle_init_producer_id(
            &ctx,
            None,  // No transactional ID
            60000, // Transaction timeout (unused for non-transactional)
            1000,  // Existing producer ID
            0,     // Current epoch
            Some("test-client"),
        )
        .unwrap();

        assert_eq!(response.error_code, ERROR_NONE);
        assert_eq!(response.producer_id.0, 1000);
        assert_eq!(response.producer_epoch, 1); // Epoch was bumped
    }

    #[test]
    fn test_handle_init_producer_id_producer_fenced() {
        let mut mock = MockKafkaStore::new();

        // Return current epoch that is higher than the client's epoch
        mock.expect_get_producer_epoch()
            .times(1)
            .returning(|_| Ok(Some(5))); // Current epoch is 5, but client has 3

        // Create HandlerContext AFTER setting expectations
        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        let result = init_producer_id::handle_init_producer_id(
            &ctx,
            None,
            60000, // Transaction timeout (unused for non-transactional)
            1000,  // Producer ID
            3,     // Old epoch (current is 5)
            Some("test-client"),
        );

        assert!(result.is_err());
        match result.unwrap_err() {
            KafkaError::ProducerFenced {
                producer_id,
                epoch,
                expected_epoch,
            } => {
                assert_eq!(producer_id, 1000);
                assert_eq!(epoch, 3);
                assert_eq!(expected_epoch, 5);
            }
            e => panic!("Expected ProducerFenced error, got {:?}", e),
        }
    }

    #[test]
    fn test_handle_init_producer_id_unknown_producer() {
        let mut mock = MockKafkaStore::new();

        // Producer doesn't exist
        mock.expect_get_producer_epoch()
            .times(1)
            .returning(|_| Ok(None));

        // Create HandlerContext AFTER setting expectations
        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        let result = init_producer_id::handle_init_producer_id(
            &ctx,
            None,
            60000, // Transaction timeout (unused for non-transactional)
            9999,  // Unknown producer ID
            0,
            Some("test-client"),
        );

        assert!(result.is_err());
        match result.unwrap_err() {
            KafkaError::UnknownProducerId { producer_id } => {
                assert_eq!(producer_id, 9999);
            }
            e => panic!("Expected UnknownProducerId error, got {:?}", e),
        }
    }

    // ========== Produce Handler with Idempotency Tests (Phase 9) ==========

    #[test]
    fn test_handle_produce_with_idempotent_producer_success() {
        let mut mock = MockKafkaStore::new();

        // Expect topic lookup
        mock.expect_get_or_create_topic()
            .returning(|_, _| Ok((1, 1)));

        // Expect sequence check to pass (returns true = insert)
        mock.expect_check_and_update_sequence()
            .times(1)
            .returning(|_, _, _, _, _, _| Ok(true));

        // Expect record insertion
        mock.expect_insert_records()
            .times(1)
            .returning(|_, _, _| Ok(0));

        let topic_data = vec![TopicProduceData {
            name: "test-topic".to_string(),
            partitions: vec![PartitionProduceData {
                partition_index: 0,
                records: vec![Record {
                    key: Some(b"key1".to_vec()),
                    value: Some(b"value1".to_vec()),
                    headers: vec![],
                    timestamp: None,
                }],
                producer_metadata: None,
            }],
        }];

        let producer_metadata = ProducerMetadata {
            producer_id: 1000,
            producer_epoch: 0,
            base_sequence: 0,
        };

        // Create HandlerContext AFTER setting expectations
        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        let response =
            produce::handle_produce(&ctx, topic_data, Some(&producer_metadata), None).unwrap();

        assert_eq!(response.responses.len(), 1);
        let partition_response = &response.responses[0].partition_responses[0];
        assert_eq!(partition_response.error_code, ERROR_NONE);
        assert_eq!(partition_response.base_offset, 0);
    }

    #[test]
    fn test_handle_produce_duplicate_sequence_rejected() {
        let mut mock = MockKafkaStore::new();

        mock.expect_get_or_create_topic()
            .returning(|_, _| Ok((1, 1)));

        // Sequence check detects duplicate (returns Ok(false) - skip insert, return success)
        mock.expect_check_and_update_sequence()
            .times(1)
            .returning(|_, _, _, _, _, _| Ok(false)); // Duplicate - skip insert

        // No expect_insert_records - should be skipped for duplicates

        let topic_data = vec![TopicProduceData {
            name: "test-topic".to_string(),
            partitions: vec![PartitionProduceData {
                partition_index: 0,
                records: vec![Record {
                    key: None,
                    value: Some(b"test".to_vec()),
                    headers: vec![],
                    timestamp: None,
                }],
                producer_metadata: None,
            }],
        }];

        let producer_metadata = ProducerMetadata {
            producer_id: 1000,
            producer_epoch: 0,
            base_sequence: 5, // Duplicate - already processed
        };

        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        let response =
            produce::handle_produce(&ctx, topic_data, Some(&producer_metadata), None).unwrap();

        let partition_response = &response.responses[0].partition_responses[0];
        // Kafka spec: Duplicates return SUCCESS (error_code=0), not error
        assert_eq!(partition_response.error_code, ERROR_NONE);
        assert_eq!(partition_response.base_offset, 0); // Duplicate - offset 0
    }

    #[test]
    fn test_handle_produce_out_of_order_sequence_rejected() {
        let mut mock = MockKafkaStore::new();

        mock.expect_get_or_create_topic()
            .returning(|_, _| Ok((1, 1)));

        // Sequence check fails with out-of-order error
        mock.expect_check_and_update_sequence()
            .times(1)
            .returning(|_, _, _, _, _, _| Err(KafkaError::out_of_order_sequence(1000, 0, 15, 10)));

        let topic_data = vec![TopicProduceData {
            name: "test-topic".to_string(),
            partitions: vec![PartitionProduceData {
                partition_index: 0,
                records: vec![Record {
                    key: None,
                    value: Some(b"test".to_vec()),
                    headers: vec![],
                    timestamp: None,
                }],
                producer_metadata: None,
            }],
        }];

        let producer_metadata = ProducerMetadata {
            producer_id: 1000,
            producer_epoch: 0,
            base_sequence: 15, // Gap - expected 10
        };

        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        let response =
            produce::handle_produce(&ctx, topic_data, Some(&producer_metadata), None).unwrap();

        let partition_response = &response.responses[0].partition_responses[0];
        assert_eq!(
            partition_response.error_code,
            ERROR_OUT_OF_ORDER_SEQUENCE_NUMBER
        );
        assert_eq!(partition_response.base_offset, -1);
    }

    #[test]
    fn test_handle_produce_non_idempotent_skips_validation() {
        let mut mock = MockKafkaStore::new();

        mock.expect_get_or_create_topic()
            .returning(|_, _| Ok((1, 1)));

        // No sequence check expected when producer_id is -1
        mock.expect_insert_records()
            .times(1)
            .returning(|_, _, _| Ok(0));

        let topic_data = vec![TopicProduceData {
            name: "test-topic".to_string(),
            partitions: vec![PartitionProduceData {
                partition_index: 0,
                records: vec![Record {
                    key: None,
                    value: Some(b"test".to_vec()),
                    headers: vec![],
                    timestamp: None,
                }],
                producer_metadata: None,
            }],
        }];

        // Non-idempotent producer (producer_id = -1)
        let producer_metadata = ProducerMetadata {
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
        };

        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        let response =
            produce::handle_produce(&ctx, topic_data, Some(&producer_metadata), None).unwrap();

        let partition_response = &response.responses[0].partition_responses[0];
        assert_eq!(partition_response.error_code, ERROR_NONE);
    }

    // ========== Transaction Handler Tests (Phase 10) ==========

    use crate::kafka::handlers::transaction;
    use crate::kafka::storage::TransactionState;

    // ========== AddPartitionsToTxn Tests ==========

    #[test]
    fn test_add_partitions_success() {
        let mut mock = MockKafkaStore::new();

        // Atomic begin_or_continue_transaction handles state transition
        mock.expect_begin_or_continue_transaction()
            .times(1)
            .returning(|_, _, _| Ok(()));

        // Topic exists with 4 partitions
        mock.expect_get_topic_partition_count()
            .times(1)
            .returning(|_| Ok(Some(4)));

        // Create HandlerContext AFTER setting expectations
        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        let topics = vec![("test-topic".to_string(), vec![0, 1])];

        let response =
            transaction::handle_add_partitions_to_txn(&ctx, "txn-1", 1000, 0, topics).unwrap();

        // Should have one topic result
        assert_eq!(response.results_by_topic_v3_and_below.len(), 1);
        let topic_result = &response.results_by_topic_v3_and_below[0];
        assert_eq!(topic_result.results_by_partition.len(), 2);

        // Both partitions should succeed
        for partition_result in &topic_result.results_by_partition {
            assert_eq!(partition_result.partition_error_code, ERROR_NONE);
        }
    }

    #[test]
    fn test_add_partitions_invalid_partition() {
        let mut mock = MockKafkaStore::new();

        mock.expect_begin_or_continue_transaction()
            .returning(|_, _, _| Ok(()));
        // Topic has only 2 partitions
        mock.expect_get_topic_partition_count()
            .returning(|_| Ok(Some(2)));

        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        // Request partition 0 (valid) and partition 5 (invalid >= 2)
        let topics = vec![("test-topic".to_string(), vec![0, 5])];

        let response =
            transaction::handle_add_partitions_to_txn(&ctx, "txn-1", 1000, 0, topics).unwrap();

        let partitions = &response.results_by_topic_v3_and_below[0].results_by_partition;
        assert_eq!(partitions.len(), 2);

        // Partition 0 should succeed
        assert_eq!(partitions[0].partition_index, 0);
        assert_eq!(partitions[0].partition_error_code, ERROR_NONE);

        // Partition 5 should fail
        assert_eq!(partitions[1].partition_index, 5);
        assert_eq!(
            partitions[1].partition_error_code,
            ERROR_UNKNOWN_TOPIC_OR_PARTITION
        );
    }

    #[test]
    fn test_add_partitions_auto_creates_topic() {
        // AddPartitionsToTxn auto-creates topics (matching Kafka behavior)
        // because rdkafka calls this before the topic exists
        let mut mock = MockKafkaStore::new();

        mock.expect_begin_or_continue_transaction()
            .returning(|_, _, _| Ok(()));
        // Topic doesn't exist initially
        mock.expect_get_topic_partition_count()
            .returning(|_| Ok(None));
        // Handler auto-creates the topic
        mock.expect_get_or_create_topic()
            .returning(|_, _| Ok((1, 1))); // topic_id=1, partition_count=1

        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        let topics = vec![("new-topic".to_string(), vec![0])];

        let response =
            transaction::handle_add_partitions_to_txn(&ctx, "txn-1", 1000, 0, topics).unwrap();

        // Should succeed because topic was auto-created
        assert_eq!(
            response.results_by_topic_v3_and_below[0].results_by_partition[0].partition_error_code,
            ERROR_NONE
        );
    }

    #[test]
    fn test_add_partitions_invalid_state() {
        let mut mock = MockKafkaStore::new();

        // Atomic method returns error for invalid state
        mock.expect_begin_or_continue_transaction()
            .returning(|_, _, _| {
                Err(KafkaError::invalid_txn_state(
                    "txn-1",
                    "CompleteCommit",
                    "AddPartitionsToTxn",
                ))
            });

        // Create HandlerContext AFTER setting expectations
        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        let topics = vec![("test-topic".to_string(), vec![0])];

        let result = transaction::handle_add_partitions_to_txn(&ctx, "txn-1", 1000, 0, topics);

        assert!(result.is_err());
        match result.unwrap_err() {
            KafkaError::InvalidTxnState { .. } => {}
            e => panic!("Expected InvalidTransactionState error, got {:?}", e),
        }
    }

    #[test]
    fn test_add_partitions_txn_not_found() {
        let mut mock = MockKafkaStore::new();

        // Atomic method returns error for non-existent transaction
        mock.expect_begin_or_continue_transaction()
            .returning(|_, _, _| Err(KafkaError::transactional_id_not_found("unknown-txn")));

        // Create HandlerContext AFTER setting expectations
        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        let topics = vec![("test-topic".to_string(), vec![0])];

        let result =
            transaction::handle_add_partitions_to_txn(&ctx, "unknown-txn", 1000, 0, topics);

        assert!(result.is_err());
        match result.unwrap_err() {
            KafkaError::TransactionalIdNotFound { .. } => {}
            e => panic!("Expected TransactionalIdNotFound error, got {:?}", e),
        }
    }

    #[test]
    fn test_add_partitions_producer_fenced() {
        let mut mock = MockKafkaStore::new();

        // Atomic method returns ProducerFenced error
        mock.expect_begin_or_continue_transaction()
            .returning(|_, _, _| Err(KafkaError::producer_fenced(1000, 0, 1)));

        // Create HandlerContext AFTER setting expectations
        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        let topics = vec![("test-topic".to_string(), vec![0])];

        let result = transaction::handle_add_partitions_to_txn(&ctx, "txn-1", 1000, 0, topics);

        assert!(result.is_err());
        match result.unwrap_err() {
            KafkaError::ProducerFenced { .. } => {}
            e => panic!("Expected ProducerFenced error, got {:?}", e),
        }
    }

    // ========== AddOffsetsToTxn Tests ==========

    #[test]
    fn test_add_offsets_success() {
        let mut mock = MockKafkaStore::new();

        // Atomic begin_or_continue_transaction handles state transition
        mock.expect_begin_or_continue_transaction()
            .returning(|_, _, _| Ok(()));

        // Create HandlerContext AFTER setting expectations
        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        let response =
            transaction::handle_add_offsets_to_txn(&ctx, "txn-1", 1000, 0, "consumer-group")
                .unwrap();

        assert_eq!(response.error_code, ERROR_NONE);
    }

    #[test]
    fn test_add_offsets_invalid_state() {
        let mut mock = MockKafkaStore::new();

        // Atomic method returns error for invalid state
        mock.expect_begin_or_continue_transaction()
            .returning(|_, _, _| {
                Err(KafkaError::invalid_txn_state(
                    "txn-1",
                    "PrepareCommit",
                    "AddOffsetsToTxn",
                ))
            });

        // Create HandlerContext AFTER setting expectations
        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        let result =
            transaction::handle_add_offsets_to_txn(&ctx, "txn-1", 1000, 0, "consumer-group");

        assert!(result.is_err());
        match result.unwrap_err() {
            KafkaError::InvalidTxnState { .. } => {}
            e => panic!("Expected InvalidTransactionState error, got {:?}", e),
        }
    }

    #[test]
    fn test_add_offsets_txn_not_found() {
        let mut mock = MockKafkaStore::new();

        // Atomic method returns error for non-existent transaction
        mock.expect_begin_or_continue_transaction()
            .returning(|_, _, _| Err(KafkaError::transactional_id_not_found("unknown-txn")));

        // Create HandlerContext AFTER setting expectations
        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        let result =
            transaction::handle_add_offsets_to_txn(&ctx, "unknown-txn", 1000, 0, "consumer-group");

        assert!(result.is_err());
        match result.unwrap_err() {
            KafkaError::TransactionalIdNotFound { .. } => {}
            e => panic!("Expected TransactionalIdNotFound error, got {:?}", e),
        }
    }

    #[test]
    fn test_add_offsets_producer_fenced() {
        let mut mock = MockKafkaStore::new();

        // Atomic method returns ProducerFenced error
        mock.expect_begin_or_continue_transaction()
            .returning(|_, _, _| Err(KafkaError::producer_fenced(1000, 0, 1)));

        // Create HandlerContext AFTER setting expectations
        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        let result =
            transaction::handle_add_offsets_to_txn(&ctx, "txn-1", 1000, 0, "consumer-group");

        assert!(result.is_err());
        match result.unwrap_err() {
            KafkaError::ProducerFenced { .. } => {}
            e => panic!("Expected ProducerFenced error, got {:?}", e),
        }
    }

    // ========== EndTxn Tests ==========

    #[test]
    fn test_end_txn_commit_success() {
        let mut mock = MockKafkaStore::new();

        mock.expect_validate_transaction()
            .returning(|_, _, _| Ok(()));
        mock.expect_get_transaction_state()
            .returning(|_| Ok(Some(TransactionState::Ongoing)));
        mock.expect_commit_transaction()
            .times(1)
            .returning(|_, _, _| Ok(()));

        // Create HandlerContext AFTER setting expectations
        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        let response = transaction::handle_end_txn(
            &ctx, "txn-1", 1000, 0, true, // committed
        )
        .unwrap();

        assert_eq!(response.error_code, ERROR_NONE);
    }

    #[test]
    fn test_end_txn_abort_success() {
        let mut mock = MockKafkaStore::new();

        mock.expect_validate_transaction()
            .returning(|_, _, _| Ok(()));
        mock.expect_get_transaction_state()
            .returning(|_| Ok(Some(TransactionState::Ongoing)));
        mock.expect_abort_transaction()
            .times(1)
            .returning(|_, _, _| Ok(()));

        // Create HandlerContext AFTER setting expectations
        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        let response = transaction::handle_end_txn(
            &ctx, "txn-1", 1000, 0, false, // abort
        )
        .unwrap();

        assert_eq!(response.error_code, ERROR_NONE);
    }

    #[test]
    fn test_end_txn_empty_noop() {
        let mut mock = MockKafkaStore::new();

        mock.expect_validate_transaction()
            .returning(|_, _, _| Ok(()));
        // State is Empty - should be a no-op
        mock.expect_get_transaction_state()
            .returning(|_| Ok(Some(TransactionState::Empty)));

        // Neither commit_transaction nor abort_transaction should be called
        // (no expects set for them)

        // Create HandlerContext AFTER setting expectations
        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        let response = transaction::handle_end_txn(&ctx, "txn-1", 1000, 0, true).unwrap();

        assert_eq!(response.error_code, ERROR_NONE);
    }

    #[test]
    fn test_end_txn_invalid_state() {
        let mut mock = MockKafkaStore::new();

        mock.expect_validate_transaction()
            .returning(|_, _, _| Ok(()));
        // State is already CompleteCommit - can't end again
        mock.expect_get_transaction_state()
            .returning(|_| Ok(Some(TransactionState::CompleteCommit)));

        // Create HandlerContext AFTER setting expectations
        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        let result = transaction::handle_end_txn(&ctx, "txn-1", 1000, 0, true);

        assert!(result.is_err());
        match result.unwrap_err() {
            KafkaError::InvalidTxnState { .. } => {}
            e => panic!("Expected InvalidTransactionState error, got {:?}", e),
        }
    }

    #[test]
    fn test_end_txn_txn_not_found() {
        let mut mock = MockKafkaStore::new();

        mock.expect_validate_transaction()
            .returning(|_, _, _| Ok(()));
        mock.expect_get_transaction_state().returning(|_| Ok(None));

        // Create HandlerContext AFTER setting expectations
        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        let result = transaction::handle_end_txn(&ctx, "unknown-txn", 1000, 0, true);

        assert!(result.is_err());
        match result.unwrap_err() {
            KafkaError::TransactionalIdNotFound { .. } => {}
            e => panic!("Expected TransactionalIdNotFound error, got {:?}", e),
        }
    }

    #[test]
    fn test_end_txn_producer_fenced() {
        let mut mock = MockKafkaStore::new();

        mock.expect_validate_transaction()
            .returning(|_, _, _| Err(KafkaError::producer_fenced(1000, 0, 1)));

        // Create HandlerContext AFTER setting expectations
        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        let result = transaction::handle_end_txn(&ctx, "txn-1", 1000, 0, true);

        assert!(result.is_err());
        match result.unwrap_err() {
            KafkaError::ProducerFenced { .. } => {}
            e => panic!("Expected ProducerFenced error, got {:?}", e),
        }
    }

    // ========== TxnOffsetCommit Tests ==========

    #[test]
    fn test_txn_offset_commit_success() {
        let mut mock = MockKafkaStore::new();

        mock.expect_validate_transaction()
            .returning(|_, _, _| Ok(()));
        mock.expect_get_transaction_state()
            .returning(|_| Ok(Some(TransactionState::Ongoing)));
        mock.expect_get_topic_id().returning(|_| Ok(Some(1)));
        mock.expect_store_txn_pending_offset()
            .times(1)
            .returning(|_, _, _, _, _, _| Ok(()));

        // Create HandlerContext AFTER setting expectations
        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        let topics = vec![(
            "test-topic".to_string(),
            vec![(0, 100i64, Some("metadata".to_string()))],
        )];

        let response =
            transaction::handle_txn_offset_commit(&ctx, "txn-1", 1000, 0, "consumer-group", topics)
                .unwrap();

        assert_eq!(response.topics.len(), 1);
        assert_eq!(response.topics[0].partitions.len(), 1);
        assert_eq!(response.topics[0].partitions[0].error_code, ERROR_NONE);
    }

    #[test]
    fn test_txn_offset_commit_multiple_partitions() {
        let mut mock = MockKafkaStore::new();

        mock.expect_validate_transaction()
            .returning(|_, _, _| Ok(()));
        mock.expect_get_transaction_state()
            .returning(|_| Ok(Some(TransactionState::Ongoing)));
        mock.expect_get_topic_id().returning(|_| Ok(Some(1)));
        // Should be called 3 times, once for each partition
        mock.expect_store_txn_pending_offset()
            .times(3)
            .returning(|_, _, _, _, _, _| Ok(()));

        // Create HandlerContext AFTER setting expectations
        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        let topics = vec![(
            "test-topic".to_string(),
            vec![(0, 10i64, None), (1, 20i64, None), (2, 30i64, None)],
        )];

        let response =
            transaction::handle_txn_offset_commit(&ctx, "txn-1", 1000, 0, "consumer-group", topics)
                .unwrap();

        assert_eq!(response.topics[0].partitions.len(), 3);
        for partition in &response.topics[0].partitions {
            assert_eq!(partition.error_code, ERROR_NONE);
        }
    }

    #[test]
    fn test_txn_offset_commit_unknown_topic() {
        let mut mock = MockKafkaStore::new();

        mock.expect_validate_transaction()
            .returning(|_, _, _| Ok(()));
        mock.expect_get_transaction_state()
            .returning(|_| Ok(Some(TransactionState::Ongoing)));
        // Topic doesn't exist
        mock.expect_get_topic_id().returning(|_| Ok(None));

        // Create HandlerContext AFTER setting expectations
        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        let topics = vec![("unknown-topic".to_string(), vec![(0, 100i64, None)])];

        let response =
            transaction::handle_txn_offset_commit(&ctx, "txn-1", 1000, 0, "consumer-group", topics)
                .unwrap();

        assert_eq!(
            response.topics[0].partitions[0].error_code,
            ERROR_UNKNOWN_TOPIC_OR_PARTITION
        );
    }

    #[test]
    fn test_txn_offset_commit_invalid_state_empty() {
        let mut mock = MockKafkaStore::new();

        mock.expect_validate_transaction()
            .returning(|_, _, _| Ok(()));
        // State is Empty - TxnOffsetCommit requires Ongoing
        mock.expect_get_transaction_state()
            .returning(|_| Ok(Some(TransactionState::Empty)));

        // Create HandlerContext AFTER setting expectations
        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        let topics = vec![("test-topic".to_string(), vec![(0, 100i64, None)])];

        let result =
            transaction::handle_txn_offset_commit(&ctx, "txn-1", 1000, 0, "consumer-group", topics);

        assert!(result.is_err());
        match result.unwrap_err() {
            KafkaError::InvalidTxnState { .. } => {}
            e => panic!("Expected InvalidTransactionState error, got {:?}", e),
        }
    }

    #[test]
    fn test_txn_offset_commit_producer_fenced() {
        let mut mock = MockKafkaStore::new();

        mock.expect_validate_transaction()
            .returning(|_, _, _| Err(KafkaError::producer_fenced(1000, 0, 1)));

        // Create HandlerContext AFTER setting expectations
        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        let topics = vec![("test-topic".to_string(), vec![(0, 100i64, None)])];

        let result =
            transaction::handle_txn_offset_commit(&ctx, "txn-1", 1000, 0, "consumer-group", topics);

        assert!(result.is_err());
        match result.unwrap_err() {
            KafkaError::ProducerFenced { .. } => {}
            e => panic!("Expected ProducerFenced error, got {:?}", e),
        }
    }

    #[test]
    fn test_txn_offset_commit_store_error() {
        let mut mock = MockKafkaStore::new();

        mock.expect_validate_transaction()
            .returning(|_, _, _| Ok(()));
        mock.expect_get_transaction_state()
            .returning(|_| Ok(Some(TransactionState::Ongoing)));
        mock.expect_get_topic_id().returning(|_| Ok(Some(1)));
        // Store fails
        mock.expect_store_txn_pending_offset()
            .returning(|_, _, _, _, _, _| Err(KafkaError::database("store failed")));

        // Create HandlerContext AFTER setting expectations
        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        let topics = vec![("test-topic".to_string(), vec![(0, 100i64, None)])];

        let response =
            transaction::handle_txn_offset_commit(&ctx, "txn-1", 1000, 0, "consumer-group", topics)
                .unwrap();

        assert_eq!(
            response.topics[0].partitions[0].error_code,
            ERROR_UNKNOWN_SERVER_ERROR
        );
    }
}
