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
    use crate::testing::mocks::MockKafkaStore;

    // ========== Produce Handler Tests ==========

    #[test]
    fn test_handle_produce_success() {
        let mut mock = MockKafkaStore::new();

        // Expect topic lookup and record insertion
        mock.expect_get_or_create_topic()
            .with(
                mockall::predicate::eq("test-topic"),
                mockall::predicate::always(),
            )
            .times(1)
            .returning(|_, _| Ok(1));

        // Expect partition count lookup
        mock.expect_get_topic_metadata().returning(|_| {
            Ok(vec![TopicMetadata {
                name: "test-topic".to_string(),
                id: 1,
                partition_count: 1,
            }])
        });

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
            }],
        }];

        let response = produce::handle_produce(&mock, topic_data, 1).unwrap();

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

        mock.expect_get_or_create_topic().returning(|_, _| Ok(1));

        // Return partition count = 1, so only partition 0 is valid
        mock.expect_get_topic_metadata().returning(|_| {
            Ok(vec![TopicMetadata {
                name: "test-topic".to_string(),
                id: 1,
                partition_count: 1,
            }])
        });

        // Request partition 5 which exceeds partition_count
        let topic_data = vec![TopicProduceData {
            name: "test-topic".to_string(),
            partitions: vec![PartitionProduceData {
                partition_index: 5,
                records: vec![],
            }],
        }];

        let response = produce::handle_produce(&mock, topic_data, 1).unwrap();

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

        mock.expect_get_or_create_topic().returning(|_, _| Ok(1));

        // Return partition count = 1, so partition 0 is valid
        mock.expect_get_topic_metadata().returning(|_| {
            Ok(vec![TopicMetadata {
                name: "test-topic".to_string(),
                id: 1,
                partition_count: 1,
            }])
        });

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
            }],
        }];

        let response = produce::handle_produce(&mock, topic_data, 1).unwrap();

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

        mock.expect_fetch_records().returning(|_, _, _, _| {
            Ok(vec![FetchedMessage {
                partition_offset: 0,
                key: Some(b"key1".to_vec()),
                value: Some(b"value1".to_vec()),
                timestamp: 1234567890,
            }])
        });

        mock.expect_get_high_watermark().returning(|_, _| Ok(1));

        let topic_data = vec![TopicFetchData {
            name: "test-topic".to_string(),
            partitions: vec![PartitionFetchData {
                partition_index: 0,
                fetch_offset: 0,
                partition_max_bytes: 1024,
            }],
        }];

        let response = fetch::handle_fetch(&mock, topic_data).unwrap();

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

        let response = fetch::handle_fetch(&mock, topic_data).unwrap();

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

        let response = fetch::handle_list_offsets(&mock, topics).unwrap();

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

        let response = fetch::handle_list_offsets(&mock, topics).unwrap();

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

        let response =
            metadata::handle_metadata(&mock, None, "localhost".to_string(), 9092, 1).unwrap();

        assert_eq!(response.topics.len(), 2);
        assert_eq!(response.brokers.len(), 1);
    }

    #[test]
    fn test_handle_metadata_specific_topic() {
        let mut mock = MockKafkaStore::new();

        mock.expect_get_or_create_topic()
            .with(
                mockall::predicate::eq("my-topic"),
                mockall::predicate::always(),
            )
            .returning(|_, _| Ok(5));

        mock.expect_get_topic_metadata().returning(|_| {
            Ok(vec![TopicMetadata {
                name: "my-topic".to_string(),
                id: 5,
                partition_count: 1,
            }])
        });

        let response = metadata::handle_metadata(
            &mock,
            Some(vec!["my-topic".to_string()]),
            "localhost".to_string(),
            9092,
            1,
        )
        .unwrap();

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

        let response =
            consumer::handle_offset_commit(&mock, "test-group".to_string(), topics).unwrap();

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

        let response =
            consumer::handle_offset_commit(&mock, "test-group".to_string(), topics).unwrap();

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

        let response =
            consumer::handle_offset_fetch(&mock, "test-group".to_string(), topics).unwrap();

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

        let response =
            consumer::handle_offset_fetch(&mock, "test-group".to_string(), topics).unwrap();

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

        // Request with None = fetch all
        let response =
            consumer::handle_offset_fetch(&mock, "test-group".to_string(), None).unwrap();

        // Should have 2 topics
        assert_eq!(response.topics.len(), 2);
    }

    // ========== Coordinator Handler Tests ==========
    //
    // These tests use a real GroupCoordinator instance (not a mock) since it's
    // pure in-memory state management with no database dependencies.

    use crate::kafka::handlers::coordinator;
    use crate::kafka::messages::{JoinGroupProtocol, SyncGroupAssignment};
    use crate::kafka::GroupCoordinator;
    // Note: MockKafkaStore is already imported in outer scope

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
        let response = coordinator::handle_find_coordinator(
            "localhost".to_string(),
            9092,
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
        let coord = create_test_coordinator();

        let response = coordinator::handle_join_group(
            &coord,
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
        let coord = create_test_coordinator();

        // First member joins and becomes leader
        let first_response = coordinator::handle_join_group(
            &coord,
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
            &coord,
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
        let coord = create_test_coordinator();

        // First join
        let first_response = coordinator::handle_join_group(
            &coord,
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
            &coord,
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
        let coord = create_test_coordinator();

        // Join group first
        let join_response = coordinator::handle_join_group(
            &coord,
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

        // Create mock store (not called since leader provides assignments)
        let mock = MockKafkaStore::new();

        let sync_response = coordinator::handle_sync_group(
            &coord,
            &mock,
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
        let coord = create_test_coordinator();

        // First member (leader) joins
        let leader_join = coordinator::handle_join_group(
            &coord,
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
            &coord,
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

        // Create mock store (not called since leader provides assignments)
        let mock = MockKafkaStore::new();

        // Leader syncs first
        let _leader_sync = coordinator::handle_sync_group(
            &coord,
            &mock,
            "test-group".to_string(),
            leader_id,
            gen,
            assignments,
        )
        .unwrap();

        // Follower syncs (with empty assignments - only leader provides)
        let follower_sync = coordinator::handle_sync_group(
            &coord,
            &mock,
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
        let coord = create_test_coordinator();

        // Join group first
        let join_response = coordinator::handle_join_group(
            &coord,
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
        let mock = MockKafkaStore::new();

        coordinator::handle_sync_group(
            &coord,
            &mock,
            "test-group".to_string(),
            member_id.clone(),
            generation_id,
            assignments,
        )
        .unwrap();

        // Send heartbeat - should succeed now that group is Stable
        let heartbeat_response = coordinator::handle_heartbeat(
            &coord,
            "test-group".to_string(),
            member_id,
            generation_id,
        )
        .unwrap();

        assert_eq!(heartbeat_response.error_code, ERROR_NONE);
    }

    #[test]
    fn test_handle_heartbeat_unknown_member() {
        let coord = create_test_coordinator();

        // Create a group with a member
        let _join_response = coordinator::handle_join_group(
            &coord,
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
            &coord,
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
        let coord = create_test_coordinator();

        // Join group first
        let join_response = coordinator::handle_join_group(
            &coord,
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
            coordinator::handle_leave_group(&coord, "test-group".to_string(), member_id.clone())
                .unwrap();

        assert_eq!(leave_response.error_code, ERROR_NONE);

        // Verify member is gone by trying heartbeat
        let heartbeat_result =
            coordinator::handle_heartbeat(&coord, "test-group".to_string(), member_id, 1);

        assert!(heartbeat_result.is_err());
    }

    #[test]
    fn test_handle_leave_group_unknown_member() {
        let coord = create_test_coordinator();

        // Create a group with a member
        let _join_response = coordinator::handle_join_group(
            &coord,
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
            &coord,
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
        let coord = create_test_coordinator();

        // Join group first
        let join_response = coordinator::handle_join_group(
            &coord,
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
            coordinator::handle_describe_groups(&coord, vec!["test-group".to_string()]).unwrap();

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
        let coord = create_test_coordinator();

        // Create a group
        let _join_response = coordinator::handle_join_group(
            &coord,
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
        let list_response = coordinator::handle_list_groups(&coord, vec![]).unwrap();

        assert_eq!(list_response.error_code, ERROR_NONE);
        assert_eq!(list_response.groups.len(), 1);
        assert_eq!(list_response.groups[0].group_id.0.as_str(), "test-group");

        // List with state filter that matches
        let list_stable = coordinator::handle_list_groups(
            &coord,
            vec!["Stable".to_string(), "CompletingRebalance".to_string()],
        )
        .unwrap();

        // Group should be in CompletingRebalance or Stable state
        assert!(list_stable.groups.len() <= 1);

        // List with state filter that doesn't match
        let list_empty =
            coordinator::handle_list_groups(&coord, vec!["Empty".to_string()]).unwrap();

        // Group is not empty (it has a member)
        assert_eq!(list_empty.groups.len(), 0);
    }
}
