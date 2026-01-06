// Handler unit tests
//
// These tests verify handler logic in isolation using MockKafkaStore.
// Each test sets up mock expectations and verifies the handler produces
// correct responses for various scenarios.

#[cfg(test)]
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
            .with(mockall::predicate::eq("test-topic"))
            .times(1)
            .returning(|_| Ok(1));

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

        let response = produce::handle_produce(&mock, topic_data).unwrap();

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

        mock.expect_get_or_create_topic().returning(|_| Ok(1));

        // Request partition 5 which we don't support (only partition 0)
        let topic_data = vec![TopicProduceData {
            name: "test-topic".to_string(),
            partitions: vec![PartitionProduceData {
                partition_index: 5,
                records: vec![],
            }],
        }];

        let response = produce::handle_produce(&mock, topic_data).unwrap();

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

        mock.expect_get_or_create_topic().returning(|_| Ok(1));

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

        let response = produce::handle_produce(&mock, topic_data).unwrap();

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
            metadata::handle_metadata(&mock, None, "localhost".to_string(), 9092).unwrap();

        assert_eq!(response.topics.len(), 2);
        assert_eq!(response.brokers.len(), 1);
    }

    #[test]
    fn test_handle_metadata_specific_topic() {
        let mut mock = MockKafkaStore::new();

        mock.expect_get_or_create_topic()
            .with(mockall::predicate::eq("my-topic"))
            .returning(|_| Ok(5));

        let response = metadata::handle_metadata(
            &mock,
            Some(vec!["my-topic".to_string()]),
            "localhost".to_string(),
            9092,
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
}
