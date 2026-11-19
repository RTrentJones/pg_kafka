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
    use crate::kafka::storage::{CommittedOffset, FetchedMessage, IsolationLevel, TopicMetadata};
    use crate::kafka::{BrokerMetadata, GroupCoordinator, HandlerContext};
    use crate::testing::mocks::MockKafkaStore;
    use kafka_protocol::records::Compression;

    // ========== Produce Handler Tests ==========

    #[test]
    fn test_handle_produce_success() {
        let mut mock = MockKafkaStore::new();
        mock.expect_get_log_start_offset().returning(|_, _| Ok(0));

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

    #[test]
    fn test_handle_produce_txn_wrong_state_rejected() {
        // A transactional produce whose transaction is not Ongoing is rejected
        // with INVALID_TXN_STATE before any record is inserted (produce.rs gate).
        let mut mock = MockKafkaStore::new();
        mock.expect_validate_transaction()
            .returning(|_, _, _| Ok(()));
        mock.expect_get_transaction_state()
            .returning(|_| Ok(Some(crate::kafka::storage::TransactionState::PrepareCommit)));

        let topic_data = vec![TopicProduceData {
            name: "t".to_string(),
            partitions: vec![PartitionProduceData {
                partition_index: 0,
                records: vec![],
                producer_metadata: None,
            }],
        }];
        let meta = crate::kafka::messages::ProducerMetadata {
            producer_id: 100,
            producer_epoch: 0,
            base_sequence: 0,
        };

        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        // Reported on every requested partition, not returned as Err.
        let response = produce::handle_produce(&ctx, topic_data, Some(&meta), Some("tx1")).unwrap();
        let codes: Vec<i16> = response
            .responses
            .iter()
            .flat_map(|t| t.partition_responses.iter())
            .map(|p| p.error_code)
            .collect();
        assert!(!codes.is_empty());
        assert!(
            codes.iter().all(|c| *c == ERROR_INVALID_TXN_STATE),
            "{:?}",
            codes
        );
    }

    #[test]
    fn test_handle_produce_txn_id_not_found_rejected() {
        // A transactional produce for an unknown transactional id is rejected
        // with TRANSACTIONAL_ID_NOT_FOUND (produce.rs gate, None arm).
        let mut mock = MockKafkaStore::new();
        mock.expect_validate_transaction()
            .returning(|_, _, _| Ok(()));
        mock.expect_get_transaction_state().returning(|_| Ok(None));

        let topic_data = vec![TopicProduceData {
            name: "t".to_string(),
            partitions: vec![PartitionProduceData {
                partition_index: 0,
                records: vec![],
                producer_metadata: None,
            }],
        }];
        let meta = crate::kafka::messages::ProducerMetadata {
            producer_id: 100,
            producer_epoch: 0,
            base_sequence: 0,
        };

        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        // Reported on every requested partition, not returned as Err.
        let response = produce::handle_produce(&ctx, topic_data, Some(&meta), Some("tx1")).unwrap();
        let codes: Vec<i16> = response
            .responses
            .iter()
            .flat_map(|t| t.partition_responses.iter())
            .map(|p| p.error_code)
            .collect();
        assert!(!codes.is_empty());
        assert!(
            codes.iter().all(|c| *c == ERROR_TRANSACTIONAL_ID_NOT_FOUND),
            "{:?}",
            codes
        );
    }

    #[test]
    fn test_handle_produce_key_based_routing() {
        // partition_index == -1 routes each record by key via compute_partition
        // and produces to the resolved partitions (produce.rs routing branch).
        let mut mock = MockKafkaStore::new();
        mock.expect_get_log_start_offset().returning(|_, _| Ok(0));
        mock.expect_get_or_create_topic()
            .returning(|_, _| Ok((1, 3)));
        mock.expect_insert_records().returning(|_, _, _| Ok(0));

        let topic_data = vec![TopicProduceData {
            name: "t".to_string(),
            partitions: vec![PartitionProduceData {
                partition_index: -1,
                records: vec![
                    Record {
                        key: Some(b"a".to_vec()),
                        value: Some(b"1".to_vec()),
                        headers: vec![],
                        timestamp: None,
                    },
                    Record {
                        key: Some(b"b".to_vec()),
                        value: Some(b"2".to_vec()),
                        headers: vec![],
                        timestamp: None,
                    },
                ],
                producer_metadata: None,
            }],
        }];

        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        let response = produce::handle_produce(&ctx, topic_data, None, None).unwrap();
        let topic_response = &response.responses[0];
        assert!(!topic_response.partition_responses.is_empty());
        for pr in &topic_response.partition_responses {
            assert_eq!(pr.error_code, ERROR_NONE);
            assert!((0..3).contains(&pr.index));
        }
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
                    partition_offset: 5,
                    key: Some(b"key1".to_vec()),
                    value: Some(b"value1".to_vec()),
                    timestamp: 1234567890,
                    headers: vec![],
                    producer_id: None,
                    producer_epoch: None,
                }])
            });

        mock.expect_get_high_watermark().returning(|_, _| Ok(6));
        mock.expect_get_last_stable_offset().returning(|_, _| Ok(6));
        // RV-10: the fetch response's log_start_offset must reflect the store's
        // earliest retained offset, not a hardcoded 0.
        mock.expect_get_earliest_offset().returning(|_, _| Ok(5));
        mock.expect_get_log_start_offset().returning(|_, _| Ok(5));

        let topic_data = vec![TopicFetchData {
            name: "test-topic".to_string(),
            partitions: vec![PartitionFetchData {
                partition_index: 0,
                fetch_offset: 5,
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
        assert_eq!(partition_data.high_watermark, 6);
        assert_eq!(partition_data.log_start_offset, 5);
    }

    /// DR-8 (DEEP-REVIEW-2026-07): stored headers must be re-encoded into the fetch
    /// response's RecordBatch. Before the fix, the handler set `headers:
    /// Default::default()` unconditionally, so consumers always saw empty headers.
    #[test]
    fn test_handle_fetch_returns_headers() {
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
                    headers: vec![("trace-id".to_string(), b"abc123".to_vec())],
                    producer_id: None,
                    producer_epoch: None,
                }])
            });

        mock.expect_get_high_watermark().returning(|_, _| Ok(1));
        mock.expect_get_last_stable_offset().returning(|_, _| Ok(1));
        mock.expect_get_earliest_offset().returning(|_, _| Ok(0));
        mock.expect_get_log_start_offset().returning(|_, _| Ok(0));

        let topic_data = vec![TopicFetchData {
            name: "test-topic".to_string(),
            partitions: vec![PartitionFetchData {
                partition_index: 0,
                fetch_offset: 0,
                partition_max_bytes: 1024,
            }],
        }];

        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        let response = fetch::handle_fetch(
            &ctx,
            topic_data,
            crate::kafka::storage::IsolationLevel::ReadUncommitted,
        )
        .unwrap();

        // Decode the encoded RecordBatch and assert the header survived.
        let partition_data = &response.responses[0].partitions[0];
        assert_eq!(partition_data.error_code, ERROR_NONE);
        let mut records_buf = partition_data.records.clone().expect("records present");
        let decoded =
            kafka_protocol::records::RecordBatchDecoder::decode(&mut records_buf).unwrap();
        assert_eq!(decoded.records.len(), 1);
        let record = &decoded.records[0];
        let header_value = record
            .headers
            .get(&kafka_protocol::protocol::StrBytes::from_static_str(
                "trace-id",
            ))
            .cloned()
            .flatten()
            .expect("trace-id header present in fetched record");
        assert_eq!(header_value.as_ref(), b"abc123");
    }

    /// DR-10 (DEEP-REVIEW-2026-07): a storage error computing the HWM must surface
    /// as a per-partition error code — not be swallowed into HWM=0, which a consumer
    /// reads as "partition is empty".
    #[test]
    fn test_handle_fetch_hwm_error_returns_partition_error() {
        let mut mock = MockKafkaStore::new();

        mock.expect_get_topic_metadata().returning(|_| {
            Ok(vec![TopicMetadata {
                name: "test-topic".to_string(),
                id: 1,
                partition_count: 1,
            }])
        });

        mock.expect_fetch_records_with_isolation()
            .returning(|_, _, _, _, _| Ok(vec![]));
        mock.expect_get_high_watermark()
            .returning(|_, _| Err(crate::kafka::error::KafkaError::Internal("db down".into())));

        let topic_data = vec![TopicFetchData {
            name: "test-topic".to_string(),
            partitions: vec![PartitionFetchData {
                partition_index: 0,
                fetch_offset: 0,
                partition_max_bytes: 1024,
            }],
        }];

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
        assert_ne!(
            partition_data.error_code, ERROR_NONE,
            "storage error must not be reported as an empty partition"
        );
        assert_eq!(partition_data.high_watermark, -1);
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

    #[test]
    fn test_handle_fetch_storage_error() {
        // A storage failure while fetching surfaces the mapped Kafka error code
        // and a sentinel high_watermark of -1 for that partition (fetch.rs error
        // arm), rather than failing the whole request.
        let mut mock = MockKafkaStore::new();

        mock.expect_get_topic_metadata().returning(|_| {
            Ok(vec![TopicMetadata {
                name: "test-topic".to_string(),
                id: 1,
                partition_count: 1,
            }])
        });

        mock.expect_get_high_watermark().returning(|_, _| Ok(1));
        mock.expect_get_earliest_offset().returning(|_, _| Ok(0));
        mock.expect_get_log_start_offset().returning(|_, _| Ok(0));
        mock.expect_fetch_records_with_isolation()
            .returning(|_, _, _, _, _| {
                Err(KafkaError::Database {
                    message: "boom".to_string(),
                })
            });

        let topic_data = vec![TopicFetchData {
            name: "test-topic".to_string(),
            partitions: vec![PartitionFetchData {
                partition_index: 0,
                fetch_offset: 0,
                partition_max_bytes: 1024,
            }],
        }];

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
        assert_eq!(partition_data.error_code, ERROR_UNKNOWN_SERVER_ERROR);
        assert_eq!(partition_data.high_watermark, -1);
    }

    #[test]
    fn test_handle_fetch_read_committed_uses_lso() {
        // Under ReadCommitted the response's last_stable_offset comes from the
        // store's LSO (fetch.rs ReadCommitted arm), not the high watermark; an
        // empty result set still returns ERROR_NONE with empty record bytes.
        let mut mock = MockKafkaStore::new();

        mock.expect_get_topic_metadata().returning(|_| {
            Ok(vec![TopicMetadata {
                name: "test-topic".to_string(),
                id: 1,
                partition_count: 1,
            }])
        });

        mock.expect_fetch_records_with_isolation()
            .returning(|_, _, _, _, _| Ok(vec![]));
        mock.expect_get_high_watermark().returning(|_, _| Ok(10));
        mock.expect_get_last_stable_offset().returning(|_, _| Ok(7));
        mock.expect_get_earliest_offset().returning(|_, _| Ok(0));
        mock.expect_get_log_start_offset().returning(|_, _| Ok(0));

        let topic_data = vec![TopicFetchData {
            name: "test-topic".to_string(),
            partitions: vec![PartitionFetchData {
                partition_index: 0,
                fetch_offset: 0,
                partition_max_bytes: 1024,
            }],
        }];

        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        let response = fetch::handle_fetch(
            &ctx,
            topic_data,
            crate::kafka::storage::IsolationLevel::ReadCommitted,
        )
        .unwrap();

        let partition_data = &response.responses[0].partitions[0];
        assert_eq!(partition_data.error_code, ERROR_NONE);
        assert_eq!(partition_data.high_watermark, 10);
        assert_eq!(partition_data.last_stable_offset, 7);
        assert!(partition_data.records.is_some());
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

        let response = fetch::handle_list_offsets(
            &ctx,
            topics,
            crate::kafka::storage::IsolationLevel::ReadUncommitted,
        )
        .unwrap();

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

        let response = fetch::handle_list_offsets(
            &ctx,
            topics,
            crate::kafka::storage::IsolationLevel::ReadUncommitted,
        )
        .unwrap();

        let partition_response = &response.topics[0].partitions[0];
        assert_eq!(partition_response.error_code, ERROR_NONE);
        assert_eq!(partition_response.offset, 100);
    }

    #[test]
    fn test_handle_list_offsets_by_timestamp() {
        // CONF-7: a positive timestamp resolves to the earliest qualifying offset (and its record
        // timestamp), instead of returning UNSUPPORTED_VERSION.
        let mut mock = MockKafkaStore::new();
        mock.expect_get_topic_metadata().returning(|_| {
            Ok(vec![TopicMetadata {
                name: "test-topic".to_string(),
                id: 1,
                partition_count: 1,
            }])
        });
        mock.expect_get_offset_for_timestamp()
            .returning(|_, _, _| Ok(Some((7, 1_600_000_002_000))));
        mock.expect_get_high_watermark().returning(|_, _| Ok(10));

        let topics = vec![ListOffsetsTopicData {
            name: "test-topic".to_string(),
            partitions: vec![ListOffsetsPartitionData {
                partition_index: 0,
                current_leader_epoch: -1,
                timestamp: 1_600_000_001_500, // a real (positive) timestamp
            }],
        }];

        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        let response = fetch::handle_list_offsets(
            &ctx,
            topics,
            crate::kafka::storage::IsolationLevel::ReadUncommitted,
        )
        .unwrap();

        let partition_response = &response.topics[0].partitions[0];
        assert_eq!(partition_response.error_code, ERROR_NONE);
        assert_eq!(partition_response.offset, 7);
        assert_eq!(partition_response.timestamp, 1_600_000_002_000);
    }

    /// wave3 txn: one ListOffsets partition query against a 2-partition topic with
    /// HWM 10, LSO 4 (a transaction open at offset 4), a timestamp match at offset 6
    /// and the max-timestamp record at offset 3.
    fn list_one(partition: i32, timestamp: i64, isolation: IsolationLevel) -> (i16, i64, i64) {
        let mut mock = MockKafkaStore::new();
        mock.expect_get_topic_metadata().returning(|_| {
            Ok(vec![TopicMetadata {
                name: "t".to_string(),
                id: 1,
                partition_count: 2,
            }])
        });
        mock.expect_get_high_watermark().returning(|_, _| Ok(10));
        mock.expect_get_last_stable_offset().returning(|_, _| Ok(4));
        mock.expect_get_earliest_offset().returning(|_, _| Ok(0));
        mock.expect_get_offset_for_timestamp()
            .returning(|_, _, ts| {
                Ok(Some(if ts == crate::kafka::storage::MAX_TIMESTAMP {
                    (3, 9_000)
                } else {
                    (6, 5_000)
                }))
            });
        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);
        let topics = vec![ListOffsetsTopicData {
            name: "t".to_string(),
            partitions: vec![ListOffsetsPartitionData {
                partition_index: partition,
                current_leader_epoch: -1,
                timestamp,
            }],
        }];
        let r = fetch::handle_list_offsets(&ctx, topics, isolation).unwrap();
        let p = &r.topics[0].partitions[0];
        (p.error_code, p.offset, p.timestamp)
    }

    #[test]
    fn test_list_offsets_latest_honours_isolation_level() {
        // Before wave 3 read_committed LATEST returned the high watermark, so a
        // read_committed consumer saw lag it could never consume.
        assert_eq!(
            list_one(0, -1, IsolationLevel::ReadUncommitted),
            (0, 10, -1)
        );
        assert_eq!(list_one(0, -1, IsolationLevel::ReadCommitted), (0, 4, -1));
        assert_eq!(list_one(0, -2, IsolationLevel::ReadCommitted), (0, 0, -1));
    }

    #[test]
    fn test_list_offsets_max_timestamp() {
        // KIP-734: -3 is the record with the largest timestamp (was: treated as a
        // literal timestamp, i.e. the first record).
        assert_eq!(
            list_one(0, -3, IsolationLevel::ReadUncommitted),
            (0, 3, 9_000)
        );
    }

    #[test]
    fn test_list_offsets_timestamp_lookup_is_bounded_by_readable_end() {
        // Kafka: a timestamp match at or past the LSO (read_committed) is offset -1.
        assert_eq!(
            list_one(0, 4_000, IsolationLevel::ReadUncommitted),
            (0, 6, 5_000)
        );
        assert_eq!(
            list_one(0, 4_000, IsolationLevel::ReadCommitted),
            (0, -1, -1)
        );
    }

    #[test]
    fn test_list_offsets_unknown_partition() {
        // Was: error NONE, offset 0 for a partition the topic does not have.
        for ts in [-1, -2, -3, 1_000] {
            assert_eq!(
                list_one(7, ts, IsolationLevel::ReadUncommitted),
                (ERROR_UNKNOWN_TOPIC_OR_PARTITION, -1, -1)
            );
        }
        assert_eq!(
            list_one(-1, -1, IsolationLevel::ReadUncommitted),
            (ERROR_UNKNOWN_TOPIC_OR_PARTITION, -1, -1)
        );
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

        let response = metadata::handle_metadata(&ctx, None, true).unwrap();

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

        let response =
            metadata::handle_metadata(&ctx, Some(vec!["my-topic".to_string()]), true).unwrap();

        assert_eq!(response.topics.len(), 1);
    }

    #[test]
    fn test_handle_metadata_no_auto_create_missing_topic() {
        // CONF-2: with allow_auto_topic_creation = false, a missing requested topic must be reported
        // as UNKNOWN_TOPIC_OR_PARTITION and NOT created. Not setting expect_get_or_create_topic means
        // the mock panics if the handler tries to create — proving the no-create contract.
        let mut mock = MockKafkaStore::new();
        mock.expect_get_topic_metadata().returning(|_| Ok(vec![])); // topic doesn't exist

        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        let response =
            metadata::handle_metadata(&ctx, Some(vec!["missing".to_string()]), false).unwrap();

        assert_eq!(response.topics.len(), 1);
        assert_eq!(
            response.topics[0].error_code,
            ERROR_UNKNOWN_TOPIC_OR_PARTITION
        );
        assert!(response.topics[0].partitions.is_empty());
    }

    #[test]
    fn test_handle_api_versions_ok() {
        // handle_api_versions delegates to the response builder; it must report a
        // NONE error code so clients accept the advertised API range.
        let response = metadata::handle_api_versions();
        assert_eq!(response.error_code, ERROR_NONE);
    }

    #[test]
    fn test_handle_metadata_storage_error_propagates() {
        // A storage failure listing all topics propagates as a handler error
        // rather than a partial/empty success (metadata.rs error path).
        let mut mock = MockKafkaStore::new();
        mock.expect_get_topic_metadata().returning(|_| {
            Err(KafkaError::Database {
                message: "boom".to_string(),
            })
        });

        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        let result = metadata::handle_metadata(&ctx, None, true);
        assert!(result.is_err());
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

        let response = consumer::handle_offset_commit(
            &ctx,
            "test-group".to_string(),
            -1,
            String::new(),
            topics,
        )
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

        let response = consumer::handle_offset_commit(
            &ctx,
            "test-group".to_string(),
            -1,
            String::new(),
            topics,
        )
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

    // ========== Produce Handler with Idempotency Tests (Phase 9) ==========

    #[test]
    fn test_handle_produce_with_idempotent_producer_success() {
        let mut mock = MockKafkaStore::new();
        mock.expect_get_log_start_offset().returning(|_, _| Ok(0));

        // Expect topic lookup
        mock.expect_get_or_create_topic()
            .returning(|_, _| Ok((1, 1)));

        // Expect sequence check to pass (returns true = insert)
        mock.expect_check_and_update_sequence()
            .times(1)
            .returning(|_, _, _, _, _, _| Ok(crate::kafka::storage::SequenceCheck::Append));

        // Expect record insertion
        mock.expect_insert_records()
            .times(1)
            .returning(|_, _, _| Ok(0));

        // RV-8: the producer batch is recorded (with its offset) after the insert
        mock.expect_record_producer_batch()
            .withf(|b| b.base_offset == 0 && b.first_sequence == 0 && b.last_sequence == 0)
            .times(1)
            .returning(|_| Ok(()));

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
    fn test_handle_produce_insert_failure_does_not_advance_sequence() {
        // RV-8: when the insert fails, the producer sequence must NOT advance, so a
        // retry re-inserts instead of being dropped as a false duplicate. No
        // record_producer_sequence expectation is set, so mockall fails on Drop if
        // the handler advances the sequence after a failed insert.
        let mut mock = MockKafkaStore::new();
        mock.expect_get_or_create_topic()
            .returning(|_, _| Ok((1, 1)));
        mock.expect_check_and_update_sequence()
            .times(1)
            .returning(|_, _, _, _, _, _| Ok(crate::kafka::storage::SequenceCheck::Append));
        mock.expect_insert_records()
            .times(1)
            .returning(|_, _, _| Err(KafkaError::database("insert boom")));
        // Intentionally NO expect_record_producer_batch — it must not be called.

        let topic_data = vec![TopicProduceData {
            name: "test-topic".to_string(),
            partitions: vec![PartitionProduceData {
                partition_index: 0,
                records: vec![Record {
                    key: None,
                    value: Some(b"v".to_vec()),
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
        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        let response =
            produce::handle_produce(&ctx, topic_data, Some(&producer_metadata), None).unwrap();
        let partition_response = &response.responses[0].partition_responses[0];
        // The insert error is surfaced and the sequence was not advanced.
        assert_ne!(partition_response.error_code, ERROR_NONE);
    }

    #[test]
    fn test_handle_produce_duplicate_sequence_rejected() {
        let mut mock = MockKafkaStore::new();
        mock.expect_get_log_start_offset().returning(|_, _| Ok(0));

        mock.expect_get_or_create_topic()
            .returning(|_, _| Ok((1, 1)));

        // Sequence check detects duplicate (returns Ok(false) - skip insert, return success)
        mock.expect_check_and_update_sequence()
            .times(1)
            .returning(|_, _, _, _, _, _| {
                Ok(crate::kafka::storage::SequenceCheck::Duplicate { base_offset: 42 })
            }); // Duplicate - skip insert

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
        assert_eq!(partition_response.base_offset, 42); // wave 3: the original offset
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
        mock.expect_get_log_start_offset().returning(|_, _| Ok(0));

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

    /// Every known topic has `partitions` partitions (id 1); names starting with
    /// "missing" do not exist.
    fn topics_with_partitions(mock: &mut MockKafkaStore, partitions: i32) {
        mock.expect_get_topic_metadata().returning(move |names| {
            Ok(names
                .into_iter()
                .flatten()
                .filter(|n| !n.starts_with("missing"))
                .map(|n| crate::kafka::storage::TopicMetadata {
                    name: n.clone(),
                    id: 1,
                    partition_count: partitions,
                })
                .collect())
        });
    }

    fn add_partitions_codes(
        response: &kafka_protocol::messages::add_partitions_to_txn_response::AddPartitionsToTxnResponse,
    ) -> Vec<i16> {
        response
            .results_by_topic_v3_and_below
            .iter()
            .flat_map(|t| t.results_by_partition.iter())
            .map(|p| p.partition_error_code)
            .collect()
    }

    #[test]
    fn test_add_partitions_success() {
        let mut mock = MockKafkaStore::new();

        // Atomic begin_or_continue_transaction handles state transition
        mock.expect_begin_or_continue_transaction()
            .times(1)
            .returning(|_, _, _| Ok(()));
        topics_with_partitions(&mut mock, 4);
        // Wave 3: the partitions are registered for the transactional produce check.
        mock.expect_add_txn_partitions()
            .withf(|txn, parts| txn == "txn-1" && parts == [(1, 0), (1, 1)])
            .times(1)
            .returning(|_, _| Ok(()));

        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        let topics = vec![("test-topic".to_string(), vec![0, 1])];

        let response =
            transaction::handle_add_partitions_to_txn(&ctx, "txn-1", 1000, 0, topics).unwrap();

        assert_eq!(response.results_by_topic_v3_and_below.len(), 1);
        assert_eq!(
            add_partitions_codes(&response),
            vec![ERROR_NONE, ERROR_NONE]
        );
    }

    #[test]
    fn test_add_partitions_invalid_partition() {
        // Wave 3 (Kafka): one unknown partition fails the request: it gets
        // UNKNOWN_TOPIC_OR_PARTITION, the valid ones OPERATION_NOT_ATTEMPTED, and
        // neither the transaction state nor the registry is touched (no expectations).
        let mut mock = MockKafkaStore::new();
        topics_with_partitions(&mut mock, 2);

        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        let topics = vec![("test-topic".to_string(), vec![0, 5])];
        let response =
            transaction::handle_add_partitions_to_txn(&ctx, "txn-1", 1000, 0, topics).unwrap();

        let partitions = &response.results_by_topic_v3_and_below[0].results_by_partition;
        assert_eq!(partitions[0].partition_index, 0);
        assert_eq!(
            partitions[0].partition_error_code,
            crate::kafka::constants::ERROR_OPERATION_NOT_ATTEMPTED
        );
        assert_eq!(partitions[1].partition_index, 5);
        assert_eq!(
            partitions[1].partition_error_code,
            ERROR_UNKNOWN_TOPIC_OR_PARTITION
        );
    }

    #[test]
    fn test_add_partitions_unknown_topic_is_not_created() {
        // Wave 3: AddPartitionsToTxn used to auto-create a missing topic. Kafka answers
        // UNKNOWN_TOPIC_OR_PARTITION (clients create topics through Metadata); no
        // get_or_create_topic expectation, so creating it would fail the test.
        let mut mock = MockKafkaStore::new();
        topics_with_partitions(&mut mock, 1);

        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        let topics = vec![
            ("missing-topic".to_string(), vec![0]),
            ("test-topic".to_string(), vec![0]),
        ];
        let response =
            transaction::handle_add_partitions_to_txn(&ctx, "txn-1", 1000, 0, topics).unwrap();
        assert_eq!(
            add_partitions_codes(&response),
            vec![
                ERROR_UNKNOWN_TOPIC_OR_PARTITION,
                crate::kafka::constants::ERROR_OPERATION_NOT_ATTEMPTED
            ]
        );
    }

    fn add_partitions_with_begin_error(err: fn() -> KafkaError) -> Vec<i16> {
        let mut mock = MockKafkaStore::new();
        topics_with_partitions(&mut mock, 1);
        mock.expect_begin_or_continue_transaction()
            .returning(move |_, _, _| Err(err()));
        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);
        let topics = vec![("test-topic".to_string(), vec![0])];
        // The error is reported on every requested partition (v0-3 has no
        // top-level error code), not returned as Err.
        let response =
            transaction::handle_add_partitions_to_txn(&ctx, "txn-1", 1000, 0, topics).unwrap();
        add_partitions_codes(&response)
    }

    #[test]
    fn test_add_partitions_invalid_state() {
        let codes = add_partitions_with_begin_error(|| {
            KafkaError::invalid_txn_state("txn-1", "CompleteCommit", "AddPartitionsToTxn")
        });
        assert_eq!(codes, vec![ERROR_INVALID_TXN_STATE]);
    }

    #[test]
    fn test_add_partitions_txn_not_found() {
        let codes = add_partitions_with_begin_error(|| {
            KafkaError::transactional_id_not_found("unknown-txn")
        });
        assert_eq!(codes, vec![ERROR_TRANSACTIONAL_ID_NOT_FOUND]);
    }

    #[test]
    fn test_add_partitions_producer_fenced() {
        let codes = add_partitions_with_begin_error(|| KafkaError::producer_fenced(1000, 0, 1));
        assert_eq!(codes, vec![ERROR_PRODUCER_FENCED]);
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
        // State is already CompleteCommit - aborting it now is an invalid transition.
        // (Committing it again is a retry and succeeds; see
        // test_end_txn_retry_with_same_outcome_succeeds.)
        mock.expect_get_transaction_state()
            .returning(|_| Ok(Some(TransactionState::CompleteCommit)));

        // Create HandlerContext AFTER setting expectations
        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        let result = transaction::handle_end_txn(&ctx, "txn-1", 1000, 0, false);

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
        mock.expect_get_topic_partition_count()
            .returning(|_| Ok(Some(3)));
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

        let response = transaction::handle_txn_offset_commit(
            &ctx,
            "txn-1",
            1000,
            0,
            "consumer-group",
            -1,
            "",
            topics,
        )
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
        mock.expect_get_topic_partition_count()
            .returning(|_| Ok(Some(3)));
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

        let response = transaction::handle_txn_offset_commit(
            &ctx,
            "txn-1",
            1000,
            0,
            "consumer-group",
            -1,
            "",
            topics,
        )
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

        let response = transaction::handle_txn_offset_commit(
            &ctx,
            "txn-1",
            1000,
            0,
            "consumer-group",
            -1,
            "",
            topics,
        )
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

        let result = transaction::handle_txn_offset_commit(
            &ctx,
            "txn-1",
            1000,
            0,
            "consumer-group",
            -1,
            "",
            topics,
        );

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

        let result = transaction::handle_txn_offset_commit(
            &ctx,
            "txn-1",
            1000,
            0,
            "consumer-group",
            -1,
            "",
            topics,
        );

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
        mock.expect_get_topic_partition_count()
            .returning(|_| Ok(Some(3)));
        // Store fails
        mock.expect_store_txn_pending_offset()
            .returning(|_, _, _, _, _, _| Err(KafkaError::database("store failed")));

        // Create HandlerContext AFTER setting expectations
        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        let topics = vec![("test-topic".to_string(), vec![(0, 100i64, None)])];

        let response = transaction::handle_txn_offset_commit(
            &ctx,
            "txn-1",
            1000,
            0,
            "consumer-group",
            -1,
            "",
            topics,
        )
        .unwrap();

        assert_eq!(
            response.topics[0].partitions[0].error_code,
            ERROR_UNKNOWN_SERVER_ERROR
        );
    }

    // ========== DescribeConfigs Handler Tests (API 32) ==========

    use crate::kafka::handlers::configs;

    const RESOURCE_TYPE_TOPIC: i8 = 2;
    const RESOURCE_TYPE_BROKER: i8 = 4;
    const OP_SET: i8 = 0;
    const OP_DELETE: i8 = 1;
    const OP_APPEND: i8 = 2;

    fn find_config<'a>(
        result: &'a kafka_protocol::messages::describe_configs_response::DescribeConfigsResult,
        name: &str,
    ) -> Option<
        &'a kafka_protocol::messages::describe_configs_response::DescribeConfigsResourceResult,
    > {
        result.configs.iter().find(|c| c.name.as_str() == name)
    }

    /// A topic with a retention override reports it as a DYNAMIC_TOPIC_CONFIG
    /// with the override value; without one, the global GUC value is reported
    /// as a DEFAULT config (-1 when the global sweep is off).
    #[test]
    fn test_handle_describe_configs_retention_sources() {
        let mut mock = MockKafkaStore::new();
        mock.expect_get_topic_id().returning(|name| {
            Ok(if name == "with-override" {
                Some(1)
            } else {
                Some(2)
            })
        });
        mock.expect_get_topic_retention_ms()
            .returning(|topic_id| Ok(if topic_id == 1 { Some(60_000) } else { None }));

        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        let response = configs::handle_describe_configs(
            &ctx,
            vec![
                (RESOURCE_TYPE_TOPIC, "with-override".to_string(), None),
                (RESOURCE_TYPE_TOPIC, "no-override".to_string(), None),
            ],
            0, // global retention disabled
        )
        .unwrap();

        assert_eq!(response.results.len(), 2);
        let with_override = &response.results[0];
        assert_eq!(with_override.error_code, ERROR_NONE);
        let retention = find_config(with_override, "retention.ms").expect("retention.ms present");
        assert_eq!(retention.value.as_ref().unwrap().as_str(), "60000");
        assert_eq!(retention.config_source, 1); // DYNAMIC_TOPIC_CONFIG

        let no_override = &response.results[1];
        let retention = find_config(no_override, "retention.ms").expect("retention.ms present");
        assert_eq!(retention.value.as_ref().unwrap().as_str(), "-1"); // infinite
        assert_eq!(retention.config_source, 5); // DEFAULT_CONFIG
        assert!(find_config(no_override, "cleanup.policy").is_some());
    }

    /// Requesting specific configuration keys filters the result set.
    #[test]
    fn test_handle_describe_configs_key_filter() {
        let mut mock = MockKafkaStore::new();
        mock.expect_get_topic_id().returning(|_| Ok(Some(1)));
        mock.expect_get_topic_retention_ms().returning(|_| Ok(None));

        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        let response = configs::handle_describe_configs(
            &ctx,
            vec![(
                RESOURCE_TYPE_TOPIC,
                "t".to_string(),
                Some(vec!["cleanup.policy".to_string()]),
            )],
            0,
        )
        .unwrap();

        let result = &response.results[0];
        assert_eq!(result.configs.len(), 1);
        assert_eq!(result.configs[0].name.as_str(), "cleanup.policy");
    }

    /// Unknown topics and non-topic resources get per-resource errors.
    #[test]
    fn test_handle_describe_configs_errors() {
        let mut mock = MockKafkaStore::new();
        mock.expect_get_topic_id().returning(|_| Ok(None));

        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        let response = configs::handle_describe_configs(
            &ctx,
            vec![
                (RESOURCE_TYPE_TOPIC, "ghost".to_string(), None),
                (RESOURCE_TYPE_BROKER, "1".to_string(), None),
            ],
            0,
        )
        .unwrap();

        assert_eq!(
            response.results[0].error_code,
            ERROR_UNKNOWN_TOPIC_OR_PARTITION
        );
        assert_eq!(response.results[1].error_code, ERROR_INVALID_REQUEST);
    }

    // ========== IncrementalAlterConfigs Handler Tests (API 44) ==========

    /// SET persists the parsed retention.ms; DELETE clears the override.
    #[test]
    fn test_handle_incremental_alter_configs_set_and_delete() {
        let mut mock = MockKafkaStore::new();
        mock.expect_get_topic_id().returning(|_| Ok(Some(7)));
        mock.expect_set_topic_retention_ms()
            .with(
                mockall::predicate::eq(7),
                mockall::predicate::eq(Some(123_000i64)),
            )
            .times(1)
            .returning(|_, _| Ok(()));
        mock.expect_set_topic_retention_ms()
            .with(mockall::predicate::eq(7), mockall::predicate::eq(None))
            .times(1)
            .returning(|_, _| Ok(()));

        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        let response = configs::handle_incremental_alter_configs(
            &ctx,
            vec![
                (
                    RESOURCE_TYPE_TOPIC,
                    "t".to_string(),
                    vec![(
                        "retention.ms".to_string(),
                        OP_SET,
                        Some("123000".to_string()),
                    )],
                ),
                (
                    RESOURCE_TYPE_TOPIC,
                    "t".to_string(),
                    vec![("retention.ms".to_string(), OP_DELETE, None)],
                ),
            ],
            false,
        )
        .unwrap();

        assert_eq!(response.responses[0].error_code, ERROR_NONE);
        assert_eq!(response.responses[1].error_code, ERROR_NONE);
    }

    /// validate_only runs the checks but must not write anything.
    #[test]
    fn test_handle_incremental_alter_configs_validate_only() {
        let mut mock = MockKafkaStore::new();
        mock.expect_get_topic_id().returning(|_| Ok(Some(7)));
        mock.expect_set_topic_retention_ms().times(0);

        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        let response = configs::handle_incremental_alter_configs(
            &ctx,
            vec![(
                RESOURCE_TYPE_TOPIC,
                "t".to_string(),
                vec![("retention.ms".to_string(), OP_SET, Some("1000".to_string()))],
            )],
            true,
        )
        .unwrap();
        assert_eq!(response.responses[0].error_code, ERROR_NONE);
    }

    /// Unsupported keys, bad values, and unsupported operations are rejected
    /// with INVALID_CONFIG and nothing is applied (all-or-nothing).
    #[test]
    fn test_handle_incremental_alter_configs_rejections() {
        let mut mock = MockKafkaStore::new();
        mock.expect_get_topic_id().returning(|_| Ok(Some(7)));
        mock.expect_set_topic_retention_ms().times(0);

        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        let response = configs::handle_incremental_alter_configs(
            &ctx,
            vec![
                (
                    RESOURCE_TYPE_TOPIC,
                    "t".to_string(),
                    vec![(
                        "max.message.bytes".to_string(),
                        OP_SET,
                        Some("1".to_string()),
                    )],
                ),
                (
                    RESOURCE_TYPE_TOPIC,
                    "t".to_string(),
                    vec![("retention.ms".to_string(), OP_SET, Some("abc".to_string()))],
                ),
                (
                    RESOURCE_TYPE_TOPIC,
                    "t".to_string(),
                    // Valid SET followed by an invalid APPEND: the valid one
                    // must NOT be applied (all-or-nothing per resource).
                    vec![
                        ("retention.ms".to_string(), OP_SET, Some("1000".to_string())),
                        ("retention.ms".to_string(), OP_APPEND, Some("1".to_string())),
                    ],
                ),
            ],
            false,
        )
        .unwrap();

        for resp in &response.responses {
            assert_eq!(resp.error_code, ERROR_INVALID_CONFIG);
        }
    }

    // ========== DeleteRecords Handler Tests (API 21) ==========

    /// Happy path: rows below the offset are deleted and the new low
    /// watermark reported; offset -1 truncates to the high watermark.
    #[test]
    fn test_handle_delete_records_success_and_minus_one() {
        let mut mock = MockKafkaStore::new();
        mock.expect_get_topic_id().returning(|_| Ok(Some(3)));
        mock.expect_get_topic_partition_count()
            .returning(|_| Ok(Some(1)));
        mock.expect_get_high_watermark().returning(|_, _| Ok(10));
        mock.expect_delete_records_before()
            .with(
                mockall::predicate::eq(3),
                mockall::predicate::eq(0),
                mockall::predicate::eq(5i64),
            )
            .times(1)
            .returning(|_, _, before| Ok(before));
        mock.expect_delete_records_before()
            .with(
                mockall::predicate::eq(3),
                mockall::predicate::eq(0),
                mockall::predicate::eq(10i64), // -1 resolved to the HWM
            )
            .times(1)
            .returning(|_, _, before| Ok(before));

        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        let response =
            configs::handle_delete_records(&ctx, vec![("t".to_string(), vec![(0, 5), (0, -1)])])
                .unwrap();

        let partitions = &response.topics[0].partitions;
        assert_eq!(partitions[0].error_code, ERROR_NONE);
        assert_eq!(partitions[0].low_watermark, 5);
        assert_eq!(partitions[1].error_code, ERROR_NONE);
        assert_eq!(partitions[1].low_watermark, 10);
    }

    /// Offsets past the high watermark, unknown topics, and out-of-range
    /// partitions produce per-partition errors without deleting anything.
    #[test]
    fn test_handle_delete_records_errors() {
        let mut mock = MockKafkaStore::new();
        mock.expect_get_topic_id()
            .returning(|name| Ok(if name == "known" { Some(3) } else { None }));
        mock.expect_get_topic_partition_count()
            .returning(|name| Ok(if name == "known" { Some(1) } else { None }));
        mock.expect_get_high_watermark().returning(|_, _| Ok(10));
        mock.expect_delete_records_before().times(0);

        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

        let response = configs::handle_delete_records(
            &ctx,
            vec![
                ("known".to_string(), vec![(0, 11), (9, 1)]),
                ("ghost".to_string(), vec![(0, 1)]),
            ],
        )
        .unwrap();

        let known = &response.topics[0].partitions;
        assert_eq!(known[0].error_code, ERROR_OFFSET_OUT_OF_RANGE);
        assert_eq!(known[1].error_code, ERROR_UNKNOWN_TOPIC_OR_PARTITION);
        assert_eq!(
            response.topics[1].partitions[0].error_code,
            ERROR_UNKNOWN_TOPIC_OR_PARTITION
        );
    }
}

/// Regression tests for the bug hunt at commit 6c982be (one test per confirmed bug
/// that a handler unit test can reach).
#[cfg(test)]
mod bug_hunt_regressions {
    use crate::kafka::constants::*;
    use crate::kafka::handlers::{coordinator, fetch, produce, transaction};
    use crate::kafka::messages::{
        PartitionFetchData, PartitionProduceData, ProducerMetadata, Record, TopicFetchData,
        TopicProduceData,
    };
    use crate::kafka::storage::{TopicMetadata, TransactionState};
    use crate::kafka::{BrokerMetadata, GroupCoordinator, HandlerContext};
    use crate::testing::mocks::MockKafkaStore;
    use kafka_protocol::records::Compression;
    use mockall::predicate::eq;

    fn record(key: &[u8]) -> Record {
        Record {
            key: Some(key.to_vec()),
            value: Some(b"v".to_vec()),
            headers: vec![],
            timestamp: None,
        }
    }

    fn meta(base_sequence: i32) -> ProducerMetadata {
        ProducerMetadata {
            producer_id: 1000,
            producer_epoch: 0,
            base_sequence,
        }
    }

    /// Bug 1: each partition's batch must be checked against its own base_sequence,
    /// not the first partition's.
    #[test]
    fn idempotent_produce_uses_each_partitions_own_sequence() {
        let mut mock = MockKafkaStore::new();
        mock.expect_get_log_start_offset().returning(|_, _| Ok(0));
        mock.expect_get_or_create_topic()
            .returning(|_, _| Ok((1, 2)));
        mock.expect_check_and_update_sequence()
            .with(eq(1000), eq(0), eq(1), eq(0), eq(5), eq(1))
            .times(1)
            .returning(|_, _, _, _, _, _| Ok(crate::kafka::storage::SequenceCheck::Append));
        mock.expect_check_and_update_sequence()
            .with(eq(1000), eq(0), eq(1), eq(1), eq(0), eq(1))
            .times(1)
            .returning(|_, _, _, _, _, _| Ok(crate::kafka::storage::SequenceCheck::Append));
        mock.expect_insert_records().returning(|_, _, _| Ok(0));
        mock.expect_record_producer_batch()
            .withf(|b| {
                (
                    b.producer_id,
                    b.topic_id,
                    b.partition_id,
                    b.first_sequence,
                    b.last_sequence,
                ) == (1000, 1, 0, 5, 5)
            })
            .times(1)
            .returning(|_| Ok(()));
        mock.expect_record_producer_batch()
            .withf(|b| {
                (
                    b.producer_id,
                    b.topic_id,
                    b.partition_id,
                    b.first_sequence,
                    b.last_sequence,
                ) == (1000, 1, 1, 0, 0)
            })
            .times(1)
            .returning(|_| Ok(()));

        let topic_data = vec![TopicProduceData {
            name: "t".to_string(),
            partitions: vec![
                PartitionProduceData {
                    partition_index: 0,
                    records: vec![record(b"a")],
                    producer_metadata: Some(meta(5)),
                },
                PartitionProduceData {
                    partition_index: 1,
                    records: vec![record(b"b")],
                    producer_metadata: Some(meta(0)),
                },
            ],
        }];

        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);
        // The request-level metadata is the first partition's (worker.rs).
        let response = produce::handle_produce(&ctx, topic_data, Some(&meta(5)), None).unwrap();
        for pr in &response.responses[0].partition_responses {
            assert_eq!(pr.error_code, ERROR_NONE, "partition {}", pr.index);
        }
    }

    /// Bug 9: an idempotent batch sent to partition -1 is rejected rather than split
    /// across partitions (which would corrupt per-partition sequence tracking).
    #[test]
    fn idempotent_produce_to_partition_minus_one_is_rejected() {
        let mut mock = MockKafkaStore::new();
        mock.expect_get_or_create_topic()
            .returning(|_, _| Ok((1, 2)));
        // No sequence/insert expectations: nothing may be written.

        let topic_data = vec![TopicProduceData {
            name: "t".to_string(),
            partitions: vec![PartitionProduceData {
                partition_index: -1,
                records: vec![record(b"a"), record(b"zzz")],
                producer_metadata: Some(meta(0)),
            }],
        }];

        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);
        let response = produce::handle_produce(&ctx, topic_data, Some(&meta(0)), None).unwrap();
        let prs = &response.responses[0].partition_responses;
        assert_eq!(prs.len(), 1);
        assert_eq!(prs[0].index, -1);
        assert_eq!(prs[0].error_code, ERROR_UNKNOWN_TOPIC_OR_PARTITION);
    }

    /// Partition -1 still routes by key for non-idempotent producers.
    #[test]
    fn non_idempotent_produce_to_partition_minus_one_still_routes() {
        let mut mock = MockKafkaStore::new();
        mock.expect_get_log_start_offset().returning(|_, _| Ok(0));
        mock.expect_get_or_create_topic()
            .returning(|_, _| Ok((1, 2)));
        mock.expect_insert_records().returning(|_, _, _| Ok(0));

        let topic_data = vec![TopicProduceData {
            name: "t".to_string(),
            partitions: vec![PartitionProduceData {
                partition_index: -1,
                records: vec![record(b"a")],
                producer_metadata: None,
            }],
        }];

        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);
        let response = produce::handle_produce(&ctx, topic_data, None, None).unwrap();
        let prs = &response.responses[0].partition_responses;
        assert_eq!(prs.len(), 1);
        assert_eq!(prs[0].error_code, ERROR_NONE);
        assert!((0..2).contains(&prs[0].index));
    }

    fn fetch_mock(partition_count: i32, hwm: i64, log_start: i64) -> MockKafkaStore {
        let mut mock = MockKafkaStore::new();
        mock.expect_get_topic_metadata().returning(move |_| {
            Ok(vec![TopicMetadata {
                name: "t".to_string(),
                id: 1,
                partition_count,
            }])
        });
        mock.expect_get_high_watermark()
            .returning(move |_, _| Ok(hwm));
        mock.expect_get_earliest_offset()
            .returning(move |_, _| Ok(log_start));
        mock.expect_get_log_start_offset()
            .returning(move |_, _| Ok(log_start));
        mock.expect_fetch_records_with_isolation()
            .returning(|_, _, _, _, _| Ok(vec![]));
        mock
    }

    fn fetch_one(mock: &MockKafkaStore, partition_index: i32, fetch_offset: i64) -> (i16, i64) {
        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(mock, &coordinator, &broker, 1, Compression::None);
        let response = fetch::handle_fetch(
            &ctx,
            vec![TopicFetchData {
                name: "t".to_string(),
                partitions: vec![PartitionFetchData {
                    partition_index,
                    fetch_offset,
                    partition_max_bytes: 1024,
                }],
            }],
            crate::kafka::storage::IsolationLevel::ReadUncommitted,
        )
        .unwrap();
        let p = &response.responses[0].partitions[0];
        (p.error_code, p.high_watermark)
    }

    /// Bug 6: fetching a partition that does not exist reports UNKNOWN_TOPIC_OR_PARTITION.
    #[test]
    fn fetch_unknown_partition_is_an_error() {
        let mock = fetch_mock(1, 10, 0);
        assert_eq!(fetch_one(&mock, 99, 0).0, ERROR_UNKNOWN_TOPIC_OR_PARTITION);
        assert_eq!(fetch_one(&mock, -1, 0).0, ERROR_UNKNOWN_TOPIC_OR_PARTITION);
    }

    /// Bug 6: an offset past the high watermark or before the log start is
    /// OFFSET_OUT_OF_RANGE; the high watermark itself is a valid (caught-up) position.
    #[test]
    fn fetch_offset_out_of_range_is_an_error() {
        let mock = fetch_mock(1, 10, 3);
        assert_eq!(fetch_one(&mock, 0, 1000), (ERROR_OFFSET_OUT_OF_RANGE, 10));
        assert_eq!(fetch_one(&mock, 0, 2), (ERROR_OFFSET_OUT_OF_RANGE, 10));
        assert_eq!(fetch_one(&mock, 0, 10).0, ERROR_NONE);
        assert_eq!(fetch_one(&mock, 0, 3).0, ERROR_NONE);
    }

    /// The fetch lower bound is the durable log start, not the first surviving row:
    /// after aborted-message cleanup deletes offset 0, a fetch from 0 must still
    /// succeed (and skip to the next committed record), not report OFFSET_OUT_OF_RANGE.
    #[test]
    fn fetch_into_cleaned_up_aborted_hole_is_not_out_of_range() {
        let mut mock = MockKafkaStore::new();
        mock.expect_get_topic_metadata().returning(|_| {
            Ok(vec![TopicMetadata {
                name: "t".to_string(),
                id: 1,
                partition_count: 1,
            }])
        });
        mock.expect_get_high_watermark().returning(|_, _| Ok(2));
        // First surviving row is offset 1 (aborted offset 0 was cleaned up) ...
        mock.expect_get_earliest_offset().returning(|_, _| Ok(1));
        // ... but nothing retired offset 0, so the durable log start is still 0.
        mock.expect_get_log_start_offset().returning(|_, _| Ok(0));
        mock.expect_fetch_records_with_isolation()
            .returning(|_, _, _, _, _| Ok(vec![]));
        assert_eq!(fetch_one(&mock, 0, 0).0, ERROR_NONE);
    }

    /// Bug 4: a request-level TxnOffsetCommit failure is echoed on every partition.
    #[test]
    fn txn_offset_commit_error_response_echoes_every_partition() {
        let topics = vec![
            ("a".to_string(), vec![(0, 1i64, None), (1, 2i64, None)]),
            ("b".to_string(), vec![(3, 4i64, None)]),
        ];
        let response = transaction::txn_offset_commit_error_response(&topics, 47);
        assert_eq!(response.topics.len(), 2);
        let all: Vec<(i32, i16)> = response
            .topics
            .iter()
            .flat_map(|t| t.partitions.iter())
            .map(|p| (p.partition_index, p.error_code))
            .collect();
        assert_eq!(all, vec![(0, 47), (1, 47), (3, 47)]);
    }

    /// Bug 10: TxnOffsetCommit rejects partitions outside [0, partition_count) and
    /// stores only the valid one.
    #[test]
    fn txn_offset_commit_checks_partition_range() {
        let mut mock = MockKafkaStore::new();
        mock.expect_validate_transaction()
            .returning(|_, _, _| Ok(()));
        mock.expect_get_transaction_state()
            .returning(|_| Ok(Some(TransactionState::Ongoing)));
        mock.expect_get_topic_id().returning(|_| Ok(Some(1)));
        mock.expect_get_topic_partition_count()
            .returning(|_| Ok(Some(1)));
        mock.expect_store_txn_pending_offset()
            .withf(|_, _, _, partition, _, _| *partition == 0)
            .times(1)
            .returning(|_, _, _, _, _, _| Ok(()));

        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);
        let topics = vec![(
            "t".to_string(),
            vec![(-1, 1i64, None), (99, 1i64, None), (0, 1i64, None)],
        )];
        let response =
            transaction::handle_txn_offset_commit(&ctx, "txn", 1000, 0, "g", -1, "", topics)
                .unwrap();
        let codes: Vec<i16> = response.topics[0]
            .partitions
            .iter()
            .map(|p| p.error_code)
            .collect();
        assert_eq!(
            codes,
            vec![
                ERROR_UNKNOWN_TOPIC_OR_PARTITION,
                ERROR_UNKNOWN_TOPIC_OR_PARTITION,
                ERROR_NONE
            ]
        );
    }

    /// Bug 11: DescribeGroups on an unknown group reports success with state Dead.
    #[test]
    fn describe_unknown_group_is_dead_not_server_error() {
        let mut mock = MockKafkaStore::new();
        mock.expect_group_has_offsets().returning(|_| Ok(false));
        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);
        let response =
            coordinator::handle_describe_groups(&ctx, vec!["missing".to_string()]).unwrap();
        assert_eq!(response.groups[0].error_code, ERROR_NONE);
        assert_eq!(response.groups[0].group_state.as_str(), "Dead");
    }
}
