// Storage layer tests
//
// These tests verify the storage abstraction layer works correctly.
// Since PostgresStore requires a real PostgreSQL database, we focus on:
// 1. Storage type construction and behavior
// 2. MockKafkaStore comprehensive coverage
// 3. KafkaStore trait contract verification
//
// NOTE: Integration tests for PostgresStore are in tests/ directory using pgrx.

#[cfg(test)]
#[allow(clippy::module_inception)]
mod tests {
    use crate::kafka::error::KafkaError;
    use crate::kafka::messages::Record;
    use crate::kafka::storage::{CommittedOffset, FetchedMessage, KafkaStore, TopicMetadata};
    use crate::testing::mocks::MockKafkaStore;

    // ========== Storage Types Tests ==========

    // ========== Transaction Types Tests (Phase 10) ==========

    use crate::kafka::storage::{IsolationLevel, TransactionState};

    #[test]
    fn test_transaction_state_from_str() {
        // Test all valid states
        assert_eq!(
            TransactionState::parse("Empty"),
            Some(TransactionState::Empty)
        );
        assert_eq!(
            TransactionState::parse("Ongoing"),
            Some(TransactionState::Ongoing)
        );
        assert_eq!(
            TransactionState::parse("PrepareCommit"),
            Some(TransactionState::PrepareCommit)
        );
        assert_eq!(
            TransactionState::parse("PrepareAbort"),
            Some(TransactionState::PrepareAbort)
        );
        assert_eq!(
            TransactionState::parse("CompleteCommit"),
            Some(TransactionState::CompleteCommit)
        );
        assert_eq!(
            TransactionState::parse("CompleteAbort"),
            Some(TransactionState::CompleteAbort)
        );

        // Test invalid states return None
        assert_eq!(TransactionState::parse("invalid"), None);
        assert_eq!(TransactionState::parse(""), None);
        assert_eq!(TransactionState::parse("EMPTY"), None); // Case-sensitive
    }

    #[test]
    fn test_transaction_state_as_str() {
        assert_eq!(TransactionState::Empty.as_str(), "Empty");
        assert_eq!(TransactionState::Ongoing.as_str(), "Ongoing");
        assert_eq!(TransactionState::PrepareCommit.as_str(), "PrepareCommit");
        assert_eq!(TransactionState::PrepareAbort.as_str(), "PrepareAbort");
        assert_eq!(TransactionState::CompleteCommit.as_str(), "CompleteCommit");
        assert_eq!(TransactionState::CompleteAbort.as_str(), "CompleteAbort");
    }

    #[test]
    fn test_isolation_level_from_i8() {
        // 0 = ReadUncommitted (default)
        assert_eq!(IsolationLevel::from_i8(0), IsolationLevel::ReadUncommitted);

        // 1 = ReadCommitted
        assert_eq!(IsolationLevel::from_i8(1), IsolationLevel::ReadCommitted);

        // Any other value defaults to ReadUncommitted
        assert_eq!(IsolationLevel::from_i8(2), IsolationLevel::ReadUncommitted);
        assert_eq!(IsolationLevel::from_i8(-1), IsolationLevel::ReadUncommitted);
        assert_eq!(
            IsolationLevel::from_i8(127),
            IsolationLevel::ReadUncommitted
        );
    }

    #[test]
    fn test_topic_metadata_construction() {
        let metadata = TopicMetadata {
            name: "test-topic".to_string(),
            id: 42,
            partition_count: 3,
        };

        assert_eq!(metadata.name, "test-topic");
        assert_eq!(metadata.id, 42);
        assert_eq!(metadata.partition_count, 3);
    }

    #[test]
    fn test_topic_metadata_clone() {
        let original = TopicMetadata {
            name: "original".to_string(),
            id: 1,
            partition_count: 1,
        };

        let cloned = original.clone();
        assert_eq!(cloned.name, original.name);
        assert_eq!(cloned.id, original.id);
        assert_eq!(cloned.partition_count, original.partition_count);
    }

    #[test]
    fn test_fetched_message_construction() {
        let msg = FetchedMessage {
            partition_offset: 100,
            key: Some(b"key".to_vec()),
            value: Some(b"value".to_vec()),
            timestamp: 1234567890,
        };

        assert_eq!(msg.partition_offset, 100);
        assert_eq!(msg.key, Some(b"key".to_vec()));
        assert_eq!(msg.value, Some(b"value".to_vec()));
        assert_eq!(msg.timestamp, 1234567890);
    }

    #[test]
    fn test_fetched_message_nullable_fields() {
        let msg = FetchedMessage {
            partition_offset: 0,
            key: None,
            value: None,
            timestamp: 0,
        };

        assert!(msg.key.is_none());
        assert!(msg.value.is_none());
    }

    #[test]
    fn test_committed_offset_construction() {
        let offset = CommittedOffset {
            offset: 500,
            metadata: Some("test-metadata".to_string()),
        };

        assert_eq!(offset.offset, 500);
        assert_eq!(offset.metadata, Some("test-metadata".to_string()));
    }

    #[test]
    fn test_committed_offset_no_metadata() {
        let offset = CommittedOffset {
            offset: 100,
            metadata: None,
        };

        assert_eq!(offset.offset, 100);
        assert!(offset.metadata.is_none());
    }

    // ========== MockKafkaStore Comprehensive Tests ==========

    #[test]
    fn test_mock_get_or_create_topic() {
        let mut mock = MockKafkaStore::new();

        // Returns (topic_id, partition_count)
        mock.expect_get_or_create_topic()
            .with(
                mockall::predicate::eq("new-topic"),
                mockall::predicate::eq(1),
            )
            .times(1)
            .returning(|_, _| Ok((1, 1)));

        let result = mock.get_or_create_topic("new-topic", 1);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), (1, 1));
    }

    #[test]
    fn test_mock_get_or_create_topic_error() {
        let mut mock = MockKafkaStore::new();

        mock.expect_get_or_create_topic()
            .returning(|_, _| Err(KafkaError::database("connection failed")));

        let result = mock.get_or_create_topic("any-topic", 1);
        assert!(result.is_err());
    }

    #[test]
    fn test_mock_get_topic_metadata_empty() {
        let mut mock = MockKafkaStore::new();

        mock.expect_get_topic_metadata().returning(|_| Ok(vec![]));

        let result = mock.get_topic_metadata(None);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_mock_get_topic_metadata_multiple() {
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
                    partition_count: 3,
                },
            ])
        });

        let result = mock.get_topic_metadata(None).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].name, "topic1");
        assert_eq!(result[1].name, "topic2");
    }

    #[test]
    fn test_mock_insert_records() {
        let mut mock = MockKafkaStore::new();

        mock.expect_insert_records()
            .withf(|topic_id, partition_id, records| {
                *topic_id == 1 && *partition_id == 0 && records.len() == 2
            })
            .times(1)
            .returning(|_, _, _| Ok(100)); // Returns base offset

        let records = vec![
            Record {
                key: Some(b"k1".to_vec()),
                value: Some(b"v1".to_vec()),
                headers: vec![],
                timestamp: None,
            },
            Record {
                key: Some(b"k2".to_vec()),
                value: Some(b"v2".to_vec()),
                headers: vec![],
                timestamp: None,
            },
        ];

        let result = mock.insert_records(1, 0, &records);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 100);
    }

    #[test]
    fn test_mock_fetch_records() {
        let mut mock = MockKafkaStore::new();

        mock.expect_fetch_records()
            .withf(|topic_id, partition_id, offset, max_bytes| {
                *topic_id == 1 && *partition_id == 0 && *offset == 0 && *max_bytes == 1024
            })
            .returning(|_, _, _, _| {
                Ok(vec![
                    FetchedMessage {
                        partition_offset: 0,
                        key: Some(b"key".to_vec()),
                        value: Some(b"value".to_vec()),
                        timestamp: 12345,
                    },
                    FetchedMessage {
                        partition_offset: 1,
                        key: None,
                        value: Some(b"value2".to_vec()),
                        timestamp: 12346,
                    },
                ])
            });

        let result = mock.fetch_records(1, 0, 0, 1024).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].partition_offset, 0);
        assert_eq!(result[1].partition_offset, 1);
    }

    #[test]
    fn test_mock_fetch_records_empty() {
        let mut mock = MockKafkaStore::new();

        mock.expect_fetch_records()
            .returning(|_, _, _, _| Ok(vec![]));

        let result = mock.fetch_records(1, 0, 1000, 1024).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_mock_get_high_watermark() {
        let mut mock = MockKafkaStore::new();

        mock.expect_get_high_watermark()
            .with(mockall::predicate::eq(1), mockall::predicate::eq(0))
            .returning(|_, _| Ok(500));

        let result = mock.get_high_watermark(1, 0).unwrap();
        assert_eq!(result, 500);
    }

    #[test]
    fn test_mock_get_earliest_offset() {
        let mut mock = MockKafkaStore::new();

        mock.expect_get_earliest_offset().returning(|_, _| Ok(0)); // Earliest is always 0 for fresh partition

        let result = mock.get_earliest_offset(1, 0).unwrap();
        assert_eq!(result, 0);
    }

    #[test]
    fn test_mock_commit_offset() {
        let mut mock = MockKafkaStore::new();

        mock.expect_commit_offset()
            .withf(|group_id, topic_id, partition_id, offset, metadata| {
                group_id == "test-group"
                    && *topic_id == 1
                    && *partition_id == 0
                    && *offset == 100
                    && *metadata == Some("meta")
            })
            .times(1)
            .returning(|_, _, _, _, _| Ok(()));

        let result = mock.commit_offset("test-group", 1, 0, 100, Some("meta"));
        assert!(result.is_ok());
    }

    #[test]
    fn test_mock_fetch_offset_found() {
        let mut mock = MockKafkaStore::new();

        mock.expect_fetch_offset().returning(|_, _, _| {
            Ok(Some(CommittedOffset {
                offset: 42,
                metadata: Some("my-metadata".to_string()),
            }))
        });

        let result = mock.fetch_offset("group", 1, 0).unwrap();
        assert!(result.is_some());
        let offset = result.unwrap();
        assert_eq!(offset.offset, 42);
        assert_eq!(offset.metadata, Some("my-metadata".to_string()));
    }

    #[test]
    fn test_mock_fetch_offset_not_found() {
        let mut mock = MockKafkaStore::new();

        mock.expect_fetch_offset().returning(|_, _, _| Ok(None));

        let result = mock.fetch_offset("group", 1, 0).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_mock_fetch_all_offsets() {
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
                    "topic1".to_string(),
                    1,
                    CommittedOffset {
                        offset: 20,
                        metadata: Some("meta".to_string()),
                    },
                ),
                (
                    "topic2".to_string(),
                    0,
                    CommittedOffset {
                        offset: 30,
                        metadata: None,
                    },
                ),
            ])
        });

        let result = mock.fetch_all_offsets("group").unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].0, "topic1");
        assert_eq!(result[0].1, 0);
        assert_eq!(result[0].2.offset, 10);
    }

    // ========== KafkaStore Trait Contract Tests ==========

    #[test]
    fn test_kafka_store_is_object_safe() {
        // Verify KafkaStore can be used as a trait object
        // This is important for dependency injection patterns
        fn accepts_store(_store: &dyn KafkaStore) {}

        let mock = MockKafkaStore::new();
        accepts_store(&mock);
    }

    #[test]
    fn test_mock_multiple_expectations() {
        let mut mock = MockKafkaStore::new();

        // Set up multiple expectations for a realistic scenario
        // get_or_create_topic returns (topic_id, partition_count)
        mock.expect_get_or_create_topic()
            .times(1)
            .returning(|_, _| Ok((1, 1)));

        mock.expect_insert_records()
            .times(1)
            .returning(|_, _, _| Ok(0));

        mock.expect_get_high_watermark()
            .times(1)
            .returning(|_, _| Ok(1));

        // Execute in sequence
        let (topic_id, partition_count) = mock.get_or_create_topic("test", 1).unwrap();
        assert_eq!(topic_id, 1);
        assert_eq!(partition_count, 1);

        let base_offset = mock.insert_records(1, 0, &[]).unwrap();
        assert_eq!(base_offset, 0);

        let hwm = mock.get_high_watermark(1, 0).unwrap();
        assert_eq!(hwm, 1);
    }

    #[test]
    fn test_mock_error_propagation() {
        let mut mock = MockKafkaStore::new();

        mock.expect_fetch_records()
            .returning(|_, _, _, _| Err(KafkaError::database("query timeout")));

        let result = mock.fetch_records(1, 0, 0, 1024);
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert!(err.to_string().contains("query timeout"));
    }

    // ========== Batch Insert Tests ==========
    //
    // These tests validate batch insert behavior which is critical for the N+1 fix.

    #[test]
    fn test_mock_insert_records_large_batch() {
        let mut mock = MockKafkaStore::new();

        // Create a large batch of 100 records
        let records: Vec<Record> = (0..100)
            .map(|i| Record {
                key: Some(format!("key-{}", i).into_bytes()),
                value: Some(format!("value-{}", i).into_bytes()),
                headers: vec![],
                timestamp: Some(1000 + i as i64),
            })
            .collect();

        // Verify mock receives all 100 records
        mock.expect_insert_records()
            .withf(|topic_id, partition_id, recs| {
                *topic_id == 1 && *partition_id == 0 && recs.len() == 100
            })
            .times(1)
            .returning(|_, _, _| Ok(0)); // Returns base offset 0

        let result = mock.insert_records(1, 0, &records);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0);
    }

    #[test]
    fn test_mock_insert_records_offset_sequence() {
        let mut mock = MockKafkaStore::new();

        // Simulate insert returning increasing base offsets
        let mut call_count = 0i64;
        mock.expect_insert_records()
            .times(3)
            .returning(move |_, _, _records| {
                let base_offset = call_count * 10; // Each call advances by 10
                call_count += 1;
                // Simulate returning base offset based on records count
                Ok(base_offset)
            });

        // First batch: offsets 0-9
        let records1: Vec<Record> = (0..10)
            .map(|i| Record {
                key: Some(format!("k{}", i).into_bytes()),
                value: Some(b"v".to_vec()),
                headers: vec![],
                timestamp: None,
            })
            .collect();

        let offset1 = mock.insert_records(1, 0, &records1).unwrap();
        assert_eq!(offset1, 0); // Base offset for first batch

        // Second batch: offsets 10-19
        let records2: Vec<Record> = (0..10)
            .map(|i| Record {
                key: Some(format!("k{}", i + 10).into_bytes()),
                value: Some(b"v".to_vec()),
                headers: vec![],
                timestamp: None,
            })
            .collect();

        let offset2 = mock.insert_records(1, 0, &records2).unwrap();
        assert_eq!(offset2, 10); // Base offset for second batch

        // Third batch: offsets 20-29
        let offset3 = mock.insert_records(1, 0, &records1).unwrap();
        assert_eq!(offset3, 20); // Base offset for third batch
    }

    #[test]
    fn test_mock_insert_records_with_headers() {
        use crate::kafka::messages::RecordHeader;

        let mut mock = MockKafkaStore::new();

        // Verify headers are passed correctly
        mock.expect_insert_records()
            .withf(|_topic_id, _partition_id, records| {
                // Check that headers are preserved
                records.len() == 1
                    && records[0].headers.len() == 2
                    && records[0].headers[0].key == "correlation-id"
                    && records[0].headers[1].key == "content-type"
            })
            .times(1)
            .returning(|_, _, _| Ok(0));

        let records = vec![Record {
            key: Some(b"key".to_vec()),
            value: Some(b"value".to_vec()),
            headers: vec![
                RecordHeader {
                    key: "correlation-id".to_string(),
                    value: b"abc123".to_vec(),
                },
                RecordHeader {
                    key: "content-type".to_string(),
                    value: b"application/json".to_vec(),
                },
            ],
            timestamp: None,
        }];

        let result = mock.insert_records(1, 0, &records);
        assert!(result.is_ok());
    }

    // ========== Transaction Mock Contract Tests (Phase 10) ==========
    //
    // These tests verify the MockKafkaStore correctly implements transaction methods

    #[test]
    fn test_mock_get_or_create_transactional_producer() {
        let mut mock = MockKafkaStore::new();

        mock.expect_get_or_create_transactional_producer()
            .withf(|txn_id, timeout_ms, client_id| {
                txn_id == "my-txn-id" && *timeout_ms == 60000 && *client_id == Some("test-client")
            })
            .times(1)
            .returning(|_, _, _| Ok((1000, 0))); // (producer_id, epoch)

        let result = mock
            .get_or_create_transactional_producer("my-txn-id", 60000, Some("test-client"))
            .unwrap();
        assert_eq!(result.0, 1000); // producer_id
        assert_eq!(result.1, 0); // epoch
    }

    #[test]
    fn test_mock_get_or_create_transactional_producer_bumps_epoch() {
        let mut mock = MockKafkaStore::new();

        // Simulate epoch bumping on second call (producer fencing)
        let mut call_count = 0i16;
        mock.expect_get_or_create_transactional_producer()
            .times(2)
            .returning(move |_, _, _| {
                let epoch = call_count;
                call_count += 1;
                Ok((1000, epoch)) // Same producer_id, incrementing epoch
            });

        // First call: epoch=0
        let (pid1, epoch1) = mock
            .get_or_create_transactional_producer("txn-1", 60000, None)
            .unwrap();
        assert_eq!(pid1, 1000);
        assert_eq!(epoch1, 0);

        // Second call: epoch=1 (old producer is fenced)
        let (pid2, epoch2) = mock
            .get_or_create_transactional_producer("txn-1", 60000, None)
            .unwrap();
        assert_eq!(pid2, 1000);
        assert_eq!(epoch2, 1);
    }

    #[test]
    fn test_mock_begin_transaction() {
        let mut mock = MockKafkaStore::new();

        mock.expect_begin_transaction()
            .withf(|txn_id, producer_id, epoch| {
                txn_id == "txn-1" && *producer_id == 1000 && *epoch == 0
            })
            .times(1)
            .returning(|_, _, _| Ok(()));

        let result = mock.begin_transaction("txn-1", 1000, 0);
        assert!(result.is_ok());
    }

    #[test]
    fn test_mock_validate_transaction_success() {
        let mut mock = MockKafkaStore::new();

        mock.expect_validate_transaction()
            .withf(|txn_id, producer_id, epoch| {
                txn_id == "txn-1" && *producer_id == 1000 && *epoch == 0
            })
            .times(1)
            .returning(|_, _, _| Ok(()));

        let result = mock.validate_transaction("txn-1", 1000, 0);
        assert!(result.is_ok());
    }

    #[test]
    fn test_mock_validate_transaction_fenced() {
        let mut mock = MockKafkaStore::new();

        mock.expect_validate_transaction()
            .times(1)
            .returning(|_, _, _| Err(KafkaError::producer_fenced(1000, 0, 1)));

        let result = mock.validate_transaction("txn-1", 1000, 0);
        assert!(result.is_err());

        let err = result.unwrap_err();
        match err {
            KafkaError::ProducerFenced { .. } => {} // Expected
            _ => panic!("Expected ProducerFenced error, got {:?}", err),
        }
    }

    #[test]
    fn test_mock_insert_transactional_records() {
        let mut mock = MockKafkaStore::new();

        mock.expect_insert_transactional_records()
            .withf(
                |topic_id, partition_id, records, producer_id, producer_epoch| {
                    *topic_id == 1
                        && *partition_id == 0
                        && records.len() == 2
                        && *producer_id == 1000
                        && *producer_epoch == 0
                },
            )
            .times(1)
            .returning(|_, _, _, _, _| Ok(100)); // Returns base offset

        let records = vec![
            Record {
                key: Some(b"k1".to_vec()),
                value: Some(b"v1".to_vec()),
                headers: vec![],
                timestamp: None,
            },
            Record {
                key: Some(b"k2".to_vec()),
                value: Some(b"v2".to_vec()),
                headers: vec![],
                timestamp: None,
            },
        ];

        let result = mock.insert_transactional_records(1, 0, &records, 1000, 0);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 100);
    }

    #[test]
    fn test_mock_store_txn_pending_offset() {
        let mut mock = MockKafkaStore::new();

        mock.expect_store_txn_pending_offset()
            .withf(
                |txn_id, group_id, topic_id, partition_id, offset, metadata| {
                    txn_id == "txn-1"
                        && group_id == "test-group"
                        && *topic_id == 1
                        && *partition_id == 0
                        && *offset == 100
                        && *metadata == Some("meta")
                },
            )
            .times(1)
            .returning(|_, _, _, _, _, _| Ok(()));

        let result = mock.store_txn_pending_offset("txn-1", "test-group", 1, 0, 100, Some("meta"));
        assert!(result.is_ok());
    }

    #[test]
    fn test_mock_commit_transaction() {
        let mut mock = MockKafkaStore::new();

        mock.expect_commit_transaction()
            .withf(|txn_id, producer_id, epoch| {
                txn_id == "txn-1" && *producer_id == 1000 && *epoch == 0
            })
            .times(1)
            .returning(|_, _, _| Ok(()));

        let result = mock.commit_transaction("txn-1", 1000, 0);
        assert!(result.is_ok());
    }

    #[test]
    fn test_mock_abort_transaction() {
        let mut mock = MockKafkaStore::new();

        mock.expect_abort_transaction()
            .withf(|txn_id, producer_id, epoch| {
                txn_id == "txn-1" && *producer_id == 1000 && *epoch == 0
            })
            .times(1)
            .returning(|_, _, _| Ok(()));

        let result = mock.abort_transaction("txn-1", 1000, 0);
        assert!(result.is_ok());
    }

    #[test]
    fn test_mock_get_transaction_state_variants() {
        // Test that mock can return all TransactionState variants
        let variants = vec![
            TransactionState::Empty,
            TransactionState::Ongoing,
            TransactionState::PrepareCommit,
            TransactionState::PrepareAbort,
            TransactionState::CompleteCommit,
            TransactionState::CompleteAbort,
        ];

        for expected_state in variants {
            let mut mock = MockKafkaStore::new();
            let state_clone = expected_state.clone();

            mock.expect_get_transaction_state()
                .times(1)
                .returning(move |_| Ok(Some(state_clone.clone())));

            let result = mock.get_transaction_state("txn-1").unwrap();
            assert!(result.is_some());
            assert_eq!(result.unwrap(), expected_state);
        }
    }

    #[test]
    fn test_mock_get_transaction_state_not_found() {
        let mut mock = MockKafkaStore::new();

        mock.expect_get_transaction_state()
            .times(1)
            .returning(|_| Ok(None));

        let result = mock.get_transaction_state("unknown-txn").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_mock_abort_timed_out_transactions() {
        let mut mock = MockKafkaStore::new();

        mock.expect_abort_timed_out_transactions()
            .times(1)
            .returning(|_| Ok(vec!["txn-1".to_string(), "txn-2".to_string()]));

        let result = mock
            .abort_timed_out_transactions(std::time::Duration::from_secs(60))
            .unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], "txn-1");
        assert_eq!(result[1], "txn-2");
    }

    #[test]
    fn test_mock_cleanup_aborted_messages() {
        let mut mock = MockKafkaStore::new();

        mock.expect_cleanup_aborted_messages()
            .times(1)
            .returning(|_| Ok(42)); // 42 messages cleaned up

        let result = mock
            .cleanup_aborted_messages(std::time::Duration::from_secs(3600))
            .unwrap();
        assert_eq!(result, 42);
    }

    #[test]
    fn test_mock_fetch_records_with_isolation() {
        let mut mock = MockKafkaStore::new();

        // Test read_committed: should only return committed messages
        mock.expect_fetch_records_with_isolation()
            .withf(|topic_id, partition_id, offset, max_bytes, isolation| {
                *topic_id == 1
                    && *partition_id == 0
                    && *offset == 0
                    && *max_bytes == 1024
                    && *isolation == IsolationLevel::ReadCommitted
            })
            .times(1)
            .returning(|_, _, _, _, _| {
                // Only committed messages returned (pending filtered out)
                Ok(vec![FetchedMessage {
                    partition_offset: 0,
                    key: Some(b"committed-key".to_vec()),
                    value: Some(b"committed-value".to_vec()),
                    timestamp: 12345,
                }])
            });

        let result = mock
            .fetch_records_with_isolation(1, 0, 0, 1024, IsolationLevel::ReadCommitted)
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].key, Some(b"committed-key".to_vec()));
    }

    #[test]
    fn test_mock_get_last_stable_offset() {
        let mut mock = MockKafkaStore::new();

        // LSO is the lowest pending offset, or HWM if no pending
        mock.expect_get_last_stable_offset()
            .withf(|topic_id, partition_id| *topic_id == 1 && *partition_id == 0)
            .times(1)
            .returning(|_, _| Ok(50)); // LSO at 50

        let result = mock.get_last_stable_offset(1, 0).unwrap();
        assert_eq!(result, 50);
    }
}
