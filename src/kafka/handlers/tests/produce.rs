//! ProduceRequest handler tests.

use crate::kafka::constants::*;
use crate::kafka::error::KafkaError;
use crate::kafka::handlers::produce;
use crate::kafka::messages::{PartitionProduceData, Record, TopicProduceData};
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
