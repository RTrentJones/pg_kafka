//! AddPartitionsToTxn, AddOffsetsToTxn, EndTxn and TxnOffsetCommit handler tests.

use crate::kafka::constants::*;
use crate::kafka::error::KafkaError;
use crate::kafka::handlers::transaction;
use crate::kafka::storage::TransactionState;
use crate::testing::mocks::MockKafkaStore;
use crate::testing::TestEnv;

// ========== Transaction Handler Tests (Phase 10) ==========

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

    let env = TestEnv::new();
    let ctx = env.ctx(&mock);

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

    let env = TestEnv::new();
    let ctx = env.ctx(&mock);

    let topics = vec![("test-topic".to_string(), vec![0, 5])];
    let response =
        transaction::handle_add_partitions_to_txn(&ctx, "txn-1", 1000, 0, topics).unwrap();

    let partitions = &response.results_by_topic_v3_and_below[0].results_by_partition;
    assert_eq!(partitions[0].partition_index, 0);
    assert_eq!(
        partitions[0].partition_error_code,
        ERROR_OPERATION_NOT_ATTEMPTED
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

    let env = TestEnv::new();
    let ctx = env.ctx(&mock);

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
            ERROR_OPERATION_NOT_ATTEMPTED
        ]
    );
}

fn add_partitions_with_begin_error(err: fn() -> KafkaError) -> Vec<i16> {
    let mut mock = MockKafkaStore::new();
    topics_with_partitions(&mut mock, 1);
    mock.expect_begin_or_continue_transaction()
        .returning(move |_, _, _| Err(err()));
    let env = TestEnv::new();
    let ctx = env.ctx(&mock);
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
    let codes =
        add_partitions_with_begin_error(|| KafkaError::transactional_id_not_found("unknown-txn"));
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
    let env = TestEnv::new();
    let ctx = env.ctx(&mock);

    let response =
        transaction::handle_add_offsets_to_txn(&ctx, "txn-1", 1000, 0, "consumer-group").unwrap();

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
    let env = TestEnv::new();
    let ctx = env.ctx(&mock);

    let result = transaction::handle_add_offsets_to_txn(&ctx, "txn-1", 1000, 0, "consumer-group");

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
    let env = TestEnv::new();
    let ctx = env.ctx(&mock);

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
    let env = TestEnv::new();
    let ctx = env.ctx(&mock);

    let result = transaction::handle_add_offsets_to_txn(&ctx, "txn-1", 1000, 0, "consumer-group");

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
    let env = TestEnv::new();
    let ctx = env.ctx(&mock);

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
    let env = TestEnv::new();
    let ctx = env.ctx(&mock);

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
    let env = TestEnv::new();
    let ctx = env.ctx(&mock);

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
    let env = TestEnv::new();
    let ctx = env.ctx(&mock);

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
    let env = TestEnv::new();
    let ctx = env.ctx(&mock);

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
    let env = TestEnv::new();
    let ctx = env.ctx(&mock);

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
    let env = TestEnv::new();
    let ctx = env.ctx(&mock);

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
    let env = TestEnv::new();
    let ctx = env.ctx(&mock);

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
    let env = TestEnv::new();
    let ctx = env.ctx(&mock);

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
    let env = TestEnv::new();
    let ctx = env.ctx(&mock);

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
    let env = TestEnv::new();
    let ctx = env.ctx(&mock);

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
    let env = TestEnv::new();
    let ctx = env.ctx(&mock);

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
