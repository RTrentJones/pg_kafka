//! AddPartitionsToTxn, AddOffsetsToTxn, EndTxn and TxnOffsetCommit handler tests.

use crate::kafka::constants::*;
use crate::kafka::error::KafkaError;
use crate::kafka::handlers::transaction;
use crate::kafka::storage::TransactionState;
use crate::testing::mocks::MockKafkaStore;
use crate::testing::TestEnv;

// ========== Transaction Handler Tests ==========

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
    // The partitions are registered for the transactional produce check.
    mock.expect_add_txn_partitions()
        .withf(|txn, parts| txn == "txn-1" && parts == [(1, 0), (1, 1)])
        .times(1)
        .returning(|_, _| Ok(()));

    let env = TestEnv::new();
    let ctx = env.ctx(&mock);

    let topics = vec![("test-topic".to_string(), vec![0, 1])];

    let response =
        transaction::handle_add_partitions_to_txn(&ctx, "txn-1", 1000, 0, &topics).unwrap();

    assert_eq!(response.results_by_topic_v3_and_below.len(), 1);
    assert_eq!(
        add_partitions_codes(&response),
        vec![ERROR_NONE, ERROR_NONE]
    );
}

#[test]
fn test_add_partitions_invalid_partition() {
    // One unknown partition fails the request: it gets
    // UNKNOWN_TOPIC_OR_PARTITION, the valid ones OPERATION_NOT_ATTEMPTED, and
    // neither the transaction state nor the registry is touched (no expectations).
    let mut mock = MockKafkaStore::new();
    topics_with_partitions(&mut mock, 2);

    let env = TestEnv::new();
    let ctx = env.ctx(&mock);

    let topics = vec![("test-topic".to_string(), vec![0, 5])];
    let response =
        transaction::handle_add_partitions_to_txn(&ctx, "txn-1", 1000, 0, &topics).unwrap();

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
    // AddPartitionsToTxn does not create a missing topic: Kafka answers
    // UNKNOWN_TOPIC_OR_PARTITION (clients create topics through Metadata). There is no
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
        transaction::handle_add_partitions_to_txn(&ctx, "txn-1", 1000, 0, &topics).unwrap();
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
        transaction::handle_add_partitions_to_txn(&ctx, "txn-1", 1000, 0, &topics).unwrap();
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
    assert_eq!(codes, vec![ERROR_INVALID_PRODUCER_ID_MAPPING]);
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
    // The group is registered in the transaction, for TxnOffsetCommit.
    mock.expect_add_txn_group()
        .withf(|txn, group| txn == "txn-1" && group == "consumer-group")
        .times(1)
        .returning(|_, _| Ok(()));

    // Create HandlerContext AFTER setting expectations
    let env = TestEnv::new();
    let ctx = env.ctx(&mock);

    let response =
        transaction::handle_add_offsets_to_txn(&ctx, "txn-1", 1000, 0, "consumer-group").unwrap();

    assert_eq!(response.error_code, ERROR_NONE);
}

/// Kafka 3.7: a TxnOffsetCommit for a group the Ongoing transaction did not add with
/// AddOffsetsToTxn is INVALID_TXN_STATE on every partition, and nothing is stored.
#[test]
fn test_txn_offset_commit_group_not_added() {
    let mut mock = MockKafkaStore::new();
    topics_with_partitions(&mut mock, 2);
    mock.expect_validate_transaction()
        .returning(|_, _, _| Ok(()));
    mock.expect_get_transaction_state()
        .returning(|_| Ok(Some(TransactionState::Ongoing)));
    mock.expect_txn_has_group()
        .withf(|txn, group| txn == "txn-1" && group == "consumer-group")
        .returning(|_, _| Ok(false));
    mock.expect_store_txn_pending_offset().never();
    let env = TestEnv::new();
    let ctx = env.ctx(&mock);
    let topics = vec![(
        "test-topic".to_string(),
        vec![(0, 1i64, None), (1, 2i64, None)],
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
    let codes: Vec<i16> = response.topics[0]
        .partitions
        .iter()
        .map(|p| p.error_code)
        .collect();
    assert_eq!(
        codes,
        vec![ERROR_INVALID_TXN_STATE, ERROR_INVALID_TXN_STATE]
    );
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
        e => panic!("Expected InvalidTransactionState error, got {e:?}"),
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
        e => panic!("Expected TransactionalIdNotFound error, got {e:?}"),
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
        e => panic!("Expected ProducerFenced error, got {e:?}"),
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
    mock.expect_transaction_partitions()
        .withf(|id| id == "txn-1")
        .times(1)
        .returning(|_| Ok(vec![("t".to_string(), vec![0, 3])]));

    // Create HandlerContext AFTER setting expectations
    let env = TestEnv::new();
    let ctx = env.ctx(&mock);

    let (response, ended) = transaction::end_txn(
        &ctx, "txn-1", 1000, 0, true, // committed
    )
    .unwrap();

    assert_eq!(response.error_code, ERROR_NONE);
    // The transaction's partitions, whose read_committed long polls the end wakes.
    assert_eq!(ended, vec![("t".to_string(), vec![0, 3])]);
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
    mock.expect_transaction_partitions()
        .withf(|id| id == "txn-1")
        .times(1)
        .returning(|_| Ok(vec![("t".to_string(), vec![0, 3])]));

    // Create HandlerContext AFTER setting expectations
    let env = TestEnv::new();
    let ctx = env.ctx(&mock);

    let (response, ended) = transaction::end_txn(
        &ctx, "txn-1", 1000, 0, false, // abort
    )
    .unwrap();

    assert_eq!(response.error_code, ERROR_NONE);
    // The transaction's partitions, whose read_committed long polls the end wakes.
    assert_eq!(ended, vec![("t".to_string(), vec![0, 3])]);
}

#[test]
fn test_end_txn_empty_is_invalid_txn_state() {
    let mut mock = MockKafkaStore::new();

    mock.expect_validate_transaction()
        .returning(|_, _, _| Ok(()));
    // State is Empty: Kafka 3.7 answers INVALID_TXN_STATE for a commit and an abort.
    mock.expect_get_transaction_state()
        .returning(|_| Ok(Some(TransactionState::Empty)));

    // Neither commit_transaction nor abort_transaction should be called
    // (no expects set for them)

    // Create HandlerContext AFTER setting expectations
    let env = TestEnv::new();
    let ctx = env.ctx(&mock);

    for committed in [true, false] {
        let err = transaction::handle_end_txn(&ctx, "txn-1", 1000, 0, committed).unwrap_err();
        assert_eq!(err.to_kafka_error_code(), ERROR_INVALID_TXN_STATE);
    }
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
        e => panic!("Expected InvalidTransactionState error, got {e:?}"),
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
        e => panic!("Expected TransactionalIdNotFound error, got {e:?}"),
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
        e => panic!("Expected ProducerFenced error, got {e:?}"),
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
    mock.expect_txn_has_group().returning(|_, _| Ok(true));
    crate::testing::expect_topics(&mut mock, |_| Ok(Some((1, 3))));
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
    mock.expect_txn_has_group().returning(|_, _| Ok(true));
    crate::testing::expect_topics(&mut mock, |_| Ok(Some((1, 3))));
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
    mock.expect_txn_has_group().returning(|_, _| Ok(true));
    // Topic doesn't exist
    crate::testing::expect_topics(&mut mock, |_| Ok(None));

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
    topics_with_partitions(&mut mock, 1);
    mock.expect_store_txn_pending_offset().never();

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

    // A refused transaction is answered on every partition, not as a handler error.
    assert_eq!(
        result.unwrap().topics[0].partitions[0].error_code,
        ERROR_INVALID_TXN_STATE
    );
}

#[test]
fn test_txn_offset_commit_producer_fenced() {
    let mut mock = MockKafkaStore::new();
    topics_with_partitions(&mut mock, 1);
    mock.expect_store_txn_pending_offset().never();

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

    // PRODUCER_FENCED here; TxnOffsetCommitApi answers it INVALID_PRODUCER_EPOCH on v0-3.
    assert_eq!(
        result.unwrap().topics[0].partitions[0].error_code,
        ERROR_PRODUCER_FENCED
    );
}

#[test]
fn test_txn_offset_commit_store_error() {
    let mut mock = MockKafkaStore::new();

    mock.expect_validate_transaction()
        .returning(|_, _, _| Ok(()));
    mock.expect_get_transaction_state()
        .returning(|_| Ok(Some(TransactionState::Ongoing)));
    mock.expect_txn_has_group().returning(|_, _| Ok(true));
    crate::testing::expect_topics(&mut mock, |_| Ok(Some((1, 3))));
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

// ========== TxnOffsetCommit check order (Kafka 3.7) ==========

/// A TxnOffsetCommit from a stale member ("zombie", generation 5) of a group the
/// coordinator does not know, answered with the codes of its partition.
fn zombie_txn_offset_commit(
    mock: &MockKafkaStore,
    group_id: &str,
    producer_epoch: i16,
) -> crate::kafka::error::Result<Vec<i16>> {
    let env = TestEnv::new();
    let ctx = env.ctx(mock);
    let topics = vec![("test-topic".to_string(), vec![(0, 1i64, None)])];
    transaction::handle_txn_offset_commit(
        &ctx,
        "txn-1",
        1000,
        producer_epoch,
        group_id,
        5,
        "zombie",
        topics,
    )
    .map(|response| {
        response
            .topics
            .iter()
            .flat_map(|t| t.partitions.iter())
            .map(|p| p.error_code)
            .collect()
    })
}

/// Kafka 3.7 verifies the transaction before the member: a group the Ongoing transaction
/// did not add is INVALID_TXN_STATE even when the generation is stale.
#[test]
fn test_txn_offset_commit_group_not_added_wins_over_stale_generation() {
    let mut mock = MockKafkaStore::new();
    topics_with_partitions(&mut mock, 1);
    mock.expect_validate_transaction()
        .times(1)
        .returning(|_, _, _| Ok(()));
    mock.expect_get_transaction_state()
        .returning(|_| Ok(Some(TransactionState::Ongoing)));
    mock.expect_txn_has_group()
        .times(1)
        .returning(|_, _| Ok(false));
    mock.expect_store_txn_pending_offset().never();
    assert_eq!(
        zombie_txn_offset_commit(&mock, "consumer-group", 0).unwrap(),
        vec![ERROR_INVALID_TXN_STATE]
    );
}

/// A fenced producer epoch wins over a stale generation: the fence error (answered as
/// INVALID_PRODUCER_EPOCH or PRODUCER_FENCED by version), not UNKNOWN_MEMBER_ID.
#[test]
fn test_txn_offset_commit_fenced_epoch_wins_over_stale_generation() {
    let mut mock = MockKafkaStore::new();
    topics_with_partitions(&mut mock, 1);
    mock.expect_validate_transaction()
        .times(1)
        .returning(|_, _, _| Err(KafkaError::producer_fenced(1000, 0, 1)));
    mock.expect_txn_has_group().never();
    mock.expect_store_txn_pending_offset().never();
    assert_eq!(
        zombie_txn_offset_commit(&mock, "consumer-group", 0).unwrap(),
        vec![ERROR_PRODUCER_FENCED]
    );
}

/// Once the transaction and the group check out, an unknown member is UNKNOWN_MEMBER_ID
/// and nothing is stored; the transaction was verified first.
#[test]
fn test_txn_offset_commit_added_group_unknown_member() {
    let mut mock = MockKafkaStore::new();
    topics_with_partitions(&mut mock, 1);
    mock.expect_validate_transaction()
        .times(1)
        .returning(|_, _, _| Ok(()));
    mock.expect_get_transaction_state()
        .returning(|_| Ok(Some(TransactionState::Ongoing)));
    mock.expect_txn_has_group()
        .times(1)
        .returning(|_, _| Ok(true));
    mock.expect_store_txn_pending_offset().never();
    assert_eq!(
        zombie_txn_offset_commit(&mock, "consumer-group", 0).unwrap(),
        vec![ERROR_UNKNOWN_MEMBER_ID]
    );
}

/// Kafka 3.7 refuses the empty group id before looking at the transaction (no transaction
/// expectations here, so a transaction lookup would panic): INVALID_GROUP_ID on every
/// partition that exists, UNKNOWN_TOPIC_OR_PARTITION on the rest, nothing stored.
#[test]
fn test_txn_offset_commit_empty_group_id() {
    let mut mock = MockKafkaStore::new();
    topics_with_partitions(&mut mock, 2);
    mock.expect_store_txn_pending_offset().never();
    let env = TestEnv::new();
    let ctx = env.ctx(&mock);
    let topics = vec![
        (
            "test-topic".to_string(),
            vec![(0, 1i64, None), (2, 1, None), (0, 3, None)],
        ),
        ("missing-topic".to_string(), vec![(0, 1i64, None)]),
    ];
    let response =
        transaction::handle_txn_offset_commit(&ctx, "nosuch", 999, 3, "", 5, "zombie", topics)
            .unwrap();
    let codes: Vec<(String, i32, i16)> = response
        .topics
        .iter()
        .flat_map(|t| {
            t.partitions
                .iter()
                .map(move |p| (t.name.0.to_string(), p.partition_index, p.error_code))
        })
        .collect();
    assert_eq!(
        codes,
        vec![
            ("test-topic".to_string(), 0, ERROR_INVALID_GROUP_ID),
            (
                "test-topic".to_string(),
                2,
                ERROR_UNKNOWN_TOPIC_OR_PARTITION
            ),
            (
                "missing-topic".to_string(),
                0,
                ERROR_UNKNOWN_TOPIC_OR_PARTITION
            ),
        ]
    );
}

/// Kafka 3.7 answers a partition that does not exist UNKNOWN_TOPIC_OR_PARTITION whatever
/// refuses the commit (KafkaApis filters those out before it asks the coordinator): the
/// refusal's code goes only to the partitions that exist, and nothing is stored.
#[test]
fn test_txn_offset_commit_refusal_spares_unknown_partitions() {
    type Setup = fn(&mut MockKafkaStore);
    let ongoing: Setup = |mock| {
        mock.expect_validate_transaction()
            .returning(|_, _, _| Ok(()));
        mock.expect_get_transaction_state()
            .returning(|_| Ok(Some(TransactionState::Ongoing)));
    };
    let cases: [(&str, Setup, i16); 5] = [
        (
            "unknown producer",
            |mock| {
                mock.expect_validate_transaction()
                    .returning(|txn, _, _| Err(KafkaError::transactional_id_not_found(txn)));
            },
            ERROR_INVALID_PRODUCER_ID_MAPPING,
        ),
        (
            "fenced epoch",
            |mock| {
                mock.expect_validate_transaction()
                    .returning(|_, _, _| Err(KafkaError::producer_fenced(1000, 0, 1)));
            },
            ERROR_PRODUCER_FENCED,
        ),
        (
            "not Ongoing",
            |mock| {
                mock.expect_validate_transaction()
                    .returning(|_, _, _| Ok(()));
                mock.expect_get_transaction_state()
                    .returning(|_| Ok(Some(TransactionState::Empty)));
            },
            ERROR_INVALID_TXN_STATE,
        ),
        (
            "group not added",
            |mock| {
                mock.expect_validate_transaction()
                    .returning(|_, _, _| Ok(()));
                mock.expect_get_transaction_state()
                    .returning(|_| Ok(Some(TransactionState::Ongoing)));
                mock.expect_txn_has_group().returning(|_, _| Ok(false));
            },
            ERROR_INVALID_TXN_STATE,
        ),
        (
            "unknown member",
            |mock| {
                mock.expect_txn_has_group().returning(|_, _| Ok(true));
            },
            ERROR_UNKNOWN_MEMBER_ID,
        ),
    ];
    for (name, setup, code) in cases {
        let mut mock = MockKafkaStore::new();
        topics_with_partitions(&mut mock, 2);
        if name == "unknown member" {
            ongoing(&mut mock);
        }
        setup(&mut mock);
        mock.expect_store_txn_pending_offset().never();
        let env = TestEnv::new();
        let ctx = env.ctx(&mock);
        let topics = vec![
            (
                "test-topic".to_string(),
                vec![(0, 1i64, None), (7, 1, None)],
            ),
            ("missing-topic".to_string(), vec![(0, 1i64, None)]),
        ];
        let response = transaction::handle_txn_offset_commit(
            &ctx,
            "txn-1",
            1000,
            0,
            "consumer-group",
            5,
            "zombie",
            topics,
        )
        .unwrap();
        let codes: Vec<i16> = response
            .topics
            .iter()
            .flat_map(|t| t.partitions.iter().map(|p| p.error_code))
            .collect();
        assert_eq!(
            codes,
            vec![
                code,
                ERROR_UNKNOWN_TOPIC_OR_PARTITION,
                ERROR_UNKNOWN_TOPIC_OR_PARTITION
            ],
            "{name}"
        );
    }
}

/// A refused TxnOffsetCommit looks each topic up once, not once per partition: a long
/// name over many partitions was hashed per partition, so four times the name took four
/// times as long.
#[test]
fn test_txn_offset_commit_refusal_costs_nothing_per_partition_for_the_name() {
    let mut mock = MockKafkaStore::new();
    topics_with_partitions(&mut mock, 1 << 20);
    mock.expect_store_txn_pending_offset().never();
    let env = TestEnv::new();
    let ctx = env.ctx(&mock);
    let growth = crate::testing::flat_growth(8 * 1024, |len| {
        let topics = vec![(
            "n".repeat(len),
            (0..10_000).map(|p| (p, 1i64, None)).collect(),
        )];
        let response =
            transaction::handle_txn_offset_commit(&ctx, "nosuch", 999, 3, "", 5, "", topics)
                .unwrap();
        assert!(response.topics[0]
            .partitions
            .iter()
            .all(|p| p.error_code == ERROR_INVALID_GROUP_ID));
    });
    assert!(
        growth < 2.0,
        "4x the name length took {growth:.1}x the time"
    );
}

/// Kafka 3.7 (offset.metadata.max.bytes = 4096) answers a transactional offset commit with
/// longer metadata OFFSET_METADATA_TOO_LARGE and stores the request's other partitions.
#[test]
fn test_txn_offset_commit_metadata_too_large() {
    let mut mock = MockKafkaStore::new();
    mock.expect_validate_transaction()
        .returning(|_, _, _| Ok(()));
    mock.expect_get_transaction_state()
        .returning(|_| Ok(Some(TransactionState::Ongoing)));
    mock.expect_txn_has_group().returning(|_, _| Ok(true));
    crate::testing::expect_topics(&mut mock, |_| Ok(Some((1, 3))));
    mock.expect_store_txn_pending_offset()
        .withf(|_, _, _, partition, _, _| *partition != 1)
        .times(2)
        .returning(|_, _, _, _, _, _| Ok(()));
    let env = TestEnv::new();
    let ctx = env.ctx(&mock);
    let topics = vec![(
        "t".to_string(),
        vec![
            (0, 1i64, Some("x".repeat(OFFSET_METADATA_MAX_LENGTH))),
            (1, 1i64, Some("x".repeat(OFFSET_METADATA_MAX_LENGTH + 1))),
            (2, 1i64, None),
        ],
    )];
    let response =
        transaction::handle_txn_offset_commit(&ctx, "txn-1", 1000, 0, "g", -1, "", topics).unwrap();
    let codes: Vec<i16> = response.topics[0]
        .partitions
        .iter()
        .map(|p| p.error_code)
        .collect();
    assert_eq!(
        codes,
        [ERROR_NONE, ERROR_OFFSET_METADATA_TOO_LARGE, ERROR_NONE]
    );
}

/// A retried EndTxn answered from the finished state ends nothing: it names no
/// partitions to wake and reads no registry (no expectation for it).
#[test]
fn test_end_txn_retry_names_no_partitions() {
    for (state, committed) in [
        (TransactionState::CompleteCommit, true),
        (TransactionState::CompleteAbort, false),
    ] {
        let mut mock = MockKafkaStore::new();
        mock.expect_validate_transaction()
            .returning(|_, _, _| Ok(()));
        mock.expect_get_transaction_state()
            .returning(move |_| Ok(Some(state.clone())));
        let env = TestEnv::new();
        let ctx = env.ctx(&mock);
        let (response, ended) = transaction::end_txn(&ctx, "txn-1", 1000, 0, committed).unwrap();
        assert_eq!(response.error_code, ERROR_NONE);
        assert_eq!(ended, crate::kafka::storage::TxnPartitions::new());
    }
}
