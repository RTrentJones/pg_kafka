//! InitProducerId and idempotent-produce handler tests.

use crate::kafka::constants::*;
use crate::kafka::error::KafkaError;
use crate::kafka::handlers::init_producer_id;
use crate::kafka::handlers::produce;
use crate::kafka::messages::ProducerMetadata;
use crate::kafka::messages::{PartitionProduceData, Record, TopicProduceData};
use crate::testing::mocks::MockKafkaStore;
use crate::testing::TestEnv;

// ========== InitProducerId Handler Tests ==========

#[test]
fn test_handle_init_producer_id_new_producer() {
    let mut mock = MockKafkaStore::new();

    // Expect allocation of new producer ID with epoch 0
    mock.expect_allocate_producer_id()
        .times(1)
        .returning(|_, _| Ok((1000, 0))); // producer_id=1000, epoch=0

    // Create HandlerContext AFTER setting expectations
    let env = TestEnv::new();
    let ctx = env.ctx(&mock);

    let response = init_producer_id::handle_init_producer_id(
        &ctx,
        None,  // No transactional ID
        60000, // New producer
        None,  // No producer id presented
        Some("test-client"),
    )
    .unwrap();

    assert_eq!(response.error_code, ERROR_NONE);
    assert_eq!(response.producer_id.0, 1000);
    assert_eq!(response.producer_epoch, 0);
}

// ========== Produce Handler with Idempotency Tests ==========

#[test]
fn test_handle_produce_with_idempotent_producer_success() {
    let mut mock = MockKafkaStore::new();
    mock.expect_get_log_start_offset().returning(|_, _| Ok(0));

    // Expect topic lookup
    mock.expect_get_or_create_topic()
        .returning(|_, _| Ok((1, 1)));

    // Expect sequence check to pass (returns true = insert)
    mock.expect_open_txn_epoch_on_partition()
        .returning(|_, _, _| Ok(None));
    mock.expect_check_and_update_sequence()
        .times(1)
        .returning(|_, _, _, _, _, _| Ok(crate::kafka::storage::SequenceCheck::Append));

    // Expect record insertion
    mock.expect_insert_records()
        .times(1)
        .returning(|_, _, _| Ok(0));

    // The producer batch is recorded (with its offset) after the insert
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
        is_transactional: false,
    };

    // Create HandlerContext AFTER setting expectations
    let env = TestEnv::new();
    let ctx = env.ctx(&mock);

    let response =
        produce::handle_produce(&ctx, topic_data, Some(&producer_metadata), None).unwrap();

    assert_eq!(response.responses.len(), 1);
    let partition_response = &response.responses[0].partition_responses[0];
    assert_eq!(partition_response.error_code, ERROR_NONE);
    assert_eq!(partition_response.base_offset, 0);
}

#[test]
fn test_handle_produce_insert_failure_does_not_advance_sequence() {
    // When the insert fails, the producer sequence must NOT advance, so a
    // retry re-inserts instead of being dropped as a false duplicate. No
    // record_producer_sequence expectation is set, so mockall fails on Drop if
    // the handler advances the sequence after a failed insert.
    let mut mock = MockKafkaStore::new();
    mock.expect_get_or_create_topic()
        .returning(|_, _| Ok((1, 1)));
    mock.expect_open_txn_epoch_on_partition()
        .returning(|_, _, _| Ok(None));
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
        is_transactional: false,
    };
    let env = TestEnv::new();
    let ctx = env.ctx(&mock);

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
    mock.expect_open_txn_epoch_on_partition()
        .returning(|_, _, _| Ok(None));
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
        is_transactional: false,
    };

    let env = TestEnv::new();
    let ctx = env.ctx(&mock);

    let response =
        produce::handle_produce(&ctx, topic_data, Some(&producer_metadata), None).unwrap();

    let partition_response = &response.responses[0].partition_responses[0];
    // Kafka spec: Duplicates return SUCCESS (error_code=0), not error
    assert_eq!(partition_response.error_code, ERROR_NONE);
    assert_eq!(partition_response.base_offset, 42); // The original offset
}

#[test]
fn test_handle_produce_out_of_order_sequence_rejected() {
    let mut mock = MockKafkaStore::new();

    mock.expect_get_or_create_topic()
        .returning(|_, _| Ok((1, 1)));

    // Sequence check fails with out-of-order error
    mock.expect_open_txn_epoch_on_partition()
        .returning(|_, _, _| Ok(None));
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
        is_transactional: false,
    };

    let env = TestEnv::new();
    let ctx = env.ctx(&mock);

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
        is_transactional: false,
    };

    let env = TestEnv::new();
    let ctx = env.ctx(&mock);

    let response =
        produce::handle_produce(&ctx, topic_data, Some(&producer_metadata), None).unwrap();

    let partition_response = &response.responses[0].partition_responses[0];
    assert_eq!(partition_response.error_code, ERROR_NONE);
}

/// One Produce with an idempotent batch to partition 0 and a plain batch to -1 whose
/// record routes to 0 (a one-partition topic): the routed record must not join the
/// idempotent batch, which would be sequence-checked and recorded as two records. The -1
/// entry is answered UNKNOWN_TOPIC_OR_PARTITION, as Kafka 3.7 answers any -1, whichever
/// order the two entries come in.
fn produce_idempotent_next_to_routed(routed_first: bool) {
    let mut mock = MockKafkaStore::new();
    mock.expect_get_log_start_offset().returning(|_, _| Ok(0));
    mock.expect_get_or_create_topic()
        .returning(|_, _| Ok((1, 1)));
    mock.expect_open_txn_epoch_on_partition()
        .returning(|_, _, _| Ok(None));
    mock.expect_check_and_update_sequence()
        .withf(|_, _, _, partition, base, count| *partition == 0 && *base == 0 && *count == 1)
        .times(1)
        .returning(|_, _, _, _, _, _| Ok(crate::kafka::storage::SequenceCheck::Append));
    mock.expect_insert_records()
        .withf(|_, partition, records| *partition == 0 && records.len() == 1)
        .times(1)
        .returning(|_, _, _| Ok(0));
    mock.expect_record_producer_batch()
        .withf(|b| b.first_sequence == 0 && b.last_sequence == 0)
        .times(1)
        .returning(|_| Ok(()));

    let record = |value: &[u8]| Record {
        key: Some(b"k".to_vec()),
        value: Some(value.to_vec()),
        headers: vec![],
        timestamp: None,
    };
    let idempotent = PartitionProduceData {
        partition_index: 0,
        records: vec![record(b"idempotent")],
        producer_metadata: Some(ProducerMetadata {
            producer_id: 1000,
            producer_epoch: 0,
            base_sequence: 0,
            is_transactional: false,
        }),
    };
    let routed = PartitionProduceData {
        partition_index: -1,
        records: vec![record(b"plain")],
        producer_metadata: Some(ProducerMetadata {
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            is_transactional: false,
        }),
    };
    let partitions = if routed_first {
        vec![routed, idempotent]
    } else {
        vec![idempotent, routed]
    };
    let topic_data = vec![TopicProduceData {
        name: "test-topic".to_string(),
        partitions,
    }];

    let env = TestEnv::new();
    let ctx = env.ctx(&mock);
    let response = produce::handle_produce(&ctx, topic_data, None, None).unwrap();
    let mut answers: Vec<(i32, i16)> = response.responses[0]
        .partition_responses
        .iter()
        .map(|p| (p.index, p.error_code))
        .collect();
    answers.sort_unstable();
    assert_eq!(
        answers,
        [(-1, ERROR_UNKNOWN_TOPIC_OR_PARTITION), (0, ERROR_NONE)]
    );
}

#[test]
fn test_produce_routed_records_after_idempotent_batch_refused() {
    produce_idempotent_next_to_routed(false);
}

#[test]
fn test_produce_routed_records_before_idempotent_batch_refused() {
    produce_idempotent_next_to_routed(true);
}
