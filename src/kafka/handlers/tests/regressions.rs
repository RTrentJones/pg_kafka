//! Regression tests for the bug hunt at commit 6c982be: one test per confirmed bug
//! that a handler unit test can reach.

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
        transaction::handle_txn_offset_commit(&ctx, "txn", 1000, 0, "g", -1, "", topics).unwrap();
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
    let response = coordinator::handle_describe_groups(&ctx, vec!["missing".to_string()]).unwrap();
    assert_eq!(response.groups[0].error_code, ERROR_NONE);
    assert_eq!(response.groups[0].group_state.as_str(), "Dead");
}
