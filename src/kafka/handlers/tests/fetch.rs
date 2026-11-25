//! Fetch and ListOffsets handler tests.

use crate::kafka::constants::*;
use crate::kafka::error::KafkaError;
use crate::kafka::handlers::fetch;
use crate::kafka::messages::{
    ListOffsetsPartitionData, ListOffsetsTopicData, PartitionFetchData, TopicFetchData,
};
use crate::kafka::storage::{FetchedMessage, IsolationLevel, TopicMetadata};
use crate::testing::mocks::MockKafkaStore;
use crate::testing::TestEnv;

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
        .returning(|_, _, _, _, _, _| {
            Ok(vec![FetchedMessage {
                partition_offset: 5,
                key: Some(b"key1".to_vec()),
                value: Some(b"value1".to_vec()),
                timestamp: 1_234_567_890,
                headers: vec![],
                producer_id: None,
                producer_epoch: None,
            }]
            .into())
        });

    mock.expect_get_high_watermark().returning(|_, _| Ok(6));
    mock.expect_get_last_stable_offset().returning(|_, _| Ok(6));
    // The fetch response's log_start_offset must reflect the store's
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
    let env = TestEnv::new();
    let ctx = env.ctx(&mock);

    let response = fetch::handle_fetch(&ctx, topic_data, IsolationLevel::ReadUncommitted).unwrap();

    assert_eq!(response.responses.len(), 1);
    let partition_data = &response.responses[0].partitions[0];
    assert_eq!(partition_data.error_code, ERROR_NONE);
    assert_eq!(partition_data.high_watermark, 6);
    assert_eq!(partition_data.log_start_offset, 5);
}

/// Stored headers are re-encoded into the fetch response's RecordBatch; a handler that
/// dropped them would hand consumers empty headers.
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
        .returning(|_, _, _, _, _, _| {
            Ok(vec![FetchedMessage {
                partition_offset: 0,
                key: Some(b"key1".to_vec()),
                value: Some(b"value1".to_vec()),
                timestamp: 1_234_567_890,
                headers: vec![("trace-id".to_string(), b"abc123".to_vec())],
                producer_id: None,
                producer_epoch: None,
            }]
            .into())
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

    let env = TestEnv::new();
    let ctx = env.ctx(&mock);

    let response = fetch::handle_fetch(&ctx, topic_data, IsolationLevel::ReadUncommitted).unwrap();

    // Decode the encoded RecordBatch and assert the header survived.
    let partition_data = &response.responses[0].partitions[0];
    assert_eq!(partition_data.error_code, ERROR_NONE);
    let mut records_buf = partition_data.records.clone().expect("records present");
    let decoded = kafka_protocol::records::RecordBatchDecoder::decode(&mut records_buf).unwrap();
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

/// A storage error computing the HWM must surface
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
        .returning(|_, _, _, _, _, _| Ok(vec![].into()));
    mock.expect_get_high_watermark()
        .returning(|_, _| Err(KafkaError::Internal("db down".into())));

    let topic_data = vec![TopicFetchData {
        name: "test-topic".to_string(),
        partitions: vec![PartitionFetchData {
            partition_index: 0,
            fetch_offset: 0,
            partition_max_bytes: 1024,
        }],
    }];

    let env = TestEnv::new();
    let ctx = env.ctx(&mock);

    let response = fetch::handle_fetch(&ctx, topic_data, IsolationLevel::ReadUncommitted).unwrap();

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
    let env = TestEnv::new();
    let ctx = env.ctx(&mock);

    let response = fetch::handle_fetch(&ctx, topic_data, IsolationLevel::ReadUncommitted).unwrap();

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
    mock.expect_get_last_stable_offset().returning(|_, _| Ok(1));
    mock.expect_get_earliest_offset().returning(|_, _| Ok(0));
    mock.expect_get_log_start_offset().returning(|_, _| Ok(0));
    mock.expect_fetch_records_with_isolation()
        .returning(|_, _, _, _, _, _| {
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

    let env = TestEnv::new();
    let ctx = env.ctx(&mock);

    let response = fetch::handle_fetch(&ctx, topic_data, IsolationLevel::ReadUncommitted).unwrap();

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
        .returning(|_, _, _, _, _, _| Ok(vec![].into()));
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

    let env = TestEnv::new();
    let ctx = env.ctx(&mock);

    let response = fetch::handle_fetch(&ctx, topic_data, IsolationLevel::ReadCommitted).unwrap();

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
    let env = TestEnv::new();
    let ctx = env.ctx(&mock);

    let response =
        fetch::handle_list_offsets(&ctx, topics, IsolationLevel::ReadUncommitted).unwrap();

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
    let env = TestEnv::new();
    let ctx = env.ctx(&mock);

    let response =
        fetch::handle_list_offsets(&ctx, topics, IsolationLevel::ReadUncommitted).unwrap();

    let partition_response = &response.topics[0].partitions[0];
    assert_eq!(partition_response.error_code, ERROR_NONE);
    assert_eq!(partition_response.offset, 100);
}

#[test]
fn test_handle_list_offsets_by_timestamp() {
    // A positive timestamp resolves to the earliest qualifying offset (and its record
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

    let env = TestEnv::new();
    let ctx = env.ctx(&mock);

    let response =
        fetch::handle_list_offsets(&ctx, topics, IsolationLevel::ReadUncommitted).unwrap();

    let partition_response = &response.topics[0].partitions[0];
    assert_eq!(partition_response.error_code, ERROR_NONE);
    assert_eq!(partition_response.offset, 7);
    assert_eq!(partition_response.timestamp, 1_600_000_002_000);
}

/// One ListOffsets partition query against a 2-partition topic with
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
    let env = TestEnv::new();
    let ctx = env.ctx(&mock);
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
    // read_committed LATEST is the last stable offset: the high watermark would show a
    // read_committed consumer lag it can never consume.
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
