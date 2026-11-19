//! Metadata handler tests.

use crate::kafka::constants::*;
use crate::kafka::error::KafkaError;
use crate::kafka::handlers::metadata;
use crate::kafka::storage::TopicMetadata;
use crate::testing::mocks::MockKafkaStore;
use crate::testing::TestEnv;

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
    let env = TestEnv::new();
    let ctx = env.ctx(&mock);

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
    let env = TestEnv::new();
    let ctx = env.ctx(&mock);

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

    let env = TestEnv::new();
    let ctx = env.ctx(&mock);

    let response =
        metadata::handle_metadata(&ctx, Some(vec!["missing".to_string()]), false).unwrap();

    assert_eq!(response.topics.len(), 1);
    assert_eq!(
        response.topics[0].error_code,
        ERROR_UNKNOWN_TOPIC_OR_PARTITION
    );
    assert_eq!(
        response.topics[0].partitions,
        [] as [kafka_protocol::messages::metadata_response::MetadataResponsePartition; 0]
    );
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

    let env = TestEnv::new();
    let ctx = env.ctx(&mock);

    let result = metadata::handle_metadata(&ctx, None, true);
    assert!(result.is_err());
}
