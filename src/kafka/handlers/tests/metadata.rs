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

    // A missing topic is created once it was looked up.
    crate::testing::expect_topics(&mut mock, |_| Ok(None));
    mock.expect_get_or_create_topics()
        .withf(|names, _| names == ["my-topic"])
        .returning(|_, partition_count| {
            Ok(vec![TopicMetadata {
                name: "my-topic".to_string(),
                id: 5,
                partition_count,
            }])
        });

    // Create HandlerContext AFTER setting expectations
    let env = TestEnv::new();
    let ctx = env.ctx(&mock);

    let response =
        metadata::handle_metadata(&ctx, Some(vec!["my-topic".to_string()]), true).unwrap();

    assert_eq!(response.topics.len(), 1);
}

#[test]
fn test_handle_metadata_no_auto_create_missing_topic() {
    // With allow_auto_topic_creation = false, a missing requested topic must be reported
    // as UNKNOWN_TOPIC_OR_PARTITION and NOT created. Not setting expect_get_or_create_topics means
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

/// Kafka 3.7 answers each distinct requested name once: a name repeated 10,000 times is
/// looked up once (one statement for every name), created once (one statement for every
/// missing name) and answered once.
#[test]
fn test_handle_metadata_repeated_name_is_answered_once() {
    let mut mock = MockKafkaStore::new();
    mock.expect_get_topic_metadata()
        .withf(|names| names.is_some_and(|n| n.len() == 3))
        .times(1)
        .returning(|_| {
            Ok(vec![TopicMetadata {
                name: "exists".to_string(),
                id: 1,
                partition_count: 2,
            }])
        });
    mock.expect_get_or_create_topics()
        .withf(|names, _| names == ["new"])
        .times(1)
        .returning(|_, _| {
            Ok(vec![TopicMetadata {
                name: "new".to_string(),
                id: 2,
                partition_count: 1,
            }])
        });

    let env = TestEnv::new();
    let ctx = env.ctx(&mock);
    let mut names = Vec::new();
    for _ in 0..10_000 {
        names.extend(["new", "exists", "bad name"].map(String::from));
    }
    let response = metadata::handle_metadata(&ctx, Some(names), true).unwrap();
    let topics: Vec<(String, i16, usize)> = response
        .topics
        .iter()
        .map(|t| {
            (
                t.name
                    .as_ref()
                    .map_or_else(String::new, |n| n.0.to_string()),
                t.error_code,
                t.partitions.len(),
            )
        })
        .collect();
    assert_eq!(
        topics,
        [
            ("new".to_string(), ERROR_NONE, 1),
            ("exists".to_string(), ERROR_NONE, 2),
            ("bad name".to_string(), ERROR_INVALID_TOPIC_EXCEPTION, 0),
        ]
    );
}

/// Without auto-creation Kafka 3.7 still reports a missing name it would not accept
/// INVALID_TOPIC_EXCEPTION, and a valid missing name UNKNOWN_TOPIC_OR_PARTITION.
#[test]
fn test_handle_metadata_no_auto_create_invalid_name_is_invalid_topic() {
    let mut mock = MockKafkaStore::new();
    mock.expect_get_topic_metadata().returning(|_| Ok(vec![]));
    let env = TestEnv::new();
    let ctx = env.ctx(&mock);
    let names = vec!["l".repeat(250), "ok-name".to_string(), "..".to_string()];
    let response = metadata::handle_metadata(&ctx, Some(names), false).unwrap();
    let codes: Vec<i16> = response.topics.iter().map(|t| t.error_code).collect();
    assert_eq!(
        codes,
        [
            ERROR_INVALID_TOPIC_EXCEPTION,
            ERROR_UNKNOWN_TOPIC_OR_PARTITION,
            ERROR_INVALID_TOPIC_EXCEPTION
        ]
    );
}

/// A Metadata auto-create that `pg_kafka.max_total_partitions` refuses reports
/// POLICY_VIOLATION on the names it would have created; a requested topic that exists is
/// still answered.
#[test]
fn test_handle_metadata_auto_create_over_the_total_partition_cap() {
    let mut mock = MockKafkaStore::new();
    crate::testing::expect_topics(&mut mock, |name| Ok((name == "there").then_some((1, 2))));
    mock.expect_get_or_create_topics()
        .withf(|names, _| names == ["new-a", "new-b"])
        .times(1)
        .returning(|_, _| {
            Err(KafkaError::PolicyViolation {
                message: "over pg_kafka.max_total_partitions".to_string(),
            })
        });
    let env = TestEnv::new();
    let ctx = env.ctx(&mock);
    let names = ["new-a", "there", "new-b"].map(str::to_string).to_vec();
    let response = metadata::handle_metadata(&ctx, Some(names), true).unwrap();
    let codes: Vec<(String, i16, usize)> = response
        .topics
        .iter()
        .map(|t| {
            (
                t.name.as_ref().unwrap().to_string(),
                t.error_code,
                t.partitions.len(),
            )
        })
        .collect();
    assert_eq!(
        codes,
        [
            ("new-a".to_string(), ERROR_POLICY_VIOLATION, 0),
            ("there".to_string(), ERROR_NONE, 2),
            ("new-b".to_string(), ERROR_POLICY_VIOLATION, 0),
        ]
    );
}
