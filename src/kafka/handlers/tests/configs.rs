//! DescribeConfigs, IncrementalAlterConfigs and DeleteRecords handler tests.

use crate::kafka::constants::*;
use crate::kafka::handlers::configs;
use crate::testing::mocks::MockKafkaStore;
use crate::testing::{expect_topics, TestEnv};

// ========== DescribeConfigs Handler Tests (API 32) ==========

const RESOURCE_TYPE_TOPIC: i8 = 2;
const RESOURCE_TYPE_BROKER: i8 = 4;
const OP_SET: i8 = 0;
const OP_DELETE: i8 = 1;
const OP_APPEND: i8 = 2;

fn find_config<'a>(
    result: &'a kafka_protocol::messages::describe_configs_response::DescribeConfigsResult,
    name: &str,
) -> Option<&'a kafka_protocol::messages::describe_configs_response::DescribeConfigsResourceResult>
{
    result.configs.iter().find(|c| c.name.as_str() == name)
}

/// A topic with a retention override reports it as a DYNAMIC_TOPIC_CONFIG
/// with the override value; without one, the global GUC value is reported
/// as a DEFAULT config (-1 when the global sweep is off).
#[test]
fn test_handle_describe_configs_retention_sources() {
    let mut mock = MockKafkaStore::new();
    expect_topics(&mut mock, |name| {
        Ok(Some(if name == "with-override" {
            (1, 1)
        } else {
            (2, 1)
        }))
    });
    mock.expect_get_topics_retention_ms().returning(|ids| {
        Ok(ids
            .iter()
            .map(|&id| (id, if id == 1 { Some(60_000) } else { None }))
            .collect())
    });

    let env = TestEnv::new();
    let ctx = env.ctx(&mock);

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
    expect_topics(&mut mock, |_| Ok(Some((1, 1))));
    mock.expect_get_topics_retention_ms()
        .returning(|ids| Ok(ids.iter().map(|&id| (id, None)).collect()));

    let env = TestEnv::new();
    let ctx = env.ctx(&mock);

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
    expect_topics(&mut mock, |_| Ok(None));

    let env = TestEnv::new();
    let ctx = env.ctx(&mock);

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
    expect_topics(&mut mock, |_| Ok(Some((7, 1))));
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

    let env = TestEnv::new();
    let ctx = env.ctx(&mock);

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
                "u".to_string(),
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
    expect_topics(&mut mock, |_| Ok(Some((7, 1))));
    mock.expect_set_topic_retention_ms().times(0);

    let env = TestEnv::new();
    let ctx = env.ctx(&mock);

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
    expect_topics(&mut mock, |_| Ok(Some((7, 1))));
    mock.expect_set_topic_retention_ms().times(0);

    let env = TestEnv::new();
    let ctx = env.ctx(&mock);

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
                "u".to_string(),
                vec![("retention.ms".to_string(), OP_SET, Some("abc".to_string()))],
            ),
            (
                RESOURCE_TYPE_TOPIC,
                "v".to_string(),
                // Valid SET followed by an unsupported key: the valid one
                // must NOT be applied (all-or-nothing per resource).
                vec![
                    ("retention.ms".to_string(), OP_SET, Some("1000".to_string())),
                    ("segment.ms".to_string(), OP_SET, Some("1".to_string())),
                ],
            ),
            (
                RESOURCE_TYPE_TOPIC,
                "w".to_string(),
                vec![("retention.ms".to_string(), OP_APPEND, Some("1".to_string()))],
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
    expect_topics(&mut mock, |_| Ok(Some((3, 2))));
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
            mockall::predicate::eq(1),
            mockall::predicate::eq(10i64), // -1 resolved to the HWM
        )
        .times(1)
        .returning(|_, _, before| Ok(before));

    let env = TestEnv::new();
    let ctx = env.ctx(&mock);

    let response =
        configs::handle_delete_records(&ctx, vec![("t".to_string(), vec![(0, 5), (1, -1)])])
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
    expect_topics(&mut mock, |name| Ok((name == "known").then_some((3, 1))));
    mock.expect_get_high_watermark().returning(|_, _| Ok(10));
    mock.expect_delete_records_before().times(0);

    let env = TestEnv::new();
    let ctx = env.ctx(&mock);

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

/// Kafka 3.7 refuses every entry of a resource named twice in IncrementalAlterConfigs
/// (INVALID_REQUEST) and alters nothing for it; other resources are altered.
#[test]
fn test_handle_incremental_alter_configs_repeated_resource_is_refused() {
    let mut mock = MockKafkaStore::new();
    expect_topics(&mut mock, |_| Ok(Some((7, 1))));
    mock.expect_set_topic_retention_ms()
        .with(
            mockall::predicate::eq(7),
            mockall::predicate::eq(Some(5i64)),
        )
        .times(1)
        .returning(|_, _| Ok(()));
    let env = TestEnv::new();
    let ctx = env.ctx(&mock);
    let set = |name: &str, ms: &str| {
        (
            RESOURCE_TYPE_TOPIC,
            name.to_string(),
            vec![("retention.ms".to_string(), OP_SET, Some(ms.to_string()))],
        )
    };
    let response = configs::handle_incremental_alter_configs(
        &ctx,
        vec![set("t", "1"), set("other", "5"), set("t", "2")],
        false,
    )
    .unwrap();
    let codes: Vec<i16> = response.responses.iter().map(|r| r.error_code).collect();
    assert_eq!(
        codes,
        [ERROR_INVALID_REQUEST, ERROR_NONE, ERROR_INVALID_REQUEST]
    );
    assert_eq!(
        response.responses[0].error_message.as_deref(),
        Some("Each resource must appear at most once.")
    );
}

/// DeleteRecords acts on a repeated partition once, with its last entry, and answers it
/// once (Kafka 3.7 collects the partitions into a map).
#[test]
fn test_handle_delete_records_repeated_partition_is_deleted_once() {
    let mut mock = MockKafkaStore::new();
    expect_topics(&mut mock, |_| Ok(Some((3, 1))));
    mock.expect_get_high_watermark()
        .times(1)
        .returning(|_, _| Ok(10));
    mock.expect_delete_records_before()
        .with(
            mockall::predicate::eq(3),
            mockall::predicate::eq(0),
            mockall::predicate::eq(4i64),
        )
        .times(1)
        .returning(|_, _, before| Ok(before));
    let env = TestEnv::new();
    let ctx = env.ctx(&mock);
    let response = configs::handle_delete_records(
        &ctx,
        vec![
            ("t".to_string(), vec![(0, 2), (0, 7)]),
            ("t".to_string(), vec![(0, 4)]),
        ],
    )
    .unwrap();
    assert_eq!(response.topics.len(), 1);
    assert_eq!(response.topics[0].partitions.len(), 1);
    assert_eq!(response.topics[0].partitions[0].low_watermark, 4);
}

/// DescribeConfigs answers every entry of a repeated topic but reads its configs once.
/// Distinct topics share one retention lookup: a statement per topic held the DB thread for
/// every topic the request names.
#[test]
fn test_handle_describe_configs_of_many_topics_reads_retention_once() {
    let mut mock = MockKafkaStore::new();
    expect_topics(&mut mock, |name| {
        Ok(name
            .strip_prefix('t')
            .and_then(|n| n.parse().ok())
            .map(|id| (id, 1)))
    });
    mock.expect_get_topics_retention_ms()
        .times(1)
        .returning(|ids| Ok(ids.iter().map(|&id| (id, Some(i64::from(id)))).collect()));
    let env = TestEnv::new();
    let ctx = env.ctx(&mock);
    let resources = (0..1000)
        .map(|i| {
            (
                RESOURCE_TYPE_TOPIC,
                format!("t{i}"),
                Some(vec!["retention.ms".to_string()]),
            )
        })
        .collect();
    let response = configs::handle_describe_configs(&ctx, resources, 0).unwrap();
    assert_eq!(response.results.len(), 1000);
    for (i, result) in response.results.iter().enumerate() {
        assert_eq!(result.error_code, ERROR_NONE);
        assert_eq!(
            result.configs[0].value.as_deref(),
            Some(i.to_string().as_str())
        );
    }
}

#[test]
fn test_handle_describe_configs_repeated_topic_is_read_once() {
    let mut mock = MockKafkaStore::new();
    expect_topics(&mut mock, |_| Ok(Some((1, 1))));
    mock.expect_get_topics_retention_ms()
        .times(1)
        .returning(|ids| Ok(ids.iter().map(|&id| (id, None)).collect()));
    let env = TestEnv::new();
    let ctx = env.ctx(&mock);
    let response = configs::handle_describe_configs(
        &ctx,
        vec![(RESOURCE_TYPE_TOPIC, "t".to_string(), None); 1000],
        0,
    )
    .unwrap();
    assert_eq!(response.results.len(), 1000);
    assert!(response.results.iter().all(|r| r.error_code == ERROR_NONE));
}

/// Kafka 3.7 refuses a SET (or APPEND / SUBTRACT) without a value as a malformed request,
/// INVALID_REQUEST, before it looks at the key; a DELETE needs no value.
#[test]
fn test_handle_incremental_alter_configs_null_value_is_invalid_request() {
    let mut mock = MockKafkaStore::new();
    expect_topics(&mut mock, |_| Ok(Some((7, 1))));
    mock.expect_set_topic_retention_ms()
        .times(1)
        .returning(|_, _| Ok(()));
    let env = TestEnv::new();
    let ctx = env.ctx(&mock);
    let resource = |name: &str, configs: Vec<(String, i8, Option<String>)>| {
        (RESOURCE_TYPE_TOPIC, name.to_string(), configs)
    };
    let response = configs::handle_incremental_alter_configs(
        &ctx,
        vec![
            resource("a", vec![("retention.ms".to_string(), OP_SET, None)]),
            resource("b", vec![("max.message.bytes".to_string(), OP_SET, None)]),
            resource(
                "c",
                vec![
                    (
                        "max.message.bytes".to_string(),
                        OP_SET,
                        Some("1".to_string()),
                    ),
                    ("retention.ms".to_string(), OP_APPEND, None),
                ],
            ),
            resource("d", vec![("retention.ms".to_string(), OP_DELETE, None)]),
        ],
        false,
    )
    .unwrap();
    let codes: Vec<i16> = response.responses.iter().map(|r| r.error_code).collect();
    assert_eq!(
        codes,
        [
            ERROR_INVALID_REQUEST,
            ERROR_INVALID_REQUEST,
            ERROR_INVALID_REQUEST,
            ERROR_NONE
        ]
    );
}

/// Kafka 3.7 (`ConfigAdminManager.preprocess`) refuses a resource that names one config
/// key twice, INVALID_REQUEST "Error due to duplicate config keys", and applies none of
/// its alterations (a SET then a DELETE of retention.ms leaves it as it is).
#[test]
fn test_handle_incremental_alter_configs_duplicate_keys_are_invalid_request() {
    let mut mock = MockKafkaStore::new();
    expect_topics(&mut mock, |_| Ok(Some((7, 1))));
    // Only the resource without a repeated key is applied.
    mock.expect_set_topic_retention_ms()
        .withf(|_, ms| *ms == Some(5))
        .times(1)
        .returning(|_, _| Ok(()));
    let env = TestEnv::new();
    let ctx = env.ctx(&mock);
    let set = |ms: &str| ("retention.ms".to_string(), OP_SET, Some(ms.to_string()));
    let delete = ("retention.ms".to_string(), OP_DELETE, None);
    let response = configs::handle_incremental_alter_configs(
        &ctx,
        vec![
            (
                RESOURCE_TYPE_TOPIC,
                "a".to_string(),
                vec![set("1000"), delete.clone()],
            ),
            (
                RESOURCE_TYPE_TOPIC,
                "b".to_string(),
                vec![set("1"), set("2")],
            ),
            (RESOURCE_TYPE_TOPIC, "c".to_string(), vec![set("5")]),
        ],
        false,
    )
    .unwrap();
    let answers: Vec<(i16, Option<&str>)> = response
        .responses
        .iter()
        .map(|r| (r.error_code, r.error_message.as_deref()))
        .collect();
    assert_eq!(
        answers[..2],
        [
            (
                ERROR_INVALID_REQUEST,
                Some("Error due to duplicate config keys")
            ),
            (
                ERROR_INVALID_REQUEST,
                Some("Error due to duplicate config keys")
            ),
        ]
    );
    assert_eq!(answers[2].0, ERROR_NONE);
}

/// Kafka 3.7 checks repeated keys and missing values before it looks the topic up, so a
/// malformed resource naming an unknown topic is INVALID_REQUEST, not
/// UNKNOWN_TOPIC_OR_PARTITION; a well-formed one is UNKNOWN_TOPIC_OR_PARTITION. The null
/// message names every key without a value.
#[test]
fn test_handle_incremental_alter_configs_malformed_before_topic_lookup() {
    let mut mock = MockKafkaStore::new();
    expect_topics(&mut mock, |_| Ok(None));
    let env = TestEnv::new();
    let ctx = env.ctx(&mock);
    let set =
        |name: &str, value: Option<&str>| (name.to_string(), OP_SET, value.map(str::to_string));
    let response = configs::handle_incremental_alter_configs(
        &ctx,
        vec![
            (
                RESOURCE_TYPE_TOPIC,
                "dup".to_string(),
                vec![
                    set("retention.ms", Some("1")),
                    set("retention.ms", Some("2")),
                ],
            ),
            (
                RESOURCE_TYPE_TOPIC,
                "null".to_string(),
                vec![set("retention.ms", None), set("segment.ms", None)],
            ),
            (
                RESOURCE_TYPE_TOPIC,
                "plain".to_string(),
                vec![set("retention.ms", Some("1"))],
            ),
        ],
        false,
    )
    .unwrap();
    let answers: Vec<(i16, Option<&str>)> = response
        .responses
        .iter()
        .map(|r| (r.error_code, r.error_message.as_deref()))
        .collect();
    assert_eq!(
        answers,
        [
            (
                ERROR_INVALID_REQUEST,
                Some("Error due to duplicate config keys")
            ),
            (
                ERROR_INVALID_REQUEST,
                Some("Null value not supported for : retention.ms, segment.ms")
            ),
            (
                ERROR_UNKNOWN_TOPIC_OR_PARTITION,
                Some("Unknown topic 'plain'")
            ),
        ]
    );
}

/// cleanup.policy is a LIST config (Kafka 3.7's ConfigType 7), retention.ms a LONG (5).
#[test]
fn test_handle_describe_configs_reports_kafka_config_types() {
    let mut mock = MockKafkaStore::new();
    expect_topics(&mut mock, |_| Ok(Some((1, 1))));
    mock.expect_get_topics_retention_ms()
        .returning(|ids| Ok(ids.iter().map(|&id| (id, None)).collect()));
    let env = TestEnv::new();
    let ctx = env.ctx(&mock);
    let response = configs::handle_describe_configs(
        &ctx,
        vec![(RESOURCE_TYPE_TOPIC, "t".to_string(), None)],
        0,
    )
    .unwrap();
    let types: Vec<(String, i8)> = response.results[0]
        .configs
        .iter()
        .map(|c| (c.name.to_string(), c.config_type))
        .collect();
    assert_eq!(
        types,
        [
            ("retention.ms".to_string(), 5),
            ("cleanup.policy".to_string(), 7)
        ]
    );
}
