//! Error-path tests for the handlers whose responses have no top-level error code.
//!
//! OffsetCommit, ListOffsets, DescribeGroups, CreateTopics, DeleteTopics,
//! CreatePartitions, DeleteGroups, DeleteRecords, DescribeConfigs and
//! IncrementalAlterConfigs carry their errors per topic/partition/resource/group, and
//! the request-level `build_*_error_response` builders used on a handler `Err` cannot
//! carry the code (they return an empty, error-free response). Each test injects a
//! storage failure with `MockKafkaStore` and checks that the handler returns `Ok`
//! with the error on every affected item, and that unaffected items are still
//! processed normally.

use crate::kafka::constants::*;
use crate::kafka::error::KafkaError;
use crate::kafka::handlers::{
    handle_create_partitions, handle_create_topics, handle_delete_groups, handle_delete_records,
    handle_delete_topics, handle_describe_configs, handle_describe_groups,
    handle_incremental_alter_configs, handle_join_group, handle_leave_group, handle_list_offsets,
    handle_offset_commit,
};
use crate::kafka::messages::{
    CreatePartitionsTopicRequest, CreateTopicRequest, JoinGroupProtocol, ListOffsetsPartitionData,
    ListOffsetsTopicData, OffsetCommitPartitionData, OffsetCommitTopicData,
};
use crate::kafka::storage::TopicMetadata;
use crate::kafka::{BrokerMetadata, GroupCoordinator, HandlerContext};
use crate::testing::mocks::MockKafkaStore;
use kafka_protocol::records::Compression;
use mockall::predicate::eq;

/// One IncrementalAlterConfigs resource: (type, name, [(key, op, value)]).
type AlterResource = (i8, String, Vec<(String, i8, Option<String>)>);

const RESOURCE_TYPE_TOPIC: i8 = 2;
const OP_SET: i8 = 0;
const OP_DELETE: i8 = 1;

fn boom() -> KafkaError {
    KafkaError::database("boom")
}

/// Run `f` with a handler context over `store` and a fresh coordinator.
fn with_ctx<R>(store: &MockKafkaStore, f: impl FnOnce(&HandlerContext) -> R) -> R {
    let coordinator = GroupCoordinator::new();
    with_ctx_coord(store, &coordinator, f)
}

fn with_ctx_coord<R>(
    store: &MockKafkaStore,
    coordinator: &GroupCoordinator,
    f: impl FnOnce(&HandlerContext) -> R,
) -> R {
    let broker = BrokerMetadata::new("localhost".to_string(), 9092);
    let ctx = HandlerContext::new(store, coordinator, &broker, 1, Compression::None);
    f(&ctx)
}

fn topic_meta(name: &str, id: i32, partitions: i32) -> TopicMetadata {
    TopicMetadata {
        name: name.to_string(),
        id,
        partition_count: partitions,
    }
}

/// Mock `get_topic_metadata`: "bad" fails, "ok" resolves to id 1 (4 partitions).
fn metadata_ok_or_fail(store: &mut MockKafkaStore) {
    store.expect_get_topic_metadata().returning(|names| {
        let name = names.and_then(|n| n.first()).cloned().unwrap_or_default();
        if name == "bad" {
            Err(boom())
        } else {
            Ok(vec![topic_meta(&name, 1, 4)])
        }
    });
}

// ========== OffsetCommit ==========

fn commit_topic(name: &str, partitions: &[i32]) -> OffsetCommitTopicData {
    OffsetCommitTopicData {
        name: name.to_string(),
        partitions: partitions
            .iter()
            .map(|&p| OffsetCommitPartitionData {
                partition_index: p,
                committed_offset: 10,
                metadata: None,
            })
            .collect(),
    }
}

#[test]
fn offset_commit_topic_lookup_failure_errors_every_partition_of_that_topic() {
    let mut store = MockKafkaStore::new();
    metadata_ok_or_fail(&mut store);
    store
        .expect_commit_offset()
        .times(1)
        .returning(|_, _, _, _, _| Ok(()));

    let resp = with_ctx(&store, |ctx| {
        handle_offset_commit(
            ctx,
            "g".to_string(),
            -1,
            "".to_string(),
            vec![commit_topic("bad", &[0, 1]), commit_topic("ok", &[0])],
        )
    })
    .expect("a storage failure must not become a handler Err");

    assert_eq!(resp.topics.len(), 2);
    let bad = &resp.topics[0];
    assert_eq!(bad.partitions.len(), 2);
    for p in &bad.partitions {
        assert_eq!(p.error_code, ERROR_UNKNOWN_SERVER_ERROR);
    }
    assert_eq!(resp.topics[1].partitions[0].error_code, ERROR_NONE);
}

#[test]
fn offset_commit_write_failure_is_per_partition() {
    let mut store = MockKafkaStore::new();
    metadata_ok_or_fail(&mut store);
    store
        .expect_commit_offset()
        .returning(|_, _, partition, _, _| if partition == 1 { Err(boom()) } else { Ok(()) });

    let resp = with_ctx(&store, |ctx| {
        handle_offset_commit(
            ctx,
            "g".to_string(),
            -1,
            "".to_string(),
            vec![commit_topic("ok", &[0, 1, 2])],
        )
    })
    .unwrap();

    let codes: Vec<i16> = resp.topics[0]
        .partitions
        .iter()
        .map(|p| p.error_code)
        .collect();
    assert_eq!(
        codes,
        vec![ERROR_NONE, ERROR_UNKNOWN_SERVER_ERROR, ERROR_NONE]
    );
}

// ========== ListOffsets ==========

fn list_topic(name: &str, partitions: &[(i32, i64)]) -> ListOffsetsTopicData {
    ListOffsetsTopicData {
        name: name.to_string(),
        partitions: partitions
            .iter()
            .map(|&(p, ts)| ListOffsetsPartitionData {
                partition_index: p,
                current_leader_epoch: -1,
                timestamp: ts,
            })
            .collect(),
    }
}

#[test]
fn list_offsets_storage_failures_are_reported_per_partition() {
    let mut store = MockKafkaStore::new();
    metadata_ok_or_fail(&mut store);
    store
        .expect_get_earliest_offset()
        .returning(|_, _| Err(boom()));
    store.expect_get_high_watermark().returning(|_, _| Ok(7));
    store
        .expect_get_offset_for_timestamp()
        .returning(|_, _, _| Err(boom()));

    let resp = with_ctx(&store, |ctx| {
        handle_list_offsets(
            ctx,
            vec![
                list_topic("bad", &[(0, -1), (1, -1)]),
                list_topic("ok", &[(0, -2), (1, -1), (2, 1_000)]),
            ],
            crate::kafka::storage::IsolationLevel::ReadUncommitted,
        )
    })
    .unwrap();

    for p in &resp.topics[0].partitions {
        assert_eq!(p.error_code, ERROR_UNKNOWN_SERVER_ERROR);
    }
    let ok = &resp.topics[1].partitions;
    assert_eq!(ok[0].error_code, ERROR_UNKNOWN_SERVER_ERROR); // earliest failed
    assert_eq!(ok[1].error_code, ERROR_NONE);
    assert_eq!(ok[1].offset, 7);
    assert_eq!(ok[2].error_code, ERROR_UNKNOWN_SERVER_ERROR); // timestamp failed
}

// ========== DescribeGroups ==========

#[test]
fn describe_groups_answers_every_requested_group() {
    let mut store = MockKafkaStore::new();
    store.expect_group_has_offsets().returning(|_| Ok(false));
    let resp = with_ctx(&store, |ctx| {
        handle_describe_groups(ctx, vec!["a".to_string(), "b".to_string(), "a".to_string()])
    })
    .unwrap();
    let ids: Vec<&str> = resp.groups.iter().map(|g| g.group_id.as_str()).collect();
    assert_eq!(ids, vec!["a", "b", "a"]);
}

#[test]
fn describe_groups_survives_a_poisoned_coordinator_lock() {
    let mut store = MockKafkaStore::new();
    store.expect_group_has_offsets().returning(|_| Ok(false));
    let coordinator = GroupCoordinator::new();
    let groups = coordinator.groups.clone();
    let _ = std::thread::spawn(move || {
        let _guard = groups.write().unwrap();
        panic!("poison the coordinator lock");
    })
    .join();
    assert!(coordinator.groups.is_poisoned());

    let resp = with_ctx_coord(&store, &coordinator, |ctx| {
        handle_describe_groups(ctx, vec!["g".to_string()])
    })
    .expect("a poisoned lock must not fail the request");
    assert_eq!(resp.groups.len(), 1);
    assert_eq!(resp.groups[0].error_code, ERROR_NONE);
}

// ========== CreateTopics ==========

fn create_req(name: &str) -> CreateTopicRequest {
    CreateTopicRequest {
        name: name.to_string(),
        num_partitions: 2,
        replication_factor: 1,
    }
}

#[test]
fn create_topics_storage_failures_are_per_topic() {
    let mut store = MockKafkaStore::new();
    store.expect_topic_exists().returning(|name| {
        if name == "exists-fails" {
            Err(boom())
        } else {
            Ok(false)
        }
    });
    store.expect_find_colliding_topic().returning(|_| Ok(None));
    store.expect_create_topic().returning(|name, _| {
        if name == "create-fails" {
            Err(boom())
        } else {
            Ok(9)
        }
    });

    let resp = with_ctx(&store, |ctx| {
        handle_create_topics(
            ctx,
            vec![
                create_req("exists-fails"),
                create_req("create-fails"),
                create_req("fine"),
            ],
            false,
        )
    })
    .unwrap();

    assert_eq!(resp.topics.len(), 3);
    assert_eq!(resp.topics[0].error_code, ERROR_UNKNOWN_SERVER_ERROR);
    assert!(resp.topics[0].error_message.is_some());
    assert_eq!(resp.topics[1].error_code, ERROR_UNKNOWN_SERVER_ERROR);
    assert!(resp.topics[1].error_message.is_some());
    assert_eq!(resp.topics[2].error_code, ERROR_NONE);
    assert_eq!(resp.topics[2].num_partitions, 2);
}

// ========== DeleteTopics ==========

#[test]
fn delete_topics_storage_failures_are_per_topic() {
    let mut store = MockKafkaStore::new();
    store.expect_get_topic_id().returning(|name| match name {
        "lookup-fails" => Err(boom()),
        "delete-fails" => Ok(Some(1)),
        _ => Ok(Some(2)),
    });
    store
        .expect_delete_topic()
        .returning(|id| if id == 1 { Err(boom()) } else { Ok(()) });

    let resp = with_ctx(&store, |ctx| {
        handle_delete_topics(
            ctx,
            vec![
                "lookup-fails".to_string(),
                "delete-fails".to_string(),
                "fine".to_string(),
            ],
        )
    })
    .unwrap();

    let codes: Vec<i16> = resp.responses.iter().map(|r| r.error_code).collect();
    assert_eq!(
        codes,
        vec![
            ERROR_UNKNOWN_SERVER_ERROR,
            ERROR_UNKNOWN_SERVER_ERROR,
            ERROR_NONE
        ]
    );
}

// ========== CreatePartitions ==========

#[test]
fn create_partitions_storage_failures_are_per_topic() {
    let mut store = MockKafkaStore::new();
    store.expect_get_topic_partition_count().returning(|name| {
        if name == "lookup-fails" {
            Err(boom())
        } else {
            Ok(Some(1))
        }
    });
    store
        .expect_set_topic_partition_count()
        .returning(|name, _| {
            if name == "set-fails" {
                Err(boom())
            } else {
                Ok(())
            }
        });

    let req = |name: &str| CreatePartitionsTopicRequest {
        name: name.to_string(),
        count: 3,
    };
    let resp = with_ctx(&store, |ctx| {
        handle_create_partitions(
            ctx,
            vec![req("lookup-fails"), req("set-fails"), req("fine")],
            false,
        )
    })
    .unwrap();

    let codes: Vec<i16> = resp.results.iter().map(|r| r.error_code).collect();
    assert_eq!(
        codes,
        vec![
            ERROR_UNKNOWN_SERVER_ERROR,
            ERROR_UNKNOWN_SERVER_ERROR,
            ERROR_NONE
        ]
    );
}

// ========== DeleteGroups ==========

/// Put an empty group named `group` into the coordinator (join, then leave).
fn make_empty_group(ctx: &HandlerContext, group: &str) {
    // A lone joiner completes at once, so the responder runs before this returns.
    let joined = std::sync::Arc::new(std::sync::Mutex::new(None));
    let slot = joined.clone();
    handle_join_group(
        ctx,
        crate::kafka::handlers::JoinGroupRequestData {
            group_id: group.to_string(),
            member_id: "".to_string(),
            client_id: "client".to_string(),
            group_instance_id: None,
            session_timeout_ms: 30_000,
            rebalance_timeout_ms: 60_000,
            protocol_type: "consumer".to_string(),
            protocols: vec![JoinGroupProtocol {
                name: "range".to_string(),
                metadata: vec![],
            }],
        },
        move |response| *slot.lock().unwrap() = Some(response),
    );
    let join = joined
        .lock()
        .unwrap()
        .take()
        .expect("lone join answered immediately");
    handle_leave_group(ctx, group.to_string(), join.member_id.to_string(), vec![]).unwrap();
    let state = ctx.coordinator.get_group_state(group).expect("group kept");
    assert!(state.members.is_empty());
}

#[test]
fn delete_groups_offset_delete_failure_is_per_group_and_keeps_the_group() {
    let mut store = MockKafkaStore::new();
    store.expect_delete_consumer_group_offsets().returning(|g| {
        if g == "fails" {
            Err(boom())
        } else {
            Ok(())
        }
    });
    let coordinator = GroupCoordinator::new();

    let resp = with_ctx_coord(&store, &coordinator, |ctx| {
        make_empty_group(ctx, "fails");
        make_empty_group(ctx, "fine");
        handle_delete_groups(ctx, vec!["fails".to_string(), "fine".to_string()])
    })
    .unwrap();

    assert_eq!(resp.results[0].error_code, ERROR_UNKNOWN_SERVER_ERROR);
    assert_eq!(resp.results[1].error_code, ERROR_NONE);
    // A failed delete must not half-delete the group: it stays in the coordinator
    // (its offsets are still stored), while the successful one is gone.
    assert!(coordinator.get_group_state("fails").is_some());
    assert!(coordinator.get_group_state("fine").is_none());
}

#[test]
fn delete_groups_unknown_group_storage_failure_is_reported() {
    let mut store = MockKafkaStore::new();
    store.expect_group_has_offsets().returning(|_| Ok(true));
    store
        .expect_delete_consumer_group_offsets()
        .returning(|_| Err(boom()));
    let resp = with_ctx(&store, |ctx| {
        handle_delete_groups(ctx, vec!["ghost".to_string()])
    })
    .unwrap();
    assert_eq!(resp.results[0].error_code, ERROR_UNKNOWN_SERVER_ERROR);
}

// ========== DescribeConfigs ==========

#[test]
fn describe_configs_topic_lookup_failure_is_per_resource() {
    let mut store = MockKafkaStore::new();
    store.expect_get_topic_id().returning(|name| {
        if name == "bad" {
            Err(boom())
        } else {
            Ok(Some(1))
        }
    });
    store
        .expect_get_topic_retention_ms()
        .returning(|_| Ok(None));

    let resp = with_ctx(&store, |ctx| {
        handle_describe_configs(
            ctx,
            vec![
                (RESOURCE_TYPE_TOPIC, "bad".to_string(), None),
                (RESOURCE_TYPE_TOPIC, "ok".to_string(), None),
            ],
            0,
        )
    })
    .expect("a storage failure must not become a handler Err");

    assert_eq!(resp.results.len(), 2);
    assert_eq!(resp.results[0].error_code, ERROR_UNKNOWN_SERVER_ERROR);
    assert!(resp.results[0].error_message.is_some());
    assert!(resp.results[0].configs.is_empty());
    assert_eq!(resp.results[1].error_code, ERROR_NONE);
    assert!(!resp.results[1].configs.is_empty());
}

#[test]
fn describe_configs_retention_lookup_failure_is_per_resource() {
    let mut store = MockKafkaStore::new();
    store
        .expect_get_topic_id()
        .returning(|name| Ok(Some(if name == "bad" { 1 } else { 2 })));
    store.expect_get_topic_retention_ms().returning(|id| {
        if id == 1 {
            Err(boom())
        } else {
            Ok(Some(5))
        }
    });

    let resp = with_ctx(&store, |ctx| {
        handle_describe_configs(
            ctx,
            vec![
                (RESOURCE_TYPE_TOPIC, "bad".to_string(), None),
                (RESOURCE_TYPE_TOPIC, "ok".to_string(), None),
            ],
            0,
        )
    })
    .unwrap();

    assert_eq!(resp.results[0].error_code, ERROR_UNKNOWN_SERVER_ERROR);
    assert!(resp.results[0].configs.is_empty());
    assert_eq!(resp.results[1].error_code, ERROR_NONE);
}

// ========== IncrementalAlterConfigs ==========

fn set_retention(name: &str, ms: &str) -> AlterResource {
    (
        RESOURCE_TYPE_TOPIC,
        name.to_string(),
        vec![("retention.ms".to_string(), OP_SET, Some(ms.to_string()))],
    )
}

#[test]
fn incremental_alter_configs_topic_lookup_failure_is_per_resource() {
    let mut store = MockKafkaStore::new();
    store.expect_get_topic_id().returning(|name| {
        if name == "bad" {
            Err(boom())
        } else {
            Ok(Some(4))
        }
    });
    store
        .expect_set_topic_retention_ms()
        .with(eq(4), eq(Some(1_000i64)))
        .times(1)
        .returning(|_, _| Ok(()));

    let resp = with_ctx(&store, |ctx| {
        handle_incremental_alter_configs(
            ctx,
            vec![set_retention("bad", "1000"), set_retention("ok", "1000")],
            false,
        )
    })
    .expect("a storage failure must not become a handler Err");

    assert_eq!(resp.responses.len(), 2);
    assert_eq!(resp.responses[0].error_code, ERROR_UNKNOWN_SERVER_ERROR);
    assert!(resp.responses[0].error_message.is_some());
    assert_eq!(resp.responses[1].error_code, ERROR_NONE);
}

#[test]
fn incremental_alter_configs_write_failure_does_not_stop_later_resources() {
    let mut store = MockKafkaStore::new();
    store
        .expect_get_topic_id()
        .returning(|name| Ok(Some(if name == "first" { 1 } else { 2 })));
    store
        .expect_set_topic_retention_ms()
        .with(eq(1), eq(Some(1_000i64)))
        .times(1)
        .returning(|_, _| Err(boom()));
    store
        .expect_set_topic_retention_ms()
        .with(eq(2), eq(Some(2_000i64)))
        .times(1)
        .returning(|_, _| Ok(()));

    let resp = with_ctx(&store, |ctx| {
        handle_incremental_alter_configs(
            ctx,
            vec![
                set_retention("first", "1000"),
                set_retention("second", "2000"),
            ],
            false,
        )
    })
    .unwrap();

    assert_eq!(resp.responses[0].error_code, ERROR_UNKNOWN_SERVER_ERROR);
    assert_eq!(resp.responses[1].error_code, ERROR_NONE);
}

/// Several alterations of one resource are applied as ONE write of the final
/// value, so a resource cannot end up half-applied.
#[test]
fn incremental_alter_configs_applies_a_resource_with_one_write() {
    let mut store = MockKafkaStore::new();
    store.expect_get_topic_id().returning(|_| Ok(Some(4)));
    store
        .expect_set_topic_retention_ms()
        .with(eq(4), eq(Some(3_000i64)))
        .times(1)
        .returning(|_, _| Ok(()));

    let resp = with_ctx(&store, |ctx| {
        handle_incremental_alter_configs(
            ctx,
            vec![(
                RESOURCE_TYPE_TOPIC,
                "t".to_string(),
                vec![
                    ("retention.ms".to_string(), OP_SET, Some("1000".to_string())),
                    ("retention.ms".to_string(), OP_DELETE, None),
                    ("retention.ms".to_string(), OP_SET, Some("3000".to_string())),
                ],
            )],
            false,
        )
    })
    .unwrap();
    assert_eq!(resp.responses[0].error_code, ERROR_NONE);
}

#[test]
fn incremental_alter_configs_last_delete_wins() {
    let mut store = MockKafkaStore::new();
    store.expect_get_topic_id().returning(|_| Ok(Some(4)));
    store
        .expect_set_topic_retention_ms()
        .with(eq(4), eq(None))
        .times(1)
        .returning(|_, _| Ok(()));

    let resp = with_ctx(&store, |ctx| {
        handle_incremental_alter_configs(
            ctx,
            vec![(
                RESOURCE_TYPE_TOPIC,
                "t".to_string(),
                vec![
                    ("retention.ms".to_string(), OP_SET, Some("1000".to_string())),
                    ("retention.ms".to_string(), OP_DELETE, None),
                ],
            )],
            false,
        )
    })
    .unwrap();
    assert_eq!(resp.responses[0].error_code, ERROR_NONE);
}

/// A resource with no alterations writes nothing and succeeds.
#[test]
fn incremental_alter_configs_empty_resource_writes_nothing() {
    let mut store = MockKafkaStore::new();
    store.expect_get_topic_id().returning(|_| Ok(Some(4)));
    store.expect_set_topic_retention_ms().times(0);

    let resp = with_ctx(&store, |ctx| {
        handle_incremental_alter_configs(
            ctx,
            vec![(RESOURCE_TYPE_TOPIC, "t".to_string(), vec![])],
            false,
        )
    })
    .unwrap();
    assert_eq!(resp.responses[0].error_code, ERROR_NONE);
}

// ========== DeleteRecords ==========

#[test]
fn delete_records_topic_lookup_failure_errors_every_partition_of_that_topic() {
    for fail_count in [false, true] {
        let mut store = MockKafkaStore::new();
        store.expect_get_topic_id().returning(move |name| {
            if name == "bad" && !fail_count {
                Err(boom())
            } else {
                Ok(Some(if name == "bad" { 1 } else { 2 }))
            }
        });
        store
            .expect_get_topic_partition_count()
            .returning(move |name| {
                if name == "bad" && fail_count {
                    Err(boom())
                } else {
                    Ok(Some(2))
                }
            });
        store.expect_get_high_watermark().returning(|_, _| Ok(10));
        store
            .expect_delete_records_before()
            .with(eq(2), eq(0), eq(5i64))
            .times(1)
            .returning(|_, _, before| Ok(before));

        let resp = with_ctx(&store, |ctx| {
            handle_delete_records(
                ctx,
                vec![
                    ("bad".to_string(), vec![(0, 5), (1, 5)]),
                    ("ok".to_string(), vec![(0, 5)]),
                ],
            )
        })
        .expect("a storage failure must not become a handler Err");

        let bad = &resp.topics[0].partitions;
        assert_eq!(bad.len(), 2, "fail_count={}", fail_count);
        for p in bad {
            assert_eq!(p.error_code, ERROR_UNKNOWN_SERVER_ERROR);
            assert_eq!(p.low_watermark, -1);
        }
        let ok = &resp.topics[1].partitions[0];
        assert_eq!(ok.error_code, ERROR_NONE);
        assert_eq!(ok.low_watermark, 5);
    }
}

#[test]
fn delete_records_partition_failures_do_not_stop_later_partitions() {
    let mut store = MockKafkaStore::new();
    store.expect_get_topic_id().returning(|_| Ok(Some(3)));
    store
        .expect_get_topic_partition_count()
        .returning(|_| Ok(Some(3)));
    store.expect_get_high_watermark().returning(
        |_, partition| {
            if partition == 0 {
                Err(boom())
            } else {
                Ok(10)
            }
        },
    );
    store
        .expect_delete_records_before()
        .returning(|_, partition, before| {
            if partition == 1 {
                Err(boom())
            } else {
                Ok(before)
            }
        });

    let resp = with_ctx(&store, |ctx| {
        handle_delete_records(ctx, vec![("t".to_string(), vec![(0, 4), (1, 4), (2, 4)])])
    })
    .unwrap();

    let parts = &resp.topics[0].partitions;
    assert_eq!(parts[0].error_code, ERROR_UNKNOWN_SERVER_ERROR); // HWM read failed
    assert_eq!(parts[0].low_watermark, -1);
    assert_eq!(parts[1].error_code, ERROR_UNKNOWN_SERVER_ERROR); // delete failed
    assert_eq!(parts[1].low_watermark, -1);
    assert_eq!(parts[2].error_code, ERROR_NONE);
    assert_eq!(parts[2].low_watermark, 4);
}
