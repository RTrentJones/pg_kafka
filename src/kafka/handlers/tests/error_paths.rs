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
use crate::kafka::{BrokerMetadata, GroupCoordinator, HandlerContext};
use crate::testing::expect_topics;
use crate::testing::mocks::MockKafkaStore;
use kafka_protocol::records::Compression;
use mockall::predicate::eq;

/// One IncrementalAlterConfigs resource: (type, name, [(key, op, value)]).
type AlterResource = (i8, String, Vec<(String, i8, Option<String>)>);

const RESOURCE_TYPE_TOPIC: i8 = 2;
const OP_SET: i8 = 0;

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

/// Mock the batched topic lookup: any request naming "bad" fails; every other topic has
/// id 1 and 4 partitions.
fn metadata_ok_or_fail(store: &mut MockKafkaStore) {
    expect_topics(store, |name| {
        if name == "bad" {
            Err(boom())
        } else {
            Ok(Some((1, 4)))
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
fn offset_commit_topic_lookup_failure_errors_every_partition() {
    // The topics are looked up once for the request: a failed lookup errors every
    // partition, commits nothing, and is no handler Err.
    let mut store = MockKafkaStore::new();
    metadata_ok_or_fail(&mut store);
    store.expect_commit_offset().times(0);

    let resp = with_ctx(&store, |ctx| {
        handle_offset_commit(
            ctx,
            "g",
            -1,
            "",
            vec![commit_topic("bad", &[0, 1]), commit_topic("ok", &[0])],
        )
    })
    .expect("a storage failure must not become a handler Err");

    assert_eq!(resp.topics.len(), 2);
    let partitions: Vec<_> = resp.topics.iter().flat_map(|t| &t.partitions).collect();
    assert_eq!(partitions.len(), 3);
    for p in partitions {
        assert_eq!(p.error_code, ERROR_UNKNOWN_SERVER_ERROR);
    }
}

#[test]
fn offset_commit_write_failure_is_per_partition() {
    let mut store = MockKafkaStore::new();
    metadata_ok_or_fail(&mut store);
    store
        .expect_commit_offset()
        .returning(|_, _, partition, _, _| if partition == 1 { Err(boom()) } else { Ok(()) });

    let resp = with_ctx(&store, |ctx| {
        handle_offset_commit(ctx, "g", -1, "", vec![commit_topic("ok", &[0, 1, 2])])
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
    // A failed topic lookup (one for the request) errors every partition.
    let mut store = MockKafkaStore::new();
    metadata_ok_or_fail(&mut store);
    let resp = with_ctx(&store, |ctx| {
        handle_list_offsets(
            ctx,
            vec![
                list_topic("bad", &[(0, -1), (1, -1)]),
                list_topic("ok", &[(0, -1)]),
            ],
            crate::kafka::storage::IsolationLevel::ReadUncommitted,
        )
    })
    .unwrap();
    for p in resp.topics.iter().flat_map(|t| &t.partitions) {
        assert_eq!(p.error_code, ERROR_UNKNOWN_SERVER_ERROR);
    }

    // A failed read errors only its partition.
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
            vec![list_topic("ok", &[(0, -2), (1, -1), (2, 1_000)])],
            crate::kafka::storage::IsolationLevel::ReadUncommitted,
        )
    })
    .unwrap();

    let ok = &resp.topics[0].partitions;
    assert_eq!(ok[0].error_code, ERROR_UNKNOWN_SERVER_ERROR); // earliest failed
    assert_eq!(ok[1].error_code, ERROR_NONE);
    assert_eq!(ok[1].offset, 7);
    assert_eq!(ok[2].error_code, ERROR_UNKNOWN_SERVER_ERROR); // timestamp failed
}

/// Kafka 3.7 answers every entry of a partition a ListOffsets request names more than
/// once INVALID_REQUEST and reads nothing for it; other partitions are answered.
#[test]
fn list_offsets_repeated_partition_is_invalid_request() {
    let mut store = MockKafkaStore::new();
    metadata_ok_or_fail(&mut store);
    store
        .expect_get_high_watermark()
        .times(1)
        .returning(|_, _| Ok(7));
    let mut repeated = vec![(0, -1); 20_000];
    repeated.push((1, -1));
    let resp = with_ctx(&store, |ctx| {
        handle_list_offsets(
            ctx,
            vec![list_topic("ok", &repeated), list_topic("ok", &[(0, -1)])],
            crate::kafka::storage::IsolationLevel::ReadUncommitted,
        )
    })
    .unwrap();
    assert_eq!(resp.topics.len(), 2);
    let codes: Vec<(i32, i16)> = resp
        .topics
        .iter()
        .flat_map(|t| {
            t.partitions
                .iter()
                .map(|p| (p.partition_index, p.error_code))
        })
        .collect();
    assert_eq!(codes.len(), 20_002);
    assert!(codes.iter().all(|&(p, code)| code
        == if p == 0 {
            ERROR_INVALID_REQUEST
        } else {
            ERROR_NONE
        }));
}

/// Repeated partitions of a long topic name cost nothing per entry: the 2,500 repeated
/// partitions of a 32 KB name each held a copy of the name (80 MB), so a 1 MB request
/// could make the DB thread allocate a gigabyte.
#[test]
fn list_offsets_repeated_partitions_do_not_copy_the_name() {
    let name = "n".repeat(32 * 1024);
    let mut store = MockKafkaStore::new();
    expect_topics(&mut store, |_| Ok(Some((1, 1 << 20))));
    let partitions: Vec<(i32, i64)> = (0..5_000).map(|i| (i / 2, -1)).collect();
    let topics = vec![list_topic(&name, &partitions)];
    let (resp, peak) = crate::testing::heap_peak(|| {
        with_ctx(&store, |ctx| {
            handle_list_offsets(
                ctx,
                topics,
                crate::kafka::storage::IsolationLevel::ReadUncommitted,
            )
        })
    });
    let resp = resp.unwrap();
    assert!(resp.topics[0]
        .partitions
        .iter()
        .all(|p| p.error_code == ERROR_INVALID_REQUEST));
    assert!(peak < 4 << 20, "held {peak} bytes besides the request");
}

/// Finding a ListOffsets request's repeated partitions costs nothing per partition entry
/// for the topic's name: keyed by the full name, each of a 32 KB name's 60,000 partition
/// entries (a 1 MB request) hashed the name twice, about 1.8 s of DB-thread CPU. Four
/// times the name length at the same entries must cost about the same.
#[test]
fn list_offsets_repeated_partitions_cost_nothing_per_entry_for_the_name() {
    let mut store = MockKafkaStore::new();
    // No partition of the topic exists, so no entry reads the store.
    expect_topics(&mut store, |_| Ok(Some((1, 0))));
    let partitions: Vec<(i32, i64)> = (0..10_000).map(|i| (i, -1)).collect();
    let growth = crate::testing::flat_growth(8 * 1024, |len| {
        let topics = vec![list_topic(&"n".repeat(len), &partitions)];
        let resp = with_ctx(&store, |ctx| {
            handle_list_offsets(
                ctx,
                topics,
                crate::kafka::storage::IsolationLevel::ReadUncommitted,
            )
        })
        .unwrap();
        assert_eq!(resp.topics[0].partitions.len(), partitions.len());
    });
    assert!(
        growth < 2.0,
        "4x the name length took {growth:.1}x the time"
    );
}

// ========== DescribeGroups ==========

#[test]
fn describe_groups_answers_every_requested_group() {
    let mut store = MockKafkaStore::new();
    store
        .expect_groups_with_offsets()
        .returning(|_| Ok(std::collections::HashSet::new()));
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
    store
        .expect_groups_with_offsets()
        .returning(|_| Ok(std::collections::HashSet::new()));
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
fn create_topics_storage_failures_are_reported_on_the_topics_they_affect() {
    // The existing topics are looked up once for the request: a failed lookup refuses
    // every topic it would have checked; a failed create refuses only its topic.
    let mut store = MockKafkaStore::new();
    store.expect_colliding_topics().returning(|_| Err(boom()));
    store.expect_create_topic().times(0);
    let resp = with_ctx(&store, |ctx| {
        handle_create_topics(ctx, vec![create_req("a"), create_req("b")], false)
    })
    .unwrap();
    assert_eq!(resp.topics.len(), 2);
    for t in &resp.topics {
        assert_eq!(t.error_code, ERROR_UNKNOWN_SERVER_ERROR);
        assert!(t.error_message.is_some());
    }

    let mut store = MockKafkaStore::new();
    store.expect_colliding_topics().returning(|_| Ok(vec![]));
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
            vec![create_req("create-fails"), create_req("fine")],
            false,
        )
    })
    .unwrap();
    assert_eq!(resp.topics.len(), 2);
    assert_eq!(resp.topics[0].error_code, ERROR_UNKNOWN_SERVER_ERROR);
    assert!(resp.topics[0].error_message.is_some());
    assert_eq!(resp.topics[1].error_code, ERROR_NONE);
    assert_eq!(resp.topics[1].num_partitions, 2);
}

// ========== DeleteTopics ==========

#[test]
fn delete_topics_storage_failures_are_reported_on_the_topics_they_affect() {
    // The topics are looked up once for the request: a failed lookup errors every topic.
    let mut store = MockKafkaStore::new();
    expect_topics(&mut store, |_| Err(boom()));
    store.expect_delete_topic().times(0);
    let resp = with_ctx(&store, |ctx| {
        handle_delete_topics(ctx, vec!["a".to_string(), "b".to_string()])
    })
    .unwrap();
    let codes: Vec<i16> = resp.responses.iter().map(|r| r.error_code).collect();
    assert_eq!(codes, vec![ERROR_UNKNOWN_SERVER_ERROR; 2]);

    // A failed delete errors only its topic.
    let mut store = MockKafkaStore::new();
    expect_topics(&mut store, |name| {
        Ok(Some(if name == "delete-fails" {
            (1, 1)
        } else {
            (2, 1)
        }))
    });
    store
        .expect_delete_topic()
        .returning(|id| if id == 1 { Err(boom()) } else { Ok(()) });
    let resp = with_ctx(&store, |ctx| {
        handle_delete_topics(ctx, vec!["delete-fails".to_string(), "fine".to_string()])
    })
    .unwrap();
    let codes: Vec<i16> = resp.responses.iter().map(|r| r.error_code).collect();
    assert_eq!(codes, vec![ERROR_UNKNOWN_SERVER_ERROR, ERROR_NONE]);
}

// ========== CreatePartitions ==========

#[test]
fn create_partitions_storage_failures_are_reported_on_the_topics_they_affect() {
    let req = |name: &str| CreatePartitionsTopicRequest {
        name: name.to_string(),
        count: 3,
    };
    // The topics are looked up once for the request: a failed lookup errors every topic.
    let mut store = MockKafkaStore::new();
    expect_topics(&mut store, |_| Err(boom()));
    store.expect_set_topic_partition_count().times(0);
    let resp = with_ctx(&store, |ctx| {
        handle_create_partitions(ctx, vec![req("a"), req("b")], false)
    })
    .unwrap();
    let codes: Vec<i16> = resp.results.iter().map(|r| r.error_code).collect();
    assert_eq!(codes, vec![ERROR_UNKNOWN_SERVER_ERROR; 2]);

    // A failed change errors only its topic.
    let mut store = MockKafkaStore::new();
    expect_topics(&mut store, |_| Ok(Some((1, 1))));
    store
        .expect_set_topic_partition_count()
        .returning(|name, _| {
            if name == "set-fails" {
                Err(boom())
            } else {
                Ok(())
            }
        });
    let resp = with_ctx(&store, |ctx| {
        handle_create_partitions(ctx, vec![req("set-fails"), req("fine")], false)
    })
    .unwrap();
    let codes: Vec<i16> = resp.results.iter().map(|r| r.error_code).collect();
    assert_eq!(codes, vec![ERROR_UNKNOWN_SERVER_ERROR, ERROR_NONE]);
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
            member_id: String::new(),
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
    handle_leave_group(
        ctx,
        group,
        crate::kafka::messages::LeavingMembers::Single(join.member_id.to_string()),
    )
    .unwrap();
    let state = ctx.coordinator.get_group_state(group).expect("group kept");
    assert!(state.members.is_empty());
}

/// Join a lone member to `group` with `metadata_len` bytes of protocol metadata; its
/// member id and generation.
fn join_lone_member(ctx: &HandlerContext, group: &str, metadata_len: usize) -> (String, i32) {
    let joined = std::sync::Arc::new(std::sync::Mutex::new(None));
    let slot = joined.clone();
    handle_join_group(
        ctx,
        crate::kafka::handlers::JoinGroupRequestData {
            group_id: group.to_string(),
            member_id: String::new(),
            client_id: "client".to_string(),
            group_instance_id: None,
            session_timeout_ms: 30_000,
            rebalance_timeout_ms: 60_000,
            protocol_type: "consumer".to_string(),
            protocols: vec![JoinGroupProtocol {
                name: "range".to_string(),
                metadata: vec![0; metadata_len],
            }],
        },
        move |response| *slot.lock().unwrap() = Some(response),
    );
    let join = joined
        .lock()
        .unwrap()
        .take()
        .expect("lone join answered immediately");
    assert_eq!(join.error_code, ERROR_NONE);
    (join.member_id.to_string(), join.generation_id)
}

/// A refused OffsetCommit and a DeleteGroups of a live group ask the coordinator whether
/// it holds the group (and whether it has members) without copying it: a copy of a group
/// of 1 MiB member metadata per refused commit is a megabyte copied on the DB thread.
#[test]
fn group_existence_checks_do_not_copy_the_group() {
    let mut store = MockKafkaStore::new();
    metadata_ok_or_fail(&mut store);
    store.expect_commit_offset().never();
    store.expect_groups_with_offsets().never();
    store.expect_delete_consumer_group_offsets().never();
    let coordinator = GroupCoordinator::new();
    with_ctx_coord(&store, &coordinator, |ctx| {
        let (member, generation) = join_lone_member(
            ctx,
            "big",
            crate::kafka::limits::MAX_MEMBER_PROTOCOL_BYTES - 64,
        );
        let topics = vec![commit_topic("ok", &[0])];
        let (resp, copied) = crate::testing::heap_allocated(|| {
            handle_offset_commit(ctx, "big", generation + 5, &member, topics).unwrap()
        });
        assert_eq!(
            resp.topics[0].partitions[0].error_code,
            ERROR_ILLEGAL_GENERATION
        );
        assert!(copied < 64 << 10, "a refused commit copied {copied} bytes");

        let (resp, copied) = crate::testing::heap_allocated(|| {
            handle_delete_groups(ctx, vec!["big".to_string()]).unwrap()
        });
        assert_eq!(resp.results[0].error_code, ERROR_NON_EMPTY_GROUP);
        assert!(copied < 64 << 10, "DeleteGroups copied {copied} bytes");
    });
    assert!(coordinator.has_group("big"));
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
    store
        .expect_groups_with_offsets()
        .returning(|groups| Ok(groups.iter().cloned().collect()));
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
fn describe_configs_topic_lookup_failure_errors_every_topic_resource() {
    // The topics are looked up once for the request: a failed lookup errors every topic
    // resource, with an empty config list, and no handler Err.
    let mut store = MockKafkaStore::new();
    expect_topics(&mut store, |_| Err(boom()));

    let resp = with_ctx(&store, |ctx| {
        handle_describe_configs(
            ctx,
            vec![
                (RESOURCE_TYPE_TOPIC, "a".to_string(), None),
                (RESOURCE_TYPE_TOPIC, "b".to_string(), None),
            ],
            0,
        )
    })
    .expect("a storage failure must not become a handler Err");

    assert_eq!(resp.results.len(), 2);
    for r in &resp.results {
        assert_eq!(r.error_code, ERROR_UNKNOWN_SERVER_ERROR);
        assert!(r.error_message.is_some());
        assert_eq!(
            r.configs,
            [] as [kafka_protocol::messages::describe_configs_response::DescribeConfigsResourceResult;
                0]
        );
    }
}

#[test]
fn describe_configs_retention_lookup_failure_is_per_resource() {
    let mut store = MockKafkaStore::new();
    expect_topics(&mut store, |name| {
        Ok(Some(if name == "bad" { (1, 1) } else { (2, 1) }))
    });
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
    assert_eq!(
        resp.results[0].configs,
        [] as [kafka_protocol::messages::describe_configs_response::DescribeConfigsResourceResult;
            0]
    );
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
fn incremental_alter_configs_topic_lookup_failure_errors_every_topic_resource() {
    // The topics are looked up once for the request: a failed lookup errors every topic
    // resource, alters nothing, and is no handler Err.
    let mut store = MockKafkaStore::new();
    expect_topics(&mut store, |_| Err(boom()));
    store.expect_set_topic_retention_ms().times(0);

    let resp = with_ctx(&store, |ctx| {
        handle_incremental_alter_configs(
            ctx,
            vec![set_retention("a", "1000"), set_retention("b", "1000")],
            false,
        )
    })
    .expect("a storage failure must not become a handler Err");

    assert_eq!(resp.responses.len(), 2);
    for r in &resp.responses {
        assert_eq!(r.error_code, ERROR_UNKNOWN_SERVER_ERROR);
        assert!(r.error_message.is_some());
    }
}

#[test]
fn incremental_alter_configs_write_failure_does_not_stop_later_resources() {
    let mut store = MockKafkaStore::new();
    expect_topics(&mut store, |name| {
        Ok(Some(if name == "first" { (1, 1) } else { (2, 1) }))
    });
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

/// A resource with no alterations writes nothing and succeeds.
#[test]
fn incremental_alter_configs_empty_resource_writes_nothing() {
    let mut store = MockKafkaStore::new();
    expect_topics(&mut store, |_| Ok(Some((4, 1))));
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
fn delete_records_topic_lookup_failure_errors_every_partition() {
    // The topics are looked up once for the request: a failed lookup errors every
    // partition, deletes nothing, and is no handler Err.
    let mut store = MockKafkaStore::new();
    expect_topics(&mut store, |_| Err(boom()));
    store.expect_delete_records_before().times(0);

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

    let partitions: Vec<_> = resp.topics.iter().flat_map(|t| &t.partitions).collect();
    assert_eq!(partitions.len(), 3);
    for p in partitions {
        assert_eq!(p.error_code, ERROR_UNKNOWN_SERVER_ERROR);
        assert_eq!(p.low_watermark, -1);
    }
}

#[test]
fn delete_records_partition_failures_do_not_stop_later_partitions() {
    let mut store = MockKafkaStore::new();
    expect_topics(&mut store, |_| Ok(Some((3, 3))));
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
