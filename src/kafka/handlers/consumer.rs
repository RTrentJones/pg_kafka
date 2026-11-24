//! OffsetCommit and OffsetFetch: a consumer group's committed offsets.

use super::helpers::{merge_by_partition, resolve_topics};
use crate::kafka::constants::{
    offset_metadata_too_large, ERROR_NONE, ERROR_OFFSET_METADATA_TOO_LARGE,
    ERROR_UNKNOWN_MEMBER_ID, ERROR_UNKNOWN_TOPIC_OR_PARTITION, ERROR_UNSTABLE_OFFSET_COMMIT,
};
use crate::kafka::error::Result;
use crate::kafka::handler_context::HandlerContext;
use crate::kafka::messages::{
    OffsetCommitPartitionData, OffsetCommitTopicData, OffsetFetchTopicData,
};
use crate::kafka::storage::{CommittedOffset, KafkaStore};
use kafka_protocol::messages::offset_commit_response::{
    OffsetCommitResponse, OffsetCommitResponsePartition, OffsetCommitResponseTopic,
};
use kafka_protocol::messages::offset_fetch_response::{
    OffsetFetchResponse, OffsetFetchResponsePartition, OffsetFetchResponseTopic,
};
use kafka_protocol::messages::TopicName;
use kafka_protocol::protocol::StrBytes;
use std::collections::{HashMap, HashSet};

/// Handle OffsetCommit request
///
/// Commits consumer offsets for a consumer group
pub fn handle_offset_commit(
    ctx: &HandlerContext,
    group_id: &str,
    generation_id: i32,
    member_id: &str,
    topics: Vec<OffsetCommitTopicData>,
) -> Result<OffsetCommitResponse> {
    let mut response = OffsetCommitResponse::default();
    response.throttle_time_ms = 0;
    let topics = merge_commit_topics(topics);

    // Kafka 3.7 answers a partition that does not exist UNKNOWN_TOPIC_OR_PARTITION before
    // anything else (KafkaApis filters those out before it asks the coordinator), so a
    // refused commit answers its code only on the partitions that exist.
    let resolved = resolve_topics(ctx.store, topics.iter().map(|t| t.name.as_str()));

    // Reject zombie commits. A member committing with a stale generation (or an unknown
    // member id) for a live group must be rejected with ILLEGAL_GENERATION / UNKNOWN_MEMBER_ID
    // rather than silently persisting an offset. Kafka 3.7's rules, see
    // `GroupCoordinator::validate_offset_commit` (generation -1 only for an unknown/Empty group).
    let refusal = match &resolved {
        Ok(_) => offset_commit_refusal(ctx, group_id, member_id, generation_id)?,
        Err(_) => None,
    };
    response.topics = topics
        .into_iter()
        .map(|topic| {
            let found = match &resolved {
                Ok(found) => Ok(found.get(&topic.name).copied()),
                Err(code) => Err(*code),
            };
            match refusal {
                Some(error_code) => refuse_topic(topic, found, error_code),
                None => commit_topic_offsets(ctx.store, group_id, topic, found),
            }
        })
        .collect();
    Ok(response)
}

/// Answer a refused commit for one topic: `error_code` on each partition that exists,
/// UNKNOWN_TOPIC_OR_PARTITION on the rest.
fn refuse_topic(
    topic: OffsetCommitTopicData,
    found: std::result::Result<Option<(i32, i32)>, i16>,
    error_code: i16,
) -> OffsetCommitResponseTopic {
    let partitions = topic
        .partitions
        .iter()
        .map(|p| {
            let exists =
                matches!(found, Ok(Some((_, count))) if (0..count).contains(&p.partition_index));
            let code = if exists {
                error_code
            } else {
                ERROR_UNKNOWN_TOPIC_OR_PARTITION
            };
            commit_partition(p.partition_index, code)
        })
        .collect();
    commit_topic(topic.name, partitions)
}

/// Why an OffsetCommit is refused, as an error code (`None` when it may commit).
///
/// Kafka 3.7 keeps a group that has committed offsets as an Empty group even when no member
/// joined since (it loads them from `__consumer_offsets`), so a generation >= 0 for such a
/// group names a member it does not have: UNKNOWN_MEMBER_ID. Only a group with neither
/// members nor offsets answers a generation >= 0 ILLEGAL_GENERATION. The coordinator does
/// not hold offsets-only groups, so this asks the store, and only on that path.
fn offset_commit_refusal(
    ctx: &HandlerContext,
    group_id: &str,
    member_id: &str,
    generation_id: i32,
) -> Result<Option<i16>> {
    let Err(e) = ctx
        .coordinator
        .validate_offset_commit(group_id, member_id, generation_id)
    else {
        return Ok(None);
    };
    if generation_id >= 0
        && !ctx.coordinator.has_group(group_id)
        && ctx
            .store
            .groups_with_offsets(&[group_id.to_string()])?
            .contains(group_id)
    {
        return Ok(Some(ERROR_UNKNOWN_MEMBER_ID));
    }
    Ok(Some(e.to_kafka_error_code()))
}

/// The request's topics with each repeated partition merged into one entry holding its
/// last offset: Kafka 3.7 collects a commit's partitions into a map, stores each once and
/// answers each once.
fn merge_commit_topics(topics: Vec<OffsetCommitTopicData>) -> Vec<OffsetCommitTopicData> {
    merge_by_partition(topics.into_iter().map(|t| (t.name, t.partitions)), |p| {
        p.partition_index
    })
    .into_iter()
    .map(|(name, partitions)| OffsetCommitTopicData { name, partitions })
    .collect()
}

/// Commit one topic's offsets and answer for each of its partitions.
fn commit_topic_offsets(
    store: &dyn KafkaStore,
    group_id: &str,
    topic: OffsetCommitTopicData,
    found: std::result::Result<Option<(i32, i32)>, i16>,
) -> OffsetCommitResponseTopic {
    let partitions = match found {
        Ok(Some((topic_id, partition_count))) => topic
            .partitions
            .iter()
            .map(|partition| {
                let partition_id = partition.partition_index;
                // Kafka refuses a commit for a partition the topic does not have.
                let error_code = if partition_id < 0 || partition_id >= partition_count {
                    ERROR_UNKNOWN_TOPIC_OR_PARTITION
                } else if offset_metadata_too_large(partition.metadata.as_deref()) {
                    // Kafka 3.7 (offset.metadata.max.bytes = 4096) stores the other
                    // partitions and answers this one.
                    ERROR_OFFSET_METADATA_TOO_LARGE
                } else {
                    commit_partition_offset(store, group_id, topic_id, partition)
                };
                commit_partition(partition_id, error_code)
            })
            .collect(),
        // A missing topic (or a failed lookup) is reported on every partition.
        unresolved => {
            let error_code = unresolved.err().unwrap_or(ERROR_UNKNOWN_TOPIC_OR_PARTITION);
            topic
                .partitions
                .iter()
                .map(|p| commit_partition(p.partition_index, error_code))
                .collect()
        }
    };
    commit_topic(topic.name, partitions)
}

/// Store one partition's committed offset: the partition's error code.
fn commit_partition_offset(
    store: &dyn KafkaStore,
    group_id: &str,
    topic_id: i32,
    partition: &OffsetCommitPartitionData,
) -> i16 {
    let partition_id = partition.partition_index;
    match store.commit_offset(
        group_id,
        topic_id,
        partition_id,
        partition.committed_offset,
        partition.metadata.as_deref(),
    ) {
        Ok(()) => ERROR_NONE,
        Err(e) => {
            if e.is_server_error() {
                crate::pg_warning!(
                    "Failed to commit offset for group='{}', topic_id={}, partition={}: {}",
                    group_id,
                    topic_id,
                    partition_id,
                    e
                );
            }
            e.to_kafka_error_code()
        }
    }
}

/// An OffsetCommit topic entry.
fn commit_topic(
    name: String,
    partitions: Vec<OffsetCommitResponsePartition>,
) -> OffsetCommitResponseTopic {
    let mut topic = OffsetCommitResponseTopic::default();
    topic.name = TopicName(StrBytes::from_string(name));
    topic.partitions = partitions;
    topic
}

/// An OffsetCommit partition entry.
fn commit_partition(partition_index: i32, error_code: i16) -> OffsetCommitResponsePartition {
    let mut partition = OffsetCommitResponsePartition::default();
    partition.partition_index = partition_index;
    partition.error_code = error_code;
    partition
}

/// The partitions of each topic, by name, with a pending transactional offset commit.
type Unstable = HashMap<String, HashSet<i32>>;

/// Handle OffsetFetch request
///
/// Fetches committed offsets for a consumer group
///
/// `require_stable` (v7+, KIP-447): a partition with a transactional offset
/// commit whose transaction is still open is answered UNSTABLE_OFFSET_COMMIT with
/// offset -1 (partition level, as Kafka 3.7), so a read_committed consumer retries
/// instead of starting from an offset the transaction may still replace.
pub fn handle_offset_fetch(
    ctx: &HandlerContext,
    group_id: &str,
    topics: Option<Vec<OffsetFetchTopicData>>,
    require_stable: bool,
) -> Result<OffsetFetchResponse> {
    let store = ctx.store;
    let mut kafka_response = OffsetFetchResponse::default();
    kafka_response.throttle_time_ms = 0;
    kafka_response.error_code = ERROR_NONE;
    if let Some(topics) = topics {
        kafka_response.topics = fetch_requested_offsets(store, group_id, topics, require_stable)?;
    } else {
        let unstable = if require_stable {
            unstable_partitions(store, group_id, None)?
        } else {
            Unstable::new()
        };
        // A fetch-all storage failure is a top-level error: a success with no offsets
        // reads to a consumer as "no committed offsets", and it may reset.
        match fetch_all_group_offsets(store, group_id, &unstable) {
            Ok(topics) => kafka_response.topics = topics,
            Err(error_code) => kafka_response.error_code = error_code,
        }
    }
    Ok(kafka_response)
}

/// The partitions of `group_id` with a pending transactional commit (require_stable):
/// among `requested` (`(topic_id, partition)`) only when given, so another transaction's
/// many pending partitions of the group are not read for a request that names a few.
fn unstable_partitions(
    store: &dyn KafkaStore,
    group_id: &str,
    requested: Option<&[(i32, i32)]>,
) -> Result<Unstable> {
    let mut unstable = Unstable::new();
    for (topic, partition) in store.pending_txn_offset_partitions(group_id, requested)? {
        unstable.entry(topic).or_default().insert(partition);
    }
    Ok(unstable)
}

/// The committed offsets of the requested partitions: each distinct topic once, with each
/// distinct partition once, in the order first named (Kafka 3.7 answers a repeated
/// partition once). The topics are looked up together and the requested partitions'
/// offsets read in one statement, whatever the number of entries or of the group's
/// offsets; a failed lookup or read is reported on every partition it affects.
fn fetch_requested_offsets(
    store: &dyn KafkaStore,
    group_id: &str,
    topics: Vec<OffsetFetchTopicData>,
    require_stable: bool,
) -> Result<Vec<OffsetFetchResponseTopic>> {
    let topics = merge_by_partition(
        topics.into_iter().map(|t| (t.name, t.partition_indexes)),
        |&p| p,
    );
    let resolved = resolve_topics(store, topics.iter().map(|(name, _)| name.as_str()));
    // Only the requested partitions of the topics that exist are read: the group may have
    // committed offsets for many more.
    let requested: Vec<(i32, i32)> = match &resolved {
        Ok(found) => topics
            .iter()
            .filter_map(|(name, partitions)| Some((found.get(name)?.0, partitions)))
            .flat_map(|(topic_id, partitions)| partitions.iter().map(move |&p| (topic_id, p)))
            .collect(),
        Err(_) => Vec::new(),
    };
    let unstable = if require_stable {
        unstable_partitions(store, group_id, Some(&requested))?
    } else {
        Unstable::new()
    };
    let mut committed: std::result::Result<HashMap<(i32, i32), CommittedOffset>, i16> =
        if requested.is_empty() {
            Ok(HashMap::new())
        } else {
            store
                .fetch_offsets(group_id, &requested)
                .map(|offsets| {
                    offsets
                        .into_iter()
                        .map(|(topic_id, partition, offset)| ((topic_id, partition), offset))
                        .collect()
                })
                .map_err(|e| {
                    crate::pg_warning!("Failed to fetch offsets for group='{}': {}", group_id, e);
                    e.to_kafka_error_code()
                })
        };
    drop(requested);
    Ok(topics
        .into_iter()
        .map(|(name, partition_indexes)| {
            // A topic that does not exist has no committed offsets: Kafka 3.7 answers its
            // partitions offset -1 with NONE, as for any partition without a commit.
            // (UNKNOWN_TOPIC_OR_PARTITION is fatal to the Java consumer: poll() throws for a
            // consumer that assign()s a topic before it exists or after it was deleted.)
            let (topic_id, topic_error) = match &resolved {
                Ok(found) => match found.get(&name) {
                    Some(&(topic_id, _)) => (Some(topic_id), committed.as_ref().err().copied()),
                    None => (None, None),
                },
                // A failed lookup is reported on every requested partition.
                Err(code) => (None, Some(*code)),
            };
            let topic_unstable = unstable.get(&name);
            let partitions = partition_indexes
                .into_iter()
                .map(|partition_index| {
                    if let Some(code) = topic_error {
                        return offset_partition(partition_index, code, None);
                    }
                    let Some(topic_id) = topic_id else {
                        return offset_partition(partition_index, ERROR_NONE, None);
                    };
                    if topic_unstable.is_some_and(|u| u.contains(&partition_index)) {
                        return offset_partition(
                            partition_index,
                            ERROR_UNSTABLE_OFFSET_COMMIT,
                            None,
                        );
                    }
                    // Each (topic, partition) is answered once: merged above, and distinct
                    // names are distinct topics.
                    let offset = committed
                        .as_mut()
                        .ok()
                        .and_then(|c| c.remove(&(topic_id, partition_index)));
                    offset_partition(partition_index, ERROR_NONE, offset)
                })
                .collect();
            let mut topic_response = OffsetFetchResponseTopic::default();
            topic_response.name = TopicName(StrBytes::from_string(name));
            topic_response.partitions = partitions;
            topic_response
        })
        .collect())
}

/// Every committed offset of the group, by topic, or the error code of a failed read.
fn fetch_all_group_offsets(
    store: &dyn KafkaStore,
    group_id: &str,
    unstable: &Unstable,
) -> std::result::Result<Vec<OffsetFetchResponseTopic>, i16> {
    let offsets = store.fetch_all_offsets(group_id).map_err(|e| {
        crate::pg_warning!(
            "Failed to fetch all offsets for group='{}': {}",
            group_id,
            e
        );
        e.to_kafka_error_code()
    })?;
    let mut by_topic: HashMap<String, Vec<OffsetFetchResponsePartition>> = HashMap::new();
    for (topic_name, partition_id, committed) in offsets {
        let pending = unstable
            .get(&topic_name)
            .is_some_and(|u| u.contains(&partition_id));
        let partition = if pending {
            offset_partition(partition_id, ERROR_UNSTABLE_OFFSET_COMMIT, None)
        } else {
            offset_partition(partition_id, ERROR_NONE, Some(committed))
        };
        by_topic.entry(topic_name).or_default().push(partition);
    }
    Ok(by_topic
        .into_iter()
        .map(|(topic_name, partitions)| {
            let mut topic_response = OffsetFetchResponseTopic::default();
            topic_response.name = TopicName(StrBytes::from_string(topic_name));
            topic_response.partitions = partitions;
            topic_response
        })
        .collect())
}

/// An OffsetFetch partition entry: the committed offset and metadata, or offset -1 when
/// there is none to report.
fn offset_partition(
    partition_index: i32,
    error_code: i16,
    committed: Option<CommittedOffset>,
) -> OffsetFetchResponsePartition {
    let mut partition = OffsetFetchResponsePartition::default();
    partition.partition_index = partition_index;
    partition.error_code = error_code;
    // Kafka answers "" for an offset committed without metadata and for no offset at all
    // (OffsetFetchResponse.NO_METADATA), never null.
    let (offset, metadata) = committed.map_or((-1, None), |c| (c.offset, c.metadata));
    partition.committed_offset = offset;
    partition.metadata = Some(metadata.map_or_else(StrBytes::default, StrBytes::from_string));
    partition
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::error::KafkaError;
    use crate::testing::mocks::MockKafkaStore;
    use crate::testing::TestEnv;

    #[test]
    fn test_offset_fetch_all_propagates_storage_error() {
        // When fetch_all_offsets fails, a fetch-all OffsetFetch must surface the error: NONE
        // with no topics reads to a consumer as "no committed offsets", and it may reset.
        let mut store = MockKafkaStore::new();
        store
            .expect_fetch_all_offsets()
            .returning(|_| Err(KafkaError::database("boom")));

        let env = TestEnv::new();
        let ctx = env.ctx(&store);

        // topics = None selects the fetch-all path.
        let resp = handle_offset_fetch(&ctx, "g", None, false).unwrap();
        assert_ne!(resp.error_code, ERROR_NONE);
    }

    /// Kafka answers UNKNOWN_TOPIC_OR_PARTITION for a commit to a partition the topic
    /// does not have, and stores nothing for it; the topic's real partitions commit.
    #[test]
    fn test_offset_commit_to_a_missing_partition_is_unknown_topic_or_partition() {
        use crate::kafka::messages::{OffsetCommitPartitionData, OffsetCommitTopicData};
        use crate::kafka::storage::TopicMetadata;
        let mut store = MockKafkaStore::new();
        store.expect_get_topic_metadata().returning(|_| {
            Ok(vec![TopicMetadata {
                name: "t".to_string(),
                id: 1,
                partition_count: 2,
            }])
        });
        store
            .expect_commit_offset()
            .withf(|_, _, partition, _, _| *partition == 1)
            .times(1)
            .returning(|_, _, _, _, _| Ok(()));
        let env = TestEnv::new();
        let ctx = env.ctx(&store);
        let partition = |partition_index| OffsetCommitPartitionData {
            partition_index,
            committed_offset: 3,
            metadata: None,
        };
        let topics = vec![OffsetCommitTopicData {
            name: "t".to_string(),
            partitions: vec![partition(1), partition(2), partition(-1)],
        }];
        let resp = handle_offset_commit(&ctx, "g", -1, "", topics).unwrap();
        let codes: Vec<(i32, i16)> = resp.topics[0]
            .partitions
            .iter()
            .map(|p| (p.partition_index, p.error_code))
            .collect();
        assert_eq!(
            codes,
            vec![
                (1, ERROR_NONE),
                (2, ERROR_UNKNOWN_TOPIC_OR_PARTITION),
                (-1, ERROR_UNKNOWN_TOPIC_OR_PARTITION)
            ]
        );
    }

    /// Kafka 3.7 answers a topic that does not exist (never created, or deleted) with
    /// offset -1 and NONE, as any partition without a commit, and metadata "" wherever there
    /// is none; UNKNOWN_TOPIC_OR_PARTITION made the Java consumer's poll() throw.
    #[test]
    fn test_offset_fetch_missing_topic_is_no_offset() {
        use crate::kafka::messages::OffsetFetchTopicData;
        let mut store = MockKafkaStore::new();
        crate::testing::expect_topics(&mut store, |name| Ok((name == "t").then_some((1, 2))));
        store.expect_fetch_offsets().returning(|_, _| {
            let offset = CommittedOffset {
                offset: 4,
                metadata: None,
            };
            Ok(vec![(1, 0, offset)])
        });
        store
            .expect_pending_txn_offset_partitions()
            .returning(|_, _| Ok(Vec::new()));
        let env = TestEnv::new();
        let ctx = env.ctx(&store);
        let topics = vec![
            OffsetFetchTopicData {
                name: "gone".to_string(),
                partition_indexes: vec![0, 3],
            },
            OffsetFetchTopicData {
                name: "t".to_string(),
                partition_indexes: vec![0, 1],
            },
        ];
        for require_stable in [false, true] {
            let resp =
                handle_offset_fetch(&ctx, "g", Some(topics.clone()), require_stable).unwrap();
            let answers: Vec<(String, i32, i64, i16, Option<String>)> = resp
                .topics
                .iter()
                .flat_map(|t| {
                    t.partitions.iter().map(|p| {
                        (
                            t.name.0.to_string(),
                            p.partition_index,
                            p.committed_offset,
                            p.error_code,
                            p.metadata.as_ref().map(ToString::to_string),
                        )
                    })
                })
                .collect();
            let none = || Some(String::new());
            assert_eq!(
                answers,
                [
                    ("gone".to_string(), 0, -1, ERROR_NONE, none()),
                    ("gone".to_string(), 3, -1, ERROR_NONE, none()),
                    ("t".to_string(), 0, 4, ERROR_NONE, none()),
                    ("t".to_string(), 1, -1, ERROR_NONE, none()),
                ]
            );
        }
    }

    /// Kafka 3.7 (offset.metadata.max.bytes = 4096, counted in UTF-16 code units) answers
    /// longer commit metadata OFFSET_METADATA_TOO_LARGE and commits the other partitions.
    #[test]
    fn test_offset_commit_metadata_too_large() {
        use crate::kafka::messages::{OffsetCommitPartitionData, OffsetCommitTopicData};
        let mut store = MockKafkaStore::new();
        crate::testing::expect_topics(&mut store, |_| Ok(Some((1, 4))));
        store
            .expect_commit_offset()
            .withf(|_, _, partition, _, _| *partition != 1)
            .times(3)
            .returning(|_, _, _, _, _| Ok(()));
        let env = TestEnv::new();
        let ctx = env.ctx(&store);
        let entry = |partition_index, metadata: Option<String>| OffsetCommitPartitionData {
            partition_index,
            committed_offset: 3,
            metadata,
        };
        let topics = vec![OffsetCommitTopicData {
            name: "t".to_string(),
            partitions: vec![
                entry(0, Some("x".repeat(4096))),
                entry(1, Some("x".repeat(4097))),
                // 4096 two-byte characters: 8192 bytes, 4096 UTF-16 code units.
                entry(2, Some("é".repeat(4096))),
                entry(3, None),
            ],
        }];
        let resp = handle_offset_commit(&ctx, "g", -1, "", topics).unwrap();
        let codes: Vec<i16> = resp.topics[0]
            .partitions
            .iter()
            .map(|p| p.error_code)
            .collect();
        assert_eq!(
            codes,
            [
                ERROR_NONE,
                ERROR_OFFSET_METADATA_TOO_LARGE,
                ERROR_NONE,
                ERROR_NONE
            ]
        );
    }

    /// KIP-447: with require_stable, a partition with a pending transactional offset
    /// commit is UNSTABLE_OFFSET_COMMIT (offset -1), so a consumer does not start from
    /// an offset the transaction may still replace; the others and the same request
    /// without require_stable read the committed offset.
    #[test]
    fn test_offset_fetch_require_stable_reports_pending_txn_commits() {
        use crate::kafka::messages::OffsetFetchTopicData;
        use crate::kafka::storage::TopicMetadata;
        let mut store = MockKafkaStore::new();
        store.expect_get_topic_metadata().returning(|_| {
            Ok(vec![TopicMetadata {
                name: "t".to_string(),
                id: 1,
                partition_count: 2,
            }])
        });
        store.expect_fetch_offsets().returning(|_, partitions| {
            Ok(partitions
                .iter()
                .map(|&(topic_id, p)| {
                    let offset = CommittedOffset {
                        offset: 5,
                        metadata: None,
                    };
                    (topic_id, p, offset)
                })
                .collect())
        });
        // Only the requested partitions are read (KIP-447 require_stable).
        store
            .expect_pending_txn_offset_partitions()
            .withf(|group, partitions| group == "g" && *partitions == Some(&[(1, 0), (1, 1)][..]))
            .times(1)
            .returning(|_, _| Ok(vec![("t".to_string(), 0)]));
        let env = TestEnv::new();
        let ctx = env.ctx(&store);
        let topics = || {
            Some(vec![OffsetFetchTopicData {
                name: "t".to_string(),
                partition_indexes: vec![0, 1],
            }])
        };
        let resp = handle_offset_fetch(&ctx, "g", topics(), true).unwrap();
        assert_eq!(resp.error_code, ERROR_NONE);
        let parts = &resp.topics[0].partitions;
        assert_eq!(parts[0].error_code, ERROR_UNSTABLE_OFFSET_COMMIT);
        assert_eq!(parts[0].committed_offset, -1);
        assert_eq!(parts[1].error_code, ERROR_NONE);
        assert_eq!(parts[1].committed_offset, 5);

        // Without require_stable the pending set is not even read (times(1) above).
        let resp = handle_offset_fetch(&ctx, "g", topics(), false).unwrap();
        assert!(resp.topics[0]
            .partitions
            .iter()
            .all(|p| p.committed_offset == 5));
    }

    /// A require_stable fetch of all the group's offsets reads the group's pending
    /// partitions whatever they are: no partition list.
    #[test]
    fn test_offset_fetch_all_require_stable_reads_the_groups_pending_partitions() {
        let mut store = MockKafkaStore::new();
        store.expect_fetch_all_offsets().returning(|_| {
            let offset = |offset| CommittedOffset {
                offset,
                metadata: None,
            };
            Ok(vec![
                ("t".to_string(), 0, offset(5)),
                ("t".to_string(), 1, offset(6)),
            ])
        });
        store
            .expect_pending_txn_offset_partitions()
            .withf(|group, partitions| group == "g" && partitions.is_none())
            .times(1)
            .returning(|_, _| Ok(vec![("t".to_string(), 1)]));
        let env = TestEnv::new();
        let ctx = env.ctx(&store);
        let resp = handle_offset_fetch(&ctx, "g", None, true).unwrap();
        let mut parts: Vec<(i32, i16, i64)> = resp.topics[0]
            .partitions
            .iter()
            .map(|p| (p.partition_index, p.error_code, p.committed_offset))
            .collect();
        parts.sort_unstable();
        assert_eq!(
            parts,
            vec![(0, ERROR_NONE, 5), (1, ERROR_UNSTABLE_OFFSET_COMMIT, -1)]
        );
    }

    /// Kafka 3.7 stores a repeated OffsetCommit partition once, with its last entry, and
    /// answers it once, however many entries name it.
    #[test]
    fn test_offset_commit_repeated_partition_is_stored_once() {
        use crate::kafka::messages::{OffsetCommitPartitionData, OffsetCommitTopicData};
        let mut store = MockKafkaStore::new();
        crate::testing::expect_topics(&mut store, |_| Ok(Some((1, 2))));
        store
            .expect_commit_offset()
            .withf(|_, _, p, offset, _| (*p, *offset) == (0, 9_999))
            .times(1)
            .returning(|_, _, _, _, _| Ok(()));
        store
            .expect_commit_offset()
            .withf(|_, _, p, offset, _| (*p, *offset) == (1, 7))
            .times(1)
            .returning(|_, _, _, _, _| Ok(()));
        let env = TestEnv::new();
        let ctx = env.ctx(&store);
        let entry = |partition_index, committed_offset| OffsetCommitPartitionData {
            partition_index,
            committed_offset,
            metadata: None,
        };
        let mut first: Vec<_> = (0..10_000).map(|i| entry(0, i)).collect();
        first.insert(1, entry(1, 7));
        let topics = vec![
            OffsetCommitTopicData {
                name: "t".to_string(),
                partitions: first,
            },
            OffsetCommitTopicData {
                name: "t".to_string(),
                partitions: vec![entry(0, 9_999)],
            },
        ];
        let resp = handle_offset_commit(&ctx, "g", -1, "", topics).unwrap();
        assert_eq!(resp.topics.len(), 1);
        let parts: Vec<(i32, i16)> = resp.topics[0]
            .partitions
            .iter()
            .map(|p| (p.partition_index, p.error_code))
            .collect();
        assert_eq!(parts, [(0, ERROR_NONE), (1, ERROR_NONE)]);
    }

    /// The partitions of an OffsetFetch topic entry cost nothing per partition in its
    /// name's length: each looked its committed offset up by a copy of the name, so a 1 MB
    /// request naming a 32 KB topic with 200,000 partitions copied 6 GB on the DB thread
    /// (5,000 partitions: 160 MB).
    #[test]
    fn test_offset_fetch_partitions_do_not_copy_the_name() {
        use crate::kafka::messages::OffsetFetchTopicData;
        let name = "n".repeat(32 * 1024);
        let mut store = MockKafkaStore::new();
        crate::testing::expect_topics(&mut store, |_| Ok(Some((1, i32::MAX))));
        store
            .expect_fetch_offsets()
            .returning(|_, _| Ok(Vec::new()));
        store
            .expect_pending_txn_offset_partitions()
            .returning(|_, _| Ok(Vec::new()));
        let env = TestEnv::new();
        let ctx = env.ctx(&store);
        let topics = vec![OffsetFetchTopicData {
            name,
            partition_indexes: (0..5_000).collect(),
        }];
        let (resp, allocated) = crate::testing::heap_allocated(|| {
            handle_offset_fetch(&ctx, "g", Some(topics), true).unwrap()
        });
        assert_eq!(resp.topics[0].partitions.len(), 5_000);
        assert!(allocated < 4 << 20, "allocated {allocated} bytes");
    }

    /// OffsetFetch answers each distinct partition once and reads the requested offsets in
    /// one statement however many entries the request has.
    #[test]
    fn test_offset_fetch_repeated_partition_is_answered_once() {
        use crate::kafka::messages::OffsetFetchTopicData;
        type Answer = (String, Vec<(i32, i16, i64)>);
        let mut store = MockKafkaStore::new();
        crate::testing::expect_topics(&mut store, |name| Ok((name == "t").then_some((1, 1))));
        store
            .expect_fetch_offsets()
            .withf(|_, partitions| partitions == [(1, 0), (1, 9)])
            .times(1)
            .returning(|_, _| {
                let offset = CommittedOffset {
                    offset: 6,
                    metadata: None,
                };
                Ok(vec![(1, 0, offset)])
            });
        let env = TestEnv::new();
        let ctx = env.ctx(&store);
        let topic = |name: &str, partition_indexes: Vec<i32>| OffsetFetchTopicData {
            name: name.to_string(),
            partition_indexes,
        };
        let topics = vec![
            topic("t", vec![0; 40_000]),
            topic("missing", vec![0, 0]),
            topic("t", vec![9, 0]),
        ];
        let resp = handle_offset_fetch(&ctx, "g", Some(topics), false).unwrap();
        let answers: Vec<Answer> = resp
            .topics
            .iter()
            .map(|t| {
                let parts = t
                    .partitions
                    .iter()
                    .map(|p| (p.partition_index, p.error_code, p.committed_offset))
                    .collect();
                (t.name.0.to_string(), parts)
            })
            .collect();
        assert_eq!(
            answers,
            [
                (
                    "t".to_string(),
                    vec![(0, ERROR_NONE, 6), (9, ERROR_NONE, -1)]
                ),
                ("missing".to_string(), vec![(0, ERROR_NONE, -1)]),
            ]
        );
    }
}
