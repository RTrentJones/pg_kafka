//! OffsetCommit and OffsetFetch: a consumer group's committed offsets.

use super::helpers::{
    resolve_topic, resolve_topic_id, topic_resolution_error_code, TopicResolution,
};
use crate::kafka::constants::{
    ERROR_NONE, ERROR_UNKNOWN_TOPIC_OR_PARTITION, ERROR_UNSTABLE_OFFSET_COMMIT,
};
use crate::kafka::error::Result;
use crate::kafka::handler_context::HandlerContext;
use crate::kafka::messages::OffsetFetchTopicData;
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
    topics: Vec<crate::kafka::messages::OffsetCommitTopicData>,
) -> Result<OffsetCommitResponse> {
    let store = ctx.store;

    // Reject zombie commits. A member committing with a stale generation (or an unknown
    // member id) for a live group must be rejected with ILLEGAL_GENERATION / UNKNOWN_MEMBER_ID
    // rather than silently persisting an offset. Kafka 3.7's rules, see
    // `GroupCoordinator::validate_offset_commit` (generation -1 only for an unknown/Empty group).
    if let Err(e) = ctx
        .coordinator
        .validate_offset_commit(group_id, member_id, generation_id)
    {
        let error_code = e.to_kafka_error_code();
        let response_topics: Vec<_> = topics
            .into_iter()
            .map(|topic| {
                let partitions: Vec<_> = topic
                    .partitions
                    .into_iter()
                    .map(|partition| {
                        let mut partition_response = OffsetCommitResponsePartition::default();
                        partition_response.partition_index = partition.partition_index;
                        partition_response.error_code = error_code;
                        partition_response
                    })
                    .collect();
                let mut topic_response = OffsetCommitResponseTopic::default();
                topic_response.name = TopicName(StrBytes::from_string(topic.name));
                topic_response.partitions = partitions;
                topic_response
            })
            .collect();
        let mut response = OffsetCommitResponse::default();
        response.topics = response_topics;
        return Ok(response);
    }

    let mut response_topics = Vec::new();

    // Process each topic
    for topic in topics {
        let topic_name = topic.name.clone();

        // Look up topic_id and partition count using helper
        let (topic_id, partition_count) = match resolve_topic(store, &topic_name) {
            Ok(Some(found)) => found,
            unresolved => {
                // Topic not found or error - return error for all partitions
                let error_code = unresolved.err().unwrap_or(ERROR_UNKNOWN_TOPIC_OR_PARTITION);
                let mut error_partitions = Vec::new();
                for partition in topic.partitions {
                    let mut partition_response = OffsetCommitResponsePartition::default();
                    partition_response.partition_index = partition.partition_index;
                    partition_response.error_code = error_code;
                    error_partitions.push(partition_response);
                }

                let mut topic_response = OffsetCommitResponseTopic::default();
                topic_response.name = TopicName(StrBytes::from_string(topic_name));
                topic_response.partitions = error_partitions;
                response_topics.push(topic_response);
                continue;
            }
        };

        let mut partition_responses = Vec::new();

        // Process each partition offset commit
        for partition in topic.partitions {
            let partition_id = partition.partition_index;
            let committed_offset = partition.committed_offset;
            let metadata = partition.metadata.as_deref();

            // Kafka refuses a commit for a partition the topic does not have.
            if partition_id < 0 || partition_id >= partition_count {
                let mut partition_response = OffsetCommitResponsePartition::default();
                partition_response.partition_index = partition_id;
                partition_response.error_code = ERROR_UNKNOWN_TOPIC_OR_PARTITION;
                partition_responses.push(partition_response);
                continue;
            }

            // Commit offset
            let error_code = match store.commit_offset(
                group_id,
                topic_id,
                partition_id,
                committed_offset,
                metadata,
            ) {
                Ok(()) => ERROR_NONE,
                Err(e) => {
                    // Use typed error's Kafka error code
                    let code = e.to_kafka_error_code();
                    if e.is_server_error() {
                        crate::pg_warning!(
                            "Failed to commit offset for group='{}', topic_id={}, partition={}: {}",
                            group_id,
                            topic_id,
                            partition_id,
                            e
                        );
                    }
                    code
                }
            };

            let mut partition_response = OffsetCommitResponsePartition::default();
            partition_response.partition_index = partition_id;
            partition_response.error_code = error_code;
            partition_responses.push(partition_response);
        }

        let mut topic_response = OffsetCommitResponseTopic::default();
        topic_response.name = TopicName(StrBytes::from_string(topic_name));
        topic_response.partitions = partition_responses;
        response_topics.push(topic_response);
    }

    // Build response
    let mut kafka_response = OffsetCommitResponse::default();
    kafka_response.throttle_time_ms = 0;
    kafka_response.topics = response_topics;

    Ok(kafka_response)
}

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
    // Partitions with a pending transactional commit (require_stable only).
    let unstable: HashSet<(String, i32)> = if require_stable {
        store
            .pending_txn_offset_partitions(group_id)?
            .into_iter()
            .collect()
    } else {
        HashSet::new()
    };

    let mut kafka_response = OffsetFetchResponse::default();
    kafka_response.throttle_time_ms = 0;
    kafka_response.error_code = ERROR_NONE;
    match topics {
        Some(topics) => {
            kafka_response.topics = topics
                .into_iter()
                .map(|topic| fetch_topic_offsets(store, group_id, topic, &unstable))
                .collect();
        }
        // A fetch-all storage failure is a top-level error: a success with no offsets
        // reads to a consumer as "no committed offsets", and it may reset.
        None => match fetch_all_group_offsets(store, group_id, &unstable) {
            Ok(topics) => kafka_response.topics = topics,
            Err(error_code) => kafka_response.error_code = error_code,
        },
    }
    Ok(kafka_response)
}

/// The committed offsets of one requested topic's partitions.
fn fetch_topic_offsets(
    store: &dyn KafkaStore,
    group_id: &str,
    topic: OffsetFetchTopicData,
    unstable: &HashSet<(String, i32)>,
) -> OffsetFetchResponseTopic {
    let partitions = match resolve_topic_id(store, &topic.name) {
        TopicResolution::Found(topic_id) => topic
            .partition_indexes
            .iter()
            .map(|&partition_index| {
                let offset = store.fetch_offset(group_id, topic_id, partition_index);
                if unstable.contains(&(topic.name.clone(), partition_index)) {
                    return offset_partition(partition_index, ERROR_UNSTABLE_OFFSET_COMMIT, None);
                }
                match offset {
                    Ok(committed) => offset_partition(partition_index, ERROR_NONE, committed),
                    Err(e) => {
                        if e.is_server_error() {
                            crate::pg_warning!(
                                "Failed to fetch offset for group='{}', topic_id={}, partition={}: {}",
                                group_id,
                                topic_id,
                                partition_index,
                                e
                            );
                        }
                        offset_partition(partition_index, e.to_kafka_error_code(), None)
                    }
                }
            })
            .collect(),
        // A missing topic (or a failed lookup) is reported on every requested partition.
        resolution => {
            let error_code = topic_resolution_error_code(&resolution);
            topic
                .partition_indexes
                .iter()
                .map(|&partition_index| offset_partition(partition_index, error_code, None))
                .collect()
        }
    };
    let mut topic_response = OffsetFetchResponseTopic::default();
    topic_response.name = TopicName(StrBytes::from_string(topic.name));
    topic_response.partitions = partitions;
    topic_response
}

/// Every committed offset of the group, by topic, or the error code of a failed read.
fn fetch_all_group_offsets(
    store: &dyn KafkaStore,
    group_id: &str,
    unstable: &HashSet<(String, i32)>,
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
        let partition = if unstable.contains(&(topic_name.clone(), partition_id)) {
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
    match committed {
        Some(CommittedOffset { offset, metadata }) => {
            partition.committed_offset = offset;
            partition.metadata = metadata.map(StrBytes::from_string);
        }
        None => partition.committed_offset = -1,
    }
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
        store.expect_fetch_offset().returning(|_, _, _| {
            Ok(Some(CommittedOffset {
                offset: 5,
                metadata: None,
            }))
        });
        store
            .expect_pending_txn_offset_partitions()
            .times(1)
            .returning(|_| Ok(vec![("t".to_string(), 0)]));
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
}
