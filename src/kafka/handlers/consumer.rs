// Consumer offset handlers
//
// Handlers for OffsetCommit and OffsetFetch requests.
// These track consumer group progress through partitions.

use super::helpers::{
    resolve_topic, resolve_topic_id, topic_resolution_error_code, TopicResolution,
};
use crate::kafka::constants::{ERROR_NONE, ERROR_UNKNOWN_TOPIC_OR_PARTITION};
use crate::kafka::error::Result;
use crate::kafka::handler_context::HandlerContext;
use crate::kafka::storage::CommittedOffset;
use kafka_protocol::messages::TopicName;
use kafka_protocol::protocol::StrBytes;

/// Handle OffsetCommit request
///
/// Commits consumer offsets for a consumer group
pub fn handle_offset_commit(
    ctx: &HandlerContext,
    group_id: &str,
    generation_id: i32,
    member_id: &str,
    topics: Vec<crate::kafka::messages::OffsetCommitTopicData>,
) -> Result<kafka_protocol::messages::offset_commit_response::OffsetCommitResponse> {
    let store = ctx.store;
    use kafka_protocol::messages::offset_commit_response::{
        OffsetCommitResponse, OffsetCommitResponsePartition, OffsetCommitResponseTopic,
    };

    // CONF-1: reject zombie commits. A member committing with a stale generation (or an unknown
    // member id) for a live group must be rejected with ILLEGAL_GENERATION / UNKNOWN_MEMBER_ID
    // rather than silently persisting an offset. wave3 groups: Kafka 3.7's rules, see
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
/// `require_stable` (v7+, KIP-447; wave 3): a partition with a transactional offset
/// commit whose transaction is still open is answered UNSTABLE_OFFSET_COMMIT with
/// offset -1 (partition level, as Kafka 3.7), so a read_committed consumer retries
/// instead of starting from an offset the transaction may still replace.
pub fn handle_offset_fetch(
    ctx: &HandlerContext,
    group_id: &str,
    topics: Option<Vec<crate::kafka::messages::OffsetFetchTopicData>>,
    require_stable: bool,
) -> Result<kafka_protocol::messages::offset_fetch_response::OffsetFetchResponse> {
    let store = ctx.store;
    // wave3 txn: partitions with a pending transactional commit (require_stable only).
    let unstable: std::collections::HashSet<(String, i32)> = if require_stable {
        store
            .pending_txn_offset_partitions(group_id)?
            .into_iter()
            .collect()
    } else {
        std::collections::HashSet::new()
    };
    let is_unstable = |topic: &str, partition: i32| {
        !unstable.is_empty() && unstable.contains(&(topic.to_string(), partition))
    };
    use kafka_protocol::messages::offset_fetch_response::{
        OffsetFetchResponse, OffsetFetchResponsePartition, OffsetFetchResponseTopic,
    };

    let mut response_topics = Vec::new();
    // BUG-6: track a top-level error so a fetch-all storage failure surfaces to the client as an
    // error instead of a success response with no offsets (which a consumer reads as "no committed
    // offsets" and may reset from).
    let mut top_level_error = ERROR_NONE;

    if let Some(topic_list) = topics {
        // Fetch specific topics and partitions
        for topic_request in topic_list {
            let topic_name = topic_request.name.clone();

            // Look up topic_id using helper
            let topic_id = match resolve_topic_id(store, &topic_name) {
                TopicResolution::Found(id) => id,
                resolution => {
                    // Topic not found or error - return error for all partitions
                    let error_code = topic_resolution_error_code(&resolution);
                    let mut error_partitions = Vec::new();
                    for partition_index in topic_request.partition_indexes {
                        let mut partition_response = OffsetFetchResponsePartition::default();
                        partition_response.partition_index = partition_index;
                        partition_response.error_code = error_code;
                        partition_response.committed_offset = -1;
                        error_partitions.push(partition_response);
                    }

                    let mut topic_response = OffsetFetchResponseTopic::default();
                    topic_response.name = TopicName(StrBytes::from_string(topic_name));
                    topic_response.partitions = error_partitions;
                    response_topics.push(topic_response);
                    continue;
                }
            };

            let mut partition_responses = Vec::new();

            // Fetch committed offset for each requested partition
            for partition_index in topic_request.partition_indexes {
                let offset_result = store.fetch_offset(group_id, topic_id, partition_index);

                let mut partition_response = OffsetFetchResponsePartition::default();
                partition_response.partition_index = partition_index;

                if is_unstable(&topic_name, partition_index) {
                    partition_response.committed_offset = -1;
                    partition_response.error_code =
                        crate::kafka::constants::ERROR_UNSTABLE_OFFSET_COMMIT;
                    partition_responses.push(partition_response);
                    continue;
                }

                match offset_result {
                    Ok(Some(CommittedOffset { offset, metadata })) => {
                        partition_response.committed_offset = offset;
                        partition_response.metadata = metadata.map(StrBytes::from_string);
                        partition_response.error_code = ERROR_NONE;
                    }
                    Ok(None) => {
                        // No committed offset yet, return -1
                        partition_response.committed_offset = -1;
                        partition_response.error_code = ERROR_NONE;
                    }
                    Err(e) => {
                        // Use typed error's Kafka error code
                        let error_code = e.to_kafka_error_code();
                        if e.is_server_error() {
                            crate::pg_warning!(
                                "Failed to fetch offset for group='{}', topic_id={}, partition={}: {}",
                                group_id,
                                topic_id,
                                partition_index,
                                e
                            );
                        }
                        partition_response.committed_offset = -1;
                        partition_response.error_code = error_code;
                    }
                }

                partition_responses.push(partition_response);
            }

            let mut topic_response = OffsetFetchResponseTopic::default();
            topic_response.name = TopicName(StrBytes::from_string(topic_name));
            topic_response.partitions = partition_responses;
            response_topics.push(topic_response);
        }
    } else {
        // Fetch all topics for this consumer group
        let all_offsets_result = store.fetch_all_offsets(group_id);

        match all_offsets_result {
            Ok(offsets) => {
                // Group by topic
                let mut topics_map: std::collections::HashMap<
                    String,
                    Vec<OffsetFetchResponsePartition>,
                > = std::collections::HashMap::new();

                for (topic_name, partition_id, CommittedOffset { offset, metadata }) in offsets {
                    let mut partition_response = OffsetFetchResponsePartition::default();
                    partition_response.partition_index = partition_id;
                    if is_unstable(&topic_name, partition_id) {
                        partition_response.committed_offset = -1;
                        partition_response.error_code =
                            crate::kafka::constants::ERROR_UNSTABLE_OFFSET_COMMIT;
                    } else {
                        partition_response.committed_offset = offset;
                        partition_response.metadata = metadata.map(StrBytes::from_string);
                        partition_response.error_code = ERROR_NONE;
                    }

                    topics_map
                        .entry(topic_name)
                        .or_default()
                        .push(partition_response);
                }

                for (topic_name, partitions) in topics_map {
                    let mut topic_response = OffsetFetchResponseTopic::default();
                    topic_response.name = TopicName(StrBytes::from_string(topic_name));
                    topic_response.partitions = partitions;
                    response_topics.push(topic_response);
                }
            }
            Err(e) => {
                // BUG-6: surface the storage failure to the client instead of swallowing it.
                top_level_error = e.to_kafka_error_code();
                crate::pg_warning!(
                    "Failed to fetch all offsets for group='{}': {}",
                    group_id,
                    e
                );
            }
        }
    }

    // Build response
    let mut kafka_response = OffsetFetchResponse::default();
    kafka_response.throttle_time_ms = 0;
    kafka_response.error_code = top_level_error;
    kafka_response.topics = response_topics;

    Ok(kafka_response)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::error::KafkaError;
    use crate::testing::mocks::MockKafkaStore;
    use crate::testing::TestEnv;

    #[test]
    fn test_offset_fetch_all_propagates_storage_error() {
        // BUG-6: when fetch_all_offsets fails, the fetch-all OffsetFetch path used to log and
        // return error_code = NONE with no topics — a consumer reads that as "no committed
        // offsets" and may reset. It must surface the error instead.
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

    /// wave3 txn (KIP-447): with require_stable, a partition with a pending
    /// transactional offset commit is UNSTABLE_OFFSET_COMMIT (offset -1); the others
    /// and the same request without require_stable read the committed offset. Before
    /// wave 3 the flag was ignored and the stale committed offset was returned.
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
        assert_eq!(
            parts[0].error_code,
            crate::kafka::constants::ERROR_UNSTABLE_OFFSET_COMMIT
        );
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
