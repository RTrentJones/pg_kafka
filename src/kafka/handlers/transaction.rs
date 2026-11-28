//! Transaction handlers: AddPartitionsToTxn, AddOffsetsToTxn, EndTxn and TxnOffsetCommit.

use std::collections::HashMap;

use super::helpers::{merge_by_partition, resolve_topics, validate_ongoing_transaction};
use crate::kafka::constants::{
    ERROR_INVALID_GROUP_ID, ERROR_INVALID_TXN_STATE, ERROR_NONE, ERROR_OPERATION_NOT_ATTEMPTED,
    ERROR_UNKNOWN_SERVER_ERROR, ERROR_UNKNOWN_TOPIC_OR_PARTITION,
};
use crate::kafka::error::{KafkaError, Result};
use crate::kafka::handler_context::HandlerContext;
use crate::kafka::storage::{KafkaStore, TxnPartitions};
use kafka_protocol::messages::add_offsets_to_txn_response::AddOffsetsToTxnResponse;
use kafka_protocol::messages::add_partitions_to_txn_response::{
    AddPartitionsToTxnPartitionResult, AddPartitionsToTxnResponse, AddPartitionsToTxnTopicResult,
};
use kafka_protocol::messages::end_txn_response::EndTxnResponse;
use kafka_protocol::messages::txn_offset_commit_response::{
    TxnOffsetCommitResponse, TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic,
};
use kafka_protocol::messages::TopicName;
use kafka_protocol::protocol::StrBytes;

use crate::kafka::messages::TxnOffsetCommitTopics;

/// A requested topic and, per requested partition, its topic id or error code.
type ResolvedTopic = (String, Vec<(i32, std::result::Result<i32, i16>)>);

/// Handle AddPartitionsToTxn request
///
/// Registers partitions in an active transaction. This must be called before
/// producing to any partition as part of a transaction. The broker tracks
/// which partitions are involved so it can properly commit/abort them later.
///
/// # Arguments
/// * `ctx` - Handler context (store, coordinator, broker metadata, etc.)
/// * `transactional_id` - The transactional ID of the producer
/// * `producer_id` - The producer ID allocated by InitProducerId
/// * `producer_epoch` - The producer epoch for fencing
/// * `topics` - List of topics and partitions to add to the transaction
///
/// # Returns
/// AddPartitionsToTxnResponse with per-partition error codes
pub fn handle_add_partitions_to_txn(
    ctx: &HandlerContext,
    transactional_id: &str,
    producer_id: i64,
    producer_epoch: i16,
    topics: &[(String, Vec<i32>)], // (topic_name, partition_ids)
) -> Result<AddPartitionsToTxnResponse> {
    let store = ctx.store;

    // Resolve every partition first, without creating topics (Kafka answers
    // UNKNOWN_TOPIC_OR_PARTITION for a missing topic; producers create topics through
    // Metadata). If any partition is unknown, nothing is added, the transaction does
    // not begin, and the valid partitions get OPERATION_NOT_ATTEMPTED, as in Kafka.
    let resolved = resolve_txn_partitions(store, topics);
    let any_failed = resolved
        .iter()
        .any(|(_, results)| results.iter().any(|(_, r)| r.is_err()));
    if any_failed {
        return Ok(add_partitions_response(
            &resolved,
            ERROR_OPERATION_NOT_ATTEMPTED,
        ));
    }

    // Atomically begin or continue the transaction (no race between checking the state
    // and the transition). On failure (fenced epoch, invalid state, ...) report the error
    // on every requested partition: the response's top-level error_code only exists from
    // v4, and an empty v0-3 response reads as success.
    if let Err(e) =
        store.begin_or_continue_transaction(transactional_id, producer_id, producer_epoch)
    {
        return Ok(add_partitions_to_txn_error_response(
            topics,
            e.to_kafka_error_code(),
        ));
    }

    // Register the partitions: a transactional Produce is accepted only for them.
    let mut partitions: Vec<(i32, i32)> = resolved
        .iter()
        .flat_map(|(_, results)| {
            results
                .iter()
                .filter_map(|(partition_id, r)| r.ok().map(|topic_id| (topic_id, *partition_id)))
        })
        .collect();
    // A repeated entry is answered each time it is named but registered once.
    partitions.sort_unstable();
    partitions.dedup();
    store.add_txn_partitions(transactional_id, &partitions)?;

    Ok(add_partitions_response(&resolved, ERROR_NONE))
}

/// Each requested partition's topic id, or the error code that refuses it (a missing topic,
/// a failed lookup, or a partition outside the topic).
fn resolve_txn_partitions(
    store: &dyn KafkaStore,
    topics: &[(String, Vec<i32>)],
) -> Vec<ResolvedTopic> {
    let resolved = resolve_topics(store, topics.iter().map(|(name, _)| name.as_str()));
    topics
        .iter()
        .map(|(topic_name, partition_ids)| {
            let topic = match &resolved {
                Ok(found) => Ok(found.get(topic_name).copied()),
                Err(code) => Err(*code),
            };
            let results = partition_ids
                .iter()
                .map(|&partition_id| {
                    let r = match topic {
                        Ok(Some((topic_id, count)))
                            if partition_id >= 0 && partition_id < count =>
                        {
                            Ok(topic_id)
                        }
                        Err(code) => Err(code),
                        _ => Err(ERROR_UNKNOWN_TOPIC_OR_PARTITION),
                    };
                    (partition_id, r)
                })
                .collect();
            (topic_name.clone(), results)
        })
        .collect()
}

/// The AddPartitionsToTxn (v0-3) response for resolved partitions: `ok_code` for each
/// resolved one, its own error code for the others.
fn add_partitions_response(resolved: &[ResolvedTopic], ok_code: i16) -> AddPartitionsToTxnResponse {
    let mut response = AddPartitionsToTxnResponse::default();
    response.results_by_topic_v3_and_below = resolved
        .iter()
        .map(|(topic_name, results)| {
            let mut topic_result = AddPartitionsToTxnTopicResult::default();
            topic_result.name = TopicName(StrBytes::from_string(topic_name.clone()));
            topic_result.results_by_partition = results
                .iter()
                .map(|(partition_id, r)| {
                    let mut p = AddPartitionsToTxnPartitionResult::default();
                    p.partition_index = *partition_id;
                    p.partition_error_code = match r {
                        Ok(_) => ok_code,
                        Err(code) => *code,
                    };
                    p
                })
                .collect();
            topic_result
        })
        .collect();
    response
}

/// Handle AddOffsetsToTxn request
///
/// Adds a consumer group's offset commits to the current transaction.
/// This allows the transaction to include offset commits, enabling
/// exactly-once semantics for consume-process-produce patterns.
///
/// # Arguments
/// * `ctx` - Handler context (store, coordinator, broker metadata, etc.)
/// * `transactional_id` - The transactional ID of the producer
/// * `producer_id` - The producer ID allocated by InitProducerId
/// * `producer_epoch` - The producer epoch for fencing
/// * `group_id` - The consumer group ID whose offsets to include
///
/// # Returns
/// AddOffsetsToTxnResponse with error code
pub fn handle_add_offsets_to_txn(
    ctx: &HandlerContext,
    transactional_id: &str,
    producer_id: i64,
    producer_epoch: i16,
    group_id: &str,
) -> Result<AddOffsetsToTxnResponse> {
    let store = ctx.store;
    // Atomically begin or continue the transaction
    store.begin_or_continue_transaction(transactional_id, producer_id, producer_epoch)?;
    // Kafka adds the group's offsets partition to the transaction; TxnOffsetCommit is
    // then accepted for the group.
    store.add_txn_group(transactional_id, group_id)?;

    // Note: In Kafka, this returns the coordinator for the consumer group.
    // In our single-node implementation, we are always the coordinator.
    let mut response = AddOffsetsToTxnResponse::default();
    response.error_code = ERROR_NONE;

    Ok(response)
}

/// Handle EndTxn request
///
/// Commits or aborts a transaction. This is the final step in a transaction.
/// - Commit: Makes all transactional records visible and applies offset commits
/// - Abort: Marks all transactional records as aborted and discards offset commits
///
/// # Arguments
/// * `ctx` - Handler context (store, coordinator, broker metadata, etc.)
/// * `transactional_id` - The transactional ID of the producer
/// * `producer_id` - The producer ID allocated by InitProducerId
/// * `producer_epoch` - The producer epoch for fencing
/// * `committed` - true to commit, false to abort
///
/// # Returns
/// EndTxnResponse with error code
pub fn handle_end_txn(
    ctx: &HandlerContext,
    transactional_id: &str,
    producer_id: i64,
    producer_epoch: i16,
    committed: bool,
) -> Result<EndTxnResponse> {
    end_txn(
        ctx,
        transactional_id,
        producer_id,
        producer_epoch,
        committed,
    )
    .map(|(response, _)| response)
}

/// [`handle_end_txn`], also returning the partitions of the transaction it committed or
/// aborted (none when it ended nothing: a retry answered from the finished state, or an
/// error), the partitions whose read_committed long polls to wake.
pub fn end_txn(
    ctx: &HandlerContext,
    transactional_id: &str,
    producer_id: i64,
    producer_epoch: i16,
    committed: bool,
) -> Result<(EndTxnResponse, TxnPartitions)> {
    let store = ctx.store;
    // Validate the transaction state
    store.validate_transaction(transactional_id, producer_id, producer_epoch)?;

    // Check current state
    let state = store.get_transaction_state(transactional_id)?;
    match state {
        Some(crate::kafka::storage::TransactionState::Ongoing) => {
            // Good, can commit or abort
        }
        // An Empty transaction (nothing added since InitProducerId) falls through to
        // INVALID_TXN_STATE, as in Kafka 3.7; the Java client and librdkafka send no EndTxn
        // for a transaction they added nothing to.
        // A retried EndTxn (its first response was lost) finds the transaction already
        // finished. When the outcome matches what the client asks for, Kafka answers
        // NONE; INVALID_TXN_STATE would be fatal to the client for a commit that did
        // succeed. The opposite outcome is still an invalid transition.
        Some(crate::kafka::storage::TransactionState::CompleteCommit) if committed => {
            return Ok((EndTxnResponse::default(), TxnPartitions::new()));
        }
        Some(crate::kafka::storage::TransactionState::CompleteAbort) if !committed => {
            return Ok((EndTxnResponse::default(), TxnPartitions::new()));
        }
        Some(other) => {
            return Err(KafkaError::invalid_txn_state(
                transactional_id,
                format!("{other:?}"),
                "EndTxn",
            ));
        }
        None => {
            return Err(KafkaError::transactional_id_not_found(transactional_id));
        }
    }

    // Commit or abort
    if committed {
        store.commit_transaction(transactional_id, producer_id, producer_epoch)?;
    } else {
        store.abort_transaction(transactional_id, producer_id, producer_epoch)?;
    }

    let ended = store.transaction_partitions(transactional_id)?;

    let mut response = EndTxnResponse::default();
    response.error_code = ERROR_NONE;
    Ok((response, ended))
}

/// Handle TxnOffsetCommit request
///
/// Commits offsets as part of a transaction. Unlike regular offset commits,
/// these are stored pending and only become visible when the transaction commits. A
/// group the transaction did not add with AddOffsetsToTxn is refused INVALID_TXN_STATE.
///
/// # Arguments
/// * `ctx` - Handler context (store, coordinator, broker metadata, etc.)
/// * `transactional_id` - The transactional ID of the producer
/// * `producer_id` - The producer ID allocated by InitProducerId
/// * `producer_epoch` - The producer epoch for fencing
/// * `group_id` - The consumer group ID
/// * `topics` - List of topics with partition offsets to commit
///
/// # Returns
/// TxnOffsetCommitResponse with per-partition error codes
#[expect(
    clippy::too_many_arguments,
    reason = "one parameter per TxnOffsetCommit request field the handler checks"
)]
pub fn handle_txn_offset_commit(
    ctx: &HandlerContext,
    transactional_id: &str,
    producer_id: i64,
    producer_epoch: i16,
    group_id: &str,
    generation_id: i32,
    member_id: &str,
    topics: TxnOffsetCommitTopics,
) -> Result<TxnOffsetCommitResponse> {
    let store = ctx.store;
    // Kafka 3.7 collects the partitions into a map: a repeated partition is stored once,
    // with its last entry, and answered once.
    let topics: TxnOffsetCommitTopics = merge_by_partition(topics, |p| p.0);

    // Kafka 3.7 answers a partition that does not exist UNKNOWN_TOPIC_OR_PARTITION before
    // anything else (KafkaApis filters those out before it asks the coordinator), so every
    // refusal below answers only the partitions that exist.
    let resolved = match resolve_topics(store, topics.iter().map(|(name, _)| name.as_str())) {
        Ok(found) => found,
        Err(code) => return Ok(txn_offset_commit_error_response(&topics, code)),
    };

    let refusal = 'refusal: {
        // An empty group id is refused before the transaction is looked at
        // (GroupCoordinator.validateGroupStatus): INVALID_GROUP_ID, nothing stored.
        if group_id.is_empty() {
            break 'refusal Some(ERROR_INVALID_GROUP_ID);
        }
        // The transaction comes next, as in Kafka 3.7 (GroupCoordinator.handleTxnCommitOffsets
        // verifies the producer and the transaction before it looks at the group): a fenced
        // or unknown producer, a transaction that is not ongoing, or a group the transaction
        // has not added wins over a stale generation or an unknown member.
        match validate_ongoing_transaction(
            store,
            transactional_id,
            producer_id,
            producer_epoch,
            "TxnOffsetCommit",
        ) {
            Err(e) if e.is_server_error() => return Err(e),
            Err(e) => break 'refusal Some(e.to_kafka_error_code()),
            Ok(()) => {}
        }
        // Kafka 3.7 verifies that the transaction holds the group's offsets partition
        // (added by AddOffsetsToTxn) and answers INVALID_TXN_STATE otherwise.
        if !store.txn_has_group(transactional_id, group_id)? {
            break 'refusal Some(ERROR_INVALID_TXN_STATE);
        }
        // Then fence a zombie EOS consumer. A member committing offsets inside a
        // transaction with a stale group generation (or an unknown member id) must be
        // rejected with ILLEGAL_GENERATION / UNKNOWN_MEMBER_ID, the same fence as on the
        // non-transactional OffsetCommit path. The producer-epoch fence
        // alone does not cover KIP-447 (the new owner uses a *different*
        // transactional_id, so the zombie keeps a valid epoch). Generation -1 is
        // exempt inside validate_commit for a group without members or with an empty
        // member id (pre-v3 clients), as in Kafka 3.7.
        ctx.coordinator
            .validate_commit(group_id, member_id, generation_id)
            .err()
            .map(|e| e.to_kafka_error_code())
    };
    if let Some(code) = refusal {
        return Ok(txn_offset_commit_refused_response(&topics, &resolved, code));
    }

    let mut response = TxnOffsetCommitResponse::default();
    response.topics = topics
        .into_iter()
        .map(|(topic_name, partitions)| {
            let topic = resolved.get(&topic_name).copied();
            let txn = PendingOffsets {
                transactional_id,
                group_id,
            };
            store_txn_topic_offsets(store, txn, topic, topic_name, partitions)
        })
        .collect();
    Ok(response)
}

/// A refused TxnOffsetCommit: `error_code` on every partition that exists,
/// UNKNOWN_TOPIC_OR_PARTITION on the rest; nothing stored. Each topic is looked up once,
/// not once per partition: a long name over many partitions would be hashed per entry.
fn txn_offset_commit_refused_response(
    topics: &TxnOffsetCommitTopics,
    resolved: &HashMap<String, (i32, i32)>,
    error_code: i16,
) -> TxnOffsetCommitResponse {
    let mut response = txn_offset_commit_error_response(topics, error_code);
    for (topic, (name, _)) in response.topics.iter_mut().zip(topics) {
        let count = resolved.get(name).map_or(0, |&(_, count)| count);
        for p in &mut topic.partitions {
            if !(0..count).contains(&p.partition_index) {
                p.error_code = ERROR_UNKNOWN_TOPIC_OR_PARTITION;
            }
        }
    }
    response
}

/// Store one topic's offsets as pending offsets of the transaction and answer for each
/// partition. The partition range check mirrors AddPartitionsToTxn: a partition outside
/// `[0, partition_count)` is UNKNOWN_TOPIC_OR_PARTITION instead of a stored offset.
fn store_txn_topic_offsets(
    store: &dyn KafkaStore,
    txn: PendingOffsets<'_>,
    topic: Option<(i32, i32)>,
    topic_name: String,
    partitions: Vec<(i32, i64, Option<String>)>,
) -> TxnOffsetCommitResponseTopic {
    let PendingOffsets {
        transactional_id,
        group_id,
    } = txn;
    let partition_results = partitions
        .into_iter()
        .map(|(partition_id, offset, metadata)| {
            let error_code = match topic {
                Some((_, partition_count))
                    if partition_id < 0 || partition_id >= partition_count =>
                {
                    ERROR_UNKNOWN_TOPIC_OR_PARTITION
                }
                // Kafka 3.7 stores the other partitions and answers this one.
                Some(_)
                    if crate::kafka::constants::offset_metadata_too_large(metadata.as_deref()) =>
                {
                    crate::kafka::constants::ERROR_OFFSET_METADATA_TOO_LARGE
                }
                Some((topic_id, _)) => match store.store_txn_pending_offset(
                    transactional_id,
                    group_id,
                    topic_id,
                    partition_id,
                    offset,
                    metadata.as_deref(),
                ) {
                    Ok(()) => ERROR_NONE,
                    Err(_) => ERROR_UNKNOWN_SERVER_ERROR,
                },
                None => ERROR_UNKNOWN_TOPIC_OR_PARTITION,
            };
            let mut partition_result = TxnOffsetCommitResponsePartition::default();
            partition_result.partition_index = partition_id;
            partition_result.error_code = error_code;
            partition_result
        })
        .collect();
    let mut topic_result = TxnOffsetCommitResponseTopic::default();
    topic_result.name = TopicName(StrBytes::from_string(topic_name));
    topic_result.partitions = partition_results;
    topic_result
}

/// The transaction and group a TxnOffsetCommit stores pending offsets for.
#[derive(Clone, Copy)]
struct PendingOffsets<'a> {
    transactional_id: &'a str,
    group_id: &'a str,
}

/// An AddPartitionsToTxn (v0-3) response reporting `error_code` on every requested
/// partition.
pub fn add_partitions_to_txn_error_response(
    topics: &[(String, Vec<i32>)],
    error_code: i16,
) -> AddPartitionsToTxnResponse {
    let mut response = AddPartitionsToTxnResponse::default();
    response.error_code = error_code; // v4+ only; ignored by the v0-3 encoder
    response.results_by_topic_v3_and_below = topics
        .iter()
        .map(|(topic_name, partitions)| {
            let mut t = AddPartitionsToTxnTopicResult::default();
            t.name = TopicName(StrBytes::from_string(topic_name.clone()));
            t.results_by_partition = partitions
                .iter()
                .map(|partition_id| {
                    let mut p = AddPartitionsToTxnPartitionResult::default();
                    p.partition_index = *partition_id;
                    p.partition_error_code = error_code;
                    p
                })
                .collect();
            t
        })
        .collect();
    response
}

/// Build a TxnOffsetCommit response that reports `error_code` for every requested
/// partition. TxnOffsetCommitResponse has no top-level error code, so a request-level
/// failure (fenced epoch, invalid transaction state, stale generation, ...) has to be
/// echoed per partition; an empty response would read as success to the client.
pub fn txn_offset_commit_error_response(
    topics: &TxnOffsetCommitTopics,
    error_code: i16,
) -> TxnOffsetCommitResponse {
    let mut response = TxnOffsetCommitResponse::default();
    response.topics = topics
        .iter()
        .map(|(topic_name, partitions)| {
            let mut t = TxnOffsetCommitResponseTopic::default();
            t.name = TopicName(StrBytes::from_string(topic_name.clone()));
            t.partitions = partitions
                .iter()
                .map(|(partition_id, _offset, _metadata)| {
                    let mut p = TxnOffsetCommitResponsePartition::default();
                    p.partition_index = *partition_id;
                    p.error_code = error_code;
                    p
                })
                .collect();
            t
        })
        .collect();
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::mocks::MockKafkaStore;
    use crate::testing::TestEnv;

    #[test]
    fn test_end_txn_retry_with_same_outcome_succeeds() {
        use crate::kafka::storage::TransactionState;
        let cases = [
            (TransactionState::CompleteCommit, true, true),
            (TransactionState::CompleteAbort, false, true),
            (TransactionState::CompleteCommit, false, false),
            (TransactionState::CompleteAbort, true, false),
        ];
        for (state, committed, ok) in cases {
            let mut store = MockKafkaStore::new();
            store
                .expect_validate_transaction()
                .returning(|_, _, _| Ok(()));
            let st = state.clone();
            store
                .expect_get_transaction_state()
                .returning(move |_| Ok(Some(st.clone())));
            let env = TestEnv::new();
            let ctx = env.ctx(&store);
            let result = handle_end_txn(&ctx, "txn", 1, 0, committed);
            assert_eq!(result.is_ok(), ok, "{state:?} commit={committed}");
            if let Ok(r) = result {
                assert_eq!(r.error_code, ERROR_NONE);
            }
        }
    }

    #[test]
    fn test_add_partitions_to_txn_rejects_negative_partition() {
        // A negative partition id must be rejected (UNKNOWN_TOPIC_OR_PARTITION), not
        // accepted as ERROR_NONE. The request then fails as a whole (Kafka), the
        // valid partition 0 gets OPERATION_NOT_ATTEMPTED and nothing begins.
        let mut store = MockKafkaStore::new();
        store.expect_get_topic_metadata().returning(|_| {
            Ok(vec![crate::kafka::storage::TopicMetadata {
                name: "t".to_string(),
                id: 1,
                partition_count: 3,
            }])
        });

        let env = TestEnv::new();
        let ctx = env.ctx(&store);

        let topics = vec![("t".to_string(), vec![-1, 0])];
        let resp = handle_add_partitions_to_txn(&ctx, "txn-1", 1, 0, &topics).unwrap();
        let by_partition = &resp.results_by_topic_v3_and_below[0].results_by_partition;
        let neg = by_partition
            .iter()
            .find(|p| p.partition_index == -1)
            .unwrap();
        let zero = by_partition
            .iter()
            .find(|p| p.partition_index == 0)
            .unwrap();
        assert_eq!(neg.partition_error_code, ERROR_UNKNOWN_TOPIC_OR_PARTITION);
        assert_eq!(zero.partition_error_code, ERROR_OPERATION_NOT_ATTEMPTED);
    }

    /// Every AddPartitionsToTxn entry is answered (as Kafka 3.7 does), but the topics are
    /// looked up in one statement and a repeated partition is registered once.
    #[test]
    fn test_add_partitions_to_txn_repeated_topic_is_looked_up_once() {
        let mut store = MockKafkaStore::new();
        store.expect_get_topic_metadata().times(1).returning(|_| {
            Ok(vec![crate::kafka::storage::TopicMetadata {
                name: "t".to_string(),
                id: 1,
                partition_count: 2,
            }])
        });
        store
            .expect_begin_or_continue_transaction()
            .times(1)
            .returning(|_, _, _| Ok(()));
        store
            .expect_add_txn_partitions()
            .withf(|_, partitions| partitions == [(1, 0), (1, 1)])
            .times(1)
            .returning(|_, _| Ok(()));
        let env = TestEnv::new();
        let ctx = env.ctx(&store);
        let mut topics = vec![("t".to_string(), vec![0, 0]); 10_000];
        topics.push(("t".to_string(), vec![1]));
        let resp = handle_add_partitions_to_txn(&ctx, "txn-1", 1, 0, &topics).unwrap();
        assert_eq!(resp.results_by_topic_v3_and_below.len(), 10_001);
        assert!(resp
            .results_by_topic_v3_and_below
            .iter()
            .flat_map(|t| &t.results_by_partition)
            .all(|p| p.partition_error_code == ERROR_NONE));
    }

    /// TxnOffsetCommit stores a repeated partition once, with its last entry, and answers
    /// it once (Kafka 3.7 collects the partitions into a map).
    #[test]
    fn test_txn_offset_commit_repeated_partition_is_stored_once() {
        let mut store = MockKafkaStore::new();
        store
            .expect_validate_transaction()
            .returning(|_, _, _| Ok(()));
        store
            .expect_get_transaction_state()
            .returning(|_| Ok(Some(crate::kafka::storage::TransactionState::Ongoing)));
        store.expect_txn_has_group().returning(|_, _| Ok(true));
        crate::testing::expect_topics(&mut store, |_| Ok(Some((1, 1))));
        store
            .expect_store_txn_pending_offset()
            .withf(|_, _, _, partition, offset, _| (*partition, *offset) == (0, 9_999))
            .times(1)
            .returning(|_, _, _, _, _, _| Ok(()));
        let env = TestEnv::new();
        let ctx = env.ctx(&store);
        let topics = vec![
            ("t".to_string(), (0..9_000).map(|i| (0, i, None)).collect()),
            ("t".to_string(), vec![(0, 9_999, None)]),
        ];
        let resp = handle_txn_offset_commit(&ctx, "txn", 1, 0, "g", -1, "", topics).unwrap();
        assert_eq!(resp.topics.len(), 1);
        assert_eq!(resp.topics[0].partitions.len(), 1);
        assert_eq!(resp.topics[0].partitions[0].error_code, ERROR_NONE);
    }

    #[test]
    fn test_txn_offset_commit_fences_stale_generation() {
        // A TxnOffsetCommit carrying a real group generation (>= 0) for an
        // unknown / stale group membership must be fenced with UNKNOWN_MEMBER_ID,
        // not silently accepted. The producer-epoch fence alone does not cover this
        // (KIP-447: the new owner uses a different transactional_id). The fence
        // comes after the transaction checks and before any offset is stored.
        let mut store = MockKafkaStore::new();
        crate::testing::expect_topics(&mut store, |_| Ok(Some((1, 1))));
        store
            .expect_validate_transaction()
            .returning(|_, _, _| Ok(()));
        store
            .expect_get_transaction_state()
            .returning(|_| Ok(Some(crate::kafka::storage::TransactionState::Ongoing)));
        store.expect_txn_has_group().returning(|_, _| Ok(true));
        store.expect_store_txn_pending_offset().never();
        let env = TestEnv::new();
        let ctx = env.ctx(&store);

        let topics = vec![("t".to_string(), vec![(0i32, 100i64, None)])];
        let resp =
            handle_txn_offset_commit(&ctx, "txn-1", 1, 0, "g", 5, "zombie-member", topics).unwrap();

        let partition = &resp.topics[0].partitions[0];
        assert_eq!(
            partition.error_code,
            crate::kafka::constants::ERROR_UNKNOWN_MEMBER_ID,
            "a stale/unknown-group txn offset commit must be fenced"
        );
    }
}
