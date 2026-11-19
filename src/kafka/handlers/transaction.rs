// Transaction handlers
//
// Handles transaction-related requests for Kafka transactions (Phase 10).
// - AddPartitionsToTxn: Register partitions in an active transaction
// - AddOffsetsToTxn: Include consumer offset commits in a transaction
// - EndTxn: Commit or abort a transaction
// - TxnOffsetCommit: Commit offsets as part of a transaction

use crate::kafka::constants::ERROR_NONE;
use crate::kafka::error::{KafkaError, Result};
use crate::kafka::handler_context::HandlerContext;
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

    // Wave 3: resolve every partition first, without creating topics (Kafka answers
    // UNKNOWN_TOPIC_OR_PARTITION for a missing topic; producers create topics through
    // Metadata). If any partition is unknown, nothing is added, the transaction does
    // not begin, and the valid partitions get OPERATION_NOT_ATTEMPTED, as in Kafka.
    let mut resolved: Vec<ResolvedTopic> = Vec::new();
    let mut any_failed = false;
    for (topic_name, partition_ids) in topics {
        let topic = super::helpers::resolve_topic(store, topic_name);
        let results = partition_ids
            .iter()
            .map(|&partition_id| {
                let r = match topic {
                    Ok(Some((topic_id, count))) if partition_id >= 0 && partition_id < count => {
                        Ok(topic_id)
                    }
                    Err(code) => Err(code),
                    _ => Err(crate::kafka::constants::ERROR_UNKNOWN_TOPIC_OR_PARTITION),
                };
                any_failed |= r.is_err();
                (partition_id, r)
            })
            .collect();
        resolved.push((topic_name.clone(), results));
    }

    let build = |resolved: &[ResolvedTopic], ok_code: i16| {
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
    };

    if any_failed {
        return Ok(build(
            &resolved,
            crate::kafka::constants::ERROR_OPERATION_NOT_ATTEMPTED,
        ));
    }

    // Atomically begin or continue the transaction
    // This handles the race condition between checking state and transitioning.
    // On failure (fenced epoch, invalid state, ...) report the error on every
    // requested partition: the response's top-level error_code only exists from v4,
    // and an empty v0-3 response reads as success.
    if let Err(e) =
        store.begin_or_continue_transaction(transactional_id, producer_id, producer_epoch)
    {
        return Ok(add_partitions_to_txn_error_response(
            topics,
            e.to_kafka_error_code(),
        ));
    }

    // Register the partitions: a transactional Produce is accepted only for them.
    let partitions: Vec<(i32, i32)> = resolved
        .iter()
        .flat_map(|(_, results)| {
            results
                .iter()
                .filter_map(|(partition_id, r)| r.ok().map(|topic_id| (topic_id, *partition_id)))
        })
        .collect();
    store.add_txn_partitions(transactional_id, &partitions)?;

    Ok(build(&resolved, ERROR_NONE))
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
    _group_id: &str,
) -> Result<AddOffsetsToTxnResponse> {
    let store = ctx.store;
    // Atomically begin or continue the transaction
    store.begin_or_continue_transaction(transactional_id, producer_id, producer_epoch)?;

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
    let store = ctx.store;
    // Validate the transaction state
    store.validate_transaction(transactional_id, producer_id, producer_epoch)?;

    // Check current state
    let state = store.get_transaction_state(transactional_id)?;
    match state {
        Some(crate::kafka::storage::TransactionState::Ongoing) => {
            // Good, can commit or abort
        }
        Some(crate::kafka::storage::TransactionState::Empty) => {
            // Empty transaction - no-op commit/abort
            let mut response = EndTxnResponse::default();
            response.error_code = ERROR_NONE;
            return Ok(response);
        }
        // A retried EndTxn (its first response was lost) finds the transaction already
        // finished. When the outcome matches what the client asks for, Kafka answers
        // NONE; INVALID_TXN_STATE would be fatal to the client for a commit that did
        // succeed. The opposite outcome is still an invalid transition.
        Some(crate::kafka::storage::TransactionState::CompleteCommit) if committed => {
            return Ok(EndTxnResponse::default());
        }
        Some(crate::kafka::storage::TransactionState::CompleteAbort) if !committed => {
            return Ok(EndTxnResponse::default());
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

    let mut response = EndTxnResponse::default();
    response.error_code = ERROR_NONE;
    Ok(response)
}

/// Handle TxnOffsetCommit request
///
/// Commits offsets as part of a transaction. Unlike regular offset commits,
/// these are stored pending and only become visible when the transaction commits.
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

    // RV-5: fence a zombie EOS consumer. A member committing offsets inside a
    // transaction with a stale group generation (or an unknown member id) must be
    // rejected with ILLEGAL_GENERATION / UNKNOWN_MEMBER_ID — the same fence RA-5
    // added to the non-transactional OffsetCommit path. The producer-epoch fence
    // alone does not cover KIP-447 (the new owner uses a *different*
    // transactional_id, so the zombie keeps a valid epoch). Simple consumers /
    // pre-v3 clients (generation -1) are exempt inside validate_commit.
    if let Err(e) = ctx
        .coordinator
        .validate_commit(group_id, member_id, generation_id)
    {
        return Ok(txn_offset_commit_error_response(
            &topics,
            e.to_kafka_error_code(),
        ));
    }

    // Validate the transaction state
    store.validate_transaction(transactional_id, producer_id, producer_epoch)?;

    // Ensure transaction is ongoing
    let state = store.get_transaction_state(transactional_id)?;
    match state {
        Some(crate::kafka::storage::TransactionState::Ongoing) => {
            // Good, transaction is active
        }
        Some(other) => {
            return Err(KafkaError::invalid_txn_state(
                transactional_id,
                format!("{other:?}"),
                "TxnOffsetCommit",
            ));
        }
        None => {
            return Err(KafkaError::transactional_id_not_found(transactional_id));
        }
    }

    // Build response with results for each partition
    let mut response = TxnOffsetCommitResponse::default();
    let mut topic_results = Vec::new();

    for (topic_name, partitions) in topics {
        let mut partition_results = Vec::new();

        // Resolve topic ID and partition count. The partition range check mirrors
        // AddPartitionsToTxn: a partition outside [0, partition_count) is rejected with
        // UNKNOWN_TOPIC_OR_PARTITION instead of being stored as a pending offset.
        let topic_result = store.get_topic_id(&topic_name).and_then(|id| match id {
            Some(id) => Ok(store
                .get_topic_partition_count(&topic_name)?
                .map(|count| (id, count))),
            None => Ok(None),
        });

        for (partition_id, offset, metadata) in partitions {
            let error_code = match &topic_result {
                Ok(Some((_, partition_count)))
                    if partition_id < 0 || partition_id >= *partition_count =>
                {
                    crate::kafka::constants::ERROR_UNKNOWN_TOPIC_OR_PARTITION
                }
                Ok(Some((topic_id, _))) => {
                    // Store the pending offset commit
                    match store.store_txn_pending_offset(
                        transactional_id,
                        group_id,
                        *topic_id,
                        partition_id,
                        offset,
                        metadata.as_deref(),
                    ) {
                        Ok(()) => ERROR_NONE,
                        Err(_) => crate::kafka::constants::ERROR_UNKNOWN_SERVER_ERROR,
                    }
                }
                Ok(None) => crate::kafka::constants::ERROR_UNKNOWN_TOPIC_OR_PARTITION,
                Err(_) => crate::kafka::constants::ERROR_UNKNOWN_SERVER_ERROR,
            };

            let mut partition_result = TxnOffsetCommitResponsePartition::default();
            partition_result.partition_index = partition_id;
            partition_result.error_code = error_code;
            partition_results.push(partition_result);
        }

        let mut topic_result = TxnOffsetCommitResponseTopic::default();
        topic_result.name = TopicName(StrBytes::from_string(topic_name));
        topic_result.partitions = partition_results;
        topic_results.push(topic_result);
    }

    response.topics = topic_results;
    Ok(response)
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
        // CONF-5: a negative partition id must be rejected (UNKNOWN_TOPIC_OR_PARTITION), not
        // accepted as ERROR_NONE. Wave 3: the request then fails as a whole (Kafka), the
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
        assert_eq!(
            neg.partition_error_code,
            crate::kafka::constants::ERROR_UNKNOWN_TOPIC_OR_PARTITION
        );
        assert_eq!(
            zero.partition_error_code,
            crate::kafka::constants::ERROR_OPERATION_NOT_ATTEMPTED
        );
    }

    #[test]
    fn test_txn_offset_commit_fences_stale_generation() {
        // RV-5: a TxnOffsetCommit carrying a real group generation (>= 0) for an
        // unknown / stale group membership must be fenced with UNKNOWN_MEMBER_ID,
        // not silently accepted. The producer-epoch fence alone does not cover this
        // (KIP-447: the new owner uses a different transactional_id). The fence
        // returns before any store access, so the mock needs no expectations.
        let store = MockKafkaStore::new();
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
