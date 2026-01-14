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
    topics: Vec<(String, Vec<i32>)>, // (topic_name, partition_ids)
) -> Result<AddPartitionsToTxnResponse> {
    let store = ctx.store;
    let default_partitions = ctx.default_partitions;
    // Validate the transaction state
    store.validate_transaction(transactional_id, producer_id, producer_epoch)?;

    // Ensure transaction is in the right state (should be Ongoing or Empty->Ongoing)
    let state = store.get_transaction_state(transactional_id)?;
    match state {
        Some(crate::kafka::storage::TransactionState::Empty) => {
            // Transition to Ongoing
            store.begin_transaction(transactional_id, producer_id, producer_epoch)?;
        }
        Some(crate::kafka::storage::TransactionState::Ongoing) => {
            // Already ongoing, good
        }
        Some(other) => {
            return Err(KafkaError::invalid_txn_state(
                transactional_id,
                format!("{:?}", other),
                "AddPartitionsToTxn",
            ));
        }
        None => {
            return Err(KafkaError::transactional_id_not_found(transactional_id));
        }
    }

    // Build response with results for each partition
    let mut response = AddPartitionsToTxnResponse::default();
    let mut topic_results = Vec::new();

    for (topic_name, partition_ids) in topics {
        let mut partition_results = Vec::new();

        // Auto-create topic if it doesn't exist (matching Produce behavior)
        // This is necessary because rdkafka calls AddPartitionsToTxn before the topic exists
        let partition_count = match store.get_topic_partition_count(&topic_name) {
            Ok(Some(count)) => count,
            Ok(None) => {
                // Topic doesn't exist, auto-create it
                match store.get_or_create_topic(&topic_name, default_partitions) {
                    Ok((_, count)) => count,
                    Err(_) => {
                        // Failed to create, return error for all partitions
                        for partition_id in partition_ids {
                            let mut partition_result = AddPartitionsToTxnPartitionResult::default();
                            partition_result.partition_index = partition_id;
                            partition_result.partition_error_code =
                                crate::kafka::constants::ERROR_UNKNOWN_SERVER_ERROR;
                            partition_results.push(partition_result);
                        }
                        let mut topic_result = AddPartitionsToTxnTopicResult::default();
                        topic_result.name = TopicName(StrBytes::from_string(topic_name));
                        topic_result.results_by_partition = partition_results;
                        topic_results.push(topic_result);
                        continue;
                    }
                }
            }
            Err(_) => {
                // Error checking topic, return error for all partitions
                for partition_id in partition_ids {
                    let mut partition_result = AddPartitionsToTxnPartitionResult::default();
                    partition_result.partition_index = partition_id;
                    partition_result.partition_error_code =
                        crate::kafka::constants::ERROR_UNKNOWN_SERVER_ERROR;
                    partition_results.push(partition_result);
                }
                let mut topic_result = AddPartitionsToTxnTopicResult::default();
                topic_result.name = TopicName(StrBytes::from_string(topic_name));
                topic_result.results_by_partition = partition_results;
                topic_results.push(topic_result);
                continue;
            }
        };

        for partition_id in partition_ids {
            let error_code = if partition_id < partition_count {
                ERROR_NONE
            } else {
                crate::kafka::constants::ERROR_UNKNOWN_TOPIC_OR_PARTITION
            };

            let mut partition_result = AddPartitionsToTxnPartitionResult::default();
            partition_result.partition_index = partition_id;
            partition_result.partition_error_code = error_code;
            partition_results.push(partition_result);
        }

        let mut topic_result = AddPartitionsToTxnTopicResult::default();
        topic_result.name = TopicName(StrBytes::from_string(topic_name));
        topic_result.results_by_partition = partition_results;
        topic_results.push(topic_result);
    }

    response.results_by_topic_v3_and_below = topic_results;
    Ok(response)
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
    // Validate the transaction state
    store.validate_transaction(transactional_id, producer_id, producer_epoch)?;

    // Ensure transaction is ongoing
    let state = store.get_transaction_state(transactional_id)?;
    match state {
        Some(crate::kafka::storage::TransactionState::Ongoing) => {
            // Good, transaction is active
        }
        Some(crate::kafka::storage::TransactionState::Empty) => {
            // Start the transaction
            store.begin_transaction(transactional_id, producer_id, producer_epoch)?;
        }
        Some(other) => {
            return Err(KafkaError::invalid_txn_state(
                transactional_id,
                format!("{:?}", other),
                "AddOffsetsToTxn",
            ));
        }
        None => {
            return Err(KafkaError::transactional_id_not_found(transactional_id));
        }
    }

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
        Some(other) => {
            return Err(KafkaError::invalid_txn_state(
                transactional_id,
                format!("{:?}", other),
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
pub fn handle_txn_offset_commit(
    ctx: &HandlerContext,
    transactional_id: &str,
    producer_id: i64,
    producer_epoch: i16,
    group_id: &str,
    topics: TxnOffsetCommitTopics,
) -> Result<TxnOffsetCommitResponse> {
    let store = ctx.store;
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
                format!("{:?}", other),
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

        // Resolve topic ID
        let topic_id_result = store.get_topic_id(&topic_name);

        for (partition_id, offset, metadata) in partitions {
            let error_code = match &topic_id_result {
                Ok(Some(topic_id)) => {
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
