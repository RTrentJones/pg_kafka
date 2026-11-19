//! Transaction SQL: transactional producers, transaction state, transactional
//! writes and offsets, commit and abort.

use super::PostgresStore;
use crate::kafka::error::{KafkaError, Result};
use crate::kafka::messages::Record;
use crate::kafka::storage::TransactionState;
use pgrx::prelude::*;

impl PostgresStore {
    /// Shared completion path for `commit_transaction` / `abort_transaction`
    /// (PR #84 documented follow-up). The two flows were ~90% duplicated SQL:
    /// identical validation (existence, fencing, Ongoing-state check), a
    /// terminal-visibility UPDATE on `kafka.messages` (NULL = visible vs
    /// 'aborted'), the pending-offset DELETE, and the terminal
    /// `kafka.transactions` state UPDATE — commit additionally promotes
    /// pending offsets into `kafka.consumer_offsets` before the delete.
    pub(super) fn end_transaction(
        &self,
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        commit: bool,
    ) -> Result<()> {
        let op = if commit { "commit" } else { "abort" };
        crate::pg_debug!(
            "PostgresStore::end_transaction ({}): transactional_id={}, producer_id={}, epoch={}",
            op,
            transactional_id,
            producer_id,
            producer_epoch
        );

        Spi::connect_mut(|client| {
            // Step 1: Validate transaction state
            let txn_table = client.select(
                "SELECT state, producer_id, producer_epoch FROM kafka.transactions WHERE transactional_id = $1",
                Some(1),
                &[transactional_id.into()],
            )?;

            if txn_table.is_empty() {
                return Err(KafkaError::transactional_id_not_found(transactional_id));
            }

            // `unwrap_or`/`unwrap_or_default` on these column reads is safe by
            // schema, not by luck: kafka.transactions.{producer_id, producer_epoch,
            // state} are all declared NOT NULL (sql/bootstrap.sql), so get_by_name
            // never returns None for a row that exists (and non-existence was already
            // handled by the is_empty() check above). The fallback value is therefore
            // unreachable and never masks a real NULL. The same reasoning applies to
            // the other `get_by_name(...).unwrap_or*` reads of NOT NULL columns
            // throughout this module.
            let row = txn_table.first();
            let current_producer_id: i64 = row.get_by_name("producer_id")?.unwrap_or(0);
            let current_epoch: i16 = row.get_by_name("producer_epoch")?.unwrap_or(0);
            let state: String = row.get_by_name("state")?.unwrap_or_default();

            if current_producer_id != producer_id || current_epoch != producer_epoch {
                return Err(KafkaError::producer_fenced(
                    producer_id,
                    producer_epoch,
                    current_epoch,
                ));
            }

            if state != "Ongoing" {
                return Err(KafkaError::invalid_txn_state(
                    transactional_id,
                    "Ongoing",
                    &state,
                ));
            }

            // Step 2: Terminal message visibility — commit makes pending rows
            // visible (txn_state = NULL); abort marks them 'aborted'.
            if commit {
                client.update(
                    "UPDATE kafka.messages SET txn_state = NULL
                     WHERE producer_id = $1 AND producer_epoch = $2 AND txn_state = 'pending'",
                    None,
                    &[producer_id.into(), producer_epoch.into()],
                )?;

                // Step 3 (commit only): Move pending offsets to consumer_offsets
                client.update(
                    "INSERT INTO kafka.consumer_offsets (group_id, topic_id, partition_id, committed_offset, metadata)
                     SELECT group_id, topic_id, partition_id, pending_offset, metadata
                     FROM kafka.txn_pending_offsets
                     WHERE transactional_id = $1
                     ON CONFLICT (group_id, topic_id, partition_id) DO UPDATE SET
                         committed_offset = EXCLUDED.committed_offset,
                         metadata = EXCLUDED.metadata,
                         commit_timestamp = NOW()",
                    None,
                    &[transactional_id.into()],
                )?;
            } else {
                client.update(
                    "UPDATE kafka.messages SET txn_state = 'aborted'
                     WHERE producer_id = $1 AND producer_epoch = $2 AND txn_state = 'pending'",
                    None,
                    &[producer_id.into(), producer_epoch.into()],
                )?;
            }

            // Step 4: Delete pending offsets (aborted ones are simply dropped)
            client.update(
                "DELETE FROM kafka.txn_pending_offsets WHERE transactional_id = $1",
                None,
                &[transactional_id.into()],
            )?;

            // Step 5: Terminal transaction state
            client.update(
                if commit {
                    "UPDATE kafka.transactions SET state = 'CompleteCommit', last_updated_at = NOW()
                     WHERE transactional_id = $1"
                } else {
                    "UPDATE kafka.transactions SET state = 'CompleteAbort', last_updated_at = NOW()
                     WHERE transactional_id = $1"
                },
                None,
                &[transactional_id.into()],
            )?;

            crate::pg_debug!("Transaction {} completed ({})", transactional_id, op);
            Ok(())
        })
        .map_err(|e| match e {
            KafkaError::TransactionalIdNotFound { .. }
            | KafkaError::ProducerFenced { .. }
            | KafkaError::InvalidTxnState { .. } => e,
            _ => KafkaError::Internal(format!("{}_transaction failed: {}", op, e)),
        })
    }

    // ===== Transaction Operations (Phase 10) =====

    pub(super) fn get_or_create_transactional_producer(
        &self,
        transactional_id: &str,
        transaction_timeout_ms: i32,
        client_id: Option<&str>,
    ) -> Result<(i64, i16)> {
        crate::pg_debug!(
            "PostgresStore::get_or_create_transactional_producer: transactional_id={}, timeout_ms={}",
            transactional_id,
            transaction_timeout_ms
        );

        Spi::connect_mut(|client| {
            // Look up existing producer by transactional_id
            let existing = client.select(
                "SELECT producer_id, epoch FROM kafka.producer_ids WHERE transactional_id = $1",
                Some(1),
                &[transactional_id.into()],
            )?;

            let (producer_id, epoch) = if existing.is_empty() {
                // New transactional producer - allocate producer_id
                let table = client.update(
                    "INSERT INTO kafka.producer_ids (client_id, transactional_id, epoch)
                     VALUES ($1, $2, 0)
                     RETURNING producer_id, epoch",
                    None,
                    &[client_id.into(), transactional_id.into()],
                )?;

                let row = table.first();
                let producer_id: i64 = row
                    .get_by_name("producer_id")?
                    .ok_or_else(|| KafkaError::Internal("Failed to get producer_id".into()))?;
                let epoch: i16 = row.get_by_name("epoch")?.unwrap_or(0);

                crate::pg_debug!(
                    "Allocated new transactional producer_id={}, epoch={}",
                    producer_id,
                    epoch
                );
                (producer_id, epoch)
            } else {
                // Existing transactional producer - bump epoch (fences old producer)
                let row = existing.first();
                let producer_id: i64 = row.get_by_name("producer_id")?.unwrap_or(0);
                let old_epoch: i16 = row.get_by_name("epoch")?.unwrap_or(0);

                // RV-2: InitProducerId must abort any transaction still in flight for this
                // transactional_id (the canonical producer crash-restart path). Otherwise the
                // previous epoch's `pending` records are stranded forever — commit/abort filter
                // on the new epoch and require state='Ongoing', the timeout sweep scans
                // state='Ongoing', and cleanup only touches 'aborted' — so nothing reclaims
                // them and get_last_stable_offset (MIN pending offset) pins the LSO permanently
                // (a hanging transaction). Abort the old-epoch pending records here, before the
                // bump. Harmless (matches nothing) when no transaction was in flight.
                client.update(
                    "UPDATE kafka.messages SET txn_state = 'aborted'
                     WHERE producer_id = $1 AND producer_epoch = $2 AND txn_state = 'pending'",
                    None,
                    &[producer_id.into(), old_epoch.into()],
                )?;
                client.update(
                    "DELETE FROM kafka.txn_pending_offsets WHERE transactional_id = $1",
                    None,
                    &[transactional_id.into()],
                )?;
                // Wave 3: the aborted transaction's partition registry goes with it.
                client.update(
                    "DELETE FROM kafka.txn_partitions WHERE transactional_id = $1",
                    None,
                    &[transactional_id.into()],
                )?;
                // The new epoch restarts sequence numbers at 0; drop the old epoch's
                // sequence state so its first batches are not dropped as duplicates.
                client.update(
                    "DELETE FROM kafka.producer_sequences WHERE producer_id = $1",
                    None,
                    &[producer_id.into()],
                )?;

                let table = if old_epoch == i16::MAX {
                    // RV-13: SMALLINT epoch exhausted. `epoch + 1` would overflow the
                    // column and fail InitProducerId forever for this transactional_id.
                    // Kafka handles exhaustion by allocating a fresh producer_id. Reassign
                    // this row's producer_id from the BIGSERIAL sequence and reset the epoch
                    // to 0. transactional_id is UNIQUE (one row), and the transactions FK is
                    // ON UPDATE CASCADE, so the transaction row's producer_id follows the new
                    // id automatically.
                    client.update(
                        "UPDATE kafka.producer_ids
                         SET producer_id = nextval(pg_get_serial_sequence('kafka.producer_ids', 'producer_id')),
                             epoch = 0, last_active_at = NOW()
                         WHERE producer_id = $1
                         RETURNING producer_id, epoch",
                        None,
                        &[producer_id.into()],
                    )?
                } else {
                    client.update(
                        "UPDATE kafka.producer_ids
                         SET epoch = epoch + 1, last_active_at = NOW()
                         WHERE producer_id = $1
                         RETURNING producer_id, epoch",
                        None,
                        &[producer_id.into()],
                    )?
                };

                let updated = table.first();
                let new_producer_id: i64 =
                    updated.get_by_name("producer_id")?.unwrap_or(producer_id);
                let new_epoch: i16 = updated
                    .get_by_name("epoch")?
                    .ok_or_else(|| KafkaError::Internal("Failed to get new epoch".into()))?;

                crate::pg_debug!(
                    "Existing transactional producer_id={} -> producer_id={}, new_epoch={}",
                    producer_id,
                    new_producer_id,
                    new_epoch
                );
                (new_producer_id, new_epoch)
            };

            // Create or update transaction record with state='Empty'
            client.update(
                "INSERT INTO kafka.transactions (transactional_id, producer_id, producer_epoch, state, timeout_ms)
                 VALUES ($1, $2, $3, 'Empty', $4)
                 ON CONFLICT (transactional_id) DO UPDATE SET
                     producer_id = EXCLUDED.producer_id,
                     producer_epoch = EXCLUDED.producer_epoch,
                     state = 'Empty',
                     timeout_ms = EXCLUDED.timeout_ms,
                     started_at = NULL,
                     last_updated_at = NOW()",
                None,
                &[
                    transactional_id.into(),
                    producer_id.into(),
                    epoch.into(),
                    transaction_timeout_ms.into(),
                ],
            )?;

            Ok((producer_id, epoch))
        })
        .map_err(|e: KafkaError| {
            KafkaError::Internal(format!("get_or_create_transactional_producer failed: {}", e))
        })
    }

    pub(super) fn begin_transaction(
        &self,
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
    ) -> Result<()> {
        crate::pg_debug!(
            "PostgresStore::begin_transaction: transactional_id={}, producer_id={}, epoch={}",
            transactional_id,
            producer_id,
            producer_epoch
        );

        Spi::connect_mut(|client| {
            clear_finished_txn_partitions(client, transactional_id, producer_id, producer_epoch)?;
            // Verify ownership and update state to 'Ongoing'
            let table = client.update(
                "UPDATE kafka.transactions
                 SET state = 'Ongoing', started_at = NOW(), last_updated_at = NOW()
                 WHERE transactional_id = $1 AND producer_id = $2 AND producer_epoch = $3
                   AND state IN ('Empty', 'CompleteCommit', 'CompleteAbort')
                 RETURNING state",
                None,
                &[transactional_id.into(), producer_id.into(), producer_epoch.into()],
            )?;

            if table.is_empty() {
                // Check if transaction exists with different producer/epoch
                let exists = client.select(
                    "SELECT state, producer_id, producer_epoch FROM kafka.transactions WHERE transactional_id = $1",
                    Some(1),
                    &[transactional_id.into()],
                )?;

                if exists.is_empty() {
                    return Err(KafkaError::transactional_id_not_found(transactional_id));
                }

                let row = exists.first();
                let current_producer_id: i64 = row.get_by_name("producer_id")?.unwrap_or(0);
                let current_epoch: i16 = row.get_by_name("producer_epoch")?.unwrap_or(0);
                let state: String = row.get_by_name("state")?.unwrap_or_default();

                if current_producer_id != producer_id || current_epoch != producer_epoch {
                    return Err(KafkaError::producer_fenced(producer_id, producer_epoch, current_epoch));
                }

                // State is not valid for beginning a transaction
                return Err(KafkaError::invalid_txn_state(
                    transactional_id,
                    "Empty, CompleteCommit, or CompleteAbort",
                    &state,
                ));
            }

            crate::pg_debug!("Transaction {} started", transactional_id);
            Ok(())
        })
        .map_err(|e| match e {
            KafkaError::TransactionalIdNotFound { .. }
            | KafkaError::ProducerFenced { .. }
            | KafkaError::InvalidTxnState { .. } => e,
            _ => KafkaError::Internal(format!("begin_transaction failed: {}", e)),
        })
    }

    pub(super) fn begin_or_continue_transaction(
        &self,
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
    ) -> Result<()> {
        crate::pg_debug!(
            "PostgresStore::begin_or_continue_transaction: transactional_id={}, producer_id={}, epoch={}",
            transactional_id,
            producer_id,
            producer_epoch
        );

        Spi::connect_mut(|client| {
            clear_finished_txn_partitions(client, transactional_id, producer_id, producer_epoch)?;
            // Atomic compare-and-swap: transition to 'Ongoing' if in valid starting state
            // OR continue if already 'Ongoing' with matching producer
            let updated = client.update(
                "UPDATE kafka.transactions
                 SET state = 'Ongoing',
                     started_at = COALESCE(started_at, NOW()),
                     last_updated_at = NOW()
                 WHERE transactional_id = $1
                   AND producer_id = $2
                   AND producer_epoch = $3
                   AND state IN ('Empty', 'CompleteCommit', 'CompleteAbort', 'Ongoing')
                 RETURNING state",
                None,
                &[transactional_id.into(), producer_id.into(), producer_epoch.into()],
            )?;

            if !updated.is_empty() {
                crate::pg_debug!("Transaction {} is now Ongoing", transactional_id);
                return Ok(());
            }

            // Update failed - check why
            let exists = client.select(
                "SELECT state, producer_id, producer_epoch FROM kafka.transactions WHERE transactional_id = $1",
                Some(1),
                &[transactional_id.into()],
            )?;

            if exists.is_empty() {
                return Err(KafkaError::transactional_id_not_found(transactional_id));
            }

            let row = exists.first();
            let current_producer_id: i64 =
                row.get_by_name("producer_id")?.ok_or_else(|| KafkaError::Database {
                    message: "Unexpected NULL in transactions.producer_id column".to_string(),
                })?;
            let current_epoch: i16 =
                row.get_by_name("producer_epoch")?.ok_or_else(|| KafkaError::Database {
                    message: "Unexpected NULL in transactions.producer_epoch column".to_string(),
                })?;
            let state: String = row.get_by_name("state")?.ok_or_else(|| KafkaError::Database {
                message: "Unexpected NULL in transactions.state column".to_string(),
            })?;

            if current_producer_id != producer_id || current_epoch != producer_epoch {
                return Err(KafkaError::producer_fenced(
                    producer_id,
                    producer_epoch,
                    current_epoch,
                ));
            }

            // State is not valid for beginning a transaction
            Err(KafkaError::invalid_txn_state(
                transactional_id,
                "Empty, CompleteCommit, or CompleteAbort",
                &state,
            ))
        })
        .map_err(|e| match e {
            KafkaError::TransactionalIdNotFound { .. }
            | KafkaError::ProducerFenced { .. }
            | KafkaError::InvalidTxnState { .. } => e,
            _ => KafkaError::Internal(format!("begin_or_continue_transaction failed: {}", e)),
        })
    }

    pub(super) fn validate_transaction(
        &self,
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
    ) -> Result<()> {
        crate::pg_debug!(
            "PostgresStore::validate_transaction: transactional_id={}, producer_id={}, epoch={}",
            transactional_id,
            producer_id,
            producer_epoch
        );

        Spi::connect(|client| {
            let table = client.select(
                "SELECT state, producer_id, producer_epoch FROM kafka.transactions WHERE transactional_id = $1",
                Some(1),
                &[transactional_id.into()],
            )?;

            if table.is_empty() {
                return Err(KafkaError::transactional_id_not_found(transactional_id));
            }

            let row = table.first();
            let current_producer_id: i64 = row.get_by_name("producer_id")?.unwrap_or(0);
            let current_epoch: i16 = row.get_by_name("producer_epoch")?.unwrap_or(0);
            let state: String = row.get_by_name("state")?.unwrap_or_default();

            if current_producer_id != producer_id || current_epoch != producer_epoch {
                return Err(KafkaError::producer_fenced(producer_id, producer_epoch, current_epoch));
            }

            // Note: We only validate ownership here, not state.
            // State validation is done by the handlers which know the expected states
            // for their specific operations (e.g., AddPartitionsToTxn allows Empty or Ongoing).
            let _ = state; // Acknowledge we read state but don't check it here

            Ok(())
        })
        .map_err(|e| match e {
            KafkaError::TransactionalIdNotFound { .. }
            | KafkaError::ProducerFenced { .. }
            | KafkaError::InvalidTxnState { .. } => e,
            _ => KafkaError::Internal(format!("validate_transaction failed: {}", e)),
        })
    }

    pub(super) fn insert_transactional_records(
        &self,
        topic_id: i32,
        partition_id: i32,
        records: &[Record],
        producer_id: i64,
        producer_epoch: i16,
    ) -> Result<i64> {
        crate::pg_debug!(
            "PostgresStore::insert_transactional_records: {} records for topic_id={}, partition_id={}, producer_id={}",
            records.len(),
            topic_id,
            partition_id,
            producer_id
        );
        // DR-24: shared implementation with the plain path; Some(..) stamps the
        // transaction columns and txn_state='pending'.
        self.insert_records_inner(
            topic_id,
            partition_id,
            records,
            Some((producer_id, producer_epoch)),
        )
    }

    pub(super) fn store_txn_pending_offset(
        &self,
        transactional_id: &str,
        group_id: &str,
        topic_id: i32,
        partition_id: i32,
        offset: i64,
        metadata: Option<&str>,
    ) -> Result<()> {
        crate::pg_debug!(
            "PostgresStore::store_txn_pending_offset: txn_id={}, group_id={}, topic_id={}, partition_id={}, offset={}",
            transactional_id,
            group_id,
            topic_id,
            partition_id,
            offset
        );

        Spi::connect_mut(|client| {
            client.update(
                "INSERT INTO kafka.txn_pending_offsets (transactional_id, group_id, topic_id, partition_id, pending_offset, metadata)
                 VALUES ($1, $2, $3, $4, $5, $6)
                 ON CONFLICT (transactional_id, group_id, topic_id, partition_id)
                 DO UPDATE SET pending_offset = EXCLUDED.pending_offset, metadata = EXCLUDED.metadata",
                None,
                &[
                    transactional_id.into(),
                    group_id.into(),
                    topic_id.into(),
                    partition_id.into(),
                    offset.into(),
                    metadata.into(),
                ],
            )?;
            Ok(())
        })
        .map_err(|e: KafkaError| {
            KafkaError::Internal(format!("store_txn_pending_offset failed: {}", e))
        })
    }

    pub(super) fn commit_transaction(
        &self,
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
    ) -> Result<()> {
        self.end_transaction(transactional_id, producer_id, producer_epoch, true)
    }

    pub(super) fn abort_transaction(
        &self,
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
    ) -> Result<()> {
        self.end_transaction(transactional_id, producer_id, producer_epoch, false)
    }

    pub(super) fn get_transaction_state(&self, transactional_id: &str) -> Result<Option<TransactionState>> {
        crate::pg_debug!(
            "PostgresStore::get_transaction_state: transactional_id={}",
            transactional_id
        );

        Spi::connect(|client| {
            let mut table = client.select(
                "SELECT state FROM kafka.transactions WHERE transactional_id = $1",
                Some(1),
                &[transactional_id.into()],
            )?;

            if let Some(row) = table.next() {
                let state_str: String = row.get_by_name("state")?.unwrap_or_default();
                Ok(TransactionState::parse(&state_str))
            } else {
                Ok(None)
            }
        })
        .map_err(|e: KafkaError| {
            KafkaError::Internal(format!("get_transaction_state failed: {}", e))
        })
    }
}

/// Wave 3: a new transaction starts with an empty partition registry. Called right
/// before a transactional id moves from a finished state (Empty, CompleteCommit,
/// CompleteAbort) to Ongoing; it deletes nothing for an Ongoing transaction or for a
/// producer that does not own the transactional id.
fn clear_finished_txn_partitions(
    client: &mut pgrx::spi::SpiClient<'_>,
    transactional_id: &str,
    producer_id: i64,
    producer_epoch: i16,
) -> std::result::Result<(), pgrx::spi::SpiError> {
    client.update(
        "DELETE FROM kafka.txn_partitions p
         USING kafka.transactions t
         WHERE p.transactional_id = $1 AND t.transactional_id = $1
           AND t.producer_id = $2 AND t.producer_epoch = $3
           AND t.state IN ('Empty', 'CompleteCommit', 'CompleteAbort')",
        None,
        &[
            transactional_id.into(),
            producer_id.into(),
            producer_epoch.into(),
        ],
    )?;
    Ok(())
}
