//! Idempotent producer SQL: producer ids, epochs, sequence state and the partitions
//! registered in a transaction.

use super::PostgresStore;
use crate::kafka::error::{KafkaError, Result};
use pgrx::prelude::*;

impl PostgresStore {
    // ===== Idempotent Producer Operations (Phase 9) =====

    pub(super) fn allocate_producer_id(
        &self,
        client_id: Option<&str>,
        transactional_id: Option<&str>,
    ) -> Result<(i64, i16)> {
        crate::pg_debug!(
            "PostgresStore::allocate_producer_id: client_id={:?}, transactional_id={:?}",
            client_id,
            transactional_id
        );

        Spi::connect_mut(|client| {
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

            crate::pg_debug!("Allocated producer_id={}, epoch={}", producer_id, epoch);
            Ok((producer_id, epoch))
        })
        .map_err(|e: KafkaError| {
            KafkaError::Internal(format!("allocate_producer_id failed: {}", e))
        })
    }

    pub(super) fn get_producer_epoch(&self, producer_id: i64) -> Result<Option<i16>> {
        crate::pg_debug!(
            "PostgresStore::get_producer_epoch: producer_id={}",
            producer_id
        );

        Spi::connect(|client| {
            let mut table = client.select(
                "SELECT epoch FROM kafka.producer_ids WHERE producer_id = $1",
                Some(1),
                &[producer_id.into()],
            )?;

            if let Some(row) = table.next() {
                let epoch: i16 = row.get_by_name("epoch")?.unwrap_or(0);
                Ok(Some(epoch))
            } else {
                Ok(None)
            }
        })
        .map_err(|e: KafkaError| KafkaError::Internal(format!("get_producer_epoch failed: {}", e)))
    }

    pub(super) fn increment_producer_epoch(&self, producer_id: i64) -> Result<(i64, i16)> {
        crate::pg_debug!(
            "PostgresStore::increment_producer_epoch: producer_id={}",
            producer_id
        );

        Spi::connect_mut(|client| {
            // Only idempotent (non-transactional) producers are re-initialised by id;
            // a transactional producer's epoch is owned by its transactional_id
            // (get_or_create_transactional_producer), and bumping it here would leave
            // kafka.transactions on the old epoch.
            let current = client.select(
                "SELECT epoch FROM kafka.producer_ids
                 WHERE producer_id = $1 AND transactional_id IS NULL",
                Some(1),
                &[producer_id.into()],
            )?;
            if current.is_empty() {
                return Err(KafkaError::unknown_producer_id(producer_id));
            }
            let old_epoch: i16 = current.first().get_by_name("epoch")?.unwrap_or(0);

            // A new epoch starts the producer's sequence numbers again at 0 on every
            // partition, so the previous epoch's sequence state must go. Otherwise the
            // first batches of the new epoch are judged against the old last_sequence
            // and silently dropped as duplicates.
            client.update(
                "DELETE FROM kafka.producer_sequences WHERE producer_id = $1",
                None,
                &[producer_id.into()],
            )?;

            let table = if old_epoch == i16::MAX {
                // Epoch exhausted (RV-13 for idempotent producers): `epoch + 1` would
                // overflow the SMALLINT and fail every re-init from now on. Like Kafka,
                // hand out a fresh producer_id at epoch 0.
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

            let row = table.first();
            let new_producer_id: i64 = row.get_by_name("producer_id")?.unwrap_or(producer_id);
            let new_epoch: i16 = row
                .get_by_name("epoch")?
                .ok_or_else(|| KafkaError::Internal("Failed to get new epoch".into()))?;

            crate::pg_debug!(
                "Re-initialised producer_id={} -> producer_id={}, epoch={}",
                producer_id,
                new_producer_id,
                new_epoch
            );
            Ok((new_producer_id, new_epoch))
        })
        .map_err(|e| match e {
            KafkaError::UnknownProducerId { .. } => e,
            _ => KafkaError::Internal(format!("increment_producer_epoch failed: {}", e)),
        })
    }

    pub(super) fn check_and_update_sequence(
        &self,
        producer_id: i64,
        producer_epoch: i16,
        topic_id: i32,
        partition_id: i32,
        base_sequence: i32,
        record_count: i32,
    ) -> Result<crate::kafka::storage::SequenceCheck> {
        crate::pg_debug!(
            "PostgresStore::check_and_update_sequence: producer_id={}, epoch={}, topic_id={}, partition_id={}, base_seq={}, count={}",
            producer_id,
            producer_epoch,
            topic_id,
            partition_id,
            base_sequence,
            record_count
        );

        Spi::connect_mut(|client| {
            // Step 1: the producer's current epoch (fencing check).
            let epoch_table = client.select(
                "SELECT epoch, transactional_id IS NOT NULL AS transactional
                 FROM kafka.producer_ids WHERE producer_id = $1",
                Some(1),
                &[producer_id.into()],
            )?;

            if epoch_table.is_empty() {
                return Err(KafkaError::unknown_producer_id(producer_id));
            }

            let row = epoch_table.first();
            let current_epoch: i16 = row.get_by_name("epoch")?.unwrap_or(0);
            let transactional: bool = row.get_by_name("transactional")?.unwrap_or(false);

            // A transactional producer's epoch is owned by its transactional id
            // (InitProducerId); it never bumps it itself, so any other epoch is stale.
            if transactional && producer_epoch != current_epoch {
                return Err(invalid_producer_epoch(producer_id, producer_epoch, current_epoch));
            }

            // Step 2: Use advisory lock to serialize sequence checks for this producer+partition
            // Combine producer_id, topic_id, partition_id into a single bigint lock key
            // Using XOR and shifts to create a unique hash that fits in i64
            let lock_key: i64 = producer_id
                ^ ((topic_id as i64) << 20)
                ^ ((partition_id as i64) << 40);
            client.select(
                "SELECT pg_advisory_xact_lock($1)",
                None,
                &[lock_key.into()],
            )?;

            // Step 3: the partition's producer state at the current epoch: the last
            // sequence and the remembered batches (every epoch change deletes both).
            let seq_table = client.select(
                "SELECT last_sequence FROM kafka.producer_sequences
                 WHERE producer_id = $1 AND topic_id = $2 AND partition_id = $3",
                Some(1),
                &[producer_id.into(), topic_id.into(), partition_id.into()],
            )?;
            let last_sequence: Option<i32> = if seq_table.is_empty() {
                None
            } else {
                seq_table
                    .first()
                    .get_by_name::<i32, _>("last_sequence")?
                    .filter(|s| *s >= 0)
            };
            let batch_table = client.select(
                "SELECT first_sequence, last_sequence, base_offset FROM kafka.producer_batches
                 WHERE producer_id = $1 AND topic_id = $2 AND partition_id = $3",
                None,
                &[producer_id.into(), topic_id.into(), partition_id.into()],
            )?;
            let mut cached = Vec::new();
            for row in batch_table {
                cached.push(crate::kafka::storage::CachedBatch {
                    first_sequence: row.get_by_name("first_sequence")?.unwrap_or(-1),
                    last_sequence: row.get_by_name("last_sequence")?.unwrap_or(-1),
                    base_offset: row.get_by_name("base_offset")?.unwrap_or(-1),
                });
            }

            // Step 4: validate (pure logic in storage::validate_sequence, Kafka's model).
            // RV-8: nothing is advanced here; record_producer_batch persists the batch
            // only after a successful insert.
            match crate::kafka::storage::validate_sequence(
                current_epoch,
                last_sequence,
                &cached,
                producer_epoch,
                base_sequence,
                record_count,
            ) {
                crate::kafka::storage::SequenceValidation::Duplicate { base_offset } => {
                    crate::pg_debug!(
                        "Duplicate batch: producer_id={}, partition_id={}, base_sequence={}, original base_offset={}",
                        producer_id,
                        partition_id,
                        base_sequence,
                        base_offset
                    );
                    Ok(crate::kafka::storage::SequenceCheck::Duplicate { base_offset })
                }
                crate::kafka::storage::SequenceValidation::OutOfOrder { expected_sequence } => {
                    Err(KafkaError::out_of_order_sequence(
                        producer_id,
                        partition_id,
                        base_sequence,
                        expected_sequence,
                    ))
                }
                crate::kafka::storage::SequenceValidation::StaleEpoch { current_epoch } => Err(
                    invalid_producer_epoch(producer_id, producer_epoch, current_epoch),
                ),
                crate::kafka::storage::SequenceValidation::Proceed { new_epoch, .. } => {
                    if new_epoch {
                        // KIP-360: an idempotent producer bumped its own epoch (the Java
                        // client does after an unresolved sequence error). Adopt it, and
                        // drop the old epoch's sequence state on every partition (the
                        // new epoch restarts all of them at 0).
                        client.update(
                            "UPDATE kafka.producer_ids SET epoch = $2, last_active_at = NOW()
                             WHERE producer_id = $1 AND epoch < $2",
                            None,
                            &[producer_id.into(), producer_epoch.into()],
                        )?;
                        client.update(
                            "DELETE FROM kafka.producer_sequences WHERE producer_id = $1",
                            None,
                            &[producer_id.into()],
                        )?;
                    }
                    Ok(crate::kafka::storage::SequenceCheck::Append)
                }
            }
        })
        .map_err(|e| match e {
            KafkaError::UnknownProducerId { .. }
            | KafkaError::ProducerFenced { .. }
            | KafkaError::Protocol { .. }
            | KafkaError::DuplicateSequence { .. }
            | KafkaError::OutOfOrderSequence { .. } => e,
            _ => KafkaError::Internal(format!("check_and_update_sequence failed: {}", e)),
        })
    }

    pub(super) fn record_producer_batch(&self, batch: crate::kafka::storage::ProducerBatch) -> Result<()> {
        // RV-8: called only AFTER the batch's records were inserted (same request
        // subtransaction). Advance the last sequence, remember the batch, and keep only
        // the newest PRODUCER_BATCH_CACHE batches of the producer on the partition.
        let keep = crate::kafka::storage::PRODUCER_BATCH_CACHE as i64;
        Spi::connect_mut(|client| {
            client.update(
                "INSERT INTO kafka.producer_sequences (producer_id, topic_id, partition_id, last_sequence)
                 VALUES ($1, $2, $3, $4)
                 ON CONFLICT (producer_id, topic_id, partition_id)
                 DO UPDATE SET last_sequence = EXCLUDED.last_sequence, updated_at = NOW()",
                None,
                &[
                    batch.producer_id.into(),
                    batch.topic_id.into(),
                    batch.partition_id.into(),
                    batch.last_sequence.into(),
                ],
            )?;
            client.update(
                "INSERT INTO kafka.producer_batches
                     (producer_id, topic_id, partition_id, first_sequence, last_sequence, base_offset)
                 VALUES ($1, $2, $3, $4, $5, $6)
                 ON CONFLICT (producer_id, topic_id, partition_id, base_offset) DO UPDATE SET
                     first_sequence = EXCLUDED.first_sequence,
                     last_sequence = EXCLUDED.last_sequence",
                None,
                &[
                    batch.producer_id.into(),
                    batch.topic_id.into(),
                    batch.partition_id.into(),
                    batch.first_sequence.into(),
                    batch.last_sequence.into(),
                    batch.base_offset.into(),
                ],
            )?;
            client.update(
                "DELETE FROM kafka.producer_batches
                 WHERE producer_id = $1 AND topic_id = $2 AND partition_id = $3
                   AND base_offset < (
                       SELECT MIN(base_offset) FROM (
                           SELECT base_offset FROM kafka.producer_batches
                           WHERE producer_id = $1 AND topic_id = $2 AND partition_id = $3
                           ORDER BY base_offset DESC LIMIT $4) newest)",
                None,
                &[
                    batch.producer_id.into(),
                    batch.topic_id.into(),
                    batch.partition_id.into(),
                    keep.into(),
                ],
            )?;
            Ok(())
        })
        .map_err(|e: KafkaError| {
            KafkaError::Internal(format!("record_producer_batch failed: {}", e))
        })
    }

    pub(super) fn add_txn_partitions(&self, transactional_id: &str, partitions: &[(i32, i32)]) -> Result<()> {
        if partitions.is_empty() {
            return Ok(());
        }
        let topic_ids: Vec<i32> = partitions.iter().map(|p| p.0).collect();
        let partition_ids: Vec<i32> = partitions.iter().map(|p| p.1).collect();
        Spi::connect_mut(|client| {
            client.update(
                "INSERT INTO kafka.txn_partitions (transactional_id, topic_id, partition_id)
                 SELECT $1, t, p FROM unnest($2::int[], $3::int[]) AS u(t, p)
                 ON CONFLICT DO NOTHING",
                None,
                &[
                    transactional_id.into(),
                    topic_ids.into(),
                    partition_ids.into(),
                ],
            )?;
            Ok(())
        })
        .map_err(|e: KafkaError| KafkaError::Internal(format!("add_txn_partitions failed: {}", e)))
    }

    pub(super) fn txn_has_partition(
        &self,
        transactional_id: &str,
        topic_id: i32,
        partition_id: i32,
    ) -> Result<bool> {
        Spi::connect(|client| {
            let table = client.select(
                "SELECT 1 FROM kafka.txn_partitions
                 WHERE transactional_id = $1 AND topic_id = $2 AND partition_id = $3",
                Some(1),
                &[
                    transactional_id.into(),
                    topic_id.into(),
                    partition_id.into(),
                ],
            )?;
            Ok(!table.is_empty())
        })
        .map_err(|e: KafkaError| KafkaError::Internal(format!("txn_has_partition failed: {}", e)))
    }

    pub(super) fn record_producer_sequence(
        &self,
        producer_id: i64,
        topic_id: i32,
        partition_id: i32,
        last_sequence: i32,
    ) -> Result<()> {
        // RV-8: persist the advanced last_sequence, called only AFTER the records
        // are durably inserted (runs in the same request subtransaction).
        Spi::connect_mut(|client| {
            client.update(
                "INSERT INTO kafka.producer_sequences (producer_id, topic_id, partition_id, last_sequence)
                 VALUES ($1, $2, $3, $4)
                 ON CONFLICT (producer_id, topic_id, partition_id)
                 DO UPDATE SET last_sequence = EXCLUDED.last_sequence, updated_at = NOW()",
                None,
                &[
                    producer_id.into(),
                    topic_id.into(),
                    partition_id.into(),
                    last_sequence.into(),
                ],
            )?;
            Ok(())
        })
        .map_err(|e: KafkaError| {
            KafkaError::Internal(format!("record_producer_sequence failed: {}", e))
        })
    }
}

/// INVALID_PRODUCER_EPOCH (47): what Kafka 3.7 answers a Produce with a stale epoch,
/// for idempotent and transactional producers alike (wave 3).
pub(super) fn invalid_producer_epoch(producer_id: i64, epoch: i16, current_epoch: i16) -> KafkaError {
    KafkaError::Protocol {
        code: crate::kafka::constants::ERROR_INVALID_PRODUCER_EPOCH,
        message: format!(
            "producer {} epoch {} is not the current epoch {}",
            producer_id, epoch, current_epoch
        ),
    }
}
