//! Idempotent producer SQL: producer ids, epochs, sequence state and the partitions
//! registered in a transaction.
//!
//! The sequence rules themselves are the pure [`validate_sequence`]; this module reads
//! the state it needs and persists what it decides.

use super::maintenance::PRODUCER_ID_RETENTION;
use super::{internal, internal_unless, select_cached, PostgresStore, INT4OID, INT8OID};
use crate::kafka::constants::ERROR_INVALID_PRODUCER_EPOCH;
use crate::kafka::error::{KafkaError, Result};
use crate::kafka::storage::{
    transactional_epoch_is_stale, validate_sequence, validate_unknown_producer, CachedBatch,
    PartitionSequence, ProducerBatch, SequenceCheck, SequenceValidation, TransactionalProducer,
    PRODUCER_BATCH_CACHE,
};
use pgrx::prelude::*;
use pgrx::spi::{SpiClient, SpiError};
use std::time::Duration;

/// INVALID_PRODUCER_EPOCH: what Kafka 3.7 answers a Produce with a stale epoch, for
/// idempotent and transactional producers alike.
fn invalid_producer_epoch(producer_id: i64, epoch: i16, current_epoch: i16) -> KafkaError {
    KafkaError::Protocol {
        code: ERROR_INVALID_PRODUCER_EPOCH,
        message: format!(
            "producer {producer_id} epoch {epoch} is not the current epoch {current_epoch}"
        ),
    }
}

/// The next producer id from the producer id sequence, or, when a producer already
/// holds that one, the first free id after the run of held ids it starts; the sequence
/// then moves to that id, so later allocations do not walk the run again. An id can be
/// held out of turn: a Produce from a producer the broker has no state for records the
/// id it presents (see [`adopt_unknown_producer`]), up to `i64::MAX`.
fn next_free_producer_id(client: &mut SpiClient<'_>) -> Result<i64> {
    let row = client
        .update(
            "WITH seq AS (
                 SELECT nextval(pg_get_serial_sequence('kafka.producer_ids', 'producer_id')) AS n
             )
             SELECT seq.n AS next,
                    CASE WHEN NOT EXISTS (SELECT 1 FROM kafka.producer_ids WHERE producer_id = seq.n)
                         THEN seq.n
                         -- The held run starting at n ends below the first id p whose
                         -- successor is free; p < i64::MAX keeps p + 1 in range.
                         ELSE (SELECT p.producer_id + 1 FROM kafka.producer_ids p
                               WHERE p.producer_id >= seq.n
                                 AND p.producer_id < 9223372036854775807
                                 AND NOT EXISTS (SELECT 1 FROM kafka.producer_ids q
                                                 WHERE q.producer_id = p.producer_id + 1)
                               ORDER BY p.producer_id
                               LIMIT 1)
                    END AS id
             FROM seq",
            None,
            &[],
        )?
        .first();
    let next: Option<i64> = row.get_by_name("next")?;
    let id: i64 = row
        .get_by_name("id")?
        .ok_or_else(|| KafkaError::Internal("no free producer id".into()))?;
    if next != Some(id) {
        client.update(
            "SELECT setval(pg_get_serial_sequence('kafka.producer_ids', 'producer_id'), $1)",
            None,
            &[id.into()],
        )?;
    }
    Ok(id)
}

/// Insert a new producer id at epoch 0 and return `(producer_id, epoch)`.
pub(super) fn insert_producer_id(
    client: &mut SpiClient<'_>,
    client_id: Option<&str>,
    transactional_id: Option<&str>,
) -> Result<(i64, i16)> {
    let id = next_free_producer_id(client)?;
    let table = client.update(
        "INSERT INTO kafka.producer_ids (producer_id, client_id, transactional_id, epoch)
         VALUES ($1, $2, $3, 0)
         RETURNING producer_id, epoch",
        None,
        &[id.into(), client_id.into(), transactional_id.into()],
    )?;
    let row = table.first();
    let producer_id: i64 = row
        .get_by_name("producer_id")?
        .ok_or_else(|| KafkaError::Internal("Failed to get producer_id".into()))?;
    let epoch: i16 = row.get_by_name("epoch")?.unwrap_or(0);
    Ok((producer_id, epoch))
}

/// Move a producer to its next epoch and return `(producer_id, epoch)`. The previous
/// epoch is forgotten (a bump fences it); see [`PostgresStore::remember_previous_epoch`]. An exhausted
/// epoch (RV-13: `epoch + 1` would overflow the SMALLINT and fail every later
/// re-init) gets a fresh producer id at epoch 0 instead, as in Kafka; the
/// transactions foreign key is ON UPDATE CASCADE, so a transaction row follows the new
/// id.
pub(super) fn bump_producer_epoch(
    client: &mut SpiClient<'_>,
    producer_id: i64,
    old_epoch: i16,
) -> Result<(i64, i16)> {
    let row = if old_epoch == i16::MAX {
        let new_id = next_free_producer_id(client)?;
        // The new id starts without state; the old one's entries name nobody.
        forget_producer_sequences(client, producer_id)?;
        client.update(
            "UPDATE kafka.producer_ids
             SET producer_id = $2, epoch = 0, last_epoch = NULL, last_active_at = NOW()
             WHERE producer_id = $1
             RETURNING producer_id, epoch",
            None,
            &[producer_id.into(), new_id.into()],
        )?
    } else {
        client.update(
            "UPDATE kafka.producer_ids
             SET epoch = epoch + 1, last_epoch = NULL, last_active_at = NOW()
             WHERE producer_id = $1
             RETURNING producer_id, epoch",
            None,
            &[producer_id.into()],
        )?
    }
    .first();
    let new_producer_id: i64 = row.get_by_name("producer_id")?.unwrap_or(producer_id);
    let new_epoch: i16 = row
        .get_by_name("epoch")?
        .ok_or_else(|| KafkaError::Internal("Failed to get new epoch".into()))?;
    Ok((new_producer_id, new_epoch))
}

/// Reset a producer's sequence state on every partition for a new epoch, which
/// restarts sequence numbers at 0: its remembered batches go, and each partition entry
/// stays with no last sequence ([`PartitionSequence::EpochStart`]), so the new epoch's
/// first batch there must start at 0, as in Kafka. Without the reset the new epoch's
/// batches would be judged against the old last sequence and dropped as duplicates.
pub(super) fn reset_producer_sequences(
    client: &mut SpiClient<'_>,
    producer_id: i64,
) -> std::result::Result<(), SpiError> {
    client.update(
        "DELETE FROM kafka.producer_batches WHERE producer_id = $1",
        None,
        &[producer_id.into()],
    )?;
    client.update(
        "UPDATE kafka.producer_sequences SET last_sequence = -1, updated_at = NOW()
         WHERE producer_id = $1",
        None,
        &[producer_id.into()],
    )?;
    Ok(())
}

/// Drop a producer's entries on every partition (and with them, by cascade, its
/// remembered batches): the id no longer names the producer that wrote them, so its
/// next holder starts without state ([`PartitionSequence::NoEntry`]).
fn forget_producer_sequences(
    client: &mut SpiClient<'_>,
    producer_id: i64,
) -> std::result::Result<(), SpiError> {
    client.update(
        "DELETE FROM kafka.producer_sequences WHERE producer_id = $1",
        None,
        &[producer_id.into()],
    )?;
    Ok(())
}

/// Advance a partition's last sequence (upsert into `kafka.producer_sequences`).
fn upsert_last_sequence(
    client: &mut SpiClient<'_>,
    producer_id: i64,
    topic_id: i32,
    partition_id: i32,
    last_sequence: i32,
) -> std::result::Result<(), SpiError> {
    client.update(
        "INSERT INTO kafka.producer_sequences (producer_id, topic_id, partition_id, last_sequence)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (producer_id, topic_id, partition_id) DO UPDATE
             SET last_sequence = EXCLUDED.last_sequence, updated_at = NOW()",
        None,
        &[
            producer_id.into(),
            topic_id.into(),
            partition_id.into(),
            last_sequence.into(),
        ],
    )?;
    Ok(())
}

/// The batch an idempotent Produce asks to append, as far as the sequence check needs it.
struct IdempotentBatch {
    producer_id: i64,
    producer_epoch: i16,
    topic_id: i32,
    partition_id: i32,
    base_sequence: i32,
}

/// A producer's stored epoch and whether it belongs to a transactional id.
struct ProducerState {
    epoch: i16,
    transactional: bool,
}

/// A producer's sequence state on one partition: its entry and the remembered batches
/// (both belong to the current epoch; every epoch change resets them).
struct PartitionSequenceState {
    sequence: PartitionSequence,
    cached: Vec<CachedBatch>,
}

/// How far a producer's `last_active_at` may fall behind its appends: half the
/// expiration window. See [`read_producer_state`].
const PRODUCER_ACTIVITY_REFRESH: Duration =
    Duration::from_secs(PRODUCER_ID_RETENTION.as_secs() / 2);

/// Read a producer's [`ProducerState`] for a batch it appends; None when the broker
/// holds none.
///
/// A producer whose `last_active_at` is more than [`PRODUCER_ACTIVITY_REFRESH`] old has
/// it set to now, through an UPDATE whose RETURNING row is the state (the newest
/// committed version, after any wait for the row). The sweep's producer prune
/// (`maintenance::producer_prune_sql`) takes only producers idle past twice that, and an
/// on-demand prune runs beside the worker: it re-checks `last_active_at` on the row it
/// locks, but reads the sequence rows a Produce writes from its own snapshot. So a
/// producer the prune can take is always one whose Produce writes its producer row
/// first: a prune that reaches the row while that Produce is open skips it (`FOR UPDATE
/// SKIP LOCKED`, never a wait) and keeps the producer; a Produce that reaches it after
/// an uncommitted prune deleted it waits for the prune, finds no row (None), and the
/// batch starts the producer afresh ([`adopt_unknown_producer`]), so what it acks is
/// recorded under a producer row. That wait cannot close a lock cycle: the sweep never
/// waits in the lock manager for a row once it holds one (`run_retention_sweep`, "Lock
/// waits"), so the counter or rows of this batch's earlier requests are never what the
/// sweep is waiting for.
///
/// Cost: one primary-key read per batch, plus one UPDATE of the row (and its
/// idx_producer_ids_last_active entry) at most once per producer per
/// [`PRODUCER_ACTIVITY_REFRESH`] of appends.
fn read_producer_state(
    client: &mut SpiClient<'_>,
    producer_id: i64,
) -> Result<Option<ProducerState>> {
    let table = client.select(
        "SELECT epoch, transactional_id IS NOT NULL AS transactional,
                last_active_at < NOW() - ($2 || ' seconds')::interval AS stale
         FROM kafka.producer_ids
         WHERE producer_id = $1",
        Some(1),
        &[producer_id.into(), refresh_seconds().into()],
    )?;
    if table.is_empty() {
        return Ok(None);
    }
    let row = table.first();
    if !row.get_by_name::<bool, _>("stale")?.unwrap_or(false) {
        return Ok(Some(ProducerState {
            epoch: row.get_by_name("epoch")?.unwrap_or(0),
            transactional: row.get_by_name("transactional")?.unwrap_or(false),
        }));
    }
    let table = client.update(
        "UPDATE kafka.producer_ids SET last_active_at = NOW()
         WHERE producer_id = $1
         RETURNING epoch, transactional_id IS NOT NULL AS transactional",
        Some(1),
        &[producer_id.into()],
    )?;
    if table.is_empty() {
        return Ok(None);
    }
    let row = table.first();
    Ok(Some(ProducerState {
        epoch: row.get_by_name("epoch")?.unwrap_or(0),
        transactional: row.get_by_name("transactional")?.unwrap_or(false),
    }))
}

/// [`PRODUCER_ACTIVITY_REFRESH`] in whole seconds, for SQL interval arithmetic.
fn refresh_seconds() -> i64 {
    i64::try_from(PRODUCER_ACTIVITY_REFRESH.as_secs()).unwrap_or(i64::MAX)
}

/// Record a producer the broker holds no state for at the id and epoch its batch
/// carries, as an idempotent (non-transactional) producer, so its later batches are
/// deduplicated and sequence-checked. Sequence rows left under the id belong to an
/// earlier holder of it and are dropped.
fn adopt_unknown_producer(
    client: &mut SpiClient<'_>,
    producer_id: i64,
    producer_epoch: i16,
) -> Result<()> {
    client.update(
        "INSERT INTO kafka.producer_ids (producer_id, epoch) VALUES ($1, $2)
         ON CONFLICT (producer_id) DO NOTHING",
        None,
        &[producer_id.into(), producer_epoch.into()],
    )?;
    forget_producer_sequences(client, producer_id)?;
    Ok(())
}

/// Read the batch's producer [`PartitionSequenceState`] on its partition.
fn read_partition_sequence_state(
    client: &SpiClient<'_>,
    batch: &IdempotentBatch,
) -> Result<PartitionSequenceState> {
    let args = [
        batch.producer_id.into(),
        batch.topic_id.into(),
        batch.partition_id.into(),
    ];
    let seq_table = client.select(
        "SELECT last_sequence FROM kafka.producer_sequences
         WHERE producer_id = $1 AND topic_id = $2 AND partition_id = $3",
        Some(1),
        &args,
    )?;
    let sequence = PartitionSequence::from_stored(if seq_table.is_empty() {
        None
    } else {
        Some(
            seq_table
                .first()
                .get_by_name::<i32, _>("last_sequence")?
                .unwrap_or(-1),
        )
    });
    let batch_table = client.select(
        "SELECT first_sequence, last_sequence, base_offset FROM kafka.producer_batches
         WHERE producer_id = $1 AND topic_id = $2 AND partition_id = $3",
        None,
        &args,
    )?;
    let mut cached = Vec::new();
    for row in batch_table {
        cached.push(CachedBatch {
            first_sequence: row.get_by_name("first_sequence")?.unwrap_or(-1),
            last_sequence: row.get_by_name("last_sequence")?.unwrap_or(-1),
            base_offset: row.get_by_name("base_offset")?.unwrap_or(-1),
        });
    }
    Ok(PartitionSequenceState { sequence, cached })
}

/// Act on a [`validate_sequence`] outcome: the produce path's [`SequenceCheck`], the
/// error Kafka answers, or, for a client-bumped epoch, adopting that epoch first.
fn apply_sequence_validation(
    client: &mut SpiClient<'_>,
    batch: &IdempotentBatch,
    validation: SequenceValidation,
) -> Result<SequenceCheck> {
    let IdempotentBatch {
        producer_id,
        producer_epoch,
        partition_id,
        base_sequence,
        ..
    } = *batch;
    match validation {
        SequenceValidation::Duplicate { base_offset } => {
            crate::pg_debug!(
                "Duplicate batch: producer_id={producer_id}, partition_id={partition_id}, base_sequence={base_sequence}, original base_offset={base_offset}"
            );
            Ok(SequenceCheck::Duplicate { base_offset })
        }
        SequenceValidation::OutOfOrder { expected_sequence } => {
            Err(KafkaError::out_of_order_sequence(
                producer_id,
                partition_id,
                base_sequence,
                expected_sequence,
            ))
        }
        SequenceValidation::StaleEpoch { current_epoch } => Err(invalid_producer_epoch(
            producer_id,
            producer_epoch,
            current_epoch,
        )),
        SequenceValidation::Proceed { new_epoch, .. } => {
            if new_epoch {
                // KIP-360: an idempotent producer bumped its own epoch (the Java
                // client does after an unresolved sequence error). Adopt it, and
                // reset the old epoch's sequence state on every partition.
                client.update(
                    "UPDATE kafka.producer_ids SET epoch = $2, last_active_at = NOW()
                     WHERE producer_id = $1 AND epoch < $2",
                    None,
                    &[producer_id.into(), producer_epoch.into()],
                )?;
                reset_producer_sequences(client, producer_id)?;
            }
            Ok(SequenceCheck::Append)
        }
    }
}

impl PostgresStore {
    pub(super) fn allocate_producer_id(
        client_id: Option<&str>,
        transactional_id: Option<&str>,
    ) -> Result<(i64, i16)> {
        crate::pg_debug!(
            "PostgresStore::allocate_producer_id: client_id={client_id:?}, transactional_id={transactional_id:?}"
        );

        Spi::connect_mut(|client| {
            let (producer_id, epoch) = insert_producer_id(client, client_id, transactional_id)?;
            crate::pg_debug!("Allocated producer_id={producer_id}, epoch={epoch}");
            Ok((producer_id, epoch))
        })
        .map_err(internal("allocate_producer_id"))
    }

    pub(super) fn check_and_update_sequence(
        producer_id: i64,
        producer_epoch: i16,
        topic_id: i32,
        partition_id: i32,
        base_sequence: i32,
        record_count: i32,
    ) -> Result<SequenceCheck> {
        crate::pg_debug!(
            "PostgresStore::check_and_update_sequence: producer_id={producer_id}, epoch={producer_epoch}, topic_id={topic_id}, partition_id={partition_id}, base_seq={base_sequence}, count={record_count}"
        );
        let batch = IdempotentBatch {
            producer_id,
            producer_epoch,
            topic_id,
            partition_id,
            base_sequence,
        };

        Spi::connect_mut(|client| {
            let Some(producer) = read_producer_state(client, producer_id)? else {
                crate::pg_debug!(
                    "No state for producer_id={producer_id}: accepting it as a new producer at epoch {producer_epoch}"
                );
                adopt_unknown_producer(client, producer_id, producer_epoch)?;
                let validation = validate_unknown_producer(base_sequence, record_count);
                return apply_sequence_validation(client, &batch, validation);
            };
            if transactional_epoch_is_stale(producer.transactional, producer_epoch, producer.epoch)
            {
                return Err(invalid_producer_epoch(
                    producer_id,
                    producer_epoch,
                    producer.epoch,
                ));
            }

            // No lock: producer state is read and written only by the worker's single
            // DB thread, so nothing else can change it between this read and the write.
            let partition = read_partition_sequence_state(client, &batch)?;

            // RV-8: nothing is advanced here; record_producer_batch persists the batch
            // only after a successful insert.
            let validation = validate_sequence(
                producer.epoch,
                partition.sequence,
                &partition.cached,
                producer_epoch,
                base_sequence,
                record_count,
            );
            apply_sequence_validation(client, &batch, validation)
        })
        .map_err(internal_unless("check_and_update_sequence", |e| {
            matches!(
                e,
                KafkaError::UnknownProducerId { .. }
                    | KafkaError::ProducerFenced { .. }
                    | KafkaError::Protocol { .. }
                    | KafkaError::DuplicateSequence { .. }
                    | KafkaError::OutOfOrderSequence { .. }
            )
        }))
    }

    /// RV-8: runs only after the batch's records were inserted (same request
    /// subtransaction). Advances the last sequence, remembers the batch and keeps only
    /// the newest [`PRODUCER_BATCH_CACHE`] batches of the producer on the partition.
    pub(super) fn record_producer_batch(batch: ProducerBatch) -> Result<()> {
        let keep = i64::try_from(PRODUCER_BATCH_CACHE).unwrap_or(i64::MAX);
        Spi::connect_mut(|client| {
            upsert_last_sequence(
                client,
                batch.producer_id,
                batch.topic_id,
                batch.partition_id,
                batch.last_sequence,
            )?;
            client.update(
                "INSERT INTO kafka.producer_batches
                     (producer_id, topic_id, partition_id, first_sequence, last_sequence, base_offset)
                 VALUES ($1, $2, $3, $4, $5, $6)
                 ON CONFLICT (producer_id, topic_id, partition_id, base_offset) DO UPDATE
                     SET first_sequence = EXCLUDED.first_sequence,
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
                           ORDER BY base_offset DESC
                           LIMIT $4) newest)",
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
        .map_err(internal("record_producer_batch"))
    }

    pub(super) fn add_txn_partitions(
        transactional_id: &str,
        partitions: &[(i32, i32)],
    ) -> Result<()> {
        if partitions.is_empty() {
            return Ok(());
        }
        let (topic_ids, partition_ids): (Vec<i32>, Vec<i32>) = partitions.iter().copied().unzip();
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
        .map_err(internal("add_txn_partitions"))
    }

    pub(super) fn txn_has_partition(
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
        .map_err(internal("txn_has_partition"))
    }

    pub(super) fn add_txn_group(transactional_id: &str, group_id: &str) -> Result<()> {
        Spi::connect_mut(|client| {
            client.update(
                "INSERT INTO kafka.txn_groups (transactional_id, group_id) VALUES ($1, $2)
                 ON CONFLICT DO NOTHING",
                None,
                &[transactional_id.into(), group_id.into()],
            )?;
            Ok(())
        })
        .map_err(internal("add_txn_group"))
    }

    pub(super) fn txn_has_group(transactional_id: &str, group_id: &str) -> Result<bool> {
        Spi::connect(|client| {
            let table = client.select(
                "SELECT 1 FROM kafka.txn_groups WHERE transactional_id = $1 AND group_id = $2",
                Some(1),
                &[transactional_id.into(), group_id.into()],
            )?;
            Ok(!table.is_empty())
        })
        .map_err(internal("txn_has_group"))
    }

    /// Every non-transactional idempotent batch asks, so the first statement is the cheap
    /// one: the producer's Ongoing transaction (an index lookup,
    /// `idx_transactions_ongoing_producer`; none for a producer without a transactional
    /// id) that added the partition. Only then are the partition's pending rows looked at.
    pub(super) fn open_txn_epoch_on_partition(
        producer_id: i64,
        topic_id: i32,
        partition_id: i32,
    ) -> Result<Option<i16>> {
        Spi::connect(|client| {
            let open = select_cached(
                client,
                "SELECT t.producer_epoch FROM kafka.transactions t
                 JOIN kafka.txn_partitions p ON p.transactional_id = t.transactional_id
                 WHERE t.producer_id = $1 AND t.state = 'Ongoing'
                   AND p.topic_id = $2 AND p.partition_id = $3",
                &[INT8OID, INT4OID, INT4OID],
                &[producer_id.into(), topic_id.into(), partition_id.into()],
            )?;
            let Some(epoch) = open.into_iter().next().map(|row| row.get::<i16>(1)) else {
                return Ok(None);
            };
            let epoch = epoch?.ok_or_else(|| KafkaError::Database {
                message: "Unexpected NULL in transactions.producer_epoch".to_string(),
            })?;
            let written = select_cached(
                client,
                "SELECT 1 FROM kafka.messages
                 WHERE topic_id = $1 AND partition_id = $2 AND producer_id = $3
                   AND txn_state = 'pending'
                 LIMIT 1",
                &[INT4OID, INT4OID, INT8OID],
                &[topic_id.into(), partition_id.into(), producer_id.into()],
            )?;
            Ok((!written.is_empty()).then_some(epoch))
        })
        .map_err(internal("open_txn_epoch_on_partition"))
    }

    /// RV-8: persist the advanced last sequence, only after the records are inserted
    /// (same request subtransaction).
    pub(super) fn record_producer_sequence(
        producer_id: i64,
        topic_id: i32,
        partition_id: i32,
        last_sequence: i32,
    ) -> Result<()> {
        Spi::connect_mut(|client| {
            upsert_last_sequence(client, producer_id, topic_id, partition_id, last_sequence)?;
            Ok(())
        })
        .map_err(internal("record_producer_sequence"))
    }

    pub(super) fn transactional_producer(
        transactional_id: &str,
    ) -> Result<Option<TransactionalProducer>> {
        Spi::connect(|client| {
            let table = client.select(
                "SELECT producer_id, epoch, last_epoch FROM kafka.producer_ids
                 WHERE transactional_id = $1",
                Some(1),
                &[transactional_id.into()],
            )?;
            if table.is_empty() {
                return Ok(None);
            }
            let row = table.first();
            Ok(Some(TransactionalProducer {
                producer_id: row.get_by_name("producer_id")?.unwrap_or(0),
                epoch: row.get_by_name("epoch")?.unwrap_or(0),
                previous_epoch: row.get_by_name("last_epoch")?,
            }))
        })
        .map_err(internal("transactional_producer"))
    }

    pub(super) fn remember_previous_epoch(producer_id: i64, previous_epoch: i16) -> Result<()> {
        Spi::connect_mut(|client| {
            client.update(
                "UPDATE kafka.producer_ids SET last_epoch = $2 WHERE producer_id = $1",
                None,
                &[producer_id.into(), previous_epoch.into()],
            )?;
            Ok(())
        })
        .map_err(internal("remember_previous_epoch"))
    }
}
