//! Transaction SQL: transactional producers, transaction state, transactional
//! writes and offsets, commit and abort.

use super::log::TxnStamp;
use super::producers::{bump_producer_epoch, insert_producer_id, reset_producer_sequences};
use super::{internal, internal_unless, is_txn_error, PostgresStore};
use crate::kafka::error::{KafkaError, Result};
use crate::kafka::messages::Record;
use crate::kafka::storage::TransactionState;
use pgrx::prelude::*;
use pgrx::spi::{SpiClient, SpiError};

/// How a transaction ends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TxnOutcome {
    Commit,
    Abort,
}

impl TxnOutcome {
    /// The operation name in errors and logs.
    fn operation(self) -> &'static str {
        match self {
            Self::Commit => "commit_transaction",
            Self::Abort => "abort_transaction",
        }
    }
}

/// The owner and state of a transactional id (its `kafka.transactions` row).
struct TxnRow {
    producer_id: i64,
    producer_epoch: i16,
    state: String,
}

impl TxnRow {
    /// Read the row; [`KafkaError::TransactionalIdNotFound`] when the transactional id has none.
    fn load(client: &SpiClient<'_>, transactional_id: &str) -> Result<Self> {
        let table = client.select(
            "SELECT state, producer_id, producer_epoch
             FROM kafka.transactions
             WHERE transactional_id = $1",
            Some(1),
            &[transactional_id.into()],
        )?;
        if table.is_empty() {
            return Err(KafkaError::transactional_id_not_found(transactional_id));
        }
        let row = table.first();
        let column = |name: &str| KafkaError::Database {
            message: format!("Unexpected NULL in transactions.{name} column"),
        };
        Ok(Self {
            producer_id: row
                .get_by_name("producer_id")?
                .ok_or_else(|| column("producer_id"))?,
            producer_epoch: row
                .get_by_name("producer_epoch")?
                .ok_or_else(|| column("producer_epoch"))?,
            state: row.get_by_name("state")?.ok_or_else(|| column("state"))?,
        })
    }

    /// Whether `producer_id`/`producer_epoch` own the transactional id: as in Kafka 3.7's
    /// transaction coordinator, another producer id is INVALID_PRODUCER_ID_MAPPING, whatever
    /// its epoch, and another epoch of the producer id PRODUCER_FENCED.
    fn check_owner(&self, producer_id: i64, producer_epoch: i16) -> Result<()> {
        if self.producer_id != producer_id {
            Err(KafkaError::invalid_producer_id_mapping(producer_id))
        } else if self.producer_epoch == producer_epoch {
            Ok(())
        } else {
            Err(KafkaError::producer_fenced(
                producer_id,
                producer_epoch,
                self.producer_epoch,
            ))
        }
    }
}

/// The states a new transaction may start from.
const STARTABLE_STATES: &str = "Empty, CompleteCommit, or CompleteAbort";

/// A new transaction starts with an empty partition registry. Called right before a
/// transactional id moves from a finished state (Empty, CompleteCommit, CompleteAbort)
/// to Ongoing; it deletes nothing for an Ongoing transaction or for a producer that
/// does not own the transactional id.
fn clear_finished_txn_partitions(
    client: &mut SpiClient<'_>,
    transactional_id: &str,
    producer_id: i64,
    producer_epoch: i16,
) -> std::result::Result<(), SpiError> {
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

/// Re-initialize an existing transactional id: abort what its previous epoch left in
/// flight, then move it to its next epoch (which fences that producer). Returns the
/// new `(producer_id, epoch)`.
///
/// RV-2: InitProducerId aborts any transaction still in flight for the
/// transactional id (the producer crash-restart path). Otherwise the old epoch's
/// pending records would be stranded: commit/abort filter on the new epoch, the
/// timeout sweep only scans Ongoing transactions and cleanup only touches aborted
/// rows, so nothing would reclaim them and the last stable offset would stay pinned
/// (a hanging transaction).
fn fence_previous_epoch(
    client: &mut SpiClient<'_>,
    transactional_id: &str,
    producer_id: i64,
    old_epoch: i16,
) -> Result<(i64, i16)> {
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
    // The aborted transaction's partition registry goes with it.
    client.update(
        "DELETE FROM kafka.txn_partitions WHERE transactional_id = $1",
        None,
        &[transactional_id.into()],
    )?;
    reset_producer_sequences(client, producer_id)?;

    let (new_producer_id, new_epoch) = bump_producer_epoch(client, producer_id, old_epoch)?;
    crate::pg_debug!(
        "Existing transactional producer_id={producer_id} -> producer_id={new_producer_id}, new_epoch={new_epoch}"
    );
    Ok((new_producer_id, new_epoch))
}

/// Point a transactional id's `kafka.transactions` row at `owner` and reset it to
/// Empty with `timeout_ms` (inserting it for a new transactional id).
fn reset_transaction_row(
    client: &mut SpiClient<'_>,
    transactional_id: &str,
    owner: TxnStamp,
    timeout_ms: i32,
) -> Result<()> {
    client.update(
        "INSERT INTO kafka.transactions (transactional_id, producer_id, producer_epoch,
                                         state, timeout_ms)
         VALUES ($1, $2, $3, 'Empty', $4)
         ON CONFLICT (transactional_id) DO UPDATE
             SET producer_id = EXCLUDED.producer_id,
                 producer_epoch = EXCLUDED.producer_epoch,
                 state = 'Empty',
                 timeout_ms = EXCLUDED.timeout_ms,
                 started_at = NULL,
                 last_updated_at = NOW()",
        None,
        &[
            transactional_id.into(),
            owner.producer_id.into(),
            owner.producer_epoch.into(),
            timeout_ms.into(),
        ],
    )?;
    Ok(())
}

impl PostgresStore {
    /// End an Ongoing transaction (shared by commit and abort): its pending rows
    /// become visible (`txn_state = NULL`) or `'aborted'`, a commit moves its pending
    /// offsets into `kafka.consumer_offsets`, the pending offsets are deleted, and the
    /// transaction reaches its terminal state.
    fn end_transaction(
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        outcome: TxnOutcome,
    ) -> Result<()> {
        crate::pg_debug!(
            "PostgresStore::end_transaction ({outcome:?}): transactional_id={transactional_id}, producer_id={producer_id}, epoch={producer_epoch}"
        );

        Spi::connect_mut(|client| {
            let txn = TxnRow::load(client, transactional_id)?;
            txn.check_owner(producer_id, producer_epoch)?;
            if txn.state != "Ongoing" {
                return Err(KafkaError::invalid_txn_state(
                    transactional_id,
                    "Ongoing",
                    &txn.state,
                ));
            }

            match outcome {
                TxnOutcome::Commit => {
                    client.update(
                        "UPDATE kafka.messages SET txn_state = NULL
                         WHERE producer_id = $1 AND producer_epoch = $2 AND txn_state = 'pending'",
                        None,
                        &[producer_id.into(), producer_epoch.into()],
                    )?;
                    client.update(
                        "INSERT INTO kafka.consumer_offsets
                             (group_id, topic_id, partition_id, committed_offset, metadata)
                         SELECT group_id, topic_id, partition_id, pending_offset, metadata
                         FROM kafka.txn_pending_offsets
                         WHERE transactional_id = $1
                         ON CONFLICT (group_id, topic_id, partition_id) DO UPDATE
                             SET committed_offset = EXCLUDED.committed_offset,
                                 metadata = EXCLUDED.metadata,
                                 commit_timestamp = NOW()",
                        None,
                        &[transactional_id.into()],
                    )?;
                }
                TxnOutcome::Abort => {
                    client.update(
                        "UPDATE kafka.messages SET txn_state = 'aborted'
                         WHERE producer_id = $1 AND producer_epoch = $2 AND txn_state = 'pending'",
                        None,
                        &[producer_id.into(), producer_epoch.into()],
                    )?;
                }
            }

            client.update(
                "DELETE FROM kafka.txn_pending_offsets WHERE transactional_id = $1",
                None,
                &[transactional_id.into()],
            )?;
            client.update(
                match outcome {
                    TxnOutcome::Commit => {
                        "UPDATE kafka.transactions
                         SET state = 'CompleteCommit', last_updated_at = NOW()
                         WHERE transactional_id = $1"
                    }
                    TxnOutcome::Abort => {
                        "UPDATE kafka.transactions
                         SET state = 'CompleteAbort', last_updated_at = NOW()
                         WHERE transactional_id = $1"
                    }
                },
                None,
                &[transactional_id.into()],
            )?;

            crate::pg_debug!("Transaction {transactional_id} completed ({outcome:?})");
            Ok(())
        })
        .map_err(internal_unless(outcome.operation(), is_txn_error))
    }

    /// InitProducerId for a transactional id: a new transactional id gets a new
    /// producer id; an existing one gets its next epoch, which fences the previous
    /// producer. Either way its transaction row is reset to Empty.
    pub(super) fn get_or_create_transactional_producer(
        transactional_id: &str,
        transaction_timeout_ms: i32,
        client_id: Option<&str>,
    ) -> Result<(i64, i16)> {
        crate::pg_debug!(
            "PostgresStore::get_or_create_transactional_producer: transactional_id={transactional_id}, timeout_ms={transaction_timeout_ms}"
        );

        Spi::connect_mut(|client| {
            let existing = client.select(
                "SELECT producer_id, epoch FROM kafka.producer_ids WHERE transactional_id = $1",
                Some(1),
                &[transactional_id.into()],
            )?;

            let (producer_id, epoch) = if existing.is_empty() {
                let (producer_id, epoch) =
                    insert_producer_id(client, client_id, Some(transactional_id))?;
                crate::pg_debug!(
                    "Allocated new transactional producer_id={producer_id}, epoch={epoch}"
                );
                (producer_id, epoch)
            } else {
                let row = existing.first();
                let producer_id: i64 = row.get_by_name("producer_id")?.unwrap_or(0);
                let old_epoch: i16 = row.get_by_name("epoch")?.unwrap_or(0);
                fence_previous_epoch(client, transactional_id, producer_id, old_epoch)?
            };

            reset_transaction_row(
                client,
                transactional_id,
                TxnStamp {
                    producer_id,
                    producer_epoch: epoch,
                },
                transaction_timeout_ms,
            )?;
            Ok((producer_id, epoch))
        })
        .map_err(internal("get_or_create_transactional_producer"))
    }

    /// Start a transaction from a finished state (Empty, CompleteCommit,
    /// CompleteAbort).
    pub(super) fn begin_transaction(
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
    ) -> Result<()> {
        crate::pg_debug!(
            "PostgresStore::begin_transaction: transactional_id={transactional_id}, producer_id={producer_id}, epoch={producer_epoch}"
        );

        Spi::connect_mut(|client| {
            clear_finished_txn_partitions(client, transactional_id, producer_id, producer_epoch)?;
            let started = client.update(
                "UPDATE kafka.transactions
                 SET state = 'Ongoing', started_at = NOW(), last_updated_at = NOW()
                 WHERE transactional_id = $1 AND producer_id = $2 AND producer_epoch = $3
                   AND state IN ('Empty', 'CompleteCommit', 'CompleteAbort')
                 RETURNING state",
                None,
                &[
                    transactional_id.into(),
                    producer_id.into(),
                    producer_epoch.into(),
                ],
            )?;
            if started.is_empty() {
                return Err(not_startable(
                    client,
                    transactional_id,
                    producer_id,
                    producer_epoch,
                ));
            }
            crate::pg_debug!("Transaction {transactional_id} started");
            Ok(())
        })
        .map_err(internal_unless("begin_transaction", is_txn_error))
    }

    /// Start a transaction from a finished state or continue the Ongoing one of the
    /// same producer, in one compare-and-swap UPDATE (no check-then-act race). The
    /// timeout clock (`started_at`) starts when the transaction does, as Kafka's
    /// txnStartTimestamp does on Empty/Complete* -> Ongoing; continuing keeps it.
    pub(super) fn begin_or_continue_transaction(
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
    ) -> Result<()> {
        crate::pg_debug!(
            "PostgresStore::begin_or_continue_transaction: transactional_id={transactional_id}, producer_id={producer_id}, epoch={producer_epoch}"
        );

        Spi::connect_mut(|client| {
            clear_finished_txn_partitions(client, transactional_id, producer_id, producer_epoch)?;
            let updated = client.update(
                "UPDATE kafka.transactions
                 SET state = 'Ongoing',
                     started_at = CASE WHEN state = 'Ongoing'
                                       THEN COALESCE(started_at, NOW())
                                       ELSE NOW() END,
                     last_updated_at = NOW()
                 WHERE transactional_id = $1
                   AND producer_id = $2
                   AND producer_epoch = $3
                   AND state IN ('Empty', 'CompleteCommit', 'CompleteAbort', 'Ongoing')
                 RETURNING state",
                None,
                &[
                    transactional_id.into(),
                    producer_id.into(),
                    producer_epoch.into(),
                ],
            )?;
            if updated.is_empty() {
                return Err(not_startable(
                    client,
                    transactional_id,
                    producer_id,
                    producer_epoch,
                ));
            }
            crate::pg_debug!("Transaction {transactional_id} is Ongoing");
            Ok(())
        })
        .map_err(internal_unless(
            "begin_or_continue_transaction",
            is_txn_error,
        ))
    }

    /// Check that `producer_id`/`producer_epoch` own the transactional id. The state is
    /// not checked: the handlers know which states their operation allows.
    pub(super) fn validate_transaction(
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
    ) -> Result<()> {
        crate::pg_debug!(
            "PostgresStore::validate_transaction: transactional_id={transactional_id}, producer_id={producer_id}, epoch={producer_epoch}"
        );

        Spi::connect(|client| {
            TxnRow::load(client, transactional_id)?.check_owner(producer_id, producer_epoch)
        })
        .map_err(internal_unless("validate_transaction", is_txn_error))
    }

    pub(super) fn insert_transactional_records(
        topic_id: i32,
        partition_id: i32,
        records: &[Record],
        producer_id: i64,
        producer_epoch: i16,
    ) -> Result<i64> {
        crate::pg_debug!(
            "PostgresStore::insert_transactional_records: {} records for topic_id={topic_id}, partition_id={partition_id}, producer_id={producer_id}",
            records.len()
        );
        Self::insert_records_inner(
            topic_id,
            partition_id,
            records,
            Some(TxnStamp {
                producer_id,
                producer_epoch,
            }),
        )
    }

    pub(super) fn store_txn_pending_offset(
        transactional_id: &str,
        group_id: &str,
        topic_id: i32,
        partition_id: i32,
        offset: i64,
        metadata: Option<&str>,
    ) -> Result<()> {
        crate::pg_debug!(
            "PostgresStore::store_txn_pending_offset: txn_id={transactional_id}, group_id={group_id}, topic_id={topic_id}, partition_id={partition_id}, offset={offset}"
        );

        Spi::connect_mut(|client| {
            client.update(
                "INSERT INTO kafka.txn_pending_offsets (transactional_id, group_id, topic_id,
                                                        partition_id, pending_offset, metadata)
                 VALUES ($1, $2, $3, $4, $5, $6)
                 ON CONFLICT (transactional_id, group_id, topic_id, partition_id) DO UPDATE
                     SET pending_offset = EXCLUDED.pending_offset, metadata = EXCLUDED.metadata",
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
        .map_err(internal("store_txn_pending_offset"))
    }

    pub(super) fn commit_transaction(
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
    ) -> Result<()> {
        Self::end_transaction(
            transactional_id,
            producer_id,
            producer_epoch,
            TxnOutcome::Commit,
        )
    }

    pub(super) fn abort_transaction(
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
    ) -> Result<()> {
        Self::end_transaction(
            transactional_id,
            producer_id,
            producer_epoch,
            TxnOutcome::Abort,
        )
    }

    pub(super) fn get_transaction_state(
        transactional_id: &str,
    ) -> Result<Option<TransactionState>> {
        crate::pg_debug!(
            "PostgresStore::get_transaction_state: transactional_id={transactional_id}"
        );

        Spi::connect(|client| {
            let mut table = client.select(
                "SELECT state FROM kafka.transactions WHERE transactional_id = $1",
                Some(1),
                &[transactional_id.into()],
            )?;
            let Some(row) = table.next() else {
                return Ok(None);
            };
            let state: String = row.get_by_name("state")?.unwrap_or_default();
            Ok(TransactionState::parse(&state))
        })
        .map_err(internal("get_transaction_state"))
    }
}

/// Why a transaction could not start: the transactional id is unknown, another
/// producer owns it, or it is in a state a transaction cannot start from.
fn not_startable(
    client: &SpiClient<'_>,
    transactional_id: &str,
    producer_id: i64,
    producer_epoch: i16,
) -> KafkaError {
    let txn = match TxnRow::load(client, transactional_id) {
        Ok(txn) => txn,
        Err(e) => return e,
    };
    if let Err(fenced) = txn.check_owner(producer_id, producer_epoch) {
        return fenced;
    }
    KafkaError::invalid_txn_state(transactional_id, STARTABLE_STATES, &txn.state)
}
