//! Idempotent producer sequence validation (Kafka's producer state model), kept
//! free of SPI so it is unit-testable.

/// How many of a producer's most recent batches per partition are remembered for
/// duplicate detection, as Kafka's `max.in.flight.requests.per.connection` bound for
/// idempotence (5).
pub const PRODUCER_BATCH_CACHE: usize = 5;

/// What the produce path does with an idempotent batch
/// ([`check_and_update_sequence`](super::ProducerStore::check_and_update_sequence)).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SequenceCheck {
    /// Insert the batch, then record it with
    /// [`record_producer_batch`](super::ProducerStore::record_producer_batch).
    Append,
    /// A retry of a batch already appended at `base_offset`: skip the insert and
    /// answer success with that offset.
    Duplicate { base_offset: i64 },
}

/// One appended idempotent batch
/// ([`record_producer_batch`](super::ProducerStore::record_producer_batch)).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerBatch {
    pub producer_id: i64,
    pub topic_id: i32,
    pub partition_id: i32,
    pub first_sequence: i32,
    pub last_sequence: i32,
    pub base_offset: i64,
}

/// A remembered batch of a producer on one partition (sequence range and base offset).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CachedBatch {
    pub first_sequence: i32,
    pub last_sequence: i32,
    pub base_offset: i64,
}

/// Outcome of validating a producer batch against the producer state of a partition
/// ([`validate_sequence`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SequenceValidation {
    /// The batch's exact range is one of the remembered batches: a retry. Skip the
    /// insert and report the original base offset.
    Duplicate { base_offset: i64 },
    /// Append the batch; the partition's last sequence becomes `new_last_sequence`.
    /// `new_epoch`: the batch carries a higher epoch than the stored one (an
    /// idempotent producer bumped its own epoch, KIP-360), so the stored epoch moves
    /// to it and the old epoch's sequence state is discarded first.
    Proceed {
        new_last_sequence: i32,
        new_epoch: bool,
    },
    /// Out of sequence: neither the next sequence nor a remembered batch.
    OutOfOrder { expected_sequence: i32 },
    /// The batch's epoch is older than the producer's current epoch.
    StaleEpoch { current_epoch: i16 },
}

/// The last sequence of a batch of `record_count` records starting at `base_sequence`.
/// Sequences wrap from `i32::MAX` to 0, as Kafka's `DefaultRecordBatch.incrementSequence`.
pub fn batch_last_sequence(base_sequence: i32, record_count: i32) -> i32 {
    let increment = record_count.max(1) - 1;
    if base_sequence > i32::MAX - increment {
        increment - (i32::MAX - base_sequence) - 1
    } else {
        base_sequence + increment
    }
}

/// The sequence expected after `last_sequence` (wrapping at `i32::MAX`); 0 when there
/// is none.
pub fn next_sequence(last_sequence: Option<i32>) -> i32 {
    match last_sequence {
        None | Some(i32::MAX) => 0,
        Some(last) => last + 1,
    }
}

/// A producer's state on one partition, Kafka's per-partition `ProducerStateEntry`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionSequence {
    /// No entry: the producer never wrote to the partition, or its state there was
    /// dropped. Kafka accepts any first sequence.
    NoEntry,
    /// An entry from an earlier epoch (an epoch change resets the producer's entries):
    /// the current epoch starts at sequence 0.
    EpochStart,
    /// The last sequence written at the current epoch.
    Last(i32),
}

impl PartitionSequence {
    /// The state a `kafka.producer_sequences` row records: no row is
    /// [`NoEntry`](Self::NoEntry), a negative last sequence
    /// [`EpochStart`](Self::EpochStart).
    pub fn from_stored(row: Option<i32>) -> Self {
        match row {
            None => Self::NoEntry,
            Some(last) if last < 0 => Self::EpochStart,
            Some(last) => Self::Last(last),
        }
    }

    /// The sequence the partition's next batch must start at; `None` when any is
    /// accepted.
    pub fn expected(self) -> Option<i32> {
        match self {
            Self::NoEntry => None,
            Self::EpochStart => Some(0),
            Self::Last(last) => Some(next_sequence(Some(last))),
        }
    }
}

/// Whether a transactional producer's batch carries a stale epoch. A transactional
/// producer's epoch is owned by its transactional id (InitProducerId); it never bumps
/// it itself, so any epoch but `current_epoch` is stale. An idempotent producer's
/// epoch is judged by [`validate_sequence`] instead.
pub fn transactional_epoch_is_stale(
    transactional: bool,
    producer_epoch: i16,
    current_epoch: i16,
) -> bool {
    transactional && producer_epoch != current_epoch
}

/// Validate an idempotent producer batch, following Kafka's producer state model
/// (`ProducerStateManager`/`ProducerAppendInfo.checkSequence`):
///
/// * a lower epoch than `current_epoch` is stale (INVALID_PRODUCER_EPOCH); a higher
///   one (a client-side epoch bump) must start at sequence 0 on a partition with an
///   entry;
/// * at the current epoch, a batch whose exact `[first, last]` sequence range is one of
///   the `cached` (last [`PRODUCER_BATCH_CACHE`]) batches is a duplicate and keeps its
///   original offset; otherwise it must start at the partition's next sequence
///   ([`PartitionSequence::expected`]), anything else is out of order;
/// * on a partition without an entry ([`PartitionSequence::NoEntry`]) any first
///   sequence is accepted, at the current or a higher epoch;
/// * sequences wrap at `i32::MAX` ([`batch_last_sequence`]).
///
/// A negative base sequence or record count is malformed and rejected as out of order
/// (RV-12). Deviation from Kafka, which keeps the epoch per partition: the epoch is
/// kept per producer, so a lower epoch is stale on a partition without an entry too.
pub fn validate_sequence(
    current_epoch: i16,
    partition: PartitionSequence,
    cached: &[CachedBatch],
    producer_epoch: i16,
    base_sequence: i32,
    record_count: i32,
) -> SequenceValidation {
    let expected = partition.expected();
    if base_sequence < 0 || record_count < 0 {
        return SequenceValidation::OutOfOrder {
            expected_sequence: expected.unwrap_or(0),
        };
    }
    let last = batch_last_sequence(base_sequence, record_count);
    if producer_epoch < current_epoch {
        return SequenceValidation::StaleEpoch { current_epoch };
    }
    if producer_epoch > current_epoch {
        return if base_sequence == 0 || partition == PartitionSequence::NoEntry {
            SequenceValidation::Proceed {
                new_last_sequence: last,
                new_epoch: true,
            }
        } else {
            SequenceValidation::OutOfOrder {
                expected_sequence: 0,
            }
        };
    }
    if let Some(dup) = cached
        .iter()
        .find(|b| b.first_sequence == base_sequence && b.last_sequence == last)
    {
        return SequenceValidation::Duplicate {
            base_offset: dup.base_offset,
        };
    }
    match expected {
        Some(next) if next != base_sequence => SequenceValidation::OutOfOrder {
            expected_sequence: next,
        },
        _ => SequenceValidation::Proceed {
            new_last_sequence: last,
            new_epoch: false,
        },
    }
}

/// Validate the batch of an idempotent producer the broker holds no state for (its
/// state was pruned after idling, lost with the extension, or never existed here).
/// Kafka (KIP-360) accepts it at any sequence and tracks the producer from there; only a
/// negative sequence or record count is out of order.
pub fn validate_unknown_producer(base_sequence: i32, record_count: i32) -> SequenceValidation {
    validate_sequence(
        0,
        PartitionSequence::NoEntry,
        &[],
        0,
        base_sequence,
        record_count,
    )
}

#[cfg(test)]
mod tests {
    use super::{
        batch_last_sequence, transactional_epoch_is_stale, validate_sequence,
        validate_unknown_producer, CachedBatch, PartitionSequence, SequenceValidation,
    };
    use PartitionSequence::{EpochStart, Last, NoEntry};

    fn proceed(last: i32) -> SequenceValidation {
        SequenceValidation::Proceed {
            new_last_sequence: last,
            new_epoch: false,
        }
    }

    fn ooo(expected: i32) -> SequenceValidation {
        SequenceValidation::OutOfOrder {
            expected_sequence: expected,
        }
    }

    /// Two remembered batches: sequences 0..=4 at offset 10 and 5..=7 at offset 20.
    fn cache() -> Vec<CachedBatch> {
        vec![
            CachedBatch {
                first_sequence: 0,
                last_sequence: 4,
                base_offset: 10,
            },
            CachedBatch {
                first_sequence: 5,
                last_sequence: 7,
                base_offset: 20,
            },
        ]
    }

    #[test]
    fn test_validate_sequence_first_batch() {
        // No entry on the partition: any first sequence proceeds, as in Kafka (a known
        // producer on a new partition, or one whose state there was dropped).
        assert_eq!(validate_sequence(0, NoEntry, &[], 0, 0, 5), proceed(4));
        assert_eq!(validate_sequence(0, NoEntry, &[], 0, 5, 1), proceed(5));
        assert_eq!(validate_sequence(3, NoEntry, &[], 3, 80, 20), proceed(99));
        // An entry reset by an epoch change: the epoch starts at 0.
        assert_eq!(validate_sequence(0, EpochStart, &[], 0, 0, 2), proceed(1));
        assert_eq!(validate_sequence(0, EpochStart, &[], 0, 5, 1), ooo(0));
    }

    #[test]
    fn test_validate_sequence_next_batch_proceeds() {
        assert_eq!(validate_sequence(0, Last(4), &[], 0, 5, 3), proceed(7));
    }

    #[test]
    fn test_validate_sequence_retry_of_a_cached_batch_keeps_its_offset() {
        // A retry of ANY of the remembered batches (not only the last one) is a
        // duplicate and carries the original base offset (was: no offset, -1).
        assert_eq!(
            validate_sequence(0, Last(7), &cache(), 0, 5, 3),
            SequenceValidation::Duplicate { base_offset: 20 }
        );
        assert_eq!(
            validate_sequence(0, Last(7), &cache(), 0, 0, 5),
            SequenceValidation::Duplicate { base_offset: 10 }
        );
    }

    #[test]
    fn test_validate_sequence_never_sent_range_is_out_of_order() {
        // A range at or below the last sequence that is NOT a remembered batch was
        // never appended: acking it as a duplicate would drop it silently.
        assert_eq!(validate_sequence(0, Last(7), &cache(), 0, 1, 1), ooo(8));
        assert_eq!(validate_sequence(0, Last(7), &cache(), 0, 7, 1), ooo(8));
        // A range older than the cache is out of order too (Kafka: same).
        assert_eq!(validate_sequence(0, Last(7), &[], 0, 0, 5), ooo(8));
    }

    #[test]
    fn test_validate_sequence_gap_is_out_of_order() {
        assert_eq!(validate_sequence(0, Last(7), &cache(), 0, 10, 3), ooo(8));
    }

    #[test]
    fn test_validate_sequence_partial_overlap_is_out_of_order() {
        // last_sequence = 7, batch covers 5..=12: records 8..=12 were never
        // persisted, so treating this as a duplicate would silently lose them
        assert_eq!(validate_sequence(0, Last(7), &cache(), 0, 5, 8), ooo(8));
        assert_eq!(validate_sequence(0, Last(7), &cache(), 0, 7, 2), ooo(8));
    }

    #[test]
    fn test_validate_sequence_negative_base_is_rejected() {
        // RV-12: a negative base_sequence must be rejected (out of order), not
        // misclassified as a Duplicate and silently dropped.
        assert_eq!(validate_sequence(0, NoEntry, &[], 0, -1, 1), ooo(0));
        assert_eq!(validate_sequence(0, NoEntry, &[], 0, 3, -1), ooo(0));
        assert_eq!(validate_sequence(0, Last(7), &cache(), 0, -5, 3), ooo(8));
        // A negative record_count is likewise rejected.
        assert_eq!(validate_sequence(0, Last(7), &cache(), 0, 8, -1), ooo(8));
    }

    #[test]
    fn test_validate_sequence_epochs() {
        // A lower epoch is stale (INVALID_PRODUCER_EPOCH), whatever the sequence.
        assert_eq!(
            validate_sequence(3, Last(7), &cache(), 2, 8, 1),
            SequenceValidation::StaleEpoch { current_epoch: 3 }
        );
        // A higher (client-bumped) epoch must restart at 0 ...
        assert_eq!(
            validate_sequence(3, Last(7), &cache(), 4, 0, 2),
            SequenceValidation::Proceed {
                new_last_sequence: 1,
                new_epoch: true
            }
        );
        // ... anything else is out of order, and old-epoch batches are not duplicates.
        assert_eq!(validate_sequence(3, Last(7), &cache(), 4, 5, 3), ooo(0));
        assert_eq!(validate_sequence(3, EpochStart, &[], 4, 5, 3), ooo(0));
        // A partition without an entry takes the new epoch at any sequence (Kafka).
        assert_eq!(
            validate_sequence(3, NoEntry, &[], 4, 5, 3),
            SequenceValidation::Proceed {
                new_last_sequence: 7,
                new_epoch: true
            }
        );
        // The epoch is the producer's, not the partition's: a lower one is stale even
        // where the producer has no entry (Kafka accepts it there).
        assert_eq!(
            validate_sequence(3, NoEntry, &[], 2, 0, 1),
            SequenceValidation::StaleEpoch { current_epoch: 3 }
        );
    }

    #[test]
    fn test_validate_sequence_wraps_at_i32_max() {
        // Kafka's incrementSequence: sequences wrap from i32::MAX to 0.
        assert_eq!(batch_last_sequence(i32::MAX - 1, 2), i32::MAX);
        assert_eq!(batch_last_sequence(i32::MAX, 2), 0);
        assert_eq!(batch_last_sequence(i32::MAX - 1, 5), 2);
        assert_eq!(batch_last_sequence(7, 0), 7);
        // The batch after the one ending at i32::MAX starts at 0.
        assert_eq!(
            validate_sequence(0, Last(i32::MAX), &[], 0, 0, 3),
            proceed(2)
        );
        // A batch crossing i32::MAX is appended with its wrapped last sequence.
        assert_eq!(
            validate_sequence(0, Last(i32::MAX - 2), &[], 0, i32::MAX - 1, 4),
            proceed(1)
        );
        // ... and its retry is recognised.
        let wrapped = [CachedBatch {
            first_sequence: i32::MAX - 1,
            last_sequence: 1,
            base_offset: 99,
        }];
        assert_eq!(
            validate_sequence(0, Last(1), &wrapped, 0, i32::MAX - 1, 4),
            SequenceValidation::Duplicate { base_offset: 99 }
        );
    }

    #[test]
    fn test_partition_sequence_from_stored_row() {
        assert_eq!(PartitionSequence::from_stored(None), NoEntry);
        assert_eq!(PartitionSequence::from_stored(Some(-1)), EpochStart);
        assert_eq!(PartitionSequence::from_stored(Some(0)), Last(0));
        assert_eq!(
            PartitionSequence::from_stored(Some(i32::MAX)),
            Last(i32::MAX)
        );
        assert_eq!(NoEntry.expected(), None);
        assert_eq!(EpochStart.expected(), Some(0));
        assert_eq!(Last(4).expected(), Some(5));
        assert_eq!(Last(i32::MAX).expected(), Some(0));
    }

    #[test]
    fn test_transactional_epoch_is_stale() {
        // A transactional producer must present exactly its current epoch ...
        assert!(!transactional_epoch_is_stale(true, 3, 3));
        assert!(transactional_epoch_is_stale(true, 2, 3));
        assert!(transactional_epoch_is_stale(true, 4, 3));
        // ... an idempotent one is left to validate_sequence (it may bump its epoch).
        assert!(!transactional_epoch_is_stale(false, 2, 3));
        assert!(!transactional_epoch_is_stale(false, 4, 3));
    }

    #[test]
    fn an_unknown_producer_is_accepted_at_any_sequence() {
        assert_eq!(validate_unknown_producer(0, 3), proceed(2));
        assert_eq!(validate_unknown_producer(5, 1), proceed(5));
        assert_eq!(validate_unknown_producer(i32::MAX, 2), proceed(0));
        assert_eq!(validate_unknown_producer(-1, 1), ooo(0));
        assert_eq!(validate_unknown_producer(0, -1), ooo(0));
    }
}
