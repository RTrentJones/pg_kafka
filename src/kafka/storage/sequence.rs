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

/// The sequence expected after `last_sequence` (wrapping at `i32::MAX`); 0 when the
/// partition has no state yet.
pub fn next_sequence(last_sequence: Option<i32>) -> i32 {
    match last_sequence {
        None | Some(i32::MAX) => 0,
        Some(last) => last + 1,
    }
}

/// The partition's last sequence as stored in `kafka.producer_sequences`: a negative
/// value records no sequence.
pub fn stored_last_sequence(stored: Option<i32>) -> Option<i32> {
    stored.filter(|s| *s >= 0)
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
/// (`ProducerStateManager`/`ProducerAppendInfo`):
///
/// * a lower epoch than `current_epoch` is stale (INVALID_PRODUCER_EPOCH); a higher
///   one (a client-side epoch bump) must start at sequence 0;
/// * at the current epoch, a batch whose exact `[first, last]` sequence range is one of
///   the `cached` (last [`PRODUCER_BATCH_CACHE`]) batches is a duplicate and keeps its
///   original offset; otherwise it must start at the next sequence (0 on a partition
///   without state), anything else is out of order;
/// * sequences wrap at `i32::MAX` ([`batch_last_sequence`]).
///
/// `last_sequence` is the partition's last sequence at `current_epoch` (`None`: no
/// state). A negative base sequence or record count is malformed and rejected as out
/// of order (RV-12). Deviation from Kafka: a producer without state on a partition
/// must start at 0 (Kafka accepts any first sequence there), which only refuses
/// batches whose predecessors never arrived.
pub fn validate_sequence(
    current_epoch: i16,
    last_sequence: Option<i32>,
    cached: &[CachedBatch],
    producer_epoch: i16,
    base_sequence: i32,
    record_count: i32,
) -> SequenceValidation {
    let expected = next_sequence(last_sequence);
    if base_sequence < 0 || record_count < 0 {
        return SequenceValidation::OutOfOrder {
            expected_sequence: expected,
        };
    }
    let last = batch_last_sequence(base_sequence, record_count);
    if producer_epoch < current_epoch {
        return SequenceValidation::StaleEpoch { current_epoch };
    }
    if producer_epoch > current_epoch {
        return if base_sequence == 0 {
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
    if base_sequence == expected {
        SequenceValidation::Proceed {
            new_last_sequence: last,
            new_epoch: false,
        }
    } else {
        SequenceValidation::OutOfOrder {
            expected_sequence: expected,
        }
    }
}

/// Validate the batch of an idempotent producer the broker holds no state for (its
/// state was pruned after idling, lost with the extension, or never existed here).
/// Kafka (KIP-360) accepts it at any sequence and tracks the producer from there; only a
/// negative sequence or record count is out of order.
pub fn validate_unknown_producer(base_sequence: i32, record_count: i32) -> SequenceValidation {
    if base_sequence < 0 || record_count < 0 {
        return SequenceValidation::OutOfOrder {
            expected_sequence: 0,
        };
    }
    SequenceValidation::Proceed {
        new_last_sequence: batch_last_sequence(base_sequence, record_count),
        new_epoch: false,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        batch_last_sequence, stored_last_sequence, transactional_epoch_is_stale, validate_sequence,
        validate_unknown_producer, CachedBatch, SequenceValidation,
    };

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
        // No state on the partition: a batch starting at 0 proceeds.
        assert_eq!(validate_sequence(0, None, &[], 0, 0, 5), proceed(4));
        // ... and any other first sequence is out of order (stricter than Kafka).
        assert_eq!(validate_sequence(0, None, &[], 0, 5, 1), ooo(0));
    }

    #[test]
    fn test_validate_sequence_next_batch_proceeds() {
        assert_eq!(validate_sequence(0, Some(4), &[], 0, 5, 3), proceed(7));
    }

    #[test]
    fn test_validate_sequence_retry_of_a_cached_batch_keeps_its_offset() {
        // A retry of ANY of the remembered batches (not only the last one) is a
        // duplicate and carries the original base offset (was: no offset, -1).
        assert_eq!(
            validate_sequence(0, Some(7), &cache(), 0, 5, 3),
            SequenceValidation::Duplicate { base_offset: 20 }
        );
        assert_eq!(
            validate_sequence(0, Some(7), &cache(), 0, 0, 5),
            SequenceValidation::Duplicate { base_offset: 10 }
        );
    }

    #[test]
    fn test_validate_sequence_never_sent_range_is_out_of_order() {
        // A range at or below the last sequence that is NOT a remembered batch was
        // never appended: acking it as a duplicate would drop it silently.
        assert_eq!(validate_sequence(0, Some(7), &cache(), 0, 1, 1), ooo(8));
        assert_eq!(validate_sequence(0, Some(7), &cache(), 0, 7, 1), ooo(8));
        // A range older than the cache is out of order too (Kafka: same).
        assert_eq!(validate_sequence(0, Some(7), &[], 0, 0, 5), ooo(8));
    }

    #[test]
    fn test_validate_sequence_gap_is_out_of_order() {
        assert_eq!(validate_sequence(0, Some(7), &cache(), 0, 10, 3), ooo(8));
    }

    #[test]
    fn test_validate_sequence_partial_overlap_is_out_of_order() {
        // last_sequence = 7, batch covers 5..=12: records 8..=12 were never
        // persisted, so treating this as a duplicate would silently lose them
        assert_eq!(validate_sequence(0, Some(7), &cache(), 0, 5, 8), ooo(8));
        assert_eq!(validate_sequence(0, Some(7), &cache(), 0, 7, 2), ooo(8));
    }

    #[test]
    fn test_validate_sequence_negative_base_is_rejected() {
        // RV-12: a negative base_sequence must be rejected (out of order), not
        // misclassified as a Duplicate and silently dropped.
        assert_eq!(validate_sequence(0, None, &[], 0, -1, 1), ooo(0));
        assert_eq!(validate_sequence(0, Some(7), &cache(), 0, -5, 3), ooo(8));
        // A negative record_count is likewise rejected.
        assert_eq!(validate_sequence(0, Some(7), &cache(), 0, 8, -1), ooo(8));
    }

    #[test]
    fn test_validate_sequence_epochs() {
        // A lower epoch is stale (INVALID_PRODUCER_EPOCH), whatever the sequence.
        assert_eq!(
            validate_sequence(3, Some(7), &cache(), 2, 8, 1),
            SequenceValidation::StaleEpoch { current_epoch: 3 }
        );
        // A higher (client-bumped) epoch must restart at 0 ...
        assert_eq!(
            validate_sequence(3, Some(7), &cache(), 4, 0, 2),
            SequenceValidation::Proceed {
                new_last_sequence: 1,
                new_epoch: true
            }
        );
        // ... anything else is out of order, and old-epoch batches are not duplicates.
        assert_eq!(validate_sequence(3, Some(7), &cache(), 4, 5, 3), ooo(0));
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
            validate_sequence(0, Some(i32::MAX), &[], 0, 0, 3),
            proceed(2)
        );
        // A batch crossing i32::MAX is appended with its wrapped last sequence.
        assert_eq!(
            validate_sequence(0, Some(i32::MAX - 2), &[], 0, i32::MAX - 1, 4),
            proceed(1)
        );
        // ... and its retry is recognised.
        let wrapped = [CachedBatch {
            first_sequence: i32::MAX - 1,
            last_sequence: 1,
            base_offset: 99,
        }];
        assert_eq!(
            validate_sequence(0, Some(1), &wrapped, 0, i32::MAX - 1, 4),
            SequenceValidation::Duplicate { base_offset: 99 }
        );
    }

    #[test]
    fn test_stored_last_sequence_negative_is_none() {
        assert_eq!(stored_last_sequence(None), None);
        assert_eq!(stored_last_sequence(Some(-1)), None);
        assert_eq!(stored_last_sequence(Some(0)), Some(0));
        assert_eq!(stored_last_sequence(Some(i32::MAX)), Some(i32::MAX));
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
