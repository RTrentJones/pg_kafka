// Copyright (c) 2026 Robert Trent Jones.
// This file is part of the "Shadow Mode" feature of pg_kafka.
//
// Use of this source code for production purposes is governed by the
// Commercial License found in the LICENSE file in this directory.
// Development and evaluation use is permitted.
//
// GitHub Sponsors: https://github.com/sponsors/RTrentJones

//! Percentage routing: which records of a forwarding topic go to the external
//! cluster.
//!
//! The decision is a deterministic function of the record: the Kafka murmur2
//! hash of its key, or its local offset when it has no key. So the same key
//! always routes the same way, raising the percentage only adds keys, and no
//! state is shared between the produce path and the transaction-commit path
//! that both decide.

use murmur2::murmur2;

/// Kafka-compatible murmur2 seed (the Java client's default partitioner).
const KAFKA_SEED: u32 = 0x9747_b28c;

/// Whether one record is forwarded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForwardDecision {
    /// Forward the record to the external cluster.
    Forward,
    /// Sampled out: the record stays local only.
    Skip,
}

/// The routing hash of a record, compared against the percentage bucket.
///
/// A non-empty key is hashed with murmur2 (as Kafka's partitioner does). A
/// record without a key, or with an empty one (a producer that did not mean to
/// key it), is routed by its local offset instead, so the decision stays
/// deterministic.
fn compute_routing_hash(key: Option<&[u8]>, offset: i64) -> u32 {
    match key {
        Some(k) if !k.is_empty() => murmur2(k, KAFKA_SEED),
        _ => u32::try_from(offset.unsigned_abs() % 100).expect("a value below 100 fits in u32"),
    }
}

/// Decide whether a record is forwarded at `forward_percentage` (0-100;
/// anything above 100 forwards everything): 0 skips everything, 100 forwards
/// everything, and in between the routing hash picks a stable subset.
pub fn make_forward_decision(
    key: Option<&[u8]>,
    offset: i64,
    forward_percentage: u8,
) -> ForwardDecision {
    if forward_percentage == 0 {
        return ForwardDecision::Skip;
    }
    if forward_percentage >= 100 {
        return ForwardDecision::Forward;
    }
    if compute_routing_hash(key, offset) % 100 < u32::from(forward_percentage) {
        ForwardDecision::Forward
    } else {
        ForwardDecision::Skip
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_routing_hash_with_key() {
        let hash1 = compute_routing_hash(Some(b"key1"), 0);
        let hash2 = compute_routing_hash(Some(b"key1"), 100); // Different offset
        assert_eq!(
            hash1, hash2,
            "Same key should produce same hash regardless of offset"
        );

        let hash3 = compute_routing_hash(Some(b"key2"), 0);
        assert_ne!(
            hash1, hash3,
            "Different keys should produce different hashes"
        );
    }

    #[test]
    fn test_compute_routing_hash_without_key() {
        let hash1 = compute_routing_hash(None, 42);
        let hash2 = compute_routing_hash(None, 42);
        assert_eq!(hash1, hash2, "Same offset should produce same hash");

        // Verify offset-based routing produces values in expected range
        for offset in 0..200i64 {
            let hash = compute_routing_hash(None, offset);
            assert!(hash < 100, "Offset-based hash should be < 100");
        }
    }

    #[test]
    fn test_compute_routing_hash_empty_key_uses_offset() {
        let hash_empty = compute_routing_hash(Some(b""), 42);
        let hash_none = compute_routing_hash(None, 42);
        assert_eq!(hash_empty, hash_none, "Empty key should use offset");
    }

    #[test]
    fn test_compute_routing_hash_negative_offset() {
        // Negative offsets should work consistently
        let hash1 = compute_routing_hash(None, -42);
        let hash2 = compute_routing_hash(None, -42);
        assert_eq!(
            hash1, hash2,
            "Same negative offset should produce same hash"
        );
        assert!(hash1 < 100, "Hash should be < 100");
    }

    #[test]
    fn test_make_forward_decision_boundaries() {
        // 0% - always skip
        assert_eq!(
            make_forward_decision(Some(b"any"), 0, 0),
            ForwardDecision::Skip
        );

        // 100% - always forward
        assert_eq!(
            make_forward_decision(Some(b"any"), 0, 100),
            ForwardDecision::Forward
        );

        // >100% - treated as 100%, always forward
        assert_eq!(
            make_forward_decision(Some(b"any"), 0, 150),
            ForwardDecision::Forward
        );
    }

    #[test]
    fn test_make_forward_decision_deterministic() {
        // Same inputs should always produce same decision
        let key = Some(b"test-key".as_slice());
        let decision1 = make_forward_decision(key, 100, 50);
        let decision2 = make_forward_decision(key, 100, 50);
        assert_eq!(
            decision1, decision2,
            "Same inputs should produce same decision"
        );
    }

    #[test]
    fn test_percentage_distribution() {
        // Statistical test: with 1000 offsets at 30%, roughly 300 should forward
        let mut forward_count = 0;
        for offset in 0..1000i64 {
            if make_forward_decision(None, offset, 30) == ForwardDecision::Forward {
                forward_count += 1;
            }
        }
        // Allow variance (20% to 40% range)
        assert!(
            (200..=400).contains(&forward_count),
            "Forward count {forward_count} should be roughly 30%"
        );
    }
}
