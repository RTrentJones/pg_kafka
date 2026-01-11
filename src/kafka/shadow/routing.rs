//! Shared routing utilities for shadow mode
//!
//! Provides deterministic routing hash computation and forward decision logic
//! used by both ShadowStore and ShadowForwarder.

use super::forwarder::ForwardDecision;
use murmur2::murmur2;

/// Kafka-compatible murmur2 seed (same as Kafka's DefaultPartitioner)
const KAFKA_SEED: u32 = 0x9747b28c;

/// Compute a deterministic routing hash for percentage-based forwarding
///
/// Uses murmur2 hash (same as Kafka partitioner) to ensure consistent routing.
/// If no key is provided or key is empty, uses the offset for deterministic routing.
///
/// # Arguments
/// * `key` - Optional message key (if Some and non-empty, used for hash-based routing)
/// * `offset` - Fallback value when key is None or empty
///
/// # Returns
/// A hash value suitable for modulo-based bucket selection
///
/// # Note on empty key handling
/// Empty keys (`Some(&[])`) are treated as "no key" and fall back to offset routing.
/// This matches the semantics where empty keys typically indicate the producer
/// did not intend to use keyed routing.
pub fn compute_routing_hash(key: Option<&[u8]>, offset: i64) -> u32 {
    match key {
        Some(k) if !k.is_empty() => murmur2(k, KAFKA_SEED),
        _ => {
            // Use offset for deterministic routing when no key is provided.
            // Use unsigned_abs() to handle negative offsets consistently,
            // then mod 100 to keep in percentage range.
            (offset.unsigned_abs() % 100) as u32
        }
    }
}

/// Make a forward decision based on routing hash and percentage threshold
///
/// # Arguments
/// * `key` - Optional message key for routing
/// * `offset` - Message offset (used for routing when key is None/empty)
/// * `forward_percentage` - Percentage of messages to forward (0-100)
///
/// # Returns
/// `ForwardDecision::Forward` if message should be forwarded
/// `ForwardDecision::Skip` if message should be skipped
///
/// # Behavior
/// - 0%: Always skip
/// - 100%: Always forward
/// - 1-99%: Use deterministic hash to decide
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

    let hash = compute_routing_hash(key, offset);
    let bucket = hash % 100;

    if bucket < forward_percentage as u32 {
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
            forward_count >= 200 && forward_count <= 400,
            "Forward count {} should be roughly 30%",
            forward_count
        );
    }
}
