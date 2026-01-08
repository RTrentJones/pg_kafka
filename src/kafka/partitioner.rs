//! Kafka-compatible partitioner
//!
//! Implements partition selection for ProduceRequests when the client
//! doesn't specify an explicit partition (partition_index = -1).
//!
//! Uses the `murmur2` crate with `KAFKA_SEED` for hash-based routing,
//! matching Apache Kafka's default partitioner behavior.

use murmur2::{murmur2, KAFKA_SEED};
use rand::Rng;

/// Compute the target partition for a record.
///
/// # Arguments
/// * `key` - Optional message key (if Some, used for hash-based routing)
/// * `partition_count` - Number of partitions for the topic (must be > 0)
/// * `explicit_partition` - Client-specified partition (-1 means unspecified)
///
/// # Returns
/// Target partition index (0 to partition_count-1)
///
/// # Behavior
/// - If `explicit_partition >= 0`: returns the explicit partition (pass-through)
/// - If key is Some: uses murmur2 hash for deterministic routing
/// - If key is None: uses random partition selection
///
/// # Note on Kafka compatibility
/// For null keys, Kafka uses a "sticky partitioner" that batches records
/// to the same partition until a batch completes. This implementation uses
/// random selection instead, which provides similar distribution but without
/// batch affinity. This deviation is documented in PROTOCOL_DEVIATIONS.md.
pub fn compute_partition(key: Option<&[u8]>, partition_count: i32, explicit_partition: i32) -> i32 {
    debug_assert!(partition_count > 0, "partition_count must be positive");

    if explicit_partition >= 0 {
        // Client explicitly specified partition - pass through
        return explicit_partition;
    }

    match key {
        Some(k) => {
            // Hash-based routing using Kafka-compatible murmur2
            let hash = murmur2(k, KAFKA_SEED);
            // Mask sign bit (& 0x7fffffff) then modulo
            // This matches Kafka's Utils.toPositive(Utils.murmur2(key)) % numPartitions
            ((hash & 0x7fffffff) as i32) % partition_count
        }
        None => {
            // Random partition for null keys
            let mut rng = rand::thread_rng();
            rng.gen_range(0..partition_count)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_explicit_partition_passes_through() {
        // Explicit partition should be returned regardless of key
        assert_eq!(compute_partition(Some(b"key"), 10, 5), 5);
        assert_eq!(compute_partition(None, 10, 3), 3);
        assert_eq!(compute_partition(Some(b"any-key"), 100, 99), 99);
    }

    #[test]
    fn test_key_based_routing_is_deterministic() {
        let key = b"test-key";
        let partition_count = 10;

        // Same key should always produce same partition
        let partition1 = compute_partition(Some(key), partition_count, -1);
        let partition2 = compute_partition(Some(key), partition_count, -1);
        let partition3 = compute_partition(Some(key), partition_count, -1);

        assert_eq!(partition1, partition2);
        assert_eq!(partition2, partition3);

        // Partition should be within valid range
        assert!(partition1 >= 0 && partition1 < partition_count);
    }

    #[test]
    fn test_different_keys_can_produce_different_partitions() {
        let partition_count = 100;

        // With enough different keys, we should see different partitions
        let mut seen_partitions = std::collections::HashSet::new();
        for i in 0..1000 {
            let key = format!("key-{}", i);
            let partition = compute_partition(Some(key.as_bytes()), partition_count, -1);
            seen_partitions.insert(partition);
        }

        // Should have seen multiple different partitions
        assert!(
            seen_partitions.len() > 1,
            "Expected keys to distribute across partitions"
        );
    }

    #[test]
    fn test_null_key_produces_valid_partition() {
        let partition_count = 10;

        // Null key should produce valid partition in range
        for _ in 0..100 {
            let partition = compute_partition(None, partition_count, -1);
            assert!(
                partition >= 0 && partition < partition_count,
                "Partition {} out of range [0, {})",
                partition,
                partition_count
            );
        }
    }

    #[test]
    fn test_null_key_distributes_randomly() {
        let partition_count = 10;

        // With random selection, we should see multiple partitions
        let mut seen_partitions = std::collections::HashSet::new();
        for _ in 0..1000 {
            let partition = compute_partition(None, partition_count, -1);
            seen_partitions.insert(partition);
        }

        // Should have seen at least a few different partitions
        assert!(
            seen_partitions.len() >= 3,
            "Expected null keys to distribute across partitions, saw only {} partitions",
            seen_partitions.len()
        );
    }

    #[test]
    fn test_single_partition_topic() {
        // With only 1 partition, all keys should route to partition 0
        assert_eq!(compute_partition(Some(b"key1"), 1, -1), 0);
        assert_eq!(compute_partition(Some(b"key2"), 1, -1), 0);
        assert_eq!(compute_partition(None, 1, -1), 0);
    }

    #[test]
    fn test_empty_key() {
        let partition_count = 10;

        // Empty key (Some(&[])) should still hash consistently
        let partition1 = compute_partition(Some(b""), partition_count, -1);
        let partition2 = compute_partition(Some(b""), partition_count, -1);
        assert_eq!(partition1, partition2);
        assert!(partition1 >= 0 && partition1 < partition_count);
    }

    #[test]
    fn test_key_distribution_is_reasonably_uniform() {
        // Statistical test: with 10000 messages across 10 partitions,
        // each partition should get roughly 1000 (allow 500-1500 range)
        let partition_count = 10;
        let mut counts = vec![0u32; partition_count as usize];

        for i in 0..10000 {
            let key = format!("unique-key-{}", i);
            let partition = compute_partition(Some(key.as_bytes()), partition_count, -1);
            counts[partition as usize] += 1;
        }

        // Check each partition got a reasonable share
        for (i, &count) in counts.iter().enumerate() {
            assert!(
                count >= 500 && count <= 1500,
                "Partition {} got {} messages, expected ~1000",
                i,
                count
            );
        }
    }
}
