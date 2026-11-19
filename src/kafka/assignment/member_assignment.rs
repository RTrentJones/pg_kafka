//! MemberAssignment parsing and encoding
//!
//! The MemberAssignment is returned to consumers in the SyncGroup response.
//! It contains the partition assignments computed by the coordinator.
//!
//! # Wire Format (Kafka ConsumerProtocolAssignment)
//!
//! ```text
//! version: i16
//! topic_partitions: [TopicPartition]
//!   - array_length: i32
//!   - for each topic:
//!     - topic_name:
//!       - string_length: i16
//!       - string_bytes: [u8]
//!     - partitions: [i32]
//!       - array_length: i32
//!       - partition_ids: [i32]
//! user_data: bytes
//!   - length: i32 (-1 for null)
//!   - data: [u8] (if length >= 0)
//! ```

use std::collections::HashMap;

use bytes::{Buf, BufMut};

use super::wire;
use crate::kafka::constants::MAX_PREALLOC_ELEMENTS;
use crate::kafka::error::{KafkaError, Result};

/// Partition assignment for a consumer member
///
/// This is returned in the SyncGroup response to tell each consumer
/// which partitions it should consume from.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MemberAssignment {
    /// Protocol version
    pub version: i16,

    /// Assigned partitions per topic: topic name -> partition ids
    pub topic_partitions: HashMap<String, Vec<i32>>,

    /// Optional user-defined data (passed through unchanged)
    pub user_data: Option<Vec<u8>>,
}

impl MemberAssignment {
    /// Create a new empty assignment
    pub fn new() -> Self {
        Self::default()
    }

    /// Create assignment with specific topic-partitions
    pub fn with_partitions(topic_partitions: HashMap<String, Vec<i32>>) -> Self {
        Self {
            version: 0,
            topic_partitions,
            user_data: None,
        }
    }

    /// Get partitions for a specific topic
    pub fn partitions(&self, topic: &str) -> Vec<i32> {
        self.topic_partitions
            .get(topic)
            .cloned()
            .unwrap_or_default()
    }

    /// Check if this assignment is empty (no partitions assigned)
    pub fn is_empty(&self) -> bool {
        self.topic_partitions.is_empty() || self.topic_partitions.values().all(Vec::is_empty)
    }

    /// Total number of partitions assigned
    pub fn partition_count(&self) -> usize {
        self.topic_partitions.values().map(Vec::len).sum()
    }

    /// Parse a MemberAssignment from raw bytes
    ///
    /// # Arguments
    /// * `bytes` - Raw bytes from SyncGroup assignment field
    ///
    /// # Returns
    /// Parsed assignment or error if format is invalid
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        if bytes.is_empty() {
            return Ok(Self::default());
        }

        let mut buf = bytes;

        // Check minimum size: version(2) + array_length(4) = 6 bytes
        if buf.remaining() < 6 {
            return Err(KafkaError::CorruptMessage {
                message: format!(
                    "MemberAssignment too short: {} bytes (need at least 6)",
                    buf.remaining()
                ),
            });
        }

        // Read version
        let version = buf.get_i16();

        // Read topic_partitions array
        let topics_len = buf.get_i32();
        let topics_len = usize::try_from(topics_len).map_err(|_| KafkaError::CorruptMessage {
            message: format!("Invalid topic_partitions array length: {topics_len}"),
        })?;

        // Cap the pre-allocation hint: `topics_len` is attacker-controlled (see
        // MAX_PREALLOC_ELEMENTS). The per-element bounds checks below still allow an
        // honestly-large array to parse; only the up-front allocation is bounded.
        let mut topic_partitions = HashMap::with_capacity(topics_len.min(MAX_PREALLOC_ELEMENTS));

        for _ in 0..topics_len {
            // Read topic name
            if buf.remaining() < 2 {
                return Err(KafkaError::CorruptMessage {
                    message: "Unexpected end of data reading topic name length".into(),
                });
            }

            // A null (negative-length) topic name is skipped.
            let Ok(str_len) = usize::try_from(buf.get_i16()) else {
                continue;
            };
            if buf.remaining() < str_len {
                return Err(KafkaError::CorruptMessage {
                    message: format!(
                        "Topic name length {} exceeds remaining data {}",
                        str_len,
                        buf.remaining()
                    ),
                });
            }

            let mut str_bytes = vec![0u8; str_len];
            buf.copy_to_slice(&mut str_bytes);

            let topic_name =
                String::from_utf8(str_bytes).map_err(|e| KafkaError::CorruptMessage {
                    message: format!("Invalid UTF-8 in topic name: {e}"),
                })?;

            // Read partitions array
            if buf.remaining() < 4 {
                return Err(KafkaError::CorruptMessage {
                    message: "Unexpected end of data reading partitions array length".into(),
                });
            }

            // A null (negative-length) partitions array is an empty assignment.
            let Ok(partitions_len) = usize::try_from(buf.get_i32()) else {
                topic_partitions.insert(topic_name, Vec::new());
                continue;
            };
            if buf.remaining() < partitions_len * 4 {
                return Err(KafkaError::CorruptMessage {
                    message: format!(
                        "Partitions array length {} exceeds remaining data {}",
                        partitions_len * 4,
                        buf.remaining()
                    ),
                });
            }

            let mut partitions = Vec::with_capacity(partitions_len.min(MAX_PREALLOC_ELEMENTS));
            for _ in 0..partitions_len {
                partitions.push(buf.get_i32());
            }

            topic_partitions.insert(topic_name, partitions);
        }

        // Read user_data (optional)
        let user_data = wire::get_trailing_user_data(&mut buf);

        Ok(Self {
            version,
            topic_partitions,
            user_data,
        })
    }

    /// Encode this assignment to bytes for wire format
    ///
    /// # Returns
    /// Encoded bytes for the SyncGroup response's assignment field, or `Internal` if a length
    /// does not fit its prefix
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(128);
        buf.put_i16(self.version);

        // Topics and their partitions are sorted so the encoding is deterministic.
        let mut topics: Vec<_> = self.topic_partitions.iter().collect();
        topics.sort_by_key(|(name, _)| *name);
        wire::put_i32_len(&mut buf, topics.len(), "topic_partitions array")?;
        for (topic_name, partitions) in topics {
            wire::put_string(&mut buf, topic_name)?;
            let mut sorted_partitions = partitions.clone();
            sorted_partitions.sort_unstable();
            wire::put_i32_len(&mut buf, sorted_partitions.len(), "partitions array")?;
            for partition in sorted_partitions {
                buf.put_i32(partition);
            }
        }

        wire::put_nullable_bytes(&mut buf, self.user_data.as_deref())?;
        Ok(buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_empty() {
        let assign = MemberAssignment::parse(&[]).unwrap();
        assert_eq!(assign.version, 0);
        assert!(assign.topic_partitions.is_empty());
        assert!(assign.user_data.is_none());
    }

    #[test]
    fn test_parse_huge_topics_len_does_not_oom() {
        // A huge declared topic_partitions length must not be pre-allocated (a multi-GB
        // HashMap would abort the process); the capacity is capped at MAX_PREALLOC_ELEMENTS
        // and the missing data is an error. An OOM here means the cap is gone.
        let mut buf = Vec::new();
        buf.extend_from_slice(&0i16.to_be_bytes()); // version
        buf.extend_from_slice(&i32::MAX.to_be_bytes()); // topic_partitions len = 2.1B, no data follows
        assert!(MemberAssignment::parse(&buf).is_err());
    }

    #[test]
    fn test_parse_single_topic() {
        // version=0, topics=[("test-topic", [0, 1, 2])], user_data=null
        let mut bytes = Vec::new();
        bytes.put_i16(0); // version
        bytes.put_i32(1); // topics count
        bytes.put_i16(10); // "test-topic" length
        bytes.put_slice(b"test-topic");
        bytes.put_i32(3); // partitions count
        bytes.put_i32(0);
        bytes.put_i32(1);
        bytes.put_i32(2);
        bytes.put_i32(-1); // null user_data

        let assign = MemberAssignment::parse(&bytes).unwrap();
        assert_eq!(assign.version, 0);
        assert_eq!(assign.partitions("test-topic"), vec![0, 1, 2]);
        assert!(assign.user_data.is_none());
    }

    #[test]
    fn test_parse_multiple_topics() {
        let mut bytes = Vec::new();
        bytes.put_i16(1); // version
        bytes.put_i32(2); // topics count

        // Topic A
        bytes.put_i16(7);
        bytes.put_slice(b"topic-a");
        bytes.put_i32(2);
        bytes.put_i32(0);
        bytes.put_i32(1);

        // Topic B
        bytes.put_i16(7);
        bytes.put_slice(b"topic-b");
        bytes.put_i32(1);
        bytes.put_i32(2);

        bytes.put_i32(-1); // null user_data

        let assign = MemberAssignment::parse(&bytes).unwrap();
        assert_eq!(assign.version, 1);
        assert_eq!(assign.partitions("topic-a"), vec![0, 1]);
        assert_eq!(assign.partitions("topic-b"), vec![2]);
    }

    #[test]
    fn test_parse_with_user_data() {
        let mut bytes = Vec::new();
        bytes.put_i16(0);
        bytes.put_i32(1);
        bytes.put_i16(5);
        bytes.put_slice(b"topic");
        bytes.put_i32(1);
        bytes.put_i32(0);
        bytes.put_i32(4); // user_data length
        bytes.put_slice(b"data");

        let assign = MemberAssignment::parse(&bytes).unwrap();
        assert_eq!(assign.user_data, Some(b"data".to_vec()));
    }

    #[test]
    fn test_encode_decode_roundtrip() {
        let mut topic_partitions = HashMap::new();
        topic_partitions.insert("topic-1".to_string(), vec![0, 2, 4]);
        topic_partitions.insert("topic-2".to_string(), vec![1, 3]);

        let original = MemberAssignment {
            version: 1,
            topic_partitions,
            user_data: Some(vec![1, 2, 3, 4]),
        };

        let encoded = original.encode().unwrap();
        let decoded = MemberAssignment::parse(&encoded).unwrap();

        assert_eq!(original.version, decoded.version);
        assert_eq!(original.user_data, decoded.user_data);

        // Check partitions (may be sorted differently but same content)
        for (topic, partitions) in &original.topic_partitions {
            let decoded_partitions = decoded.partitions(topic);
            let mut orig_sorted = partitions.clone();
            orig_sorted.sort_unstable();
            assert_eq!(orig_sorted, decoded_partitions);
        }
    }

    #[test]
    fn test_encode_empty_assignment() {
        let assign = MemberAssignment::default();
        let encoded = assign.encode().unwrap();
        let decoded = MemberAssignment::parse(&encoded).unwrap();

        assert!(decoded.is_empty());
    }

    #[test]
    fn test_is_empty() {
        let empty = MemberAssignment::default();
        assert!(empty.is_empty());

        let mut with_empty_partitions = MemberAssignment::default();
        with_empty_partitions
            .topic_partitions
            .insert("topic".to_string(), Vec::new());
        assert!(with_empty_partitions.is_empty());

        let mut with_partitions = MemberAssignment::default();
        with_partitions
            .topic_partitions
            .insert("topic".to_string(), vec![0]);
        assert!(!with_partitions.is_empty());
    }

    #[test]
    fn test_partition_count() {
        let mut assign = MemberAssignment::default();
        assert_eq!(assign.partition_count(), 0);

        assign
            .topic_partitions
            .insert("topic-a".to_string(), vec![0, 1, 2]);
        assert_eq!(assign.partition_count(), 3);

        assign
            .topic_partitions
            .insert("topic-b".to_string(), vec![0, 1]);
        assert_eq!(assign.partition_count(), 5);
    }

    #[test]
    fn test_parse_too_short() {
        let bytes = vec![0, 0, 0]; // Only 3 bytes, need at least 6
        let result = MemberAssignment::parse(&bytes);
        assert!(result.is_err());
    }
}
