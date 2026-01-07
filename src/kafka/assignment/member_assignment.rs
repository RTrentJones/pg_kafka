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

use crate::kafka::error::{KafkaError, Result};

/// Partition assignment for a consumer member
///
/// This is returned in the SyncGroup response to tell each consumer
/// which partitions it should consume from.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MemberAssignment {
    /// Protocol version
    pub version: i16,

    /// Assigned partitions per topic: topic_name -> [partition_ids]
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
        self.topic_partitions.is_empty() || self.topic_partitions.values().all(|p| p.is_empty())
    }

    /// Total number of partitions assigned
    pub fn partition_count(&self) -> usize {
        self.topic_partitions.values().map(|p| p.len()).sum()
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
        if topics_len < 0 {
            return Err(KafkaError::CorruptMessage {
                message: format!("Invalid topic_partitions array length: {}", topics_len),
            });
        }

        let mut topic_partitions = HashMap::with_capacity(topics_len as usize);

        for _ in 0..topics_len {
            // Read topic name
            if buf.remaining() < 2 {
                return Err(KafkaError::CorruptMessage {
                    message: "Unexpected end of data reading topic name length".into(),
                });
            }

            let str_len = buf.get_i16();
            if str_len < 0 {
                continue; // Null topic - skip
            }

            let str_len = str_len as usize;
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
                    message: format!("Invalid UTF-8 in topic name: {}", e),
                })?;

            // Read partitions array
            if buf.remaining() < 4 {
                return Err(KafkaError::CorruptMessage {
                    message: "Unexpected end of data reading partitions array length".into(),
                });
            }

            let partitions_len = buf.get_i32();
            if partitions_len < 0 {
                topic_partitions.insert(topic_name, Vec::new());
                continue;
            }

            let partitions_len = partitions_len as usize;
            if buf.remaining() < partitions_len * 4 {
                return Err(KafkaError::CorruptMessage {
                    message: format!(
                        "Partitions array length {} exceeds remaining data {}",
                        partitions_len * 4,
                        buf.remaining()
                    ),
                });
            }

            let mut partitions = Vec::with_capacity(partitions_len);
            for _ in 0..partitions_len {
                partitions.push(buf.get_i32());
            }

            topic_partitions.insert(topic_name, partitions);
        }

        // Read user_data (optional)
        let user_data = if buf.remaining() >= 4 {
            let data_len = buf.get_i32();
            if data_len < 0 {
                None
            } else if buf.remaining() >= data_len as usize {
                let mut data = vec![0u8; data_len as usize];
                buf.copy_to_slice(&mut data);
                Some(data)
            } else {
                None
            }
        } else {
            None
        };

        Ok(Self {
            version,
            topic_partitions,
            user_data,
        })
    }

    /// Encode this assignment to bytes for wire format
    ///
    /// # Returns
    /// Encoded bytes suitable for SyncGroup response assignment field
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(128);

        // Write version
        buf.put_i16(self.version);

        // Write topic_partitions array
        // Sort topics for deterministic output
        let mut topics: Vec<_> = self.topic_partitions.iter().collect();
        topics.sort_by_key(|(name, _)| *name);

        buf.put_i32(topics.len() as i32);
        for (topic_name, partitions) in topics {
            // Write topic name
            let name_bytes = topic_name.as_bytes();
            buf.put_i16(name_bytes.len() as i16);
            buf.put_slice(name_bytes);

            // Write partitions array (sorted for determinism)
            let mut sorted_partitions = partitions.clone();
            sorted_partitions.sort();

            buf.put_i32(sorted_partitions.len() as i32);
            for partition in sorted_partitions {
                buf.put_i32(partition);
            }
        }

        // Write user_data
        match &self.user_data {
            Some(data) => {
                buf.put_i32(data.len() as i32);
                buf.put_slice(data);
            }
            None => {
                buf.put_i32(-1); // null marker
            }
        }

        buf
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

        let encoded = original.encode();
        let decoded = MemberAssignment::parse(&encoded).unwrap();

        assert_eq!(original.version, decoded.version);
        assert_eq!(original.user_data, decoded.user_data);

        // Check partitions (may be sorted differently but same content)
        for (topic, partitions) in &original.topic_partitions {
            let decoded_partitions = decoded.partitions(topic);
            let mut orig_sorted = partitions.clone();
            orig_sorted.sort();
            assert_eq!(orig_sorted, decoded_partitions);
        }
    }

    #[test]
    fn test_encode_empty_assignment() {
        let assign = MemberAssignment::default();
        let encoded = assign.encode();
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
