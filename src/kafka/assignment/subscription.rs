//! MemberSubscription parsing and encoding
//!
//! The MemberSubscription is sent by consumers in the JoinGroup request's protocol metadata.
//! It contains the list of topics the consumer wants to subscribe to.
//!
//! # Wire Format (Kafka ConsumerProtocolSubscription)
//!
//! ```text
//! version: i16
//! topics: [String]
//!   - array_length: i32
//!   - for each topic:
//!     - string_length: i16
//!     - string_bytes: [u8]
//! user_data: bytes
//!   - length: i32 (-1 for null)
//!   - data: [u8] (if length >= 0)
//! owned_partitions: [TopicPartition] (v1+ only, for cooperative rebalancing)
//!   - Not implemented in this version
//! ```

use bytes::{Buf, BufMut};

use crate::kafka::error::{KafkaError, Result};

/// Consumer subscription metadata from JoinGroup request
///
/// This is parsed from the `metadata` field of the JoinGroup protocol list.
/// Each consumer advertises which topics it wants to consume.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MemberSubscription {
    /// Protocol version (0 = basic, 1+ = cooperative features)
    pub version: i16,

    /// List of topic names the consumer wants to subscribe to
    pub topics: Vec<String>,

    /// Optional user-defined data (passed through unchanged)
    pub user_data: Option<Vec<u8>>,
}

impl MemberSubscription {
    /// Create a new subscription with the given topics
    pub fn new(topics: Vec<String>) -> Self {
        Self {
            version: 0,
            topics,
            user_data: None,
        }
    }

    /// Parse a MemberSubscription from raw bytes
    ///
    /// # Arguments
    /// * `bytes` - Raw bytes from JoinGroup protocol metadata field
    ///
    /// # Returns
    /// Parsed subscription or error if format is invalid
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        if bytes.is_empty() {
            return Ok(Self::default());
        }

        let mut buf = bytes;

        // Check minimum size: version(2) + array_length(4) = 6 bytes
        if buf.remaining() < 6 {
            return Err(KafkaError::CorruptMessage {
                message: format!(
                    "MemberSubscription too short: {} bytes (need at least 6)",
                    buf.remaining()
                ),
            });
        }

        // Read version
        let version = buf.get_i16();

        // Read topics array
        let topics_len = buf.get_i32();
        if topics_len < 0 {
            return Err(KafkaError::CorruptMessage {
                message: format!("Invalid topics array length: {}", topics_len),
            });
        }

        let mut topics = Vec::with_capacity(topics_len as usize);
        for _ in 0..topics_len {
            // Check we have enough for string length
            if buf.remaining() < 2 {
                return Err(KafkaError::CorruptMessage {
                    message: "Unexpected end of data reading topic string length".into(),
                });
            }

            let str_len = buf.get_i16();
            if str_len < 0 {
                // Null string - skip
                continue;
            }

            let str_len = str_len as usize;
            if buf.remaining() < str_len {
                return Err(KafkaError::CorruptMessage {
                    message: format!(
                        "Topic string length {} exceeds remaining data {}",
                        str_len,
                        buf.remaining()
                    ),
                });
            }

            let mut str_bytes = vec![0u8; str_len];
            buf.copy_to_slice(&mut str_bytes);

            let topic = String::from_utf8(str_bytes).map_err(|e| KafkaError::CorruptMessage {
                message: format!("Invalid UTF-8 in topic name: {}", e),
            })?;

            topics.push(topic);
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
                // Not enough data - ignore
                None
            }
        } else {
            None
        };

        Ok(Self {
            version,
            topics,
            user_data,
        })
    }

    /// Encode this subscription to bytes for wire format
    ///
    /// # Returns
    /// Encoded bytes suitable for JoinGroup protocol metadata
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(64);

        // Write version
        buf.put_i16(self.version);

        // Write topics array
        buf.put_i32(self.topics.len() as i32);
        for topic in &self.topics {
            let topic_bytes = topic.as_bytes();
            buf.put_i16(topic_bytes.len() as i16);
            buf.put_slice(topic_bytes);
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
        let sub = MemberSubscription::parse(&[]).unwrap();
        assert_eq!(sub.version, 0);
        assert!(sub.topics.is_empty());
        assert!(sub.user_data.is_none());
    }

    #[test]
    fn test_parse_single_topic() {
        // version=0, topics=["test-topic"], user_data=null
        let mut bytes = Vec::new();
        bytes.put_i16(0); // version
        bytes.put_i32(1); // topics count
        bytes.put_i16(10); // "test-topic" length
        bytes.put_slice(b"test-topic");
        bytes.put_i32(-1); // null user_data

        let sub = MemberSubscription::parse(&bytes).unwrap();
        assert_eq!(sub.version, 0);
        assert_eq!(sub.topics, vec!["test-topic"]);
        assert!(sub.user_data.is_none());
    }

    #[test]
    fn test_parse_multiple_topics() {
        let mut bytes = Vec::new();
        bytes.put_i16(1); // version
        bytes.put_i32(3); // topics count
        bytes.put_i16(7);
        bytes.put_slice(b"topic-a");
        bytes.put_i16(7);
        bytes.put_slice(b"topic-b");
        bytes.put_i16(7);
        bytes.put_slice(b"topic-c");
        bytes.put_i32(-1); // null user_data

        let sub = MemberSubscription::parse(&bytes).unwrap();
        assert_eq!(sub.version, 1);
        assert_eq!(sub.topics, vec!["topic-a", "topic-b", "topic-c"]);
    }

    #[test]
    fn test_parse_with_user_data() {
        let mut bytes = Vec::new();
        bytes.put_i16(0);
        bytes.put_i32(1);
        bytes.put_i16(5);
        bytes.put_slice(b"topic");
        bytes.put_i32(4); // user_data length
        bytes.put_slice(b"data");

        let sub = MemberSubscription::parse(&bytes).unwrap();
        assert_eq!(sub.user_data, Some(b"data".to_vec()));
    }

    #[test]
    fn test_encode_decode_roundtrip() {
        let original = MemberSubscription {
            version: 1,
            topics: vec!["topic-1".to_string(), "topic-2".to_string()],
            user_data: Some(vec![1, 2, 3, 4]),
        };

        let encoded = original.encode();
        let decoded = MemberSubscription::parse(&encoded).unwrap();

        assert_eq!(original, decoded);
    }

    #[test]
    fn test_encode_empty_subscription() {
        let sub = MemberSubscription::default();
        let encoded = sub.encode();
        let decoded = MemberSubscription::parse(&encoded).unwrap();

        assert_eq!(sub, decoded);
    }

    #[test]
    fn test_parse_too_short() {
        let bytes = vec![0, 0, 0]; // Only 3 bytes, need at least 6
        let result = MemberSubscription::parse(&bytes);
        assert!(result.is_err());
    }
}
