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

use super::wire;
use crate::kafka::constants::MAX_PREALLOC_ELEMENTS;
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
        Self::parse_with(bytes, true)
    }

    /// Parse like [`Self::parse`] without copying `user_data` (`None` in the result): the
    /// server-side assignors read only the topics, and a member's metadata may carry up to
    /// about a megabyte of user data. Cost: time linear in the topics' bytes; memory: the
    /// topic names, nothing for the user data.
    pub fn parse_topics(bytes: &[u8]) -> Result<Self> {
        Self::parse_with(bytes, false)
    }

    fn parse_with(bytes: &[u8], keep_user_data: bool) -> Result<Self> {
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
        let topics_len = usize::try_from(topics_len).map_err(|_| KafkaError::CorruptMessage {
            message: format!("Invalid topics array length: {topics_len}"),
        })?;

        // Cap the pre-allocation hint: `topics_len` is attacker-controlled, so never
        // pre-allocate the claimed count directly (memory-exhaustion DoS). The loop below
        // bounds-checks every element, so an honestly-large array still parses correctly.
        let mut topics = Vec::with_capacity(topics_len.min(MAX_PREALLOC_ELEMENTS));
        for _ in 0..topics_len {
            // Check we have enough for string length
            if buf.remaining() < 2 {
                return Err(KafkaError::CorruptMessage {
                    message: "Unexpected end of data reading topic string length".into(),
                });
            }

            // A null (negative-length) topic name is skipped.
            let Ok(str_len) = usize::try_from(buf.get_i16()) else {
                continue;
            };
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
                message: format!("Invalid UTF-8 in topic name: {e}"),
            })?;

            topics.push(topic);
        }

        // Read user_data (optional)
        let user_data = if keep_user_data {
            wire::get_trailing_user_data(&mut buf)
        } else {
            None
        };

        Ok(Self {
            version,
            topics,
            user_data,
        })
    }

    /// The topic count `bytes` claims (its topics array length), read without parsing the
    /// topics: an upper bound on the entries [`Self::parse`] returns, 0 for metadata too
    /// short to carry one or a negative length.
    pub fn topic_count_hint(bytes: &[u8]) -> usize {
        bytes
            .get(2..6)
            .and_then(|len| <[u8; 4]>::try_from(len).ok())
            .and_then(|len| usize::try_from(i32::from_be_bytes(len)).ok())
            .unwrap_or(0)
    }

    /// Encode this subscription to bytes for wire format
    ///
    /// # Returns
    /// Encoded bytes for JoinGroup protocol metadata, or `Internal` if a length does not fit
    /// its prefix
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(64);
        buf.put_i16(self.version);
        wire::put_i32_len(&mut buf, self.topics.len(), "topics array")?;
        for topic in &self.topics {
            wire::put_string(&mut buf, topic)?;
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
        let sub = MemberSubscription::parse(&[]).unwrap();
        assert_eq!(sub.version, 0);
        assert_eq!(sub.topics, [] as [String; 0]);
        assert!(sub.user_data.is_none());
    }

    #[test]
    fn test_parse_huge_topics_len_does_not_oom() {
        // A tiny request can declare a huge topics array length. Pre-allocating it
        // (~2.1B * sizeof(String)) would abort the process; the capacity is capped at
        // MAX_PREALLOC_ELEMENTS and the per-element bounds check fails instead. An OOM here
        // means the cap is gone.
        let mut buf = Vec::new();
        buf.extend_from_slice(&0i16.to_be_bytes()); // version
        buf.extend_from_slice(&i32::MAX.to_be_bytes()); // topics_len = 2.1B, no data follows
        assert!(MemberSubscription::parse(&buf).is_err());
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

    /// `parse_topics` reads the same topics as `parse` and keeps none of the user data, and
    /// fails where `parse` fails.
    #[test]
    fn test_parse_topics_skips_user_data() {
        let sub = MemberSubscription {
            version: 1,
            topics: vec!["a".to_string(), "b".to_string()],
            user_data: Some(vec![7; 512 * 1024]),
        };
        let encoded = sub.encode().unwrap();
        let topics = MemberSubscription::parse_topics(&encoded).unwrap();
        assert_eq!((topics.version, &topics.topics), (1, &sub.topics));
        assert!(topics.user_data.is_none());
        assert_eq!(MemberSubscription::parse(&encoded).unwrap(), sub);
        assert!(MemberSubscription::parse_topics(&encoded[..9]).is_err());
        assert_eq!(
            MemberSubscription::parse_topics(&[]).unwrap(),
            MemberSubscription::default()
        );
    }

    #[test]
    fn test_encode_decode_roundtrip() {
        let original = MemberSubscription {
            version: 1,
            topics: vec!["topic-1".to_string(), "topic-2".to_string()],
            user_data: Some(vec![1, 2, 3, 4]),
        };

        let encoded = original.encode().unwrap();
        let decoded = MemberSubscription::parse(&encoded).unwrap();

        assert_eq!(original, decoded);
    }

    #[test]
    fn test_encode_empty_subscription() {
        let sub = MemberSubscription::default();
        let encoded = sub.encode().unwrap();
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
