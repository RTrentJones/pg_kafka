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
            // A null (negative-length) topic name is skipped.
            let Some(topic_name) = read_topic_name(&mut buf)? else {
                continue;
            };
            topic_partitions.insert(topic_name, read_partitions(&mut buf)?);
        }

        // Read user_data (optional)
        let user_data = wire::get_trailing_user_data(&mut buf);

        Ok(Self {
            version,
            topic_partitions,
            user_data,
        })
    }

    /// Parse like [`Self::parse`], keeping only what the sticky assignor can keep of a
    /// previous assignment: the entries for topics in `topics` with partitions. The result
    /// keeps the same partitions per topic as `parse` (where a later entry for a topic
    /// replaces an earlier one, and an empty or null list keeps nothing), and fails where
    /// `parse` fails; `user_data` is not kept.
    ///
    /// A leader's assignments may hold millions of entries naming topics that do not exist
    /// or listing no partitions, at a few bytes each. Cost: time linear in `bytes`; memory:
    /// only the kept entries (a copy of each kept topic name and its partitions), nothing
    /// for the rest and nothing pre-allocated from a declared length.
    pub fn parse_kept(bytes: &[u8], topics: &HashMap<String, i32>) -> Result<Self> {
        let mut kept = Self::default();
        if bytes.is_empty() {
            return Ok(kept);
        }
        let mut buf = bytes;
        if buf.remaining() < 6 {
            return Err(KafkaError::CorruptMessage {
                message: format!(
                    "MemberAssignment too short: {} bytes (need at least 6)",
                    buf.remaining()
                ),
            });
        }
        kept.version = buf.get_i16();
        let topics_len = buf.get_i32();
        let topics_len = usize::try_from(topics_len).map_err(|_| KafkaError::CorruptMessage {
            message: format!("Invalid topic_partitions array length: {topics_len}"),
        })?;
        for _ in 0..topics_len {
            let Some(name) = read_topic_name_str(&mut buf)? else {
                continue;
            };
            let len = read_partitions_len(&mut buf)?;
            let wanted = topics.contains_key(name);
            if wanted && len > 0 {
                let mut partitions = Vec::with_capacity(len);
                for _ in 0..len {
                    partitions.push(buf.get_i32());
                }
                kept.topic_partitions.insert(name.to_string(), partitions);
            } else {
                buf.advance(len * 4);
                if wanted {
                    kept.topic_partitions.remove(name);
                }
            }
        }
        Ok(kept)
    }

    /// The work an assignment's bytes take to parse and keep: its topic entries (null
    /// names included) plus the partitions they list, read from the lengths without
    /// copying anything, and counted up to just over `limit` (the walk stops there).
    /// Entries after data [`Self::parse`] would reject are not counted.
    ///
    /// Cost: one step per counted entry, at most `limit + 1`; no memory.
    pub fn work_hint(bytes: &[u8], limit: usize) -> usize {
        let mut buf = bytes;
        if buf.remaining() < 6 {
            return 0;
        }
        buf.advance(2);
        let Ok(topics_len) = usize::try_from(buf.get_i32()) else {
            return 0;
        };
        let mut work = 0_usize;
        for _ in 0..topics_len {
            if work > limit || buf.remaining() < 2 {
                break;
            }
            work += 1;
            let Ok(name_len) = usize::try_from(buf.get_i16()) else {
                continue;
            };
            if buf.remaining() < name_len + 4 {
                break;
            }
            buf.advance(name_len);
            let Ok(len) = usize::try_from(buf.get_i32()) else {
                continue;
            };
            if buf.remaining() < len * 4 {
                break;
            }
            buf.advance(len * 4);
            work = work.saturating_add(len);
        }
        work
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

/// One topic name of the assignment array, or `None` for a null (negative-length) one.
fn read_topic_name(buf: &mut &[u8]) -> Result<Option<String>> {
    read_topic_name_str(buf).map(|name| name.map(str::to_string))
}

/// [`read_topic_name`] without copying the name.
fn read_topic_name_str<'a>(buf: &mut &'a [u8]) -> Result<Option<&'a str>> {
    if buf.remaining() < 2 {
        return Err(KafkaError::CorruptMessage {
            message: "Unexpected end of data reading topic name length".into(),
        });
    }
    let Ok(str_len) = usize::try_from(buf.get_i16()) else {
        return Ok(None);
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
    let (name, rest) = buf.split_at(str_len);
    *buf = rest;
    std::str::from_utf8(name)
        .map(Some)
        .map_err(|e| KafkaError::CorruptMessage {
            message: format!("Invalid UTF-8 in topic name: {e}"),
        })
}

/// One topic's partitions array; a null (negative-length) array is an empty assignment.
fn read_partitions(buf: &mut &[u8]) -> Result<Vec<i32>> {
    let partitions_len = read_partitions_len(buf)?;
    // Capped like the topics array: the declared length is client-controlled.
    let mut partitions = Vec::with_capacity(partitions_len.min(MAX_PREALLOC_ELEMENTS));
    for _ in 0..partitions_len {
        partitions.push(buf.get_i32());
    }
    Ok(partitions)
}

/// The length of one topic's partitions array (0 for a null one), checked against the data
/// that follows it.
fn read_partitions_len(buf: &mut &[u8]) -> Result<usize> {
    if buf.remaining() < 4 {
        return Err(KafkaError::CorruptMessage {
            message: "Unexpected end of data reading partitions array length".into(),
        });
    }
    let Ok(partitions_len) = usize::try_from(buf.get_i32()) else {
        return Ok(0);
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
    Ok(partitions_len)
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

    /// An assignment of `entries`: a topic name (`None` for a null one, which carries no
    /// partitions array) and its partitions (`None` for a null array).
    type Entry<'a> = (Option<&'a [u8]>, Option<&'a [i32]>);

    fn encoded(entries: &[Entry<'_>]) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.put_i16(0);
        bytes.put_i32(i32::try_from(entries.len()).unwrap());
        for (name, partitions) in entries {
            let Some(name) = name else {
                bytes.put_i16(-1);
                continue;
            };
            bytes.put_i16(i16::try_from(name.len()).unwrap());
            bytes.put_slice(name);
            match partitions {
                Some(partitions) => {
                    bytes.put_i32(i32::try_from(partitions.len()).unwrap());
                    for &p in *partitions {
                        bytes.put_i32(p);
                    }
                }
                None => bytes.put_i32(-1),
            }
        }
        bytes.put_i32(-1);
        bytes
    }

    fn topics(names: &[&str]) -> HashMap<String, i32> {
        names.iter().map(|n| ((*n).to_string(), 8)).collect()
    }

    #[test]
    fn test_work_hint_counts_entries_and_partitions() {
        let bytes = encoded(&[
            (Some(b"a"), Some(&[0, 1])),
            (None, None),
            (Some(b"b"), None),
            (Some(b"c"), Some(&[3])),
        ]);
        // Four entries, three partitions.
        assert_eq!(MemberAssignment::work_hint(&bytes, usize::MAX), 7);
        // The walk stops once the count passes the limit.
        assert_eq!(MemberAssignment::work_hint(&bytes, 0), 3);
        assert_eq!(MemberAssignment::work_hint(&bytes, 3), 4);
    }

    #[test]
    fn test_work_hint_of_short_or_malformed_bytes() {
        assert_eq!(MemberAssignment::work_hint(&[], 10), 0);
        assert_eq!(MemberAssignment::work_hint(&[0, 0, 0, 0, 0], 10), 0);
        // A negative topics array length.
        assert_eq!(
            MemberAssignment::work_hint(&[0, 0, 0xff, 0xff, 0xff, 0xff], 10),
            0
        );
        // A claimed array longer than the data: only the entries present count.
        let mut bytes = encoded(&[(Some(b"a"), Some(&[0]))]);
        bytes.truncate(bytes.len() - 4);
        bytes[2..6].copy_from_slice(&5_i32.to_be_bytes());
        assert_eq!(MemberAssignment::work_hint(&bytes, 10), 2);
        // A name longer than the data: the walk stops at it, uncounted beyond the entry.
        let mut bytes = encoded(&[(Some(b"abc"), Some(&[0]))]);
        bytes.truncate(8);
        assert_eq!(MemberAssignment::work_hint(&bytes, 10), 1);
        // A partitions array longer than the data.
        let mut bytes = encoded(&[(Some(b"a"), Some(&[0, 1, 2]))]);
        bytes.truncate(bytes.len() - 8);
        assert_eq!(MemberAssignment::work_hint(&bytes, 10), 1);
    }

    #[test]
    fn test_parse_kept_keeps_only_wanted_entries_with_partitions() {
        let bytes = encoded(&[
            (Some(b"a"), Some(&[0, 1])),
            (None, None),
            (Some(b"gone"), Some(&[5, 6])),
            (Some(b"b"), Some(&[2])),
            (Some(b"b"), Some(&[3, 4])),
            (Some(b"c"), Some(&[7])),
            (Some(b"c"), Some(&[])),
            (Some(b"d"), Some(&[1])),
            (Some(b"d"), None),
        ]);
        let wanted = topics(&["a", "b", "c", "d"]);
        let kept = MemberAssignment::parse_kept(&bytes, &wanted).unwrap();
        // A later entry replaces an earlier one; an empty or null list keeps nothing.
        assert_eq!(kept.partitions("a"), vec![0, 1]);
        assert_eq!(kept.partitions("b"), vec![3, 4]);
        assert!(!kept.topic_partitions.contains_key("c"));
        assert!(!kept.topic_partitions.contains_key("d"));
        assert!(!kept.topic_partitions.contains_key("gone"));
        assert!(kept.user_data.is_none());
        // The same partitions as `parse` for the kept topics.
        let parsed = MemberAssignment::parse(&bytes).unwrap();
        assert_eq!(parsed.partitions("b"), kept.partitions("b"));
        assert!(MemberAssignment::parse_kept(&[], &wanted)
            .unwrap()
            .topic_partitions
            .is_empty());
    }

    #[test]
    fn test_parse_kept_fails_where_parse_fails() {
        let wanted = topics(&["a"]);
        let mut cases: Vec<Vec<u8>> = vec![
            vec![0, 0, 0],                      // too short
            vec![0, 0, 0xff, 0xff, 0xff, 0xff], // negative topics array length
        ];
        // A claimed entry with no data.
        let mut missing = encoded(&[]);
        missing.truncate(6);
        missing[2..6].copy_from_slice(&1_i32.to_be_bytes());
        cases.push(missing);
        // A name longer than the data.
        let mut name = encoded(&[(Some(b"abc"), Some(&[0]))]);
        name.truncate(8);
        cases.push(name);
        // A name that is not UTF-8.
        cases.push(encoded(&[(Some(&[0xff, 0xfe]), Some(&[0]))]));
        // No partitions array length after the name.
        let mut no_len = encoded(&[(Some(b"a"), Some(&[0]))]);
        no_len.truncate(9);
        cases.push(no_len);
        // A partitions array longer than the data, for a kept and an unkept topic.
        for topic in [b"a", b"z"] {
            let mut partitions = encoded(&[(Some(topic), Some(&[0, 1, 2]))]);
            partitions.truncate(partitions.len() - 8);
            cases.push(partitions);
        }
        for (i, bytes) in cases.iter().enumerate() {
            assert!(MemberAssignment::parse(bytes).is_err(), "case {i}: parse");
            assert!(
                MemberAssignment::parse_kept(bytes, &wanted).is_err(),
                "case {i}: parse_kept"
            );
        }
    }
}
