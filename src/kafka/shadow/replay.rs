// Copyright (c) 2026 Robert Trent Jones.
// This file is part of the "Shadow Mode" feature of pg_kafka.
//
// Use of this source code for production purposes is governed by the
// Commercial License found in the LICENSE file in this directory.
// Development and evaluation use is permitted.
//
// GitHub Sponsors: https://github.com/sponsors/RTrentJones

//! Shadow mode replay support
//!
//! This module provides functionality to replay messages that were stored locally
//! but not forwarded to external Kafka, or that need to be re-forwarded.
//!
//! ## Use Cases
//!
//! - **Initial migration**: Forward historical messages to external Kafka
//! - **Recovery**: Re-forward messages after an external Kafka outage
//! - **Testing**: Verify forwarding works correctly
//!
//! ## How It Works
//!
//! Replay reads messages from `kafka.messages` within a specified offset range
//! and forwards them to external Kafka using the ShadowProducer. Progress is
//! tracked in `kafka.shadow_tracking` to allow resumption.

use super::config::TopicShadowConfig;
use super::error::{ShadowError, ShadowResult};
use super::forwarder::ForwardMessage;
use super::producer::ShadowProducer;
use std::sync::Arc;

/// Replay progress tracker
#[derive(Debug, Clone)]
pub struct ReplayProgress {
    /// Topic being replayed
    pub topic_name: String,
    /// Partition being replayed
    pub partition_id: i32,
    /// Starting offset (inclusive)
    pub from_offset: i64,
    /// Ending offset (exclusive)
    pub to_offset: i64,
    /// Current offset being processed
    pub current_offset: i64,
    /// Number of messages successfully forwarded
    pub forwarded_count: u64,
    /// Number of messages that failed
    pub failed_count: u64,
}

impl ReplayProgress {
    /// Create new replay progress
    pub fn new(topic_name: &str, partition_id: i32, from_offset: i64, to_offset: i64) -> Self {
        Self {
            topic_name: topic_name.to_string(),
            partition_id,
            from_offset,
            to_offset,
            current_offset: from_offset,
            forwarded_count: 0,
            failed_count: 0,
        }
    }

    /// Check if replay is complete
    pub fn is_complete(&self) -> bool {
        self.current_offset >= self.to_offset
    }

    /// Get percentage complete
    pub fn percent_complete(&self) -> f64 {
        if self.to_offset <= self.from_offset {
            return 100.0;
        }
        let total = (self.to_offset - self.from_offset) as f64;
        let done = (self.current_offset - self.from_offset) as f64;
        (done / total * 100.0).min(100.0)
    }

    /// Get remaining messages to process
    pub fn remaining(&self) -> i64 {
        (self.to_offset - self.current_offset).max(0)
    }
}

/// Replay request parameters
#[derive(Debug, Clone)]
pub struct ReplayRequest {
    /// Topic name to replay
    pub topic_name: String,
    /// Partition ID (or None for all partitions)
    pub partition_id: Option<i32>,
    /// Starting offset (None = earliest)
    pub from_offset: Option<i64>,
    /// Ending offset (None = latest)
    pub to_offset: Option<i64>,
    /// Batch size for reading from database
    pub batch_size: i32,
    /// Whether to skip already-forwarded messages
    pub skip_forwarded: bool,
}

impl ReplayRequest {
    /// Create a new replay request for a topic
    pub fn new(topic_name: &str) -> Self {
        Self {
            topic_name: topic_name.to_string(),
            partition_id: None,
            from_offset: None,
            to_offset: None,
            batch_size: 1000,
            skip_forwarded: true,
        }
    }

    /// Set specific partition
    pub fn partition(mut self, partition_id: i32) -> Self {
        self.partition_id = Some(partition_id);
        self
    }

    /// Set offset range
    pub fn range(mut self, from: i64, to: i64) -> Self {
        self.from_offset = Some(from);
        self.to_offset = Some(to);
        self
    }

    /// Set batch size
    pub fn batch_size(mut self, size: i32) -> Self {
        self.batch_size = size;
        self
    }

    /// Set whether to skip already-forwarded messages
    pub fn skip_forwarded(mut self, skip: bool) -> Self {
        self.skip_forwarded = skip;
        self
    }
}

/// Result of a replay operation
#[derive(Debug, Default)]
pub struct ReplayResult {
    /// Total messages processed
    pub total_processed: u64,
    /// Messages successfully forwarded
    pub forwarded: u64,
    /// Messages skipped (already forwarded or filtered)
    pub skipped: u64,
    /// Messages that failed to forward
    pub failed: u64,
    /// First error encountered (if any)
    pub first_error: Option<ShadowError>,
    /// Whether replay completed successfully
    pub completed: bool,
}

/// Shadow replay engine
///
/// Handles replaying messages from local storage to external Kafka.
pub struct ReplayEngine {
    /// Producer for sending to external Kafka
    producer: Arc<ShadowProducer>,
}

impl ReplayEngine {
    /// Create a new replay engine
    pub fn new(producer: Arc<ShadowProducer>) -> Self {
        Self { producer }
    }

    /// Replay a batch of messages
    ///
    /// Takes pre-fetched messages and forwards them to external Kafka.
    /// Returns the number of successfully forwarded messages.
    pub async fn replay_batch(
        &self,
        messages: &[ForwardMessage],
        topic_config: &TopicShadowConfig,
    ) -> ShadowResult<u64> {
        let external_topic = topic_config.effective_external_topic();
        let mut forwarded = 0u64;

        for msg in messages {
            let result = self
                .producer
                .send_async(
                    external_topic,
                    Some(msg.partition),
                    msg.key.as_deref(),
                    msg.value.as_deref(),
                )
                .await;

            match result {
                Ok(()) => {
                    forwarded += 1;
                    tracing::trace!(
                        "Replayed message to {} partition {} offset {}",
                        external_topic,
                        msg.partition,
                        msg.partition_offset
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to replay message to {} partition {} offset {}: {:?}",
                        external_topic,
                        msg.partition,
                        msg.partition_offset,
                        e
                    );
                    return Err(ShadowError::ReplayFailed {
                        topic: msg.topic.clone(),
                        from_offset: msg.partition_offset,
                        to_offset: msg.partition_offset + 1,
                        error: e.to_string(),
                    });
                }
            }
        }

        Ok(forwarded)
    }

    /// Get the underlying producer
    pub fn producer(&self) -> &ShadowProducer {
        &self.producer
    }
}

/// SQL helper for building replay queries
#[allow(dead_code)]
pub struct ReplayQueryBuilder {
    topic_name: String,
    partition_id: Option<i32>,
    from_offset: Option<i64>,
    to_offset: Option<i64>,
    limit: i32,
    skip_forwarded: bool,
}

impl ReplayQueryBuilder {
    /// Create a new query builder
    pub fn new(topic_name: &str) -> Self {
        Self {
            topic_name: topic_name.to_string(),
            partition_id: None,
            from_offset: None,
            to_offset: None,
            limit: 1000,
            skip_forwarded: true,
        }
    }

    /// Set partition filter
    pub fn partition(mut self, partition_id: i32) -> Self {
        self.partition_id = Some(partition_id);
        self
    }

    /// Set offset range
    pub fn range(mut self, from: i64, to: i64) -> Self {
        self.from_offset = Some(from);
        self.to_offset = Some(to);
        self
    }

    /// Set limit
    pub fn limit(mut self, limit: i32) -> Self {
        self.limit = limit;
        self
    }

    /// Set skip forwarded flag
    pub fn skip_forwarded(mut self, skip: bool) -> Self {
        self.skip_forwarded = skip;
        self
    }

    /// Build the SQL query for fetching messages to replay
    ///
    /// Returns (query, params) tuple.
    pub fn build(&self) -> String {
        let mut query = String::from(
            r#"
            SELECT m.global_offset, m.partition_id, m.partition_offset, m.key, m.value
            FROM kafka.messages m
            JOIN kafka.topics t ON m.topic_id = t.id
            WHERE t.name = $1
            "#,
        );

        let mut param_idx = 2;

        if let Some(partition) = self.partition_id {
            query.push_str(&format!(" AND m.partition_id = ${}", param_idx));
            param_idx += 1;
            let _ = partition; // Used in actual query execution
        }

        if let Some(from) = self.from_offset {
            query.push_str(&format!(" AND m.partition_offset >= ${}", param_idx));
            param_idx += 1;
            let _ = from;
        }

        if let Some(to) = self.to_offset {
            query.push_str(&format!(" AND m.partition_offset < ${}", param_idx));
            // Note: param_idx not incremented since no more parameters follow
            let _ = to;
        }

        if self.skip_forwarded {
            // Join with tracking table to skip already-forwarded messages
            query.push_str(
                r#"
                AND NOT EXISTS (
                    SELECT 1 FROM kafka.shadow_tracking st
                    WHERE st.topic_id = m.topic_id
                    AND st.partition_id = m.partition_id
                    AND st.partition_offset = m.partition_offset
                    AND st.forwarded = true
                )
                "#,
            );
        }

        query.push_str(&format!(
            " ORDER BY m.partition_offset ASC LIMIT {}",
            self.limit
        ));

        query
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replay_progress_new() {
        let progress = ReplayProgress::new("test-topic", 0, 0, 100);
        assert_eq!(progress.topic_name, "test-topic");
        assert_eq!(progress.partition_id, 0);
        assert_eq!(progress.from_offset, 0);
        assert_eq!(progress.to_offset, 100);
        assert_eq!(progress.current_offset, 0);
        assert!(!progress.is_complete());
    }

    #[test]
    fn test_replay_progress_percent_complete() {
        let mut progress = ReplayProgress::new("test", 0, 0, 100);
        assert_eq!(progress.percent_complete(), 0.0);

        progress.current_offset = 50;
        assert_eq!(progress.percent_complete(), 50.0);

        progress.current_offset = 100;
        assert_eq!(progress.percent_complete(), 100.0);

        progress.current_offset = 150;
        assert_eq!(progress.percent_complete(), 100.0); // Capped at 100%
    }

    #[test]
    fn test_replay_progress_remaining() {
        let mut progress = ReplayProgress::new("test", 0, 0, 100);
        assert_eq!(progress.remaining(), 100);

        progress.current_offset = 50;
        assert_eq!(progress.remaining(), 50);

        progress.current_offset = 100;
        assert_eq!(progress.remaining(), 0);

        progress.current_offset = 150;
        assert_eq!(progress.remaining(), 0); // Capped at 0
    }

    #[test]
    fn test_replay_progress_is_complete() {
        let mut progress = ReplayProgress::new("test", 0, 0, 100);
        assert!(!progress.is_complete());

        progress.current_offset = 99;
        assert!(!progress.is_complete());

        progress.current_offset = 100;
        assert!(progress.is_complete());
    }

    #[test]
    fn test_replay_request_builder() {
        let request = ReplayRequest::new("my-topic")
            .partition(1)
            .range(100, 200)
            .batch_size(500)
            .skip_forwarded(false);

        assert_eq!(request.topic_name, "my-topic");
        assert_eq!(request.partition_id, Some(1));
        assert_eq!(request.from_offset, Some(100));
        assert_eq!(request.to_offset, Some(200));
        assert_eq!(request.batch_size, 500);
        assert!(!request.skip_forwarded);
    }

    #[test]
    fn test_replay_query_builder_basic() {
        let query = ReplayQueryBuilder::new("test-topic").build();
        assert!(query.contains("t.name = $1"));
        assert!(query.contains("ORDER BY m.partition_offset ASC"));
        assert!(query.contains("LIMIT 1000"));
    }

    #[test]
    fn test_replay_query_builder_with_partition() {
        let query = ReplayQueryBuilder::new("test-topic").partition(2).build();
        assert!(query.contains("m.partition_id = $2"));
    }

    #[test]
    fn test_replay_query_builder_with_range() {
        let query = ReplayQueryBuilder::new("test-topic")
            .range(100, 200)
            .build();
        assert!(query.contains("m.partition_offset >= $"));
        assert!(query.contains("m.partition_offset < $"));
    }

    #[test]
    fn test_replay_query_builder_skip_forwarded() {
        let query = ReplayQueryBuilder::new("test-topic")
            .skip_forwarded(true)
            .build();
        assert!(query.contains("NOT EXISTS"));
        assert!(query.contains("kafka.shadow_tracking"));

        let query_no_skip = ReplayQueryBuilder::new("test-topic")
            .skip_forwarded(false)
            .build();
        assert!(!query_no_skip.contains("NOT EXISTS"));
    }

    #[test]
    fn test_replay_result_default() {
        let result = ReplayResult::default();
        assert_eq!(result.total_processed, 0);
        assert_eq!(result.forwarded, 0);
        assert_eq!(result.skipped, 0);
        assert_eq!(result.failed, 0);
        assert!(result.first_error.is_none());
        assert!(!result.completed);
    }
}
