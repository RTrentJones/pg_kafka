//! Custom test assertions
//!
//! Provides domain-specific assertions for verifying Kafka/database state.

use std::fmt;
use tokio_postgres::Client;

/// Custom assertion error with detailed information
#[derive(Debug)]
pub struct AssertionError {
    pub message: String,
    pub expected: String,
    pub actual: String,
}

impl fmt::Display for AssertionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}\n  Expected: {}\n  Actual: {}",
            self.message, self.expected, self.actual
        )
    }
}

impl std::error::Error for AssertionError {}

impl AssertionError {
    pub fn new(
        message: impl Into<String>,
        expected: impl Into<String>,
        actual: impl Into<String>,
    ) -> Self {
        Self {
            message: message.into(),
            expected: expected.into(),
            actual: actual.into(),
        }
    }
}

/// Assert that a topic exists in the database
pub async fn assert_topic_exists(
    db: &Client,
    topic_name: &str,
) -> Result<i32, Box<dyn std::error::Error>> {
    let row = db
        .query_opt(
            "SELECT id FROM kafka.topics WHERE name = $1",
            &[&topic_name],
        )
        .await?;

    match row {
        Some(row) => Ok(row.get(0)),
        None => Err(Box::new(AssertionError::new(
            "Topic does not exist",
            format!("Topic '{}' to exist", topic_name),
            "Topic not found",
        ))),
    }
}

/// Assert that a topic has the expected partition count
pub async fn assert_partition_count(
    db: &Client,
    topic_name: &str,
    expected: i32,
) -> Result<(), Box<dyn std::error::Error>> {
    let row = db
        .query_one(
            "SELECT partitions FROM kafka.topics WHERE name = $1",
            &[&topic_name],
        )
        .await?;

    let actual: i32 = row.get(0);

    if actual != expected {
        return Err(Box::new(AssertionError::new(
            format!("Partition count mismatch for topic '{}'", topic_name),
            expected.to_string(),
            actual.to_string(),
        )));
    }

    Ok(())
}

/// Assert that a specific number of messages exist for a topic
pub async fn assert_message_count(
    db: &Client,
    topic_name: &str,
    expected: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    let row = db
        .query_one(
            "SELECT COUNT(*) FROM kafka.messages m
             JOIN kafka.topics t ON m.topic_id = t.id
             WHERE t.name = $1",
            &[&topic_name],
        )
        .await?;

    let actual: i64 = row.get(0);

    if actual != expected {
        return Err(Box::new(AssertionError::new(
            format!("Message count mismatch for topic '{}'", topic_name),
            expected.to_string(),
            actual.to_string(),
        )));
    }

    Ok(())
}

/// Assert that at least N messages exist for a topic
pub async fn assert_message_count_at_least(
    db: &Client,
    topic_name: &str,
    minimum: i64,
) -> Result<i64, Box<dyn std::error::Error>> {
    let row = db
        .query_one(
            "SELECT COUNT(*) FROM kafka.messages m
             JOIN kafka.topics t ON m.topic_id = t.id
             WHERE t.name = $1",
            &[&topic_name],
        )
        .await?;

    let actual: i64 = row.get(0);

    if actual < minimum {
        return Err(Box::new(AssertionError::new(
            format!("Not enough messages for topic '{}'", topic_name),
            format!("at least {}", minimum),
            actual.to_string(),
        )));
    }

    Ok(actual)
}

/// Assert that a message exists with specific content
pub async fn assert_message_content(
    db: &Client,
    topic_name: &str,
    partition: i32,
    offset: i64,
    expected_key: Option<&[u8]>,
    expected_value: Option<&[u8]>,
) -> Result<(), Box<dyn std::error::Error>> {
    let row = db
        .query_opt(
            "SELECT key, value FROM kafka.messages m
             JOIN kafka.topics t ON m.topic_id = t.id
             WHERE t.name = $1 AND m.partition_id = $2 AND m.partition_offset = $3",
            &[&topic_name, &partition, &offset],
        )
        .await?;

    let row = row.ok_or_else(|| {
        AssertionError::new(
            format!(
                "Message not found at topic={}, partition={}, offset={}",
                topic_name, partition, offset
            ),
            "Message to exist",
            "Message not found",
        )
    })?;

    let actual_key: Option<Vec<u8>> = row.get(0);
    let actual_value: Option<Vec<u8>> = row.get(1);

    if let Some(expected) = expected_key {
        let actual = actual_key.as_deref().unwrap_or(&[]);
        if actual != expected {
            return Err(Box::new(AssertionError::new(
                "Key mismatch",
                String::from_utf8_lossy(expected).to_string(),
                String::from_utf8_lossy(actual).to_string(),
            )));
        }
    }

    if let Some(expected) = expected_value {
        let actual = actual_value.as_deref().unwrap_or(&[]);
        if actual != expected {
            return Err(Box::new(AssertionError::new(
                "Value mismatch",
                String::from_utf8_lossy(expected).to_string(),
                String::from_utf8_lossy(actual).to_string(),
            )));
        }
    }

    Ok(())
}

/// Assert that an offset has been committed for a consumer group
pub async fn assert_offset_committed(
    db: &Client,
    group_id: &str,
    topic_name: &str,
    partition: i32,
    expected_offset: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    let row = db
        .query_opt(
            "SELECT co.committed_offset FROM kafka.consumer_offsets co
             JOIN kafka.topics t ON co.topic_id = t.id
             WHERE co.group_id = $1 AND t.name = $2 AND co.partition_id = $3",
            &[&group_id, &topic_name, &partition],
        )
        .await?;

    let row = row.ok_or_else(|| {
        AssertionError::new(
            format!(
                "No committed offset for group={}, topic={}, partition={}",
                group_id, topic_name, partition
            ),
            "Committed offset to exist",
            "No offset found",
        )
    })?;

    let actual: i64 = row.get(0);

    if actual != expected_offset {
        return Err(Box::new(AssertionError::new(
            format!(
                "Committed offset mismatch for group={}, topic={}, partition={}",
                group_id, topic_name, partition
            ),
            expected_offset.to_string(),
            actual.to_string(),
        )));
    }

    Ok(())
}

/// Assert that offsets are sequential (no gaps) for a partition
pub async fn assert_offsets_sequential(
    db: &Client,
    topic_name: &str,
    partition: i32,
) -> Result<(), Box<dyn std::error::Error>> {
    let rows = db
        .query(
            "SELECT partition_offset FROM kafka.messages m
             JOIN kafka.topics t ON m.topic_id = t.id
             WHERE t.name = $1 AND m.partition_id = $2
             ORDER BY partition_offset",
            &[&topic_name, &partition],
        )
        .await?;

    for (i, row) in rows.iter().enumerate() {
        let offset: i64 = row.get(0);
        if offset != i as i64 {
            return Err(Box::new(AssertionError::new(
                format!(
                    "Gap in offsets for topic={}, partition={}",
                    topic_name, partition
                ),
                format!("offset {} at position {}", i, i),
                format!("offset {} at position {}", offset, i),
            )));
        }
    }

    Ok(())
}

/// Assert that the high watermark (next offset) matches expected value
pub async fn assert_high_watermark(
    db: &Client,
    topic_name: &str,
    partition: i32,
    expected: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    let row = db
        .query_one(
            "SELECT COALESCE(MAX(partition_offset) + 1, 0) FROM kafka.messages m
             JOIN kafka.topics t ON m.topic_id = t.id
             WHERE t.name = $1 AND m.partition_id = $2",
            &[&topic_name, &partition],
        )
        .await?;

    let actual: i64 = row.get(0);

    if actual != expected {
        return Err(Box::new(AssertionError::new(
            format!(
                "High watermark mismatch for topic={}, partition={}",
                topic_name, partition
            ),
            expected.to_string(),
            actual.to_string(),
        )));
    }

    Ok(())
}

/// Assert that earliest offset matches expected value
pub async fn assert_earliest_offset(
    db: &Client,
    topic_name: &str,
    partition: i32,
    expected: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    let row = db
        .query_one(
            "SELECT COALESCE(MIN(partition_offset), 0) FROM kafka.messages m
             JOIN kafka.topics t ON m.topic_id = t.id
             WHERE t.name = $1 AND m.partition_id = $2",
            &[&topic_name, &partition],
        )
        .await?;

    let actual: i64 = row.get(0);

    if actual != expected {
        return Err(Box::new(AssertionError::new(
            format!(
                "Earliest offset mismatch for topic={}, partition={}",
                topic_name, partition
            ),
            expected.to_string(),
            actual.to_string(),
        )));
    }

    Ok(())
}

/// Assert that no committed offset exists for a group/topic/partition
pub async fn assert_no_committed_offset(
    db: &Client,
    group_id: &str,
    topic_name: &str,
    partition: i32,
) -> Result<(), Box<dyn std::error::Error>> {
    let row = db
        .query_opt(
            "SELECT co.committed_offset FROM kafka.consumer_offsets co
             JOIN kafka.topics t ON co.topic_id = t.id
             WHERE co.group_id = $1 AND t.name = $2 AND co.partition_id = $3",
            &[&group_id, &topic_name, &partition],
        )
        .await?;

    if row.is_some() {
        return Err(Box::new(AssertionError::new(
            format!(
                "Unexpected committed offset exists for group={}, topic={}, partition={}",
                group_id, topic_name, partition
            ),
            "No committed offset",
            "Committed offset exists",
        )));
    }

    Ok(())
}
