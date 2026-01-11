//! Shadow mode test assertions
//!
//! Provides domain-specific assertions for verifying shadow mode behavior,
//! including external Kafka message verification and metrics checking.

use super::external_client::{consume_from_external, ExternalMessage};
use crate::common::TestResult;
use std::time::Duration;
use tokio_postgres::Client;

/// Assert that a message was forwarded to external Kafka
pub async fn assert_message_in_external_kafka(
    topic: &str,
    expected_key: Option<&[u8]>,
    expected_value: Option<&[u8]>,
    timeout: Duration,
) -> TestResult {
    let messages = consume_from_external(topic, 100, timeout).await?;

    let found = messages.iter().any(|msg| {
        let key_matches = match (expected_key, &msg.key) {
            (None, None) => true,
            (Some(k), Some(mk)) => k == mk.as_slice(),
            _ => false,
        };
        let value_matches = match (expected_value, &msg.value) {
            (None, None) => true,
            (Some(v), Some(mv)) => v == mv.as_slice(),
            _ => false,
        };
        key_matches && value_matches
    });

    if !found {
        return Err(format!(
            "Expected message not found in external Kafka topic '{}'. \
             Found {} messages, expected key={:?}, value={:?}",
            topic,
            messages.len(),
            expected_key.map(|k| String::from_utf8_lossy(k).to_string()),
            expected_value.map(|v| String::from_utf8_lossy(v).to_string())
        )
        .into());
    }

    Ok(())
}

/// Assert exact message count in external Kafka topic
pub async fn assert_external_message_count(
    topic: &str,
    expected_count: usize,
    timeout: Duration,
) -> TestResult {
    let messages = consume_from_external(topic, expected_count + 100, timeout).await?;

    if messages.len() != expected_count {
        return Err(format!(
            "Expected {} messages in external Kafka topic '{}', found {}",
            expected_count,
            topic,
            messages.len()
        )
        .into());
    }

    Ok(())
}

/// Assert message count in external Kafka is within a range
pub async fn assert_external_message_count_in_range(
    topic: &str,
    min_count: usize,
    max_count: usize,
    timeout: Duration,
) -> TestResult {
    let messages = consume_from_external(topic, max_count + 100, timeout).await?;

    if messages.len() < min_count || messages.len() > max_count {
        return Err(format!(
            "Expected {}-{} messages in external Kafka topic '{}', found {}",
            min_count,
            max_count,
            topic,
            messages.len()
        )
        .into());
    }

    Ok(())
}

/// Assert no messages in external Kafka topic
pub async fn assert_external_kafka_empty(topic: &str, timeout: Duration) -> TestResult {
    let messages = consume_from_external(topic, 100, timeout).await?;

    if !messages.is_empty() {
        return Err(format!(
            "Expected no messages in external Kafka topic '{}', found {}",
            topic,
            messages.len()
        )
        .into());
    }

    Ok(())
}

/// Assert local database message count for a topic
pub async fn assert_local_message_count(
    db: &Client,
    topic_name: &str,
    expected_count: i64,
) -> TestResult {
    let row = db
        .query_one(
            r#"
        SELECT COUNT(*) as count
        FROM kafka.messages m
        JOIN kafka.topics t ON m.topic_id = t.id
        WHERE t.name = $1
        "#,
            &[&topic_name],
        )
        .await?;

    let count: i64 = row.get(0);

    if count != expected_count {
        return Err(format!(
            "Expected {} messages in local DB for topic '{}', found {}",
            expected_count, topic_name, count
        )
        .into());
    }

    Ok(())
}

/// Assert shadow config exists for a topic
pub async fn assert_shadow_config_exists(db: &Client, topic_name: &str) -> TestResult {
    let row = db
        .query_opt(
            r#"
        SELECT sc.mode, sc.forward_percentage
        FROM kafka.shadow_config sc
        JOIN kafka.topics t ON sc.topic_id = t.id
        WHERE t.name = $1
        "#,
            &[&topic_name],
        )
        .await?;

    if row.is_none() {
        return Err(format!("Shadow config not found for topic '{}'", topic_name).into());
    }

    Ok(())
}

/// Assert shadow config mode
pub async fn assert_shadow_mode(
    db: &Client,
    topic_name: &str,
    expected_mode: &str,
) -> TestResult {
    let row = db
        .query_one(
            r#"
        SELECT sc.mode
        FROM kafka.shadow_config sc
        JOIN kafka.topics t ON sc.topic_id = t.id
        WHERE t.name = $1
        "#,
            &[&topic_name],
        )
        .await?;

    let mode: String = row.get(0);

    if mode != expected_mode {
        return Err(format!(
            "Expected shadow mode '{}' for topic '{}', found '{}'",
            expected_mode, topic_name, mode
        )
        .into());
    }

    Ok(())
}

/// Get all messages from external Kafka for verification
pub async fn get_external_messages(
    topic: &str,
    max_count: usize,
    timeout: Duration,
) -> Result<Vec<ExternalMessage>, Box<dyn std::error::Error>> {
    consume_from_external(topic, max_count, timeout).await
}

/// Verify message content matches between local and external
pub async fn verify_message_forwarded_correctly(
    db: &Client,
    topic_name: &str,
    partition_offset: i64,
    timeout: Duration,
) -> TestResult {
    // Get local message
    let local_row = db
        .query_one(
            r#"
        SELECT m.key, m.value
        FROM kafka.messages m
        JOIN kafka.topics t ON m.topic_id = t.id
        WHERE t.name = $1 AND m.partition_offset = $2
        "#,
            &[&topic_name, &partition_offset],
        )
        .await?;

    let local_key: Option<Vec<u8>> = local_row.get(0);
    let local_value: Option<Vec<u8>> = local_row.get(1);

    // Find matching message in external Kafka
    let external_messages = consume_from_external(topic_name, 100, timeout).await?;

    let found = external_messages.iter().any(|msg| {
        msg.key == local_key && msg.value == local_value
    });

    if !found {
        return Err(format!(
            "Message at partition_offset {} not found in external Kafka",
            partition_offset
        )
        .into());
    }

    Ok(())
}

/// Calculate forwarding percentage from message counts
pub fn calculate_forward_percentage(forwarded: usize, total: usize) -> f64 {
    if total == 0 {
        0.0
    } else {
        (forwarded as f64 / total as f64) * 100.0
    }
}

/// Assert forwarding percentage is within tolerance
pub fn assert_percentage_in_range(
    forwarded: usize,
    total: usize,
    expected_percentage: f64,
    tolerance: f64,
) -> TestResult {
    let actual = calculate_forward_percentage(forwarded, total);
    let min = expected_percentage - tolerance;
    let max = expected_percentage + tolerance;

    if actual < min || actual > max {
        return Err(format!(
            "Expected forwarding percentage {:.1}% +/- {:.1}%, got {:.1}% ({}/{})",
            expected_percentage, tolerance, actual, forwarded, total
        )
        .into());
    }

    Ok(())
}
