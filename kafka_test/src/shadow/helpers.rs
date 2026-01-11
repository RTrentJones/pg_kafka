//! Shadow mode test helpers
//!
//! Provides utilities for enabling/disabling shadow mode, configuring topics,
//! and verifying forwarding behavior.

use crate::common::TestResult;
use tokio_postgres::Client;

/// Shadow mode configuration for a topic
#[derive(Debug, Clone)]
pub struct ShadowTopicConfig {
    pub mode: ShadowMode,
    pub forward_percentage: u8,
    pub external_topic_name: Option<String>,
    pub sync_mode: SyncMode,
    pub write_mode: WriteMode,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ShadowMode {
    LocalOnly,
    Shadow,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SyncMode {
    Async,
    Sync,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum WriteMode {
    DualWrite,
    ExternalOnly,
}

impl Default for ShadowTopicConfig {
    fn default() -> Self {
        Self {
            mode: ShadowMode::Shadow,
            forward_percentage: 100,
            external_topic_name: None,
            sync_mode: SyncMode::Sync,
            write_mode: WriteMode::DualWrite,
        }
    }
}

/// Enable shadow mode for a topic
pub async fn enable_shadow_mode(
    db: &Client,
    topic_name: &str,
    config: &ShadowTopicConfig,
) -> TestResult {
    // First ensure topic exists
    let topic_id = get_or_create_topic_id(db, topic_name).await?;

    let mode_str = match config.mode {
        ShadowMode::LocalOnly => "local_only",
        ShadowMode::Shadow => "shadow",
    };
    let sync_mode_str = match config.sync_mode {
        SyncMode::Async => "async",
        SyncMode::Sync => "sync",
    };
    let write_mode_str = match config.write_mode {
        WriteMode::DualWrite => "dual_write",
        WriteMode::ExternalOnly => "external_only",
    };

    db.execute(
        r#"
        INSERT INTO kafka.shadow_config
            (topic_id, mode, forward_percentage, external_topic_name, sync_mode, write_mode, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, NOW())
        ON CONFLICT (topic_id) DO UPDATE SET
            mode = EXCLUDED.mode,
            forward_percentage = EXCLUDED.forward_percentage,
            external_topic_name = EXCLUDED.external_topic_name,
            sync_mode = EXCLUDED.sync_mode,
            write_mode = EXCLUDED.write_mode,
            updated_at = NOW()
        "#,
        &[
            &topic_id,
            &mode_str,
            &(config.forward_percentage as i32),
            &config.external_topic_name,
            &sync_mode_str,
            &write_mode_str,
        ],
    )
    .await?;

    Ok(())
}

/// Disable shadow mode for a topic
pub async fn disable_shadow_mode(db: &Client, topic_name: &str) -> TestResult {
    db.execute(
        r#"
        UPDATE kafka.shadow_config sc
        SET mode = 'local_only', updated_at = NOW()
        FROM kafka.topics t
        WHERE sc.topic_id = t.id AND t.name = $1
        "#,
        &[&topic_name],
    )
    .await?;
    Ok(())
}

/// Update forward percentage for a topic
pub async fn set_forward_percentage(db: &Client, topic_name: &str, percentage: u8) -> TestResult {
    db.execute(
        r#"
        UPDATE kafka.shadow_config sc
        SET forward_percentage = $2, updated_at = NOW()
        FROM kafka.topics t
        WHERE sc.topic_id = t.id AND t.name = $1
        "#,
        &[&topic_name, &(percentage as i32)],
    )
    .await?;
    Ok(())
}

/// Get or create topic ID
async fn get_or_create_topic_id(
    db: &Client,
    topic_name: &str,
) -> Result<i32, Box<dyn std::error::Error>> {
    // Try to get existing topic
    let row = db
        .query_opt(
            "SELECT id FROM kafka.topics WHERE name = $1",
            &[&topic_name],
        )
        .await?;

    match row {
        Some(r) => Ok(r.get(0)),
        None => {
            // Create topic
            let row = db
                .query_one(
                    "INSERT INTO kafka.topics (name, partitions) VALUES ($1, 1) RETURNING id",
                    &[&topic_name],
                )
                .await?;
            Ok(row.get(0))
        }
    }
}

/// Shadow metrics snapshot
#[derive(Debug, Default)]
pub struct ShadowMetricsSnapshot {
    pub messages_forwarded: i64,
    pub messages_skipped: i64,
    pub messages_failed: i64,
    pub fallback_local: i64,
}

/// Get shadow metrics for a topic from the in-memory metrics
///
/// Note: Since shadow metrics are stored in-memory in the Rust code (ShadowMetrics struct),
/// we verify forwarding by counting messages in the external Kafka cluster.
/// This function queries the messages table to count what was forwarded.
pub async fn get_shadow_metrics(
    db: &Client,
    topic_name: &str,
) -> Result<ShadowMetricsSnapshot, Box<dyn std::error::Error>> {
    // Count local messages for this topic
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

    let local_count: i64 = row.get(0);

    // For now, return a placeholder - actual metrics come from external Kafka consumption
    Ok(ShadowMetricsSnapshot {
        messages_forwarded: 0, // Will be set by external Kafka verification
        messages_skipped: 0,
        messages_failed: 0,
        fallback_local: local_count,
    })
}

/// Wait for shadow forwarding to complete (with timeout)
///
/// Polls the external Kafka to wait for expected message count.
pub async fn wait_for_forwarding(
    _db: &Client,
    topic_name: &str,
    expected_count: usize,
    timeout: std::time::Duration,
) -> TestResult {
    use super::external_client::consume_from_external;

    let start = std::time::Instant::now();

    while start.elapsed() < timeout {
        let messages = consume_from_external(topic_name, expected_count, timeout).await?;
        if messages.len() >= expected_count {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }

    Err(format!(
        "Timeout waiting for {} messages to be forwarded to external Kafka",
        expected_count
    )
    .into())
}
