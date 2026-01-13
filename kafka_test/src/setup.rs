//! Test environment setup and teardown
//!
//! Provides TestContext for test isolation with automatic cleanup via RAII.

use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

use crate::common::{get_database_url, TestResult};

/// Test context providing isolation and automatic cleanup
///
/// Each test should create its own TestContext which:
/// - Generates unique topic/group names to prevent collisions
/// - Tracks created resources for cleanup
/// - Automatically cleans up on drop
pub struct TestContext {
    /// Unique identifier for this test run
    pub test_id: String,
    /// Database client for verification and cleanup
    pub db_client: Arc<Client>,
    /// Topics created during this test
    topics_created: Arc<Mutex<Vec<String>>>,
    /// Consumer groups created during this test
    groups_created: Arc<Mutex<Vec<String>>>,
}

impl TestContext {
    /// Create a new test context
    ///
    /// Establishes a database connection and generates a unique test ID.
    pub async fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let db_url = get_database_url();
        let (client, connection) = tokio_postgres::connect(&db_url, NoTls).await?;

        // Spawn connection handler
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("PostgreSQL connection error: {}", e);
            }
        });

        Ok(Self {
            test_id: Uuid::new_v4().to_string()[..8].to_string(),
            db_client: Arc::new(client),
            topics_created: Arc::new(Mutex::new(Vec::new())),
            groups_created: Arc::new(Mutex::new(Vec::new())),
        })
    }

    /// Generate a unique topic name for this test
    ///
    /// The name is tracked for automatic cleanup.
    pub async fn unique_topic(&self, base: &str) -> String {
        let uuid_str = Uuid::new_v4().to_string();
        let name = format!("{}-{}-{}", base, self.test_id, &uuid_str[..8]);
        self.topics_created.lock().await.push(name.clone());
        name
    }

    /// Generate a unique consumer group ID for this test
    ///
    /// The group is tracked for automatic cleanup.
    pub async fn unique_group(&self, base: &str) -> String {
        let uuid_str = Uuid::new_v4().to_string();
        let name = format!("{}-{}-{}", base, self.test_id, &uuid_str[..8]);
        self.groups_created.lock().await.push(name.clone());
        name
    }

    /// Manually trigger cleanup (also called automatically on drop)
    pub async fn cleanup(&self) -> TestResult {
        // NOTE: We intentionally do NOT reset shadow mode GUCs here.
        // Resetting GUCs between tests causes race conditions where the SIGHUP
        // from the reset arrives while the next test is setting up, causing
        // shadow_mode_enabled to be false when the test expects it to be true.
        //
        // Instead, each test sets the GUCs it needs via enable_shadow_mode(),
        // and the per-topic shadow_config table controls forwarding behavior.
        // Tests that don't need shadow mode simply don't insert shadow_config rows.

        let topics = self.topics_created.lock().await;
        let groups = self.groups_created.lock().await;

        // Delete messages for test topics
        for topic in topics.iter() {
            let _ = self
                .db_client
                .execute(
                    "DELETE FROM kafka.messages WHERE topic_id IN
                     (SELECT id FROM kafka.topics WHERE name = $1)",
                    &[topic],
                )
                .await;

            // Delete topic itself
            let _ = self
                .db_client
                .execute("DELETE FROM kafka.topics WHERE name = $1", &[topic])
                .await;
        }

        // Delete consumer offsets for test groups
        for group in groups.iter() {
            let _ = self
                .db_client
                .execute(
                    "DELETE FROM kafka.consumer_offsets WHERE group_id = $1",
                    &[group],
                )
                .await;
        }

        Ok(())
    }

    /// Get a reference to the database client
    pub fn db(&self) -> &Client {
        &self.db_client
    }
}

impl Drop for TestContext {
    fn drop(&mut self) {
        // Attempt cleanup in a blocking context
        // This is best-effort - some resources may not be cleaned if the runtime is shutting down
        let topics = self.topics_created.clone();
        let groups = self.groups_created.clone();
        let db = self.db_client.clone();

        // Try to get the current runtime handle for cleanup
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let topics = topics.lock().await;
                let groups = groups.lock().await;

                for topic in topics.iter() {
                    let _ = db
                        .execute(
                            "DELETE FROM kafka.messages WHERE topic_id IN
                             (SELECT id FROM kafka.topics WHERE name = $1)",
                            &[topic],
                        )
                        .await;
                    let _ = db
                        .execute("DELETE FROM kafka.topics WHERE name = $1", &[topic])
                        .await;
                }

                for group in groups.iter() {
                    let _ = db
                        .execute(
                            "DELETE FROM kafka.consumer_offsets WHERE group_id = $1",
                            &[group],
                        )
                        .await;
                }
            });
        }
    }
}

/// Verify that the pg_kafka server is ready to accept connections
pub async fn verify_server_ready() -> TestResult {
    use rdkafka::config::ClientConfig;
    use rdkafka::producer::{FutureProducer, Producer};

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("message.timeout.ms", "5000")
        .create()?;

    // Try to get metadata - this verifies the connection
    let timeout = std::time::Duration::from_secs(5);
    producer.client().fetch_metadata(None, timeout)?;

    Ok(())
}

/// Verify database connectivity
pub async fn verify_database_ready() -> TestResult {
    let db_url = get_database_url();
    let (client, connection) = tokio_postgres::connect(&db_url, NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("PostgreSQL connection error: {}", e);
        }
    });

    // Verify kafka schema exists
    let row = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = 'kafka')",
            &[],
        )
        .await?;

    let exists: bool = row.get(0);
    if !exists {
        return Err("kafka schema does not exist".into());
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_unique_names_are_unique() {
        let ctx = TestContext::new().await.unwrap();
        let topic1 = ctx.unique_topic("test").await;
        let topic2 = ctx.unique_topic("test").await;
        assert_ne!(topic1, topic2);
    }
}
