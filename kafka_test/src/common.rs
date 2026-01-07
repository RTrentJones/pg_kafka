//! Common utilities shared across E2E test modules
//!
//! This module provides shared functionality for database connections,
//! Kafka client creation, and test result types.

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, StreamConsumer};
use rdkafka::producer::FutureProducer;
use std::env;
use std::time::Duration;
use tokio_postgres::{Client, NoTls};

/// Test result type alias for cleaner function signatures
pub type TestResult = Result<(), Box<dyn std::error::Error>>;

/// Get database connection string from DATABASE_URL env var or use default
pub fn get_database_url() -> String {
    env::var("DATABASE_URL")
        .unwrap_or_else(|_| "host=localhost port=28814 user=postgres dbname=postgres".to_string())
}

/// Get Kafka bootstrap servers from KAFKA_BOOTSTRAP_SERVERS env var or use default
pub fn get_bootstrap_servers() -> String {
    env::var("KAFKA_BOOTSTRAP_SERVERS").unwrap_or_else(|_| "localhost:9092".to_string())
}

/// Create a new database client connection
pub async fn create_db_client() -> Result<Client, Box<dyn std::error::Error>> {
    let db_url = get_database_url();
    let (client, connection) = tokio_postgres::connect(&db_url, NoTls).await?;

    // Spawn connection handler
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("PostgreSQL connection error: {}", e);
        }
    });

    Ok(client)
}

/// Create a Kafka producer with default settings
pub fn create_producer() -> Result<FutureProducer, Box<dyn std::error::Error>> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", get_bootstrap_servers())
        .set("message.timeout.ms", "5000")
        .set("client.id", "test-client")
        .create()?;

    Ok(producer)
}

/// Create a Kafka producer with extended timeout for batch operations
pub fn create_batch_producer() -> Result<FutureProducer, Box<dyn std::error::Error>> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", get_bootstrap_servers())
        .set("message.timeout.ms", "30000")
        .set("batch.num.messages", "100")
        .set("linger.ms", "10")
        .create()?;

    Ok(producer)
}

/// Create a BaseConsumer with manual partition assignment capability
pub fn create_base_consumer(group_id: &str) -> Result<BaseConsumer, Box<dyn std::error::Error>> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", get_bootstrap_servers())
        .set("group.id", group_id)
        .create()?;

    Ok(consumer)
}

/// Create a BaseConsumer with manual commit settings
pub fn create_manual_commit_consumer(
    group_id: &str,
) -> Result<BaseConsumer, Box<dyn std::error::Error>> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", get_bootstrap_servers())
        .set("group.id", group_id)
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .create()?;

    Ok(consumer)
}

/// Create a StreamConsumer for subscription-based consumption
pub fn create_stream_consumer(
    group_id: &str,
) -> Result<StreamConsumer, Box<dyn std::error::Error>> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", get_bootstrap_servers())
        .set("group.id", group_id)
        .set("session.timeout.ms", "10000")
        .set("heartbeat.interval.ms", "1000")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()?;

    Ok(consumer)
}

/// Create a StreamConsumer with custom settings
pub fn create_stream_consumer_with_config(
    group_id: &str,
    session_timeout_ms: u32,
    auto_commit: bool,
    auto_offset_reset: &str,
) -> Result<StreamConsumer, Box<dyn std::error::Error>> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", get_bootstrap_servers())
        .set("group.id", group_id)
        .set("session.timeout.ms", session_timeout_ms.to_string())
        .set("enable.auto.commit", auto_commit.to_string())
        .set("auto.offset.reset", auto_offset_reset)
        .create()?;

    Ok(consumer)
}

/// Default poll timeout for consumer operations
pub const POLL_TIMEOUT: Duration = Duration::from_millis(100);

/// Default test timeout duration
pub const TEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Extended timeout for batch operations
pub const BATCH_TIMEOUT: Duration = Duration::from_secs(30);

/// Short timeout for expected-to-fail operations
pub const SHORT_TIMEOUT: Duration = Duration::from_secs(2);

/// Get test timeout from environment or use default
pub fn get_test_timeout() -> Duration {
    env::var("TEST_TIMEOUT_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .map(Duration::from_secs)
        .unwrap_or(TEST_TIMEOUT)
}

/// Get poll timeout from environment or use default
pub fn get_poll_timeout() -> Duration {
    env::var("POLL_TIMEOUT_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .map(Duration::from_millis)
        .unwrap_or(POLL_TIMEOUT)
}
