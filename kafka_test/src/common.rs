//! Common utilities shared across E2E test modules
//!
//! This module provides shared functionality for database connections,
//! Kafka client creation, and test result types.

use rdkafka::config::ClientConfig;
use rdkafka::consumer::BaseConsumer;
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
        .set("bootstrap.servers", "localhost:9092")
        .set("message.timeout.ms", "5000")
        .set("client.id", "test-client")
        .create()?;

    Ok(producer)
}

/// Create a Kafka producer with extended timeout for batch operations
pub fn create_batch_producer() -> Result<FutureProducer, Box<dyn std::error::Error>> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("message.timeout.ms", "30000")
        .set("batch.num.messages", "100")
        .set("linger.ms", "10")
        .create()?;

    Ok(producer)
}

/// Create a BaseConsumer with manual partition assignment capability
pub fn create_base_consumer(group_id: &str) -> Result<BaseConsumer, Box<dyn std::error::Error>> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", group_id)
        .create()?;

    Ok(consumer)
}

/// Create a BaseConsumer with manual commit settings
pub fn create_manual_commit_consumer(
    group_id: &str,
) -> Result<BaseConsumer, Box<dyn std::error::Error>> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", group_id)
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .create()?;

    Ok(consumer)
}

/// Default poll timeout for consumer operations
pub const POLL_TIMEOUT: Duration = Duration::from_millis(100);

/// Default test timeout duration
pub const TEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Extended timeout for batch operations
pub const BATCH_TIMEOUT: Duration = Duration::from_secs(30);
