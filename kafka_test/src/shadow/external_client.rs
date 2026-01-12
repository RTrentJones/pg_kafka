//! External Kafka client factory
//!
//! Creates rdkafka clients configured to connect to the external Kafka
//! broker on port 9093 with SASL/PLAIN authentication.
//!
//! NOTE: There are TWO different bootstrap server addresses:
//! - EXTERNAL (localhost:9093): For host-based test code to verify messages
//! - INTERNAL (external-kafka:9094): For pg_kafka extension inside container
//!
//! The E2E tests run on the HOST and connect via localhost:9093.
//! The pg_kafka extension runs INSIDE the container and needs external-kafka:9094.

use crate::common::TestResult;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::FutureProducer;
use rdkafka::Message;
use std::env;
use std::time::Duration;

/// Get external Kafka bootstrap servers for HOST-based test verification
/// This is the EXTERNAL listener (port 9093) accessed from the host machine
pub fn get_external_bootstrap_servers() -> String {
    env::var("EXTERNAL_KAFKA_BOOTSTRAP_SERVERS").unwrap_or_else(|_| "localhost:9093".to_string())
}

/// Get internal Kafka bootstrap servers for pg_kafka extension to use
///
/// This determines the address that pg_kafka will use to connect to external Kafka:
/// - In Docker (pg_kafka in container): external-kafka:9094 (INTERNAL listener via docker network)
/// - On host/CI (pg_kafka on host): localhost:9093 (EXTERNAL listener via port forwarding)
///
/// Detection: Check if running in Docker by looking for /.dockerenv or container hostname
pub fn get_internal_bootstrap_servers() -> String {
    // Allow explicit override via environment variable
    if let Ok(servers) = env::var("INTERNAL_KAFKA_BOOTSTRAP_SERVERS") {
        return servers;
    }

    // Detect if pg_kafka is running inside Docker
    // Note: The test process runs on host, but pg_kafka extension runs wherever Postgres is
    // In CI, Postgres runs directly on the runner (not in Docker)
    // In docker-compose dev, Postgres runs inside pg_kafka_dev container

    // Check for CI environment (GitHub Actions sets CI=true)
    if env::var("CI").is_ok() {
        // CI: pg_kafka runs on host, external Kafka in Docker with port forwarding
        return "localhost:9093".to_string();
    }

    // Check if we're running in Docker (test process itself)
    // If tests run inside container, pg_kafka is also in container
    if std::path::Path::new("/.dockerenv").exists() {
        return "external-kafka:9094".to_string();
    }

    // Default: Assume host environment (local dev without docker-compose)
    // Use localhost:9093 as that's the forwarded port
    "localhost:9093".to_string()
}

/// Get SASL username for external Kafka
pub fn get_external_sasl_username() -> String {
    env::var("EXTERNAL_KAFKA_SASL_USERNAME").unwrap_or_else(|_| "test-user".to_string())
}

/// Get SASL password for external Kafka
pub fn get_external_sasl_password() -> String {
    env::var("EXTERNAL_KAFKA_SASL_PASSWORD").unwrap_or_else(|_| "test-password".to_string())
}

/// Create a producer for external Kafka (9093)
pub fn create_external_producer() -> Result<FutureProducer, Box<dyn std::error::Error>> {
    // Check if SASL is configured via environment
    let use_sasl = env::var("EXTERNAL_KAFKA_USE_SASL")
        .unwrap_or_else(|_| "false".to_string())
        .parse::<bool>()
        .unwrap_or(false);

    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", get_external_bootstrap_servers())
        .set("message.timeout.ms", "10000")
        .set("broker.address.family", "v4");

    if use_sasl {
        config
            .set("security.protocol", "SASL_PLAINTEXT")
            .set("sasl.mechanism", "PLAIN")
            .set("sasl.username", get_external_sasl_username())
            .set("sasl.password", get_external_sasl_password());
    } else {
        config.set("security.protocol", "PLAINTEXT");
    }

    let producer: FutureProducer = config.create()?;
    Ok(producer)
}

/// Create a consumer for external Kafka (9093)
pub fn create_external_consumer(
    group_id: &str,
) -> Result<BaseConsumer, Box<dyn std::error::Error>> {
    // Check if SASL is configured via environment
    let use_sasl = env::var("EXTERNAL_KAFKA_USE_SASL")
        .unwrap_or_else(|_| "false".to_string())
        .parse::<bool>()
        .unwrap_or(false);

    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", get_external_bootstrap_servers())
        .set("group.id", group_id)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set("broker.address.family", "v4");

    if use_sasl {
        config
            .set("security.protocol", "SASL_PLAINTEXT")
            .set("sasl.mechanism", "PLAIN")
            .set("sasl.username", get_external_sasl_username())
            .set("sasl.password", get_external_sasl_password());
    } else {
        config.set("security.protocol", "PLAINTEXT");
    }

    let consumer: BaseConsumer = config.create()?;
    Ok(consumer)
}

/// Verify external Kafka is available
pub async fn verify_external_kafka_ready() -> TestResult {
    use rdkafka::producer::Producer;

    let producer = create_external_producer()?;
    let timeout = Duration::from_secs(10);

    producer.client().fetch_metadata(None, timeout)?;

    Ok(())
}

/// Message consumed from external Kafka
#[derive(Debug, Clone)]
pub struct ExternalMessage {
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub partition: i32,
    pub offset: i64,
}

/// Consume messages from external Kafka topic
pub async fn consume_from_external(
    topic: &str,
    max_count: usize,
    timeout: Duration,
) -> Result<Vec<ExternalMessage>, Box<dyn std::error::Error>> {
    let group_id = format!("shadow-test-{}", uuid::Uuid::new_v4());
    let consumer = create_external_consumer(&group_id)?;

    consumer.subscribe(&[topic])?;

    let mut messages = Vec::new();
    let start = std::time::Instant::now();

    while messages.len() < max_count && start.elapsed() < timeout {
        match consumer.poll(Duration::from_millis(100)) {
            Some(Ok(msg)) => {
                messages.push(ExternalMessage {
                    key: msg.key().map(|k| k.to_vec()),
                    value: msg.payload().map(|v| v.to_vec()),
                    partition: msg.partition(),
                    offset: msg.offset(),
                });
            }
            Some(Err(e)) => {
                eprintln!("Error consuming from external Kafka: {}", e);
            }
            None => {}
        }
    }

    Ok(messages)
}

/// Count messages in external Kafka topic
pub async fn count_external_messages(
    topic: &str,
    timeout: Duration,
) -> Result<usize, Box<dyn std::error::Error>> {
    let messages = consume_from_external(topic, 10000, timeout).await?;
    Ok(messages.len())
}
