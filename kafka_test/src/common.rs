//! Common utilities shared across E2E test modules
//!
//! This module provides shared functionality for database connections,
//! Kafka client creation, and test result types.

use rdkafka::admin::AdminClient;
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, StreamConsumer};
use rdkafka::producer::FutureProducer;
use std::env;
use std::time::Duration;
use tokio_postgres::{Client, NoTls};

/// The error-carrying result of test code.
pub type DynResult<T> = Result<T, Box<dyn std::error::Error>>;

/// What every test returns.
pub type TestResult = DynResult<()>;

/// A count or index of test records as an `i64` (database counts, offsets).
pub fn to_i64(n: usize) -> i64 {
    i64::try_from(n).expect("a test count fits in i64")
}

/// A count or index of test requests as an `i32` (correlation ids, sizes).
pub fn to_i32(n: usize) -> i32 {
    i32::try_from(n).expect("a test count fits in i32")
}

/// A count of test records as an `f64`, for rates and percentages.
#[expect(
    clippy::cast_precision_loss,
    reason = "test counts are far below 2^53, where every integer is an exact f64"
)]
pub fn to_f64(n: usize) -> f64 {
    n as f64
}

/// An rdkafka admin client for the broker under test.
pub fn create_admin_client() -> DynResult<AdminClient<DefaultClientContext>> {
    Ok(ClientConfig::new()
        .set("bootstrap.servers", get_bootstrap_servers())
        .set("broker.address.family", "v4")
        .create()?)
}

/// Get database connection string from DATABASE_URL env var or use default
pub fn get_database_url() -> String {
    env::var("DATABASE_URL")
        .unwrap_or_else(|_| "host=localhost port=28814 user=postgres dbname=postgres".to_string())
}

/// The broker under test when `KAFKA_BOOTSTRAP_SERVERS` is unset: pg_kafka's default
/// listener (`pg_kafka.port` 9092) on this host. Every test reaches the broker through
/// [`get_bootstrap_servers`], [`broker_addr`] or [`broker_port`], never a literal address.
pub const DEFAULT_BOOTSTRAP_SERVERS: &str = "localhost:9092";

/// The broker under test as an rdkafka `bootstrap.servers` list: `KAFKA_BOOTSTRAP_SERVERS`,
/// or [`DEFAULT_BOOTSTRAP_SERVERS`].
pub fn get_bootstrap_servers() -> String {
    env::var("KAFKA_BOOTSTRAP_SERVERS").unwrap_or_else(|_| DEFAULT_BOOTSTRAP_SERVERS.to_string())
}

/// The first `host:port` of [`get_bootstrap_servers`], for raw TCP connections
/// (`TcpStream::connect(broker_addr())`). A `PLAINTEXT://` prefix is dropped.
pub fn broker_addr() -> String {
    let servers = get_bootstrap_servers();
    let first = servers.split(',').next().unwrap_or_default().trim();
    first
        .rsplit_once("://")
        .map_or(first, |(_, addr)| addr)
        .to_string()
}

/// The port of [`broker_addr`], for tests that connect to the listener on the database
/// host's loopback addresses (`network`, `auth`).
pub fn broker_port() -> u16 {
    let addr = broker_addr();
    addr.rsplit_once(':')
        .and_then(|(_, port)| port.parse().ok())
        .unwrap_or_else(|| panic!("KAFKA_BOOTSTRAP_SERVERS: no port in {addr:?}"))
}

/// Create a new database client connection
pub async fn create_db_client() -> Result<Client, Box<dyn std::error::Error>> {
    let db_url = get_database_url();
    let (client, connection) = tokio_postgres::connect(&db_url, NoTls).await?;

    // Spawn connection handler
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("PostgreSQL connection error: {e}");
        }
    });

    Ok(client)
}

/// The columns of `table` that some index of it keys, or names in its predicate or an
/// expression: an UPDATE that changes any of them is never HOT, so it writes an entry in
/// every index of the table.
pub async fn hot_blocking_columns(db: &Client, table: &str) -> DynResult<Vec<String>> {
    Ok(db
        .query(
            r"SELECT DISTINCT a.attname::text
             FROM pg_index i
             JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum > 0
             WHERE i.indrelid = $1::text::regclass
               AND (a.attnum = ANY (i.indkey::int2[])
                    OR COALESCE(pg_get_expr(i.indpred, i.indrelid), '')
                           ~ ('\m' || a.attname || '\M')
                    OR COALESCE(pg_get_expr(i.indexprs, i.indrelid), '')
                           ~ ('\m' || a.attname || '\M'))
             ORDER BY 1",
            &[&table],
        )
        .await?
        .iter()
        .map(|r| r.get(0))
        .collect())
}

/// Create a Kafka producer with default settings
pub fn create_producer() -> Result<FutureProducer, Box<dyn std::error::Error>> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", get_bootstrap_servers())
        .set("broker.address.family", "v4")
        .set("message.timeout.ms", "5000")
        .set("client.id", "test-client")
        .create()?;

    Ok(producer)
}

/// Create a Kafka producer with extended timeout for batch operations
pub fn create_batch_producer() -> Result<FutureProducer, Box<dyn std::error::Error>> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", get_bootstrap_servers())
        .set("broker.address.family", "v4")
        .set("message.timeout.ms", "30000")
        .set("batch.num.messages", "100")
        .set("linger.ms", "10")
        .create()?;

    Ok(producer)
}

/// Create an idempotent Kafka producer (Phase 9)
pub fn create_idempotent_producer() -> Result<FutureProducer, Box<dyn std::error::Error>> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", get_bootstrap_servers())
        .set("broker.address.family", "v4")
        .set("message.timeout.ms", "5000")
        .set("enable.idempotence", "true")
        .set("max.in.flight.requests.per.connection", "5")
        .set("retries", "3")
        .create()?;

    Ok(producer)
}

/// Create a BaseConsumer with manual partition assignment capability
pub fn create_base_consumer(group_id: &str) -> Result<BaseConsumer, Box<dyn std::error::Error>> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", get_bootstrap_servers())
        .set("broker.address.family", "v4")
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
        .set("broker.address.family", "v4")
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
        .set("broker.address.family", "v4")
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
        .set("broker.address.family", "v4")
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
        .map_or(TEST_TIMEOUT, Duration::from_secs)
}

/// Get poll timeout from environment or use default
pub fn get_poll_timeout() -> Duration {
    env::var("POLL_TIMEOUT_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .map_or(POLL_TIMEOUT, Duration::from_millis)
}

// ========== Transaction Support (Phase 10) ==========

/// Create a transactional Kafka producer
///
/// Transactional producers enable exactly-once semantics by:
/// - Atomic writes across multiple partitions
/// - Integration with consumer offset commits
pub fn create_transactional_producer(
    transactional_id: &str,
) -> Result<FutureProducer, Box<dyn std::error::Error>> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", get_bootstrap_servers())
        .set("broker.address.family", "v4")
        .set("message.timeout.ms", "5000")
        .set("transactional.id", transactional_id)
        .set("enable.idempotence", "true") // Required for transactions
        .create()?;

    Ok(producer)
}

/// Create a consumer with read_committed isolation level
///
/// This consumer only sees committed transactional messages,
/// filtering out pending and aborted transactions.
pub fn create_read_committed_consumer(
    group_id: &str,
) -> Result<BaseConsumer, Box<dyn std::error::Error>> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", get_bootstrap_servers())
        .set("broker.address.family", "v4")
        .set("group.id", group_id)
        .set("isolation.level", "read_committed")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()?;

    Ok(consumer)
}

/// Create a consumer with read_uncommitted isolation level (default)
///
/// This consumer sees all messages including pending transactions.
pub fn create_read_uncommitted_consumer(
    group_id: &str,
) -> Result<BaseConsumer, Box<dyn std::error::Error>> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", get_bootstrap_servers())
        .set("broker.address.family", "v4")
        .set("group.id", group_id)
        .set("isolation.level", "read_uncommitted")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()?;

    Ok(consumer)
}
