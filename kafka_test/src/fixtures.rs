//! Test fixtures and data builders
//!
//! Provides builder patterns for creating test data with sensible defaults.

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;

use crate::common::TestResult;
use crate::setup::TestContext;

/// Builder for creating test messages
pub struct TestMessageBuilder {
    key: Option<String>,
    value: String,
    partition: Option<i32>,
}

impl TestMessageBuilder {
    pub fn new(value: impl Into<String>) -> Self {
        Self {
            key: None,
            value: value.into(),
            partition: None,
        }
    }

    pub fn with_key(mut self, key: impl Into<String>) -> Self {
        self.key = Some(key.into());
        self
    }

    pub fn with_partition(mut self, partition: i32) -> Self {
        self.partition = Some(partition);
        self
    }

    pub fn build(self) -> TestMessage {
        TestMessage {
            key: self.key,
            value: self.value,
            partition: self.partition,
        }
    }
}

/// A test message to be produced
#[derive(Clone, Debug)]
pub struct TestMessage {
    pub key: Option<String>,
    pub value: String,
    pub partition: Option<i32>,
}

impl TestMessage {
    pub fn new(value: impl Into<String>) -> Self {
        Self {
            key: None,
            value: value.into(),
            partition: None,
        }
    }

    pub fn with_key(value: impl Into<String>, key: impl Into<String>) -> Self {
        Self {
            key: Some(key.into()),
            value: value.into(),
            partition: None,
        }
    }
}

/// Generate N test messages with sequential keys and values
pub fn generate_messages(count: usize, prefix: &str) -> Vec<TestMessage> {
    (0..count)
        .map(|i| TestMessage {
            key: Some(format!("{}-key-{}", prefix, i)),
            value: format!("{}-value-{}", prefix, i),
            partition: None,
        })
        .collect()
}

/// Generate messages for specific partitions
pub fn generate_partitioned_messages(
    partitions: i32,
    messages_per_partition: usize,
    prefix: &str,
) -> Vec<TestMessage> {
    let mut messages = Vec::new();
    for p in 0..partitions {
        for i in 0..messages_per_partition {
            messages.push(TestMessage {
                key: Some(format!("{}-p{}-key-{}", prefix, p, i)),
                value: format!("{}-p{}-value-{}", prefix, p, i),
                partition: Some(p),
            });
        }
    }
    messages
}

/// Builder for test topics
pub struct TestTopicBuilder<'a> {
    ctx: &'a TestContext,
    base_name: String,
    partitions: i32,
}

impl<'a> TestTopicBuilder<'a> {
    pub fn new(ctx: &'a TestContext, base_name: impl Into<String>) -> Self {
        Self {
            ctx,
            base_name: base_name.into(),
            partitions: 1,
        }
    }

    pub fn with_partitions(mut self, partitions: i32) -> Self {
        self.partitions = partitions;
        self
    }

    pub async fn build(self) -> Result<TestTopic, Box<dyn std::error::Error>> {
        let name = self.ctx.unique_topic(&self.base_name).await;

        // Create topic in database with specified partition count
        self.ctx
            .db()
            .execute(
                "INSERT INTO kafka.topics (name, partitions) VALUES ($1, $2)
                 ON CONFLICT (name) DO UPDATE SET partitions = $2",
                &[&name, &self.partitions],
            )
            .await?;

        Ok(TestTopic {
            name,
            partitions: self.partitions,
        })
    }
}

/// A test topic that has been created
#[derive(Clone, Debug)]
pub struct TestTopic {
    pub name: String,
    pub partitions: i32,
}

impl TestTopic {
    /// Produce messages to this topic
    pub async fn produce(
        &self,
        messages: &[TestMessage],
    ) -> Result<Vec<i64>, Box<dyn std::error::Error>> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .set("message.timeout.ms", "10000")
            .create()?;

        let mut offsets = Vec::new();

        for msg in messages {
            let mut record = FutureRecord::to(&self.name).payload(&msg.value);

            if let Some(ref key) = msg.key {
                record = record.key(key);
            }

            if let Some(partition) = msg.partition {
                record = record.partition(partition);
            }

            let (_, offset) = producer
                .send(record, Duration::from_secs(10))
                .await
                .map_err(|(e, _)| e)?;

            offsets.push(offset);
        }

        Ok(offsets)
    }

    /// Produce a single message and return its offset
    pub async fn produce_one(&self, msg: &TestMessage) -> Result<i64, Box<dyn std::error::Error>> {
        let offsets = self.produce(&[msg.clone()]).await?;
        Ok(offsets[0])
    }
}

/// Builder for test consumers
pub struct TestConsumerBuilder<'a> {
    ctx: &'a TestContext,
    base_group: String,
    auto_commit: bool,
    session_timeout_ms: u32,
    auto_offset_reset: String,
}

impl<'a> TestConsumerBuilder<'a> {
    pub fn new(ctx: &'a TestContext, base_group: impl Into<String>) -> Self {
        Self {
            ctx,
            base_group: base_group.into(),
            auto_commit: false,
            session_timeout_ms: 10000,
            auto_offset_reset: "earliest".to_string(),
        }
    }

    pub fn with_auto_commit(mut self, enabled: bool) -> Self {
        self.auto_commit = enabled;
        self
    }

    pub fn with_session_timeout(mut self, ms: u32) -> Self {
        self.session_timeout_ms = ms;
        self
    }

    pub fn with_auto_offset_reset(mut self, reset: impl Into<String>) -> Self {
        self.auto_offset_reset = reset.into();
        self
    }

    pub async fn build_base(self) -> Result<(BaseConsumer, String), Box<dyn std::error::Error>> {
        let group_id = self.ctx.unique_group(&self.base_group).await;

        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .set("group.id", &group_id)
            .set("session.timeout.ms", self.session_timeout_ms.to_string())
            .set("enable.auto.commit", self.auto_commit.to_string())
            .set("auto.offset.reset", &self.auto_offset_reset)
            .create()?;

        Ok((consumer, group_id))
    }

    pub async fn build_stream(
        self,
    ) -> Result<(StreamConsumer, String), Box<dyn std::error::Error>> {
        let group_id = self.ctx.unique_group(&self.base_group).await;

        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .set("group.id", &group_id)
            .set("session.timeout.ms", self.session_timeout_ms.to_string())
            .set("enable.auto.commit", self.auto_commit.to_string())
            .set("auto.offset.reset", &self.auto_offset_reset)
            .create()?;

        Ok((consumer, group_id))
    }
}

/// Builder for test producers
pub struct TestProducerBuilder {
    timeout_ms: u32,
    batch_size: Option<u32>,
    linger_ms: Option<u32>,
}

impl Default for TestProducerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl TestProducerBuilder {
    pub fn new() -> Self {
        Self {
            timeout_ms: 5000,
            batch_size: None,
            linger_ms: None,
        }
    }

    pub fn with_timeout(mut self, ms: u32) -> Self {
        self.timeout_ms = ms;
        self
    }

    pub fn with_batching(mut self, batch_size: u32, linger_ms: u32) -> Self {
        self.batch_size = Some(batch_size);
        self.linger_ms = Some(linger_ms);
        self
    }

    pub fn build(self) -> Result<FutureProducer, Box<dyn std::error::Error>> {
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", "localhost:9092");
        config.set("message.timeout.ms", self.timeout_ms.to_string());

        if let Some(batch_size) = self.batch_size {
            config.set("batch.num.messages", batch_size.to_string());
        }

        if let Some(linger_ms) = self.linger_ms {
            config.set("linger.ms", linger_ms.to_string());
        }

        Ok(config.create()?)
    }
}

/// Helper to wait for a condition with timeout
pub async fn wait_for<F, Fut>(
    timeout: Duration,
    poll_interval: Duration,
    condition: F,
) -> TestResult
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let start = std::time::Instant::now();

    while start.elapsed() < timeout {
        if condition().await {
            return Ok(());
        }
        tokio::time::sleep(poll_interval).await;
    }

    Err("Timeout waiting for condition".into())
}
