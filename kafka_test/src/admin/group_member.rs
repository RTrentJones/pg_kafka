//! A consumer-group member for the DeleteGroups tests whose every step is bounded.
//!
//! A `StreamConsumer` that is never polled joins in librdkafka's background thread
//! but its rebalance events wait in the application queue, and dropping it runs
//! `rd_kafka_consumer_close` with no deadline (rust-rdkafka polls until the consumer
//! reports closed). A close that never completes then holds the test, silently,
//! until the runner's per-test timeout. This member is a `BaseConsumer` the test
//! polls itself: joining waits until the assignment is applied, closing waits a
//! bounded time, and a failure reports librdkafka's consumer-group log.

use crate::common::{get_bootstrap_servers, DynResult};
use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// How many of librdkafka's last log lines a failure reports.
const LOG_TAIL: usize = 60;

/// Keeps librdkafka's recent log lines (the `cgrp` debug context is enabled).
#[derive(Clone, Default)]
pub struct LogContext {
    lines: Arc<Mutex<VecDeque<String>>>,
}

impl LogContext {
    fn tail(&self) -> String {
        match self.lines.lock() {
            Ok(lines) => lines.iter().cloned().collect::<Vec<_>>().join("\n"),
            Err(_) => "(log unavailable)".to_string(),
        }
    }
}

impl ClientContext for LogContext {
    fn log(&self, _level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        if let Ok(mut lines) = self.lines.lock() {
            if lines.len() == LOG_TAIL {
                lines.pop_front();
            }
            lines.push_back(format!("{fac}: {log_message}"));
        }
    }
}

impl ConsumerContext for LogContext {}

/// One member of a consumer group.
pub struct GroupMember {
    consumer: Option<BaseConsumer<LogContext>>,
    context: LogContext,
}

impl GroupMember {
    /// Join `group_id` subscribed to `topic`: returns once the member's assignment is
    /// applied (JoinGroup and SyncGroup answered, the rebalance event served), or fails
    /// after `timeout`.
    pub fn join(group_id: &str, topic: &str, timeout: Duration) -> DynResult<Self> {
        let context = LogContext::default();
        let consumer: BaseConsumer<LogContext> = ClientConfig::new()
            .set("bootstrap.servers", get_bootstrap_servers())
            .set("broker.address.family", "v4")
            .set("group.id", group_id)
            .set("session.timeout.ms", "10000")
            .set("heartbeat.interval.ms", "1000")
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .set("debug", "cgrp")
            .create_with_context(context.clone())?;
        consumer.subscribe(&[topic])?;
        let deadline = Instant::now() + timeout;
        loop {
            // Serves the rebalance callback that applies the assignment.
            let _ = consumer.poll(Duration::from_millis(100));
            if consumer.assignment().is_ok_and(|a| a.count() > 0) {
                return Ok(GroupMember {
                    consumer: Some(consumer),
                    context,
                });
            }
            if Instant::now() > deadline {
                let log = context.tail();
                // Not dropped here: the drop could wait on the very join that failed.
                std::thread::spawn(move || drop(consumer));
                return Err(
                    format!("no assignment in group {group_id} after {timeout:?}\n{log}").into(),
                );
            }
        }
    }

    /// Leave the group (LeaveGroup) and close the consumer, failing after `timeout`.
    pub fn close(mut self, timeout: Duration) -> DynResult<()> {
        let Some(consumer) = self.consumer.take() else {
            return Ok(());
        };
        consumer.close_queue()?;
        let deadline = Instant::now() + timeout;
        while !consumer.closed() {
            if Instant::now() > deadline {
                let log = self.context.tail();
                // Dropping a consumer that is not closed blocks until it is; leave
                // that to a thread of its own so the failure is reported now.
                std::thread::spawn(move || drop(consumer));
                return Err(format!("consumer close did not finish in {timeout:?}\n{log}").into());
            }
            let _ = consumer.poll(Duration::from_millis(100));
        }
        drop(consumer);
        Ok(())
    }
}

impl Drop for GroupMember {
    fn drop(&mut self) {
        // A member not closed (an assertion failed first) is dropped off the
        // test's thread, for the same reason as in `close`.
        if let Some(consumer) = self.consumer.take() {
            std::thread::spawn(move || drop(consumer));
        }
    }
}
