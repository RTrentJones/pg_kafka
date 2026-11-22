//! Bounded shutdown for the consumers a test subscribes to a consumer group.
//!
//! Dropping an rdkafka consumer that has a `group.id` closes it: rust-rdkafka polls the
//! consumer until librdkafka reports it closed, with no deadline. librdkafka finishes a
//! close only once its LeaveGroup is answered, and a connection's responses come back in
//! request order, so after any JoinGroup of the member's still held by the coordinator.
//! The coordinator holds a JoinGroup until every member of the group has rejoined; a member
//! whose rebalance event sits unpolled in its application queue never rejoins, so it is
//! evicted only at the rebalance timeout (librdkafka's `max.poll.interval.ms`, 300 s). A
//! test that drops members one by one (`drop(a); drop(b)`, or the end of a scope) while a
//! rebalance is in progress therefore blocks in the first drop for as long as the
//! runner's per-test timeout; the drop also holds a runtime worker thread the runner's
//! abort cannot reach, since it is not at an await.
//!
//! [`Member`] owns such a consumer. [`close_all`] (or [`Member::close`]) closes members
//! each on a thread of its own and all at once, so every close polls its own consumer and
//! none waits on another member's queue, and fails the test when they have not closed by
//! a deadline. A member dropped without being closed (a failed assertion, an early
//! return) is closed off the test's thread.
//!
//! A message fetched from a member must be dropped before the member is closed: the
//! message keeps its partition referenced, and librdkafka's destroy (the last step of
//! the close) waits until it is released. The compiler does not catch one held too long
//! (`BorrowedMessage` has no drop of its own, so its borrow of the consumer ends at its
//! last use), but the close then fails at its deadline.
//!
//! [`GroupMember`] is a member for tests that need a Stable member at a known point: a
//! `BaseConsumer` the test drives itself, whose join waits until its assignment is
//! applied and whose close is bounded, and whose failures report the consumer's
//! group state.

use crate::common::{get_bootstrap_servers, DynResult, TestResult};
use rdkafka::client::ClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
use rdkafka::statistics::Statistics;
use std::fmt::Write as _;
use std::ops::Deref;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

/// The bound on closing the members of a group. A close that waits on a JoinGroup held
/// for another member resolves once that member's own close leaves the group; the slowest
/// legitimate case is a member that left without a LeaveGroup, evicted by the
/// coordinator after its session timeout (at most 10 s in these tests).
pub const CLOSE_TIMEOUT: Duration = Duration::from_secs(30);

/// A consumer whose close is bounded (see the module docs). It dereferences to the
/// consumer.
pub struct Member<C: Send + 'static> {
    consumer: Option<C>,
}

impl<C: Send + 'static> Member<C> {
    pub fn new(consumer: C) -> Self {
        Member {
            consumer: Some(consumer),
        }
    }

    /// Start closing the consumer on a thread of its own; [`close_all`] waits for it.
    pub fn closing(mut self) -> Closing {
        let consumer = self.consumer.take();
        let (done, closed) = oneshot::channel();
        std::thread::spawn(move || {
            drop(consumer);
            let _ = done.send(());
        });
        Closing { closed }
    }

    /// Close this member alone, failing after `timeout`.
    pub async fn close(self, timeout: Duration) -> TestResult {
        close_all(vec![self.closing()], timeout).await
    }
}

impl<C: Send + 'static> Deref for Member<C> {
    type Target = C;

    fn deref(&self) -> &C {
        // Only `closing` takes the consumer, and it consumes the member.
        self.consumer
            .as_ref()
            .expect("a member holds its consumer until closed")
    }
}

impl<C: Send + 'static> Drop for Member<C> {
    fn drop(&mut self) {
        if let Some(consumer) = self.consumer.take() {
            std::thread::spawn(move || drop(consumer));
        }
    }
}

/// A member's close in progress, started by [`Member::closing`].
pub struct Closing {
    closed: oneshot::Receiver<()>,
}

/// Wait until every member in `closing` (each already closing) has closed, failing with
/// the members still open after `timeout`. Members are numbered from 1 in the order
/// given.
pub async fn close_all(closing: Vec<Closing>, timeout: Duration) -> TestResult {
    let deadline = tokio::time::Instant::now() + timeout;
    let total = closing.len();
    let mut open = Vec::new();
    for (i, member) in closing.into_iter().enumerate() {
        match tokio::time::timeout_at(deadline, member.closed).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => {
                return Err(format!("consumer {} of {total} panicked closing", i + 1).into())
            }
            Err(_) => open.push(i + 1),
        }
    }
    if open.is_empty() {
        Ok(())
    } else {
        // The stalled closes finish (or not) on their own threads.
        Err(format!("consumers {open:?} of {total} did not close within {timeout:?}").into())
    }
}

/// Keeps the consumer-group part of librdkafka's latest statistics (delivered on the
/// consumer's queue, so updated whenever the member is polled): the local group and
/// join states and the requests awaiting a response per broker connection. A stalled
/// join or close reports it; nothing is printed otherwise.
#[derive(Clone, Default)]
pub struct GroupState {
    latest: Arc<Mutex<Option<String>>>,
}

impl GroupState {
    fn report(&self) -> String {
        match self.latest.lock() {
            Ok(latest) => latest
                .clone()
                .unwrap_or_else(|| "(no statistics delivered yet)".to_string()),
            Err(_) => "(statistics unavailable)".to_string(),
        }
    }
}

impl ClientContext for GroupState {
    fn stats(&self, statistics: Statistics) {
        let mut line = match &statistics.cgrp {
            Some(g) => format!(
                "group state {} ({} ms), join state {}, {} rebalance(s), last: {:?}, \
                 {} assigned partition(s)",
                g.state,
                g.stateage,
                g.join_state,
                g.rebalance_cnt,
                g.rebalance_reason,
                g.assignment_size
            ),
            None => "no consumer group state".to_string(),
        };
        let mut brokers: Vec<_> = statistics.brokers.values().collect();
        brokers.sort_by(|a, b| a.name.cmp(&b.name));
        for b in brokers {
            let _ = write!(
                line,
                "; {} {}: {} awaiting a response, {} queued",
                b.name, b.state, b.waitresp_cnt, b.outbuf_cnt
            );
        }
        if let Ok(mut latest) = self.latest.lock() {
            *latest = Some(line);
        }
    }
}

impl ConsumerContext for GroupState {}

/// One member of a consumer group whose join and close are bounded.
pub struct GroupMember {
    consumer: Member<BaseConsumer<GroupState>>,
    state: GroupState,
}

impl GroupMember {
    /// Join `group_id` subscribed to `topic`: returns once the member's assignment is
    /// applied (JoinGroup and SyncGroup answered, the rebalance event served), or fails
    /// after `timeout`.
    pub fn join(group_id: &str, topic: &str, timeout: Duration) -> DynResult<Self> {
        let state = GroupState::default();
        let consumer: BaseConsumer<GroupState> = ClientConfig::new()
            .set("bootstrap.servers", get_bootstrap_servers())
            .set("broker.address.family", "v4")
            .set("group.id", group_id)
            .set("session.timeout.ms", "10000")
            .set("heartbeat.interval.ms", "1000")
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .set("statistics.interval.ms", "500")
            .create_with_context(state.clone())?;
        // Dropped off this thread on a failure: the drop could wait on the very join
        // that failed.
        let consumer = Member::new(consumer);
        consumer.subscribe(&[topic])?;
        let deadline = Instant::now() + timeout;
        loop {
            // Serves the rebalance callback that applies the assignment.
            let _ = consumer.poll(Duration::from_millis(100));
            if consumer.assignment().is_ok_and(|a| a.count() > 0) {
                return Ok(GroupMember { consumer, state });
            }
            if Instant::now() > deadline {
                return Err(format!(
                    "no assignment in group {group_id} after {timeout:?}: {}",
                    state.report()
                )
                .into());
            }
        }
    }

    /// Leave the group (LeaveGroup) and close the consumer, failing after `timeout`.
    pub fn close(self, timeout: Duration) -> DynResult<()> {
        let GroupMember { consumer, state } = self;
        consumer.close_queue()?;
        let deadline = Instant::now() + timeout;
        while !consumer.closed() {
            if Instant::now() > deadline {
                // `consumer` is dropped off this thread, so the failure is reported now.
                return Err(format!(
                    "consumer close did not finish in {timeout:?}: {}",
                    state.report()
                )
                .into());
            }
            let _ = consumer.poll(Duration::from_millis(100));
        }
        Ok(())
    }
}
