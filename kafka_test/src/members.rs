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

use crate::common::TestResult;
use std::ops::Deref;
use std::time::Duration;
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
