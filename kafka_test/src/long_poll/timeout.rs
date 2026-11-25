//! Long polling timeout tests
//!
//! How the broker answers a Fetch that finds no data: at once for `max_wait_ms = 0` or
//! `min_bytes = 0`, otherwise after about `max_wait_ms`. Measured at the wire level: an
//! rdkafka consumer's `recv()` does not return on an empty fetch, so a poll timed through it
//! only measures the test's own client timeout.

use crate::common::{DynResult, TestResult};
use crate::setup::TestContext;
use crate::wire::requests::{creatable, create_topics_raw, fetch_request};
use crate::wire::{decode_response, read_frame_within, request_frame};
use bytes::Bytes;
use kafka_protocol::messages::FetchRequest;
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

/// How much later than its `max_wait_ms` (or than at once) an empty Fetch may be answered:
/// the broker's own work and the test host's scheduling, well under the 1000 ms safety-net
/// re-check of a waiting fetch (`pg_kafka.fetch_poll_interval_ms`).
pub(super) const ANSWER_SLACK: Duration = Duration::from_millis(500);
/// How much earlier than its `max_wait_ms` an empty Fetch may be answered: the client's
/// clock starts before the broker's, so a broker that waits `max_wait_ms` is never early by
/// more than timer granularity.
pub(super) const EARLY_SLACK: Duration = Duration::from_millis(20);

/// A new empty topic of one partition, named after `prefix`.
pub(super) async fn empty_topic(ctx: &TestContext, prefix: &str) -> DynResult<String> {
    let topic = ctx.unique_topic(prefix).await;
    let created = create_topics_raw(vec![creatable(&topic, 1, 1)]).await?;
    if created[0].error_code != 0 {
        return Err(format!("CreateTopics answered {created:?}").into());
    }
    Ok(topic)
}

/// Fetch (v4) partition 0 of the empty `topic` from offset 0, its end, with `max_wait_ms` and
/// `min_bytes`, on `s`: how long the answer took. Fails unless the answer is an empty success.
pub(super) async fn timed_empty_fetch(
    s: &mut TcpStream,
    corr: i32,
    topic: &str,
    max_wait_ms: i32,
    min_bytes: i32,
) -> DynResult<Duration> {
    let mut req = fetch_request(topic, 0, 0, max_wait_ms);
    req.min_bytes = min_bytes;
    let started = Instant::now();
    s.write_all(&request_frame(4, corr, &req)).await?;
    let frame = read_frame_within(s, Duration::from_secs(30))
        .await?
        .ok_or("closed on fetch")?;
    let took = started.elapsed();
    let (c, resp) = decode_response::<FetchRequest>(4, &frame)?;
    let partition = &resp.responses[0].partitions[0];
    if c != corr
        || partition.error_code != 0
        || partition
            .records
            .as_ref()
            .is_some_and(|r: &Bytes| !r.is_empty())
    {
        return Err(format!(
            "expected an empty success for correlation {corr}, got {c}: error {}, {} bytes",
            partition.error_code,
            partition.records.as_ref().map_or(0, Bytes::len)
        )
        .into());
    }
    Ok(took)
}

/// An empty Fetch with `max_wait_ms = 0` is answered at once, and so is one with
/// `min_bytes = 0` whatever its `max_wait_ms` (any amount of data, none included, satisfies
/// it, as in Kafka 3.7).
pub async fn test_long_poll_immediate_return() -> TestResult {
    println!("=== Test: Long Poll Immediate Return (max_wait_ms=0, min_bytes=0) ===\n");
    let ctx = TestContext::new().await?;
    let topic = empty_topic(&ctx, "longpoll-immediate").await?;
    let mut s = TcpStream::connect(("127.0.0.1", crate::common::broker_port())).await?;
    println!("Step 1: empty topic {topic}");

    for (corr, max_wait_ms, min_bytes) in [(1, 0, 1), (2, 0, 1_000_000), (3, 5_000, 0)] {
        let took = timed_empty_fetch(&mut s, corr, &topic, max_wait_ms, min_bytes).await?;
        println!("  max_wait_ms={max_wait_ms}, min_bytes={min_bytes}: answered in {took:?}");
        assert!(
            took < ANSWER_SLACK,
            "an empty Fetch with max_wait_ms={max_wait_ms} and min_bytes={min_bytes} is answered \
             at once, took {took:?}"
        );
    }
    ctx.cleanup().await?;
    println!("\n✅ Long poll immediate return test PASSED\n");
    Ok(())
}

/// An empty Fetch with `max_wait_ms = 2000` and a `min_bytes` no data meets is answered after
/// about 2 seconds: not before, and not much later.
pub async fn test_long_poll_timeout() -> TestResult {
    println!("=== Test: Long Poll Timeout Behavior ===\n");
    let ctx = TestContext::new().await?;
    let topic = empty_topic(&ctx, "longpoll-timeout").await?;
    let mut s = TcpStream::connect(("127.0.0.1", crate::common::broker_port())).await?;
    println!("Step 1: empty topic {topic}");

    let max_wait = Duration::from_millis(2_000);
    for (corr, min_bytes) in [(1, 1), (2, 1_000_000)] {
        let took = timed_empty_fetch(&mut s, corr, &topic, 2_000, min_bytes).await?;
        println!("  max_wait_ms=2000, min_bytes={min_bytes}: answered in {took:?}");
        assert!(
            took + EARLY_SLACK >= max_wait && took < max_wait + ANSWER_SLACK,
            "an empty Fetch with max_wait_ms=2000 is answered after about 2 s, took {took:?}"
        );
    }
    ctx.cleanup().await?;
    println!("\n✅ Long poll timeout test PASSED\n");
    Ok(())
}
