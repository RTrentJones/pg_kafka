//! What a Fetch may return, and what a connection's unwritten responses may hold.
//!
//! `pg_kafka.fetch_max_bytes` caps every Fetch response whatever the request's `max_bytes`
//! (Kafka's broker `fetch.max.bytes`); the tests lower it so a few hundred KB show the cap.

use crate::common::{create_db_client, DynResult, TestResult};
use crate::setup::{GucError, SystemGucGuard, TestContext};
use crate::wire::records::{fetched_values, plain_batch};
use crate::wire::requests::{creatable, create_topics_raw, fetch_request, produce_request};
use crate::wire::{decode_response, read_frame_within, request_frame};
use bytes::Bytes;
use kafka_protocol::messages::{FetchRequest, FetchResponse, ProduceRequest};
use kafka_protocol::records::Compression;
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio_postgres::Client;

/// The GUC the tests lower.
const GUC: &str = "pg_kafka.fetch_max_bytes";

/// What `test_fetch_responses_are_capped_by_fetch_max_bytes` lowers it to.
const CAP: i32 = 64 * 1024;

/// Set `pg_kafka.fetch_max_bytes` for a test with `ALTER SYSTEM`, then reload. The returned
/// guard puts the earlier setting back: the test calls `restore` on success, and the guard's
/// drop restores it when an assert panics, an error returns early or the runner aborts the
/// test at its timeout. The network thread reads the new value for the next Fetch it reads;
/// a connection's response budget is read when it opens.
pub async fn set_fetch_max_bytes(db: &Client, value: i32) -> DynResult<SystemGucGuard> {
    SystemGucGuard::set(db, GUC, &value.to_string())
        .await
        .map_err(widen)?
        .ok_or_else(|| format!("the server refused {GUC} = {value}").into())
}

/// Put `pg_kafka.fetch_max_bytes` back (see [`set_fetch_max_bytes`]).
pub async fn restore_fetch_max_bytes(guard: SystemGucGuard) -> DynResult<()> {
    guard.restore().await.map_err(widen)
}

/// The guard's errors are `Send + Sync`; this file's results are not.
fn widen(e: GucError) -> Box<dyn std::error::Error> {
    e
}

/// Produce `values` to partition 0 of `topic` (one batch, acks=1) on `s`: one value, or
/// several the encoder keeps in one batch.
async fn produce(s: &mut TcpStream, corr: i32, topic: &str, values: &[Bytes]) -> DynResult<()> {
    let req = produce_request(topic, vec![(0, plain_batch(values, Compression::None))], 1);
    s.write_all(&request_frame(3, corr, &req)).await?;
    let frame = read_frame_within(s, Duration::from_secs(20))
        .await?
        .ok_or("closed on produce")?;
    let (_, resp) = decode_response::<ProduceRequest>(3, &frame)?;
    let code = resp.responses[0].partition_responses[0].error_code;
    if code != 0 {
        return Err(format!("produce answered {code}").into());
    }
    Ok(())
}

/// A Fetch v4 of partition 0 from `offset` that asks for everything: `max_bytes` and
/// `partition_max_bytes` of `i32::MAX`, and `min_bytes` as given.
fn fetch_everything(topic: &str, offset: i64, max_wait_ms: i32, min_bytes: i32) -> FetchRequest {
    let mut req = fetch_request(topic, 0, offset, max_wait_ms);
    req.max_bytes = i32::MAX;
    req.min_bytes = min_bytes;
    req.topics[0].partitions[0].partition_max_bytes = i32::MAX;
    req
}

/// The records partition 0 of a Fetch returned: (their encoded bytes, offset and value).
async fn fetch_records(
    s: &mut TcpStream,
    corr: i32,
    req: &FetchRequest,
) -> DynResult<(usize, Vec<(i64, String)>)> {
    s.write_all(&request_frame(4, corr, req)).await?;
    let frame = read_frame_within(s, Duration::from_secs(30))
        .await?
        .ok_or("closed on fetch")?;
    let (c, resp) = decode_response::<FetchRequest>(4, &frame)?;
    let resp: FetchResponse = resp;
    assert_eq!(c, corr);
    let partition = &resp.responses[0].partitions[0];
    assert_eq!(
        partition.error_code, 0,
        "fetch error {}",
        partition.error_code
    );
    let records = partition.records.clone().unwrap_or_default();
    Ok((records.len(), fetched_values(&records)?))
}

/// With `pg_kafka.fetch_max_bytes` at 64 KiB, a Fetch whose `max_bytes`,
/// `partition_max_bytes` and `min_bytes` are `i32::MAX` returns at most 64 KiB of 300 KB of
/// records, and at once (its `min_bytes` is lowered to the cap, which the data meets, as
/// Kafka lowers it), not after `max_wait_ms`. A record larger than the cap is still returned
/// whole when it is the first, so the consumer makes progress (KIP-74).
pub async fn test_fetch_responses_are_capped_by_fetch_max_bytes() -> TestResult {
    println!("=== Test: Fetch responses are capped by pg_kafka.fetch_max_bytes ===\n");
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("net-fetch-cap").await;
    let db = create_db_client().await?;
    let guc = set_fetch_max_bytes(&db, CAP).await?;
    {
        let created = create_topics_raw(vec![creatable(&topic, 1, 1)]).await?;
        assert_eq!(created[0].error_code, 0, "{created:?}");
        let mut s = TcpStream::connect(("127.0.0.1", super::port())).await?;
        // One record per request: each Produce partition carries exactly one batch.
        for i in 0..30u8 {
            let value = Bytes::from(vec![b'a' + i % 26; 10_000]);
            produce(&mut s, i32::from(i), &topic, &[value]).await?;
        }
        produce(&mut s, 30, &topic, &[Bytes::from(vec![b'z'; 100_000])]).await?;
        println!("Step 1: 30 records of 10 KB and one of 100 KB produced");

        let started = Instant::now();
        let (bytes, records) =
            fetch_records(&mut s, 3, &fetch_everything(&topic, 0, 5000, i32::MAX)).await?;
        let took = started.elapsed();
        println!(
            "Step 2: max_bytes = i32::MAX returned {} records, {bytes} bytes, in {took:?}",
            records.len()
        );
        assert!(
            usize::try_from(CAP).is_ok_and(|cap| bytes <= cap),
            "{bytes} record bytes returned, more than the {CAP} byte cap"
        );
        assert!(
            !records.is_empty() && records.len() < 30,
            "the cap cuts the response short: {} records",
            records.len()
        );
        assert!(
            took < Duration::from_secs(3),
            "min_bytes is lowered to the cap: answered after {took:?}, not at max_wait_ms"
        );

        let (bytes, records) =
            fetch_records(&mut s, 4, &fetch_everything(&topic, 30, 0, 1)).await?;
        println!(
            "Step 3: from the 100 KB record: {} records, {bytes} bytes",
            records.len()
        );
        assert_eq!(
            records.len(),
            1,
            "the first record is returned although larger"
        );
        assert_eq!(records[0].0, 30);
        assert_eq!(records[0].1.len(), 100_000);
    }
    restore_fetch_max_bytes(guc).await?;
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// A client that reads nothing sends a long-poll Fetch that waits (the head of its
/// connection), 30 Fetches behind it of records of 900 KB each, then 300 KB of Metadata
/// requests. With `pg_kafka.fetch_max_bytes` at 1 MiB (the connection's response budget), the
/// broker reads the first Fetch behind the long poll, whose response then waits unwritten,
/// and stops reading there: the rest stays in its socket's receive queue instead of 30
/// responses of 900 KB being held. And though the long poll is still waiting, a response
/// waits for the client to read it: with `pg_kafka.connections_max_idle_ms` at `idle` (the
/// `limits` setup) the connection closes once nothing was read or written for that long, as
/// Kafka's idle expiry closes it, not when the long poll's 60 s `max_wait_ms` are up.
pub async fn non_reading_client_behind_a_long_poll_closes_after(idle: Duration) -> TestResult {
    use super::stalled_writers::{
        broker_side_state, broker_side_unread, metadata_of, non_reading, ESTABLISHED,
    };
    println!("=== Test: a non-reading client behind a long poll ===\n");
    let ctx = TestContext::new().await?;
    let data = ctx.unique_topic("net-nr-data").await;
    let quiet = ctx.unique_topic("net-nr-lp").await;
    let db = create_db_client().await?;
    let guc = set_fetch_max_bytes(&db, 1 << 20).await?;
    {
        let created =
            create_topics_raw(vec![creatable(&data, 1, 1), creatable(&quiet, 1, 1)]).await?;
        assert!(created.iter().all(|t| t.error_code == 0), "{created:?}");
        let mut s = TcpStream::connect(("127.0.0.1", super::port())).await?;
        for i in 0..4 {
            produce(&mut s, i, &data, &[Bytes::from(vec![b'd'; 900_000])]).await?;
        }
        produce(&mut s, 9, &quiet, &[Bytes::from_static(b"seed")]).await?;
        println!("Step 1: 4 records of 900 KB produced");

        let mut hog = non_reading(std::net::Ipv4Addr::new(127, 0, 0, 31)).await?;
        let client = hog.local_addr()?;
        let mut frames = request_frame(4, 0, &fetch_everything(&quiet, 1, 60_000, 1));
        for corr in 1..=30 {
            frames.extend(request_frame(4, corr, &fetch_everything(&data, 0, 0, 1)));
        }
        let prefix = uuid::Uuid::new_v4().simple().to_string();
        for corr in 100..110 {
            let names = (0..1000).map(|i| format!("{prefix}-{corr}-{i:0>8}"));
            frames.extend(request_frame(4, corr, &metadata_of(names)));
        }
        println!("  {} bytes of requests, reading nothing", frames.len());
        let started = Instant::now();
        let writer = tokio::spawn(async move {
            let _ = hog.write_all(&frames).await;
            // Kept open, reading nothing, until the test ends.
            tokio::time::sleep(Duration::from_secs(120)).await;
            drop(hog);
        });
        let _abort = AbortOnDrop(writer);

        tokio::time::sleep(Duration::from_millis(1500)).await;
        let unread = broker_side_unread(client)?.ok_or("the connection is gone")?;
        println!("Step 2: {unread} bytes the broker has not read");
        assert!(
            unread > 100_000,
            "the broker read on ({unread} bytes unread) while 900 KB responses wait unread"
        );

        let mut closed_after = None;
        while started.elapsed() < idle * 3 + Duration::from_secs(10) {
            if broker_side_state(client)? != Some(ESTABLISHED) {
                closed_after = Some(started.elapsed());
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let t = closed_after.ok_or("the connection stayed open while its long poll waited")?;
        println!("Step 3: the broker closed it after {t:?}");
        assert!(
            t >= idle.saturating_sub(Duration::from_millis(500)),
            "closed after {t:?}, before the idle timeout of {idle:?}"
        );
        let mut ok = TcpStream::connect(("127.0.0.1", super::port())).await?;
        assert!(super::answers(&mut ok, 1).await, "other clients unaffected");
    }
    restore_fetch_max_bytes(guc).await?;
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// Aborts a spawned task when dropped, on every path out of a test.
pub(super) struct AbortOnDrop(pub(super) tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}
