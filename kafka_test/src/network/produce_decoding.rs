//! Produce requests whose records the broker must refuse while decoding them: compressed
//! batches that expand past the per-request decode limit (a decompression bomb), and an
//! acks=0 Produce at a version the broker does not serve.

use super::{answers, client_config, rdkafka_roundtrip, stored_rows, time_until_closed, PORT};
use crate::common::{DynResult, TestResult};
use crate::setup::TestContext;
use crate::wire::records::plain_batch;
use crate::wire::requests::{creatable, create_topics_raw, produce_request};
use crate::wire::{decode_response, read_frame_within, request_frame};
use bytes::Bytes;
use kafka_protocol::messages::ProduceRequest;
use kafka_protocol::records::Compression;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

/// MESSAGE_TOO_LARGE (Java's RecordTooLargeException).
const MESSAGE_TOO_LARGE: i16 = 10;

/// The broker's per-request limit on decoded record bytes (MAX_REQUEST_SIZE).
const DECODE_LIMIT: usize = 100_000_000;

/// Send a Produce v7 with `parts` and return each partition's error code.
async fn produce_codes(
    stream: &mut TcpStream,
    corr: i32,
    topic: &str,
    parts: Vec<(i32, Bytes)>,
) -> DynResult<Vec<i16>> {
    let request = produce_request(topic, parts, 1);
    let frame = request_frame(7, corr, &request);
    println!("  request frame: {} bytes", frame.len());
    stream.write_all(&frame).await?;
    let response = read_frame_within(stream, Duration::from_secs(60))
        .await?
        .ok_or("connection closed instead of a produce response")?;
    let (c, resp) = decode_response::<ProduceRequest>(7, &response)?;
    assert_eq!(c, corr);
    Ok(resp.responses[0]
        .partition_responses
        .iter()
        .map(|p| p.error_code)
        .collect())
}

/// The listener backend's resident memory (kB), after resetting its peak, or `None` when
/// this process cannot read the backend's /proc entry (the DB runs elsewhere, or as
/// another user).
async fn reset_listener_peak(ctx: &TestContext) -> DynResult<Option<(i32, u64)>> {
    let pid: i32 = ctx
        .db()
        .query_one(
            "SELECT pid FROM pg_stat_activity WHERE backend_type = 'pg_kafka_listener'",
            &[],
        )
        .await?
        .get(0);
    if std::fs::write(format!("/proc/{pid}/clear_refs"), "5").is_err() {
        return Ok(None);
    }
    Ok(proc_kb(pid, "VmRSS:").map(|rss| (pid, rss)))
}

/// A `/proc/<pid>/status` field in kB.
fn proc_kb(pid: i32, field: &str) -> Option<u64> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    status
        .lines()
        .find(|l| l.starts_with(field))?
        .split_whitespace()
        .nth(1)?
        .parse()
        .ok()
}

/// A compressed request that decodes past the per-request limit is refused with
/// MESSAGE_TOO_LARGE on every partition and nothing is written; its decode stops at the
/// limit, so the broker's memory stays bounded however far the batches would expand, and
/// the broker keeps serving. A large compressed request within the limit is stored.
pub async fn test_compressed_produce_past_the_decode_limit_is_refused() -> TestResult {
    println!("=== Test: decompression bomb refused while decoding ===\n");
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("net-bomb").await;
    let created = create_topics_raw(vec![creatable(&topic, 8, 1)]).await?;
    assert_eq!(created[0].error_code, 0, "{created:?}");
    let mut s = TcpStream::connect(("127.0.0.1", PORT)).await?;
    let zeros = |n: usize| Bytes::from(vec![0u8; n]);

    println!(
        "Step 1: one gzip batch of {} bytes of zeros",
        DECODE_LIMIT + 1_000_000
    );
    let gzip = plain_batch(&[zeros(DECODE_LIMIT + 1_000_000)], Compression::Gzip);
    let codes = produce_codes(&mut s, 1, &topic, vec![(0, gzip)]).await?;
    assert_eq!(codes, vec![MESSAGE_TOO_LARGE]);
    println!("✅ refused: {codes:?}");

    println!("Step 2: four zstd batches of 30 MB (each legal, 120 MB together)");
    let zstd = plain_batch(&[zeros(30_000_000)], Compression::Zstd);
    let parts = (0..4).map(|p| (p, zstd.clone())).collect();
    let codes = produce_codes(&mut s, 2, &topic, parts).await?;
    assert_eq!(codes, vec![MESSAGE_TOO_LARGE; 4]);
    println!("✅ refused: {codes:?}");

    println!("Step 3: eight gzip batches of 60 MB (480 MB decoded) under a memory watch");
    let peak_watch = reset_listener_peak(&ctx).await?;
    let gzip = plain_batch(&[zeros(60_000_000)], Compression::Gzip);
    let parts = (0..8).map(|p| (p, gzip.clone())).collect();
    let codes = produce_codes(&mut s, 3, &topic, parts).await?;
    assert_eq!(codes, vec![MESSAGE_TOO_LARGE; 8]);
    match peak_watch.and_then(|(pid, rss)| Some((rss, proc_kb(pid, "VmHWM:")?))) {
        Some((rss, peak)) => {
            let grew_mb = peak.saturating_sub(rss) / 1024;
            println!(
                "  listener RSS {} MB, peak while decoding +{grew_mb} MB",
                rss / 1024
            );
            // The decode stops at the 100 MB limit (plus the copies of what it decoded so
            // far); holding the 480 MB the batches expand to would show here.
            assert!(grew_mb < 300, "decoding grew the listener by {grew_mb} MB");
        }
        None => println!("  (listener memory not readable from here; skipping the RSS check)"),
    }
    println!("✅ refused: {codes:?}");

    assert_eq!(
        stored_rows(ctx.db(), &topic).await?,
        0,
        "nothing is written"
    );

    println!("Step 4: the broker still serves, and a 20 MB compressed request is stored");
    assert!(answers(&mut s, 4).await, "the connection still answers");
    let ok = plain_batch(&[zeros(10_000_000)], Compression::Lz4);
    let codes = produce_codes(&mut s, 5, &topic, vec![(0, ok.clone()), (1, ok)]).await?;
    assert_eq!(codes, vec![0, 0]);
    let stored: i64 = ctx
        .db()
        .query_one(
            "SELECT coalesce(sum(octet_length(m.value)), 0)::bigint FROM kafka.messages m \
             JOIN kafka.topics t ON t.id = m.topic_id WHERE t.name = $1",
            &[&topic],
        )
        .await?
        .get(0);
    assert_eq!(stored, 20_000_000);
    rdkafka_roundtrip(client_config(PORT), &topic, 3).await?;
    println!("✅ stored {stored} bytes; rdkafka round trip works");

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// An acks=0 Produce at a version the broker does not serve (v10; it serves 3-9) gets no
/// response, so it cannot be told UNSUPPORTED_VERSION: the broker closes the connection,
/// as Kafka does, instead of dropping the records unnoticed.
pub async fn test_unsupported_version_acks0_produce_closes_connection() -> TestResult {
    println!("=== Test: acks=0 Produce at an unsupported version closes ===\n");
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("net-acks0-v10").await;
    let batch = plain_batch(&[Bytes::from_static(b"lost?")], Compression::None);

    let mut s = TcpStream::connect(("127.0.0.1", PORT)).await?;
    let request = produce_request(&topic, vec![(0, batch.clone())], 0);
    s.write_all(&request_frame(10, 1, &request)).await?;
    let closed = time_until_closed(&mut s, Duration::from_secs(5)).await;
    assert!(
        closed.is_some(),
        "acks=0 Produce v10 must close the connection"
    );
    println!("✅ closed after {:?}", closed.unwrap_or_default());

    // acks=1 at the same version is answered UNSUPPORTED_VERSION; the connection stays.
    let mut s = TcpStream::connect(("127.0.0.1", PORT)).await?;
    let request = produce_request(&topic, vec![(0, batch)], 1);
    s.write_all(&request_frame(10, 2, &request)).await?;
    let response = read_frame_within(&mut s, Duration::from_secs(5))
        .await?
        .ok_or("acks=1 v10 closed instead of answering")?;
    let (_, resp) = decode_response::<ProduceRequest>(10, &response)?;
    assert_eq!(
        resp.responses[0].partition_responses[0].error_code,
        crate::error_codes::UNSUPPORTED_VERSION
    );
    assert!(answers(&mut s, 3).await, "the acks=1 connection stays open");
    println!("✅ acks=1 v10: UNSUPPORTED_VERSION, connection open");

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
