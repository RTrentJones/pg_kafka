//! The large-decode path under attack: Produce requests that decode to more than the inline
//! limit wait for one of the broker's two large-decode slots. Hostile connections that keep
//! the slots and their queue full must not get a legitimate request's connection closed:
//! it waits its turn and is answered.

use super::produce_decoding::{raw_batch, record, varint};
use super::{client_config, connect_from, rdkafka_roundtrip, stored_rows, PORT};
use crate::common::{DynResult, TestResult};
use crate::setup::TestContext;
use crate::wire::records::plain_batch;
use crate::wire::requests::{creatable, create_topics_raw, produce_request};
use crate::wire::{decode_response, read_frame_within, request_frame};
use bytes::Bytes;
use kafka_protocol::messages::ProduceRequest;
use kafka_protocol::records::Compression;
use std::net::Ipv4Addr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// MESSAGE_TOO_LARGE (Java's RecordTooLargeException).
const MESSAGE_TOO_LARGE: i16 = 10;

/// Hostile connections, each pipelining decompression bombs.
const HOSTILE_CONNECTIONS: usize = 16;

/// Records of a legitimate request: 1000 bytes each, gzip-compressed to about 1 MB on the
/// wire (the clients' default request size), they decode to more than the 2 MiB inline
/// limit and so take the large-decode path.
const LEGIT_RECORDS: usize = 2100;

/// How many legitimate requests are sent while the attack runs.
const LEGIT_REQUESTS: usize = 5;

/// A value of `len` hexadecimal digits from a simple generator: compresses about 2:1, like
/// real payloads, unlike zeros.
fn hex_value(seed: &mut u64, len: usize) -> Bytes {
    let mut out = Vec::with_capacity(len);
    while out.len() < len {
        *seed = seed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        for b in format!("{:016x}", *seed).bytes() {
            if out.len() < len {
                out.push(b);
            }
        }
    }
    Bytes::from(out)
}

/// Keep one hostile connection after another (from `src`) busy sending `frame` until
/// `stop`, counting the connections the broker closed in `closed`.
async fn flood(
    src: Ipv4Addr,
    frame: Arc<Vec<u8>>,
    stop: Arc<AtomicBool>,
    closed: Arc<AtomicUsize>,
) {
    while !stop.load(Ordering::Relaxed) {
        let Ok(stream) = connect_from(src, PORT).await else {
            tokio::time::sleep(Duration::from_millis(20)).await;
            continue;
        };
        let (mut read, mut write) = stream.into_split();
        let gone = Arc::new(AtomicBool::new(false));
        let gone_r = Arc::clone(&gone);
        // Read and discard the responses, noting when the broker closes the connection.
        let reader = tokio::spawn(async move {
            let mut buf = vec![0u8; 64 * 1024];
            while !matches!(read.read(&mut buf).await, Ok(0) | Err(_)) {}
            gone_r.store(true, Ordering::Relaxed);
        });
        while !stop.load(Ordering::Relaxed) && !gone.load(Ordering::Relaxed) {
            match tokio::time::timeout(Duration::from_millis(500), write.write_all(&frame)).await {
                Ok(Ok(())) | Err(_) => {}
                Ok(Err(_)) => break,
            }
        }
        if !stop.load(Ordering::Relaxed) {
            closed.fetch_add(1, Ordering::Relaxed);
        }
        reader.abort();
    }
}

/// Send `frame` (a Produce v7 of one partition) on a fresh connection and return the
/// partition's error code, or an error when the broker closes the connection instead.
async fn legit_produce(frame: &[u8], corr: i32) -> DynResult<(i16, Duration)> {
    let started = Instant::now();
    let mut s = TcpStream::connect(("127.0.0.1", PORT)).await?;
    s.write_all(frame).await?;
    let response = read_frame_within(&mut s, Duration::from_secs(40))
        .await?
        .ok_or("the broker closed the connection instead of answering the produce")?;
    let (c, resp) = decode_response::<ProduceRequest>(7, &response)?;
    assert_eq!(c, corr);
    Ok((
        resp.responses[0].partition_responses[0].error_code,
        started.elapsed(),
    ))
}

/// Sixteen hostile connections pipeline gzip bombs that each take a large-decode slot for
/// the decode of 100 MB before they are refused MESSAGE_TOO_LARGE. Meanwhile legitimate
/// compressed requests of the clients' default size (about 1 MB on the wire, over 2 MiB
/// decoded) wait their turn and are stored, and no connection is closed: neither theirs nor
/// the hostile ones, which are answered.
pub async fn test_saturated_large_decodes_wait_instead_of_closing() -> TestResult {
    println!("=== Test: a saturated large-decode path waits instead of closing ===\n");
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("net-large").await;
    let bombs = ctx.unique_topic("net-bomb").await;
    for name in [&topic, &bombs] {
        let created = create_topics_raw(vec![creatable(name, 1, 1)]).await?;
        assert_eq!(created[0].error_code, 0, "{created:?}");
    }

    println!("Step 1: {HOSTILE_CONNECTIONS} hostile connections pipelining gzip bombs");
    let bomb = plain_batch(&[Bytes::from(vec![0u8; 101_000_000])], Compression::Gzip);
    let mut bomb_request = produce_request(&bombs, vec![(0, bomb)], 1);
    bomb_request.timeout_ms = 30_000;
    let bomb_frame = Arc::new(request_frame(7, 1, &bomb_request));
    println!("  bomb frame: {} bytes", bomb_frame.len());
    // One check first: a bomb alone is refused, not stored.
    {
        let mut s = TcpStream::connect(("127.0.0.1", PORT)).await?;
        s.write_all(&bomb_frame).await?;
        let response = read_frame_within(&mut s, Duration::from_secs(30))
            .await?
            .ok_or("connection closed instead of a produce response")?;
        let (_, resp) = decode_response::<ProduceRequest>(7, &response)?;
        assert_eq!(
            resp.responses[0].partition_responses[0].error_code,
            MESSAGE_TOO_LARGE
        );
    }
    let stop = Arc::new(AtomicBool::new(false));
    let closed = Arc::new(AtomicUsize::new(0));
    let flooders: Vec<_> = (0..HOSTILE_CONNECTIONS)
        .map(|_| {
            tokio::spawn(flood(
                Ipv4Addr::new(127, 0, 0, 2),
                Arc::clone(&bomb_frame),
                Arc::clone(&stop),
                Arc::clone(&closed),
            ))
        })
        .collect();
    tokio::time::sleep(Duration::from_secs(2)).await;

    println!("Step 2: {LEGIT_REQUESTS} legitimate compressed requests while the attack runs");
    let mut seed = 7u64;
    let values: Vec<Bytes> = (0..LEGIT_RECORDS)
        .map(|_| hex_value(&mut seed, 1000))
        .collect();
    // One gzip batch of them, as a client batches them (kafka-protocol's encoder would
    // write one batch per record).
    let section: Vec<u8> = (0..)
        .zip(&values)
        .flat_map(|(i, value)| {
            let len = i64::try_from(value.len()).expect("small");
            record(&[
                &[0],
                &varint(0),
                &varint(i),
                &varint(-1),
                &varint(len),
                value,
                &varint(0),
            ])
        })
        .collect();
    let batch = raw_batch(&section, i32::try_from(LEGIT_RECORDS)?);
    let mut outcome = Ok(());
    for i in 0..LEGIT_REQUESTS {
        let corr = 100 + i32::try_from(i)?;
        let mut request = produce_request(&topic, vec![(0, batch.clone())], 1);
        // The clients' default request.timeout.ms, which they send as the Produce timeout.
        request.timeout_ms = 30_000;
        let frame = request_frame(7, corr, &request);
        if i == 0 {
            println!("  request frame: {} bytes", frame.len());
        }
        match legit_produce(&frame, corr).await.map_err(|e| e.to_string()) {
            Ok((code, took)) => {
                println!("  request {i}: error code {code} after {took:?}");
                if code != 0 {
                    outcome = Err(format!("request {i} answered with error {code}"));
                    break;
                }
            }
            Err(e) => {
                outcome = Err(format!("request {i}: {e}"));
                break;
            }
        }
    }
    println!("Step 3: an rdkafka gzip producer works during the attack");
    let mut cfg = client_config(PORT);
    cfg.set("compression.type", "gzip");
    let rd_topic = ctx.unique_topic("net-large-rd").await;
    let roundtrip = rdkafka_roundtrip(cfg, &rd_topic, 3)
        .await
        .map_err(|e| e.to_string());

    stop.store(true, Ordering::Relaxed);
    for f in flooders {
        let _ = tokio::time::timeout(Duration::from_secs(5), f).await;
    }
    let closed = closed.load(Ordering::Relaxed);
    println!("  hostile connections the broker closed: {closed}");
    outcome?;
    roundtrip?;
    assert_eq!(
        stored_rows(ctx.db(), &topic).await?,
        i64::try_from(LEGIT_RECORDS * LEGIT_REQUESTS)?,
        "every legitimate request is stored"
    );
    assert_eq!(stored_rows(ctx.db(), &bombs).await?, 0, "no bomb is stored");
    assert_eq!(closed, 0, "the broker answers the hostile requests too");
    println!("✅ every legitimate request stored, no connection closed");

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
