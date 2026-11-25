//! What a Fetch counts against its byte limits and its `min_bytes`, and what the responses of
//! pipelined requests on a connection that reads nothing may hold together.

use super::fetch_limits::{restore_fetch_max_bytes, set_fetch_max_bytes, AbortOnDrop};
use super::stalled_writers::{metadata_of, non_reading};
use crate::common::{DynResult, TestResult};
use crate::setup::{GucError, SystemGucGuard, TestContext};
use crate::wire::records::{batch, encode, plain_batch, plain_record};
use crate::wire::requests::{creatable, create_topics_raw, fetch_request, produce_request};
use crate::wire::{decode_response, read_frame_within, request_frame};
use bytes::Bytes;
use kafka_protocol::messages::{FetchRequest, FetchResponse, MetadataRequest, ProduceRequest};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{Compression, RecordBatchDecoder};
use std::net::{Ipv4Addr, SocketAddr};
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

/// The value of each of the large records.
const VALUE: usize = 1_000_000;
/// The header value of each of the header-heavy records.
const HEADER: usize = 96_000;
/// Metadata requests the non-reading client pipelines.
const METADATA: usize = 128;
/// Fetch requests the non-reading client pipelines.
const FETCHES: i32 = 30;

/// What one record of no key adds to a Fetch response, as the broker counts it: value and
/// header bytes uncompressed, 64 bytes a record and 10 a header (the records here have at
/// most one).
fn record_bytes(value: usize, header_bytes: usize) -> usize {
    value
        + 64
        + if header_bytes > 0 {
            header_bytes + 10
        } else {
            0
        }
}

/// Produce `batch` to partition 0 of `topic` (acks=1) on `s`.
async fn produce_batch(s: &mut TcpStream, corr: i32, topic: &str, batch: Bytes) -> DynResult<()> {
    let req = produce_request(topic, vec![(0, batch)], 1);
    s.write_all(&request_frame(3, corr, &req)).await?;
    let frame = read_frame_within(s, Duration::from_secs(30))
        .await?
        .ok_or("closed on produce")?;
    let (_, resp) = decode_response::<ProduceRequest>(3, &frame)?;
    let code = resp.responses[0].partition_responses[0].error_code;
    if code != 0 {
        return Err(format!("produce answered {code}").into());
    }
    Ok(())
}

/// A Fetch v4 of partition 0 of `topic` from `offset` that answers at once, with the given
/// `max_bytes` and `partition_max_bytes`.
fn fetch_with_limits(topic: &str, offset: i64, max_bytes: i32, partition_max: i32) -> FetchRequest {
    let mut req = fetch_request(topic, 0, offset, 0);
    req.max_bytes = max_bytes;
    req.min_bytes = 1;
    req.topics[0].partitions[0].partition_max_bytes = partition_max;
    req
}

/// The records (offset, value length, header bytes) partition 0 of a Fetch response holds.
fn partition_records(resp: &FetchResponse) -> DynResult<Vec<(i64, usize, usize)>> {
    let partition = &resp.responses[0].partitions[0];
    if partition.error_code != 0 {
        return Err(format!("fetch error {}", partition.error_code).into());
    }
    let mut records = partition.records.clone().unwrap_or_default();
    let mut out = Vec::new();
    for set in RecordBatchDecoder::decode_all(&mut records)? {
        for r in set.records {
            let headers = r
                .headers
                .iter()
                .map(|(k, v)| k.len() + v.as_ref().map_or(0, Bytes::len))
                .sum();
            out.push((r.offset, r.value.map_or(0, |v| v.len()), headers));
        }
    }
    Ok(out)
}

/// Fetch `req` on `s` and return what partition 0 holds.
async fn fetch(
    s: &mut TcpStream,
    corr: i32,
    req: &FetchRequest,
) -> DynResult<Vec<(i64, usize, usize)>> {
    s.write_all(&request_frame(4, corr, req)).await?;
    let frame = read_frame_within(s, Duration::from_secs(60))
        .await?
        .ok_or("closed on fetch")?;
    let (c, resp) = decode_response::<FetchRequest>(4, &frame)?;
    assert_eq!(c, corr);
    partition_records(&resp)
}

/// The review's reproductions. The byte limits counted each record's stored size
/// (`pg_column_size`, compressed by TOAST) and no headers at all: 20 records of 1 MB of zeros
/// count some 11 KB each, so a `partition_max_bytes` of 1 MiB and a request `max_bytes` of
/// 3 MiB both returned all 20 MB (and the DB thread loaded 87 times the limit where more
/// were stored); records of no key or value and a 96 KB header counted 64 bytes each, so a
/// 1 MiB limit returned all 100 (9.6 MB). Now a record counts its uncompressed key, value and
/// header bytes: 1, 3 and 10 records.
pub async fn test_fetch_limits_count_uncompressed_and_header_bytes() -> TestResult {
    println!("=== Test: Fetch limits count uncompressed and header bytes ===\n");
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("net-fetch-bytes").await;
    let created = create_topics_raw(vec![creatable(&topic, 1, 1)]).await?;
    assert_eq!(created[0].error_code, 0, "{created:?}");
    let mut s = TcpStream::connect(("127.0.0.1", super::port())).await?;

    let zeros = Bytes::from(vec![0u8; VALUE]);
    for i in 0..20 {
        let batch = plain_batch(std::slice::from_ref(&zeros), Compression::None);
        produce_batch(&mut s, i, &topic, batch).await?;
    }
    for i in 0..10 {
        let records: Vec<_> = (0..10)
            .map(|j| {
                let mut r = plain_record(j, None, b"");
                r.value = None;
                // One batch: offset - sequence is the same for every record.
                r.sequence = i32::try_from(j).unwrap_or(0) - 1;
                r.headers.insert(
                    StrBytes::from_static_str("h"),
                    Some(Bytes::from(vec![0u8; HEADER])),
                );
                r
            })
            .collect();
        produce_batch(&mut s, 100 + i, &topic, encode(&records, Compression::None)).await?;
    }
    println!(
        "Step 1: 20 records of 1 MB of zeros, 100 of a 96 KB header (stored TOAST-compressed)"
    );

    let mib = 1 << 20;
    let check = |what: &str, records: &[(i64, usize, usize)], limit: usize, expected: usize| {
        let bytes: usize = records.iter().map(|&(_, v, h)| record_bytes(v, h)).sum();
        println!("  {what}: {} records, {bytes} bytes counted", records.len());
        assert_eq!(
            records.len(),
            expected,
            "{what}: {} records ({bytes} bytes) for a limit of {limit}",
            records.len()
        );
        assert!(
            bytes <= limit,
            "{what}: {bytes} bytes for a limit of {limit}"
        );
    };
    let one = fetch(&mut s, 1, &fetch_with_limits(&topic, 0, i32::MAX, mib)).await?;
    check("partition_max_bytes 1 MiB", &one, 1 << 20, 1);
    let three = fetch(&mut s, 2, &fetch_with_limits(&topic, 0, 3 * mib, i32::MAX)).await?;
    check("max_bytes 3 MiB", &three, 3 << 20, 3);
    assert_eq!(three.iter().map(|r| r.0).collect::<Vec<_>>(), vec![0, 1, 2]);
    let headers = fetch(&mut s, 3, &fetch_with_limits(&topic, 20, i32::MAX, mib)).await?;
    check("headers, partition_max_bytes 1 MiB", &headers, 1 << 20, 10);
    assert!(headers.iter().all(|&(_, v, h)| v == 0 && h == HEADER + 1));
    let headers = fetch(&mut s, 4, &fetch_with_limits(&topic, 20, mib, i32::MAX)).await?;
    check("headers, max_bytes 1 MiB", &headers, 1 << 20, 10);

    // The first record is still returned whole when it is larger than both limits.
    let first = fetch(&mut s, 5, &fetch_with_limits(&topic, 0, 1000, 1000)).await?;
    assert_eq!(
        first.len(),
        1,
        "the first record is returned although larger"
    );
    assert_eq!(first[0].1, VALUE);
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// Kafka's `DelayedFetch` counts, per partition, the bytes available up to its
/// `partition_max_bytes` against `min_bytes`. A long poll whose partition stops short because
/// of a small `partition_max_bytes` therefore waits for its `min_bytes` (the broker answered
/// at once, because the partition had more than it returned), while one the request's
/// `max_bytes` cuts short is answered at once (the data available meets its `min_bytes`).
pub async fn test_long_poll_min_bytes_counts_each_partition_up_to_its_limit() -> TestResult {
    println!("=== Test: a long poll counts each partition up to its partition_max_bytes ===\n");
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("net-lp-minbytes").await;
    let created = create_topics_raw(vec![creatable(&topic, 1, 1)]).await?;
    assert_eq!(created[0].error_code, 0, "{created:?}");
    let mut s = TcpStream::connect(("127.0.0.1", super::port())).await?;
    let value = "m".repeat(1000);
    let values = [value.as_str(); 5];
    produce_batch(&mut s, 1, &topic, batch(&values, -1, -1, -1, false, None)).await?;
    println!("Step 1: 5 records of 1000 bytes");

    // partition_max_bytes 1500 holds one record; 1500 bytes available < min_bytes 100,000.
    let mut req = fetch_request(&topic, 0, 0, 2000);
    req.min_bytes = 100_000;
    req.max_bytes = 1 << 20;
    req.topics[0].partitions[0].partition_max_bytes = 1500;
    let started = Instant::now();
    let records = fetch(&mut s, 2, &req).await?;
    let took = started.elapsed();
    println!(
        "Step 2: partition_max_bytes 1500, min_bytes 100000: {} record(s) after {took:?}",
        records.len()
    );
    assert_eq!(records.len(), 1);
    assert!(
        took >= Duration::from_millis(1500),
        "answered after {took:?}: 1500 bytes available do not meet min_bytes 100000"
    );

    // max_bytes 3000 (and min_bytes 3000) holds two records of the 5000 bytes available.
    let mut req = fetch_request(&topic, 0, 0, 5000);
    req.min_bytes = 3000;
    req.max_bytes = 3000;
    req.topics[0].partitions[0].partition_max_bytes = i32::MAX;
    let started = Instant::now();
    let records = fetch(&mut s, 3, &req).await?;
    let took = started.elapsed();
    println!(
        "Step 3: max_bytes = min_bytes = 3000: {} records after {took:?}",
        records.len()
    );
    assert_eq!(records.len(), 2);
    assert!(
        took < Duration::from_secs(2),
        "answered after {took:?}: the data available meets min_bytes"
    );
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// From `/proc/net/tcp`, the bytes of a connection's requests the broker has not read (its
/// receive queue on the broker's side and the client's send queue), and the bytes of
/// responses the broker has written that sit in the kernel (its send queue and the client's
/// receive queue): those are on the socket, no longer held by the broker.
fn socket_queues(client: SocketAddr) -> DynResult<(u64, u64)> {
    let SocketAddr::V4(client) = client else {
        return Err("an IPv4 client address".into());
    };
    let hex = |a: Ipv4Addr, p: u16| format!("{:08X}:{p:04X}", u32::from_le_bytes(a.octets()));
    let broker = hex(Ipv4Addr::LOCALHOST, super::port());
    let me = hex(*client.ip(), client.port());
    let table = std::fs::read_to_string("/proc/net/tcp")?;
    let (mut not_read, mut in_kernel) = (0, 0);
    let mut found = 0;
    for line in table.lines().skip(1) {
        let cols: Vec<&str> = line.split_whitespace().collect();
        if cols.len() <= 4 {
            continue;
        }
        let (tx, rx) = cols[4]
            .split_once(':')
            .ok_or("a tx_queue:rx_queue column")?;
        let (tx, rx) = (u64::from_str_radix(tx, 16)?, u64::from_str_radix(rx, 16)?);
        if cols[1] == broker && cols[2] == me {
            not_read += rx;
            in_kernel += tx;
            found += 1;
        } else if cols[1] == me && cols[2] == broker {
            not_read += tx;
            in_kernel += rx;
            found += 1;
        }
    }
    if found != 2 {
        return Err("the connection is gone".into());
    }
    Ok((not_read, in_kernel))
}

/// `pg_kafka.test_pre_commit_delay_ms`, set so that one Produce holds the DB thread.
const STALL_GUC: &str = "pg_kafka.test_pre_commit_delay_ms";

/// Hold the DB thread for `pg_kafka.test_pre_commit_delay_ms` (a Produce on a connection of
/// its own whose commit that delays): the requests a client pipelines meanwhile are
/// all read, as far as the broker reads, before any is answered.
async fn stall_db_thread(topic: &str) -> DynResult<tokio::task::JoinHandle<()>> {
    let mut s = TcpStream::connect(("127.0.0.1", super::port())).await?;
    let topic = topic.to_string();
    let task = tokio::spawn(async move {
        let batch = plain_batch(&[Bytes::from_static(b"stall")], Compression::None);
        let _ = produce_batch(&mut s, 0, &topic, batch).await;
    });
    // The Produce reaches the DB thread before the client's requests do.
    tokio::time::sleep(Duration::from_millis(200)).await;
    Ok(task)
}

/// The guard's errors are `Send + Sync`; this file's results are not.
fn widen(e: GucError) -> Box<dyn std::error::Error> {
    e
}

/// The connection's response budget (`pg_kafka.fetch_max_bytes`, 1 MiB here) bounds what the
/// responses of its unwritten requests hold, for every request type. The reader stopped only
/// once the responses held more than the budget, and only a Fetch reserved bytes before it
/// ran: so with the DB thread busy (here: held 3 s by a Produce) a client that pipelines and
/// reads nothing had all its requests read, then held all their responses: 128 Metadata
/// requests answered with some 100 KB each held 12 MB, 30 Fetches of `max_bytes` 1 held 30
/// first records of 900 KB. Now at most 8 requests whose response size is unknown (anything
/// but Fetch and Heartbeat) wait for their response at once, and only a Fetch read while the
/// connection held nothing else returns a first record past its limits: the broker reads
/// the Metadata requests whose answers the budget and the socket buffers hold, and 8 more,
/// and one Fetch response holds the record.
pub async fn test_pipelined_responses_stay_within_the_connection_budget() -> TestResult {
    println!("=== Test: pipelined responses stay within the connection's budget ===\n");
    let ctx = TestContext::new().await?;
    let data = ctx.unique_topic("net-bound-data").await;
    let stall = ctx.unique_topic("net-bound-stall").await;
    let created = create_topics_raw(vec![creatable(&data, 1, 1), creatable(&stall, 1, 1)]).await?;
    assert!(created.iter().all(|t| t.error_code == 0), "{created:?}");
    let db = crate::common::create_db_client().await?;
    let guc = set_fetch_max_bytes(&db, 1 << 20).await?;
    let delay = SystemGucGuard::set(&db, STALL_GUC, "3000")
        .await
        .map_err(widen)?
        .ok_or("the server refused pg_kafka.test_pre_commit_delay_ms")?;
    {
        let mut s = TcpStream::connect(("127.0.0.1", super::port())).await?;
        let record = Bytes::from(vec![b'f'; 900_000]);
        produce_batch(
            &mut s,
            1,
            &data,
            plain_batch(std::slice::from_ref(&record), Compression::None),
        )
        .await?;
        println!("Step 1: one record of 900 KB produced");

        // One Metadata request of 2000 missing names, and the size of its answer.
        let prefix = uuid::Uuid::new_v4().simple().to_string();
        let names: Vec<String> = (0..2000).map(|i| format!("{prefix}-{i:0>8}")).collect();
        let metadata = metadata_of(names);
        let frame = request_frame(4, 7, &metadata);
        s.write_all(&frame).await?;
        let answer = read_frame_within(&mut s, Duration::from_secs(30))
            .await?
            .ok_or("closed on metadata")?;
        let _ = decode_response::<MetadataRequest>(4, &answer)?;
        let (request_len, answer_len) = (frame.len(), answer.len());
        println!("  a Metadata request of {request_len} bytes is answered with {answer_len}");

        // Metadata: 128 pipelined requests while the DB thread is held.
        let mut hog = non_reading(Ipv4Addr::new(127, 0, 0, 41)).await?;
        let client = hog.local_addr()?;
        let frames = frame.repeat(METADATA);
        let stalled = stall_db_thread(&stall).await?;
        // What the client's socket has accepted of the frames so far.
        let sent = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let writer = tokio::spawn({
            let sent = std::sync::Arc::clone(&sent);
            async move {
                let mut at = 0;
                while at < frames.len() {
                    match hog.write(&frames[at..]).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            at += n;
                            sent.store(at, std::sync::atomic::Ordering::SeqCst);
                        }
                    }
                }
                tokio::time::sleep(Duration::from_secs(60)).await;
                drop(hog);
            }
        });
        let abort = AbortOnDrop(writer);
        stalled.await?;
        tokio::time::sleep(Duration::from_millis(1500)).await;
        let (not_read, in_kernel) = socket_queues(client)?;
        let (not_read, in_kernel) = (usize::try_from(not_read)?, usize::try_from(in_kernel)?);
        let sent = sent.load(std::sync::atomic::Ordering::SeqCst);
        let read = sent.saturating_sub(not_read).div_ceil(request_len);
        // Answers the socket took, answers within the budget, at most 8 waiting for an
        // answer, the head and one partly read.
        let bound = (in_kernel + (1 << 20)) / answer_len + 1 + 8 + 2;
        println!(
            "Step 2: {not_read} bytes of requests not read, {in_kernel} bytes of answers in the \
             kernel: some {read} of {METADATA} read (bound {bound})"
        );
        assert!(
            read <= bound,
            "{read} Metadata requests read, answered with {answer_len} bytes each: more than a 1 MiB budget holds"
        );
        drop(abort);

        // Fetch: 30 pipelined Fetches of max_bytes 1 while the DB thread is held.
        let mut hog = non_reading(Ipv4Addr::new(127, 0, 0, 42)).await?;
        let mut frames = Vec::new();
        for corr in 0..FETCHES {
            frames.extend(request_frame(4, corr, &fetch_with_limits(&data, 0, 1, 1)));
        }
        let stalled = stall_db_thread(&stall).await?;
        hog.write_all(&frames).await?;
        stalled.await?;
        let mut holding = 0;
        for corr in 0..FETCHES {
            let frame = read_frame_within(&mut hog, Duration::from_secs(30))
                .await?
                .ok_or("closed on fetch")?;
            let (c, resp) = decode_response::<FetchRequest>(4, &frame)?;
            assert_eq!(c, corr);
            let records = partition_records(&resp)?;
            if !records.is_empty() {
                holding += 1;
            }
        }
        println!(
            "Step 3: {holding} of {FETCHES} Fetch responses of max_bytes 1 hold the 900 KB record"
        );
        assert!(
            (1..=2).contains(&holding),
            "{holding} responses of 900 KB for Fetches that reserved one byte each, on a 1 MiB budget"
        );
        assert!(
            super::answers(&mut hog, 99).await,
            "the connection keeps working"
        );
    }
    delay.restore().await.map_err(widen)?;
    restore_fetch_max_bytes(guc).await?;
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
