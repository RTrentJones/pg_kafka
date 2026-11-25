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
/// header bytes uncompressed, 84 bytes a record and 10 a header (the records here have at
/// most one).
fn record_bytes(value: usize, header_bytes: usize) -> usize {
    value
        + 84
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

/// Partitions of the topic [`test_fetch_reads_no_first_record_a_partition_cannot_return`]
/// fetches.
const FANOUT_PARTITIONS: i32 = 300;
/// The first record of each of those partitions: 1 MB of a repeated pattern, which TOAST
/// stores compressed in a few KB and must decompress to read.
const FANOUT_LARGE: usize = 1_000_000;

/// A Fetch v4 that answers at once of partitions `0..partitions` of `topic` from `offset`,
/// each up to `partition_max` bytes, with a `max_bytes` of `i32::MAX`.
fn fetch_partitions(topic: &str, partitions: i32, offset: i64, partition_max: i32) -> FetchRequest {
    let mut req = fetch_request(topic, 0, offset, 0);
    req.max_bytes = i32::MAX;
    let template = req.topics[0].partitions[0].clone();
    req.topics[0].partitions = (0..partitions)
        .map(|p| {
            let mut entry = template.clone();
            entry.partition = p;
            entry.partition_max_bytes = partition_max;
            entry
        })
        .collect();
    req
}

/// Fetch `req` on `s`: how long it took to be answered, and per partition its error code and
/// the encoded length of its records.
async fn timed_fetch(
    s: &mut TcpStream,
    corr: i32,
    req: &FetchRequest,
) -> DynResult<(Duration, Vec<(i32, i16, usize)>)> {
    let started = Instant::now();
    s.write_all(&request_frame(4, corr, req)).await?;
    let frame = read_frame_within(s, Duration::from_secs(60))
        .await?
        .ok_or("closed on fetch")?;
    let took = started.elapsed();
    let (c, resp) = decode_response::<FetchRequest>(4, &frame)?;
    assert_eq!(c, corr);
    let partitions = resp.responses[0]
        .partitions
        .iter()
        .map(|p| {
            (
                p.partition_index,
                p.error_code,
                p.records.as_ref().map_or(0, Bytes::len),
            )
        })
        .collect();
    Ok((took, partitions))
}

/// Kafka returns a first record larger than the limits only for the first partition with data
/// (KIP-74); every later partition returns what fits its `partition_max_bytes`. The broker
/// still read the first record of each later partition from the database, whatever its size,
/// and dropped it in the handler: a Fetch of 300 partitions whose first records are 1 MB
/// (stored compressed, so each read decompresses 1 MB) held the DB thread for 299 such reads,
/// some 10 times as long as the same Fetch from the small records behind them. Now a
/// partition that may not return its first record whole does not read it, so the two cost
/// about the same. The later partitions answer no records (and no gap batch, which would move
/// their consumers past the records they did not get).
pub async fn test_fetch_reads_no_first_record_a_partition_cannot_return() -> TestResult {
    println!("=== Test: Fetch reads no first record a partition cannot return ===\n");
    fanout_skips_first_records("net-fetch-first", LargePart::Value).await
}

/// [`test_fetch_reads_no_first_record_a_partition_cannot_return`] with the size of each first
/// record in one header value instead of its value. The fetch query computed every row's size
/// from its headers JSONB (up to the query's row limit, and past the budget), which reads the
/// whole headers value from TOAST and parses it: a later partition still read its 2 MB of
/// stored hex just to leave the record out. Now Produce stores each record's size
/// (`kafka.messages.record_bytes`), the fetch sums that, and reads headers only of the records
/// it returns.
pub async fn test_fetch_reads_no_first_record_header_a_partition_cannot_return() -> TestResult {
    println!("=== Test: Fetch reads no first record header a partition cannot return ===\n");
    fanout_skips_first_records("net-fetch-first-hdr", LargePart::Header).await
}

/// Where the size of the large first records of [`fanout_skips_first_records`] is.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum LargePart {
    /// A value of [`FANOUT_LARGE`] bytes.
    Value,
    /// A one-byte value and one header `h` of [`FANOUT_LARGE`] bytes.
    Header,
}

/// [`FANOUT_PARTITIONS`] partitions of a new topic named after `prefix`, each a large record
/// (`large`) at offset 0 and one of 100 bytes at offset 1: a Fetch from offset 0 of every
/// partition, 1000 bytes each, returns the first partition's large record whole and nothing of
/// the others, and takes about as long as the Fetch of the small records from offset 1.
async fn fanout_skips_first_records(prefix: &str, large: LargePart) -> TestResult {
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic(prefix).await;
    let created = create_topics_raw(vec![creatable(&topic, FANOUT_PARTITIONS, 1)]).await?;
    assert_eq!(created[0].error_code, 0, "{created:?}");
    let mut s = TcpStream::connect(("127.0.0.1", super::port())).await?;
    let pattern = Bytes::from(b"abcdefgh".repeat(FANOUT_LARGE / 8));
    let small = Bytes::from(vec![b's'; 100]);
    let first = match large {
        LargePart::Value => pattern.clone(),
        LargePart::Header => Bytes::from_static(b"x"),
    };
    let partitions: Vec<i32> = (0..FANOUT_PARTITIONS).collect();
    for (corr, chunk) in (0..).zip(partitions.chunks(10)) {
        let parts = chunk
            .iter()
            .map(|&p| {
                let records: Vec<_> = [&first, &small]
                    .into_iter()
                    .zip(0..)
                    .map(|(value, offset)| {
                        let mut r = plain_record(offset, None, value);
                        // One batch: offset - sequence is the same for every record.
                        r.sequence = i32::try_from(offset).unwrap_or(0) - 1;
                        if offset == 0 && large == LargePart::Header {
                            r.headers
                                .insert(StrBytes::from_static_str("h"), Some(pattern.clone()));
                        }
                        r
                    })
                    .collect();
                (p, encode(&records, Compression::None))
            })
            .collect();
        let req = produce_request(&topic, parts, 1);
        s.write_all(&request_frame(3, corr, &req)).await?;
        let frame = read_frame_within(&mut s, Duration::from_secs(60))
            .await?
            .ok_or("closed on produce")?;
        let (_, resp) = decode_response::<ProduceRequest>(3, &frame)?;
        let codes: Vec<i16> = resp.responses[0]
            .partition_responses
            .iter()
            .map(|p| p.error_code)
            .collect();
        assert!(codes.iter().all(|&c| c == 0), "produce answered {codes:?}");
    }
    println!(
        "Step 1: {FANOUT_PARTITIONS} partitions, each a record of {FANOUT_LARGE} bytes ({large:?}) \
         at offset 0 and one of 100 bytes at offset 1"
    );

    // Each Fetch three times; the fastest of each is compared.
    let mut large_first = Duration::MAX;
    let mut small_first = Duration::MAX;
    let mut answers = (Vec::new(), Vec::new());
    for attempt in 0..3 {
        let (took, answer) = timed_fetch(
            &mut s,
            100 + attempt,
            &fetch_partitions(&topic, FANOUT_PARTITIONS, 0, 1000),
        )
        .await?;
        assert!(answer.iter().all(|&(_, code, _)| code == 0), "{answer:?}");
        assert!(
            answer[0].2 > FANOUT_LARGE,
            "the first partition returns its first record whole: {:?}",
            answer[0]
        );
        let later: Vec<_> = answer[1..].iter().filter(|p| p.2 > 0).collect();
        assert!(
            later.is_empty(),
            "the later partitions return no records, nor a gap batch: {later:?}"
        );
        large_first = large_first.min(took);
        answers.0 = answer;

        let (took, answer) = timed_fetch(
            &mut s,
            200 + attempt,
            &fetch_partitions(&topic, FANOUT_PARTITIONS, 1, 1000),
        )
        .await?;
        assert!(
            answer.iter().all(|&(_, code, len)| code == 0 && len > 100),
            "every partition returns its small record: {answer:?}"
        );
        small_first = small_first.min(took);
        answers.1 = answer;
        println!(
            "  attempt {attempt}: from the large records {large_first:?}, from the small ones \
             {small_first:?} (fastest so far)"
        );
    }
    println!(
        "Step 2: a Fetch from the large records took {large_first:?}, from the small ones \
         {small_first:?}"
    );
    assert!(
        large_first < small_first * 2 + Duration::from_millis(40),
        "the Fetch from the large records took {large_first:?}, the one from the small records \
         {small_first:?}: the later partitions read their first records although they cannot \
         return them"
    );
    // Produce stored each record's size as a Fetch counts it: key, value, each header's key
    // and value, 84 bytes a record and 10 a header.
    let large_bytes = match large {
        LargePart::Value => i32::try_from(FANOUT_LARGE)? + 84,
        LargePart::Header => 1 + 84 + 1 + i32::try_from(FANOUT_LARGE)? + 10,
    };
    let wrong: i64 = ctx
        .db()
        .query_one(
            "SELECT count(*) FROM kafka.messages m JOIN kafka.topics t ON t.id = m.topic_id \
             WHERE t.name = $1 AND m.record_bytes IS DISTINCT FROM \
                   CASE m.partition_offset WHEN 0 THEN $2 ELSE 100 + 84 END",
            &[&topic, &large_bytes],
        )
        .await?
        .get(0);
    assert_eq!(wrong, 0, "records stored without their fetch size");
    // Rows without a stored size (written by SQL outside the extension) are sized from their
    // columns, and the Fetches cut them at the same records.
    ctx.db()
        .execute(
            "UPDATE kafka.messages m SET record_bytes = NULL FROM kafka.topics t \
             WHERE t.id = m.topic_id AND t.name = $1",
            &[&topic],
        )
        .await?;
    for (corr, offset, expected) in [(300, 0, &answers.0), (301, 1, &answers.1)] {
        let (_, answer) = timed_fetch(
            &mut s,
            corr,
            &fetch_partitions(&topic, FANOUT_PARTITIONS, offset, 1000),
        )
        .await?;
        assert_eq!(&answer, expected, "unsized rows from offset {offset}");
    }
    println!("Step 3: stored sizes as counted; rows without one are cut at the same records");
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// A partition with no visible records before its readable end returns a control batch that
/// moves its consumer past them (a gap batch, PROTOCOL_DEVIATIONS §7). Those batches counted
/// against neither byte limit, so every partition of a Fetch returned one, whatever was left
/// of its `max_bytes`. Kafka 3.7 counts the abort marker batches it returns in the fetch size
/// like any other: now a gap batch is returned only by the first partition of a response or
/// within what is left of the limits.
pub async fn test_gap_batches_count_against_max_bytes() -> TestResult {
    println!("=== Test: Gap batches count against max_bytes ===\n");
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("net-fetch-gap").await;
    let created = create_topics_raw(vec![creatable(&topic, 3, 1)]).await?;
    assert_eq!(created[0].error_code, 0, "{created:?}");
    let mut s = TcpStream::connect(("127.0.0.1", super::port())).await?;
    for partition in 0..3 {
        let records = batch(&["value"; 3], -1, -1, -1, false, None);
        let req = produce_request(&topic, vec![(partition, records)], 1);
        s.write_all(&request_frame(3, partition, &req)).await?;
        let frame = read_frame_within(&mut s, Duration::from_secs(30))
            .await?
            .ok_or("closed on produce")?;
        let (_, resp) = decode_response::<ProduceRequest>(3, &frame)?;
        assert_eq!(resp.responses[0].partition_responses[0].error_code, 0);
    }
    // Aborted: invisible to read_committed, with nothing pending (the readable end is 3).
    let aborted = ctx
        .db()
        .execute(
            "UPDATE kafka.messages m SET txn_state = 'aborted' FROM kafka.topics t \
             WHERE t.id = m.topic_id AND t.name = $1",
            &[&topic],
        )
        .await?;
    assert_eq!(aborted, 9);
    println!("Step 1: 3 partitions of 3 aborted records each");

    let fetch = |max_bytes: i32| {
        let mut req = fetch_partitions(&topic, 3, 0, 1 << 20);
        req.isolation_level = 1;
        req.max_bytes = max_bytes;
        req
    };
    let (_, unlimited) = timed_fetch(&mut s, 10, &fetch(i32::MAX)).await?;
    let gap = unlimited[0].2;
    assert!(
        unlimited
            .iter()
            .all(|&(_, code, len)| code == 0 && len == gap && gap > 0),
        "every partition returns a gap batch: {unlimited:?}"
    );
    println!("Step 2: without a limit each partition returns a gap batch of {gap} bytes");
    let gap = i32::try_from(gap)?;
    for (corr, max_bytes, gaps) in [(11, gap + gap / 2, 1), (12, 2 * gap, 2), (13, 1, 1)] {
        let (_, answer) = timed_fetch(&mut s, corr, &fetch(max_bytes)).await?;
        let returned: Vec<i32> = answer
            .iter()
            .filter(|&&(_, code, len)| code == 0 && len > 0)
            .map(|&(p, _, _)| p)
            .collect();
        assert_eq!(
            returned,
            (0..gaps).collect::<Vec<_>>(),
            "max_bytes {max_bytes}: the partitions with a gap batch, {answer:?}"
        );
        println!("  max_bytes {max_bytes}: gap batches from partitions {returned:?}");
    }
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// Records of [`test_fetch_limits_count_records_alone_in_their_batch`].
const LONE_RECORDS: i64 = 400;
/// The value of each of them.
const LONE_VALUE: usize = 10;

/// A record alone in its RecordBatch carries the whole 61-byte batch header besides its own
/// framing: here records whose neighbours were deleted (as aborted-row cleanup deletes them),
/// so no two returned offsets are consecutive. Each record of 10 bytes takes 78 bytes of the
/// response, and the byte limits counted 74 (10 plus 64 a record), so a Fetch returned more
/// than its `partition_max_bytes` and its `max_bytes`. Now a record counts 84 bytes besides
/// its key, value and headers, what one alone in its batch takes at most, and the records
/// returned fit both limits.
pub async fn test_fetch_limits_count_records_alone_in_their_batch() -> TestResult {
    println!("=== Test: Fetch limits count records alone in their batch ===\n");
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("net-fetch-lone").await;
    let created = create_topics_raw(vec![creatable(&topic, 1, 1)]).await?;
    assert_eq!(created[0].error_code, 0, "{created:?}");
    let mut s = TcpStream::connect(("127.0.0.1", super::port())).await?;
    let value = [b'v'; LONE_VALUE];
    let records: Vec<_> = (0..LONE_RECORDS)
        .map(|offset| {
            let mut r = plain_record(offset, None, &value);
            // One batch: offset - sequence is the same for every record.
            r.sequence = i32::try_from(offset).unwrap_or(0) - 1;
            r
        })
        .collect();
    produce_batch(&mut s, 1, &topic, encode(&records, Compression::None)).await?;
    let deleted = ctx
        .db()
        .execute(
            "DELETE FROM kafka.messages m USING kafka.topics t \
             WHERE t.id = m.topic_id AND t.name = $1 AND m.partition_offset % 2 = 1",
            &[&topic],
        )
        .await?;
    assert_eq!(i64::try_from(deleted)?, LONE_RECORDS / 2);
    println!(
        "Step 1: {LONE_RECORDS} records of {LONE_VALUE} bytes, every second one deleted: each \
         left is alone in its batch"
    );

    let limit: i32 = 74 * 100;
    for (corr, (max_bytes, partition_max)) in [(i32::MAX, limit), (limit, i32::MAX)]
        .into_iter()
        .enumerate()
    {
        let req = fetch_with_limits(&topic, 0, max_bytes, partition_max);
        s.write_all(&request_frame(4, i32::try_from(corr)? + 2, &req))
            .await?;
        let frame = read_frame_within(&mut s, Duration::from_secs(30))
            .await?
            .ok_or("closed on fetch")?;
        let (_, resp) = decode_response::<FetchRequest>(4, &frame)?;
        let partition = &resp.responses[0].partitions[0];
        assert_eq!(partition.error_code, 0);
        let mut records = partition.records.clone().unwrap_or_default();
        let bytes = records.len();
        let sets = RecordBatchDecoder::decode_all(&mut records)?;
        let count: usize = sets.iter().map(|set| set.records.len()).sum();
        println!(
            "Step {}: max_bytes {max_bytes}, partition_max_bytes {partition_max}: {count} \
             records in {} batches, {bytes} bytes",
            corr + 2,
            sets.len()
        );
        assert!(count > 1, "{count} records returned");
        assert!(
            usize::try_from(limit).is_ok_and(|limit| bytes <= limit),
            "{bytes} bytes of records returned for a limit of {limit}"
        );
    }
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
