//! The decode work of one Produce request is bounded, however many partitions it refuses:
//! a request whose partitions each decode a large batch and are then refused (or are
//! repeated entries of one partition) must neither hold a network I/O thread nor a
//! large-decode slot for long.

use super::large_decodes::hex_value;
use super::produce_decoding::{batch_with, raw_batch, record, varint, BatchHeader};
use super::{api_versions_frame, stored_rows, PORT};
use crate::common::{DynResult, TestResult};
use crate::setup::TestContext;
use crate::wire::records::plain_batch;
use crate::wire::requests::{creatable, create_topics_raw, produce_request};
use crate::wire::{correlation_of, decode_response, read_frame_within, request_frame, Wire};
use bytes::Bytes;
use kafka_protocol::messages::ProduceRequest;
use kafka_protocol::records::Compression;
use std::collections::BTreeSet;
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

/// UNKNOWN_TOPIC_OR_PARTITION.
const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
/// CORRUPT_MESSAGE.
const CORRUPT_MESSAGE: i16 = 2;
/// INVALID_TIMESTAMP.
const INVALID_TIMESTAMP: i16 = 32;

/// Partition entries of each hostile request: with the 111-byte batch below, a 1.19 MB
/// request that declares less than the 2 MiB inline decode limit.
const ENTRIES: i32 = 10_000;

/// A 111-byte zstd batch of one record of 900 KB of zeros (raw block of the record's
/// head, then RLE blocks), with LogAppendTime and maxTimestamp -1: it decodes 900 KB and
/// is then refused INVALID_TIMESTAMP.
fn zstd_rle_batch() -> Bytes {
    const VALUE: usize = 900_000;
    let mut rec = record(&[
        &[0],
        &varint(0),
        &varint(0),
        &varint(-1),
        &varint(i64::try_from(VALUE).expect("small")),
        &vec![0u8; VALUE],
        &varint(0),
    ]);
    let total = rec.len();
    rec.truncate(total - VALUE - 1); // the head, before the zero value and header count
    let head = rec;
    let block = |last: bool, kind: u32, size: usize| {
        let v = u32::from(last) | (kind << 1) | (u32::try_from(size).expect("small") << 3);
        v.to_le_bytes()[..3].to_vec()
    };
    let mut frame = 0xFD2F_B528u32.to_le_bytes().to_vec();
    frame.push(0xA0); // single segment, a 4-byte content size
    frame.extend_from_slice(&u32::try_from(total).expect("small").to_le_bytes());
    frame.extend(block(false, 0, head.len()));
    frame.extend_from_slice(&head);
    let mut zeros = VALUE + 1;
    while zeros > 0 {
        let n = zeros.min(131_072);
        zeros -= n;
        frame.extend(block(zeros == 0, 1, n));
        frame.push(0);
    }
    batch_with(
        &frame,
        1,
        BatchHeader {
            attributes: 0x0C, // zstd, LogAppendTime
            max_timestamp: Some(-1),
            ..BatchHeader::default()
        },
    )
}

/// Send `frame` on a fresh connection and return the error codes it was answered with,
/// and how long that took.
async fn send_produce(frame: Vec<u8>, corr: i32) -> DynResult<(BTreeSet<i16>, usize, Duration)> {
    let started = Instant::now();
    let mut s = TcpStream::connect(("127.0.0.1", PORT)).await?;
    s.write_all(&frame).await?;
    let response = read_frame_within(&mut s, Duration::from_secs(120))
        .await?
        .ok_or("connection closed instead of a produce response")?;
    let (c, resp) = decode_response::<ProduceRequest>(7, &response)?;
    assert_eq!(c, corr);
    let answers: Vec<i16> = resp
        .responses
        .iter()
        .flat_map(|t| t.partition_responses.iter().map(|p| p.error_code))
        .collect();
    Ok((
        answers.iter().copied().collect(),
        answers.len(),
        started.elapsed(),
    ))
}

/// [`send_produce`] for a spawned task.
async fn spawned_produce(
    frame: Vec<u8>,
    corr: i32,
) -> Result<(BTreeSet<i16>, usize, Duration), String> {
    send_produce(frame, corr).await.map_err(|e| e.to_string())
}

/// The longest an ApiVersions on a fresh connection took, measured back to back for
/// `during`.
async fn worst_api_versions(during: Duration) -> DynResult<(Duration, usize)> {
    let end = Instant::now() + during;
    let mut worst = Duration::ZERO;
    let mut calls = 0;
    while Instant::now() < end {
        let started = Instant::now();
        let mut s = TcpStream::connect(("127.0.0.1", PORT)).await?;
        s.write_all(&api_versions_frame(5)).await?;
        let frame = read_frame_within(&mut s, Duration::from_secs(30))
            .await?
            .ok_or("connection closed instead of an ApiVersions response")?;
        assert_eq!(correlation_of(&frame), 5);
        worst = worst.max(started.elapsed());
        calls += 1;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Ok((worst, calls))
}

/// Four requests of 10,000 partition entries, each a 111-byte zstd batch that decodes
/// 900 KB before it is refused: two repeat the two partitions of an existing topic, two
/// name distinct partitions of a topic that does not exist (refused once their records
/// are looked at, after they decode). Each declares less than the inline decode limit;
/// decoded on a network I/O thread, they would decode 9 GB each there. While they run,
/// ApiVersions on another connection stays fast, and each is answered once per partition.
pub async fn test_refused_partitions_do_not_hold_the_io_threads() -> TestResult {
    println!("=== Test: refused partitions do not hold the network I/O threads ===\n");
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("net-work").await;
    let missing = ctx.unique_topic("net-work-missing").await;
    let created = create_topics_raw(vec![creatable(&topic, 2, 1)]).await?;
    assert_eq!(created[0].error_code, 0, "{created:?}");
    let batch = zstd_rle_batch();
    assert_eq!(batch.len(), 111);

    let repeated = produce_request(
        &topic,
        (0..ENTRIES).map(|p| (p % 2, batch.clone())).collect(),
        1,
    );
    let distinct = produce_request(
        &missing,
        (0..ENTRIES).map(|p| (p, batch.clone())).collect(),
        1,
    );
    println!("Step 1: two requests of each shape at once, ApiVersions alongside");
    let started = Instant::now();
    let hostile: Vec<_> = [&repeated, &repeated, &distinct, &distinct]
        .into_iter()
        .zip(1..)
        .map(|(request, corr)| tokio::spawn(spawned_produce(request_frame(7, corr, request), corr)))
        .collect();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let (worst, calls) = worst_api_versions(Duration::from_secs(3)).await?;
    println!("  ApiVersions: {calls} calls, the slowest {worst:?}");
    assert!(
        worst < Duration::from_millis(500),
        "an ApiVersions took {worst:?} while the requests decoded"
    );
    for (i, task) in hostile.into_iter().enumerate() {
        let (codes, answers, took) = task.await??;
        println!("  request {i}: {answers} answers {codes:?} after {took:?}");
        if i < 2 {
            assert_eq!(answers, 2, "one answer per partition");
            assert_eq!(codes, BTreeSet::from([INVALID_TIMESTAMP]));
        } else {
            assert_eq!(answers, usize::try_from(ENTRIES)?);
            assert_eq!(codes, BTreeSet::from([UNKNOWN_TOPIC_OR_PARTITION]));
        }
    }
    println!(
        "  all answered {:?} after they were sent",
        started.elapsed()
    );
    assert_eq!(stored_rows(ctx.db(), &topic).await?, 0);
    println!("✅ ApiVersions stayed fast; every partition answered once");

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// Two requests of 200 distinct partitions of a topic that does not exist, each a 112 KB
/// gzip batch of 110 MB of zeros (refused MESSAGE_TOO_LARGE at the decode limit, then
/// answered UNKNOWN_TOPIC_OR_PARTITION once looked up), take both large-decode slots.
/// Decoding every partition to the limit would hold the slots for seconds per request; a
/// legitimate request that needs a slot (a 5 MB gzip batch) is still stored within its
/// wait.
pub async fn test_refused_partitions_do_not_monopolize_the_decode_slots() -> TestResult {
    println!("=== Test: refused partitions do not monopolize the large-decode slots ===\n");
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("net-slot").await;
    let missing = ctx.unique_topic("net-slot-missing").await;
    let created = create_topics_raw(vec![creatable(&topic, 1, 1)]).await?;
    assert_eq!(created[0].error_code, 0, "{created:?}");

    let value = |v: &[u8]| {
        record(&[
            &[0],
            &varint(0),
            &varint(0),
            &varint(-1),
            &varint(i64::try_from(v.len()).expect("small")),
            v,
            &varint(0),
        ])
    };
    let bomb = raw_batch(&value(&vec![0u8; 110 << 20]), 1);
    println!("  bomb batch: {} bytes", bomb.len());
    let mut seed = 11u64;
    let legit_batch = raw_batch(&value(&hex_value(&mut seed, 5_000_000)), 1);
    println!("  legitimate batch: {} bytes", legit_batch.len());

    println!("Step 1: two hostile requests, then a legitimate one");
    let mut hostile_request =
        produce_request(&missing, (0..200).map(|p| (p, bomb.clone())).collect(), 1);
    hostile_request.timeout_ms = 30_000;
    let hostile: Vec<_> = (1..=2)
        .map(|corr| {
            tokio::spawn(spawned_produce(
                request_frame(7, corr, &hostile_request),
                corr,
            ))
        })
        .collect();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let mut legit = produce_request(&topic, vec![(0, legit_batch)], 1);
    legit.timeout_ms = 10_000;
    let (codes, _, took) = send_produce(request_frame(7, 50, &legit), 50).await?;
    println!("  legitimate request: {codes:?} after {took:?}");
    for (i, task) in hostile.into_iter().enumerate() {
        let (codes, answers, took) = task.await??;
        println!("  hostile request {i}: {answers} answers {codes:?} after {took:?}");
        assert_eq!(codes, BTreeSet::from([UNKNOWN_TOPIC_OR_PARTITION]));
    }
    assert_eq!(
        codes,
        BTreeSet::from([0]),
        "the legitimate request is stored"
    );
    assert_eq!(stored_rows(ctx.db(), &topic).await?, 1);
    println!("✅ the legitimate request stored within its wait");

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// A batch whose CRC is wrong.
fn bad_crc(batch: &Bytes) -> Bytes {
    let mut b = batch.to_vec();
    b[17..21].copy_from_slice(&[0, 0, 0, 0]);
    Bytes::from(b)
}

/// Kafka 3.7 keeps one entry per partition of a Produce (`KafkaApis.handleProduceRequest`
/// collects them in maps): a partition named twice, in one topic entry or in two, is
/// answered once, for its last entry, and only that entry is written.
pub async fn test_repeated_partition_entries_are_answered_once() -> TestResult {
    println!("=== Test: repeated partition entries are answered once ===\n");
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("net-dup").await;
    let missing = ctx.unique_topic("net-dup-missing").await;
    let created = create_topics_raw(vec![creatable(&topic, 2, 1)]).await?;
    assert_eq!(created[0].error_code, 0, "{created:?}");
    let mut wire = Wire::connect().await?;
    let value =
        |v: &'static str| plain_batch(&[Bytes::from_static(v.as_bytes())], Compression::None);
    let answers = |got: Vec<(String, i32, i16, i64)>| -> Vec<(String, i32, i16)> {
        got.into_iter()
            .map(|(t, p, code, _)| (t, p, code))
            .collect()
    };
    let t = |p, code| (topic.clone(), p, code);

    println!("Step 1: partition 0 twice, both valid: the second is written");
    let got = wire
        .produce_many(
            &[(&topic, vec![(0, value("first")), (0, value("second"))])],
            None,
        )
        .await?;
    assert_eq!(answers(got), vec![t(0, 0)]);
    let stored: Vec<String> = ctx
        .db()
        .query(
            "SELECT convert_from(m.value, 'UTF8') FROM kafka.messages m
             JOIN kafka.topics t ON t.id = m.topic_id WHERE t.name = $1",
            &[&topic],
        )
        .await?
        .iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(stored, vec!["second".to_string()]);
    println!("✅ one answer; only the last entry written");

    println!("Step 2: a corrupt entry, then a valid one; a valid one, then a corrupt one");
    let got = wire
        .produce_many(
            &[(&topic, vec![(0, bad_crc(&value("x"))), (0, value("y"))])],
            None,
        )
        .await?;
    assert_eq!(answers(got), vec![t(0, 0)]);
    let got = wire
        .produce_many(
            &[(&topic, vec![(0, value("z")), (0, bad_crc(&value("w")))])],
            None,
        )
        .await?;
    assert_eq!(answers(got), vec![t(0, CORRUPT_MESSAGE)]);
    assert_eq!(stored_rows(ctx.db(), &topic).await?, 2);
    println!("✅ the last entry decides");

    println!("Step 3: one partition in two topic entries, beside another partition");
    let got = wire
        .produce_many(
            &[
                (&topic, vec![(0, value("a")), (1, value("b"))]),
                (&topic, vec![(0, value("c"))]),
            ],
            None,
        )
        .await?;
    assert_eq!(answers(got), vec![t(0, 0), t(1, 0)]);
    assert_eq!(stored_rows(ctx.db(), &topic).await?, 4);
    println!("✅ answered once each");

    println!("Step 4: a partition of a topic that does not exist, twice");
    let got = wire
        .produce_many(
            &[(
                &missing,
                vec![(0, bad_crc(&value("x"))), (0, bad_crc(&value("y")))],
            )],
            None,
        )
        .await?;
    assert_eq!(
        answers(got),
        vec![(missing.clone(), 0, UNKNOWN_TOPIC_OR_PARTITION)]
    );
    println!("✅ answered once");

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
