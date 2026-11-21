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

/// Topics of the many-topics request: one partition each, of empty records, some 630 KB
/// on the wire.
const MANY_TOPICS: usize = 30_000;

/// A Produce naming 30,000 topics that do not exist, one partition of empty records each:
/// every partition is refused and answered UNKNOWN_TOPIC_OR_PARTITION on the DB thread.
/// Answering takes time linear in the topics, so the request is answered quickly and a
/// produce on another connection meanwhile is not held up behind it.
pub async fn test_a_produce_of_many_refused_topics_is_answered_quickly() -> TestResult {
    use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
    use kafka_protocol::messages::TopicName;
    use kafka_protocol::protocol::StrBytes;
    println!("=== Test: a Produce of many refused topics is answered quickly ===\n");
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("net-many-probe").await;
    let prefix = ctx.unique_topic("net-many").await;
    let created = create_topics_raw(vec![creatable(&topic, 1, 1)]).await?;
    assert_eq!(created[0].error_code, 0, "{created:?}");

    let mut request = ProduceRequest::default();
    request.acks = -1;
    request.timeout_ms = 30_000;
    request.topic_data = (0..MANY_TOPICS)
        .map(|i| {
            let mut p = PartitionProduceData::default();
            p.index = 0;
            p.records = Some(Bytes::new());
            let mut t = TopicProduceData::default();
            t.name = TopicName(StrBytes::from_string(format!("{prefix}-{i:06}")));
            t.partition_data = vec![p];
            t
        })
        .collect();
    let frame = request_frame(7, 1, &request);
    println!(
        "Step 1: one request of {MANY_TOPICS} topics ({} bytes), produces alongside",
        frame.len()
    );
    let mut probe = Wire::connect().await?;
    let value = plain_batch(&[Bytes::from_static(b"p")], Compression::None);
    let (code, _) = probe.produce(&topic, 0, value.clone(), None).await?;
    assert_eq!(code, 0);
    let many = tokio::spawn(spawned_produce(frame, 1));
    let mut worst = Duration::ZERO;
    let mut probes = 0;
    while !many.is_finished() {
        let started = Instant::now();
        let (code, _) = probe.produce(&topic, 0, value.clone(), None).await?;
        assert_eq!(code, 0);
        worst = worst.max(started.elapsed());
        probes += 1;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let (codes, answers, took) = many.await??;
    println!("  answered {answers} partitions {codes:?} after {took:?}");
    println!("  {probes} produces alongside, the slowest {worst:?}");
    assert_eq!(answers, MANY_TOPICS);
    assert_eq!(codes, BTreeSet::from([UNKNOWN_TOPIC_OR_PARTITION]));
    assert!(took < Duration::from_secs(3), "answered after {took:?}");
    assert!(
        worst < Duration::from_secs(2),
        "a produce alongside took {worst:?}"
    );
    let created: i64 = ctx
        .db()
        .query_one(
            "SELECT count(*) FROM kafka.topics WHERE name LIKE $1 || '-%'",
            &[&prefix],
        )
        .await?
        .get(0);
    assert_eq!(created, 0, "no refused topic is created");
    println!("✅ answered quickly; the produce alongside was not held up");

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// TRANSACTIONAL_ID_AUTHORIZATION_FAILED.
const TRANSACTIONAL_ID_AUTHORIZATION_FAILED: i16 = 53;
/// INVALID_RECORD.
const INVALID_RECORD: i16 = 87;

/// The values stored in `topic`'s partition `partition`, in offset order.
async fn stored_values(ctx: &TestContext, topic: &str, partition: i32) -> DynResult<Vec<String>> {
    Ok(ctx
        .db()
        .query(
            "SELECT convert_from(m.value, 'UTF8') FROM kafka.messages m
             JOIN kafka.topics t ON t.id = m.topic_id
             WHERE t.name = $1 AND m.partition_id = $2 ORDER BY m.partition_offset",
            &[&topic, &partition],
        )
        .await?
        .iter()
        .map(|r| r.get(0))
        .collect())
}

/// Kafka 3.7 reads the first batch of every entry of a Produce as sent before it keeps
/// one entry per partition, and of a partition's entries writes the last one
/// `ProduceRequest.validateRecords` accepts while answering the last one it refuses:
/// a replaced transactional batch without a transactional id, or a replaced batch whose
/// length cannot be read, still fails the whole request; a valid entry beside one of two
/// batches is written and answered INVALID_RECORD, in either order.
pub async fn test_repeated_entries_are_checked_as_kafka_checks_them() -> TestResult {
    use crate::wire::records::batch;
    println!("=== Test: repeated partition entries are checked as Kafka checks them ===\n");
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("net-dup-check").await;
    let created = create_topics_raw(vec![creatable(&topic, 2, 1)]).await?;
    assert_eq!(created[0].error_code, 0, "{created:?}");
    let mut wire = Wire::connect().await?;
    let value = |v: &str| plain_batch(&[Bytes::from(v.to_string())], Compression::None);
    let codes = |got: Vec<(String, i32, i16, i64)>| -> Vec<(i32, i16)> {
        got.into_iter().map(|(_, p, code, _)| (p, code)).collect()
    };
    let two = |b: Bytes| Bytes::from([&b[..], &b[..]].concat());

    println!("Step 1: a replaced transactional batch, no transactional id");
    let txn = batch(&["t"], 7, 0, 0, true, None);
    let got = wire
        .produce_many(
            &[(&topic, vec![(0, txn), (0, value("a")), (1, value("b"))])],
            None,
        )
        .await?;
    assert_eq!(
        codes(got),
        vec![
            (0, TRANSACTIONAL_ID_AUTHORIZATION_FAILED),
            (1, TRANSACTIONAL_ID_AUTHORIZATION_FAILED)
        ]
    );
    println!("Step 2: a replaced batch of length 13");
    let mut short = value("x").to_vec();
    short[8..12].copy_from_slice(&13i32.to_be_bytes());
    let got = wire
        .produce_many(
            &[(
                &topic,
                vec![(0, Bytes::from(short)), (0, value("a")), (1, value("b"))],
            )],
            None,
        )
        .await?;
    assert_eq!(codes(got), vec![(0, CORRUPT_MESSAGE), (1, CORRUPT_MESSAGE)]);
    assert_eq!(stored_rows(ctx.db(), &topic).await?, 0, "nothing written");
    println!("✅ the whole request refused; nothing written");

    println!("Step 3: a valid entry, then one of two batches; then the other order");
    let got = wire
        .produce_many(
            &[(&topic, vec![(0, value("a")), (0, two(value("b")))])],
            None,
        )
        .await?;
    assert_eq!(codes(got), vec![(0, INVALID_RECORD)]);
    let got = wire
        .produce_many(
            &[(&topic, vec![(0, two(value("c"))), (0, value("d"))])],
            None,
        )
        .await?;
    assert_eq!(codes(got), vec![(0, INVALID_RECORD)]);
    assert_eq!(
        stored_values(&ctx, &topic, 0).await?,
        vec!["a".to_string(), "d".to_string()]
    );
    println!("✅ the valid entry written, INVALID_RECORD answered");

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// REQUEST_TIMED_OUT.
const REQUEST_TIMED_OUT: i16 = 7;

/// A gzip stream of `blocks` dynamic-Huffman deflate blocks that decode to nothing (each
/// declares its own code tables, some 11 bytes), then a final empty block: input that
/// costs a decoder some 200 ns per byte and charges no output.
fn gzip_of_empty_blocks(blocks: usize) -> Vec<u8> {
    struct Bits {
        acc: u64,
        n: u32,
        out: Vec<u8>,
    }
    impl Bits {
        fn put(&mut self, value: u64, bits: u32) {
            self.acc |= value << self.n;
            self.n += bits;
            while self.n >= 8 {
                self.out.push((self.acc & 0xff) as u8);
                self.acc >>= 8;
                self.n -= 8;
            }
        }
    }
    const ORDER: [u8; 18] = [16, 17, 18, 0, 8, 7, 9, 6, 10, 5, 11, 4, 12, 3, 13, 2, 14, 1];
    let mut bits = Bits {
        acc: 0,
        n: 0,
        out: vec![0x1f, 0x8b, 8, 0, 0, 0, 0, 0, 0, 0xff],
    };
    for _ in 0..blocks {
        bits.put(0, 1);
        bits.put(2, 2);
        bits.put(0, 5);
        bits.put(0, 5);
        bits.put(14, 4);
        for symbol in ORDER {
            bits.put(u64::from(symbol == 1 || symbol == 18), 3);
        }
        for (value, n) in [(0, 1), (1, 1), (127, 7), (1, 1), (106, 7), (0, 1), (0, 1)] {
            bits.put(value, n);
        }
        bits.put(1, 1);
    }
    bits.put(1, 1);
    bits.put(1, 2);
    bits.put(0, 7);
    if bits.n > 0 {
        bits.out.push((bits.acc & 0xff) as u8);
    }
    bits.out.extend_from_slice(&[0; 8]);
    bits.out
}

/// A one-record gzip batch whose section is [`gzip_of_empty_blocks`].
fn empty_blocks_batch(blocks: usize) -> Bytes {
    batch_with(
        &gzip_of_empty_blocks(blocks),
        1,
        BatchHeader {
            attributes: 1,
            ..BatchHeader::default()
        },
    )
}

/// The decode of a Produce is bounded by time as well as by what it charges, so input that
/// decodes to nothing cannot hold a network I/O thread or a large-decode slot. Four
/// requests of 1.7 MB of empty gzip blocks at once (each some 0.35 s to decode) leave
/// ApiVersions on another connection fast; two of 94 MB (some 15 s each) hold the two
/// large-decode slots for about two seconds each and are answered REQUEST_TIMED_OUT, and a
/// legitimate request that needs a slot (a 5 MB gzip batch) is stored behind them.
pub async fn test_input_that_decodes_to_nothing_is_bounded_in_time() -> TestResult {
    println!("=== Test: input that decodes to nothing is bounded in time ===\n");
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("net-empty-blocks").await;
    let created = create_topics_raw(vec![creatable(&topic, 1, 1)]).await?;
    assert_eq!(created[0].error_code, 0, "{created:?}");

    let small = produce_request(&topic, vec![(0, empty_blocks_batch(150_000))], 1);
    println!("Step 1: four 1.7 MB requests at once, ApiVersions alongside");
    let inline: Vec<_> = (1..=4)
        .map(|corr| tokio::spawn(spawned_produce(request_frame(7, corr, &small), corr)))
        .collect();
    tokio::time::sleep(Duration::from_millis(20)).await;
    let (worst, calls) = worst_api_versions(Duration::from_secs(2)).await?;
    println!("  ApiVersions: {calls} calls, the slowest {worst:?}");
    for (i, task) in inline.into_iter().enumerate() {
        let (codes, answers, took) = task.await??;
        println!("  request {i}: {answers} answers {codes:?} after {took:?}");
        assert_eq!(answers, 1);
        assert!(!codes.contains(&0), "nothing decodes: {codes:?}");
    }
    assert!(
        worst < Duration::from_millis(120),
        "an ApiVersions took {worst:?} while the requests decoded"
    );
    println!("✅ ApiVersions stayed fast");

    println!("Step 2: two 94 MB requests, then a legitimate 5 MB gzip request");
    let mut large = produce_request(&topic, vec![(0, empty_blocks_batch(8_500_000))], 1);
    large.timeout_ms = 30_000;
    println!(
        "  hostile request: {} bytes",
        request_frame(7, 0, &large).len()
    );
    let hostile: Vec<_> = (11..=12)
        .map(|corr| tokio::spawn(spawned_produce(request_frame(7, corr, &large), corr)))
        .collect();
    tokio::time::sleep(Duration::from_millis(500)).await;
    let mut seed = 5u64;
    let value = hex_value(&mut seed, 5_000_000);
    let legit_batch = raw_batch(
        &record(&[
            &[0],
            &varint(0),
            &varint(0),
            &varint(-1),
            &varint(i64::try_from(value.len())?),
            &value,
            &varint(0),
        ]),
        1,
    );
    let mut legit = produce_request(&topic, vec![(0, legit_batch)], 1);
    legit.timeout_ms = 10_000;
    let (codes, _, took) = send_produce(request_frame(7, 50, &legit), 50).await?;
    println!("  legitimate request: {codes:?} after {took:?}");
    for (i, task) in hostile.into_iter().enumerate() {
        let (codes, _, took) = task.await??;
        println!("  hostile request {i}: {codes:?} after {took:?}");
        assert_eq!(codes, BTreeSet::from([REQUEST_TIMED_OUT]));
        assert!(took < Duration::from_secs(8), "answered after {took:?}");
    }
    assert_eq!(
        codes,
        BTreeSet::from([0]),
        "the legitimate request is stored"
    );
    assert!(took < Duration::from_secs(5), "stored after {took:?}");
    assert_eq!(stored_rows(ctx.db(), &topic).await?, 1);
    println!("✅ the slots freed in time; the legitimate request stored");

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// Partition entries of the long-name requests below: some 200 KB on the wire each.
const LONG_NAME_ENTRIES: i32 = 20_000;
/// A topic name as long as a frame allows (Kafka answers a name this long, of a topic that
/// cannot exist, UNKNOWN_TOPIC_OR_PARTITION on every partition).
const LONG_NAME_BYTES: usize = 32_000;

/// How much the listener's peak memory grew (MB) while `send` ran, or `None` when this
/// process cannot read the backend's /proc entry.
async fn listener_growth_mb<F: std::future::Future<Output = DynResult<()>>>(
    ctx: &TestContext,
    send: F,
) -> DynResult<Option<u64>> {
    use super::produce_decoding::{proc_kb, reset_listener_peak};
    let watch = reset_listener_peak(ctx).await?;
    send.await?;
    Ok(watch.and_then(|(pid, rss)| Some(proc_kb(pid, "VmHWM:")?.saturating_sub(rss) / 1024)))
}

/// A Produce and a long-polling Fetch of one 32,000-byte topic name and 20,000 partition
/// entries (8 and 16 bytes each on the wire): the broker holds the name once per topic,
/// not once per partition (640 MB for one 200 KB request), answers every partition
/// UNKNOWN_TOPIC_OR_PARTITION, as Kafka 3.7 does, and keeps serving.
pub async fn test_a_long_topic_name_is_held_once_per_topic() -> TestResult {
    use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
    use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
    use kafka_protocol::messages::{FetchRequest, TopicName};
    use kafka_protocol::protocol::StrBytes;
    println!("=== Test: a long topic name is held once per topic ===\n");
    let ctx = TestContext::new().await?;
    let name = TopicName(StrBytes::from_string("n".repeat(LONG_NAME_BYTES)));

    println!("Step 1: Produce, {LONG_NAME_ENTRIES} partitions of null records");
    let mut produce = ProduceRequest::default();
    produce.acks = 1;
    produce.timeout_ms = 30_000;
    let mut topic = TopicProduceData::default();
    topic.name = name.clone();
    topic.partition_data = (0..LONG_NAME_ENTRIES)
        .map(|i| {
            let mut p = PartitionProduceData::default();
            p.index = i;
            p
        })
        .collect();
    produce.topic_data = vec![topic];
    let frame = request_frame(7, 1, &produce);
    println!("  request frame: {} bytes", frame.len());
    let mut s = TcpStream::connect(("127.0.0.1", PORT)).await?;
    let mut produced = None;
    let grew = listener_growth_mb(&ctx, async {
        s.write_all(&frame).await?;
        let response = read_frame_within(&mut s, Duration::from_secs(60))
            .await?
            .ok_or("connection closed instead of a produce response")?;
        produced = Some(decode_response::<ProduceRequest>(7, &response)?.1);
        Ok(())
    })
    .await?;
    let produced = produced.ok_or("no produce response")?;
    assert_eq!(produced.responses.len(), 1, "one topic answered");
    let codes: Vec<i16> = produced.responses[0]
        .partition_responses
        .iter()
        .map(|p| p.error_code)
        .collect();
    assert_eq!(codes.len(), usize::try_from(LONG_NAME_ENTRIES)?);
    assert!(codes.iter().all(|&c| c == UNKNOWN_TOPIC_OR_PARTITION));
    check_growth("produce", grew);

    println!("Step 2: a long-polling Fetch v4, {LONG_NAME_ENTRIES} partitions");
    let mut fetch = FetchRequest::default();
    fetch.replica_id = (-1).into();
    fetch.max_wait_ms = 500;
    fetch.min_bytes = 1;
    fetch.max_bytes = 1 << 20;
    let mut topic = FetchTopic::default();
    topic.topic = name;
    topic.partitions = (0..LONG_NAME_ENTRIES)
        .map(|i| {
            let mut p = FetchPartition::default();
            p.partition = i;
            p.partition_max_bytes = 1 << 20;
            p
        })
        .collect();
    fetch.topics = vec![topic];
    let frame = request_frame(4, 2, &fetch);
    println!("  request frame: {} bytes", frame.len());
    let mut fetched = None;
    let grew = listener_growth_mb(&ctx, async {
        s.write_all(&frame).await?;
        let response = read_frame_within(&mut s, Duration::from_secs(60))
            .await?
            .ok_or("connection closed instead of a fetch response")?;
        fetched = Some(decode_response::<FetchRequest>(4, &response)?.1);
        Ok(())
    })
    .await?;
    let fetched = fetched.ok_or("no fetch response")?;
    assert_eq!(fetched.responses.len(), 1, "one topic answered");
    assert_eq!(
        fetched.responses[0].partitions.len(),
        usize::try_from(LONG_NAME_ENTRIES)?
    );
    assert!(fetched.responses[0]
        .partitions
        .iter()
        .all(|p| p.error_code == UNKNOWN_TOPIC_OR_PARTITION));
    check_growth("fetch", grew);

    let mut probe = TcpStream::connect(("127.0.0.1", PORT)).await?;
    probe.write_all(&api_versions_frame(3)).await?;
    let answered = read_frame_within(&mut probe, Duration::from_secs(10)).await?;
    assert!(answered.is_some(), "the broker still answers");
    println!("✅ answered; the broker still serves");

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// Assert the listener grew by little: the request itself is 200 KB; a copy of the name
/// per partition would be 640 MB.
fn check_growth(what: &str, grew: Option<u64>) {
    match grew {
        Some(grew) => {
            println!("  {what}: listener peak +{grew} MB");
            assert!(grew < 64, "{what} grew the listener by {grew} MB");
        }
        None => println!("  (listener memory not readable from here; skipping the RSS check)"),
    }
}
