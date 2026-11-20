//! Produce requests whose records the broker must refuse while decoding them: compressed
//! batches that expand past the per-request decode limit (a decompression bomb), partition
//! data that is not exactly one RecordBatch v2, and an acks=0 Produce at a version the
//! broker does not serve.

use super::{answers, client_config, rdkafka_roundtrip, stored_rows, time_until_closed, PORT};
use crate::common::{DynResult, TestResult};
use crate::setup::TestContext;
use crate::wire::records::plain_batch;
use crate::wire::requests::{creatable, create_topics_raw, produce_request};
use crate::wire::{decode_response, read_frame_within, request_frame};
use bytes::Bytes;
use kafka_protocol::messages::ProduceRequest;
use kafka_protocol::records::Compression;
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

/// MESSAGE_TOO_LARGE (Java's RecordTooLargeException).
const MESSAGE_TOO_LARGE: i16 = 10;
/// CORRUPT_MESSAGE (Java's CorruptRecordException).
const CORRUPT_MESSAGE: i16 = 2;
/// INVALID_RECORD (Java's InvalidRecordException).
const INVALID_RECORD: i16 = 87;

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

/// A RecordBatch v2 built by hand around the records section `section`, gzip-compressed,
/// declaring `record_count` records, with a valid CRC.
pub(crate) fn raw_batch(section: &[u8], record_count: i32) -> Bytes {
    use bytes::BufMut;
    use kafka_protocol::compression::{Compressor, Gzip};
    let mut records = bytes::BytesMut::new();
    Gzip::compress(&mut records, |tmp: &mut bytes::BytesMut| {
        tmp.put_slice(section);
        Ok(())
    })
    .expect("gzip compresses");
    batch_around(&records, record_count, 1)
}

/// `section` as one zstd frame of raw (stored) blocks whose header declares a
/// 2^`window_log`-byte window and no content size, as a streaming encoder writes it.
fn zstd_raw_frame(section: &[u8], window_log: u8) -> Vec<u8> {
    let mut frame = 0xFD2F_B528u32.to_le_bytes().to_vec();
    frame.push(0); // frame header descriptor: no content size, no checksum, no dictionary
    frame.push((window_log - 10) << 3); // window descriptor
    let blocks: Vec<&[u8]> = section.chunks(65_536).collect();
    for (i, block) in blocks.iter().enumerate() {
        let last = u32::from(i + 1 == blocks.len());
        let header = last | (u32::try_from(block.len()).expect("64 KiB") << 3); // raw block
        frame.extend_from_slice(&header.to_le_bytes()[..3]);
        frame.extend_from_slice(block);
    }
    frame
}

/// The header fields of a hand-built batch ([`batch_with`]).
#[derive(Debug, Clone, Copy)]
pub(crate) struct BatchHeader {
    pub(crate) base_offset: i64,
    /// Attributes, codec bits included.
    pub(crate) attributes: i16,
    /// `None`: one less than the record count (at least 0).
    pub(crate) last_offset_delta: Option<i32>,
    pub(crate) base_timestamp: i64,
    pub(crate) producer_id: i64,
    pub(crate) producer_epoch: i16,
    pub(crate) base_sequence: i32,
}

impl Default for BatchHeader {
    fn default() -> Self {
        Self {
            base_offset: 0,
            attributes: 0,
            last_offset_delta: None,
            base_timestamp: 0,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
        }
    }
}

/// A RecordBatch v2 around `records` (compressed with the codec the attributes name),
/// declaring `record_count` records, with a valid CRC.
pub(crate) fn batch_with(records: &[u8], record_count: i32, header: BatchHeader) -> Bytes {
    use bytes::BufMut;
    let mut tail = bytes::BytesMut::new();
    tail.put_i16(header.attributes);
    tail.put_i32(
        header
            .last_offset_delta
            .unwrap_or_else(|| record_count.max(1) - 1),
    );
    tail.put_i64(header.base_timestamp); // baseTimestamp
    tail.put_i64(header.base_timestamp); // maxTimestamp
    tail.put_i64(header.producer_id);
    tail.put_i16(header.producer_epoch);
    tail.put_i32(header.base_sequence);
    tail.put_i32(record_count);
    tail.put_slice(records);
    let mut batch = bytes::BytesMut::new();
    batch.put_i64(header.base_offset);
    batch.put_i32(i32::try_from(4 + 1 + 4 + tail.len()).expect("a test batch fits an i32"));
    batch.put_i32(0); // partitionLeaderEpoch
    batch.put_i8(2); // magic
    batch.put_u32(crc32c::crc32c(&tail));
    batch.put_slice(&tail);
    batch.freeze()
}

/// A RecordBatch v2 around `records` (compressed with the codec `attributes` names),
/// declaring `record_count` records, with a valid CRC.
fn batch_around(records: &[u8], record_count: i32, attributes: i16) -> Bytes {
    let header = BatchHeader {
        attributes,
        ..BatchHeader::default()
    };
    batch_with(records, record_count, header)
}

/// `v` as a zigzag varint (as long as it takes).
pub(crate) fn varint(v: i64) -> Vec<u8> {
    let mut z = ((v << 1) ^ (v >> 63)).cast_unsigned();
    let mut out = Vec::new();
    while z >= 0x80 {
        out.push(u8::try_from(z & 0x7f).expect("7 bits") | 0x80);
        z >>= 7;
    }
    out.push(u8::try_from(z).expect("7 bits"));
    out
}

/// One record: its body (the concatenated `fields`) behind its varint length.
pub(crate) fn record(fields: &[&[u8]]) -> Vec<u8> {
    let body = fields.concat();
    let mut record = varint(i64::try_from(body.len()).expect("small"));
    record.extend_from_slice(&body);
    record
}

/// `n` of the smallest records: 7 bytes each (null key and value, no headers).
fn tiny_records(n: usize) -> Vec<u8> {
    [12u8, 0, 0, 0, 1, 1, 0].repeat(n)
}

/// `n` records of `headers` headers each, every header 2 bytes (an empty key and a null
/// value).
fn header_records(n: usize, headers: u16) -> Vec<u8> {
    let zigzag = u32::from(headers) << 1;
    let mut count = Vec::new();
    let mut z = zigzag;
    while z >= 0x80 {
        count.push(u8::try_from(z & 0x7f).expect("7 bits") | 0x80);
        z >>= 7;
    }
    count.push(u8::try_from(z).expect("7 bits"));
    let mut body = vec![0u8, 0, 0, 1, 1];
    body.extend_from_slice(&count);
    body.extend_from_slice(&[0, 1].repeat(usize::from(headers)));
    let mut size = Vec::new();
    let mut z = u32::try_from(body.len()).expect("small") << 1;
    while z >= 0x80 {
        size.push(u8::try_from(z & 0x7f).expect("7 bits") | 0x80);
        z >>= 7;
    }
    size.push(u8::try_from(z).expect("7 bits"));
    let mut record = size;
    record.extend_from_slice(&body);
    record.repeat(n)
}

/// Batches that are small decoded but many records or headers, which each take some 50 to
/// 90 bytes once decoded: their declared record count is charged before anything decodes
/// and their headers before they are built, so they are refused with
/// MESSAGE_TOO_LARGE quickly and the listener's memory stays bounded. A record count that
/// does not match the records is INVALID_RECORD, also before the records are built.
/// Unchecked, these four requests took gigabytes.
pub async fn test_record_and_header_bombs_are_refused_before_decoding() -> TestResult {
    println!("=== Test: record-count and header bombs refused before decoding ===\n");
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("net-rec-bomb").await;
    let created = create_topics_raw(vec![creatable(&topic, 4, 1)]).await?;
    assert_eq!(created[0].error_code, 0, "{created:?}");
    let mut s = TcpStream::connect(("127.0.0.1", PORT)).await?;
    let peak_watch = reset_listener_peak(&ctx).await?;
    let timed = |started: Instant, what: &str| {
        let took = started.elapsed();
        println!("  {what}: answered in {took:?}");
        assert!(took < Duration::from_secs(2), "{what} took {took:?}");
    };

    let n = 1_300_000;
    println!("Step 1: three gzip batches of {n} 7-byte records (27 MB decoded)");
    let tiny = raw_batch(&tiny_records(n), i32::try_from(n)?);
    let parts = (0..3).map(|p| (p, tiny.clone())).collect();
    let started = Instant::now();
    let codes = produce_codes(&mut s, 1, &topic, parts).await?;
    timed(started, "tiny records");
    assert_eq!(codes, vec![MESSAGE_TOO_LARGE; 3]);
    println!("✅ refused: {codes:?}");

    println!("Step 2: one gzip batch of 1000 records of 8192 2-byte headers (16 MB decoded)");
    let headers = raw_batch(&header_records(1000, 8192), 1000);
    let started = Instant::now();
    let codes = produce_codes(&mut s, 2, &topic, vec![(0, headers)]).await?;
    timed(started, "header-heavy records");
    assert_eq!(codes, vec![MESSAGE_TOO_LARGE]);
    println!("✅ refused: {codes:?}");

    println!("Step 3: record counts that do not match the records");
    let section = tiny_records(300_000);
    for (corr, count) in (3..).zip([299_999, 300_001, 0]) {
        let started = Instant::now();
        let codes =
            produce_codes(&mut s, corr, &topic, vec![(1, raw_batch(&section, count))]).await?;
        timed(started, &format!("300000 records declared as {count}"));
        assert_eq!(codes, vec![INVALID_RECORD], "declared {count}");
    }
    println!("✅ refused as INVALID_RECORD");

    match peak_watch.and_then(|(pid, rss)| Some((rss, proc_kb(pid, "VmHWM:")?))) {
        Some((rss, peak)) => {
            let grew_mb = peak.saturating_sub(rss) / 1024;
            println!(
                "  listener RSS {} MB, peak while decoding +{grew_mb} MB",
                rss / 1024
            );
            assert!(grew_mb < 300, "decoding grew the listener by {grew_mb} MB");
        }
        None => println!("  (listener memory not readable from here; skipping the RSS check)"),
    }
    assert_eq!(
        stored_rows(ctx.db(), &topic).await?,
        0,
        "nothing is written"
    );

    println!("Step 4: the matching count is stored, and the broker still serves");
    let codes = produce_codes(
        &mut s,
        10,
        &topic,
        vec![(2, raw_batch(&tiny_records(1000), 1000))],
    )
    .await?;
    assert_eq!(codes, vec![0]);
    assert_eq!(stored_rows(ctx.db(), &topic).await?, 1000);
    assert!(answers(&mut s, 11).await, "the connection still answers");
    rdkafka_roundtrip(client_config(PORT), &topic, 3).await?;
    println!("✅ stored; rdkafka round trip works");

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// zstd batches whose frames declare windows up to 2^27 bytes, Kafka's limit (Java
/// producers at compression.zstd.level 20-22 use 2^25-2^27), are stored; a larger window is
/// refused and nothing of it is written.
pub async fn test_zstd_batches_with_large_windows_are_stored() -> TestResult {
    println!("=== Test: zstd windows up to 2^27 ===\n");
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("net-zstd-window").await;
    let created = create_topics_raw(vec![creatable(&topic, 1, 1)]).await?;
    assert_eq!(created[0].error_code, 0, "{created:?}");
    let mut s = TcpStream::connect(("127.0.0.1", PORT)).await?;
    let section = tiny_records(1000);
    let zstd = |window_log| batch_around(&zstd_raw_frame(&section, window_log), 1000, 4);

    for (corr, window_log) in (1..).zip([21u8, 25, 27]) {
        let codes = produce_codes(&mut s, corr, &topic, vec![(0, zstd(window_log))]).await?;
        assert_eq!(codes, vec![0], "window 2^{window_log}");
        println!("✅ window 2^{window_log}: stored");
    }
    assert_eq!(stored_rows(ctx.db(), &topic).await?, 3000);

    let codes = produce_codes(&mut s, 10, &topic, vec![(0, zstd(28))]).await?;
    assert_eq!(codes, vec![CORRUPT_MESSAGE], "window 2^28");
    assert_eq!(stored_rows(ctx.db(), &topic).await?, 3000);
    println!("✅ window 2^28: refused {codes:?}");

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// CRC-32 (IEEE), the checksum of a legacy message.
fn crc32_ieee(data: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &byte in data {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            crc = if crc & 1 == 0 {
                crc >> 1
            } else {
                (crc >> 1) ^ 0xEDB8_8320
            };
        }
    }
    !crc
}

/// One legacy MessageSet v0/v1 entry holding `value` (offset, size, CRC, magic,
/// attributes, a v1 timestamp, a null key, the value), well-formed.
fn legacy_entry(magic: u8, attributes: u8, value: &[u8]) -> Vec<u8> {
    let mut body = vec![magic, attributes];
    if magic == 1 {
        body.extend_from_slice(&0i64.to_be_bytes());
    }
    body.extend_from_slice(&(-1i32).to_be_bytes());
    body.extend_from_slice(&i32::try_from(value.len()).expect("small").to_be_bytes());
    body.extend_from_slice(value);
    let mut entry = 0i64.to_be_bytes().to_vec();
    entry.extend_from_slice(&i32::try_from(4 + body.len()).expect("small").to_be_bytes());
    entry.extend_from_slice(&crc32_ieee(&body).to_be_bytes());
    entry.extend_from_slice(&body);
    entry
}

/// Partition data that is not exactly one RecordBatch v2 is refused as Kafka 3.7 refuses
/// it, and nothing is written: legacy MessageSets (Produce v3+ carries magic 2 only; one
/// was stored before, a gzip wrapper as one garbage record and a cut-short set as its
/// prefix), empty or cut-short data and a negative record count are INVALID_RECORD, a bad
/// CRC is CORRUPT_MESSAGE.
pub async fn test_produce_of_data_that_is_not_one_v2_batch_is_refused() -> TestResult {
    println!("=== Test: Produce v3+ partition data must be one RecordBatch v2 ===\n");
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("net-magic").await;
    let created = create_topics_raw(vec![creatable(&topic, 1, 1)]).await?;
    assert_eq!(created[0].error_code, 0, "{created:?}");
    let mut s = TcpStream::connect(("127.0.0.1", PORT)).await?;

    let good = plain_batch(&[Bytes::from_static(b"v")], Compression::None);
    let negative_count = batch_with(&tiny_records(1), -1, BatchHeader::default());
    let mut bad_crc = good.to_vec();
    *bad_crc.last_mut().expect("not empty") ^= 1;
    let mut two_legacy = legacy_entry(1, 0, b"one");
    two_legacy.extend_from_slice(&legacy_entry(1, 0, b"two"));
    let mut cut_short_legacy = legacy_entry(0, 0, b"kept");
    cut_short_legacy.extend_from_slice(&legacy_entry(0, 0, b"lost")[..20]);
    let cases: Vec<(&str, Bytes, i16)> = vec![
        (
            "legacy v0",
            legacy_entry(0, 0, b"v0").into(),
            INVALID_RECORD,
        ),
        (
            "legacy v1",
            legacy_entry(1, 0, b"v1").into(),
            INVALID_RECORD,
        ),
        (
            "legacy gzip wrapper",
            legacy_entry(1, 1, b"zz").into(),
            INVALID_RECORD,
        ),
        ("two legacy messages", two_legacy.into(), INVALID_RECORD),
        (
            "cut-short legacy set",
            cut_short_legacy.into(),
            INVALID_RECORD,
        ),
        ("empty", Bytes::new(), INVALID_RECORD),
        (
            "cut-short batch",
            good.slice(..good.len() - 3),
            INVALID_RECORD,
        ),
        ("negative record count", negative_count, INVALID_RECORD),
        ("bad CRC", bad_crc.into(), CORRUPT_MESSAGE),
    ];
    for (corr, (what, data, expected)) in (1..).zip(cases) {
        let codes = produce_codes(&mut s, corr, &topic, vec![(0, data)]).await?;
        assert_eq!(codes, vec![expected], "{what}");
        println!("✅ {what}: {codes:?}");
    }
    assert_eq!(
        stored_rows(ctx.db(), &topic).await?,
        0,
        "nothing is written"
    );

    let codes = produce_codes(&mut s, 100, &topic, vec![(0, good)]).await?;
    assert_eq!(codes, vec![0]);
    assert_eq!(stored_rows(ctx.db(), &topic).await?, 1);
    println!("✅ a valid batch on the same connection is stored");

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

/// UNSUPPORTED_COMPRESSION_TYPE: zstd on Produce v3-v6.
const UNSUPPORTED_COMPRESSION_TYPE: i16 = 76;

/// The `headers` column of `topic`'s rows as text, and their keys, in offset order.
async fn stored_keys_and_headers(
    db: &tokio_postgres::Client,
    topic: &str,
) -> DynResult<Vec<(Option<Vec<u8>>, String)>> {
    Ok(db
        .query(
            "SELECT m.key, coalesce(m.headers::text, '') FROM kafka.messages m \
             JOIN kafka.topics t ON t.id = m.topic_id WHERE t.name = $1 \
             ORDER BY m.partition_offset",
            &[&topic],
        )
        .await?
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect())
}

/// Batches are read with Kafka 3.7's grammar and checks, and nothing else is written:
/// a 6-byte timestamp-delta varlong (a 5-byte varint reader takes the key that follows
/// for headers it charged nothing for) holds just its key; duplicate header keys are kept
/// in order and a non-UTF-8 header key is accepted; a batch of no records (which, from an
/// idempotent producer, used to enter its batch cache and turn its next batch into a
/// "duplicate") is INVALID_RECORD, as are a control batch, a lastOffsetDelta that
/// disagrees with the record count and a base offset other than 0; a magic past 2 is
/// CORRUPT_MESSAGE; zstd on Produce v3 is UNSUPPORTED_COMPRESSION_TYPE; and bytes after the
/// batch too few for another batch are ignored.
pub async fn test_record_batches_are_read_as_kafka_reads_them() -> TestResult {
    use crate::wire::Wire;
    println!("=== Test: record batches read as Kafka 3.7 reads them ===\n");
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("net-grammar").await;
    let created = create_topics_raw(vec![creatable(&topic, 3, 1)]).await?;
    assert_eq!(created[0].error_code, 0, "{created:?}");
    let mut wire = Wire::connect().await?;
    let x = record(&[
        &[0],
        &varint(0),
        &varint(0),
        &varint(-1),
        &varint(1),
        b"x",
        &varint(0),
    ]);

    println!("Step 1: 20 records whose timestamp delta is a 6-byte varlong");
    let mut key = varint(-1);
    key.extend(varint(500));
    for i in 0..500u16 {
        let (high, low) = (u8::try_from(i / 96)?, u8::try_from(i % 96)?);
        key.extend_from_slice(&[0x04, 0x20 + high, 0x20 + low, 0x01]);
    }
    let key_length = varint(i64::try_from(key.len())?);
    let shifted = record(&[
        &[0],
        &[0x80, 0x80, 0x80, 0x80, 0x80, 0x00],
        &varint(i64::try_from(key_length.len())?),
        &key_length,
        &key,
        &varint(-1),
        &varint(0),
    ]);
    let batch = batch_with(&shifted.repeat(20), 20, BatchHeader::default());
    assert_eq!(wire.produce(&topic, 0, batch, None).await?.0, 0);
    let rows = stored_keys_and_headers(ctx.db(), &topic).await?;
    assert_eq!(rows.len(), 20);
    for (stored_key, headers) in &rows {
        assert_eq!(stored_key.as_deref(), Some(&key[..]), "the key Kafka reads");
        assert_eq!(headers, "{}", "no headers");
    }
    println!(
        "✅ 20 records, each its {}-byte key and no headers",
        key.len()
    );

    println!("Step 2: duplicate and non-UTF-8 header keys");
    let headed = record(&[
        &[0],
        &varint(0),
        &varint(0),
        &varint(-1),
        &varint(1),
        b"h",
        &varint(3),
        &varint(1),
        b"a",
        &varint(1),
        b"1",
        &varint(1),
        b"a",
        &varint(1),
        b"2",
        &varint(2),
        &[0xff, 0xfe],
        &varint(-1),
    ]);
    let batch = batch_with(&headed, 1, BatchHeader::default());
    assert_eq!(wire.produce(&topic, 0, batch, None).await?.0, 0);
    let rows = stored_keys_and_headers(ctx.db(), &topic).await?;
    let headers: serde_json::Value = serde_json::from_str(&rows[20].1)?;
    assert_eq!(
        headers,
        serde_json::json!([["a", "31"], ["a", "32"], ["\u{fffd}\u{fffd}", ""]]),
        "every header, in order"
    );
    println!("✅ stored {headers}");

    println!("Step 3: an idempotent producer's batch of no records, then its first batch");
    let (code, producer_id, epoch) = wire.init_producer_id(None, 60_000).await?;
    assert_eq!(code, 0);
    let idempotent = |count: i32, records: &[u8], base_sequence: i32| {
        let header = BatchHeader {
            producer_id,
            producer_epoch: epoch,
            base_sequence,
            last_offset_delta: Some(0),
            ..BatchHeader::default()
        };
        batch_with(records, count, header)
    };
    let (code, _) = wire.produce(&topic, 1, idempotent(0, &[], 0), None).await?;
    assert_eq!(code, INVALID_RECORD, "a batch of no records");
    for sequence in [0, 1] {
        let (code, base) = wire
            .produce(&topic, 1, idempotent(1, &x, sequence), None)
            .await?;
        assert_eq!(
            (code, base),
            (0, i64::from(sequence)),
            "sequence {sequence}"
        );
    }
    println!("✅ refused; sequences 0 and 1 then stored at offsets 0 and 1");

    println!("Step 4: batch headers Kafka refuses, and data after a batch it ignores");
    let with = |header: BatchHeader| batch_with(&x, 1, header);
    let xy = [
        x.clone(),
        record(&[
            &[0],
            &varint(0),
            &varint(1),
            &varint(-1),
            &varint(1),
            b"y",
            &varint(0),
        ]),
    ]
    .concat();
    let mut magic9 = with(BatchHeader::default()).to_vec();
    magic9[16] = 9;
    let cases: Vec<(&str, Bytes, i16)> = vec![
        (
            "a control batch",
            with(BatchHeader {
                attributes: 0x20,
                ..BatchHeader::default()
            }),
            INVALID_RECORD,
        ),
        (
            "lastOffsetDelta past the records",
            batch_with(
                &xy,
                2,
                BatchHeader {
                    last_offset_delta: Some(5),
                    ..BatchHeader::default()
                },
            ),
            INVALID_RECORD,
        ),
        (
            "base offset 5",
            with(BatchHeader {
                base_offset: 5,
                ..BatchHeader::default()
            }),
            INVALID_RECORD,
        ),
        ("magic 9", magic9.into(), CORRUPT_MESSAGE),
    ];
    for (what, data, expected) in cases {
        let (code, _) = wire.produce(&topic, 2, data, None).await?;
        assert_eq!(code, expected, "{what}");
        println!("  {what}: {code}");
    }
    let zstd = with(BatchHeader {
        attributes: 4,
        ..BatchHeader::default()
    });
    let mut request = produce_request(&topic, vec![(2, zstd)], -1);
    request.timeout_ms = 10_000;
    let response: kafka_protocol::messages::ProduceResponse = wire.call(3, &request).await?;
    let code = response.responses[0].partition_responses[0].error_code;
    assert_eq!(code, UNSUPPORTED_COMPRESSION_TYPE, "zstd on Produce v3");
    println!("  zstd on Produce v3: {code}");
    assert_eq!(
        stored_rows(ctx.db(), &topic).await?,
        23,
        "nothing more is written"
    );
    let trailing = Bytes::from([&with(BatchHeader::default())[..], &[0; 5]].concat());
    assert_eq!(wire.produce(&topic, 2, trailing, None).await?, (0, 0));
    assert_eq!(stored_rows(ctx.db(), &topic).await?, 24);
    println!("✅ refused as Kafka refuses them; 5 trailing bytes ignored");

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
