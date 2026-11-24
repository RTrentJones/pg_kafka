//! The memory a request's decode may hold (`decode_meter`): frames of the smallest entries
//! of the APIs whose entries expand the most, stopped at the limit; what legitimate large
//! requests take; the inline / large-decode / budget path of [`decode_request`].

use bytes::{BufMut, BytesMut};

use super::super::*;
use crate::kafka::api::{RequestBody, APIS};
use crate::kafka::protocol::decode_budget::{
    DECODED_REQUESTS, INLINE_REQUEST_DECODE_BYTES, MAX_REQUEST_DECODE_BYTES,
};
use crate::testing::heap_peak;

fn uvarint(buf: &mut BytesMut, mut n: u64) {
    loop {
        let b = u8::try_from(n & 0x7f).unwrap();
        n >>= 7;
        if n == 0 {
            buf.put_u8(b);
            return;
        }
        buf.put_u8(b | 0x80);
    }
}

/// A request header v2 (flexible) or v1 for `api_key`/`version`, null client id.
fn header(api_key: i16, version: i16, flexible: bool) -> BytesMut {
    let mut buf = BytesMut::new();
    buf.put_i16(api_key);
    buf.put_i16(version);
    buf.put_i32(7);
    buf.put_i16(-1);
    if flexible {
        buf.put_u8(0);
    }
    buf
}

/// A compact array of `n` copies of `entry`.
fn compact_array(buf: &mut BytesMut, n: usize, entry: &[u8]) {
    uvarint(buf, n as u64 + 1);
    for _ in 0..n {
        buf.put_slice(entry);
    }
}

/// The frames of `n` of the smallest entries of the APIs whose entries expand the most
/// once decoded, as `(name, frame)`.
fn worst_case_frames(n: usize) -> Vec<(&'static str, BytesMut)> {
    let mut frames = Vec::new();
    // Metadata v9: topics of an empty name (2 bytes each).
    let mut f = header(3, 9, true);
    compact_array(&mut f, n, &[0x01, 0x00]);
    f.put_slice(&[0, 0, 0, 0]);
    frames.push(("Metadata v9 (empty names)", f));
    // DeleteGroups v2: empty group names (1 byte each).
    let mut f = header(42, 2, true);
    compact_array(&mut f, n, &[0x01]);
    f.put_u8(0);
    frames.push(("DeleteGroups v2 (empty names)", f));
    // DescribeGroups v5: empty group names.
    let mut f = header(15, 5, true);
    compact_array(&mut f, n, &[0x01]);
    f.put_slice(&[0, 0]);
    frames.push(("DescribeGroups v5 (empty names)", f));
    // OffsetFetch v8: groups of an empty id and null topics (3 bytes each).
    let mut f = header(9, 8, true);
    compact_array(&mut f, n, &[0x01, 0x00, 0x00]);
    f.put_slice(&[0, 0]);
    frames.push(("OffsetFetch v8 (empty groups)", f));
    // OffsetCommit v8: topics of an empty name and no partitions (3 bytes each).
    let mut f = header(8, 8, true);
    f.put_slice(&[0x02, b'g']);
    f.put_i32(-1);
    f.put_u8(0x01);
    f.put_u8(0x00);
    compact_array(&mut f, n, &[0x01, 0x01, 0x00]);
    f.put_u8(0);
    frames.push(("OffsetCommit v8 (empty topics)", f));
    // Fetch v12: topics of an empty name and no partitions.
    let mut f = header(1, 12, true);
    f.put_i32(-1);
    f.put_i32(0);
    f.put_i32(0);
    f.put_i32(1 << 20);
    f.put_u8(0);
    f.put_i32(0);
    f.put_i32(-1);
    compact_array(&mut f, n, &[0x01, 0x01, 0x00]);
    f.put_slice(&[0x01, 0x01, 0x00]);
    frames.push(("Fetch v12 (empty topics)", f));
    // ListOffsets v7: topics of an empty name and no partitions.
    let mut f = header(2, 7, true);
    f.put_i32(-1);
    f.put_u8(0);
    compact_array(&mut f, n, &[0x01, 0x01, 0x00]);
    f.put_u8(0);
    frames.push(("ListOffsets v7 (empty topics)", f));
    // DescribeConfigs v4: topic resources of an empty name and null keys (4 bytes each).
    let mut f = header(32, 4, true);
    compact_array(&mut f, n, &[0x02, 0x01, 0x00, 0x00]);
    f.put_slice(&[0, 0, 0]);
    frames.push(("DescribeConfigs v4 (empty resources)", f));
    // CreateTopics v5: one topic of `n` replica assignments without brokers (6 bytes each).
    let mut f = header(19, 5, true);
    f.put_u8(0x02);
    f.put_slice(&[0x02, b't']);
    f.put_i32(-1);
    f.put_i16(-1);
    let mut assignment = BytesMut::new();
    assignment.put_i32(0);
    assignment.put_slice(&[0x01, 0x00]);
    compact_array(&mut f, n / 6, &assignment);
    f.put_slice(&[0x01, 0x00]);
    f.put_i32(30_000);
    f.put_slice(&[0, 0]);
    frames.push(("CreateTopics v5 (empty assignments)", f));
    // AddPartitionsToTxn v3: topics of an empty name and no partitions.
    let mut f = header(24, 3, true);
    f.put_slice(&[0x02, b'x']);
    f.put_i64(1);
    f.put_i16(0);
    compact_array(&mut f, n, &[0x01, 0x01, 0x00]);
    f.put_u8(0);
    frames.push(("AddPartitionsToTxn v3 (empty topics)", f));
    // Produce v9: topics of an empty name and no partitions.
    let mut f = header(0, 9, true);
    f.put_u8(0x00);
    f.put_i16(1);
    f.put_i32(30_000);
    compact_array(&mut f, n, &[0x01, 0x01, 0x00]);
    f.put_u8(0);
    frames.push(("Produce v9 (empty topics)", f));
    frames
}

/// What decoding `frame` without a limit holds at once and at its end, per frame byte.
fn expansion(frame: &BytesMut) -> (f64, f64) {
    let len = frame.len();
    let frame = frame.clone().freeze();
    let (attempt, peak) = heap_peak(|| {
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        parse_within(frame, tx, usize::MAX)
    });
    let Attempt::Parsed(_, held) = attempt else {
        panic!("an unlimited decode cannot pass its limit");
    };
    (ratio(peak, len), ratio(held, len))
}

#[expect(clippy::cast_precision_loss, reason = "a ratio to print and compare")]
fn ratio(a: usize, b: usize) -> f64 {
    a as f64 / b as f64
}

#[test]
fn test_worst_case_entries_expansion_is_measured() {
    // 256 KiB of entries of each API: decoded, they take up to some 100 times that.
    for (name, frame) in worst_case_frames(128 * 1024) {
        let (peak, held) = expansion(&frame);
        println!(
            "{name}: {} bytes, peak {peak:.1}x, held {held:.1}x",
            frame.len()
        );
        assert!(peak < 200.0, "{name}: peak {peak:.1}x the frame");
    }
}

#[test]
fn test_a_decode_past_its_limit_is_stopped_near_it() {
    const LIMIT: usize = 4 * 1024 * 1024;
    // 2 MiB of entries of each API decode to far more than 4 MiB: each is stopped, holds
    // at most three times the limit at once, nothing is answered, and what it decoded is
    // freed.
    for (name, frame) in worst_case_frames(1024 * 1024) {
        let frame = frame.freeze();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let (attempt, peak) = heap_peak(|| parse_within(frame.clone(), tx, LIMIT));
        assert!(
            matches!(attempt, Attempt::OverLimit),
            "{name}: a decode of {} bytes must pass {LIMIT}",
            frame.len()
        );
        drop(attempt);
        println!(
            "{name}: stopped at a peak of {peak} bytes ({:.2}x the limit)",
            ratio(peak, LIMIT)
        );
        assert!(
            peak <= 3 * LIMIT,
            "{name}: held {peak} bytes for a limit of {LIMIT}"
        );
        assert!(
            rx.try_recv().is_err(),
            "{name}: a stopped decode answers nothing"
        );
    }
}

#[test]
fn test_the_reviewers_metadata_frame_is_refused() {
    // One Metadata v9 frame of 49 million empty topic names (98 MB) decoded to some 6 GB
    // and aborted the worker under a 4 GB address-space limit. It is now refused within
    // the request limit, and parse_request (the limit for callers outside the listener)
    // closes the connection.
    let frame = worst_case_frames(49_000_000).swap_remove(0).1;
    assert!(frame.len() > 98_000_000);
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let (result, peak) = heap_peak(|| parse_request(frame, tx));
    assert!(result.is_err(), "the connection closes");
    assert!(peak <= 3 * MAX_REQUEST_DECODE_BYTES, "held {peak} bytes");
    assert!(rx.try_recv().is_err());
}

/// A Metadata v10 request of `n` topic names of `len` bytes.
fn metadata_of_names(n: usize, len: usize) -> BytesMut {
    use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
    use kafka_protocol::messages::{MetadataRequest, TopicName};
    use kafka_protocol::protocol::{Encodable, StrBytes};
    let mut request = MetadataRequest::default();
    request.topics = Some(
        (0..n)
            .map(|i| {
                MetadataRequestTopic::default()
                    .with_name(Some(TopicName(StrBytes::from_string(format!("{i:0len$}")))))
            })
            .collect(),
    );
    let mut f = header(3, 10, true);
    request.encode(&mut f, 10).unwrap();
    f
}

#[test]
fn test_large_legitimate_requests_decode() {
    // 300,000 topic names of 50 bytes (a 16 MB frame): far more than a client asks for at
    // once, within the request limit.
    let frame = metadata_of_names(300_000, 50);
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    let (attempt, peak) = heap_peak(|| parse_within(frame.freeze(), tx, MAX_REQUEST_DECODE_BYTES));
    let Attempt::Parsed(Ok(Some(request)), held) = attempt else {
        panic!(
            "a Metadata of 300,000 names decodes: {}",
            matches!(attempt, Attempt::OverLimit)
        );
    };
    let RequestBody::Metadata(m) = &request.body else {
        panic!("a Metadata request");
    };
    assert_eq!(m.topics.as_ref().map(Vec::len), Some(300_000));
    println!("300,000 names: held {held}, peak {peak}");
}

#[test]
fn test_every_api_with_huge_array_counts_returns() {
    // A body of 0x7f bytes claims arrays of 2^31 entries (and 127 in varints) at every
    // level of every API and version: each decode reads at least one byte per entry, so
    // it ends with the frame or the meter, never in a loop over entries of nothing.
    for info in APIS {
        for version in info.min_version..=info.max_version {
            for flexible in [false, true] {
                let mut f = header(info.key, version, flexible);
                f.put_slice(&[0x7f; 4096]);
                let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
                let _ = parse_within(f.freeze(), tx, INLINE_REQUEST_DECODE_BYTES);
            }
        }
    }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("a test runtime")
}

#[test]
fn test_decode_request_decodes_large_requests_under_a_slot_and_charges_them() {
    runtime().block_on(async {
        // Decodes to more than the inline limit, less than the request limit.
        let frame = metadata_of_names(40_000, 20);
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let before = DECODED_REQUESTS.remaining();
        let request = decode_request(frame, tx).await.unwrap().unwrap();
        assert!(matches!(request.body, RequestBody::Metadata(_)));
        let charged = request.decode_charge.bytes();
        assert!(charged > INLINE_REQUEST_DECODE_BYTES, "{charged}");
        assert!(DECODED_REQUESTS.remaining() <= before - charged);
        drop(request);

        // Decodes to more than the request limit: the connection closes, nothing answered.
        let frame = worst_case_frames(40_000_000).swap_remove(0).1;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        assert!(decode_request(frame, tx).await.is_err());
        assert!(rx.try_recv().is_err());

        // A small request decodes inline and holds little.
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let frame = metadata_of_names(3, 10);
        let request = decode_request(frame, tx).await.unwrap().unwrap();
        assert!(
            request.decode_charge.bytes() < 4096,
            "{}",
            request.decode_charge.bytes()
        );
    });
}

#[test]
fn test_large_decoded_requests_leave_room_for_small_ones() {
    use crate::kafka::protocol::decode_budget::{
        DecodePool, CONTROL_RESERVE_BYTES, SMALL_REQUEST_BYTES,
    };
    static POOL: DecodePool = DecodePool::new(100 << 20);
    let big = 100 * 1024 * 1024 - CONTROL_RESERVE_BYTES;
    let held = POOL.charge_request(big).expect("fits leaving the reserve");
    assert!(POOL.charge_request(SMALL_REQUEST_BYTES + 1).is_none());
    let small: Vec<_> = (0..CONTROL_RESERVE_BYTES / SMALL_REQUEST_BYTES)
        .map(|_| {
            POOL.charge_request(SMALL_REQUEST_BYTES)
                .expect("the reserve")
        })
        .collect();
    assert_eq!(POOL.remaining(), 0);
    assert!(POOL.charge_request(1).is_none());
    drop(small);
    drop(held);
    assert_eq!(POOL.remaining(), 100 << 20);
    assert_eq!(
        DecodePool::request_room(SMALL_REQUEST_BYTES + 1),
        SMALL_REQUEST_BYTES + 1 + CONTROL_RESERVE_BYTES
    );
}

#[test]
fn test_a_decoded_request_waits_for_room_and_decodes_again() {
    use crate::kafka::protocol::decode_budget::DecodePool;
    use std::time::Duration;
    static POOL: DecodePool = DecodePool::new(1 << 20);
    static ROOMY: DecodePool = DecodePool::new(64 << 20);
    runtime().block_on(async {
        // Holds some 100 KB decoded: more than a small request, so it must leave the
        // control reserve, which this 1 MiB pool cannot.
        let frame = metadata_of_names(1500, 20);
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let started = std::time::Instant::now();
        assert!(
            decode_request_in(frame.clone(), tx, &POOL, Duration::from_millis(100))
                .await
                .is_err()
        );
        assert!(started.elapsed() >= Duration::from_millis(100), "it waited");
        assert_eq!(POOL.remaining(), 1 << 20, "a refused request holds nothing");

        // Room that comes free while it waits: it decodes again and is charged.
        let filler = ROOMY.charge_leaving(60 << 20, 0).unwrap();
        let release = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            drop(filler);
        });
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let request = decode_request_in(frame, tx, &ROOMY, Duration::from_secs(10))
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(request.body, RequestBody::Metadata(_)));
        assert!(request.decode_charge.bytes() > 64 * 1024);
        release.await.unwrap();
        // The request carries its charge: dropping it (as the DB thread does once its handler
        // ran) gives the charge back, whether or not a response was ever written.
        let held = 64 << 20;
        assert!(ROOMY.remaining() < held);
        drop(request);
        assert_eq!(ROOMY.remaining(), held);
    });
}
