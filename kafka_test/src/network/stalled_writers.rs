//! Clients that stop reading their responses. A connection whose responses cannot reach the
//! socket keeps at most its in-flight requests' responses, never the memory of the requests
//! they answer: what a request decoded to is given back once the DB thread has run it, so
//! connections that pipeline requests and then read nothing cannot hold the decoded-request
//! budget and lock other clients out. And a connection whose writer makes no progress counts
//! as idle: `pg_kafka.connections_max_idle_ms` closes it, as Kafka's idle expiry closes a
//! muted connection whose response never goes out.

use super::fetch_limits::AbortOnDrop;
use super::{client_config, port, rdkafka_roundtrip};
use crate::common::{DynResult, TestResult};
use crate::setup::TestContext;
use crate::wire::{correlation_of, decode_response, read_frame, read_frame_within, request_frame};
use kafka_protocol::messages::metadata_request::{MetadataRequest, MetadataRequestTopic};
use kafka_protocol::messages::TopicName;
use kafka_protocol::protocol::StrBytes;
use std::net::{Ipv4Addr, SocketAddr};
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpSocket, TcpStream};

/// Requests a connection may have read but not answered (the broker's in-flight cap).
const IN_FLIGHT: usize = 128;

/// Connections that stop reading.
const HOGS: u8 = 3;

/// The Metadata version the requests use: v4 has `allow_auto_topic_creation`, so names
/// that do not exist are answered UNKNOWN_TOPIC_OR_PARTITION and nothing is created.
const METADATA_VERSION: i16 = 4;

/// Metadata v4 of `names`, without auto-creation.
pub(super) fn metadata_of(names: impl IntoIterator<Item = String>) -> MetadataRequest {
    let mut req = MetadataRequest::default();
    req.topics = Some(
        names
            .into_iter()
            .map(|n| {
                let mut t = MetadataRequestTopic::default();
                t.name = Some(TopicName(StrBytes::from_string(n)));
                t
            })
            .collect(),
    );
    req.allow_auto_topic_creation = false;
    req
}

/// `n` distinct names that do not exist: `prefix`, a counter, padded to 24 bytes.
fn distinct(prefix: &str, n: usize) -> impl Iterator<Item = String> + '_ {
    (0..n).map(move |i| format!("{prefix}-{i:0>16}"))
}

/// A connection from `src` that reads nothing: its receive buffer is as small as the
/// kernel allows, so a few responses fill it and the broker's writer stalls.
pub(super) async fn non_reading(src: Ipv4Addr) -> std::io::Result<TcpStream> {
    let socket = TcpSocket::new_v4()?;
    socket.set_recv_buffer_size(4096)?;
    socket.bind(SocketAddr::from((src, 0)))?;
    socket
        .connect(SocketAddr::from((Ipv4Addr::LOCALHOST, port())))
        .await
}

/// The frames of a connection whose writer stalls on the first responses and whose other
/// requests each decode to some 1.3 MB but cost the DB thread next to nothing: `stall`
/// Metadata requests of 100,000 distinct missing names (each decodes to some 12 MB and is
/// answered with some 3 MB, more than the socket buffers between broker and client hold),
/// then Metadata requests that name one missing topic 10,000 times (looked up once,
/// answered with one entry), up to the in-flight cap.
fn stalling_frames(prefix: &str, stall: usize) -> Vec<u8> {
    let big = metadata_of(distinct(prefix, 100_000));
    let repeated = metadata_of(std::iter::repeat_n(format!("{prefix}-once"), 10_000));
    let mut out = Vec::new();
    for i in 0..IN_FLIGHT {
        let request = if i < stall { &big } else { &repeated };
        out.extend(request_frame(
            METADATA_VERSION,
            i32::try_from(i).expect("small"),
            request,
        ));
    }
    out
}

/// Ask Metadata of 3,000 distinct missing names (some 200 KB decoded: more than the small
/// requests the reserve of the decoded-request budget is kept for) on a fresh connection,
/// and how long the answer took; an error when the broker closes the connection instead.
async fn fresh_metadata(prefix: &str) -> DynResult<Duration> {
    let started = Instant::now();
    let mut s = TcpStream::connect(("127.0.0.1", port())).await?;
    let req = metadata_of(distinct(prefix, 3000));
    s.write_all(&request_frame(METADATA_VERSION, 7, &req))
        .await?;
    let frame = read_frame_within(&mut s, Duration::from_secs(30))
        .await?
        .ok_or("the broker closed the fresh client's connection instead of answering")?;
    let (corr, resp) = decode_response::<MetadataRequest>(METADATA_VERSION, &frame)?;
    assert_eq!(corr, 7);
    assert_eq!(resp.topics.len(), 3000);
    Ok(started.elapsed())
}

/// Three connections pipeline the in-flight cap of requests that decode to some 1.3 MB each
/// (about 600 MB in all, more than twice the 256 MiB decoded-request budget) and never read
/// a response, so their writers stall on the first responses. Once the DB thread has run their
/// requests the memory of those requests is free again: a fresh client's 200 KB Metadata is
/// answered as usual, as is an rdkafka client, instead of waiting out the 10 s decode wait
/// and being closed unanswered.
pub async fn test_clients_that_stop_reading_do_not_lock_out_others() -> TestResult {
    println!("=== Test: non-reading connections do not hold the decoded-request budget ===\n");
    let ctx = TestContext::new().await?;
    let prefix = format!("nr{}", &uuid::Uuid::new_v4().simple().to_string()[..6]);

    println!("Step 1: {HOGS} non-reading connections pipeline {IN_FLIGHT} requests each");
    let mut hogs = Vec::new();
    for i in 0..HOGS {
        let mut s = non_reading(Ipv4Addr::new(127, 0, 0, 20 + i)).await?;
        let frames = stalling_frames(&format!("{prefix}{i}"), 3);
        // The broker reads at most the in-flight cap, which is all of them. Where decoded
        // requests held the budget until their responses were written, a connection whose
        // request found no room was closed after the 10 s decode wait and this write
        // failed: what counts is whether the fresh client below is served meanwhile.
        // Aborted when `hogs` drops, whichever way the test ends.
        hogs.push(AbortOnDrop(tokio::spawn(async move {
            let _ = s.write_all(&frames).await;
            // Kept open, reading nothing.
            tokio::time::sleep(Duration::from_secs(600)).await;
            drop(s);
        })));
    }
    tokio::time::sleep(Duration::from_secs(5)).await;

    println!("Step 2: a fresh client asks Metadata of 3,000 names");
    for attempt in 0..3 {
        let took = fresh_metadata(&format!("{prefix}f{attempt}")).await?;
        println!("  answered in {took:?}");
        assert!(
            took < Duration::from_secs(8),
            "the fresh client waited {took:?} for room the non-reading connections hold"
        );
    }
    rdkafka_roundtrip(client_config(port()), &ctx.unique_topic("net-nr").await, 10).await?;
    println!("✅ fresh clients served while the non-reading connections are open");
    drop(hogs);
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// The TCP state (`/proc/net/tcp`, hex) of the broker's end of the connection whose client
/// end is `client`, or `None` once the broker has no such socket. A client that reads
/// nothing sees no FIN even after the broker closed the connection (the FIN queues behind
/// the unsent data), and reading would change what it tests; the broker's end shows the
/// close.
pub(super) fn broker_side_state(client: SocketAddr) -> DynResult<Option<u8>> {
    let SocketAddr::V4(client) = client else {
        return Err("an IPv4 client address".into());
    };
    let hex = |a: Ipv4Addr, p: u16| format!("{:08X}:{p:04X}", u32::from_le_bytes(a.octets()));
    let local = hex(Ipv4Addr::LOCALHOST, port());
    let remote = hex(*client.ip(), client.port());
    let table = std::fs::read_to_string("/proc/net/tcp")?;
    for line in table.lines().skip(1) {
        let cols: Vec<&str> = line.split_whitespace().collect();
        if cols.len() > 3 && cols[1] == local && cols[2] == remote {
            return Ok(Some(u8::from_str_radix(cols[3], 16)?));
        }
    }
    Ok(None)
}

/// The bytes waiting in the broker's receive queue (`/proc/net/tcp` `rx_queue`) of the
/// connection whose client end is `client`: what the client sent and the broker has not read.
pub(super) fn broker_side_unread(client: SocketAddr) -> DynResult<Option<u64>> {
    let SocketAddr::V4(client) = client else {
        return Err("an IPv4 client address".into());
    };
    let hex = |a: Ipv4Addr, p: u16| format!("{:08X}:{p:04X}", u32::from_le_bytes(a.octets()));
    let local = hex(Ipv4Addr::LOCALHOST, port());
    let remote = hex(*client.ip(), client.port());
    let table = std::fs::read_to_string("/proc/net/tcp")?;
    for line in table.lines().skip(1) {
        let cols: Vec<&str> = line.split_whitespace().collect();
        if cols.len() > 4 && cols[1] == local && cols[2] == remote {
            let rx = cols[4]
                .split(':')
                .nth(1)
                .ok_or("a tx_queue:rx_queue column")?;
            return Ok(Some(u64::from_str_radix(rx, 16)?));
        }
    }
    Ok(None)
}

/// TCP_ESTABLISHED in `/proc/net/tcp`.
pub(super) const ESTABLISHED: u8 = 1;

/// A connection that sends four requests answered with some 3 MB each and reads nothing:
/// the broker's writer stalls on the first response. With
/// `pg_kafka.connections_max_idle_ms` at `idle` (the `limits` setup), the broker closes the
/// connection once its writer has made no progress for that long, as Kafka closes a muted
/// connection whose response never goes out, and not before. The client, reading at last,
/// gets the bytes that were on their way and then the close, not every response.
pub async fn stalled_writer_closes_after(idle: Duration) -> TestResult {
    println!("=== Test: a stalled writer's connection closes after the idle timeout ===\n");
    let prefix = format!("sw{}", &uuid::Uuid::new_v4().simple().to_string()[..6]);
    let mut s = non_reading(Ipv4Addr::new(127, 0, 0, 30)).await?;
    let client = s.local_addr()?;
    let big = metadata_of(distinct(&prefix, 100_000));
    let sent = 4;
    for corr in 0..sent {
        s.write_all(&request_frame(METADATA_VERSION, corr, &big))
            .await?;
    }
    let started = Instant::now();
    println!("Step 1: {sent} requests sent; reading nothing");

    let mut closed_after = None;
    while started.elapsed() < idle * 4 + Duration::from_secs(10) {
        if broker_side_state(client)? != Some(ESTABLISHED) {
            closed_after = Some(started.elapsed());
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let t = closed_after.ok_or("the broker never closed a connection whose writer stalled")?;
    println!("  the broker closed it after {t:?}");
    assert!(
        t >= idle.saturating_sub(Duration::from_millis(500)),
        "closed after {t:?}, before the idle timeout of {idle:?}"
    );

    let mut answered = 0;
    while let Some(frame) = tokio::time::timeout(Duration::from_secs(10), read_frame(&mut s))
        .await
        .map_err(|_| "the broker's end closed, but the client's end saw no close")??
    {
        assert_eq!(correlation_of(&frame), answered);
        answered += 1;
    }
    println!("  the client read {answered} of {sent} responses, then the close");
    assert!(
        answered < sent,
        "every response was written: the writer never stalled"
    );

    let mut ok = TcpStream::connect(("127.0.0.1", port())).await?;
    assert!(super::answers(&mut ok, 1).await, "other clients unaffected");
    println!("✅ Test PASSED\n");
    Ok(())
}
