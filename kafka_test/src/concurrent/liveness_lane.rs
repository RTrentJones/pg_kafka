//! Heartbeat liveness lane (DR-12, DEEP-REVIEW-2026-07).
//!
//! Heartbeats travel on a dedicated liveness lane the worker drains before each
//! main-lane request. On one FIFO channel into the single DB thread a Heartbeat
//! queued behind a backlog of heavy Produce work would wait for the whole queue,
//! and under sustained DB pressure that cascades into spurious consumer-group
//! rebalances.
//!
//! The test floods the main lane with large pipelined Produce requests from one
//! connection, then sends a Heartbeat on a second connection mid-flood and asserts
//! it answers long before the flood drains.

use crate::common::to_i32;
use crate::common::TestResult;
use crate::wire::requests::sequenced_produce_request;
use crate::wire::{read_raw_response, request_frame};
use kafka_protocol::messages::heartbeat_request::HeartbeatRequest;
use kafka_protocol::messages::GroupId;
use kafka_protocol::protocol::StrBytes;
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use uuid::Uuid;

const FLOOD_REQUESTS: usize = 400;
const FLOOD_PAYLOAD_BYTES: usize = 512 * 1024;

pub async fn test_heartbeat_bypasses_produce_backlog() -> TestResult {
    println!("=== Test: Heartbeat Bypasses Produce Backlog (DR-12) ===\n");

    let topic = format!("liveness-flood-{}", Uuid::new_v4());
    let group = format!("liveness-group-{}", Uuid::new_v4());
    let bootstrap = crate::common::get_bootstrap_servers();

    // 1. Flood connection: pipeline many large acks=1 produce requests without
    //    reading responses, so they queue on the worker's main lane.
    println!(
        "Step 1: Flooding main lane with {} x {}KB produce requests...",
        FLOOD_REQUESTS,
        FLOOD_PAYLOAD_BYTES / 1024
    );
    let payload = "x".repeat(FLOOD_PAYLOAD_BYTES);
    let flood_topic = topic.clone();
    let flood_addr = bootstrap.clone();
    let flood_start = Instant::now();
    let flood_task = tokio::spawn(async move {
        let mut stream = TcpStream::connect(&flood_addr)
            .await
            .map_err(|e| e.to_string())?;
        // Interleave writes and reads loosely: write everything first (the
        // writer blocks on TCP backpressure once buffers fill, which is fine —
        // the backlog is what we want), then drain all responses.
        let writer_payload = payload;
        for i in 0..FLOOD_REQUESTS {
            let req = sequenced_produce_request(
                &flood_topic,
                0,
                -1, // non-idempotent
                -1,
                -1,
                &[&writer_payload],
            );
            stream
                .write_all(&request_frame(7, 1000 + to_i32(i), &req))
                .await
                .map_err(|e| e.to_string())?;
        }
        for _ in 0..FLOOD_REQUESTS {
            read_raw_response(&mut stream)
                .await
                .map_err(|e| e.to_string())?;
        }
        Ok::<Duration, String>(flood_start.elapsed())
    });

    // 2. Give the flood a moment to fill the queue, then probe with heartbeats
    //    on a fresh connection. The group doesn't exist — an UNKNOWN_MEMBER_ID
    //    error response is fine; only the LATENCY matters, and an invalid
    //    heartbeat takes the same lane as a valid one. A heartbeat can
    //    legitimately wait behind the ONE request currently executing (which can
    //    be slow if it hits a checkpoint/WAL stall), so probe several times and
    //    judge the fastest: on a shared lane every probe waits for the whole
    //    remaining queue, so even the minimum stays large.
    tokio::time::sleep(Duration::from_millis(150)).await;
    println!("Step 2: Sending heartbeat probes mid-flood...");
    let mut hb_stream = TcpStream::connect(&bootstrap).await?;
    let mut heartbeat = HeartbeatRequest::default();
    heartbeat.group_id = GroupId(StrBytes::from_string(group.clone()));
    heartbeat.generation_id = 1;
    heartbeat.member_id = StrBytes::from_static_str("liveness-probe-member");
    let mut hb_rtt = Duration::MAX;
    for probe in 0..3 {
        let hb_start = Instant::now();
        hb_stream
            .write_all(&request_frame(0, 42 + probe, &heartbeat))
            .await
            .map_err(|e| format!("heartbeat send failed: {e}"))?;
        tokio::time::timeout(Duration::from_secs(30), read_raw_response(&mut hb_stream))
            .await
            .map_err(|_| "heartbeat response timed out entirely")?
            .map_err(|e| format!("heartbeat read failed: {e}"))?;
        let rtt = hb_start.elapsed();
        println!("  Heartbeat probe {probe} RTT: {rtt:?}");
        hb_rtt = hb_rtt.min(rtt);
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    println!("  Best heartbeat RTT: {hb_rtt:?}");

    // 3. Wait for the flood to drain and compare.
    let flood_total = flood_task
        .await?
        .map_err(|e| format!("flood task failed: {e}"))?;
    println!("  Flood drained in: {flood_total:?}\n");

    // 4. Assertions. If the machine drained the flood too fast to build a real
    //    backlog, the probe proves nothing — pass with a warning rather than
    //    flake (the assertion is meaningful on any CI-class machine).
    if flood_total < Duration::from_millis(1500) {
        println!(
            "⚠️  Flood drained in {flood_total:?} (<1.5s) — backlog too small to exercise the lane; inconclusive pass"
        );
        return Ok(());
    }
    assert!(
        hb_rtt < Duration::from_millis(1000),
        "Heartbeat took {hb_rtt:?} while the produce backlog drained in {flood_total:?} — liveness traffic is queueing behind the main lane (DR-12 regression)"
    );
    assert!(
        hb_rtt < flood_total / 2,
        "Heartbeat RTT {hb_rtt:?} is not meaningfully faster than the {flood_total:?} backlog — liveness lane not effective"
    );
    println!("  ✅ Heartbeat answered in {hb_rtt:?} despite {flood_total:?} backlog\n");

    println!("✅ Test PASSED\n");
    Ok(())
}

/// Heartbeats the idle-broker test sends.
const IDLE_PROBES: usize = 20;

/// On an idle broker the worker waits for main-lane requests up to its receive timeout
/// (100 ms); a Heartbeat arriving meanwhile must wake it, not wait for the timeout and
/// the next loop's liveness check: a 100 ms delay on every heartbeat of an idle group
/// eats into session timeouts for nothing.
///
/// Asserts: 20 Heartbeats (to a group that does not exist: the error answer takes the
/// same lane and path), sent at spacings that do not line up with the 100 ms wait, have
/// a median round trip below 20 ms (it was about 50 ms: half the receive timeout).
pub async fn test_heartbeat_answered_at_once_on_an_idle_broker() -> TestResult {
    println!("=== Test: a Heartbeat on an idle broker is answered at once ===\n");
    let group = format!("liveness-idle-{}", Uuid::new_v4());
    let mut stream = TcpStream::connect(crate::common::get_bootstrap_servers()).await?;
    stream.set_nodelay(true)?;
    let mut heartbeat = HeartbeatRequest::default();
    heartbeat.group_id = GroupId(StrBytes::from_string(group));
    heartbeat.generation_id = 1;
    heartbeat.member_id = StrBytes::from_static_str("liveness-idle-member");
    let mut rtts = Vec::with_capacity(IDLE_PROBES);
    for probe in 0..IDLE_PROBES {
        // 37 ms plus a varying part: the probes land anywhere in the worker's wait.
        tokio::time::sleep(Duration::from_millis(37 + 13 * (probe as u64 % 7))).await;
        let started = Instant::now();
        stream
            .write_all(&request_frame(0, 100 + to_i32(probe), &heartbeat))
            .await?;
        tokio::time::timeout(Duration::from_secs(10), read_raw_response(&mut stream))
            .await
            .map_err(|_| "heartbeat response timed out")?
            .map_err(|e| format!("heartbeat read failed: {e}"))?;
        rtts.push(started.elapsed());
    }
    rtts.sort();
    let median = rtts[IDLE_PROBES / 2];
    println!(
        "  Heartbeat RTTs: min {:?}, median {median:?}, max {:?}",
        rtts[0],
        rtts[IDLE_PROBES - 1]
    );
    assert!(
        median < Duration::from_millis(20),
        "idle-broker Heartbeats waited {median:?} (median): the worker does not wake for the liveness lane"
    );
    println!("✅ Test PASSED\n");
    Ok(())
}
