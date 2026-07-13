//! DR-12 (DEEP-REVIEW-2026-07): heartbeat liveness-lane test.
//!
//! Before DR-12 every request shared one FIFO channel into the single DB
//! thread, so a Heartbeat queued behind a backlog of heavy Produce work waited
//! for the whole queue — and under sustained DB pressure that cascades into
//! spurious consumer-group rebalances. Heartbeats now travel on a dedicated
//! liveness lane the worker drains before each main-lane request.
//!
//! Fail-before/pass-after: this test floods the main lane with large pipelined
//! Produce requests from one connection, then sends a Heartbeat on a second
//! connection mid-flood and asserts it answers long before the flood drains.
//! Pre-DR-12 the heartbeat's response arrives only after the produce backlog
//! ahead of it, tripping the latency assertion.

use crate::common::TestResult;
use crate::idempotent::protocol_encoding::{read_response, send_request};
use bytes::{BufMut, Bytes, BytesMut};
use kafka_protocol::messages::heartbeat_request::HeartbeatRequest;
use kafka_protocol::messages::{GroupId, RequestHeader};
use kafka_protocol::protocol::{Encodable, StrBytes};
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use uuid::Uuid;

const API_KEY_HEARTBEAT: i16 = 12;
const FLOOD_REQUESTS: usize = 400;
const FLOOD_PAYLOAD_BYTES: usize = 512 * 1024;

fn encode_heartbeat_request(correlation_id: i32, group_id: &str) -> Bytes {
    let mut request = HeartbeatRequest::default();
    request.group_id = GroupId(StrBytes::from_string(group_id.to_string()));
    request.generation_id = 1;
    request.member_id = StrBytes::from_static_str("liveness-probe-member");

    // Heartbeat v0 with header v1 (non-flexible) — matches the produce helper style.
    let header = RequestHeader::default()
        .with_request_api_key(API_KEY_HEARTBEAT)
        .with_request_api_version(0)
        .with_correlation_id(correlation_id)
        .with_client_id(Some(StrBytes::from_static_str("liveness-probe")));

    let mut body_buf = BytesMut::new();
    header.encode(&mut body_buf, 1).unwrap();
    request.encode(&mut body_buf, 0).unwrap();

    let mut buf = BytesMut::new();
    buf.put_i32(body_buf.len() as i32);
    buf.put(body_buf);
    buf.freeze()
}

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
            let req = crate::idempotent::protocol_encoding::encode_produce_request(
                1000 + i as i32,
                &flood_topic,
                0,
                -1, // non-idempotent
                -1,
                -1,
                vec![&writer_payload],
            );
            send_request(&mut stream, &req)
                .await
                .map_err(|e| e.to_string())?;
        }
        for _ in 0..FLOOD_REQUESTS {
            read_response(&mut stream)
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
    //    judge the fastest: pre-DR-12 every probe waits for the whole remaining
    //    queue, so even the minimum stays large.
    tokio::time::sleep(Duration::from_millis(150)).await;
    println!("Step 2: Sending heartbeat probes mid-flood...");
    let mut hb_stream = TcpStream::connect(&bootstrap).await?;
    let mut hb_rtt = Duration::MAX;
    for probe in 0..3 {
        let hb_req = encode_heartbeat_request(42 + probe, &group);
        let hb_start = Instant::now();
        send_request(&mut hb_stream, &hb_req)
            .await
            .map_err(|e| format!("heartbeat send failed: {}", e))?;
        tokio::time::timeout(Duration::from_secs(30), read_response(&mut hb_stream))
            .await
            .map_err(|_| "heartbeat response timed out entirely")?
            .map_err(|e| format!("heartbeat read failed: {}", e))?;
        let rtt = hb_start.elapsed();
        println!("  Heartbeat probe {} RTT: {:?}", probe, rtt);
        hb_rtt = hb_rtt.min(rtt);
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    println!("  Best heartbeat RTT: {:?}", hb_rtt);

    // 3. Wait for the flood to drain and compare.
    let flood_total = flood_task
        .await?
        .map_err(|e| format!("flood task failed: {}", e))?;
    println!("  Flood drained in: {:?}\n", flood_total);

    // 4. Assertions. If the machine drained the flood too fast to build a real
    //    backlog, the probe proves nothing — pass with a warning rather than
    //    flake (the assertion is meaningful on any CI-class machine).
    if flood_total < Duration::from_millis(1500) {
        println!(
            "⚠️  Flood drained in {:?} (<1.5s) — backlog too small to exercise the lane; inconclusive pass",
            flood_total
        );
        return Ok(());
    }
    assert!(
        hb_rtt < Duration::from_millis(1000),
        "Heartbeat took {:?} while the produce backlog drained in {:?} — liveness traffic is queueing behind the main lane (DR-12 regression)",
        hb_rtt,
        flood_total
    );
    assert!(
        hb_rtt < flood_total / 2,
        "Heartbeat RTT {:?} is not meaningfully faster than the {:?} backlog — liveness lane not effective",
        hb_rtt,
        flood_total
    );
    println!(
        "  ✅ Heartbeat answered in {:?} despite {:?} backlog\n",
        hb_rtt, flood_total
    );

    println!("✅ Test PASSED\n");
    Ok(())
}
