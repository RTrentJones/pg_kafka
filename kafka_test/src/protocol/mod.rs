//! Protocol compliance tests
//!
//! Tests for Kafka wire protocol compliance including:
//! - API version negotiation
//! - Correlation ID handling
//! - Unknown API key handling
//! - Request/response framing

use crate::common::TestResult;
use crate::wire::{raw_frame, read_raw_response};
use bytes::{BufMut, BytesMut};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

/// Test: API versions request/response
/// Verifies that ApiVersions (API key 18) returns supported API versions
pub async fn test_api_versions_negotiation() -> TestResult {
    println!("=== Test: API Versions Negotiation ===\n");

    let mut stream = TcpStream::connect(crate::common::broker_addr()).await?;
    stream.set_nodelay(true)?;

    println!("Step 1: Connected to broker\n");

    // Send ApiVersions request (API key 18, version 0)
    let request = raw_frame(18, 0, 12345, &[]);
    stream.write_all(&request).await?;

    println!("Step 2: Sent ApiVersions request\n");

    // Read response
    let (correlation_id, body) =
        tokio::time::timeout(Duration::from_secs(5), read_raw_response(&mut stream)).await??;

    println!("Step 3: Received response\n");

    // Verify correlation ID
    assert_eq!(correlation_id, 12345, "Correlation ID should match request");

    // Parse error code (first 2 bytes of body)
    if body.len() >= 2 {
        let error_code = i16::from_be_bytes([body[0], body[1]]);
        assert_eq!(
            error_code, 0,
            "ApiVersions should succeed with error_code=0"
        );
        println!("✅ ApiVersions returned error_code=0 (success)\n");
    }

    // Parse API versions array
    if body.len() >= 6 {
        let api_count = i32::from_be_bytes([body[2], body[3], body[4], body[5]]);
        println!("✅ Server supports {api_count} API keys\n");
        assert!(api_count > 0, "Server should support at least one API");
    }

    println!("✅ Test PASSED\n");
    Ok(())
}

/// Test: Correlation ID preserved across multiple requests
/// Verifies that each response has the correct correlation ID
pub async fn test_correlation_id_preserved() -> TestResult {
    println!("=== Test: Correlation ID Preserved ===\n");

    let mut stream = TcpStream::connect(crate::common::broker_addr()).await?;
    stream.set_nodelay(true)?;

    println!("Step 1: Connected to broker\n");

    // Send multiple requests with different correlation IDs
    let correlation_ids = [1001, 2002, 3003, 4004, 5005];

    for &corr_id in &correlation_ids {
        let request = raw_frame(18, 0, corr_id, &[]); // ApiVersions
        stream.write_all(&request).await?;
    }

    println!("Step 2: Sent 5 requests with correlation IDs: {correlation_ids:?}\n");

    // Read all responses
    let mut received_ids = Vec::new();
    for _ in 0..5 {
        let (corr_id, _body) =
            tokio::time::timeout(Duration::from_secs(5), read_raw_response(&mut stream)).await??;
        received_ids.push(corr_id);
    }

    println!("Step 3: Received responses with correlation IDs: {received_ids:?}\n");

    // Verify all correlation IDs match (order may vary if pipelined)
    for expected in &correlation_ids {
        assert!(
            received_ids.contains(expected),
            "Response for correlation ID {expected} not received"
        );
    }

    println!("✅ All correlation IDs preserved correctly\n");
    println!("✅ Test PASSED\n");
    Ok(())
}

/// Test: Unknown API key returns appropriate error
/// Verifies that sending an unknown API key returns an error response
pub async fn test_unknown_api_key_handling() -> TestResult {
    println!("=== Test: Unknown API Key Handling ===\n");

    let mut stream = TcpStream::connect(crate::common::broker_addr()).await?;
    stream.set_nodelay(true)?;

    println!("Step 1: Connected to broker\n");

    // Send request with unknown API key (999)
    let request = raw_frame(999, 0, 77777, &[]);
    stream.write_all(&request).await?;

    println!("Step 2: Sent request with unknown API key 999\n");

    // Read response (may be error or connection close)
    let result = tokio::time::timeout(Duration::from_secs(5), read_raw_response(&mut stream)).await;

    match result {
        Ok(Ok((corr_id, body))) => {
            println!("Step 3: Received response with correlation ID: {corr_id}\n");

            // Check for error code in response
            if body.len() >= 2 {
                let error_code = i16::from_be_bytes([body[0], body[1]]);
                println!("✅ Error code in response: {error_code}\n");
                // UNSUPPORTED_VERSION = 35
                assert!(error_code != 0, "Unknown API key should return an error");
            }
        }
        Ok(Err(e)) => {
            println!("✅ Connection error (expected for unknown API): {e}\n");
        }
        Err(_) => {
            println!("✅ Request timed out (acceptable behavior)\n");
        }
    }

    println!("✅ Test PASSED\n");
    Ok(())
}

/// Test: Request pipelining with correct response ordering
/// Verifies that multiple pipelined requests get correct responses
pub async fn test_protocol_request_pipelining() -> TestResult {
    println!("=== Test: Request Pipelining ===\n");

    let mut stream = TcpStream::connect(crate::common::broker_addr()).await?;
    stream.set_nodelay(true)?;

    println!("Step 1: Connected to broker\n");

    // Build 10 pipelined requests
    let mut all_requests = BytesMut::new();
    for i in 0..10 {
        let request = raw_frame(18, 0, 1000 + i, &[]); // ApiVersions
        all_requests.extend_from_slice(&request);
    }

    // Send all at once
    stream.write_all(&all_requests).await?;

    println!("Step 2: Sent 10 pipelined ApiVersions requests\n");

    // Read all responses
    let mut received = std::collections::HashSet::new();
    for _ in 0..10 {
        let (corr_id, body) =
            tokio::time::timeout(Duration::from_secs(10), read_raw_response(&mut stream)).await??;

        received.insert(corr_id);

        // Verify each response is valid
        if body.len() >= 2 {
            let error_code = i16::from_be_bytes([body[0], body[1]]);
            assert_eq!(error_code, 0, "Pipelined request {corr_id} should succeed");
        }
    }

    println!("Step 3: Received {} unique responses\n", received.len());

    // Verify all 10 correlation IDs received
    for i in 0..10 {
        assert!(
            received.contains(&(1000 + i)),
            "Missing response for correlation ID {}",
            1000 + i
        );
    }

    println!("✅ All 10 pipelined requests got correct responses\n");
    println!("✅ Test PASSED\n");
    Ok(())
}

/// Encode a FetchRequest v4 body (non-flexible encoding)
///
/// Used to issue a long-poll fetch over raw TCP so the test controls exactly
/// what is pipelined behind it on the same connection.
fn encode_fetch_v4_body(
    topic: &str,
    partition: i32,
    fetch_offset: i64,
    max_wait_ms: i32,
    min_bytes: i32,
) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(-1); // replica_id
    body.put_i32(max_wait_ms);
    body.put_i32(min_bytes);
    body.put_i32(1024 * 1024); // max_bytes (v3+)
    body.put_i8(0); // isolation_level: read_uncommitted (v4+)
    body.put_i32(1); // topics array count
    body.put_i16(i16::try_from(topic.len()).expect("a test topic name fits a Kafka string"));
    body.extend_from_slice(topic.as_bytes());
    body.put_i32(1); // partitions array count
    body.put_i32(partition);
    body.put_i64(fetch_offset);
    body.put_i32(1024 * 1024); // partition_max_bytes
    body
}

/// Test: Responses are written in strict request order, even across long polls
///
/// Kafka requires per-connection FIFO responses; the official Java client
/// kills the connection on a correlation-id mismatch. A long-poll fetch is
/// handled in a detached task by the broker, so this test pins the regression
/// where a request pipelined behind it could have its response written first:
/// 1. Fetch at the high watermark (no data) with max_wait_ms=1500 -> long poll
/// 2. Pipeline an ApiVersions request behind it on the same connection
/// 3. The fetch response must arrive FIRST (after ~max_wait_ms), then ApiVersions
pub async fn test_response_ordering_with_long_poll() -> TestResult {
    const FETCH_CORR: i32 = 111;
    const API_VERSIONS_CORR: i32 = 222;
    const MAX_WAIT_MS: i32 = 1500;

    println!("=== Test: Response Ordering Across Long Poll ===\n");

    // Create the topic with one message so the fetch targets a real partition
    let ctx = crate::setup::TestContext::new().await?;
    let topic = ctx.unique_topic("resp-order").await;
    let producer = crate::common::create_producer()?;
    producer
        .send(
            rdkafka::producer::FutureRecord::to(&topic)
                .key("k")
                .payload("v"),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(err, _)| err)?;
    println!("Step 1: Topic created with one message\n");

    let mut stream = TcpStream::connect(crate::common::broker_addr()).await?;
    stream.set_nodelay(true)?;

    // Fetch from offset 1 (the high watermark): no data available, so the
    // broker long-polls for max_wait_ms

    let fetch_body = encode_fetch_v4_body(&topic, 0, 1, MAX_WAIT_MS, 1);
    let fetch_request = raw_frame(1, 4, FETCH_CORR, &fetch_body);
    let api_versions_request = raw_frame(18, 0, API_VERSIONS_CORR, &[]);

    let start = std::time::Instant::now();
    stream.write_all(&fetch_request).await?;
    stream.write_all(&api_versions_request).await?;
    println!("Step 2: Sent long-poll Fetch + pipelined ApiVersions\n");

    // First response on the wire must be the fetch, even though ApiVersions
    // completed long before the long poll expired
    let (first_corr, _) =
        tokio::time::timeout(Duration::from_secs(10), read_raw_response(&mut stream)).await??;
    let first_elapsed = start.elapsed();
    let (second_corr, _) =
        tokio::time::timeout(Duration::from_secs(10), read_raw_response(&mut stream)).await??;

    println!(
        "Step 3: Responses: first corr={first_corr} after {first_elapsed:?}, second corr={second_corr}\n"
    );

    assert_eq!(
        first_corr, FETCH_CORR,
        "Fetch response must be written first (request order), got corr {first_corr}"
    );
    assert_eq!(
        second_corr, API_VERSIONS_CORR,
        "ApiVersions response must follow the fetch"
    );
    // The fetch response should have actually long-polled (close to
    // max_wait_ms), proving the ApiVersions response was held behind it
    assert!(
        first_elapsed >= Duration::from_millis(1000),
        "Fetch should have long-polled ~{MAX_WAIT_MS}ms, returned after {first_elapsed:?}"
    );

    ctx.cleanup().await?;
    println!("✅ Responses arrived in strict request order\n");
    println!("✅ Test PASSED\n");
    Ok(())
}

/// The `protocol` tests, in run order.
pub fn tests() -> Vec<crate::registry::TestDef> {
    crate::test_defs!["protocol";
        test_api_versions_negotiation,
        test_correlation_id_preserved,
        test_unknown_api_key_handling,
        test_protocol_request_pipelining,
        test_response_ordering_with_long_poll: sequential, // timing-sensitive: asserts long-poll wait duration
    ]
}
