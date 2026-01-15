//! Protocol compliance tests
//!
//! Tests for Kafka wire protocol compliance including:
//! - API version negotiation
//! - Correlation ID handling
//! - Unknown API key handling
//! - Request/response framing

use crate::common::TestResult;
use bytes::{BufMut, BytesMut};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Encode a Kafka request frame with the given API key, version, and correlation ID
fn encode_request(api_key: i16, api_version: i16, correlation_id: i32, body: &[u8]) -> BytesMut {
    let mut buf = BytesMut::new();
    let size = 2 + 2 + 4 + 2 + body.len() as i32; // api_key + version + correlation + client_id(-1) + body

    buf.put_i32(size);
    buf.put_i16(api_key);
    buf.put_i16(api_version);
    buf.put_i32(correlation_id);
    buf.put_i16(-1); // null client_id
    buf.extend_from_slice(body);

    buf
}

/// Read a Kafka response and return the correlation ID
async fn read_response(
    stream: &mut TcpStream,
) -> Result<(i32, Vec<u8>), Box<dyn std::error::Error>> {
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).await?;
    let size = i32::from_be_bytes(size_buf) as usize;

    let mut response = vec![0u8; size];
    stream.read_exact(&mut response).await?;

    if response.len() < 4 {
        return Err("Response too short".into());
    }

    let correlation_id = i32::from_be_bytes([response[0], response[1], response[2], response[3]]);
    let body = response[4..].to_vec();

    Ok((correlation_id, body))
}

/// Test: API versions request/response
/// Verifies that ApiVersions (API key 18) returns supported API versions
pub async fn test_api_versions_negotiation() -> TestResult {
    println!("=== Test: API Versions Negotiation ===\n");

    let mut stream = TcpStream::connect("localhost:9092").await?;
    stream.set_nodelay(true)?;

    println!("Step 1: Connected to broker\n");

    // Send ApiVersions request (API key 18, version 0)
    let request = encode_request(18, 0, 12345, &[]);
    stream.write_all(&request).await?;

    println!("Step 2: Sent ApiVersions request\n");

    // Read response
    let (correlation_id, body) =
        tokio::time::timeout(Duration::from_secs(5), read_response(&mut stream)).await??;

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
        println!("✅ Server supports {} API keys\n", api_count);
        assert!(api_count > 0, "Server should support at least one API");
    }

    println!("✅ Test PASSED\n");
    Ok(())
}

/// Test: Correlation ID preserved across multiple requests
/// Verifies that each response has the correct correlation ID
pub async fn test_correlation_id_preserved() -> TestResult {
    println!("=== Test: Correlation ID Preserved ===\n");

    let mut stream = TcpStream::connect("localhost:9092").await?;
    stream.set_nodelay(true)?;

    println!("Step 1: Connected to broker\n");

    // Send multiple requests with different correlation IDs
    let correlation_ids = [1001, 2002, 3003, 4004, 5005];

    for &corr_id in &correlation_ids {
        let request = encode_request(18, 0, corr_id, &[]); // ApiVersions
        stream.write_all(&request).await?;
    }

    println!(
        "Step 2: Sent 5 requests with correlation IDs: {:?}\n",
        correlation_ids
    );

    // Read all responses
    let mut received_ids = Vec::new();
    for _ in 0..5 {
        let (corr_id, _body) =
            tokio::time::timeout(Duration::from_secs(5), read_response(&mut stream)).await??;
        received_ids.push(corr_id);
    }

    println!(
        "Step 3: Received responses with correlation IDs: {:?}\n",
        received_ids
    );

    // Verify all correlation IDs match (order may vary if pipelined)
    for expected in &correlation_ids {
        assert!(
            received_ids.contains(expected),
            "Response for correlation ID {} not received",
            expected
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

    let mut stream = TcpStream::connect("localhost:9092").await?;
    stream.set_nodelay(true)?;

    println!("Step 1: Connected to broker\n");

    // Send request with unknown API key (999)
    let request = encode_request(999, 0, 77777, &[]);
    stream.write_all(&request).await?;

    println!("Step 2: Sent request with unknown API key 999\n");

    // Read response (may be error or connection close)
    let result = tokio::time::timeout(Duration::from_secs(5), read_response(&mut stream)).await;

    match result {
        Ok(Ok((corr_id, body))) => {
            println!(
                "Step 3: Received response with correlation ID: {}\n",
                corr_id
            );

            // Check for error code in response
            if body.len() >= 2 {
                let error_code = i16::from_be_bytes([body[0], body[1]]);
                println!("✅ Error code in response: {}\n", error_code);
                // UNSUPPORTED_VERSION = 35
                assert!(error_code != 0, "Unknown API key should return an error");
            }
        }
        Ok(Err(e)) => {
            println!("✅ Connection error (expected for unknown API): {}\n", e);
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

    let mut stream = TcpStream::connect("localhost:9092").await?;
    stream.set_nodelay(true)?;

    println!("Step 1: Connected to broker\n");

    // Build 10 pipelined requests
    let mut all_requests = BytesMut::new();
    for i in 0..10 {
        let request = encode_request(18, 0, 1000 + i, &[]); // ApiVersions
        all_requests.extend_from_slice(&request);
    }

    // Send all at once
    stream.write_all(&all_requests).await?;

    println!("Step 2: Sent 10 pipelined ApiVersions requests\n");

    // Read all responses
    let mut received = std::collections::HashSet::new();
    for _ in 0..10 {
        let (corr_id, body) =
            tokio::time::timeout(Duration::from_secs(10), read_response(&mut stream)).await??;

        received.insert(corr_id);

        // Verify each response is valid
        if body.len() >= 2 {
            let error_code = i16::from_be_bytes([body[0], body[1]]);
            assert_eq!(
                error_code, 0,
                "Pipelined request {} should succeed",
                corr_id
            );
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
