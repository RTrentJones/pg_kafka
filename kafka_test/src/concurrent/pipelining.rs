//! Pipelining E2E test
//!
//! Verifies that the broker correctly handles pipelined requests
//! (multiple requests sent without waiting for responses).

use crate::common::{get_bootstrap_servers, TestResult};
use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Build an ApiVersionsRequest with the given correlation ID
///
/// Wire format:
/// - 4 bytes: Size (big endian, excludes this field)
/// - 2 bytes: API Key (18 for ApiVersions)
/// - 2 bytes: API Version (0)
/// - 4 bytes: Correlation ID
/// - 2 bytes: Client ID length (-1 for null)
fn build_api_versions_request(correlation_id: i32) -> BytesMut {
    let mut buf = BytesMut::new();

    // Request body (without size prefix)
    let mut body = BytesMut::new();
    body.put_i16(18); // API Key: ApiVersions
    body.put_i16(0); // API Version: 0
    body.put_i32(correlation_id);
    body.put_i16(-1); // Null client ID

    // Size prefix
    buf.put_i32(body.len() as i32);
    buf.extend_from_slice(&body);

    buf
}

/// Read a single response and extract its correlation ID
///
/// Response format:
/// - 4 bytes: Size (big endian)
/// - 4 bytes: Correlation ID
/// - ... rest of response body
async fn read_response_correlation_id(
    stream: &mut TcpStream,
) -> Result<i32, Box<dyn std::error::Error>> {
    // Read size prefix
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).await?;
    let size = i32::from_be_bytes(size_buf);

    // Read correlation ID (first 4 bytes of response body)
    let mut corr_buf = [0u8; 4];
    stream.read_exact(&mut corr_buf).await?;
    let correlation_id = i32::from_be_bytes(corr_buf);

    // Read and discard rest of response
    let remaining = size as usize - 4;
    let mut discard = vec![0u8; remaining];
    stream.read_exact(&mut discard).await?;

    Ok(correlation_id)
}

/// Test request pipelining
///
/// This test verifies that the broker correctly handles pipelined requests:
/// 1. Opens a raw TCP connection
/// 2. Sends 5 ApiVersionsRequests in rapid succession WITHOUT waiting for responses
/// 3. Reads all 5 responses
/// 4. Verifies responses arrive in correct order (matching correlation IDs)
pub async fn test_request_pipelining() -> TestResult {
    println!("=== Test: Request Pipelining ===\n");

    let bootstrap = get_bootstrap_servers();
    println!("Connecting to {}...", bootstrap);

    let mut stream = TcpStream::connect(&bootstrap).await?;
    println!("Connected\n");

    // Define correlation IDs for our pipelined requests
    let correlation_ids: Vec<i32> = vec![1001, 1002, 1003, 1004, 1005];
    let num_requests = correlation_ids.len();

    // Send all requests WITHOUT waiting for responses
    println!(
        "Sending {} requests without waiting for responses...",
        num_requests
    );

    for &corr_id in &correlation_ids {
        let request = build_api_versions_request(corr_id);
        stream.write_all(&request).await?;
        println!("  Sent request with correlation_id={}", corr_id);
    }

    // Flush to ensure all requests are sent
    stream.flush().await?;
    println!("All requests sent\n");

    // Now read all responses and verify ordering
    println!("Reading responses...");
    let mut received_ids = Vec::new();

    for i in 0..num_requests {
        let received_id = read_response_correlation_id(&mut stream).await?;
        received_ids.push(received_id);
        println!(
            "  Response {}: correlation_id={} (expected={})",
            i + 1,
            received_id,
            correlation_ids[i]
        );
    }

    // Verify responses arrived in correct order
    println!("\nVerifying response order...");
    for (i, (&expected, &received)) in correlation_ids.iter().zip(received_ids.iter()).enumerate() {
        assert_eq!(
            expected, received,
            "Response {} has wrong correlation ID: expected {}, got {}",
            i + 1,
            expected,
            received
        );
    }

    println!("All {} responses received in correct order", num_requests);
    println!("Pipelining test PASSED\n");

    Ok(())
}
