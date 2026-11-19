//! Pipelining E2E test
//!
//! Verifies that the broker correctly handles pipelined requests
//! (multiple requests sent without waiting for responses).

use crate::common::{get_bootstrap_servers, TestResult};
use crate::wire::{raw_frame, read_raw_response};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

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
        let request = raw_frame(18, 0, corr_id, &[]);
        stream.write_all(&request).await?;
        println!("  Sent request with correlation_id={}", corr_id);
    }

    // Flush to ensure all requests are sent
    stream.flush().await?;
    println!("All requests sent\n");

    // Now read all responses and verify ordering
    println!("Reading responses...");
    let mut received_ids = Vec::new();

    for (i, &expected_id) in correlation_ids.iter().enumerate().take(num_requests) {
        let received_id = read_raw_response(&mut stream).await?.0;
        received_ids.push(received_id);
        println!(
            "  Response {}: correlation_id={} (expected={})",
            i + 1,
            received_id,
            expected_id
        );
    }

    // Verify responses arrived in correct order
    println!("\nVerifying response order...");
    for (i, (&expected, &received)) in correlation_ids.iter().zip(received_ids.iter()).enumerate() {
        assert_eq!(
            expected,
            received,
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
