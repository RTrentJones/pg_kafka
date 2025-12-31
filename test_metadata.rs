// Test Kafka Metadata request/response
// This sends a Metadata request to pg_kafka and parses the response

use std::io::{Read, Write};
use std::net::TcpStream;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Testing Kafka Metadata Request ===\n");

    println!("Connecting to localhost:9092...");
    let mut stream = TcpStream::connect("127.0.0.1:9092")?;
    println!("Connected!\n");

    // Build a Metadata request (API key 3)
    // Format: [Size: 4 bytes][API Key: 2 bytes][API Version: 2 bytes][Correlation ID: 4 bytes][Client ID: string][Topics: nullable array]

    let mut request = Vec::new();

    // API Key: 3 (Metadata)
    request.extend_from_slice(&3i16.to_be_bytes());

    // API Version: 9 (we claim to support 0-9)
    request.extend_from_slice(&9i16.to_be_bytes());

    // Correlation ID: 99 (arbitrary, should be echoed back)
    request.extend_from_slice(&99i32.to_be_bytes());

    // Client ID: "test-client" (nullable string)
    let client_id = "test-client";
    request.extend_from_slice(&(client_id.len() as i16).to_be_bytes());
    request.extend_from_slice(client_id.as_bytes());

    // Topics: null (means "all topics")
    // In Kafka protocol v9+, nullable array is encoded as:
    // - length = -1 (i32) for null
    // - length = N followed by N elements for non-null array
    // For simplicity, we'll request all topics by sending null
    request.extend_from_slice(&(-1i32).to_be_bytes());

    // Prepend the size
    let size = request.len() as i32;
    let mut full_request = Vec::new();
    full_request.extend_from_slice(&size.to_be_bytes());
    full_request.extend_from_slice(&request);

    println!("Sending Metadata request ({} bytes)...", full_request.len());
    println!("Request bytes: {:?}\n", full_request);

    stream.write_all(&full_request)?;
    stream.flush()?;

    println!("Request sent! Waiting for response...\n");

    // Read response size (4 bytes)
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf)?;
    let response_size = i32::from_be_bytes(size_buf);

    println!("Response size: {} bytes", response_size);

    // Read response payload
    let mut response = vec![0u8; response_size as usize];
    stream.read_exact(&mut response)?;

    println!("Response received ({} bytes)!\n", response.len());

    // Parse response
    println!("=== Parsing Metadata Response ===");
    let mut offset = 0;

    // Correlation ID (should be 99)
    if response.len() >= 4 {
        let correlation_id = i32::from_be_bytes([
            response[offset], response[offset+1], response[offset+2], response[offset+3]
        ]);
        println!("Correlation ID: {} (expected: 99)", correlation_id);
        offset += 4;

        if correlation_id == 99 {
            println!("✅ Correlation ID matches!");
        } else {
            println!("❌ Correlation ID mismatch!");
        }
    }

    println!("\nRaw response payload:");
    for (i, chunk) in response.chunks(16).enumerate() {
        print!("{:04x}: ", i * 16);
        for byte in chunk {
            print!("{:02x} ", byte);
        }
        println!();
    }

    println!("\n=== Basic Response Structure ===");
    println!("Total response size: {} bytes", response.len());
    println!("Expected structure:");
    println!("  - Correlation ID: 4 bytes");
    println!("  - Brokers array: variable");
    println!("  - Topics array: variable");
    println!("\n✅ Metadata request/response test completed!");

    Ok(())
}
