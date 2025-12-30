// Simple Kafka client to test ApiVersions request/response
// This is a one-off test script, not part of the extension

use std::io::{Read, Write};
use std::net::TcpStream;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Connecting to localhost:9092...");
    let mut stream = TcpStream::connect("127.0.0.1:9092")?;
    println!("Connected!");

    // Build an ApiVersions request (API key 18)
    // Format: [Size: 4 bytes][API Key: 2 bytes][API Version: 2 bytes][Correlation ID: 4 bytes][Client ID: string]

    let mut request = Vec::new();

    // API Key: 18 (ApiVersions)
    request.extend_from_slice(&18i16.to_be_bytes());

    // API Version: 3 (we claim to support 0-3)
    request.extend_from_slice(&3i16.to_be_bytes());

    // Correlation ID: 42 (arbitrary, should be echoed back)
    request.extend_from_slice(&42i32.to_be_bytes());

    // Client ID: "test-client" (nullable string)
    // Format: length (2 bytes) + string bytes
    let client_id = "test-client";
    request.extend_from_slice(&(client_id.len() as i16).to_be_bytes());
    request.extend_from_slice(client_id.as_bytes());

    // Prepend the size (4 bytes, big-endian)
    let size = request.len() as i32;
    let mut full_request = Vec::new();
    full_request.extend_from_slice(&size.to_be_bytes());
    full_request.extend_from_slice(&request);

    println!("Sending ApiVersions request ({} bytes)...", full_request.len());
    println!("Request bytes: {:?}", full_request);

    stream.write_all(&full_request)?;
    stream.flush()?;

    println!("Request sent! Waiting for response...");

    // Read response size (4 bytes)
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf)?;
    let response_size = i32::from_be_bytes(size_buf);

    println!("Response size: {} bytes", response_size);

    // Read response payload
    let mut response = vec![0u8; response_size as usize];
    stream.read_exact(&mut response)?;

    println!("Response received ({} bytes)!", response.len());
    println!("Response bytes: {:?}", response);

    // Parse response
    println!("\n=== Parsing Response ===");
    let mut offset = 0;

    // Correlation ID (should be 42)
    if response.len() >= 4 {
        let correlation_id = i32::from_be_bytes([
            response[offset], response[offset+1], response[offset+2], response[offset+3]
        ]);
        println!("Correlation ID: {} (expected: 42)", correlation_id);
        offset += 4;

        if correlation_id == 42 {
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

    Ok(())
}
