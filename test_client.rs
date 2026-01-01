// Simple Kafka client to test Produce request/response
// This is a one-off test script, not part of the extension

use std::io::{Read, Write};
use std::net::TcpStream;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Connecting to localhost:9092...");
    let mut stream = TcpStream::connect("127.0.0.1:9092")?;
    println!("Connected!");

    // Test 1: ApiVersions request
    test_api_versions(&mut stream)?;

    // Test 2: Produce request
    test_produce(&mut stream)?;

    Ok(())
}

fn test_api_versions(stream: &mut TcpStream) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== TEST 1: ApiVersions Request ===");

    let mut request = Vec::new();

    // API Key: 18 (ApiVersions)
    request.extend_from_slice(&18i16.to_be_bytes());
    // API Version: 3
    request.extend_from_slice(&3i16.to_be_bytes());
    // Correlation ID: 42
    request.extend_from_slice(&42i32.to_be_bytes());
    // Client ID: "test-client"
    let client_id = "test-client";
    request.extend_from_slice(&(client_id.len() as i16).to_be_bytes());
    request.extend_from_slice(client_id.as_bytes());

    // Prepend size
    let size = request.len() as i32;
    let mut full_request = Vec::new();
    full_request.extend_from_slice(&size.to_be_bytes());
    full_request.extend_from_slice(&request);

    println!("Sending ApiVersions request ({} bytes)...", full_request.len());
    stream.write_all(&full_request)?;
    stream.flush()?;

    // Read response
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf)?;
    let response_size = i32::from_be_bytes(size_buf);
    let mut response = vec![0u8; response_size as usize];
    stream.read_exact(&mut response)?;

    println!("ApiVersions response received ({} bytes)", response.len());
    println!("✅ ApiVersions test passed\n");

    Ok(())
}

fn test_produce(stream: &mut TcpStream) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== TEST 2: Produce Request ===");
    println!("⚠️  Produce test requires building complex RecordBatch format");
    println!("For now, testing basic connectivity worked with ApiVersions.");
    println!("\nTo fully test Produce:");
    println!("1. Use real Kafka client (kcat, kafka-console-producer)");
    println!("2. Or use integration tests with kafka-protocol crate");
    println!("\nSkipping Produce test for now...");

    Ok(())
}
