// Test helpers and utilities for pg_kafka unit tests
//
// This module provides mock implementations and utilities for testing
// the Kafka protocol implementation without requiring real TCP connections.

use bytes::{Buf, BufMut, BytesMut};
use std::io::{self, Read, Write};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// MockStream simulates a bidirectional TCP stream for testing
///
/// This allows us to test protocol parsing and response encoding without
/// actually creating TCP connections. Data written to the stream can be
/// read back, and vice versa.
///
/// # Example
/// ```rust
/// let mut stream = MockStream::new();
/// stream.write_all(&[0x00, 0x01, 0x02]).unwrap();
/// stream.rewind(); // Reset read position to beginning
///
/// let mut buf = [0u8; 3];
/// stream.read_exact(&mut buf).unwrap();
/// assert_eq!(buf, [0x00, 0x01, 0x02]);
/// ```
pub struct MockStream {
    /// Internal buffer holding all data
    buffer: BytesMut,
    /// Current read position in the buffer
    read_pos: usize,
}

impl MockStream {
    /// Create a new empty MockStream
    pub fn new() -> Self {
        MockStream {
            buffer: BytesMut::new(),
            read_pos: 0,
        }
    }

    /// Create a MockStream pre-populated with data
    pub fn with_data(data: &[u8]) -> Self {
        let mut buffer = BytesMut::new();
        buffer.put_slice(data);
        MockStream {
            buffer,
            read_pos: 0,
        }
    }

    /// Reset the read position to the beginning of the buffer
    pub fn rewind(&mut self) {
        self.read_pos = 0;
    }

    /// Get the number of bytes available for reading
    pub fn available(&self) -> usize {
        self.buffer.len() - self.read_pos
    }

    /// Get all data that has been written to the stream
    pub fn written_data(&self) -> &[u8] {
        &self.buffer[..]
    }

    /// Clear all data from the stream
    pub fn clear(&mut self) {
        self.buffer.clear();
        self.read_pos = 0;
    }
}

impl Default for MockStream {
    fn default() -> Self {
        Self::new()
    }
}

// Synchronous Read implementation
impl Read for MockStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let available = self.available();
        if available == 0 {
            return Ok(0);
        }

        let to_read = buf.len().min(available);
        buf[..to_read].copy_from_slice(&self.buffer[self.read_pos..self.read_pos + to_read]);
        self.read_pos += to_read;
        Ok(to_read)
    }
}

// Synchronous Write implementation
impl Write for MockStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.buffer.put_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

// Async Read implementation for tokio compatibility
impl AsyncRead for MockStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let available = self.available();
        if available == 0 {
            return Poll::Ready(Ok(()));
        }

        let to_read = buf.remaining().min(available);
        buf.put_slice(&self.buffer[self.read_pos..self.read_pos + to_read]);
        self.read_pos += to_read;
        Poll::Ready(Ok(()))
    }
}

// Async Write implementation for tokio compatibility
impl AsyncWrite for MockStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.buffer.put_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// Helper function to create a Kafka request with proper framing
///
/// Kafka requests are framed as: [Size: i32][ApiKey: i16][ApiVersion: i16][CorrelationId: i32][...]
/// This function prepends the size field to a request body.
pub fn frame_kafka_request(request_body: &[u8]) -> Vec<u8> {
    let size = request_body.len() as i32;
    let mut framed = Vec::new();
    framed.extend_from_slice(&size.to_be_bytes());
    framed.extend_from_slice(request_body);
    framed
}

/// Helper function to parse the size prefix from a Kafka response
///
/// Returns (size, remaining_data) where size is the message size and
/// remaining_data is everything after the size field.
pub fn parse_kafka_response_size(response: &[u8]) -> io::Result<(i32, &[u8])> {
    if response.len() < 4 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "Response too short to contain size field",
        ));
    }
    let size = i32::from_be_bytes([response[0], response[1], response[2], response[3]]);
    Ok((size, &response[4..]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};

    #[test]
    fn test_mock_stream_read_write() {
        let mut stream = MockStream::new();

        // Write data
        stream.write_all(&[1, 2, 3, 4]).unwrap();

        // Reset to read from beginning
        stream.rewind();

        // Read data back
        let mut buf = [0u8; 4];
        stream.read_exact(&mut buf).unwrap();
        assert_eq!(buf, [1, 2, 3, 4]);
    }

    #[test]
    fn test_mock_stream_with_data() {
        let mut stream = MockStream::with_data(&[5, 6, 7, 8]);

        let mut buf = [0u8; 4];
        stream.read_exact(&mut buf).unwrap();
        assert_eq!(buf, [5, 6, 7, 8]);
    }

    #[test]
    fn test_mock_stream_available() {
        let mut stream = MockStream::with_data(&[1, 2, 3, 4, 5]);
        assert_eq!(stream.available(), 5);

        let mut buf = [0u8; 2];
        stream.read_exact(&mut buf).unwrap();
        assert_eq!(stream.available(), 3);
    }

    #[test]
    fn test_frame_kafka_request() {
        let request_body = vec![0x00, 0x12, 0x00, 0x03]; // ApiKey=18, Version=3
        let framed = frame_kafka_request(&request_body);

        // Should have size prefix (4 bytes) + body
        assert_eq!(framed.len(), 8);

        // Size should be 4 (length of body)
        let size = i32::from_be_bytes([framed[0], framed[1], framed[2], framed[3]]);
        assert_eq!(size, 4);

        // Body should match
        assert_eq!(&framed[4..], &request_body);
    }

    #[test]
    fn test_parse_kafka_response_size() {
        let response = vec![0x00, 0x00, 0x00, 0x10, 0xDE, 0xAD, 0xBE, 0xEF];
        let (size, remaining) = parse_kafka_response_size(&response).unwrap();

        assert_eq!(size, 16);
        assert_eq!(remaining, &[0xDE, 0xAD, 0xBE, 0xEF]);
    }

    #[test]
    fn test_parse_kafka_response_size_too_short() {
        let response = vec![0x00, 0x01]; // Only 2 bytes
        let result = parse_kafka_response_size(&response);
        assert!(result.is_err());
    }
}
