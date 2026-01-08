//! Compression tests for pg_kafka
//!
//! These tests verify compression support:
//! - Compressed producer: Clients can send compressed messages
//! - Roundtrip: Produce compressed, consume decompressed

mod producer;
mod roundtrip;

pub use producer::{
    test_compressed_producer_gzip, test_compressed_producer_lz4, test_compressed_producer_snappy,
    test_compressed_producer_zstd,
};
pub use roundtrip::test_compression_roundtrip;
