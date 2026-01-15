//! Compression tests for pg_kafka
//!
//! These tests verify compression support:
//! - Compressed producer: Clients can send compressed messages
//! - Roundtrip: Produce compressed, consume decompressed

mod edge_cases;
mod producer;
mod roundtrip;

pub use edge_cases::{
    test_compression_incompressible_data, test_compression_mixed_formats,
    test_compression_ratio_verification, test_compression_small_message_overhead,
    test_large_value_compression,
};
pub use producer::{
    test_compressed_producer_gzip, test_compressed_producer_lz4, test_compressed_producer_snappy,
    test_compressed_producer_zstd,
};
pub use roundtrip::test_compression_roundtrip;
