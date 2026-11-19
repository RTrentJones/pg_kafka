//! Compression tests for pg_kafka
//!
//! These tests verify compression support:
//! - Compressed producer: Clients can send compressed messages
//! - Roundtrip: Produce compressed, consume decompressed

mod edge_cases;
mod producer;
mod roundtrip;

/// The `compression` tests, in run order.
pub fn tests() -> Vec<crate::registry::TestDef> {
    crate::test_defs!["compression";
        producer::test_compressed_producer_gzip,
        producer::test_compressed_producer_snappy,
        producer::test_compressed_producer_lz4,
        producer::test_compressed_producer_zstd,
        roundtrip::test_compression_roundtrip,
        edge_cases::test_large_value_compression,
        edge_cases::test_compression_mixed_formats,
        edge_cases::test_compression_ratio_verification,
        edge_cases::test_compression_incompressible_data,
        edge_cases::test_compression_small_message_overhead,
    ]
}
