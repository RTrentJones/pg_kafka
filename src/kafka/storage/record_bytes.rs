//! The bytes one record adds to a Fetch response: what both Fetch byte limits
//! (`partition_max_bytes` and the request-level `max_bytes`) and the storage layer's
//! fetch budget count.

/// The framing bytes [`record_fetch_bytes`] counts per record and per header, upper
/// bounds of what an uncompressed RecordBatch v2 spends on them. A record alone in its batch
/// (each run of one producer's consecutive offsets is a batch: read_committed skipping
/// aborted offsets, or alternating producers, leave records alone) carries the whole 61-byte
/// batch header and at most 23 bytes of its own: its length, key and value length and header
/// count varints (at most 5 bytes each), attributes, and timestamp and offset deltas of 0
/// (1 byte each). A record sharing its batch takes at most 31 bytes of its own (both deltas
/// up to 5) and at most half the header. A header's key and value lengths are two varints of
/// at most 5 bytes.
pub const RECORD_OVERHEAD: i64 = 61 + 23;
pub const HEADER_OVERHEAD: i64 = 10;

/// The bytes a record of a `key_len`-byte key and a `value_len`-byte value with headers of
/// the given (key length, value length) pairs adds to a Fetch response: those lengths
/// uncompressed, plus [`RECORD_OVERHEAD`] and [`HEADER_OVERHEAD`] per header. Stored
/// (compressed) sizes would let a highly compressible record count as a small fraction of
/// what the response and the DB thread hold of it.
///
/// Produce stores it with each row (`kafka.messages.record_bytes`), so a fetch sums it
/// without reading a row's key, value or headers; the SQL fallback for rows written without
/// it (`record_bytes_sql!` in `storage/postgres/log.rs`) counts the same.
pub fn record_fetch_bytes(
    key_len: usize,
    value_len: usize,
    headers: impl IntoIterator<Item = (usize, usize)>,
) -> i64 {
    let len = |n: usize| i64::try_from(n).unwrap_or(i64::MAX);
    let headers = headers.into_iter().fold(0i64, |sum, (k, v)| {
        sum.saturating_add(len(k))
            .saturating_add(len(v))
            .saturating_add(HEADER_OVERHEAD)
    });
    len(key_len)
        .saturating_add(len(value_len))
        .saturating_add(RECORD_OVERHEAD)
        .saturating_add(headers)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_key_value_headers_and_framing() {
        assert_eq!(record_fetch_bytes(0, 0, []), 84);
        assert_eq!(record_fetch_bytes(3, 7, []), 94);
        assert_eq!(record_fetch_bytes(1, 0, [(2, 3)]), 84 + 1 + 2 + 3 + 10);
        assert_eq!(
            record_fetch_bytes(0, 5, [(1, 0), (4, 250_000)]),
            84 + 5 + (1 + 10) + (4 + 250_000 + 10)
        );
        // Saturates instead of overflowing.
        assert_eq!(record_fetch_bytes(usize::MAX, 1, []), i64::MAX);
    }
}
