//! Building and parsing record batches (RecordBatch v2).

use crate::common::{to_i32, to_i64, DynResult as Result};
use bytes::{Buf, Bytes, BytesMut};
use kafka_protocol::indexmap::IndexMap;
use kafka_protocol::records::{
    Compression, Record, RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};
use std::time::{SystemTime, UNIX_EPOCH};

/// A record of a plain (non-idempotent) producer: no producer id or sequence,
/// timestamp 0.
pub fn plain_record(offset: i64, key: Option<&str>, value: &[u8]) -> Record {
    Record {
        transactional: false,
        control: false,
        partition_leader_epoch: 0,
        producer_id: -1,
        producer_epoch: -1,
        timestamp_type: TimestampType::Creation,
        offset,
        sequence: -1,
        timestamp: 0,
        key: key.map(|k| Bytes::copy_from_slice(k.as_bytes())),
        value: Some(Bytes::copy_from_slice(value)),
        headers: IndexMap::default(),
    }
}

/// `records` as one RecordBatch v2.
pub fn encode(records: &[Record], compression: Compression) -> Bytes {
    let mut buf = BytesMut::new();
    RecordBatchEncoder::encode(
        &mut buf,
        records.iter(),
        &RecordEncodeOptions {
            version: 2,
            compression,
        },
    )
    .expect("a test record batch encodes");
    buf.freeze()
}

/// One batch of a plain producer holding `values`, at offsets 0, 1, ...
pub fn plain_batch(values: &[Bytes], compression: Compression) -> Bytes {
    let records: Vec<Record> = (0_i64..)
        .zip(values)
        .map(|(offset, value)| plain_record(offset, None, value))
        .collect();
    encode(&records, compression)
}

/// Milliseconds since the Unix epoch.
pub fn now_ms() -> i64 {
    let since_epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the clock is after 1970");
    i64::try_from(since_epoch.as_millis()).expect("milliseconds since 1970 fit in i64")
}

/// A RecordBatch v2 of `values` (timestamps `timestamps[i]`, or now), written by
/// `producer_id`/`epoch` from `base_sequence` (all -1 for a plain producer).
pub fn batch(
    values: &[&str],
    producer_id: i64,
    epoch: i16,
    base_sequence: i32,
    transactional: bool,
    timestamps: Option<&[i64]>,
) -> Bytes {
    let now = now_ms();
    let records: Vec<Record> = values
        .iter()
        .enumerate()
        .map(|(i, v)| Record {
            transactional,
            control: false,
            partition_leader_epoch: -1,
            producer_id,
            producer_epoch: epoch,
            timestamp_type: TimestampType::Creation,
            offset: to_i64(i),
            // The encoder keeps records in one batch while offset - sequence is
            // constant; the batch's base sequence is the first record's (-1 = none).
            sequence: base_sequence.wrapping_add(to_i32(i)),
            timestamp: timestamps.map_or(now, |t| t[i]),
            key: None,
            value: Some(Bytes::copy_from_slice(v.as_bytes())),
            headers: IndexMap::default(),
        })
        .collect();
    encode(&records, Compression::None)
}

/// The batch headers inside a fetched `records` blob:
/// `(base_offset, last_offset_delta, record_count)`.
pub fn batch_headers(records: &Bytes) -> Vec<(i64, i32, i32)> {
    let mut out = Vec::new();
    let mut b = records.clone();
    while b.remaining() >= 61 {
        let base = b.get_i64();
        // A negative length (corrupt input) takes the rest of the blob.
        let len = usize::try_from(b.get_i32()).unwrap_or(usize::MAX);
        let body = b.copy_to_bytes(len.min(b.remaining()));
        // body: leader epoch(4) magic(1) crc(4) attributes(2) last delta(4) ...
        let last_delta = i32::from_be_bytes(body[11..15].try_into().unwrap());
        let count = i32::from_be_bytes(body[45..49].try_into().unwrap());
        out.push((base, last_delta, count));
    }
    out
}

/// Every record (offset, value) in a fetched `records` blob.
pub fn fetched_values(records: &Bytes) -> Result<Vec<(i64, String)>> {
    let mut b = records.clone();
    let mut out = Vec::new();
    for set in RecordBatchDecoder::decode_all(&mut b)? {
        for r in set.records {
            let v = r
                .value
                .map_or_default(|v| String::from_utf8_lossy(&v).into_owned());
            out.push((r.offset, v));
        }
    }
    Ok(out)
}
