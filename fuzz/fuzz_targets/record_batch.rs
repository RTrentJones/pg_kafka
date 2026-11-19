//! Fuzz the RecordBatch / legacy MessageSet parser directly (no catch_unwind around it):
//! it must return Ok or Err for any bytes, never panic or abort.
#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let bytes = bytes::Bytes::copy_from_slice(data);
    let _ = pg_kafka::kafka::protocol::parse_record_batch_with_metadata(&bytes);
});
