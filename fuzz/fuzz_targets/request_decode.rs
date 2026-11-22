//! Fuzz the request decoder on whole frames: header parsing, the API registry's version
//! checks, every API's body decoder and `KafkaApi::decode` (including the RecordBatch
//! parser behind Produce), and the encoding of whatever error response the decoder
//! queues. Invariants: no panic in pg_kafka's code (the kafka-protocol decoders' own
//! panics are contained by design, see `crash_only_on_own_panics`), no abort, and every
//! queued response encodes.
#![no_main]

use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    pg_kafka_fuzz::crash_only_on_own_panics();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let _ = pg_kafka::kafka::protocol::parse_request(BytesMut::from(data), tx);
    while let Ok(response) = rx.try_recv() {
        if !matches!(
            response.body,
            pg_kafka::kafka::api::ResponseBody::CloseConnection
        ) {
            pg_kafka::kafka::protocol::encode_response(response)
                .expect("a response the decoder queued must encode");
        }
    }
});
