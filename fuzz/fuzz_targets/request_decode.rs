//! Fuzz the request decoder on whole frames the way the listener decodes them
//! (`protocol::decode_request`): header parsing, the API registry's version checks, every
//! API's body decoder and `KafkaApi::decode` under the decode meter, the retry of a decode
//! past the inline limit under a large-decode slot, and the RecordBatch parser behind
//! Produce on both of its paths: inline, and the records of a Produce that waits for a
//! large-decode slot or for decode budget (`finish_request`), then the encoding of whatever
//! error response the decoder queues. Invariants: no panic in pg_kafka's code (the
//! kafka-protocol decoders' own panics are contained by design, see
//! `crash_only_on_own_panics`), no abort, and every queued response encodes.
#![no_main]

use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;

thread_local! {
    /// One runtime for every input: the large-decode slots and the decode waits need its
    /// timer and its blocking pool.
    static RUNTIME: tokio::runtime::Runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("a runtime");
}

fuzz_target!(|data: &[u8]| {
    pg_kafka_fuzz::crash_only_on_own_panics();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let decoded = RUNTIME.with(|rt| {
        rt.block_on(pg_kafka::kafka::protocol::decode_request(
            BytesMut::from(data),
            tx,
        ))
    });
    drop(decoded);
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
