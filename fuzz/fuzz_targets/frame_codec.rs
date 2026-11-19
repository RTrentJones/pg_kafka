//! Fuzz the request framing (`protocol::framing::FrameCodec`) and, for every frame it
//! yields, the request decoder (`protocol::parse_request`: header + API registry).
//!
//! The input is split into reads of varying size (the first byte picks the chunking) to
//! exercise partial prefixes and frames spanning reads. Invariants: no panic in
//! pg_kafka's code, every yielded frame is within the request limit, and the budget the
//! codec charged comes back when it is dropped.
#![no_main]

use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;
use pg_kafka::kafka::constants::MAX_REQUEST_SIZE;
use pg_kafka::kafka::protocol::framing::{FrameClock, FrameCodec, FRAME_RESERVE_BUDGET_BYTES};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio_util::codec::Decoder;

static BUDGET: AtomicUsize = AtomicUsize::new(FRAME_RESERVE_BUDGET_BYTES);

fuzz_target!(|data: &[u8]| {
    pg_kafka_fuzz::crash_only_on_own_panics();
    let Some((&chunking, data)) = data.split_first() else {
        return;
    };
    let chunk = 1 + (chunking as usize % 64);
    let limit = Arc::new(AtomicUsize::new(if chunking & 0x80 != 0 {
        pg_kafka::kafka::protocol::framing::PRE_AUTH_MAX_FRAME
    } else {
        MAX_REQUEST_SIZE
    }));
    {
        let mut codec = FrameCodec::with_budget(limit, Arc::new(FrameClock::new()), &BUDGET);
        let mut src = BytesMut::new();
        'outer: for piece in data.chunks(chunk) {
            src.extend_from_slice(piece);
            loop {
                match codec.decode(&mut src) {
                    Ok(Some(frame)) => {
                        assert!(frame.len() <= MAX_REQUEST_SIZE);
                        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
                        let _ = pg_kafka::kafka::protocol::parse_request(frame, tx);
                    }
                    Ok(None) => break,
                    Err(_) => break 'outer,
                }
            }
        }
    }
    assert_eq!(
        BUDGET.load(Ordering::Relaxed),
        FRAME_RESERVE_BUDGET_BYTES,
        "the codec leaked budget"
    );
});
