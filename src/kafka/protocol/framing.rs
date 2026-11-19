//! Request framing on the network thread: the 4-byte big-endian length prefix.
//!
//! [`FrameCodec`] splits the byte stream into request frames and writes response frames.
//! It replaces `LengthDelimitedCodec`, which (a) reserved the full declared length of a
//! frame as soon as its 4-byte prefix arrived and (b) capped *outbound* frames at the
//! inbound request limit. With (a), the global reservation budget had to be charged on the
//! prefix, so a handful of connections that each sent only a length prefix held the whole
//! budget and locked every other client out (wave 3, net item 1). With (b), a response
//! larger than 100 MB failed to encode and the connection hung (item 2).
//!
//! Now:
//!
//! * memory is reserved as payload bytes actually arrive, and the global budget
//!   ([`FRAME_RESERVE_REMAINING`]) is charged for the bytes buffered of an incomplete
//!   frame, so holding budget costs the peer real traffic; frames up to
//!   [`UNMETERED_FRAME_BYTES`] are never charged (control traffic such as ApiVersions,
//!   Metadata and Heartbeat cannot be locked out by large frames);
//! * one connection holds at most one incomplete frame, i.e. at most
//!   `MAX_REQUEST_SIZE` (under a fifth of the budget);
//! * [`FrameClock`] records when the incomplete frame started arriving so the connection
//!   can enforce a per-frame read deadline (`pg_kafka.request_read_timeout_ms`);
//! * outbound frames are only bounded by the wire format (`i32::MAX`); a response's size
//!   is bounded by the request's own `max_bytes`, as in Kafka.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_util::codec::{Decoder, Encoder};

use crate::kafka::constants::MAX_REQUEST_SIZE;

/// Total bytes that may be buffered across all connections for incomplete frames larger
/// than [`UNMETERED_FRAME_BYTES`] (RV-9).
pub const FRAME_RESERVE_BUDGET_BYTES: usize = 512 * 1024 * 1024;

/// Frames up to this size are never charged to the budget. They are bounded by the
/// connection caps (one incomplete frame per connection) instead.
pub const UNMETERED_FRAME_BYTES: usize = 1024 * 1024;

/// Largest frame an unauthenticated client may send on a SASL listener (Kafka's
/// `sasl.server.max.receive.size` default).
pub const PRE_AUTH_MAX_FRAME: usize = 512 * 1024;

/// Remaining global budget, in bytes.
pub static FRAME_RESERVE_REMAINING: AtomicUsize = AtomicUsize::new(FRAME_RESERVE_BUDGET_BYTES);

/// Try to charge `n` bytes to `budget`. Returns false (charging nothing) if it lacks them.
pub fn charge_frame_budget(budget: &AtomicUsize, n: usize) -> bool {
    let mut remaining = budget.load(Ordering::Relaxed);
    loop {
        if n > remaining {
            return false;
        }
        match budget.compare_exchange_weak(
            remaining,
            remaining - n,
            Ordering::Acquire,
            Ordering::Relaxed,
        ) {
            Ok(_) => return true,
            Err(actual) => remaining = actual,
        }
    }
}

/// Give `n` bytes back to `budget`.
pub fn release_frame_budget(budget: &AtomicUsize, n: usize) {
    if n > 0 {
        budget.fetch_add(n, Ordering::Release);
    }
}

/// When the frame currently being received started arriving, shared between the codec
/// (inside the connection's `Framed`) and the connection's read loop.
#[derive(Debug)]
pub struct FrameClock {
    epoch: Instant,
    /// Milliseconds since `epoch` + 1 when an incomplete frame started; 0 = none.
    started: AtomicU64,
}

impl Default for FrameClock {
    fn default() -> Self {
        Self::new()
    }
}

impl FrameClock {
    pub fn new() -> Self {
        FrameClock {
            epoch: Instant::now(),
            started: AtomicU64::new(0),
        }
    }

    fn now_mark(&self) -> u64 {
        self.epoch.elapsed().as_millis() as u64 + 1
    }

    /// A frame has started arriving (no-op while one is already pending).
    pub fn mark_started(&self) {
        let _ =
            self.started
                .compare_exchange(0, self.now_mark(), Ordering::Relaxed, Ordering::Relaxed);
    }

    /// The pending frame completed.
    pub fn clear(&self) {
        self.started.store(0, Ordering::Relaxed);
    }

    /// Restart the clock of a pending frame (the connection stopped reading for a while,
    /// e.g. under in-flight backpressure; that time must not count against the peer).
    pub fn restart_if_pending(&self) {
        if self.started.load(Ordering::Relaxed) != 0 {
            self.started.store(self.now_mark(), Ordering::Relaxed);
        }
    }

    /// When the pending frame started arriving, if one is pending.
    pub fn pending_since(&self) -> Option<Instant> {
        match self.started.load(Ordering::Relaxed) {
            0 => None,
            m => Some(self.epoch + Duration::from_millis(m - 1)),
        }
    }
}

/// The request/response frame codec of one connection (see the module docs).
pub struct FrameCodec {
    /// Largest frame accepted right now: [`PRE_AUTH_MAX_FRAME`] on a SASL listener until
    /// the connection authenticates, then `MAX_REQUEST_SIZE`.
    limit: Arc<AtomicUsize>,
    budget: &'static AtomicUsize,
    /// Bytes charged to `budget` for the incomplete frame.
    charged: usize,
    clock: Arc<FrameClock>,
}

impl FrameCodec {
    /// A codec charging the process-wide budget.
    pub fn new(limit: Arc<AtomicUsize>, clock: Arc<FrameClock>) -> Self {
        Self::with_budget(limit, clock, &FRAME_RESERVE_REMAINING)
    }

    /// A codec charging `budget` (tests use their own).
    pub fn with_budget(
        limit: Arc<AtomicUsize>,
        clock: Arc<FrameClock>,
        budget: &'static AtomicUsize,
    ) -> Self {
        FrameCodec {
            limit,
            budget,
            charged: 0,
            clock,
        }
    }

    /// Bytes charged to the budget for the incomplete frame.
    pub fn charged(&self) -> usize {
        self.charged
    }

    fn release(&mut self) {
        release_frame_budget(self.budget, self.charged);
        self.charged = 0;
    }
}

impl Decoder for FrameCodec {
    type Item = BytesMut;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> std::io::Result<Option<BytesMut>> {
        if src.is_empty() {
            return Ok(None);
        }
        self.clock.mark_started();
        if src.len() < 4 {
            return Ok(None);
        }
        let len = u32::from_be_bytes([src[0], src[1], src[2], src[3]]) as usize;
        if len > MAX_REQUEST_SIZE as usize {
            self.release();
            return Err(std::io::Error::other(format!(
                "frame of {len} bytes exceeds the maximum request size of {MAX_REQUEST_SIZE} bytes"
            )));
        }
        let limit = self.limit.load(Ordering::Relaxed);
        if len > limit {
            self.release();
            return Err(std::io::Error::other(
                "frame larger than allowed before SASL authentication",
            ));
        }
        let total = 4 + len;
        if src.len() >= total {
            self.release();
            self.clock.clear();
            let mut frame = src.split_to(total);
            frame.advance(4);
            if !src.is_empty() {
                // The next frame has started arriving.
                self.clock.mark_started();
            }
            return Ok(Some(frame));
        }
        // Incomplete: charge what is buffered of a large frame, never what is merely
        // declared, and let the buffer grow as bytes arrive (at most doubling, bounded by
        // the frame) instead of reserving the declared length up front.
        if len > UNMETERED_FRAME_BYTES {
            let buffered = src.len() - 4;
            if buffered > self.charged {
                let delta = buffered - self.charged;
                if !charge_frame_budget(self.budget, delta) {
                    self.release();
                    return Err(std::io::Error::other(
                        "aggregate request reservation budget exhausted",
                    ));
                }
                self.charged = buffered;
            }
            let want = (total - src.len()).min(src.len().max(64 * 1024));
            src.reserve(want);
        }
        Ok(None)
    }
}

/// Release any charge still held for a partially received frame when the codec goes away
/// (the client disconnected mid-frame, or the connection was closed).
impl Drop for FrameCodec {
    fn drop(&mut self) {
        self.release();
    }
}

impl Encoder<Bytes> for FrameCodec {
    type Error = std::io::Error;

    fn encode(&mut self, item: Bytes, dst: &mut BytesMut) -> std::io::Result<()> {
        if item.len() > i32::MAX as usize {
            return Err(std::io::Error::other(format!(
                "response of {} bytes does not fit a Kafka frame",
                item.len()
            )));
        }
        dst.reserve(4 + item.len());
        dst.put_u32(item.len() as u32);
        dst.extend_from_slice(&item);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn codec(budget: &'static AtomicUsize) -> (FrameCodec, Arc<FrameClock>) {
        let clock = Arc::new(FrameClock::new());
        (
            FrameCodec::with_budget(
                Arc::new(AtomicUsize::new(MAX_REQUEST_SIZE as usize)),
                clock.clone(),
                budget,
            ),
            clock,
        )
    }

    fn frame(payload: &[u8]) -> Vec<u8> {
        let mut v = (payload.len() as u32).to_be_bytes().to_vec();
        v.extend_from_slice(payload);
        v
    }

    #[test]
    fn decodes_pipelined_frames_and_tracks_the_pending_one() {
        static B: AtomicUsize = AtomicUsize::new(FRAME_RESERVE_BUDGET_BYTES);
        let (mut c, clock) = codec(&B);
        let mut src = BytesMut::new();
        src.extend_from_slice(&frame(b"one"));
        src.extend_from_slice(&frame(b"two"));
        src.extend_from_slice(&[0, 0]); // half a prefix of a third frame
        assert_eq!(&c.decode(&mut src).unwrap().unwrap()[..], b"one");
        assert_eq!(&c.decode(&mut src).unwrap().unwrap()[..], b"two");
        assert!(
            clock.pending_since().is_some(),
            "the third frame is pending"
        );
        assert!(c.decode(&mut src).unwrap().is_none());
        src.extend_from_slice(&[0, 1, b'x']);
        assert_eq!(&c.decode(&mut src).unwrap().unwrap()[..], b"x");
        assert!(clock.pending_since().is_none());
        // A zero-length frame is a (empty) frame; parsing rejects it later.
        src.extend_from_slice(&frame(b""));
        assert_eq!(c.decode(&mut src).unwrap().unwrap().len(), 0);
    }

    #[test]
    fn a_length_prefix_alone_reserves_and_charges_nothing() {
        // Item 1 (fail-before: LengthDelimitedCodec reserved the declared 99 MB and the old
        // codec charged it to the budget on the prefix).
        static B: AtomicUsize = AtomicUsize::new(FRAME_RESERVE_BUDGET_BYTES);
        let (mut c, _clock) = codec(&B);
        let mut src = BytesMut::from(&99_999_999u32.to_be_bytes()[..]);
        assert!(c.decode(&mut src).unwrap().is_none());
        assert_eq!(c.charged(), 0);
        assert_eq!(B.load(Ordering::Relaxed), FRAME_RESERVE_BUDGET_BYTES);
        assert!(src.capacity() < 1024 * 1024, "no up-front reservation");
    }

    #[test]
    fn large_frames_charge_received_bytes_and_release_on_completion() {
        static B: AtomicUsize = AtomicUsize::new(FRAME_RESERVE_BUDGET_BYTES);
        let (mut c, _clock) = codec(&B);
        let len = 3 * 1024 * 1024;
        let mut src = BytesMut::new();
        src.extend_from_slice(&(len as u32).to_be_bytes());
        src.extend_from_slice(&vec![7u8; 2 * 1024 * 1024]);
        assert!(c.decode(&mut src).unwrap().is_none());
        assert_eq!(c.charged(), 2 * 1024 * 1024);
        assert_eq!(
            B.load(Ordering::Relaxed),
            FRAME_RESERVE_BUDGET_BYTES - 2 * 1024 * 1024
        );
        src.extend_from_slice(&vec![7u8; 1024 * 1024]);
        let f = c.decode(&mut src).unwrap().unwrap();
        assert_eq!(f.len(), len);
        assert_eq!(c.charged(), 0);
        assert_eq!(B.load(Ordering::Relaxed), FRAME_RESERVE_BUDGET_BYTES);
    }

    #[test]
    fn small_frames_are_never_charged_even_with_the_budget_exhausted() {
        static B: AtomicUsize = AtomicUsize::new(0);
        let (mut c, _clock) = codec(&B);
        let mut src = BytesMut::new();
        src.extend_from_slice(&(1000u32).to_be_bytes());
        src.extend_from_slice(&[1u8; 10]);
        assert!(c.decode(&mut src).unwrap().is_none());
        src.extend_from_slice(&[1u8; 990]);
        assert_eq!(c.decode(&mut src).unwrap().unwrap().len(), 1000);
    }

    #[test]
    fn exhausted_budget_rejects_a_large_frame_and_drop_releases() {
        static B: AtomicUsize = AtomicUsize::new(2 * 1024 * 1024);
        {
            let (mut c, _clock) = codec(&B);
            let mut src = BytesMut::new();
            src.extend_from_slice(&(10_000_000u32).to_be_bytes());
            src.extend_from_slice(&vec![0u8; 1_500_000]);
            assert!(c.decode(&mut src).unwrap().is_none());
            assert_eq!(B.load(Ordering::Relaxed), 2 * 1024 * 1024 - 1_500_000);
            // Dropped mid-frame (client went away): the charge comes back.
        }
        assert_eq!(B.load(Ordering::Relaxed), 2 * 1024 * 1024);
        let (mut c, _clock) = codec(&B);
        let mut src = BytesMut::new();
        src.extend_from_slice(&(10_000_000u32).to_be_bytes());
        src.extend_from_slice(&vec![0u8; 3 * 1024 * 1024]);
        let err = c.decode(&mut src).unwrap_err();
        assert!(err.to_string().contains("budget exhausted"), "{err}");
        assert_eq!(c.charged(), 0);
        assert_eq!(B.load(Ordering::Relaxed), 2 * 1024 * 1024);
    }

    #[test]
    fn oversized_and_pre_auth_frames_are_refused() {
        static B: AtomicUsize = AtomicUsize::new(FRAME_RESERVE_BUDGET_BYTES);
        let (mut c, _clock) = codec(&B);
        let mut src = BytesMut::from(&(MAX_REQUEST_SIZE as u32 + 1).to_be_bytes()[..]);
        assert!(c.decode(&mut src).is_err());
        // Lengths with the top bit set (negative i32) are oversized too.
        let mut src = BytesMut::from(&u32::MAX.to_be_bytes()[..]);
        assert!(c.decode(&mut src).is_err());

        let clock = Arc::new(FrameClock::new());
        let mut c =
            FrameCodec::with_budget(Arc::new(AtomicUsize::new(PRE_AUTH_MAX_FRAME)), clock, &B);
        let mut src = BytesMut::from(&(PRE_AUTH_MAX_FRAME as u32 + 1).to_be_bytes()[..]);
        assert!(c.decode(&mut src).is_err());
        assert_eq!(c.charged(), 0);
    }

    #[test]
    fn encoder_has_no_request_size_cap() {
        // Item 2 (fail-before: LengthDelimitedCodec refused to encode > MAX_REQUEST_SIZE).
        static B: AtomicUsize = AtomicUsize::new(FRAME_RESERVE_BUDGET_BYTES);
        let (mut c, _clock) = codec(&B);
        let big = Bytes::from(vec![0u8; MAX_REQUEST_SIZE as usize + 10]);
        let mut dst = BytesMut::new();
        c.encode(big, &mut dst).unwrap();
        assert_eq!(dst.len(), MAX_REQUEST_SIZE as usize + 14);
        assert_eq!(
            u32::from_be_bytes([dst[0], dst[1], dst[2], dst[3]]) as usize,
            MAX_REQUEST_SIZE as usize + 10
        );
    }

    #[test]
    fn clock_restart_and_clear() {
        let clock = FrameClock::new();
        assert!(clock.pending_since().is_none());
        clock.restart_if_pending();
        assert!(
            clock.pending_since().is_none(),
            "restart does not start a frame"
        );
        clock.mark_started();
        let first = clock.pending_since().unwrap();
        std::thread::sleep(Duration::from_millis(5));
        clock.mark_started();
        assert_eq!(
            clock.pending_since().unwrap(),
            first,
            "mark keeps the start"
        );
        clock.restart_if_pending();
        assert!(clock.pending_since().unwrap() > first);
        clock.clear();
        assert!(clock.pending_since().is_none());
    }
}
