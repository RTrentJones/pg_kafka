//! Request framing on the network thread: the 4-byte big-endian length prefix.
//!
//! [`FrameCodec`] splits a connection's byte stream into request frames and writes response
//! frames. It bounds the raw frames peers can make the broker hold (what a request decodes
//! to, a Produce's compressed records included, is bounded separately, by
//! [`super::decode_meter`] and [`super::decode_budget`]):
//!
//! * memory is reserved as payload bytes arrive, never for a length that is merely
//!   declared, and the global budget ([`FRAME_RESERVE_REMAINING`]) is charged for the bytes
//!   buffered of an incomplete frame, so holding budget costs the peer real traffic. Charging
//!   on the prefix would let a handful of connections that send only a length prefix hold
//!   the whole budget and lock every other client out;
//! * frames up to [`UNMETERED_FRAME_BYTES`] are never charged, so control traffic
//!   (ApiVersions, Metadata, Heartbeat) cannot be locked out by large frames;
//! * one connection holds at most one incomplete frame, i.e. at most `MAX_REQUEST_SIZE`
//!   (under a fifth of the budget);
//! * [`FrameClock`] records when the incomplete frame started arriving so the connection
//!   can enforce a per-frame read deadline (`pg_kafka.request_read_timeout_ms`).
//!
//! A connection's read buffer does not keep a large frame's memory: the frame takes the
//! allocation it arrived in, and the buffer starts a new small one ([`READ_BUFFER_BYTES`]),
//! as Kafka allocates each receive.
//!
//! [`FrameSink`] writes response frames. They are bounded only by the wire format
//! (`i32::MAX`), not by the inbound request limit: a response's size is bounded by the
//! request's `max_bytes` and `pg_kafka.fetch_max_bytes`, as in Kafka. A large response is
//! written from its own buffer, never copied into a write buffer that would keep the largest
//! response's memory for the connection's life.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::sink::Sink;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{ready, Context, Poll};
use std::time::{Duration, Instant};
use tokio::io::AsyncWrite;
use tokio_util::codec::Decoder;

use crate::kafka::constants::MAX_REQUEST_SIZE;

/// Total bytes that may be buffered across all connections for incomplete frames larger
/// than [`UNMETERED_FRAME_BYTES`].
pub const FRAME_RESERVE_BUDGET_BYTES: usize = 512 * 1024 * 1024;

/// Frames up to this size are never charged to the budget. They are bounded by the
/// connection caps (one incomplete frame per connection) instead.
pub const UNMETERED_FRAME_BYTES: usize = 1024 * 1024;

/// Largest frame an unauthenticated client may send on a SASL listener (Kafka's
/// `sasl.server.max.receive.size` default).
pub const PRE_AUTH_MAX_FRAME: usize = 512 * 1024;

/// Bytes of the big-endian length prefix in front of every frame.
const LENGTH_PREFIX_BYTES: usize = 4;

/// Smallest step the buffer of an incomplete large frame grows by.
const MIN_BUFFER_GROWTH: usize = 64 * 1024;

/// What a connection's read buffer starts with again after a frame larger than
/// [`UNMETERED_FRAME_BYTES`] (tokio-util's own initial read buffer).
pub const READ_BUFFER_BYTES: usize = 8 * 1024;

/// Responses up to this size are copied behind their length prefix and written at once;
/// larger ones are written from their own buffer.
const COPIED_RESPONSE_BYTES: usize = 64 * 1024;

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
        // Saturates after 584 million years of uptime.
        u64::try_from(self.epoch.elapsed().as_millis())
            .unwrap_or(u64::MAX)
            .saturating_add(1)
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
        let Some(prefix) = src.first_chunk::<LENGTH_PREFIX_BYTES>() else {
            return Ok(None);
        };
        let len = u32::from_be_bytes(*prefix) as usize;
        if len > MAX_REQUEST_SIZE {
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
        let total = LENGTH_PREFIX_BYTES + len;
        if src.len() >= total {
            self.release();
            self.clock.clear();
            let mut frame = src.split_to(total);
            frame.advance(LENGTH_PREFIX_BYTES);
            if total > UNMETERED_FRAME_BYTES {
                // The frame keeps the large allocation it arrived in (freed with it); the
                // buffer moves what follows it to a small new one instead of reusing that
                // allocation for the connection's life.
                let mut rest = BytesMut::with_capacity(src.len().max(READ_BUFFER_BYTES));
                rest.extend_from_slice(src);
                *src = rest;
            }
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
            let buffered = src.len() - LENGTH_PREFIX_BYTES;
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
            let want = (total - src.len()).min(src.len().max(MIN_BUFFER_GROWTH));
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

/// Writes response frames (`[i32 length][response]`) to a connection's socket. A response
/// up to [`COPIED_RESPONSE_BYTES`] is copied behind its prefix and written in one piece; a
/// larger one is written from its own buffer (the prefix and the response in one vectored
/// write where the socket supports it), so the sink holds nothing once a response is out.
pub struct FrameSink<W> {
    io: W,
    prefix: [u8; LENGTH_PREFIX_BYTES],
    /// Bytes of `prefix` still to write (0: none, or it went out with the response).
    prefix_left: usize,
    /// What is left to write of the current response.
    body: Bytes,
}

impl<W> FrameSink<W> {
    /// A sink writing to `io`.
    pub fn new(io: W) -> Self {
        FrameSink {
            io,
            prefix: [0; LENGTH_PREFIX_BYTES],
            prefix_left: 0,
            body: Bytes::new(),
        }
    }

    /// Bytes the sink holds of responses not yet written (none once a response is out).
    pub fn buffered(&self) -> usize {
        self.prefix_left + self.body.len()
    }
}

impl<W: AsyncWrite + Unpin> FrameSink<W> {
    /// Write what is left of the current frame.
    fn poll_write_frame(&mut self, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        while self.prefix_left > 0 || !self.body.is_empty() {
            let written = if self.prefix_left > 0 {
                let start = LENGTH_PREFIX_BYTES - self.prefix_left;
                let parts = [
                    std::io::IoSlice::new(&self.prefix[start..]),
                    std::io::IoSlice::new(&self.body),
                ];
                ready!(Pin::new(&mut self.io).poll_write_vectored(cx, &parts))?
            } else {
                ready!(Pin::new(&mut self.io).poll_write(cx, &self.body))?
            };
            if written == 0 {
                return Poll::Ready(Err(std::io::ErrorKind::WriteZero.into()));
            }
            let of_prefix = written.min(self.prefix_left);
            self.prefix_left -= of_prefix;
            self.body.advance(written - of_prefix);
        }
        // Let go of the response's buffer.
        self.body = Bytes::new();
        Poll::Ready(Ok(()))
    }
}

impl<W: AsyncWrite + Unpin> Sink<Bytes> for FrameSink<W> {
    type Error = std::io::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.get_mut().poll_write_frame(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: Bytes) -> std::io::Result<()> {
        let this = self.get_mut();
        let len = i32::try_from(item.len()).map_err(|_| {
            std::io::Error::other(format!(
                "response of {} bytes does not fit a Kafka frame",
                item.len()
            ))
        })?;
        if item.len() <= COPIED_RESPONSE_BYTES {
            let mut frame = BytesMut::with_capacity(LENGTH_PREFIX_BYTES + item.len());
            frame.put_i32(len);
            frame.extend_from_slice(&item);
            this.prefix_left = 0;
            this.body = frame.freeze();
        } else {
            this.prefix = len.to_be_bytes();
            this.prefix_left = LENGTH_PREFIX_BYTES;
            this.body = item;
        }
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        ready!(this.poll_write_frame(cx))?;
        Pin::new(&mut this.io).poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        ready!(this.poll_write_frame(cx))?;
        ready!(Pin::new(&mut this.io).poll_flush(cx))?;
        Pin::new(&mut this.io).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn codec(budget: &'static AtomicUsize) -> (FrameCodec, Arc<FrameClock>) {
        let clock = Arc::new(FrameClock::new());
        (
            FrameCodec::with_budget(
                Arc::new(AtomicUsize::new(MAX_REQUEST_SIZE)),
                clock.clone(),
                budget,
            ),
            clock,
        )
    }

    fn frame(payload: &[u8]) -> Vec<u8> {
        let mut v = u32::try_from(payload.len()).unwrap().to_be_bytes().to_vec();
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
        // A codec that reserved (or charged) the declared 99 MB on the prefix would let a
        // few idle connections exhaust memory or the budget.
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
        src.extend_from_slice(&u32::try_from(len).unwrap().to_be_bytes());
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
        let oversized = u32::try_from(MAX_REQUEST_SIZE + 1).unwrap();
        let mut src = BytesMut::from(&oversized.to_be_bytes()[..]);
        assert!(c.decode(&mut src).is_err());
        // Lengths with the top bit set (negative i32) are oversized too.
        let mut src = BytesMut::from(&u32::MAX.to_be_bytes()[..]);
        assert!(c.decode(&mut src).is_err());

        let clock = Arc::new(FrameClock::new());
        let mut c =
            FrameCodec::with_budget(Arc::new(AtomicUsize::new(PRE_AUTH_MAX_FRAME)), clock, &B);
        let mut src =
            BytesMut::from(&(u32::try_from(PRE_AUTH_MAX_FRAME).unwrap() + 1).to_be_bytes()[..]);
        assert!(c.decode(&mut src).is_err());
        assert_eq!(c.charged(), 0);
    }

    /// Send `responses` through a [`FrameSink`] over an in-memory pipe whose reader keeps
    /// everything; returns the bytes the reader got and the sink's buffered bytes after each.
    fn sink_roundtrip(responses: Vec<Bytes>) -> (Vec<u8>, Vec<usize>) {
        use futures::SinkExt;
        use tokio::io::AsyncReadExt;
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(async {
            let (client, server) = tokio::io::duplex(64 * 1024);
            let reader = tokio::spawn(async move {
                let mut client = client;
                let mut out = Vec::new();
                client.read_to_end(&mut out).await.unwrap();
                out
            });
            let mut sink = FrameSink::new(server);
            let mut buffered = Vec::new();
            for response in responses {
                sink.send(response).await.unwrap();
                buffered.push(sink.buffered());
            }
            sink.close().await.unwrap();
            drop(sink);
            (reader.await.unwrap(), buffered)
        })
    }

    #[test]
    fn frame_sink_has_no_request_size_cap_and_frames_every_response() {
        // A response larger than the request limit must still be framed, or the
        // connection would hang on it; small and large responses keep their order.
        let big = Bytes::from(vec![7u8; MAX_REQUEST_SIZE + 10]);
        let (out, buffered) = sink_roundtrip(vec![
            Bytes::from_static(b"small"),
            big,
            Bytes::from_static(b"after"),
        ]);
        assert_eq!(
            buffered,
            vec![0, 0, 0],
            "nothing held once a response is out"
        );
        assert_eq!(out.len(), 3 * 4 + 5 + MAX_REQUEST_SIZE + 10 + 5);
        assert_eq!(&out[..9], b"\0\0\0\x05small");
        let len = u32::from_be_bytes([out[9], out[10], out[11], out[12]]) as usize;
        assert_eq!(len, MAX_REQUEST_SIZE + 10);
        assert!(out[13..13 + len].iter().all(|&b| b == 7));
        assert_eq!(&out[13 + len..], b"\0\0\0\x05after");
    }

    #[test]
    fn frame_sink_does_not_copy_a_large_response() {
        // The connection's write path does not copy a large response into a buffer of its
        // own (which would take twice the response's memory while it is written, and keep
        // the largest response's memory for the connection's life).
        use futures::SinkExt;
        let response = Bytes::from(vec![1u8; 8 << 20]);
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let ((), copied) = crate::testing::heap_allocated(|| {
            rt.block_on(async {
                let mut sink = FrameSink::new(tokio::io::sink());
                sink.send(response.clone()).await.unwrap();
                sink.send(Bytes::from_static(b"small")).await.unwrap();
            });
        });
        assert!(copied < 1 << 20, "{copied} bytes allocated to write 8 MiB");
    }

    #[test]
    fn the_read_buffer_lets_go_of_a_large_frame() {
        // After a 3 MiB frame the connection's buffer does not keep (and reuse) its
        // allocation: once the frame is dropped and the next read reserves room, as
        // tokio-util's FramedRead does, the buffer is small again.
        static B: AtomicUsize = AtomicUsize::new(FRAME_RESERVE_BUDGET_BYTES);
        let (mut c, _clock) = codec(&B);
        let mut src = BytesMut::new();
        src.extend_from_slice(&frame(&vec![3u8; 3 << 20]));
        src.extend_from_slice(&frame(b"next"));
        let large = c.decode(&mut src).unwrap().unwrap();
        assert_eq!(large.len(), 3 << 20);
        drop(large);
        assert_eq!(&c.decode(&mut src).unwrap().unwrap()[..], b"next");
        src.reserve(1);
        assert!(
            src.capacity() < UNMETERED_FRAME_BYTES,
            "the buffer kept {} bytes after a large frame",
            src.capacity()
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
