use bytes::{Buf, Bytes};

use super::{metered, unmetered, MeteredBuf};

#[test]
fn test_a_metered_decode_counts_what_it_keeps() {
    let (kept, m) = metered(1 << 20, || vec![0u8; 1000]);
    assert_eq!(kept.len(), 1000);
    assert_eq!(m.held, 1000);
    assert!(!m.over);
    let ((), m) = metered(1 << 20, || drop(vec![0u8; 100_000]));
    assert_eq!(m.held, 0, "what a decode frees it does not hold");
}

#[test]
fn test_a_metered_buffer_stops_once_the_decode_holds_more_than_its_limit() {
    let (read, m) = metered(10_000, || {
        let mut buf = MeteredBuf::new(Bytes::from(vec![1u8; 100]));
        let mut kept = Vec::new();
        let mut read = 0;
        while buf.remaining() > 0 {
            buf.advance(1);
            read += 1;
            kept.push(vec![0u8; 1000]);
        }
        assert!(buf.stopped());
        assert_eq!(buf.chunk(), &[] as &[u8]);
        drop(kept);
        read
    });
    // Ten 1000-byte blocks and the vector holding them pass 10,000 bytes.
    assert_eq!(read, 10, "stopped at the first read past 10,000 bytes held");
    assert!(m.over);
}

#[test]
fn test_unmetered_memory_does_not_count() {
    let ((a, b), m) = metered(10_000, || {
        let a = unmetered(|| vec![0u8; 50_000]);
        let mut buf = MeteredBuf::new(Bytes::from_static(b"x"));
        assert_eq!(
            buf.remaining(),
            1,
            "not over: the unmetered bytes do not count"
        );
        buf.advance(1);
        (a, vec![0u8; 100])
    });
    assert_eq!((a.len(), b.len()), (50_000, 100));
    assert_eq!(m.held, 100);
    assert!(!m.over);
}

#[test]
fn test_without_a_meter_nothing_is_stopped() {
    let big = vec![0u8; 1 << 20];
    let buf = MeteredBuf::new(Bytes::from_static(b"abc"));
    assert_eq!(buf.remaining(), 3);
    drop(big);
}

#[test]
fn test_the_meter_is_restored_after_a_panic() {
    let r = std::panic::catch_unwind(|| metered(1, || panic!("decoder panic")));
    assert!(r.is_err());
    let buf = MeteredBuf::new(Bytes::from_static(b"abc"));
    let _held = vec![0u8; 1000];
    assert_eq!(buf.remaining(), 3, "no meter runs after the scope");
}
