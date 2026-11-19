//! Length-prefixed fields shared by the consumer-protocol subscription and assignment encodings:
//! i32 array lengths, i16-prefixed strings and nullable i32-prefixed bytes.

use bytes::{Buf, BufMut};

use crate::kafka::error::{KafkaError, Result};

/// Writes `len` as an i32 array or bytes length.
pub(super) fn put_i32_len(buf: &mut Vec<u8>, len: usize, what: &str) -> Result<()> {
    let len = i32::try_from(len)
        .map_err(|_| KafkaError::Internal(format!("{what} length {len} exceeds i32::MAX")))?;
    buf.put_i32(len);
    Ok(())
}

/// Writes an i16-length-prefixed string.
pub(super) fn put_string(buf: &mut Vec<u8>, s: &str) -> Result<()> {
    let len = i16::try_from(s.len()).map_err(|_| {
        KafkaError::Internal(format!("string of {} bytes exceeds i16::MAX", s.len()))
    })?;
    buf.put_i16(len);
    buf.put_slice(s.as_bytes());
    Ok(())
}

/// Writes nullable bytes: an i32 length (-1 for null) and the data.
pub(super) fn put_nullable_bytes(buf: &mut Vec<u8>, data: Option<&[u8]>) -> Result<()> {
    match data {
        Some(data) => {
            put_i32_len(buf, data.len(), "user_data")?;
            buf.put_slice(data);
        }
        None => buf.put_i32(-1),
    }
    Ok(())
}

/// Reads the trailing optional `user_data`: `None` when it is absent, null, or longer than what
/// remains (clients differ in whether they send it, so a short tail is not an error).
pub(super) fn get_trailing_user_data(buf: &mut &[u8]) -> Option<Vec<u8>> {
    if buf.remaining() < 4 {
        return None;
    }
    let len = usize::try_from(buf.get_i32()).ok()?;
    if buf.remaining() < len {
        return None;
    }
    let mut data = vec![0u8; len];
    buf.copy_to_slice(&mut data);
    Some(data)
}
