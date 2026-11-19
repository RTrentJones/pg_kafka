//! SASL/PLAIN message parsing (RFC 4616).
//!
//! `message = [authzid] NUL authcid NUL passwd`. As in Kafka, an authorization id is
//! accepted only when it is empty or equal to the authentication id. The password is
//! held only long enough to check it against the stored SCRAM credential; it is never
//! stored, logged or shown by `Debug`.

/// RFC 4616 limits each field to 255 octets.
pub const MAX_PLAIN_FIELD_LEN: usize = 255;

/// The credentials carried by a PLAIN message.
pub struct PlainCredentials {
    pub username: String,
    pub password: Vec<u8>,
}

impl std::fmt::Debug for PlainCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PlainCredentials")
            .field("username", &self.username)
            .field("password", &"[REDACTED]")
            .finish()
    }
}

impl Drop for PlainCredentials {
    /// Best effort: overwrite the password bytes before the allocation is freed.
    fn drop(&mut self) {
        for b in &mut self.password {
            // SAFETY: `b` is a valid, aligned, exclusive reference into the Vec.
            unsafe { std::ptr::write_volatile(b, 0) };
        }
    }
}

/// Why a PLAIN message was rejected (for logs; the client only sees "authentication failed").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlainError {
    Malformed(&'static str),
    AuthzidMismatch,
}

impl std::fmt::Display for PlainError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PlainError::Malformed(m) => write!(f, "malformed PLAIN message: {m}"),
            PlainError::AuthzidMismatch => {
                write!(f, "authorization id differs from the username")
            }
        }
    }
}

/// Parse a PLAIN message.
pub fn parse_plain(message: &[u8]) -> Result<PlainCredentials, PlainError> {
    let mut parts = message.split(|b| *b == 0);
    let (authzid, authcid, passwd) = match (parts.next(), parts.next(), parts.next(), parts.next())
    {
        (Some(z), Some(c), Some(p), None) => (z, c, p),
        _ => {
            return Err(PlainError::Malformed(
                "expected authzid NUL authcid NUL passwd",
            ))
        }
    };
    if authcid.is_empty() {
        return Err(PlainError::Malformed("empty username"));
    }
    if passwd.is_empty() {
        return Err(PlainError::Malformed("empty password"));
    }
    if authzid.len() > MAX_PLAIN_FIELD_LEN
        || authcid.len() > MAX_PLAIN_FIELD_LEN
        || passwd.len() > MAX_PLAIN_FIELD_LEN
    {
        return Err(PlainError::Malformed("field longer than 255 bytes"));
    }
    let username = std::str::from_utf8(authcid)
        .map_err(|_| PlainError::Malformed("username is not UTF-8"))?
        .to_string();
    std::str::from_utf8(passwd).map_err(|_| PlainError::Malformed("password is not UTF-8"))?;
    if !authzid.is_empty() && authzid != authcid {
        return Err(PlainError::AuthzidMismatch);
    }
    Ok(PlainCredentials {
        username,
        password: passwd.to_vec(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_with_and_without_authzid() {
        let c = parse_plain(b"\0alice\0secret").unwrap();
        assert_eq!(c.username, "alice");
        assert_eq!(c.password, b"secret");
        let c = parse_plain(b"alice\0alice\0secret").unwrap();
        assert_eq!(c.username, "alice");
        // UTF-8 is allowed in both fields.
        let c = parse_plain("\0j\u{f6}rg\0p\u{e4}ss".as_bytes()).unwrap();
        assert_eq!(c.username, "j\u{f6}rg");
    }

    #[test]
    fn rejects_malformed_messages() {
        let err = |m: &[u8]| parse_plain(m).unwrap_err();
        assert_eq!(err(b"bob\0alice\0secret"), PlainError::AuthzidMismatch);
        assert!(matches!(err(b"alice\0secret"), PlainError::Malformed(_)));
        assert!(matches!(
            err(b"\0alice\0secret\0x"),
            PlainError::Malformed(_)
        ));
        assert!(matches!(err(b"\0\0secret"), PlainError::Malformed(_)));
        assert!(matches!(err(b"\0alice\0"), PlainError::Malformed(_)));
        assert!(matches!(err(b""), PlainError::Malformed(_)));
        assert!(matches!(err(b"\0\xff\0pw"), PlainError::Malformed(_)));
        assert!(matches!(err(b"\0u\0\xff"), PlainError::Malformed(_)));
        let mut long = b"\0".to_vec();
        long.extend(std::iter::repeat_n(b'a', 256));
        long.extend_from_slice(b"\0pw");
        assert!(matches!(err(&long), PlainError::Malformed(_)));
    }

    #[test]
    fn debug_hides_the_password() {
        let c = parse_plain(b"\0alice\0hunter2").unwrap();
        let s = format!("{:?}", c);
        assert!(s.contains("alice"));
        assert!(!s.contains("hunter2"));
    }
}
