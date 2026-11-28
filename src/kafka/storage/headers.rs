//! Record headers as stored in the `kafka.messages.headers` JSONB column.

/// Encode record headers for the JSONB headers column: an array of `[key, hex(value)]`
/// pairs, which keeps the producer's header order (a JSONB object would reorder its
/// keys). No headers encode as `{}`. A key holding U+0000, which JSONB cannot store
/// (it refuses the `\u0000` escape), is stored as hex too, marked by a third element:
/// `[hex(key), hex(value), "hex"]`; [`decode_headers_json`] reverses it.
pub fn encode_headers_json<'a>(
    headers: impl ExactSizeIterator<Item = (&'a str, &'a [u8])>,
) -> String {
    if headers.len() == 0 {
        return EMPTY_HEADERS.to_string();
    }
    let entries: Vec<serde_json::Value> = headers
        .map(|(k, v)| {
            if k.contains('\0') {
                serde_json::json!([hex_encode(k.as_bytes()), hex_encode(v), HEX_KEY])
            } else {
                serde_json::json!([k, hex_encode(v)])
            }
        })
        .collect();
    serde_json::to_string(&entries).unwrap_or_else(|_| EMPTY_HEADERS.to_string())
}

/// The headers column of a record without headers.
const EMPTY_HEADERS: &str = "{}";

/// The third element of a stored header whose key is hex.
const HEX_KEY: &str = "hex";

/// Lowercase hex of `bytes`.
fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut out, b| {
            let _ = write!(out, "{b:02x}");
            out
        })
}

/// Decode the JSONB headers column back into header pairs (DR-8). The column holds an
/// ordered array of `[key, hex(value)]` pairs (`[hex(key), hex(value), "hex"]` for a key
/// stored as hex); rows written before that hold a `{key: hex(value)}` object, which is
/// still accepted.
///
/// Pure function so the decoding logic is unit-testable even though its only
/// caller lives in the coverage-ignored SPI layer (QA-1 pattern, like
/// `validate_sequence`). Malformed JSON or hex yields an empty/partial result
/// rather than an error: headers are metadata, and failing a whole fetch over
/// an undecodable header would be worse than dropping it.
pub fn decode_headers_json(json: Option<&str>) -> Vec<(String, Vec<u8>)> {
    let Some(json) = json else {
        return Vec::new();
    };
    match serde_json::from_str::<serde_json::Value>(json) {
        Ok(serde_json::Value::Array(entries)) => entries.iter().filter_map(decode_entry).collect(),
        Ok(serde_json::Value::Object(map)) => map
            .into_iter()
            .filter_map(|(k, v)| hex_decode(v.as_str()?).map(|v| (k, v)))
            .collect(),
        _ => Vec::new(),
    }
}

/// One `[key, hex(value)]` or `[hex(key), hex(value), "hex"]` entry; `None` if malformed.
fn decode_entry(entry: &serde_json::Value) -> Option<(String, Vec<u8>)> {
    match entry.as_array()?.as_slice() {
        [key, value] => Some((key.as_str()?.to_owned(), hex_decode(value.as_str()?)?)),
        [key, value, marker] if marker.as_str() == Some(HEX_KEY) => {
            let key = String::from_utf8(hex_decode(key.as_str()?)?).ok()?;
            Some((key, hex_decode(value.as_str()?)?))
        }
        _ => None,
    }
}

/// Decode a lowercase/uppercase hex string into bytes; `None` on malformed input.
fn hex_decode(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        // `get` instead of slicing: a non-ASCII char would put `i` off a char
        // boundary and slicing would panic. Invalid input yields None instead.
        .map(|i| s.get(i..i + 2).and_then(|h| u8::from_str_radix(h, 16).ok()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{decode_headers_json, encode_headers_json};

    const NO_HEADERS: Vec<(String, Vec<u8>)> = Vec::new();

    #[test]
    fn encoded_headers_decode_in_order() {
        let headers: [(&str, &[u8]); 3] = [("z", b"\x01"), ("a", b""), ("k", b"hi")];
        let json = encode_headers_json(headers.iter().copied());
        assert_eq!(json, r#"[["z","01"],["a",""],["k","6869"]]"#);
        assert_eq!(
            decode_headers_json(Some(&json)),
            vec![
                ("z".to_string(), vec![1u8]),
                ("a".to_string(), Vec::new()),
                ("k".to_string(), b"hi".to_vec()),
            ]
        );
        assert_eq!(encode_headers_json(std::iter::empty()), "{}");
    }

    /// JSONB refuses `\u0000`: a key holding U+0000 is stored as hex and decodes back
    /// unchanged; a value holding NUL bytes is hex like any other.
    #[test]
    fn a_key_with_nul_is_stored_as_hex_and_decodes_unchanged() {
        let headers: [(&str, &[u8]); 3] = [("a\0b", b"\0v\0"), ("k", b"\0"), ("\0", b"")];
        let json = encode_headers_json(headers.iter().copied());
        assert_eq!(
            json,
            r#"[["610062","007600","hex"],["k","00"],["00","","hex"]]"#
        );
        assert!(!json.contains("\\u0000"));
        assert_eq!(
            decode_headers_json(Some(&json)),
            headers
                .iter()
                .map(|(k, v)| ((*k).to_string(), v.to_vec()))
                .collect::<Vec<_>>()
        );
        // A malformed entry is dropped, the others kept.
        assert_eq!(
            decode_headers_json(Some(
                r#"[["zz","00","hex"],["6b","00","other"],["k","01"]]"#
            )),
            vec![("k".to_string(), vec![1u8])]
        );
    }

    #[test]
    fn test_decode_headers_json_roundtrip() {
        // The produce path stores headers as {key: hex(value)}
        let json = r#"{"trace-id":"616263","empty":""}"#;
        let mut headers = decode_headers_json(Some(json));
        headers.sort();
        assert_eq!(
            headers,
            vec![
                ("empty".to_string(), b"".to_vec()),
                ("trace-id".to_string(), b"abc".to_vec()),
            ]
        );
    }

    #[test]
    fn test_decode_headers_json_absent_or_empty() {
        assert_eq!(decode_headers_json(None), NO_HEADERS);
        assert_eq!(decode_headers_json(Some("{}")), NO_HEADERS);
        assert_eq!(decode_headers_json(Some("")), NO_HEADERS);
    }

    #[test]
    fn test_decode_headers_json_malformed_is_lenient() {
        // Malformed JSON: drop all headers rather than failing the fetch
        assert_eq!(decode_headers_json(Some("not-json")), NO_HEADERS);
        // Malformed hex value: that header is dropped, valid ones survive
        let json = r#"{"bad":"zz","good":"6869"}"#;
        assert_eq!(
            decode_headers_json(Some(json)),
            vec![("good".to_string(), b"hi".to_vec())]
        );
        // Odd-length hex is malformed
        assert_eq!(decode_headers_json(Some(r#"{"odd":"abc"}"#)), NO_HEADERS);
    }

    #[test]
    fn test_decode_headers_json_non_ascii_hex_does_not_panic() {
        // A multi-byte char puts the 2-byte hex window off a char boundary; that must
        // drop the header (None), not panic with "not a char boundary".
        assert_eq!(decode_headers_json(Some(r#"{"k":"aé0"}"#)), NO_HEADERS);
        let ok = decode_headers_json(Some(r#"{"bad":"aé0","good":"6869"}"#));
        assert_eq!(ok, vec![("good".to_string(), b"hi".to_vec())]);
    }

    #[test]
    fn test_decode_headers_json_array_keeps_order() {
        let decoded = decode_headers_json(Some(r#"[["z","01"],["a","02"]]"#));
        assert_eq!(
            decoded,
            vec![("z".to_string(), vec![1u8]), ("a".to_string(), vec![2u8])]
        );
        // Rows written in the old object format still decode.
        assert_eq!(
            decode_headers_json(Some(r#"{"k":"6869"}"#)),
            vec![("k".to_string(), b"hi".to_vec())]
        );
    }
}
