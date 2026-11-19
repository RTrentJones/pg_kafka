//! Record headers as stored in the `kafka.messages.headers` JSONB column.

/// Decode the JSONB headers column back into header pairs (DR-8). The column holds an
/// ordered array of `[key, hex(value)]` pairs; rows written before that hold a
/// `{key: hex(value)}` object, which is still accepted.
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
    let pairs: Vec<(String, String)> = if let Ok(pairs) =
        serde_json::from_str::<Vec<(String, String)>>(json)
    {
        pairs
    } else if let Ok(map) = serde_json::from_str::<std::collections::BTreeMap<String, String>>(json)
    {
        map.into_iter().collect()
    } else {
        return Vec::new();
    };
    pairs
        .into_iter()
        .filter_map(|(k, hex)| hex_decode(&hex).map(|v| (k, v)))
        .collect()
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
    use super::decode_headers_json;

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
        assert!(decode_headers_json(None).is_empty());
        assert!(decode_headers_json(Some("{}")).is_empty());
        assert!(decode_headers_json(Some("")).is_empty());
    }

    #[test]
    fn test_decode_headers_json_malformed_is_lenient() {
        // Malformed JSON: drop all headers rather than failing the fetch
        assert!(decode_headers_json(Some("not-json")).is_empty());
        // Malformed hex value: that header is dropped, valid ones survive
        let json = r#"{"bad":"zz","good":"6869"}"#;
        assert_eq!(
            decode_headers_json(Some(json)),
            vec![("good".to_string(), b"hi".to_vec())]
        );
        // Odd-length hex is malformed
        assert!(decode_headers_json(Some(r#"{"odd":"abc"}"#)).is_empty());
    }

    #[test]
    fn test_decode_headers_json_non_ascii_hex_does_not_panic() {
        // A multi-byte char puts the 2-byte hex window off a char boundary; that must
        // drop the header (None), not panic with "not a char boundary".
        assert!(decode_headers_json(Some(r#"{"k":"aé0"}"#)).is_empty());
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
