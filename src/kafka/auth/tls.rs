//! TLS for SSL / SASL_SSL listeners (rustls, ring provider).
//!
//! The certificate chain, key and optional client CA bundle are read once, when the
//! worker starts; any problem (unreadable file, no PEM item, key/cert mismatch) is an
//! error naming the setting and the file. With a client CA (`pg_kafka.ssl_ca_file`),
//! every TLS handshake requires a client certificate that chains to it (mutual TLS),
//! and the connection's principal is the certificate subject, rendered like Kafka's
//! default SSL principal (`User:CN=...,OU=...,O=...`).

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use std::sync::Arc;

fn read_file(setting: &str, path: &str) -> Result<Vec<u8>, String> {
    std::fs::read(path).map_err(|e| format!("cannot read {setting} '{path}': {e}"))
}

fn load_certs(setting: &str, path: &str) -> Result<Vec<CertificateDer<'static>>, String> {
    let pem = read_file(setting, path)?;
    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(&pem)
        .collect::<Result<_, _>>()
        .map_err(|e| format!("{setting} '{path}': invalid PEM: {e}"))?;
    if certs.is_empty() {
        return Err(format!("{setting} '{path}' contains no PEM CERTIFICATE"));
    }
    Ok(certs)
}

fn ring_provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// Build the shared rustls server configuration.
pub fn build_server_config(
    cert_file: &str,
    key_file: &str,
    ca_file: Option<&str>,
) -> Result<Arc<rustls::ServerConfig>, String> {
    let certs = load_certs("pg_kafka.ssl_cert_file", cert_file)?;
    let key_pem = read_file("pg_kafka.ssl_key_file", key_file)?;
    let key = PrivateKeyDer::from_pem_slice(&key_pem).map_err(|e| {
        format!("pg_kafka.ssl_key_file '{key_file}': no usable PEM private key ({e})")
    })?;
    let provider = ring_provider();
    let builder = rustls::ServerConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(|e| format!("TLS configuration: {e}"))?;
    let builder = match ca_file {
        Some(ca) => {
            let mut roots = rustls::RootCertStore::empty();
            for cert in load_certs("pg_kafka.ssl_ca_file", ca)? {
                roots
                    .add(cert)
                    .map_err(|e| format!("pg_kafka.ssl_ca_file '{ca}': {e}"))?;
            }
            let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
                Arc::new(roots),
                provider,
            )
            .build()
            .map_err(|e| format!("pg_kafka.ssl_ca_file '{ca}': {e}"))?;
            builder.with_client_cert_verifier(verifier)
        }
        None => builder.with_no_client_auth(),
    };
    let config = builder.with_single_cert(certs, key).map_err(|e| {
        format!("pg_kafka.ssl_cert_file '{cert_file}' / pg_kafka.ssl_key_file '{key_file}': {e}")
    })?;
    Ok(Arc::new(config))
}

// ===== Certificate subject -> principal =====

/// One DER TLV: `(tag, contents, rest)`.
fn der_tlv(input: &[u8]) -> Option<(u8, &[u8], &[u8])> {
    let (&tag, rest) = input.split_first()?;
    let (&first, rest) = rest.split_first()?;
    let (len, rest) = if first < 0x80 {
        (first as usize, rest)
    } else {
        let n = (first & 0x7f) as usize;
        if n == 0 || n > 4 || rest.len() < n {
            return None;
        }
        let len = rest[..n]
            .iter()
            .fold(0usize, |acc, b| (acc << 8) | *b as usize);
        (len, &rest[n..])
    };
    if rest.len() < len {
        return None;
    }
    Some((tag, &rest[..len], &rest[len..]))
}

fn oid_to_string(oid: &[u8]) -> Option<String> {
    let (&first, rest) = oid.split_first()?;
    let mut parts = vec![u64::from(first / 40), u64::from(first % 40)];
    let mut value: u64 = 0;
    for &b in rest {
        value = value.checked_mul(128)? | u64::from(b & 0x7f);
        if b & 0x80 == 0 {
            parts.push(value);
            value = 0;
        }
    }
    Some(
        parts
            .iter()
            .map(std::string::ToString::to_string)
            .collect::<Vec<_>>()
            .join("."),
    )
}

fn attribute_name(oid: &str) -> Option<&'static str> {
    Some(match oid {
        "2.5.4.3" => "CN",
        "2.5.4.6" => "C",
        "2.5.4.7" => "L",
        "2.5.4.8" => "ST",
        "2.5.4.9" => "STREET",
        "2.5.4.10" => "O",
        "2.5.4.11" => "OU",
        "0.9.2342.19200300.100.1.1" => "UID",
        "0.9.2342.19200300.100.1.25" => "DC",
        _ => return None,
    })
}

/// RFC 2253 escaping of an attribute value.
fn escape_value(v: &str) -> String {
    let mut out = String::with_capacity(v.len());
    let last = v.chars().count().saturating_sub(1);
    for (i, c) in v.chars().enumerate() {
        let special = matches!(c, ',' | '+' | '"' | '\\' | '<' | '>' | ';')
            || (i == 0 && (c == '#' || c == ' '))
            || (i == last && c == ' ');
        if special {
            out.push('\\');
        }
        if c.is_control() {
            out.push_str(&format!("\\{:02X}", c as u32 & 0xff));
        } else {
            out.push(c);
        }
    }
    out
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Render one AttributeTypeAndValue.
fn render_attribute(atv: &[u8]) -> Option<String> {
    let (tag, oid, rest) = der_tlv(atv)?;
    if tag != 0x06 {
        return None;
    }
    let oid = oid_to_string(oid)?;
    let (vtag, value, after) = der_tlv(rest)?;
    let value_der = &rest[..rest.len() - after.len()];
    let text = match vtag {
        // UTF8String, PrintableString, IA5String, NumericString, VisibleString
        0x0c | 0x13 | 0x16 | 0x12 | 0x1a => std::str::from_utf8(value).ok().map(str::to_string),
        // TeletexString: treat as Latin-1
        0x14 => Some(value.iter().map(|b| *b as char).collect()),
        // BMPString: UCS-2 big endian
        0x1e if value.len() % 2 == 0 => String::from_utf16(
            &value
                .chunks(2)
                .map(|c| u16::from_be_bytes([c[0], c[1]]))
                .collect::<Vec<_>>(),
        )
        .ok(),
        _ => None,
    };
    match (attribute_name(&oid), text) {
        (Some(name), Some(t)) => Some(format!("{}={}", name, escape_value(&t))),
        // Unknown type or non-string value: RFC 2253 dotted OID + '#' hex of the DER.
        _ => Some(format!("{}=#{}", oid, hex(value_der))),
    }
}

/// The subject of a DER certificate as an RFC 2253 string (last RDN first, the way
/// Java's `X500Principal.getName()` and therefore Kafka print it), or `None` if the
/// certificate cannot be parsed.
pub fn certificate_subject(cert_der: &[u8]) -> Option<String> {
    let (tag, cert, _) = der_tlv(cert_der)?;
    if tag != 0x30 {
        return None;
    }
    let (tag, tbs, _) = der_tlv(cert)?;
    if tag != 0x30 {
        return None;
    }
    let mut rest = tbs;
    // Optional [0] EXPLICIT version.
    if rest.first() == Some(&0xa0) {
        rest = der_tlv(rest)?.2;
    }
    // serialNumber, signature, issuer, validity, then subject.
    for _ in 0..4 {
        rest = der_tlv(rest)?.2;
    }
    let (tag, mut name, _) = der_tlv(rest)?;
    if tag != 0x30 {
        return None;
    }
    let mut rdns = Vec::new();
    while !name.is_empty() {
        let (tag, mut set, next) = der_tlv(name)?;
        if tag != 0x31 {
            return None;
        }
        let mut attrs = Vec::new();
        while !set.is_empty() {
            let (tag, atv, next_atv) = der_tlv(set)?;
            if tag != 0x30 {
                return None;
            }
            attrs.push(render_attribute(atv)?);
            set = next_atv;
        }
        rdns.push(attrs.join("+"));
        name = next;
    }
    rdns.reverse();
    Some(rdns.join(","))
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine as _;

    // Throwaway self-signed test certificate and key (P-256), subject
    // C=US, O="Acme, Inc.", OU=Eng+OU=Ops, CN=client1. Test data only.
    const TEST_CERT_PEM: &str = "-----BEGIN CERTIFICATE-----
MIIB8zCCAZmgAwIBAgIUa1AdR2vchqfGhR9F2nFZlYfzuG4wCgYIKoZIzj0EAwIw
TjELMAkGA1UEBhMCVVMxEzARBgNVBAoMCkFjbWUsIEluYy4xGDAKBgNVBAsMA0Vu
ZzAKBgNVBAsMA09wczEQMA4GA1UEAwwHY2xpZW50MTAgFw0yNjExMTgwNzM5MjJa
GA8yMTI2MTAyNTA3MzkyMlowTjELMAkGA1UEBhMCVVMxEzARBgNVBAoMCkFjbWUs
IEluYy4xGDAKBgNVBAsMA0VuZzAKBgNVBAsMA09wczEQMA4GA1UEAwwHY2xpZW50
MTBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABD18BObmHoGRbNJlX57KkerGA0Dv
wJI0WWOUs3eGvjVUfEflVbkI5k8Y1M3LIf10xStK8Z7NgKlvR5LrwxSPNUmjUzBR
MB0GA1UdDgQWBBQ6lBdlB6a/mcjPxzt2DbVrfdfu3TAfBgNVHSMEGDAWgBQ6lBdl
B6a/mcjPxzt2DbVrfdfu3TAPBgNVHRMBAf8EBTADAQH/MAoGCCqGSM49BAMCA0gA
MEUCIQC7KK3W9rivMW1H/KY1b+f5Rpdez5dNMDvyTl1DkfjbTQIgQkng2N6J+SK/
nvrNDhWeW7VOCUYkY1wAY+1PUhuYbwU=
-----END CERTIFICATE-----
";
    const TEST_KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgOJ5vi7mqfvdIt9fG
pjWowKso7XkPNUq3Rd5+8v/LkzuhRANCAAQ9fATm5h6BkWzSZV+eypHqxgNA78CS
NFljlLN3hr41VHxH5VW5COZPGNTNyyH9dMUrSvGezYCpb0eS68MUjzVJ
-----END PRIVATE KEY-----
";

    fn test_cert_der() -> Vec<u8> {
        let b64: String = TEST_CERT_PEM
            .lines()
            .filter(|l| !l.starts_with("-----"))
            .collect();
        base64::engine::general_purpose::STANDARD
            .decode(b64)
            .unwrap()
    }

    fn write_temp(name: &str, contents: &str) -> String {
        let dir = std::env::temp_dir().join(format!("pg_kafka_tls_test_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(name);
        std::fs::write(&path, contents).unwrap();
        path.to_string_lossy().into_owned()
    }

    #[test]
    fn subject_is_rendered_like_kafka() {
        assert_eq!(
            certificate_subject(&test_cert_der()).as_deref(),
            Some("CN=client1,OU=Eng+OU=Ops,O=Acme\\, Inc.,C=US")
        );
    }

    #[test]
    fn subject_parsing_rejects_garbage() {
        assert_eq!(certificate_subject(b""), None);
        assert_eq!(certificate_subject(&[0x30, 0x82, 0xff]), None);
        let der = test_cert_der();
        for cut in [1, 10, 100, 150] {
            assert_eq!(certificate_subject(&der[..cut]), None, "cut at {cut}");
        }
    }

    #[test]
    fn escaping_and_oids() {
        assert_eq!(escape_value(" a,b "), "\\ a\\,b\\ ");
        assert_eq!(escape_value("#x"), "\\#x");
        assert_eq!(
            oid_to_string(&[0x55, 0x04, 0x03]).as_deref(),
            Some("2.5.4.3")
        );
        assert_eq!(
            oid_to_string(&[0x09, 0x92, 0x26, 0x89, 0x93, 0xf2, 0x2c, 0x64, 0x01, 0x19]).as_deref(),
            Some("0.9.2342.19200300.100.1.25")
        );
        // Unknown attribute type: dotted OID and hex DER value.
        let atv = [0x06, 0x03, 0x2a, 0x03, 0x04, 0x0c, 0x01, b'x'];
        assert_eq!(render_attribute(&atv).as_deref(), Some("1.2.3.4=#0c0178"));
    }

    #[test]
    fn server_config_loads_and_reports_bad_files() {
        let cert = write_temp("cert.pem", TEST_CERT_PEM);
        let key = write_temp("key.pem", TEST_KEY_PEM);
        assert!(build_server_config(&cert, &key, None).is_ok());
        // Mutual TLS with the self-signed cert as its own CA.
        assert!(build_server_config(&cert, &key, Some(&cert)).is_ok());

        let missing = build_server_config("/nonexistent/c.pem", &key, None).unwrap_err();
        assert!(
            missing.contains("pg_kafka.ssl_cert_file") && missing.contains("/nonexistent/c.pem")
        );
        let garbage = write_temp("garbage.pem", "not pem");
        let no_cert = build_server_config(&garbage, &key, None).unwrap_err();
        assert!(no_cert.contains("no PEM CERTIFICATE"), "{no_cert}");
        let no_key = build_server_config(&cert, &garbage, None).unwrap_err();
        assert!(no_key.contains("pg_kafka.ssl_key_file"), "{no_key}");
        let bad_ca = build_server_config(&cert, &key, Some(&garbage)).unwrap_err();
        assert!(bad_ca.contains("pg_kafka.ssl_ca_file"), "{bad_ca}");
    }
}
