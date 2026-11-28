//! The TLS side of a connection on an SSL / SASL_SSL listener: the handshake under the
//! pre-authentication deadline, and the principal of a verified client certificate.

use tokio::net::TcpStream;
use tokio_rustls::server::TlsStream;

use super::connection::Tracked;
use tracing::{debug, warn};

use super::accept::ListenerSecurity;
use super::is_peer_disconnect;
use crate::kafka::auth::ConnectionPrincipal;

/// Run the TLS handshake, or `None` (logged) when it fails or misses `deadline`.
pub(super) async fn handshake(
    acceptor: &tokio_rustls::TlsAcceptor,
    socket: Tracked<TcpStream>,
    deadline: tokio::time::Instant,
    peer: &str,
    security: &ListenerSecurity,
) -> Option<TlsStream<Tracked<TcpStream>>> {
    match tokio::time::timeout_at(deadline, acceptor.accept(socket)).await {
        Ok(Ok(tls)) => Some(tls),
        Ok(Err(e)) => {
            // A peer that connects and leaves without a ClientHello (port probe, load
            // balancer health check) is routine; a real handshake failure is not.
            if is_peer_disconnect(&e) {
                debug!("TLS peer {peer} left before the handshake: {e}");
            } else {
                warn!(
                    "TLS handshake with {peer} on {} failed: {e}",
                    security.describe()
                );
            }
            None
        }
        Err(_) => {
            warn!("TLS handshake with {peer} timed out");
            None
        }
    }
}

/// Under mutual TLS, the verified client certificate's subject is the principal
/// (Kafka's default SSL principal, `User:<RFC 2253 subject>`) until SASL replaces it;
/// otherwise the connection is anonymous.
pub(super) fn client_principal<S>(tls: &TlsStream<S>) -> ConnectionPrincipal {
    tls.get_ref()
        .1
        .peer_certificates()
        .and_then(|certs| certs.first())
        .and_then(|cert| crate::kafka::auth::tls::certificate_subject(cert.as_ref()))
        .map_or_else(ConnectionPrincipal::anonymous, |subject| {
            ConnectionPrincipal {
                principal: format!("User:{subject}"),
                method: "SSL",
            }
        })
}

#[cfg(test)]
mod tests {
    use super::super::connection::{ActivityClock, Tracked};
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // Throwaway self-signed test certificate and key (P-256). Test data only.
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

    /// A client verifier that accepts any server (the test's self-signed certificate).
    #[derive(Debug)]
    struct AcceptAny;

    impl rustls::client::danger::ServerCertVerifier for AcceptAny {
        fn verify_server_cert(
            &self,
            _end_entity: &rustls_pki_types::CertificateDer<'_>,
            _intermediates: &[rustls_pki_types::CertificateDer<'_>],
            _server_name: &rustls_pki_types::ServerName<'_>,
            _ocsp_response: &[u8],
            _now: rustls_pki_types::UnixTime,
        ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
            Ok(rustls::client::danger::ServerCertVerified::assertion())
        }

        fn verify_tls12_signature(
            &self,
            _message: &[u8],
            _cert: &rustls_pki_types::CertificateDer<'_>,
            _dss: &rustls::DigitallySignedStruct,
        ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
            Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
        }

        fn verify_tls13_signature(
            &self,
            _message: &[u8],
            _cert: &rustls_pki_types::CertificateDer<'_>,
            _dss: &rustls::DigitallySignedStruct,
        ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
            Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
        }

        fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
            rustls::crypto::ring::default_provider()
                .signature_verification_algorithms
                .supported_schemes()
        }
    }

    fn write_temp(name: &str, contents: &str) -> String {
        let dir =
            std::env::temp_dir().join(format!("pg_kafka_listener_tls_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(name);
        std::fs::write(&path, contents).unwrap();
        path.to_string_lossy().into_owned()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_tls_records_flushed_to_a_slow_reader_count_as_activity() {
        // rustls buffers up to 64 KiB of records that go out on a later write or on flush.
        // The activity clock sits under TLS, on the TCP socket, so bytes a slow reader takes
        // while the connection flushes those records count too: a client reading the end of
        // a response slowly is not closed as stalled while the response still goes out.
        let cert = write_temp("cert.pem", TEST_CERT_PEM);
        let key = write_temp("key.pem", TEST_KEY_PEM);
        let acceptor = tokio_rustls::TlsAcceptor::from(
            crate::kafka::auth::tls::build_server_config(&cert, &key, None).unwrap(),
        );
        // Small socket buffers on both ends, so the kernel holds little of what is written.
        let listener = tokio::net::TcpSocket::new_v4().unwrap();
        listener.set_send_buffer_size(4096).unwrap();
        listener.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let listener = listener.listen(1).unwrap();
        let addr = listener.local_addr().unwrap();
        let client = tokio::spawn(async move {
            let config = rustls::ClientConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .unwrap()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(AcceptAny))
            .with_no_client_auth();
            let socket = tokio::net::TcpSocket::new_v4().unwrap();
            socket.set_recv_buffer_size(4096).unwrap();
            let tcp = socket.connect(addr).await.unwrap();
            let name = rustls_pki_types::ServerName::try_from("localhost").unwrap();
            let mut tls = tokio_rustls::TlsConnector::from(Arc::new(config))
                .connect(name, tcp)
                .await
                .unwrap();
            // Read slowly: 16 KiB every 20 ms.
            let mut buf = vec![0u8; 16 * 1024];
            let mut total = 0usize;
            loop {
                tokio::time::sleep(Duration::from_millis(20)).await;
                match tls.read(&mut buf).await {
                    Ok(0) | Err(_) => return total,
                    Ok(n) => total += n,
                }
            }
        });
        let (tcp, _) = listener.accept().await.unwrap();
        let activity = Arc::new(ActivityClock::new());
        let security = ListenerSecurity::plaintext("127.0.0.1:0");
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let mut tls = handshake(
            &acceptor,
            Tracked::new(tcp, Arc::clone(&activity)),
            deadline,
            "test",
            &security,
        )
        .await
        .expect("the handshake");
        tls.write_all(&vec![7u8; 1 << 20]).await.unwrap();
        // Everything is with rustls or the socket; what rustls holds goes out on flush.
        let written = activity.last();
        tls.flush().await.unwrap();
        let flushed = activity.last();
        tls.shutdown().await.unwrap();
        drop(tls);
        assert!(client.await.unwrap() >= 1 << 20, "the client gets it all");
        assert!(
            flushed >= written + Duration::from_millis(40),
            "the writes during the flush are activity: {:?} after the last write",
            flushed.saturating_duration_since(written)
        );
    }
}
