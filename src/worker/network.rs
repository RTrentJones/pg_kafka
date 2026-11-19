//! Listener security for the network thread.

/// Build the per-listener security (TLS acceptor, SASL mechanisms, advertised address)
/// for every configured listener, primary first. Loads the TLS files once.
pub(super) fn build_listener_securities(
    settings: &crate::kafka::auth::SecuritySettings,
) -> Result<Vec<std::sync::Arc<crate::kafka::listener::ListenerSecurity>>, String> {
    let tls_config = if settings.needs_tls() {
        Some(crate::kafka::auth::tls::build_server_config(
            &settings.ssl_cert_file,
            &settings.ssl_key_file,
            settings
                .mutual_tls()
                .then_some(settings.ssl_ca_file.as_str()),
        )?)
    } else {
        None
    };
    let mechanisms = std::sync::Arc::new(settings.sasl_mechanisms.clone());
    Ok(settings
        .listeners
        .iter()
        .map(|spec| {
            std::sync::Arc::new(crate::kafka::listener::ListenerSecurity {
                protocol: spec.protocol,
                bind_addr: spec.bind_addr(),
                tls: spec
                    .protocol
                    .uses_tls()
                    .then(|| tls_config.clone())
                    .flatten()
                    .map(tokio_rustls::TlsAcceptor::from),
                sasl_mechanisms: mechanisms.clone(),
                advertised: spec.advertised.clone(),
                // wave3 net: connection caps and timeouts.
                limits: settings.limits,
            })
        })
        .collect())
}
