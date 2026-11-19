//! Listener security settings: Kafka-style listener security protocols, TLS files and
//! the enabled SASL mechanisms.
//!
//! The primary listener (`pg_kafka.host`:`pg_kafka.port`) speaks
//! `pg_kafka.security_protocol` (default `PLAINTEXT`). `pg_kafka.additional_listeners`
//! adds more, Kafka `listeners` syntax: `SASL_SSL://0.0.0.0:9093,SSL://:9094`. Every
//! listener runs at once, so a deployment can keep a loopback PLAINTEXT port for local
//! tools and expose only an authenticated one.
//!
//! All settings here are read once, when the background worker starts (a bad file or
//! spec stops the worker with a clear error, which the postmaster retries every 5 s).
//! They are `SIGHUP` GUCs so `ALTER SYSTEM` + `pg_reload_conf()` can change them, but
//! the change takes effect only when the worker restarts (`pg_terminate_backend()` on
//! the `pg_kafka_listener` backend, or a server restart); the worker logs a reminder
//! when it sees a changed value.

use super::scram::ScramMechanism;
use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting};
use std::ffi::CString;

/// Kafka's listener security protocols.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SecurityProtocol {
    Plaintext,
    Ssl,
    SaslPlaintext,
    SaslSsl,
}

impl SecurityProtocol {
    pub fn parse(s: &str) -> Result<Self, String> {
        match s.trim().to_ascii_uppercase().as_str() {
            "PLAINTEXT" => Ok(SecurityProtocol::Plaintext),
            "SSL" => Ok(SecurityProtocol::Ssl),
            "SASL_PLAINTEXT" => Ok(SecurityProtocol::SaslPlaintext),
            "SASL_SSL" => Ok(SecurityProtocol::SaslSsl),
            other => Err(format!(
                "unknown security protocol '{other}' (expected PLAINTEXT, SSL, SASL_PLAINTEXT or SASL_SSL)"
            )),
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            SecurityProtocol::Plaintext => "PLAINTEXT",
            SecurityProtocol::Ssl => "SSL",
            SecurityProtocol::SaslPlaintext => "SASL_PLAINTEXT",
            SecurityProtocol::SaslSsl => "SASL_SSL",
        }
    }

    pub fn uses_tls(self) -> bool {
        matches!(self, SecurityProtocol::Ssl | SecurityProtocol::SaslSsl)
    }

    pub fn uses_sasl(self) -> bool {
        matches!(
            self,
            SecurityProtocol::SaslPlaintext | SecurityProtocol::SaslSsl
        )
    }
}

/// A SASL mechanism the broker can offer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SaslMechanism {
    Plain,
    Scram(ScramMechanism),
}

impl SaslMechanism {
    pub fn parse(s: &str) -> Result<Self, String> {
        let t = s.trim();
        if t.eq_ignore_ascii_case("PLAIN") {
            return Ok(SaslMechanism::Plain);
        }
        ScramMechanism::from_name(t)
            .map(SaslMechanism::Scram)
            .ok_or_else(|| {
                format!(
                    "unsupported SASL mechanism '{t}' (expected PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512)"
                )
            })
    }

    pub fn name(self) -> &'static str {
        match self {
            SaslMechanism::Plain => "PLAIN",
            SaslMechanism::Scram(m) => m.name(),
        }
    }
}

/// Parse `pg_kafka.sasl_enabled_mechanisms` (comma-separated, duplicates removed).
pub fn parse_mechanisms(s: &str) -> Result<Vec<SaslMechanism>, String> {
    let mut out = Vec::new();
    for part in s.split(',').filter(|p| !p.trim().is_empty()) {
        let m = SaslMechanism::parse(part)?;
        if !out.contains(&m) {
            out.push(m);
        }
    }
    Ok(out)
}

/// One listener: where it binds, what it speaks, and what it advertises in
/// Metadata/FindCoordinator (Kafka's advertised.listeners, per listener).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListenerSpec {
    pub protocol: SecurityProtocol,
    pub host: String,
    pub port: i32,
    /// `None` for the primary listener (the worker's BrokerMetadata already advertises
    /// it); `Some((host, port))` for an additional listener.
    pub advertised: Option<(String, i32)>,
}

impl ListenerSpec {
    pub fn bind_addr(&self) -> String {
        if self.host.contains(':') && !self.host.starts_with('[') {
            format!("[{}]:{}", self.host, self.port)
        } else {
            format!("{}:{}", self.host, self.port)
        }
    }
}

/// The host clients are told to connect to for a bind host (same rule as the primary
/// listener: an unspecified address is not routable, so advertise `localhost`).
pub fn advertised_host_for(bind_host: &str) -> String {
    let h = bind_host.trim_start_matches('[').trim_end_matches(']');
    if h.is_empty() || h == "0.0.0.0" || h == "::" {
        "localhost".to_string()
    } else {
        h.to_string()
    }
}

/// Parse `pg_kafka.additional_listeners`: comma-separated `PROTOCOL://host:port`
/// (`host` may be empty for `pg_kafka.host`, or a bracketed IPv6 literal).
pub fn parse_listeners(s: &str, default_host: &str) -> Result<Vec<ListenerSpec>, String> {
    let mut out = Vec::new();
    for raw in s.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        let (proto, addr) = raw
            .split_once("://")
            .ok_or_else(|| format!("listener '{raw}' must look like PROTOCOL://host:port"))?;
        let protocol = SecurityProtocol::parse(proto)?;
        let (host, port) = addr
            .rsplit_once(':')
            .ok_or_else(|| format!("listener '{raw}' has no port"))?;
        let port: i32 = port
            .parse()
            .map_err(|_| format!("listener '{raw}' has an invalid port"))?;
        if !(1024..=65535).contains(&port) {
            return Err(format!(
                "listener '{raw}': port must be between 1024 and 65535"
            ));
        }
        let host = host.trim_start_matches('[').trim_end_matches(']');
        let host = if host.is_empty() { default_host } else { host }.to_string();
        out.push(ListenerSpec {
            protocol,
            advertised: Some((advertised_host_for(&host), port)),
            host,
            port,
        });
    }
    Ok(out)
}

/// Everything the network thread needs about listener security, validated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecuritySettings {
    /// Primary listener first, then the additional ones.
    pub listeners: Vec<ListenerSpec>,
    pub ssl_cert_file: String,
    pub ssl_key_file: String,
    /// Client CA bundle; when set, TLS listeners require a client certificate signed
    /// by it (mutual TLS).
    pub ssl_ca_file: String,
    pub sasl_mechanisms: Vec<SaslMechanism>,
    /// wave3 net: connection limits, idle timeout and request read deadline.
    pub limits: crate::kafka::conn_limits::NetworkLimits,
}

/// The raw GUC values, before validation.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RawSecuritySettings {
    pub security_protocol: String,
    pub additional_listeners: String,
    pub ssl_cert_file: String,
    pub ssl_key_file: String,
    pub ssl_ca_file: String,
    pub sasl_enabled_mechanisms: String,
    /// wave3 net: `pg_kafka.max_connections`, `pg_kafka.max_connections_per_ip`,
    /// `pg_kafka.connections_max_idle_ms`, `pg_kafka.request_read_timeout_ms`.
    pub limits: crate::kafka::conn_limits::NetworkLimits,
}

impl SecuritySettings {
    /// Validate the raw settings against the primary listener's host and port.
    pub fn resolve(
        raw: &RawSecuritySettings,
        primary_host: &str,
        primary_port: i32,
    ) -> Result<Self, String> {
        let primary = ListenerSpec {
            protocol: SecurityProtocol::parse(&raw.security_protocol)
                .map_err(|e| format!("pg_kafka.security_protocol: {e}"))?,
            host: primary_host.to_string(),
            port: primary_port,
            advertised: None,
        };
        let mut listeners = vec![primary];
        listeners.extend(
            parse_listeners(&raw.additional_listeners, primary_host)
                .map_err(|e| format!("pg_kafka.additional_listeners: {e}"))?,
        );
        for (i, l) in listeners.iter().enumerate() {
            if listeners[..i].iter().any(|o| o.port == l.port) {
                return Err(format!(
                    "pg_kafka.additional_listeners: port {} is used by more than one listener",
                    l.port
                ));
            }
        }
        let sasl_mechanisms = parse_mechanisms(&raw.sasl_enabled_mechanisms)
            .map_err(|e| format!("pg_kafka.sasl_enabled_mechanisms: {e}"))?;
        let settings = SecuritySettings {
            listeners,
            ssl_cert_file: raw.ssl_cert_file.trim().to_string(),
            ssl_key_file: raw.ssl_key_file.trim().to_string(),
            ssl_ca_file: raw.ssl_ca_file.trim().to_string(),
            sasl_mechanisms,
            limits: raw.limits,
        };
        if settings.needs_tls()
            && (settings.ssl_cert_file.is_empty() || settings.ssl_key_file.is_empty())
        {
            return Err(
                "an SSL or SASL_SSL listener is configured but pg_kafka.ssl_cert_file and \
                 pg_kafka.ssl_key_file are not both set"
                    .into(),
            );
        }
        if settings.needs_sasl() && settings.sasl_mechanisms.is_empty() {
            return Err(
                "a SASL listener is configured but pg_kafka.sasl_enabled_mechanisms is empty"
                    .into(),
            );
        }
        Ok(settings)
    }

    pub fn needs_tls(&self) -> bool {
        self.listeners.iter().any(|l| l.protocol.uses_tls())
    }

    pub fn needs_sasl(&self) -> bool {
        self.listeners.iter().any(|l| l.protocol.uses_sasl())
    }

    pub fn mutual_tls(&self) -> bool {
        !self.ssl_ca_file.is_empty()
    }

    /// Whether a listener authenticates its clients (SASL, or mutual TLS).
    pub fn authenticates(&self, listener: &ListenerSpec) -> bool {
        listener.protocol.uses_sasl() || (listener.protocol.uses_tls() && self.mutual_tls())
    }

    /// Operator warnings for this configuration (exposed listeners without client
    /// authentication, PLAIN passwords in clear text on an exposed listener).
    pub fn warnings(&self) -> Vec<String> {
        let mut out = Vec::new();
        for l in &self.listeners {
            let exposed = crate::config::bind_is_publicly_exposed(&l.host);
            if exposed && !self.authenticates(l) {
                out.push(format!(
                    "pg_kafka listener {}://{} is reachable from other hosts and does NOT \
                     authenticate clients: anyone who can reach it can read and write any \
                     topic. Use SASL_SSL / SASL_PLAINTEXT (or SSL with pg_kafka.ssl_ca_file), \
                     a firewall, or bind it to 127.0.0.1.",
                    l.protocol.name(),
                    l.bind_addr()
                ));
            }
            if exposed
                && l.protocol == SecurityProtocol::SaslPlaintext
                && self.sasl_mechanisms.contains(&SaslMechanism::Plain)
            {
                out.push(format!(
                    "pg_kafka listener SASL_PLAINTEXT://{} accepts SASL/PLAIN: passwords cross \
                     the network unencrypted. Prefer SASL_SSL or the SCRAM mechanisms.",
                    l.bind_addr()
                ));
            }
        }
        out
    }
}

// ===== GUCs =====

static SECURITY_PROTOCOL: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c"PLAINTEXT"));
static ADDITIONAL_LISTENERS: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
static SSL_CERT_FILE: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
static SSL_KEY_FILE: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
static SSL_CA_FILE: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
static SASL_ENABLED_MECHANISMS: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c"PLAIN,SCRAM-SHA-256,SCRAM-SHA-512"));

// wave3 net: connection-limit GUCs.
static MAX_CONNECTIONS: GucSetting<i32> =
    GucSetting::<i32>::new(crate::kafka::conn_limits::DEFAULT_MAX_CONNECTIONS);
static MAX_CONNECTIONS_PER_IP: GucSetting<i32> =
    GucSetting::<i32>::new(crate::kafka::conn_limits::DEFAULT_MAX_CONNECTIONS_PER_IP);
static CONNECTIONS_MAX_IDLE_MS: GucSetting<i32> =
    GucSetting::<i32>::new(crate::kafka::conn_limits::DEFAULT_CONNECTIONS_MAX_IDLE_MS);
static REQUEST_READ_TIMEOUT_MS: GucSetting<i32> =
    GucSetting::<i32>::new(crate::kafka::conn_limits::DEFAULT_REQUEST_READ_TIMEOUT_MS);

/// Register the listener-security GUCs. Called from `config::init()`.
pub fn register_gucs() {
    let define = |name: &'static std::ffi::CStr,
                  short: &'static std::ffi::CStr,
                  long: &'static std::ffi::CStr,
                  setting: &'static GucSetting<Option<CString>>| {
        GucRegistry::define_string_guc(
            name,
            short,
            long,
            setting,
            GucContext::Sighup,
            GucFlags::default(),
        );
    };
    define(
        c"pg_kafka.security_protocol",
        c"Security protocol of the primary listener (pg_kafka.host:pg_kafka.port)",
        c"PLAINTEXT (default), SSL, SASL_PLAINTEXT or SASL_SSL. Read when the worker starts.",
        &SECURITY_PROTOCOL,
    );
    define(
        c"pg_kafka.additional_listeners",
        c"Extra Kafka listeners, e.g. 'SASL_SSL://0.0.0.0:9093,SSL://:9094'",
        c"Comma-separated PROTOCOL://host:port. Each advertises its own host:port in Metadata. Empty (default) = none. Read when the worker starts.",
        &ADDITIONAL_LISTENERS,
    );
    define(
        c"pg_kafka.ssl_cert_file",
        c"PEM certificate chain for SSL / SASL_SSL listeners",
        c"Read when the worker starts.",
        &SSL_CERT_FILE,
    );
    define(
        c"pg_kafka.ssl_key_file",
        c"PEM private key for SSL / SASL_SSL listeners",
        c"Read when the worker starts.",
        &SSL_KEY_FILE,
    );
    define(
        c"pg_kafka.ssl_ca_file",
        c"PEM CA bundle for client certificates; when set, TLS listeners require a client certificate (mutual TLS)",
        c"Empty (default) = no client certificates. Read when the worker starts.",
        &SSL_CA_FILE,
    );
    define(
        c"pg_kafka.sasl_enabled_mechanisms",
        c"SASL mechanisms offered on SASL listeners",
        c"Comma-separated subset of PLAIN, SCRAM-SHA-256, SCRAM-SHA-512 (default: all three). Read when the worker starts.",
        &SASL_ENABLED_MECHANISMS,
    );

    // wave3 net: connection limits (read when the worker starts, like the listener GUCs).
    let define_int = |name: &'static std::ffi::CStr,
                      short: &'static std::ffi::CStr,
                      long: &'static std::ffi::CStr,
                      setting: &'static GucSetting<i32>,
                      min: i32,
                      max: i32,
                      flags: GucFlags| {
        GucRegistry::define_int_guc(
            name,
            short,
            long,
            setting,
            min,
            max,
            GucContext::Sighup,
            flags,
        );
    };
    define_int(
        c"pg_kafka.max_connections",
        c"Maximum concurrent Kafka client connections per listener",
        c"Each listener has its own cap, so a flood on one listener cannot lock clients out of another (e.g. a loopback admin listener). Connections over the cap are closed at accept. Read when the worker starts.",
        &MAX_CONNECTIONS,
        1,
        crate::kafka::conn_limits::MAX_CONNECTIONS_LIMIT,
        GucFlags::default(),
    );
    define_int(
        c"pg_kafka.max_connections_per_ip",
        c"Maximum concurrent Kafka client connections from one IP address, across all listeners",
        c"Kafka's max.connections.per.ip. Connections over the cap are closed at accept. Read when the worker starts.",
        &MAX_CONNECTIONS_PER_IP,
        1,
        crate::kafka::conn_limits::MAX_CONNECTIONS_LIMIT,
        GucFlags::default(),
    );
    define_int(
        c"pg_kafka.connections_max_idle_ms",
        c"Close Kafka client connections idle for this long",
        c"Kafka's connections.max.idle.ms: a connection with no request read, no response written and nothing outstanding for this long is closed. Read when the worker starts.",
        &CONNECTIONS_MAX_IDLE_MS,
        100,
        i32::MAX,
        GucFlags::UNIT_MS,
    );
    define_int(
        c"pg_kafka.request_read_timeout_ms",
        c"Deadline for receiving one request frame, from its first byte",
        c"A client that starts a request and does not finish sending it within this time is disconnected (slow-loris protection). Read when the worker starts.",
        &REQUEST_READ_TIMEOUT_MS,
        100,
        3_600_000,
        GucFlags::UNIT_MS,
    );
}

fn guc_string(setting: &GucSetting<Option<CString>>) -> String {
    setting
        .get()
        .map_or_default(|c| c.to_string_lossy().into_owned())
}

/// Read the current GUC values.
pub fn raw_from_gucs() -> RawSecuritySettings {
    RawSecuritySettings {
        security_protocol: guc_string(&SECURITY_PROTOCOL),
        additional_listeners: guc_string(&ADDITIONAL_LISTENERS),
        ssl_cert_file: guc_string(&SSL_CERT_FILE),
        ssl_key_file: guc_string(&SSL_KEY_FILE),
        ssl_ca_file: guc_string(&SSL_CA_FILE),
        sasl_enabled_mechanisms: guc_string(&SASL_ENABLED_MECHANISMS),
        limits: crate::kafka::conn_limits::NetworkLimits::from_raw(
            MAX_CONNECTIONS.get(),
            MAX_CONNECTIONS_PER_IP.get(),
            CONNECTIONS_MAX_IDLE_MS.get(),
            REQUEST_READ_TIMEOUT_MS.get(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw(protocol: &str, extra: &str) -> RawSecuritySettings {
        RawSecuritySettings {
            security_protocol: protocol.into(),
            additional_listeners: extra.into(),
            sasl_enabled_mechanisms: "PLAIN,SCRAM-SHA-256,SCRAM-SHA-512".into(),
            ..Default::default()
        }
    }

    #[test]
    fn default_is_one_plaintext_listener() {
        let s = SecuritySettings::resolve(&raw("PLAINTEXT", ""), "127.0.0.1", 9092).unwrap();
        assert_eq!(s.listeners.len(), 1);
        assert_eq!(s.listeners[0].protocol, SecurityProtocol::Plaintext);
        assert_eq!(s.listeners[0].advertised, None);
        assert!(!s.needs_tls() && !s.needs_sasl());
        assert_eq!(s.warnings(), Vec::<String>::new());
    }

    #[test]
    fn parses_kafka_style_listeners() {
        let l = parse_listeners(
            " SASL_PLAINTEXT://127.0.0.1:9093 , ssl://:9094,SASL_SSL://[::1]:9095,SSL://0.0.0.0:9096",
            "10.0.0.1",
        )
        .unwrap();
        assert_eq!(l.len(), 4);
        assert_eq!(l[0].protocol, SecurityProtocol::SaslPlaintext);
        assert_eq!(l[0].advertised, Some(("127.0.0.1".into(), 9093)));
        assert_eq!(l[1].host, "10.0.0.1");
        assert_eq!(l[2].host, "::1");
        assert_eq!(l[2].bind_addr(), "[::1]:9095");
        assert_eq!(l[3].advertised, Some(("localhost".into(), 9096)));
        assert_eq!(parse_listeners("", "h").unwrap(), Vec::new());
    }

    #[test]
    fn rejects_bad_listener_specs() {
        for bad in [
            "127.0.0.1:9093",
            "TLS://:9093",
            "SSL://host",
            "SSL://:abc",
            "SSL://:80",
            "SSL://:70000",
        ] {
            assert!(parse_listeners(bad, "h").is_err(), "{bad}");
        }
        // Port reuse across listeners.
        let err = SecuritySettings::resolve(
            &raw("PLAINTEXT", "SASL_PLAINTEXT://:9092"),
            "127.0.0.1",
            9092,
        )
        .unwrap_err();
        assert!(err.contains("more than one listener"), "{err}");
        assert!(SecuritySettings::resolve(&raw("KERBEROS", ""), "h", 9092).is_err());
    }

    #[test]
    fn tls_listener_needs_cert_and_key() {
        let mut r = raw("PLAINTEXT", "SSL://:9094");
        assert!(SecuritySettings::resolve(&r, "127.0.0.1", 9092)
            .unwrap_err()
            .contains("ssl_cert_file"));
        r.ssl_cert_file = "/c.pem".into();
        r.ssl_key_file = "/k.pem".into();
        let s = SecuritySettings::resolve(&r, "127.0.0.1", 9092).unwrap();
        assert!(s.needs_tls() && !s.mutual_tls());
    }

    #[test]
    fn sasl_listener_needs_a_mechanism() {
        let mut r = raw("SASL_PLAINTEXT", "");
        r.sasl_enabled_mechanisms = " ".into();
        assert!(SecuritySettings::resolve(&r, "127.0.0.1", 9092).is_err());
        r.sasl_enabled_mechanisms = "scram-sha-512, SCRAM-SHA-512".into();
        let s = SecuritySettings::resolve(&r, "127.0.0.1", 9092).unwrap();
        assert_eq!(
            s.sasl_mechanisms,
            vec![SaslMechanism::Scram(ScramMechanism::Sha512)]
        );
        r.sasl_enabled_mechanisms = "GSSAPI".into();
        assert!(SecuritySettings::resolve(&r, "127.0.0.1", 9092).is_err());
    }

    #[test]
    fn warnings_flag_exposed_unauthenticated_listeners() {
        let mut r = raw(
            "PLAINTEXT",
            "SASL_PLAINTEXT://0.0.0.0:9093,SSL://0.0.0.0:9094",
        );
        r.ssl_cert_file = "c".into();
        r.ssl_key_file = "k".into();
        let s = SecuritySettings::resolve(&r, "0.0.0.0", 9092).unwrap();
        let w = s.warnings();
        // PLAINTEXT and SSL (no client CA) are unauthenticated; SASL_PLAINTEXT offers PLAIN.
        assert_eq!(w.len(), 3, "{w:?}");
        r.ssl_ca_file = "ca".into();
        r.sasl_enabled_mechanisms = "SCRAM-SHA-256".into();
        let s = SecuritySettings::resolve(&r, "0.0.0.0", 9092).unwrap();
        assert_eq!(s.warnings().len(), 1, "only the PLAINTEXT listener is left");
        // Loopback binds never warn.
        let s = SecuritySettings::resolve(&raw("PLAINTEXT", ""), "127.0.0.1", 9092).unwrap();
        assert_eq!(s.warnings(), Vec::<String>::new());
    }
}
