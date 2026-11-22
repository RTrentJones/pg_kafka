//! Listener security: TLS, SASL (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512) and the
//! per-connection principal.
//!
//! * [`settings`] - Kafka-style listener security protocols (`PLAINTEXT`, `SSL`,
//!   `SASL_PLAINTEXT`, `SASL_SSL`), additional listeners, TLS files, enabled
//!   mechanisms; the GUCs.
//! * [`tls`] - rustls server configuration and the client-certificate principal.
//! * [`session`] - the per-connection SASL state machine (network thread).
//! * [`scram`] / [`plain`] - the mechanisms and the stored credential format.
//! * [`credentials`] - the DB-thread credential lookup; [`users`] - the SQL functions
//!   that manage Kafka users.
//!
//! Scope: authentication only. An authenticated principal may do everything; there are
//! no ACLs yet. The principal is recorded on the connection
//! ([`ConnectionPrincipal`]) and logged, so authorization can be added later.

pub mod credentials;
pub mod plain;
pub mod scram;
pub mod session;
pub mod settings;
pub mod tls;
pub mod users;

pub use session::{ChannelLookup, CredentialRequest, SaslSession, ANONYMOUS_PRINCIPAL};
pub use settings::{SaslMechanism, SecurityProtocol, SecuritySettings};

/// Who a connection is, as far as the broker knows. Kafka principal syntax
/// (`User:<name>`): the SASL username after SASL authentication, the certificate
/// subject for mutual TLS without SASL, otherwise `User:ANONYMOUS`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionPrincipal {
    pub principal: String,
    /// How it was established: `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, `SSL`, or
    /// `NONE`.
    pub method: &'static str,
}

impl ConnectionPrincipal {
    pub fn anonymous() -> Self {
        ConnectionPrincipal {
            principal: ANONYMOUS_PRINCIPAL.to_string(),
            method: "NONE",
        }
    }
}

impl std::fmt::Display for ConnectionPrincipal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({})", self.principal, self.method)
    }
}
