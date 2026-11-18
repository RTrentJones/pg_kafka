//! Worker-side credential lookup (`kafka.sasl_credentials`, SELECT only).
//!
//! The network thread cannot use SPI, so a SASL exchange sends a
//! `session::CredentialRequest` over the worker's credential channel; the DB thread
//! answers it with [`lookup_credential`]. `pgkafka_worker` holds only SELECT on the table.

use super::scram::{ScramCredential, ScramMechanism};
use pgrx::prelude::*;

/// Look up a stored credential (DB thread only). `mechanism = None` (PLAIN) takes any
/// stored mechanism, preferring SCRAM-SHA-256.
pub fn lookup_credential(
    username: &str,
    mechanism: Option<ScramMechanism>,
) -> Result<Option<ScramCredential>, String> {
    Spi::connect(|client| {
        let rows = match mechanism {
            Some(m) => client.select(
                "SELECT mechanism, salt, iterations, stored_key, server_key \
                 FROM kafka.sasl_credentials WHERE username = $1 AND mechanism = $2",
                Some(1),
                &[username.into(), m.name().into()],
            )?,
            None => client.select(
                "SELECT mechanism, salt, iterations, stored_key, server_key \
                 FROM kafka.sasl_credentials WHERE username = $1 ORDER BY mechanism LIMIT 1",
                Some(1),
                &[username.into()],
            )?,
        };
        let Some(row) = rows.into_iter().next() else {
            return Ok(None);
        };
        let mechanism: String = row.get_by_name("mechanism")?.unwrap_or_default();
        let iterations: i32 = row.get_by_name("iterations")?.unwrap_or(0);
        let salt: Vec<u8> = row.get_by_name("salt")?.unwrap_or_default();
        let stored_key: Vec<u8> = row.get_by_name("stored_key")?.unwrap_or_default();
        let server_key: Vec<u8> = row.get_by_name("server_key")?.unwrap_or_default();
        Ok::<_, pgrx::spi::SpiError>(Some((mechanism, iterations, salt, stored_key, server_key)))
    })
    .map_err(|e| format!("credential lookup failed: {}", e))
    .and_then(|row| {
        let Some((mechanism, iterations, salt, stored_key, server_key)) = row else {
            return Ok(None);
        };
        let cred = ScramCredential {
            mechanism: ScramMechanism::from_name(&mechanism)
                .ok_or_else(|| format!("stored credential has unknown mechanism {}", mechanism))?,
            salt,
            iterations: iterations.max(0) as u32,
            stored_key,
            server_key,
        };
        cred.validate()
            .map_err(|e| format!("stored credential is invalid: {}", e))?;
        Ok(Some(cred))
    })
}
