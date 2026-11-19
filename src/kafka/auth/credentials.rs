//! Worker-side credential lookup (`kafka.sasl_credentials`, SELECT only).
//!
//! The network thread cannot use SPI, so a SASL exchange sends a
//! `session::CredentialRequest` over the worker's credential channel; the DB thread
//! answers it with [`lookup_credential`]. `pgkafka_worker` holds only SELECT on the table.
//!
//! wave3 net: for a user that does not exist the DB thread builds the fake credential
//! itself, from the durable secret in `kafka.sasl_server_secret` and the iteration count
//! and salt length most common among the stored credentials of the mechanism (cached
//! for [`FAKE_PARAMS_TTL`], so an unknown user normally costs the same one SELECT as a
//! known one).

use super::scram::{fake_credential_with, FakeCredentialParams, ScramCredential, ScramMechanism};
use super::session::StoredCredential;
use pgrx::prelude::*;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// How long the imitated fake-credential parameters are cached.
pub const FAKE_PARAMS_TTL: Duration = Duration::from_secs(60);

type ParamsCache = HashMap<&'static str, (Instant, FakeCredentialParams)>;

/// DB thread only (the lock is never contended).
static FAKE_PARAMS: Mutex<Option<ParamsCache>> = Mutex::new(None);

/// Look up a stored credential (DB thread only). `mechanism = None` (PLAIN) takes any
/// stored mechanism, preferring SCRAM-SHA-256. An unknown user gets
/// [`StoredCredential::Unknown`] with a fake credential.
pub fn lookup_credential(
    username: &str,
    mechanism: Option<ScramMechanism>,
) -> Result<StoredCredential, String> {
    match lookup_stored(username, mechanism)? {
        Some(cred) => Ok(StoredCredential::Known(cred)),
        None => {
            let m = mechanism.unwrap_or(ScramMechanism::Sha256);
            let params = fake_params(m);
            Ok(StoredCredential::Unknown(fake_credential_with(
                m, username, &params,
            )))
        }
    }
}

fn lookup_stored(
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

/// The fake-credential parameters for `mechanism`, cached for [`FAKE_PARAMS_TTL`].
fn fake_params(mechanism: ScramMechanism) -> FakeCredentialParams {
    let now = Instant::now();
    let mut cache = FAKE_PARAMS.lock().unwrap_or_else(|p| p.into_inner());
    let cache = cache.get_or_insert_with(HashMap::new);
    if let Some((at, params)) = cache.get(mechanism.name()) {
        if now.duration_since(*at) < FAKE_PARAMS_TTL {
            return params.clone();
        }
    }
    let params = read_fake_params(mechanism).unwrap_or_else(|e| {
        pgrx::debug1!(
            "pg_kafka: fake SASL credential parameters unavailable: {}",
            e
        );
        FakeCredentialParams::process_default()
    });
    cache.insert(mechanism.name(), (now, params.clone()));
    params
}

/// Read the durable secret and the most common (iterations, salt length) of the stored
/// credentials of `mechanism`. Defaults where there is nothing to imitate.
fn read_fake_params(mechanism: ScramMechanism) -> Result<FakeCredentialParams, String> {
    Spi::connect(|client| {
        let mut params = FakeCredentialParams::process_default();
        // The secret table is new in 0.1.0 (wave 3); a schema without it keeps the
        // per-process secret instead of failing the lookup.
        let has_secret = client
            .select(
                "SELECT to_regclass('kafka.sasl_server_secret') IS NOT NULL AS present",
                Some(1),
                &[],
            )?
            .next()
            .and_then(|r| r.get_by_name::<bool, _>("present").ok().flatten())
            .unwrap_or(false);
        if has_secret {
            if let Some(row) = client
                .select("SELECT secret FROM kafka.sasl_server_secret", Some(1), &[])?
                .next()
            {
                if let Some(secret) = row.get_by_name::<Vec<u8>, _>("secret")? {
                    if secret.len() >= 32 {
                        params.secret = secret;
                    }
                }
            }
        }
        if let Some(row) = client
            .select(
                "SELECT iterations, octet_length(salt) AS salt_len \
                 FROM kafka.sasl_credentials WHERE mechanism = $1 \
                 GROUP BY 1, 2 ORDER BY count(*) DESC, 1, 2 LIMIT 1",
                Some(1),
                &[mechanism.name().into()],
            )?
            .next()
        {
            if let Some(i) = row.get_by_name::<i32, _>("iterations")? {
                params.iterations = i.max(0) as u32;
            }
            if let Some(l) = row.get_by_name::<i32, _>("salt_len")? {
                params.salt_len = l.max(1) as usize;
            }
        }
        Ok::<_, pgrx::spi::SpiError>(params)
    })
    .map_err(|e| e.to_string())
}
