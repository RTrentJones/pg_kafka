//! Worker-side credential lookup (`kafka.sasl_credentials`, SELECT only).
//!
//! The network thread cannot use SPI, so a SASL exchange sends a
//! `session::CredentialRequest` over the worker's credential channel; the DB thread
//! answers it with [`lookup_credential`]. `pgkafka_worker` holds only SELECT on the table.
//!
//! For a user that does not exist the DB thread builds the fake credential
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
/// [`StoredCredential::Unknown`] with a fake credential, and so does a user whose stored
/// credential is unusable (logged as a WARNING): its authentication then fails like a
/// wrong password, which says nothing a client can retry its way past. `Err` only when
/// the lookup itself failed.
pub fn lookup_credential(
    username: &str,
    mechanism: Option<ScramMechanism>,
) -> Result<StoredCredential, String> {
    let stored = lookup_stored(username, mechanism)?.map(StoredRow::parse);
    let (answer, unusable) = answer_lookup(stored, || {
        let m = mechanism.unwrap_or(ScramMechanism::Sha256);
        fake_credential_with(m, username, &fake_params(m))
    });
    if let Some(e) = unusable {
        crate::pg_warning!(
            "pg_kafka: the stored SASL credential of user \"{}\" is invalid ({}); its \
             authentication fails until the credential is set again \
             (pg_kafka_alter_user or pg_kafka_set_user_verifier)",
            username,
            e
        );
    }
    Ok(answer)
}

/// The answer to a lookup that found `stored` (`None`: no such user; `Err`: a row that
/// is not a usable credential), and the reason a found row was unusable. Every user
/// without a usable credential is answered with a fake one (`fake`), so its exchange
/// runs to the end and fails like a wrong password.
fn answer_lookup(
    stored: Option<Result<ScramCredential, String>>,
    fake: impl FnOnce() -> ScramCredential,
) -> (StoredCredential, Option<String>) {
    match stored {
        Some(Ok(cred)) => (StoredCredential::Known(cred), None),
        Some(Err(e)) => (StoredCredential::Unknown(fake()), Some(e)),
        None => (StoredCredential::Unknown(fake()), None),
    }
}

/// One `kafka.sasl_credentials` row as read.
#[derive(Debug, Default)]
struct StoredRow {
    mechanism: String,
    iterations: i32,
    salt: Vec<u8>,
    stored_key: Vec<u8>,
    server_key: Vec<u8>,
}

impl StoredRow {
    /// The credential the row holds, or why it is not a usable one.
    fn parse(self) -> Result<ScramCredential, String> {
        let cred = ScramCredential {
            mechanism: ScramMechanism::from_name(&self.mechanism)
                .ok_or_else(|| format!("unknown mechanism {}", self.mechanism))?,
            salt: self.salt,
            iterations: u32::try_from(self.iterations).unwrap_or(0),
            stored_key: self.stored_key,
            server_key: self.server_key,
        };
        cred.validate()?;
        Ok(cred)
    }
}

fn lookup_stored(
    username: &str,
    mechanism: Option<ScramMechanism>,
) -> Result<Option<StoredRow>, String> {
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
        Ok::<_, spi::SpiError>(Some(StoredRow {
            mechanism: row.get_by_name("mechanism")?.unwrap_or_default(),
            iterations: row.get_by_name("iterations")?.unwrap_or(0),
            salt: row.get_by_name("salt")?.unwrap_or_default(),
            stored_key: row.get_by_name("stored_key")?.unwrap_or_default(),
            server_key: row.get_by_name("server_key")?.unwrap_or_default(),
        }))
    })
    .map_err(|e| format!("credential lookup failed: {e}"))
}

/// The fake-credential parameters for `mechanism`, cached for [`FAKE_PARAMS_TTL`].
fn fake_params(mechanism: ScramMechanism) -> FakeCredentialParams {
    let now = Instant::now();
    let mut cache = FAKE_PARAMS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let cache = cache.get_or_insert_with(HashMap::new);
    if let Some((at, params)) = cache.get(mechanism.name()) {
        if now.duration_since(*at) < FAKE_PARAMS_TTL {
            return params.clone();
        }
    }
    let params = read_fake_params(mechanism).unwrap_or_else(|e| {
        crate::pg_debug!(
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
        // The secret table is part of the 0.1.0 schema; a 0.0.0 schema without it keeps the
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
                params.iterations = u32::try_from(i).unwrap_or(0);
            }
            if let Some(l) = row.get_by_name::<i32, _>("salt_len")? {
                params.salt_len = usize::try_from(l).unwrap_or(0).max(1);
            }
        }
        Ok::<_, spi::SpiError>(params)
    })
    .map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::auth::scram::fake_credential;

    fn row() -> StoredRow {
        let cred = fake_credential(ScramMechanism::Sha256, "alice");
        StoredRow {
            mechanism: "SCRAM-SHA-256".into(),
            iterations: i32::try_from(cred.iterations).unwrap(),
            salt: cred.salt,
            stored_key: cred.stored_key,
            server_key: cred.server_key,
        }
    }

    fn fake() -> ScramCredential {
        fake_credential(ScramMechanism::Sha256, "alice")
    }

    #[test]
    fn a_valid_row_is_the_known_credential() {
        let (answer, unusable) = answer_lookup(Some(row().parse()), fake);
        assert!(matches!(answer, StoredCredential::Known(_)));
        assert_eq!(unusable, None);
    }

    #[test]
    fn no_row_is_an_unknown_user() {
        let (answer, unusable) = answer_lookup(None, fake);
        assert!(matches!(answer, StoredCredential::Unknown(_)));
        assert_eq!(unusable, None);
    }

    #[test]
    fn an_invalid_row_is_answered_like_an_unknown_user() {
        // A corrupt row is permanent: answering it as a failed lookup (the connection
        // closes, the client reconnects) would retry forever and tell the user apart
        // from an unknown one. It fails the authentication instead.
        let corrupt = [
            StoredRow {
                stored_key: vec![0],
                ..row()
            },
            StoredRow {
                iterations: 1,
                ..row()
            },
            StoredRow {
                salt: Vec::new(),
                ..row()
            },
            StoredRow {
                mechanism: "SCRAM-SHA-1".into(),
                ..row()
            },
        ];
        for bad in corrupt {
            let (answer, unusable) = answer_lookup(Some(bad.parse()), fake);
            assert!(matches!(answer, StoredCredential::Unknown(_)));
            assert!(unusable.is_some());
        }
    }
}
