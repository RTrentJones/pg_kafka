//! SCRAM (RFC 5802, RFC 7677) server side and the stored credential format.
//!
//! Credentials are stored the way Kafka stores them (`ScramCredential`): a random
//! salt, an iteration count, `StoredKey = H(HMAC(SaltedPassword, "Client Key"))` and
//! `ServerKey = HMAC(SaltedPassword, "Server Key")`, with
//! `SaltedPassword = PBKDF2-HMAC(password, salt, iterations)`. The plaintext password is
//! never stored; `StoredKey` lets the server check a client proof (SCRAM) or a password
//! (PLAIN) without being able to log in as the user.
//!
//! As in Kafka (and librdkafka), the password is used as its raw UTF-8 bytes, without
//! SASLprep normalization.
//!
//! The server state machine is sans-IO: [`parse_client_first`] parses the
//! `client-first-message`, [`ScramServerState::new`] produces the `server-first-message`,
//! and [`ScramServerState::verify_client_final`] checks the client proof in constant time
//! and returns the `server-final-message`.

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use ring::{digest, hmac, pbkdf2};
use std::num::NonZeroU32;

/// Minimum iteration count accepted for a stored credential (Kafka's minimum).
pub const MIN_ITERATIONS: u32 = 4096;
/// Maximum iteration count accepted for a stored credential (Kafka's maximum). It also
/// bounds the PBKDF2 work a PLAIN authentication attempt can cost the broker.
pub const MAX_ITERATIONS: u32 = 16384;
/// Iteration count used when none is given.
pub const DEFAULT_ITERATIONS: u32 = 4096;
/// Length of generated salts, in bytes.
pub const SALT_LEN: usize = 32;
/// Upper bound on a SCRAM client message; real ones are well under 300 bytes.
pub const MAX_SCRAM_MESSAGE_LEN: usize = 4096;

/// The hash a SCRAM mechanism is built on.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ScramMechanism {
    Sha256,
    Sha512,
    /// SCRAM-SHA-1, only so the RFC 5802 test vector can exercise the state machine.
    #[cfg(test)]
    Sha1,
}

impl ScramMechanism {
    /// Every mechanism a credential can be stored for, in lookup-preference order.
    pub const STORED: [ScramMechanism; 2] = [ScramMechanism::Sha256, ScramMechanism::Sha512];

    /// The IANA SASL mechanism name.
    pub fn name(self) -> &'static str {
        match self {
            ScramMechanism::Sha256 => "SCRAM-SHA-256",
            ScramMechanism::Sha512 => "SCRAM-SHA-512",
            #[cfg(test)]
            ScramMechanism::Sha1 => "SCRAM-SHA-1",
        }
    }

    /// Parse a mechanism name (case-insensitive).
    pub fn from_name(name: &str) -> Option<Self> {
        match name.trim().to_ascii_uppercase().as_str() {
            "SCRAM-SHA-256" => Some(ScramMechanism::Sha256),
            "SCRAM-SHA-512" => Some(ScramMechanism::Sha512),
            _ => None,
        }
    }

    fn hmac_algorithm(self) -> hmac::Algorithm {
        match self {
            ScramMechanism::Sha256 => hmac::HMAC_SHA256,
            ScramMechanism::Sha512 => hmac::HMAC_SHA512,
            #[cfg(test)]
            ScramMechanism::Sha1 => hmac::HMAC_SHA1_FOR_LEGACY_USE_ONLY,
        }
    }

    fn digest_algorithm(self) -> &'static digest::Algorithm {
        match self {
            ScramMechanism::Sha256 => &digest::SHA256,
            ScramMechanism::Sha512 => &digest::SHA512,
            #[cfg(test)]
            ScramMechanism::Sha1 => &digest::SHA1_FOR_LEGACY_USE_ONLY,
        }
    }

    fn pbkdf2_algorithm(self) -> pbkdf2::Algorithm {
        match self {
            ScramMechanism::Sha256 => pbkdf2::PBKDF2_HMAC_SHA256,
            ScramMechanism::Sha512 => pbkdf2::PBKDF2_HMAC_SHA512,
            #[cfg(test)]
            ScramMechanism::Sha1 => pbkdf2::PBKDF2_HMAC_SHA1,
        }
    }

    /// Output length of the hash (and of every derived key), in bytes.
    pub fn key_len(self) -> usize {
        self.digest_algorithm().output_len()
    }
}

/// Why a SCRAM or PLAIN exchange failed. The variants are for logs and tests; the client
/// is only ever told "authentication failed" (see `session.rs`), so a failure does not
/// reveal whether the user exists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScramError {
    /// The message is not valid SCRAM syntax.
    Malformed(&'static str),
    /// A feature this server does not implement (channel binding, extensions).
    Unsupported(&'static str),
    /// The proof (or password) did not match, or the user does not exist.
    InvalidCredentials,
}

impl std::fmt::Display for ScramError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScramError::Malformed(m) => write!(f, "malformed SCRAM message: {}", m),
            ScramError::Unsupported(m) => write!(f, "unsupported SCRAM feature: {}", m),
            ScramError::InvalidCredentials => write!(f, "invalid credentials"),
        }
    }
}

/// Constant-time byte comparison: the running time depends only on the lengths, which
/// are public (fixed per mechanism), never on where the inputs differ.
pub fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b) {
        diff |= x ^ y;
    }
    std::hint::black_box(diff) == 0
}

fn hmac_bytes(mechanism: ScramMechanism, key: &[u8], message: &[u8]) -> Vec<u8> {
    let key = hmac::Key::new(mechanism.hmac_algorithm(), key);
    hmac::sign(&key, message).as_ref().to_vec()
}

fn hash_bytes(mechanism: ScramMechanism, data: &[u8]) -> Vec<u8> {
    digest::digest(mechanism.digest_algorithm(), data)
        .as_ref()
        .to_vec()
}

/// `SaltedPassword := Hi(password, salt, i)` (PBKDF2 with one output block).
pub fn salted_password(
    mechanism: ScramMechanism,
    password: &[u8],
    salt: &[u8],
    iterations: u32,
) -> Vec<u8> {
    let iterations = NonZeroU32::new(iterations.max(1)).expect("max(1) is non-zero");
    let mut out = vec![0u8; mechanism.key_len()];
    pbkdf2::derive(
        mechanism.pbkdf2_algorithm(),
        iterations,
        salt,
        password,
        &mut out,
    );
    out
}

/// Fill a buffer from the operating system CSPRNG.
pub fn random_bytes(len: usize) -> Vec<u8> {
    use ring::rand::SecureRandom;
    let mut out = vec![0u8; len];
    ring::rand::SystemRandom::new()
        .fill(&mut out)
        .expect("the system CSPRNG is available");
    out
}

/// A stored SCRAM credential (one per user and mechanism). Never holds the password.
#[derive(Clone, PartialEq, Eq)]
pub struct ScramCredential {
    pub mechanism: ScramMechanism,
    pub salt: Vec<u8>,
    pub iterations: u32,
    pub stored_key: Vec<u8>,
    pub server_key: Vec<u8>,
}

/// Redacting Debug: the keys are password-equivalent for this server's users.
impl std::fmt::Debug for ScramCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScramCredential")
            .field("mechanism", &self.mechanism)
            .field("iterations", &self.iterations)
            .field("salt", &"[REDACTED]")
            .field("stored_key", &"[REDACTED]")
            .field("server_key", &"[REDACTED]")
            .finish()
    }
}

impl ScramCredential {
    /// Derive the credential for `password` with the given salt and iteration count.
    pub fn derive(
        mechanism: ScramMechanism,
        password: &[u8],
        salt: &[u8],
        iterations: u32,
    ) -> Self {
        let salted = salted_password(mechanism, password, salt, iterations);
        let client_key = hmac_bytes(mechanism, &salted, b"Client Key");
        ScramCredential {
            mechanism,
            salt: salt.to_vec(),
            iterations,
            stored_key: hash_bytes(mechanism, &client_key),
            server_key: hmac_bytes(mechanism, &salted, b"Server Key"),
        }
    }

    /// Derive a credential with a fresh random salt.
    pub fn generate(mechanism: ScramMechanism, password: &[u8], iterations: u32) -> Self {
        Self::derive(mechanism, password, &random_bytes(SALT_LEN), iterations)
    }

    /// Whether `password` matches this credential (used by SASL/PLAIN). Costs one PBKDF2
    /// with `self.iterations` (bounded by [`MAX_ITERATIONS`] when stored); the final
    /// comparison is constant time.
    pub fn verify_password(&self, password: &[u8]) -> bool {
        let salted = salted_password(self.mechanism, password, &self.salt, self.iterations);
        let client_key = hmac_bytes(self.mechanism, &salted, b"Client Key");
        ct_eq(&hash_bytes(self.mechanism, &client_key), &self.stored_key)
    }

    /// Check the shape of a credential read from storage or given by an operator.
    pub fn validate(&self) -> Result<(), String> {
        if !(MIN_ITERATIONS..=MAX_ITERATIONS).contains(&self.iterations) {
            return Err(format!(
                "iterations must be between {} and {}, got {}",
                MIN_ITERATIONS, MAX_ITERATIONS, self.iterations
            ));
        }
        if self.salt.is_empty() {
            return Err("salt must not be empty".into());
        }
        let n = self.mechanism.key_len();
        if self.stored_key.len() != n || self.server_key.len() != n {
            return Err(format!(
                "{} keys must be {} bytes",
                self.mechanism.name(),
                n
            ));
        }
        Ok(())
    }

    /// Render in the PostgreSQL verifier format
    /// `SCRAM-SHA-256$<iterations>:<salt>$<StoredKey>:<ServerKey>` (base64 fields).
    pub fn to_verifier(&self) -> String {
        format!(
            "{}${}:{}${}:{}",
            self.mechanism.name(),
            self.iterations,
            B64.encode(&self.salt),
            B64.encode(&self.stored_key),
            B64.encode(&self.server_key)
        )
    }

    /// Parse the PostgreSQL verifier format (what `libpq`'s `PQencryptPasswordConn`
    /// produces for SCRAM-SHA-256, extended here to SCRAM-SHA-512), so an operator can
    /// install a credential without the plaintext password ever reaching the server.
    pub fn from_verifier(verifier: &str) -> Result<Self, String> {
        let bad = || "expected <mechanism>$<iterations>:<salt>$<StoredKey>:<ServerKey>".to_string();
        let mut parts = verifier.trim().splitn(3, '$');
        let mechanism = parts
            .next()
            .and_then(ScramMechanism::from_name)
            .ok_or_else(|| "unknown or missing SCRAM mechanism".to_string())?;
        let (iterations, salt) = parts
            .next()
            .and_then(|p| p.split_once(':'))
            .ok_or_else(bad)?;
        let (stored, server) = parts
            .next()
            .and_then(|p| p.split_once(':'))
            .ok_or_else(bad)?;
        let iterations: u32 = iterations.parse().map_err(|_| bad())?;
        let decode = |s: &str| B64.decode(s).map_err(|_| "invalid base64".to_string());
        let cred = ScramCredential {
            mechanism,
            salt: decode(salt)?,
            iterations,
            stored_key: decode(stored)?,
            server_key: decode(server)?,
        };
        cred.validate()?;
        Ok(cred)
    }
}

/// A per-process random key, the fallback when the durable secret
/// (`kafka.sasl_server_secret`, read on the DB thread) is not available.
fn fake_credential_secret() -> &'static [u8] {
    static SECRET: once_cell::sync::OnceCell<Vec<u8>> = once_cell::sync::OnceCell::new();
    SECRET.get_or_init(|| random_bytes(32))
}

/// What the fake credential of an unknown user looks like on the wire (wave3 net): the
/// salt length and iteration count are those most common among the stored credentials
/// of the mechanism, and the salt is keyed by a durable secret, so it is stable for a
/// username across worker restarts.
#[derive(Clone, PartialEq, Eq)]
pub struct FakeCredentialParams {
    pub secret: Vec<u8>,
    pub iterations: u32,
    pub salt_len: usize,
}

impl std::fmt::Debug for FakeCredentialParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FakeCredentialParams")
            .field("secret", &"[REDACTED]")
            .field("iterations", &self.iterations)
            .field("salt_len", &self.salt_len)
            .finish()
    }
}

impl FakeCredentialParams {
    /// The defaults (no stored credential to imitate): what `pg_kafka_create_user`
    /// generates, keyed by the per-process secret.
    pub fn process_default() -> Self {
        FakeCredentialParams {
            secret: fake_credential_secret().to_vec(),
            iterations: DEFAULT_ITERATIONS,
            salt_len: SALT_LEN,
        }
    }
}

/// A credential for a user that does not exist, with the default parameters and the
/// per-process secret. See [`fake_credential_with`].
pub fn fake_credential(mechanism: ScramMechanism, username: &str) -> ScramCredential {
    fake_credential_with(
        mechanism,
        username,
        &FakeCredentialParams::process_default(),
    )
}

/// A credential for a user that does not exist. A SCRAM exchange for an unknown user
/// runs to the end with this (deterministic salt, imitated iteration count and salt
/// length, random keys) and then fails exactly like a wrong password, so the
/// server-first-message does not reveal which users exist. Residual leak (documented in
/// docs/AUTHENTICATION.md): a real user whose iteration count or salt length differs
/// from the most common one is recognizable as real.
pub fn fake_credential_with(
    mechanism: ScramMechanism,
    username: &str,
    params: &FakeCredentialParams,
) -> ScramCredential {
    let mut msg = Vec::with_capacity(username.len() + 32);
    msg.extend_from_slice(b"pg_kafka-fake-salt:");
    msg.extend_from_slice(mechanism.name().as_bytes());
    msg.push(b':');
    msg.extend_from_slice(username.as_bytes());
    // Expand HMAC output in counter mode to any salt length (1..=1024 bytes).
    let salt_len = params.salt_len.clamp(1, 1024);
    let mut salt = Vec::with_capacity(salt_len + 32);
    let mut counter = 0u32;
    while salt.len() < salt_len {
        let mut block = msg.clone();
        block.extend_from_slice(&counter.to_be_bytes());
        salt.extend(hmac_bytes(ScramMechanism::Sha256, &params.secret, &block));
        counter += 1;
    }
    salt.truncate(salt_len);
    ScramCredential {
        mechanism,
        salt,
        iterations: params.iterations.clamp(MIN_ITERATIONS, MAX_ITERATIONS),
        stored_key: random_bytes(mechanism.key_len()),
        server_key: random_bytes(mechanism.key_len()),
    }
}

/// Decode a SCRAM `saslname` (`=2C` -> `,`, `=3D` -> `=`; any other `=` is an error).
fn decode_saslname(raw: &str) -> Result<String, ScramError> {
    let mut out = String::with_capacity(raw.len());
    let mut rest = raw;
    while let Some(pos) = rest.find('=') {
        out.push_str(&rest[..pos]);
        let esc = rest.get(pos..pos + 3);
        match esc {
            Some("=2C") | Some("=2c") => out.push(','),
            Some("=3D") | Some("=3d") => out.push('='),
            _ => return Err(ScramError::Malformed("invalid escape in username")),
        }
        rest = &rest[pos + 3..];
    }
    out.push_str(rest);
    if out.is_empty() {
        return Err(ScramError::Malformed("empty username"));
    }
    Ok(out)
}

/// A nonce is printable ASCII except ','.
fn valid_nonce(nonce: &str) -> bool {
    !nonce.is_empty()
        && nonce
            .bytes()
            .all(|b| (0x21..=0x7e).contains(&b) && b != b',')
}

/// The parsed `client-first-message`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientFirst {
    /// The GS2 header (`n,,` or `y,,`, possibly with `a=authzid`), echoed in `c=`.
    pub gs2_header: String,
    /// The decoded username.
    pub username: String,
    /// The client nonce.
    pub client_nonce: String,
    /// `client-first-message-bare`, part of the AuthMessage.
    pub bare: String,
}

/// Parse a `client-first-message` (RFC 5802 section 7).
///
/// Channel binding (`p=`) and mandatory extensions (`m=`) are rejected; so is Kafka's
/// `tokenauth=true` extension (delegation tokens are not supported). An `a=` authzid
/// is accepted only when it equals the username, as Kafka does.
pub fn parse_client_first(message: &[u8]) -> Result<ClientFirst, ScramError> {
    if message.len() > MAX_SCRAM_MESSAGE_LEN {
        return Err(ScramError::Malformed("message too long"));
    }
    let msg = std::str::from_utf8(message).map_err(|_| ScramError::Malformed("not UTF-8"))?;
    let (cbind, rest) = msg
        .split_once(',')
        .ok_or(ScramError::Malformed("missing GS2 header"))?;
    match cbind {
        "n" | "y" => {}
        c if c.starts_with("p=") => return Err(ScramError::Unsupported("channel binding")),
        _ => return Err(ScramError::Malformed("invalid channel-binding flag")),
    }
    let (authzid, bare) = rest
        .split_once(',')
        .ok_or(ScramError::Malformed("missing GS2 header"))?;
    let authzid = if authzid.is_empty() {
        None
    } else if let Some(a) = authzid.strip_prefix("a=") {
        Some(decode_saslname(a)?)
    } else {
        return Err(ScramError::Malformed("invalid authzid"));
    };
    let gs2_header = format!("{},{},", cbind, &rest[..rest.len() - bare.len() - 1]);

    let mut attrs = bare.split(',');
    let first = attrs.next().unwrap_or("");
    if first.starts_with("m=") {
        return Err(ScramError::Unsupported("mandatory extension"));
    }
    let username = decode_saslname(
        first
            .strip_prefix("n=")
            .ok_or(ScramError::Malformed("missing username"))?,
    )?;
    let client_nonce = attrs
        .next()
        .and_then(|a| a.strip_prefix("r="))
        .ok_or(ScramError::Malformed("missing nonce"))?
        .to_string();
    if !valid_nonce(&client_nonce) {
        return Err(ScramError::Malformed("invalid nonce"));
    }
    for ext in attrs {
        let (key, value) = ext
            .split_once('=')
            .ok_or(ScramError::Malformed("invalid extension"))?;
        if key == "tokenauth" && value.eq_ignore_ascii_case("true") {
            return Err(ScramError::Unsupported("delegation tokens"));
        }
        // Other (optional) extensions are ignored, as RFC 5802 allows.
    }
    if let Some(a) = authzid {
        if a != username {
            return Err(ScramError::Unsupported("authzid different from username"));
        }
    }
    Ok(ClientFirst {
        gs2_header,
        username,
        client_nonce,
        bare: bare.to_string(),
    })
}

/// A fresh server nonce (24 random bytes, base64 without padding: printable, no ',').
pub fn generate_server_nonce() -> String {
    base64::engine::general_purpose::STANDARD_NO_PAD.encode(random_bytes(24))
}

/// Server state between the server-first-message and the client-final-message.
pub struct ScramServerState {
    mechanism: ScramMechanism,
    credential: ScramCredential,
    /// True when `credential` is a [`fake_credential`]: verification always fails.
    unknown_user: bool,
    gs2_header: String,
    client_first_bare: String,
    server_first: String,
    nonce: String,
    username: String,
}

impl std::fmt::Debug for ScramServerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScramServerState")
            .field("mechanism", &self.mechanism)
            .finish_non_exhaustive()
    }
}

impl ScramServerState {
    /// Start an exchange. `credential` is the stored credential for the user, or `None`
    /// when the user does not exist (a fake one is used, see [`fake_credential`]).
    /// `server_nonce` comes from [`generate_server_nonce`] (fixed in tests).
    pub fn new(
        mechanism: ScramMechanism,
        client_first: ClientFirst,
        credential: Option<ScramCredential>,
        server_nonce: &str,
    ) -> Self {
        let fake = credential
            .is_none()
            .then(|| fake_credential(mechanism, &client_first.username));
        Self::start(mechanism, client_first, credential, fake, server_nonce)
    }

    /// wave3 net: start an exchange with the stored credential, or (unknown user) with
    /// `fake`, a fake credential built on the DB thread ([`fake_credential_with`]).
    pub fn start(
        mechanism: ScramMechanism,
        client_first: ClientFirst,
        credential: Option<ScramCredential>,
        fake: Option<ScramCredential>,
        server_nonce: &str,
    ) -> Self {
        let credential = credential.filter(|c| c.mechanism == mechanism);
        let unknown_user = credential.is_none();
        let credential = credential.unwrap_or_else(|| {
            fake.filter(|c| c.mechanism == mechanism)
                .unwrap_or_else(|| fake_credential(mechanism, &client_first.username))
        });
        let nonce = format!("{}{}", client_first.client_nonce, server_nonce);
        let server_first = format!(
            "r={},s={},i={}",
            nonce,
            B64.encode(&credential.salt),
            credential.iterations
        );
        ScramServerState {
            mechanism,
            credential,
            unknown_user,
            gs2_header: client_first.gs2_header,
            client_first_bare: client_first.bare,
            server_first,
            nonce,
            username: client_first.username,
        }
    }

    /// The `server-first-message` to send.
    pub fn server_first(&self) -> &str {
        &self.server_first
    }

    /// The mechanism of this exchange.
    pub fn mechanism(&self) -> ScramMechanism {
        self.mechanism
    }

    /// The username the client claimed.
    pub fn username(&self) -> &str {
        &self.username
    }

    /// Verify a `client-final-message`; on success return the `server-final-message`.
    /// The proof check is constant time, and an unknown user fails the same way a wrong
    /// password does.
    pub fn verify_client_final(&self, message: &[u8]) -> Result<String, ScramError> {
        if message.len() > MAX_SCRAM_MESSAGE_LEN {
            return Err(ScramError::Malformed("message too long"));
        }
        let msg = std::str::from_utf8(message).map_err(|_| ScramError::Malformed("not UTF-8"))?;
        let idx = msg
            .rfind(",p=")
            .ok_or(ScramError::Malformed("missing proof"))?;
        let without_proof = &msg[..idx];
        let proof_b64 = &msg[idx + 3..];
        if proof_b64.contains(',') {
            return Err(ScramError::Malformed("proof must be the last attribute"));
        }
        let mut attrs = without_proof.split(',');
        let channel_binding = attrs
            .next()
            .and_then(|a| a.strip_prefix("c="))
            .ok_or(ScramError::Malformed("missing channel binding"))?;
        let expected_cb = B64.encode(self.gs2_header.as_bytes());
        if channel_binding != expected_cb {
            return Err(ScramError::Malformed("channel binding mismatch"));
        }
        let nonce = attrs
            .next()
            .and_then(|a| a.strip_prefix("r="))
            .ok_or(ScramError::Malformed("missing nonce"))?;
        if nonce != self.nonce {
            return Err(ScramError::Malformed("nonce mismatch"));
        }
        let proof = B64
            .decode(proof_b64)
            .map_err(|_| ScramError::Malformed("invalid proof encoding"))?;
        let key_len = self.mechanism.key_len();
        if proof.len() != key_len {
            return Err(ScramError::Malformed("invalid proof length"));
        }

        let auth_message = format!(
            "{},{},{}",
            self.client_first_bare, self.server_first, without_proof
        );
        let client_signature = hmac_bytes(
            self.mechanism,
            &self.credential.stored_key,
            auth_message.as_bytes(),
        );
        let client_key: Vec<u8> = proof
            .iter()
            .zip(&client_signature)
            .map(|(p, s)| p ^ s)
            .collect();
        let matches = ct_eq(
            &hash_bytes(self.mechanism, &client_key),
            &self.credential.stored_key,
        );
        if !matches || self.unknown_user {
            return Err(ScramError::InvalidCredentials);
        }
        let server_signature = hmac_bytes(
            self.mechanism,
            &self.credential.server_key,
            auth_message.as_bytes(),
        );
        Ok(format!("v={}", B64.encode(server_signature)))
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// A minimal SCRAM client, to drive the server through full exchanges.
    pub(crate) fn client_final_for(
        mechanism: ScramMechanism,
        password: &str,
        client_first_bare: &str,
        gs2_header: &str,
        server_first: &str,
    ) -> (String, String) {
        let mut nonce = "";
        let mut salt = Vec::new();
        let mut iterations = 0;
        for attr in server_first.split(',') {
            if let Some(v) = attr.strip_prefix("r=") {
                nonce = v;
            } else if let Some(v) = attr.strip_prefix("s=") {
                salt = B64.decode(v).unwrap();
            } else if let Some(v) = attr.strip_prefix("i=") {
                iterations = v.parse().unwrap();
            }
        }
        let salted = salted_password(mechanism, password.as_bytes(), &salt, iterations);
        let client_key = hmac_bytes(mechanism, &salted, b"Client Key");
        let stored_key = hash_bytes(mechanism, &client_key);
        let without_proof = format!("c={},r={}", B64.encode(gs2_header), nonce);
        let auth_message = format!("{},{},{}", client_first_bare, server_first, without_proof);
        let signature = hmac_bytes(mechanism, &stored_key, auth_message.as_bytes());
        let proof: Vec<u8> = client_key
            .iter()
            .zip(&signature)
            .map(|(a, b)| a ^ b)
            .collect();
        let server_key = hmac_bytes(mechanism, &salted, b"Server Key");
        let server_sig = hmac_bytes(mechanism, &server_key, auth_message.as_bytes());
        (
            format!("{},p={}", without_proof, B64.encode(proof)),
            format!("v={}", B64.encode(server_sig)),
        )
    }

    /// RFC 5802 section 5 (SCRAM-SHA-1): user "user", password "pencil".
    #[test]
    fn rfc5802_sha1_test_vector() {
        let salt = B64.decode("QSXCR+Q6sek8bf92").unwrap();
        let cred = ScramCredential::derive(ScramMechanism::Sha1, b"pencil", &salt, 4096);
        let first = parse_client_first(b"n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL").unwrap();
        assert_eq!(first.username, "user");
        assert_eq!(first.gs2_header, "n,,");
        let state = ScramServerState::new(
            ScramMechanism::Sha1,
            first,
            Some(cred),
            "3rfcNHYJY1ZVvWVs7j",
        );
        assert_eq!(
            state.server_first(),
            "r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096"
        );
        let server_final = state
            .verify_client_final(
                b"c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,p=v0X8v3Bz2T0CJGbJQyF0X+HI4Ts=",
            )
            .unwrap();
        assert_eq!(server_final, "v=rmF9pqV8S7suAoZWja4dJRkFsKQ=");
    }

    /// RFC 7677 section 3 (SCRAM-SHA-256): user "user", password "pencil".
    #[test]
    fn rfc7677_sha256_test_vector() {
        let salt = B64.decode("W22ZaJ0SNY7soEsUEjb6gQ==").unwrap();
        let cred = ScramCredential::derive(ScramMechanism::Sha256, b"pencil", &salt, 4096);
        let first = parse_client_first(b"n,,n=user,r=rOprNGfwEbeRWgbNEkqO").unwrap();
        let state = ScramServerState::new(
            ScramMechanism::Sha256,
            first,
            Some(cred.clone()),
            "%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0",
        );
        assert_eq!(
            state.server_first(),
            "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096"
        );
        let server_final = state
            .verify_client_final(
                b"c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=",
            )
            .unwrap();
        assert_eq!(
            server_final,
            "v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4="
        );
        // The same credential checks the password for PLAIN.
        assert!(cred.verify_password(b"pencil"));
        assert!(!cred.verify_password(b"pencil "));
    }

    #[test]
    fn rfc7677_vector_with_wrong_proof_fails() {
        let salt = B64.decode("W22ZaJ0SNY7soEsUEjb6gQ==").unwrap();
        let cred = ScramCredential::derive(ScramMechanism::Sha256, b"pencil", &salt, 4096);
        let first = parse_client_first(b"n,,n=user,r=rOprNGfwEbeRWgbNEkqO").unwrap();
        let state = ScramServerState::new(
            ScramMechanism::Sha256,
            first,
            Some(cred),
            "%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0",
        );
        // Last proof byte flipped.
        let err = state
            .verify_client_final(
                b"c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVU=",
            )
            .unwrap_err();
        assert_eq!(err, ScramError::InvalidCredentials);
    }

    fn full_exchange(
        mechanism: ScramMechanism,
        stored: &str,
        offered: &str,
    ) -> Result<(), ScramError> {
        let cred = ScramCredential::generate(mechanism, stored.as_bytes(), 4096);
        let first = parse_client_first(b"n,,n=alice,r=abcdefghijklmnop").unwrap();
        let bare = first.bare.clone();
        let state = ScramServerState::new(mechanism, first, Some(cred), &generate_server_nonce());
        let (client_final, expected_server_final) =
            client_final_for(mechanism, offered, &bare, "n,,", state.server_first());
        let server_final = state.verify_client_final(client_final.as_bytes())?;
        assert_eq!(server_final, expected_server_final);
        Ok(())
    }

    #[test]
    fn sha512_and_sha256_round_trips() {
        for m in ScramMechanism::STORED {
            assert_eq!(full_exchange(m, "s3cret", "s3cret"), Ok(()));
            assert_eq!(
                full_exchange(m, "s3cret", "s3cret!"),
                Err(ScramError::InvalidCredentials)
            );
        }
        assert_eq!(ScramMechanism::Sha512.key_len(), 64);
    }

    #[test]
    fn unknown_user_fails_like_a_wrong_password() {
        let m = ScramMechanism::Sha256;
        let first = parse_client_first(b"n,,n=ghost,r=nonce1").unwrap();
        let bare = first.bare.clone();
        let state = ScramServerState::new(m, first, None, "srv");
        // The fake salt is stable per user and iterations look like a real credential.
        assert_eq!(
            fake_credential(m, "ghost").salt,
            fake_credential(m, "ghost").salt
        );
        assert_ne!(
            fake_credential(m, "ghost").salt,
            fake_credential(m, "other").salt
        );
        assert!(state.server_first().ends_with(",i=4096"));
        let (client_final, _) = client_final_for(m, "anything", &bare, "n,,", state.server_first());
        assert_eq!(
            state.verify_client_final(client_final.as_bytes()),
            Err(ScramError::InvalidCredentials)
        );
    }

    #[test]
    fn client_final_is_bound_to_this_exchange() {
        let m = ScramMechanism::Sha256;
        let cred = ScramCredential::generate(m, b"pw", 4096);
        let first = parse_client_first(b"n,,n=u,r=cnonce").unwrap();
        let bare = first.bare.clone();
        let state = ScramServerState::new(m, first, Some(cred), "snonce");
        let (good, _) = client_final_for(m, "pw", &bare, "n,,", state.server_first());
        // Wrong nonce.
        let bad_nonce = good.replace("r=cnoncesnonce", "r=cnonceXnonce");
        assert_eq!(
            state.verify_client_final(bad_nonce.as_bytes()),
            Err(ScramError::Malformed("nonce mismatch"))
        );
        // Wrong channel binding ("y,," instead of "n,,").
        let bad_cb = good.replace("c=biws", "c=eSws");
        assert_eq!(
            state.verify_client_final(bad_cb.as_bytes()),
            Err(ScramError::Malformed("channel binding mismatch"))
        );
        // Missing / truncated proof.
        assert!(state.verify_client_final(b"c=biws,r=cnoncesnonce").is_err());
        assert!(state
            .verify_client_final(b"c=biws,r=cnoncesnonce,p=AAAA")
            .is_err());
        assert!(state.verify_client_final(&[0xff, 0xfe]).is_err());
        assert_eq!(
            state.verify_client_final(good.as_bytes()).map(|_| ()),
            Ok(())
        );
    }

    #[test]
    fn client_first_parsing() {
        let f = parse_client_first(b"n,a=bob,n=bob,r=xyz").unwrap();
        assert_eq!(f.gs2_header, "n,a=bob,");
        assert_eq!(f.bare, "n=bob,r=xyz");
        let f = parse_client_first(b"y,,n=a=2Cb=3Dc,r=xyz,ext=1").unwrap();
        assert_eq!(f.username, "a,b=c");
        assert_eq!(f.gs2_header, "y,,");

        let reject = |m: &[u8]| parse_client_first(m).unwrap_err();
        assert_eq!(
            reject(b"p=tls-unique,,n=u,r=x"),
            ScramError::Unsupported("channel binding")
        );
        assert_eq!(
            reject(b"n,,m=ext,n=u,r=x"),
            ScramError::Unsupported("mandatory extension")
        );
        assert_eq!(
            reject(b"n,,n=u,r=x,tokenauth=true"),
            ScramError::Unsupported("delegation tokens")
        );
        assert_eq!(
            reject(b"n,a=other,n=u,r=x"),
            ScramError::Unsupported("authzid different from username")
        );
        assert!(matches!(reject(b"n,,n=,r=x"), ScramError::Malformed(_)));
        assert!(matches!(
            reject(b"n,,n=a=2Xb,r=x"),
            ScramError::Malformed(_)
        ));
        assert!(matches!(reject(b"n,,n=u"), ScramError::Malformed(_)));
        assert!(matches!(reject(b"n,,n=u,r=a,b"), ScramError::Malformed(_)));
        assert!(matches!(reject(b"n,,n=u,r="), ScramError::Malformed(_)));
        assert!(matches!(reject(b"x,,n=u,r=x"), ScramError::Malformed(_)));
        assert!(matches!(reject(b"n"), ScramError::Malformed(_)));
        assert!(matches!(
            reject(&[b'n', b',', b',', 0xff]),
            ScramError::Malformed(_)
        ));
        let long = vec![b'a'; MAX_SCRAM_MESSAGE_LEN + 1];
        assert!(matches!(reject(&long), ScramError::Malformed(_)));
    }

    #[test]
    fn ct_eq_compares_whole_slices() {
        assert!(ct_eq(b"", b""));
        assert!(ct_eq(b"abc", b"abc"));
        assert!(!ct_eq(b"abc", b"abd"));
        assert!(!ct_eq(b"abc", b"ab"));
    }

    #[test]
    fn verifier_round_trip_and_validation() {
        let cred = ScramCredential::generate(ScramMechanism::Sha512, b"pw", 8192);
        let v = cred.to_verifier();
        assert!(v.starts_with("SCRAM-SHA-512$8192:"));
        assert_eq!(ScramCredential::from_verifier(&v).unwrap(), cred);

        // The PostgreSQL verifier format for the RFC 7677 credential.
        let salt = B64.decode("W22ZaJ0SNY7soEsUEjb6gQ==").unwrap();
        let rfc = ScramCredential::derive(ScramMechanism::Sha256, b"pencil", &salt, 4096);
        let parsed = ScramCredential::from_verifier(&rfc.to_verifier()).unwrap();
        assert!(parsed.verify_password(b"pencil"));

        assert!(ScramCredential::from_verifier("md5abc").is_err());
        assert!(ScramCredential::from_verifier("SCRAM-SHA-256$100:AAAA$AAAA:AAAA").is_err());
        assert!(ScramCredential::from_verifier("SCRAM-SHA-256$4096:AAAA$AAAA:AAAA").is_err());
        assert!(ScramCredential::from_verifier("SCRAM-SHA-256$4096:!!$AAAA:AAAA").is_err());
        assert!(ScramCredential::from_verifier("SCRAM-SHA-256$x:AAAA$AAAA:AAAA").is_err());
        let mut short = rfc.clone();
        short.iterations = MAX_ITERATIONS + 1;
        assert!(short.validate().is_err());
    }

    #[test]
    fn debug_output_redacts_keys() {
        let cred = ScramCredential::generate(ScramMechanism::Sha256, b"pw", 4096);
        let s = format!("{:?}", cred);
        assert!(s.contains("REDACTED"));
        assert!(!s.contains(&format!("{:?}", cred.stored_key)));
    }

    #[test]
    fn mechanism_names() {
        for m in ScramMechanism::STORED {
            assert_eq!(ScramMechanism::from_name(m.name()), Some(m));
        }
        assert_eq!(
            ScramMechanism::from_name("scram-sha-256"),
            Some(ScramMechanism::Sha256)
        );
        assert_eq!(ScramMechanism::from_name("PLAIN"), None);
        assert!(valid_nonce(&generate_server_nonce()));
    }
}
