//! E2E tests for the `auth` stream: TLS and SASL (PLAIN, SCRAM-SHA-256/512) on the
//! Kafka listener, with real rdkafka clients.
//!
//! The default suite runs against the PLAINTEXT listener on 9092, which stays
//! unauthenticated. These tests add three listeners next to it, through the
//! `pg_kafka.additional_listeners` GUC:
//!
//! * `SASL_PLAINTEXT://127.0.0.1:9093`
//! * `SSL://127.0.0.1:9094`
//! * `SASL_SSL://127.0.0.1:9095`
//!
//! The first auth test ([`auth_env`]) generates a throwaway CA, server and client
//! certificates with the `openssl` CLI into `$TMPDIR/pg_kafka_auth_e2e`, points the
//! TLS GUCs at them with ALTER SYSTEM, reloads, and restarts the `pg_kafka_listener`
//! worker (listener settings are read at worker start), then creates a Kafka user. The
//! last test ([`test_auth_teardown_restores_default_listeners`]) resets the GUCs and
//! restarts the worker again, so the server is left as it was.
//!
//! Requirements: the test process runs on the database host (the server reads the
//! generated files), connects as a superuser, and has `openssl` on PATH. All tests are
//! registered `parallel_safe: false` because they restart the listener.

pub mod tests;
// Stream `api`: SASL requests in kafka.api_metrics.
pub mod api_metrics;

use crate::common::{create_db_client, TestResult};
use crate::error_paths::bug_hunt::roundtrip;
use kafka_protocol::messages::ApiVersionsRequest;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio_postgres::Client;

pub const PLAINTEXT_PORT: u16 = 9092;
pub const SASL_PLAINTEXT_PORT: u16 = 9093;
pub const SSL_PORT: u16 = 9094;
pub const SASL_SSL_PORT: u16 = 9095;

/// The Kafka user the tests create. The `=` and `,` exercise SCRAM saslname escaping.
pub const USER: &str = "e2e-auth=user,1";
pub const PASSWORD: &str = "e2e auth pässword";

const LISTENERS: &str =
    "SASL_PLAINTEXT://127.0.0.1:9093,SSL://127.0.0.1:9094,SASL_SSL://127.0.0.1:9095";

pub type DynResult<T> = Result<T, Box<dyn std::error::Error>>;

/// Where the generated certificates live.
pub fn cert_dir() -> PathBuf {
    std::env::temp_dir().join("pg_kafka_auth_e2e")
}

pub fn cert_path(name: &str) -> String {
    cert_dir().join(name).to_string_lossy().into_owned()
}

fn openssl(dir: &Path, args: &[&str]) -> DynResult<()> {
    let out = Command::new("openssl")
        .args(args)
        .current_dir(dir)
        .output()?;
    if !out.status.success() {
        return Err(format!(
            "openssl {:?} failed: {}",
            args,
            String::from_utf8_lossy(&out.stderr)
        )
        .into());
    }
    Ok(())
}

/// Generate: `ca.pem`/`ca.key` (trusted CA), `server.pem`/`server.key` (signed by it,
/// SAN localhost + 127.0.0.1), `client.pem`/`client.key` (client certificate signed
/// by it), and `other-ca.pem` (an unrelated CA a client must not accept the server with).
fn generate_certificates() -> DynResult<PathBuf> {
    let dir = cert_dir();
    std::fs::create_dir_all(&dir)?;
    let ec = [
        "-newkey",
        "ec",
        "-pkeyopt",
        "ec_paramgen_curve:prime256v1",
        "-nodes",
    ];
    for (name, cn) in [("ca", "pg_kafka e2e CA"), ("other-ca", "untrusted e2e CA")] {
        let key = format!("{}.key", name);
        let pem = format!("{}.pem", name);
        let subj = format!("/CN={}", cn);
        let mut args = vec!["req", "-x509"];
        args.extend(ec);
        args.extend([
            "-keyout",
            &key,
            "-out",
            &pem,
            "-days",
            "2",
            "-subj",
            &subj,
            "-addext",
            "basicConstraints=critical,CA:TRUE",
            "-addext",
            "keyUsage=critical,keyCertSign,cRLSign",
        ]);
        openssl(&dir, &args)?;
    }
    std::fs::write(
        dir.join("server.ext"),
        "subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n",
    )?;
    std::fs::write(dir.join("client.ext"), "extendedKeyUsage=clientAuth\n")?;
    for (name, subj) in [
        ("server", "/CN=localhost"),
        ("client", "/O=pg_kafka e2e/CN=e2e-client"),
    ] {
        let key = format!("{}.key", name);
        let csr = format!("{}.csr", name);
        let pem = format!("{}.pem", name);
        let ext = format!("{}.ext", name);
        let mut args = vec!["req"];
        args.extend(ec);
        args.extend(["-keyout", &key, "-out", &csr, "-subj", subj]);
        openssl(&dir, &args)?;
        openssl(
            &dir,
            &[
                "x509",
                "-req",
                "-in",
                &csr,
                "-CA",
                "ca.pem",
                "-CAkey",
                "ca.key",
                "-CAcreateserial",
                "-out",
                &pem,
                "-days",
                "2",
                "-extfile",
                &ext,
            ],
        )?;
    }
    // The server process (another OS user) must read these. Test material only.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755))?;
        for entry in std::fs::read_dir(&dir)? {
            std::fs::set_permissions(entry?.path(), std::fs::Permissions::from_mode(0o644))?;
        }
    }
    Ok(dir)
}

fn quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// Point the listener GUCs at the generated files (or reset them) and reload.
pub async fn configure_listeners(db: &Client, enable: bool, mutual_tls: bool) -> DynResult<()> {
    let settings = [
        ("pg_kafka.additional_listeners", LISTENERS.to_string()),
        ("pg_kafka.ssl_cert_file", cert_path("server.pem")),
        ("pg_kafka.ssl_key_file", cert_path("server.key")),
        ("pg_kafka.ssl_ca_file", cert_path("ca.pem")),
    ];
    for (name, value) in settings {
        let set = enable && (name != "pg_kafka.ssl_ca_file" || mutual_tls);
        let sql = if set {
            format!("ALTER SYSTEM SET {} = {}", name, quote(&value))
        } else {
            format!("ALTER SYSTEM RESET {}", name)
        };
        db.execute(&sql, &[]).await?;
    }
    db.execute("SELECT pg_reload_conf()", &[]).await?;
    Ok(())
}

async fn listener_pid(db: &Client) -> DynResult<Option<i32>> {
    let rows = db
        .query(
            "SELECT pid FROM pg_stat_activity WHERE backend_type = 'pg_kafka_listener'",
            &[],
        )
        .await?;
    Ok(rows.first().map(|r| r.get(0)))
}

async fn port_open(port: u16) -> bool {
    matches!(
        tokio::time::timeout(
            Duration::from_secs(1),
            TcpStream::connect(("127.0.0.1", port))
        )
        .await,
        Ok(Ok(_))
    )
}

async fn plaintext_answers() -> bool {
    let attempt = async {
        let mut s = TcpStream::connect(("127.0.0.1", PLAINTEXT_PORT)).await?;
        let r = roundtrip(&mut s, 0, 1, &ApiVersionsRequest::default()).await?;
        Ok::<i16, Box<dyn std::error::Error>>(r.error_code)
    };
    matches!(
        tokio::time::timeout(Duration::from_secs(2), attempt).await,
        Ok(Ok(0))
    )
}

/// Terminate the listener worker and wait until its replacement serves 9092 and
/// every port in `open` (and none in `closed`).
pub async fn restart_listener(db: &Client, open: &[u16], closed: &[u16]) -> DynResult<()> {
    let old = listener_pid(db).await?;
    if let Some(pid) = old {
        db.execute("SELECT pg_terminate_backend($1)", &[&pid])
            .await?;
    }
    let deadline = Instant::now() + Duration::from_secs(40);
    while Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(250)).await;
        let Some(pid) = listener_pid(db).await? else {
            continue;
        };
        if Some(pid) == old || !plaintext_answers().await {
            continue;
        }
        let mut ready = true;
        for p in open {
            ready &= port_open(*p).await;
        }
        for p in closed {
            ready &= !port_open(*p).await;
        }
        if ready {
            return Ok(());
        }
    }
    Err(format!(
        "listener did not come back with ports {:?} open and {:?} closed within 40 s \
         (check the server log for a pg_kafka listener security error)",
        open, closed
    )
    .into())
}

static ENV: tokio::sync::OnceCell<Result<(), String>> = tokio::sync::OnceCell::const_new();

async fn setup() -> DynResult<()> {
    generate_certificates()?;
    let db = create_db_client().await?;
    configure_listeners(&db, true, false).await?;
    restart_listener(&db, &[SASL_PLAINTEXT_PORT, SSL_PORT, SASL_SSL_PORT], &[]).await?;
    db.execute("SELECT pg_kafka_drop_user($1)", &[&USER])
        .await?;
    db.execute("SELECT pg_kafka_create_user($1, $2)", &[&USER, &PASSWORD])
        .await?;
    Ok(())
}

/// Bring up the auth listeners once per test process.
pub async fn auth_env() -> TestResult {
    ENV.get_or_init(|| async { setup().await.map_err(|e| e.to_string()) })
        .await
        .clone()
        .map_err(|e| format!("auth E2E setup failed: {}", e).into())
}
