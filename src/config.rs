//! The extension's own GUCs (`pg_kafka.*` settings in postgresql.conf), their registration in
//! `_PG_init`, and [`Config`], the snapshot of them the worker and the network thread read.
//!
//! The GUCs of the listener security, the least-privilege role and the resource limits are
//! declared next to the code that uses them and registered from [`init`].

use pgrx::{GucContext, GucFlags, GucRegistry};
use std::time::Duration;

#[cfg(test)]
use crate::kafka::constants::TEST_HOST;
use crate::kafka::constants::{
    DEFAULT_COMPRESSION_TYPE, DEFAULT_CONFIG_RELOAD_MS, DEFAULT_DATABASE,
    DEFAULT_FETCH_POLL_INTERVAL_MS, DEFAULT_HOST, DEFAULT_KAFKA_PORT, DEFAULT_SHADOW_BATCH_SIZE,
    DEFAULT_SHADOW_BOOTSTRAP_SERVERS, DEFAULT_SHADOW_LINGER_MS, DEFAULT_SHADOW_MAX_RETRIES,
    DEFAULT_SHADOW_METRICS_ENABLED, DEFAULT_SHADOW_MODE_ENABLED, DEFAULT_SHADOW_RETRY_BACKOFF_MS,
    DEFAULT_SHADOW_SASL_MECHANISM, DEFAULT_SHADOW_SECURITY_PROTOCOL,
    DEFAULT_SHADOW_SYNC_TIMEOUT_MS, DEFAULT_SHUTDOWN_TIMEOUT_MS, DEFAULT_TOPIC_PARTITIONS,
    MAX_CONFIG_RELOAD_MS, MAX_FETCH_POLL_INTERVAL_MS, MAX_SHADOW_BATCH_SIZE, MAX_SHADOW_LINGER_MS,
    MAX_SHADOW_MAX_RETRIES, MAX_SHADOW_RETRY_BACKOFF_MS, MAX_SHADOW_SYNC_TIMEOUT_MS,
    MIN_CONFIG_RELOAD_MS, MIN_FETCH_POLL_INTERVAL_MS, MIN_SHADOW_BATCH_SIZE, MIN_SHADOW_LINGER_MS,
    MIN_SHADOW_MAX_RETRIES, MIN_SHADOW_RETRY_BACKOFF_MS, MIN_SHADOW_SYNC_TIMEOUT_MS,
    MIN_SHUTDOWN_TIMEOUT_MS,
};

/// Default of `pg_kafka.worker_lock_timeout_ms`.
pub const DEFAULT_WORKER_LOCK_TIMEOUT_MS: i32 = 5000;
/// Upper bound of `pg_kafka.worker_lock_timeout_ms`.
pub const MAX_WORKER_LOCK_TIMEOUT_MS: i32 = 600_000;

/// Upper bound of `pg_kafka.shutdown_timeout_ms`.
const MAX_SHUTDOWN_TIMEOUT_MS: i32 = 60_000;

/// The Rust type of a GUC kind in [`gucs!`].
macro_rules! guc_type {
    (int) => { i32 };
    (bool) => { bool };
    (string) => { Option<::std::ffi::CString> };
}
pub(crate) use guc_type;

/// Registers one GUC of a kind in [`gucs!`] (an `int` takes its range).
macro_rules! define_guc {
    (int, $name:literal, $short:literal, $long:literal, $setting:expr, $context:ident,
     [$min:expr, $max:expr], $flags:expr) => {
        ::pgrx::GucRegistry::define_int_guc(
            $name,
            $short,
            $long,
            $setting,
            $min,
            $max,
            ::pgrx::GucContext::$context,
            $flags,
        )
    };
    (bool, $name:literal, $short:literal, $long:literal, $setting:expr, $context:ident,
     [], $flags:expr) => {
        ::pgrx::GucRegistry::define_bool_guc(
            $name,
            $short,
            $long,
            $setting,
            ::pgrx::GucContext::$context,
            $flags,
        )
    };
    (string, $name:literal, $short:literal, $long:literal, $setting:expr, $context:ident,
     [], $flags:expr) => {
        ::pgrx::GucRegistry::define_string_guc(
            $name,
            $short,
            $long,
            $setting,
            ::pgrx::GucContext::$context,
            $flags,
        )
    };
}
pub(crate) use define_guc;

/// The flags of a GUC in [`gucs!`]: the given ones, else the defaults.
macro_rules! guc_flags {
    () => {
        ::pgrx::GucFlags::default()
    };
    ($flags:expr) => {
        $flags
    };
}
pub(crate) use guc_flags;

/// Declares GUCs: one static per entry, and the named function, which registers them
/// in declaration order. The block starts with that function's signature, then the
/// entries; an entry reads
///
/// ```text
/// /// docs of the function
/// [vis] fn register_x;
///
/// /// docs of the static
/// [vis] int|bool|string STATIC = default => c"pg_kafka.name" {
///     short: c"...", long: c"...", context: Postmaster|Sighup|...,
///     range: (min, max),   // int only
///     flags: GucFlags::..., // optional, default GucFlags::default()
/// }
/// ```
///
/// so each setting's name, description, bounds and storage are written once, in one place.
macro_rules! gucs {
    (
    $(#[doc = $fn_doc:literal])*
    $fn_vis:vis fn $register:ident;
    $(
        $(#[doc = $doc:literal])*
        $vis:vis $kind:ident $static:ident = $default:expr => $name:literal {
            short: $short:literal,
            long: $long:literal,
            context: $context:ident,
            $(range: ($min:expr, $max:expr),)?
            $(flags: $flags:expr,)?
        }
    )*) => {
        $(
            $(#[doc = $doc])*
            $vis static $static: ::pgrx::GucSetting<$crate::config::guc_type!($kind)> =
                ::pgrx::GucSetting::<$crate::config::guc_type!($kind)>::new($default);
        )*

        $(#[doc = $fn_doc])*
        $fn_vis fn $register() {
            $(
                $crate::config::define_guc!(
                    $kind, $name, $short, $long, &$static, $context,
                    [$($min, $max)?],
                    $crate::config::guc_flags!($($flags)?)
                );
            )*
        }
    };
}
pub(crate) use gucs;

gucs! {
    /// Register the GUCs declared in this module, in declaration order.
    fn register_declared_gucs;

    /// TCP port of the primary Kafka listener.
    int PORT = DEFAULT_KAFKA_PORT => c"pg_kafka.port" {
        short: c"Port to listen on for Kafka protocol connections",
        long: c"The TCP port that pg_kafka will bind to. Default: 9092. Requires restart to take effect.",
        context: Postmaster,
        range: (1024, 65535),
    }

    /// Bind address of the primary listener (unset or blank: [`DEFAULT_HOST`], see
    /// [`normalize_host`]).
    string HOST = None => c"pg_kafka.host" {
        short: c"Host/interface to bind to",
        long: c"The network interface to listen on. Default 127.0.0.1 (local clients only). This listener speaks pg_kafka.security_protocol (default PLAINTEXT, unauthenticated), so a non-loopback PLAINTEXT bind (e.g. 0.0.0.0 = all interfaces) must be protected by network controls (firewall/security groups); see docs/AUTHENTICATION.md. Requires restart.",
        context: Postmaster,
    }

    /// The database the worker connects to (where `CREATE EXTENSION pg_kafka` ran).
    string DATABASE = None => c"pg_kafka.database" {
        short: c"Database to connect to for SPI operations",
        long: c"The database where CREATE EXTENSION pg_kafka was run. Default: postgres. Requires restart.",
        context: Postmaster,
    }

    /// Log every incoming connection.
    ///
    /// This and the other Postmaster GUCs are read once when the worker (and its listener)
    /// starts, so their context says so: a runtime `SET` fails loudly instead of being
    /// silently ignored.
    bool LOG_CONNECTIONS = false => c"pg_kafka.log_connections" {
        short: c"Whether to log each connection",
        long: c"When enabled, logs every incoming connection. Read at worker start; requires a restart.",
        context: Postmaster,
    }

    /// How long a stopping worker waits for the network thread.
    int SHUTDOWN_TIMEOUT_MS = DEFAULT_SHUTDOWN_TIMEOUT_MS => c"pg_kafka.shutdown_timeout_ms" {
        short: c"Timeout for graceful shutdown in milliseconds",
        long: c"How long to wait for the TCP listener to shut down cleanly. Default: 5000ms.",
        context: Postmaster,
        range: (MIN_SHUTDOWN_TIMEOUT_MS, MAX_SHUTDOWN_TIMEOUT_MS),
    }

    /// Partition count of topics created on first produce.
    int DEFAULT_PARTITIONS = DEFAULT_TOPIC_PARTITIONS => c"pg_kafka.default_partitions" {
        short: c"Default number of partitions for auto-created topics",
        long: c"When a topic is auto-created via produce, this sets the partition count. Default: 1.",
        context: Postmaster,
        range: (1, 10000),
    }

    /// Safety-net re-check interval of a waiting (long-poll) fetch.
    int FETCH_POLL_INTERVAL_MS = DEFAULT_FETCH_POLL_INTERVAL_MS => c"pg_kafka.fetch_poll_interval_ms" {
        short: c"Polling interval for long polling fallback (milliseconds)",
        long: c"A waiting FetchRequest re-runs when new data is signalled (Produce, EndTxn) or max_wait_ms expires; this interval is only the safety-net re-check for a missed signal. Default: 1000ms.",
        context: Postmaster,
        range: (MIN_FETCH_POLL_INTERVAL_MS, MAX_FETCH_POLL_INTERVAL_MS),
    }

    /// DR-2: time-based retention of kafka.messages, enforced by the retention sweep
    /// (0 = keep forever, up to ten years). Pruning of the auxiliary tables (stale producers,
    /// terminal transactions, delivered shadow-outbox rows) always runs.
    pub(crate) int MESSAGE_RETENTION_HOURS = 0 => c"pg_kafka.message_retention_hours" {
        short: c"Delete messages older than this many hours (0 = keep forever)",
        long: c"Time-based retention for kafka.messages, enforced by the worker's periodic \
          retention sweep. 0 (default) disables the sweep. Pending transactional \
          messages are never deleted regardless of age. Reloadable via SIGHUP.",
        context: Sighup,
        range: (0, 87_600),
    }

    /// ADR-001: width of one time partition (leaf) of kafka.messages. The worker keeps
    /// the current leaf and the next two; the retention sweep drops whole leaves once every
    /// row in them is expired.
    pub(crate) int MESSAGE_PARTITION_MINUTES = crate::kafka::storage::retention::DEFAULT_PARTITION_MINUTES => c"pg_kafka.message_partition_minutes" {
        short: c"Width of one time partition of kafka.messages, in minutes (default 1440 = one day)",
        long: c"kafka.messages is partitioned by created_at into leaves of this width, aligned to \
          UTC. The worker creates the current leaf and the next two ahead of time; the \
          retention sweep drops a whole leaf once every row in it is expired under its \
          topic's retention. Topics whose retention window is shorter than one width are \
          still expired row by row. A change applies to leaves created afterwards (existing \
          leaves keep their bounds). Reloadable via SIGHUP.",
        context: Sighup,
        range: (crate::kafka::storage::retention::MIN_PARTITION_MINUTES, crate::kafka::storage::retention::MAX_PARTITION_MINUTES),
    }

    /// Test-only fault injection for the shadow outbox in-flight gate: the forwarder
    /// (network thread) holds each ForwardAck this long after delivery, so the E2E suite can
    /// make an ack deterministically slower than the outbox retry lease and check that a
    /// late ack causes no duplicate delivery. Read through the RuntimeContext snapshot.
    /// Never set in production.
    int TEST_FORWARD_ACK_DELAY_MS = 0 => c"pg_kafka.test_forward_ack_delay_ms" {
        short: c"TEST ONLY: delay (ms) before the shadow forwarder returns each ForwardAck",
        long: c"Fault-injection knob for the shadow outbox in-flight gate E2E test \
          (issue #93): the network-thread forwarder sleeps this long after a \
          delivery before acking it to the DB thread. 0 (default) disables it. \
          Do not set in production. Reloadable via SIGHUP.",
        context: Sighup,
        range: (0, 30_000),
    }

    /// RB-1 test-only fault injection: the worker sleeps this long inside the
    /// transaction after a Produce handler and before the commit, widening the
    /// response-send to commit window so the E2E suite can check that an acked produce is
    /// already committed. Never set in production: it stalls the single DB thread.
    pub(crate) int TEST_PRE_COMMIT_DELAY_MS = 0 => c"pg_kafka.test_pre_commit_delay_ms" {
        short: c"TEST ONLY: delay (ms) between a Produce handler and its commit",
        long: c"Fault-injection knob for the response-after-commit barrier E2E test: \
          sleeps on the DB thread inside the transaction after a Produce handler \
          runs, before commit. 0 (default) disables it. Do not set in production. \
          Reloadable via SIGHUP.",
        context: Sighup,
        range: (0, 10_000),
    }

    /// Let a FetchRequest wait up to its max_wait_ms for data.
    bool ENABLE_LONG_POLLING = true => c"pg_kafka.enable_long_polling" {
        short: c"Enable long polling for FetchRequest",
        long: c"When enabled, FetchRequest will wait up to max_wait_ms for data. Default: true.",
        context: Postmaster,
    }

    /// Codec of outbound (fetched) record batches.
    string COMPRESSION_TYPE = None => c"pg_kafka.compression_type" {
        short: c"Compression type for FetchResponse encoding",
        long: c"Compression codec for outbound messages: none, gzip, snappy, lz4, zstd. Default: none.",
        context: Postmaster,
    }

    /// Log handler and transaction time per request batch (benchmarking).
    bool LOG_TIMING = false => c"pg_kafka.log_timing" {
        short: c"Enable timing instrumentation for performance research",
        long: c"When enabled, logs transaction and handler timing for each request. Use for benchmarking.",
        context: Postmaster,
    }

    /// Forward records to the external Kafka (checked per request, reloadable).
    pub(crate) bool SHADOW_MODE_ENABLED = DEFAULT_SHADOW_MODE_ENABLED => c"pg_kafka.shadow_mode_enabled" {
        short: c"Enable shadow mode for forwarding to external Kafka",
        long: c"When enabled, messages can be forwarded to an external Kafka cluster. Can be changed with pg_reload_conf().",
        context: Sighup,
    }

    /// Bootstrap servers of the external Kafka.
    pub(crate) string SHADOW_BOOTSTRAP_SERVERS = None => c"pg_kafka.shadow_bootstrap_servers" {
        short: c"External Kafka bootstrap servers",
        long: c"Comma-separated list of external Kafka brokers (e.g., 'kafka1:9092,kafka2:9092'). Reloadable with pg_reload_conf().",
        context: Sighup,
    }

    /// Security protocol towards the external Kafka.
    string SHADOW_SECURITY_PROTOCOL = None => c"pg_kafka.shadow_security_protocol" {
        short: c"Security protocol for external Kafka",
        long: c"Security protocol: SASL_SSL, SASL_PLAINTEXT, SSL, PLAINTEXT. Default: SASL_SSL. Reloadable with pg_reload_conf().",
        context: Sighup,
    }

    /// SASL mechanism towards the external Kafka.
    string SHADOW_SASL_MECHANISM = None => c"pg_kafka.shadow_sasl_mechanism" {
        short: c"SASL mechanism for external Kafka",
        long: c"SASL mechanism: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512. Default: PLAIN. Reloadable with pg_reload_conf() (the forwarder rebuilds its producer).",
        context: Sighup,
    }

    /// SASL username for the external Kafka. Locked down like the password (DR-21): it
    /// names the external-broker principal, so it is superuser-only and not in SHOW ALL.
    string SHADOW_SASL_USERNAME = None => c"pg_kafka.shadow_sasl_username" {
        short: c"SASL username for external Kafka",
        long: c"Username for SASL authentication to external Kafka. Reloadable with pg_reload_conf().",
        context: Sighup,
        flags: GucFlags::NO_SHOW_ALL | GucFlags::SUPERUSER_ONLY,
    }

    /// SASL password for the external Kafka. SEC-8: superuser-only and not in SHOW ALL
    /// / pg_settings, or any role could read the cleartext credential with SHOW.
    string SHADOW_SASL_PASSWORD = None => c"pg_kafka.shadow_sasl_password" {
        short: c"SASL password for external Kafka",
        long: c"Password for SASL authentication to external Kafka. Reloadable with pg_reload_conf().",
        context: Sighup,
        flags: GucFlags::NO_SHOW_ALL | GucFlags::SUPERUSER_ONLY,
    }

    /// CA bundle for TLS to the external Kafka (empty: system CAs).
    string SHADOW_SSL_CA_LOCATION = None => c"pg_kafka.shadow_ssl_ca_location" {
        short: c"SSL CA certificate location for external Kafka",
        long: c"Path to CA certificate file for SSL verification. Leave empty to use system CAs. Reloadable with pg_reload_conf().",
        context: Sighup,
    }

    /// Records per outbox claim and librdkafka batch.
    int SHADOW_BATCH_SIZE = DEFAULT_SHADOW_BATCH_SIZE => c"pg_kafka.shadow_batch_size" {
        short: c"Messages per batch for shadow forwarding",
        long: c"Max records the outbox claims per poll, and librdkafka's batch.num.messages (records per produce batch). Default: 1000. Reloadable with pg_reload_conf().",
        context: Sighup,
        range: (MIN_SHADOW_BATCH_SIZE, MAX_SHADOW_BATCH_SIZE),
    }

    /// librdkafka linger.ms of the shadow producer.
    int SHADOW_LINGER_MS = DEFAULT_SHADOW_LINGER_MS => c"pg_kafka.shadow_linger_ms" {
        short: c"Batching delay for shadow forwarding (milliseconds)",
        long: c"librdkafka linger.ms: how long to wait for more messages before sending a batch. Default: 10ms. Reloadable with pg_reload_conf().",
        context: Sighup,
        range: (MIN_SHADOW_LINGER_MS, MAX_SHADOW_LINGER_MS),
    }

    /// Base of the shadow retry back-off (librdkafka and the outbox).
    int SHADOW_RETRY_BACKOFF_MS = DEFAULT_SHADOW_RETRY_BACKOFF_MS => c"pg_kafka.shadow_retry_backoff_ms" {
        short: c"Retry backoff for shadow forwarding (milliseconds)",
        long: c"librdkafka retry.backoff.ms, and the base of the outbox re-attempt backoff after a failed delivery (doubling per failure, capped at 30 s). Default: 100ms. Reloadable with pg_reload_conf().",
        context: Sighup,
        range: (MIN_SHADOW_RETRY_BACKOFF_MS, MAX_SHADOW_RETRY_BACKOFF_MS),
    }

    /// Failed delivery attempts after which an outbox row is dead-lettered.
    int SHADOW_MAX_RETRIES = DEFAULT_SHADOW_MAX_RETRIES => c"pg_kafka.shadow_max_retries" {
        short: c"Failed delivery attempts after which a shadow outbox row is dead-lettered",
        long: c"A record whose forward failed this many times stays pending in kafka.shadow_tracking but is not retried (re-queue it with kafka.replay_shadow_messages). Raising it revives dead letters below the new cap. Default: 10. Reloadable with pg_reload_conf().",
        context: Sighup,
        range: (MIN_SHADOW_MAX_RETRIES, MAX_SHADOW_MAX_RETRIES),
    }

    /// Count forwarded, failed and sampled-out records in kafka.shadow_metrics.
    bool SHADOW_METRICS_ENABLED = DEFAULT_SHADOW_METRICS_ENABLED => c"pg_kafka.shadow_metrics_enabled" {
        short: c"Enable shadow mode metrics",
        long: c"When enabled, the worker counts forwarded, failed and sampled-out records in kafka.shadow_metrics. Default: true. Reloadable with pg_reload_conf().",
        context: Sighup,
    }

    /// RV-6: when true, external-primary topics keep local reads suppressed even after
    /// a forward is dead-lettered (the record is accepted as lost). When false, a
    /// dead-lettered external-primary topic falls back to local reads, so a record is never
    /// readable nowhere.
    pub(crate) bool SHADOW_EXTERNAL_PRIMARY_STRICT = false => c"pg_kafka.shadow_external_primary_strict" {
        short: c"Keep external-primary topics fully read-suppressed even when a forward is dead-lettered",
        long: c"When true, external-primary topics never fall back to local reads (external-only-strict; a dead-lettered record is accepted as lost). When false (default), a dead-lettered external-primary topic serves the record locally so it is never readable nowhere (RV-6).",
        context: Sighup,
    }

    /// Shadow mode license key (`sponsor_id:token` or `eval`); superuser-only.
    string SHADOW_LICENSE_KEY = None => c"pg_kafka.shadow_license_key" {
        short: c"License key for Shadow Mode production use",
        long: c"Format: 'sponsor_id:token' or 'eval' for evaluation. Obtain from https://github.com/sponsors/RTrentJones",
        context: Postmaster,
        flags: GucFlags::NO_SHOW_ALL | GucFlags::SUPERUSER_ONLY,
    }

    /// How often the worker reloads GUCs and the shadow configuration without a SIGHUP.
    pub(crate) int CONFIG_RELOAD_INTERVAL_MS = DEFAULT_CONFIG_RELOAD_MS => c"pg_kafka.config_reload_interval_ms" {
        short: c"Configuration reload interval in milliseconds",
        long: c"How often to reload GUCs and shadow config from the database. Default 30000ms (30s). Tests can set to 1000-2000ms for faster iteration. Reloadable with pg_reload_conf().",
        context: Sighup,
        range: (MIN_CONFIG_RELOAD_MS, MAX_CONFIG_RELOAD_MS),
    }

    /// How long a sync-mode shadow produce response waits for the external ack.
    int SHADOW_SYNC_TIMEOUT_MS = DEFAULT_SHADOW_SYNC_TIMEOUT_MS => c"pg_kafka.shadow_sync_timeout_ms" {
        short: c"How long a sync-mode shadow produce response waits for the external ack (milliseconds)",
        long: c"The response of a produce to a sync_mode = 'sync' topic is released when its records are delivered to the external cluster or after this long (the record stays in the outbox and is retried). Only that response waits, never the worker. 0 = do not hold sync responses. Default: 5000. Reloadable with pg_reload_conf().",
        context: Sighup,
        range: (MIN_SHADOW_SYNC_TIMEOUT_MS, MAX_SHADOW_SYNC_TIMEOUT_MS),
    }

    /// `lock_timeout` of every worker transaction (applied by the worker after each
    /// reload); 0 waits forever.
    pub(crate) int WORKER_LOCK_TIMEOUT_MS = DEFAULT_WORKER_LOCK_TIMEOUT_MS => c"pg_kafka.worker_lock_timeout_ms" {
        short: c"lock_timeout of the pg_kafka background worker, in milliseconds (0 = wait forever)",
        long: c"Bounds every lock wait of the worker's transactions, so one session holding a lock the broker needs fails one request batch (retriable error) instead of wedging the single database thread. Default 5000. Reloadable with pg_reload_conf().",
        context: Sighup,
        range: (0, MAX_WORKER_LOCK_TIMEOUT_MS),
        flags: GucFlags::UNIT_MS,
    }
}

/// Register every pg_kafka GUC (called once from `_PG_init`).
pub fn init() {
    register_declared_gucs();
    // pg_kafka.run_as_role (least privilege, storage::privileges).
    crate::kafka::storage::privileges::register_gucs();
    // Listener security protocols, TLS files, SASL mechanisms, connection limits.
    crate::kafka::auth::settings::register_gucs();
    // Partition, group-size and group-count caps (kafka::limits).
    crate::kafka::limits::register_gucs();
    // Kafka's transaction.max.timeout.ms; the setting lives with the InitProducerId handler.
    GucRegistry::define_int_guc(
        c"pg_kafka.transaction_max_timeout_ms",
        c"Largest transaction timeout a transactional producer may request (milliseconds)",
        c"InitProducerId with a transaction_timeout_ms <= 0 or above this value is refused with INVALID_TRANSACTION_TIMEOUT, as Kafka's transaction.max.timeout.ms. Default 900000 (15 minutes). Reloadable.",
        &crate::kafka::handlers::TRANSACTION_MAX_TIMEOUT_MS,
        1,
        i32::MAX,
        GucContext::Sighup,
        GucFlags::default(),
    );
    pgrx::log!("pg_kafka GUC parameters initialized (including shadow mode)");
}

/// The value of a string GUC, `None` when unset.
#[cfg(not(test))]
fn guc_string(setting: &pgrx::GucSetting<Option<std::ffi::CString>>) -> Option<String> {
    setting
        .get()
        .as_deref()
        .map(|c| c.to_string_lossy().into_owned())
}

/// An integer millisecond GUC value as a `Duration`; a negative value (which no GUC
/// range allows) is zero.
pub fn guc_millis(ms: i32) -> Duration {
    Duration::from_millis(u64::try_from(ms).unwrap_or(0))
}

/// The shadow outbox GUCs as the DB thread uses them (read per use: all are Sighup, so a
/// reload takes effect at once).
pub fn shadow_outbox_settings() -> crate::kafka::shadow::outbox::OutboxSettings {
    crate::kafka::shadow::outbox::OutboxSettings {
        max_retries: i64::from(SHADOW_MAX_RETRIES.get()),
        retry_backoff_ms: i64::from(SHADOW_RETRY_BACKOFF_MS.get()),
        batch_size: i64::from(SHADOW_BATCH_SIZE.get()),
        metrics_enabled: SHADOW_METRICS_ENABLED.get(),
        sync_timeout_ms: u64::try_from(SHADOW_SYNC_TIMEOUT_MS.get()).unwrap_or(0),
    }
}

/// A snapshot of the pg_kafka GUCs the worker and the network thread read
/// ([`crate::kafka::RuntimeContext`] holds the current one).
pub struct Config {
    pub port: i32,
    pub host: String,
    pub database: String,
    pub log_connections: bool,
    pub shutdown_timeout_ms: i32,
    pub default_partitions: i32,
    /// Polling interval for long polling fallback (milliseconds)
    pub fetch_poll_interval_ms: i32,
    /// Whether long polling is enabled
    pub enable_long_polling: bool,
    /// Compression type for FetchResponse encoding (none, gzip, snappy, lz4, zstd)
    pub compression_type: String,
    /// Enable timing instrumentation for performance research
    pub log_timing: bool,
    /// Whether shadow mode is enabled globally
    pub shadow_mode_enabled: bool,
    /// External Kafka bootstrap servers (e.g., "kafka1:9092,kafka2:9092")
    pub shadow_bootstrap_servers: String,
    /// Security protocol for external Kafka (SASL_SSL, SASL_PLAINTEXT, SSL, PLAINTEXT)
    pub shadow_security_protocol: String,
    /// SASL mechanism for external Kafka (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)
    pub shadow_sasl_mechanism: String,
    /// SASL username for external Kafka
    pub shadow_sasl_username: String,
    /// SASL password for external Kafka
    pub shadow_sasl_password: String,
    /// SSL CA certificate location for external Kafka
    pub shadow_ssl_ca_location: String,
    /// Messages per batch for shadow forwarding
    pub shadow_batch_size: i32,
    /// Batching delay in milliseconds
    pub shadow_linger_ms: i32,
    /// Retry backoff in milliseconds
    pub shadow_retry_backoff_ms: i32,
    /// Maximum retries per message
    pub shadow_max_retries: i32,
    /// Whether shadow metrics are enabled
    pub shadow_metrics_enabled: bool,
    /// License key for Shadow Mode production use: "sponsor_id:token", or "eval" for
    /// evaluation.
    pub shadow_license_key: String,
    /// Test only: `pg_kafka.test_forward_ack_delay_ms` (see that GUC).
    pub test_forward_ack_delay_ms: i32,
}

/// Debug output that redacts the credentials.
impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("port", &self.port)
            .field("host", &self.host)
            .field("database", &self.database)
            .field("log_connections", &self.log_connections)
            .field("shutdown_timeout_ms", &self.shutdown_timeout_ms)
            .field("default_partitions", &self.default_partitions)
            .field("fetch_poll_interval_ms", &self.fetch_poll_interval_ms)
            .field("enable_long_polling", &self.enable_long_polling)
            .field("compression_type", &self.compression_type)
            .field("log_timing", &self.log_timing)
            .field("shadow_mode_enabled", &self.shadow_mode_enabled)
            .field("shadow_bootstrap_servers", &self.shadow_bootstrap_servers)
            .field("shadow_security_protocol", &self.shadow_security_protocol)
            .field("shadow_sasl_mechanism", &self.shadow_sasl_mechanism)
            // REDACT sensitive credentials to prevent log exposure
            .field("shadow_sasl_username", &"[REDACTED]")
            .field("shadow_sasl_password", &"[REDACTED]")
            .field("shadow_ssl_ca_location", &self.shadow_ssl_ca_location)
            .field("shadow_batch_size", &self.shadow_batch_size)
            .field("shadow_linger_ms", &self.shadow_linger_ms)
            .field("shadow_retry_backoff_ms", &self.shadow_retry_backoff_ms)
            .field("shadow_max_retries", &self.shadow_max_retries)
            .field("shadow_metrics_enabled", &self.shadow_metrics_enabled)
            .field("shadow_license_key", &"[REDACTED]")
            .field("test_forward_ack_delay_ms", &self.test_forward_ack_delay_ms)
            .finish()
    }
}

impl Config {
    /// The current values of the GUCs.
    #[cfg(not(test))]
    pub fn load() -> Self {
        Config {
            port: PORT.get(),
            host: normalize_host(guc_string(&HOST).as_deref()),
            database: guc_string(&DATABASE).unwrap_or_else(|| DEFAULT_DATABASE.to_string()),
            log_connections: LOG_CONNECTIONS.get(),
            shutdown_timeout_ms: SHUTDOWN_TIMEOUT_MS.get(),
            default_partitions: DEFAULT_PARTITIONS.get(),
            fetch_poll_interval_ms: FETCH_POLL_INTERVAL_MS.get(),
            enable_long_polling: ENABLE_LONG_POLLING.get(),
            compression_type: guc_string(&COMPRESSION_TYPE)
                .unwrap_or_else(|| DEFAULT_COMPRESSION_TYPE.to_string()),
            log_timing: LOG_TIMING.get(),
            shadow_mode_enabled: SHADOW_MODE_ENABLED.get(),
            shadow_bootstrap_servers: guc_string(&SHADOW_BOOTSTRAP_SERVERS)
                .unwrap_or_else(|| DEFAULT_SHADOW_BOOTSTRAP_SERVERS.to_string()),
            shadow_security_protocol: guc_string(&SHADOW_SECURITY_PROTOCOL)
                .unwrap_or_else(|| DEFAULT_SHADOW_SECURITY_PROTOCOL.to_string()),
            shadow_sasl_mechanism: guc_string(&SHADOW_SASL_MECHANISM)
                .unwrap_or_else(|| DEFAULT_SHADOW_SASL_MECHANISM.to_string()),
            shadow_sasl_username: guc_string(&SHADOW_SASL_USERNAME).unwrap_or_default(),
            shadow_sasl_password: guc_string(&SHADOW_SASL_PASSWORD).unwrap_or_default(),
            shadow_ssl_ca_location: guc_string(&SHADOW_SSL_CA_LOCATION).unwrap_or_default(),
            shadow_batch_size: SHADOW_BATCH_SIZE.get(),
            shadow_linger_ms: SHADOW_LINGER_MS.get(),
            shadow_retry_backoff_ms: SHADOW_RETRY_BACKOFF_MS.get(),
            shadow_max_retries: SHADOW_MAX_RETRIES.get(),
            shadow_metrics_enabled: SHADOW_METRICS_ENABLED.get(),
            shadow_license_key: guc_string(&SHADOW_LICENSE_KEY).unwrap_or_default(),
            test_forward_ack_delay_ms: TEST_FORWARD_ACK_DELAY_MS.get(),
        }
    }

    /// Unit tests (no Postgres): the defaults, with the test host and the eval license.
    #[cfg(test)]
    pub fn load() -> Self {
        Config {
            port: DEFAULT_KAFKA_PORT,
            host: TEST_HOST.to_string(),
            database: DEFAULT_DATABASE.to_string(),
            log_connections: false,
            shutdown_timeout_ms: DEFAULT_SHUTDOWN_TIMEOUT_MS,
            default_partitions: DEFAULT_TOPIC_PARTITIONS,
            fetch_poll_interval_ms: DEFAULT_FETCH_POLL_INTERVAL_MS,
            enable_long_polling: true,
            compression_type: DEFAULT_COMPRESSION_TYPE.to_string(),
            log_timing: false,
            shadow_mode_enabled: DEFAULT_SHADOW_MODE_ENABLED,
            shadow_bootstrap_servers: DEFAULT_SHADOW_BOOTSTRAP_SERVERS.to_string(),
            shadow_security_protocol: DEFAULT_SHADOW_SECURITY_PROTOCOL.to_string(),
            shadow_sasl_mechanism: DEFAULT_SHADOW_SASL_MECHANISM.to_string(),
            shadow_sasl_username: String::new(),
            shadow_sasl_password: String::new(),
            shadow_ssl_ca_location: String::new(),
            shadow_batch_size: DEFAULT_SHADOW_BATCH_SIZE,
            shadow_linger_ms: DEFAULT_SHADOW_LINGER_MS,
            shadow_retry_backoff_ms: DEFAULT_SHADOW_RETRY_BACKOFF_MS,
            shadow_max_retries: DEFAULT_SHADOW_MAX_RETRIES,
            shadow_metrics_enabled: DEFAULT_SHADOW_METRICS_ENABLED,
            shadow_license_key: "eval".to_string(),
            test_forward_ack_delay_ms: 0,
        }
    }
}

/// The bind host from the `pg_kafka.host` GUC, trimmed. Unset, empty or all-whitespace means the
/// default (`127.0.0.1`, loopback only): an empty host would reach the bind as `":9092"`, which
/// does not resolve.
pub fn normalize_host(raw: Option<&str>) -> String {
    match raw.map(str::trim) {
        Some(h) if !h.is_empty() => h.to_string(),
        _ => DEFAULT_HOST.to_string(),
    }
}

/// SEC-7: whether `host` binds a Kafka listener to a non-loopback interface, i.e. the listener
/// is reachable from other machines. A listener that does not authenticate clients (PLAINTEXT,
/// SSL without client certificates) and has no ACLs then gives unauthenticated read/write to
/// anyone who can reach the port, so the listener security settings warn about it at worker
/// start (`SecuritySettings::warnings`).
///
/// Loopback (`127.0.0.0/8`, `::1`, `localhost`) is not exposed; `0.0.0.0`/`::` ("all
/// interfaces") and any specific LAN/public address are. An empty host is treated as exposed to
/// err on the safe side ([`normalize_host`] maps it to the loopback default before it gets
/// here).
pub fn bind_is_publicly_exposed(host: &str) -> bool {
    // Accept the bracketed IPv6 form (`[::1]`) a host:port-style config might carry.
    let h = host
        .strip_prefix('[')
        .and_then(|s| s.strip_suffix(']'))
        .unwrap_or(host);
    match h.parse::<std::net::IpAddr>() {
        // Loopback (the whole `127.0.0.0/8` block, `::1`) is the only not-exposed family. A specific
        // LAN/public address, or `0.0.0.0` / `::` ("all interfaces", `is_unspecified`), is reachable.
        Ok(addr) => !addr.is_loopback(),
        // Not an IP literal (empty included, conservatively): exposed; only `localhost` is loopback.
        // Host names are case-insensitive, so `LOCALHOST` is loopback too.
        Err(_) => !h.eq_ignore_ascii_case("localhost"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The GUC bounds and defaults are consistent (checked at compile time).
    const _: () = {
        assert!(MIN_SHADOW_BATCH_SIZE <= DEFAULT_SHADOW_BATCH_SIZE);
        assert!(DEFAULT_SHADOW_BATCH_SIZE <= MAX_SHADOW_BATCH_SIZE);
        assert!(MIN_SHADOW_LINGER_MS <= DEFAULT_SHADOW_LINGER_MS);
        assert!(DEFAULT_SHADOW_LINGER_MS <= MAX_SHADOW_LINGER_MS);
        assert!(MIN_SHADOW_RETRY_BACKOFF_MS <= DEFAULT_SHADOW_RETRY_BACKOFF_MS);
        assert!(DEFAULT_SHADOW_RETRY_BACKOFF_MS <= MAX_SHADOW_RETRY_BACKOFF_MS);
        assert!(MIN_SHADOW_MAX_RETRIES <= DEFAULT_SHADOW_MAX_RETRIES);
        assert!(DEFAULT_SHADOW_MAX_RETRIES <= MAX_SHADOW_MAX_RETRIES);
        assert!(MIN_FETCH_POLL_INTERVAL_MS <= DEFAULT_FETCH_POLL_INTERVAL_MS);
        assert!(DEFAULT_FETCH_POLL_INTERVAL_MS <= MAX_FETCH_POLL_INTERVAL_MS);
        assert!(MIN_CONFIG_RELOAD_MS <= DEFAULT_CONFIG_RELOAD_MS);
        assert!(DEFAULT_CONFIG_RELOAD_MS <= MAX_CONFIG_RELOAD_MS);
        assert!(MIN_SHUTDOWN_TIMEOUT_MS > 0);
        assert!(DEFAULT_SHUTDOWN_TIMEOUT_MS <= MAX_SHUTDOWN_TIMEOUT_MS);
        assert!(DEFAULT_WORKER_LOCK_TIMEOUT_MS <= MAX_WORKER_LOCK_TIMEOUT_MS);
    };

    #[test]
    fn test_config_load_returns_the_defaults() {
        let config = Config::load();
        assert_eq!(config.port, DEFAULT_KAFKA_PORT);
        assert_eq!(config.port, 9092);
        assert_eq!(config.host, TEST_HOST);
        assert_eq!(config.host, "localhost");
        assert_eq!(config.database, DEFAULT_DATABASE);
        assert_eq!(config.database, "postgres");
        assert_eq!(config.default_partitions, DEFAULT_TOPIC_PARTITIONS);
        assert_eq!(config.default_partitions, 1);
        assert_eq!(config.compression_type, DEFAULT_COMPRESSION_TYPE);
        assert_eq!(config.compression_type, "none");
        assert!(config.enable_long_polling);
        assert_eq!(
            config.fetch_poll_interval_ms,
            DEFAULT_FETCH_POLL_INTERVAL_MS
        );
        assert_eq!(config.fetch_poll_interval_ms, 1000);
        assert_eq!(config.shutdown_timeout_ms, DEFAULT_SHUTDOWN_TIMEOUT_MS);
        assert_eq!(config.shutdown_timeout_ms, 5000);
        assert!(!config.log_connections);
        assert!(!config.log_timing);
    }

    #[test]
    fn test_config_load_returns_the_shadow_defaults() {
        let config = Config::load();
        assert_eq!(config.shadow_mode_enabled, DEFAULT_SHADOW_MODE_ENABLED);
        assert!(!config.shadow_mode_enabled);
        assert_eq!(config.shadow_batch_size, DEFAULT_SHADOW_BATCH_SIZE);
        assert_eq!(config.shadow_batch_size, 1000);
        assert_eq!(config.shadow_linger_ms, DEFAULT_SHADOW_LINGER_MS);
        assert_eq!(config.shadow_linger_ms, 10);
        assert_eq!(
            config.shadow_retry_backoff_ms,
            DEFAULT_SHADOW_RETRY_BACKOFF_MS
        );
        assert_eq!(config.shadow_retry_backoff_ms, 100);
        assert_eq!(config.shadow_max_retries, DEFAULT_SHADOW_MAX_RETRIES);
        assert_eq!(config.shadow_max_retries, 10);
        assert_eq!(
            config.shadow_security_protocol,
            DEFAULT_SHADOW_SECURITY_PROTOCOL
        );
        assert_eq!(config.shadow_security_protocol, "SASL_SSL");
        assert_eq!(config.shadow_sasl_mechanism, DEFAULT_SHADOW_SASL_MECHANISM);
        assert_eq!(config.shadow_sasl_mechanism, "PLAIN");
        assert_eq!(
            config.shadow_metrics_enabled,
            DEFAULT_SHADOW_METRICS_ENABLED
        );
        assert!(config.shadow_metrics_enabled);
        assert_eq!(config.shadow_sasl_username, "");
        assert_eq!(config.shadow_sasl_password, "");
        assert_eq!(config.shadow_ssl_ca_location, "");
        assert_eq!(
            config.shadow_bootstrap_servers,
            DEFAULT_SHADOW_BOOTSTRAP_SERVERS
        );
        assert_eq!(config.shadow_bootstrap_servers, "");
        // Tests run in eval mode.
        assert_eq!(config.shadow_license_key, "eval");
    }

    #[test]
    fn test_config_multiple_loads_consistent() {
        let config1 = Config::load();
        let config2 = Config::load();
        assert_eq!(config1.port, config2.port);
        assert_eq!(config1.host, config2.host);
        assert_eq!(config1.shadow_mode_enabled, config2.shadow_mode_enabled);
        assert_eq!(config1.shadow_batch_size, config2.shadow_batch_size);
    }

    #[test]
    fn test_config_debug_redacts_credentials() {
        let mut config = Config::load();
        config.shadow_sasl_username = "kafka-user".into();
        config.shadow_sasl_password = "hunter2".into();
        config.shadow_license_key = "sponsor:secret".into();
        let shown = format!("{config:?}");
        for secret in ["kafka-user", "hunter2", "sponsor:secret"] {
            assert!(!shown.contains(secret), "{secret} leaked: {shown}");
        }
        assert!(shown.contains("[REDACTED]"));
    }

    #[test]
    fn test_guc_bounds() {
        // (name, actual, expected)
        let bounds = [
            ("MIN_SHADOW_BATCH_SIZE", MIN_SHADOW_BATCH_SIZE, 1),
            ("MAX_SHADOW_BATCH_SIZE", MAX_SHADOW_BATCH_SIZE, 100_000),
            ("MIN_SHADOW_LINGER_MS", MIN_SHADOW_LINGER_MS, 0),
            ("MAX_SHADOW_LINGER_MS", MAX_SHADOW_LINGER_MS, 60_000),
            (
                "MIN_SHADOW_RETRY_BACKOFF_MS",
                MIN_SHADOW_RETRY_BACKOFF_MS,
                10,
            ),
            (
                "MAX_SHADOW_RETRY_BACKOFF_MS",
                MAX_SHADOW_RETRY_BACKOFF_MS,
                60_000,
            ),
            ("MIN_SHADOW_MAX_RETRIES", MIN_SHADOW_MAX_RETRIES, 0),
            ("MAX_SHADOW_MAX_RETRIES", MAX_SHADOW_MAX_RETRIES, 100),
            ("MIN_FETCH_POLL_INTERVAL_MS", MIN_FETCH_POLL_INTERVAL_MS, 10),
            (
                "MAX_FETCH_POLL_INTERVAL_MS",
                MAX_FETCH_POLL_INTERVAL_MS,
                60_000,
            ),
            ("MIN_CONFIG_RELOAD_MS", MIN_CONFIG_RELOAD_MS, 100),
            ("MAX_CONFIG_RELOAD_MS", MAX_CONFIG_RELOAD_MS, 300_000),
            ("MIN_SHUTDOWN_TIMEOUT_MS", MIN_SHUTDOWN_TIMEOUT_MS, 100),
        ];
        for (name, actual, expected) in bounds {
            assert_eq!(actual, expected, "{name}");
        }
    }

    #[test]
    fn test_guc_millis_maps_negative_to_zero() {
        assert_eq!(guc_millis(1500), Duration::from_millis(1500));
        assert_eq!(guc_millis(0), Duration::ZERO);
        assert_eq!(guc_millis(-1), Duration::ZERO);
    }

    #[test]
    fn test_bind_all_interfaces_is_exposed() {
        // "all interfaces" forms — reachable from other hosts.
        assert!(bind_is_publicly_exposed("0.0.0.0"));
        assert!(bind_is_publicly_exposed("::"));
        assert!(bind_is_publicly_exposed("[::]"));
        // An empty host is treated as exposed (conservative); normalize_host never passes one.
        assert!(bind_is_publicly_exposed(""));
    }

    #[test]
    fn test_default_bind_is_not_exposed() {
        // An unset pg_kafka.host binds loopback only, so the no-auth listener is not reachable
        // from other machines out of the box (and the SEC-7 startup warning stays quiet).
        assert!(!bind_is_publicly_exposed(&normalize_host(None)));
        assert_eq!(normalize_host(None), "127.0.0.1");
    }

    #[test]
    fn test_bind_loopback_is_not_exposed() {
        assert!(!bind_is_publicly_exposed("127.0.0.1"));
        assert!(!bind_is_publicly_exposed("localhost"));
        assert!(!bind_is_publicly_exposed("::1"));
        assert!(!bind_is_publicly_exposed("[::1]"));
        // the whole 127.0.0.0/8 block is loopback
        assert!(!bind_is_publicly_exposed("127.0.0.53"));
    }

    #[test]
    fn test_bind_specific_address_is_exposed() {
        // A concrete LAN/public interface is network-reachable.
        assert!(bind_is_publicly_exposed("10.0.0.5"));
        assert!(bind_is_publicly_exposed("192.168.1.10"));
        assert!(bind_is_publicly_exposed("0.0.0.1"));
    }

    #[test]
    fn test_normalize_host_empty_means_default() {
        assert_eq!(normalize_host(None), DEFAULT_HOST);
        assert_eq!(normalize_host(Some("")), DEFAULT_HOST);
        assert_eq!(normalize_host(Some("  ")), DEFAULT_HOST);
        assert_eq!(normalize_host(Some(" 127.0.0.1 ")), "127.0.0.1");
    }

    #[test]
    fn test_bind_localhost_is_case_insensitive() {
        assert!(!bind_is_publicly_exposed("LOCALHOST"));
        assert!(!bind_is_publicly_exposed("LocalHost"));
    }

    #[test]
    fn test_bind_non_ip_hostname_defaults_to_exposed() {
        // A non-IP host that isn't "localhost" can't be proven loopback, so the security warning
        // errs toward exposed.
        assert!(bind_is_publicly_exposed("broker.internal"));
        assert!(bind_is_publicly_exposed("myhost"));
    }
}
