// Copyright (c) 2026 Robert Trent Jones.
// This file is part of the "Shadow Mode" feature of pg_kafka.
//
// Use of this source code for production purposes is governed by the
// Commercial License found in the LICENSE file in this directory.
// Development and evaluation use is permitted.
//
// GitHub Sponsors: https://github.com/sponsors/RTrentJones

//! Shadow mode Kafka producer
//!
//! This module provides a wrapper around rdkafka's FutureProducer configured
//! with SASL/SSL authentication for forwarding messages to external Kafka.
//!
//! ## Security
//!
//! SASL/SSL is required by default. Supported mechanisms:
//! - PLAIN (username/password over SSL)
//! - SCRAM-SHA-256
//! - SCRAM-SHA-512
//!
//! ## OpenSSL Warning
//!
//! This uses system OpenSSL via librdkafka. NEVER enable the `ssl-vendored`
//! feature as it will conflict with PostgreSQL's OpenSSL symbols and crash.

use super::config::ShadowConfig;
use super::error::{ShadowError, ShadowResult};
use super::ForwardRecord;
use rdkafka::bindings as rdsys;
use rdkafka::config::ClientConfig;
use rdkafka::error::RDKafkaErrorCode;
use rdkafka::message::{BorrowedHeaders, OwnedHeaders};
use rdkafka::producer::{DeliveryFuture, FutureProducer, FutureRecord, Producer, PurgeConfig};
use rdkafka::types::RDKafkaRespErr;
use rdkafka::util::Timeout;
use std::ptr::NonNull;
use std::sync::Arc;
use std::time::Duration;

/// librdkafka tuning for the shadow forwarding producer. Named here rather than
/// inlined as string literals at the `client_config.set` call sites so the intent
/// (and the idempotence invariants) are explicit.
///
/// Max in-flight requests per connection. librdkafka requires this to be ≤ 5 for
/// `enable.idempotence=true` (ordering guarantee), so 5 is the idempotent maximum.
const PRODUCER_MAX_IN_FLIGHT: &str = "5";
/// Per-request timeout (30 s).
const PRODUCER_REQUEST_TIMEOUT_MS: &str = "30000";
/// librdkafka's per-message timeout: 0, infinite. librdkafka never fails a
/// record by timeout on its own, because such a failure is not covered by
/// `enable.gapless.guarantee`: a record that times out alone leaves the later
/// records of its partition queued, and they would be delivered ahead of its
/// retry. The forwarder enforces [`DELIVERY_TIMEOUT`] itself instead, failing
/// every record outstanding on the producer at once (see `forwarder_task`).
const PRODUCER_MESSAGE_TIMEOUT_MS: &str = "0";
/// How long a record may stay undelivered after the forwarder received it
/// (waiting in its backlog for queue room, then on a producer) before the
/// forwarder fails it, together with every other record outstanding on that
/// producer. A record is handed to a producer only with at least a quarter of
/// it left (`forwarder_task::send_margin`, 30 s); one still in the backlog with
/// less left is answered there, so the forwarder's own queueing cannot time out
/// a healthy producer: as a failed attempt only if nothing older is still
/// outstanding on a live producer, otherwise not attempted (a queue full of
/// another partition's stuck records is not the record's failure, and that
/// producer times out by the older record's deadline, freeing the queue). One
/// outbox attempt therefore resolves (delivered, failed or not attempted)
/// within about this long of its dispatch, which the DB thread's
/// in-flight eviction (`INFLIGHT_EVICT_AFTER`, 150 s) relies on.
/// `pg_kafka.test_shadow_delivery_timeout_ms` shortens it for tests.
pub(crate) const DELIVERY_TIMEOUT: Duration = Duration::from_secs(120);
/// librdkafka's local queue, in KiB (its default is 1 GiB). The queue lives in
/// the listener worker's memory; 64 MiB holds far more than one claim of
/// records (whose size librdkafka caps at `message.max.bytes`, 1 MB). A full
/// queue holds back only the forwarder's later records (see `forwarder_task`).
const PRODUCER_QUEUE_MAX_KBYTES: &str = "65536";

/// librdkafka's default `retry.backoff.max.ms`, kept unless `retry.backoff.ms`
/// is larger (see [`build_producer_settings`]).
const LIBRDKAFKA_RETRY_BACKOFF_MAX_MS: i32 = 1000;
/// The most librdkafka's own retry backoff (`retry.backoff.ms` and
/// `retry.backoff.max.ms`) is set to, whatever `shadow_retry_backoff_ms` is:
/// librdkafka retries inside one delivery attempt, so two retriable errors in a
/// row (each up to a request timeout plus a backoff, 2 x 40 s) stay well inside
/// [`DELIVERY_TIMEOUT`]. With the GUC's 60 s maximum applied as is, they took
/// longer and the attempt timed out with its whole producer generation.
const LIBRDKAFKA_RETRY_BACKOFF_CAP_MS: i32 = 10_000;
/// `topic.metadata.refresh.fast.interval.ms`, librdkafka's documented default.
/// When a partition loses its leader, librdkafka queries metadata at once and
/// then re-arms the query with an exponential backoff from `retry.backoff.ms` up
/// to `retry.backoff.max.ms` (`rd_kafka_metadata_leader_query_tmr_cb`); this
/// setting is only the threshold of a new leader loss: it restarts the query at
/// once when the next one is more than this far away
/// (`rd_kafka_metadata_fast_leader_query`). Unset, it follows `retry.backoff.ms`
/// (up to 10 s here), so a leader loss during a long backoff would wait for the
/// scheduled query; pinned, it is noticed at once.
const LIBRDKAFKA_LEADER_QUERY_THRESHOLD_MS: &str = "100";

/// Assemble the ordered librdkafka settings for the shadow forwarding producer.
///
/// Pure — no client creation, no IO — so the security/idempotence invariants can
/// be unit-tested without a live broker (mirrors the `validate_sequence`
/// extraction in `storage/mod.rs`). [`ShadowProducer::create_producer`] applies
/// these to a `ClientConfig` and calls `.create()`.
///
/// What each shadow GUC maps to:
/// - `shadow_batch_size` (messages) → `batch.num.messages` (librdkafka's
///   per-batch message cap; `batch.size` would be bytes);
/// - `shadow_linger_ms` → `linger.ms`;
/// - `shadow_retry_backoff_ms` → `retry.backoff.ms`, capped at 10 s
///   ([`LIBRDKAFKA_RETRY_BACKOFF_CAP_MS`]: librdkafka's own retries inside one
///   delivery attempt; the outbox uses the uncapped value as the base of its
///   re-attempt backoff, see `ShadowStore`), and `retry.backoff.max.ms` to the
///   larger of that and librdkafka's default 1000 ms: librdkafka caps the
///   backoff at `retry.backoff.max.ms`, so a larger `retry.backoff.ms` alone
///   would be ignored (with only a configuration warning). The same two
///   settings space librdkafka's leader queries after a partition loses its
///   leader; `topic.metadata.refresh.fast.interval.ms`, the threshold at which a
///   new leader loss restarts that query at once, is pinned to librdkafka's
///   default instead of following the backoff
///   ([`LIBRDKAFKA_LEADER_QUERY_THRESHOLD_MS`]);
/// - `shadow_max_retries` is not a librdkafka setting: it is the outbox's
///   dead-letter cap (failed delivery attempts per record). Inside one attempt
///   librdkafka retries (its default `retries`, which is what idempotence
///   wants) until the record is delivered or the forwarder's
///   [`DELIVERY_TIMEOUT`] ends the attempt.
pub(crate) fn build_producer_settings(config: &ShadowConfig) -> Vec<(&'static str, String)> {
    // Basic configuration + security protocol are always set.
    // (SASL_SSL, SASL_PLAINTEXT, SSL, or PLAINTEXT for security.protocol.)
    let mut settings: Vec<(&'static str, String)> = vec![
        ("bootstrap.servers", config.bootstrap_servers.clone()),
        ("security.protocol", config.security_protocol.clone()),
    ];

    // SASL configuration (if using a SASL_* protocol)
    if config.security_protocol.starts_with("SASL") {
        settings.push(("sasl.mechanism", config.sasl_mechanism.clone()));

        // Empty credentials are omitted — librdkafka rejects empty SASL fields.
        if !config.sasl_username.is_empty() {
            settings.push(("sasl.username", config.sasl_username.clone()));
        }
        if !config.sasl_password.is_empty() {
            settings.push(("sasl.password", config.sasl_password.clone()));
        }
    }

    // SSL configuration (if using an *_SSL protocol)
    if config.security_protocol.ends_with("SSL") {
        if !config.ssl_ca_location.is_empty() {
            settings.push(("ssl.ca.location", config.ssl_ca_location.clone()));
        }
        // Enable hostname verification for security.
        let https = "https".to_string();
        settings.push(("ssl.endpoint.identification.algorithm", https));
    }

    // Producer performance settings (see the GUC mapping above).
    settings.push(("batch.num.messages", config.batch_size.to_string()));
    settings.push(("linger.ms", config.linger_ms.to_string()));
    let retry_backoff = config.retry_backoff_ms.min(LIBRDKAFKA_RETRY_BACKOFF_CAP_MS);
    settings.push(("retry.backoff.ms", retry_backoff.to_string()));
    settings.push((
        "retry.backoff.max.ms",
        retry_backoff
            .max(LIBRDKAFKA_RETRY_BACKOFF_MAX_MS)
            .to_string(),
    ));
    settings.push((
        "topic.metadata.refresh.fast.interval.ms",
        LIBRDKAFKA_LEADER_QUERY_THRESHOLD_MS.to_string(),
    ));

    // Compression (use none for maximum compatibility)
    settings.push(("compression.type", "none".to_string()));

    // The partitioner of the Java client (murmur2 over the key, random for a
    // null key), so a keyed record forwarded by pg_kafka lands in the same
    // external partition as the same record produced by a Java client: a
    // migration that cuts producers over keeps per-key placement. librdkafka's
    // default (CRC32) would scatter keys differently.
    settings.push(("partitioner", "murmur2_random".to_string()));

    // An idempotent, order-preserving producer. Many deliveries are
    // in flight at once (the forwarder pipelines), so ordering rests on the
    // idempotent producer: per-partition sequence numbers, acks=all, at most 5
    // requests in flight, librdkafka's own retries. `enable.gapless.guarantee`
    // turns any error that could leave a gap (a later record delivered while an
    // earlier one failed) into a fatal producer error that fails every
    // outstanding record instead; the forwarder then rebuilds the producer and
    // the outbox re-sends from the first undelivered offset of each partition,
    // so a record is never delivered after a later record of its local
    // partition. The guarantee does not cover records failed by
    // `message.timeout.ms`, so that is infinite and the forwarder's own
    // delivery timeout fails the whole producer instead. Duplicates are suppressed only within one producer session
    // (stable PID): a re-send that crosses a producer rebuild is at-least-once.
    let max_in_flight = PRODUCER_MAX_IN_FLIGHT.to_string();
    settings.push(("enable.idempotence", "true".to_string()));
    settings.push(("enable.gapless.guarantee", "true".to_string()));
    settings.push(("max.in.flight.requests.per.connection", max_in_flight));
    settings.push(("acks", "all".to_string()));

    // The deprecated api.version.* settings are deliberately not set: modern
    // brokers negotiate the API version and answer them with "Required
    // feature not supported by broker".
    let request_timeout = PRODUCER_REQUEST_TIMEOUT_MS.to_string();
    let message_timeout = PRODUCER_MESSAGE_TIMEOUT_MS.to_string();
    settings.push(("request.timeout.ms", request_timeout));
    settings.push(("message.timeout.ms", message_timeout));
    let queue_kbytes = PRODUCER_QUEUE_MAX_KBYTES.to_string();
    settings.push(("queue.buffering.max.kbytes", queue_kbytes));

    settings
}

/// Why [`ShadowProducer::enqueue`] did not accept a record.
#[derive(Debug)]
pub enum EnqueueError {
    /// librdkafka's local queue is full: back off and retry the same record.
    QueueFull,
    /// The record was refused (too large, unknown topic, fatal producer...).
    Refused(String),
}

/// Shadow producer for forwarding messages to external Kafka
pub struct ShadowProducer {
    /// The underlying rdkafka producer
    producer: FutureProducer,
    /// Configuration used to create this producer
    config: Arc<ShadowConfig>,
}

impl ShadowProducer {
    /// Create a new shadow producer from configuration
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Shadow mode is not enabled
    /// - Bootstrap servers are not configured
    /// - Producer creation fails (connection, auth, etc.)
    pub fn new(config: Arc<ShadowConfig>) -> ShadowResult<Self> {
        if !config.enabled {
            return Err(ShadowError::NotEnabled);
        }

        if config.bootstrap_servers.is_empty() {
            return Err(ShadowError::NotConfigured(
                "bootstrap.servers is empty".to_string(),
            ));
        }

        let producer = Self::create_producer(&config)?;

        Ok(Self { producer, config })
    }

    /// Create the rdkafka producer with SASL/SSL configuration.
    ///
    /// The librdkafka key/value assembly lives in the pure
    /// [`build_producer_settings`] free function so the security and idempotence
    /// invariants can be unit-tested without a live broker (mirrors the
    /// `validate_sequence` extraction pattern). This shell only applies the
    /// settings and calls `.create()`, which connects and is therefore E2E-only.
    fn create_producer(config: &ShadowConfig) -> ShadowResult<FutureProducer> {
        let mut client_config = ClientConfig::new();
        for (key, value) in build_producer_settings(config) {
            client_config.set(key, value);
        }
        client_config
            .create()
            .map_err(|e| ShadowError::ProducerError(format!("Failed to create producer: {e}")))
    }

    /// Hand one outbox record to librdkafka without waiting for its delivery
    /// (the forwarder keeps many deliveries in flight). The record keeps its key,
    /// value, headers and timestamp; `partition` is left to the partitioner (the
    /// external cluster re-partitions by key).
    ///
    /// Every byte of the record comes from a client, so nothing here may panic on
    /// it: rdkafka converts the topic name and header keys to C strings with an
    /// `unwrap`, so a topic name holding U+0000 is refused here and header keys go
    /// through [`owned_headers`], which passes each key's length.
    pub fn enqueue(&self, record: &ForwardRecord) -> Result<DeliveryFuture, EnqueueError> {
        if record.topic_name.contains('\0') {
            return Err(EnqueueError::Refused(format!(
                "the external topic name {:?} holds U+0000",
                record.topic_name
            )));
        }
        let mut future_record: FutureRecord<'_, [u8], [u8]> = FutureRecord::to(&record.topic_name);
        if let Some(k) = record.key.as_deref() {
            future_record = future_record.key(k);
        }
        if let Some(v) = record.value.as_deref() {
            future_record = future_record.payload(v);
        }
        if let Some(ts) = record.timestamp_ms {
            future_record = future_record.timestamp(ts);
        }
        if !record.headers.is_empty() {
            let headers = owned_headers(&record.headers).map_err(EnqueueError::Refused)?;
            future_record = future_record.headers(headers);
        }
        self.producer.send_result(future_record).map_err(|(e, _)| {
            if e.rdkafka_error_code() == Some(RDKafkaErrorCode::QueueFull) {
                EnqueueError::QueueFull
            } else {
                EnqueueError::Refused(e.to_string())
            }
        })
    }

    /// The producer's fatal error, if it has one (it then fails every record and
    /// must be rebuilt; see `enable.gapless.guarantee`).
    pub fn fatal_error(&self) -> Option<String> {
        self.producer
            .client()
            .fatal_error()
            .map(|(code, msg)| format!("{code:?}: {msg}"))
    }

    /// Fail every queued and in-flight record now (their delivery futures
    /// resolve with a purge error). Used when the producer is replaced.
    pub fn purge_outstanding(&self) {
        self.producer
            .purge(PurgeConfig::default().queue().inflight());
    }

    /// Serve librdkafka's delivery callbacks for a moment (the FutureProducer's
    /// own polling thread does this too; used while waiting on a full queue).
    pub fn poll(&self, timeout: Duration) {
        self.producer.poll(Timeout::After(timeout));
    }
}

/// `headers` as librdkafka record headers, in order, each key and value byte for byte.
///
/// rdkafka's `OwnedHeaders::insert` passes the key as a C string and panics on a key
/// holding U+0000, which Kafka allows (a header key is any UTF-8 string, and Kafka
/// 3.7 stores and serves such keys). librdkafka itself takes the key's length
/// (`rd_kafka_header_add`), stores that many bytes and writes them to the wire with
/// that length, so the headers are built in a list of librdkafka's own and copied
/// into an `OwnedHeaders` (`rd_kafka_headers_copy` re-adds each header with its
/// stored length).
pub(crate) fn owned_headers(headers: &[(String, Vec<u8>)]) -> Result<OwnedHeaders, String> {
    let mut list = NativeHeaders::new(headers.len())?;
    for (key, value) in headers {
        list.add(key.as_bytes(), value)?;
    }
    Ok(list.to_owned_headers())
}

/// An `rd_kafka_headers_t` owned by this value and destroyed with it.
struct NativeHeaders(NonNull<rdsys::rd_kafka_headers_t>);

/// What an empty header value points at: librdkafka copies zero bytes from it, and a
/// non-NULL pointer keeps the value empty rather than null.
const EMPTY_VALUE: &[u8] = &[0];

impl NativeHeaders {
    fn new(capacity: usize) -> Result<Self, String> {
        // SAFETY: `rd_kafka_headers_new` allocates a new, empty list and takes no
        // pointers; a NULL result (allocation failure) is refused below.
        let list = unsafe { rdsys::rd_kafka_headers_new(capacity) };
        NonNull::new(list)
            .map(Self)
            .ok_or_else(|| "librdkafka could not allocate the record headers".to_string())
    }

    fn add(&mut self, key: &[u8], value: &[u8]) -> Result<(), String> {
        let too_long = |_| "a record header is too long for librdkafka".to_string();
        let key_len = isize::try_from(key.len()).map_err(too_long)?;
        let value_len = isize::try_from(value.len()).map_err(too_long)?;
        let value_ptr = if value.is_empty() {
            EMPTY_VALUE.as_ptr()
        } else {
            value.as_ptr()
        };
        // SAFETY: `self.0` is a live, writable list owned by `self` (from
        // `rd_kafka_headers_new`, destroyed only on drop). `key` and `value_ptr` are
        // valid for reads of `key_len` and `value_len` bytes for the call;
        // librdkafka copies exactly that many bytes of each (it calls `strlen` only
        // for a size of -1) and keeps no pointer to them.
        let err = unsafe {
            rdsys::rd_kafka_header_add(
                self.0.as_ptr(),
                key.as_ptr().cast(),
                key_len,
                value_ptr.cast(),
                value_len,
            )
        };
        if err == RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR {
            Ok(())
        } else {
            Err(format!("librdkafka refused a record header: {err:?}"))
        }
    }

    /// A copy of the list as rdkafka's `OwnedHeaders`.
    fn to_owned_headers(&self) -> OwnedHeaders {
        // SAFETY: `BorrowedHeaders` is rdkafka's zero-sized view of an
        // `rd_kafka_headers_t`, made by casting the list's pointer (as rdkafka does
        // for a message's headers). The list is live and not written while the view
        // exists: `detach` only reads it, copying it with `rd_kafka_headers_copy`,
        // and the view ends before `self` can be dropped.
        let borrowed = unsafe { &*self.0.as_ptr().cast::<BorrowedHeaders>() };
        borrowed.detach()
    }
}

impl Drop for NativeHeaders {
    fn drop(&mut self) {
        // SAFETY: the list came from `rd_kafka_headers_new`, is owned by `self`
        // alone (`to_owned_headers` hands out a copy) and is destroyed only here.
        unsafe { rdsys::rd_kafka_headers_destroy(self.0.as_ptr()) };
    }
}

impl std::fmt::Debug for ShadowProducer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShadowProducer")
            .field("bootstrap_servers", &self.config.bootstrap_servers)
            .field("security_protocol", &self.config.security_protocol)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The native list behind `headers` (rdkafka's `BorrowedHeaders` is a cast of it).
    fn native(headers: &OwnedHeaders) -> *const rdsys::rd_kafka_headers_t {
        std::ptr::from_ref(headers.as_borrowed()).cast()
    }

    /// Whether librdkafka finds a header named exactly `name` (it compares the stored
    /// key length with `strlen(name)`, so `"a"` does not find a key `"a\0b"`), and the
    /// value of the last one.
    fn lookup(headers: &OwnedHeaders, name: &str) -> Option<Vec<u8>> {
        let name = std::ffi::CString::new(name).expect("a test name without NUL");
        let mut value: *const std::ffi::c_void = std::ptr::null();
        let mut size = 0usize;
        // SAFETY: `headers` is a live list; the out pointers are valid locals.
        let err = unsafe {
            rdsys::rd_kafka_header_get_last(
                native(headers),
                name.as_ptr(),
                &raw mut value,
                &raw mut size,
            )
        };
        (err == RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR).then(|| {
            // SAFETY: librdkafka returned a value of `size` bytes owned by `headers`.
            unsafe { std::slice::from_raw_parts(value.cast::<u8>(), size) }.to_vec()
        })
    }

    /// A header key holding U+0000 keeps all its bytes (rdkafka's `insert` panics on
    /// it); values keep theirs, NUL bytes and empty values included, in order.
    #[test]
    fn owned_headers_keep_a_key_holding_nul() {
        use rdkafka::message::Headers;
        let headers = owned_headers(&[
            ("a\0b".to_string(), b"\0v\0".to_vec()),
            ("k".to_string(), b"plain".to_vec()),
            ("e".to_string(), Vec::new()),
        ])
        .expect("headers build");
        assert_eq!(headers.count(), 3);
        // rdkafka reads a key as a C string, so it shows the key up to its NUL.
        let first = headers.get(0);
        assert_eq!((first.key, first.value), ("a", Some(&b"\0v\0"[..])));
        assert_eq!(
            lookup(&headers, "a"),
            None,
            "the key is stored with all 3 bytes"
        );
        assert_eq!(lookup(&headers, "k"), Some(b"plain".to_vec()));
        assert_eq!(
            lookup(&headers, "e"),
            Some(Vec::new()),
            "an empty value is not null"
        );
    }

    fn record(topic: &str, headers: Vec<(String, Vec<u8>)>) -> ForwardRecord {
        ForwardRecord {
            row: super::super::RowKey::new(1, 0, 0),
            topic_name: topic.to_string(),
            key: Some(b"k".to_vec()),
            value: Some(b"v".to_vec()),
            headers,
            timestamp_ms: None,
            restart: true,
        }
    }

    /// Nothing a client can put into a record panics in `enqueue`: a header key
    /// holding U+0000 is queued, a topic name holding one is refused.
    #[test]
    fn enqueue_does_not_panic_on_nul_in_a_header_key_or_topic_name() {
        let producer = ShadowProducer::new(Arc::new(ShadowConfig {
            enabled: true,
            bootstrap_servers: "127.0.0.1:1".to_string(),
            security_protocol: "PLAINTEXT".to_string(),
            ..Default::default()
        }))
        .expect("a producer builds without connecting");
        let nul_key = record("t", vec![("a\0b".to_string(), b"v".to_vec())]);
        assert!(producer.enqueue(&nul_key).is_ok());
        match producer.enqueue(&record("t\0x", Vec::new())) {
            Err(EnqueueError::Refused(e)) => assert!(e.contains("U+0000"), "{e}"),
            Ok(_) => panic!("a topic name holding U+0000 was queued"),
            Err(EnqueueError::QueueFull) => panic!("expected a refusal, got QueueFull"),
        }
        producer.purge_outstanding();
    }

    #[test]
    fn test_shadow_producer_not_enabled() {
        let config = Arc::new(ShadowConfig {
            enabled: false,
            bootstrap_servers: "localhost:9092".to_string(),
            ..Default::default()
        });

        let result = ShadowProducer::new(config);
        assert!(matches!(result, Err(ShadowError::NotEnabled)));
    }

    #[test]
    fn test_shadow_producer_no_bootstrap_servers() {
        let config = Arc::new(ShadowConfig {
            enabled: true,
            bootstrap_servers: String::new(),
            ..Default::default()
        });

        let result = ShadowProducer::new(config);
        assert!(matches!(result, Err(ShadowError::NotConfigured(_))));
    }

    #[test]
    fn test_shadow_config_default() {
        let config = ShadowConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.bootstrap_servers, "");
        assert_eq!(config.security_protocol, "SASL_SSL");
        assert_eq!(config.sasl_mechanism, "PLAIN");
    }

    /// True if the assembled settings contain `key` mapped exactly to `value`.
    fn has(settings: &[(&'static str, String)], key: &str, value: &str) -> bool {
        for (k, v) in settings {
            if *k == key && v.as_str() == value {
                return true;
            }
        }
        false
    }

    /// True if the assembled settings contain `key` with any value.
    fn has_key(settings: &[(&'static str, String)], key: &str) -> bool {
        settings.iter().any(|(k, _)| *k == key)
    }

    #[test]
    fn test_build_producer_settings_idempotence_invariants() {
        // A valid idempotent producer requires acks=all, a bounded in-flight
        // window (librdkafka caps this at 5 for enable.idempotence=true), and
        // idempotence enabled. These invariants must hold for any config.
        let config = ShadowConfig {
            enabled: true,
            bootstrap_servers: "kafka:9092".to_string(),
            security_protocol: "SASL_SSL".to_string(),
            ..Default::default()
        };
        let s = build_producer_settings(&config);
        assert!(has(&s, "enable.idempotence", "true"));
        assert!(has(&s, "max.in.flight.requests.per.connection", "5"));
        assert!(has(&s, "acks", "all"));
        assert!(has(&s, "compression.type", "none"));
    }

    #[test]
    fn test_build_producer_settings_leave_retries_to_librdkafka() {
        // shadow_max_retries is the outbox dead-letter cap. librdkafka keeps
        // its own retries, as idempotence wants, and the
        // ordering guard is on.
        let config = ShadowConfig {
            enabled: true,
            bootstrap_servers: "kafka:9092".to_string(),
            ..Default::default()
        };
        let s = build_producer_settings(&config);
        assert!(!has_key(&s, "retries"));
        assert!(has(&s, "enable.gapless.guarantee", "true"));
        // The local queue is bounded explicitly, not librdkafka's 1 GiB default.
        assert!(has(&s, "queue.buffering.max.kbytes", "65536"));
        assert!(has(&s, "partitioner", "murmur2_random"));
    }

    #[test]
    fn test_build_producer_settings_sasl_only_for_sasl_protocol() {
        // SASL keys are present for SASL* protocols and absent otherwise.
        let sasl = ShadowConfig {
            enabled: true,
            bootstrap_servers: "kafka:9092".to_string(),
            security_protocol: "SASL_SSL".to_string(),
            sasl_mechanism: "SCRAM-SHA-256".to_string(),
            sasl_username: "user".to_string(),
            sasl_password: "pass".to_string(),
            ..Default::default()
        };
        let s = build_producer_settings(&sasl);
        assert!(has(&s, "sasl.mechanism", "SCRAM-SHA-256"));
        assert!(has(&s, "sasl.username", "user"));
        assert!(has(&s, "sasl.password", "pass"));

        let plaintext = ShadowConfig {
            enabled: true,
            bootstrap_servers: "kafka:9092".to_string(),
            security_protocol: "PLAINTEXT".to_string(),
            ..Default::default()
        };
        let s = build_producer_settings(&plaintext);
        assert!(!has_key(&s, "sasl.mechanism"));
        assert!(!has_key(&s, "sasl.username"));
        assert!(!has_key(&s, "sasl.password"));
    }

    #[test]
    fn test_build_producer_settings_empty_sasl_credentials_omitted() {
        // Empty username/password must not be emitted (librdkafka rejects empty
        // SASL fields); the mechanism is still set for a SASL protocol.
        let config = ShadowConfig {
            enabled: true,
            bootstrap_servers: "kafka:9092".to_string(),
            security_protocol: "SASL_PLAINTEXT".to_string(),
            sasl_username: String::new(),
            sasl_password: String::new(),
            ..Default::default()
        };
        let s = build_producer_settings(&config);
        assert!(has_key(&s, "sasl.mechanism"));
        assert!(!has_key(&s, "sasl.username"));
        assert!(!has_key(&s, "sasl.password"));
    }

    #[test]
    fn test_build_producer_settings_ssl_endpoint_only_for_ssl() {
        // Hostname verification is set only for *SSL protocols.
        let ssl = ShadowConfig {
            enabled: true,
            bootstrap_servers: "kafka:9092".to_string(),
            security_protocol: "SSL".to_string(),
            ..Default::default()
        };
        let key = "ssl.endpoint.identification.algorithm";
        assert!(has(&build_producer_settings(&ssl), key, "https"));

        let plaintext = ShadowConfig {
            enabled: true,
            bootstrap_servers: "kafka:9092".to_string(),
            security_protocol: "PLAINTEXT".to_string(),
            ..Default::default()
        };
        assert!(!has_key(&build_producer_settings(&plaintext), key));
    }

    #[test]
    fn test_build_producer_settings_ca_location_conditional() {
        // ssl.ca.location is emitted only when configured.
        let with_ca = ShadowConfig {
            enabled: true,
            bootstrap_servers: "kafka:9092".to_string(),
            security_protocol: "SSL".to_string(),
            ssl_ca_location: "/etc/ssl/ca.pem".to_string(),
            ..Default::default()
        };
        let s = build_producer_settings(&with_ca);
        assert!(has(&s, "ssl.ca.location", "/etc/ssl/ca.pem"));

        let without_ca = ShadowConfig {
            enabled: true,
            bootstrap_servers: "kafka:9092".to_string(),
            security_protocol: "SSL".to_string(),
            ssl_ca_location: String::new(),
            ..Default::default()
        };
        let s = build_producer_settings(&without_ca);
        assert!(!has_key(&s, "ssl.ca.location"));
    }

    #[test]
    fn test_build_producer_settings_maps_config_values() {
        // Bootstrap servers, timeouts, and performance knobs carry through.
        let config = ShadowConfig {
            enabled: true,
            bootstrap_servers: "a:9092,b:9092".to_string(),
            security_protocol: "PLAINTEXT".to_string(),
            batch_size: 4096,
            linger_ms: 25,
            retry_backoff_ms: 200,
            ..Default::default()
        };
        let s = build_producer_settings(&config);
        assert!(has(&s, "bootstrap.servers", "a:9092,b:9092"));
        assert!(has(&s, "security.protocol", "PLAINTEXT"));
        // shadow_batch_size counts messages: batch.num.messages, never the
        // byte-sized batch.size.
        assert!(has(&s, "batch.num.messages", "4096"));
        assert!(!has_key(&s, "batch.size"));
        assert!(has(&s, "linger.ms", "25"));
        assert!(has(&s, "retry.backoff.ms", "200"));
        // librdkafka's default cap, above the backoff.
        assert!(has(&s, "retry.backoff.max.ms", "1000"));
        assert!(has(&s, "request.timeout.ms", "30000"));
        // Infinite: the forwarder enforces the delivery timeout itself, so a
        // timeout can never fail one record alone.
        assert!(has(&s, "message.timeout.ms", "0"));
    }

    /// A backoff above librdkafka's default cap raises the cap with it, or
    /// librdkafka would apply the cap instead; up to its own cap.
    #[test]
    fn test_build_producer_settings_applies_a_long_retry_backoff() {
        let config = ShadowConfig {
            enabled: true,
            bootstrap_servers: "a:9092".to_string(),
            security_protocol: "PLAINTEXT".to_string(),
            retry_backoff_ms: 3_000,
            ..Default::default()
        };
        let s = build_producer_settings(&config);
        assert!(has(&s, "retry.backoff.ms", "3000"));
        assert!(has(&s, "retry.backoff.max.ms", "3000"));
    }

    /// librdkafka's own retries stay well inside one delivery attempt
    /// ([`DELIVERY_TIMEOUT`]) whatever `shadow_retry_backoff_ms` is: its retry
    /// backoff cap (which also caps the spacing of the leader queries after a
    /// lost leader) is bounded, and the threshold at which a new leader loss
    /// restarts the leader query at once does not follow the backoff and stays
    /// within `retry.backoff.max.ms` (above it librdkafka warns and uses the
    /// maximum). With the GUC's 60 s maximum applied as is, two retriable errors
    /// in a row (each a request timeout and a backoff) took longer than the
    /// timeout.
    #[test]
    fn test_build_producer_settings_bound_librdkafka_retries() {
        let ms = |s: &[(&str, String)], key: &str| -> u64 {
            s.iter()
                .find(|(k, _)| *k == key)
                .map_or_else(|| panic!("{key} not set"), |(_, v)| v.parse().unwrap())
        };
        for backoff in [
            crate::kafka::constants::MIN_SHADOW_RETRY_BACKOFF_MS,
            crate::kafka::constants::DEFAULT_SHADOW_RETRY_BACKOFF_MS,
            30_000,
            crate::kafka::constants::MAX_SHADOW_RETRY_BACKOFF_MS,
        ] {
            let config = ShadowConfig {
                enabled: true,
                bootstrap_servers: "a:9092".to_string(),
                security_protocol: "PLAINTEXT".to_string(),
                retry_backoff_ms: backoff,
                ..Default::default()
            };
            let s = build_producer_settings(&config);
            let (base, max) = (ms(&s, "retry.backoff.ms"), ms(&s, "retry.backoff.max.ms"));
            let leader_query = ms(&s, "topic.metadata.refresh.fast.interval.ms");
            let request = ms(&s, "request.timeout.ms");
            assert!(base <= max && leader_query <= max, "backoff {backoff}");
            assert!(
                max <= 10_000,
                "backoff {backoff}: retry.backoff.max.ms {max}"
            );
            let two_retries = 2 * (request + max);
            assert!(
                u128::from(two_retries) < DELIVERY_TIMEOUT.as_millis(),
                "backoff {backoff}: two retries take {two_retries} ms"
            );
        }
    }
}
