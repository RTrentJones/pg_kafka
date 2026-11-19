# Configuration reference

Every `pg_kafka.*` setting (GUC), generated from the settings the extension registers
(`src/config.rs`, `src/kafka/auth/settings.rs`, `src/kafka/storage/privileges.rs`), as a running
server reports them in `pg_settings`. Do not edit the table by hand: run
`scripts/gen-docs.py --write --only gucs` against a server with the current build preloaded. CI
fails when it is stale, and also when the docs name a `pg_kafka.*` setting that does not exist.

"Changes take effect": **restart** = read when the server starts (`GucContext::Postmaster`);
**reload** = `pg_reload_conf()` (some listener settings are reloadable but read only when the
worker starts: reload, then `pg_terminate_backend()` the `pg_kafka_listener` worker, see
[AUTHENTICATION.md](AUTHENTICATION.md)); **superuser SET, reload** = `Suset`: a superuser may
`SET` it in a session, but only the server-wide value (reload) reaches the worker. A string shown
as "(unset)" has no boot value; the code then applies the default its description names.

<!--gen:guc-count-->36<!--/gen--> settings:

<!--gen:guc-table-->
| Name | Type | Default | Range | Changes take effect | Description |
|------|------|---------|-------|---------------------|-------------|
| `pg_kafka.additional_listeners` | string | (unset) |  | reload | Extra Kafka listeners, e.g. 'SASL_SSL://0.0.0.0:9093,SSL://:9094'. Comma-separated PROTOCOL://host:port. Each advertises its own host:port in Metadata. Empty (default) = none. Read when the worker starts. |
| `pg_kafka.compression_type` | string | (unset) |  | restart | Compression type for FetchResponse encoding. Compression codec for outbound messages: none, gzip, snappy, lz4, zstd. Default: none. |
| `pg_kafka.config_reload_interval_ms` | integer | `30000` | 100-300000 | reload | Configuration reload interval in milliseconds. How often to reload GUCs and shadow config from the database. Default 30000ms (30s). Tests can set to 1000-2000ms for faster iteration. Reloadable with pg_reload_conf(). |
| `pg_kafka.database` | string | (unset) |  | restart | Database to connect to for SPI operations. The database where CREATE EXTENSION pg_kafka was run. Default: postgres. Requires restart. |
| `pg_kafka.default_partitions` | integer | `1` | 1-10000 | restart | Default number of partitions for auto-created topics. When a topic is auto-created via produce, this sets the partition count. Default: 1. |
| `pg_kafka.enable_long_polling` | bool | `on` |  | restart | Enable long polling for FetchRequest. When enabled, FetchRequest will wait up to max_wait_ms for data. Default: true. |
| `pg_kafka.fetch_poll_interval_ms` | integer | `1000` | 10-60000 | restart | Polling interval for long polling fallback (milliseconds). A waiting FetchRequest re-runs when new data is signalled (Produce, EndTxn) or max_wait_ms expires; this interval is only the safety-net re-check for a missed signal. Default: 1000ms. |
| `pg_kafka.host` | string | (unset) |  | restart | Host/interface to bind to. The network interface to listen on. Default 127.0.0.1 (local clients only). This listener speaks pg_kafka.security_protocol (default PLAINTEXT, unauthenticated), so a non-loopback PLAINTEXT bind (e.g. 0.0.0.0 = all interfaces) must be protected by network controls (firewall/security groups); see docs/AUTHENTICATION.md. Requires restart. |
| `pg_kafka.log_connections` | bool | `off` |  | restart | Whether to log each connection. When enabled, logs every incoming connection. Read at worker start; requires a restart. |
| `pg_kafka.log_timing` | bool | `off` |  | restart | Enable timing instrumentation for performance research. When enabled, logs transaction and handler timing for each request. Use for benchmarking. |
| `pg_kafka.message_partition_minutes` | integer | `1440` | 5-43200 | reload | Width of one time partition of kafka.messages, in minutes (default 1440 = one day). kafka.messages is partitioned by created_at into leaves of this width, aligned to UTC. The worker creates the current leaf and the next two ahead of time; the retention sweep drops a whole leaf once every row in it is expired under its topic's retention. Topics whose retention window is shorter than one width are still expired row by row. A change applies to leaves created afterwards (existing leaves keep their bounds). Reloadable via SIGHUP. |
| `pg_kafka.message_retention_hours` | integer | `0` | 0-87600 | reload | Delete messages older than this many hours (0 = keep forever). Time-based retention for kafka.messages, enforced by the worker's periodic retention sweep. 0 (default) disables the sweep. Pending transactional messages are never deleted regardless of age. Reloadable via SIGHUP. |
| `pg_kafka.port` | integer | `9092` | 1024-65535 | restart | Port to listen on for Kafka protocol connections. The TCP port that pg_kafka will bind to. Default: 9092. Requires restart to take effect. |
| `pg_kafka.run_as_role` | string | `'pgkafka_worker'` |  | restart | Role the background worker switches to once the schema exists. Default 'pgkafka_worker', created by the extension with only the privileges the worker's SQL needs. Empty = keep running as the bootstrap superuser. If the role does not exist or lacks a required privilege (e.g. the binary was upgraded but ALTER EXTENSION pg_kafka UPDATE has not run yet) the worker logs a WARNING and keeps the superuser. Requires restart. |
| `pg_kafka.sasl_enabled_mechanisms` | string | `'PLAIN,SCRAM-SHA-256,SCRAM-SHA-512'` |  | reload | SASL mechanisms offered on SASL listeners. Comma-separated subset of PLAIN, SCRAM-SHA-256, SCRAM-SHA-512 (default: all three). Read when the worker starts. |
| `pg_kafka.security_protocol` | string | `'PLAINTEXT'` |  | reload | Security protocol of the primary listener (pg_kafka.host:pg_kafka.port). PLAINTEXT (default), SSL, SASL_PLAINTEXT or SASL_SSL. Read when the worker starts. |
| `pg_kafka.shadow_batch_size` | integer | `1000` | 1-100000 | superuser SET, reload | Messages per batch for shadow forwarding. Number of messages to batch before sending to external Kafka. Default: 1000. |
| `pg_kafka.shadow_bootstrap_servers` | string | (unset) |  | reload | External Kafka bootstrap servers. Comma-separated list of external Kafka brokers (e.g., 'kafka1:9092,kafka2:9092'). Reloadable with pg_reload_conf(). |
| `pg_kafka.shadow_external_primary_strict` | bool | `off` |  | superuser SET, reload | Keep external-primary topics fully read-suppressed even when a forward is dead-lettered. When true, external-primary topics never fall back to local reads (external-only-strict; a dead-lettered record is accepted as lost). When false (default), a dead-lettered external-primary topic serves the record locally so it is never readable nowhere (RV-6). |
| `pg_kafka.shadow_license_key` | string | (unset) | | restart | Superuser-only, hidden from `SHOW ALL` and `pg_settings`; see the source for details |
| `pg_kafka.shadow_linger_ms` | integer | `10` | 0-60000 | superuser SET, reload | Batching delay for shadow forwarding (milliseconds). How long to wait for more messages before sending a batch. Default: 10ms. |
| `pg_kafka.shadow_max_retries` | integer | `3` | 0-100 | superuser SET, reload | Maximum retries per message for shadow forwarding. Maximum number of retry attempts for failed forwards. Default: 3. |
| `pg_kafka.shadow_metrics_enabled` | bool | `on` |  | superuser SET, reload | Enable shadow mode metrics. When enabled, tracks forwarding statistics in kafka.shadow_metrics table. Default: true. |
| `pg_kafka.shadow_mode_enabled` | bool | `off` |  | reload | Enable shadow mode for forwarding to external Kafka. When enabled, messages can be forwarded to an external Kafka cluster. Can be changed with pg_reload_conf(). |
| `pg_kafka.shadow_retry_backoff_ms` | integer | `100` | 10-60000 | superuser SET, reload | Retry backoff for shadow forwarding (milliseconds). Initial backoff between retries for failed forwards. Default: 100ms. |
| `pg_kafka.shadow_sasl_mechanism` | string | (unset) |  | restart | SASL mechanism for external Kafka. SASL mechanism: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512. Default: PLAIN. Requires restart. |
| `pg_kafka.shadow_sasl_password` | string | (unset) | | restart | Superuser-only, hidden from `SHOW ALL` and `pg_settings`; see the source for details |
| `pg_kafka.shadow_sasl_username` | string | (unset) | | restart | Superuser-only, hidden from `SHOW ALL` and `pg_settings`; see the source for details |
| `pg_kafka.shadow_security_protocol` | string | (unset) |  | reload | Security protocol for external Kafka. Security protocol: SASL_SSL, SASL_PLAINTEXT, SSL, PLAINTEXT. Default: SASL_SSL. Reloadable with pg_reload_conf(). |
| `pg_kafka.shadow_ssl_ca_location` | string | (unset) |  | restart | SSL CA certificate location for external Kafka. Path to CA certificate file for SSL verification. Leave empty to use system CAs. Requires restart. |
| `pg_kafka.shutdown_timeout_ms` | integer | `5000` | 100-60000 | restart | Timeout for graceful shutdown in milliseconds. How long to wait for the TCP listener to shut down cleanly. Default: 5000ms. |
| `pg_kafka.ssl_ca_file` | string | (unset) |  | reload | PEM CA bundle for client certificates; when set, TLS listeners require a client certificate (mutual TLS). Empty (default) = no client certificates. Read when the worker starts. |
| `pg_kafka.ssl_cert_file` | string | (unset) |  | reload | PEM certificate chain for SSL / SASL_SSL listeners. Read when the worker starts. |
| `pg_kafka.ssl_key_file` | string | (unset) |  | reload | PEM private key for SSL / SASL_SSL listeners. Read when the worker starts. |
| `pg_kafka.test_forward_ack_delay_ms` | integer | `0` | 0-30000 | reload | TEST ONLY: delay (ms) before the shadow forwarder returns each ForwardAck. Fault-injection knob for the shadow outbox in-flight gate E2E test (issue #93): the network-thread forwarder sleeps this long after a delivery before acking it to the DB thread. 0 (default) disables it. Do not set in production. Reloadable via SIGHUP. |
| `pg_kafka.test_pre_commit_delay_ms` | integer | `0` | 0-10000 | reload | TEST ONLY: delay (ms) between a Produce handler and its commit. Fault-injection knob for the response-after-commit barrier E2E test: sleeps on the DB thread inside the transaction after a Produce handler runs, before commit. 0 (default) disables it. Do not set in production. Reloadable via SIGHUP. |
<!--/gen-->
