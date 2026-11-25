# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`pg_kafka` is a PostgreSQL extension written in Rust (via pgrx) that embeds a Kafka-compatible wire
protocol listener in the Postgres runtime: standard Kafka clients produce and consume, and
PostgreSQL is the store. A portfolio/learning project; Phase 11 (shadow mode) is complete.

What it implements (details: `docs/KAFKA_PROTOCOL_COVERAGE.md`, differences from Kafka:
`docs/PROTOCOL_DEVIATIONS.md`):

- **APIs:** <!--gen:api-coverage-->28 of ~50 standard Kafka APIs (56%)<!--/gen--> (the table in
  README "Implemented APIs" is generated from the `kafka_apis!` registry): metadata, produce
  (acks 0/1/-1), fetch with long polling, ListOffsets (earliest/latest/max-timestamp/timestamp,
  isolation-aware), consumer groups with server-side Range/RoundRobin/Sticky assignment and
  rebalancing, offsets, admin (CreateTopics, DeleteTopics, CreatePartitions, DeleteGroups),
  config/log management (DescribeConfigs, IncrementalAlterConfigs for `retention.ms` only,
  DeleteRecords), idempotent and transactional producers, SASL.
- **Idempotence:** Kafka's model: a retry of one of a producer's last 5 batches is acked with its
  original offset; anything else out of sequence is OUT_OF_ORDER_SEQUENCE_NUMBER.
- **Transactions:** read_committed is filtered server-side; consumers move past offsets they cannot
  see through a gap control batch (PROTOCOL_DEVIATIONS §7); fetched records are regrouped into one
  RecordBatch per run of a producer's offsets; transactional Produce only to partitions added with
  AddPartitionsToTxn; OffsetFetch `require_stable` (KIP-447).
- **Storage:** time-partitioned `kafka.messages` with retention by dropping whole leaves (ADR-001),
  per-topic `retention.ms`, compression (gzip, snappy, lz4, zstd), murmur2 key routing.
- **Security:** PLAINTEXT / SSL (rustls, optional mutual TLS) / SASL_PLAINTEXT / SASL_SSL listeners
  with SASL PLAIN and SCRAM-SHA-256/512; **no ACLs** (`docs/AUTHENTICATION.md`).
- **Shadow mode:** forwarding to an external Kafka through a durable outbox (below).

**Tests:** unit + property + E2E + interop + fuzz, all CI-gated. Counts live only in
`docs/TEST_STRATEGY.md` ("Test Summary"); do not repeat them elsewhere.

## Development Setup

The devcontainer (`.devcontainer/`, VS Code "Dev Containers: Reopen in Container") or
`docker compose up -d && docker exec -it pg_kafka_dev bash` gives a nightly Rust toolchain, pgrx
initialised for pg14 and the `external-kafka` broker for shadow tests. The devcontainer forwards
PostgreSQL (28814, pgrx's pg14) and the Kafka port (9092).

Install the git hooks once per clone: `./hooks/install.sh` (pre-commit: `cargo fmt --check` in both
crates; pre-push: clippy `-D warnings` in both crates; see `hooks/README.md`).

[rust-toolchain.toml](rust-toolchain.toml) pins a dated nightly (pgrx 0.16.1 needs nightly); bump
it deliberately and re-run clippy on both crates. The default feature is `pg14`; `pg13` to `pg18`
exist, CI builds and tests 14 to 18 (`pg13` is end-of-life and built by no job). PG18-specific code
is `#[cfg(feature = "pg18")]`.

## Build & Test Commands

```bash
cargo build --features pg14                          # build
cargo clippy --all-targets --features pg14 -- -D warnings   # lint (the [lints] policy is in Cargo.toml)
cargo fmt                                            # format (also in kafka_test/)

cargo test --features pg14 --lib                     # unit tests (no PostgreSQL needed)
cargo test --features pg14 --lib kafka::protocol     # tests under a module path
cargo test --features pg14 --test property_tests     # property tests (tests/property_tests.rs)
cargo llvm-cov --lib --features pg14 --summary-only  # coverage

cargo pgrx run pg14                                  # Postgres with the extension, psql shell
cargo pgrx start pg14 / stop pg14 / status pg14
./scripts/restart.sh [--fresh]                       # rebuild, install, restart; --fresh also
                                                     # DROP/CREATE EXTENSION (deletes all data)
cd kafka_test && cargo run --release                 # E2E suite against the running extension
scripts/ci-pg.sh start <dir>                         # a throwaway cluster, the way CI runs one
scripts/check-upgrade.sh                             # 0.0.0 -> current upgrade equals a fresh install
python3 scripts/gen-docs.py --check                  # generated doc regions are current
```

`cargo pgrx test` is not used: the extension's `PGC_POSTMASTER` GUCs need
`shared_preload_libraries` before the server starts, and `cargo pgrx test` creates the extension
afterwards ("FATAL: cannot create PGC_POSTMASTER variables after startup"). SQL-visible code is
covered by the E2E suite instead (`docs/PGRX_TESTING_GUIDE.md`).

Generated doc regions (between `gen:` marker comments): the `api-*` ones are owned by the unit test
`kafka::api::docs::generated_api_docs` (rerun with `PG_KAFKA_UPDATE_DOCS=1` to rewrite them), the
`guc-*` and `tests-*` ones by `scripts/gen-docs.py` (`--write`, needs a running server for GUCs).

## Code Coverage and the QA-1 Rule

Codecov gates project coverage at 78% (target 80% less its 2% threshold) and patch coverage at 85%
(target 90% less 5%) (`codecov.yml`).

Code that needs a live Postgres (SPI) or the tokio listener is invisible to unit coverage.
`codecov.yml` lists it: `ignore:` entries (SPI/async/generated through and through) and
`# qa1-guard:` lines (files that mix SPI calls with unit-tested logic and stay in the figures).
**Every behavioural change to a file on either list must ship a fail-before / pass-after
`kafka_test/` regression test.** The `spi-e2e-guard` CI job (`.github/scripts/require-e2e-for-spi.sh`)
fails a PR that touches one without changing a Rust file under `kafka_test/src/`, and
`.github/scripts/check-coverage-policy.sh` fails when a `src/` file that calls SPI (`Spi::*`,
`BackgroundWorker::*`, `#[pg_extern]`) is on neither list. A genuinely test-neutral edit
(comments, docs, renames) gets the `no-e2e-needed` label. Do **not** add a file to the ignore list
to route untested logic around the patch gate; extract the pure logic into a testable function
instead (see `validate_sequence` in `storage/sequence.rs`, `build_producer_settings` in
`shadow/producer.rs`). When you split or move a listed file, update `codecov.yml` with it.

**Single-DB-thread invariant:** all handlers, the `Arc<RwLock>` coordinator and the `ShadowStore`
run on the one background-worker DB thread; only the crossbeam channel ends cross to the tokio
network thread. Several structures (coordinator lock, shadow config-cache reload, metrics
read-modify-write) are correct *only* under this invariant. Making one reachable from another
thread requires adding synchronization.

## Codebase Structure

```
src/
├── lib.rs              # _PG_init, bootstrap SQL include, #[pg_extern] SQL functions
├── config.rs           # GUC definitions and loading
├── worker/             # Background worker (DB thread)
│   ├── mod.rs          # Entry point and main loop
│   ├── batch.rs        # Group commit: request batches, subtransactions, the response barrier
│   ├── lifecycle.rs    # SIGTERM, postmaster death, restart and exit
│   ├── maintenance.rs  # Periodic maintenance (time partitions, retention, txn timeouts) and its schedule
│   ├── network.rs      # Channels, listener sockets and the network thread's start
│   ├── schema.rs       # Waiting for the schema, feature detection, role reset
│   └── txn.rs          # worker_transaction, the extension and schema locks, subtransactions
├── testing/            # MockKafkaStore and test helpers
├── bin/pgrx_embed.rs   # pgrx embedding binary (generated)
└── kafka/
    ├── api/            # One `KafkaApi` impl per API + the kafka_apis! registry (decode, versions,
    │                   # lanes, error responses, dispatch, generated docs)
    ├── protocol/       # Framing (FrameCodec), header decoding, response encoding, RecordBatch parsing
    ├── listener/       # Network thread: its setup (thread), accept and connection caps
    │                   # (accept), TLS (tls), the read loop (connection), ordered responses
    │                   # (writer) and what they may hold (response_budget), SASL frames
    │                   # (sasl), hand-off to the DB thread (dispatch), Fetch long polling
    │                   # (long_poll)
    ├── conn_limits.rs  # Per-listener / per-IP connection caps, SASL lookup rate limit
    ├── auth/           # TLS config, SASL session state machine, SCRAM/PLAIN, credentials, user SQL
    ├── messages.rs     # KafkaRequest/KafkaResponse envelopes, request lanes
    ├── dispatch.rs     # Handler Err/panic -> error response, RequestOutcome
    ├── handlers/       # handle_* per API, taking &HandlerContext
    ├── handler_context.rs  # Store, coordinator and broker metadata passed to handlers
    ├── coordinator/    # Consumer group coordinator (in-memory group state): mod.rs, group.rs, tests.rs
    ├── assignment/     # Range, RoundRobin, Sticky; subscription/assignment encoding
    ├── limits.rs       # Caps on client-controlled state (partitions, group size, group count)
    ├── storage/        # Storage role traits (mod.rs), delegation macros, retention planning,
    │   │               # sequence validation, privileges, schema checks
    │   ├── postgres.rs # PostgresStore and the kept-plan cache (select_cached)
    │   └── postgres/   # Its SQL per concern: topics, log, offsets, producers, transactions,
    │                   # maintenance; roles.rs implements the role traits; worker_role.rs
    ├── shadow/         # Shadow mode: ShadowStore (store.rs; unit tests in store/), the outbox
    │                   # trait (outbox.rs; in-memory test double outbox/fake.rs), forwarder task,
    │                   # in-flight tracking, routing, rdkafka producer, license
    ├── notifications.rs, pending_fetches.rs  # Long polling
    ├── metrics.rs      # Request metrics (atomics) flushed to kafka.broker_* tables
    ├── broker_metadata.rs, context.rs, partitioner.rs, response_builders.rs
    └── constants.rs, error.rs  # Protocol constants, error codes; KafkaError -> Kafka error code
tests/property_tests.rs # proptest suite
kafka_test/             # E2E suite (rdkafka + raw-protocol tests), its own crate and Cargo.lock
fuzz/                   # libFuzzer targets for the network-edge decoders (cargo-fuzz)
interop/                # Java client checks + differential harness against Apache Kafka 3.7
sql/                    # bootstrap.sql, the 0.0.0 -> 0.1.0 upgrade script, upgrade-test/, tuning
scripts/                # restart.sh, ci-pg.sh, check-upgrade.sh, gen-docs.py, soak.sh,
                        # postmaster-death-check.sh, run-shadow-tests.sh, evidence scripts
evidence/               # Published conformance/benchmark/shadow evidence (evidence.yml)
docs/                   # See docs/README.md; archive/ holds dated reviews (source of DR-* IDs)
```

## Architecture

### Async/Sync Bridge

The extension runs **one BackgroundWorker process**. Its tokio runtime serves the network; its main
thread is the only one that touches the database:

```
TCP:9092 → tokio runtime (listener) → crossbeam-channel → background worker (src/worker/) → SPI
```

1. **Network thread** (`src/kafka/listener/`): accepts TCP connections on every configured
   listener (TLS handshake where configured) and parses the Kafka framing and header.
   SaslHandshake/SaslAuthenticate never become `KafkaRequest`s: the per-connection `SaslSession`
   (`src/kafka/auth/session.rs`) answers them and gates every other request until authentication
   succeeds; its credential lookups go to the worker over a separate `CredentialRequest` channel.
2. **Channels** (`src/kafka/messages.rs`): `KafkaRequest`s travel on two lanes (`RequestLanes`:
   `main`, and a `liveness` lane for Heartbeat that the worker drains first); each carries a tokio
   `response_tx` the worker answers with a `KafkaResponse`.
3. **DB thread** (`src/worker/`, the batch in `batch.rs`): **group commit**: every request already queued (up to 64,
   ~50 ms budget; Heartbeats arriving meanwhile are answered between its requests) runs in one
   top-level transaction, each request in its own subtransaction (a handler `Err` returns
   `RequestOutcome::Rollback`; a panic is caught and rolled back), and all responses are released
   only after the single commit, so an ack implies a durable write.

Lifecycle: the worker restarts 5 s after it exits and exits with status 1 on SIGTERM, so
`pg_terminate_backend()` or a crash brings up a fresh listener. It starts after recovery (never on
a hot standby), waits indefinitely for the schema, and exits when the postmaster dies. Every worker
transaction goes through `worker_transaction`, which first takes a share lock on the extension
object (so DROP EXTENSION cannot deadlock with it) and the schema advisory lock
(`SCHEMA_LOCK_KEY`) in SHARE mode; upgrade scripts and the move of rows out of
`kafka.messages_default` take that lock exclusively. A dropped extension sends the loop back to the
schema wait, which answers requests with KAFKA_STORAGE_ERROR; a failed COMMIT is rolled back and
answered with errors. `pg_kafka.worker_lock_timeout_ms` bounds every lock wait. A schema older than
the library (objects its requests use are missing, e.g. 0.0.0 before `ALTER EXTENSION pg_kafka
UPDATE`) is not served: one WARNING, every request answered with KAFKA_STORAGE_ERROR, no
maintenance, and the schema re-checked every 500 ms until it is current
(`schema::wait_for_schema_update`).

**Critical constraint:** the tokio runtime cannot call SPI. All database work goes through the
worker. One process multiplexes every client connection onto one SPI connection.

### Repository Pattern (storage role traits)

Storage is a set of role traits in `src/kafka/storage/mod.rs`; `KafkaStore` is their sum,
implemented for every type that has all six:

```rust
pub trait TopicStore { /* catalog, admin, retention.ms */ }
pub trait LogStore { /* insert/fetch records, offset bounds, DeleteRecords */ }
pub trait OffsetStore { /* group offsets */ }
pub trait ProducerStore { /* producer ids, epochs, sequences */ }
pub trait TransactionStore { /* transactional producers, writes, commit/abort */ }
pub trait MaintenanceStore { /* transaction timeouts, aborted-row cleanup */ }
pub trait KafkaStore: TopicStore + LogStore + OffsetStore + ProducerStore + TransactionStore + MaintenanceStore {}
```

- `PostgresStore` (`storage/postgres.rs`): the SPI implementation, its SQL in one
  `storage/postgres/` submodule per concern; `storage/postgres/roles.rs` implements the roles by
  forwarding to those inherent methods.
- `ShadowStore` (`shadow/store.rs`): wraps `PostgresStore`, forwards the roles it does not change
  (`delegate_topic_store!` and friends) and implements `LogStore`, `TransactionStore` and
  `MaintenanceStore` to write the shadow outbox.
- `MockKafkaStore` (`src/testing/mocks.rs`): the test double for handler unit tests.
- Helpers take the narrowest role (`resolve_topic_id<S: TopicStore + ?Sized>`); calling store
  methods on a concrete store needs `use crate::kafka::storage::roles::*`.

Handlers (`src/kafka/handlers/`) are `handle_*` functions taking `&HandlerContext`, whose
`store: &dyn KafkaStore` keeps them testable; the pipeline around them is the API's `KafkaApi` impl.
`KafkaError` (`src/kafka/error.rs`) is typed, and `to_kafka_error_code()` maps every variant to a
Kafka error code.

### Consumer Group Coordinator

Group state is in memory (`src/kafka/coordinator/`, `Arc<RwLock<..>>`); only committed offsets
persist. JoinGroup responses are held until the join phase completes (every known member rejoined,
or the rest left or timed out), as in Kafka; the generation moves when the join completes.
`GroupCoordinator::join_group` takes a `JoinResponder` it calls exactly once (immediately, or later
from another member's JoinGroup, a LeaveGroup or the ~1 s `check_and_remove_timed_out_members`
sweep). JoinGroup and SyncGroup answer on the client's own sender (`KafkaApi::DEFERRED_REPLY`),
wrapped in `handlers::JoinGroupReply` / `SyncGroupReply`, which answer UNKNOWN_SERVER_ERROR if
dropped unsent; a follower's SyncGroup waits (`pending_syncs`) until the leader's delivers the
assignment. JoinGroup follows Kafka 3.7's checks (INCONSISTENT_GROUP_PROTOCOL, UNKNOWN_MEMBER_ID for
an unknown non-empty member id, protocol chosen by member vote) and OffsetCommit its rules
(`validate_offset_commit`: generation -1 only for an unknown or Empty group). Groups with
committed offsets but no in-memory state are listed and described as Empty "consumer" groups.
Group size and count are capped (`kafka::limits`).

### Storage Schema

`sql/bootstrap.sql` is the schema; README "Storage Schema" has an excerpt. The invariants code
must keep:

- **Dual offsets:** `partition_offset` is the Kafka offset (per partition, from 0);
  `global_offset` orders all messages. Offsets are allocated from `kafka.partition_offsets`
  (next offset + durable log start offset) under lock and never reused, even after aborted rows
  are cleaned up or records deleted.
- **Time partitions (ADR-001):** `kafka.messages` is `PARTITION BY RANGE (created_at)`, with leaves
  `kafka.messages_pYYYYMMDD_HHMMSS` of `pg_kafka.message_partition_minutes` (UTC-aligned) plus
  `kafka.messages_default`. The parent has no primary key; every leaf carries
  `UNIQUE (topic_id, partition_id, partition_offset)` and `UNIQUE (global_offset)`. Leaves are
  extension members. All partition DDL runs in SECURITY DEFINER functions
  (`kafka.ensure_message_partitions`, `drop_message_partition`, `message_partition_contents`,
  internal `create_message_partition`, and the internal lock helpers `try_maintenance_lock`
  and `lock_message_tables`; `kafka.message_partitions()` lists the leaves), and the
  drop/trim decisions are pure functions in `storage/retention.rs`. A leaf is dropped only at
  READ COMMITTED (its re-check needs a snapshot taken after the drop's lock wait; the worker
  pins its session to that level, and a sweep at another skips its drops). Retention for a window of at
  least one width is leaf-granular; shorter windows and the DEFAULT leaf expire row by row.
  **Never** write an UPDATE that changes `created_at` (it moves rows between leaves), confine a
  ctid-based statement to one leaf (`tableoid`; ctids repeat across leaves), and send hot
  statements that do not name `created_at` through `select_cached` in `storage/postgres.rs` (kept
  prepared plans, run with a fresh snapshot).
- **Visibility:** `messages.txn_state` is NULL (visible), `'pending'` or `'aborted'`; no transaction
  markers are written. `kafka.producer_batches` holds the last 5 batches per producer and partition
  (cascade-deleted with their `producer_sequences` row); `kafka.txn_partitions` the partitions of
  each open transaction. Group membership is not stored.
- **Roles:** `pgkafka_reader` (read-only, nothing is granted to PUBLIC) and `pgkafka_worker` (exactly
  what the worker's SQL needs). `src/kafka/storage/privileges.rs` lists the worker's privileges and
  its unit tests check them against the GRANTs in `bootstrap.sql` and scan the worker's SQL: a new
  worker statement on a new table needs a GRANT plus an entry there. `kafka.sasl_credentials` is
  readable only by superusers and `pgkafka_worker`; `pgkafka_reader` gets the secret-free
  `kafka.sasl_users` view.
- **Upgrades:** `bootstrap.sql` and `sql/pg_kafka--0.0.0--0.1.0.sql` must produce the same schema
  (`scripts/check-upgrade.sh`; `docs/UPGRADING.md`).

SQL functions (public schema, `src/lib.rs` and `src/kafka/auth/users.rs`):
`pg_kafka_run_retention_sweep(message_retention_hours, aborted_grace_seconds)` and
`pg_kafka_run_partition_maintenance(partition_minutes)` run the worker's maintenance on demand;
`pg_kafka_create_user` / `_alter_user` / `_set_user_verifier` / `_drop_user` manage SASL users
(SECURITY INVOKER, so superusers by default); `hello_pg_kafka()` is a smoke test.

## Configuration

`docs/CONFIGURATION.md` is the generated reference of every GUC (type, default, range, when a
change applies). The rules of thumb: `shared_preload_libraries = 'pg_kafka'` is required;
`pg_kafka.port`, `host`, `database`, `run_as_role` and the other Postmaster GUCs need a restart;
the listener-security and network-limit GUCs are Sighup but read only when the worker starts
(reload, then `pg_terminate_backend()` the `pg_kafka_listener` worker); every shadow GUC except the
license key is Sighup and a session `SET` is refused. The `test_*` GUCs are fault injection
for tests only.

## Shadow Mode

Shadow mode stores every record locally and forwards it to an external Kafka cluster
(`docs/SHADOW_MODE_FLOW_DIAGRAM.md`). `ShadowStore` writes a `kafka.shadow_tracking` outbox row in
the same transaction as the local write; the DB thread claims due rows and the network-thread
forwarder (`shadow/forwarder_task.rs`) sends them in order and reports acks. Failed rows are
retried with backoff and dead-lettered after `pg_kafka.shadow_max_retries`. Delivery is
at-least-once.

Topics opt in through `kafka.shadow_config`, keyed by `topic_id`: `mode` (`local_only` | `shadow`),
`forward_percentage` (0-100), `external_topic_name`, `sync_mode` (`async` | `sync`, default `sync`:
the produce response is held after the local commit until the external ack or
`pg_kafka.shadow_sync_timeout_ms`; the worker never waits) and `write_mode` (`dual_write` |
`external_only`). The worker reloads it on `pg_reload_conf()` or every
`pg_kafka.config_reload_interval_ms`.

```sql
INSERT INTO kafka.shadow_config (topic_id, mode, forward_percentage, external_topic_name, sync_mode, write_mode)
SELECT id, 'shadow', 100, 'prod-orders', 'sync', 'dual_write' FROM kafka.topics WHERE name = 'orders';
SELECT pg_reload_conf();

-- Re-queue partition_offset range [from, to) of a topic (NULL = to the end); committed rows only
SELECT kafka.replay_shadow_messages(p_topic_id => 1, p_from_offset => 0, p_to_offset => NULL);

SELECT * FROM kafka.shadow_status;            -- per topic; lag = records still to forward
SELECT * FROM kafka.shadow_partition_status;  -- per partition
```

## Testing with Kafka Clients

```bash
kcat -L -b localhost:9092                                       # metadata
echo "key1:value1" | kcat -P -b localhost:9092 -t my-topic -K:  # produce
kcat -C -b localhost:9092 -t my-topic -p 0 -o beginning         # consume
psql -h localhost -p 28814 -U postgres -d postgres -c \
  "SELECT topic_id, partition_id, partition_offset, key, value FROM kafka.messages;"
```

## Development Workflow

### Adding SQL-visible functionality

Mark the function `#[pg_extern]` (it lands in the `public` schema), try it with `cargo pgrx run
pg14`, and cover it with a `kafka_test/` E2E test (`#[pg_test]` cannot run here; for example
`edge_cases/retention_sweep.rs` calls `pg_kafka_run_retention_sweep()`). A new worker statement on
a new table also needs its GRANT and a `privileges.rs` entry.

### Adding a New Kafka API Handler

Every API is described once, by a type implementing `KafkaApi` (`src/kafka/api/`), and listed
once in the `kafka_apis!` registry at the bottom of `src/kafka/api/mod.rs`. The registry
generates the `RequestBody`/`ResponseBody` enums, the decode/encode/dispatch matches, the
ApiVersions list, the metrics names and the error path; nothing else lists the APIs.

1. Write the handler in `src/kafka/handlers/` taking `&HandlerContext` (its
   `store: &dyn KafkaStore` keeps it testable) and returning the kafka-protocol response or a
   `KafkaError`. Add unit tests with `MockKafkaStore` (`src/kafka/handlers/tests/`).
2. In the fitting `src/kafka/api/*.rs` file, add a request data struct and `impl KafkaApi`:
   - `type Wire` = the kafka-protocol request type (its `KEY` is the api key, its `Response`
     the response body); `NAME`, `MIN_VERSION`, `MAX_VERSION` (advertised and enforced),
     and `LANE` / `DEFERRED_REPLY` if not the defaults (main lane, reply after commit);
   - `decode`: wire request → your data struct (return `Rejection::Reply` to answer an
     invalid request with an error, `Rejection::Silent` to send nothing);
   - `type Echo` + `echo` + `error_response`: what an error response needs from the request
     (`()` when the response has a top-level error code; the requested topics/partitions/
     groups otherwise, so every entry carries the code);
   - `response_has_error` (metrics) and `handle`, usually
     `call.run(request, |r, header| handlers::handle_x(&env.ctx, ...))`.
3. Add `Variant => YourApi,` to `kafka_apis!` (and an `API_KEY_*` constant in `constants.rs`
   if other code needs the key). The registry tests (`src/kafka/api/tests.rs`) then check that
   every advertised version encodes and that the error responses encode.
4. Add an E2E test under the fitting `kafka_test/src/<category>/` and list it in that
   category's `tests()` (see "E2E Testing" below).

**An API answered on the network thread** (from per-connection state, never sent to the
worker; today SaslHandshake/SaslAuthenticate, answered by `auth::session` from
`listener/sasl.rs`) implements only `ApiSchema` (wire types, name, versions, error response,
`response_has_error`; see `src/kafka/api/sasl.rs`) and goes in the registry's
`network_thread { .. }` section. It is advertised, encoded and named from the table like
the others (`ApiInfo::served_by` is `ServedBy::NetworkThread`) but has no `RequestBody`
variant: the listener must intercept its frames before `parse_request`, build responses as
`ResponseBody::<Variant>`, and count them with `metrics::RequestTimer` itself.

## E2E Testing (kafka_test/)

Shared infrastructure in `kafka_test/src/`: `common.rs` (client factories such as
`create_producer()`, `create_idempotent_producer()`, `create_transactional_producer()`,
`create_stream_consumer()`), `setup.rs` (`TestContext`: unique names, cleanup), `fixtures.rs`
(builders), `assertions.rs` (domain assertions), `wire/` (raw protocol), `error_codes.rs`
(expected Kafka error codes), `registry.rs` (`TestDef`, `test_defs!`).

### Test Categories

Per-category counts are in `docs/TEST_STRATEGY.md`; `cargo run --release -- --list` prints the live registry and total.

| Category | Purpose |
|----------|---------|
| **admin** | CreateTopics (incl. validation, the per-topic and total partition caps), DeleteTopics, CreatePartitions, DeleteGroups, DescribeConfigs, IncrementalAlterConfigs, DeleteRecords, per-topic retention |
| **producer** | Basic produce, batch produce, acks=0, ack-implies-committed, timestamp roundtrip |
| **consumer** | Basic consume, from offset, multiple messages, headers, fetch record batching |
| **consumer_group** | Lifecycle, two-member groups, rebalancing, strategies, JoinGroup protocol and member-id rules, no common assignor, offsets-only groups after a restart, a silent follower outliving a slow leader's SyncGroup, a lowered group cap, server-side assignment computed only for the sync-phase leader and in about linear time |
| **offset_management** | Commit/fetch, OffsetCommit generation/member rules, boundary conditions, multi-partition, by-timestamp, ListOffsets isolation, OffsetFetch require_stable, no offset reuse |
| **partition** | Multi-partition topics, key routing, distribution |
| **compression** | gzip, snappy, lz4, zstd, roundtrip, edge cases |
| **idempotent** | InitProducerId, deduplication, epoch, multi-partition, Kafka's idempotent model (5-batch cache, original offsets, epochs, sequence wrap), producers the broker holds no state for |
| **transaction** | Transactions, isolation levels, EOS, fencing, atomicity, timeouts (each transaction's own clock, an exhausted epoch fenced), InitProducerId's presented epoch, validation; read_committed consumers past aborted/cleaned-up/DeleteRecords holes and stopping at an open transaction |
| **shadow** | External forwarding, SASL/SSL, topic mapping, replay, dial-up, throughput floor, headers/timestamps, order across a retried failure and a delivery timeout, sync-stall bound, GUC reload, per-partition lag view |
| **long_poll** | Timeout behavior, immediate return, producer wakeup, precision, per-connection task cap, a transaction end waking a read_committed fetch |
| **error_paths** | Invalid partitions, unknown topics, coordinator errors, protocol/transaction/group regressions |
| **edge_cases** | Empty topics, large messages, boundary values, retention sweep |
| **part** | Time partitions of kafka.messages: whole-leaf retention drops, row-delete fallback, pending-transaction blocking, maintenance and its lock waits, maintenance locking kafka.messages before its leaves, maintenance and sweeps against a concurrent DROP EXTENSION, EndTxn during the default-leaf move, offsets kept after retention empties a partition, concurrent sweeps against maintenance, the worker's sweep skipping while the maintenance lock is held, the sweep reading only what it deletes (indexed aborted-row and outbox cleanup), outbox rows of removed messages reaped, budget-limited passes still reaching every leaf (each step with work takes a turn every pass, found trims and drops kept until done), the stale-producer prune reading through its indexes, leaf drops bounded behind a reader holding a lock, least privilege |
| **concurrent** | Multi-producer, multi-consumer, request pipelining, races, heartbeat liveness lane, heartbeats inside group-commit batches |
| **negative** | Connection refused, timeouts, expected failures, superuser-only SASL password GUC |
| **performance** | Throughput baselines, latency percentiles, scaling |
| **metadata** | Metadata refresh, all topics, nonexistent topic, a corrupt stored partition count |
| **protocol** | ApiVersions negotiation, correlation ID, unknown API, pipelining, response ordering |
| **auth** | SASL_PLAINTEXT (PLAIN, SCRAM-SHA-256/512), SSL (+ untrusted CA, mutual TLS), SASL_SSL, wrong passwords, pre-auth gating and pre-auth ApiVersions on the network thread, advertised addresses, user SQL. Sequential: generates certs with the `openssl` CLI, adds listeners 19093-19095 via ALTER SYSTEM and restarts the worker; the last test resets them. Needs the DB host's filesystem and superuser |
| **network** | Hostile clients at the network edge: frame-budget lockout, >100 MB response, Nagle, acks=0 close / invalid acks, response before close, per-IP and per-listener caps, idle and frame-read deadlines, pre-auth deadline from accept, SASL lookup rate limit, unknown-user SCRAM, decompression bombs, acks=0 Produce at an unsupported version, a saturated large-decode path, requests that decode to many times their frame (Metadata, DeleteGroups), a long topic name over many partitions (Produce, long-polling Fetch), clients that stop reading their responses (no hold on the decoded-request budget, a stalled writer or one behind a long poll closed after the idle timeout), Fetch responses capped by `pg_kafka.fetch_max_bytes`, frames waiting to decode charged to the frame budget. Sequential; the `limits` tests add listeners 19096-19098 via ALTER SYSTEM, connect from 127.0.0.x source addresses and need `openssl` |
| **worker** | Worker lifecycle and robustness: DROP/CREATE/ALTER EXTENSION under load, old 0.0.0 schema, SIGTERM in a lock wait, worker lock_timeout, commit failure. Sequential; needs superuser and the DB host's filesystem |

### Running E2E Tests with CLI Options

```bash
# Run all tests
cd kafka_test && cargo run --release

# Run specific category
cargo run --release -- --category compression
cargo run --release -- --category shadow

# Run single test by name
cargo run --release -- --test test_idempotent_producer_basic

# Exclude categories (repeatable; CI runs functional tests this way)
cargo run --release -- --exclude performance --exclude shadow

# By default parallel_safe tests run concurrently (up to --concurrency, default 16)
# and the rest run sequentially; force everything sequential
cargo run --release -- --sequential

# JSON output for CI
cargo run --release -- --json

# List all available tests
cargo run --release -- --list

# Per-test timeout (default 300 s). Each test runs in its own task: a panic or a
# timeout is reported as FAILED and the run continues
cargo run --release -- --timeout-secs 120

# Combine flags
cargo run --release -- --category transaction --json
```

### Standard Test Structure

Every test follows a 4-step pattern:

```rust
pub async fn test_feature_name() -> TestResult {
    println!("=== Test: Feature Name ===\n");

    // 1. SETUP: Create context and unique resources
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("feature").await;
    let group = ctx.unique_group("consumer").await;

    // 2. ACTION: Execute test scenario
    println!("Step 1: Performing action...");
    let producer = create_producer()?;
    producer.send(FutureRecord::to(&topic).payload("test"), Duration::from_secs(5)).await?;
    println!("✅ Action completed\n");

    // 3. VERIFY: Assert via database queries
    println!("=== Database Verification ===\n");
    let count_row = ctx.db().query_one(
        "SELECT COUNT(*) FROM kafka.messages m JOIN kafka.topics t ON m.topic_id = t.id WHERE t.name = $1",
        &[&topic]
    ).await?;
    let count: i64 = count_row.get(0);
    assert_eq!(count, 1, "Expected 1 message");
    println!("✅ Verification passed\n");

    // 4. CLEANUP: Automatic via TestContext drop (or explicit)
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
```

### Key Principles

1. Use unique resource names (`ctx.unique_topic()`, `ctx.unique_group()`).
2. Verify both the Kafka protocol response and the database state (through `ctx.db()`).
3. Use the shared client factories.
4. Mark timing-sensitive tests or tests that change global state `: sequential`.
5. Print step-by-step progress; CI logs are the debugging aid.

To add a test: write it in the fitting category directory and add its path to the
`test_defs!` list in that category's `tests()` (`kafka_test/src/<category>/mod.rs`), with
`: sequential` if it must not run concurrently; nothing else lists tests (`registry.rs`,
`lib.rs`'s `all_tests()`). A new category is a new module plus one line in `all_tests()`.
When rdkafka hides what the test asserts on, speak the protocol with `kafka_test/src/wire/`
(framing, the typed `Wire` connection, record batches, shared request builders), compare
against the codes in `error_codes.rs`, and verify through the database.

## Debugging

The devcontainer has vscode-lldb, rust-analyzer and `SYS_PTRACE` for gdb/perf; attach to the
Postgres process `cargo pgrx run` starts. The network thread's `tracing` output goes to the server's
stderr: with `RUST_LOG` unset it logs `pg_kafka=warn` and above, and a set `RUST_LOG` replaces that
filter (`RUST_LOG=debug cargo pgrx run pg14`).

## Key Dependencies

- **pgrx 0.16.1:** the extension framework (nightly toolchain)
- **tokio 1.48:** the network runtime inside the background worker
- **kafka-protocol 0.17:** wire types, patched (`[patch.crates-io]` in `Cargo.toml`) to a pinned
  fork that bounds allocation and decompression sizes
- **crossbeam-channel 0.5:** network thread ↔ DB thread
- **rustls 0.23 (ring):** TLS listeners; ring also provides the SCRAM primitives
- **rdkafka 0.36:** the shadow-mode producer (system OpenSSL; never `ssl-vendored`) and the E2E client

## Non-Goals

- Full Kafka protocol parity (core APIs only)
- High availability or broker clustering (single-node broker; rely on PostgreSQL HA and replication)

Future work: cooperative rebalancing (KIP-429), static membership (KIP-345), log compaction
(KIP-58), read replicas.
