# Shadow Mode Testing Guide

This document describes how to set up and run the shadow mode E2E tests in the devcontainer environment.

## Prerequisites

- Docker and docker-compose
- The devcontainer running (`pg_kafka_dev`)

## Quick Start

From inside the devcontainer, run:

```bash
# Full setup and test run (starts Docker, builds, configures, runs tests)
./scripts/run-shadow-tests.sh

# Or run individual steps:
./scripts/run-shadow-tests.sh --setup-only    # Setup only (Docker + PostgreSQL + build)
./scripts/run-shadow-tests.sh --test-only     # Build the test client, then run tests (assumes setup done)
./scripts/run-shadow-tests.sh --docker-only   # Just start the external Kafka container
./scripts/run-shadow-tests.sh --skip-docker   # Everything but starting the container

# Run a specific test
./scripts/run-shadow-tests.sh --test test_dual_write_sync
```

## What the Script Does

1. **Start external Kafka** - Starts `external_kafka` (recreates it if it is running but unhealthy), then waits for the broker API and for topic creation to work, and warns if the container's address is not the `172.18.0.2` that `docker-compose.yml` advertises
2. **Rebuild and restart PostgreSQL** - `PG_MAJOR=14 ./scripts/restart.sh` builds and installs the extension and restarts the pgrx-managed PostgreSQL 14, keeping the data
3. **Configure GUCs** - Sets the shadow mode PostgreSQL parameters with `ALTER SYSTEM`:
   - `pg_kafka.shadow_mode_enabled = 'true'`
   - `pg_kafka.shadow_bootstrap_servers = '<kafka_ip>:9095'`
   - `pg_kafka.shadow_security_protocol = 'PLAINTEXT'`
   - `pg_kafka.shadow_max_retries = '20'`
   - `pg_kafka.shadow_retry_backoff_ms = '500'`
   - `pg_kafka.config_reload_interval_ms = '2000'`
4. **Reload and wait** - Runs `SELECT pg_reload_conf()` and waits 1 second; the background worker handles the SIGHUP on its next loop iteration (at most 100 ms later), so no reload-interval wait is needed
5. **Build the test client** - `cargo build --release` in `kafka_test/`
6. **Run tests** - Executes the `shadow` test category (or the single test passed with `--test`)

Each test also re-applies the GUCs it needs through `enable_shadow_mode()` in `kafka_test/src/shadow/helpers.rs`, so step 3 mainly matters for manual runs.

## Network Architecture

The shadow mode tests require communication between:

1. **Host machine**: Runs E2E test code, connects to external Kafka via `localhost:9093`
2. **pg_kafka_dev container**: Runs PostgreSQL with pg_kafka extension
3. **external_kafka container**: External Kafka broker for shadow forwarding

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                              HOST MACHINE                                     │
│                                                                              │
│   ┌────────────────────────────────────────────────────────────────────┐    │
│   │  E2E Test Process (kafka_test)                                      │    │
│   │  Connects to external Kafka via localhost:9093 (EXTERNAL listener)  │    │
│   │  Connects to PostgreSQL / pg_kafka via forwarded ports              │    │
│   └──────────────────────────────┬─────────────────────────────────────┘    │
│                                  │ Port forwarding                           │
│                                  ▼                                           │
├──────────────── DOCKER NETWORK pg_kafka_network (172.18.0.0/16) ────────────┤
│                                                                              │
│   ┌─────────────────────┐         ┌─────────────────────────────────┐       │
│   │   pg_kafka_dev      │         │   external-kafka  (172.18.0.2)  │       │
│   │   container         │         │                                 │       │
│   │                     │────────▶│   :9095 CONTAINER listener      │       │
│   │   pg_kafka extension│ docker  │   Advertises: 172.18.0.2:9095   │       │
│   │   GUC set by tests: │ network │                                 │       │
│   │   172.18.0.2:9095   │         │   :9094 INTERNAL (inter-broker) │       │
│   │                     │         │   Advertises: external-kafka:9094│      │
│   │                     │         │                                 │       │
│   │                     │         │   :9093 EXTERNAL listener       │◀──────┤
│   │                     │         │   Advertises: localhost:9093    │  port │
│   └─────────────────────┘         │                                 │  fwd  │
│                                   │   :29094 CONTROLLER (KRaft)     │       │
│                                   └─────────────────────────────────┘       │
└──────────────────────────────────────────────────────────────────────────────┘
```

### Kafka Listeners

The external Kafka broker has three client-facing listeners plus the KRaft controller listener:

| Listener   | Port | Purpose                              | Advertised Address |
|------------|------|--------------------------------------|-------------------|
| INTERNAL   | 9094 | Inter-broker                         | external-kafka:9094 |
| CONTAINER  | 9095 | pg_kafka → broker (container or host-run Postgres) | 172.18.0.2:9095   |
| EXTERNAL   | 9093 | Host access (E2E tests, kcat)        | localhost:9093    |
| CONTROLLER | 29094 | KRaft controller quorum             | (not advertised)  |

### Why Use Direct IP for CONTAINER Listener?

The CONTAINER listener advertises `172.18.0.2:9095` instead of `external-kafka:9095` because:

1. **Stale DNS**: In devcontainer environments, DNS resolution for `external-kafka` can become stale when containers are recreated, pointing to old IP addresses.

2. **Metadata Redirection**: When rdkafka connects to a Kafka broker, it receives metadata containing the broker's advertised address. Subsequent connections use this advertised address, not the original bootstrap server.

3. **Deterministic IP**: `docker-compose.yml` pins the `pg_kafka_network` subnet to `172.18.0.0/16` and gives `external-kafka` the static `ipv4_address: 172.18.0.2`, so the advertised address is stable across container recreation.

4. **Host-run Postgres**: in CI, pg_kafka runs on the runner (PostgreSQL installed from apt.postgresql.org by `.github/actions/setup-pg`, the extension installed into it with `cargo pgrx install --pg-config`, the cluster started by `scripts/ci-pg.sh`), where Docker's DNS name `external-kafka` does not resolve. The static IP is reachable from both the host and containers.

The test code reads the pg_kafka-side address from `INTERNAL_KAFKA_BOOTSTRAP_SERVERS` (default `172.18.0.2:9095`) and the host-side address from `EXTERNAL_KAFKA_BOOTSTRAP_SERVERS` (default `localhost:9093`); see `kafka_test/src/shadow/external_client.rs`.

## Configuration

### PostgreSQL GUCs

Shadow mode is configured via PostgreSQL GUCs:

```sql
-- Enable shadow mode
ALTER SYSTEM SET pg_kafka.shadow_mode_enabled = 'true';

-- Set bootstrap servers (use CONTAINER listener IP)
ALTER SYSTEM SET pg_kafka.shadow_bootstrap_servers = '172.18.0.2:9095';

-- Set security protocol
ALTER SYSTEM SET pg_kafka.shadow_security_protocol = 'PLAINTEXT';

-- Reload configuration
SELECT pg_reload_conf();
```

### Per-Topic Configuration

Topics are configured for shadow mode via the `kafka.shadow_config` table, keyed by `topic_id` (the topic must already exist in `kafka.topics`):

```sql
INSERT INTO kafka.shadow_config
    (topic_id, mode, forward_percentage, external_topic_name, sync_mode, write_mode)
SELECT id, 'shadow', 100, NULL, 'sync', 'dual_write'
FROM kafka.topics WHERE name = 'my-topic'
ON CONFLICT (topic_id) DO UPDATE SET
    mode = EXCLUDED.mode,
    forward_percentage = EXCLUDED.forward_percentage,
    external_topic_name = EXCLUDED.external_topic_name,
    sync_mode = EXCLUDED.sync_mode,
    write_mode = EXCLUDED.write_mode,
    updated_at = NOW();

SELECT pg_reload_conf();  -- the worker reloads kafka.shadow_config on SIGHUP
```

Column values: `mode` is `local_only` or `shadow`; `forward_percentage` is 0-100; `sync_mode` is `async` or `sync`; `write_mode` is `dual_write` or `external_only`. Without a reload the worker picks the row up within `pg_kafka.config_reload_interval_ms`.

## Troubleshooting

### External Kafka Not Running

The tests first fetch metadata from the broker on `localhost:9093` and fail after 10 s with the
metadata error when it is down. Start it with `docker compose up -d external-kafka`.

### Connection Refused Errors

If you see errors like:
```
external-kafka:9095/1: Connect to ipv4#172.23.0.2:9095 failed: Connection refused
```

The IP `172.23.0.2` is stale. Fix by:

1. Verify actual Kafka IP: `docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' external_kafka`
2. If it is not `172.18.0.2`, another network on the host may already claim the `172.18.0.0/16` subnet; free it, or change the subnet, `ipv4_address`, and CONTAINER advertised listener together in `docker-compose.yml` and set `INTERNAL_KAFKA_BOOTSTRAP_SERVERS` to match
3. Recreate Kafka container: `docker-compose up -d --force-recreate external-kafka`
4. Restart PostgreSQL: `./scripts/restart.sh` (rebuilds and restarts, keeping the data; `--fresh` would also drop and recreate the extension, deleting all pg_kafka data)

### Producer Settings Changes

The forwarding producer lives in the network thread (`run_shadow_forwarder` in
`src/kafka/shadow/forwarder_task.rs`). It is rebuilt whenever any producer GUC changes (bootstrap
servers, security protocol, SASL, CA, batch size, linger, backoff), within about 0.5 s of the reload
(`SELECT pg_reload_conf()`); no restart is needed. Every shadow GUC is `Sighup`: set it with
`ALTER SYSTEM` (a session `SET` is refused).

### Running Against a Local Kafka Without Docker

Any Apache Kafka reachable from Postgres works. For example, a KRaft broker on `127.0.0.1:9093`:
set `pg_kafka.shadow_bootstrap_servers = '127.0.0.1:9093'` (plus the other GUCs above) and run the
suite with `INTERNAL_KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9093 EXTERNAL_KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9093`.
The order test creates and alters an external topic through the admin API, so the broker must allow
it (the default does).
The delivery-timeout order test and `test_dual_write_external_down` pause and resume the broker with the shell commands in
`EXTERNAL_KAFKA_PAUSE_CMD` / `EXTERNAL_KAFKA_RESUME_CMD` (default `docker pause external_kafka` /
`docker unpause external_kafka`); without Docker, stop the broker's process instead, for example
`pkill -STOP -f '[k]afka.Kafka'` / `pkill -CONT -f '[k]afka.Kafka'` (the bracket keeps the pattern
from matching the `sh -c` that runs it).

### DNS Resolution

To check DNS resolution inside the devcontainer:
```bash
getent hosts external-kafka
```

Should show `172.18.0.2` (or current actual IP). If it shows a different IP, Docker DNS is stale.

## Test Categories

The `shadow` category's tests are listed in `tests()` in `kafka_test/src/shadow/mod.rs`; all but `test_shadow_lag_view_is_per_partition` are sequential (`cargo run --release -- --list` prints the live set under `shadow:`):

| Test | Description |
|------|-------------|
| `test_dual_write_sync` | DualWrite + sync: stored locally and delivered to external Kafka |
| `test_dual_write_async` | DualWrite + async: stored locally and delivered by the outbox poll |
| `test_async_forwarding_no_duplicates_on_slow_ack` | With `pg_kafka.test_forward_ack_delay_ms` holding acks for 6.5 s, each record reaches external Kafka exactly once and no attempt is re-claimed (`retry_count` stays 0) |
| `test_external_only_mode` | ExternalOnly (external-primary): delivered externally, stored locally, but a pg_kafka consumer reads nothing |
| `test_local_only_mode` | `mode = 'local_only'`: stored locally, not forwarded |
| `test_reload_clears_deleted_topic_config` | Deleting a `kafka.shadow_config` row stops forwarding the topic's new records after reload |
| `test_outbox_row_written_and_finalized` | A forwarded record gets a `kafka.shadow_tracking` row whose `external_offset` is set |
| `test_zero_percent_forwarding` / `test_hundred_percent_forwarding` / `test_fifty_percent_forwarding` | Percentage-based routing at 0%, 100%, 50% |
| `test_deterministic_routing` | The same key always gets the same forward decision |
| `test_dialup_{0,10,25,50,75,100}_percent` | Dial-up routing at 0, 10, 25, 50, 75, 100 percent |
| `test_topic_name_mapping` | `external_topic_name` maps to a different external topic |
| `test_committed_transaction_forwards_without_ram_buffer` | Records produced in a transaction before its topic was shadowed are forwarded when it commits: the commit finds them in `kafka.messages`, routed by the topic config at commit time |
| `test_committed_transaction_forwarded` | Committed transaction records are forwarded |
| `test_aborted_transaction_not_forwarded` | Aborted transaction records are not forwarded |
| `test_committed_transaction_uses_durable_outbox` | Committed transaction records go through `kafka.shadow_tracking` |
| `test_committed_txn_leaves_no_orphan_outbox_rows` | A committed transaction leaves no outbox rows without a backing message |
| `test_large_committed_transaction_is_enqueued_exactly_once` | A 250 000-record transaction on a 100% and a 50% topic: at commit exactly the records routing selects get one outbox row each, and the worker's private memory does not grow while the transaction is open |
| `test_dual_write_external_down` | DualWrite (async) with the external broker paused (`EXTERNAL_KAFKA_PAUSE_CMD`): the produce is acked at once and stored locally, its outbox row stays undelivered, and the record is forwarded once the broker resumes |
| `test_external_only_fallback` | ExternalOnly with a record the external topic refuses (`max.message.bytes`): every attempt fails, the row is dead-lettered at `shadow_max_retries` with its error, and a pg_kafka consumer then reads the record locally |
| `test_shadow_unbuildable_settings_dead_letter_after_a_working_producer` | A producer setting no producer can be built from (`shadow_security_protocol = 'BOGUS'`), set while a working producer holds 500 records: each partition's head counts failed attempts and is dead-lettered with the configuration error, then the next head; the records behind a head are not charged |
| `test_external_primary_dead_letter_serves_local` | Once a forward is dead-lettered (`retry_count >= pg_kafka.shadow_max_retries`), an external-primary topic serves the record locally |
| `test_replay_historical_messages` | `kafka.replay_shadow_messages()` re-queues stored messages and they are forwarded |
| `test_replay_skips_aborted_records` | Replay never re-queues aborted transactional records |
| `test_sync_forward_skips_rolled_back_request` | A sync-mode Produce whose request rolls back (a test trigger raises on its second partition) leaves no copy on the external broker; a clean produce afterwards is still delivered |
| `test_shadow_forward_throughput` | 20 000 records forwarded end to end at ≥ 2000 msg/s with no failed attempts (pipelined forwarder) |
| `test_shadow_forwards_headers_and_timestamps` | Record headers (in order, binary values) and the producer's CreateTime reach the external topic |
| `test_shadow_forwards_a_header_key_holding_nul` | A record whose header keys hold U+0000 (`"a\0b"`, `"\0"`) is forwarded with its headers byte for byte, and the forwarder keeps forwarding: the next record of its topic and one of another shadow topic are delivered |
| `test_shadow_order_preserved_across_a_retried_failure` | An external topic rejects the batch holding an oversized record until its `max.message.bytes` is raised; nothing after the failed record is delivered before its retry and the external order equals the local order |
| `test_shadow_order_preserved_across_a_delivery_timeout` | With the external broker paused and records enqueued at staggered times (`pg_kafka.test_shadow_delivery_timeout_ms`), the first records' delivery timeout does not let later records overtake them once the broker resumes |
| `test_shadow_sync_stall_is_bounded_to_its_own_response` | External cluster unreachable: a sync produce is held ~`shadow_sync_timeout_ms` (3 s) while produces to another topic stay < 1 s |
| `test_shadow_gucs_take_effect_on_reload` | Session `SET` of shadow GUCs is refused; a security-protocol change rebuilds the producer; `shadow_metrics_enabled = off` writes no metrics; sampled-out records are counted in `messages_skipped` |
| `test_shadow_max_retries_refuses_zero` | `pg_kafka.shadow_max_retries = 0` (no record would ever be claimed) is refused |
| `test_shadow_backlog_does_not_starve_other_topics` | A large replayed backlog shares each claim with other partitions: another topic's record queued with it is forwarded while most of the backlog still waits |
| `test_shadow_full_producer_queue_does_not_block_sync_timeouts` | External cluster unreachable and librdkafka's queue full: held sync-mode responses are still released at their timeout |
| `test_shadow_backlogged_records_are_answered_before_their_in_flight_entry_expires` | Records waiting in the forwarder's backlog time out from their dispatch, so they are answered before the DB thread's in-flight eviction and never sent twice |
| `test_shadow_timed_out_transaction_is_not_forwarded` | A transaction abandoned by its producer is aborted by the timeout sweep and leaves no outbox row; nothing of it reaches external Kafka |
| `test_shadow_ack_application_error_loses_no_ack` | An ERROR while applying a delivery ack rolls the application back; the ack is applied on a later tick, with no lost attempt counted and no second forward |
| `test_shadow_failed_tick_commit_loses_no_ack` | A failed COMMIT of the tick that applied an ack: the ack is applied again later, the row stays in flight meanwhile, and the record is forwarded once |
| `test_shadow_stuck_partitions_do_not_throttle_a_backlog` | Partitions whose rows cannot be claimed (orphan pointers, dead letters, rows in flight or backing off) take no share of a claim, and a backlog drains claim after claim |
| `test_shadow_unconfigured_broker_does_not_spin_the_worker` | With `pg_kafka.shadow_bootstrap_servers` empty the worker does not re-claim the outbox without pause; the rows are forwarded once a broker is configured |
| `test_shadow_claims_stay_fast_with_a_full_in_flight_window` | The claim's in-flight exclusion is hashed: filling the 8 192-row in-flight window takes well under a second |
| `test_shadow_claims_stay_fast_behind_a_large_backlog` | A claim's cost does not grow with a 150 000-row backlog (ordered LATERAL probe, not a hash semi join) |
| `test_shadow_claims_stay_fast_with_a_tiny_work_mem` | The claim raises `work_mem` for itself, so a tiny server `work_mem` does not make it rescan the in-flight list per row |
| `test_shadow_claims_are_not_jit_compiled` | The claim turns JIT off for itself, so low JIT thresholds do not slow it (skipped without llvmjit) |
| `test_shadow_worker_stays_idle_beside_a_blocked_partition` | A backing-off head of a 200 000-row backlog does not make each tick read the partition's backlog: the idle worker's CPU stays within 3 points |
| `test_shadow_worker_stays_idle_beside_dead_letters` | 200 000 dead letters are skipped by the dead flag: the idle worker's CPU stays within bounds |
| `test_shadow_dead_letters_follow_the_retry_cap` | Dead letters follow `pg_kafka.shadow_max_retries` as it is raised and lowered, and a replay forwards them again |
| `test_shadow_failure_acks_make_dead_letters` | Failure acks that reach the retry cap flag their rows dead; raising the cap revives them, and they are forwarded once the external topic takes them |
| `test_shadow_lag_view_is_per_partition` | `kafka.shadow_partition_status` / `kafka.shadow_status` report lag, dead letters and in-flight counts per partition (needs no external Kafka; runs in the shadow job with the rest of the category) |
| `test_shadow_acks_from_a_dropped_schema_are_ignored` | Acks of forwards dispatched before DROP EXTENSION + CREATE EXTENSION (held back by `pg_kafka.test_forward_ack_delay_ms`) do not finalize the re-created schema's outbox rows that reuse their keys |
| `test_shadow_teardown_turns_shadow_mode_off` | The category's teardown, registered last: resets the settings its tests leave (shadow mode on, the test broker, a 2 s reload interval) and fails unless the server reports each at its default, so later categories and runs do not run with shadow mode on |

## Running Tests

```bash
# Run all shadow tests
./kafka_test/target/release/kafka_test --category shadow

# Run a specific test
./kafka_test/target/release/kafka_test --test test_dual_write_sync

# Run with verbose output
RUST_LOG=debug ./kafka_test/target/release/kafka_test --category shadow
```
