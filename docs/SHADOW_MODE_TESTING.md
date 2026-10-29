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
./scripts/run-shadow-tests.sh --test-only     # Build the test client if stale, then run tests (assumes setup done)
./scripts/run-shadow-tests.sh --docker-only   # Just start Docker containers
./scripts/run-shadow-tests.sh --skip-docker   # Skip Docker, just setup PostgreSQL

# Run a specific test
./scripts/run-shadow-tests.sh --test test_dual_write_sync
```

## What the Script Does

1. **Start Docker containers** - Starts `external_kafka` if not running (recreates it if it is running but unhealthy), then waits for the broker API, metadata requests, and topic creation to work
2. **Build pg_kafka extension** - Compiles with `cargo build --features pg14`
3. **Restart PostgreSQL** - Uses `./scripts/restart.sh` to load the extension
4. **Configure GUCs** - Sets the shadow mode PostgreSQL parameters with `ALTER SYSTEM`:
   - `pg_kafka.shadow_mode_enabled = 'true'`
   - `pg_kafka.shadow_bootstrap_servers = '<kafka_ip>:9095'`
   - `pg_kafka.shadow_security_protocol = 'PLAINTEXT'`
   - `pg_kafka.shadow_max_retries = '20'`
   - `pg_kafka.shadow_retry_backoff_ms = '500'`
   - `pg_kafka.config_reload_interval_ms = '2000'`
5. **Reload and wait** - Runs `SELECT pg_reload_conf()` and waits 1 second; the background worker handles the SIGHUP on its next loop iteration (every 100 ms), so no reload-interval wait is needed
6. **Build test client** - Compiles `kafka_test` in release mode if the binary is missing or older than `kafka_test/src`
7. **Run tests** - Executes the `shadow` test category (or the single test passed with `--test`)

Each test also re-applies the GUCs it needs through `enable_shadow_mode()` in `kafka_test/src/shadow/helpers.rs`, so step 4 mainly matters for manual runs.

## Network Architecture

The shadow mode tests require communication between:

1. **Host machine**: Runs E2E test code, connects to external Kafka via `localhost:9093`
2. **pg_kafka_dev container**: Runs PostgreSQL with pg_kafka extension
3. **external_kafka container**: External Kafka broker for shadow forwarding

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

4. **Host-run Postgres**: in CI, pg_kafka runs on the runner (pgrx-managed Postgres), where Docker's DNS name `external-kafka` does not resolve. The static IP is reachable from both the host and containers.

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

### Connection Refused Errors

If you see errors like:
```
external-kafka:9095/1: Connect to ipv4#172.23.0.2:9095 failed: Connection refused
```

The IP `172.23.0.2` is stale. Fix by:

1. Verify actual Kafka IP: `docker inspect external_kafka | grep IPAddress`
2. If it is not `172.18.0.2`, another network on the host may already claim the `172.18.0.0/16` subnet; free it, or change the subnet, `ipv4_address`, and CONTAINER advertised listener together in `docker-compose.yml` and set `INTERNAL_KAFKA_BOOTSTRAP_SERVERS` to match
3. Recreate Kafka container: `docker-compose up -d --force-recreate external-kafka`
4. Restart PostgreSQL: `./scripts/restart.sh`

### Producer Cache Issues

If the producer uses old bootstrap servers after a GUC change:

1. The forwarding producer lives in the network thread (`run_shadow_forwarder` in `src/kafka/listener.rs`) and is rebuilt when the reloaded `pg_kafka.shadow_bootstrap_servers` differs from the value it was built with
2. Make sure the change was reloaded (`SELECT pg_reload_conf()`); the forwarder reads the worker's reloaded config snapshot
3. Other producer settings (batch size, linger, retries, SASL/SSL) are applied only when the producer is rebuilt, so restart PostgreSQL to force a fresh producer: `./scripts/restart.sh`

### DNS Resolution

To check DNS resolution inside the devcontainer:
```bash
getent hosts external-kafka
```

Should show `172.18.0.2` (or current actual IP). If it shows a different IP, Docker DNS is stale.

## Test Categories

The `shadow` category registers 27 tests in `kafka_test/src/main.rs`, all `parallel_safe: false`:

| Test | Description |
|------|-------------|
| `test_dual_write_sync` | DualWrite + sync: stored locally and delivered to external Kafka |
| `test_dual_write_async` | DualWrite + async: stored locally and delivered by the outbox poll |
| `test_async_forwarding_no_duplicates_on_slow_ack` | Issue #93: with `pg_kafka.test_forward_ack_delay_ms` holding acks past the 5 s retry lease, each record reaches external Kafka exactly once |
| `test_external_only_mode` | ExternalOnly (external-primary): delivered externally, stored locally, but a pg_kafka consumer reads nothing |
| `test_local_only_mode` | `mode = 'local_only'`: stored locally, not forwarded |
| `test_reload_clears_deleted_topic_config` | Deleting a `kafka.shadow_config` row stops forwarding after reload |
| `test_outbox_row_written_and_finalized` | A forwarded record gets a `kafka.shadow_tracking` row whose `external_offset` is set |
| `test_zero_percent_forwarding` / `test_hundred_percent_forwarding` / `test_fifty_percent_forwarding` | Percentage-based routing at 0%, 100%, 50% |
| `test_deterministic_routing` | The same key always gets the same forward decision |
| `test_dialup_{0,10,25,50,75,100}_percent` | Dial-up routing at 0, 10, 25, 50, 75, 100 percent |
| `test_topic_name_mapping` | `external_topic_name` maps to a different external topic |
| `test_committed_transaction_forwarded` | Committed transaction records are forwarded |
| `test_aborted_transaction_not_forwarded` | Aborted transaction records are not forwarded |
| `test_committed_transaction_uses_durable_outbox` | Committed transaction records go through `kafka.shadow_tracking` |
| `test_committed_txn_leaves_no_orphan_outbox_rows` | A committed transaction leaves no outbox rows without a backing message |
| `test_dual_write_external_down` | DualWrite (async): the local write succeeds regardless of forwarding. It runs against the normal broker config and does not take the broker down |
| `test_external_only_fallback` | ExternalOnly produce succeeds. It accepts either local count, and with the current code the record is always stored locally |
| `test_external_primary_dead_letter_serves_local` | RV-6: once a forward is dead-lettered (`retry_count >= 10`), an external-primary topic serves the record locally |
| `test_replay_historical_messages` | `kafka.replay_shadow_messages()` re-queues stored messages and they are forwarded |
| `test_replay_skips_aborted_records` | Replay never re-queues aborted transactional records |

## Running Tests

```bash
# Run all shadow tests
./kafka_test/target/release/kafka_test --category shadow

# Run a specific test
./kafka_test/target/release/kafka_test --test test_dual_write_sync

# Run with verbose output
RUST_LOG=debug ./kafka_test/target/release/kafka_test --category shadow
```
