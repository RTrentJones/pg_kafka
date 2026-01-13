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
./scripts/run-shadow-tests.sh --test-only     # Just run tests (assumes setup done)
./scripts/run-shadow-tests.sh --docker-only   # Just start Docker containers
./scripts/run-shadow-tests.sh --skip-docker   # Skip Docker, just setup PostgreSQL

# Run a specific test
./scripts/run-shadow-tests.sh --test test_dual_write_sync
```

## What the Script Does

1. **Start Docker containers** - Starts `external_kafka` if not running
2. **Build pg_kafka extension** - Compiles with `cargo build --features pg14`
3. **Restart PostgreSQL** - Uses `./restart.sh` to load the extension
4. **Configure GUCs** - Sets all shadow mode PostgreSQL parameters:
   - `pg_kafka.shadow_mode_enabled = 'true'`
   - `pg_kafka.shadow_bootstrap_servers = '<kafka_ip>:9095'`
   - `pg_kafka.shadow_security_protocol = 'PLAINTEXT'`
   - `pg_kafka.shadow_config_reload_interval_ms = '2000'`
5. **Build test client** - Compiles `kafka_test` in release mode
6. **Run tests** - Executes shadow mode test category

## Network Architecture

The shadow mode tests require communication between:

1. **Host machine**: Runs E2E test code, connects to external Kafka via `localhost:9093`
2. **pg_kafka_dev container**: Runs PostgreSQL with pg_kafka extension
3. **external_kafka container**: External Kafka broker for shadow forwarding

### Kafka Listeners

The external Kafka broker has three listeners:

| Listener   | Port | Purpose                              | Advertised Address |
|------------|------|--------------------------------------|-------------------|
| INTERNAL   | 9094 | Inter-broker (KRaft)                 | external-kafka:9094 |
| CONTAINER  | 9095 | Container-to-container (pg_kafka)    | 172.18.0.2:9095   |
| EXTERNAL   | 9093 | Host access (E2E tests, kcat)        | localhost:9093    |

### Why Use Direct IP for CONTAINER Listener?

The CONTAINER listener advertises `172.18.0.2:9095` instead of `external-kafka:9095` because:

1. **Stale DNS**: In devcontainer environments, DNS resolution for `external-kafka` can become stale when containers are recreated, pointing to old IP addresses.

2. **Metadata Redirection**: When rdkafka connects to a Kafka broker, it receives metadata containing the broker's advertised address. Subsequent connections use this advertised address, not the original bootstrap server.

3. **Deterministic IP**: Docker assigns `172.18.0.2` consistently to the first non-gateway container in the network.

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

Topics are configured for shadow mode via the `kafka.topic_shadow_config` table:

```sql
INSERT INTO kafka.topic_shadow_config (topic_name, enabled, write_mode, sync_mode, forward_percentage)
VALUES ('my-topic', true, 'dual_write', 'sync', 100);
```

## Troubleshooting

### Connection Refused Errors

If you see errors like:
```
external-kafka:9095/1: Connect to ipv4#172.23.0.2:9095 failed: Connection refused
```

The IP `172.23.0.2` is stale. Fix by:

1. Verify actual Kafka IP: `docker inspect external_kafka | grep IPAddress`
2. Update `docker-compose.yml` CONTAINER advertised listener if IP changed
3. Recreate Kafka container: `docker-compose up -d --force-recreate external-kafka`
4. Restart PostgreSQL: `./restart.sh`

### Producer Cache Issues

If the producer uses old bootstrap servers after a GUC change:

1. The producer cache now detects config changes automatically
2. Ensure you're using the latest code with `producer_bootstrap_servers` tracking
3. Force a fresh producer by restarting PostgreSQL: `./restart.sh`

### DNS Resolution

To check DNS resolution inside the devcontainer:
```bash
getent hosts external-kafka
```

Should show `172.18.0.2` (or current actual IP). If it shows a different IP, Docker DNS is stale.

## Test Categories

The shadow mode tests cover:

| Test | Description |
|------|-------------|
| `test_dual_write_sync` | Write to local + external, sync mode |
| `test_dual_write_async` | Write to local + external, async mode |
| `test_external_only_mode` | Write to external only |
| `test_local_only_mode` | Write to local only (shadow disabled) |
| `test_*_percent_forwarding` | Percentage-based routing (0%, 50%, 100%) |
| `test_dialup_*_percent` | Dial-up routing at various percentages |
| `test_topic_name_mapping` | Custom external topic names |
| `test_committed_transaction_forwarded` | Transaction commits forwarded |
| `test_aborted_transaction_not_forwarded` | Aborted transactions not forwarded |
| `test_dual_write_external_down` | Graceful degradation when external down |
| `test_external_only_fallback` | Fallback to local when external fails |
| `test_replay_historical_messages` | Replay historical messages to external |

## Running Tests

```bash
# Run all shadow tests
./kafka_test/target/release/kafka_test --category shadow

# Run a specific test
./kafka_test/target/release/kafka_test --test test_dual_write_sync

# Run with verbose output
RUST_LOG=debug ./kafka_test/target/release/kafka_test --category shadow
```
