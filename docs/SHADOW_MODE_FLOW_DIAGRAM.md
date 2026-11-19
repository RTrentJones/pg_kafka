# Shadow Mode Flow Diagram

This document describes the shadow mode lifecycle in pg_kafka: configuration, the forwarder,
message flow, ordering, sync mode, observability, and the E2E setup.

Shadow mode forwards produced records to an external Kafka cluster through a **durable outbox**
(`kafka.shadow_tracking`). A produce never forwards inline: it writes the record locally and, in the
same transaction, a pending outbox pointer row. The DB thread's outbox tick claims due rows and hands
them to the network thread, whose forwarder owns the idempotent rdkafka producer, keeps many
deliveries in flight, and returns a `ForwardAck` per record. Delivery to the external broker is
at-least-once and, per local partition, in order. The code lives in `src/kafka/shadow/`
(`store.rs`: outbox logic on the DB thread; `outbox.rs`: its SQL; `forwarder_task.rs`: the
network-thread forwarder; `producer.rs`: librdkafka settings) and the worker loop in `src/worker.rs`.

## Architecture Overview

```
DB thread (SPI, single-threaded)                       network thread (tokio)
─────────────────────────────────                      ──────────────────────────────────────
produce → ShadowStore::insert_records
   1. inner.insert_records        → kafka.messages
   2. enqueue_outbox: percentage routing; INSERT pending
      rows into kafka.shadow_tracking (same subtxn);
      sampled-out records counted in shadow_metrics
   3. sync topic: rows noted under the request's slot
commit (group commit, RB-1)
   sync produce: response handed over ──Hold──────────▶ forwarder holds it until its records
   (other responses released at once)                   are delivered or shadow_sync_timeout_ms

outbox tick (every 250 ms, or at once after a full
claim / committed sync rows; backs off after failures)
   1. drain_forward_acks: apply acks in batches  ◀──ForwardAck── delivery reports (never dropped)
   2. poll_and_forward_outbox: claim ≤ batch_size ──ForwardRecord─▶ fence check → librdkafka
      rows (≤ in-flight room 8192), in row-key order                (pipelined: idempotent,
                                                                     max.in.flight 5, gapless)
                       crossbeam bounded(10 000) both ways                     │
                                                                               ▼
                                                          external Kafka (re-partitions by key,
                                                          murmur2 like the Java client)
```

Only the network thread talks to the external broker, and the DB thread never waits for it: not for
async topics, and not for sync topics either (the produce response waits, the worker does not).

## Configuration

GUCs (all reloadable with `pg_reload_conf()` unless noted; the forwarder reads the reloaded
`RuntimeContext` snapshot, the DB thread reads the GUCs directly):

| GUC | Default | Meaning |
|---|---|---|
| `pg_kafka.shadow_mode_enabled` | false | Global switch. |
| `pg_kafka.shadow_bootstrap_servers` | '' | External brokers. |
| `pg_kafka.shadow_security_protocol` | SASL_SSL | PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL. |
| `pg_kafka.shadow_sasl_mechanism` | PLAIN | PLAIN, SCRAM-SHA-256, SCRAM-SHA-512. |
| `pg_kafka.shadow_sasl_username` / `_password` | '' | Superuser-only, hidden from SHOW ALL. |
| `pg_kafka.shadow_ssl_ca_location` | '' | CA file for SSL. |
| `pg_kafka.shadow_batch_size` | 1000 | Records per outbox claim, and librdkafka `batch.num.messages`. |
| `pg_kafka.shadow_linger_ms` | 10 | librdkafka `linger.ms`. |
| `pg_kafka.shadow_retry_backoff_ms` | 100 | librdkafka `retry.backoff.ms`, and the base of the outbox re-attempt backoff (doubling per failure, capped at 30 s). |
| `pg_kafka.shadow_max_retries` | 10 | Failed delivery attempts after which a row is a dead letter. |
| `pg_kafka.shadow_sync_timeout_ms` | 5000 | How long a sync-mode produce response is held for the external ack (0 = not held). |
| `pg_kafka.shadow_metrics_enabled` | true | Write `kafka.shadow_metrics`. |
| `pg_kafka.shadow_external_primary_strict` | false | See "Read Path". |
| `pg_kafka.shadow_license_key` | '' | Restart; read once at worker start. |

Every shadow GUC except the license key is `Sighup`: it is set in `postgresql.conf` or with
`ALTER SYSTEM` and reloaded. A session `SET` is refused, since it could never reach the worker (the
batching, retry and metrics GUCs used to be `Suset`, which accepted a session `SET` with no effect;
the SASL/SSL ones used to need a restart). Any change to a producer setting (servers, protocol, SASL,
CA, batch size, linger, backoff) makes the forwarder rebuild its producer, on the next record or
within about 0.5 s; the old producer's outstanding records are purged and re-sent by the new one
without counting as failures. The config goes through `ShadowConfig::validate()` first; an invalid
or unbuildable config is logged once per distinct error and every record fails (and is eventually
dead-lettered) until it is fixed.

Per-topic settings are rows of `kafka.shadow_config` (`mode`, `forward_percentage`,
`external_topic_name`, `sync_mode`, `write_mode`), reloaded into the `TopicConfigCache` on SIGHUP or
every `pg_kafka.config_reload_interval_ms`. The reload replaces the cache (a deleted row stops
forwarding) only when the query succeeds. A row with `write_mode = 'external_only'` and
`forward_percentage <> 100` is forced to 100% with a warning, because a sampled-out record would
otherwise be readable nowhere.

## The Forwarder (network thread)

`run_shadow_forwarder()` (`forwarder_task.rs`) receives `ForwardRequest`s (records and sync holds)
in batches from a blocking-pool bridge and waits on three things at once: new requests, delivery
reports of the records in flight, and a 20 ms tick for hold timeouts and settings changes.

A record is handed to librdkafka as soon as it arrives (`ShadowProducer::enqueue`, no wait for the
previous delivery); a full librdkafka queue pauses intake briefly, keeping order. Key, value,
headers and the timestamp are forwarded (`kafka.messages.timestamp_ms`, or the insert time when the
producer sent none, which is what local consumers see). `partition` is left to the partitioner
(`murmur2_random`, the Java client's), so the external cluster re-partitions by key.

### Ordering

The records of one local partition reach the external topic in local-offset order (the first copy
of each; see duplicates below). That rests on:

1. **The claim.** Rows are claimed in row-key order and dispatched over one FIFO channel. A row is
   never claimed behind an earlier row of its partition that is backing off after a failure.
2. **The producer.** `enable.idempotence=true`, `acks=all`, `max.in.flight.requests.per.connection=5`
   keep the order on the wire through librdkafka's own retries. `enable.gapless.guarantee=true`
   turns any error that could deliver a later record after an earlier failed one into a fatal
   producer error that fails every outstanding record; the forwarder then rebuilds the producer.
3. **The fence.** Once a record of a partition fails, every later record of that partition the
   forwarder receives is answered `NotAttempted` without being sent, until the partition restarts:
   a record at or below the failed offset, or one the claim marks as the partition head (`restart`,
   the lowest pending row, which is how a partition moves on after its failed record is
   dead-lettered).

Failures are classified: a real delivery failure is `Failed` (counts against
`shadow_max_retries`); a record fenced, failed only because the producer went fatal on another
record's error (the first failure of a fatal producer generation is taken as its cause), purged by a
settings change, or received while shadow mode is off is `NotAttempted` and does not consume its
retry budget.

Trade-off: with the gapless guarantee one poison record (for example one over the external topic's
`max.message.bytes`) makes the producer fatal on each attempt, so every partition's in-flight
records are re-sent until the poison record is dead-lettered. That is the price of never
reordering a partition; `shadow_max_retries` and the backoff bound it.

### Held sync-mode responses

A `Hold` carries the buffered response of a committed sync-mode produce and the outbox rows it waits
for. The forwarder releases it when every one of those rows is delivered, or at the timeout. If a
hold cannot be handed over (full channel, no forwarder), or is dropped, the response is released at
once (`HeldRelease` releases on drop), so a client is never left hanging.

## Message Flow: Produce Path

`ShadowStore::insert_records()`: if shadow mode is off or this node is a standby, it only writes
locally. Otherwise it writes locally (real offsets), then, when the topic forwards
(`mode = 'shadow'`, `forward_percentage > 0`), `enqueue_outbox()` routes each record
(`make_forward_decision`: murmur2 of the key, or the offset for a null key, `% 100 < pct`), counts
the skipped ones in `kafka.shadow_metrics.messages_skipped`, and inserts one pending
`kafka.shadow_tracking` row per selected record in the same subtransaction. `dual_write` and
`external_only` write identically; `write_mode` only governs reads.

### Read Path (external_only)

`external_only` marks a topic as **external-primary**. Its writes are forwarded as above, but local
reads are suppressed so consumers cut over to the external broker: fetches return no records and the
offset bounds report an empty partition. Suppression depends only on the global GUC plus the
per-topic config, so a standby suppresses these reads too. If any forward of the topic is
dead-lettered (`external_offset IS NULL` and `retry_count >= pg_kafka.shadow_max_retries`),
suppression is lifted and the topic is served locally again, so a record is never readable nowhere;
`pg_kafka.shadow_external_primary_strict = true` keeps full suppression.

### Transactional Produce

`insert_transactional_records()` writes locally and, if the topic forwards, buffers the records'
keys in RAM. `commit_transaction()` reads the transaction's records from `kafka.messages` (falling
back to the RAM buffer), commits, re-checks the topic config, routes them and inserts their outbox
rows in the commit transaction. An abort discards them. Transactional records are forwarded by the
outbox tick (they are never held as sync).

## Sync vs Async Forwarding

`sync_mode` only changes **when the client gets its response**, never whether the record is durable
or how it is forwarded. Both modes write the same outbox row and are dispatched by the same claim.

- **async**: the response is released right after the commit; the outbox tick forwards the row
  within about 250 ms.
- **sync**: after the commit the worker takes the request's committed sync rows
  (`take_sync_holds`, by request slot; a rolled-back request's rows are gone and nothing is held),
  hands its response to the forwarder (`hold_response`) and asks for an immediate outbox tick. The
  response is released when the records are delivered to the external cluster, or after
  `pg_kafka.shadow_sync_timeout_ms`; on timeout the record stays pending and is retried like any
  other. Nothing is sent before the commit, so a rolled-back request never leaves a copy on the
  external broker.

What sync guarantees: an OK response means the record was committed locally, and, unless the timeout
expired first, delivered externally. It does not stall the broker: only that client's response
waits (and the responses behind it on the same connection, which Kafka orders), the worker and every
other client carry on. The old design waited on the DB thread for up to 5 s per batch, stalling all
clients while the external cluster was slow or down. With `shadow_sync_timeout_ms = 0` sync topics
behave like async ones.

## Outbox Rows, Retries and Dead Letters

`kafka.shadow_tracking` columns: `external_offset` (set = delivered), `forwarded_at` (last claim or
failure), `retry_count` (failed delivery attempts), `attempt_open` (claimed, ack not yet applied),
`error_message` (last failure; withheld from `pgkafka_reader`).

The claim (`SpiShadowDb::claim_due_outbox_rows`) takes, in row-key order, up to
`min(shadow_batch_size, 8192 - in flight)` rows that are pending, under the retry cap, not in flight
in this worker, whose message still exists, that are not backing off (failed, attempt closed, and
`forwarded_at` within `retry_backoff_ms * 2^(retry_count-1)`, at most 30 s), and that have no
backing-off row of their partition below them. It opens their attempts and returns payload, headers,
timestamp, external topic name and the `restart` flag. It reads the pending index up to the limit
(O(limit), not O(backlog)); the first backing-off row per partition comes from the small partial
index `idx_shadow_tracking_retrying`. A full claim requests another tick at once.

Acks are applied in three statements per tick (delivered, failed, not attempted). Delivered sets
`external_offset` and counts `messages_forwarded` once per row (a duplicate ack matches nothing).
Failed bumps `retry_count`, records the error, starts the backoff and counts `messages_failed`; at the
cap the row is a dead letter, logged once. Not attempted only closes the attempt. A row whose attempt
is still open but that this worker no longer tracks (worker restart, or an in-flight entry evicted
after 150 s, above librdkafka's 120 s message timeout) lost its ack: the claim takes it again and
counts the lost attempt as a failure, so a forward that never answers still reaches the cap. A dead
letter stays pending until it is replayed (`kafka.replay_shadow_messages`) or the cap is raised.

At-least-once and duplicates: the idempotent producer suppresses duplicates of its own retries
within one producer session. A record re-sent by a new producer (rebuilt after a fatal error or a
settings change) or after a lost ack can land twice. Consumers that need exactly-once should
deduplicate on an application id.

### Replay

`kafka.replay_shadow_messages(p_topic_id INT, p_from_offset BIGINT DEFAULT 0, p_to_offset BIGINT DEFAULT NULL)`
inserts (or resets to pending, closing any open attempt) a `kafka.shadow_tracking` row for every
committed or non-transactional message of that topic in `[p_from_offset, p_to_offset)` (`NULL` = to
the end), across all partitions, and returns the number of rows re-queued.

```sql
SELECT kafka.replay_shadow_messages(
    (SELECT id FROM kafka.topics WHERE name = 'orders'), 0, NULL);
```

### Observability

- `kafka.shadow_partition_status` (view, one row per partition that has outbox rows or metrics):
  `lag` = records of the partition still to forward (pending rows under the cap; exact under
  sampling, since sampled-out records are never queued, and 0 when caught up), `dead_lettered`,
  `in_flight`, `oldest_pending_offset`, `last_forwarded_offset`, and the three counters.
- `kafka.shadow_status` (view, per topic): config, summed counters, `lag` (sum of the partition
  lags), `dead_lettered`, `max_partition_lag`. (Its `lag` used to be `MAX(partition_offset) -
  MAX(last_forwarded_offset)` across partitions: it hid a lagging partition, was off by one and never
  reached 0 under sampling.)
- `kafka.shadow_metrics`: per-partition `messages_forwarded`, `messages_failed`, `messages_skipped`,
  `last_forwarded_offset`, `last_forwarded_at`, written only while `shadow_metrics_enabled`.
- `kafka.metrics`, category `shadow`: `forwarded`, `failed`, `lag` (all pending rows, dead letters
  included).
- Logs: no per-produce lines (`insert_records` used to log twice per Produce, even with shadow mode
  off); a failing outbox tick (for example after `DROP EXTENSION`) pauses up to 30 s and logs at the
  1st, 2nd, 4th, 8th... failure in a row.

## E2E Test Setup Sequence

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         E2E TEST SETUP SEQUENCE                                  │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│   ┌─────────────────────────────────────────────────────────────────────────┐   │
│   │  ONE-TIME SETUP (shadow_test_setup, kafka_test/src/shadow/helpers.rs)    │   │
│   │  Runs once per test process via an AtomicBool guard                     │   │
│   │                                                                          │   │
│   │  ┌─────────────────────────────────────────────────────────────────┐    │   │
│   │  │  1. ALTER SYSTEM SET pg_kafka.config_reload_interval_ms = 2000  │    │   │
│   │  │  2. SELECT pg_reload_conf()   (SIGHUP → immediate reload)       │    │   │
│   │  │  3. Sleep 500 ms              (worker loop wakes every 100 ms)  │    │   │
│   │  └─────────────────────────────────────────────────────────────────┘    │   │
│   └─────────────────────────────────────────────────────────────────────────┘   │
│                                        │                                         │
│                                        ▼                                         │
│   ┌─────────────────────────────────────────────────────────────────────────┐   │
│   │  PER-TEST SETUP (enable_shadow_mode)                                     │   │
│   │                                                                          │   │
│   │  ┌─────────────────────────────────────────────────────────────────┐    │   │
│   │  │  1. Ensure topic exists (get/create topic_id)                    │    │   │
│   │  │  2. ALTER SYSTEM SET pg_kafka.shadow_mode_enabled = true         │    │   │
│   │  │  3. ALTER SYSTEM SET pg_kafka.shadow_bootstrap_servers =         │    │   │
│   │  │       get_internal_bootstrap_servers()  (172.18.0.2:9095)       │    │   │
│   │  │  4. ALTER SYSTEM SET pg_kafka.shadow_security_protocol =         │    │   │
│   │  │       'PLAINTEXT'                                                │    │   │
│   │  │  5. INSERT INTO kafka.shadow_config ... ON CONFLICT (topic_id)   │    │   │
│   │  │       DO UPDATE  (BEFORE the reload, so the reload sees it)      │    │   │
│   │  │  6. SELECT pg_reload_conf()                                      │    │   │
│   │  │  7. Sleep 500 ms                                                 │    │   │
│   │  └─────────────────────────────────────────────────────────────────┘    │   │
│   └─────────────────────────────────────────────────────────────────────────┘   │
│                                        │                                         │
│                                        ▼                                         │
│   ┌─────────────────────────────────────────────────────────────────────────┐   │
│   │  TEST EXECUTION                                                          │   │
│   │                                                                          │   │
│   │  1. Verify external Kafka ready (producer.fetch_metadata, 10s timeout)  │   │
│   │  2. Produce message to pg_kafka (port 9092)                             │   │
│   │  3. Verify in local DB (ctx.db().query)                                 │   │
│   │  4. Verify in external Kafka (consumer from localhost:9093)             │   │
│   └─────────────────────────────────────────────────────────────────────────┘   │
│                                        │                                         │
│                                        ▼                                         │
│   ┌─────────────────────────────────────────────────────────────────────────┐   │
│   │  CLEANUP (TestContext::cleanup)                                          │   │
│   │                                                                          │   │
│   │  - Delete messages from kafka.messages                                  │   │
│   │  - Delete topic from kafka.topics (cascades kafka.shadow_config and     │   │
│   │    kafka.shadow_tracking rows)                                          │   │
│   │  - Delete consumer offsets for created groups                           │   │
│   │  - GUCs are deliberately NOT reset (a reset SIGHUP could race the next  │   │
│   │    test's setup); disable_shadow_mode() resets them when a test asks    │   │
│   └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## Potential Issues for E2E Tests

### Issue 1: External Kafka Not Running (CRITICAL)

```
┌──────────────────────────────────────────────────────────────────────────────┐
│ FAILURE MODE: External Kafka broker not available                            │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Test expects:    localhost:9093 (external Kafka, EXTERNAL listener)         │
│  Test expects:    localhost:9092 (pg_kafka)                                  │
│  pg_kafka uses:   172.18.0.2:9095 (external Kafka, CONTAINER listener)       │
│                                                                              │
│  If 9093 not running:                                                        │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ verify_external_kafka_ready() times out after 10s                      │ │
│  │ Test fails with: "Cannot connect to external Kafka"                    │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  FIX: docker compose up -d external-kafka (ci.yml does this before the      │
│       shadow category runs)                                                  │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

### Issue 2: Config Reload Timing (FIXED)

```
┌──────────────────────────────────────────────────────────────────────────────┐
│ HISTORICAL ISSUE: Atomic flag across processes doesn't work                  │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  OLD BROKEN APPROACH:                                                        │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ static RELOAD_FLAG: AtomicBool (in backend process)                    │ │
│  │                       ↓                                                │ │
│  │ Background worker in SEPARATE PROCESS cannot see this flag!            │ │
│  │ PostgreSQL uses fork(), not threads                                    │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  CURRENT WORKING APPROACH:                                                   │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ The worker handles SIGHUP: pg_reload_conf() sets sighup_pending, and   │ │
│  │ the next loop iteration (≤ 100 ms) calls ProcessConfigFile, reloads    │ │
│  │ the RuntimeContext, and reloads kafka.shadow_config.                   │ │
│  │ pg_kafka.config_reload_interval_ms is the periodic fallback (default   │ │
│  │ 30s, tests 2000ms). Tests wait 500 ms after pg_reload_conf().          │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  STATUS: ✅ FIXED                                                            │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

### Issue 3: Channel Backpressure

Neither channel loses anything. The DB thread dispatches at most 8192 records in flight, below the
forward channel's 10 000, so a record send does not fail for lack of room (if it ever does, the
attempt is closed without counting it and the row is claimed again). The forwarder keeps acks the
ack channel cannot take yet and sends them later instead of dropping them.

### Issue 4: Test Isolation via Sequential Execution

```
┌──────────────────────────────────────────────────────────────────────────────┐
│ REQUIREMENT: Shadow tests must run sequentially                              │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  All shadow tests modify GLOBAL GUCs:                                        │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ ALTER SYSTEM SET pg_kafka.shadow_mode_enabled = true                   │ │
│  │ ALTER SYSTEM SET pg_kafka.shadow_bootstrap_servers = '172.18.0.2:9095' │ │
│  │ ALTER SYSTEM SET pg_kafka.test_forward_ack_delay_ms = ... (one test)   │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  If run in parallel:                                                         │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ Test A: sets bootstrap_servers = 'kafka1:9092'                         │ │
│  │ Test B: sets bootstrap_servers = 'kafka2:9092'                         │ │
│  │ Test A's messages might go to kafka2! (race condition)                 │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  MITIGATION:                                                                 │
│  - All 27 shadow tests are marked parallel_safe: false in main.rs           │
│  - Tests use unique topic names via ctx.unique_topic(...)                    │
│                                                                              │
│  STATUS: ✅ MITIGATED                                                        │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

### Issue 5: One-Time Setup Delay (FIXED)

```
┌──────────────────────────────────────────────────────────────────────────────┐
│ TIMING: One-time setup no longer waits for the default reload interval       │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  HISTORICAL: the one-time setup slept 31 s so the worker's 30 s default     │
│  reload interval could pick up the 2 s test interval.                        │
│                                                                              │
│  CURRENT TIMELINE:                                                           │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ 0s:    shadow_test_setup() runs (first test only)                      │ │
│  │ 0s:    ALTER SYSTEM SET config_reload_interval_ms = 2000               │ │
│  │ 0s:    SELECT pg_reload_conf()  → SIGHUP handled within ~100 ms        │ │
│  │ 0.5s:  enable_shadow_mode(): set GUCs, upsert shadow_config, reload    │ │
│  │ 1.0s:  Actual test execution begins                                    │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  Remaining per-test time is dominated by each test's own forwarding waits.   │
│                                                                              │
│  STATUS: ✅ FIXED (SIGHUP-driven reload)                                     │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

## Docker Networking Issue (FIXED)

```
┌──────────────────────────────────────────────────────────────────────────────┐
│ HISTORICAL: Kafka Advertised Listener Mismatch                               │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ORIGINAL DOCKER-COMPOSE CONFIGURATION (single listener):                    │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │  pg_kafka_dev container:                                               │ │
│  │    PG_KAFKA_SHADOW_BOOTSTRAP_SERVERS=external-kafka:9093              │ │
│  │                                                                        │ │
│  │  external-kafka container:                                             │ │
│  │    KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9093              │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  WHAT HAPPENED:                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │  1. pg_kafka connects to external-kafka:9093 (works - docker network)  │ │
│  │  2. External Kafka returns metadata: "Use localhost:9093"              │ │
│  │  3. pg_kafka tries to produce to localhost:9093                        │ │
│  │  4. localhost inside pg_kafka_dev container = itself (NOT external!)   │ │
│  │  5. CONNECTION FAILS - no Kafka broker on pg_kafka_dev:9093            │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  DIAGRAM:                                                                    │
│                                                                              │
│  ┌─────────────────────┐         ┌─────────────────────┐                    │
│  │   pg_kafka_dev      │         │   external-kafka    │                    │
│  │   container         │         │   container         │                    │
│  │                     │         │                     │                    │
│  │  pg_kafka extension │──(1)───▶│   :9093 listener    │                    │
│  │                     │         │                     │                    │
│  │                     │◀──(2)───│  Returns metadata:  │                    │
│  │                     │         │  "localhost:9093"   │                    │
│  │                     │         │                     │                    │
│  │  localhost:9093 ◀───┼──(3)────│                     │                    │
│  │  (NOTHING HERE!)    │  FAIL!  │                     │                    │
│  └─────────────────────┘         └─────────────────────┘                    │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘

OPTIONS CONSIDERED:

Option A: Add a second listener to external-kafka (CHOSEN, extended to three listeners below)
┌──────────────────────────────────────────────────────────────────────────────┐
│  INTERNAL://external-kafka:9094 for containers, EXTERNAL://localhost:9093   │
│  for the host                                                                │
└──────────────────────────────────────────────────────────────────────────────┘

Option B: Use host network mode
┌──────────────────────────────────────────────────────────────────────────────┐
│  network_mode: host  (for both containers)                                   │
│  - All containers share host's network namespace                             │
│  - localhost:9093 works from everywhere                                      │
│  - May conflict with host services                                           │
└──────────────────────────────────────────────────────────────────────────────┘

Option C: Use docker host IP (brittle)
┌──────────────────────────────────────────────────────────────────────────────┐
│  KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://host.docker.internal:9093          │
│  pg_kafka.shadow_bootstrap_servers = 'host.docker.internal:9093'            │
│  - Requires Docker Desktop (macOS/Windows) or extra_hosts on Linux          │
│  - Not portable                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

## Summary: E2E Test Prerequisites

| Requirement | Status | Notes |
|-------------|--------|-------|
| pg_kafka running on 9092 | Required | `cargo pgrx start pg14` |
| External Kafka on 9093 (host) and 172.18.0.2:9095 (pg_kafka) | Required | `docker compose up -d external-kafka` |
| Docker network fix | ✅ Fixed | Triple listener + static IP on a pinned subnet |
| PLAINTEXT protocol | Default | Tests assume no SASL |
| Sequential execution | Required | `parallel_safe: false` on all 27 shadow tests |
| Config reload interval | Auto-set | Tests set it to 2000 ms; SIGHUP makes changes immediate |

## Implemented Fix (docker-compose.yml + test code)

### Understanding the Multi-Address Architecture

Two different bootstrap addresses are needed because the E2E test process and the pg_kafka
extension reach the broker from different places:

- **E2E tests** connect from the host via the **EXTERNAL** listener, `localhost:9093`.
- **pg_kafka** connects via the **CONTAINER** listener, which advertises the static IP
  `172.18.0.2:9095`. That address works both from inside the `pg_kafka_dev` container and from a
  host-run Postgres (CI runs pgrx on the runner, where Docker's DNS for `external-kafka` does not
  resolve).

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

### Docker Compose Configuration

```yaml
external-kafka:
  container_name: external_kafka
  ports:
    - "9093:9093"    # EXTERNAL listener for host access
  environment:
    KAFKA_LISTENERS: INTERNAL://0.0.0.0:9094,CONTAINER://0.0.0.0:9095,EXTERNAL://0.0.0.0:9093,CONTROLLER://0.0.0.0:29094
    KAFKA_ADVERTISED_LISTENERS: INTERNAL://external-kafka:9094,CONTAINER://172.18.0.2:9095,EXTERNAL://localhost:9093
    KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INTERNAL:PLAINTEXT,CONTAINER:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT
    KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
  networks:
    pg_kafka_network:
      ipv4_address: 172.18.0.2

networks:
  pg_kafka_network:
    driver: bridge
    ipam:
      config:
        - subnet: 172.18.0.0/16
          gateway: 172.18.0.1
```

The `pg_kafka_dev` service still sets `PG_KAFKA_SHADOW_BOOTSTRAP_SERVERS=external-kafka:9094`, but the
E2E tests override the GUC with `ALTER SYSTEM` on every `enable_shadow_mode()` call.

### Test Code Configuration

```rust
// kafka_test/src/shadow/external_client.rs

/// For HOST-based test verification (E2E tests run on host)
pub fn get_external_bootstrap_servers() -> String {
    env::var("EXTERNAL_KAFKA_BOOTSTRAP_SERVERS").unwrap_or_else(|_| "localhost:9093".to_string())
}

/// For the pg_kafka extension (the value written to pg_kafka.shadow_bootstrap_servers)
pub fn get_internal_bootstrap_servers() -> String {
    env::var("INTERNAL_KAFKA_BOOTSTRAP_SERVERS").unwrap_or_else(|_| "172.18.0.2:9095".to_string())
}
```

### Address Selection

There is no environment auto-detection. Each address is an environment variable with a fixed
default:

| Address | Env override | Default | Used by |
|---------|--------------|---------|---------|
| External (host-side) | `EXTERNAL_KAFKA_BOOTSTRAP_SERVERS` | `localhost:9093` | Test consumers/producers verifying delivery |
| Internal (pg_kafka-side) | `INTERNAL_KAFKA_BOOTSTRAP_SERVERS` | `172.18.0.2:9095` | `pg_kafka.shadow_bootstrap_servers` |

```rust
// kafka_test/src/shadow/helpers.rs

pub async fn enable_shadow_mode(...) {
    // GUC is for pg_kafka - use the CONTAINER-listener address
    let bootstrap_servers = get_internal_bootstrap_servers();  // "172.18.0.2:9095"
    db.execute(&format!(
        "ALTER SYSTEM SET pg_kafka.shadow_bootstrap_servers = '{}'",
        bootstrap_servers
    ), &[]).await?;
}
```
