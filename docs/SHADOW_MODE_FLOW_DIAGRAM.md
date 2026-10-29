# Shadow Mode Flow Diagram

This document describes the shadow mode lifecycle in pg_kafka, including configuration, client creation, message flow, and potential issues.

Shadow mode forwards produced records to an external Kafka cluster through a **durable outbox**
(`kafka.shadow_tracking`). A produce never forwards inline: it writes the record locally and, in the
same transaction, a pending outbox pointer row. The DB thread's periodic poll claims due rows and
hands them to the network thread, which owns the idempotent rdkafka producer and returns a
`ForwardAck` per attempt. Delivery to the external broker is at-least-once. The code lives in
`src/kafka/shadow/` (`store.rs` is the core), `src/kafka/listener.rs` (`run_shadow_forwarder`), and the
worker loop in `src/worker.rs`.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                     pg_kafka Background Worker process                           │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                    DB Thread (SPI, single-threaded)                      │    │
│  │                                                                          │    │
│  │  ┌──────────────┐    ┌──────────────┐    ┌──────────────────────────┐   │    │
│  │  │ GUCs (read   │───▶│ ShadowStore  │───▶│ TopicConfigCache         │   │    │
│  │  │ directly)    │    │ (decorator   │    │ (kafka.shadow_config,    │   │    │
│  │  └──────────────┘    │ over         │    │  keyed by topic_id)      │   │    │
│  │                      │ PostgresStore│    └──────────────────────────┘   │    │
│  │                      └──────┬───────┘                                   │    │
│  │                             ▼                                           │    │
│  │  ┌────────────────────────────────────────────────────────────────────┐ │    │
│  │  │ insert_records()  (produce, inside the request's subtransaction)   │ │    │
│  │  │  is_enabled()? ─▶ is_primary()? ─▶ topic_cache.get(topic_id)        │ │    │
│  │  │                                                                    │ │    │
│  │  │  1. inner.insert_records()          → kafka.messages (local write) │ │    │
│  │  │  2. enqueue_outbox(): percentage routing, then INSERT pending      │ │    │
│  │  │     rows into kafka.shadow_tracking (same transaction)             │ │    │
│  │  │  3. sync_mode = sync only: forward_sync_bounded() sends the rows   │ │    │
│  │  │     now and waits ≤ 5s (SYNC_FORWARD_WAIT_MS) for their acks       │ │    │
│  │  │                                                                    │ │    │
│  │  │  write_mode (dual_write | external_only) does NOT change this      │ │    │
│  │  │  path; it only controls READS (external_only suppresses local      │ │    │
│  │  │  fetches, see "Read Path")                                         │ │    │
│  │  └────────────────────────────────────────────────────────────────────┘ │    │
│  │                                                                          │    │
│  │  ┌────────────────────────────────────────────────────────────────────┐ │    │
│  │  │ Outbox poll (worker loop, every 250 ms while shadow_mode_enabled)  │ │    │
│  │  │  1. drain_forward_acks()     → apply_ack() finalizes rows          │ │    │
│  │  │  2. poll_and_forward_outbox() → claim ≤ 256 due rows, join         │ │    │
│  │  │     kafka.messages for the payload, try_send(ForwardRequest)       │ │    │
│  │  └──────────────┬──────────────────────────────────▲──────────────────┘ │    │
│  └─────────────────┼──────────────────────────────────┼────────────────────┘    │
│                    │ ForwardRequest                   │ ForwardAck              │
│                    │ crossbeam bounded(10_000)        │ crossbeam bounded(10_000)│
│                    ▼                                  │                         │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                 Network Thread: Tokio Runtime (2 workers)                │    │
│  │                                                                          │    │
│  │   ┌──────────────────────────────────────────────────────────────┐      │    │
│  │   │  run_shadow_forwarder() task (listener.rs)                   │      │    │
│  │   │   1. spawn_blocking(recv_timeout 100ms from forward_rx)      │      │    │
│  │   │   2. (re)create ShadowProducer if bootstrap_servers changed  │      │    │
│  │   │   3. producer.send_async_offset(topic, partition=None, ...)  │      │    │
│  │   │   4. ack_tx.try_send(ForwardAck { Ok(ext_offset) | Err })    │      │    │
│  │   └──────────────────────────────────────────────────────────────┘      │    │
│  │                                                                          │    │
│  │   ┌──────────────────────────────────────────────────────────────┐      │    │
│  │   │      TCP Listener (port 9092) — Kafka client connections     │      │    │
│  │   └──────────────────────────────────────────────────────────────┘      │    │
│  └──────────────────────────────────┬──────────────────────────────────────┘    │
└─────────────────────────────────────┼───────────────────────────────────────────┘
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          External Kafka Cluster                                  │
│               (pg_kafka.shadow_bootstrap_servers; re-partitions by key)          │
└─────────────────────────────────────────────────────────────────────────────────┘
```

Only the network thread talks to the external broker. The DB thread never blocks on rdkafka; the
only wait on the DB thread is the bounded sync wait (at most 5 s per produce), which receives acks
from the network thread over the ack channel.

## Configuration Lifecycle

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          CONFIGURATION SOURCES                                   │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│   ┌────────────────────────────────────────────────────────────────────────┐    │
│   │                          postgresql.conf / GUCs                         │    │
│   │                                                                         │    │
│   │  Reloadable (Sighup, ALTER SYSTEM + pg_reload_conf()):                  │    │
│   │  pg_kafka.shadow_mode_enabled = true                  (default false)  │    │
│   │  pg_kafka.shadow_bootstrap_servers = 'kafka1:9092,kafka2:9092'         │    │
│   │  pg_kafka.shadow_security_protocol = 'SASL_SSL' | 'PLAINTEXT' ...      │    │
│   │  pg_kafka.config_reload_interval_ms = 30000  (tests: 2000)             │    │
│   │                                                                         │    │
│   │  Restart required (Postmaster):                                         │    │
│   │  pg_kafka.shadow_sasl_mechanism = 'SCRAM-SHA-256'      (default PLAIN) │    │
│   │  pg_kafka.shadow_sasl_username = 'user'     (superuser-only, hidden)   │    │
│   │  pg_kafka.shadow_sasl_password = 'pass'     (superuser-only, hidden)   │    │
│   │  pg_kafka.shadow_ssl_ca_location = '/path/to/ca.pem'                   │    │
│   │  pg_kafka.shadow_license_key = 'eval'       (superuser-only, hidden)   │    │
│   │                                                                         │    │
│   │  Superuser-settable (Suset):                                            │    │
│   │  pg_kafka.shadow_batch_size = 1000                                     │    │
│   │  pg_kafka.shadow_linger_ms = 10                                        │    │
│   │  pg_kafka.shadow_retry_backoff_ms = 100                                │    │
│   │  pg_kafka.shadow_max_retries = 3                                       │    │
│   │  pg_kafka.shadow_default_sync_mode = 'async'                           │    │
│   │  pg_kafka.shadow_metrics_enabled = true                                │    │
│   │  pg_kafka.shadow_external_primary_strict = false                       │    │
│   │  pg_kafka.shadow_otel_endpoint = ''                                    │    │
│   └────────────────────────────────────────────────────────────────────────┘    │
│                                        │                                         │
│                                        ▼                                         │
│   ┌────────────────────────────────────────────────────────────────────────┐    │
│   │                        kafka.shadow_config TABLE                        │    │
│   │                 (per-topic, PRIMARY KEY topic_id → kafka.topics)        │    │
│   │                                                                         │    │
│   │  topic_id | mode       | forward_percentage | external_topic_name |    │    │
│   │           |            |                    | sync_mode | write_mode  │    │
│   │  ─────────┼────────────┼────────────────────┼─────────────────────    │    │
│   │  1        | shadow     | 100                | prod-orders         |    │    │
│   │           |            |                    | sync      | dual_write  │    │
│   │  2        | shadow     | 50                 | NULL                |    │    │
│   │           |            |                    | async     | dual_write  │    │
│   │  3        | shadow     | 100 (forced)       | events-external     |    │    │
│   │           |            |                    | sync      | external_only│   │
│   │  4        | local_only | 0                  | NULL                |    │    │
│   │           |            |                    | sync      | dual_write  │    │
│   └────────────────────────────────────────────────────────────────────────┘    │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          CONFIG RELOAD CYCLE                                     │
│        (on SIGHUP, or every config_reload_interval_ms; checked each loop)        │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│   ┌──────────────────────────────────────────────────────────────────────────┐  │
│   │  Worker main loop (worker.rs)                                            │  │
│   │                                                                          │  │
│   │  loop {                                                                  │  │
│   │      // 1. SIGHUP or interval elapsed → config reload                   │  │
│   │      if sighup_pending || last_config_reload.elapsed() >= interval {    │  │
│   │          if sighup_pending { ProcessConfigFile(PGC_SIGHUP); }           │  │
│   │          runtime_context.reload();          // GUC snapshot             │  │
│   │          // in a guarded transaction:                                   │  │
│   │          shadow_store.load_topic_config_from_db();  // replace cache    │  │
│   │          shadow_store.refresh_primary_status();     // failover pickup  │  │
│   │          shadow_store.flush_metrics_to_db();  // logs + resets RAM ctrs │  │
│   │      }                                                                  │  │
│   │      // 2. Outbox poll, every 250 ms while shadow_mode_enabled          │  │
│   │      drain_forward_acks(); poll_and_forward_outbox();                   │  │
│   │      // 3. Liveness lane, then the next request                         │  │
│   │      match request_rx.recv_timeout(100ms) {                             │  │
│   │          Ok(request) => execute_request_in_transaction(request, ...),   │  │
│   │          Err(Timeout) => {}                                             │  │
│   │      }                                                                  │  │
│   │      // 4. periodic maintenance (member timeouts, txn timeouts, ...)    │  │
│   │  }                                                                      │  │
│   └──────────────────────────────────────────────────────────────────────────┘  │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

Notes on what the reload cycle does and does not do:

- `load_topic_config_from_db()` **replaces** the cache (so a deleted `shadow_config` row stops
  forwarding), but only when the query succeeds; a failed query keeps the previous cache.
- When a row has `write_mode = 'external_only'` and `forward_percentage <> 100`, the loader logs a
  warning and forces 100% in the cache, because a sampled-out record would otherwise be readable
  nowhere.
- `flush_metrics_to_db()` only logs and resets the in-RAM counters. The persistent per-partition
  counters in `kafka.shadow_metrics` are written by `apply_ack()` in the same statement that
  finalizes the outbox row.
- `ShadowStore::is_enabled()` reads `pg_kafka.shadow_mode_enabled` directly on every produce, while
  the network-thread forwarder reads the `RuntimeContext` snapshot refreshed by `runtime_context.reload()`.
- The license key is read once at worker start (`set_license_key`), since its GUC is Postmaster.

## Shadow Producer Initialization

The producer that actually forwards lives in the network thread's `run_shadow_forwarder()` task. It
is created lazily on the first `ForwardRequest` and recreated whenever the reloaded
`shadow_bootstrap_servers` value differs from the one it was built with.

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                       LAZY PRODUCER INITIALIZATION                               │
│                  run_shadow_forwarder() (listener.rs, network thread)            │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│   ┌───────────────────────────┐                                                 │
│   │ ForwardRequest received   │                                                 │
│   └─────────────┬─────────────┘                                                 │
│                 ▼                                                                │
│   ┌──────────────────────────────────────────────┐                              │
│   │ config = runtime_context.config()            │                              │
│   │ shadow_mode_enabled && bootstrap non-empty?  │── NO ──▶ drop request         │
│   └─────────────┬────────────────────────────────┘          (no ack; the row    │
│                 │ YES                                         stays pending and  │
│                 │                                             is re-claimed once │
│                 │                                             its in-flight entry│
│                 ▼                                             is evicted)        │
│   ┌──────────────────────────────────────────────┐                              │
│   │ bootstrap_servers changed since last build?  │                              │
│   └──────┬───────────────────────────┬───────────┘                              │
│          │ YES                       │ NO                                       │
│          ▼                           ▼                                          │
│   ┌─────────────────────────┐   ┌──────────────────────┐                        │
│   │ ShadowProducer::new()   │   │ reuse cached producer│                        │
│   │ rdkafka FutureProducer: │   └──────────────────────┘                        │
│   │ - enable.idempotence    │                                                   │
│   │ - acks=all              │                                                   │
│   │ - max.in.flight=5       │                                                   │
│   │ - retries=max_retries   │                                                   │
│   │   (3 if < 1)            │                                                   │
│   │ - compression.type=none │                                                   │
│   │ - batch.size, linger.ms,│                                                   │
│   │   retry.backoff.ms      │                                                   │
│   │ - SASL/SSL config       │                                                   │
│   │ - request.timeout=30s   │                                                   │
│   │ - message.timeout=120s  │                                                   │
│   └──────────┬──────────────┘                                                   │
│              │ Err → warn, clear producer, drop request (row stays pending)     │
│              ▼                                                                   │
│   ┌──────────────────────────────────────────────┐                              │
│   │ send_async_offset(external_topic, None, ...) │                              │
│   └──────────────────────────────────────────────┘                              │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

The producer settings are assembled by the pure `build_producer_settings()` in
`src/kafka/shadow/producer.rs` (unit-tested without a broker). Because the producer is only rebuilt
when `bootstrap_servers` changes, edits to other producer GUCs reach an existing producer only after
a bootstrap change or a restart.

`ShadowStore` still contains an older, DB-thread producer cache (`ensure_producer()`, with config
validation and a 30 s health-check TTL). In the current code it is reachable only from
`ShadowStore::flush()`, which nothing calls, so it is not on the forwarding path. Removing it is the
"PR4" step tracked in [SHADOW_EVIDENCE_PLAN.md](SHADOW_EVIDENCE_PLAN.md).

## Message Flow: Produce Path

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                            PRODUCE REQUEST HANDLING                              │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│   ┌────────────┐                                                                │
│   │ Kafka      │                                                                │
│   │ Client     │──────TCP:9092──────▶┌─────────────────────────────────────┐    │
│   │ (rdkafka)  │                     │ TCP Listener (listener.rs)          │    │
│   └────────────┘                     │ Parse ProduceRequest                │    │
│                                      └──────────────┬──────────────────────┘    │
│                                                     ▼                           │
│                                      ┌─────────────────────────────────────┐    │
│                                      │ Request via crossbeam               │    │
│                                      │ (async → sync bridge)               │    │
│                                      └──────────────┬──────────────────────┘    │
│                                                     ▼                           │
│   ┌─────────────────────────────────────────────────────────────────────────┐   │
│   │   Background Worker: execute_request_in_transaction() (worker.rs)        │   │
│   │   handle_produce() → ShadowStore::insert_records()                       │   │
│   │                                                                          │   │
│   │   Step 1: Guards                                                         │   │
│   │   ┌─────────────────┐  ┌────────────────────────────┐                    │   │
│   │   │ is_enabled()?   │  │ is_primary()?              │                    │   │
│   │   │ (reads GUC)     │  │ (cached NOT                │                    │   │
│   │   │                 │  │  pg_is_in_recovery(),      │                    │   │
│   │   │                 │  │  refreshed on each reload) │                    │   │
│   │   └────────┬────────┘  └─────────────┬──────────────┘                    │   │
│   │            └──── either false ───────┴──▶ inner.insert_records() only    │   │
│   │                                                                          │   │
│   │   Step 2: Local write (always)                                           │   │
│   │   base_offset = inner.insert_records()   → kafka.messages                │   │
│   │                                                                          │   │
│   │   Step 3: topic_cache.get(topic_id); if should_forward()                 │   │
│   │           (mode = 'shadow' AND forward_percentage > 0):                  │   │
│   │   ┌──────────────────────────────────────────────────────────────────┐  │   │
│   │   │ enqueue_outbox()                                                  │  │   │
│   │   │  for each record: make_forward_decision(key, offset, pct)         │  │   │
│   │   │    Skip    → metrics.skipped++ (in-RAM)                           │  │   │
│   │   │    Forward → selected                                             │  │   │
│   │   │  INSERT INTO kafka.shadow_tracking (topic_id, partition_id,       │  │   │
│   │   │         local_offset) ... ON CONFLICT DO NOTHING   (same txn)     │  │   │
│   │   │  if sync_mode = sync → forward_sync_bounded()                     │  │   │
│   │   └──────────────────────────────────────────────────────────────────┘  │   │
│   │                                                                          │   │
│   │   Step 4: Commit, then the buffered produce response is sent             │   │
│   │           (response-after-commit barrier)                                │   │
│   └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

`dual_write` and `external_only` follow this path identically. There is no "forward first, fall
back to local" branch any more, and no synthetic offsets: every record is stored locally with a real
offset.

### Read Path (external_only)

`external_only` marks a topic as **external-primary**. Its writes are forwarded as above, but local
reads are suppressed so consumers cut over to the external broker: `fetch_records` /
`fetch_records_with_isolation` return no records, and `get_high_watermark`,
`get_earliest_offset` and `get_last_stable_offset` return 0 (`get_offset_for_timestamp` returns
none). Suppression depends only on the global GUC plus the per-topic config, so a standby suppresses
these reads too.

If any forward for the topic is dead-lettered (`external_offset IS NULL` and
`retry_count >= 10`), suppression is lifted and the topic is served locally again, so a record is
never readable nowhere. Setting `pg_kafka.shadow_external_primary_strict = true` keeps full
suppression and accepts the dead-lettered record as lost.

### Transactional Produce

`insert_transactional_records()` writes locally (real offsets) and, if the topic forwards, buffers
`(topic_id, partition_id, offset, key)` in RAM keyed by `(producer_id, producer_epoch)`. On
`commit_transaction()` the inner commit runs first, then the buffered records are re-checked against
the current topic config, percentage-routed, and inserted into `kafka.shadow_tracking` in the same
commit transaction (only where the backing `kafka.messages` row exists). On `abort_transaction()` the
buffer is discarded, so aborted records are never forwarded. Transactional records are always
forwarded by the poll, regardless of `sync_mode`. Buffers are also evicted by the transaction-timeout
sweeper and when a producer re-initializes with a new epoch.

## Sync vs Async Forwarding

`sync_mode` only changes **when** a record is first sent, never whether it is durable. Both modes
write the same outbox row and both are finalized by the same `apply_ack()`.

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          FORWARDING DECISION TREE                                │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│   enqueue_outbox(records)   (outbox rows already written, same txn)             │
│                     │                                                            │
│          ┌──────────┴───────────────────────────────────────┐                   │
│          ▼                                                  ▼                   │
│   ┌──────────────────────────────┐      ┌──────────────────────────────────┐    │
│   │       SYNC MODE              │      │         ASYNC MODE               │    │
│   │  forward_sync_bounded()      │      │                                  │    │
│   │                              │      │  return immediately; the outbox  │    │
│   │  for each selected record:   │      │  poll (≤ 250 ms later, after     │    │
│   │   inflight.try_begin(row)    │      │  commit) claims and dispatches   │    │
│   │   forward_tx.try_send(req)   │      │  the row                         │    │
│   │   (full → leave for poll)    │      │                                  │    │
│   │                              │      └──────────────────────────────────┘    │
│   │  wait ≤ SYNC_FORWARD_WAIT_MS │                                              │
│   │  (5s) on the ack channel,    │                                              │
│   │  apply_ack() for EVERY ack   │                                              │
│   │  received (ours or not)      │                                              │
│   │                              │                                              │
│   │  timeout → return anyway;    │                                              │
│   │  the row stays pending and   │                                              │
│   │  the poll retries it         │                                              │
│   └──────────────────────────────┘                                              │
│                                                                                  │
│   ┌──────────────────────────────────────────────────────────────────────────┐  │
│   │ OUTBOX POLL: poll_and_forward_outbox()                                    │  │
│   │                                                                           │  │
│   │  claim (one UPDATE ... RETURNING, LIMIT 256) rows where:                  │  │
│   │    external_offset IS NULL                                                │  │
│   │    AND retry_count < 10                         (MAX_FORWARD_RETRIES)     │  │
│   │    AND (forwarded_at IS NULL OR forwarded_at < NOW() - 5000 ms)           │  │
│   │                                                 (OUTBOX_RETRY_INTERVAL_MS)│  │
│   │    AND row not in the in-flight set             (issue #93)               │  │
│   │    AND the kafka.messages row still exists                                │  │
│   │  claim sets forwarded_at = NOW(), retry_count = retry_count + 1           │  │
│   │  payload: JOIN kafka.messages; topic: COALESCE(external_topic_name, name) │  │
│   │  per row: inflight.try_begin → forward_tx.try_send                        │  │
│   │           (full → inflight.complete, retried after the lease)             │  │
│   └──────────────────────────────────────────────────────────────────────────┘  │
│                                                                                  │
│   ┌──────────────────────────────────────────────────────────────────────────┐  │
│   │ apply_ack(ack)                                                            │  │
│   │  inflight.complete(row)                                                   │  │
│   │  Ok(ext_offset): UPDATE ... SET external_offset, forwarded_at = NOW(),    │  │
│   │                  error_message = NULL WHERE external_offset IS NULL       │  │
│   │                  + kafka.shadow_metrics.messages_forwarded++ (same stmt)  │  │
│   │  Err(msg):       UPDATE ... SET error_message = msg                       │  │
│   │                  + kafka.shadow_metrics.messages_failed++                 │  │
│   │                  (retry_count >= 10 → logged as dead-lettered)            │  │
│   │  A duplicate ack matches no pending row and counts nothing.               │  │
│   └──────────────────────────────────────────────────────────────────────────┘  │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

Delivery guarantees and their limits:

- **At-least-once.** A crash, a dropped request or a dropped ack leaves the row pending. A row that
  never reached the forward channel (channel full) is re-claimed after the 5 s lease. A row the
  network thread dropped (shadow disabled or no producer on that side) or whose ack was dropped (ack
  channel full) stays in the in-flight set, so it is re-claimed only after that entry is evicted
  (150 s, see below). After a worker restart the in-flight set is empty and the lease alone applies. The idempotent producer suppresses a re-send only within one producer session, so a re-send
  that spans a producer restart lands on the external topic as a duplicate.
- **In-flight gate (issue #93).** `InflightTracker` (`inflight.rs`) records every dispatched row until
  its ack is applied, and the poll excludes those rows from the claim, so a forward that is merely
  slower than the 5 s lease is no longer re-sent. Entries are evicted after 150 s
  (`INFLIGHT_EVICT_AFTER`, above rdkafka's 120 s `message.timeout.ms`), after which a genuinely lost
  ack falls back to lease-based retry. The tracker is in-memory and is forgotten on worker restart.
- **Dead-letter.** Because the claim bumps `retry_count`, every row gets at most 10 attempts. A row
  at the cap stays in `kafka.shadow_tracking` (pending, with `error_message`) and is not re-sent until
  an operator re-queues it with `kafka.replay_shadow_messages`.
- **Partitioning.** Forwards are sent with `partition = None`, so the external cluster re-partitions
  by key. Local and external partition numbers are independent.
- **Percentage routing.** `make_forward_decision()` (`routing.rs`): 0% skips, 100% forwards, otherwise
  `murmur2(key, seed 0x9747b28c) % 100 < pct`. Records with no key or an empty key use
  `local_offset % 100` instead.

### Replay

`kafka.replay_shadow_messages(p_topic_id INT, p_from_offset BIGINT DEFAULT 0, p_to_offset BIGINT DEFAULT NULL)`
inserts (or resets to pending) a `kafka.shadow_tracking` row for every committed or non-transactional
message (`txn_state IS NULL`) of that topic with `p_from_offset <= partition_offset < p_to_offset`
(`NULL` = to the end), across all partitions. It returns the number of rows re-queued; the normal
outbox poll then forwards them.

```sql
SELECT kafka.replay_shadow_messages(
    (SELECT id FROM kafka.topics WHERE name = 'orders'), 0, NULL);
```

### Observability

- `kafka.shadow_tracking`: one row per selected record. `external_offset IS NOT NULL` means
  delivered. `error_message` is withheld from `PUBLIC` (column-level grant).
- `kafka.shadow_metrics`: per-partition `messages_forwarded`, `messages_failed`,
  `last_forwarded_offset`, `last_forwarded_at`. The `messages_skipped` column exists but is never
  incremented by the code; skips are only counted in RAM and logged.
- `kafka.shadow_status` view: per-topic config plus summed metrics. Its `lag` column is
  `MAX(partition_offset) - MAX(last_forwarded_offset)` across partitions, which is approximate for
  multi-partition or sampled topics.
- `kafka.metrics` view, category `shadow`: `forwarded`, `failed`, and `lag` (the count of pending
  outbox rows).

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

```
┌──────────────────────────────────────────────────────────────────────────────┐
│ DESIGN: Bounded channels never lose a record; they defer it                  │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────────────────┐      ┌───────────────────────────────────────┐ │
│  │ DB thread               │─────▶│ forward channel bounded(10_000)       │ │
│  │ ForwardRequest          │      │  try_send() full → row stays pending, │ │
│  └─────────────────────────┘      │  retried after the 5s lease           │ │
│                                   └───────────────────────────────────────┘ │
│  ┌─────────────────────────┐      ┌───────────────────────────────────────┐ │
│  │ Network thread          │─────▶│ ack channel bounded(10_000)           │ │
│  │ ForwardAck              │      │  try_send() full → ack dropped, row   │ │
│  └─────────────────────────┘      │  re-claimed after the in-flight entry │ │
│                                   │  is evicted (150s); may duplicate     │ │
│                                   └───────────────────────────────────────┘ │
│                                                                              │
│  Test Impact: LOW                                                            │
│  - Tests produce 1-500 messages; the poll claims ≤ 256 rows per 250 ms tick │
│  - Channel capacity: 10,000                                                  │
│                                                                              │
│  Production Risk: LOW for loss (the outbox is durable); a sustained burst   │
│  raises forwarding latency and each deferral consumes one of the 10 retry   │
│  attempts                                                                    │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

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
