# Shadow Mode Flow Diagram

This document describes the shadow mode lifecycle in pg_kafka, including configuration, client creation, message flow, and potential issues.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           PostgreSQL PostMaster                                  │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                    Background Worker Thread                              │    │
│  │                                                                          │    │
│  │  ┌──────────────┐    ┌──────────────┐    ┌──────────────────────────┐   │    │
│  │  │ RuntimeConfig│───▶│ ShadowStore  │───▶│ TopicConfigCache        │   │    │
│  │  │ (GUCs)       │    │ (wrapper)    │    │ (per-topic config)      │   │    │
│  │  └──────────────┘    └──────┬───────┘    └──────────────────────────┘   │    │
│  │                              │                                           │    │
│  │                              ▼                                           │    │
│  │  ┌────────────────────────────────────────────────────────────────────┐ │    │
│  │  │                     insert_records()                                │ │    │
│  │  │  ┌─────────────────┐  ┌─────────────────┐  ┌───────────────────┐   │ │    │
│  │  │  │ is_enabled()?   │─▶│ is_primary()?   │─▶│ get_topic_config()│   │ │    │
│  │  │  └─────────────────┘  └─────────────────┘  └─────────┬─────────┘   │ │    │
│  │  │                                                       │             │ │    │
│  │  │                              ┌────────────────────────┼────────────┐│ │    │
│  │  │                              ▼                        ▼            ││ │    │
│  │  │                    ┌─────────────────┐      ┌──────────────────┐  ││ │    │
│  │  │                    │ DUAL_WRITE mode │      │ EXTERNAL_ONLY    │  ││ │    │
│  │  │                    │                 │      │                  │  ││ │    │
│  │  │                    │ 1. Write local  │      │ 1. Forward first │  ││ │    │
│  │  │                    │ 2. Forward async│      │ 2. Fallback local│  ││ │    │
│  │  │                    └────────┬────────┘      └────────┬─────────┘  ││ │    │
│  │  │                             │                        │            ││ │    │
│  │  └─────────────────────────────┼────────────────────────┼────────────┘│ │    │
│  │                                │                        │             │ │    │
│  │                                ▼                        ▼             │ │    │
│  │  ┌────────────────────────────────────────────────────────────────┐   │ │    │
│  │  │                   forward_records_best_effort()                 │   │ │    │
│  │  │                                                                 │   │ │    │
│  │  │    ┌──────────────────┐              ┌──────────────────────┐  │   │ │    │
│  │  │    │   SYNC MODE      │              │    ASYNC MODE        │  │   │ │    │
│  │  │    │                  │              │                      │  │   │ │    │
│  │  │    │ producer.send()  │              │ forward_tx.try_send()│  │   │ │    │
│  │  │    │   [BLOCKS]       │              │   [NON-BLOCKING]     │  │   │ │    │
│  │  │    └────────┬─────────┘              └──────────┬───────────┘  │   │ │    │
│  │  │             │                                   │              │   │ │    │
│  │  │             │                                   │              │   │ │    │
│  │  │             │                                   ▼              │   │ │    │
│  │  │             │                        ┌─────────────────────┐   │   │ │    │
│  │  │             │                        │ crossbeam-channel   │   │   │ │    │
│  │  │             │                        │ (bounded: 10,000)   │   │   │ │    │
│  │  │             │                        └──────────┬──────────┘   │   │ │    │
│  │  └─────────────┼───────────────────────────────────┼──────────────┘   │ │    │
│  │                │                                   │                  │ │    │
│  └────────────────┼───────────────────────────────────┼──────────────────┘ │    │
│                   │                                   │                    │    │
│                   │         ┌─────────────────────────┘                    │    │
│                   │         │                                              │    │
│  ┌────────────────┼─────────┼──────────────────────────────────────────┐   │    │
│  │                │         ▼               Network Thread              │   │    │
│  │                │  ┌──────────────────────────────────────────────┐  │   │    │
│  │                │  │           Tokio Runtime (2 workers)          │  │   │    │
│  │                │  │                                              │  │   │    │
│  │                │  │   ┌─────────────────────────────────────┐    │  │   │    │
│  │                │  │   │     run_shadow_forwarder() task     │    │  │   │    │
│  │                │  │   │                                     │    │  │   │    │
│  │                │  │   │  1. spawn_blocking(recv from rx)    │    │  │   │    │
│  │                │  │   │  2. producer.send_async()           │    │  │   │    │
│  │                │  │   │  3. Log errors (fire-and-forget)    │    │  │   │    │
│  │                │  │   └─────────────────────────────────────┘    │  │   │    │
│  │                │  │                                              │  │   │    │
│  │                │  │   ┌─────────────────────────────────────┐    │  │   │    │
│  │                │  │   │      TCP Listener (port 9092)       │    │  │   │    │
│  │                │  │   │      Kafka client connections       │    │  │   │    │
│  │                │  │   └─────────────────────────────────────┘    │  │   │    │
│  │                │  └──────────────────────────────────────────────┘  │   │    │
│  │                │                                                    │   │    │
│  │                └───────────────────────────────────────────────────▶│   │    │
│  │                              SYNC: Direct send                      │   │    │
│  └─────────────────────────────────────────────────────────────────────┘   │    │
│                                                                             │    │
└─────────────────────────────────────────────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          External Kafka Cluster                                  │
│                            (localhost:9093)                                      │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## Configuration Lifecycle

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          CONFIGURATION SOURCES                                   │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│   ┌────────────────────────────────────────────────────────────────────────┐    │
│   │                          postgresql.conf / GUCs                         │    │
│   │                                                                         │    │
│   │  pg_kafka.shadow_mode_enabled = true                                   │    │
│   │  pg_kafka.shadow_bootstrap_servers = 'kafka1:9092,kafka2:9092'         │    │
│   │  pg_kafka.shadow_security_protocol = 'SASL_SSL' | 'PLAINTEXT'          │    │
│   │  pg_kafka.shadow_sasl_mechanism = 'SCRAM-SHA-256'                      │    │
│   │  pg_kafka.shadow_sasl_username = 'user'                                │    │
│   │  pg_kafka.shadow_sasl_password = 'pass'                                │    │
│   │  pg_kafka.shadow_ssl_ca_location = '/path/to/ca.pem'                   │    │
│   │  pg_kafka.shadow_batch_size = 1000                                     │    │
│   │  pg_kafka.shadow_linger_ms = 10                                        │    │
│   │  pg_kafka.shadow_default_sync_mode = 'async'                           │    │
│   │  pg_kafka.config_reload_interval_ms = 30000  (tests: 2000)             │    │
│   └────────────────────────────────────────────────────────────────────────┘    │
│                                        │                                         │
│                                        ▼                                         │
│   ┌────────────────────────────────────────────────────────────────────────┐    │
│   │                        kafka.shadow_config TABLE                        │    │
│   │                         (per-topic overrides)                           │    │
│   │                                                                         │    │
│   │  topic_id | mode        | forward_pct | external_topic | sync_mode    │    │
│   │  ─────────┼─────────────┼─────────────┼────────────────┼──────────    │    │
│   │  1        | DualWrite   | 100         | prod-orders    | sync         │    │
│   │  2        | DualWrite   | 50          | NULL           | async        │    │
│   │  3        | ExternalOnly| 100         | events-external| sync         │    │
│   └────────────────────────────────────────────────────────────────────────┘    │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          CONFIG RELOAD CYCLE                                     │
│                  (every config_reload_interval_ms)                               │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│   ┌──────────────────────────────────────────────────────────────────────────┐  │
│   │  Worker Loop (worker.rs:568-601)                                         │  │
│   │                                                                          │  │
│   │  loop {                                                                  │  │
│   │      // 1. Process Kafka requests from channel                          │  │
│   │      match request_rx.recv_timeout(Duration::from_millis(reload_ms)) {  │  │
│   │          Ok(request) => handle_request(request),                        │  │
│   │          Err(Timeout) => {                                              │  │
│   │              // 2. Reload GUCs                                          │  │
│   │              RuntimeContext::reload();  ───────────────────────────▶    │  │
│   │                                                                          │  │
│   │              // 3. Reload topic config from DB                           │  │
│   │              shadow_store.load_topic_config_from_db();  ────────────▶   │  │
│   │                                                                          │  │
│   │              // 4. Flush metrics to DB                                   │  │
│   │              shadow_store.flush_metrics_to_db();                        │  │
│   │          }                                                              │  │
│   │      }                                                                  │  │
│   │  }                                                                      │  │
│   └──────────────────────────────────────────────────────────────────────────┘  │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## Shadow Producer Initialization

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                       LAZY PRODUCER INITIALIZATION                               │
│                         ShadowStore::ensure_producer()                           │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│   ┌───────────────────┐                                                         │
│   │ insert_records()  │                                                         │
│   │ called            │                                                         │
│   └─────────┬─────────┘                                                         │
│             │                                                                    │
│             ▼                                                                    │
│   ┌─────────────────────┐     ┌─────────────────────────────────────────────┐   │
│   │ Read Lock Check     │────▶│ producer: Option<Arc<ShadowProducer>>       │   │
│   │ (fast path)         │     │ is Some?                                    │   │
│   └─────────────────────┘     └──────────────────┬──────────────────────────┘   │
│                                                   │                              │
│                          ┌────────────────────────┼───────────────────────┐      │
│                          │ YES                    │ NO                    │      │
│                          ▼                        ▼                       │      │
│           ┌──────────────────────┐    ┌──────────────────────────────┐    │      │
│           │ Return cached        │    │ Acquire Write Lock           │    │      │
│           │ Arc<ShadowProducer>  │    │ (slow path)                  │    │      │
│           └──────────────────────┘    └──────────────┬───────────────┘    │      │
│                                                      │                    │      │
│                                                      ▼                    │      │
│                                       ┌──────────────────────────────┐    │      │
│                                       │ Double-check: still None?    │    │      │
│                                       └──────────────┬───────────────┘    │      │
│                                                      │                    │      │
│                                          ┌───────────┴───────────┐        │      │
│                                          │ YES                   │ NO     │      │
│                                          ▼                       ▼        │      │
│                               ┌─────────────────────┐    ┌───────────┐    │      │
│                               │ Validate Config:    │    │ Return    │    │      │
│                               │ - shadow_enabled?   │    │ existing  │    │      │
│                               │ - bootstrap_servers │    └───────────┘    │      │
│                               │   not empty?        │                     │      │
│                               └─────────┬───────────┘                     │      │
│                                         │                                 │      │
│                                         ▼                                 │      │
│                               ┌─────────────────────┐                     │      │
│                               │ Create rdkafka      │                     │      │
│                               │ FutureProducer:     │                     │      │
│                               │                     │                     │      │
│                               │ - idempotence=true  │                     │      │
│                               │ - acks=all          │                     │      │
│                               │ - compression=zstd  │                     │      │
│                               │ - SASL/SSL config   │                     │      │
│                               │ - request.timeout=  │                     │      │
│                               │   30000ms           │                     │      │
│                               └─────────┬───────────┘                     │      │
│                                         │                                 │      │
│                                         ▼                                 │      │
│                               ┌─────────────────────┐                     │      │
│                               │ Store in RwLock,    │                     │      │
│                               │ Return Arc clone    │                     │      │
│                               └─────────────────────┘                     │      │
│                                                                           │      │
└───────────────────────────────────────────────────────────────────────────┘      │
                                                                                   │
└──────────────────────────────────────────────────────────────────────────────────┘
```

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
│                                                     │                           │
│                                                     ▼                           │
│                                      ┌─────────────────────────────────────┐    │
│                                      │ RequestMessage via crossbeam        │    │
│                                      │ (async → sync bridge)               │    │
│                                      └──────────────┬──────────────────────┘    │
│                                                     │                           │
│                                                     ▼                           │
│   ┌─────────────────────────────────────────────────────────────────────────┐   │
│   │                        Background Worker (worker.rs)                     │   │
│   │                                                                          │   │
│   │   ┌───────────────────────────────────────────────────────────────────┐ │   │
│   │   │              handle_produce() → ShadowStore::insert_records()      │ │   │
│   │   └───────────────────────────────────────────────────────────────────┘ │   │
│   │                                       │                                  │   │
│   │                                       ▼                                  │   │
│   │   ┌───────────────────────────────────────────────────────────────────┐ │   │
│   │   │  Step 1: Check Guards                                             │ │   │
│   │   │  ┌─────────────────┐  ┌────────────────────┐                      │ │   │
│   │   │  │ is_enabled()?   │  │ is_primary()?      │                      │ │   │
│   │   │  │ (reads GUC)     │  │ (cached SPI call)  │                      │ │   │
│   │   │  └────────┬────────┘  └─────────┬──────────┘                      │ │   │
│   │   │           │ false               │ false                           │ │   │
│   │   │           └─────────┬───────────┘                                 │ │   │
│   │   │                     ▼                                             │ │   │
│   │   │           ┌─────────────────────┐                                 │ │   │
│   │   │           │ Skip shadow, write  │                                 │ │   │
│   │   │           │ to local DB only    │                                 │ │   │
│   │   │           └─────────────────────┘                                 │ │   │
│   │   │                                                                   │ │   │
│   │   │  (if both true) ────────────────────────────────▶                 │ │   │
│   │   │                                                                   │ │   │
│   │   │  Step 2: Get Topic Config                                         │ │   │
│   │   │  ┌──────────────────────────────────────────────────────────────┐│ │   │
│   │   │  │ TopicConfigCache::get(topic_name)                            ││ │   │
│   │   │  │ Returns: mode, forward_percentage, external_topic, sync_mode ││ │   │
│   │   │  └──────────────────────────────────────────────────────────────┘│ │   │
│   │   │                             │                                     │ │   │
│   │   │        ┌────────────────────┴──────────────────────┐              │ │   │
│   │   │        ▼                                           ▼              │ │   │
│   │   │  ┌─────────────────┐                    ┌────────────────────┐    │ │   │
│   │   │  │  DUAL_WRITE     │                    │  EXTERNAL_ONLY     │    │ │   │
│   │   │  └────────┬────────┘                    └─────────┬──────────┘    │ │   │
│   │   │           │                                       │               │ │   │
│   │   │           ▼                                       ▼               │ │   │
│   │   │  ┌─────────────────────────┐          ┌───────────────────────┐   │ │   │
│   │   │  │ 1. Write to local DB    │          │ 1. forward_required() │   │ │   │
│   │   │  │    inner.insert_records │          │    (sync, must work)  │   │ │   │
│   │   │  └─────────┬───────────────┘          └───────────┬───────────┘   │ │   │
│   │   │            │                                      │               │ │   │
│   │   │            ▼                               ┌──────┴──────┐        │ │   │
│   │   │  ┌─────────────────────────┐               ▼             ▼        │ │   │
│   │   │  │ 2. forward_best_effort()│          ┌────────┐   ┌──────────┐   │ │   │
│   │   │  │    (async, fire-forget) │          │ Success│   │ Failed   │   │ │   │
│   │   │  └─────────────────────────┘          │ return │   │ fallback │   │ │   │
│   │   │                                       │   0    │   │ to local │   │ │   │
│   │   │                                       └────────┘   └──────────┘   │ │   │
│   │   └───────────────────────────────────────────────────────────────────┘ │   │
│   └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## Sync vs Async Forwarding

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          FORWARDING DECISION TREE                                │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│   forward_records_best_effort(records)                                          │
│                     │                                                            │
│                     ▼                                                            │
│   ┌─────────────────────────────────────────────────────────────────────────┐   │
│   │ For each record:                                                         │   │
│   │   1. Check percentage routing (murmur2 hash of key % 100 < pct)         │   │
│   │   2. If not sampled: metrics.skipped++, continue                        │   │
│   │   3. Resolve external_topic (or use local topic name)                   │   │
│   └─────────────────────────────────────────────────────────────────────────┘   │
│                     │                                                            │
│                     ▼                                                            │
│   ┌─────────────────────────────────────────────────────────────────────────┐   │
│   │ Check sync_mode from topic config                                        │   │
│   └──────────────────────────────┬──────────────────────────────────────────┘   │
│                                  │                                               │
│          ┌───────────────────────┴───────────────────────┐                      │
│          ▼                                               ▼                      │
│   ┌──────────────────────────────┐      ┌──────────────────────────────────┐    │
│   │       SYNC MODE              │      │         ASYNC MODE               │    │
│   │                              │      │                                  │    │
│   │  ┌────────────────────────┐  │      │  ┌────────────────────────────┐  │    │
│   │  │ producer.send_sync()   │  │      │  │ forward_tx.try_send(req)   │  │    │
│   │  │ (blocks up to 30s)     │  │      │  │ (non-blocking)             │  │    │
│   │  └────────────────────────┘  │      │  └────────────────────────────┘  │    │
│   │              │               │      │              │                   │    │
│   │              ▼               │      │         ┌────┴────┐              │    │
│   │  ┌────────────────────────┐  │      │         ▼         ▼              │    │
│   │  │ Wait for ack from      │  │      │    ┌────────┐ ┌────────┐         │    │
│   │  │ external Kafka         │  │      │    │ Ok     │ │ Full   │         │    │
│   │  └────────────────────────┘  │      │    │ queued │ │ dropped│         │    │
│   │              │               │      │    └────┬───┘ └────┬───┘         │    │
│   │         ┌────┴────┐          │      │         │          │             │    │
│   │         ▼         ▼          │      │         ▼          ▼             │    │
│   │    ┌────────┐ ┌────────┐     │      │  metrics++    warn! +            │    │
│   │    │ Success│ │ Failed │     │      │               metrics.failed++   │    │
│   │    └────┬───┘ └────┬───┘     │      │                                  │    │
│   │         │          │         │      └──────────────────────────────────┘    │
│   │         ▼          ▼         │                     │                        │
│   │  forwarded++   failed++      │                     ▼                        │
│   │                              │      ┌──────────────────────────────────┐    │
│   └──────────────────────────────┘      │     NETWORK THREAD               │    │
│                                         │  run_shadow_forwarder() task     │    │
│                                         │                                  │    │
│                                         │  loop {                          │    │
│                                         │    // Blocking recv in spawn_    │    │
│                                         │    // blocking to not block      │    │
│                                         │    // tokio runtime              │    │
│                                         │    req = forward_rx.recv();      │    │
│                                         │    producer.send_async(req);     │    │
│                                         │    // Fire-and-forget            │    │
│                                         │    // Errors logged, not retried │    │
│                                         │  }                               │    │
│                                         └──────────────────────────────────┘    │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## E2E Test Setup Sequence

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         E2E TEST SETUP SEQUENCE                                  │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│   ┌─────────────────────────────────────────────────────────────────────────┐   │
│   │  ONE-TIME SETUP (shadow_test_setup)                                      │   │
│   │  Runs once per test suite via AtomicBool guard                          │   │
│   │                                                                          │   │
│   │  ┌─────────────────────────────────────────────────────────────────┐    │   │
│   │  │  1. ALTER SYSTEM SET pg_kafka.config_reload_interval_ms = 2000  │    │   │
│   │  │  2. SELECT pg_reload_conf()                                      │    │   │
│   │  │  3. Sleep 31 seconds  ◀── Wait for worker's first reload cycle   │    │   │
│   │  │                           (default 30s + 1s buffer)              │    │   │
│   │  │                           After this, worker uses 2s interval    │    │   │
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
│   │  │  3. ALTER SYSTEM SET pg_kafka.shadow_bootstrap_servers = ...     │    │   │
│   │  │  4. ALTER SYSTEM SET pg_kafka.shadow_security_protocol = ...     │    │   │
│   │  │  5. SELECT pg_reload_conf()                                      │    │   │
│   │  │  6. Sleep 1 second (pg_reload_conf is async)                     │    │   │
│   │  │  7. INSERT INTO kafka.shadow_config (topic config)               │    │   │
│   │  │  8. Sleep 3 seconds  ◀── Wait for worker reload cycle (2s + 1s) │    │   │
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
│   │  4. Verify in external Kafka (consumer from port 9093)                  │   │
│   └─────────────────────────────────────────────────────────────────────────┘   │
│                                        │                                         │
│                                        ▼                                         │
│   ┌─────────────────────────────────────────────────────────────────────────┐   │
│   │  CLEANUP (TestContext::cleanup)                                          │   │
│   │                                                                          │   │
│   │  - Delete topic from kafka.topics                                       │   │
│   │  - Delete messages from kafka.messages                                  │   │
│   │  - Delete shadow config from kafka.shadow_config                        │   │
│   │  - Reset GUCs (optional, next test will set them)                       │   │
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
│  Test expects:    localhost:9093 (external Kafka)                            │
│  Test expects:    localhost:9092 (pg_kafka)                                  │
│                                                                              │
│  If 9093 not running:                                                        │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ verify_external_kafka_ready() times out after 10s                      │ │
│  │ Test fails with: "Cannot connect to external Kafka"                    │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  FIX: Ensure docker-compose includes external Kafka service on port 9093    │
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
│  CURRENT WORKING APPROACH (commit 4d4ab5d):                                  │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ Use GUC: pg_kafka.config_reload_interval_ms                            │ │
│  │ Worker polls every N ms, reloads GUCs + shadow_config table            │ │
│  │ Tests set to 2000ms, wait 3s after config changes                      │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  STATUS: ✅ FIXED                                                            │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

### Issue 3: Async Channel Backpressure

```
┌──────────────────────────────────────────────────────────────────────────────┐
│ DESIGN: Bounded channel can drop messages under load                         │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────────────────┐      ┌───────────────────────────────────────┐ │
│  │ DB Worker produces      │─────▶│ crossbeam_channel::bounded(10_000)    │ │
│  │ ForwardRequest          │      │                                       │ │
│  └─────────────────────────┘      │  try_send() ───▶ If full: DROP + log  │ │
│                                   └───────────────────────────────────────┘ │
│                                                                              │
│  Test Impact: LOW                                                            │
│  - Tests produce 1-500 messages                                              │
│  - Channel capacity: 10,000                                                  │
│  - Network thread processes continuously                                     │
│  - Would need extreme burst to trigger                                       │
│                                                                              │
│  Production Risk: MEDIUM (intentional trade-off for non-blocking worker)     │
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
│  │ ALTER SYSTEM SET pg_kafka.shadow_bootstrap_servers = 'localhost:9093'  │ │
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
│  - All shadow tests marked parallel_safe: false in main.rs                   │
│  - Tests use unique topic names: ctx.unique_topic("shadow-test")             │
│                                                                              │
│  STATUS: ✅ MITIGATED                                                        │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

### Issue 5: First Test Takes 35+ Seconds

```
┌──────────────────────────────────────────────────────────────────────────────┐
│ TIMING: One-time setup adds significant delay to first test                  │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  TIMELINE:                                                                   │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ 0s:  Test starts                                                       │ │
│  │ 0s:  shadow_test_setup() runs (first time only)                        │ │
│  │ 0s:  ALTER SYSTEM SET config_reload_interval_ms = 2000                 │ │
│  │ 0s:  SELECT pg_reload_conf()                                           │ │
│  │ 0-31s: Sleep 31 seconds (wait for worker's 30s default interval)       │ │
│  │ 31s: enable_shadow_mode() runs                                         │ │
│  │ 31s: Set GUCs, insert shadow_config                                    │ │
│  │ 32-35s: Sleep 3 seconds (wait for 2s reload interval + buffer)         │ │
│  │ 35s+: Actual test execution begins                                     │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  Subsequent tests: Only 3-10 seconds (no one-time setup)                     │
│                                                                              │
│  Total for 20 shadow tests: ~90 seconds (vs 620s if each did 31s setup)      │
│                                                                              │
│  STATUS: ✅ OPTIMIZED (commit f4300a5)                                       │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

## Docker Networking Issue (FIXED)

```
┌──────────────────────────────────────────────────────────────────────────────┐
│ CRITICAL: Kafka Advertised Listener Mismatch                                 │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  CURRENT DOCKER-COMPOSE CONFIGURATION:                                       │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │  pg_kafka_dev container:                                               │ │
│  │    PG_KAFKA_SHADOW_BOOTSTRAP_SERVERS=external-kafka:9093              │ │
│  │                                                                        │ │
│  │  external-kafka container:                                             │ │
│  │    KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9093              │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  WHAT HAPPENS:                                                               │
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
│  NOTE: The docker-compose.yml even has a comment about this!                │
│  > "For container-to-container communication (e.g., pg_kafka_dev ->         │
│  >  external-kafka), you would need a dual-listener setup or host           │
│  >  network mode"                                                           │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘

FIXES (choose one):

Option A: Add dual listener to external-kafka (RECOMMENDED)
┌──────────────────────────────────────────────────────────────────────────────┐
│  KAFKA_LISTENERS: INTERNAL://0.0.0.0:9094,EXTERNAL://0.0.0.0:9093,...       │
│  KAFKA_ADVERTISED_LISTENERS:                                                 │
│    INTERNAL://external-kafka:9094,EXTERNAL://localhost:9093,...              │
│  KAFKA_LISTENER_SECURITY_PROTOCOL_MAP:                                       │
│    INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT               │
│                                                                              │
│  pg_kafka uses: external-kafka:9094 (INTERNAL)                               │
│  Host tests use: localhost:9093 (EXTERNAL)                                   │
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
| External Kafka on 9093 | Required | Docker or local Kafka |
| Docker network fix | ✅ Fixed | Dual listener setup implemented |
| PLAINTEXT protocol | Default | Tests assume no SASL |
| Sequential execution | Required | `parallel_safe: false` |
| ~90 second total runtime | Expected | First test takes 35s |
| Config reload interval | Auto-set | Tests set to 2000ms |

## Implemented Fix (docker-compose.yml)

The dual-listener configuration has been implemented:

```yaml
external-kafka:
  environment:
    # DUAL LISTENER SETUP for container + host access
    # - INTERNAL:9094 for container-to-container (pg_kafka_dev -> external-kafka)
    # - EXTERNAL:9093 for host access (E2E tests, kcat from host)
    # - CONTROLLER:29094 for KRaft controller
    KAFKA_LISTENERS: INTERNAL://0.0.0.0:9094,EXTERNAL://0.0.0.0:9093,CONTROLLER://0.0.0.0:29094
    KAFKA_ADVERTISED_LISTENERS: INTERNAL://external-kafka:9094,EXTERNAL://localhost:9093
    KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT
    KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL

pg_kafka_dev:
  environment:
    # Use INTERNAL listener for container-to-container
    - PG_KAFKA_SHADOW_BOOTSTRAP_SERVERS=external-kafka:9094
  depends_on:
    external-kafka:
      condition: service_healthy
```
