# Design Document: pg_kafka (Native Postgres Extension)

**Author:** Trent Jones
**Status:** Draft
**Date:** December 2025
**Repository:** [GitHub Link]

---

## 1. Executive Summary

pg_kafka is a PostgreSQL extension written in Rust (via pgrx) that embeds a Kafka-compatible wire protocol listener directly into the Postgres runtime. It allows standard Kafka clients to produce and consume messages using Postgres as the backing store, eliminating the need for a separate Zookeeper/Controller cluster for small-to-medium workloads. It additionally provides a "Shadow Mode" for zero-downtime migration, asynchronously replicating data from Postgres to a remote Kafka cluster via Logical Decoding.

---

## 2. Problem Statement

For early-stage startups and internal tools, introducing Kafka adds significant operational overhead (ZooKeeper/KRaft, JVM tuning, disk management). Teams often start with Postgres for queues but eventually hit scaling limits, requiring a painful "stop-the-world" migration to Kafka.

**Opportunity:** There is no smooth graduation path from "Postgres as a Queue" to "Enterprise Kafka."

---

## 3. Goals & Non-Goals

### Goals (MVP)

- **Wire Compatibility:** Support the minimal Kafka Protocol (v2+) so standard clients (Java, Python, kcat) can connect without code changes.
- **Native Integration:** Run entirely within Postgres as a BackgroundWorker. No external sidecars.
- **Shadow Replication:** Enable seamless migration by forwarding writes to an external Kafka cluster.
- **Safety:** Ensure extension crashes do not corrupt the main Postgres database.

### Non-Goals

- **Full Protocol Compliance:** We will not support Consumer Groups, Transactions, or Compression in v1.
- **High Availability:** We rely on standard Postgres HA (Patroni/RDS) rather than Kafka's ISR (In-Sync Replicas).
- **Broker Clustering:** This is a single-node "broker" design.

---

## 4. System Architecture

The extension introduces two persistent BackgroundWorker processes managed by the Postgres Postmaster.

### 4.1 Component Breakdown

**The Protocol Listener (Ingress):**

- **Role:** Binds to TCP port 9092.
- **Tech:** Rust tokio runtime running inside a Postgres BackgroundWorker.
- **Flow:** Accepts connections → Parses Request bytes → Maps to SPI (Server Programming Interface) calls → Writes to Postgres Tables.

**The Storage Engine (Tables):**

- Standard Postgres Heap Tables used to emulate Kafka Logs.
- Uses Unlogged Tables (optional) for performance or standard tables for durability.

**The Replicator (Egress/Shadow):**

- **Role:** Reads Postgres WAL (Write Ahead Log) and produces to external Kafka.
- **Tech:** Uses Postgres Logical Decoding (via pgoutput plugin).
- **Flow:** Tails WAL → Decodes INSERT → Sends ProduceRequest to Real Kafka.

---

## 5. Detailed Design

### 5.1 Storage Schema

Kafka's hierarchy (Topic → Partition → Offset) maps natively to a relational schema.

```sql
-- The "Log"
CREATE TABLE kafka.messages (
    topic_id INT NOT NULL,
    partition_id INT NOT NULL,
    "offset" BIGSERIAL, -- Auto-incrementing global order
    key BYTEA,
    value BYTEA,
    headers JSONB,
    created_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (topic_id, partition_id, "offset")
);

-- Metadata
CREATE TABLE kafka.topics (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    partitions INT DEFAULT 1
);
```

### 5.2 The Protocol Handler (Rust/pgrx)

We cannot block the Postgres main loop. We must use pgrx's background worker support to spin up a tokio runtime.

**Handling ProduceRequest:**

1. **Parse:** Deserialize binary frame (Size + Header + Body).
2. **SPI Context:** Attach to the Postgres Shared Memory.
3. **Transaction:** Start a Postgres Transaction (`SPI_connect`).
4. **Write:** Execute `INSERT INTO kafka.messages ... RETURNING offset`.
5. **Response:** Serialize ProduceResponse with the new offset and send back to client.

**Handling FetchRequest:**

1. **Query:** `SELECT * FROM kafka.messages WHERE topic_id=$1 AND partition_id=$2 AND "offset" >= $3 LIMIT $4`.
2. **Wait:** If no rows return, we must simulate Kafka's "Long Poll."
3. **Implementation:** Use Postgres `LISTEN/NOTIFY`. The Fetch handler waits on a condition variable. The Produce handler fires `NOTIFY` upon commit.

### 5.3 Shadow Replication (The "Staff" Feature)

To achieve the "Soft Cutover," we avoid dual-writing in the application.

**Mechanism:** The Replicator worker connects to Postgres internally using the Streaming Replication Protocol.

**Slot Creation:**

```sql
pg_create_logical_replication_slot('pg_kafka_shadow', 'pgoutput')
```

**Loop:**

1. Receive `XLogData` message.
2. Parse binary tuple data (using `pgrx::pg_sys` definitions).
3. Extract topic, partition, key, value.
4. **Async Produce:** Use rdkafka (Rust client) to send to the external target.
5. **Checkpoint:** Only advance the Replication Slot LSN (Log Sequence Number) after the external Kafka broker acknowledges receipt. This guarantees At-Least-Once delivery.

---

## 6. Technical Challenges & Mitigations

| Challenge | Risk | Mitigation |
|-----------|------|------------|
| Connection Limits | Postgres processes are heavy (10MB+). 10k Kafka clients = 10k PG processes. | **Multiplexing:** The tokio listener runs in one BGWorker process. It handles 10k TCP connections but only uses one SPI connection to the DB. |
| Blocking IO | If tokio blocks on a DB lock, the port 9092 stops responding. | Use pgrx's `run_in_background` for SPI calls to ensure the async runtime remains responsive for heartbeats. |
| Offset Gaps | BIGSERIAL can have gaps if transactions roll back. | Kafka clients generally tolerate offset gaps, but we must ensure strictly monotonic increase. The Postgres Sequence guarantees this. |

---

## 7. Implementation Roadmap

### Phase 1: The "Hello World" (2 Weeks)

**Objective:** `kcat -L -b localhost:9092` returns metadata.

**Tasks:**

- Scaffold pgrx extension.
- Implement `ApiVersions` and `Metadata` requests in Rust.
- Bind TCP listener in `_PG_init`.

### Phase 2: The Producer (3 Weeks)

**Objective:** `kcat -P` writes data to Postgres table.

**Tasks:**

- Implement `ProduceRequest` parser.
- Wire up SPI INSERT.
- Handle ACKs logic.

### Phase 3: The Consumer (3 Weeks)

**Objective:** `kcat -C` reads data back.

**Tasks:**

- Implement `FetchRequest`.
- Implement "Long Polling" via ConditionVariable.

### Phase 4: Shadow Mode (Staff Level)

**Objective:** Data written to Port 9092 appears in Confluent Cloud.

**Tasks:**

- Implement Logical Decoding client.
- Integrate rdkafka crate.

---

## 8. Why this is "Staff Engineering"

- **Protocol Engineering:** Implementing binary wire protocols requires exactness and low-level buffer manipulation.
- **Systems Integration:** Embedding an async runtime (tokio) inside a synchronous process model (postgres) is non-trivial.
- **Migration Strategy:** The "Shadow Mode" solves a business problem (vendor lock-in/migration risk), not just a technical one.