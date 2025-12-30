# pg_kafka 🐘➡️🕸️

**A high-performance PostgreSQL extension that speaks the Kafka Wire Protocol.**

`pg_kafka` allows standard Kafka clients (Java, Python, Go, etc.) to produce and consume messages directly from a PostgreSQL database. It runs as a native background worker in Postgres itself, eliminating the network hop of external proxies and the operational overhead of managing a separate Kafka cluster for small-to-medium workloads.

### 🚀 Why?
Teams often choose Postgres for queues to keep infrastructure simple, but hit a "scaling cliff" that forces a painful rewrite to Kafka. `pg_kafka` bridges this gap:
1.  **Dev/Test:** Use standard Kafka clients with zero infrastructure setup.
2.  **Production:** Run on RDS/Aurora until you hit scale limits.
3.  **Migration:** Use the built-in **Shadow Replicator** to dual-write to a real Kafka cluster and switch over with zero downtime.

### 🛠️ Architecture
* **Language:** Rust (via [`pgrx`](https://github.com/pgcentralfoundation/pgrx))
* **Protocol:** Kafka v2+ (Metadata, Produce, Fetch)
* **Storage:** Native Postgres Heap Tables
* **Concurrency:** `tokio` async runtime embedded in a Postgres Background Worker

---
*⚠️ Status: Active Development / Experimental (Staff Engineer Portfolio Project)*