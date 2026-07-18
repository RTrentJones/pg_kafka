# pg_kafka deep review — July 2026

A whole-repo review focused on **what the June 2026 audit did not cover**: operational lifecycle,
schema/write-path efficiency, protocol-fidelity gaps outside the security boundary, architecture
ceilings, CI/test-infrastructure quality, and documentation accuracy. It deliberately does **not**
re-litigate `AUDIT-2026-06.md` findings — every DR item below was checked against that audit's
status table and is new. Where this review touches an area the audit marked ✅/🔵, it verifies the
fix holds rather than re-reporting it.

- **Method:** 3 parallel deep readers (core `src/` · tests + CI/tooling · schema/docs/ops) over
  ~55k lines (~35k `src/`, ~19k `kafka_test/` + workflows/sql/docs), followed by a firsthand
  verification pass of every High/Medium claim against source at `013aa9b` (main, 2026-07-11).
- **Verified legend:** ✅ = confirmed firsthand against the code during the verification pass;
  ○ = reader-reported, plausible, spot-checked but not exhaustively re-derived.
- **Status legend:** 🔴 Open · ✅ Fixed (PR #) · 🔵 Accepted / by-design · ⚪ Deferred. All
  findings start 🔴 Open; this table is intended as the remediation tracker, same contract as
  `AUDIT-2026-06.md`.

---

## Executive summary

The codebase is in unusually good shape where the prior audits looked: the untrusted-input
boundary, SQL parameterization, panic/rollback containment, and offset monotonicity are
genuinely strong (re-verified, §"found clean"). The gaps that remain cluster in four places the
bug-hunts didn't target:

1. **Storage lifecycle** — nothing ever deletes anything. Most acutely, `cleanup_aborted_messages`
   is fully implemented, tested, and **never called from production code** (DR-1), so every
   aborted transactional message leaks forever. Retention for `messages` and pruning for the
   producer/txn/outbox tables don't exist (DR-2).
2. **Write-path efficiency** — a byte-for-byte duplicate index on `global_offset` (DR-3), record
   headers written on every produce but never returned to any consumer (DR-8), and a metrics view
   that full-scans the one unbounded table per scrape (DR-4).
3. **CI blind spots** — pg14-only despite advertising pg13–pg18, a floating `nightly` toolchain,
   non-blocking `cargo audit`, no `cargo-deny`, clippy/fmt that never see `kafka_test/`, and
   property tests that never run (DR-14…DR-18).
4. **Docs drift** — three different test-count claims across four docs, performance numbers
   borrowed from an external benchmark presented as pg_kafka results, and a README that documents
   the `0.0.0.0` default without mentioning the listener has no authentication (DR-20…DR-23).

Recommended order of attack: DR-1 (one-line wiring + E2E test), DR-3 (index drop), DR-14/DR-15
(CI pinning + matrix — cheap, high leverage), then the retention design (DR-2, executes ADR-001),
then the fetch-path fidelity bundle (DR-8, DR-9, DR-10, DR-11).

---

## Status table

| ID | Sev | Area | Finding | Location | Verified | Status |
|----|-----|------|---------|----------|:---:|:---:|
| DR-1 | 🔴 High | Storage lifecycle | `cleanup_aborted_messages` is production-dead: implemented, on the trait, delegated by `ShadowStore`, mocked, cited by ~6 offset-monotonicity comments as if live — but **no production caller exists** (`worker.rs` never invokes it; the only non-storage hit is a test mock). Aborted/timed-out txns only ever *mark* rows `txn_state='aborted'`; no path reclaims them → permanent bloat in `messages` + the `idx_messages_txn_pending` domain and LSO subquery. | `postgres.rs:1837`; trait `storage/mod.rs:642`; no caller in `worker.rs` | ✅ | ✅ Fixed (2f5c1e8) |
| DR-2 | 🔴 High | Storage lifecycle | No retention or pruning anywhere: `messages` grows unbounded (ADR-001 acknowledges, still "Proposed"); `producer_ids`/`producer_sequences` are never pruned despite `last_active_at` being maintained (a code comment at `postgres.rs:980` admits it); terminal-state `transactions` rows and delivered `shadow_tracking` outbox rows are never reaped. | `bootstrap.sql`; `postgres.rs:980`; ADR-001 | ✅ | ✅ Fixed (2f5c1e8) |
| DR-3 | 🟠 Med | Schema/write path | Duplicate index on the hottest column: `UNIQUE (global_offset)` already builds a btree; `idx_messages_global_offset` is a second identical single-column btree. Every insert maintains two hot right-edge monotonic indexes for zero read benefit. | `bootstrap.sql:45` vs `bootstrap.sql:55-56` | ✅ | ✅ Fixed (f7e76f7) |
| DR-4 | 🟠 Med | Observability | `kafka.metrics` view full-scans `kafka.messages` per scrape: `COUNT(*)` (×2), `SUM(pg_column_size(key)+pg_column_size(value))`, and a global `MAX(partition_offset)` for `lag`. On the unbounded table (DR-2) each scrape is O(n) and degrades monitoring exactly when it's most needed. The `lag` metric also takes one `MAX` across *all* topics/partitions — semantically wrong for multi-topic deployments. | `bootstrap.sql:419-431,492-494` | ✅ | ✅ Fixed (f7e76f7) |
| DR-5 | 🟡 Low-Med | Schema/ops | Autovacuum tuning targets the wrong table: `tune_autovacuum.sql` tunes only `kafka.messages` (append-mostly; UPDATE only on abort-marking, DELETE never — see DR-1). The genuinely high-churn tables — `partition_offsets` (UPDATE per produce), `producer_sequences` (UPDATE per idempotent batch), `consumer_offsets` (UPSERT per commit), `shadow_tracking` — get no treatment. | `tune_autovacuum.sql:24` | ✅ | ✅ Fixed (f7e76f7) |
| DR-6 | 🟡 Low | Schema | `kafka.consumer_groups` (+ `idx_consumer_groups_heartbeat`) is dead schema: nothing in `src/` reads or writes it except the `verify_schema` existence list. Its comment ("simplified static assignment (no rebalancing)") contradicts the actual in-memory Range/RoundRobin/Sticky rebalancing coordinator. | `bootstrap.sql:96-108`; only ref `worker.rs:119` | ✅ | ✅ Fixed (f7e76f7) |
| DR-7 | 🟡 Low | Schema | No validation that `partition_id` is within the topic's declared partition count — it's a bare `INT` with no FK (no partitions table) and no CHECK against `kafka.topics.partitions`, in `messages`, `consumer_offsets`, `producer_sequences`, `txn_pending_offsets`. Handler-level routing keeps conformant clients in range; direct SQL or a handler bug can create phantom partitions silently. | `bootstrap.sql:24-47,80-95` | ○ | ✅ Fixed (f7e76f7) |
| DR-8 | 🟠 Med | Protocol fidelity | Record headers are stored but **never returned to consumers**: every produce writes `headers JSONB`, but neither fetch query selects the column and `FetchedMessage` has no headers field — consumers always receive empty headers. Cost paid on every write for data that is unreadable over the wire; undisclosed in `PROTOCOL_DEVIATIONS.md` (which lists Fetch as fully working). Breaks tracing/schema-registry-style clients that rely on headers. | `storage/mod.rs:33-42`; `postgres.rs:284-292,1649+`; write side `postgres.rs:179-193` | ✅ | ✅ Fixed (075b1cc) |
| DR-9 | 🟠 Med | Protocol fidelity / perf | Fetch under-delivers vs `max_bytes`: `initial_limit = (max_bytes / ESTIMATE_BYTES_PER_MESSAGE).clamp(10, 5_000)` caps every fetch at 5,000 rows regardless of byte budget. A 1 MB fetch of small messages returns tens of KB, forcing extra round trips through the single DB thread (compounds DR-12). | `postgres.rs:279,1643` | ✅ | ✅ Fixed (075b1cc) |
| DR-10 | 🟠 Med | Correctness | Fetch handler swallows storage errors as offset 0: `get_high_watermark(...).unwrap_or(0)` and `get_earliest_offset(...).unwrap_or(0)`. A transient Postgres error is presented to the consumer as HWM/log-start = 0 (empty partition) instead of a retriable error code — a client may reset position or believe data vanished. | `fetch.rs:108,124` | ✅ | ✅ Fixed (075b1cc) |
| DR-11 | 🟡 Med | Protocol fidelity | read_committed is served entirely by server-side filtering: fetched batches are re-encoded with `producer_id: -1`, no control/abort markers, and an empty `aborted_transactions` list. Works for rdkafka-style clients; a strict client doing client-side abort filtering (the wire contract for Fetch ≥ v4) would see no aborted ranges and trust unfiltered data. Also loses original batch boundaries and producer identity on replay. Should be documented in `PROTOCOL_DEVIATIONS.md` (or the supported Fetch version range justified against it). Related smalls: `throttle_time_ms` hardcoded 0 everywhere; `log_append_time_ms` always -1. | `fetch.rs:127-171`; `produce.rs:118,141,180` | ○ | ✅ Fixed (075b1cc) |
| DR-12 | 🟡 Med | Architecture | Head-of-line blocking across API types: all requests share one FIFO crossbeam channel into the single DB thread, so a heavy Produce (large UNNEST, advisory-lock wait) delays Heartbeat/Metadata/OffsetFetch for *every* client. Under DB pressure, delayed heartbeats can cascade into spurious consumer-group rebalances — a self-amplifying failure mode. No priority lane exists for liveness traffic even though Heartbeat is served from in-memory coordinator state and needs no SPI. | `worker.rs:311-312,679-735`; `listener.rs:519+` | ✅ | ✅ Fixed (60ecc53) |
| DR-13 | 🟡 Low | Architecture | Per-partition `pg_advisory_xact_lock` on both insert paths is pure overhead in the shipped topology: with exactly one DB thread there is never a concurrent in-extension writer; the lock only guards against out-of-band SQL writers. Worth keeping as defense-in-depth, but the cost/benefit deserves an explicit comment + benchmark, since it taxes every produce. | `postgres.rs:142-146,1284-1288` | ✅ | 🔵 Accepted (documented, 60ecc53) |
| DR-14 | 🟠 Med | CI / supply chain | Toolchain is floating `nightly` (no date pin) in `rust-toolchain.toml` and every CI job — any upstream nightly regression breaks all builds simultaneously and irreproducibly. Compounding: `kafka-protocol` is pinned to a personal fork rev (`RTrentJones/kafka-protocol-rs`); necessary (carries the SEC-9/RV-1 alloc caps) but a single-point supply-chain dependency with no vendoring/fallback documented. | `rust-toolchain.toml:2`; `ci.yml:33,105,378`; `Cargo.toml` `[patch.crates-io]` | ✅ | ✅ Fixed (e8137f9) |
| DR-15 | 🟠 Med | CI | No PostgreSQL version matrix: features advertise `pg13`–`pg18`, but CI hardcodes `PG_VERSION: "14.20"` and `--features pg14` in every job. Breakage on pg15–pg18 ships undetected; the multi-version claim is untested. | `ci.yml:18,73` et al.; `Cargo.toml` features | ✅ | ✅ Fixed (e8137f9) |
| DR-16 | 🟠 Med | CI | Security/lint gates don't gate: `cargo audit` is `continue-on-error: true`; no `cargo-deny`/`deny.toml` (no license/ban/duplicate enforcement); clippy runs without `--all-targets` (test code unlinted); the entire `kafka_test/` crate is never fmt-checked, clippy-checked, or `--locked`-verified in CI. | `ci.yml:37,73,367-368`; absent `deny.toml` | ✅ | ✅ Fixed (e8137f9) |
| DR-17 | 🟡 Med | Testing | E2E timing is sleep-based: 91 fixed `tokio::time::sleep` calls across the suite (hot spots: `consumer_group/coordinator_state.rs` ×11, `long_poll/edge_cases.rs` ×8, `transaction/atomicity.rs` ×6, incl. a 3 s sleep in `shadow/basic_forwarding.rs:376`), plus a bare `sleep 3` readiness hack in CI itself. A condition-polling `wait_for` helper already exists (`fixtures.rs:311-330`) but is rarely used. 65/191 tests are `parallel_safe: false`, largely to protect these timings. This is the suite's main flakiness reservoir. | `kafka_test/src/**`; `ci.yml:286` | ✅ | ✅ Fixed (072b62f) |
| DR-18 | 🟡 Med | Testing | Performance tests cannot fail: thresholds of 5 msg/s, misses print `WARNING` instead of failing, and the CI job is `continue-on-error: true` — three layers of advisory. Zero regression protection despite the benchmark evidence pipeline. Separately, `tests/property_tests.rs` never runs in CI (`cargo llvm-cov --lib` excludes integration targets) and contains tautological asserts (e.g. asserting a `0..100` sample is `>=0 && <100`). | `kafka_test/src/performance/throughput.rs:14-18,69-74,225-229`; `ci.yml:325-334`; `tests/property_tests.rs:28-33` | ✅ | ✅ Fixed (e8137f9, 072b62f) |
| DR-19 | 🟡 Low | Testing / CI | `spi-e2e-guard` is satisfiable by touching *any* path under `kafka_test/` (a comment edit passes); `SPI_REGEX` must be hand-synced with `codecov.yml`'s ignore list (two sources of truth, drift risk). `TestContext::drop` cleanup is a detached, un-awaited task and `cleanup()` swallows all errors — leaked rows between runs are invisible. | `require-e2e-for-spi.sh:16-19,25`; `setup.rs:88-113,124-161` | ✅ | ✅ Fixed (072b62f) |
| DR-20 | 🟠 Med | Docs / security | README documents `pg_kafka.host = '0.0.0.0'` with **no mention that the wire protocol has no authentication or TLS** — the only auth references are Shadow-mode SASL (outbound). The startup WARNING (SEC-7) exists in code, but a README reader deploying to a network never learns the exposure. One paragraph + firewall guidance closes it. | `README.md:299` (grep: no auth warning anywhere) | ✅ | ✅ Fixed (1ff5d90) |
| DR-21 | 🟡 Low | Security | `pg_kafka.shadow_sasl_username` registers with `GucFlags::default()` — visible in `pg_settings`/`SHOW ALL` to every role, unlike the password (`NO_SHOW_ALL \| SUPERUSER_ONLY`). Discloses the external-broker principal; the same two flags fix it. (`Debug` output already redacts it.) | `config.rs:451` vs `config.rs:463` | ✅ | ✅ Fixed (1ff5d90) |
| DR-22 | 🟡 Low | Docs | Test counts are stale and mutually contradictory: README/CLAUDE.md/TEST_STRATEGY claim 609 unit + 173 E2E (= 782); PROTOCOL_DEVIATIONS.md claims 672 + 181; actual: **672 `#[test]` in `src/` (+9 `#[tokio::test]`) and 191 registered E2E tests**. All three claims are wrong; the drift direction (understatement) is benign, but hand-maintained counts in four places will never stay right — generate or drop them. | `README.md:24,266-267`; `TEST_STRATEGY.md:12-13`; `PROTOCOL_DEVIATIONS.md:363`; `CLAUDE.md:49` | ✅ | ✅ Fixed (1ff5d90) |
| DR-23 | 🟠 Med | Docs | `PERFORMANCE.md` misleads: "1,000,000+ reads/sec / 200,000+ writes/sec" are external-benchmark numbers not measured on pg_kafka (whose ceiling is the single serialized SPI thread); "one SPI worker is already optimal! No additional pooling needed" reframes the hard bottleneck as an optimization; the config section documents GUCs that don't exist (`pg_kafka.retention_days`, `pg_kafka.partition_interval`, `pg_kafka.autovacuum_aggressive`). The real measured numbers already exist in the evidence pipeline (`bench.json`) — the doc should cite those. | `PERFORMANCE.md:12-16,190-193,371-380`; `config.rs` (GUCs absent) | ✅ | ✅ Fixed (1ff5d90) |
| DR-24 | 🟡 Low | Code quality | `process_request` is a ~977-line, 30+-arm match with 21 near-identical `HandlerContext::new(...)` constructions; `insert_records`/`insert_transactional_records` are ~90% duplicated (advisory lock → GREATEST base offset → UNNEST → counter advance) and the `GREATEST(next_offset, MAX+1)` SQL is copy-pasted ×4; `fetch_records` duplicates the ReadUncommitted branch of `fetch_records_with_isolation` byte-for-byte and survives only for the ShadowStore SH-6 path; repeated `#[allow(clippy::too_many_arguments)]` (e.g. 12-arg `handle_fetch_long_poll`) mark missing parameter structs. | `worker.rs:879+`; `postgres.rs:128-248` vs `1262-1391`; `postgres.rs:154,351,1295,1756`; `listener.rs:750-763` | ✅ | ✅ Fixed (075b1cc, 60ecc53, 5dddedb) |
| DR-25 | 🟡 Low | Dev experience | `docker-compose.yml` hardcodes container IP `172.18.0.2` as the advertised listener and pins a `/16` subnet (brittle across hosts; obsolete `version:` key). `.devcontainer/devcontainer.json` places `RUST_LOG`/`RUSTUP_PERMIT_COPY_RENAME` as top-level keys — not valid devcontainer properties, silently ignored (belong under `containerEnv`). Pre-commit hook runs full clippy (minutes cold) → invites `--no-verify`; consider fmt-only pre-commit + clippy in pre-push/CI. `CLAUDE.md` references `sql/pg_kafka--0.0.0.sql` and root `restart.sh`; actual paths are `sql/bootstrap.sql` and `scripts/restart.sh`. | `docker-compose.yml`; `.devcontainer/devcontainer.json`; `hooks/pre-commit:29`; `CLAUDE.md` | ○ | ✅ Fixed (1ff5d90) |

---

## Strategic recommendations (beyond the table)

### S-1. Ship a lifecycle story (DR-1, DR-2, DR-4, DR-5 together)
The single biggest gap between "portfolio-complete" and "operable" is that the system only ever
grows. A coherent v1 lifecycle PR would: wire `cleanup_aborted_messages` into the existing 10 s
maintenance loop (the offset-monotonicity design already anticipates it — the comments say so);
add a retention sweep for `messages` per ADR-001 (even a simple `DELETE ... WHERE created_at <
now() - retention` behind a GUC beats nothing; native partitioning can come later); prune stale
`producer_ids`/`producer_sequences` by `last_active_at`, terminal `transactions`, and delivered
`shadow_tracking` rows; re-point `tune_autovacuum.sql` at the churny tables; and replace the
`kafka.metrics` full scans with `pg_stat_user_tables`/`pg_class.reltuples` estimates plus
per-topic lag. Each piece is small; together they close the "runs forever" gap.

### S-2. A liveness fast lane before any bigger concurrency work (DR-12)
The single-DB-thread ceiling is a deliberate, well-documented trade — don't abandon it. But
Heartbeat (and arguably JoinGroup/SyncGroup/FindCoordinator) never touch SPI; they read the
in-memory coordinator. Serving those directly on the network thread (the coordinator lock is
already `Arc<RwLock>`; the single-DB-thread invariant note in CLAUDE.md would need the
synchronization review it prescribes) or via a second, higher-priority channel drained before the
main one would decouple group liveness from DB latency and eliminate the pressure→rebalance
cascade. This is the highest-leverage architectural improvement available without redesigning
the storage path.

### S-3. Make CI match the project's own rigor (DR-14…DR-18)
The remediation culture here (fail-before/pass-after tests, machine-enforced QA-1) is better than
most production repos — but CI undercuts it: pin `nightly-YYYY-MM-DD`; add a pg14/pg16/pg17
matrix (even `cargo check` per version catches API breaks cheaply); make `cargo audit` blocking
with a documented-exceptions file and add `cargo-deny`; run clippy `--all-targets` plus
fmt/clippy/`--locked` over `kafka_test/`; either run the proptest target in CI or delete it; give
the two throughput tests honest thresholds derived from the evidence-pipeline baselines (e.g.
fail below 50 % of the rolling median) instead of 5 msg/s warn-only.

### S-4. Kill the sleeps with the tool that already exists (DR-17)
`fixtures.rs` already has `wait_for`. A mechanical migration of the 91 fixed sleeps to
condition-polls (poll the DB row count / group state / offset instead of sleeping a guessed
duration) would cut suite wall-time, unlock parallelism for some of the 65 serialized tests, and
remove the main CI-flake reservoir. Do it category-by-category; `consumer_group/` and
`transaction/` first.

### S-5. Docs: correct, then generate (DR-20…DR-23)
One honesty pass: add the README no-auth warning (mirror the SEC-7 startup warning text); rewrite
`PERFORMANCE.md` around the evidence pipeline's measured `bench.json` numbers and delete the
phantom GUCs; fix the four stale test counts — then stop hand-maintaining them (a tiny script in
CI can inject counts, or replace numbers with "see CI"). Drop the vestigial `consumer_groups`
table (DR-6) or comment it as reserved-for-future-durability so schema and reality agree.

### S-6. Fetch-path fidelity bundle (DR-8, DR-9, DR-10, DR-11)
These four are one themed PR: select + return `headers` (add the field to `FetchedMessage`,
decode JSONB → wire headers); replace the 5,000-row clamp with byte-budget-driven iteration (or
raise the cap and let the existing cumulative-bytes window do its job); surface HWM/earliest-
offset storage errors as per-partition error codes instead of `unwrap_or(0)`; and document the
server-side-filtering isolation model in `PROTOCOL_DEVIATIONS.md` (empty `aborted_transactions`,
re-encoded batches, `producer_id=-1`) so strict-client behavior is a stated deviation, not a
surprise.

---

## What was re-verified and found clean

Consistent with the second-pass sweep in `AUDIT-2026-06.md`, this review independently confirmed
(rather than re-reported): fully parameterized SQL throughout the storage layer (no injection
surface; the one dynamically-built query in `verify_schema` interpolates only a hardcoded table
list); the two-layer panic containment (dispatch `catch_unwind` reply + subtransaction rollback,
maintenance loops included); the DoS posture on the untrusted boundary (512 MiB aggregate frame
budget, connection semaphore, bounded channels, decompression caps); offset monotonicity via the
never-decremented `partition_offsets` counter taken `GREATEST` with `MAX+1` across all four call
sites; pipelined per-connection response ordering; shadow SASL **password** GUC protection
(`NO_SHOW_ALL | SUPERUSER_ONLY`) and `[REDACTED]` Debug impls (the *username* gap is DR-21); and
the transaction-timeout sweep honoring per-row `timeout_ms`. The tracked-tag comment discipline
(BUG-x/RV-x/SEC-x annotations at fix sites) deserves explicit praise — it made this review
materially faster and is rare in any codebase.

---

## Resolution changelog

- 2026-07-14 — **follow-up round (post-merge of PR #94)**: the strategic
  recommendations and the remaining tractable items from the sweep were
  addressed on a fresh branch off `main`. This closes the root cause behind the
  DR-era database-verification flakiness rather than only the test-side symptom.
  - **RB-1 — response-after-commit barrier** (`src/worker.rs`): Kafka responses
    were sent from *inside* `BackgroundWorker::transaction`, so an acked
    produce/offset-commit could be lost on a crash before commit (acks≥1
    durability violation) and a client could race a separate connection into
    pre-commit state (the S-tier version of the admin CI flake). The worker now
    buffers each response and flushes it only after the transaction commits;
    a commit-time panic sends an API-typed error instead of a false success.
    Deterministic fail-before/pass-after E2E via a test-only
    `pg_kafka.test_pre_commit_delay_ms` GUC. Documented in
    PROTOCOL_DEVIATIONS.md (§ Acknowledgment Durability).
  - **Issue #93 — shadow outbox duplicate delivery**: an in-flight tracker
    (`src/kafka/shadow/inflight.rs`, unit-tested) + a claim-query anti-join stop
    the outbox re-dispatching a row whose forward is merely slow; duplicates now
    occur only on genuine ack loss, keeping the async path at-least-once instead
    of ~8%-duplicating. E2E via `pg_kafka.test_forward_ack_delay_ms`.
  - **S-2 partial — per-connection long-poll task cap** (`listener.rs`, the
    RV-10 deferred item): bounded at 64; over-cap fetches degrade to the
    immediate path. Raw-wire E2E (80 pipelined → exactly 64 long-poll / 16
    immediate).
  - **API completeness (23 → 26)**: DescribeConfigs (32), IncrementalAlterConfigs
    (44), DeleteRecords (21) — honest-minimal (`retention.ms` per topic,
    persisted and enforced by the DR-2 sweep, which lands ADR-001's per-topic
    retention question; unsupported keys rejected). 11 new unit + 4 new E2E.
  - **Code/test hygiene**: txn commit/abort SQL unified into `end_transaction`
    (PR #84 follow-up); `wait_for_count` promoted into `fixtures.rs` and the
    fragile fixed-sleep verification sites converted to bounded condition polls;
    the stale devcontainer-networking TODO replaced with accurate docs.
  - Validation: 705 unit + property tests green; clippy `--all-targets` clean on
    both crates; every new QA-1 E2E demonstrated fail-before/pass-after on a live
    pg16 instance.

- 2026-07-11 — review created on `013aa9b`; all findings 🔴 Open.
- 2026-07-11 — **full remediation pass** (same branch, seven themed commits, each
  validated against a live pg16 instance — which itself exercises DR-15):
  - `f7e76f7` — **DR-3..DR-7** (schema): dropped the duplicate `global_offset`
    index and the dead `consumer_groups` table; `kafka.metrics` rewritten to O(1)
    catalog estimates + pending-outbox lag; `CHECK (partition_id >= 0)` backstops;
    `tune_autovacuum.sql` retargeted at the churny counter tables.
  - `2f5c1e8` — **DR-1, DR-2** (lifecycle): `cleanup_aborted_messages` wired into a
    new 60 s retention sweep (fixing, en route, a latent read-only-SPI DELETE bug
    the missing caller had hidden); `pg_kafka.message_retention_hours` GUC;
    pruning for stale producers, terminal transactions, delivered outbox rows;
    `pg_kafka_run_retention_sweep()` for operators/tests; 2 E2E tests.
  - `075b1cc` — **DR-8..DR-11** (+part of DR-24): headers now roundtrip to
    consumers (E2E-verified); byte-budget fetch iteration replaces the 5,000-row
    cap; HWM/LSO/log-start storage errors surface as partition error codes; the
    server-side read_committed model documented in PROTOCOL_DEVIATIONS.md; both
    fetch paths share one implementation.
  - `60ecc53` — **DR-12, DR-13**: Heartbeat liveness lane drained before each
    main-lane request (measured ~3 ms heartbeat RTT against a ~2.5 s produce
    backlog, stable across runs; raw-wire E2E test); advisory-lock trade
    documented at both insert sites (kept as defense-in-depth → 🔵).
  - `e8137f9` — **DR-14..DR-16, DR-18-part**: nightly pinned to a date and CI reads
    rust-toolchain.toml everywhere; pg15/16/17 check+unit matrix; cargo audit
    blocking over BOTH lockfiles (kafka_test's was still carrying RV-11/RV-14
    vulnerable versions); cargo-deny with fork-pinned sources; clippy
    --all-targets + kafka_test fmt/clippy/--locked; property tests run in CI.
  - `072b62f` — **DR-17..DR-19**: perf thresholds are real regression floors and
    the CI perf job is blocking; the tautological proptest asserts a real
    roundtrip; CI's `sleep 3` removed; the 13 s sweep-wait replaced by a sentinel
    condition poll (3 s, sounder); guard script derives its file list from
    codecov.yml and requires a kafka_test/src *.rs change; TestContext cleanup
    failures are logged. Residual fixed sleeps are deliberate scenario pacing,
    not condition waits.
  - `1ff5d90` — **DR-20..DR-23, DR-25**: README no-auth warning;
    `shadow_sasl_username` locked down like the password (E2E-probed); test
    counts corrected everywhere and pointed at CI as source of truth;
    PERFORMANCE.md rewritten around measured evidence with the single-thread
    ceiling stated plainly and phantom GUCs removed; devcontainer env keys fixed
    (file was also invalid JSON); compose `version:` key dropped and static-IP
    trade documented; pre-commit split into fast fmt + pre-push clippy.
  - `5dddedb` — **DR-24**: one HandlerContext per dispatch (was 21 copies); plain
    and transactional inserts unified into `insert_records_inner` (the twin-path
    shape behind the RA-1/RA-2/RV-5 class of bugs).
  - Validation: 686 unit + 10 property tests green; clippy `--all-targets` clean
    on both crates; cargo audit/deny clean; full functional E2E suite (162
    tests — the CI selection) green on a live pg16 instance, twice consecutively
    after the final refactor.
