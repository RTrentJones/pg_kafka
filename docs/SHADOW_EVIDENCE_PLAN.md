# Shadow-mode forwarding — evidence & reporting plan

> Status: **planned, not yet implemented.** This doc is the execution spec (every file, schema, and wiring
> point is named). Written to a temp branch (`docs/shadow-evidence-plan`) as a hand-off; it can be executed
> as its own focused PRs. All paths are relative to the repo root shown, absolute where cross-repo.

## Why

Two goals, coupled:

1. **Continuously-verified proof that shadow forwarding is compliant** — pg_kafka is an implementation, not a
   hosted service, so "it works" is shown as *evidence regenerated in CI and published as data* (the existing
   `conformance.json` / `bench.json` / `session.svg`). Shadow-mode forwarding is currently **absent** from
   that pipeline and from the site.
2. **Before/after verification for the dead-`ShadowStore`-producer removal (PR4).** PR4 deletes the *dead*
   pre-outbox forwarding path; the evidence exercises the *live* path (`forward_sync_bounded` + durable outbox
   + network-thread producer). So the shadow evidence must be **identical before and after PR4** = published,
   objective proof the removal changed nothing.

Two kinds of shadow evidence:
- **Compliance** (pass/fail + parity counts) → a new `shadow.json` artifact + a new site table.
- **Throughput (TPS)** → new shadow scenarios folded into the **existing `bench.json`**, rendered in the
  **existing benchmark table** next to `produce`/`consume`/`end-to-end` (floor: raw-INSERT, ceiling: real
  broker) — so the forwarding cost is shown in-context.

## Architecture recap (the evidence pipeline)

`.github/workflows/evidence.yml` (weekly cron + on `v*` tags + `workflow_dispatch`) brings up pgrx pg_kafka
on `127.0.0.1:9092` and `docker compose up -d external-kafka` (the real broker, host port **9093** =
`REAL_BROKER`; the same broker pg_kafka's shadow config forwards to). It then runs
`scripts/generate-evidence.sh <out-dir>`, which calls N best-effort steps (each `|| echo "::warning::…"`),
each writing one JSON/SVG. The publish step force-pushes the artifacts to the orphan **`evidence`** branch;
`bench.json` is also normalized and POSTed to Tracer.

Consumption: the site (`/home/rtj/workspace/RTrentJones.dev/apps/blog`) fetches the `evidence` branch at
**build time** from `raw.githubusercontent…/pg_kafka/evidence` and renders it server-side (a null result
shows an honest "regenerating in CI" note — never fabricated green).

Existing artifact contracts (from `evidence/README.md`):
- `conformance.json`: `{ generatedAt, version, gitSha, apis:[{key,name}], clients:[{name,lang,version,
  results:{ApiName:"pass"|"fail"|"na"}}] }`.
- `bench.json`: `{ generatedAt, version, gitSha, config:{warmupExcluded,messageSizes}, scenarios:[{name,
  p50Ms,p99Ms,msgsPerSec}], baselines:{rawInsert,realBroker} }`.

---

## Part A — Shadow evidence generation (this repo, `tools/pg_kafka`)

Both harnesses below slot into the existing `evidence.yml` "Generate evidence" job (extension + real broker +
Node/Go/Python already set up). Both must first point pg_kafka's shadow at the real broker via the
runtime-reloadable GUCs, then per-topic shadow config — mirror `kafka_test/src/shadow/helpers.rs`
`enable_shadow_mode`:

```sql
ALTER SYSTEM SET pg_kafka.shadow_mode_enabled = true;
ALTER SYSTEM SET pg_kafka.shadow_bootstrap_servers = '<REAL_BROKER e.g. localhost:9093>';
ALTER SYSTEM SET pg_kafka.shadow_security_protocol = 'PLAINTEXT';
ALTER SYSTEM SET pg_kafka.config_reload_interval_ms = 2000;
-- per topic (topic must exist; resolve id via kafka.topics.name):
INSERT INTO kafka.shadow_config
  (topic_id, mode, forward_percentage, external_topic_name, sync_mode, write_mode, updated_at)
VALUES ($id, 'shadow', 100, NULL, '<async|sync>', 'dual_write', NOW())
ON CONFLICT (topic_id) DO UPDATE SET mode=EXCLUDED.mode, forward_percentage=EXCLUDED.forward_percentage,
  external_topic_name=EXCLUDED.external_topic_name, sync_mode=EXCLUDED.sync_mode,
  write_mode=EXCLUDED.write_mode, updated_at=NOW();
SELECT pg_reload_conf();   -- SIGHUP; the bgworker reloads (config_reload_interval also applies)
```
Config lives in `kafka.shadow_config`; results are readable from the `kafka.shadow_status` view
(`total_forwarded/total_skipped/total_failed/last_forwarded_offset/lag` per topic) and `kafka.shadow_tracking`
(`external_offset IS NOT NULL` = forwarded/finalized). Both granted `SELECT` to PUBLIC.

### A1 — Compliance artifact `shadow.json` (new dir `evidence/shadow/`)

Mirror `evidence/conformance/` + `evidence/bench/` (a `run.sh` that runs a harness into `$WORK/` then a small
`assemble.mjs` stamper; stamp from env `GENERATED_AT`/`VERSION`/`GIT_SHA`; degrade to a "pending" shape on
failure, never fabricate).

Recommended: a **self-contained kafkajs harness** `evidence/shadow/shadow.mjs` (like `bench.mjs`), *not* a
rebuild of the E2E suite — it directly measures forwarding parity, needs no `cargo build`. For each scenario
it: enables the topic's shadow config (SQL above, via `pg` or shelling `psql`), produces N records to
pg_kafka (`PG_KAFKA_BROKER`), consumes the external/real broker (`REAL_BROKER`) to count delivered, and reads
`kafka.shadow_status`. Scenarios: `dual_write_async`, `dual_write_sync`, `external_only`, `percentage_50`,
`committed_txn_forwarded`, `aborted_txn_not_forwarded`. (Alternative if you'd rather reuse the proven suite:
build `kafka_test` and capture `kafka_test --category shadow --json` — its `SuiteResult{categories:[{tests:
[{name,passed,duration_ms,error}]}]}` maps 1:1 onto `checks[]`; heavier but zero new assertion code.)

`shadow.json` schema (house style — top-level stamps, structured body):
```jsonc
{ "generatedAt":"<ISO>", "version":"…", "gitSha":"…",
  "config": { "records":500, "forwardPercentage":100, "writeMode":"dual_write", "syncMode":"sync",
              "realBroker":"localhost:9093" },
  "checks": [ { "name":"dual_write_async", "status":"pass" },        // pass | fail | na, one per scenario
              { "name":"committed_txn_forwarded", "status":"pass" } ],
  "counts": { "produced":500, "forwardedToBroker":500, "localStored":500, "shadowMetricsForwarded":500,
              "skipped":0, "failed":0, "outboxFinalized":500, "lag":0 },
  "passed": true }                                                    // all checks pass AND forwarded==produced (100% topic)
```

### A2 — Throughput scenarios in the EXISTING `bench.json`

Extend `evidence/bench/` so shadow-forwarding TPS lands in `bench.json.scenarios[]` and renders in the
existing table (no new component):

- **`evidence/bench/bench.mjs`** — add three env knobs (keep behaviour identical when unset):
  - `TOPIC` — if set, produce to this fixed topic instead of the random `bench-${LABEL}-${Date.now()}` (so
    the run targets a pre-shadow-configured topic).
  - `TAG` — if set, inject into scenario names, e.g. `produce · batched(100) · <TAG> · 1024B`.
  - `ONLY` — comma-list of scenario keys to run (e.g. `produce-batched`); shadow only needs the produce path
    (forwarding happens on produce; consume/e2e aren't shadow-specific).
- **`evidence/bench/run.sh`** — after the existing pg_kafka/real/raw runs, add two shadow runs (each: create
  the topic, apply the shadow SQL for `async`/`sync` dual-write → `$REAL_BROKER`, then run `bench.mjs`):
  ```bash
  BROKER="$PG_KAFKA_BROKER" LABEL=pg_kafka TOPIC=bench-shadow-async TAG=shadow-async ONLY=produce-batched \
    OUT="$WORK/bench-shadow-async.json" timeout -k 10 240 node bench.mjs
  BROKER="$PG_KAFKA_BROKER" LABEL=pg_kafka TOPIC=bench-shadow-sync  TAG=shadow-sync  ONLY=produce-batched \
    OUT="$WORK/bench-shadow-sync.json"  timeout -k 10 240 node bench.mjs
  ```
  Expected: `shadow-async` ≈ plain produce (forward is off the ack path — proves async forwarding is
  near-free to the client); `shadow-sync` lower (bounded wait for the external ack — the real forwarding TPS
  cost). Optionally consume `$REAL_BROKER` afterward to assert delivery, so a "fast but not forwarding" run
  can't masquerade as a pass.
- **`evidence/bench/assemble.mjs`** — read `bench-shadow-async.json` / `bench-shadow-sync.json` and append
  their scenario(s) to `bench.json.scenarios[]` (baselines `rawInsert`/`realBroker` unchanged).

### A3 — Wire-in + docs

- **`scripts/generate-evidence.sh`** — add a 4th best-effort step (A1): after the recording step,
  `echo "### shadow"; bash "$ROOT/evidence/shadow/run.sh" "$OUT_DIR" || echo "::warning::shadow step failed"`.
  A2 rides the existing `evidence/bench/run.sh` step (no orchestrator change).
- **`.github/workflows/evidence.yml`** — the publish loop (~line 170) hard-codes the file list; add
  `shadow.json`: `for f in conformance.json bench.json session.svg shadow.json; do …`. (`bench.json` is
  already published, so A2 needs no publish change.)
- **`evidence/README.md`** — add a `shadow.json` row to the artifact table + a Contract block, and note the
  two shadow bench scenarios.
- **`/home/rtj/workspace/RTrentJones.dev/docs/ci-throughline.md`** — add `shadow.json` to the mermaid
  `evidence branch` node and the per-repo table.
- **Optional (defer):** a `mode:'shadow'` Tracer POST — Tracer's `evalRunInput` schema already accepts a
  free-form `mode`, so **no Tracer code change**; add a shadow variant to `.github/scripts/ingest-tracer.mjs`
  (map each shadow check → a `cases[]` entry, `score` = fraction forwarded), inert without
  `TRACER_INGEST_TOKEN`.

**CI note:** Part A touches only `evidence/*`, `scripts/*`, and the workflow — **not** the SPI source
(`worker.rs`/`storage/postgres.rs`/`shadow/store.rs`/`listener.rs`), so the `spi-e2e-guard` job does not fire
and no `no-e2e-needed` label is needed.

---

## Part B — Shadow reporting (site repo, `/home/rtj/workspace/RTrentJones.dev/apps/blog`)

The build-time loader hub is `apps/blog/src/lib/evidence.ts` (`EVIDENCE_BASE =
raw.githubusercontent…/pg_kafka/evidence`, a generic `load<T>(file)` → `{data, source}` with a 5s timeout and
no fabricated fallback, plus `loadConformance`/`loadBench`/`loadSessionSvg`, typed interfaces, and
`fmtVerified(iso)`).

- **Throughput → existing table, no new component.** The A2 shadow scenarios flow through the existing
  `loadBench()` → `apps/blog/src/components/BenchTable.astro` (it already maps `scenarios[]`), so they appear
  as rows automatically. Only tweak: a one-line caption noting the `shadow-async`/`shadow-sync` rows.
- **Compliance → new table.**
  - `apps/blog/src/lib/evidence.ts` — add a `ShadowData` interface + `loadShadow()` → `shadow.json` (one
    loader, mirroring `loadBench`).
  - `apps/blog/src/components/ShadowTable.astro` — new component copying `BenchTable.astro`'s structure:
    `const { data } = await loadShadow()` → null-guard `.evidence-pending` note → a `checks` pass/fail table +
    a `counts` parity row → `fmtVerified(data.generatedAt)` stamp.
  - `apps/blog/src/content/projects/pg_kafka.mdx` — import `ShadowTable`; add a `##` "Shadow-mode forwarding,
    verified" section (compliance table + a note that its throughput is the shadow rows in the benchmark table
    above), framed as *compliance verified before/after*. Shadow is currently unmentioned on the page — also
    add a one-line mention to the intro feature list.
- Ships via the **blog's Greenlight deploy-verify-promote loop** (branch → preview → `greenlight verify` →
  beta → prod). `apps/blog/verify.config.ts` already asserts `/pg_kafka/` returns 200.
- *Optional:* a paragraph in `apps/blog/src/content/blog/pg-kafka-proof.mdx` (the "building the proof" post)
  on the before/after forwarding story.

**Note:** pg_kafka is deliberately **not** a Greenlight tool (`greenlight.config.ts` lists only
heistmind/bamcp/tracer/muse + blog); its *page* rides the blog's lifecycle. No `greenlight.config.ts` change.

---

## Sequencing & before/after protocol

Sequential merges under the strict `main` gate; rebase each on latest `main`.

1. **PR-A (evidence: A1 `shadow.json` + A2 bench scenarios + wiring/docs)** → merge → **manually dispatch
   `evidence.yml`** (`gh workflow run evidence.yml`). Publishes the first `shadow.json` + shadow-throughput
   rows = **BASELINE** (dead producer code still present, but off the live path).
2. **PR-B (site reporting)** → blog loop; renders the compliance table + shadow throughput rows (shows the
   baseline once published).
3. **PR4 (dead-`ShadowStore`-producer removal)** → merge (rebased on post-A `main`).
4. **Re-dispatch `evidence.yml`** → new evidence: every compliance check still `pass`, `counts` identical, and
   the shadow-async/-sync TPS rows unchanged = **AFTER**, published proof PR4 was behaviour-neutral for
   forwarding.

## Verification

- **PR-A CI:** `fmt`/`clippy` unaffected (no Rust changes); the shadow harnesses run only in `evidence.yml`.
- **The evidence run is the end-to-end check:** `shadow.json` `passed:true` with `forwardedToBroker ==
  produced`, every check green, and the shadow TPS rows populated — before AND after PR4.
- **Site:** `/pg_kafka/` renders the compliance table + shadow benchmark rows (blog `verify.config.ts`).

---

## File / reference index

Generation (this repo):
- `scripts/generate-evidence.sh` — orchestrator (add A1 step).
- `.github/workflows/evidence.yml` — publish loop ~L170 (add `shadow.json`); real broker already up.
- `.github/scripts/ingest-tracer.mjs` — Tracer POST (optional shadow variant).
- `evidence/README.md` — artifact table + contracts.
- `evidence/conformance/{run.sh,merge.mjs,apis.json}`, `evidence/bench/{run.sh,bench.mjs,assemble.mjs}` —
  patterns to mirror.
- `kafka_test/src/shadow/helpers.rs` (`enable_shadow_mode` SQL), `kafka_test/src/main.rs` (`--json`
  `SuiteResult`, structs ~L316-343, emit ~L1890), `sql/bootstrap.sql` L273-359 (`shadow_config`/
  `shadow_tracking`/`shadow_metrics`/`shadow_status`), `docker-compose.yml` (`external-kafka`, host 9093).

Reporting (site repo `/home/rtj/workspace/RTrentJones.dev`):
- `apps/blog/src/lib/evidence.ts`, `apps/blog/src/components/{BenchTable,ConformanceMatrix,SessionCast}.astro`,
  `apps/blog/src/content/projects/pg_kafka.mdx`, `apps/blog/src/content/blog/pg-kafka-proof.mdx`,
  `apps/blog/verify.config.ts`.
- Tracer: `tools/tracer/lib/schema.ts` (free-form `mode`, no change), `app/api/ingest/route.ts`,
  `app/runs/page.tsx` (Mode column, generic).
- `docs/ci-throughline.md` (cross-repo CI map).
