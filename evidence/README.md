# Evidence harnesses

pg_kafka is an *implementation*, not a hosted service — so the way to show it's "live" is
**continuously-verified evidence**, regenerated in CI and published as data, not a running endpoint.
This directory holds the harnesses that produce that evidence; [`.github/workflows/evidence.yml`](../.github/workflows/evidence.yml)
runs them (it brings the extension up the same way `ci.yml`'s E2E job does — pgrx on the runner plus
the real-broker compose).

The artifacts are force-pushed to the orphan **`evidence`** branch and consumed by the site
([rtrentjones.dev/pg_kafka](https://rtrentjones.dev/pg_kafka)) at build time:

| Artifact | Produced by | Rendered as |
| --- | --- | --- |
| `conformance.json` | `evidence/conformance/` | the client × API green/red/na grid |
| `bench.json` | `evidence/bench/` | the throughput/latency table (+ a Tracer trend) |
| `session.svg` | `scripts/record-session.sh` | the animated `kcat`/`SELECT` session |
| `shadow.json` | `evidence/shadow/` | the shadow-mode forwarding compliance table |

Shadow-mode **throughput** is not a separate artifact — `evidence/bench/` adds two shadow-forwarding
rows (`shadow-async`, `shadow-sync`) directly into `bench.json.scenarios[]`, so they render inline in
the same benchmark table.

## Layout

```
scripts/generate-evidence.sh   # orchestrator → writes the 3 artifacts into <out-dir>
scripts/record-session.sh      # asciinema rec of kcat -P / -C + SELECT → svg-term → session.svg
evidence/
  conformance/
    apis.json                  # the 23 implemented APIs (single source of truth)
    kafkajs/   conformance.mjs  # Node client
    python/    kafka_python_conformance.py   # pure-Python client
               confluent_conformance.py      # librdkafka (via confluent-kafka)
    sarama/    conformance.go   # Go client
    merge.mjs                  # results-*.json → conformance.json
    run.sh                     # run all client harnesses + merge
  bench/
    bench.mjs                  # Kafka-wire throughput/latency (pg_kafka + real broker; TOPIC/TAG/ONLY knobs)
    raw_insert.mjs             # raw-INSERT floor (libpq)
    assemble.mjs               # → bench.json (incl. the two shadow-forwarding rows)
    run.sh                     # run all three + the two shadow runs + assemble
  shadow/
    shadow.mjs                 # shadow-forwarding compliance (produce → pg_kafka, confirm on real broker)
    assemble.mjs               # → shadow.json
    run.sh                     # run the harness + assemble
.github/scripts/ingest-tracer.mjs   # normalize bench.json → Tracer /api/ingest
```

## How a client maps to APIs

Each harness drives the client through real operations and records, per API, `pass | fail | na`:
- every probe is independently error-trapped, so one failing API never aborts the run — it just
  colours that one cell;
- some wire APIs are *inferred* from the higher-level op that drives them (a working group consume
  implies `FindCoordinator` / `SyncGroup` / `Heartbeat`);
- `na` means the client doesn't exercise that API at all — e.g. `kafka-python` implements no
  transactions, so the five txn APIs are `na` by design (an honest gap, not a pg_kafka failure).

## Contract

`conformance.json`
```jsonc
{
  "generatedAt": "<ISO>", "version": "<git describe>", "gitSha": "<short sha>",
  "apis": [{ "key": 0, "name": "Produce" } /* …23… */],
  "clients": [{ "name": "kafkajs", "lang": "Node", "version": "2.2.4",
               "results": { "Produce": "pass", "AddPartitionsToTxn": "na" /* … */ } }]
}
```

`bench.json`
```jsonc
{
  "generatedAt": "<ISO>", "version": "…", "gitSha": "…",
  "config": { "warmupExcluded": true, "messageSizes": [1024] },
  "scenarios": [{ "name": "produce · batched(100) · 1024B", "p50Ms": 0, "p99Ms": 0, "msgsPerSec": 0 }],
  "baselines": { "rawInsert": { "msgsPerSec": 0, "p50Ms": 0, "p99Ms": 0 },
                 "realBroker": { "msgsPerSec": 0, "p50Ms": 0, "p99Ms": 0 } }
}
```

`shadow.json` — one `checks[]` row per forwarding scenario (`pass|fail|na`), a `counts` parity block for
the headline 100% dual-write topic, and `passed` (all checks pass **and** `forwardedToBroker == produced`).
Each scenario produces to pg_kafka, drives the live forwarding path, then independently consumes the real
broker to confirm delivery.
```jsonc
{
  "generatedAt": "<ISO>", "version": "…", "gitSha": "…",
  "config": { "records": 500, "forwardPercentage": 100, "writeMode": "dual_write", "syncMode": "sync",
              "realBroker": "localhost:9093" },
  "checks": [{ "name": "dual_write_sync", "status": "pass" }, { "name": "aborted_txn_not_forwarded", "status": "pass" } /* …6… */],
  "counts": { "produced": 500, "forwardedToBroker": 500, "localStored": 500, "shadowMetricsForwarded": 500,
              "skipped": 0, "failed": 0, "outboxFinalized": 500, "lag": 0 },
  "passed": true
}
```

## Run it locally

With the extension up on `:9092` (`cargo pgrx run pg14`), a real broker on `:9093`
(`docker compose up -d external-kafka`), and libpq env pointing at the same Postgres:

```bash
scripts/generate-evidence.sh ./evidence-out
```

> **Status:** these harnesses were authored to the contract above and need a first CI run to shake
> out client/version specifics — the matrix reports `fail`/`na` truthfully wherever a probe doesn't
> succeed, which is the point. The pipeline is resilient: any harness that fails to build or run
> degrades to an all-`na` row (conformance) or zeros (bench) rather than breaking the artifact.
