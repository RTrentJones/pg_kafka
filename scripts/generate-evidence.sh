#!/usr/bin/env bash
# Orchestrate the three evidence artifacts into <out-dir>: conformance.json, bench.json, session.svg.
# This is the single entrypoint the CI evidence workflow calls once the environment is up. Assumes
# (the workflow sets these up):
#   - pg_kafka's Kafka listener on $PG_KAFKA_BROKER (default localhost:9092)
#   - a real broker on $REAL_BROKER               (default localhost:9093)
#   - libpq env (PGHOST/PGPORT/PGUSER/PGDATABASE) pointing at the same Postgres (for psql + the
#     raw-INSERT baseline + the recorded SELECT)
# Each step is best-effort so a single failure during a shakeout still yields the other artifacts.
# Usage: scripts/generate-evidence.sh <out-dir>
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${1:?usage: generate-evidence.sh <out-dir>}"
mkdir -p "$OUT_DIR"

# Stamp the artifacts (read by merge.mjs / assemble.mjs).
export GENERATED_AT="${GENERATED_AT:-$(date -u +%Y-%m-%dT%H:%M:%SZ)}"
export GIT_SHA="${GIT_SHA:-$(git -C "$ROOT" rev-parse --short HEAD 2>/dev/null || echo unknown)}"
export VERSION="${VERSION:-$(git -C "$ROOT" describe --tags --always 2>/dev/null || echo main)}"
export PG_KAFKA_BROKER="${PG_KAFKA_BROKER:-localhost:9092}"
export REAL_BROKER="${REAL_BROKER:-localhost:9093}"

echo "### conformance ($GENERATED_AT @ $GIT_SHA)"
bash "$ROOT/evidence/conformance/run.sh" "$OUT_DIR" || echo "::warning::conformance step failed"

echo "### benchmark"
bash "$ROOT/evidence/bench/run.sh" "$OUT_DIR" || echo "::warning::benchmark step failed"

echo "### recording"
bash "$ROOT/scripts/record-session.sh" "$OUT_DIR" || echo "::warning::recording step failed"

echo "### artifacts in $OUT_DIR"
ls -la "$OUT_DIR"
