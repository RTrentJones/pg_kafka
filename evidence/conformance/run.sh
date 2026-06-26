#!/usr/bin/env bash
# Run every client conformance harness against pg_kafka, then merge → <out-dir>/conformance.json.
# Each harness is best-effort: a failing one leaves its results-*.json absent and merge emits an
# all-na row, so the matrix always renders. Assumes pg_kafka's Kafka listener is up on
# $PG_KAFKA_BROKER (default localhost:9092).
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT_DIR="${1:-$PWD}"
WORK="$(mktemp -d)"
export PG_KAFKA_BROKER="${PG_KAFKA_BROKER:-localhost:9092}"
export APIS_JSON="$HERE/apis.json"
mkdir -p "$OUT_DIR"

echo "== kafkajs =="
( cd "$HERE/kafkajs" && npm install --no-audit --no-fund --loglevel=error \
  && OUT="$WORK/results-kafkajs.json" node conformance.mjs ) || echo "::warning::kafkajs harness failed"

echo "== python clients (kafka-python + librdkafka) =="
python3 -m pip install --quiet --disable-pip-version-check -r "$HERE/python/requirements.txt" \
  || echo "::warning::pip install failed"
( cd "$HERE/python" && OUT="$WORK/results-kafka-python.json" python3 kafka_python_conformance.py ) \
  || echo "::warning::kafka-python harness failed"
( cd "$HERE/python" && OUT="$WORK/results-librdkafka.json" python3 confluent_conformance.py ) \
  || echo "::warning::librdkafka harness failed"

echo "== sarama =="
( cd "$HERE/sarama" && go mod tidy && OUT="$WORK/results-sarama.json" go run . ) \
  || echo "::warning::sarama harness failed"

node "$HERE/merge.mjs" "$WORK" "$OUT_DIR/conformance.json"
