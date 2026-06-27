#!/usr/bin/env bash
# Record the canonical pg_kafka session — kcat -P produces, kcat -C consumes, then a SELECT over
# kafka.messages shows the same row in Postgres — and render it to an animated SVG (svg-term) so the
# site can embed it with zero client JS. Best-effort: if asciinema/svg-term/kcat aren't available it
# warns and leaves no session.svg (the site falls back to its committed placeholder).
# Usage: scripts/record-session.sh <out-dir>
set -uo pipefail
OUT_DIR="${1:-$PWD}"
mkdir -p "$OUT_DIR"
BROKER="${PG_KAFKA_BROKER:-localhost:9092}"
CAST="$(mktemp --suffix=.cast)"
INNER="$(mktemp --suffix=.sh)"

cat > "$INNER" <<INNER_EOF
set -e
run() { printf '\$ %s\n' "\$*"; eval "\$*"; echo; sleep 1; }
echo "# pg_kafka — produce over the Kafka wire protocol, read it back as SQL"; echo; sleep 1
run 'echo "key1:value1" | kcat -P -b $BROKER -t demo -K:'
run 'kcat -C -b $BROKER -t demo -o beginning -e'
run "psql -c 'SELECT key, value FROM kafka.messages ORDER BY partition_offset;'"
echo "# the broker and the table are the same data."; sleep 2
INNER_EOF
chmod +x "$INNER"

if ! command -v asciinema >/dev/null 2>&1; then
  echo "::warning::asciinema not installed — skipping session recording"
  exit 0
fi
timeout -k 10 120 asciinema rec --overwrite -c "bash $INNER" "$CAST" \
  || { echo "::warning::asciinema rec failed/timed out"; exit 0; }

if command -v svg-term >/dev/null 2>&1 && [ -s "$CAST" ]; then
  svg-term --in "$CAST" --out "$OUT_DIR/session.svg" --window --width 92 --height 22 \
    || echo "::warning::svg-term conversion failed"
else
  echo "::warning::svg-term not installed (or empty cast) — no session.svg produced"
fi

[ -s "$OUT_DIR/session.svg" ] && echo "wrote $OUT_DIR/session.svg" || echo "::warning::session.svg not produced"
exit 0
