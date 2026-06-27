#!/usr/bin/env bash
# Record the canonical pg_kafka session — kcat -P produces, kcat -C consumes, then a SELECT over
# kafka.messages shows the same row in Postgres — and render it to an animated SVG (svg-term) so the
# site can embed it with zero client JS. Best-effort: warns and leaves no session.svg if the tools
# aren't available (the site falls back to its committed placeholder).
# Usage: scripts/record-session.sh <out-dir>
set -uo pipefail
OUT_DIR="${1:-$PWD}"
mkdir -p "$OUT_DIR"
# Force IPv4: pg_kafka binds 0.0.0.0:9092, but kcat/librdkafka resolve "localhost" to ::1 first and
# get connection-refused — which aborted the recording after the first command.
BROKER="${PG_KAFKA_BROKER:-127.0.0.1:9092}"
BROKER="${BROKER/localhost/127.0.0.1}"
export BROKER
# psql opens its pager (less) when stdout is a tty — inside asciinema's pty that hangs forever, which
# truncated the recording right at the SELECT. Disable it; force psql onto IPv4 too.
export PAGER=cat PSQL_PAGER=cat
[ -n "${PGHOST:-}" ] && export PGHOST="${PGHOST/localhost/127.0.0.1}"
CAST="$(mktemp --suffix=.cast)"
INNER="$(mktemp --suffix=.sh)"
# The benchmark fills kafka.messages with 1KB payloads first, so filter to the demo key and decode
# the bytea — that's the "same row, now as SQL" punchline. Kept in a file to dodge nested quoting.
SQLFILE="$(mktemp --suffix=.sql)"
cat > "$SQLFILE" <<'SQL'
SELECT convert_from(key, 'UTF8') AS key, convert_from(value, 'UTF8') AS value
FROM kafka.messages
WHERE key = convert_to('key1', 'UTF8');
SQL

# Escaped heredoc: \$* / \$BROKER stay literal for runtime. No `set -e` — one hiccup shouldn't abort
# the narrative, and the SELECT proves the produced message is sitting in Postgres regardless.
cat > "$INNER" <<INNER_EOF
run() { printf '\$ %s\n' "\$*"; eval "\$* 2>&1"; echo; sleep 1.6; }
echo "# pg_kafka — produce over the Kafka wire protocol, read it back as SQL"; echo; sleep 1.6
run "echo 'key1:value1' | kcat -P -b $BROKER -t demo -K:"
sleep 0.8
run "kcat -C -b $BROKER -t demo -o beginning -c1 -e"
run "psql -P pager=off -f $SQLFILE"
echo "# the broker and the table are the same data."; sleep 2.5
INNER_EOF
chmod +x "$INNER"

if ! command -v asciinema >/dev/null 2>&1; then
  echo "::warning::asciinema not installed — skipping session recording"
  exit 0
fi
# Bound the recorded command (not asciinema itself — wrapping asciinema in `timeout` strips its pty).
asciinema rec --overwrite -c "timeout -k 5 90 bash $INNER" "$CAST" \
  || { echo "::warning::asciinema rec failed"; exit 0; }

if command -v svg-term >/dev/null 2>&1 && [ -s "$CAST" ]; then
  svg-term --in "$CAST" --out "$OUT_DIR/session.svg" --window --width 92 --height 22 \
    || echo "::warning::svg-term conversion failed"
else
  echo "::warning::svg-term not installed (or empty cast) — no session.svg produced"
fi

[ -s "$OUT_DIR/session.svg" ] && echo "wrote $OUT_DIR/session.svg" || echo "::warning::session.svg not produced"
exit 0
