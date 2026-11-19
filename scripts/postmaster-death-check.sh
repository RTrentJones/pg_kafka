#!/usr/bin/env bash
# Check that the pg_kafka listener worker does not outlive its postmaster (wave 3).
#
# Against a running cluster with pg_kafka installed and its listener up, this
#   1. finds the postmaster (from $PGDATA/postmaster.pid) and the pg_kafka_listener pid;
#   2. kill -9's the postmaster (simulating a crash or an OOM kill);
#   3. fails unless the listener process is gone within MAX_EXIT_SECS (default 2);
#   4. waits for every remaining process of the cluster to exit, starts the cluster
#      again with pg_ctl, and fails unless it comes up and the Kafka port answers.
#
# Before the fix the worker never noticed: it kept serving (and committing) Kafka
# requests as an orphan, and the new postmaster refused to start because the old
# shared memory segment was still attached.
#
# DESTRUCTIVE for the target cluster (it is crashed and restarted). Use a scratch
# cluster. Usage (as root or the cluster owner):
#   PGDATA=/path/to/data PG_CTL=/usr/lib/postgresql/17/bin/pg_ctl PGLOG=/path/server.log \
#   PGHOST=127.0.0.1 PGPORT=28814 PGUSER=postgres KAFKA_PORT=9092 scripts/postmaster-death-check.sh
set -euo pipefail

: "${PGDATA:?set PGDATA}"
: "${PG_CTL:?set PG_CTL (path to pg_ctl)}"
PGLOG="${PGLOG:-$PGDATA/server.log}"
KAFKA_PORT="${KAFKA_PORT:-9092}"
MAX_EXIT_SECS="${MAX_EXIT_SECS:-2}"
RUN_AS="${RUN_AS:-$(stat -c %U "$PGDATA")}"
PSQL=(psql -X -q -At -v ON_ERROR_STOP=1 -d postgres)

as_owner() {
  if [ "$(id -un)" = "$RUN_AS" ]; then "$@"; else su "$RUN_AS" -c "$(printf '%q ' "$@")"; fi
}

pm_pid="$(head -1 "$PGDATA/postmaster.pid")"
listener="$("${PSQL[@]}" -c "SELECT pid FROM pg_stat_activity WHERE backend_type = 'pg_kafka_listener'")"
if [ -z "$listener" ]; then
  echo "ERROR: no pg_kafka_listener is running" >&2
  exit 1
fi
echo "postmaster $pm_pid, pg_kafka_listener $listener; killing the postmaster with SIGKILL"
kill -9 "$pm_pid"
start=$(date +%s.%N)

gone=""
while :; do
  now=$(date +%s.%N)
  elapsed=$(echo "$now - $start" | bc)
  # Gone, or a zombie waiting to be reaped by its new parent (it has exited).
  if ! kill -0 "$listener" 2>/dev/null || grep -q '^State:[[:space:]]*Z' "/proc/$listener/status" 2>/dev/null; then
    gone="$elapsed"
    break
  fi
  if (( $(echo "$elapsed > 10" | bc) )); then
    break
  fi
  sleep 0.05
done

status=0
if [ -z "$gone" ]; then
  echo "FAIL: pg_kafka_listener $listener still alive 10 s after the postmaster died"
  kill -9 "$listener" 2>/dev/null || true
  status=1
elif (( $(echo "$gone > $MAX_EXIT_SECS" | bc) )); then
  echo "FAIL: pg_kafka_listener exited only after ${gone}s (limit ${MAX_EXIT_SECS}s)"
  status=1
else
  echo "OK: pg_kafka_listener exited ${gone}s after the postmaster died"
fi

# Wait for the rest of the cluster's processes (they detect the death too).
for _ in $(seq 1 100); do
  if ! pgrep -f -- "-D $PGDATA" >/dev/null && ! pgrep -P "$pm_pid" >/dev/null; then break; fi
  sleep 0.1
done
for p in $(pgrep -u "$RUN_AS" -f "postgres" || true); do
  # Only processes still attached to this data directory's shared memory matter:
  # postgres children have no -D of their own, so check their cwd.
  if [ "$(readlink "/proc/$p/cwd" 2>/dev/null || true)" = "$(readlink -f "$PGDATA")" ]; then
    echo "  leftover process $p ($(tr '\0' ' ' < /proc/$p/cmdline 2>/dev/null | cut -c1-60))"
  fi
done

echo "restarting the cluster"
# The killed postmaster may linger as a zombie until its new parent reaps it, and
# Postgres refuses to start while that pid exists: retry for up to 30 s.
started=""
for _ in $(seq 1 30); do
  if as_owner "$PG_CTL" -D "$PGDATA" -l "$PGLOG" -w -t 60 start >/dev/null 2>&1; then
    started=1
    break
  fi
  sleep 1
done
if [ -z "$started" ]; then
  echo "FAIL: the cluster did not restart"
  tail -20 "$PGLOG"
  exit 1
fi
for _ in $(seq 1 60); do
  if (echo > "/dev/tcp/127.0.0.1/$KAFKA_PORT") 2>/dev/null; then
    echo "OK: cluster restarted and the Kafka port answers"
    exit $status
  fi
  sleep 1
done
echo "FAIL: the Kafka port did not come back after the restart"
exit 1
