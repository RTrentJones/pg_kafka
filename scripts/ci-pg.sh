#!/usr/bin/env bash
# Run a throwaway PostgreSQL cluster with pg_kafka preloaded, the way CI's E2E jobs do.
#
#   scripts/ci-pg.sh start <dir>   initdb in <dir>, preload pg_kafka, start, CREATE EXTENSION,
#                                  wait for the Kafka port; prints the connection settings
#                                  (and appends them to $GITHUB_ENV when set)
#   scripts/ci-pg.sh stop <dir>    stop it
#   scripts/ci-pg.sh log <dir>     print the server log
#
# The extension must already be installed into the PostgreSQL that PG_CONFIG names
# (cargo pgrx install --release --pg-config "$PG_CONFIG"). Run as a non-root user.
# Environment:
#   PG_CONFIG      pg_config of the target PostgreSQL (default: pg_config on PATH)
#   PGPORT         Postgres port (default 28814, what kafka_test defaults to)
#   KAFKA_PORT     pg_kafka.port (default 9092)
#   PG_KAFKA_CONF  extra postgresql.conf lines (optional)
set -euo pipefail

action=${1:?usage: ci-pg.sh start|stop|log <dir>}
dir=${2:?usage: ci-pg.sh start|stop|log <dir>}
PG_CONFIG=${PG_CONFIG:-pg_config}
bindir="$("$PG_CONFIG" --bindir)"
PGPORT=${PGPORT:-28814}
KAFKA_PORT=${KAFKA_PORT:-9092}
data="$dir/data"
log="$dir/server.log"

case "$action" in
  start)
    mkdir -p "$dir"
    if [ ! -f "$data/PG_VERSION" ]; then
      "$bindir/initdb" -D "$data" -U postgres -A trust >/dev/null
      cat >> "$data/postgresql.conf" <<CONF

port = $PGPORT
listen_addresses = '127.0.0.1'
unix_socket_directories = '$dir'
shared_preload_libraries = 'pg_kafka'
pg_kafka.port = $KAFKA_PORT
${PG_KAFKA_CONF:-}
CONF
    fi
    "$bindir/pg_ctl" -D "$data" -l "$log" -w start >/dev/null
    psql=("$bindir/psql" -X -q -v ON_ERROR_STOP=1 -h 127.0.0.1 -p "$PGPORT" -U postgres -d postgres)
    "${psql[@]}" -c "CREATE EXTENSION IF NOT EXISTS pg_kafka"
    # The worker waits for the schema, then binds the port only after full init; requests
    # that arrive before its first loop iteration queue on the request channel.
    for _ in $(seq 1 60); do
      if (echo > "/dev/tcp/127.0.0.1/$KAFKA_PORT") 2>/dev/null; then
        echo "pg_kafka listening on 127.0.0.1:$KAFKA_PORT ($("$PG_CONFIG" --version))"
        settings="PGHOST=127.0.0.1
PGPORT=$PGPORT
PGUSER=postgres
PGDATABASE=postgres
DATABASE_URL=postgresql://postgres@127.0.0.1:$PGPORT/postgres
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:$KAFKA_PORT"
        echo "$settings"
        if [ -n "${GITHUB_ENV:-}" ]; then echo "$settings" >> "$GITHUB_ENV"; fi
        exit 0
      fi
      sleep 1
    done
    echo "pg_kafka did not open port $KAFKA_PORT within 60 s" >&2
    tail -50 "$log" >&2
    exit 1
    ;;
  stop)
    "$bindir/pg_ctl" -D "$data" -m fast -w stop || true
    ;;
  log)
    cat "$log" 2>/dev/null || echo "no server log at $log"
    ;;
  *)
    echo "unknown action $action" >&2
    exit 2
    ;;
esac
