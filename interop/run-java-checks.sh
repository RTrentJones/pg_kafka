#!/usr/bin/env bash
# Run the Apache Kafka Java client checks (interop/java_checks.py) against a running pg_kafka.
#
#   KAFKA_HOME=/path/to/kafka_2.13-3.7.0 PGHOST=... PGPORT=... PGUSER=postgres \
#     interop/run-java-checks.sh [--only check,check] [--list]
#
# BOOTSTRAP defaults to 127.0.0.1:9092; SASL_PORT (default 19096) is the SASL_PLAINTEXT listener
# the SASL check adds for the duration of the check (ALTER SYSTEM + worker restart, then reset).
# Exit status 1 when a check that is not marked XFAIL fails.
set -euo pipefail
: "${KAFKA_HOME:?set KAFKA_HOME to an unpacked kafka_2.13-3.7.0}"
command -v java >/dev/null || { echo "java not found" >&2; exit 1; }
command -v javac >/dev/null || { echo "javac not found (a JDK is needed)" >&2; exit 1; }
exec python3 "$(dirname "$0")/java_checks.py" "$@"
