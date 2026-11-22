#!/usr/bin/env bash
# Run the shadow-mode E2E tests against the pgrx-managed PostgreSQL 14 and docker-compose.yml's
# external-kafka broker. Works from the devcontainer or the host.
#
#   scripts/run-shadow-tests.sh                # start Kafka, rebuild + restart, run the tests
#   scripts/run-shadow-tests.sh --setup-only   # everything but the tests
#   scripts/run-shadow-tests.sh --test-only    # only (re)build the test client and run the tests
#   scripts/run-shadow-tests.sh --docker-only  # only start external-kafka
#   scripts/run-shadow-tests.sh --skip-docker  # assume external-kafka is already up
#   scripts/run-shadow-tests.sh --test <name>  # run one test instead of the shadow category
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

SETUP_ONLY=false
TEST_ONLY=false
DOCKER_ONLY=false
SKIP_DOCKER=false
SPECIFIC_TEST=""

# docker-compose.yml pins external-kafka to this address on pg_kafka_network and advertises it
# on the CONTAINER listener (9095), which pg_kafka forwards to.
EXPECTED_KAFKA_IP="172.18.0.2"
PG_PORT=28814 # pgrx runs PostgreSQL 14 on 28800 + 14

usage() {
  sed -n '2,10s/^# \{0,1\}//p' "$0"
}

while [[ $# -gt 0 ]]; do
  case $1 in
    --setup-only) SETUP_ONLY=true; shift ;;
    --test-only) TEST_ONLY=true; shift ;;
    --docker-only) DOCKER_ONLY=true; shift ;;
    --skip-docker) SKIP_DOCKER=true; shift ;;
    --test) SPECIFIC_TEST="${2:?--test needs a test name}"; shift 2 ;;
    -h | --help) usage; exit 0 ;;
    *) echo -e "${RED}Unknown option: $1${NC} (see --help)" >&2; exit 1 ;;
  esac
done

kafka_cli() {
  docker exec external_kafka "/opt/kafka/bin/$1" --bootstrap-server localhost:9093 "${@:2}"
}

kafka_ip() {
  docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' external_kafka \
    2>/dev/null || true
}

# Retry "$@" once a second up to $1 times; returns 1 if it never succeeds.
retry() {
  local tries=$1
  shift
  for ((i = 1; i <= tries; i++)); do
    if "$@" >/dev/null 2>&1; then return 0; fi
    echo -n "."
    sleep 1
  done
  return 1
}

start_docker() {
  echo -e "${YELLOW}=== Start external-kafka ===${NC}"
  local compose=(docker compose)
  if ! docker compose version >/dev/null 2>&1; then compose=(docker-compose); fi

  if docker ps --format '{{.Names}}' | grep -qx external_kafka \
    && ! kafka_cli kafka-broker-api-versions.sh >/dev/null 2>&1; then
    echo -e "${YELLOW}external_kafka is not responding, recreating it${NC}"
    "${compose[@]}" up -d --force-recreate external-kafka
  else
    "${compose[@]}" up -d external-kafka
  fi

  echo "Waiting for the broker API..."
  if ! retry 60 kafka_cli kafka-broker-api-versions.sh; then
    echo -e "${RED}ERROR: Kafka did not become healthy (docker logs external_kafka)${NC}" >&2
    exit 1
  fi
  # A fresh broker answers ApiVersions before it can serve metadata and create topics.
  echo "Waiting for topic creation to work..."
  kafka_cli kafka-topics.sh --create --topic __pg_kafka_health_check --partitions 1 \
    --replication-factor 1 --if-not-exists >/dev/null 2>&1 || true
  if ! retry 30 bash -c "docker exec external_kafka /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server localhost:9093 --list | grep -qx __pg_kafka_health_check"; then
    echo -e "${YELLOW}WARNING: topic creation not confirmed, continuing${NC}"
  fi
  echo -e "\n${GREEN}✓ Kafka is up${NC}"

  local ip
  ip="$(kafka_ip)"
  if [ "$ip" != "$EXPECTED_KAFKA_IP" ]; then
    echo -e "${YELLOW}WARNING: external_kafka has IP '$ip', not $EXPECTED_KAFKA_IP${NC}"
    echo "Update the CONTAINER advertised listener and the subnet in docker-compose.yml together."
  fi
}

setup_postgresql() {
  echo -e "${YELLOW}=== Rebuild, install and restart PostgreSQL 14 ===${NC}"
  PG_MAJOR=14 ./scripts/restart.sh

  echo -e "${YELLOW}=== Configure shadow mode ===${NC}"
  local psql
  psql="$("$(cargo pgrx info pg-config pg14)" --bindir)/psql"
  local psql_cmd=("$psql" -X -q -v ON_ERROR_STOP=1 -h localhost -p "$PG_PORT" -U postgres -d postgres)
  local ip
  ip="$(kafka_ip)"
  ip="${ip:-$EXPECTED_KAFKA_IP}"

  "${psql_cmd[@]}" \
    -c "ALTER SYSTEM SET pg_kafka.shadow_mode_enabled = 'true'" \
    -c "ALTER SYSTEM SET pg_kafka.shadow_bootstrap_servers = '${ip}:9095'" \
    -c "ALTER SYSTEM SET pg_kafka.shadow_security_protocol = 'PLAINTEXT'" \
    -c "ALTER SYSTEM SET pg_kafka.shadow_max_retries = '20'" \
    -c "ALTER SYSTEM SET pg_kafka.shadow_retry_backoff_ms = '500'" \
    -c "ALTER SYSTEM SET pg_kafka.config_reload_interval_ms = '2000'" \
    -c "SELECT pg_reload_conf()"
  # pg_reload_conf() signals the worker, which reloads its configuration on its next loop turn.
  sleep 1
  "${psql_cmd[@]}" -c "SELECT name, setting FROM pg_settings WHERE name LIKE 'pg_kafka.shadow%' ORDER BY name"
}

build_tests() {
  echo -e "${YELLOW}=== Build the test client ===${NC}"
  (cd kafka_test && cargo build --release)
}

run_tests() {
  echo -e "${YELLOW}=== Run the shadow tests ===${NC}"
  if [ -n "$SPECIFIC_TEST" ]; then
    ./kafka_test/target/release/kafka_test --test "$SPECIFIC_TEST"
  else
    ./kafka_test/target/release/kafka_test --category shadow
  fi
}

full_setup() {
  if [ "$SKIP_DOCKER" != true ]; then start_docker; fi
  setup_postgresql
  build_tests
  echo -e "${GREEN}=== Setup complete ===${NC}"
  echo "Run the tests with: ./kafka_test/target/release/kafka_test --category shadow"
}

if [ "$DOCKER_ONLY" = true ]; then
  start_docker
elif [ "$TEST_ONLY" = true ]; then
  build_tests
  run_tests
elif [ "$SETUP_ONLY" = true ]; then
  full_setup
else
  full_setup
  run_tests
fi
echo -e "${GREEN}Done${NC}"
