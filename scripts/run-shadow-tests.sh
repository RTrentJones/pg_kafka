#!/bin/bash
# Shadow Mode Test Runner
#
# This script sets up Docker, PostgreSQL, and runs the shadow mode E2E tests.
# Can be run from inside the devcontainer or from the host.
#
# Usage:
#   ./scripts/run-shadow-tests.sh              # Full setup + run tests
#   ./scripts/run-shadow-tests.sh --setup-only # Just setup, don't run tests
#   ./scripts/run-shadow-tests.sh --test-only  # Just run tests (assumes setup done)
#   ./scripts/run-shadow-tests.sh --test <name> # Run specific test
#   ./scripts/run-shadow-tests.sh --docker-only # Just start Docker containers

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
SETUP_ONLY=false
TEST_ONLY=false
DOCKER_ONLY=false
SPECIFIC_TEST=""
SKIP_DOCKER=false

# Expected Kafka IP (from docker network)
EXPECTED_KAFKA_IP="172.18.0.2"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --setup-only)
            SETUP_ONLY=true
            shift
            ;;
        --test-only)
            TEST_ONLY=true
            shift
            ;;
        --docker-only)
            DOCKER_ONLY=true
            shift
            ;;
        --skip-docker)
            SKIP_DOCKER=true
            shift
            ;;
        --test)
            SPECIFIC_TEST="$2"
            shift 2
            ;;
        -h|--help)
            echo "Shadow Mode Test Runner"
            echo ""
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --setup-only    Only run setup (Docker + PostgreSQL), don't run tests"
            echo "  --test-only     Only run tests, skip all setup"
            echo "  --docker-only   Only start Docker containers"
            echo "  --skip-docker   Skip Docker setup (assume containers running)"
            echo "  --test <name>   Run a specific test by name"
            echo "  -h, --help      Show this help"
            echo ""
            echo "Examples:"
            echo "  $0                           # Full setup + all shadow tests"
            echo "  $0 --test test_dual_write_sync  # Run single test"
            echo "  $0 --setup-only              # Prepare environment only"
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

cd "$PROJECT_ROOT"

echo -e "${GREEN}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║           Shadow Mode Test Runner                          ║${NC}"
echo -e "${GREEN}╚════════════════════════════════════════════════════════════╝${NC}"
echo ""

# Detect environment
detect_environment() {
    if [ -f "/.dockerenv" ] || grep -q docker /proc/1/cgroup 2>/dev/null; then
        echo "devcontainer"
    else
        echo "host"
    fi
}

ENV_TYPE=$(detect_environment)
echo -e "${BLUE}Environment: ${ENV_TYPE}${NC}"
echo ""

# Start Docker containers
start_docker() {
    echo -e "${YELLOW}=== Step 1: Start Docker Containers ===${NC}"

    # Check if docker-compose is available
    if ! command -v docker-compose &> /dev/null && ! command -v docker &> /dev/null; then
        echo -e "${RED}ERROR: docker-compose or docker not found${NC}"
        exit 1
    fi

    # Use docker compose (v2) or docker-compose (v1)
    if docker compose version &> /dev/null; then
        COMPOSE_CMD="docker compose"
    else
        COMPOSE_CMD="docker-compose"
    fi

    # Check if external_kafka is already running
    if docker ps | grep -q external_kafka; then
        echo "external_kafka container is already running"

        # Verify it's healthy
        if docker exec external_kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9093 >/dev/null 2>&1; then
            echo -e "${GREEN}✓ Kafka is healthy${NC}"
        else
            echo -e "${YELLOW}Kafka not responding, recreating...${NC}"
            $COMPOSE_CMD up -d --force-recreate external-kafka
        fi
    else
        echo "Starting external_kafka container..."
        $COMPOSE_CMD up -d external-kafka
    fi

    # Wait for Kafka to be healthy (two-phase check)
    echo "Waiting for Kafka to be healthy..."

    # Phase 1: Wait for broker API to be available
    for i in {1..60}; do
        if docker exec external_kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9093 >/dev/null 2>&1; then
            echo -e "${GREEN}✓ Kafka broker API available${NC}"
            break
        fi
        if [ $i -eq 60 ]; then
            echo -e "${RED}ERROR: Kafka did not become healthy in time${NC}"
            echo "Check logs with: docker logs external_kafka"
            exit 1
        fi
        echo -n "."
        sleep 1
    done

    # Phase 2: Verify Kafka can handle topic metadata requests
    # This ensures Kafka is fully initialized and can accept producer connections
    echo "Verifying Kafka can handle metadata requests..."
    for i in {1..30}; do
        if docker exec external_kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9093 --list >/dev/null 2>&1; then
            echo -e "${GREEN}✓ Kafka metadata requests working${NC}"
            break
        fi
        if [ $i -eq 30 ]; then
            echo -e "${YELLOW}WARNING: Kafka metadata check timed out, continuing anyway...${NC}"
            break
        fi
        echo -n "."
        sleep 1
    done

    # Phase 3: Verify topic creation capability (critical for fresh Kafka starts)
    # This ensures auto-creation works before tests run
    echo "Verifying topic creation capability..."
    TEST_TOPIC="__pg_kafka_health_check"
    docker exec external_kafka /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server localhost:9093 \
        --create --topic "$TEST_TOPIC" --partitions 1 --replication-factor 1 \
        --if-not-exists >/dev/null 2>&1 || true

    for i in {1..10}; do
        if docker exec external_kafka /opt/kafka/bin/kafka-topics.sh \
            --bootstrap-server localhost:9093 --list 2>/dev/null | grep -q "$TEST_TOPIC"; then
            echo -e "${GREEN}✓ Kafka fully operational (topic creation verified)${NC}"
            break
        fi
        if [ $i -eq 10 ]; then
            echo -e "${YELLOW}WARNING: Topic creation verification timed out${NC}"
        fi
        sleep 1
    done
    echo ""

    # Get and verify Kafka IP
    KAFKA_IP=$(docker inspect external_kafka 2>/dev/null | grep -m1 '"IPAddress"' | grep -oE '[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+' || echo "")
    echo "External Kafka IP: $KAFKA_IP"

    if [ "$KAFKA_IP" != "$EXPECTED_KAFKA_IP" ]; then
        echo -e "${YELLOW}WARNING: Kafka IP ($KAFKA_IP) differs from expected ($EXPECTED_KAFKA_IP)${NC}"
        echo "The CONTAINER advertised listener in docker-compose.yml may need updating."
        echo "Current setting uses $EXPECTED_KAFKA_IP - update if your network assigns different IPs."
    fi

    # Export for use by other functions
    export KAFKA_IP
    echo ""
}

# Build and configure PostgreSQL
setup_postgresql() {
    echo -e "${YELLOW}=== Step 2: Build pg_kafka Extension ===${NC}"

    # Build the extension
    echo "Building pg_kafka extension..."
    cargo build --features pg14
    echo -e "${GREEN}✓ Build complete${NC}"
    echo ""

    echo -e "${YELLOW}=== Step 3: Restart PostgreSQL ===${NC}"

    # Use restart.sh which handles stopping, starting, and schema creation
    ./scripts/restart.sh
    echo -e "${GREEN}✓ PostgreSQL restarted${NC}"
    echo ""

    echo -e "${YELLOW}=== Step 4: Configure PostgreSQL GUCs ===${NC}"

    # Find psql
    PSQL=""
    if [ -f "$HOME/.pgrx/14.20/pgrx-install/bin/psql" ]; then
        PSQL="$HOME/.pgrx/14.20/pgrx-install/bin/psql"
    elif command -v psql &> /dev/null; then
        PSQL="psql"
    else
        echo -e "${RED}ERROR: psql not found${NC}"
        exit 1
    fi

    PSQL_CMD="$PSQL -h localhost -p 28814 -U postgres -d postgres -q"

    # Get Kafka IP if not set
    if [ -z "$KAFKA_IP" ]; then
        KAFKA_IP=$(docker inspect external_kafka 2>/dev/null | grep -m1 '"IPAddress"' | grep -oE '[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+' || echo "$EXPECTED_KAFKA_IP")
    fi

    echo "Configuring shadow mode settings..."

    # Core shadow mode settings
    $PSQL_CMD -c "ALTER SYSTEM SET pg_kafka.shadow_mode_enabled = 'true';"
    $PSQL_CMD -c "ALTER SYSTEM SET pg_kafka.shadow_bootstrap_servers = '${KAFKA_IP}:9095';"
    $PSQL_CMD -c "ALTER SYSTEM SET pg_kafka.shadow_security_protocol = 'PLAINTEXT';"
    $PSQL_CMD -c "ALTER SYSTEM SET pg_kafka.shadow_max_retries = '20';"
    $PSQL_CMD -c "ALTER SYSTEM SET pg_kafka.shadow_retry_backoff_ms = '500';"

    # Config reload settings (fast reload for tests)
    # Note: Only config_reload_interval_ms exists; shadow_config_reload_interval_ms was removed
    $PSQL_CMD -c "ALTER SYSTEM SET pg_kafka.config_reload_interval_ms = '2000';"

    # Reload configuration - this sends SIGHUP to the worker which triggers
    # immediate config reload, picking up the new GUC values
    $PSQL_CMD -c "SELECT pg_reload_conf();"

    # Wait for SIGHUP to be processed by the background worker
    # The worker checks every 100ms, so 1 second is plenty
    echo "⏳ Waiting for SIGHUP processing (1s)..."
    sleep 1
    echo "✅ Config reload interval now set to 2s"

    echo ""
    echo "Current shadow mode configuration:"
    echo "─────────────────────────────────────────"
    $PSQL -h localhost -p 28814 -U postgres -d postgres -c "
        SELECT name, setting
        FROM pg_settings
        WHERE name LIKE 'pg_kafka.shadow%'
        ORDER BY name;
    "

    echo -e "${GREEN}✓ PostgreSQL configured${NC}"
    echo ""
}

# Build test client
build_tests() {
    echo -e "${YELLOW}=== Step 5: Build Test Client ===${NC}"

    cd "$PROJECT_ROOT/kafka_test"

    if [ ! -f "target/release/kafka_test" ] || [ "$PROJECT_ROOT/kafka_test/src" -nt "target/release/kafka_test" ]; then
        echo "Building test client..."
        cargo build --release
        echo -e "${GREEN}✓ Test client built${NC}"
    else
        echo -e "${GREEN}✓ Test client up to date${NC}"
    fi

    cd "$PROJECT_ROOT"
    echo ""
}

# Run tests
run_tests() {
    echo -e "${YELLOW}=== Running Shadow Mode Tests ===${NC}"
    echo ""

    cd "$PROJECT_ROOT/kafka_test"

    # Run tests
    if [ -n "$SPECIFIC_TEST" ]; then
        echo "Running specific test: $SPECIFIC_TEST"
        echo "─────────────────────────────────────────"
        ./target/release/kafka_test --test "$SPECIFIC_TEST"
    else
        echo "Running all shadow tests..."
        echo "─────────────────────────────────────────"
        ./target/release/kafka_test --category shadow
    fi

    cd "$PROJECT_ROOT"
}

# Full setup function
full_setup() {
    if [ "$SKIP_DOCKER" != true ]; then
        start_docker
    fi
    setup_postgresql
    build_tests
    echo -e "${GREEN}=== Setup Complete ===${NC}"
    echo ""
    echo "To run tests manually:"
    echo "  ./kafka_test/target/release/kafka_test --category shadow"
    echo "  ./kafka_test/target/release/kafka_test --test test_dual_write_sync"
}

# Main logic
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

echo ""
echo -e "${GREEN}Done!${NC}"
