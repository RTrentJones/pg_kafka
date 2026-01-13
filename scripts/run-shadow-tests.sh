#!/bin/bash
# Shadow Mode Test Runner
#
# This script sets up and runs the shadow mode E2E tests.
# Run from inside the devcontainer.
#
# Usage:
#   ./scripts/run-shadow-tests.sh              # Full setup + run tests
#   ./scripts/run-shadow-tests.sh --setup-only # Just setup
#   ./scripts/run-shadow-tests.sh --test-only  # Just run tests
#   ./scripts/run-shadow-tests.sh --test <name> # Run specific test

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Default values
SETUP_ONLY=false
TEST_ONLY=false
SPECIFIC_TEST=""

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
        --test)
            SPECIFIC_TEST="$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --setup-only    Only run setup, don't run tests"
            echo "  --test-only     Only run tests, skip setup"
            echo "  --test <name>   Run a specific test"
            echo "  -h, --help      Show this help"
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            exit 1
            ;;
    esac
done

cd "$PROJECT_ROOT"

echo -e "${GREEN}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║           Shadow Mode Test Runner                          ║${NC}"
echo -e "${GREEN}╚════════════════════════════════════════════════════════════╝${NC}"
echo ""

# Setup function
setup() {
    echo -e "${YELLOW}=== Step 1: Verify External Kafka Container ===${NC}"

    # Check if external_kafka is running
    if ! docker ps | grep -q external_kafka; then
        echo -e "${RED}ERROR: external_kafka container is not running${NC}"
        echo "Start it with: docker-compose up -d external-kafka"
        exit 1
    fi

    # Get the actual IP of external_kafka
    KAFKA_IP=$(docker inspect external_kafka 2>/dev/null | grep -m1 '"IPAddress"' | grep -oE '[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+')
    echo "External Kafka IP: $KAFKA_IP"

    # Verify the expected IP
    EXPECTED_IP="172.18.0.2"
    if [ "$KAFKA_IP" != "$EXPECTED_IP" ]; then
        echo -e "${YELLOW}WARNING: Kafka IP ($KAFKA_IP) differs from expected ($EXPECTED_IP)${NC}"
        echo "You may need to update docker-compose.yml CONTAINER advertised listener"
        echo "and run: docker-compose up -d --force-recreate external-kafka"
    fi

    # Wait for Kafka to be healthy
    echo "Waiting for Kafka to be healthy..."
    for i in {1..30}; do
        if docker exec external_kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9093 >/dev/null 2>&1; then
            echo -e "${GREEN}Kafka is healthy${NC}"
            break
        fi
        if [ $i -eq 30 ]; then
            echo -e "${RED}ERROR: Kafka did not become healthy in time${NC}"
            exit 1
        fi
        sleep 1
    done

    echo ""
    echo -e "${YELLOW}=== Step 2: Build and Restart PostgreSQL ===${NC}"

    # Build the extension
    echo "Building pg_kafka extension..."
    cargo build --features pg14

    # Restart PostgreSQL with fresh extension
    echo "Restarting PostgreSQL..."
    ./restart.sh

    echo ""
    echo -e "${YELLOW}=== Step 3: Configure Shadow Mode ===${NC}"

    # Set shadow mode configuration
    PSQL="$HOME/.pgrx/14.20/pgrx-install/bin/psql -h localhost -p 28814 -U postgres -d postgres"

    $PSQL -c "ALTER SYSTEM SET pg_kafka.shadow_mode_enabled = 'true';"
    $PSQL -c "ALTER SYSTEM SET pg_kafka.shadow_bootstrap_servers = '${KAFKA_IP}:9095';"
    $PSQL -c "ALTER SYSTEM SET pg_kafka.shadow_security_protocol = 'PLAINTEXT';"
    $PSQL -c "ALTER SYSTEM SET pg_kafka.shadow_config_reload_interval_ms = '2000';"
    $PSQL -c "SELECT pg_reload_conf();"

    echo "Shadow mode configuration:"
    $PSQL -c "SHOW pg_kafka.shadow_mode_enabled;"
    $PSQL -c "SHOW pg_kafka.shadow_bootstrap_servers;"

    echo ""
    echo -e "${GREEN}=== Setup Complete ===${NC}"
}

# Run tests function
run_tests() {
    echo ""
    echo -e "${YELLOW}=== Running Shadow Mode Tests ===${NC}"

    cd "$PROJECT_ROOT/kafka_test"

    # Build test client if needed
    if [ ! -f "target/release/kafka_test" ]; then
        echo "Building test client..."
        cargo build --release
    fi

    # Run tests
    if [ -n "$SPECIFIC_TEST" ]; then
        echo "Running specific test: $SPECIFIC_TEST"
        ./target/release/kafka_test --test "$SPECIFIC_TEST"
    else
        echo "Running all shadow tests..."
        ./target/release/kafka_test --category shadow
    fi
}

# Main logic
if [ "$TEST_ONLY" = true ]; then
    run_tests
elif [ "$SETUP_ONLY" = true ]; then
    setup
else
    setup
    run_tests
fi

echo ""
echo -e "${GREEN}Done!${NC}"
