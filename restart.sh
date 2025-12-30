#!/bin/bash
# Quick restart script for pg_kafka development

set -e

echo "=== Killing all postgres processes ==="
pkill -9 postgres 2>/dev/null || true
sleep 1

echo "=== Cleaning up lock files ==="
rm -f ~/.pgrx/.s.PGSQL.28814* ~/.pgrx/data-14/postmaster.pid 2>/dev/null || true

echo "=== Building extension ==="
cargo build --features pg14

echo "=== Installing extension ==="
cp target/debug/libpg_kafka.so ~/.pgrx/14.20/pgrx-install/lib/pg_kafka.so

echo "=== Starting PostgreSQL ==="
cargo pgrx start pg14

echo "=== Waiting for startup ==="
sleep 3

echo "=== PostgreSQL status ==="
cargo pgrx status pg14

echo ""
echo "✅ Done! You can now run ./test_client"
