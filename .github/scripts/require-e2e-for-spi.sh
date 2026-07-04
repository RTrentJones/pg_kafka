#!/usr/bin/env bash
# QA-1 enforcement (see codecov.yml and CLAUDE.md).
#
# The files matched by SPI_REGEX are excluded from unit coverage because they
# require a live PostgreSQL (SPI) or drive the tokio listener, so `cargo llvm-cov`
# always reads 0% for them. The agreed mitigation is that every *behavioural*
# change to one of them must ship a fail-before / pass-after regression test in
# kafka_test/. This script makes that mitigation a machine check instead of prose.
#
# Usage: pipe the PR's changed file paths (one per line) on stdin.
#   gh pr view "$PR_NUMBER" --json files --jq '.files[].path' | bash "$0"
# Env:
#   PR_LABELS  comma-separated PR label names; 'no-e2e-needed' waives the check
#              (for genuinely test-neutral edits: comments/docs/renames).
#
# Keep SPI_REGEX in sync with the SPI/async entries in codecov.yml `ignore:`.
set -euo pipefail

SPI_REGEX='^(src/worker\.rs|src/kafka/storage/postgres\.rs|src/kafka/shadow/store\.rs|src/kafka/listener\.rs)$'

changed="$(cat)"
echo "Changed files:"
if [ -n "$changed" ]; then echo "$changed" | sed 's/^/  /'; else echo "  (none)"; fi

spi_hits="$(echo "$changed" | grep -E "$SPI_REGEX" || true)"
if [ -z "$spi_hits" ]; then
  echo "No coverage-excluded SPI/async files changed — nothing to enforce."
  exit 0
fi

echo "Coverage-excluded SPI/async file(s) changed:"
echo "$spi_hits" | sed 's/^/  /'

if echo "${PR_LABELS:-}" | tr ',' '\n' | grep -qx 'no-e2e-needed'; then
  echo "::notice::'no-e2e-needed' label present — E2E-test requirement waived."
  exit 0
fi

if echo "$changed" | grep -q '^kafka_test/'; then
  echo "kafka_test/ was also updated — requirement satisfied."
  exit 0
fi

echo "::error::A coverage-excluded SPI/async file changed with no kafka_test/ E2E regression test."
cat >&2 <<'MSG'
QA-1 (see codecov.yml and CLAUDE.md): worker.rs, storage/postgres.rs,
shadow/store.rs and listener.rs are excluded from unit coverage because they
require a live PostgreSQL / the tokio listener. Every behavioural change to them
must ship a fail-before / pass-after kafka_test regression test.

Fix: add or update a test under kafka_test/ that exercises this change.
If the change is genuinely test-neutral (comments/docs/renames only), add the
'no-e2e-needed' label to the PR.
MSG
exit 1
