#!/usr/bin/env bash
# QA-1 enforcement (see codecov.yml and CLAUDE.md).
#
# The SPI/async files listed in codecov.yml `ignore:` are excluded from unit
# coverage because they require a live PostgreSQL (SPI) or drive the tokio
# listener, so `cargo llvm-cov` always reads 0% for them. The agreed mitigation
# is that every *behavioural* change to one of them must ship a fail-before /
# pass-after regression test in kafka_test/. This script makes that mitigation
# a machine check instead of prose.
#
# DR-19 (DEEP-REVIEW-2026-07) hardening:
#   - The SPI file list is DERIVED from codecov.yml's ignore section at run
#     time, so there is one source of truth instead of a hand-synced regex.
#   - The satisfying change must touch a Rust source file under kafka_test/src/
#     (touching a README or Cargo.lock under kafka_test/ no longer counts).
#
# Usage: pipe the PR's changed file paths (one per line) on stdin.
#   gh pr view "$PR_NUMBER" --json files --jq '.files[].path' | bash "$0"
# Env:
#   PR_LABELS  comma-separated PR label names; 'no-e2e-needed' waives the check
#              (for genuinely test-neutral edits: comments/docs/renames).
set -euo pipefail

CODECOV_YML="${CODECOV_YML:-codecov.yml}"

# Extract the src/ ignore entries from codecov.yml (the SPI/async list).
# Lines look like:   - "src/worker.rs"
spi_files="$(grep -oE '"src/[^"]+"' "$CODECOV_YML" | tr -d '"' || true)"
if [ -z "$spi_files" ]; then
  echo "::error::Could not derive the SPI file list from $CODECOV_YML ignore section."
  exit 1
fi
echo "SPI/async files (derived from $CODECOV_YML):"
echo "$spi_files" | sed 's/^/  /'

changed="$(cat)"
echo "Changed files:"
if [ -n "$changed" ]; then echo "$changed" | sed 's/^/  /'; else echo "  (none)"; fi

spi_hits="$(grep -Fx -f <(echo "$spi_files") <(echo "$changed") || true)"
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

# DR-19: require an actual Rust test-source change, not just any kafka_test/ path.
if echo "$changed" | grep -qE '^kafka_test/src/.*\.rs$'; then
  echo "kafka_test/src/*.rs was also updated — requirement satisfied."
  exit 0
fi

echo "::error::A coverage-excluded SPI/async file changed with no kafka_test/src Rust change."
cat >&2 <<'MSG'
QA-1 (see codecov.yml and CLAUDE.md): the SPI/async files in codecov.yml's
ignore list are excluded from unit coverage because they require a live
PostgreSQL / the tokio listener. Every behavioural change to them must ship a
fail-before / pass-after kafka_test regression test.

Fix: add or update a Rust test under kafka_test/src/ that exercises this change.
If the change is genuinely test-neutral (comments/docs/renames only), add the
'no-e2e-needed' label to the PR.
MSG
exit 1
