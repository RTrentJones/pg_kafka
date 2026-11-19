#!/usr/bin/env bash
# Install the pg_kafka git hooks into this clone's .git/hooks.
#
#   pre-commit  cargo fmt --check (both crates; seconds)
#   pre-push    cargo clippy --all-targets -D warnings (both crates; minutes)
#
# Usage: ./hooks/install.sh
set -euo pipefail

HOOKS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GIT_HOOKS_DIR="$(git rev-parse --git-path hooks)"
mkdir -p "$GIT_HOOKS_DIR"

for hook in pre-commit pre-push; do
  if [ ! -f "$HOOKS_DIR/$hook" ]; then
    echo "❌ $hook hook not found in $HOOKS_DIR" >&2
    exit 1
  fi
  cp "$HOOKS_DIR/$hook" "$GIT_HOOKS_DIR/$hook"
  chmod +x "$GIT_HOOKS_DIR/$hook"
  echo "✅ Installed $hook"
done

echo "Hooks installed. CI runs the same checks; --no-verify only defers a failure."
