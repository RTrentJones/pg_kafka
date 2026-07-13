#!/bin/bash
# Install git hooks for pg_kafka project
#
# Usage: ./hooks/install.sh

set -e

HOOKS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GIT_HOOKS_DIR="$(git rev-parse --git-dir)/hooks"

echo "Installing git hooks..."

# Install hooks (DR-25: pre-commit = fast fmt checks; pre-push = clippy)
for hook in pre-commit pre-push; do
    if [ -f "$HOOKS_DIR/$hook" ]; then
        cp "$HOOKS_DIR/$hook" "$GIT_HOOKS_DIR/$hook"
        chmod +x "$GIT_HOOKS_DIR/$hook"
        echo "✅ Installed $hook hook"
    else
        echo "❌ $hook hook not found in $HOOKS_DIR"
        exit 1
    fi
done

echo ""
echo "✅ All hooks installed successfully!"
echo ""
echo "The following hooks are now active:"
echo "  - pre-commit: cargo fmt --check (both crates; fast)"
echo "  - pre-push:   cargo clippy --all-targets -D warnings (both crates)"
echo ""
echo "To bypass hooks (not recommended), use: git commit --no-verify"
