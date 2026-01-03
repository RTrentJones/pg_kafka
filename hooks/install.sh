#!/bin/bash
# Install git hooks for pg_kafka project
#
# Usage: ./hooks/install.sh

set -e

HOOKS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GIT_HOOKS_DIR="$(git rev-parse --git-dir)/hooks"

echo "Installing git hooks..."

# Install pre-commit hook
if [ -f "$HOOKS_DIR/pre-commit" ]; then
    cp "$HOOKS_DIR/pre-commit" "$GIT_HOOKS_DIR/pre-commit"
    chmod +x "$GIT_HOOKS_DIR/pre-commit"
    echo "✅ Installed pre-commit hook"
else
    echo "❌ pre-commit hook not found in $HOOKS_DIR"
    exit 1
fi

echo ""
echo "✅ All hooks installed successfully!"
echo ""
echo "The following hooks are now active:"
echo "  - pre-commit: Runs 'cargo fmt --check' and 'cargo clippy -- -D warnings'"
echo ""
echo "To bypass hooks (not recommended), use: git commit --no-verify"
