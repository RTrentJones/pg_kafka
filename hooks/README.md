# Git Hooks for pg_kafka

This directory contains git hooks that enforce code quality standards for the pg_kafka project.

## Available Hooks

### pre-commit (fast)

Runs before every commit to ensure:
- Code is properly formatted in both crates (`cargo fmt --check`, root + kafka_test)

### pre-push (slower)

Runs once per push (DR-25: keeping the minutes-long lint off the per-commit
path so nobody is tempted into `--no-verify`):
- No clippy warnings in either crate
  (`cargo clippy --all-targets --features pg14 -- -D warnings` and the
  kafka_test equivalent — the same gates CI enforces)

## Installation

### Quick Install

Run the install script from the repository root:

```bash
./hooks/install.sh
```

### Manual Install

Copy hooks to your `.git/hooks/` directory:

```bash
cp hooks/pre-commit .git/hooks/pre-commit
cp hooks/pre-push .git/hooks/pre-push
chmod +x .git/hooks/pre-commit .git/hooks/pre-push
```

## For New Contributors

**Important:** Git hooks are not automatically installed when cloning a repository (for security reasons). Each developer must manually install them.

Add this step to your onboarding:

1. Clone the repository
2. Run `./hooks/install.sh` to install hooks
3. Hooks will now run automatically before commits

## Bypassing Hooks (Not Recommended)

In rare cases where you need to bypass hooks (e.g., work-in-progress commits):

```bash
git commit --no-verify
```

**Warning:** Do not push commits that haven't passed the hooks checks. The CI pipeline will reject them.

## Updating Hooks

If hooks are updated in the repository:

1. Pull the latest changes
2. Re-run `./hooks/install.sh` to update your local hooks

## Why Not cargo-husky?

We considered using `cargo-husky` for automatic hook installation, but:
- It adds an extra dev-dependency
- It requires build script execution which adds complexity
- Manual installation is simple and explicit
- Gives developers control over when hooks are installed

The trade-off is that new contributors must remember to run `./hooks/install.sh`.
