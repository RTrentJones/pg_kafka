# GitHub CI/CD Setup Guide

This document explains how to configure your GitHub repository to use the CI/CD pipeline.

## Step 1: Enable GitHub Actions

1. Go to your repository on GitHub
2. Navigate to **Settings** → **Actions** → **General**
3. Under "Actions permissions", select **Allow all actions and reusable workflows**
4. Click **Save**

## Step 2: Configure Branch Protection Rules

To ensure all PRs must pass tests before merging:

1. Go to **Settings** → **Branches**
2. Click **Add branch protection rule**
3. Configure the following:
   - **Branch name pattern**: `main`
   - ✅ **Require a pull request before merging**
   - ✅ **Require status checks to pass before merging**
     - Click **Add status check** and add the job names from `.github/workflows/ci.yml`:
       - `Lint & Format`
       - `Test Suite`
       - `Security Audit`
       - `Verify Lockfile`
       - `Build kafka_test`
       - `E2E pg14`, `E2E pg15`, `E2E pg16`, `E2E pg17`, `E2E pg18`
       - `Interop (Java clients, differential vs Kafka)`
       - `SPI change needs E2E test` (from `.github/workflows/spi-e2e-guard.yml`; pull requests only)
     - Codecov reports `codecov/project` and `codecov/patch` as separate statuses once Step 3 is done
   - ✅ **Require branches to be up to date before merging**
   - ✅ **Do not allow bypassing the above settings**
4. Click **Create** or **Save changes**

## Step 3: Set Up Code Coverage Reporting

The `Test Suite` job uploads `lcov.info` with `codecov/codecov-action@v5` and `fail_ci_if_error: true`,
so a failed upload fails CI. Coverage gating is configured in `codecov.yml`: project coverage passes
at 78% and above (target 80%, threshold 2%), patch coverage at 85% and above (target 90%, threshold 5%).

### Option A: Codecov (as configured)

1. Go to [codecov.io](https://codecov.io)
2. Sign in with GitHub
3. Add your `pg_kafka` repository
4. Copy the repository upload token
5. Add the token as a GitHub secret (the workflow passes `secrets.CODECOV_TOKEN` to the upload step):
   - Go to **Settings** → **Secrets and variables** → **Actions**
   - Click **New repository secret**
   - Name: `CODECOV_TOKEN`
   - Value: [paste token]
6. If you are setting this up on a fork, update the badge URLs in README.md (see Step 4)

### Option B: Coveralls

If you prefer Coveralls over Codecov:

1. Go to [coveralls.io](https://coveralls.io)
2. Sign in with GitHub
3. Add your repository
4. Update `.github/workflows/ci.yml`:
   - Replace the Codecov upload step with:
     ```yaml
     - name: Upload coverage to Coveralls
       uses: coverallsapp/github-action@v2
       with:
         github-token: ${{ secrets.GITHUB_TOKEN }}
         path-to-lcov: lcov.info
     ```
5. Update the badge in README.md
6. Keep `codecov.yml` even after switching services: `.github/scripts/require-e2e-for-spi.sh` derives its
   SPI file list from that file's `ignore:` section. The project/patch coverage gates it defines apply only to Codecov

## Step 4: Update README Badges (forks only)

[README.md](../README.md) already points its badges at `RTrentJones/pg_kafka`. On a fork, replace
`RTrentJones` with your GitHub username:

```markdown
[![CI/CD Pipeline](https://github.com/RTrentJones/pg_kafka/actions/workflows/ci.yml/badge.svg)](https://github.com/RTrentJones/pg_kafka/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/RTrentJones/pg_kafka/branch/main/graph/badge.svg)](https://codecov.io/gh/RTrentJones/pg_kafka)
```

## Step 5: Test the Pipeline

1. Create a new branch:
   ```bash
   git checkout -b test-ci
   ```

2. Make a small change (e.g., add a comment)

3. Commit and push:
   ```bash
   git add .
   git commit -m "test: Verify CI/CD pipeline"
   git push origin test-ci
   ```

4. Create a pull request on GitHub targeting `main` (CI only runs for pushes to `main` and pull requests
   into `main`)

5. Verify that the checks listed in Step 2 run (plus the `codecov/project` and `codecov/patch`
   statuses)

6. Check that the PR cannot be merged until all checks pass

## What the Workflows Do

`.github/workflows/ci.yml` runs on pushes to `main` and pull requests into `main`; a new commit
cancels the run in progress for the same ref. The workflow file is the reference; in short:

| Job | Does |
|-----|------|
| `Lint & Format` | `cargo fmt --check` and `cargo clippy --all-targets -D warnings` for both crates (pg14); the QA-1 coverage-policy check |
| `Test Suite` | Unit tests with coverage (`cargo llvm-cov --lib --features pg14`), uploaded to Codecov; property tests |
| `Build kafka_test` | Builds the E2E runner once for the E2E jobs |
| `E2E pgN` (14-18) | Clippy and unit/property tests on that major, installs the extension into PostgreSQL N (apt.postgresql.org), starts it with `scripts/ci-pg.sh`, runs the functional E2E suite, `scripts/check-upgrade.sh` and the generated-GUC-docs check; pg14 also runs the shadow (against `docker compose`'s `external-kafka`) and performance categories and the test-count docs check; pg18 ends with `scripts/postmaster-death-check.sh` (SIGKILL the postmaster, the listener must exit, the cluster must restart) |
| `Interop (Java clients, differential vs Kafka)` | `interop/run-java-checks.sh` and `interop/diff/run.py` against a reference Apache Kafka 3.7 |
| `Security Audit` | `cargo audit` and `cargo deny check` for both crates (stable toolchain) |
| `Verify Lockfile` | `cargo check --locked` for both crates |

`.github/workflows/spi-e2e-guard.yml` (job `SPI change needs E2E test`) runs on pull requests,
also when a label is added or removed: `.github/scripts/require-e2e-for-spi.sh` on every changed
file of the PR (all pages of the files API) and the labels read at run time; the `no-e2e-needed`
label waives it.

`.github/workflows/fuzz.yml` runs each libFuzzer target in `fuzz/` for 3 minutes on pushes and
pull requests, and for 30 minutes weekly. `.github/workflows/evidence.yml` runs weekly, on `v*`
tags and on demand: it brings the extension up the same way, runs `scripts/generate-evidence.sh`,
force-pushes the artifacts to the orphan `evidence` branch, and posts the benchmark to Tracer when
`TRACER_INGEST_TOKEN` is set.

The shared setup lives in composite actions under `.github/actions/`: `setup-pg` (PostgreSQL from
apt.postgresql.org, the cargo-pgrx version `Cargo.lock` pins, the Rust build cache),
`free-disk-space` and `start-external-kafka`. Caches: `Swatinem/rust-cache` per job and the
`cargo-pgrx` / `cargo-fuzz` binaries.

## Troubleshooting

### Tests fail locally but pass in CI (or vice versa)

Ensure your local environment matches CI:
```bash
# Use the same Rust toolchain: rustup reads rust-toolchain.toml (nightly) automatically
rustup show

# Clean and rebuild
cargo clean
cargo build --features pg14

# Run tests the same way CI does
cargo llvm-cov --lib --features pg14 --summary-only
cargo test --features pg14 --test property_tests
```

### Coverage upload fails

If you see errors uploading to Codecov:
1. Verify `CODECOV_TOKEN` is set correctly (for private repos)
2. Check that the token has the correct permissions
3. Review the Codecov upload logs in the Actions tab

### E2E tests time out

`scripts/ci-pg.sh start` waits up to 60 seconds for the worker to open the Kafka port; each E2E
step has its own `timeout-minutes` in `ci.yml`. If they fail:
1. Check that the background worker starts (`shared_preload_libraries` must include `pg_kafka`)
2. Read the PostgreSQL log the job prints on failure (`scripts/ci-pg.sh log`)

### Clippy warnings fail the build

The CI is configured with `-D warnings` (treat warnings as errors). To fix:
```bash
# Run clippy locally (same as CI and the pre-push hook)
cargo clippy --all-targets --features pg14 -- -D warnings
cd kafka_test && cargo clippy --all-targets -- -D warnings

# Fix all warnings before pushing
```

## Local Development Workflow

To ensure your PR will pass CI before pushing:

```bash
# 1. Format code
cargo fmt && (cd kafka_test && cargo fmt)

# 2. Run clippy
cargo clippy --all-targets --features pg14 -- -D warnings

# 3. Run unit and property tests
cargo test --features pg14

# 4. Run E2E tests (PostgreSQL running with pg_kafka in shared_preload_libraries)
cargo pgrx start pg14
cd kafka_test && cargo run --release && cd ..

# 5. If all pass, commit and push
git add .
git commit -m "feat: Your feature description"
git push origin your-branch
```

## Continuous Deployment (Future)

Currently, the pipelines run tests, coverage, security audits (`cargo audit`, `cargo deny`), and the
evidence workflow, which tracks benchmark results over time. In the future, you can add:

- **Automated releases**: Publish to crates.io on version tags
- **Documentation deployment**: Auto-generate and publish rustdoc

## Getting Help

If you encounter issues with the CI/CD setup:

1. Check the [GitHub Actions logs](../../actions) for detailed error messages
2. Review the [pgrx documentation](https://docs.rs/pgrx/latest/pgrx/)
3. Open an issue on the repository

## Summary Checklist

- [ ] GitHub Actions enabled
- [ ] Branch protection rules configured
- [ ] Codecov account linked
- [ ] `CODECOV_TOKEN` secret added
- [ ] README badges updated with correct username (forks only)
- [ ] Test PR created to verify pipeline
- [ ] All checks passing on test PR
