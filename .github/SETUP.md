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
       - `Check pg15`, `Check pg16`, `Check pg17`
       - `SPI change needs E2E test` (runs on pull requests only)
     - Codecov reports `codecov/project` and `codecov/patch` as separate statuses once Step 3 is done
   - ✅ **Require branches to be up to date before merging**
   - ✅ **Do not allow bypassing the above settings**
4. Click **Create** or **Save changes**

## Step 3: Set Up Code Coverage Reporting

The `Test Suite` job uploads `lcov.info` with `codecov/codecov-action@v5` and `fail_ci_if_error: true`,
so a failed upload fails CI. Coverage gating (project 80%, patch 90%) is configured in `codecov.yml`.

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

5. Verify that all checks run:
   - ✅ Lint & Format
   - ✅ Test Suite (plus the `codecov/project` and `codecov/patch` statuses)
   - ✅ Security Audit
   - ✅ Verify Lockfile
   - ✅ Check pg15 / pg16 / pg17
   - ✅ SPI change needs E2E test

6. Check that the PR cannot be merged until all checks pass

## What the CI/CD Pipeline Does

### On Pushes to `main` and Pull Requests into `main`

`.github/workflows/ci.yml` (workflow name `CI/CD Pipeline`) runs these jobs. In-progress runs for the same ref
are cancelled when a new commit arrives.

#### 1. Lint & Format (`lint`)
- ✅ `cargo fmt -- --check` for the root crate and `kafka_test/`
- ✅ `cargo clippy --all-targets --features pg14 -- -D warnings`, and the `kafka_test/` equivalent

#### 2. Test Suite (`test`, runs after `lint`)
- ✅ Unit tests with coverage: `cargo llvm-cov --lib --features pg14 --lcov --output-path lcov.info`
  (`cargo pgrx test` is not used; see [docs/PGRX_TESTING_GUIDE.md](../docs/PGRX_TESTING_GUIDE.md))
- ✅ Property tests: `cargo test --features pg14 --test property_tests`
- ✅ Uploads `lcov.info` to Codecov
- ✅ Installs the release build into the pgrx-managed PostgreSQL 14, adds `shared_preload_libraries = 'pg_kafka'`,
  starts it, and runs `CREATE EXTENSION` in the `postgres` database
- ✅ Functional E2E tests: `cargo run --release -- --exclude performance --exclude shadow` (10-minute timeout)
- ✅ Shadow E2E tests against the `external-kafka` docker compose service: `--category shadow` (15-minute timeout)
- ✅ Performance E2E tests, blocking: `--category performance` (5-minute timeout)
- ✅ Dumps PostgreSQL logs on failure

#### 3. Security Audit (`security`)
- ✅ `cargo audit` and `cargo deny check` for both crates (stable toolchain)

#### 4. Verify Lockfile (`lockfile`)
- ✅ `cargo check --locked --features pg14`, and `cargo check --locked` in `kafka_test/`

#### 5. Check pgN (`pg-matrix`, runs after `lint`)
- ✅ For PostgreSQL 15, 16, and 17: `cargo check` and `cargo test --lib` with `--no-default-features --features pgN`

#### 6. SPI change needs E2E test (`spi-e2e-guard`, pull requests only)
- ✅ Runs `.github/scripts/require-e2e-for-spi.sh` on the PR's changed files. It fails when a `src/` file in
  `codecov.yml`'s `ignore:` list changes without a change to a Rust file under `kafka_test/src/`. The
  `no-e2e-needed` label waives it.

### Evidence Workflow

`.github/workflows/evidence.yml` runs weekly, on `v*` tags, and on demand. It brings the extension up the same
way, runs `scripts/generate-evidence.sh` (client conformance matrix, benchmark, recorded session, shadow
compliance), force-pushes the artifacts to the orphan `evidence` branch, and optionally posts the benchmark to
Tracer when `TRACER_INGEST_TOKEN` is set.

### Caching Strategy

The pipeline uses caching to speed up builds:
- `Swatinem/rust-cache` for the root crate and `kafka_test/` (registry, git dependencies, build artifacts)
- The `cargo-pgrx` binary (keyed on `PGRX_VERSION`, currently 0.16.1)
- The pgrx-downloaded PostgreSQL 14 binaries (keyed on `PGRX_VERSION` and `PG_VERSION`)

Typical run times:
- **First run** (cold cache): ~15-20 minutes
- **Subsequent runs** (warm cache): ~5-8 minutes

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

### E2E tests timeout

CI waits up to 30 seconds for the worker to open port 9092, then gives the functional, shadow, and
performance E2E steps 10, 15, and 5 minutes respectively. If they fail:
1. Check that the background worker is starting correctly (`shared_preload_libraries` must include `pg_kafka`)
2. Verify PostgreSQL is reachable on the pgrx port (28814 for pg14 locally; CI reads it from `postgresql.conf`)
   and that `DATABASE_URL` points at it
3. Check the Postgres logs for errors (CI dumps them on failure)

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
