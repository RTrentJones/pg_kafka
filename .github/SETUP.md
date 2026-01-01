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
     - Click **Add status check**
     - Add: `Test Suite`
     - Add: `Code Coverage`
     - Add: `Linting and Format Check`
   - ✅ **Require branches to be up to date before merging**
   - ✅ **Do not allow bypassing the above settings**
4. Click **Create** or **Save changes**

## Step 3: Set Up Code Coverage Reporting (Optional)

### Option A: Codecov (Recommended)

1. Go to [codecov.io](https://codecov.io)
2. Sign in with GitHub
3. Add your `pg_kafka` repository
4. Copy the upload token (if private repo)
5. Add the token as a GitHub secret:
   - Go to **Settings** → **Secrets and variables** → **Actions**
   - Click **New repository secret**
   - Name: `CODECOV_TOKEN`
   - Value: [paste token]
6. Update the badge URLs in README.md:
   - Replace `YOUR_USERNAME` with your GitHub username

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
         path-to-lcov: ./coverage/lcov.info
     ```
5. Update the badge in README.md

## Step 4: Update README Badges

Edit [README.md](../README.md) and replace `YOUR_USERNAME` with your actual GitHub username:

```markdown
[![CI/CD Pipeline](https://github.com/YOUR_USERNAME/pg_kafka/actions/workflows/ci.yml/badge.svg)](https://github.com/YOUR_USERNAME/pg_kafka/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/YOUR_USERNAME/pg_kafka/branch/main/graph/badge.svg)](https://codecov.io/gh/YOUR_USERNAME/pg_kafka)
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

4. Create a pull request on GitHub

5. Verify that all checks run:
   - ✅ Test Suite
   - ✅ Code Coverage
   - ✅ Linting and Format Check

6. Check that the PR cannot be merged until all checks pass

## What the CI/CD Pipeline Does

### On Every Push and PR

The pipeline runs three parallel jobs:

#### 1. Test Suite (`test`)
- ✅ Checks code formatting with `cargo fmt`
- ✅ Runs clippy linting with warnings as errors
- ✅ Runs pgrx unit tests (`cargo pgrx test pg14`)
- ✅ Builds the extension in release mode
- ✅ Installs the extension to PostgreSQL
- ✅ Runs E2E tests in `kafka_test/`

#### 2. Code Coverage (`coverage`)
- ✅ Generates coverage report using `cargo-tarpaulin`
- ✅ Uploads to Codecov for visualization
- ✅ Archives coverage artifacts

#### 3. Linting (`lint`)
- ✅ Verifies code formatting
- ✅ Runs clippy on all targets and features

### Caching Strategy

The pipeline uses caching to speed up builds:
- Cargo registry cache
- Cargo git dependencies cache
- Build artifacts cache

Typical run times:
- **First run** (cold cache): ~15-20 minutes
- **Subsequent runs** (warm cache): ~5-8 minutes

## Troubleshooting

### Tests fail locally but pass in CI (or vice versa)

Ensure your local environment matches CI:
```bash
# Use the same Rust toolchain
rustup update nightly

# Clean and rebuild
cargo clean
cargo build --features pg14

# Run tests the same way CI does
cargo pgrx test pg14
```

### Coverage upload fails

If you see errors uploading to Codecov:
1. Verify `CODECOV_TOKEN` is set correctly (for private repos)
2. Check that the token has the correct permissions
3. Review the Codecov upload logs in the Actions tab

### E2E tests timeout

The E2E tests have a 30-second timeout. If they fail:
1. Check that the background worker is starting correctly
2. Verify PostgreSQL is accessible on port 5432
3. Check the Postgres logs for errors

### Clippy warnings fail the build

The CI is configured with `-D warnings` (treat warnings as errors). To fix:
```bash
# Run clippy locally
cargo clippy --features pg14 -- -D warnings

# Fix all warnings before pushing
```

## Local Development Workflow

To ensure your PR will pass CI before pushing:

```bash
# 1. Format code
cargo fmt

# 2. Run clippy
cargo clippy --features pg14 -- -D warnings

# 3. Run unit tests
cargo pgrx test pg14

# 4. Run E2E tests
cd kafka_test && cargo run && cd ..

# 5. If all pass, commit and push
git add .
git commit -m "feat: Your feature description"
git push origin your-branch
```

## Continuous Deployment (Future)

Currently, the pipeline only runs tests and coverage. In the future, you can add:

- **Automated releases**: Publish to crates.io on version tags
- **Documentation deployment**: Auto-generate and publish rustdoc
- **Performance benchmarks**: Track performance over time
- **Security audits**: Run `cargo audit` on dependencies

## Getting Help

If you encounter issues with the CI/CD setup:

1. Check the [GitHub Actions logs](../../actions) for detailed error messages
2. Review the [pgrx documentation](https://docs.rs/pgrx/latest/pgrx/)
3. Open an issue on the repository

## Summary Checklist

- [ ] GitHub Actions enabled
- [ ] Branch protection rules configured
- [ ] Codecov account linked (optional)
- [ ] `CODECOV_TOKEN` secret added (if private repo)
- [ ] README badges updated with correct username
- [ ] Test PR created to verify pipeline
- [ ] All checks passing on test PR
