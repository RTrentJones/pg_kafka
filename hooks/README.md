# Git hooks

| Hook | Runs | Takes |
|------|------|-------|
| `pre-commit` | `cargo fmt --check` in the root crate and `kafka_test/` | seconds |
| `pre-push` | `cargo clippy --all-targets --features pg14 -- -D warnings` and the `kafka_test/` equivalent | minutes |

Clippy runs on push rather than on every commit: a slow pre-commit hook trains contributors to
reach for `--no-verify`, which skips everything.

## Install

Git does not install hooks on clone. Run, from anywhere in the clone (and again after the hooks
change):

```bash
./hooks/install.sh
```

It copies both hooks into the clone's hooks directory (`git rev-parse --git-path hooks`).
The hooks are plain scripts rather than `cargo-husky`, which would add a dev-dependency and a
build script.

## Bypassing

`git commit --no-verify` / `git push --no-verify` skip them. CI's `Lint & Format` job runs the
same checks for both crates, so a bypass only defers the failure.
