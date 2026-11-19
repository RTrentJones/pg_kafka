#!/usr/bin/env bash
# QA-1 policy check (see codecov.yml): make "every SPI file is either ignored by the coverage
# gate or guarded by spi-e2e-guard" mechanically true.
#   1. every file under src/ that calls SPI (Spi::*, BackgroundWorker::*, #[pg_extern]) is
#      listed in codecov.yml, as an ignore entry or a `# qa1-guard:` line;
#   2. every listed path exists;
#   3. require-e2e-for-spi.sh fires for each listed file and stays quiet for an unlisted one.
set -euo pipefail
cd "$(dirname "$0")/../.."
CODECOV_YML=codecov.yml

listed="$(grep -E '^[[:space:]]*(- |# qa1-guard: )"src/[^"*]+"' "$CODECOV_YML" \
  | grep -oE '"src/[^"]+"' | tr -d '"' | sort -u)"
spi="$(grep -rlE '\bSpi::[a-z_]+(::<[^>]*>)?\(|BackgroundWorker::[a-z_]+\(|#\[pg_extern' src \
  | sort -u)"

fail=0
for f in $spi; do
  if ! grep -qxF "$f" <<<"$listed"; then
    echo "::error file=$f::$f calls SPI but is neither ignored nor qa1-guarded in codecov.yml"
    fail=1
  fi
done
for f in $listed; do
  if [ ! -f "$f" ]; then
    echo "::error::codecov.yml lists $f, which does not exist"
    fail=1
  fi
done

guard=.github/scripts/require-e2e-for-spi.sh
for f in $listed; do
  if printf '%s\n' "$f" | PR_LABELS= CODECOV_YML="$CODECOV_YML" bash "$guard" >/dev/null 2>&1; then
    echo "::error::spi-e2e-guard does not fire for $f"
    fail=1
  fi
  if ! printf '%s\nkafka_test/src/main.rs\n' "$f" | PR_LABELS= bash "$guard" >/dev/null 2>&1; then
    echo "::error::spi-e2e-guard rejects $f even with a kafka_test/src change"
    fail=1
  fi
done
if ! printf 'src/kafka/handlers/fetch.rs\n' | PR_LABELS= bash "$guard" >/dev/null 2>&1; then
  echo "::error::spi-e2e-guard fires for a file that is not listed"
  fail=1
fi

[ "$fail" = 0 ] || exit 1
echo "QA-1 coverage policy holds: $(wc -l <<<"$spi") SPI files, all listed; $(wc -l <<<"$listed") guarded files."
