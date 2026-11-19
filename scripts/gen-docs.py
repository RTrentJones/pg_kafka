#!/usr/bin/env python3
"""Render the generated regions of the docs, or check that they are current.

The docs carry regions between marker comments, inline or spanning lines:

    <!--gen:KIND-->...<!--/gen-->
    <!--gen:KIND:ARG-->...<!--/gen-->

Kinds and their sources:

  api-*     the kafka_apis! registry. Owned by the Rust unit test
            src/kafka/api/docs.rs (`cargo test --lib generated_api_docs` fails when stale);
            --write runs it with PG_KAFKA_UPDATE_DOCS=1. This script leaves them alone.
  guc-*     the GUCs as registered by the extension, read from a running server with
            pg_kafka in shared_preload_libraries (pg_settings, plus SHOW for the GUCs that
            are hidden from SHOW ALL). Connection from the usual PG* environment variables.
              guc-table           the full reference table (docs/CONFIGURATION.md)
              guc-count           number of pg_kafka.* GUCs
  tests-*   test counts:
              tests-unit          `cargo test --lib -- --list`
              tests-property      `cargo test --test property_tests -- --list`
              tests-e2e           `kafka_test --list` total
              tests-total         the sum of the three
              tests-e2e-categories  the per-category table (block)

The GUC pass also fails on a `pg_kafka.<name>` in the docs that is no registered GUC (a
renamed or removed setting left behind in prose).

Usage:
  scripts/gen-docs.py [--check | --write] [--only apis,gucs,tests]
Environment:
  GEN_DOCS_FEATURES  cargo feature flags for the root crate (default: --features pg14)
  KAFKA_TEST_BIN     a built kafka_test binary (default: cargo run --release in kafka_test/)
  PSQL               psql binary (default: psql)
"""
import argparse
import os
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MARKER = re.compile(r"<!--gen:([a-z0-9-]+)(?::([^>]*?))?-->(.*?)<!--/gen-->", re.S)
# Not GUCs: file names and the like that share the prefix.
NOT_GUCS = {"pg_kafka.so", "pg_kafka.control", "pg_kafka.sql", "pg_kafka.mdx"}
# Settings that never existed but are named on purpose: the original ADR-001 proposal and
# PERFORMANCE.md's note that they were never implemented (DR-23).
HISTORICAL_GUCS = {"pg_kafka.retention_days", "pg_kafka.partition_interval",
                   "pg_kafka.autovacuum_aggressive"}


def doc_files():
    files = [ROOT / "README.md", ROOT / "CLAUDE.md"]
    files += sorted((ROOT / "docs").glob("*.md"))
    files += sorted((ROOT / "docs" / "architecture").glob("*.md"))
    return [f for f in files if f.exists()]


def run(cmd, **kw):
    r = subprocess.run(cmd, cwd=kw.pop("cwd", ROOT), capture_output=True, text=True, **kw)
    if r.returncode != 0:
        sys.exit(f"gen-docs: {' '.join(cmd)} failed:\n{r.stdout[-2000:]}{r.stderr[-4000:]}")
    return r.stdout


# ---------------------------------------------------------------- GUCs


def psql(sql):
    out = run([os.environ.get("PSQL", "psql"), "-X", "-At", "-F", "\x1f", "-v",
               "ON_ERROR_STOP=1", "-d", os.environ.get("PGDATABASE", "postgres"), "-c", sql])
    return [line.split("\x1f") for line in out.splitlines() if line]


def source_guc_names():
    names = set()
    for f in (ROOT / "src").rglob("*.rs"):
        names.update(re.findall(r'c"(pg_kafka\.[a-z0-9_]+)"', f.read_text()))
    return names


CONTEXT = {
    "postmaster": "restart",
    "sighup": "reload",
    "superuser": "superuser SET, reload",
    "user": "SET",
    "backend": "connection start",
    "superuser-backend": "connection start",
    "internal": "fixed",
}


def load_gucs():
    rows = psql(
        "SELECT name, vartype, boot_val, min_val, max_val, coalesce(unit, ''), "
        "array_to_string(enumvals, ', '), context, short_desc, coalesce(extra_desc, '') "
        "FROM pg_settings WHERE name LIKE 'pg_kafka.%' ORDER BY name"
    )
    if not rows:
        sys.exit("gen-docs: no pg_kafka.* rows in pg_settings; is pg_kafka in "
                 "shared_preload_libraries of the server PG* points at?")
    gucs = {}
    for name, vartype, boot, lo, hi, unit, enums, ctx, short, extra in rows:
        gucs[name] = dict(vartype=vartype, boot=boot, lo=lo, hi=hi, unit=unit, enums=enums,
                          ctx=ctx, short=short, extra=extra, hidden=False)
    # GUC_NO_SHOW_ALL settings are not in pg_settings; the source names them, and SHOW
    # (as a superuser) proves they are registered.
    for name in sorted(source_guc_names() - set(gucs)):
        exists = psql(f"SELECT current_setting('{name}', true) IS NOT NULL")
        if exists and exists[0][0] == "t":
            gucs[name] = dict(vartype="string", boot="", lo="", hi="", unit="", enums="",
                              ctx="", short="", extra="", hidden=True)
        else:
            sys.exit(f"gen-docs: {name} appears in src/ as a GUC name but the server "
                     "does not know it (stale binary installed?)")
    return gucs


def cell(text):
    return text.replace("|", "\\|").replace("\n", " ").strip()


def render_guc_table(gucs):
    out = ["", "| Name | Type | Default | Range | Changes take effect | Description |",
           "|------|------|---------|-------|---------------------|-------------|"]
    for name in sorted(gucs):
        g = gucs[name]
        if g["hidden"]:
            out.append(f"| `{name}` | string | (unset) | | see `src/` | Superuser-only, hidden "
                       "from `SHOW ALL` and `pg_settings`, so only its name is generated; "
                       "see its definition in `src/` |")
            continue
        default = g["boot"]
        if g["vartype"] == "string":
            default = f"`'{default}'`" if default else "(unset)"
        elif default:
            default = f"`{default}`"
        rng = ""
        if g["lo"] or g["hi"]:
            rng = f"{g['lo']}-{g['hi']}" + (f" {g['unit']}" if g["unit"] else "")
        elif g["enums"]:
            rng = g["enums"]
        desc = g["short"] + (f". {g['extra']}" if g["extra"] else "")
        ctx = CONTEXT.get(g["ctx"], g["ctx"])
        out.append(f"| `{name}` | {g['vartype']} | {default} | {rng} | {ctx} | {cell(desc)} |")
    return "\n".join(out) + "\n"


def phantom_gucs(gucs, texts):
    errors = []
    for path, text in texts.items():
        for m in re.finditer(r"(?<![\w.])(pg_kafka\.[a-z][a-z0-9_]*)", text):
            name = m.group(1)
            if name in gucs or name in NOT_GUCS or name in HISTORICAL_GUCS:
                continue
            line = text.count("\n", 0, m.start()) + 1
            errors.append(f"{path.relative_to(ROOT)}:{line}: {name} is not a registered GUC")
    return errors


# ---------------------------------------------------------------- tests


def count_listed(output):
    return sum(1 for line in output.splitlines() if line.endswith(": test"))


def e2e_list():
    binary = os.environ.get("KAFKA_TEST_BIN")
    if binary:
        out = run([binary, "--list"], cwd=ROOT / "kafka_test")
    else:
        out = run(["cargo", "run", "--quiet", "--release", "--", "--list"],
                  cwd=ROOT / "kafka_test")
    per_cat, order, cat = {}, [], None
    for line in out.splitlines():
        if re.fullmatch(r"[a-z0-9_]+:", line):
            cat = line[:-1]
            if cat not in per_cat:
                per_cat[cat] = 0
                order.append(cat)
        elif line.startswith("  - ") and cat:
            per_cat[cat] += 1
    return order, per_cat


def load_tests():
    feats = os.environ.get("GEN_DOCS_FEATURES", "--features pg14").split()
    unit = count_listed(run(["cargo", "test", "--lib", *feats, "--", "--list"]))
    prop = count_listed(run(["cargo", "test", "--test", "property_tests", *feats, "--",
                             "--list"]))
    order, per_cat = e2e_list()
    return dict(unit=unit, prop=prop, order=order, per_cat=per_cat,
                e2e=sum(per_cat.values()))


def render_e2e_table(t):
    out = ["", "| Category | Tests |", "|----------|-------|"]
    for cat in t["order"]:
        out.append(f"| `{cat}` | {t['per_cat'][cat]} |")
    out.append(f"| **Total** | **{t['e2e']}** |")
    return "\n".join(out) + "\n"


# ---------------------------------------------------------------- driver


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--check", action="store_true", help="fail if a region is stale (default)")
    mode.add_argument("--write", action="store_true", help="rewrite the regions in place")
    ap.add_argument("--only", default="apis,gucs,tests",
                    help="comma-separated subset of: apis (--write only), gucs, tests")
    args = ap.parse_args()
    only = set(args.only.split(","))

    texts = {p: p.read_text() for p in doc_files()}
    gucs = load_gucs() if "gucs" in only else None
    tests = load_tests() if "tests" in only else None

    errors = []

    def render(kind, arg):
        if kind.startswith("guc-") and gucs is not None:
            if kind == "guc-table":
                return render_guc_table(gucs)
            if kind == "guc-count":
                return str(len(gucs))
        if kind.startswith("tests-") and tests is not None:
            simple = {"tests-unit": tests["unit"], "tests-property": tests["prop"],
                      "tests-e2e": tests["e2e"],
                      "tests-total": tests["unit"] + tests["prop"] + tests["e2e"]}
            if kind in simple:
                return str(simple[kind])
            if kind == "tests-e2e-categories":
                return render_e2e_table(tests)
        if kind.startswith(("guc-", "tests-")) and (
            (kind.startswith("guc-") and gucs is not None)
            or (kind.startswith("tests-") and tests is not None)
        ):
            errors.append(f"unknown generated-doc kind {kind}")
        return None

    stale = []
    for path, text in texts.items():
        def sub(m):
            body = render(m.group(1), m.group(2))
            if body is None:
                return m.group(0)
            tag = m.group(1) + (f":{m.group(2)}" if m.group(2) else "")
            return f"<!--gen:{tag}-->{body}<!--/gen-->"
        new = MARKER.sub(sub, text)
        if new != text:
            if args.write:
                path.write_text(new)
                texts[path] = new
            else:
                stale.append(str(path.relative_to(ROOT)))

    if gucs is not None:
        errors += phantom_gucs(gucs, texts)
    if args.write and "apis" in only:
        env = dict(os.environ, PG_KAFKA_UPDATE_DOCS="1")
        feats = os.environ.get("GEN_DOCS_FEATURES", "--features pg14").split()
        run(["cargo", "test", "--lib", *feats, "generated_api_docs"], env=env)
    if errors:
        print("\n".join(errors), file=sys.stderr)
    if stale:
        print(f"gen-docs: stale generated regions in {', '.join(stale)}; run "
              f"scripts/gen-docs.py --write --only {args.only} and commit the result",
              file=sys.stderr)
    if errors or stale:
        sys.exit(1)
    print("gen-docs: generated regions are current")


if __name__ == "__main__":
    main()
