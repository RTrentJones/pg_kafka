// Stamp the shadow harness's raw result into the shadow.json contract the site's ShadowTable renders.
// Mirrors bench/assemble.mjs + conformance/merge.mjs: top-level generatedAt/version/gitSha from env,
// structured body from the harness. A missing/unreadable raw file degrades to a "pending" shape (every
// check `na`, passed:false) — shakeout-friendly, never a fabricated green and never a crash.
//
// Usage: node assemble.mjs <results-dir> <out-file>
//   env: GENERATED_AT (ISO), VERSION, GIT_SHA, RECORDS
import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';

const dir = process.argv[2] || process.cwd();
const out = process.argv[3] || join(dir, 'shadow.json');
const N = Number(process.env.RECORDS || 500);

// The six scenarios the harness drives — used to emit an all-`na` pending body if the raw file is absent.
const SCENARIOS = [
  'dual_write_sync',
  'dual_write_async',
  'external_only',
  'percentage_50',
  'committed_txn_forwarded',
  'aborted_txn_not_forwarded',
];

const read = (file) => {
  const path = join(dir, file);
  if (!existsSync(path)) {
    console.error(`[assemble] ${file} missing — emitting pending shadow.json`);
    return null;
  }
  try {
    return JSON.parse(readFileSync(path, 'utf8'));
  } catch (err) {
    console.error(`[assemble] ${file} unreadable: ${err.message} — emitting pending shadow.json`);
    return null;
  }
};

const raw = read('shadow-raw.json');

const config = raw?.config ?? {
  records: N,
  forwardPercentage: 100,
  writeMode: 'dual_write',
  syncMode: 'sync',
  realBroker: process.env.REAL_BROKER || 'localhost:9093',
};
const checks = raw?.checks ?? SCENARIOS.map((name) => ({ name, status: 'na' }));
const counts = raw?.counts ?? {
  produced: N,
  forwardedToBroker: 0,
  localStored: 0,
  shadowMetricsForwarded: 0,
  skipped: 0,
  failed: 0,
  outboxFinalized: 0,
  lag: 0,
};

// The artifact passes iff every check passes AND the 100% topic reached full parity (forwarded==produced).
const passed = checks.length > 0 && checks.every((c) => c.status === 'pass') && counts.forwardedToBroker === counts.produced;

const shadow = {
  generatedAt: process.env.GENERATED_AT || new Date().toISOString(),
  version: process.env.VERSION || 'main',
  gitSha: process.env.GIT_SHA || '',
  config,
  checks,
  counts,
  passed,
};

writeFileSync(out, `${JSON.stringify(shadow, null, 2)}\n`);
console.log(`[assemble] wrote ${out} — ${checks.length} checks, passed=${passed}`);
