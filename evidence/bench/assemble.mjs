// Assemble the per-run bench files into the bench.json contract the site's BenchTable renders:
// pg_kafka's full scenario list, plus a raw-INSERT floor and a real-broker ceiling baseline. Missing
// inputs degrade to zeros (a shakeout-friendly default), never a crash.
//
// Usage: node assemble.mjs <results-dir> <out-file>
//   env: GENERATED_AT (ISO), VERSION, GIT_SHA, MSG_SIZE
import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';

const dir = process.argv[2] || process.cwd();
const out = process.argv[3] || join(dir, 'bench.json');

const read = (file) => {
  const path = join(dir, file);
  if (!existsSync(path)) {
    console.error(`[assemble] ${file} missing`);
    return null;
  }
  try {
    return JSON.parse(readFileSync(path, 'utf8'));
  } catch (err) {
    console.error(`[assemble] ${file} unreadable: ${err.message}`);
    return null;
  }
};

const pgk = read('bench-pg_kafka.json');
const realBroker = read('bench-realbroker.json');
const rawInsert = read('bench-rawinsert.json');

const scenarios = pgk?.scenarios ?? [];

// Representative throughput for a scenario-style run = its batched-produce scenario (best signal of
// sustained throughput), else the first scenario.
const representative = (run) => {
  if (!run?.scenarios?.length) return { msgsPerSec: 0, p50Ms: 0, p99Ms: 0 };
  const s = run.scenarios.find((x) => /batched/i.test(x.name)) ?? run.scenarios[0];
  return { msgsPerSec: s.msgsPerSec, p50Ms: s.p50Ms, p99Ms: s.p99Ms };
};

const bench = {
  generatedAt: process.env.GENERATED_AT || new Date().toISOString(),
  version: process.env.VERSION || 'main',
  gitSha: process.env.GIT_SHA || '',
  config: { warmupExcluded: true, messageSizes: [Number(process.env.MSG_SIZE || 1024)] },
  scenarios,
  baselines: {
    rawInsert: rawInsert ?? { msgsPerSec: 0, p50Ms: 0, p99Ms: 0 },
    realBroker: representative(realBroker),
  },
};

writeFileSync(out, `${JSON.stringify(bench, null, 2)}\n`);
console.log(`[assemble] wrote ${out} — ${scenarios.length} scenarios`);
