// Normalize a pg_kafka benchmark run into Tracer's eval shape and POST it to /api/ingest, so the
// throughput trend (and any regression) shows up in Tracer "for free" — the same way an eval
// pass-rate does. Throughput is normalized against the real-broker ceiling into a 0..1 score; the
// absolute numbers ride along in each case's judge_rationale (the site's BenchTable keeps the raw
// figures). Inert when TRACER_INGEST_TOKEN is unset — skips cleanly, never fails the run.
//
// Usage: node ingest-tracer.mjs <bench.json> [tracerBaseUrl]
import { readFileSync } from 'node:fs';

const [, , benchPath, baseUrl = 'https://tracer.rtrentjones.dev'] = process.argv;
const token = process.env.TRACER_INGEST_TOKEN;

if (!token) {
  console.log('TRACER_INGEST_TOKEN unset — skipping Tracer ingest.');
  process.exit(0);
}
if (!benchPath) {
  console.error('usage: node ingest-tracer.mjs <bench.json> [tracerBaseUrl]');
  process.exit(1);
}

const bench = JSON.parse(readFileSync(benchPath, 'utf8'));
const clamp = (x) => Math.max(0, Math.min(1, x));
const best = Math.max(0, ...bench.scenarios.map((s) => s.msgsPerSec));
const ceiling = bench.baselines?.realBroker?.msgsPerSec || best || 1;

const cases = bench.scenarios.map((s) => ({
  name: s.name,
  passed: true,
  score: clamp(s.msgsPerSec / ceiling),
  output: String(s.msgsPerSec),
  judge_rationale: `${s.msgsPerSec.toLocaleString('en-US')} msgs/sec · p50 ${s.p50Ms}ms · p99 ${s.p99Ms}ms`,
}));

const payload = {
  tool: 'pg_kafka',
  model: bench.version || 'main',
  mode: 'benchmark',
  env: 'ci',
  git_sha: bench.gitSha,
  passed: true,
  pass_rate: clamp(best / ceiling),
  cases,
};

const res = await fetch(`${baseUrl}/api/ingest`, {
  method: 'POST',
  headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' },
  body: JSON.stringify(payload),
});
console.log(`Tracer ingest → ${res.status}: ${await res.text()}`);
if (!res.ok) process.exit(1);
