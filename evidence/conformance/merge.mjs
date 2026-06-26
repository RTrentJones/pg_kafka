// Fold the per-client results-*.json files into a single conformance.json (the contract the site's
// ConformanceMatrix renders). Tolerant by design: a client whose harness produced no file (build or
// runtime failure during a shakeout) becomes an all-"na" row rather than breaking the artifact, so
// the pipeline always emits a valid conformance.json.
//
// Usage: node merge.mjs <results-dir> <out-file>
//   env: GENERATED_AT (ISO), VERSION (e.g. git describe), GIT_SHA
import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const here = dirname(fileURLToPath(import.meta.url));
const resultsDir = process.argv[2] || process.cwd();
const outFile = process.argv[3] || join(resultsDir, 'conformance.json');

const apis = JSON.parse(readFileSync(join(here, 'apis.json'), 'utf8')).apis;
const apiNames = apis.map((a) => a.name);

const expected = [
  { file: 'results-kafkajs.json', name: 'kafkajs', lang: 'Node' },
  { file: 'results-librdkafka.json', name: 'librdkafka', lang: 'C / confluent-kafka' },
  { file: 'results-kafka-python.json', name: 'kafka-python', lang: 'Python' },
  { file: 'results-sarama.json', name: 'Sarama', lang: 'Go' },
];

const clients = expected.map((spec) => {
  const path = join(resultsDir, spec.file);
  let client = { name: spec.name, lang: spec.lang, version: 'unknown', results: {} };
  if (existsSync(path)) {
    try {
      client = { ...client, ...JSON.parse(readFileSync(path, 'utf8')) };
    } catch (err) {
      console.error(`[merge] ${spec.file} unreadable (${err.message}) — emitting all-na row`);
    }
  } else {
    console.error(`[merge] ${spec.file} missing — emitting all-na row`);
  }
  // Normalize: every API present, only the known statuses.
  const results = {};
  for (const name of apiNames) {
    const s = client.results?.[name];
    results[name] = s === 'pass' || s === 'fail' || s === 'na' ? s : 'na';
  }
  return { name: client.name, lang: client.lang, version: client.version, results };
});

const conformance = {
  generatedAt: process.env.GENERATED_AT || new Date().toISOString(),
  version: process.env.VERSION || 'main',
  gitSha: process.env.GIT_SHA || '',
  apis,
  clients,
};

writeFileSync(outFile, `${JSON.stringify(conformance, null, 2)}\n`);

let pass = 0;
let attempted = 0;
for (const c of clients) {
  for (const name of apiNames) {
    if (c.results[name] === 'pass') {
      pass++;
      attempted++;
    } else if (c.results[name] === 'fail') {
      attempted++;
    }
  }
}
console.log(
  `[merge] wrote ${outFile} — ${clients.length} clients, ${apiNames.length} APIs, ${
    attempted ? Math.round((pass / attempted) * 100) : 0
  }% pass`,
);
