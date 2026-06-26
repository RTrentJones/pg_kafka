// Raw-INSERT floor: the storage layer with no Kafka protocol on top. Single-row INSERTs into a temp
// table, warm-up excluded, p50/p99 + msgs/sec — the "how fast is Postgres itself" anchor the
// pg_kafka numbers are read against. Connects via libpq env (PGHOST/PGPORT/PGUSER/PGDATABASE).
// Writes bench-rawinsert.json = { msgsPerSec, p50Ms, p99Ms }.
import pg from 'pg';
import { writeFileSync } from 'node:fs';
import { performance } from 'node:perf_hooks';

const OUT = process.env.OUT || 'bench-rawinsert.json';
const N = Number(process.env.MESSAGES || 2000);
const WARMUP = Number(process.env.WARMUP || 200);
const SIZE = Number(process.env.MSG_SIZE || 1024);
const body = Buffer.alloc(SIZE, 0x78).toString('latin1');

const pct = (xs, p) => {
  if (!xs.length) return 0;
  const s = [...xs].sort((a, b) => a - b);
  const i = Math.min(s.length - 1, Math.ceil((p / 100) * s.length) - 1);
  return Math.round(s[Math.max(0, i)] * 100) / 100;
};

async function main() {
  const client = new pg.Client(); // libpq env vars
  await client.connect();
  await client.query(
    'CREATE TEMP TABLE bench_raw (id bigserial primary key, k text, v text, ts timestamptz default now())',
  );

  for (let i = 0; i < WARMUP; i++) {
    await client.query('INSERT INTO bench_raw (k, v) VALUES ($1, $2)', ['k', body]);
  }

  const lat = [];
  const t0 = performance.now();
  for (let i = 0; i < N; i++) {
    const s = performance.now();
    await client.query('INSERT INTO bench_raw (k, v) VALUES ($1, $2)', ['k', body]);
    lat.push(performance.now() - s);
  }
  const wall = performance.now() - t0;
  await client.end();

  const out = {
    msgsPerSec: Math.round(N / (wall / 1000)),
    p50Ms: pct(lat, 50),
    p99Ms: pct(lat, 99),
  };
  writeFileSync(OUT, `${JSON.stringify(out, null, 2)}\n`);
  console.log(`[bench:rawInsert] ${out.msgsPerSec} msgs/s, p50 ${out.p50Ms}ms, p99 ${out.p99Ms}ms`);
}

main().catch((err) => {
  console.error(`[bench:rawInsert] ${err.stack || err}`);
  process.exit(1);
});
