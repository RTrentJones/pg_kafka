// Throughput + latency bench over the Kafka wire protocol — run against pg_kafka (:9092) and, with
// the same code, against a real broker (:9093) for the ceiling. Warm-up is excluded. Each scenario
// reports p50/p99 (ms) over per-operation latencies and msgs/sec over wall time. Writes
// bench-<LABEL>.json = { scenarios: [...] }.
//
// Each scenario is independently try/caught: a single flake (e.g. an e2e join-race on a real broker)
// is skipped, never discarding the scenarios that did succeed. kafkajs keeps the event loop alive, so
// the process exits explicitly at the end.
import { Kafka, logLevel } from 'kafkajs';
import { writeFileSync } from 'node:fs';
import { performance } from 'node:perf_hooks';

const BROKER = process.env.BROKER || 'localhost:9092';
const LABEL = process.env.LABEL || 'pg_kafka';
const OUT = process.env.OUT || `bench-${LABEL}.json`;
const N = Number(process.env.MESSAGES || 2000);
const WARMUP = Number(process.env.WARMUP || 200);
const BATCH = Number(process.env.BATCH || 100);
const SIZE = Number(process.env.MSG_SIZE || 1024);

const body = Buffer.alloc(SIZE, 0x78).toString('latin1'); // ~SIZE-byte payload
const pct = (xs, p) => {
  if (!xs.length) return 0;
  const s = [...xs].sort((a, b) => a - b);
  const i = Math.min(s.length - 1, Math.ceil((p / 100) * s.length) - 1);
  return Math.round(s[Math.max(0, i)] * 100) / 100;
};
const summarize = (name, latencies, wallMs, count) => ({
  name,
  p50Ms: pct(latencies, 50),
  p99Ms: pct(latencies, 99),
  msgsPerSec: Math.round(count / (wallMs / 1000)),
});

const kafka = new Kafka({
  clientId: `bench-${LABEL}`,
  brokers: [BROKER],
  logLevel: logLevel.NOTHING,
  retry: { retries: 5 },
});

const scenarios = [];
async function scenario(name, fn) {
  try {
    scenarios.push(await fn());
    const s = scenarios[scenarios.length - 1];
    console.log(`  ${s.name}: ${s.msgsPerSec} msgs/s, p50 ${s.p50Ms}ms, p99 ${s.p99Ms}ms`);
  } catch (err) {
    console.error(`  [skip] ${name}: ${err.message}`);
  }
}

async function main() {
  const admin = kafka.admin();
  const producer = kafka.producer({ allowAutoTopicCreation: true });
  await admin.connect();
  await producer.connect();
  const topic = `bench-${LABEL}-${Date.now()}`;
  await admin.createTopics({ topics: [{ topic, numPartitions: 1 }], waitForLeaders: true });

  await scenario('produce-unbatched', async () => {
    for (let i = 0; i < WARMUP; i++) await producer.send({ topic, messages: [{ value: body }] });
    const lat = [];
    const t0 = performance.now();
    for (let i = 0; i < N; i++) {
      const s = performance.now();
      await producer.send({ topic, messages: [{ value: body }] });
      lat.push(performance.now() - s);
    }
    return summarize(`produce · unbatched · ${SIZE}B`, lat, performance.now() - t0, N);
  });

  await scenario('produce-batched', async () => {
    const batch = Array.from({ length: BATCH }, () => ({ value: body }));
    for (let i = 0; i < Math.ceil(WARMUP / BATCH); i++) await producer.send({ topic, messages: batch });
    const lat = [];
    const batches = Math.ceil(N / BATCH);
    const t0 = performance.now();
    for (let i = 0; i < batches; i++) {
      const s = performance.now();
      await producer.send({ topic, messages: batch });
      lat.push(performance.now() - s);
    }
    return summarize(`produce · batched(${BATCH}) · ${SIZE}B`, lat, performance.now() - t0, batches * BATCH);
  });

  await scenario('consume', async () => {
    const consumer = kafka.consumer({ groupId: `bench-${LABEL}-c-${Date.now()}` });
    await consumer.connect();
    await consumer.subscribe({ topic, fromBeginning: true });
    const lat = [];
    let count = 0;
    let last = null;
    const t0 = performance.now();
    try {
      await new Promise((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error('consume timed out')), 60000);
        consumer
          .run({
            autoCommit: false,
            eachMessage: async () => {
              const t = performance.now();
              if (last !== null) lat.push(t - last);
              last = t;
              if (++count >= N) {
                clearTimeout(timer);
                resolve();
              }
            },
          })
          .catch(reject);
      });
      return summarize(`consume · ${SIZE}B`, lat, performance.now() - t0, count);
    } finally {
      await consumer.disconnect().catch(() => {});
    }
  });

  await scenario('end-to-end', async () => {
    const consumer = kafka.consumer({ groupId: `bench-e2e-${LABEL}-${Date.now()}` });
    await consumer.connect();
    await consumer.subscribe({ topic, fromBeginning: false });
    const lat = [];
    let count = 0;
    const M = Math.min(N, 1000);
    try {
      const collected = new Promise((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error('e2e timed out')), 45000);
        consumer
          .run({
            autoCommit: false,
            eachMessage: async ({ message }) => {
              const sent = Number(message.value.toString('latin1').slice(0, 16));
              if (Number.isFinite(sent)) lat.push(performance.now() - sent);
              if (++count >= M) {
                clearTimeout(timer);
                resolve();
              }
            },
          })
          .catch(reject);
      });
      await new Promise((r) => setTimeout(r, 1500)); // let the consumer settle at the tail first
      const t0 = performance.now();
      for (let i = 0; i < M; i++) {
        const stamp = String(performance.now()).padEnd(16, '0').slice(0, 16);
        await producer.send({ topic, messages: [{ value: stamp + body.slice(16) }] });
      }
      await collected;
      return summarize(`end-to-end · ${SIZE}B`, lat, performance.now() - t0, count);
    } finally {
      await consumer.disconnect().catch(() => {});
    }
  });

  await Promise.allSettled([admin.disconnect(), producer.disconnect()]);
  writeFileSync(OUT, `${JSON.stringify({ label: LABEL, scenarios }, null, 2)}\n`);
  console.log(`[bench:${LABEL}] wrote ${OUT} — ${scenarios.length} scenario(s)`);
}

main()
  .then(() => process.exit(0))
  .catch((err) => {
    console.error(`[bench:${LABEL}] ${err.stack || err}`);
    process.exit(1);
  });
