// Shadow-mode forwarding compliance harness. Proves — as regenerated CI evidence, not a claim — that
// pg_kafka forwards to an external broker per its per-topic shadow config. For each scenario it drives
// the *live* forwarding path (produce to pg_kafka :9092, config-reload the bgworker, then independently
// consume the real broker :9093 to confirm delivery) and reads pg_kafka's own accounting from
// kafka.shadow_status / kafka.shadow_tracking. Writes a raw result ({config, checks, counts}) that
// assemble.mjs stamps into shadow.json — degrading to a "pending" shape rather than a fabricated green.
//
// Every scenario logs its full number set (broker delivered, local stored, forwarded metrics, outbox
// finalized/pending, failed, retries, lag) and embeds it as `detail` — so a red cell is debuggable from
// the artifact alone (never-enqueued vs still-pending vs dead-lettered vs under-delivered).
//
// Each scenario is independently try/caught (house style, cf. bench.mjs): one flake colours its own cell
// `fail`, never aborts the run. kafkajs keeps the event loop alive, so the process exits explicitly.
//
// Env: PG_KAFKA_BROKER (:9092), REAL_BROKER (:9093), libpq (PGHOST/PGPORT/PGUSER/PGDATABASE), OUT, RECORDS.
import pg from 'pg';
import { Kafka, logLevel } from 'kafkajs';
import { writeFileSync } from 'node:fs';
import { performance } from 'node:perf_hooks';

const PG_KAFKA_BROKER = process.env.PG_KAFKA_BROKER || 'localhost:9092';
const REAL_BROKER = process.env.REAL_BROKER || 'localhost:9093';
const OUT = process.env.OUT || 'shadow-raw.json';
const N = Number(process.env.RECORDS || 500);
const RELOAD_WAIT_MS = Number(process.env.RELOAD_WAIT_MS || 3000); // > config_reload_interval_ms (2s)
const FORWARD_DEADLINE_MS = Number(process.env.FORWARD_DEADLINE_MS || 30000); // outbox polls at 250ms
const SETTLE_MS = Number(process.env.SETTLE_MS || 10000); // dwell for the partial / must-not-forward cases
const stamp = Date.now();
const body = Buffer.alloc(256, 0x78).toString('latin1');

const kafkaPg = new Kafka({ clientId: 'shadow-evi-pg', brokers: [PG_KAFKA_BROKER], logLevel: logLevel.NOTHING, retry: { retries: 5 } });
const kafkaReal = new Kafka({ clientId: 'shadow-evi-real', brokers: [REAL_BROKER], logLevel: logLevel.NOTHING, retry: { retries: 5 } });
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const between = (x, lo, hi) => x >= lo && x <= hi;

// ── DB helpers (single client; libpq env) ────────────────────────────────────────────────────────
async function topicId(db, name) {
  const r = await db.query('SELECT id FROM kafka.topics WHERE name = $1', [name]);
  if (!r.rows.length) throw new Error(`topic ${name} not found in kafka.topics`);
  return r.rows[0].id;
}

// Upsert the per-topic shadow config, then SIGHUP. Ordering mirrors kafka_test enable_shadow_mode:
// the config row must be committed BEFORE pg_reload_conf() so the bgworker sees it on reload; the
// shadow GUCs are set once in main() via ALTER SYSTEM (the reader is the bgworker, not this session).
async function applyShadowConfig(db, id, { mode = 'shadow', pct = 100, external = null, sync = 'sync', write = 'dual_write' }) {
  await db.query(
    `INSERT INTO kafka.shadow_config
       (topic_id, mode, forward_percentage, external_topic_name, sync_mode, write_mode, updated_at)
     VALUES ($1, $2, $3, $4, $5, $6, NOW())
     ON CONFLICT (topic_id) DO UPDATE SET mode = EXCLUDED.mode,
       forward_percentage = EXCLUDED.forward_percentage, external_topic_name = EXCLUDED.external_topic_name,
       sync_mode = EXCLUDED.sync_mode, write_mode = EXCLUDED.write_mode, updated_at = NOW()`,
    [id, mode, pct, external, sync, write],
  );
  await db.query('SELECT pg_reload_conf()');
  await sleep(RELOAD_WAIT_MS); // let the bgworker pick up the new config on its (2s) reload poll
}

// shadow_status is a per-topic aggregate view; select only the granted subset.
async function readStatus(db, name) {
  const r = await db.query(
    `SELECT total_forwarded, total_skipped, total_failed, last_forwarded_offset, lag
       FROM kafka.shadow_status WHERE topic_name = $1`,
    [name],
  );
  return r.rows[0] ?? { total_forwarded: 0, total_skipped: 0, total_failed: 0, last_forwarded_offset: -1, lag: 0 };
}

// Outbox rows: external_offset IS NOT NULL = a forward finalized; IS NULL = still pending. error_message
// is withheld from PUBLIC (SEC-8), so never SELECT * here — only the granted columns (retry_count is one).
async function readTracking(db, id) {
  const r = await db.query(
    `SELECT count(*) FILTER (WHERE external_offset IS NOT NULL)::int AS finalized,
            count(*) FILTER (WHERE external_offset IS NULL)::int     AS pending,
            COALESCE(SUM(retry_count), 0)::int                        AS retries
       FROM kafka.shadow_tracking WHERE topic_id = $1`,
    [id],
  );
  return r.rows[0];
}

async function readLocalStored(db, id) {
  const r = await db.query('SELECT count(*)::int AS n FROM kafka.messages WHERE topic_id = $1', [id]);
  return r.rows[0].n;
}

// pg_kafka's full self-reported accounting for a topic.
async function gather(db, name, id) {
  const [s, t, local] = await Promise.all([readStatus(db, name), readTracking(db, id), readLocalStored(db, id)]);
  return {
    forwardedMetrics: Number(s.total_forwarded),
    skipped: Number(s.total_skipped),
    failed: Number(s.total_failed),
    lag: Number(s.lag),
    finalized: t.finalized,
    pending: t.pending,
    retries: t.retries,
    localStored: local,
  };
}

// Poll until pg_kafka reaches a terminal forwarding state for `expect`: all finalized, or the outbox
// has drained to a terminal mix of finalized+failed with nothing pending, or the deadline elapses.
async function waitForForwarding(db, name, id, expect, deadlineMs) {
  const t0 = performance.now();
  let g = await gather(db, name, id);
  while (
    g.finalized < expect &&
    !(g.pending === 0 && g.finalized + g.failed >= expect) &&
    performance.now() - t0 < deadlineMs
  ) {
    await sleep(1000);
    g = await gather(db, name, id);
  }
  return g;
}

// ── independent confirmation: consume the real broker ─────────────────────────────────────────────
// Returns how many records actually landed. Resolves as soon as `min` arrive, else after `quietMs` of
// silence (for the percentage / aborted cases where the arriving count is intentionally < produced).
async function countExternal(topic, { min = null, quietMs = 6000, hardMs = 30000 }) {
  const consumer = kafkaReal.consumer({ groupId: `shadow-evi-count-${topic}` });
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });
  let count = 0;
  let lastAt = performance.now();
  await new Promise((resolve) => {
    const hard = setTimeout(resolve, hardMs);
    const tick = setInterval(() => {
      if ((min !== null && count >= min) || performance.now() - lastAt > quietMs) {
        clearTimeout(hard);
        clearInterval(tick);
        resolve();
      }
    }, 500);
    consumer
      .run({ autoCommit: false, eachMessage: async () => { count += 1; lastAt = performance.now(); } })
      .catch(() => { clearTimeout(hard); clearInterval(tick); resolve(); });
  });
  await consumer.disconnect().catch(() => {});
  return count;
}

// ── producers ─────────────────────────────────────────────────────────────────────────────────────
async function produceN(topic, n) {
  const producer = kafkaPg.producer({ allowAutoTopicCreation: false });
  await producer.connect();
  try {
    const BATCH = 100;
    for (let i = 0; i < n; i += BATCH) {
      const messages = Array.from({ length: Math.min(BATCH, n - i) }, (_, k) => ({ value: `${i + k}:${body}` }));
      await producer.send({ topic, messages });
    }
  } finally {
    await producer.disconnect().catch(() => {});
  }
}

async function produceTxn(topic, n, commit) {
  const producer = kafkaPg.producer({
    transactionalId: `shadow-evi-txn-${topic}`,
    idempotent: true,
    maxInFlightRequests: 1,
    allowAutoTopicCreation: false,
  });
  await producer.connect();
  try {
    const txn = await producer.transaction();
    try {
      const BATCH = 100;
      for (let i = 0; i < n; i += BATCH) {
        const messages = Array.from({ length: Math.min(BATCH, n - i) }, (_, k) => ({ value: `${i + k}:${body}` }));
        await txn.send({ topic, messages });
      }
      if (commit) await txn.commit();
      else await txn.abort();
    } catch (err) {
      await txn.abort().catch(() => {});
      throw err;
    }
  } finally {
    await producer.disconnect().catch(() => {});
  }
}

// ── scenario prep + evaluation ───────────────────────────────────────────────────────────────────
async function prepTopic(db, key) {
  const topic = `shadow-evi-${key}-${stamp}`;
  const adminPg = kafkaPg.admin();
  const adminReal = kafkaReal.admin();
  await Promise.all([adminPg.connect(), adminReal.connect()]);
  await adminPg.createTopics({ topics: [{ topic, numPartitions: 1 }], waitForLeaders: true });
  // Pre-create on the real broker so the confirming consumer never races an unknown topic (esp. the
  // aborted case, where nothing is ever forwarded to create it).
  await adminReal.createTopics({ topics: [{ topic, numPartitions: 1 }], waitForLeaders: true }).catch(() => {});
  await Promise.allSettled([adminPg.disconnect(), adminReal.disconnect()]);
  const id = await topicId(db, topic);
  return { topic, id };
}

async function main() {
  const db = new pg.Client(); // libpq env
  await db.connect();

  // One-time: point shadow at the real broker + speed the bgworker's config-reload poll. These are
  // Sighup GUCs, so ALTER SYSTEM + pg_reload_conf() applies them without a restart (config.rs QA-8).
  await db.query('ALTER SYSTEM SET pg_kafka.shadow_mode_enabled = true');
  await db.query(`ALTER SYSTEM SET pg_kafka.shadow_bootstrap_servers = '${REAL_BROKER}'`);
  await db.query(`ALTER SYSTEM SET pg_kafka.shadow_security_protocol = 'PLAINTEXT'`);
  await db.query('ALTER SYSTEM SET pg_kafka.config_reload_interval_ms = 2000');
  await db.query('SELECT pg_reload_conf()');
  await sleep(RELOAD_WAIT_MS);

  const checks = [];
  let counts = null;
  const record = (name, status, detail) => {
    checks.push({ name, status, detail });
    const d = detail || {};
    console.log(
      `  ${name}: ${status}  [broker=${d.forwardedToBroker} local=${d.localStored} metrics=${d.forwardedMetrics} ` +
        `finalized=${d.finalized} pending=${d.pending} failed=${d.failed} retries=${d.retries} lag=${d.lag}]`,
    );
  };

  // dual_write_sync — the headline 100% parity scenario; its numbers become the artifact's `counts`.
  try {
    const { topic, id } = await prepTopic(db, 'dual-write-sync');
    await applyShadowConfig(db, id, { sync: 'sync', write: 'dual_write', pct: 100 });
    await produceN(topic, N);
    const g = await waitForForwarding(db, topic, id, N, FORWARD_DEADLINE_MS);
    const forwardedToBroker = await countExternal(topic, { min: N });
    const detail = { produced: N, forwardedToBroker, ...g };
    counts = {
      produced: N,
      forwardedToBroker,
      localStored: g.localStored,
      shadowMetricsForwarded: g.forwardedMetrics,
      skipped: g.skipped,
      failed: g.failed,
      outboxFinalized: g.finalized,
      lag: g.lag,
    };
    record('dual_write_sync', forwardedToBroker >= N && g.localStored === N && g.finalized >= N ? 'pass' : 'fail', detail);
  } catch (err) {
    console.error(`  [dual_write_sync] ${err.message}`);
    record('dual_write_sync', 'fail', null);
  }

  // dual_write_async — forwarding off the ack path; still full parity once the outbox drains.
  try {
    const { topic, id } = await prepTopic(db, 'dual-write-async');
    await applyShadowConfig(db, id, { sync: 'async', write: 'dual_write', pct: 100 });
    await produceN(topic, N);
    const g = await waitForForwarding(db, topic, id, N, FORWARD_DEADLINE_MS);
    const forwardedToBroker = await countExternal(topic, { min: N });
    record('dual_write_async', forwardedToBroker >= N && g.localStored === N && g.finalized >= N ? 'pass' : 'fail', {
      produced: N,
      forwardedToBroker,
      ...g,
    });
  } catch (err) {
    console.error(`  [dual_write_async] ${err.message}`);
    record('dual_write_async', 'fail', null);
  }

  // external_only — forwards without dual-writing. Definite property: forwarding still reaches 100%
  // (localStored is recorded for context, not asserted — its semantics vary with dead-letter reads).
  try {
    const { topic, id } = await prepTopic(db, 'external-only');
    await applyShadowConfig(db, id, { sync: 'sync', write: 'external_only', pct: 100 });
    await produceN(topic, N);
    const g = await waitForForwarding(db, topic, id, N, FORWARD_DEADLINE_MS);
    const forwardedToBroker = await countExternal(topic, { min: N });
    record('external_only', forwardedToBroker >= N && g.finalized >= N ? 'pass' : 'fail', { produced: N, forwardedToBroker, ...g });
  } catch (err) {
    console.error(`  [external_only] ${err.message}`);
    record('external_only', 'fail', null);
  }

  // percentage_50 — sampled forwarding. Wide band around N/2 (binomial 3σ for N=500 is ±33). Banded on
  // pg_kafka's own forwarded metric (stabler than a partial external consume).
  try {
    const { topic, id } = await prepTopic(db, 'percentage-50');
    await applyShadowConfig(db, id, { sync: 'sync', write: 'dual_write', pct: 50 });
    await produceN(topic, N);
    await sleep(SETTLE_MS);
    const g = await gather(db, topic, id);
    const forwardedToBroker = await countExternal(topic, { min: null, quietMs: 6000 });
    record('percentage_50', between(g.forwardedMetrics, Math.floor(N * 0.3), Math.ceil(N * 0.7)) ? 'pass' : 'fail', {
      produced: N,
      forwardedToBroker,
      ...g,
    });
  } catch (err) {
    console.error(`  [percentage_50] ${err.message}`);
    record('percentage_50', 'fail', null);
  }

  // committed_txn_forwarded — records in a committed txn are forwarded on commit.
  try {
    const { topic, id } = await prepTopic(db, 'committed-txn');
    await applyShadowConfig(db, id, { sync: 'sync', write: 'dual_write', pct: 100 });
    await produceTxn(topic, N, true);
    const g = await waitForForwarding(db, topic, id, N, FORWARD_DEADLINE_MS);
    const forwardedToBroker = await countExternal(topic, { min: N });
    record('committed_txn_forwarded', forwardedToBroker >= N && g.finalized >= N ? 'pass' : 'fail', { produced: N, forwardedToBroker, ...g });
  } catch (err) {
    console.error(`  [committed_txn_forwarded] ${err.message}`);
    record('committed_txn_forwarded', 'fail', null);
  }

  // aborted_txn_not_forwarded — records in an aborted txn are NEVER forwarded (read-committed).
  try {
    const { topic, id } = await prepTopic(db, 'aborted-txn');
    await applyShadowConfig(db, id, { sync: 'sync', write: 'dual_write', pct: 100 });
    await produceTxn(topic, N, false);
    await sleep(SETTLE_MS);
    const g = await gather(db, topic, id);
    const forwardedToBroker = await countExternal(topic, { min: 0, quietMs: 6000 });
    record('aborted_txn_not_forwarded', forwardedToBroker === 0 && g.forwardedMetrics === 0 && g.finalized === 0 ? 'pass' : 'fail', {
      produced: N,
      forwardedToBroker,
      ...g,
    });
  } catch (err) {
    console.error(`  [aborted_txn_not_forwarded] ${err.message}`);
    record('aborted_txn_not_forwarded', 'fail', null);
  }

  await db.end().catch(() => {});

  const raw = {
    config: { records: N, forwardPercentage: 100, writeMode: 'dual_write', syncMode: 'sync', realBroker: REAL_BROKER },
    checks,
    counts: counts ?? { produced: N, forwardedToBroker: 0, localStored: 0, shadowMetricsForwarded: 0, skipped: 0, failed: 0, outboxFinalized: 0, lag: 0 },
  };
  writeFileSync(OUT, `${JSON.stringify(raw, null, 2)}\n`);
  console.log(`[shadow] wrote ${OUT} — ${checks.length} checks, ${checks.filter((c) => c.status === 'pass').length} pass`);
}

main()
  .then(() => process.exit(0))
  .catch((err) => {
    console.error(`[shadow] ${err.stack || err}`);
    process.exit(1);
  });
