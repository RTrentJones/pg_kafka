// Conformance harness — kafkajs (Node), unmodified, against pg_kafka on PG_KAFKA_BROKER (:9092).
//
// For each of the 23 implemented Kafka APIs we run the kafkajs operation that exercises it and
// record pass | fail | na. Every probe is independently try/caught (`mark`), so one failing API
// never aborts the run — it just colours that one cell. Some wire APIs are exercised implicitly by
// a higher-level op (a successful group consume implies FindCoordinator/SyncGroup/Heartbeat); those
// are inferred from the op that drives them. Writes results-kafkajs.json (the merge step folds all
// clients into conformance.json).
import { Kafka, logLevel } from 'kafkajs';
import { readFileSync, writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const here = dirname(fileURLToPath(import.meta.url));
const apis = JSON.parse(readFileSync(join(here, '..', 'apis.json'), 'utf8')).apis;
const pkg = JSON.parse(readFileSync(join(here, 'package.json'), 'utf8'));
const BROKER = process.env.PG_KAFKA_BROKER || 'localhost:9092';
const OUT = process.env.OUT || join(process.cwd(), 'results-kafkajs.json');

const results = Object.fromEntries(apis.map((a) => [a.name, 'na']));
async function mark(name, fn) {
  try {
    await fn();
    if (results[name] !== 'fail') results[name] = 'pass';
  } catch (err) {
    results[name] = 'fail';
    console.error(`[kafkajs] ${name}: ${err.message}`);
  }
}

const stamp = Date.now();
const topic = `conf-kjs-${stamp}`;
const groupId = `conf-kjs-grp-${stamp}`;
const kafka = new Kafka({
  clientId: 'conf-kafkajs',
  brokers: [BROKER],
  logLevel: logLevel.NOTHING,
  retry: { retries: 3, initialRetryTime: 300 },
});
const admin = kafka.admin();
const producer = kafka.producer();
const txProducer = kafka.producer({
  transactionalId: `conf-kjs-tx-${stamp}`,
  idempotent: true,
  maxInFlightRequests: 1,
});
const consumer = kafka.consumer({ groupId });

try {
  // Connecting negotiates ApiVersions; admin metadata calls exercise Metadata.
  await mark('ApiVersions', () => admin.connect());
  await mark('Metadata', () => admin.listTopics());
  await mark('CreateTopics', () =>
    admin.createTopics({ topics: [{ topic, numPartitions: 2 }], waitForLeaders: true }),
  );
  await mark('CreatePartitions', () =>
    admin.createPartitions({ topicPartitions: [{ topic, count: 3 }] }),
  );

  await mark('Produce', async () => {
    await producer.connect();
    await producer.send({
      topic,
      messages: [
        { key: 'k1', value: 'v1' },
        { key: 'k2', value: 'v2' },
      ],
    });
  });
  await mark('ListOffsets', () => admin.fetchTopicOffsets(topic));

  await mark('JoinGroup', () => consumer.connect());
  let received = 0;
  await mark('Fetch', async () => {
    await consumer.subscribe({ topic, fromBeginning: true });
    await new Promise((resolve, reject) => {
      const timer = setTimeout(
        () => (received > 0 ? resolve() : reject(new Error('no messages within 15s'))),
        15000,
      );
      consumer
        .run({
          eachMessage: async () => {
            if (++received >= 2) {
              clearTimeout(timer);
              resolve();
            }
          },
        })
        .catch(reject);
    });
  });
  // A working group consume means the coordinator handshake + group protocol all succeeded.
  if (results.JoinGroup === 'pass' && results.Fetch === 'pass') {
    results.FindCoordinator = 'pass';
    results.SyncGroup = 'pass';
    results.Heartbeat = 'pass';
  }
  await mark('OffsetCommit', () =>
    consumer.commitOffsets([{ topic, partition: 0, offset: '1' }]),
  );
  await mark('OffsetFetch', () => admin.fetchOffsets({ groupId, topics: [topic] }));
  await mark('DescribeGroups', () => admin.describeGroups([groupId]));
  await mark('ListGroups', () => admin.listGroups());
  await mark('LeaveGroup', () => consumer.disconnect());
  await mark('DeleteGroups', () => admin.deleteGroups([groupId]));

  // Transactions: connecting a transactional producer drives InitProducerId; a committed
  // transactional send drives AddPartitionsToTxn + EndTxn; sendOffsets drives AddOffsetsToTxn +
  // TxnOffsetCommit.
  await mark('InitProducerId', () => txProducer.connect());
  await mark('AddPartitionsToTxn', async () => {
    const tx = await txProducer.transaction();
    try {
      await tx.send({ topic, messages: [{ value: 'tx-1' }] });
      await tx.commit();
    } catch (err) {
      await tx.abort().catch(() => {});
      throw err;
    }
  });
  if (results.AddPartitionsToTxn === 'pass') results.EndTxn = 'pass';
  await mark('AddOffsetsToTxn', async () => {
    const tx = await txProducer.transaction();
    try {
      await tx.send({ topic, messages: [{ value: 'tx-2' }] });
      await tx.sendOffsets({
        consumerGroupId: groupId,
        topics: [{ topic, partitions: [{ partition: 0, offset: '2' }] }],
      });
      await tx.commit();
    } catch (err) {
      await tx.abort().catch(() => {});
      throw err;
    }
  });
  if (results.AddOffsetsToTxn === 'pass') results.TxnOffsetCommit = 'pass';

  await mark('DeleteTopics', () => admin.deleteTopics({ topics: [topic] }));
} finally {
  await Promise.allSettled([
    admin.disconnect(),
    producer.disconnect(),
    txProducer.disconnect(),
    consumer.disconnect(),
  ]);
}

const client = {
  name: 'kafkajs',
  lang: 'Node',
  version: (pkg.dependencies?.kafkajs || '').replace(/[^0-9.]/g, '') || 'latest',
  results,
};
writeFileSync(OUT, `${JSON.stringify(client, null, 2)}\n`);
const tally = Object.values(results).reduce((a, s) => ((a[s] = (a[s] || 0) + 1), a), {});
console.log(`[kafkajs] wrote ${OUT} —`, tally);
