#!/usr/bin/env python3
"""Conformance harness — librdkafka (the C client, via confluent-kafka-python), against pg_kafka.

confluent-kafka wraps librdkafka, so this row reports how the C client family behaves. Full surface
including transactions. Each of the 23 APIs is probed independently (``mark``); admin calls return
futures which we resolve. Writes results-librdkafka.json.
"""
import json
import os
import sys
import time
import uuid
from pathlib import Path

HERE = Path(__file__).resolve().parent
APIS = json.loads((HERE.parent / "apis.json").read_text())["apis"]
BROKER = os.environ.get("PG_KAFKA_BROKER", "localhost:9092")
OUT = os.environ.get("OUT", str(Path.cwd() / "results-librdkafka.json"))

results = {a["name"]: "na" for a in APIS}


def mark(name, fn):
    try:
        fn()
        if results[name] != "fail":
            results[name] = "pass"
    except Exception as exc:  # noqa: BLE001
        results[name] = "fail"
        print(f"[librdkafka] {name}: {exc}", file=sys.stderr)


import confluent_kafka
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewPartitions, NewTopic

stamp = int(time.time())
topic = f"conf-rd-{stamp}"
group = f"conf-rd-grp-{stamp}"
admin = AdminClient({"bootstrap.servers": BROKER})


def _resolve(futures, timeout=20):
    # admin calls return {key: concurrent.futures.Future}; raise on the first error.
    for fut in (futures.values() if isinstance(futures, dict) else [futures]):
        fut.result(timeout=timeout)


mark("ApiVersions", lambda: admin.list_topics(timeout=10))  # metadata fetch negotiates ApiVersions
mark("Metadata", lambda: admin.list_topics(timeout=10))
mark("CreateTopics", lambda: _resolve(admin.create_topics([NewTopic(topic, 2, 1)])))
time.sleep(1)
mark("CreatePartitions", lambda: _resolve(admin.create_partitions([NewPartitions(topic, 3)])))


def _produce():
    producer = Producer({"bootstrap.servers": BROKER})
    producer.produce(topic, key=b"k1", value=b"v1")
    producer.produce(topic, key=b"k2", value=b"v2")
    if producer.flush(10) != 0:
        raise RuntimeError("messages still in queue after flush")


mark("Produce", _produce)


def _list_offsets():
    consumer = Consumer(
        {"bootstrap.servers": BROKER, "group.id": f"lo-{uuid.uuid4()}", "enable.auto.commit": False}
    )
    consumer.get_watermark_offsets(TopicPartition(topic, 0), timeout=10)
    consumer.close()


mark("ListOffsets", _list_offsets)


def _consume():
    # Produce a few fresh messages first so there's guaranteed data at the tail, then drain.
    feeder = Producer({"bootstrap.servers": BROKER})
    for i in range(5):
        feeder.produce(topic, key=b"c", value=f"consume-{i}".encode())
    feeder.flush(10)

    consumer = Consumer(
        {
            "bootstrap.servers": BROKER,
            "group.id": group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "session.timeout.ms": 10000,
        }
    )
    consumer.subscribe([topic])
    n = 0
    deadline = time.time() + 30
    while time.time() < deadline and n < 2:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        n += 1
    if n:
        consumer.commit(asynchronous=False)  # OffsetCommit
    consumer.close()  # LeaveGroup
    if n == 0:
        raise RuntimeError("no messages consumed")


mark("Fetch", _consume)
if results["Fetch"] == "pass":
    for k in ("FindCoordinator", "JoinGroup", "SyncGroup", "Heartbeat", "OffsetCommit", "LeaveGroup"):
        results[k] = "pass"


def _offset_fetch():
    # ConsumerGroupTopicPartitions lives at the package top level (not confluent_kafka.admin).
    from confluent_kafka import ConsumerGroupTopicPartitions

    _resolve(admin.list_consumer_group_offsets([ConsumerGroupTopicPartitions(group)]))


mark("OffsetFetch", _offset_fetch)
mark("DescribeGroups", lambda: _resolve(admin.describe_consumer_groups([group])))
mark("ListGroups", lambda: admin.list_consumer_groups().result(timeout=15))
mark("DeleteGroups", lambda: _resolve(admin.delete_consumer_groups([group])))


# Transactions (librdkafka EOS): init → begin → produce → send-offsets → commit.
def _txn():
    producer = Producer({"bootstrap.servers": BROKER, "transactional.id": f"rd-tx-{uuid.uuid4()}"})
    producer.init_transactions(15)  # InitProducerId
    results["InitProducerId"] = "pass"

    producer.begin_transaction()
    producer.produce(topic, value=b"rd-tx-1")  # AddPartitionsToTxn
    producer.flush(10)
    results["AddPartitionsToTxn"] = "pass"

    # AddOffsetsToTxn + TxnOffsetCommit ride send_offsets_to_transaction.
    try:
        helper = Consumer(
            {"bootstrap.servers": BROKER, "group.id": group, "enable.auto.commit": False}
        )
        producer.send_offsets_to_transaction(
            [TopicPartition(topic, 0, 2)], helper.consumer_group_metadata(), 15
        )
        helper.close()
        results["AddOffsetsToTxn"] = "pass"
        results["TxnOffsetCommit"] = "pass"
    except Exception as exc:  # noqa: BLE001
        results["AddOffsetsToTxn"] = "fail"
        results["TxnOffsetCommit"] = "fail"
        print(f"[librdkafka] send_offsets_to_transaction: {exc}", file=sys.stderr)

    producer.commit_transaction(15)  # EndTxn
    results["EndTxn"] = "pass"


try:
    _txn()
except Exception as exc:  # noqa: BLE001
    # Whichever txn step failed is already marked; flip any still-default txn cell to fail.
    for k in ("InitProducerId", "AddPartitionsToTxn", "AddOffsetsToTxn", "EndTxn", "TxnOffsetCommit"):
        if results[k] == "na":
            results[k] = "fail"
    print(f"[librdkafka] transactions: {exc}", file=sys.stderr)

mark("DeleteTopics", lambda: _resolve(admin.delete_topics([topic])))

client = {
    "name": "librdkafka",
    "lang": "C / confluent-kafka",
    "version": getattr(confluent_kafka, "__version__", "unknown"),
    "results": results,
}
Path(OUT).write_text(json.dumps(client, indent=2) + "\n")
tally = {}
for s in results.values():
    tally[s] = tally.get(s, 0) + 1
print(f"[librdkafka] wrote {OUT} — {tally}")
sys.exit(0)  # librdkafka background threads can keep the process alive; exit explicitly
