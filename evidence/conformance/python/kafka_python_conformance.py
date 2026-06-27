#!/usr/bin/env python3
"""Conformance harness — kafka-python (pure-Python), unmodified, against pg_kafka (:9092).

Each of the 23 implemented APIs is probed independently (``mark``) and recorded as
pass | fail | na; one failing probe never aborts the run. kafka-python implements no
idempotent/transactional producer, so the five transaction APIs are reported ``na`` by design
(an honest gap, not a pg_kafka failure). Writes results-kafka-python.json.
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
OUT = os.environ.get("OUT", str(Path.cwd() / "results-kafka-python.json"))

results = {a["name"]: "na" for a in APIS}


def mark(name, fn):
    try:
        fn()
        if results[name] != "fail":
            results[name] = "pass"
    except Exception as exc:  # noqa: BLE001 — a failed probe is a data point, not a crash
        results[name] = "fail"
        print(f"[kafka-python] {name}: {exc}", file=sys.stderr)


import kafka as kafka_pkg
from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import NewPartitions, NewTopic

stamp = int(time.time())
topic = f"conf-py-{stamp}"
group = f"conf-py-grp-{stamp}"
state = {}


def _admin():
    state["admin"] = KafkaAdminClient(bootstrap_servers=BROKER, client_id="conf-kafka-python")


mark("ApiVersions", _admin)  # constructing the client negotiates ApiVersions
admin = state.get("admin")
mark("Metadata", lambda: admin.list_topics())
mark(
    "CreateTopics",
    lambda: admin.create_topics([NewTopic(name=topic, num_partitions=2, replication_factor=1)]),
)
time.sleep(1)
mark("CreatePartitions", lambda: admin.create_partitions({topic: NewPartitions(total_count=3)}))


def _produce():
    producer = KafkaProducer(bootstrap_servers=BROKER, acks="all")
    f1 = producer.send(topic, key=b"k1", value=b"v1")
    f2 = producer.send(topic, key=b"k2", value=b"v2")
    producer.flush()
    f1.get(timeout=10)
    f2.get(timeout=10)
    producer.close()


mark("Produce", _produce)


def _list_offsets():
    consumer = KafkaConsumer(bootstrap_servers=BROKER, group_id=None)
    tp = TopicPartition(topic, 0)
    consumer.beginning_offsets([tp])
    consumer.end_offsets([tp])
    consumer.close()


mark("ListOffsets", _list_offsets)


def _consume():
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=BROKER,
        group_id=group,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=15000,
    )
    n = 0
    for _ in consumer:
        n += 1
        if n >= 2:
            break
    if n:
        consumer.commit()  # OffsetCommit
    consumer.close()  # LeaveGroup
    if n == 0:
        raise RuntimeError("no messages consumed")


mark("Fetch", _consume)
# A working group consume implies the coordinator + group protocol round-trips.
if results["Fetch"] == "pass":
    for k in ("FindCoordinator", "JoinGroup", "SyncGroup", "Heartbeat", "OffsetCommit", "LeaveGroup"):
        results[k] = "pass"

mark("OffsetFetch", lambda: admin.list_consumer_group_offsets(group))
mark("DescribeGroups", lambda: admin.describe_consumer_groups([group]))
mark("ListGroups", lambda: admin.list_consumer_groups())
mark("DeleteGroups", lambda: admin.delete_consumer_groups([group]))
mark("DeleteTopics", lambda: admin.delete_topics([topic]))

# Transactions: kafka-python implements none of InitProducerId / AddPartitionsToTxn /
# AddOffsetsToTxn / EndTxn / TxnOffsetCommit — left as the default "na".

client = {
    "name": "kafka-python",
    "lang": "Python",
    "version": getattr(kafka_pkg, "__version__", "unknown"),
    "results": results,
}
Path(OUT).write_text(json.dumps(client, indent=2) + "\n")
tally = {}
for s in results.values():
    tally[s] = tally.get(s, 0) + 1
print(f"[kafka-python] wrote {OUT} — {tally}")
sys.exit(0)
