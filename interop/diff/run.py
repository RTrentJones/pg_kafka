#!/usr/bin/env python3
"""Differential test: the same raw request sequences against pg_kafka and Apache Kafka.

Each scenario below runs once against each broker through KafkaWire (Kafka's own generated
protocol classes, see KafkaWire.java), records named observations, normalises them (broker ids,
cluster ids, hosts, throttle times, timestamps, error messages, generated member/producer ids)
and compares pg_kafka's against Kafka's.

Every difference must be classified in KNOWN:
  ("deviation", "PROTOCOL_DEVIATIONS.md, <section>")  documented, intentional: reported DEV
  ("bug", "<the defect>")                            a defect: reported XFAIL (XPASS once fixed)
An unclassified difference fails the run. A classified observation that no longer differs is
reported XPASS / DEV-GONE (the run still passes) so the entry can be removed.

  interop/diff/run.py --pg 127.0.0.1:9092 --kafka 127.0.0.1:9093 [--only scenario] [--show]

Needs KAFKA_HOME (kafka_2.13-3.7.0: its libs/ provide the protocol classes), java and javac.
"""
import argparse
import base64
import json
import os
import struct
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

HERE = Path(__file__).resolve().parent
KAFKA_HOME = Path(os.environ.get("KAFKA_HOME", "/var/tmp/kafka/kafka_2.13-3.7.0"))
RUN = uuid.uuid4().hex[:8]

ERRORS = {0: "NONE", 1: "OFFSET_OUT_OF_RANGE", 3: "UNKNOWN_TOPIC_OR_PARTITION",
          14: "COORDINATOR_LOAD_IN_PROGRESS", 15: "COORDINATOR_NOT_AVAILABLE",
          16: "NOT_COORDINATOR", 17: "INVALID_TOPIC_EXCEPTION", 21: "INVALID_REQUIRED_ACKS",
          22: "ILLEGAL_GENERATION", 23: "INCONSISTENT_GROUP_PROTOCOL", 24: "INVALID_GROUP_ID",
          25: "UNKNOWN_MEMBER_ID", 26: "INVALID_SESSION_TIMEOUT", 27: "REBALANCE_IN_PROGRESS",
          35: "UNSUPPORTED_VERSION", 36: "TOPIC_ALREADY_EXISTS", 37: "INVALID_PARTITIONS",
          38: "INVALID_REPLICATION_FACTOR", 42: "INVALID_REQUEST", 45: "OUT_OF_ORDER_SEQUENCE_NUMBER",
          46: "DUPLICATE_SEQUENCE_NUMBER", 47: "INVALID_PRODUCER_EPOCH", 48: "INVALID_TXN_STATE",
          49: "INVALID_PRODUCER_ID_MAPPING", 50: "INVALID_TRANSACTION_TIMEOUT",
          53: "TRANSACTIONAL_ID_AUTHORIZATION_FAILED", 68: "NON_EMPTY_GROUP",
          69: "GROUP_ID_NOT_FOUND", 79: "MEMBER_ID_REQUIRED", 88: "UNSTABLE_OFFSET_COMMIT",
          90: "PRODUCER_FENCED"}

# (scenario, observation) -> (kind, reason). A deviation points at where it is documented; a
# bug says what pg_kafka does wrong.
DEV_MARKERS = ("deviation", "PROTOCOL_DEVIATIONS.md §4: no transaction markers are written, so "
               "offsets after a transaction are one lower per committed/aborted transaction")
KNOWN = {
    # ---- documented deviations
    ("metadata", "create_reports_configs"): ("deviation", "PROTOCOL_DEVIATIONS.md §13: topic "
                                             "configs are not reported by CreateTopics"),
    ("metadata", "v10_topic_id"): ("deviation", "PROTOCOL_DEVIATIONS.md, API Version Support: "
                                   "Metadata v10 topic_id is the zero UUID"),
    ("groups", "join.first_join"): ("deviation", "PROTOCOL_DEVIATIONS.md §5: no "
                                    "MEMBER_ID_REQUIRED (KIP-394), a first JoinGroup is admitted"),
    ("groups", "join_consumer.first_join"): ("deviation", "PROTOCOL_DEVIATIONS.md §5: no "
                                             "MEMBER_ID_REQUIRED (KIP-394)"),
    ("produce_idempotent_partitions", "p3_old_epoch_first"): (
        "deviation", "PROTOCOL_DEVIATIONS.md §3: the epoch is kept per producer, so an older "
        "epoch is stale on a partition without producer state too"),
    ("transactions", "produce_a"): DEV_MARKERS,
    ("transactions", "lo_latest_uncommitted"): DEV_MARKERS,
    ("transactions", "fetch_uncommitted_from_0.offsets"): DEV_MARKERS,
    ("transactions", "fetch_uncommitted_at_aborted_tail.offsets"): DEV_MARKERS,
    ("transactions", "lo_latest_committed"): DEV_MARKERS,
    ("transactions", "fetch_committed_at_aborted_tail.offsets"): DEV_MARKERS,
    ("transactions", "fetch_committed_from_0.offsets"): (
        "deviation", "PROTOCOL_DEVIATIONS.md §7: aborted records are filtered server-side, so "
        "the fetch ends at the last visible record and the control batch covering the aborted "
        "tail comes with the next fetch (and §4: no markers)"),
    ("transactions", "batch_layout"): (
        "deviation", "PROTOCOL_DEVIATIONS.md §7: records are regrouped at read time into one "
        "batch per run of a producer's consecutive offsets (and §4: no control batches)"),
}

# Response fields that differ between any two brokers and carry no protocol meaning here.
DROP = {"throttleTimeMs", "clusterId", "controllerId", "errorMessage", "clusterAuthorizedOperations",
        "topicAuthorizedOperations", "authorizedOperations", "logAppendTimeMs", "rack",
        "leaderEpoch", "currentLeader", "nodeEndpoints", "preferredReadReplica", "sessionId",
        "divergingEpoch", "snapshotId", "recordErrors", "supportedFeatures", "finalizedFeatures",
        "finalizedFeaturesEpoch", "zkMigrationReady", "timestamp"}
BROKER_ID = {"nodeId", "leaderId", "coordinatorKey"}
SYMBOLIC = {"memberId", "producerId", "leader", "groupInstanceId"}


class Target:
    def __init__(self, name, addr, classes):
        self.name = name
        self.p = subprocess.Popen(
            ["java", "-Xmx256m", "-cp", f"{KAFKA_HOME}/libs/*:{classes}", "KafkaWire", addr],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
        self.symbols = {}
        # Kafka creates __consumer_offsets / __transaction_state on the first FindCoordinator
        # for a group / transactional id; until then every coordinator request gets
        # NOT_COORDINATOR. Warm both coordinators up front (a no-op for pg_kafka).
        for key_type in (0, 1):
            for _ in range(120):
                r = self.raw({"conn": "warm", "api": 10, "version": 3,
                              "body": {"key": "pgk-diff-warmup", "keyType": key_type}})
                if r.get("body", {}).get("errorCode") == 0:
                    break
                time.sleep(0.5)
        self.close("warm")

    def raw(self, cmd):
        self.p.stdin.write(json.dumps(cmd) + "\n")
        self.p.stdin.flush()
        line = self.p.stdout.readline()
        if not line:
            raise RuntimeError(f"KafkaWire for {self.name} died")
        return json.loads(line)

    def req(self, api, version, body=None, conn="a", retry=True):
        """Send a request, retrying while the coordinator is still loading (Kafka creates
        __consumer_offsets / __transaction_state on first use)."""
        for _ in range(40):
            r = self.raw({"conn": conn, "api": api, "version": version, "body": body or {}})
            if not retry or "body" not in r or not (
                    r["body"].get("errorCode") in (14, 15, 16) or has_code(r["body"], 51)):
                return r
            time.sleep(0.5)
        return r

    def send(self, api, version, body, conn="a"):
        return self.raw({"conn": conn, "op": "send", "api": api, "version": version, "body": body})

    def recv(self, api, version, conn="a"):
        return self.raw({"conn": conn, "op": "recv", "api": api, "version": version})

    def close(self, conn="a"):
        self.raw({"conn": conn, "op": "close"})

    def sym(self, kind, value):
        # None, "" and -1 mean "no id"; 0 is a real producer id (Kafka's first).
        if value in (None, "", -1):
            return value
        key = (kind, value)
        if key not in self.symbols:
            self.symbols[key] = f"<{kind}#{sum(1 for k in self.symbols if k[0] == kind) + 1}>"
        return self.symbols[key]


def has_code(obj, code):
    """Whether any error code in the response is `code` (51 CONCURRENT_TRANSACTIONS: Kafka is
    still completing the previous transaction; clients retry)."""
    if isinstance(obj, dict):
        return any((k.endswith("rrorCode") and v == code) or has_code(v, code)
                   for k, v in obj.items())
    if isinstance(obj, list):
        return any(has_code(x, code) for x in obj)
    return False


ERRORS.update({-1: "UNKNOWN_SERVER_ERROR", 51: "CONCURRENT_TRANSACTIONS",
               59: "UNKNOWN_PRODUCER_ID"})


def errname(code):
    if code is None:
        return None
    return f"{code} {ERRORS.get(code, '?')}"


def join_error(r):
    return errname(r["errorCode"]) if "errorCode" in r else r


def normalise(t, obj):
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            if k in DROP:
                continue
            if k in ("host", "port"):
                out[k] = "<broker>"
            elif k in BROKER_ID and isinstance(v, int):
                out[k] = "<broker>" if v >= 0 else v
            elif k in ("replicaNodes", "isrNodes"):
                out[k] = ["<broker>" for _ in v]
            elif k in SYMBOLIC:
                out[k] = t.sym(k, v)
            elif k == "topicId":
                out[k] = "<zero uuid>" if v == "AAAAAAAAAAAAAAAAAAAAAA" else "<uuid>"
            elif k.endswith("rrorCode") and isinstance(v, int):
                out[k] = errname(v)
            elif isinstance(v, str) and RUN in v:
                out[k] = v.replace(RUN, "<run>")
            else:
                out[k] = normalise(t, v)
        return out
    if isinstance(obj, list):
        out = [normalise(t, x) for x in obj]
        # Kafka returns partitions in no particular order.
        for key in ("partitionIndex", "partition", "index"):
            if out and all(isinstance(x, dict) and key in x for x in out):
                return sorted(out, key=lambda x: x[key])
        return out
    return obj


def body(r):
    if "error" in r:
        return {"transport": r["error"]}
    return r["body"]


def records(values, **kw):
    return dict(records=[{"key": None, "value": v} for v in values], **kw)


def visible(t, part, fetch_offset, isolation):
    """What a consumer gets from one Fetch partition: the records it delivers and the position
    it moves to (Kafka clients drop aborted transactional data via abortedTransactions and the
    abort marker; control batches are skipped but advance the position)."""
    aborted = {a["producerId"]: a["firstOffset"] for a in (part.get("abortedTransactions") or [])}
    out, pos, live_aborted = [], fetch_offset, set()
    for b in part.get("records", []):
        pos = max(pos, b["lastOffset"] + 1)
        if b["isControlBatch"]:
            live_aborted.discard(b["producerId"])
            continue
        if isolation == 1 and b["isTransactional"] and b["producerId"] in aborted \
                and aborted[b["producerId"]] <= b["baseOffset"]:
            live_aborted.add(b["producerId"])
        if b["producerId"] in live_aborted:
            continue
        out += [r["value"] for r in b["records"] if r["offset"] >= fetch_offset]
    return {"errorCode": errname(part["errorCode"]), "records": out}, {
        "highWatermark": part["highWatermark"], "lastStableOffset": part["lastStableOffset"],
        "logStartOffset": part["logStartOffset"], "position": pos}


def obs_fetch(obs, name, part, offset, isolation):
    """Two observations per fetch: what the consumer gets, and the offsets (which move with
    Kafka's transaction markers, PROTOCOL_DEVIATIONS §4)."""
    if part is None:
        obs(name, "transport error")
        return
    data, offsets = visible(None, part, offset, isolation)
    obs(name, data)
    obs(f"{name}.offsets", offsets)


def fetch(t, topic, offset, isolation=0, partition=0, wait=100, version=11):
    r = t.req(1, version, {"replicaId": -1, "maxWaitMs": wait, "minBytes": 1,
                           "maxBytes": 1 << 20, "isolationLevel": isolation,
                           "topics": [{"topic": topic, "partitions": [
                               {"partition": partition, "fetchOffset": offset,
                                "partitionMaxBytes": 1 << 20, "logStartOffset": -1,
                                "currentLeaderEpoch": -1}]}]})
    if "error" in r:
        return r, None
    return r, r["body"]["responses"][0]["partitions"][0]


def produce(t, topic, values, partition=0, acks=-1, txn=None, version=9, **rec):
    return t.req(0, version, {"transactionalId": txn, "acks": acks, "timeoutMs": 10000,
                              "topicData": [{"name": topic, "partitionData": [
                                  {"index": partition, "records": records(values, **rec)}]}]})


def produce_part(r):
    if "error" in r:
        return {"transport": r["error"]}
    p = r["body"]["responses"][0]["partitionResponses"][0]
    return {"errorCode": errname(p["errorCode"]), "baseOffset": p["baseOffset"]}


def produce_log_start(r):
    if "error" in r:
        return {"transport": r["error"]}
    return r["body"]["responses"][0]["partitionResponses"][0]["logStartOffset"]


def strip_configs(resp):
    """CreateTopics without the per-topic configs (compared separately: pg_kafka returns none)."""
    for tp in resp.get("topics", []):
        tp.pop("configs", None)
    return resp


def reports_configs(resp):
    return any(tp.get("configs") for tp in resp.get("topics", []))


def create_topics(t, topics, validate_only=False, version=5):
    return t.req(19, version, {"topics": topics, "timeoutMs": 10000,
                               "validateOnly": validate_only})


def topic_spec(name, partitions=1, rf=1):
    return {"name": name, "numPartitions": partitions, "replicationFactor": rf}


def list_offsets(t, topic, ts, isolation=0, partition=0, version=7):
    r = t.req(2, version, {"replicaId": -1, "isolationLevel": isolation, "topics": [
        {"name": topic, "partitions": [{"partitionIndex": partition, "timestamp": ts,
                                        "currentLeaderEpoch": -1}]}]})
    return r if "error" in r else r["body"]["topics"][0]["partitions"][0]


def init_pid(t, txn=None, timeout=60000):
    r = t.req(22, 4, {"transactionalId": txn, "transactionTimeoutMs": timeout,
                      "producerId": -1, "producerEpoch": -1})
    b = body(r)
    return b, b.get("producerId"), b.get("producerEpoch")


def join(t, group, member="", protocol_type="consumer", protocols=None, conn="a", version=5):
    protocols = protocols if protocols is not None else [
        {"name": "range", "metadata": base64.b64encode(b"\x00\x01\x00\x00\x00\x00\xff\xff\xff\xff").decode()}]
    return t.req(11, version, {"groupId": group, "sessionTimeoutMs": 6000,
                               "rebalanceTimeoutMs": 3000, "memberId": member,
                               "groupInstanceId": None, "protocolType": protocol_type,
                               "protocols": protocols}, conn=conn)


# --------------------------------------------------------------------------- scenarios


def sc_api_versions(t, obs):
    r = t.req(18, 3, {"clientSoftwareName": "pgk-diff", "clientSoftwareVersion": "1"})
    b = body(r)
    obs("error", errname(b.get("errorCode")))
    obs("ranges", {k["apiKey"]: (k["minVersion"], k["maxVersion"]) for k in b.get("apiKeys", [])})


def cmp_api_subset(pg, kafka):
    """pg_kafka's advertised range of every API must lie inside Kafka 3.7's."""
    bad = [f"{k}: {pg[k]} vs {kafka.get(k)}" for k in pg
           if k not in kafka or pg[k][0] < kafka[k][0] or pg[k][1] > kafka[k][1]]
    return bad


def sc_metadata(t, obs):
    topic = f"diff-md-{RUN}"
    r = body(create_topics(t, [topic_spec(topic, 3)]))
    obs("create_reports_configs", reports_configs(r))
    obs("create", normalise(t, strip_configs(r)))
    md = body(t.req(3, 9, {"topics": [{"name": topic}], "allowAutoTopicCreation": False}))
    obs("topic", normalise(t, md.get("topics")))
    obs("brokers", normalise(t, md.get("brokers")))
    md = body(t.req(3, 9, {"topics": [{"name": f"diff-md-missing-{RUN}"}],
                           "allowAutoTopicCreation": False}))
    obs("unknown_topic", normalise(t, md.get("topics")))
    md = body(t.req(3, 9, {"topics": [{"name": "bad name!"}], "allowAutoTopicCreation": True}))
    obs("invalid_name", normalise(t, md.get("topics")))
    md = body(t.req(3, 10, {"topics": [{"name": topic, "topicId": "AAAAAAAAAAAAAAAAAAAAAA"}],
                            "allowAutoTopicCreation": False}))
    obs("v10_topic_id", [x.get("topicId") for x in normalise(t, md.get("topics", []))])


def sc_produce_plain(t, obs):
    topic = f"diff-pp-{RUN}"
    create_topics(t, [topic_spec(topic, 2)])
    r = produce(t, topic, ["a", "b", "c"])
    obs("first", produce_part(r))
    obs("log_start_offset", produce_log_start(r))
    obs("second", produce_part(produce(t, topic, ["d"])))
    obs("unknown_partition", produce_part(produce(t, topic, ["x"], partition=7)))
    obs("acks_2", produce_part(produce(t, topic, ["y"], acks=2)))
    _, p = fetch(t, topic, 0)
    obs_fetch(obs, "fetch", p, 0, 0)
    obs("fetch_out_of_range", errname(fetch(t, topic, 99)[1]["errorCode"]))


def sc_produce_idempotent(t, obs):
    topic = f"diff-pi-{RUN}"
    create_topics(t, [topic_spec(topic, 1)])
    b, pid, ep = init_pid(t)
    obs("init", normalise(t, b))

    def p(values, seq, epoch=None):
        return produce_part(produce(t, topic, values, producerId=pid,
                                    producerEpoch=ep if epoch is None else epoch,
                                    baseSequence=seq))
    obs("seq0", p(["a", "b", "c"], 0))
    obs("retry_seq0", p(["a", "b", "c"], 0))
    obs("gap_seq5", p(["x"], 5))
    obs("seq3", p(["d", "e"], 3))
    obs("retry_older_seq0", p(["a", "b", "c"], 0))
    obs("seq5", p(["f"], 5))
    obs("unknown_pid", produce_part(produce(t, topic, ["z"], producerId=pid + 1000,
                                            producerEpoch=0, baseSequence=0)))
    _, part = fetch(t, topic, 0)
    obs_fetch(obs, "log", part, 0, 0)


def sc_produce_idempotent_partitions(t, obs):
    """Producer state is per partition: a known producer's first batch on a partition is
    accepted at any sequence, and after a client epoch bump a partition with state at the
    old epoch must restart at 0 while one without state accepts any sequence."""
    topic = f"diff-pip-{RUN}"
    create_topics(t, [topic_spec(topic, 4)])
    b, pid, ep = init_pid(t)
    obs("init", normalise(t, b))

    def p(partition, values, seq, epoch):
        return produce_part(produce(t, topic, values, partition=partition, producerId=pid,
                                    producerEpoch=epoch, baseSequence=seq))
    obs("p0_seq0", p(0, ["a", "b", "c"], 0, ep))
    obs("p1_first_seq7", p(1, ["d", "e"], 7, ep))
    obs("p1_retry_seq7", p(1, ["d", "e"], 7, ep))
    obs("p1_gap_seq10", p(1, ["x"], 10, ep))
    obs("p1_seq9", p(1, ["f"], 9, ep))
    obs("p0_bump_seq0", p(0, ["g"], 0, ep + 1))
    obs("p1_bumped_seq10", p(1, ["y"], 10, ep + 1))
    obs("p1_bumped_seq0", p(1, ["h"], 0, ep + 1))
    obs("p2_bumped_first_seq4", p(2, ["i"], 4, ep + 1))
    obs("p2_old_epoch", p(2, ["z"], 5, ep))
    obs("p3_old_epoch_first", p(3, ["z"], 0, ep))


def sc_transactions(t, obs):
    topic, txn, group = f"diff-tx-{RUN}", f"diff-tx-{RUN}", f"diff-txg-{RUN}"
    create_topics(t, [topic_spec(topic, 1)])
    b, pid, ep = init_pid(t, txn)
    obs("init", normalise(t, b))
    obs("init_bad_timeout", normalise(t, init_pid(t, f"{txn}-bad", timeout=0)[0]))
    # Produce before AddPartitionsToTxn
    obs("produce_unadded", produce_part(produce(t, topic, ["u"], txn=txn, producerId=pid,
                                                producerEpoch=ep, baseSequence=0,
                                                transactional=True)))
    b, pid, ep = init_pid(t, txn)  # a fresh epoch in case the previous produce broke the txn
    add = {"v3AndBelowTransactionalId": txn, "v3AndBelowProducerId": pid,
           "v3AndBelowProducerEpoch": ep,
           "v3AndBelowTopics": [{"name": topic, "partitions": [0]}]}
    obs("add_partitions", normalise(t, body(t.req(24, 3, add))))
    seq = 0

    def tp(values):
        nonlocal seq
        r = produce_part(produce(t, topic, values, txn=txn, producerId=pid, producerEpoch=ep,
                                 baseSequence=seq, transactional=True))
        seq += len(values)
        return r
    obs("produce_c", tp(["c0", "c1", "c2"]))
    end = {"transactionalId": txn, "producerId": pid, "producerEpoch": ep}
    obs("commit", normalise(t, body(t.req(26, 3, dict(end, committed=True)))))
    obs("add_partitions_2", normalise(t, body(t.req(24, 3, add))))
    obs("produce_a", tp(["a0", "a1"]))
    # Pending: read_committed sees the LSO before the open transaction.
    obs("lo_latest_uncommitted", normalise(t, list_offsets(t, topic, -1, 0)))
    obs("lo_latest_committed", normalise(t, list_offsets(t, topic, -1, 1)))
    obs("abort", normalise(t, body(t.req(26, 3, dict(end, committed=False)))))
    _, p = fetch(t, topic, 0, 0)
    # The first aborted record: offset 3 in pg_kafka, 4 in Kafka (after the commit marker).
    first_aborted = next((r["offset"] for b in (p or {}).get("records", []) for r in b["records"]
                          if r["value"] == "a0"), None)
    for iso, name in ((0, "uncommitted"), (1, "committed")):
        _, p = fetch(t, topic, 0, iso)
        obs_fetch(obs, f"fetch_{name}_from_0", p, 0, iso)
        if first_aborted is not None:
            _, p = fetch(t, topic, first_aborted, iso)
            obs_fetch(obs, f"fetch_{name}_at_aborted_tail", p, first_aborted, iso)
    # Batch layout: Kafka keeps the produced batches and adds markers; pg_kafka regroups.
    _, p = fetch(t, topic, 0, 0)
    obs("batch_layout", [(b["baseOffset"], b["lastOffset"], b["isControlBatch"])
                         for b in p.get("records", [])])
    # require_stable with a pending TxnOffsetCommit
    b, pid, ep = init_pid(t, txn)
    obs("add_offsets", normalise(t, body(t.req(25, 3, {"transactionalId": txn, "producerId": pid,
                                                      "producerEpoch": ep, "groupId": group}))))
    toc = body(t.req(28, 3, {"transactionalId": txn, "groupId": group, "producerId": pid,
                             "producerEpoch": ep, "topics": [{"name": topic, "partitions": [
                                 {"partitionIndex": 0, "committedOffset": 3}]}]}))
    obs("txn_offset_commit", normalise(t, toc))
    of = body(t.req(9, 7, {"groupId": group, "requireStable": True,
                           "topics": [{"name": topic, "partitionIndexes": [0]}]}))
    obs("offset_fetch_require_stable", normalise(t, of))
    of = body(t.req(9, 7, {"groupId": group, "requireStable": False,
                           "topics": [{"name": topic, "partitionIndexes": [0]}]}))
    obs("offset_fetch_not_stable", normalise(t, of))
    obs("commit_offsets", normalise(t, body(t.req(26, 3, {"transactionalId": txn,
                                                          "producerId": pid,
                                                          "producerEpoch": ep,
                                                          "committed": True}))))
    # Kafka writes the commit markers asynchronously after EndTxn, so a require_stable fetch
    # can still be UNSTABLE_OFFSET_COMMIT for a moment; clients retry it, and so does this.
    for _ in range(40):
        of = body(t.req(9, 7, {"groupId": group, "requireStable": True,
                               "topics": [{"name": topic, "partitionIndexes": [0]}]}))
        if not has_code(of, 88):
            break
        time.sleep(0.25)
    obs("offset_fetch_after_commit", normalise(t, of))
    # A stale epoch after a re-init (fencing)
    b2, pid2, ep2 = init_pid(t, txn)
    obs("stale_epoch_end_txn", normalise(t, body(t.req(26, 3, {"transactionalId": txn,
                                                               "producerId": pid,
                                                               "producerEpoch": ep,
                                                               "committed": True}))))


def sc_list_offsets(t, obs):
    topic = f"diff-lo-{RUN}"
    create_topics(t, [topic_spec(topic, 1)])
    now = int(time.time() * 1000)
    produce(t, topic, ["a", "b", "c"], timestamp=now - 3000)
    produce(t, topic, ["d"], timestamp=now + 60000)  # the max timestamp
    produce(t, topic, ["e"], timestamp=now - 1000)
    for ts, name in ((-1, "latest"), (-2, "earliest"), (-3, "max_timestamp"),
                     (now - 2000, "by_timestamp"), (now + 120000, "after_last")):
        obs(name, normalise(t, list_offsets(t, topic, ts)))
    obs("unknown_partition", normalise(t, list_offsets(t, topic, -1, partition=5)))
    obs("v1_latest", normalise(t, list_offsets(t, topic, -1, version=1)))


def sc_offsets(t, obs):
    topic, group = f"diff-oc-{RUN}", f"diff-oc-{RUN}"
    create_topics(t, [topic_spec(topic, 2)])

    def commit(g, gen, member, offset=5, partition=0):
        return normalise(t, body(t.req(8, 8, {"groupId": g, "generationIdOrMemberEpoch": gen,
                                               "memberId": member, "topics": [{"name": topic,
                                               "partitions": [{"partitionIndex": partition,
                                                               "committedOffset": offset,
                                                               "committedMetadata": "m"}]}]})))
    obs("simple_commit", commit(group, -1, ""))
    obs("fetch", normalise(t, body(t.req(9, 7, {"groupId": group, "topics": [
        {"name": topic, "partitionIndexes": [0, 1]}]}))))
    obs("fetch_all_topics", normalise(t, body(t.req(9, 7, {"groupId": group, "topics": None}))))
    obs("unknown_group_generation_5", commit(f"{group}-new", 5, "someone"))
    obs("unknown_topic", normalise(t, body(t.req(8, 8, {
        "groupId": group, "generationIdOrMemberEpoch": -1, "memberId": "",
        "topics": [{"name": f"diff-nope-{RUN}", "partitions": [
            {"partitionIndex": 0, "committedOffset": 1}]}]}))))
    obs("unknown_partition", commit(group, -1, "", partition=9))
    obs("fetch_unknown_group", normalise(t, body(t.req(9, 7, {"groupId": f"{group}-none",
                                                             "topics": [{"name": topic,
                                                             "partitionIndexes": [0]}]}))))


def joined(t, obs, group, label, conn="a", **kw):
    """Join `group` as a new member the way a KIP-394 client does: a first JoinGroup without a
    member id, and when the broker answers MEMBER_ID_REQUIRED, again with the id it assigned."""
    r = body(join(t, group, conn=conn, **kw))
    obs(f"{label}.first_join", errname(r.get("errorCode")))
    if r.get("errorCode") == 79:
        r = body(join(t, group, member=r["memberId"], conn=conn, **kw))
    return r


def sc_groups(t, obs):
    group = f"diff-gr-{RUN}"
    r = joined(t, obs, group, "join")
    obs("join", normalise(t, {k: r.get(k) for k in
                              ("errorCode", "generationId", "protocolType", "protocolName",
                               "leader", "memberId")}))
    member, gen = r.get("memberId", ""), r.get("generationId", -1)

    def hb(g, m, grp=group):
        return normalise(t, body(t.req(12, 3, {"groupId": grp, "generationId": g,
                                               "memberId": m})))
    obs("heartbeat_before_sync", hb(gen, member))
    obs("sync_wrong_generation", normalise(t, body(t.req(14, 3, {
        "groupId": group, "generationId": gen + 5, "memberId": member, "assignments": []}))))
    obs("sync_unknown_member", normalise(t, body(t.req(14, 3, {
        "groupId": group, "generationId": gen, "memberId": "nobody", "assignments": []}))))
    obs("sync", normalise(t, body(t.req(14, 3, {
        "groupId": group, "generationId": gen, "memberId": member,
        "assignments": [{"memberId": member, "assignment": ""}]}))))
    obs("heartbeat", hb(gen, member))
    obs("heartbeat_wrong_generation", hb(gen + 5, member))
    obs("heartbeat_unknown_member", hb(gen, "nobody"))
    obs("heartbeat_unknown_group", hb(1, "x", grp=f"{group}-none"))
    obs("describe", normalise(t, [{k: g.get(k) for k in ("errorCode", "groupState",
                                                          "protocolType", "protocolData")}
                                  for g in body(t.req(15, 5, {"groups": [group]}))["groups"]]))
    obs("leave_unknown", normalise(t, body(t.req(13, 3, {"groupId": group, "members": [
        {"memberId": "nobody", "groupInstanceId": None}]}))))
    obs("leave", normalise(t, body(t.req(13, 3, {"groupId": group, "members": [
        {"memberId": member, "groupInstanceId": None}]}))))
    obs("describe_after_leave", normalise(t, [{k: g.get(k) for k in ("errorCode", "groupState",
                                                                      "protocolType")}
                                              for g in body(t.req(15, 5, {"groups": [group]}))["groups"]]))
    obs("delete_group", normalise(t, body(t.req(42, 2, {"groupsNames": [group]}))))
    obs("delete_unknown_group", normalise(t, body(t.req(42, 2, {"groupsNames": [f"{group}-none"]}))))
    # Error cases, each on its own group so a broker that admits the request does not
    # disturb the others.
    r = body(join(t, f"{group}-u", member="not-a-member", conn="b"))
    obs("join_unknown_member_id", join_error(r))
    r = body(join(t, f"{group}-p", conn="c", protocols=[]))
    obs("join_no_protocols", join_error(r))
    joined(t, obs, f"{group}-t", "join_consumer")
    r = body(join(t, f"{group}-t", member="", protocol_type="connect", conn="d",
                  protocols=[{"name": "default", "metadata": ""}]))
    obs("join_other_protocol_type", join_error(r))


def sc_admin(t, obs):
    base = f"diff-ad-{RUN}"
    obs("create", normalise(t, strip_configs(body(create_topics(t, [topic_spec(f"{base}-a", 3)])))))
    obs("create_again", normalise(t, body(create_topics(t, [topic_spec(f"{base}-a", 3)]))))
    obs("zero_partitions", normalise(t, body(create_topics(t, [topic_spec(f"{base}-z", 0)]))))
    obs("replication_3", normalise(t, body(create_topics(t, [topic_spec(f"{base}-r", 1, 3)]))))
    obs("invalid_name", normalise(t, body(create_topics(t, [topic_spec("bad name!", 1)]))))
    obs("dup_in_request", normalise(t, body(create_topics(t, [topic_spec(f"{base}-d", 1),
                                                              topic_spec(f"{base}-d", 1)]))))
    create_topics(t, [topic_spec(f"{base}.x", 1)])
    obs("collision", normalise(t, body(create_topics(t, [topic_spec(f"{base}_x", 1)]))))
    obs("validate_only", normalise(t, strip_configs(body(
        create_topics(t, [topic_spec(f"{base}-v", 2)], True)))))
    md = body(t.req(3, 9, {"topics": [{"name": f"{base}-v"}], "allowAutoTopicCreation": False}))
    obs("validate_only_not_created", normalise(t, [x["errorCode"] for x in md["topics"]]))
    cp = lambda name, n: normalise(t, body(t.req(37, 2, {
        "topics": [{"name": name, "count": n, "assignments": None}],
        "timeoutMs": 10000, "validateOnly": False})))
    obs("partitions_grow", cp(f"{base}-a", 5))
    obs("partitions_shrink", cp(f"{base}-a", 2))
    obs("partitions_same", cp(f"{base}-a", 5))
    obs("partitions_unknown", cp(f"{base}-nope", 5))
    obs("delete", normalise(t, body(t.req(20, 4, {"topicNames": [f"{base}-a"],
                                                  "timeoutMs": 10000}))))
    obs("delete_unknown", normalise(t, body(t.req(20, 4, {"topicNames": [f"{base}-nope"],
                                                          "timeoutMs": 10000}))))


SCENARIOS = [
    ("api_versions", sc_api_versions),
    ("metadata", sc_metadata),
    ("produce_plain", sc_produce_plain),
    ("produce_idempotent", sc_produce_idempotent),
    ("produce_idempotent_partitions", sc_produce_idempotent_partitions),
    ("transactions", sc_transactions),
    ("list_offsets", sc_list_offsets),
    ("offsets", sc_offsets),
    ("groups", sc_groups),
    ("admin", sc_admin),
]
COMPARE = {("api_versions", "ranges"): cmp_api_subset}


def compile_wire():
    if not (KAFKA_HOME / "libs").is_dir():
        sys.exit(f"KAFKA_HOME={KAFKA_HOME} has no libs/ directory; set KAFKA_HOME to an unpacked "
                 "kafka_2.13-3.7.0 distribution")
    classes = Path(tempfile.mkdtemp(prefix="pgk-diff-"))
    r = subprocess.run(["javac", "-cp", f"{KAFKA_HOME}/libs/*", "-d", str(classes),
                        str(HERE / "KafkaWire.java")], capture_output=True, text=True)
    if r.returncode != 0:
        sys.exit(f"javac KafkaWire.java failed:\n{r.stderr}")
    return classes


def run_scenario(fn, target):
    seen = {}
    try:
        fn(target, lambda name, value: seen.__setitem__(name, value))
    except Exception as e:  # keep what was observed; the crash is an observation too
        seen["<crash>"] = f"{type(e).__name__}: {e}"
    return seen


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--pg", default="127.0.0.1:9092")
    ap.add_argument("--kafka", default="127.0.0.1:9093")
    ap.add_argument("--only")
    ap.add_argument("--show", action="store_true", help="print every observation")
    args = ap.parse_args()
    classes = compile_wire()
    only = set(args.only.split(",")) if args.only else None
    tally = {"SAME": 0, "DEV": 0, "XFAIL": 0, "XPASS": 0, "DEV-GONE": 0, "FAIL": 0}
    for name, fn in SCENARIOS:
        if only and name not in only:
            continue
        pg_t, k_t = Target("pg_kafka", args.pg, classes), Target("kafka", args.kafka, classes)
        pg, ka = run_scenario(fn, pg_t), run_scenario(fn, k_t)
        for t in (pg_t, k_t):
            t.p.stdin.close()
            t.p.wait(timeout=30)
        print(f"== {name}", flush=True)
        for obs in list(dict.fromkeys([*ka, *pg])):
            a, b = pg.get(obs, "<missing>"), ka.get(obs, "<missing>")
            cmp = COMPARE.get((name, obs))
            if cmp:
                problems = cmp(a, b) if isinstance(a, dict) and isinstance(b, dict) else [a, b]
                differs = bool(problems)
                shown_pg, shown_k = problems, "(pg_kafka's ranges within Kafka's)"
            else:
                differs = a != b
                shown_pg, shown_k = a, b
            known = KNOWN.get((name, obs))
            if not differs:
                outcome = {"bug": "XPASS", "deviation": "DEV-GONE"}.get(known and known[0], "SAME")
            else:
                outcome = {"bug": "XFAIL", "deviation": "DEV"}.get(known and known[0], "FAIL")
            tally[outcome] += 1
            if outcome != "SAME" or args.show:
                print(f"  {outcome:8} {obs}")
                if differs or args.show:
                    print(f"           pg_kafka: {json.dumps(shown_pg, default=str)[:600]}")
                    print(f"           kafka:    {json.dumps(shown_k, default=str)[:600]}")
                if known:
                    print(f"           {known[0]}: {known[1]}")
    print("Differential: " + ", ".join(f"{v} {k}" for k, v in tally.items()))
    sys.exit(1 if tally["FAIL"] else 0)


if __name__ == "__main__":
    main()
