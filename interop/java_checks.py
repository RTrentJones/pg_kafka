#!/usr/bin/env python3
"""Real-client checks: the Apache Kafka Java tools and clients against a running pg_kafka.

Run through interop/run-java-checks.sh. Needs KAFKA_HOME (an unpacked kafka_2.13-3.7.0), java and
javac, psql reaching the pg_kafka database as a superuser (PG* environment), and pg_kafka on
BOOTSTRAP (default 127.0.0.1:9092). Every check uses fresh topic and group names, so the script
can be re-run against the same database.

A check listed in XFAIL is expected to fail on the current code (a defect another stream is
fixing). It is reported XFAIL when it fails and XPASS when it passes; neither fails the run, so
the integrator flips the mark once the fix is merged. Any other failure fails the run.

  interop/java_checks.py [--only name,name] [--list]
"""
import argparse
import json
import os
import re
import signal
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from pathlib import Path

HERE = Path(__file__).resolve().parent
KAFKA_HOME = Path(os.environ.get("KAFKA_HOME", "/var/tmp/kafka/kafka_2.13-3.7.0"))
BOOTSTRAP = os.environ.get("BOOTSTRAP", "127.0.0.1:9092")
SASL_PORT = int(os.environ.get("SASL_PORT", "19096"))
RUN = uuid.uuid4().hex[:8]
WORK = Path(tempfile.mkdtemp(prefix="pgk-java-"))
ENV = dict(os.environ, KAFKA_HEAP_OPTS=os.environ.get("KAFKA_HEAP_OPTS", "-Xmx256m"),
           KAFKA_LOG4J_OPTS=f"-Dlog4j.configuration=file:{HERE / 'log4j.properties'}")

# check name -> why it is expected to fail on the base code (wave 3; flip when fixed)
XFAIL = {
    "groups_after_restart": "stream groups item 2: offsets-only groups are invisible to "
                            "ListGroups/DescribeGroups after a worker restart",
    "get_offsets": "stream txn item 3b: ListOffsets MAX_TIMESTAMP (-3) is treated as a "
                   "literal timestamp",
    "read_committed_aborted_tail": "stream txn item 1: no abort markers, so a read_committed "
                                   "consumer never moves past an aborted transaction at the tail",
}


class CheckFailed(Exception):
    pass


def expect(cond, msg):
    if not cond:
        raise CheckFailed(msg)


def tool(name, *args, timeout=120, check=True, input=None):
    cmd = [str(KAFKA_HOME / "bin" / f"{name}.sh"), *map(str, args)]
    r = subprocess.run(cmd, env=ENV, capture_output=True, text=True, timeout=timeout, input=input)
    if check and r.returncode != 0:
        raise CheckFailed(f"{name} exited {r.returncode}: {(r.stderr or r.stdout)[-1500:]}")
    return r


def psql(sql):
    r = subprocess.run(["psql", "-X", "-At", "-v", "ON_ERROR_STOP=1", "-c", sql],
                       capture_output=True, text=True, timeout=60)
    if r.returncode != 0:
        raise CheckFailed(f"psql failed: {r.stderr[-800:]}")
    return r.stdout.strip()


def wait_port(port, up=True, timeout=60):
    import socket
    end = time.time() + timeout
    while time.time() < end:
        try:
            socket.create_connection(("127.0.0.1", port), timeout=1).close()
            if up:
                return
        except OSError:
            if not up:
                return
        time.sleep(0.5)
    raise CheckFailed(f"port {port} {'never opened' if up else 'never closed'}")


def restart_worker():
    """pg_terminate_backend the listener worker; the postmaster restarts it after 5 s."""
    psql("SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
         "WHERE backend_type = 'pg_kafka_listener'")
    time.sleep(1)
    wait_port(int(BOOTSTRAP.rsplit(":", 1)[1]), up=True, timeout=60)
    time.sleep(1)


def create_topic(topic, partitions):
    tool("kafka-topics", "--bootstrap-server", BOOTSTRAP, "--create", "--topic", topic,
         "--partitions", partitions, "--replication-factor", 1)


def json_lines(text):
    out = []
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("{"):
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    return out


class Proc:
    """A long-running Kafka tool whose JSON stdout is collected in a thread."""

    def __init__(self, name, *args):
        self.p = subprocess.Popen([str(KAFKA_HOME / "bin" / f"{name}.sh"), *map(str, args)],
                                  env=ENV, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                  text=True, start_new_session=True)
        self.events, self.err = [], []
        self.lock = threading.Lock()
        threading.Thread(target=self._read, daemon=True).start()
        threading.Thread(target=lambda: self.err.extend(self.p.stderr), daemon=True).start()

    def _read(self):
        for line in self.p.stdout:
            for ev in json_lines(line):
                with self.lock:
                    self.events.append(ev)

    def named(self, name):
        with self.lock:
            return [e for e in self.events if e.get("name") == name]

    def stop(self, timeout=30):
        if self.p.poll() is None:
            os.killpg(self.p.pid, signal.SIGTERM)
        try:
            self.p.wait(timeout)
        except subprocess.TimeoutExpired:
            os.killpg(self.p.pid, signal.SIGKILL)
            self.p.wait()
        time.sleep(0.5)


def consumed_ranges(consumers):
    """{partition: [(min, max, count)]} over every records_consumed event."""
    ranges = {}
    for c in consumers:
        for ev in c.named("records_consumed"):
            for p in ev.get("partitions", []):
                ranges.setdefault(p["partition"], []).append(
                    (p["minOffset"], p["maxOffset"], p["count"]))
    return ranges


def describe_group(group):
    r = tool("kafka-consumer-groups", "--bootstrap-server", BOOTSTRAP, "--describe",
             "--group", group, check=False, timeout=60)
    rows = []
    for line in r.stdout.splitlines():
        cols = line.split()
        if len(cols) >= 6 and cols[0] == group:
            rows.append(dict(topic=cols[1], partition=int(cols[2]), current=cols[3],
                             end=cols[4], lag=cols[5]))
    return r, rows


def console_consume(topic, n, *extra, group=None, timeout_ms=30000):
    """Consume from the beginning until `n` records (None: until idle for timeout_ms, so the
    position also passes trailing transaction markers before the offsets are committed)."""
    args = ["--bootstrap-server", BOOTSTRAP, "--topic", topic, "--from-beginning",
            "--timeout-ms", timeout_ms, *extra]
    if n is not None:
        args += ["--max-messages", n]
    if group:
        args += ["--group", group]
    r = tool("kafka-console-consumer", *args, check=False, timeout=timeout_ms / 1000 + 60)
    return [line for line in r.stdout.splitlines() if line and not line.startswith("Processed")]


def get_offsets(topic, time_spec):
    r = tool("kafka-get-offsets", "--bootstrap-server", BOOTSTRAP, "--topic", topic,
             "--time", time_spec)
    out = {}
    for line in r.stdout.splitlines():
        parts = line.strip().split(":")
        if len(parts) >= 3 and parts[0] == topic:
            out[int(parts[1])] = int(parts[2]) if parts[2] else None
    return out


# --------------------------------------------------------------------------- checks

STATE = {}


def check_topics_cli():
    """kafka-topics --create / --describe / --alter --partitions."""
    t = f"j-topics-{RUN}"
    create_topic(t, 3)
    d = tool("kafka-topics", "--bootstrap-server", BOOTSTRAP, "--describe", "--topic", t).stdout
    expect("PartitionCount: 3" in d, f"describe after create: {d[:300]}")
    tool("kafka-topics", "--bootstrap-server", BOOTSTRAP, "--alter", "--topic", t,
         "--partitions", 5)
    d = tool("kafka-topics", "--bootstrap-server", BOOTSTRAP, "--describe", "--topic", t).stdout
    expect("PartitionCount: 5" in d, f"describe after alter: {d[:300]}")
    lst = tool("kafka-topics", "--bootstrap-server", BOOTSTRAP, "--list").stdout.split()
    expect(t in lst, "topic missing from --list")


def check_verifiable_rebalance():
    """3 verifiable consumers through joins and a graceful leave while the verifiable producer
    runs: every acked record consumed exactly once, a bounded number of rebalances."""
    t, g, n = f"j-vc-{RUN}", f"j-vc-{RUN}", 3000
    create_topic(t, 6)
    common = ["--bootstrap-server", BOOTSTRAP, "--topic", t, "--group-id", g,
              "--reset-policy", "earliest", "--session-timeout", 10000]
    consumers = [Proc("kafka-verifiable-consumer", *common)]
    time.sleep(4)
    producer = Proc("kafka-verifiable-producer", "--bootstrap-server", BOOTSTRAP, "--topic", t,
                    "--max-messages", n, "--throughput", 300, "--acks", -1)
    for _ in range(2):
        time.sleep(3)
        consumers.append(Proc("kafka-verifiable-consumer", *common))
    time.sleep(4)
    consumers[2].stop()  # graceful leave in the middle of the run
    producer.p.wait(timeout=120)
    time.sleep(1)
    acked = {(e["partition"], e["offset"]) for e in producer.named("producer_send_success")}
    expect(len(acked) == n, f"producer acked {len(acked)} of {n}: {''.join(producer.err)[-800:]}")
    deadline = time.time() + 90
    while time.time() < deadline:
        got = sum(c for rs in consumed_ranges(consumers).values() for _, _, c in rs)
        if got >= n:
            break
        time.sleep(1)
    time.sleep(3)
    for c in consumers:
        c.stop()
    seen, dups = {}, 0
    for part, rs in consumed_ranges(consumers).items():
        for lo, hi, cnt in rs:
            for off in range(lo, hi + 1):
                seen[(part, off)] = seen.get((part, off), 0) + 1
    dups = sum(v - 1 for v in seen.values() if v > 1)
    missing = acked - set(seen)
    rebalances = sum(len(c.named("partitions_assigned")) for c in consumers)
    STATE["vc_group"], STATE["vc_topic"], STATE["vc_n"] = g, t, n
    expect(not missing, f"{len(missing)} acked records never consumed, e.g. {sorted(missing)[:5]}")
    expect(dups == 0, f"{dups} records consumed more than once")
    # 3 joins + 1 leave with the eager protocol: 4 + 3 + 2 assignments; allow some slack.
    expect(rebalances <= 15, f"{rebalances} partition assignments (rebalance storm)")


def check_groups_describe_lag():
    """kafka-consumer-groups --list/--describe after the verifiable consumers left: the group
    is listed and every partition shows lag 0."""
    g, n = STATE.get("vc_group"), STATE.get("vc_n")
    expect(g, "needs verifiable_rebalance")
    lst = tool("kafka-consumer-groups", "--bootstrap-server", BOOTSTRAP, "--list").stdout.split()
    expect(g in lst, f"group {g} not in --list")
    r, rows = describe_group(g)
    # An Empty group shows the partitions it committed offsets for: every partition the
    # producer wrote to (the sticky partitioner may leave one of the six empty).
    written = {p for p, end in get_offsets(STATE["vc_topic"], -1).items() if end}
    expect({row["partition"] for row in rows} == written,
           f"describe shows partitions {sorted(row['partition'] for row in rows)}, data in "
           f"{sorted(written)}: {r.stdout[-600:]}{r.stderr[-300:]}")
    expect(all(row["lag"] == "0" for row in rows), f"non-zero lag: {rows}")
    expect(sum(int(row["current"]) for row in rows) == n, f"committed offsets: {rows}")


def check_groups_after_restart():
    """After a worker restart, a group that only has committed offsets is still listed and
    described with its offsets (Kafka keeps offsets-only groups as Empty groups)."""
    g = STATE.get("vc_group")
    expect(g, "needs verifiable_rebalance")
    restart_worker()
    lst = tool("kafka-consumer-groups", "--bootstrap-server", BOOTSTRAP, "--list").stdout.split()
    expect(g in lst, f"group {g} not in --list after a worker restart")
    r, rows = describe_group(g)
    expect(rows and all(row["lag"] == "0" for row in rows),
           f"describe after restart: {r.stdout[-400:]}{r.stderr[-400:]}")


def check_get_offsets():
    """kafka-get-offsets --time -1 / -2 / -3 (latest, earliest, max timestamp)."""
    t, n = f"j-go-{RUN}", 50
    create_topic(t, 1)
    p = Proc("kafka-verifiable-producer", "--bootstrap-server", BOOTSTRAP, "--topic", t,
             "--max-messages", n, "--acks", -1)
    p.p.wait(timeout=60)
    expect(get_offsets(t, -1) == {0: n}, f"latest: {get_offsets(t, -1)}")
    expect(get_offsets(t, -2) == {0: 0}, f"earliest: {get_offsets(t, -2)}")
    # Records carry increasing create times, so the max-timestamp record is the last one.
    mt = get_offsets(t, -3)
    expect(mt == {0: n - 1}, f"max-timestamp (-3): {mt}, expected offset {n - 1}")


def check_transactional_perf_read_committed():
    """kafka-producer-perf-test --transactional-id, then a read_committed console consumer sees
    every record and its group ends with lag 0."""
    t, g, n = f"j-txp-{RUN}", f"j-txp-{RUN}", 2000
    create_topic(t, 1)
    tool("kafka-producer-perf-test", "--topic", t, "--num-records", n, "--record-size", 100,
         "--throughput", -1, "--transactional-id", f"j-txp-{RUN}",
         "--transaction-duration-ms", 100, "--producer-props", f"bootstrap.servers={BOOTSTRAP}",
         timeout=180)
    got = console_consume(t, None, "--isolation-level", "read_committed", group=g,
                          timeout_ms=10000)
    expect(len(got) == n, f"read_committed consumer got {len(got)} of {n}")
    _, rows = describe_group(g)
    expect(rows and all(r["lag"] == "0" for r in rows), f"lag after consuming: {rows}")


def check_transactional_copier_eos():
    """TransactionalMessageCopier with random aborts (consume-transform-produce with
    sendOffsetsToTransaction): the output topic holds every input record exactly once."""
    src, dst, n = f"j-cin-{RUN}", f"j-cout-{RUN}", 500
    create_topic(src, 1)
    create_topic(dst, 1)
    p = Proc("kafka-verifiable-producer", "--bootstrap-server", BOOTSTRAP, "--topic", src,
             "--max-messages", n, "--acks", -1, "--value-prefix", 7)
    p.p.wait(timeout=60)
    tool("kafka-run-class", "org.apache.kafka.tools.TransactionalMessageCopier",
         "--broker-list", BOOTSTRAP, "--input-topic", src, "--input-partition", 0,
         "--output-topic", dst, "--transactional-id", f"j-copier-{RUN}",
         "--consumer-group", f"j-copier-{RUN}", "--max-messages", n, "--transaction-size", 50,
         "--enable-random-aborts", "--use-group-metadata", timeout=240)
    got = console_consume(dst, n + 50, "--isolation-level", "read_committed", timeout_ms=15000)
    want = console_consume(src, n, timeout_ms=15000)
    expect(sorted(got) == sorted(want),
           f"output has {len(got)} records ({len(set(got))} distinct), input {len(want)}")


def java_scenario(*args):
    classes = WORK / "classes"
    if not (classes / "TxnScenario.class").exists():
        classes.mkdir(exist_ok=True)
        r = subprocess.run(["javac", "-cp", f"{KAFKA_HOME}/libs/*", "-d", str(classes),
                            str(HERE / "java" / "TxnScenario.java")], capture_output=True,
                           text=True)
        if r.returncode != 0:
            raise CheckFailed(f"javac: {r.stderr[-800:]}")
    r = subprocess.run(["java", "-Xmx256m", f"-Dlog4j.configuration=file:{HERE / 'log4j.properties'}",
                        "-cp", f"{classes}:{KAFKA_HOME}/libs/*", "TxnScenario", BOOTSTRAP,
                        *map(str, args)], capture_output=True, text=True, timeout=120)
    if r.returncode != 0:
        raise CheckFailed(f"TxnScenario {args}: {r.stderr[-800:]}")


def check_read_committed_aborted_tail():
    """An aborted transaction at the end of a partition: a read_committed consumer reads the
    committed records, its position reaches the log end and its group shows lag 0."""
    t, g = f"j-abt-{RUN}", f"j-abt-{RUN}"
    create_topic(t, 1)
    java_scenario(t, f"j-abt-{RUN}", 5, 5)
    got = console_consume(t, None, "--isolation-level", "read_committed", group=g,
                          timeout_ms=10000)
    expect(got == [f"c{i}" for i in range(5)], f"read_committed got {got}")
    end = get_offsets(t, -1)[0]
    _, rows = describe_group(g)
    expect(rows and rows[0]["lag"] == "0",
           f"group position stuck before the aborted tail (log end {end}): {rows}")


def check_read_committed_open_txn_blocks():
    """An open transaction keeps a read_committed consumer at the LSO: it sees only the
    records committed before the open transaction."""
    t = f"j-open-{RUN}"
    create_topic(t, 1)
    java_scenario(t, f"j-open-{RUN}", 3, 0, "open")
    got = console_consume(t, 100, "--isolation-level", "read_committed", timeout_ms=8000)
    expect(got == ["c0", "c1", "c2"], f"read_committed past an open transaction: {got}")
    unc = console_consume(t, 100, timeout_ms=8000)
    expect("open" in unc, f"read_uncommitted does not see the open record: {unc}")


def sasl_config(mechanism, user, password):
    module = ("org.apache.kafka.common.security.plain.PlainLoginModule" if mechanism == "PLAIN"
              else "org.apache.kafka.common.security.scram.ScramLoginModule")
    path = WORK / f"sasl-{mechanism}-{password}.properties"
    path.write_text(
        "security.protocol=SASL_PLAINTEXT\n"
        f"sasl.mechanism={mechanism}\n"
        f'sasl.jaas.config={module} required username="{user}" password="{password}";\n')
    return path


def with_sasl_listener(fn):
    psql(f"ALTER SYSTEM SET pg_kafka.additional_listeners = "
         f"'SASL_PLAINTEXT://127.0.0.1:{SASL_PORT}'")
    psql("SELECT pg_reload_conf()")
    restart_worker()
    wait_port(SASL_PORT)
    psql(f"SELECT pg_kafka_drop_user('java{RUN}')")
    psql(f"SELECT pg_kafka_create_user('java{RUN}', 'pw-{RUN}')")
    try:
        fn()
    finally:
        psql(f"SELECT pg_kafka_drop_user('java{RUN}')")
        psql("ALTER SYSTEM RESET pg_kafka.additional_listeners")
        psql("SELECT pg_reload_conf()")
        restart_worker()


def check_sasl_java_clients():
    """Java console producer/consumer over SASL_PLAINTEXT with SCRAM-SHA-256, SCRAM-SHA-512 and
    PLAIN; a wrong password is refused."""
    sasl_bootstrap = f"127.0.0.1:{SASL_PORT}"

    def body():
        for mech in ("SCRAM-SHA-256", "SCRAM-SHA-512", "PLAIN"):
            t = f"j-sasl-{mech.lower()}-{RUN}"
            cfg = sasl_config(mech, f"java{RUN}", f"pw-{RUN}")
            lines = [f"{mech}-{i}" for i in range(5)]
            tool("kafka-console-producer", "--bootstrap-server", sasl_bootstrap, "--topic", t,
                 "--producer.config", cfg, input="\n".join(lines) + "\n", timeout=90)
            r = tool("kafka-console-consumer", "--bootstrap-server", sasl_bootstrap,
                     "--topic", t, "--from-beginning", "--max-messages", 5, "--timeout-ms",
                     20000, "--consumer.config", cfg, check=False, timeout=90)
            got = [x for x in r.stdout.splitlines() if x]
            expect(got == lines, f"{mech}: consumed {got} {r.stderr[-400:]}")
        bad = sasl_config("SCRAM-SHA-256", f"java{RUN}", "wrong")
        r = tool("kafka-topics", "--bootstrap-server", sasl_bootstrap, "--list",
                 "--command-config", bad, check=False, timeout=90)
        expect(r.returncode != 0 and "uthentication" in (r.stderr + r.stdout),
               f"wrong password not refused: rc={r.returncode} {r.stdout[-300:]}")

    with_sasl_listener(body)


def check_streams_wordcount():
    """Kafka Streams smoke test: the WordCountDemo application (internal repartition and
    changelog topics, consumer group, producer) counts words end to end."""
    src, dst = "streams-plaintext-input", "streams-wordcount-output"
    for topic in (src, dst):  # Streams does not create its source topics
        tool("kafka-topics", "--bootstrap-server", BOOTSTRAP, "--create", "--if-not-exists",
             "--topic", topic, "--partitions", 1, "--replication-factor", 1)
    props = WORK / "streams.properties"
    props.write_text(f"bootstrap.servers={BOOTSTRAP}\napplication.id=j-wc-{RUN}\n"
                     f"state.dir={WORK / 'streams-state'}\ncommit.interval.ms=500\n")
    app = Proc("kafka-run-class", "org.apache.kafka.streams.examples.wordcount.WordCountDemo",
               props)
    try:
        time.sleep(8)
        words = f"alpha{RUN} beta{RUN} alpha{RUN}"
        tool("kafka-console-producer", "--bootstrap-server", BOOTSTRAP, "--topic", src,
             input=words + "\n", timeout=60)
        counts = {}
        deadline = time.time() + 90
        while time.time() < deadline and not (counts.get(f"alpha{RUN}") == 2
                                               and counts.get(f"beta{RUN}") == 1):
            r = tool("kafka-console-consumer", "--bootstrap-server", BOOTSTRAP, "--topic", dst,
                     "--from-beginning", "--timeout-ms", 15000, "--property", "print.key=true",
                     "--property", "value.deserializer="
                     "org.apache.kafka.common.serialization.LongDeserializer",
                     check=False, timeout=90)
            for line in r.stdout.splitlines():
                parts = line.split("\t")
                if len(parts) == 2 and parts[0].endswith(RUN):
                    counts[parts[0]] = int(parts[1])
        expect(counts.get(f"alpha{RUN}") == 2 and counts.get(f"beta{RUN}") == 1,
               f"word counts {counts}; app errors: "
               f"{[l for l in app.err if 'ERROR' in l or 'Exception' in l][:6]}")
    finally:
        app.stop()


CHECKS = [
    ("topics_cli", check_topics_cli),
    ("verifiable_rebalance", check_verifiable_rebalance),
    ("groups_describe_lag", check_groups_describe_lag),
    ("groups_after_restart", check_groups_after_restart),
    ("get_offsets", check_get_offsets),
    ("transactional_perf_read_committed", check_transactional_perf_read_committed),
    ("transactional_copier_eos", check_transactional_copier_eos),
    ("read_committed_aborted_tail", check_read_committed_aborted_tail),
    ("read_committed_open_txn_blocks", check_read_committed_open_txn_blocks),
    ("sasl_java_clients", check_sasl_java_clients),
    ("streams_wordcount", check_streams_wordcount),
]


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--only", help="comma-separated check names")
    ap.add_argument("--list", action="store_true")
    args = ap.parse_args()
    if args.list:
        for name, fn in CHECKS:
            mark = f"  [XFAIL: {XFAIL[name]}]" if name in XFAIL else ""
            print(f"{name}: {fn.__doc__.split(chr(10))[0]}{mark}")
        return
    only = set(args.only.split(",")) if args.only else None
    results = []
    for name, fn in CHECKS:
        if only and name not in only:
            continue
        start = time.time()
        try:
            fn()
            outcome, detail = ("XPASS" if name in XFAIL else "PASS"), ""
        except CheckFailed as e:
            outcome, detail = ("XFAIL" if name in XFAIL else "FAIL"), str(e)
        except Exception as e:  # a crashed check is a failure too
            outcome, detail = ("XFAIL" if name in XFAIL else "FAIL"), f"{type(e).__name__}: {e}"
        results.append((name, outcome))
        line = f"{outcome:5} {name} ({time.time() - start:.0f}s)"
        if detail:
            line += f": {detail}"
        if outcome == "XFAIL":
            line += f"\n      expected: {XFAIL[name]}"
        if outcome == "XPASS":
            line += f"\n      fixed? remove it from XFAIL ({XFAIL[name]})"
        print(line, flush=True)
    counts = {k: sum(1 for _, o in results if o == k) for k in ("PASS", "FAIL", "XFAIL", "XPASS")}
    print("Java checks: " + ", ".join(f"{v} {k}" for k, v in counts.items()))
    sys.exit(1 if counts["FAIL"] else 0)


if __name__ == "__main__":
    main()
