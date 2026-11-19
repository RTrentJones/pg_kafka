#!/usr/bin/env bash
# Start / stop a single-node Apache Kafka (KRaft) on 127.0.0.1:9093, the reference broker the
# interop checks and the differential harness compare pg_kafka against.
#
#   interop/kafka-ref.sh start <kafka_home> <workdir>
#   interop/kafka-ref.sh stop  <kafka_home> <workdir>
#
# <kafka_home> is an unpacked kafka_2.13-3.7.0 distribution. Settings match what the review used:
# one partition per topic by default, replication factor 1 for the internal topics, no initial
# rebalance delay (so group timings are comparable with pg_kafka).
set -euo pipefail
action=${1:?start|stop}; home=${2:?kafka_home}; work=${3:?workdir}
case "$action" in
  start)
    mkdir -p "$work"
    if [ ! -f "$work/server.properties" ]; then
      cat > "$work/server.properties" <<P
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:29094
listeners=PLAINTEXT://127.0.0.1:9093,CONTROLLER://127.0.0.1:29094
advertised.listeners=PLAINTEXT://127.0.0.1:9093
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
log.dirs=$work/data
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
auto.create.topics.enable=true
num.partitions=1
group.initial.rebalance.delay.ms=0
P
      "$home/bin/kafka-storage.sh" format -t "$("$home/bin/kafka-storage.sh" random-uuid)" \
        -c "$work/server.properties" >/dev/null
    fi
    KAFKA_HEAP_OPTS=${KAFKA_HEAP_OPTS:--Xmx512m} nohup "$home/bin/kafka-server-start.sh" \
      "$work/server.properties" > "$work/server.log" 2>&1 &
    for _ in $(seq 1 90); do
      if (echo > /dev/tcp/127.0.0.1/9093) 2>/dev/null; then echo "reference Kafka up on 127.0.0.1:9093"; exit 0; fi
      sleep 1
    done
    echo "reference Kafka did not start" >&2; tail -30 "$work/server.log" >&2; exit 1;;
  stop)
    pkill -f "kafka.Kafka $work/server.properties" || true;;
  *) echo "unknown action $action" >&2; exit 2;;
esac
