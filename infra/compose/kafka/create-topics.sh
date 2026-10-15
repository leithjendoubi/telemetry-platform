#!/usr/bin/env bash
# Creates the Kafka topics of spec 8.3 (event catalogue).
# Compose (R1) runs ONE broker, so the replication factor is 1 here.
# On Kubernetes (R2, Strimzi, 3 brokers) the same topics use replication factor 3 and min.insync.replicas=2.
set -euo pipefail

BOOTSTRAP="${BOOTSTRAP_SERVERS:-kafka:19092}"
KAFKA_TOPICS="/opt/kafka/bin/kafka-topics.sh"

HOUR_MS=3600000
DAY_MS=86400000

# create_topic <name> <partitions> <retention-ms | compact>
create_topic() {
  local name="$1" partitions="$2" retention="$3"
  local config
  if [[ "$retention" == "compact" ]]; then
    config="cleanup.policy=compact"
  else
    config="retention.ms=${retention}"
  fi
  "$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$name" --partitions "$partitions" --replication-factor 1 --config "$config"
}

echo "Creating topics on ${BOOTSTRAP} ..."

#            topic                     partitions  retention
create_topic telemetry.raw.v1           12  $((72 * HOUR_MS))   # key: instanceId
create_topic telemetry.rejected.v1       3  $((7 * DAY_MS))     # key: agentId
create_topic telemetry.scored.v1        12  $((72 * HOUR_MS))   # key: instanceId
create_topic anomaly.episode.v1          3  $((30 * DAY_MS))    # key: instanceId
create_topic alert.state-changed.v1      3  $((30 * DAY_MS))    # key: alertId
create_topic feedback.label.v1           3  $((365 * DAY_MS))   # key: instanceId
create_topic model.lifecycle.v1          1  compact             # key: modelName

# Dead-letter topics (spec 8.3 lists "<topic>.dlq.v1"; FR-DET-10 names "telemetry.raw.v1.dlq".
# We follow FR-DET-10 for the raw topic. Rename here if the supervisor prefers the other form.)
create_topic telemetry.raw.v1.dlq        3  $((14 * DAY_MS))
create_topic telemetry.scored.v1.dlq     3  $((14 * DAY_MS))
create_topic anomaly.episode.v1.dlq      3  $((14 * DAY_MS))

echo "Topics now present:"
"$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP" --list
