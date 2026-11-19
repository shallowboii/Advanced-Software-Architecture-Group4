#!/bin/sh
# Creates one topic per machine type, named after the type. Commands
# (scheduler -> machines) and statuses (machines -> scheduler) share it.
#
# Safe to run again (--if-not-exists). Adding a new machine type:
#   ./scripts/create-topics.sh centrifuge
#
# Settings (environment variables):
#   BOOTSTRAP     broker address                  (localhost:9092)
#   PARTITIONS    partitions per topic            (1)
#   REPLICATION   replication factor              (1; use 3 in production)
#   MIN_ISR       min.insync.replicas             (1; use 2 in production)
#   RETENTION_MS  how long messages are kept      (2592000000 = 30 days)
#   KAFKA_TOPICS  path of kafka-topics.sh         (kafka-topics.sh)
set -eu

BOOTSTRAP=${BOOTSTRAP:-localhost:9092}
PARTITIONS=${PARTITIONS:-1}
REPLICATION=${REPLICATION:-1}
MIN_ISR=${MIN_ISR:-1}
RETENTION_MS=${RETENTION_MS:-2592000000}
KAFKA_TOPICS=${KAFKA_TOPICS:-kafka-topics.sh}

if [ "$#" -gt 0 ]; then
  TYPES="$*"
else
  TYPES="mashing lautering boiling whirlpooling fermentation filtering"
fi

if [ "$REPLICATION" -lt 2 ]; then
  echo "warning: replication factor $REPLICATION: acks=all only waits for this" \
       "one broker, so a broker disk loss still loses messages." >&2
fi
if [ "$MIN_ISR" -ge "$REPLICATION" ] && [ "$REPLICATION" -gt 1 ]; then
  echo "warning: min.insync.replicas=$MIN_ISR >= replication $REPLICATION:" \
       "one broker down blocks all producers." >&2
fi

for type in $TYPES; do
  case "$type" in
    [a-z]*) ;;
    *) echo "invalid machine type '$type' (must match [a-z][a-z0-9_-]*)" >&2; exit 1 ;;
  esac
  # cleanup.policy=delete, not compact: compaction would keep only the last
  # message per machine_id and destroy the history (QA-4).
  "$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$type" \
    --partitions "$PARTITIONS" \
    --replication-factor "$REPLICATION" \
    --config min.insync.replicas="$MIN_ISR" \
    --config retention.ms="$RETENTION_MS" \
    --config cleanup.policy=delete
  echo "$type"
done
