#!/bin/sh
## Create topics if not exists
set -eu

until /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server kafka:9093 >/dev/null 2>&1; do
  echo "Waiting for Kafka..."
  sleep 5
done

while IFS= read -r topic; do
  [ -n "$topic" ] || continue
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9093 \
    --create --if-not-exists --topic "$topic" \
    --partitions 1 --replication-factor 1
done < /bootstrap/topics.txt
