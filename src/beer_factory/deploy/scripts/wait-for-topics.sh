#!/bin/sh
## Wait for all topics to exist
set -eu

while :; do
  if /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9093 --list > /tmp/topics.txt 2>/dev/null; then
    ready=true
    while IFS= read -r topic; do
      [ -n "$topic" ] || continue
      if ! grep -Fxq "$topic" /tmp/topics.txt; then
        ready=false
      fi
    done < /bootstrap/topics.txt
    if [ "$ready" = true ]; then
      echo "All machine topics are ready."
      exit 0
    fi
  fi
  echo "Waiting for the Kafka topic initialization job..."
  sleep 5
done
