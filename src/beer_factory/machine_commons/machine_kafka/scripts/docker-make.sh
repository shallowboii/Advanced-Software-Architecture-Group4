#!/bin/sh
# Runs make inside a throw-away Linux container (gcc:14 + librdkafka + cJSON),
# for Windows or any host without the libraries. Run it from Git Bash, WSL or
# Linux in the machine_kafka folder, after `docker compose up -d`:
#
#   ./scripts/docker-make.sh                  # make test test-kafka
#   ./scripts/docker-make.sh trace            # any make targets
#   ./scripts/docker-make.sh clean all test
#
# The container joins the compose network, so it reaches the broker as
# kafka:29092 (localhost:9092 inside the container would be the container).
set -e

cd "$(dirname "$0")/.."
targets=${*:-test test-kafka}

# Git Bash: give docker a Windows path and stop MSYS from rewriting /src.
if pwd -W >/dev/null 2>&1; then
  src=$(pwd -W)
  export MSYS_NO_PATHCONV=1
else
  src=$(pwd)
fi

if ! docker network inspect machine_kafka_default >/dev/null 2>&1; then
  echo "broker network not found: run 'docker compose up -d' in this folder first" >&2
  exit 1
fi

echo "installing librdkafka + cJSON in the container (about 10 s)..."
docker run --rm --network machine_kafka_default -v "$src:/src" -w /src \
  -e DEBIAN_FRONTEND=noninteractive gcc:14 sh -c "
    apt-get update -qq >/dev/null &&
    apt-get install -y -qq librdkafka-dev libcjson-dev pkg-config >/dev/null 2>&1 &&
    make $targets KAFKA_BROKERS=kafka:29092"
