#!/bin/sh
# Runs make inside the dev container (gcc 14 + librdkafka + cJSON), for
# Windows or any host without the libraries. Run it from Git Bash, WSL or
# Linux in the machine_kafka folder, after `docker compose up -d`:
#
#   ./scripts/docker-make.sh                  # make test test-kafka
#   ./scripts/docker-make.sh trace            # any make targets
#   ./scripts/docker-make.sh clean all test examples
#
# The container joins the compose network, so it reaches the broker as
# kafka:29092 (localhost:9092 inside the container would be the container).
# Any other command: ../dev/run.sh <command> (see that script).
set -e
cd "$(dirname "$0")/.."
targets=${*:-test test-kafka}
# shellcheck disable=SC2086
exec ../dev/run.sh make $targets KAFKA_BROKERS=kafka:29092
