#!/bin/sh
# Runs a command in the dev container (gcc 14, librdkafka, cJSON, MQTT-C), in
# the folder you call it from. src/beer_factory is mounted at /src, and the
# container joins the broker network from machine_kafka/docker-compose.yml,
# where Kafka is kafka:29092 (KAFKA_BROKERS is set accordingly). MQTT_HOST,
# MQTT_PORT, MACHINE_ID and TIME_SCALE are passed through when set.
#
#   cd src/beer_factory/whirlpooling
#   ../machine_commons/dev/run.sh make test
#   ../machine_commons/dev/run.sh make run        # the controller
#
# Works from Git Bash, WSL and Linux.
set -e

root=$(cd "$(dirname "$0")/../.." && pwd) # src/beer_factory
here=$(pwd)
case "$here" in
  "$root"*) rel=${here#"$root"} ;;
  *) echo "run.sh: call it from a folder inside $root" >&2; exit 1 ;;
esac

# Git Bash: give docker a Windows path and stop MSYS from rewriting /src.
mount=$root
if (cd "$root" && pwd -W) >/dev/null 2>&1; then
  mount=$(cd "$root" && pwd -W)
  export MSYS_NO_PATHCONV=1
fi

if ! docker image inspect beer-factory-dev >/dev/null 2>&1; then
  echo "building the dev image once (about a minute)..." >&2
fi
docker build --quiet -t beer-factory-dev "$mount/machine_commons/dev" >/dev/null

network=""
if docker network inspect machine_kafka_default >/dev/null 2>&1; then
  network="--network machine_kafka_default"
else
  echo "run.sh: broker network not found; start it with 'docker compose up -d'" \
       "in machine_commons/machine_kafka (continuing without it)" >&2
fi

# In Git Bash, Ctrl-C only ends the docker client, not the container: a
# controller started with "make run" would keep running in the background
# (and keep the broker network busy). So the container gets a name and is
# stopped explicitly when this script is interrupted. --init plus
# TINI_KILL_PROCESS_GROUP pass the stop on to make *and* the program it runs.
name="beer-dev-$$"
stop_container() {
  docker stop -t 5 "$name" >/dev/null 2>&1 || true
}
trap 'stop_container; exit 130' INT
trap 'stop_container; exit 143' TERM HUP

# Started in the background and waited for: the shell only runs a trap while
# it waits with "wait", not while a foreground command is running.
# shellcheck disable=SC2086
docker run --rm --init --name "$name" $network ${DEV_DOCKER_ARGS:-} \
  -e TINI_KILL_PROCESS_GROUP=1 \
  -v "$mount:/src" -w "/src$rel" \
  -e KAFKA_BROKERS="${KAFKA_BROKERS:-kafka:29092}" \
  -e MQTT_HOST -e MQTT_PORT -e MACHINE_ID -e TIME_SCALE \
  beer-factory-dev "$@" &
wait $!
