# Whirlpooling

This is the whirlpooling machine in C, made from the Whirlpooling template in the UPPAAL model (`src/docs/Beer_Factory.xml`).

## How it works

`whirlpooling.c` / `whirlpooling.h` are done the same way as boiling: a state machine that follows the UPPAAL model, works in seconds and returns result codes. It waits for hot wort, takes one batch, whirlpools it for 1 hour, then makes one clarified wort batch and goes back to waiting. Same states, guards and updates as in UPPAAL.

`src/main.c` is the controller and the reference machine for `machine_kafka`. It takes commands over Kafka, drives the state machine and sends an MQTT heartbeat (`machine_mqtt`), see [Controller](#controller).

## Files

| File | What it is |
| --- | --- |
| `src/whirlpooling.c`, `src/whirlpooling.h` | the machine (boiling style) |
| `tests/test_whirlpooling.c` | tests for the machine |
| `examples/demo.c` | runs one batch and prints what happens |
| `src/main.c` | the controller: Kafka (`machine_kafka`) + MQTT heartbeat (`machine_mqtt`) |

## How to run it

From this folder in Git Bash:

```bash
make test   # the tests
make demo   # one batch, step by step
make clean  # deletes build/
```

If gcc fails on Windows:

- `Cannot create temporary file in C:\WINDOWS\`: run `make test demo TMP="$TMP" TEMP="$TEMP"`.
- `cannot execute binary file`: `build/` has Linux binaries from a container, so run `make clean` first.

**The Dockerfile doesn't build at the moment**, so don't copy it as a template yet. It copies `scheduling/src/`, which isn't in `src/beer_factory`. It also lacks `machine_commons`, librdkafka and MQTT-C, which `make all` needs, and it expects `whirlpooling-demo`, which `make all` no longer builds. To run the controller, see [Run](#run).

## Differences from boiling

- **No output limit.** Boiling won't start if its output is full. The UPPAAL whirlpooling template doesn't have that guard (`MAX_WHIRLPOOLED_BATCHES` is there but never used), so I left it out too.
- **No error while waiting.** Calling update while the machine is waiting just returns "no change" instead of an error, so a caller can tick every machine regularly, even the idle ones.

## Controller

At startup `main.c` starts the MQTT heartbeat, then connects to Kafka. Then it loops: handle a Kafka message (waits up to 100 ms), update the whirlpool, update the heartbeat. Ctrl-C closes both connections and exits.

### Kafka commands

Topic `machine.whirlpooling`, key = machine id. The machine handles only commands for its own id and skips everything else (statuses, including its own).

| Command | Whirlpool | Answer |
| --- | --- | --- |
| `start B-7` | runs for the default 1 h | `running`, then `idle` when done |
| `start B-8` while B-7 runs | **known bug:** doesn't start B-8, but replaces the batch id | `running` for B-8, later `idle` for B-8. B-7 never gets its `idle` |
| `stop`, `flush` | not handled yet | - |

Until the bug is fixed, send the next start only after the `idle`.

### MQTT heartbeat

It uses `machine_mqtt` the same way as boiling.

| What | Value |
| --- | --- |
| Broker | `localhost:1883`, fixed in `heartbeat.c` |
| Client id | `WHIRLPOOLING-<id>`, e.g. `WHIRLPOOLING-1` |
| Topic | `whirlpooling`, shared by all whirlpools (not the Kafka topic) |
| Payload | `machine_heartbeat_t` as raw bytes: `int timestamp` (Unix seconds), `int machine_idx` |
| QoS | 1 |
| Interval | the first right away, then every 5 s in real time (`TIME_SCALE` doesn't change it) |

If MQTT has a problem, Kafka and the whirlpool keep working:

- **No broker at startup:** prints `Could not start MQTT!` and runs without a heartbeat. If the host doesn't respond at all, startup first waits up to 5 s per address.
- **Broker lost while running:** prints one error and stops the heartbeat. It doesn't reconnect, so restart the machine to get it back.

To watch the heartbeats during the [machine_kafka test](../machine_commons/machine_kafka/README.md#test-end-to-end-with-whirlpooling), start a broker in a second Git Bash window before step 4:

```bash
docker run -d --name mqtt --network container:kafka eclipse-mosquitto:2.0.22
docker exec mqtt mosquitto_sub -W 120 -t whirlpooling -F '%I %t %x'   # listens for 2 min
```

Each line is time, topic and payload in hex. The payload ends in `01000000` for machine 1. Clean up with `docker rm -f mqtt`.

### Run

The controller needs librdkafka and MQTT-C, so on Windows run it in Linux (WSL or a container):

```bash
TIME_SCALE=600 make run MQTT_C_DIR=/opt/MQTT-C   # 1 h run = 6 s
```

`MQTT_C_DIR` points to an MQTT-C checkout; leave it out to link an installed `-lmqttc`. Create the Kafka topic before starting the machine. For a full test with a broker and a start command, see the [machine_kafka test](../machine_commons/machine_kafka/README.md#test-end-to-end-with-whirlpooling).

| Environment | Default | Meaning |
| --- | --- | --- |
| `KAFKA_BROKERS` | required (`make run` uses `localhost:9092`) | Kafka servers, e.g. `kafka:29092` |
| `MACHINE_ID` | number in the machine name | machine id, used for both Kafka and MQTT |
| `TIME_SCALE` | 1 | simulated seconds per real second |

Argument: `whirlpooling [machine-name]`, default `whirlpooling-1`.
