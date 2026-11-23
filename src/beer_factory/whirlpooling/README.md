# Whirlpooling

This is the whirlpooling machine in C, made from the Whirlpooling template in the UPPAAL model (`src/docs/Beer_Factory.xml`).

## How it works

`whirlpooling.c` / `whirlpooling.h` are done the same way as boiling: a state machine that follows the UPPAAL model, works in seconds and returns result codes. It waits for hot wort, takes one batch, whirlpools it for 1 hour, then makes one clarified wort batch and goes back to waiting. Same states, guards and updates as in UPPAAL.

`src/main.c` is the controller and the reference machine for `machine_kafka`. It takes commands over Kafka, drives the state machine and sends an MQTT heartbeat (`machine_mqtt`). If MQTT can't start, it prints a warning and keeps running.

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

With Docker, from `src/beer_factory`:

```bash
docker build -f whirlpooling/Dockerfile -t whirlpooling .
docker run --rm whirlpooling
```

The Docker build runs the tests first, so if they fail the image isn't built.

The Dockerfile is also meant as a template for the other machines: copy it to your folder, change `MACHINE` and `BINARY` at the top, and you're done (the comments in it explain the rest).

## Differences from boiling

- **No output limit.** Boiling won't start if its output is full. The UPPAAL whirlpooling template doesn't have that guard (`MAX_WHIRLPOOLED_BATCHES` is there but never used), so I left it out too.
- **No error while waiting.** Calling update while the machine is waiting just returns "no change" instead of an error, so a caller can tick every machine regularly, even the idle ones.

## Controller (Kafka)

Topic `machine.whirlpooling`, key = machine id. The machine handles only commands for its own id and skips everything else (statuses, including its own).

| Command | Whirlpool | Answer |
| --- | --- | --- |
| `start B-7` | runs for the default 1 h | `running`, then `idle` when done |
| `stop`, `flush` | not handled yet | - |

### Run

The controller needs librdkafka and MQTT-C, so on Windows run it in Linux (WSL or a container):

```bash
TIME_SCALE=600 make run MQTT_C_DIR=/opt/MQTT-C   # 1 h run = 6 s
```

`MQTT_C_DIR` points to an MQTT-C checkout; leave it out to link an installed `-lmqttc`. For a full test with a Kafka broker and a start command, see the "Test" section in `../machine_commons/machine_kafka/README.md`.

| Environment | Default | Meaning |
| --- | --- | --- |
| `KAFKA_BROKERS` | (required) | Kafka servers, e.g. `kafka:29092` |
| `MACHINE_ID` | number in the machine name | Kafka `machine_id` |
| `TIME_SCALE` | 1 | simulated seconds per real second |

The MQTT heartbeat broker is fixed to `localhost:1883` in `heartbeat.c`.

Argument: `whirlpooling [machine-name]`, default `whirlpooling-1`.
