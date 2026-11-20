# Whirlpooling

This is the whirlpooling machine in C, made from the Whirlpooling template in the UPPAAL model (`src/docs/Beer_Factory.xml`).

## How it works

`whirlpooling.c` / `whirlpooling.h` are done the same way as boiling: a state machine that follows the UPPAAL model, works in seconds and returns result codes. It waits for hot wort, takes one batch, whirlpools it for 1 hour, then makes one clarified wort batch and goes back to waiting. Same states, guards and updates as in UPPAAL.

`src/main.c` is the controller and the reference machine for `machine_kafka`. It takes commands over Kafka, drives the state machine and, if `MQTT_HOST` is set, sends an MQTT heartbeat every 2 seconds (`machine_mqtt`).

## Files

| File | What it is |
| --- | --- |
| `src/whirlpooling.c`, `src/whirlpooling.h` | the machine (boiling style) |
| `tests/test_whirlpooling.c` | tests for the machine |
| `examples/demo.c` | runs one batch and prints what happens |
| `src/main.c` | the controller: Kafka + ledger + stop/flush (+ optional MQTT heartbeat) |

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

| Kafka command (topic `whirlpooling`) | Whirlpool | Answer |
| --- | --- | --- |
| `start B-7` | empty: ledger written, then runs for `duration_s` | `running`, later `done`, then `idle` |
| `start B-7` again (any message id) | nothing (ledger: seen before) | the batch's last status again |
| `start B-8` while running | nothing | `error BUSY` |
| `stop` (any batch, or the running one) | stops, wort stays inside | `error STOPPED` (`NOT_RUNNING` if nothing runs) |
| `flush B-7` (running or stopped) | wort discarded, vessel empty | `error FLUSHED`, then `idle` (`NOT_HERE` for another batch) |

There is no resume after a stop; the way out is a flush. A stopped or flushed batch is recorded in the ledger, so a re-sent start never runs it again. After a crash the batch that was running is reported as `error INTERRUPTED`.

Start parameter: `duration_s` (1 to 86400, default 3600); anything outside is refused with `error INVALID_PARAM`.

### Run

Kafka first:

```bash
(cd ../machine_commons/machine_kafka && docker compose up -d)
```

The controller needs librdkafka, cJSON and MQTT-C, so on Windows run it through the dev container (Git Bash):

```bash
TIME_SCALE=600 ../machine_commons/dev/run.sh make run   # 1 h run = 6 s
```

Send it work from the scheduler service (`backend/src/scheduling`) or by hand in the Kafka UI (<http://localhost:8080>, topic `whirlpooling`, Produce Message, key `1`; example JSON in the machine_kafka README).

| Environment | Default | Meaning |
| --- | --- | --- |
| `KAFKA_BROKERS` | (required) | Kafka servers, e.g. `kafka:29092` |
| `MACHINE_ID` | number in the machine name | Kafka `machine_id` |
| `LEDGER_PATH` | `whirlpooling-<id>.ledger` | the ledger file; must survive restarts |
| `MQTT_HOST`, `MQTT_PORT` | unset, 1883 | heartbeat broker; unset = no heartbeat |
| `TIME_SCALE` | 1 | simulated seconds per real second |

Argument: `whirlpooling [machine-name]`, default `whirlpooling-1`.

The Dockerfile still builds the demo (`make all`), since it is the template for the other machines.
