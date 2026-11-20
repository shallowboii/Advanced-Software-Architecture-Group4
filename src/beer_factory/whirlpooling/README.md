# Whirlpooling

This is the whirlpooling machine in C, made from the Whirlpooling template in the UPPAAL model (`src/docs/Beer_Factory.xml`).

## How it works

`whirlpooling.c` / `whirlpooling.h` are done the same way as boiling: a state machine that follows the UPPAAL model, works in seconds and returns result codes. It waits for hot wort, takes one batch, whirlpools it for 1 hour, then makes one clarified wort batch and goes back to waiting. Same states, guards and updates as in UPPAAL.

`src/main.c` is the controller: the test machine for the shared libraries. It takes commands over Kafka (`machine_kafka`), sends an MQTT heartbeat every 2 seconds (`machine_mqtt`) and drives the state machine.

## Files

| File | What it is |
| --- | --- |
| `src/whirlpooling.c`, `src/whirlpooling.h` | the machine (boiling style) |
| `tests/test_whirlpooling.c` | tests for the machine |
| `examples/demo.c` | runs one batch and prints what happens |
| `src/main.c` | the controller: Kafka + ledger + MQTT heartbeat + stop/flush |
| `tests/test_mqtt.py` | heartbeats against a fake MQTT broker |
| `tests/test_controller.c` | the controller end to end over real Kafka |

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

## Controller (Kafka + MQTT)

| Kafka command (topic `whirlpooling`) | Whirlpool | Answer |
| --- | --- | --- |
| `start B-7` with recipe `params` | empty: ledger written, then runs for `duration_s` | `running`, later `done`, then `idle` |
| `start B-7` again (any message id) | nothing (ledger: seen before) | the batch's last status again |
| `start B-8` while running | nothing | `error BUSY` |
| `stop` (any batch, or the running one) | stops, wort stays inside | `error STOPPED` (`NOT_RUNNING` if nothing runs) |
| `flush B-7` (running or stopped) | wort discarded, vessel empty | `error FLUSHED`, then `idle` (`NOT_HERE` for another batch) |

Stop and flush are kept very simple for testing: there is no resume after a stop, the only way out is a flush. A stopped or flushed batch is recorded in the ledger, so a re-sent start never runs it again. After a crash the batch that was running is reported as `error INTERRUPTED`.

**Recipe parameters (experimental, temporary).** The controller reads `duration_s` (1 to 86400, default 3600) and `speed_rpm` from each start. The Beer A / Beer B values come from `../machine_commons/machine_kafka/examples/recipes.c`, which only exists for testing; where recipes really live and who turns them into start parameters is still open.

**Heartbeats.** `machine_heartbeat_t` on MQTT topic `heartbeat`, QoS 1, every 2 s, with the machine name (`whirlpooling-2`). The Kafka `machine_id` is the number in that name.

### Run and test

Brokers first (Kafka, Kafka UI and Mosquitto):

```bash
(cd ../machine_commons/machine_kafka && docker compose up -d)
```

These targets need librdkafka, cJSON and MQTT-C, so on Windows run them through the dev container (`../machine_commons/dev/run.sh`, Git Bash):

```bash
../machine_commons/dev/run.sh make test test-mqtt   # state machine + heartbeats
../machine_commons/dev/run.sh make test-kafka       # start/stop/flush/restart end to end (~1 min)
TIME_SCALE=600 ../machine_commons/dev/run.sh make run   # the controller; 1 h run = 6 s
```

From a second terminal, play the scheduler:

```bash
cd ../machine_commons/machine_kafka
../dev/run.sh make examples
../dev/run.sh ./build/scheduler-cli start whirlpooling 1 B-0001 B   # Beer B (45 rpm)
../dev/run.sh ./build/scheduler-cli stop  whirlpooling 1
../dev/run.sh ./build/scheduler-cli flush whirlpooling 1 B-0001
```

The messages show up in the Kafka UI (<http://localhost:8080>, topic `whirlpooling`).

| Environment | Default | Meaning |
| --- | --- | --- |
| `KAFKA_BROKERS` | unset | Kafka servers. Unset: standalone demo, runs one batch at startup and only sends heartbeats. |
| `MACHINE_ID` | number in the machine name | Kafka `machine_id` |
| `LEDGER_PATH` | `whirlpooling-<id>.ledger` | the ledger file; must survive restarts |
| `TIME_SCALE` | 1 | simulated seconds per real second |

Arguments: `whirlpooling [mqtt-host [mqtt-port [machine-name]]]`, defaults `localhost 1883 whirlpooling-1`.

The Dockerfile still builds the demo (`make all`), since it is the template for the other machines.
