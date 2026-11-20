# Machine Kafka (production bus)

Shared C library for the production messages between the scheduler and the
machines. It wraps [librdkafka](https://github.com/confluentinc/librdkafka)
and [cJSON](https://github.com/DaveGamble/cJSON). The full design (topics,
JSON schema, sequences, configuration reasons, failure scenarios,
requirements mapping) is in [docs/kafka-design.md](docs/kafka-design.md);
diagrams are in [docs/uml/](docs/uml/).

## In short

| | |
|---|---|
| Topics | One per machine type, named after it: `mashing`, `lautering`, `boiling`, `whirlpooling`, `fermentation`, `filtering`. Commands and statuses share it. |
| Key | `machine_id`, so all messages for one machine stay in order. |
| Commands (scheduler → machine) | `start` (with `params`, e.g. `duration_s`), `stop` (FR-13), `flush` (FR-14: discard the batch). |
| Statuses (machine → scheduler) | `idle`, `running`, `done`, `error` (+ `code`/`reason`). |
| Who gets what | `mk_poll` gives a machine only commands (`MK_RECEIVE_COMMANDS`) and the scheduler only statuses (`MK_RECEIVE_STATUSES`). |
| Duplicates | Delivery is at-least-once. The **ledger** guarantees a machine never runs the same `batch_id` twice, across restarts of either side. |
| New machines | Same type: start it with a new id. New type: create its topic and add it to `MK_MACHINE_TYPES`. |

## Who uses it

| | Machine (every controller) | Scheduler |
|---|---|---|
| Bus | `mk_open` with its own type as topic, `MK_RECEIVE_COMMANDS` | `mk_open` with all types (`MK_MACHINE_TYPES`), `MK_RECEIVE_STATUSES` |
| Receives | `start`, `stop`, `flush` (ignores other `machine_id`s) | `idle`, `running`, `done`, `error` of every machine |
| Sends | `mk_make_idle/running/done/error` + `mk_send` | `mk_make_start` + `mk_param_set`, `mk_make_command(MK_STOP/MK_FLUSH)` |
| Ledger | yes: `mk_ledger_*` decides "run or answer from memory" | no: re-sending a start is safe |
| Reference | [`whirlpooling/src/main.c`](../../whirlpooling/src/main.c) | [`backend/src/scheduling`](../../backend/src/scheduling/src/scheduler.h) |

A machine has no "change recipe" function: the values for a batch travel in
the `params` of its `start`, and the machine reads them with `mk_param_get`.

## The ledger file

A small text file per machine that remembers the last 16 batches it accepted
and how they ended:

```
mk-ledger 2 boiling 3
D B-0007 scheduler-6aff1da4-17 -
E B-0008 scheduler-6aff1da4-21 OVERHEAT kettle above 105 C
X B-0009 scheduler-6aff1da4-22 BUSY B-0008
R B-0010 scheduler-6aff1da4-30 -
```

`D` done, `E` error (also STOPPED / FLUSHED), `X` refused without running,
`R` running. For every start the machine asks the ledger first
(`mk_ledger_lookup`): a known batch gets its last status back instead of
running again. A new batch is written to the file (`mk_ledger_accept`, with
fsync) **before** the machine starts, so a crash can't lead to a second run:
after the restart the `R` line becomes `error INTERRUPTED`, reported once.

## Files

| File | Contents |
|---|---|
| `machine_kafka.h` | The whole public API (`mk_` prefix: names like `send` or `poll` would clash with system functions). |
| `message.c` | Build, validate, encode and decode messages; params |
| `ledger.c` | The ledger file: lookup, accept, refuse, record |
| `bus.c` | librdkafka wrapper: open, send, poll, commit, rebalance, delivery reports |
| `tests/` | message, ledger and broker tests |
| `docker-compose.yml`, `scripts/` | Kafka, topic creation, Kafka UI |
| `../dev/` | dev container (gcc, librdkafka, cJSON, MQTT-C) and `run.sh` |

## Broker

```sh
docker compose up -d            # Kafka 3.9 (KRaft) + topics + Kafka UI
docker compose down -v          # stop and delete all messages and offsets
```

Kafka listens on `localhost:9092` (host) and `kafka:29092` (containers). The
Kafka UI runs on <http://localhost:8080>. Topic auto-creation is off; topics
come from [`scripts/create-topics.sh`](scripts/create-topics.sh). A new
machine type: `docker compose run --rm topics centrifuge`.

## Build and test

```sh
make             # build/libmachinekafka.a
make test        # message + ledger tests, no broker needed
make test-kafka  # bus tests against the broker (~35 s)
make libs        # compile/link flags for using the library
```

On Windows (Git Bash) or without the libraries installed, run make in the
dev container (the broker must be up for `test-kafka`):

```sh
../dev/run.sh make test test-kafka
```

Prerequisites on Linux: librdkafka ≥ 1.4 and cJSON ≥ 1.7.13
(Debian: `apt-get install librdkafka-dev libcjson-dev pkg-config`).

## Looking at messages

Kafka UI → Topics → e.g. `whirlpooling` → Messages shows commands and
statuses in order. A start by hand (Produce Message, key `1`; `ts_utc` must
be less than 5 minutes old or the machine drops it):

```json
{"schema_version":3,"message_id":"manual-1","type":"start","machine_type":"whirlpooling",
 "machine_id":1,"batch_id":"B-0007","ts_utc":"2026-11-20T10:00:00Z",
 "params":{"duration_s":3600},"error":null}
```

## Notes

- All calls on one bus come from one thread; `mk_poll` also serves the
  producer's delivery reports.
- A broker that is down is not an error: messages are delivered once it is
  back. Only `MK_ERR_FATAL` means the client has to be recreated.
- Clocks of scheduler and machines must agree within seconds (commands older
  than 300 s are dropped).
