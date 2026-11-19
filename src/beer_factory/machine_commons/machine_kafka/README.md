# Machine Kafka (production bus)

Shared C library for the production messages between the scheduler and the
machines. It wraps [librdkafka](https://github.com/confluentinc/librdkafka)
and [cJSON](https://github.com/DaveGamble/cJSON). MQTT heartbeats
(liveness) are in `../machine_mqtt`.

The full design (topic table, JSON schema for other languages, sequence
diagrams, configuration reasons, failure scenarios, requirements mapping) is
in [docs/kafka-design.md](docs/kafka-design.md).

## In short

| | |
|---|---|
| Topics | One per machine type, named after it: `mashing`, `lautering`, `boiling`, `whirlpooling`, `fermentation`, `filtering`. Commands (scheduler → machines) and statuses (machines → scheduler) share it. |
| Key | `machine_id`, so all messages for one machine stay in order. |
| Commands | `start` (with recipe `params`), `stop` (FR-13) and `flush` (FR-14: discard the batch in the machine). Machines may answer `stop`/`flush` with error `UNSUPPORTED` until they implement them. |
| Statuses | `idle`, `running`, `done`, `error` (+ `code`/`reason`), sent on every state change. |
| Who gets what | `mk_poll` gives a machine only commands (`receive = MK_RECEIVE_COMMANDS`) and the scheduler only statuses (`MK_RECEIVE_STATUSES`). |
| Duplicates | Delivery is at-least-once. The **ledger** (`mk_ledger_*`, a small file per machine) guarantees a machine never runs the same `batch_id` twice, across restarts of either side. A repeated start is answered with the batch's last status. |
| New machines | Same type: start it, nothing else. New type: create its topic and add it to the scheduler's topic list. |
| Discovery | Not over Kafka: the scheduler learns about machines from MQTT heartbeats and dispatches once it has seen their `idle`. |

## Files

| File | Contents |
|---|---|
| `machine_kafka.h` | The whole public API. Functions keep the `mk_` prefix: C has no namespaces, and names like `send`, `poll` or `open` would clash with system functions. |
| `message.c` | Build, validate, encode and decode messages; params |
| `ledger.c` | The ledger file: lookup, accept, refuse, record |
| `bus.c` | librdkafka wrapper: open, send, poll, commit, rebalance, delivery reports |
| `trace.c`, `trace.h` | `MK_TRACE=1` debug output (internal) |
| `tests/` | message, ledger and broker tests |
| `docker-compose.yml`, `scripts/` | broker, topic creation, Kafka UI, `docker-make.sh` |

## Prerequisites

| | Version | Arch / CachyOS | Debian / Ubuntu |
|---|---|---|---|
| C compiler, make, pkg-config | C11 | `sudo pacman -S base-devel pkgconf` | `sudo apt-get install build-essential pkg-config` |
| librdkafka | ≥ 1.4 (uses `rd_kafka_produceva`), tested with 2.8.0; Debian 12 ships 2.0.2 | `sudo pacman -S librdkafka` | `sudo apt-get install librdkafka-dev` |
| cJSON | ≥ 1.7.13 (`cJSON_ParseWithLength`), tested with 1.7.18 | `sudo pacman -S cjson` | `sudo apt-get install libcjson-dev` |
| Docker + Compose | for the broker | `sudo pacman -S docker docker-compose` | [docs.docker.com/engine/install/debian](https://docs.docker.com/engine/install/debian/) |

Runtime image of a machine (Debian): `apt-get install librdkafka1 libcjson1`.

On Windows, build and test inside a container instead (see below).

## Broker

```sh
docker compose up -d            # Kafka 3.9 in KRaft mode + creates the topics + Kafka UI
docker compose logs topics      # list of created topics
docker compose down -v          # stop and delete all messages and offsets
```

Kafka UI (topics, messages, consumer groups and their lag) runs on
<http://localhost:8080>.

The broker listens on `localhost:9092` for programs on the host and on
`kafka:29092` for containers in the compose network. Topic auto-creation is
off; topics come from [`scripts/create-topics.sh`](scripts/create-topics.sh)
(6 topics, 1 partition, 30 days retention). For a new machine type:

```sh
docker compose run --rm topics centrifuge
```

Production values: `REPLICATION=3 MIN_ISR=2 ./scripts/create-topics.sh`.

## Build and test

```sh
make             # build/libmachinekafka.a
make test        # message + ledger tests, no broker needed
make test-kafka  # integration tests, needs the broker (KAFKA_BROKERS=localhost:9092)
make libs        # prints the compile/link flags for using the library
```

`make test-kafka` creates and deletes its own topics (`mktest<time>`,
`mktestnew<time>`) and needs topic auto-creation **off**, as in
`docker-compose.yml` (one test checks that sending to a missing topic is
reported). It takes about 35 s.

On Windows (Git Bash) or any host without the libraries, run make inside a
throw-away container instead. The broker from `docker compose up -d` must be
running:

```sh
./scripts/docker-make.sh            # make test test-kafka, in a gcc:14 container
./scripts/docker-make.sh trace      # any other targets
```

The script installs librdkafka and cJSON in the container, mounts this folder
and points the tests at `kafka:29092`. Plain `make` in cmd or PowerShell does
not work on Windows: cJSON and librdkafka are not installed there.

## Usage

### Machine

One thread, one loop. The handler only decides and records; the long work
happens in the loop (the handler must return quickly, `mk_poll` has to be
called at least every 5 minutes).

```c
#include "machine_kafka.h"

typedef struct {
  mk_bus_t *bus;
  mk_ledger_t ledger;
  const char *type;
  int id;
  int busy;
  mk_message_t current; /* the start being worked on */
} machine_t;

static void on_command(const mk_message_t *msg, void *user) {
  machine_t *m = user;
  mk_message_t status;
  if (msg->machine_id != m->id)
    return; /* for another boiler */
  if (msg->type != MK_START) { /* stop / flush: not implemented yet */
    if (mk_make_error(&status, msg, "UNSUPPORTED", mk_type_name(msg->type)) == MK_OK)
      mk_send(m->bus, &status);
    return;
  }
  if (mk_ledger_lookup(&m->ledger, msg, &status) == 1) {
    mk_send(m->bus, &status); /* seen before: repeat its status, don't run */
    return;
  }
  double temp, duration;
  if (m->busy) {
    mk_ledger_refuse(&m->ledger, msg, "BUSY", m->current.batch_id, &status);
  } else if (!mk_param_get(msg, "target_temp_c", &temp) ||
             !mk_param_get(msg, "duration_s", &duration)) {
    mk_ledger_refuse(&m->ledger, msg, "MISSING_PARAM", "target_temp_c, duration_s", &status);
  } else if (mk_ledger_accept(&m->ledger, msg) != MK_OK) {
    mk_make_error(&status, msg, "LEDGER", "cannot write state file");
  } else {
    /* Recorded on disk: only now touch the hardware. */
    m->current = *msg;
    m->busy = 1; /* boiling_start(temp, duration) */
    mk_make_running(&status, msg);
  }
  mk_send(m->bus, &status);
}

int main(void) {
  machine_t m = {.type = "boiling", .id = 3};
  /* On a volume, so it survives container restarts. */
  if (mk_ledger_open(&m.ledger, "/var/lib/boiling/ledger-3.state", m.type, m.id) != MK_OK)
    return 1; /* corrupt or foreign file: don't run without protection */

  const char *topics[] = {m.type}; /* the topic is the machine type */
  mk_config_t cfg = {.brokers = "kafka:29092", .client_id = "boiling-3",
                     .topics = topics, .n_topics = 1,
                     .receive = MK_RECEIVE_COMMANDS};
  /* group_id = client_id, starts at the end of the topic,
   * commands older than 300 s are dropped */
  m.bus = mk_open(&cfg);
  if (m.bus == NULL)
    return 1;

  int announced = 0;
  for (;;) {
    if (mk_poll(m.bus, 100, on_command, &m) == MK_ERR_FATAL)
      break;
    mk_message_t status;
    if (!announced && mk_ready(m.bus)) { /* now no new command can be missed */
      while (mk_ledger_next_interrupted(&m.ledger, &status))
        mk_send(m.bus, &status); /* batches cut off by the last crash */
      mk_make_idle(&status, m.type, m.id);
      mk_send(m.bus, &status);
      announced = 1;
    }
    if (m.busy /* && boiling_update(...) == BOILING_DONE */) {
      mk_make_done(&status, &m.current);
      mk_ledger_record(&m.ledger, &status); /* before sending */
      mk_send(m.bus, &status);
      m.busy = 0;
      mk_make_idle(&status, m.type, m.id); /* ready for the next batch */
      mk_send(m.bus, &status);
    }
  }
  mk_close(m.bus, 5000);
}
```

### Scheduler (snippet)

```c
#include "machine_kafka.h"

static void on_status(const mk_message_t *s, void *user) {
  (void)user;
  switch (s->type) {
  case MK_IDLE:    /* (s->machine_type, s->machine_id) may get work, if its heartbeat is alive */ break;
  case MK_RUNNING: /* s->batch_id is being processed on that machine */ break;
  case MK_DONE:    /* move s->batch_id on, unless already done (statuses can repeat) */ break;
  case MK_ERROR:   /* hold s->batch_id, alert the operator: s->error_code, s->error_reason */ break;
  default: break;
  }
}

const char *topics[] = {MK_MACHINE_TYPES}; /* all six machine types */
mk_config_t cfg = {.brokers = "kafka:29092", .client_id = "scheduler",
                   .topics = topics, .n_topics = sizeof(topics) / sizeof(topics[0]),
                   .receive = MK_RECEIVE_STATUSES}; /* starts at the beginning: never miss a status */
mk_bus_t *bus = mk_open(&cfg);

mk_message_t start;
mk_make_start(&start, "boiling", 3, "B-0007");
mk_param_set(&start, "target_temp_c", 100);
mk_param_set(&start, "duration_s", 3600);
mk_param_set(&start, "hops_g", 250);
mk_send(bus, &start); /* -> topic "boiling", key "3"; start.message_id is now set */

for (;;)
  mk_poll(bus, 100, on_status, NULL);
```

Match statuses on `(machine_type, machine_id, batch_id)`. Re-sending a start
(after a timeout or a scheduler restart) is safe: the machine answers with
the batch's last status instead of running it again.

## Debugging

| Environment variable | Effect |
|---|---|
| `MK_TRACE=1` | One line per library call and Kafka event on stderr: `mk <time> <who> <event> key=value ...` (send, recv, handler.call/return, commit, delivery, rebalance and start offsets, ledger lookup/accept/refuse/record, and the exact reason for every rejected message). |
| `MK_KAFKA_DEBUG=cgrp,topic,msg` | librdkafka's own debug log (any value of its `debug` property, e.g. `all`). |

```sh
make trace                                  # broker test with MK_TRACE=1 -> build/trace.log
MK_TRACE=1 ./my-machine 2> trace.log        # any program using the library
make clean && make build/test-bus CFLAGS="-std=c11 -Wall -Wextra -g -O0" && gdb ./build/test-bus
```

The trace code is internal (`trace.c`/`trace.h`) and does nothing unless
`MK_TRACE` is set.

## Looking at messages

The Kafka UI (<http://localhost:8080>) shows every topic's messages and every
consumer group. From a terminal (in Git Bash prefix these with
`MSYS_NO_PATHCONV=1`, otherwise `/opt/...` is rewritten to a Windows path):

```sh
docker exec beer-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
# everything on the boiling topic, with key (= machine id)
docker exec beer-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic boiling --from-beginning --property print.key=true
# where each consumer group is (lag)
docker exec beer-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --all-groups
```

A start by hand (Kafka UI → Topics → `boiling` → Produce Message, key `3`;
`ts_utc` must be less than 5 minutes old or the machine drops it):

```json
{"schema_version":3,"message_id":"manual-1","type":"start","machine_type":"boiling",
 "machine_id":3,"batch_id":"B-0007","ts_utc":"2026-11-19T10:00:00Z",
 "params":{"target_temp_c":100,"duration_s":3600},"error":null}
```

## API reference (short)

| Function | Purpose |
|---|---|
| `mk_make_start`, `mk_make_command` | build a start / stop / flush |
| `mk_param_set`, `mk_param_get` | recipe parameters on a start (≤ 16, name → finite number) |
| `mk_make_idle`, `mk_make_running`, `mk_make_done`, `mk_make_error` | build statuses (the last three answer a received command) |
| `mk_encode`, `mk_decode` | JSON ↔ `mk_message_t` (validating) |
| `mk_ledger_open/lookup/accept/refuse/record/next_interrupted` | never run a `batch_id` twice |
| `mk_open`, `mk_ready`, `mk_send`, `mk_poll`, `mk_close` | the bus |
| `mk_outstanding`, `mk_delivery_failures` | producer queue / permanent failures |

## Notes

- All calls on one bus come from one thread; `mk_poll` also serves the
  producer's delivery reports.
- A broker that is down is not an error: `mk_open` and `mk_send` succeed and
  messages are delivered once it is back. librdkafka logs "brokers are down"
  meanwhile.
- `mk_close` waits up to the given time and returns the number of messages
  that were not delivered.
- Only `MK_ERR_FATAL` means the client has to be recreated.
- Clocks of scheduler and machines must agree within seconds (command expiry
  uses `ts_utc`).
