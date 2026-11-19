# Machine Kafka (production bus)

Shared C library for sending and receiving production messages between the
scheduler and the machines over Kafka. It wraps
[librdkafka](https://github.com/confluentinc/librdkafka) and
[cJSON](https://github.com/DaveGamble/cJSON). MQTT heartbeats (monitoring bus) are
in `../machine_mqtt`.

## Design

| | |
|---|---|
| Topics | One per machine type: `boiling`, `whirlpooling`, `tank`, … The topic of a message is its `machine_type`. |
| Key | `machine_id` (e.g. `"3"`), so all messages for one machine stay in order. |
| Messages | `start` (scheduler → machine), `done` and `error` (machine → scheduler). |
| Discovery | None over Kafka. The scheduler learns about machines from their MQTT heartbeats. |
| Producer | Idempotent (`enable.idempotence=true`, `acks=all`), no batching (`linger.ms=0`), retries until the broker is back (`message.timeout.ms=0`). |
| Consumer | Own consumer group per process, offsets committed **after** the handler returns, `auto.offset.reset=earliest`. |

### Exactly once

Kafka's own exactly-once only covers "read from Kafka → write to Kafka".
Starting a machine happens outside Kafka, so the library gives you:

1. **No duplicates from the producer.** The idempotent producer de-duplicates its own retries.
2. **No lost messages.** The offset is committed only after handling, so a crash means the message is delivered again (at-least-once).
3. **Duplicate detection.** Every message has a unique `message_id`. A machine
   calls `mk_dedup_seen()` and ignores a start it has already handled. Together with 1 and 2
   this gives exactly-once processing.

`mk_dedup_t` lives in memory, so it is lost when the machine restarts. That is fine
as long as the machine state itself isn't persisted either. Once machines persist
state, persist the handled ids with it.

Since commands and replies share one topic, every consumer also sees the
other messages on it. A machine ignores everything except `start` with its own
`machine_id`; the scheduler ignores `start`.

## Message format

```json
{"schema_version":1,"message_id":"scheduler-6afdb02e-1","type":"start",
 "machine_type":"boiling","machine_id":3,"batch_id":"B-0007","reply_to":null,
 "ts_utc":"2026-11-18T12:00:00Z","error":null}
```

| Field | start | done | error |
|---|---|---|---|
| `message_id` | required (set by `mk_send`) | required | required |
| `machine_type`, `machine_id` | target machine | sending machine | sending machine |
| `batch_id` | required | copied from start | copied from start |
| `reply_to` | `null` | `message_id` of the start | `message_id` of the start |
| `error` | `null` | `null` | `{"code":"BUSY","reason":"..."}` |

Empty fields are `null`. Decoding ignores unknown fields, so fields can be
added later without breaking older readers. Change `schema_version` only for
incompatible changes.

## Install

```sh
sudo pacman -S librdkafka cjson                 # Arch / CachyOS
sudo apt-get install librdkafka-dev libcjson-dev # Debian (gcc:14 image)
```

The runtime Docker image needs `librdkafka1 libcjson1`.

## Build and test

```sh
make             # build/libmachinekafka.a
make test        # message encode/decode tests, no broker needed
make test-kafka  # round trip against a real broker (KAFKA_BROKERS=localhost:9092)
make libs        # prints the compile/link flags for using the library
```

`make test-kafka` needs a broker where topics are auto-created, for example:

```sh
docker run --rm -p 9092:9092 apache/kafka:3.9.1
```

## Usage

### Machine

```c
#include "machine_kafka.h"

typedef struct { mk_bus_t *bus; int id; mk_dedup_t seen;
                 mk_message_t current; int busy; } ctx_t;

static void on_message(const mk_message_t *msg, void *user) {
  ctx_t *ctx = user;
  if (msg->type != MK_START || msg->machine_id != ctx->id) return;
  if (mk_dedup_seen(&ctx->seen, msg->message_id)) return;   /* redelivery */

  if (ctx->busy) {                       /* e.g. boiling_start() refused */
    mk_message_t err;
    mk_make_error(&err, msg, "BUSY", "already processing a batch");
    mk_send(ctx->bus, &err);
    return;
  }
  ctx->current = *msg;                   /* keep it to reply to later */
  ctx->busy = 1;                         /* boiling_start(...) */
}

int main(void) {
  const char *topics[] = {"boiling"};
  mk_config_t cfg = {.brokers = "kafka:9092", .client_id = "boiling-3",
                     .topics = topics, .n_topics = 1};
  ctx_t ctx = {.bus = mk_open(&cfg), .id = 3};
  mk_dedup_init(&ctx.seen);

  for (;;) {
    if (mk_poll(ctx.bus, 100, on_message, &ctx) == MK_ERR_FATAL) break;
    if (ctx.busy /* && boiling_update(...) == BOILING_DONE */) {
      mk_message_t done;
      mk_make_done(&done, &ctx.current);
      mk_send(ctx.bus, &done);
      ctx.busy = 0;
    }
  }
  mk_close(ctx.bus, 5000);
}
```

### Scheduler

Subscribe to every machine type topic with `.client_id = "scheduler"`. Send
with `mk_make_start` + `mk_send`, then remember `start.message_id` (filled in by
`mk_send`). Match incoming `done` / `error` messages on `reply_to`.

## Notes

- All calls are made from one thread (the machine's main loop); `mk_poll`
  also serves the producer's delivery reports.
- A broker that is down is not an error: `mk_open` and `mk_send` succeed, and
  messages are delivered once the broker is reachable. librdkafka logs
  "brokers are down" to stderr meanwhile.
- `mk_close` waits up to the given time and returns the number of messages that
  were not delivered.
- Only `MK_ERR_FATAL` means the client has to be recreated.
