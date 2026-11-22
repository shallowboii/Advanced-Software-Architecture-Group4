# machine_kafka

A thin C wrapper around [librdkafka](https://github.com/confluentinc/librdkafka). The scheduler uses it to send commands to the machines, and the machines use it to send their status back. It is two files: `machine_kafka.h` and `machine_kafka.c`.

The library only covers the happy path for now. Places that still need work are marked `TODO` in the code.

## How it works

- **One topic per machine type**, e.g. `machine.whirlpooling`. Commands and statuses go on the same topic. The library does not create topics.
- **The key is the machine id**, so all messages for one machine land on the same partition and stay in order.
- **Everyone reads their own messages back.** Skip them by type: a machine handles only commands with its own id, and the scheduler handles only statuses. The library doesn't filter; your handler does.
- **Producer:** `enable.idempotence=true` (retries can't duplicate or reorder) and `linger.ms=0` (no batching). Nothing else is configured.
- **Consumer:** `enable.auto.commit=false`. `kafka_poll` commits a message only after your handler returned, so delivery is at-least-once. If a process crashes mid-handling, it gets the message again, which means duplicates are possible.
- **Payload:** for now, the raw bytes of `kafka_message_t`. Both sides must be built from the same header.

| Type | Direction |
| --- | --- |
| `KAFKA_START`, `KAFKA_STOP`, `KAFKA_FLUSH` | command: scheduler → machine |
| `KAFKA_IDLE`, `KAFKA_RUNNING`, `KAFKA_ERROR` | status: machine → scheduler |

## API

| Function | What it does |
| --- | --- |
| `kafka_producer_new(brokers)` | creates a producer |
| `kafka_send(producer, topic, &msg)` | queues `msg` on `topic` with key `msg.machine_id` |
| `kafka_producer_close(producer)` | waits up to 10 s for outstanding messages, then frees the producer |
| `kafka_consumer_new(brokers, group_id, topic)` | creates a consumer subscribed to one topic |
| `kafka_poll(consumer, handler, context)` | waits up to 100 ms for one message, calls `handler(&msg, context)`, then commits. Kafka errors and messages of the wrong size are printed and skipped. Returns 1 if it handled a message, 0 if not |
| `kafka_consumer_close(consumer)` | leaves the group and frees the consumer |

`handler` is your function of type `kafka_message_handler`: `void handler(const kafka_message_t *msg, void *context)`. `context` is passed through to it unchanged. Use it to reach your own state without globals.

If a sent message can't be delivered, `on_delivery_report` in `machine_kafka.c` prints it to stderr. It only runs during `kafka_send`, `kafka_producer_close` or `rd_kafka_poll(producer, 0)`, so call the last one in your loop.

Every participant needs one producer and one consumer, because a librdkafka handle is either one or the other. Each machine instance needs its **own** `group_id` (e.g. `whirlpooling-1`) so that it reads the whole topic. If instances shared a group, Kafka would split the partitions between them, and a machine could miss its own commands.

## Known limitations

- **Create the topic before starting a consumer.** If it doesn't exist yet, `kafka_poll` prints `Kafka: Subscribed topic not available: ...` and keeps waiting. It picks the topic up once it's created, but messages sent before that are missed.
- **Messages of the wrong size are skipped.** `kafka_poll` prints `Kafka: skipped a message of N bytes` and commits it, so it isn't read again.
- **Send only messages built with `kafka_message_t`.** A message of the right size whose text fields have no closing `'\0'` still reaches the handler, and printing such a field reads past its end. Don't use "Produce Message" in a Kafka UI on these topics.
- **Failures aren't reported to the caller.** `kafka_send` always returns 0, and the create functions don't check their settings.

## Usage

### Machine side

From `whirlpooling/src/main.c`, shortened:

```c
static void on_message(const kafka_message_t *msg, void *context) {
  controller_t *c = context;
  if (msg->machine_id != c->id)
    return;                          /* for another machine */
  switch (msg->type) {
  case KAFKA_START:
    /* start the work, then: */
    send_status(c, KAFKA_RUNNING);   /* kafka_send(c->producer, TOPIC, &status) */
    break;
  default:
    break;                           /* statuses, including our own */
  }
}

c.producer = kafka_producer_new(brokers);
c.consumer = kafka_consumer_new(brokers, "whirlpooling-1", "machine.whirlpooling");
while (!stopping) {
  kafka_poll(c.consumer, on_message, &c);   /* also the loop's 100 ms sleep */
  rd_kafka_poll(c.producer, 0);             /* serve delivery reports */
  /* advance the machine; when done: send_status(c, KAFKA_IDLE) */
}
kafka_consumer_close(c.consumer);
kafka_producer_close(c.producer);
```

### Scheduler side

This is a complete program. It sends one start to whirlpooling 1 and prints the answers, so it also works as the test sender below.

```c
#include "machine_kafka.h"

#include <stdio.h>
#include <string.h>

#define BROKERS "localhost:9092"
#define TOPIC "machine.whirlpooling"
#define WARMUP_POLLS 60  /* ~6 s: let the group assign partitions first */
#define LISTEN_POLLS 150 /* ~15 s: wait for the answers */

static void on_message(const kafka_message_t *msg, void *context) {
  (void)context; /* not used */
  if (msg->type == KAFKA_START || msg->type == KAFKA_STOP ||
      msg->type == KAFKA_FLUSH)
    return; /* our own command */
  printf("status %d from %s-%d, batch %s\n", msg->type, msg->machine_type,
         msg->machine_id, msg->batch_id);
}

int main(void) {
  rd_kafka_t *producer = kafka_producer_new(BROKERS);
  rd_kafka_t *consumer = kafka_consumer_new(BROKERS, "scheduler", TOPIC);
  for (int i = 0; i < WARMUP_POLLS; i++)
    kafka_poll(consumer, on_message, NULL);

  kafka_message_t start;
  memset(&start, 0, sizeof(start));
  start.type = KAFKA_START;
  strcpy(start.machine_type, "whirlpooling");
  start.machine_id = 1;
  strcpy(start.batch_id, "B-1");
  kafka_send(producer, TOPIC, &start);
  printf("sent start B-1\n");

  for (int i = 0; i < LISTEN_POLLS; i++)
    kafka_poll(consumer, on_message, NULL);
  kafka_consumer_close(consumer);
  kafka_producer_close(producer);
  return 0;
}
```

## Build

You need librdkafka (Debian/Ubuntu: `apt-get install librdkafka-dev`, Arch: `pacman -S librdkafka`). It builds on Linux; on Windows, use Docker or WSL.

Either compile `machine_kafka.c` together with your sources, which is what whirlpooling does:

```bash
gcc -I path/to/machine_kafka my_machine.c path/to/machine_kafka/machine_kafka.c -lrdkafka
```

or build the static library with `make` (output: `build/libmachinekafka.a`). `make libs` prints the compiler flags to link against it.

## Test (end to end with whirlpooling)

This runs a real broker, whirlpooling 1, and the scheduler program from above. All commands run from `src/beer_factory` in Git Bash. `MSYS_NO_PATHCONV=1` stops Git Bash from rewriting the container paths.

**1. Start a broker and create the topic:**

```bash
export MSYS_NO_PATHCONV=1
docker run -d --name kafka apache/kafka:4.1.0
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic machine.whirlpooling --partitions 3
```

If the topic command fails with a connection error, the broker isn't up yet. Wait a few seconds and run it again.

**2. Open a build shell that shares the broker's network**, so `localhost:9092` works inside it:

```bash
docker run --rm -it --network container:kafka -v "$(pwd -W)":/src -w /src gcc:14 bash
```

**3. Inside that shell, install the dependencies.** MQTT-C is only needed because whirlpooling also has an MQTT heartbeat:

```bash
apt-get update && apt-get install -y librdkafka-dev
git -c advice.detachedHead=false clone -q --depth 1 -b v1.1.7 \
  https://github.com/LiamBindle/MQTT-C /opt/MQTT-C
```

**4. Build and start whirlpooling 1.** With `TIME_SCALE=600`, a one-hour run takes 6 s.

```bash
make -C whirlpooling build/whirlpooling MQTT_C_DIR=/opt/MQTT-C
KAFKA_BROKERS=localhost:9092 TIME_SCALE=600 ./whirlpooling/build/whirlpooling whirlpooling-1 &
```

This test has no MQTT broker, so whirlpooling first prints the following. That's expected, and the Kafka test works without it. To test the heartbeat too, see the [whirlpooling README](../../whirlpooling/README.md#mqtt-heartbeat).

```text
Could not connect to MQTT broker localhost:1883.
Could not start MQTT!
```

**5. Wait about 10 s** so that whirlpooling's consumer gets its partitions. Then save the scheduler program from above as `/tmp/send_start.c` and run it:

```bash
gcc -I machine_commons/machine_kafka /tmp/send_start.c \
  machine_commons/machine_kafka/machine_kafka.c -lrdkafka -o /tmp/send_start
/tmp/send_start
```

Expected output: `KAFKA_RUNNING` (4) about a second after the start, then `KAFKA_IDLE` (3) about 6 s later:

```text
sent start B-1
Whirlpooling B-1.
status 4 from whirlpooling-1, batch B-1
Whirlpooled B-1.
status 3 from whirlpooling-1, batch B-1
```

**6. Optional: check the keys and partitions.** From a second Git Bash window on the host:

```bash
MSYS_NO_PATHCONV=1 docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic machine.whirlpooling --from-beginning \
  --property print.key=true --property print.partition=true --property print.value=false \
  --timeout-ms 5000
```

All three messages (the start and both statuses) show key `1` on the same partition. The values are binary for now, which is why they aren't printed.

**7. Clean up:** `exit` the build shell, then run `docker rm -f kafka`.
