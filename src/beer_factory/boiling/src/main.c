#include "boiling.h"
#include "heartbeat.h"
#include "machine_kafka.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <netdb.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define MACHINE_TYPE "boiling"
#define TOPIC "asaat67.machine." MACHINE_TYPE

typedef struct {
  int id;
  rd_kafka_t *producer;
  boiling_t boiling;
  boiling_batches_t batches;
  char batch_id[KAFKA_BATCH_LEN];
  uint64_t now;
} controller_t;

static void send_status(controller_t *c, kafka_type_t type) {
  kafka_message_t status = {0};
  status.type = type;
  snprintf(status.machine_type, sizeof(status.machine_type), "%s",
           MACHINE_TYPE);
  status.machine_id = c->id;
  memcpy(status.batch_id, c->batch_id, sizeof(status.batch_id));
  kafka_send(c->producer, TOPIC, &status);
}

static void on_message(const kafka_message_t *msg, void *context) {
  controller_t *c = context;
  if (msg->machine_id != c->id)
    return;

  switch (msg->type) {
  case KAFKA_START:
    if (c->boiling.state != BOILING_WAIT)
      return;

    /* The scheduler has supplied one batch of sweet wort. */
    c->batches.sweet_wort_batches = 1;
    if (boiling_start(&c->boiling, c->now) != BOILING_STARTED)
      return;
    memcpy(c->batch_id, msg->batch_id, sizeof(c->batch_id));
    printf("Boiling started\n");
    send_status(c, KAFKA_RUNNING);
    break;

  case KAFKA_STOP:
    boiling_stop(&c->boiling);
    printf("Boiling stopped\n");
    send_status(c, KAFKA_IDLE);
    break;

  case KAFKA_FLUSH:
    boiling_flush(&c->boiling);
    printf("Boiling flushed\n");
    send_status(c, KAFKA_IDLE);
    break;

  default:
    break;
  }
}

int main(int argc, char **argv) {
  if (argc != 2) {
    fprintf(stderr, "Usage: %s <machine-number>\n", argv[0]);
    return EXIT_FAILURE;
  }

  long machine_number = atoi(argv[1]);
  if (machine_number < 0) {
    printf("Machine number must be >= 0\n");
    return EXIT_FAILURE;
  }

  const char *brokers = getenv("KAFKA_BROKERS");
  if (brokers == NULL || brokers[0] == '\0')
    brokers = "localhost:9092";

  enum MQTTErrors *mqtt_result = heartbeat_init("BOILING", machine_number);
  if (*mqtt_result != MQTT_OK) {
    printf("Could not start MQTT!\n");
  }

  struct timespec started;
  if (clock_gettime(CLOCK_MONOTONIC, &started) != 0) {
    perror("clock_gettime");
    return EXIT_FAILURE;
  }

  controller_t c = {.id = machine_number};
  boiling_init(&c.boiling, &c.batches);
  char group_id[48];
  snprintf(group_id, sizeof(group_id), "%s-%d", MACHINE_TYPE, c.id);
  c.producer = kafka_producer_new(brokers);
  rd_kafka_t *consumer = kafka_consumer_new(brokers, group_id, TOPIC);

  int status = EXIT_SUCCESS;
  while (1) {
    struct timespec now;
    if (clock_gettime(CLOCK_MONOTONIC, &now) != 0) {
      perror("clock_gettime");
      status = EXIT_FAILURE;
      break;
    }

    /* Simulated seconds: real time scaled by TIME_DIVISOR, kept below a second
     * so a fast clock does not round steps to whole real seconds. */
    c.now = (uint64_t)now.tv_sec * TIME_DIVISOR +
            (uint64_t)now.tv_nsec * TIME_DIVISOR / 1000000000;
    kafka_poll(consumer, on_message, &c);
    rd_kafka_poll(c.producer, 0);

    if (c.boiling.state == BOILING_BOIL &&
        boiling_update(&c.boiling, c.now) == BOILING_DONE) {
      printf("Boiling complete; waiting.");
      c.batches.hot_wort_batches = 0; /* pumped to the next machine */
      send_status(&c, KAFKA_IDLE);
    }

    heartbeat_update("boiling", now.tv_sec);
    /* Short pause: at 1 s = 1 h a one-second sleep is a simulated hour. */
    nanosleep(&(struct timespec){.tv_nsec = 2000000}, NULL);
  }

  heartbeat_destroy();
  kafka_consumer_close(consumer);
  kafka_producer_close(c.producer);
  return status;
}
