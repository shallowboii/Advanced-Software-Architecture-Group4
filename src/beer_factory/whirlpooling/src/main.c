#include "heartbeat.h"
#include "machine_kafka.h"
#include "whirlpooling.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define MACHINE_TYPE "whirlpooling"
#define TOPIC "asaat67.machine." MACHINE_TYPE

typedef struct {
  int id;
  rd_kafka_t *producer;
  whirlpooling_t whirlpooling;
  whirlpooling_batches_t batches;
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
    if (c->whirlpooling.state != WHIRLPOOLING_WAIT)
      return;

    c->batches.hot_wort_batches = 1;
    if (whirlpooling_start(&c->whirlpooling, c->now) != WHIRLPOOLING_STARTED)
      return;
    memcpy(c->batch_id, msg->batch_id, sizeof(c->batch_id));
    printf("Whirlpooling started\n");
    send_status(c, KAFKA_RUNNING);
    break;

  case KAFKA_STOP:
    whirlpooling_stop(&c->whirlpooling);
    printf("Whirlpooling stopped\n");
    send_status(c, KAFKA_IDLE);
    break;

  case KAFKA_FLUSH:
    whirlpooling_flush(&c->whirlpooling);
    printf("Whirlpooling flushed\n");
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

  controller_t c = {.id = (int)machine_number};
  whirlpooling_init(&c.whirlpooling, &c.batches);
  char group_id[48];
  snprintf(group_id, sizeof(group_id), "%s-%d", MACHINE_TYPE, c.id);
  c.producer = kafka_producer_new(brokers);
  rd_kafka_t *consumer = kafka_consumer_new(brokers, group_id, TOPIC);

  int mqtt_on = *heartbeat_init("WHIRLPOOLING", machine_number) == MQTT_OK;
  if (!mqtt_on) {
    printf("Could not start MQTT!\n");
  }

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

    if (c.whirlpooling.state == WHIRLPOOLING_WHIRLPOOL &&
        whirlpooling_update(&c.whirlpooling, c.now) == WHIRLPOOLING_DONE) {
      printf("Whirlpooling complete; waiting.\n");
      c.batches.clarified_wort_batches = 0; /* pumped to a tank */
      send_status(&c, KAFKA_IDLE);
    }

    heartbeat_update(MACHINE_TYPE, now.tv_sec);
    /* Short pause: at 1 s = 1 h a one-second sleep is a simulated hour. */
    nanosleep(&(struct timespec){.tv_nsec = 2000000}, NULL);
  }

  if (mqtt_on)
    heartbeat_destroy();
  kafka_consumer_close(consumer);
  kafka_producer_close(c.producer);
  return status;
}
