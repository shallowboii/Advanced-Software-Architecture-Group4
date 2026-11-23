#include "heartbeat.h"
#include "lautering.h"
#include "machine_kafka.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define MACHINE_TYPE "lautering"
#define TOPIC "machine." MACHINE_TYPE

typedef struct {
  int id;
  rd_kafka_t *producer;
  lautering_t lautering;
  lautering_batches_t batches;
  char batch_id[KAFKA_BATCH_LEN];
  uint64_t now;
} controller_t;

static void send_status(controller_t *c, kafka_type_t type) {
  kafka_message_t status = {0};
  status.type = type;
  snprintf(status.machine_type, sizeof(status.machine_type), "%s", MACHINE_TYPE);
  status.machine_id = c->id;
  memcpy(status.batch_id, c->batch_id, sizeof(status.batch_id));
  kafka_send(c->producer, TOPIC, &status);
}

static void on_message(const kafka_message_t *msg, void *context) {
  controller_t *c = context;
  if (msg->machine_id != c->id || msg->type != KAFKA_START ||
      c->lautering.state != LAUTERING_WAIT)
    return;

  c->batches.wort_batches = 1;
  if (lautering_start(&c->lautering, c->now) != LAUTERING_STARTED)
    return;
  memcpy(c->batch_id, msg->batch_id, sizeof(c->batch_id));
  printf("Lautering started\n");
  send_status(c, KAFKA_RUNNING);
  /* STOP / FLUSH and start parameters are not handled, as in whirlpooling. */
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
  if (brokers == NULL || brokers[0] == '\0') {
    fprintf(stderr, "KAFKA_BROKERS is not set (e.g. kafka:29092).\n");
    return EXIT_FAILURE;
  }

  enum MQTTErrors *mqtt_result = heartbeat_init("LAUTERING", machine_number);
  if (*mqtt_result != MQTT_OK) {
    printf("Could not start MQTT!\n");
  }

  struct timespec started;
  if (clock_gettime(CLOCK_MONOTONIC, &started) != 0) {
    perror("clock_gettime");
    return EXIT_FAILURE;
  }

  controller_t c = {.id = (int)machine_number};
  lautering_init(&c.lautering, &c.batches);
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

    c.now = now.tv_sec;
    kafka_poll(consumer, on_message, &c);
    rd_kafka_poll(c.producer, 0);

    if (c.lautering.state == LAUTERING_LAUTER &&
        lautering_update(&c.lautering, c.now) == LAUTERING_DONE) {
      printf("Lautering complete; waiting.\n");
      c.batches.sweet_wort_batches = 0; /* pumped to the next machine */
      send_status(&c, KAFKA_IDLE);
    }

    heartbeat_update("lautering", now.tv_sec);
    sleep(1);
  }

  heartbeat_destroy();
  kafka_consumer_close(consumer);
  kafka_producer_close(c.producer);
  return status;
}
