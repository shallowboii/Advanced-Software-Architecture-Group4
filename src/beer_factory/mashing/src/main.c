#include "heartbeat.h"
#include "machine_kafka.h"
#include "mashing.h"

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

#define MACHINE_TYPE "mashing"
#define TOPIC "asaat67.machine." MACHINE_TYPE

typedef struct {
  int id;
  rd_kafka_t *producer;
  mashing_t mashing;
  unsigned int wort_batches;
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
      c->mashing.state != MASHING_WAIT)
    return;

  if (mashing_start(&c->mashing, c->now) != MASHING_STARTED)
    return;
  memcpy(c->batch_id, msg->batch_id, sizeof(c->batch_id));
  printf("Mashing started\n");
  send_status(c, KAFKA_RUNNING);
  /* STOP / FLUSH and start parameters are not handled, as in other machines. */
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

  controller_t c = {.id = (int)machine_number};
  mashing_init(&c.mashing, &c.wort_batches);
  char group_id[48];
  snprintf(group_id, sizeof(group_id), "%s-%d", MACHINE_TYPE, c.id);
  c.producer = kafka_producer_new(brokers);
  rd_kafka_t *consumer = kafka_consumer_new(brokers, group_id, TOPIC);

  int mqtt_on = *heartbeat_init("MASHING", machine_number) == MQTT_OK;
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

    c.now = now.tv_sec;
    kafka_poll(consumer, on_message, &c);
    rd_kafka_poll(c.producer, 0);

    if (c.mashing.state == MASHING_MASH &&
        mashing_update(&c.mashing, c.now) == MASHING_DONE) {
      puts("done_mashing");
      fflush(stdout);
      c.wort_batches = 0; /* pumped to lautering */
      send_status(&c, KAFKA_IDLE);
    }

    heartbeat_update("mashing", now.tv_sec);
    sleep(1);
  }

  if (mqtt_on)
    heartbeat_destroy();
  kafka_consumer_close(consumer);
  kafka_producer_close(c.producer);
  return status;
}
