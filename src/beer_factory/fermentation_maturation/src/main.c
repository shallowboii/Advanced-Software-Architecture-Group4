#include "heartbeat.h"
#include "machine_kafka.h"
#include "mqtt.h"
#include "tank.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define MACHINE_TYPE "fermentation_maturation"
#define TOPIC "asaat67.machine." MACHINE_TYPE

typedef struct {
  int id;
  rd_kafka_t *producer;
  tank_t tank;
  tank_shared_t shared;
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
  if (msg->machine_id != c->id)
    return;

  switch (msg->type) {
  case KAFKA_START:
    if (c->tank.state != TANK_WAIT)
      return;

    c->shared.chosen_tank = c->id;
    c->shared.clarified_wort_batches = 1;
    if (tank_start(&c->tank, c->now) != TANK_STARTED)
      return;
    memcpy(c->batch_id, msg->batch_id, sizeof(c->batch_id));
    printf("Fermentation started\n");
    send_status(c, KAFKA_RUNNING);
    break;

  case KAFKA_STOP:
    tank_stop(&c->tank);
    printf("Fermentation stopped\n");
    send_status(c, KAFKA_IDLE);
    break;

  case KAFKA_FLUSH:
    tank_flush(&c->tank);
    printf("Fermentation flushed\n");
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
  if (tank_shared_init(&c.shared) != TANK_OK ||
      tank_init(&c.tank, &c.shared, c.id) != TANK_OK) {
    fprintf(stderr, "Could not initialize tank.\n");
    return EXIT_FAILURE;
  }

  enum MQTTErrors *mqtt_result =
      heartbeat_init("FERMENTATION_MATURATION", machine_number);
  if (*mqtt_result != MQTT_OK) {
    printf("Could not start MQTT!\n");
  }

  struct timespec started;
  if (clock_gettime(CLOCK_MONOTONIC, &started) != 0) {
    perror("clock_gettime");
    return EXIT_FAILURE;
  }

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

    if (c.tank.state != TANK_WAIT) {
      tank_state_t previous_state = c.tank.state;
      if (tank_update(&c.tank, c.now) != TANK_OK) {
        fprintf(stderr, "Could not update fermentation/maturation.\n");
        status = EXIT_FAILURE;
        break;
      }

      if (previous_state == TANK_FERMENTATION &&
          c.tank.state == TANK_MATURATION) {
        printf("Fermentation complete; maturation started.\n");
      }

      if (c.tank.state == TANK_WAIT) {
        printf("Maturation complete; waiting.\n");
        c.shared.beer_batches = 0; /* pumped to filtering */
        send_status(&c, KAFKA_IDLE);
      }
    }

    heartbeat_update("fermentation_maturation", now.tv_sec);
    sleep(1);
  }

  heartbeat_destroy();
  kafka_consumer_close(consumer);
  kafka_producer_close(c.producer);
  return status;
}
