/* Test doubles for the controller's external services; no real time passes. */
#include "heartbeat.h"
#include "machine_kafka.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

static const time_t ticks[] = {0, 1, 4, 5, 3599, 3600, 3605};
static size_t tick;
static unsigned int clock_calls;
static enum MQTTErrors status;
static uint64_t next_heartbeat;

static int scenario(const char *name) {
  const char *value = getenv("CONTROLLER_SCENARIO");
  return value != NULL && strcmp(value, name) == 0;
}

struct rd_kafka_s { int unused; };
static rd_kafka_t producer, consumer;
static int machine_id;

rd_kafka_t *kafka_producer_new(const char *brokers) {
  if (strcmp(brokers, "test-broker:9092") != 0 &&
      strcmp(brokers, "localhost:9092") != 0)
    abort();
  printf("BROKERS %s\n", brokers);
  return &producer;
}

rd_kafka_t *kafka_consumer_new(const char *brokers, const char *group_id,
                             const char *topic) {
  char expected_group[48];
  snprintf(expected_group, sizeof(expected_group), "boiling-%d", machine_id);
  if ((strcmp(brokers, "test-broker:9092") != 0 &&
       strcmp(brokers, "localhost:9092") != 0) ||
      strcmp(group_id, expected_group) != 0 ||
      strcmp(topic, "asaat67.machine.boiling") != 0)
    abort();
  return &consumer;
}

int kafka_poll(rd_kafka_t *handle, kafka_message_handler handler, void *context) {
  if (handle != &consumer)
    abort();
  if (tick > 1 || scenario("no-start"))
    return 0;
  kafka_message_t msg = {0};
  msg.type = scenario("status-only") ? KAFKA_RUNNING : KAFKA_START;
  msg.machine_id = machine_id + (scenario("wrong-id") ? 1 : 0);
  strcpy(msg.batch_id, tick == 0 ? "B-1" : "B-busy");
  handler(&msg, context);
  return 1;
}

int kafka_send(rd_kafka_t *handle, const char *topic,
               const kafka_message_t *msg) {
  if (handle != &producer || strcmp(topic, "asaat67.machine.boiling") != 0 ||
      strcmp(msg->machine_type, "boiling") != 0 || msg->machine_id != machine_id)
    abort();
  printf("KAFKA %d %s\n", msg->type, msg->batch_id);
  return 0;
}

int rd_kafka_poll(rd_kafka_t *handle, int timeout_ms) {
  if (handle != &producer || timeout_ms != 0)
    abort();
  return 0;
}

void kafka_consumer_close(rd_kafka_t *handle) {
  if (handle != &consumer)
    abort();
  puts("CONSUMER_CLOSE");
}

void kafka_producer_close(rd_kafka_t *handle) {
  if (handle != &producer)
    abort();
  puts("PRODUCER_CLOSE");
}

enum MQTTErrors *heartbeat_init(char *prefix, int machine_idx) {
  if (strcmp(prefix, "BOILING") != 0)
    abort();
  printf("INIT %d\n", machine_idx);
  machine_id = machine_idx;
  status = scenario("init-error") ? MQTT_ERROR_SOCKET_ERROR : MQTT_OK;
  return &status;
}

enum MQTTErrors *heartbeat_status(void) { return &status; }

enum MQTTErrors *heartbeat_sync(void) {
  printf("SYNC %ld\n", (long)ticks[tick]);
  status = scenario("sync-error") ? MQTT_ERROR_SOCKET_ERROR : MQTT_OK;
  return &status;
}

enum MQTTErrors *heartbeat_update(const char *topic, uint64_t now_s) {
  if (status != MQTT_OK)
    return &status;
  if (now_s >= next_heartbeat) {
    printf("SEND %ld %s\n", (long)ticks[tick], topic);
    status = scenario("send-error") ? MQTT_ERROR_SOCKET_ERROR : MQTT_OK;
    next_heartbeat = now_s + 5;
  }
  if (status == MQTT_OK)
    heartbeat_sync();
  if (status != MQTT_OK)
    fprintf(stderr, "Error while handling heartbeats: simulated socket error.\n");
  return &status;
}

void heartbeat_destroy(void) { puts("DESTROY"); }

const char *mqtt_error_str(enum MQTTErrors error) {
  (void)error;
  return "simulated socket error";
}

int test_clock_gettime(clockid_t clock_id, struct timespec *value) {
  if (clock_id != CLOCK_MONOTONIC)
    abort();
  ++clock_calls;
  if (scenario("clock-error") && clock_calls == 4) {
    errno = EIO;
    return -1;
  }
  value->tv_sec = 100000 + ticks[tick];
  value->tv_nsec = 0;
  return 0;
}

int test_nanosleep(const struct timespec *request, struct timespec *remaining) {
  (void)request;
  (void)remaining;
  if (++tick == sizeof(ticks) / sizeof(ticks[0]))
    exit(EXIT_SUCCESS); /* End the otherwise infinite controller loop. */
  return 0;
}
