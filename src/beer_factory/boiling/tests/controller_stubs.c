/* Test doubles for the controller's external services; no real time passes. */
#include "heartbeat.h"

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

enum MQTTErrors *heartbeat_init(char *prefix, int machine_idx) {
  if (strcmp(prefix, "BOILING") != 0)
    abort();
  printf("INIT %d\n", machine_idx);
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

unsigned int test_sleep(unsigned int seconds) {
  if (seconds != 1)
    abort();
  if (++tick == sizeof(ticks) / sizeof(ticks[0]))
    exit(EXIT_SUCCESS); /* End the otherwise infinite controller loop. */
  return 0;
}
