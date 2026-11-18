#pragma once

#include "mqtt.h"

#define MAX_MACHINE_NAME_LEN 32

typedef struct {
  int timestamp;
  char machine_name[MAX_MACHINE_NAME_LEN];
} machine_heartbeat_t;

enum MQTTErrors heartbeat_init(char *machine_name);
enum MQTTErrors heartbeat_send(const char *topic);
enum MQTTErrors heartbeat_sync();
void heartbeat_destroy();
