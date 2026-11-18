#pragma once

#include "mqtt.h"

typedef struct {
  int timestamp;
  int machine_idx;
} machine_heartbeat_t;

enum MQTTErrors heartbeat_init(int machine_idx);
enum MQTTErrors heartbeat_send(const char *topic);
enum MQTTErrors heartbeat_sync();
void heartbeat_destroy();
