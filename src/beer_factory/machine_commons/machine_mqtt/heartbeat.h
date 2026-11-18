#pragma once

#include "mqtt.h"

typedef struct {
  int timestamp;
  int machine_idx;
} machine_heartbeat_t;

enum MQTTErrors *heartbeat_init(int machine_idx);
enum MQTTErrors *heartbeat_update(const char *topic, uint64_t now_s);
enum MQTTErrors *heartbeat_sync();
void heartbeat_destroy();
enum MQTTErrors *heartbeat_status();
