#pragma once

#include "mqtt.h"

typedef struct {
  int timestamp;
  int machine_idx;
} machine_heartbeat_t;

// Machine prefix is something like "BOILING" or "FERMENTATION" etc...
// It will form the machine id, like -> "%MACHINE_PREFIX%-%MACHINE_IDX%"
// Reinitialization closes an existing heartbeat connection first.
enum MQTTErrors *heartbeat_init(char *machine_prefix, int machine_idx);
enum MQTTErrors *heartbeat_update(const char *topic, uint64_t now_s);
enum MQTTErrors *heartbeat_sync(void);
/* Safe before init, after failed init, and when called more than once. */
void heartbeat_destroy(void);
enum MQTTErrors *heartbeat_status(void);
