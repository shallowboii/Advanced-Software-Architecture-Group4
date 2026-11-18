#pragma once

#include "mqtt.h"

#define MAX_MACHINE_NAME_LEN 32

typedef struct {
  int timestamp;
  char machine_name[MAX_MACHINE_NAME_LEN];
} machine_heartbeat_t;

enum MQTTErrors heartbeat_init(struct mqtt_client *client, char *machine_name);
enum MQTTErrors heartbeat_send(struct mqtt_client *client);
