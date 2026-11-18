#pragma once

#include "mqtt.h"

#define MAX_MACHINE_NAME_LEN 32

typedef struct {
  int timestamp;
  char machine_name[MAX_MACHINE_NAME_LEN];
} machine_heartbeat_t;

enum MQTTErrors heartbeat_init(struct mqtt_client *client,
                               mqtt_pal_socket_handle socketfd,
                               uint8_t *sendbuf, size_t sendbufsz,
                               uint8_t *recvbuf, size_t recvbufsz,
                               const char *client_id);

machine_heartbeat_t heartbeat_make(const char *machine_name);
enum MQTTErrors heartbeat_send(struct mqtt_client *client,
                               machine_heartbeat_t hb);
