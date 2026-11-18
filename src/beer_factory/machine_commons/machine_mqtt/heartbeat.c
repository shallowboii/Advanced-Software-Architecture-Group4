#include "heartbeat.h"
#include <stdlib.h>
#include <string.h>
#include <time.h>

enum MQTTErrors heartbeat_init(struct mqtt_client *client,
                               mqtt_pal_socket_handle socketfd,
                               uint8_t *sendbuf, size_t sendbufsz,
                               uint8_t *recvbuf, size_t recvbufsz,
                               const char *client_id) {
  if (client == NULL || sendbuf == NULL || recvbuf == NULL) {
    return MQTT_ERROR_NULLPTR;
  }

  if (sendbufsz == 0 || recvbufsz == 0) {
    return MQTT_ERROR_MALFORMED_REQUEST;
  }

  /*
   *   client: MQTT client structure to initialize in place.
   *   socketfd: already connected, nonblocking socket to the broker.
   *   sendbuf: storage for outgoing packets and message queue bookkeeping.
   *   sendbufsz: size of sendbuf in bytes.
   *   recvbuf: storage for incoming MQTT packets.
   *   recvbufsz: size of recvbuf in bytes.
   *   NULL: no callback for incoming published messages; we only send
   * heartbeats.
   */
  enum MQTTErrors result =
      mqtt_init(client, socketfd, sendbuf, sendbufsz, recvbuf, recvbufsz, NULL);

  if (result != MQTT_OK) {
    return result;
  }

  /*
   *   client: the MQTT client initialized above.
   *   client_id: identifier the broker uses to distinguish this client.
   *   NULL: no Last Will topic. A Last Will is published by the broker if the
   *         client disconnects unexpectedly.
   *   NULL: no Last Will message payload.
   *   0: Last Will payload size in bytes.
   *   NULL: no username.
   *   NULL: no password.
   *   MQTT_CONNECT_CLEAN_SESSION: start a fresh session instead of resuming
   *                               saved subscriptions and pending messages.
   *   60: keep-alive interval in seconds for maintaining/detecting connection
   * loss. Call mqtt_sync regularly to transmit the request, process the
   * broker's response, and maintain the connection.
   */
  return mqtt_connect(client, client_id, NULL, NULL, 0, NULL, NULL,
                      MQTT_CONNECT_CLEAN_SESSION, 60);
}

machine_heartbeat_t heartbeat_make(const char *machine_name) {
  if (machine_name == NULL) {
    abort();
  }

  size_t length = strlen(machine_name);
  if (length >= MAX_MACHINE_NAME_LEN) {
    abort();
  }

  machine_heartbeat_t hb = {
      .timestamp = time(NULL),
  };

  memcpy(hb.machine_name, machine_name, length + 1);
  return hb;
}

enum MQTTErrors heartbeat_send(struct mqtt_client *client,
                               machine_heartbeat_t hb) {
  if (client == NULL) {
    return MQTT_ERROR_NULLPTR;
  }

  return mqtt_publish(client, "heartbeat", &hb, sizeof(hb), MQTT_PUBLISH_QOS_1);
}
