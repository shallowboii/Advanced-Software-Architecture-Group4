#include "heartbeat.h"
#include "mqtt.h"
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define SENDBUFSZ 2048
#define RECVBUFSZ 2048

#define HEARTBEAT_INTERVAL_S 5

#define MQTT_HOST "localhost"
#define MQTT_PORT "1883"
#define MQTT_NAME "boiling-1"

int mqtt_fd;
int mqtt_machine_idx;
struct mqtt_client client;
uint8_t sendbuf[SENDBUFSZ], recvbuf[RECVBUFSZ];

uint64_t last_heartbeat_s = 0;
enum MQTTErrors mqtt_result = MQTT_OK;

/* Finish nonblocking TCP connection attempts within five seconds per address.
 */
static int connect_broker(const char *host, const char *port) {
  struct addrinfo hints = {0}, *addresses;
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  int error = getaddrinfo(host, port, &hints, &addresses);
  if (error != 0) {
    fprintf(stderr, "Broker address: %s\n", gai_strerror(error));
    return -1;
  }

  int fd = -1;
  for (struct addrinfo *address = addresses; address != NULL;
       address = address->ai_next) {
    fd = socket(address->ai_family, address->ai_socktype, address->ai_protocol);
    if (fd < 0)
      continue;
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags >= 0 && fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0) {
      int connected = connect(fd, address->ai_addr, address->ai_addrlen);
      if (connected == 0)
        break;
      if (errno == EINPROGRESS) {
        struct pollfd event = {.fd = fd, .events = POLLOUT};
        int ready = poll(&event, 1, 5000);
        int socket_error = 0;
        socklen_t length = sizeof(socket_error);
        if (ready > 0 &&
            getsockopt(fd, SOL_SOCKET, SO_ERROR, &socket_error, &length) == 0 &&
            socket_error == 0)
          break;
      }
    }
    close(fd);
    fd = -1;
  }
  freeaddrinfo(addresses);
  return fd;
}

enum MQTTErrors *heartbeat_init(char *machine_prefix, int machine_idx) {
  mqtt_machine_idx = machine_idx;

  int fd = connect_broker(MQTT_HOST, MQTT_PORT);
  if (fd < 0) {
    fprintf(stderr, "Could not connect to MQTT broker %s:%s.\n", MQTT_HOST,
            MQTT_PORT);
    mqtt_result = MQTT_ERROR_SOCKET_ERROR;
    return &mqtt_result;
  }

  /*
   *   client: MQTT client structure to initialize in place.
   *   socketfd: already connected, nonblocking socket to the broker.
   *   NULL: no callback for incoming published messages; we only send
   * heartbeats.
   */
  mqtt_result =
      mqtt_init(&client, fd, sendbuf, SENDBUFSZ, recvbuf, RECVBUFSZ, NULL);

  if (mqtt_result != MQTT_OK) {
    close(fd);
    return &mqtt_result;
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
  char machine_name[50];
  snprintf(machine_name, sizeof(machine_name), "%s-%i", machine_prefix,
           machine_idx);
  mqtt_result = mqtt_connect(&client, machine_name, NULL, NULL, 0, NULL, NULL,
                             MQTT_CONNECT_CLEAN_SESSION, 60);

  if (mqtt_result != MQTT_OK) {
    fprintf(stderr, "MQTT initialization: %s\n", mqtt_error_str(mqtt_result));
    pthread_mutex_destroy(&client.mutex);
    close(fd);
  }

  mqtt_fd = fd;
  return &mqtt_result;
}

enum MQTTErrors *heartbeat_send(const char *topic) {
  machine_heartbeat_t hb = {
      .timestamp = time(NULL),
      .machine_idx = mqtt_machine_idx,
  };

  mqtt_result =
      mqtt_publish(&client, topic, &hb, sizeof(hb), MQTT_PUBLISH_QOS_1);
  return &mqtt_result;
}

void heartbeat_destroy(void) {
  if (mqtt_disconnect(&client) == MQTT_OK) {
    (void)mqtt_sync(&client);
  }

  pthread_mutex_destroy(&client.mutex);
  close(mqtt_fd);
}

enum MQTTErrors *heartbeat_sync(void) {
  mqtt_result = mqtt_sync(&client);
  return &mqtt_result;
}

enum MQTTErrors *heartbeat_update(const char *topic, uint64_t now_s) {
  uint64_t elapsed_s = (uint64_t)(now_s - last_heartbeat_s);

  if (mqtt_result == MQTT_OK) {
    if (elapsed_s >= HEARTBEAT_INTERVAL_S) {
      heartbeat_send(topic);
      last_heartbeat_s = now_s;
    }

    if (mqtt_result == MQTT_OK) {
      heartbeat_sync();
    }

    if (mqtt_result != MQTT_OK) {
      fprintf(stderr, "Error while handling heartbeats: %s.\n",
              mqtt_error_str(mqtt_result));
    }
  }

  return &mqtt_result;
}

enum MQTTErrors *heartbeat_status(void) { return &mqtt_result; }
