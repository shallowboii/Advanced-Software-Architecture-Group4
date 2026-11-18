#include "boiling.h"
#include "../../machine_mqtt/heartbeat.h"

#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <poll.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

static volatile sig_atomic_t stopping;

static void stop_controller(int signum) {
  (void)signum;
  stopping = 1;
}

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
  for (struct addrinfo *address = addresses; address != NULL && !stopping;
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

int main(int argc, char **argv) {
  if (argc > 4) {
    fprintf(stderr, "Usage: %s [broker-host [broker-port [machine-name]]]\n",
            argv[0]);
    return EXIT_FAILURE;
  }
  const char *host = argc > 1 ? argv[1] : "localhost";
  const char *port = argc > 2 ? argv[2] : "1883";
  const char *name = argc > 3 ? argv[3] : "boiling-1";
  if (*name == '\0' || strlen(name) >= MAX_MACHINE_NAME_LEN) {
    fprintf(stderr, "Machine name must contain 1 to 31 bytes.\n");
    return EXIT_FAILURE;
  }

  struct sigaction action = {0};
  action.sa_handler = stop_controller;
  sigemptyset(&action.sa_mask);
  if (sigaction(SIGINT, &action, NULL) != 0 ||
      sigaction(SIGTERM, &action, NULL) != 0) {
    perror("sigaction");
    return EXIT_FAILURE;
  }
  /* A lost broker must be reported as an MQTT error, not terminate boiling. */
  action.sa_handler = SIG_IGN;
  if (sigaction(SIGPIPE, &action, NULL) != 0) {
    perror("sigaction");
    return EXIT_FAILURE;
  }

  int fd = connect_broker(host, port);
  if (fd < 0) {
    fprintf(stderr, "Could not connect to MQTT broker %s:%s.\n", host, port);
    return EXIT_FAILURE;
  }

  struct mqtt_client client;
  uint8_t sendbuf[2048], recvbuf[2048];
  enum MQTTErrors mqtt_result = heartbeat_init(
      &client, fd, sendbuf, sizeof(sendbuf), recvbuf, sizeof(recvbuf), name);
  /* Valid buffers guarantee mqtt_init reached mutex initialization. */
  if (mqtt_result != MQTT_OK) {
    fprintf(stderr, "MQTT initialization: %s\n", mqtt_error_str(mqtt_result));
    pthread_mutex_destroy(&client.mutex);
    close(fd);
    return EXIT_FAILURE;
  }

  struct timespec started;
  if (clock_gettime(CLOCK_MONOTONIC, &started) != 0) {
    perror("clock_gettime");
    pthread_mutex_destroy(&client.mutex);
    close(fd);
    return EXIT_FAILURE;
  }

  /* Run one normal, one-hour batch; keep reporting while waiting afterward. */
  boiling_batches_t batches = {.sweet_wort_batches = 1, .hot_wort_batches = 0};
  boiling_t boiling;
  boiling_init(&boiling, &batches);
  (void)boiling_start(&boiling, 0);
  puts("Boiling started.");
  fflush(stdout);

  uint64_t next_heartbeat = 0;
  int status = EXIT_SUCCESS;
  while (!stopping) {
    struct timespec now;
    if (clock_gettime(CLOCK_MONOTONIC, &now) != 0) {
      perror("clock_gettime");
      status = EXIT_FAILURE;
      break;
    }
    uint64_t elapsed_ms = (uint64_t)(now.tv_sec - started.tv_sec) * 1000;
    if (now.tv_nsec >= started.tv_nsec) {
      elapsed_ms += (uint64_t)(now.tv_nsec - started.tv_nsec) / 1000000;
    } else {
      elapsed_ms -= 1000;
      elapsed_ms +=
          (uint64_t)(1000000000L + now.tv_nsec - started.tv_nsec) / 1000000;
    }

    if (boiling.state == BOILING_BOIL &&
        boiling_update(&boiling, elapsed_ms / 1000) == BOILING_DONE) {
      puts("Boiling complete; waiting. Heartbeats remain active.");
      fflush(stdout);
    }

    if (mqtt_result == MQTT_OK) {
      if (elapsed_ms >= next_heartbeat) {
        mqtt_result = heartbeat_send(&client, heartbeat_make(name));
        next_heartbeat = elapsed_ms + 2000;
      }
      if (mqtt_result == MQTT_OK)
        mqtt_result = mqtt_sync(&client);
      if (mqtt_result != MQTT_OK) {
        fprintf(stderr,
                "Heartbeat publishing stopped: %s; boiling continues.\n",
                mqtt_error_str(mqtt_result));
      }
    }
    const struct timespec pause = {.tv_sec = 0, .tv_nsec = 100000000};
    (void)nanosleep(&pause, NULL);
  }

  if (mqtt_result == MQTT_OK && mqtt_disconnect(&client) == MQTT_OK) {
    (void)mqtt_sync(&client);
  }
  pthread_mutex_destroy(&client.mutex);
  close(fd);
  return status;
}
