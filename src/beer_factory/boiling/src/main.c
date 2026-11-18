#include "boiling.h"
#include "heartbeat.h"

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

int main(int argc, char **argv) {
  if (*name == '\0' || strlen(name) >= MAX_MACHINE_NAME_LEN) {
    fprintf(stderr, "Machine name must contain 1 to 31 bytes.\n");
    return EXIT_FAILURE;
  }

  struct mqtt_client client;
  enum MQTTErrors mqtt_result = heartbeat_init(
      &client, fd, sendbuf, sizeof(sendbuf), recvbuf, sizeof(recvbuf), name);

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
