#include "boiling.h"
#include "heartbeat.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <netdb.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

int main(int argc, char **argv) {
  if (argc != 2) {
    fprintf(stderr, "Usage: %s <machine-number>\n", argv[0]);
    return EXIT_FAILURE;
  }

  long machine_number = atoi(argv[1]);
  enum MQTTErrors mqtt_result = heartbeat_init(machine_number);

  struct timespec started;
  if (clock_gettime(CLOCK_MONOTONIC, &started) != 0) {
    perror("clock_gettime");
    return EXIT_FAILURE;
  }

  /* Run one normal, one-hour batch; keep reporting while waiting afterward. */
  boiling_batches_t batches = {.sweet_wort_batches = 1, .hot_wort_batches = 0};
  boiling_t boiling;
  boiling_init(&boiling, &batches);
  (void)boiling_start(&boiling, 0);
  printf("Boiling started\n");

  uint64_t next_heartbeat_s = 0;
  int status = EXIT_SUCCESS;
  while (1) {
    struct timespec now;
    if (clock_gettime(CLOCK_MONOTONIC, &now) != 0) {
      perror("clock_gettime");
      status = EXIT_FAILURE;
      break;
    }

    uint64_t elapsed_s = (uint64_t)(now.tv_sec - started.tv_sec);
    if (boiling.state == BOILING_BOIL &&
        boiling_update(&boiling, elapsed_s) == BOILING_DONE) {
      printf("Boiling complete; waiting.");
    }

    // Are we connected?
    if (mqtt_result == MQTT_OK) {
      if (elapsed_s >= next_heartbeat_s) {
        mqtt_result = heartbeat_send("boiling");
        next_heartbeat_s = elapsed_s + 5;
      }

      if (mqtt_result == MQTT_OK) {
        mqtt_result = heartbeat_sync();
      }

      if (mqtt_result != MQTT_OK) {
        fprintf(stderr, "Error while handling heartbeats: %s.\n",
                mqtt_error_str(mqtt_result));
      }
    }

    sleep(1);
  }

  heartbeat_destroy();
  return status;
}
