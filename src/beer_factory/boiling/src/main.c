#include "boiling.h"
#include "heartbeat.h"

#include "args.h"
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
  long machine_number;
  if (!get_machine_number(argc, argv, &machine_number)) {
    return EXIT_FAILURE;
  }

  enum MQTTErrors *mqtt_result = heartbeat_init("BOILING", machine_number);
  if (*mqtt_result != MQTT_OK) {
    printf("Could not start MQTT!\n");
  }

  struct timespec started;
  if (clock_gettime(CLOCK_MONOTONIC, &started) != 0) {
    perror("clock_gettime");
    return EXIT_FAILURE;
  }

  /* Run one normal, one-hour batch; keep reporting while waiting afterward. */
  boiling_batches_t batches = {.sweet_wort_batches = 1, .hot_wort_batches = 0};
  boiling_t boiling;
  boiling_init(&boiling, &batches);
  boiling_start(&boiling, started.tv_sec);
  printf("Boiling started\n");

  int status = EXIT_SUCCESS;
  while (1) {
    struct timespec now;
    if (clock_gettime(CLOCK_MONOTONIC, &now) != 0) {
      perror("clock_gettime");
      status = EXIT_FAILURE;
      break;
    }

    if (boiling.state == BOILING_BOIL &&
        boiling_update(&boiling, now.tv_sec) == BOILING_DONE) {
      printf("Boiling complete; waiting.");
    }

    heartbeat_update("boiling", now.tv_sec);
    sleep(1);
  }

  heartbeat_destroy();
  return status;
}
