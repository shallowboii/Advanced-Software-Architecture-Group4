#include "heartbeat.h"
#include "mashing.h"

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
  if (machine_number < 0) {
    printf("Machine number must be >= 0\n");
    return EXIT_FAILURE;
  }

  enum MQTTErrors *mqtt_result = heartbeat_init("MASHING", machine_number);
  if (*mqtt_result != MQTT_OK) {
    printf("Could not start MQTT!\n");
  }

  struct timespec started;
  if (clock_gettime(CLOCK_MONOTONIC, &started) != 0) {
    perror("clock_gettime");
    return EXIT_FAILURE;
  }

  /* Run one two-hour demo batch, then keep reporting while waiting. */
  unsigned int wort_batches = 0;
  mashing_t mashing;
  mashing_init(&mashing, &wort_batches);
  mashing_start(&mashing, (uint64_t)started.tv_sec);
  printf("Mashing started\n");

  int status = EXIT_SUCCESS;
  while (1) {
    struct timespec now;
    if (clock_gettime(CLOCK_MONOTONIC, &now) != 0) {
      perror("clock_gettime");
      status = EXIT_FAILURE;
      break;
    }

    uint64_t now_seconds = (uint64_t)now.tv_sec;
    if (mashing.state == MASHING_MASH &&
        mashing_update(&mashing, now_seconds) == MASHING_DONE) {
      puts("done_mashing");
      fflush(stdout);
    }

    heartbeat_update("mashing", now.tv_sec);
    sleep(1);
  }

  heartbeat_destroy();
  return status;
}
