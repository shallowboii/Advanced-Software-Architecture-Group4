#include "heartbeat.h"
#include "lautering.h"

#include <stdio.h>
#include <stdlib.h>
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

  enum MQTTErrors *mqtt_result = heartbeat_init("LAUTERING", machine_number);
  if (*mqtt_result != MQTT_OK) {
    printf("Could not start MQTT!\n");
  }

  struct timespec started;
  if (clock_gettime(CLOCK_MONOTONIC, &started) != 0) {
    perror("clock_gettime");
    return EXIT_FAILURE;
  }

  /* Run one normal, two-hour batch; keep reporting while waiting afterward. */
  lautering_batches_t batches = {.wort_batches = 1, .sweet_wort_batches = 0};
  lautering_t lautering;
  lautering_init(&lautering, &batches);
  lautering_start(&lautering, started.tv_sec);
  printf("Lautering started\n");

  int status = EXIT_SUCCESS;
  while (1) {
    struct timespec now;
    if (clock_gettime(CLOCK_MONOTONIC, &now) != 0) {
      perror("clock_gettime");
      status = EXIT_FAILURE;
      break;
    }

    if (lautering.state == LAUTERING_LAUTER &&
        lautering_update(&lautering, now.tv_sec) == LAUTERING_DONE) {
      printf("Lautering complete; waiting.\n");
    }

    heartbeat_update("lautering", now.tv_sec);
    sleep(1);
  }

  heartbeat_destroy();
  return status;
}
