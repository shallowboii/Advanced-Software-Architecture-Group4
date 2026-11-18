#include "heartbeat.h"
#include "mqtt.h"
#include "tank.h"

#include <errno.h>
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

  tank_shared_t shared;
  tank_t tank;
  if (tank_shared_init(&shared) != TANK_OK ||
      tank_init(&tank, &shared, machine_number) != TANK_OK) {
    fprintf(stderr, "Could not initialize tank.\n");
    return EXIT_FAILURE;
  }

  enum MQTTErrors *mqtt_result = heartbeat_init(machine_number);
  if (*mqtt_result != MQTT_OK) {
    printf("Could not start MQTT!\n");
  }

  struct timespec started;
  if (clock_gettime(CLOCK_MONOTONIC, &started) != 0) {
    perror("clock_gettime");
    return EXIT_FAILURE;
  }

  shared.chosen_tank = machine_number;
  shared.clarified_wort_batches = 100;
  if (tank_start(&tank, started.tv_sec) != TANK_STARTED) {
    fprintf(stderr, "Could not start fermentation.\n");
    heartbeat_destroy();
    return EXIT_FAILURE;
  }

  printf("Fermentation started\n");

  int status = EXIT_SUCCESS;
  while (1) {
    struct timespec now;
    if (clock_gettime(CLOCK_MONOTONIC, &now) != 0) {
      perror("clock_gettime");
      status = EXIT_FAILURE;
      break;
    }

    if (tank.state != TANK_WAIT) {
      tank_state_t previous_state = tank.state;
      if (tank_update(&tank, now.tv_sec) != TANK_OK) {
        fprintf(stderr, "Could not update fermentation/maturation.\n");
        status = EXIT_FAILURE;
        break;
      }

      if (previous_state == TANK_FERMENTATION &&
          tank.state == TANK_MATURATION) {
        printf("Fermentation complete; maturation started.\n");
      }

      if (tank.state == TANK_WAIT) {
        printf("Maturation complete; waiting.\n");
      }
    }

    heartbeat_update("fermentation_maturation", now.tv_sec);
    sleep(1);
  }

  heartbeat_destroy();
  return status;
}
