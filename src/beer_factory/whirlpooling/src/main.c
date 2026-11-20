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

  enum MQTTErrors *mqtt_result = heartbeat_init("WHIRLPOOLING", machine_number);
  if (*mqtt_result != MQTT_OK) {
    printf("Could not start MQTT!\n");
  }

  int status = EXIT_SUCCESS;
  while (1) {
    struct timespec now;
    if (clock_gettime(CLOCK_MONOTONIC, &now) != 0) {
      perror("clock_gettime");
      status = EXIT_FAILURE;
      break;
    }

    heartbeat_update("whirlpooling", now.tv_sec);
    sleep(1);
  }

  heartbeat_destroy();
  return status;
}
