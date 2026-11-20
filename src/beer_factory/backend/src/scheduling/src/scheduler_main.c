/*
 * Scheduler service: follows every machine over Kafka and hands each status
 * to dispatch() (dispatch.c).
 *
 * Environment: KAFKA_BROKERS (required), e.g. kafka:29092.
 */
#define _POSIX_C_SOURCE 200809L

#include "scheduler.h"

#include <signal.h>
#include <stdio.h>
#include <stdlib.h>

static volatile sig_atomic_t stopping;

static void stop_scheduler(int signum) {
  (void)signum;
  stopping = 1;
}

int main(void) {
  setvbuf(stdout, NULL, _IOLBF, 0);
  const char *brokers = getenv("KAFKA_BROKERS");
  if (brokers == NULL || brokers[0] == '\0') {
    fprintf(stderr, "KAFKA_BROKERS is not set (e.g. kafka:29092).\n");
    return EXIT_FAILURE;
  }

  struct sigaction action = {0};
  action.sa_handler = stop_scheduler;
  sigemptyset(&action.sa_mask);
  if (sigaction(SIGINT, &action, NULL) != 0 ||
      sigaction(SIGTERM, &action, NULL) != 0) {
    perror("sigaction");
    return EXIT_FAILURE;
  }

  scheduler_t *s = scheduler_open(brokers);
  if (s == NULL)
    return EXIT_FAILURE;
  printf("Scheduler: Kafka %s, following every machine type.\n", brokers);

  int status = EXIT_SUCCESS;
  while (!stopping) {
    if (scheduler_poll(s, 200) == MK_ERR_FATAL) {
      fprintf(stderr, "kafka: fatal error, exiting\n");
      status = EXIT_FAILURE;
      break;
    }
  }
  scheduler_close(s);
  return status;
}
