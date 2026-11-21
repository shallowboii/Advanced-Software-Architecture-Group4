/*
 * Whirlpooling controller: one thread, one loop that
 *   - takes commands from Kafka (machine_kafka, topic "machine.whirlpooling")
 *     and answers with statuses on the same topic,
 *   - drives the whirlpool state machine (whirlpooling.c),
 *   - sends an MQTT heartbeat (machine_mqtt; broker fixed in heartbeat.c). If
 *     MQTT can't start, it prints a warning and keeps running.
 *
 * Usage: whirlpooling [machine-name]      (default whirlpooling-1)
 *
 * Environment:
 *   KAFKA_BROKERS  Kafka bootstrap servers, e.g. kafka:29092 (required).
 *   MACHINE_ID     machine id; default: the number after the last '-'
 *                  of machine-name (whirlpooling-2 -> 2), otherwise 1.
 *   TIME_SCALE     simulated seconds per real second (default 1; 600 makes a
 *                  one-hour run take 6 s).
 */
#define _POSIX_C_SOURCE 200809L

#include "heartbeat.h"
#include "machine_kafka.h"
#include "whirlpooling.h"

#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define MACHINE_TYPE "whirlpooling"
#define HEARTBEAT_PREFIX "WHIRLPOOLING" /* MQTT client id: WHIRLPOOLING-<id> */
#define TOPIC "machine." MACHINE_TYPE
#define GROUP_ID_LEN 48 /* "whirlpooling-<id>", this machine's consumer group */

static volatile sig_atomic_t stopping;

static void stop_controller(int signum) {
  (void)signum;
  stopping = 1;
}

/* ---- Kafka side --------------------------------------------------------- */

typedef struct {
  int id;
  rd_kafka_t *producer;
  rd_kafka_t *consumer;
  whirlpooling_t whirlpool;
  whirlpooling_batches_t batches;
  char batch_id[KAFKA_BATCH_LEN]; /* the batch in the vessel */
  uint64_t now;                   /* simulated seconds since startup */
} controller_t;

/* Sends a status about the batch in the vessel. */
static void send_status(controller_t *c, kafka_type_t type) {
  kafka_message_t status;
  memset(&status, 0, sizeof(status));
  status.type = type;
  snprintf(status.machine_type, sizeof(status.machine_type), "%s",
           MACHINE_TYPE);
  status.machine_id = c->id;
  memcpy(status.batch_id, c->batch_id, sizeof(status.batch_id));
  // TODO(marton): message_id and ts_utc
  kafka_send(c->producer, TOPIC, &status);
}

static void on_message(const kafka_message_t *msg, void *context) {
  controller_t *c = context;
  if (msg->machine_id != c->id)
    return; /* for another whirlpool */

  switch (msg->type) {
  case KAFKA_START:
    // TODO(marton): what to answer when the whirlpool is busy
    // TODO(marton): start params (duration_s); the default run time is used
    c->batches.hot_wort_batches = 1; /* the kettle pumped the hot wort over */
    whirlpooling_start(&c->whirlpool, c->now);
    memcpy(c->batch_id, msg->batch_id, sizeof(c->batch_id));
    printf("Whirlpooling %s.\n", c->batch_id);
    send_status(c, KAFKA_RUNNING);
    break;
  case KAFKA_STOP:
  case KAFKA_FLUSH:
    // TODO(marton): handle KAFKA_STOP / KAFKA_FLUSH (or answer UNSUPPORTED)
    break;
  default:
    break; /* a status (also our own): only the scheduler handles those */
  }
}

static void finish_batch(controller_t *c) {
  printf("Whirlpooled %s.\n", c->batch_id);
  c->batches.clarified_wort_batches = 0; /* pumped to a tank */
  // TODO(marton): no KAFKA_DONE; should this KAFKA_IDLE carry batch_id?
  send_status(c, KAFKA_IDLE);
}

/* ---- main ----------------------------------------------------------------- */

static int machine_id_from(const char *name) {
  const char *env = getenv("MACHINE_ID");
  const char *digits = env != NULL ? env : strrchr(name, '-');
  if (digits == NULL)
    return 1;
  if (*digits == '-')
    ++digits;
  char *end;
  long id = strtol(digits, &end, 10);
  if (end == digits || *end != '\0' || id < 1 || id > 1000000)
    return env != NULL ? -1 : 1;
  return (int)id;
}

static uint64_t elapsed_ms_since(const struct timespec *started) {
  struct timespec now;
  clock_gettime(CLOCK_MONOTONIC, &now);
  return (uint64_t)(now.tv_sec - started->tv_sec) * 1000 +
         (uint64_t)(now.tv_nsec / 1000000) -
         (uint64_t)(started->tv_nsec / 1000000);
}

int main(int argc, char **argv) {
  if (argc > 2) {
    fprintf(stderr, "Usage: %s [machine-name]\n", argv[0]);
    return EXIT_FAILURE;
  }
  setvbuf(stdout, NULL, _IOLBF, 0);
  const char *name = argc > 1 ? argv[1] : "whirlpooling-1";
  const char *brokers = getenv("KAFKA_BROKERS");
  if (brokers == NULL || brokers[0] == '\0') {
    fprintf(stderr, "KAFKA_BROKERS is not set (e.g. kafka:29092).\n");
    return EXIT_FAILURE;
  }

  controller_t c;
  memset(&c, 0, sizeof(c));
  c.id = machine_id_from(name);
  const char *scale_env = getenv("TIME_SCALE");
  double time_scale = scale_env != NULL ? strtod(scale_env, NULL) : 1.0;
  if (c.id < 1 || !(time_scale > 0.0)) {
    fprintf(stderr, "MACHINE_ID must be >= 1 and TIME_SCALE > 0.\n");
    return EXIT_FAILURE;
  }
  whirlpooling_init(&c.whirlpool, &c.batches);

  struct sigaction action = {0};
  action.sa_handler = stop_controller;
  sigemptyset(&action.sa_mask);
  if (sigaction(SIGINT, &action, NULL) != 0 ||
      sigaction(SIGTERM, &action, NULL) != 0) {
    perror("sigaction");
    return EXIT_FAILURE;
  }
  /* A lost MQTT broker must be reported as an error, not end the process. */
  action.sa_handler = SIG_IGN;
  if (sigaction(SIGPIPE, &action, NULL) != 0) {
    perror("sigaction");
    return EXIT_FAILURE;
  }

  /* Own consumer group per machine, so every machine reads the whole topic
   * and keeps only the commands for its own id. */
  char group_id[GROUP_ID_LEN];
  snprintf(group_id, sizeof(group_id), "%s-%d", MACHINE_TYPE, c.id);
  c.producer = kafka_producer_new(brokers);
  c.consumer = kafka_consumer_new(brokers, group_id, TOPIC);
  // TODO: error handling
  // TODO(marton): send KAFKA_IDLE on startup? (once partitions are assigned)
  printf("Whirlpooling %d: Kafka %s, topic %s, time x%g.\n", c.id, brokers,
         TOPIC, time_scale);

  int mqtt_on = *heartbeat_init(HEARTBEAT_PREFIX, c.id) == MQTT_OK;
  if (!mqtt_on)
    printf("Could not start MQTT; running without heartbeat.\n");

  struct timespec started;
  clock_gettime(CLOCK_MONOTONIC, &started);
  while (!stopping) {
    uint64_t elapsed_ms = elapsed_ms_since(&started);
    c.now = (uint64_t)((double)elapsed_ms * time_scale / 1000.0);

    /* Waits up to KAFKA_POLL_TIMEOUT_MS for a command; this is the loop's
     * sleep. */
    kafka_poll(c.consumer, on_message, &c);
    rd_kafka_poll(c.producer, 0); /* serve delivery reports */

    if (whirlpooling_update(&c.whirlpool, c.now) == WHIRLPOOLING_DONE)
      finish_batch(&c);

    /* Real seconds, not simulated: does nothing if MQTT didn't start. */
    heartbeat_update(MACHINE_TYPE, elapsed_ms / 1000);
  }

  /* Only after a successful init: heartbeat_destroy closes the socket. */
  if (mqtt_on)
    heartbeat_destroy();
  kafka_consumer_close(c.consumer);
  kafka_producer_close(c.producer);
  return EXIT_SUCCESS;
}
