/*
 * Whirlpooling controller: one thread, one loop that
 *   - takes commands from Kafka (machine_kafka, topic "machine.whirlpooling")
 *     and answers with statuses on the same topic,
 *   - drives the whirlpool state machine (whirlpooling.c),
 *   - publishes an MQTT heartbeat every 2 s (machine_mqtt) if MQTT_HOST is set.
 *
 * Usage: whirlpooling [machine-name]      (default whirlpooling-1)
 *
 * Environment:
 *   KAFKA_BROKERS  Kafka bootstrap servers, e.g. kafka:29092 (required).
 *   MACHINE_ID     Kafka machine_id; default: the number after the last '-'
 *                  of machine-name (whirlpooling-2 -> 2), otherwise 1.
 *   MQTT_HOST      MQTT broker for the heartbeat; unset = no heartbeat.
 *   MQTT_PORT      default 1883.
 *   TIME_SCALE     simulated seconds per real second (default 1; 600 makes a
 *                  one-hour run take 6 s).
 */
#define _POSIX_C_SOURCE 200809L

#include "heartbeat.h"
#include "machine_kafka.h"
#include "whirlpooling.h"

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

#define MACHINE_TYPE "whirlpooling"
#define TOPIC "machine." MACHINE_TYPE
#define GROUP_ID_LEN 48 /* "whirlpooling-<id>", this machine's consumer group */
#define HEARTBEAT_INTERVAL_MS 2000

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
  char batch_id[MK_BATCH_LEN]; /* the batch in the vessel */
  uint64_t now;                /* simulated seconds since startup */
} controller_t;

/* Sends a status about the batch in the vessel. */
static void send_status(controller_t *c, mk_type_t type) {
  mk_message_t status;
  memset(&status, 0, sizeof(status));
  status.type = type;
  snprintf(status.machine_type, sizeof(status.machine_type), "%s",
           MACHINE_TYPE);
  status.machine_id = c->id;
  memcpy(status.batch_id, c->batch_id, sizeof(status.batch_id));
  // TODO(marton): message_id and ts_utc
  mk_send(c->producer, TOPIC, &status);
}

static void on_message(const mk_message_t *msg, void *opaque) {
  controller_t *c = opaque;
  if (msg->machine_id != c->id)
    return; /* for another whirlpool */

  switch (msg->type) {
  case MK_START:
    // TODO(marton): what to answer when the whirlpool is busy
    // TODO(marton): start params (duration_s); the default run time is used
    c->batches.hot_wort_batches = 1; /* the kettle pumped the hot wort over */
    whirlpooling_start(&c->whirlpool, c->now);
    memcpy(c->batch_id, msg->batch_id, sizeof(c->batch_id));
    printf("Whirlpooling %s.\n", c->batch_id);
    send_status(c, MK_RUNNING);
    break;
  case MK_STOP:
  case MK_FLUSH:
    // TODO(marton): handle MK_STOP / MK_FLUSH (or answer UNSUPPORTED)
    break;
  default:
    break; /* a status (also our own): only the scheduler handles those */
  }
}

static void finish_batch(controller_t *c) {
  printf("Whirlpooled %s.\n", c->batch_id);
  c->batches.clarified_wort_batches = 0; /* pumped to a tank */
  // TODO(marton): there is no MK_DONE; should this MK_IDLE carry batch_id?
  send_status(c, MK_IDLE);
}

/* ---- MQTT heartbeat (optional) ------------------------------------------- */

typedef struct {
  int on; /* MQTT_HOST was set and the connection works */
  int fd;
  struct mqtt_client client;
  uint8_t sendbuf[2048], recvbuf[2048];
  const char *name;
  uint64_t next_ms;
} heartbeat_link_t;

/* Nonblocking TCP connect to the MQTT broker, five seconds per address. */
static int connect_broker(const char *host, const char *port) {
  struct addrinfo hints = {0}, *addresses;
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  int error = getaddrinfo(host, port, &hints, &addresses);
  if (error != 0) {
    fprintf(stderr, "MQTT broker address: %s\n", gai_strerror(error));
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

/* Returns 0 when the heartbeat is off (no MQTT_HOST) or running, -1 if
 * MQTT_HOST is set but the broker can't be used. */
static int heartbeat_open(heartbeat_link_t *hb, const char *name) {
  memset(hb, 0, sizeof(*hb));
  hb->fd = -1;
  hb->name = name;
  const char *host = getenv("MQTT_HOST");
  if (host == NULL || host[0] == '\0') {
    printf("MQTT_HOST not set: no heartbeat.\n");
    return 0;
  }
  const char *port = getenv("MQTT_PORT");
  if (port == NULL || port[0] == '\0')
    port = "1883";
  hb->fd = connect_broker(host, port);
  if (hb->fd < 0) {
    fprintf(stderr, "Could not connect to MQTT broker %s:%s.\n", host, port);
    return -1;
  }
  enum MQTTErrors result =
      heartbeat_init(&hb->client, hb->fd, hb->sendbuf, sizeof(hb->sendbuf),
                     hb->recvbuf, sizeof(hb->recvbuf), name);
  if (result != MQTT_OK) {
    /* Valid buffers guarantee mqtt_init reached mutex initialization. */
    fprintf(stderr, "MQTT initialization: %s\n", mqtt_error_str(result));
    pthread_mutex_destroy(&hb->client.mutex);
    close(hb->fd);
    hb->fd = -1;
    return -1;
  }
  hb->on = 1;
  printf("Heartbeat to %s:%s every %d ms.\n", host, port,
         HEARTBEAT_INTERVAL_MS);
  return 0;
}

/* Publishes when due and services the connection. A broken connection turns
 * the heartbeat off; the controller keeps working. */
static void heartbeat_tick(heartbeat_link_t *hb, uint64_t elapsed_ms) {
  if (!hb->on)
    return;
  enum MQTTErrors result = MQTT_OK;
  if (elapsed_ms >= hb->next_ms) {
    result = heartbeat_send(&hb->client, heartbeat_make(hb->name));
    hb->next_ms = elapsed_ms + HEARTBEAT_INTERVAL_MS;
  }
  if (result == MQTT_OK)
    result = mqtt_sync(&hb->client);
  if (result != MQTT_OK) {
    fprintf(stderr, "Heartbeat stopped: %s; whirlpooling continues.\n",
            mqtt_error_str(result));
    hb->on = 0;
  }
}

static void heartbeat_close(heartbeat_link_t *hb) {
  if (hb->fd < 0)
    return;
  if (hb->on && mqtt_disconnect(&hb->client) == MQTT_OK)
    (void)mqtt_sync(&hb->client);
  pthread_mutex_destroy(&hb->client.mutex);
  close(hb->fd);
  hb->fd = -1;
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
  if (*name == '\0' || strlen(name) >= MAX_MACHINE_NAME_LEN) {
    fprintf(stderr, "Machine name must contain 1 to 31 bytes.\n");
    return EXIT_FAILURE;
  }
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
  c.producer = mk_producer_new(brokers);
  c.consumer = mk_consumer_new(brokers, group_id, TOPIC);
  // TODO: error handling
  // TODO(marton): send MK_IDLE on startup? (only once partitions are assigned)
  printf("Whirlpooling %d: Kafka %s, topic %s, time x%g.\n", c.id, brokers,
         TOPIC, time_scale);

  heartbeat_link_t hb;
  if (heartbeat_open(&hb, name) != 0) {
    mk_consumer_close(c.consumer);
    mk_producer_close(c.producer);
    return EXIT_FAILURE;
  }

  struct timespec started;
  clock_gettime(CLOCK_MONOTONIC, &started);
  while (!stopping) {
    uint64_t elapsed_ms = elapsed_ms_since(&started);
    c.now = (uint64_t)((double)elapsed_ms * time_scale / 1000.0);

    /* Waits up to MK_POLL_TIMEOUT_MS for a command; this is the loop's
     * sleep. */
    mk_poll(c.consumer, on_message, &c);
    rd_kafka_poll(c.producer, 0); /* serve delivery reports */

    if (whirlpooling_update(&c.whirlpool, c.now) == WHIRLPOOLING_DONE)
      finish_batch(&c);

    heartbeat_tick(&hb, elapsed_ms);
  }

  heartbeat_close(&hb);
  mk_consumer_close(c.consumer);
  mk_producer_close(c.producer);
  return EXIT_SUCCESS;
}
