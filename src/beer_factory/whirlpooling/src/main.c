/*
 * Whirlpooling controller: one thread, one loop that
 *   - takes start / stop / flush from Kafka (machine_kafka, topic
 *     "whirlpooling") and answers with idle / running / done / error; the
 *     ledger file makes sure a batch is never whirlpooled twice,
 *   - drives the whirlpool state machine (whirlpooling.c),
 *   - publishes an MQTT heartbeat every 2 s (machine_mqtt) if MQTT_HOST is set.
 *
 * Usage: whirlpooling [machine-name]      (default whirlpooling-1)
 *
 * Environment:
 *   KAFKA_BROKERS  Kafka bootstrap servers, e.g. kafka:29092 (required).
 *   MACHINE_ID     Kafka machine_id; default: the number after the last '-'
 *                  of machine-name (whirlpooling-2 -> 2), otherwise 1.
 *   LEDGER_PATH    ledger file, default whirlpooling-<id>.ledger. Put it on a
 *                  volume: losing it removes the duplicate protection.
 *   MQTT_HOST      MQTT broker for the heartbeat; unset = no heartbeat.
 *   MQTT_PORT      default 1883.
 *   TIME_SCALE     simulated seconds per real second (default 1; 600 makes a
 *                  one-hour run take 6 s).
 *
 * Start parameters: duration_s (1..86400, default 3600).
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
#define HEARTBEAT_INTERVAL_MS 2000
#define MAX_DURATION_S 86400.0

static volatile sig_atomic_t stopping;

static void stop_controller(int signum) {
  (void)signum;
  stopping = 1;
}

/* ---- Kafka side --------------------------------------------------------- */

typedef struct {
  int id;
  mk_bus_t *bus;
  mk_ledger_t ledger;
  whirlpooling_t whirlpool;
  whirlpooling_batches_t batches;
  mk_message_t current; /* the start of the batch in the vessel */
  uint64_t now;         /* simulated seconds since startup */
  int announced;        /* first idle sent */
} controller_t;

/* Not called "send": that is the POSIX socket function. */
static void send_status(controller_t *c, mk_message_t *status) {
  int result = mk_send(c->bus, status);
  if (result != MK_OK) {
    fprintf(stderr, "kafka: cannot send %s: %s\n", mk_type_name(status->type),
            mk_result_name(result));
    return;
  }
  printf("-> %s%s%s%s%s\n", mk_type_name(status->type),
         status->batch_id[0] ? " " : "", status->batch_id,
         status->error_code[0] ? " " : "", status->error_code);
}

static void handle_start(controller_t *c, const mk_message_t *start) {
  mk_message_t status;
  int seen = mk_ledger_lookup(&c->ledger, start, &status);
  if (seen < 0)
    return;
  if (seen == 1) { /* re-sent or redelivered: repeat, never run again */
    printf("start %s seen before\n", start->batch_id);
    send_status(c, &status);
    return;
  }
  if (c->whirlpool.state != WHIRLPOOLING_WAIT) {
    if (mk_ledger_refuse(&c->ledger, start, "BUSY", c->current.batch_id,
                         &status) == MK_OK)
      send_status(c, &status);
    return;
  }

  double duration = WHIRLPOOLING_DURATION_SECONDS;
  mk_param_get(start, "duration_s", &duration);
  if (!(duration >= 1.0 && duration <= MAX_DURATION_S)) {
    if (mk_ledger_refuse(&c->ledger, start, "INVALID_PARAM",
                         "duration_s must be 1..86400", &status) == MK_OK)
      send_status(c, &status);
    return;
  }

  /* Write the ledger first; only then start the whirlpool. */
  if (mk_ledger_accept(&c->ledger, start) != MK_OK) {
    if (mk_make_error(&status, start, "LEDGER", "cannot write ledger") ==
        MK_OK)
      send_status(c, &status);
    return;
  }
  c->batches.hot_wort_batches = 1; /* the kettle pumped the hot wort over */
  whirlpooling_set_duration(&c->whirlpool, (uint64_t)duration);
  if (whirlpooling_start(&c->whirlpool, c->now) != WHIRLPOOLING_STARTED) {
    mk_make_error(&status, start, "WHIRLPOOL", "whirlpool refused to start");
    mk_ledger_record(&c->ledger, &status);
    send_status(c, &status);
    return;
  }
  c->current = *start;
  printf("Whirlpooling %s for %.0f s.\n", start->batch_id, duration);
  mk_make_running(&status, start);
  send_status(c, &status);
}

static void handle_stop(controller_t *c, const mk_message_t *stop) {
  mk_message_t status;
  if (c->whirlpool.state != WHIRLPOOLING_WHIRLPOOL ||
      (stop->batch_id[0] != '\0' &&
       strcmp(stop->batch_id, c->current.batch_id) != 0)) {
    if (mk_make_error(&status, stop, "NOT_RUNNING",
                      "that batch is not running here") == MK_OK)
      send_status(c, &status);
    return;
  }
  whirlpooling_stop(&c->whirlpool);
  printf("Stopped %s: whirlpool off, wort stays in the vessel.\n",
         c->current.batch_id);
  mk_make_error(&status, &c->current, "STOPPED",
                "stopped by command; flush to empty the vessel");
  mk_ledger_record(&c->ledger, &status); /* a re-sent start won't run it */
  send_status(c, &status);
}

static void handle_flush(controller_t *c, const mk_message_t *flush) {
  mk_message_t status;
  if (c->whirlpool.state == WHIRLPOOLING_WAIT ||
      strcmp(flush->batch_id, c->current.batch_id) != 0) {
    if (mk_make_error(&status, flush, "NOT_HERE",
                      "batch is not in this whirlpool") == MK_OK)
      send_status(c, &status);
    return;
  }
  whirlpooling_flush(&c->whirlpool);
  printf("Flushed %s: wort discarded, vessel empty.\n", c->current.batch_id);
  mk_make_error(&status, &c->current, "FLUSHED", "batch discarded");
  mk_ledger_record(&c->ledger, &status);
  send_status(c, &status);
  mk_make_idle(&status, MACHINE_TYPE, c->id);
  send_status(c, &status);
}

static void on_command(const mk_message_t *msg, void *user) {
  controller_t *c = user;
  if (msg->machine_id != c->id)
    return; /* for another whirlpool */
  printf("<- %s %s\n", mk_type_name(msg->type), msg->batch_id);
  switch (msg->type) {
  case MK_START:
    handle_start(c, msg);
    break;
  case MK_STOP:
    handle_stop(c, msg);
    break;
  case MK_FLUSH:
    handle_flush(c, msg);
    break;
  default:
    break; /* mk_poll only delivers commands */
  }
}

/* Once the bus is ready: report batches cut off by the last crash, then
 * tell the scheduler the whirlpool is free. */
static void announce(controller_t *c) {
  mk_message_t status;
  while (mk_ledger_next_interrupted(&c->ledger, &status))
    send_status(c, &status);
  mk_make_idle(&status, MACHINE_TYPE, c->id);
  send_status(c, &status);
  c->announced = 1;
}

static void finish_batch(controller_t *c) {
  mk_message_t status;
  printf("Whirlpooled %s.\n", c->current.batch_id);
  mk_make_done(&status, &c->current);
  mk_ledger_record(&c->ledger, &status); /* before sending */
  send_status(c, &status);
  c->batches.clarified_wort_batches = 0; /* pumped to a tank */
  mk_make_idle(&status, MACHINE_TYPE, c->id);
  send_status(c, &status);
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

  /* Ledger first (refuse to run without duplicate protection), then the bus.
   * A Kafka broker that is down is not an error; the bus reconnects. */
  char path[MK_PATH_LEN], client_id[48];
  const char *ledger_env = getenv("LEDGER_PATH");
  if (ledger_env != NULL)
    snprintf(path, sizeof(path), "%s", ledger_env);
  else
    snprintf(path, sizeof(path), "whirlpooling-%d.ledger", c.id);
  if (mk_ledger_open(&c.ledger, path, MACHINE_TYPE, c.id) != MK_OK) {
    fprintf(stderr, "Ledger %s is unusable; refusing to start.\n", path);
    return EXIT_FAILURE;
  }
  snprintf(client_id, sizeof(client_id), "%s-%d", MACHINE_TYPE, c.id);
  const char *topics[] = {MACHINE_TYPE};
  mk_config_t cfg = {.brokers = brokers,
                     .client_id = client_id,
                     .topics = topics,
                     .n_topics = 1,
                     .receive = MK_RECEIVE_COMMANDS};
  c.bus = mk_open(&cfg);
  if (c.bus == NULL)
    return EXIT_FAILURE;
  printf("Whirlpooling %d: Kafka %s, ledger %s, time x%g.\n", c.id, brokers,
         path, time_scale);

  heartbeat_link_t hb;
  if (heartbeat_open(&hb, name) != 0) {
    mk_close(c.bus, 1000);
    return EXIT_FAILURE;
  }

  struct timespec started;
  clock_gettime(CLOCK_MONOTONIC, &started);
  int status = EXIT_SUCCESS;
  while (!stopping) {
    uint64_t elapsed_ms = elapsed_ms_since(&started);
    c.now = (uint64_t)((double)elapsed_ms * time_scale / 1000.0);

    /* Waits up to 100 ms for commands; this is the loop's sleep. */
    if (mk_poll(c.bus, 100, on_command, &c) == MK_ERR_FATAL) {
      fprintf(stderr, "kafka: fatal error, exiting\n");
      status = EXIT_FAILURE;
      break;
    }
    if (!c.announced && mk_ready(c.bus))
      announce(&c);

    if (whirlpooling_update(&c.whirlpool, c.now) == WHIRLPOOLING_DONE)
      finish_batch(&c);

    heartbeat_tick(&hb, elapsed_ms);
  }

  heartbeat_close(&hb);
  mk_close(c.bus, 5000);
  return status;
}
