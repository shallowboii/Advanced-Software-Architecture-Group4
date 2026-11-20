/*
 * A tiny stand-in for the scheduler: sends one command and prints the
 * statuses the machine answers with. Shows the scheduler half of the
 * library (the machine half is src/beer_factory/whirlpooling/src/main.c).
 *
 *   scheduler-cli start <type> <id> <batch> <beer> [key=value ...]
 *   (beer A/B from the EXPERIMENTAL recipes.c, see recipes.h)
 *   scheduler-cli stop  <type> <id> [batch]
 *   scheduler-cli flush <type> <id> <batch>
 *   scheduler-cli watch <type>
 *
 *   start whirlpooling 1 B-0001 B             Beer B (Lager) for whirlpool 1
 *   start whirlpooling 1 B-0002 A duration_s=30   recipe value overridden
 *
 * KAFKA_BROKERS (default localhost:9092); WAIT_S how long to wait for answers
 * (default 15, watch: forever). Ctrl-C to quit.
 */
#define _POSIX_C_SOURCE 200809L

#include "machine_kafka.h"
#include "recipes.h"

#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static volatile sig_atomic_t stopping;
static void on_signal(int s) {
  (void)s;
  stopping = 1;
}

typedef struct {
  int id;          /* 0 = every machine */
  char since[MK_TS_LEN]; /* ignore statuses older than this */
  char batch[MK_BATCH_LEN];
  int finished;    /* got done/error for batch */
} watch_t;

static void on_status(const mk_message_t *s, void *user) {
  watch_t *w = user;
  if ((w->id != 0 && s->machine_id != w->id) || strcmp(s->ts_utc, w->since) < 0)
    return; /* another machine, or from before this run */
  printf("%s  %s#%d  %-7s %s", s->ts_utc, s->machine_type, s->machine_id,
         mk_type_name(s->type), s->batch_id);
  if (s->type == MK_ERROR)
    printf("  %s: %s", s->error_code, s->error_reason);
  printf("\n");
  if (w->batch[0] != '\0' && strcmp(s->batch_id, w->batch) == 0 &&
      (s->type == MK_DONE || s->type == MK_ERROR))
    w->finished = 1;
}

static void now_utc(char *buf, size_t len, int minus_s) {
  time_t t = time(NULL) - minus_s;
  struct tm utc;
  gmtime_r(&t, &utc);
  strftime(buf, len, "%Y-%m-%dT%H:%M:%SZ", &utc);
}

static int usage(void) {
  fprintf(stderr,
          "usage: scheduler-cli start <type> <id> <batch> <A|B> [name=value ...]\n"
          "       scheduler-cli stop  <type> <id> [batch]\n"
          "       scheduler-cli flush <type> <id> <batch>\n"
          "       scheduler-cli watch <type>\n");
  return 2;
}

int main(int argc, char **argv) {
  setvbuf(stdout, NULL, _IOLBF, 0);
  if (argc < 3)
    return usage();
  const char *cmd = argv[1], *type = argv[2];
  int watch_only = strcmp(cmd, "watch") == 0;
  if (!watch_only && argc < 4)
    return usage();

  mk_message_t msg;
  watch_t w = {.id = watch_only ? 0 : atoi(argv[3])};
  if (strcmp(cmd, "start") == 0 && argc >= 6) {
    if (mk_make_start(&msg, type, w.id, argv[4]) != MK_OK ||
        recipe_apply(&msg, argv[5]) != MK_OK) {
      fprintf(stderr, "bad start (machine type, id, batch or beer A/B)\n");
      return 2;
    }
    for (int i = 6; i < argc; ++i) { /* overrides, e.g. duration_s=30 */
      char name[MK_NAME_LEN];
      const char *eq = strchr(argv[i], '=');
      if (eq == NULL || (size_t)(eq - argv[i]) >= sizeof(name))
        return usage();
      memcpy(name, argv[i], (size_t)(eq - argv[i]));
      name[eq - argv[i]] = '\0';
      if (mk_param_set(&msg, name, atof(eq + 1)) != MK_OK)
        return usage();
    }
    printf("Beer %s (%s) for %s#%d:", argv[5], recipe_name(argv[5]), type,
           w.id);
    for (size_t i = 0; i < msg.n_params; ++i)
      printf(" %s=%g", msg.params[i].name, msg.params[i].value);
    printf("\n");
  } else if (strcmp(cmd, "stop") == 0) {
    if (mk_make_command(&msg, MK_STOP, type, w.id, argc > 4 ? argv[4] : NULL) !=
        MK_OK)
      return usage();
  } else if (strcmp(cmd, "flush") == 0 && argc >= 5) {
    if (mk_make_command(&msg, MK_FLUSH, type, w.id, argv[4]) != MK_OK)
      return usage();
  } else if (!watch_only) {
    return usage();
  }
  if (!watch_only)
    snprintf(w.batch, sizeof(w.batch), "%s", msg.batch_id);

  signal(SIGINT, on_signal);
  signal(SIGTERM, on_signal);
  const char *brokers = getenv("KAFKA_BROKERS");
  const char *topics[] = {type};
  mk_config_t cfg = {.brokers = brokers ? brokers : "localhost:9092",
                     .client_id = "scheduler-cli",
                     .topics = topics,
                     .n_topics = 1,
                     .receive = MK_RECEIVE_STATUSES,
                     .offset_reset = "latest"};
  mk_bus_t *bus = mk_open(&cfg);
  if (bus == NULL)
    return 1;

  /* Subscribe before sending, so the answer cannot be missed. */
  now_utc(w.since, sizeof(w.since), 1);
  time_t give_up = time(NULL) + 30;
  while (!mk_ready(bus) && !stopping && time(NULL) < give_up)
    mk_poll(bus, 100, on_status, &w);
  if (!mk_ready(bus)) {
    fprintf(stderr, "no Kafka connection (is the broker running?)\n");
    mk_close(bus, 1000);
    return 1;
  }
  now_utc(w.since, sizeof(w.since), 1);

  if (!watch_only) {
    if (mk_send(bus, &msg) != MK_OK) {
      mk_close(bus, 1000);
      return 1;
    }
    printf("sent %s %s to %s#%d (id %s)\n", cmd, msg.batch_id, type, w.id,
           msg.message_id);
  }

  const char *wait_env = getenv("WAIT_S");
  int wait_s = wait_env ? atoi(wait_env) : 15;
  time_t end = time(NULL) + wait_s;
  while (!stopping && (watch_only || (time(NULL) < end && !w.finished)))
    mk_poll(bus, 200, on_status, &w);
  /* Statuses that answer stop/flush come right after each other. */
  for (time_t t = time(NULL) + 1; !watch_only && time(NULL) < t;)
    mk_poll(bus, 200, on_status, &w);
  mk_close(bus, 5000);
  return 0;
}
