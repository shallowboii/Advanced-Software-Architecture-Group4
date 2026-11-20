/*
 * End-to-end test of the whirlpooling controller: this program plays the
 * scheduler (machine_kafka + the experimental Beer A/B recipes) and runs
 * build/whirlpooling as a separate process against real brokers
 * (KAFKA_BROKERS, MQTT_HOST; the controller needs MQTT to start).
 *
 *   1. start with the Beer B recipe -> running -> done -> idle
 *   2. the same batch re-sent with a new message_id -> done, not run again
 *   3. busy, stop, start of a stopped batch, flush of a foreign batch, flush
 *   4. controller killed while running -> INTERRUPTED after the restart
 *
 * Uses a machine id derived from the time, so it does not disturb other
 * whirlpools on the "whirlpooling" topic. TIME_SCALE 600: 1 h takes 6 s.
 */
#define _POSIX_C_SOURCE 200809L

#include "machine_kafka.h"
#include "recipes.h"

#include <assert.h>
#include <signal.h>
#include <spawn.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <time.h>

extern char **environ;

#define TYPE "whirlpooling"
#define MAX_STATUS 64

static mk_bus_t *bus;
static int id;
static long run;
static mk_message_t statuses[MAX_STATUS];
static size_t n_statuses;
static const char *controller;
static char id_text[16], name[32], ledger[64];

static void on_status(const mk_message_t *s, void *user) {
  (void)user;
  if (s->machine_id != id)
    return;
  assert(n_statuses < MAX_STATUS);
  statuses[n_statuses++] = *s;
  printf("  %s#%d %s %s %s\n", s->machine_type, s->machine_id,
         mk_type_name(s->type), s->batch_id, s->error_code);
}

/* First status from `from` on with this type (and batch / error code, if
 * given), waiting up to `seconds`; -1 on timeout. */
static int wait_for(size_t from, mk_type_t type, const char *batch,
                    const char *code, int seconds) {
  time_t end = time(NULL) + seconds;
  for (;;) {
    for (size_t i = from; i < n_statuses; ++i) {
      const mk_message_t *s = &statuses[i];
      if (s->type == type &&
          (batch == NULL || strcmp(s->batch_id, batch) == 0) &&
          (code == NULL || strcmp(s->error_code, code) == 0))
        return (int)i;
    }
    if (time(NULL) > end) {
      fprintf(stderr, "timeout waiting for %s %s %s\n", mk_type_name(type),
              batch ? batch : "", code ? code : "");
      return -1;
    }
    mk_poll(bus, 100, on_status, NULL);
  }
}

static void batch_name(char *buf, size_t len, int n) {
  snprintf(buf, len, "T%ld-%d", run, n);
}

static void send_start(const char *batch, const char *beer, double duration) {
  mk_message_t start;
  assert(mk_make_start(&start, TYPE, id, batch) == MK_OK);
  assert(recipe_apply(&start, beer) == MK_OK);
  if (duration > 0)
    assert(mk_param_set(&start, "duration_s", duration) == MK_OK);
  assert(mk_send(bus, &start) == MK_OK);
}

static void send_command(mk_type_t type, const char *batch) {
  mk_message_t cmd;
  assert(mk_make_command(&cmd, type, TYPE, id, batch) == MK_OK);
  assert(mk_send(bus, &cmd) == MK_OK);
}

static pid_t start_controller(void) {
  const char *mqtt = getenv("MQTT_HOST");
  char *argv[] = {(char *)controller, (char *)(mqtt ? mqtt : "localhost"),
                  "1883", name, NULL};
  pid_t pid;
  assert(posix_spawn(&pid, controller, NULL, NULL, argv, environ) == 0);
  return pid;
}

static void stop_controller(pid_t pid, int sig) {
  int status;
  kill(pid, sig);
  assert(waitpid(pid, &status, 0) == pid);
  if (sig == SIGTERM)
    assert(WIFEXITED(status) && WEXITSTATUS(status) == 0);
}

int main(int argc, char **argv) {
  setvbuf(stdout, NULL, _IOLBF, 0);
  if (argc != 2) {
    fprintf(stderr, "usage: %s ./build/whirlpooling\n", argv[0]);
    return 2;
  }
  controller = argv[1];
  run = (long)time(NULL);
  id = 100 + (int)(run % 800);
  snprintf(id_text, sizeof(id_text), "%d", id);
  snprintf(name, sizeof(name), "whirlpooling-%d", id);
  snprintf(ledger, sizeof(ledger), "build/test-controller-%ld.ledger", run);
  const char *brokers = getenv("KAFKA_BROKERS");
  if (brokers == NULL)
    brokers = "localhost:9092";
  setenv("KAFKA_BROKERS", brokers, 1); /* for the controller */
  setenv("MACHINE_ID", id_text, 1);
  setenv("LEDGER_PATH", ledger, 1);
  setenv("TIME_SCALE", "600", 1);
  printf("controller test: whirlpooling#%d against %s\n", id, brokers);

  const char *topics[] = {TYPE};
  char group[48];
  snprintf(group, sizeof(group), "test-controller-%ld", run);
  mk_config_t cfg = {.brokers = brokers,
                     .client_id = group,
                     .topics = topics,
                     .n_topics = 1,
                     .receive = MK_RECEIVE_STATUSES,
                     .offset_reset = "latest"};
  bus = mk_open(&cfg);
  assert(bus != NULL);
  for (time_t end = time(NULL) + 30; !mk_ready(bus) && time(NULL) < end;)
    mk_poll(bus, 100, on_status, NULL);
  assert(mk_ready(bus));

  pid_t pid = start_controller();
  assert(wait_for(0, MK_IDLE, NULL, NULL, 30) >= 0);

  char b1[32], b2[32], b3[32], b4[32], other[32];
  batch_name(b1, sizeof(b1), 1);
  batch_name(b2, sizeof(b2), 2);
  batch_name(b3, sizeof(b3), 3);
  batch_name(b4, sizeof(b4), 4);
  batch_name(other, sizeof(other), 9);

  puts("1. Beer B recipe: start -> running -> done -> idle");
  size_t from = n_statuses;
  send_start(b1, "B", 0); /* recipe: 3600 s = 6 s at x600 */
  int r = wait_for(from, MK_RUNNING, b1, NULL, 10);
  assert(r >= 0);
  time_t started = time(NULL);
  int d = wait_for(from, MK_DONE, b1, NULL, 20);
  assert(d > r && time(NULL) - started >= 4);
  assert(wait_for((size_t)d, MK_IDLE, NULL, NULL, 10) > d);

  puts("2. same batch again (new message_id) -> done, not run again");
  from = n_statuses;
  send_start(b1, "B", 0);
  d = wait_for(from, MK_DONE, b1, NULL, 10);
  assert(d >= 0);
  for (size_t i = from; i < (size_t)d; ++i)
    assert(statuses[i].type != MK_RUNNING);

  puts("3. busy, stop, flush");
  from = n_statuses;
  send_start(b2, "A", 36000); /* 60 s at x600: still running below */
  assert(wait_for(from, MK_RUNNING, b2, NULL, 10) >= 0);
  send_start(b3, "A", 0);
  assert(wait_for(from, MK_ERROR, b3, "BUSY", 10) >= 0);
  send_command(MK_STOP, NULL);
  assert(wait_for(from, MK_ERROR, b2, "STOPPED", 10) >= 0);
  size_t after_stop = n_statuses;
  send_start(b2, "A", 0); /* the stopped batch is never run again */
  assert(wait_for(after_stop, MK_ERROR, b2, "STOPPED", 10) >= 0);
  send_command(MK_FLUSH, other);
  assert(wait_for(after_stop, MK_ERROR, other, "NOT_HERE", 10) >= 0);
  send_command(MK_FLUSH, b2);
  int f = wait_for(after_stop, MK_ERROR, b2, "FLUSHED", 10);
  assert(f >= 0 && wait_for((size_t)f, MK_IDLE, NULL, NULL, 10) > f);
  from = n_statuses;
  send_command(MK_STOP, NULL);
  assert(wait_for(from, MK_ERROR, NULL, "NOT_RUNNING", 10) >= 0);

  puts("4. controller killed while running -> INTERRUPTED after restart");
  from = n_statuses;
  send_start(b4, "A", 36000);
  assert(wait_for(from, MK_RUNNING, b4, NULL, 10) >= 0);
  stop_controller(pid, SIGKILL); /* crash */
  from = n_statuses;
  pid = start_controller();
  /* The killed consumer stays in the group until its session times out
   * (45 s), so the restarted controller may need that long to be ready. */
  int e = wait_for(from, MK_ERROR, b4, "INTERRUPTED", 75);
  assert(e >= 0 && wait_for((size_t)e, MK_IDLE, NULL, NULL, 10) > e);

  stop_controller(pid, SIGTERM);
  mk_close(bus, 5000);
  remove(ledger);
  printf("whirlpooling controller test passed (whirlpooling#%d)\n", id);
  return 0;
}
