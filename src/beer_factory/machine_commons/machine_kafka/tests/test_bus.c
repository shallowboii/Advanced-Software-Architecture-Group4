/*
 * Integration test against a real broker (KAFKA_BROKERS, default
 * localhost:9092). Plays scheduler and machine "testmachine" 1 in one process:
 *
 *   scheduler --start(id 1)--> machine 1 --done--> scheduler
 *   scheduler --start(id 2)--> ignored by machine 1
 *   same start delivered twice --> handled once (dedup)
 *
 * Uses a fresh topic and consumer groups per run, so it can be repeated.
 */
#include "machine_kafka.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

typedef struct {
  mk_bus_t *bus;
  int machine_id;
  mk_dedup_t dedup;
  int starts_handled;
} machine_t;

typedef struct {
  int dones;
  char last_reply_to[MK_ID_LEN];
} scheduler_t;

static void machine_handler(const mk_message_t *msg, void *user) {
  machine_t *m = user;
  if (msg->type != MK_START || msg->machine_id != m->machine_id)
    return;
  if (mk_dedup_seen(&m->dedup, msg->message_id))
    return;
  ++m->starts_handled;
  mk_message_t done;
  assert(mk_make_done(&done, msg) == MK_OK);
  assert(mk_send(m->bus, &done) == MK_OK);
}

static void scheduler_handler(const mk_message_t *msg, void *user) {
  scheduler_t *s = user;
  if (msg->type == MK_DONE) {
    ++s->dones;
    strcpy(s->last_reply_to, msg->reply_to);
  }
}

/* Polls both sides until the scheduler has want_dones replies or 30 s pass. */
static int pump(mk_bus_t *sched_bus, scheduler_t *s, machine_t *m,
                int want_dones) {
  for (int i = 0; i < 300; ++i) {
    assert(mk_poll(sched_bus, 50, scheduler_handler, s) >= 0);
    assert(mk_poll(m->bus, 50, machine_handler, m) >= 0);
    if (s->dones >= want_dones)
      return 1;
  }
  return 0;
}

int main(void) {
  const char *brokers = getenv("KAFKA_BROKERS");
  if (brokers == NULL)
    brokers = "localhost:9092";

  char topic[64], sched_group[96], machine_group[96];
  long run = (long)time(NULL);
  snprintf(topic, sizeof(topic), "mktest%ld", run);
  snprintf(sched_group, sizeof(sched_group), "scheduler-%ld", run);
  snprintf(machine_group, sizeof(machine_group), "%s-1", topic);
  const char *topics[] = {topic};

  mk_config_t sched_cfg = {.brokers = brokers,
                           .client_id = "scheduler",
                           .group_id = sched_group,
                           .topics = topics,
                           .n_topics = 1};
  mk_config_t machine_cfg = {.brokers = brokers,
                             .client_id = machine_group,
                             .topics = topics,
                             .n_topics = 1};
  mk_bus_t *sched_bus = mk_open(&sched_cfg);
  machine_t machine = {.bus = mk_open(&machine_cfg), .machine_id = 1};
  assert(sched_bus != NULL && machine.bus != NULL);
  mk_dedup_init(&machine.dedup);
  scheduler_t sched = {0};

  /* 1: start for machine 1 is answered with done. */
  mk_message_t start;
  assert(mk_make_start(&start, topic, 1, "B-0001") == MK_OK);
  assert(mk_send(sched_bus, &start) == MK_OK);
  assert(start.message_id[0] != '\0'); /* filled in by mk_send */
  if (!pump(sched_bus, &sched, &machine, 1)) {
    fprintf(stderr, "no done received (is Kafka at %s?)\n", brokers);
    return 1;
  }
  assert(strcmp(sched.last_reply_to, start.message_id) == 0);
  assert(machine.starts_handled == 1);

  /* 2: the same start again (a redelivery) does not start a second batch. */
  assert(mk_send(sched_bus, &start) == MK_OK); /* keeps its message_id */
  /* 3: a start for machine 2 is ignored by machine 1. */
  mk_message_t other;
  assert(mk_make_start(&other, topic, 2, "B-0002") == MK_OK);
  assert(mk_send(sched_bus, &other) == MK_OK);
  /* 4: a fresh start is handled. It has the same key as the duplicate, so it
   * arrives after it: by the time its done comes back, the duplicate must
   * already have been skipped. */
  mk_message_t next;
  assert(mk_make_start(&next, topic, 1, "B-0003") == MK_OK);
  assert(mk_send(sched_bus, &next) == MK_OK);
  assert(pump(sched_bus, &sched, &machine, 2));
  assert(strcmp(sched.last_reply_to, next.message_id) == 0);
  assert(machine.starts_handled == 2);
  assert(sched.dones == 2);

  assert(mk_delivery_failures(sched_bus) == 0);
  assert(mk_delivery_failures(machine.bus) == 0);
  assert(mk_close(machine.bus, 5000) == 0);
  assert(mk_close(sched_bus, 5000) == 0);
  printf("machine_kafka: bus test passed against %s (topic %s)\n", brokers,
         topic);
  return 0;
}
