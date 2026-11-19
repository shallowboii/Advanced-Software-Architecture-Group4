/*
 * Integration test against a real broker (KAFKA_BROKERS, default
 * localhost:9092) with topic auto-creation disabled (docker-compose.yml).
 * Plays the scheduler and several machines in one process:
 *
 *   1. start -> running -> done on the machine type's topic; machines only
 *      get commands, the scheduler only statuses; params round-trip
 *   2. the same batch started again with a new message_id -> runs once
 *   3. machine restart (new bus, ledger reloaded from its file) and a lost
 *      offset commit (full redelivery) -> nothing runs twice
 *   4. new machine with a reused id: latest skips old commands, earliest
 *      would replay them
 *   5. the scheduler reads the topics of several machine types
 *   6. malformed, misplaced and expired messages are skipped
 *   7. a message to a missing topic reaches the delivery-failure callback
 *
 * Every run uses fresh machine types (topics) and groups, and deletes its
 * topics at the end.
 */
#define _POSIX_C_SOURCE 200809L

#include "machine_kafka.h"

#include <assert.h>
#include <librdkafka/rdkafka.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define MAX_MACHINES 8
#define MAX_STATUS 128

static const char *brokers;
static char type1[MK_NAME_LEN]; /* machine type used by most scenarios */
static char type2[MK_NAME_LEN]; /* a second machine type (scenario 5) */
static rd_kafka_t *admin;       /* plain producer for admin and raw messages */

/* ---- Test machine ------------------------------------------------------ */

typedef struct {
  const char *name;
  mk_bus_t *bus;
  mk_ledger_t ledger;
  char type[MK_NAME_LEN];
  int id;
  int use_ledger; /* 0: only count received starts */
  int announced;  /* idle sent */
  int busy;
  mk_message_t current;
  int received; /* starts addressed to this machine */
  double last_temp;
} machine_t;

static machine_t *machines[MAX_MACHINES];
static size_t n_machines;
/* Physical executions per batch, across all "processes" of a machine. */
static int executions[100];

static int batch_no(const char *batch_id) { return atoi(batch_id + 2); }

static void machine_handler(const mk_message_t *msg, void *user) {
  machine_t *m = user;
  /* mk_poll only delivers commands of our type, never statuses. */
  assert(mk_is_command(msg->type));
  assert(strcmp(msg->machine_type, m->type) == 0);
  if (msg->type != MK_START || msg->machine_id != m->id)
    return;
  ++m->received;
  if (!m->use_ledger)
    return;

  mk_message_t reply;
  int dup = mk_ledger_lookup(&m->ledger, msg, &reply);
  assert(dup >= 0);
  if (dup == 1) { /* already answered: repeat its status, do not run */
    assert(mk_send(m->bus, &reply) == MK_OK);
    return;
  }
  if (m->busy) {
    assert(mk_ledger_refuse(&m->ledger, msg, "BUSY", m->current.batch_id,
                            &reply) == MK_OK);
    assert(mk_send(m->bus, &reply) == MK_OK);
    return;
  }
  assert(mk_ledger_accept(&m->ledger, msg) == MK_OK);
  ++executions[batch_no(msg->batch_id)]; /* "actuate" */
  m->busy = 1;
  m->current = *msg;
  m->last_temp = -1;
  mk_param_get(msg, "target_temp_c", &m->last_temp);
  assert(mk_make_running(&reply, msg) == MK_OK);
  assert(mk_send(m->bus, &reply) == MK_OK);
}

static machine_t *machine_open(machine_t *m, const char *name,
                               const char *type, int id, const char *group,
                               const char *reset, int use_ledger) {
  memset(m, 0, sizeof(*m));
  m->name = name;
  m->id = id;
  m->use_ledger = use_ledger;
  strcpy(m->type, type);
  const char *topics[] = {type};
  mk_config_t cfg = {.brokers = brokers,
                     .client_id = group,
                     .topics = topics,
                     .n_topics = 1,
                     .receive = MK_RECEIVE_COMMANDS,
                     .offset_reset = reset};
  m->bus = mk_open(&cfg);
  assert(m->bus != NULL);
  char path[MK_PATH_LEN];
  snprintf(path, sizeof(path), "build/test-bus-%s-%d.state", type, id);
  if (use_ledger)
    assert(mk_ledger_open(&m->ledger, path, type, id) == MK_OK);
  machines[n_machines++] = m;
  return m;
}

static void machine_close(machine_t *m) {
  assert(mk_delivery_failures(m->bus) == 0);
  assert(mk_close(m->bus, 5000) == 0);
  for (size_t i = 0; i < n_machines; ++i) {
    if (machines[i] == m) {
      machines[i] = machines[--n_machines];
      break;
    }
  }
}

/* What the machine's main loop does besides polling. */
static void machine_tick(machine_t *m) {
  mk_message_t status;
  if (!m->announced && mk_ready(m->bus)) {
    while (m->use_ledger && mk_ledger_next_interrupted(&m->ledger, &status))
      assert(mk_send(m->bus, &status) == MK_OK);
    assert(mk_make_idle(&status, m->type, m->id) == MK_OK);
    assert(mk_send(m->bus, &status) == MK_OK);
    m->announced = 1;
  }
}

/* Finishes the current batch: done, then idle. */
static void machine_finish(machine_t *m) {
  assert(m->busy);
  mk_message_t status;
  assert(mk_make_done(&status, &m->current) == MK_OK);
  assert(mk_ledger_record(&m->ledger, &status) == MK_OK);
  assert(mk_send(m->bus, &status) == MK_OK);
  m->busy = 0;
  assert(mk_make_idle(&status, m->type, m->id) == MK_OK);
  assert(mk_send(m->bus, &status) == MK_OK);
}

/* ---- Test scheduler ---------------------------------------------------- */

static mk_bus_t *sched_bus;
static mk_message_t statuses[MAX_STATUS];
static size_t n_statuses;

static void scheduler_handler(const mk_message_t *msg, void *user) {
  (void)user;
  assert(!mk_is_command(msg->type)); /* its own commands are skipped */
  assert(n_statuses < MAX_STATUS);
  statuses[n_statuses++] = *msg;
}

/* Index of the first status from `from` on matching the filter, or -1.
 * batch NULL matches any batch. */
static int find_status(size_t from, const char *type, int id, mk_type_t st,
                       const char *batch) {
  for (size_t i = from; i < n_statuses; ++i) {
    const mk_message_t *s = &statuses[i];
    if (strcmp(s->machine_type, type) == 0 && s->machine_id == id &&
        s->type == st && (batch == NULL || strcmp(s->batch_id, batch) == 0))
      return (int)i;
  }
  return -1;
}

static void send_start(mk_message_t *start, const char *type, int id,
                       const char *batch) {
  assert(mk_make_start(start, type, id, batch) == MK_OK);
  assert(mk_send(sched_bus, start) == MK_OK);
}

/* ---- Polling ----------------------------------------------------------- */

static mk_bus_t *orphan_bus; /* scenario 7 */
static int orphan_failures;

static void pump_once(void) {
  assert(mk_poll(sched_bus, 20, scheduler_handler, NULL) >= 0);
  for (size_t i = 0; i < n_machines; ++i) {
    assert(mk_poll(machines[i]->bus, 20, machine_handler, machines[i]) >= 0);
    machine_tick(machines[i]);
  }
  if (orphan_bus != NULL)
    assert(mk_poll(orphan_bus, 0, NULL, NULL) >= 0);
}

typedef int (*cond_fn)(const void *arg);

/* Pumps until cond holds (returns 1) or seconds pass (returns 0). */
static int pump_until(cond_fn cond, const void *arg, int seconds) {
  time_t end = time(NULL) + seconds;
  while (time(NULL) <= end) {
    pump_once();
    if (cond != NULL && cond(arg))
      return 1;
  }
  return cond == NULL;
}

typedef struct {
  const char *type;
  int id;
  mk_type_t status;
  const char *batch;
  size_t from;
} want_t;

static int has_status(const void *arg) {
  const want_t *w = arg;
  return find_status(w->from, w->type, w->id, w->status, w->batch) >= 0;
}

static int wait_status(const char *type, int id, mk_type_t status,
                       const char *batch, size_t from, int seconds) {
  want_t w = {type, id, status, batch, from};
  if (!pump_until(has_status, &w, seconds)) {
    fprintf(stderr, "timeout waiting for %s %s from %s %d\n",
            mk_type_name(status), batch ? batch : "", type, id);
    return -1;
  }
  return find_status(from, type, id, status, batch);
}

static int bus_ready(const void *arg) { return mk_ready((mk_bus_t *)arg); }

/* ---- Raw Kafka helpers ------------------------------------------------- */

static void admin_topic(const char *topic, int create) {
  rd_kafka_queue_t *q = rd_kafka_queue_new(admin);
  char err[256];
  if (create) {
    rd_kafka_NewTopic_t *nt = rd_kafka_NewTopic_new(topic, 1, 1, err, sizeof(err));
    rd_kafka_CreateTopics(admin, &nt, 1, NULL, q);
    rd_kafka_NewTopic_destroy(nt);
  } else {
    rd_kafka_DeleteTopic_t *dt = rd_kafka_DeleteTopic_new(topic);
    rd_kafka_DeleteTopics(admin, &dt, 1, NULL, q);
    rd_kafka_DeleteTopic_destroy(dt);
  }
  rd_kafka_event_t *ev = rd_kafka_queue_poll(q, 20000);
  assert(ev != NULL);
  if (rd_kafka_event_error(ev))
    fprintf(stderr, "admin: %s\n", rd_kafka_event_error_string(ev));
  if (create) {
    size_t n;
    const rd_kafka_topic_result_t **res = rd_kafka_CreateTopics_result_topics(
        rd_kafka_event_CreateTopics_result(ev), &n);
    for (size_t i = 0; i < n; ++i)
      assert(rd_kafka_topic_result_error(res[i]) == 0);
  }
  rd_kafka_event_destroy(ev);
  rd_kafka_queue_destroy(q);
}

static void produce_raw(const char *topic, const char *key,
                        const char *payload) {
  rd_kafka_vu_t vus[4];
  memset(vus, 0, sizeof(vus));
  vus[0].vtype = RD_KAFKA_VTYPE_TOPIC;
  vus[0].u.cstr = topic;
  vus[1].vtype = RD_KAFKA_VTYPE_KEY;
  vus[1].u.mem.ptr = (void *)key;
  vus[1].u.mem.size = strlen(key);
  vus[2].vtype = RD_KAFKA_VTYPE_VALUE;
  vus[2].u.mem.ptr = (void *)payload;
  vus[2].u.mem.size = strlen(payload);
  vus[3].vtype = RD_KAFKA_VTYPE_MSGFLAGS;
  vus[3].u.i = RD_KAFKA_MSG_F_COPY;
  rd_kafka_error_t *e = rd_kafka_produceva(admin, vus, 4);
  assert(e == NULL);
  assert(rd_kafka_flush(admin, 10000) == 0);
}

/* Reads a whole topic, checks that every valid message is keyed by its
 * machine_id and counts commands and statuses. */
static void count_topic(const char *topic, int *commands, int *statuses_out) {
  char errstr[256], group[96];
  rd_kafka_conf_t *conf = rd_kafka_conf_new();
  snprintf(group, sizeof(group), "spy-%s", topic);
  assert(rd_kafka_conf_set(conf, "bootstrap.servers", brokers, errstr,
                           sizeof(errstr)) == RD_KAFKA_CONF_OK);
  assert(rd_kafka_conf_set(conf, "group.id", group, errstr, sizeof(errstr)) ==
         RD_KAFKA_CONF_OK);
  assert(rd_kafka_conf_set(conf, "enable.partition.eof", "true", errstr,
                           sizeof(errstr)) == RD_KAFKA_CONF_OK);
  rd_kafka_t *rk =
      rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
  assert(rk != NULL);
  rd_kafka_topic_partition_list_t *parts =
      rd_kafka_topic_partition_list_new(1);
  rd_kafka_topic_partition_list_add(parts, topic, 0)->offset =
      RD_KAFKA_OFFSET_BEGINNING;
  assert(rd_kafka_assign(rk, parts) == 0);
  rd_kafka_topic_partition_list_destroy(parts);

  *commands = *statuses_out = 0;
  time_t end = time(NULL) + 20;
  while (time(NULL) <= end) {
    rd_kafka_message_t *rkm = rd_kafka_consumer_poll(rk, 200);
    if (rkm == NULL)
      continue;
    int eof = rkm->err == RD_KAFKA_RESP_ERR__PARTITION_EOF;
    mk_message_t msg;
    if (!rkm->err && mk_decode(rkm->payload, rkm->len, &msg) == MK_OK) {
      char key[16];
      snprintf(key, sizeof(key), "%d", msg.machine_id);
      assert(rkm->key_len == strlen(key) &&
             memcmp(rkm->key, key, rkm->key_len) == 0);
      ++*(mk_is_command(msg.type) ? commands : statuses_out);
    }
    rd_kafka_message_destroy(rkm);
    if (eof)
      break;
  }
  rd_kafka_consumer_close(rk);
  rd_kafka_destroy(rk);
}

static void on_delivery_failure(const mk_message_t *msg, const char *reason,
                                void *user) {
  (void)user;
  assert(msg != NULL && msg->type == MK_IDLE);
  fprintf(stderr, "  (expected) delivery failed: %s\n", reason);
  ++orphan_failures;
}

/* ---- Scenarios --------------------------------------------------------- */

static machine_t m1, m1_restarted, m1_redelivered, m5_latest, m5_earliest,
    n1;
static char group1[64];

static void scenario_happy_path(void) {
  puts("1. start -> running -> done on the machine type's topic");
  machine_open(&m1, "m1", type1, 1, group1, NULL, 1);
  assert(wait_status(type1, 1, MK_IDLE, NULL, 0, 30) >= 0);

  size_t from = n_statuses;
  mk_message_t start;
  assert(mk_make_start(&start, type1, 1, "B-1") == MK_OK);
  assert(mk_param_set(&start, "target_temp_c", 99.5) == MK_OK);
  assert(mk_param_set(&start, "duration_s", 3600) == MK_OK);
  assert(mk_send(sched_bus, &start) == MK_OK);

  int r = wait_status(type1, 1, MK_RUNNING, "B-1", from, 30);
  assert(r >= 0);
  assert(m1.last_temp == 99.5); /* params arrived intact */
  machine_finish(&m1);
  int d = wait_status(type1, 1, MK_DONE, "B-1", from, 30);
  assert(d > r);
  assert(wait_status(type1, 1, MK_IDLE, NULL, (size_t)d, 30) > d);
  assert(executions[1] == 1);

  /* Both directions on one topic: 1 start; idle, running, done, idle. */
  int commands, status_count;
  count_topic(type1, &commands, &status_count);
  assert(commands == 1 && status_count == 4);
}

static void scenario_duplicate_start(void) {
  puts("2. same batch, new message_id -> executed once");
  size_t from = n_statuses;
  mk_message_t again;
  send_start(&again, type1, 1, "B-1"); /* e.g. from a restarted scheduler */
  assert(wait_status(type1, 1, MK_DONE, "B-1", from, 30) >= 0);
  assert(executions[1] == 1);

  /* While running, a duplicate gets running and another batch gets BUSY. */
  mk_message_t b2, b2_again, b3;
  send_start(&b2, type1, 1, "B-2");
  int r = wait_status(type1, 1, MK_RUNNING, "B-2", from, 30);
  assert(r >= 0);
  send_start(&b2_again, type1, 1, "B-2");
  send_start(&b3, type1, 1, "B-3");
  assert(wait_status(type1, 1, MK_RUNNING, "B-2", (size_t)r + 1, 30) >= 0);
  int e = wait_status(type1, 1, MK_ERROR, "B-3", from, 30);
  assert(e >= 0 && strcmp(statuses[e].error_code, "BUSY") == 0);
  machine_finish(&m1);
  assert(wait_status(type1, 1, MK_DONE, "B-2", from, 30) >= 0);
  assert(executions[2] == 1 && executions[3] == 0);
}

static void scenario_restart(void) {
  puts("3. machine restart and lost commit -> nothing runs twice");
  /* Start B-4, then "crash" while it runs: no done, bus just goes away. */
  size_t from = n_statuses;
  mk_message_t b4;
  send_start(&b4, type1, 1, "B-4");
  assert(wait_status(type1, 1, MK_RUNNING, "B-4", from, 30) >= 0);
  machine_close(&m1);

  /* New process, same group, ledger reloaded from the file. */
  from = n_statuses;
  machine_open(&m1_restarted, "m1'", type1, 1, group1, NULL, 1);
  int e = wait_status(type1, 1, MK_ERROR, "B-4", from, 30);
  assert(e >= 0 && strcmp(statuses[e].error_code, "INTERRUPTED") == 0);
  int idle = wait_status(type1, 1, MK_IDLE, NULL, from, 30);
  assert(idle >= 0);

  from = (size_t)idle + 1;
  mk_message_t again1, again4;
  send_start(&again1, type1, 1, "B-1");
  send_start(&again4, type1, 1, "B-4");
  assert(wait_status(type1, 1, MK_DONE, "B-1", from, 30) >= 0);
  e = wait_status(type1, 1, MK_ERROR, "B-4", from, 30);
  assert(e >= 0 && strcmp(statuses[e].error_code, "INTERRUPTED") == 0);
  machine_close(&m1_restarted);

  /* Lost commit: a group with no offsets and earliest gets every command of
   * the topic again, like a machine that crashed before committing. */
  char group[80];
  snprintf(group, sizeof(group), "%s-recovered", group1);
  machine_open(&m1_redelivered, "m1''", type1, 1, group, "earliest", 1);
  pump_until(NULL, NULL, 6);
  assert(m1_redelivered.received >= 8); /* all starts above */
  /* B-3 was refused (BUSY): its redelivery is refused again, not run. */
  assert(executions[1] == 1 && executions[2] == 1 && executions[3] == 0 &&
         executions[4] == 1);
  machine_close(&m1_redelivered);
}

static void scenario_reused_id(void) {
  puts("4. replacement machine with a reused id: latest skips history");
  mk_message_t old;
  send_start(&old, type1, 5, "B-50"); /* nobody consumes this yet */
  pump_until(NULL, NULL, 1);

  char g_latest[80], g_earliest[80];
  snprintf(g_latest, sizeof(g_latest), "%s-5", type1);
  snprintf(g_earliest, sizeof(g_earliest), "%s-5-earliest", type1);
  machine_open(&m5_latest, "m5", type1, 5, g_latest, NULL, 0);
  machine_open(&m5_earliest, "m5e", type1, 5, g_earliest, "earliest", 0);
  assert(pump_until(bus_ready, m5_latest.bus, 30));
  assert(pump_until(bus_ready, m5_earliest.bus, 30));
  pump_until(NULL, NULL, 4);
  assert(m5_latest.received == 0);   /* old start not replayed */
  assert(m5_earliest.received == 1); /* the hazard latest avoids */

  /* After mk_ready, nothing new is missed. */
  mk_message_t fresh;
  send_start(&fresh, type1, 5, "B-51");
  pump_until(NULL, NULL, 4);
  assert(m5_latest.received == 1);
  machine_close(&m5_latest);
  machine_close(&m5_earliest);
}

static void scenario_second_type(void) {
  puts("5. the scheduler reads several machine types");
  char group[80];
  snprintf(group, sizeof(group), "%s-1", type2);
  machine_open(&n1, "n1", type2, 1, group, NULL, 0);
  assert(wait_status(type2, 1, MK_IDLE, NULL, 0, 30) >= 0);
  machine_close(&n1);
}

static void scenario_bad_messages(void) {
  puts("6. malformed, misplaced and expired messages are skipped");
  machine_open(&m1, "m1", type1, 1, group1, NULL, 1);
  assert(pump_until(bus_ready, m1.bus, 30));

  char json[MK_MAX_JSON_LEN];
  mk_message_t m;
  produce_raw(type1, "1", "not json");
  produce_raw(type1, "1",
              "{\"schema_version\":2,\"message_id\":\"v2\",\"type\":\"start\","
              "\"machine_type\":\"x\",\"machine_id\":1,\"batch_id\":\"B-59\","
              "\"step\":\"boiling\"}");
  /* A start for another machine type on this topic. */
  assert(mk_make_start(&m, "othertype", 1, "B-60") == MK_OK);
  strcpy(m.message_id, "misplaced-1");
  assert(mk_encode(&m, json, sizeof(json)) > 0);
  produce_raw(type1, "1", json);
  /* Expired: queued for years. */
  mk_message_t stale;
  assert(mk_make_start(&stale, type1, 1, "B-61") == MK_OK);
  strcpy(stale.ts_utc, "2020-01-01T00:00:00Z");
  assert(mk_send(sched_bus, &stale) == MK_OK);

  size_t from = n_statuses;
  mk_message_t good;
  send_start(&good, type1, 1, "B-62");
  assert(wait_status(type1, 1, MK_RUNNING, "B-62", from, 30) >= 0);
  assert(m1.received == 1);
  assert(executions[59] == 0 && executions[60] == 0 && executions[61] == 0 &&
         executions[62] == 1);
  machine_finish(&m1);
  machine_close(&m1);
}

int main(void) {
  /* Print each scenario as it passes, also when stdout is a pipe (docker run
     without -t): otherwise the run looks stuck for ~35 s. */
  setvbuf(stdout, NULL, _IOLBF, 0);
  brokers = getenv("KAFKA_BROKERS");
  if (brokers == NULL)
    brokers = "localhost:9092";
  printf("bus test against %s (7 scenarios, about 35 s)\n", brokers);
  long run = (long)time(NULL);
  snprintf(type1, sizeof(type1), "mktest%ld", run);
  snprintf(type2, sizeof(type2), "mktestnew%ld", run);
  snprintf(group1, sizeof(group1), "%s-1", type1);

  char errstr[256];
  rd_kafka_conf_t *conf = rd_kafka_conf_new();
  assert(rd_kafka_conf_set(conf, "bootstrap.servers", brokers, errstr,
                           sizeof(errstr)) == RD_KAFKA_CONF_OK);
  admin = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
  assert(admin != NULL);
  admin_topic(type1, 1);
  admin_topic(type2, 1);

  /* Scenario 7 runs in the background: librdkafka fails messages to a
   * missing topic after topic.metadata.propagation.max.ms (30 s). */
  char orphan_type[MK_NAME_LEN];
  snprintf(orphan_type, sizeof(orphan_type), "mkmissing%ld", run);
  mk_config_t orphan_cfg = {.brokers = brokers,
                            .client_id = "orphan",
                            .on_delivery_failure = on_delivery_failure};
  orphan_bus = mk_open(&orphan_cfg);
  mk_message_t idle;
  assert(mk_make_idle(&idle, orphan_type, 1) == MK_OK);
  assert(mk_send(orphan_bus, &idle) == MK_OK);

  /* The scheduler: the statuses of both test machine types (a real
   * scheduler uses {MK_MACHINE_TYPES}); starts at the beginning. */
  char sched_group[64];
  snprintf(sched_group, sizeof(sched_group), "scheduler-%ld", run);
  const char *sched_topics[] = {type1, type2};
  mk_config_t sched_cfg = {.brokers = brokers,
                           .client_id = "scheduler",
                           .group_id = sched_group,
                           .topics = sched_topics,
                           .n_topics = 2,
                           .receive = MK_RECEIVE_STATUSES};
  sched_bus = mk_open(&sched_cfg);
  assert(sched_bus != NULL);

  scenario_happy_path();
  scenario_duplicate_start();
  scenario_restart();
  scenario_reused_id();
  scenario_second_type();
  scenario_bad_messages();

  puts("7. delivery to a missing topic reaches the callback");
  for (time_t end = time(NULL) + 60; orphan_failures == 0 && time(NULL) < end;)
    assert(mk_poll(orphan_bus, 200, NULL, NULL) >= 0);
  assert(orphan_failures == 1 && mk_delivery_failures(orphan_bus) == 1);
  assert(mk_close(orphan_bus, 1000) == 0);

  assert(mk_delivery_failures(sched_bus) == 0);
  assert(mk_close(sched_bus, 5000) == 0);
  admin_topic(type1, 0);
  admin_topic(type2, 0);
  rd_kafka_destroy(admin);
  char path[MK_PATH_LEN];
  snprintf(path, sizeof(path), "build/test-bus-%s-1.state", type1);
  remove(path);
  printf("machine_kafka: bus test passed against %s (%s, %s)\n", brokers,
         type1, type2);
  return 0;
}
