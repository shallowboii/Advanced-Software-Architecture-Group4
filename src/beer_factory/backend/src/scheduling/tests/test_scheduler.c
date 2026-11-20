/* Machine table of the scheduler: statuses in, rows out. No broker needed. */
#include "scheduler.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(cond)                                                            \
  do {                                                                         \
    if (!(cond)) {                                                             \
      fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond); \
      exit(EXIT_FAILURE);                                                      \
    }                                                                          \
  } while (0)

/* A status as the machine would send it (message_id/ts set like mk_send). */
static mk_message_t status(mk_type_t type, const char *machine_type, int id,
                           const char *batch) {
  mk_message_t start, out;
  CHECK(mk_make_start(&start, machine_type, id, batch ? batch : "B-0") ==
        MK_OK);
  if (type == MK_IDLE)
    CHECK(mk_make_idle(&out, machine_type, id) == MK_OK);
  else if (type == MK_RUNNING)
    CHECK(mk_make_running(&out, &start) == MK_OK);
  else if (type == MK_DONE)
    CHECK(mk_make_done(&out, &start) == MK_OK);
  else
    CHECK(mk_make_error(&out, &start, "INTERRUPTED", "restarted") == MK_OK);
  snprintf(out.ts_utc, sizeof(out.ts_utc), "2026-11-20T10:00:00Z");
  return out;
}

static void test_batch_lifecycle(void) {
  machine_table_t t = {0};
  mk_message_t s = status(MK_IDLE, "whirlpooling", 1, NULL);
  const machine_t *m = machine_table_apply(&t, &s);
  CHECK(m != NULL && t.count == 1);
  CHECK(strcmp(m->type, "whirlpooling") == 0 && m->id == 1);
  CHECK(m->state == MK_IDLE && m->batch_id[0] == '\0');
  CHECK(strcmp(m->last_ts_utc, "2026-11-20T10:00:00Z") == 0);

  s = status(MK_RUNNING, "whirlpooling", 1, "B-0001");
  m = machine_table_apply(&t, &s);
  CHECK(t.count == 1 && m->state == MK_RUNNING);
  CHECK(strcmp(m->batch_id, "B-0001") == 0);

  s = status(MK_DONE, "whirlpooling", 1, "B-0001");
  m = machine_table_apply(&t, &s);
  CHECK(m->state == MK_DONE && strcmp(m->batch_id, "B-0001") == 0);

  s = status(MK_IDLE, "whirlpooling", 1, NULL);
  m = machine_table_apply(&t, &s);
  CHECK(m->state == MK_IDLE && m->batch_id[0] == '\0');
  CHECK(machine_table_find(&t, "whirlpooling", 1) == m);
}

static void test_error_and_machines(void) {
  machine_table_t t = {0};
  mk_message_t s = status(MK_ERROR, "boiling", 2, "B-0002");
  const machine_t *m = machine_table_apply(&t, &s);
  CHECK(m->state == MK_ERROR && strcmp(m->error_code, "INTERRUPTED") == 0);
  CHECK(strcmp(m->error_reason, "restarted") == 0);

  /* Same type, other id; other type, same id: separate rows. */
  s = status(MK_IDLE, "boiling", 3, NULL);
  machine_table_apply(&t, &s);
  s = status(MK_IDLE, "mashing", 2, NULL);
  machine_table_apply(&t, &s);
  CHECK(t.count == 3);
  CHECK(machine_table_find(&t, "boiling", 2)->state == MK_ERROR);
  CHECK(machine_table_find(&t, "mashing", 2)->state == MK_IDLE);
  CHECK(machine_table_find(&t, "lautering", 1) == NULL);

  /* error_code is cleared by the next non-error status. */
  s = status(MK_IDLE, "boiling", 2, NULL);
  m = machine_table_apply(&t, &s);
  CHECK(m->state == MK_IDLE && m->error_code[0] == '\0');
}

static void test_rejects(void) {
  machine_table_t t = {0};
  mk_message_t cmd;
  CHECK(mk_make_start(&cmd, "boiling", 1, "B-1") == MK_OK);
  CHECK(machine_table_apply(&t, &cmd) == NULL && t.count == 0);
  CHECK(machine_table_apply(NULL, &cmd) == NULL);

  for (int id = 1; id <= SCHEDULER_MAX_MACHINES; ++id) {
    mk_message_t s = status(MK_IDLE, "filtering", id, NULL);
    CHECK(machine_table_apply(&t, &s) != NULL);
  }
  mk_message_t s = status(MK_IDLE, "filtering", SCHEDULER_MAX_MACHINES + 1,
                          NULL);
  CHECK(machine_table_apply(&t, &s) == NULL);
  s = status(MK_RUNNING, "filtering", 1, "B-9"); /* known row still updates */
  CHECK(machine_table_apply(&t, &s) != NULL);
}

int main(void) {
  test_batch_lifecycle();
  test_error_and_machines();
  test_rejects();
  printf("scheduler: all machine table tests passed\n");
  return 0;
}
