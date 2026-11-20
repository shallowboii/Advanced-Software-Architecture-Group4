#include "scheduler.h"

#include <stdio.h>
#include <stdlib.h>

struct scheduler {
  mk_bus_t *bus;
  machine_table_t table;
};

static void on_status(const mk_message_t *status, void *user) {
  scheduler_t *s = user;
  const machine_t *m = machine_table_apply(&s->table, status);
  if (m == NULL) {
    fprintf(stderr, "scheduler: machine table full, ignoring %s#%d\n",
            status->machine_type, status->machine_id);
    return;
  }
  printf("%s  %s#%d  %s%s%s%s%s\n", m->last_ts_utc, m->type, m->id,
         mk_type_name(m->state), m->batch_id[0] ? " " : "", m->batch_id,
         m->error_code[0] ? " " : "", m->error_code);
  dispatch(s, m);
}

scheduler_t *scheduler_open(const char *brokers) {
  static const char *const topics[] = {MK_MACHINE_TYPES};
  scheduler_t *s = calloc(1, sizeof(*s));
  if (s == NULL)
    return NULL;
  mk_config_t cfg = {.brokers = brokers,
                     .client_id = "scheduler",
                     .topics = topics,
                     .n_topics = sizeof(topics) / sizeof(topics[0]),
                     .receive = MK_RECEIVE_STATUSES};
  s->bus = mk_open(&cfg);
  if (s->bus == NULL) {
    free(s);
    return NULL;
  }
  return s;
}

int scheduler_poll(scheduler_t *s, int timeout_ms) {
  if (s == NULL)
    return MK_ERR_ARG;
  return mk_poll(s->bus, timeout_ms, on_status, s);
}

static mk_result_t send_command(scheduler_t *s, mk_message_t *cmd) {
  mk_result_t result = mk_send(s->bus, cmd);
  if (result == MK_OK)
    printf("sent %s %s to %s#%d\n", mk_type_name(cmd->type), cmd->batch_id,
           cmd->machine_type, cmd->machine_id);
  else
    fprintf(stderr, "scheduler: cannot send %s to %s#%d: %s\n",
            mk_type_name(cmd->type), cmd->machine_type, cmd->machine_id,
            mk_result_name(result));
  return result;
}

mk_result_t scheduler_start(scheduler_t *s, const char *type, int id,
                            const char *batch_id, const mk_param_t *params,
                            size_t n_params) {
  if (s == NULL || (n_params > 0 && params == NULL))
    return MK_ERR_ARG;
  mk_message_t start;
  mk_result_t result = mk_make_start(&start, type, id, batch_id);
  for (size_t i = 0; result == MK_OK && i < n_params; ++i)
    result = mk_param_set(&start, params[i].name, params[i].value);
  return result == MK_OK ? send_command(s, &start) : result;
}

mk_result_t scheduler_stop(scheduler_t *s, const char *type, int id,
                           const char *batch_id) {
  if (s == NULL)
    return MK_ERR_ARG;
  mk_message_t stop;
  mk_result_t result = mk_make_command(&stop, MK_STOP, type, id, batch_id);
  return result == MK_OK ? send_command(s, &stop) : result;
}

mk_result_t scheduler_flush(scheduler_t *s, const char *type, int id,
                            const char *batch_id) {
  if (s == NULL)
    return MK_ERR_ARG;
  mk_message_t flush;
  mk_result_t result = mk_make_command(&flush, MK_FLUSH, type, id, batch_id);
  return result == MK_OK ? send_command(s, &flush) : result;
}

const machine_table_t *scheduler_machines(const scheduler_t *s) {
  return s == NULL ? NULL : &s->table;
}

void scheduler_close(scheduler_t *s) {
  if (s == NULL)
    return;
  mk_close(s->bus, 5000);
  free(s);
}
