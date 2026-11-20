#include "scheduler.h"

#include <string.h>

static int find_index(const machine_table_t *table, const char *type, int id) {
  for (size_t i = 0; i < table->count; ++i) {
    const machine_t *m = &table->machines[i];
    if (m->id == id && strcmp(m->type, type) == 0)
      return (int)i;
  }
  return -1;
}

const machine_t *machine_table_find(const machine_table_t *table,
                                    const char *type, int id) {
  if (table == NULL || type == NULL)
    return NULL;
  int i = find_index(table, type, id);
  return i < 0 ? NULL : &table->machines[i];
}

const machine_t *machine_table_apply(machine_table_t *table,
                                     const mk_message_t *status) {
  if (table == NULL || status == NULL || mk_is_command(status->type))
    return NULL;
  int i = find_index(table, status->machine_type, status->machine_id);
  machine_t *m;
  if (i >= 0) {
    m = &table->machines[i];
  } else {
    if (table->count == SCHEDULER_MAX_MACHINES)
      return NULL;
    m = &table->machines[table->count++];
    memset(m, 0, sizeof(*m));
    memcpy(m->type, status->machine_type, sizeof(m->type));
    m->id = status->machine_id;
  }
  /* All fields have the same sizes as in mk_message_t. */
  m->state = status->type;
  memcpy(m->batch_id, status->batch_id, sizeof(m->batch_id));
  memcpy(m->error_code, status->error_code, sizeof(m->error_code));
  memcpy(m->error_reason, status->error_reason, sizeof(m->error_reason));
  memcpy(m->last_ts_utc, status->ts_utc, sizeof(m->last_ts_utc));
  return m;
}
