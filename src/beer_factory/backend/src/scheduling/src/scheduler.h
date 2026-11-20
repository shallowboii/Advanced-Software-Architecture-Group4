#pragma once

/*
 * Kafka side of the scheduler.
 *
 * The scheduler reads the statuses of every machine type (one topic per type,
 * MK_MACHINE_TYPES), keeps one row per machine instance (type + id) in a
 * table, and calls dispatch() after every status. dispatch() (dispatch.c) is
 * where the scheduling policy goes; it sends work with scheduler_start /
 * scheduler_stop / scheduler_flush.
 *
 * Consumer group "scheduler": a restart resumes after the last committed
 * status; the very first start reads the topics from the beginning.
 */

#include "machine_kafka.h"

#include <stddef.h>

#define SCHEDULER_MAX_MACHINES 64

/* What the scheduler knows about one machine instance. */
typedef struct {
  char type[MK_NAME_LEN];         /* machine_type = topic, e.g. "boiling" */
  int id;                         /* machine_id */
  mk_type_t state;                /* MK_IDLE, MK_RUNNING, MK_DONE or MK_ERROR */
  char batch_id[MK_BATCH_LEN];    /* batch of the last status ("" for idle) */
  char error_code[MK_NAME_LEN];   /* error only */
  char error_reason[MK_REASON_LEN];
  char last_ts_utc[MK_TS_LEN];    /* ts_utc of the last status */
} machine_t;

typedef struct {
  machine_t machines[SCHEDULER_MAX_MACHINES]; /* in order of first status */
  size_t count;
} machine_table_t;

/* ---- Machine table (no Kafka) ------------------------------------------- */

/* Stores a status in the row of its machine (added on its first status).
 * Returns that row, or NULL for a command or when the table is full. */
const machine_t *machine_table_apply(machine_table_t *table,
                                     const mk_message_t *status);

/* The row of type + id, or NULL if no status was seen from it yet. */
const machine_t *machine_table_find(const machine_table_t *table,
                                    const char *type, int id);

/* ---- Scheduler (Kafka) --------------------------------------------------- */

typedef struct scheduler scheduler_t;

/* Connects to brokers (e.g. "kafka:29092") and subscribes to every machine
 * type. NULL on error (printed to stderr). */
scheduler_t *scheduler_open(const char *brokers);

/* Waits up to timeout_ms for statuses, updates the table and calls dispatch()
 * for each. Returns the number handled or a negative mk_result_t
 * (MK_ERR_FATAL: close and open again). */
int scheduler_poll(scheduler_t *s, int timeout_ms);

/* Commands. message_id and ts_utc are filled in by the library. */
mk_result_t scheduler_start(scheduler_t *s, const char *type, int id,
                            const char *batch_id, const mk_param_t *params,
                            size_t n_params);
mk_result_t scheduler_stop(scheduler_t *s, const char *type, int id,
                           const char *batch_id /* NULL = whatever runs */);
mk_result_t scheduler_flush(scheduler_t *s, const char *type, int id,
                            const char *batch_id);

const machine_table_t *scheduler_machines(const scheduler_t *s);

/* Sends what is still queued (up to 5 s) and closes. */
void scheduler_close(scheduler_t *s);

/* ---- Policy (dispatch.c) ------------------------------------------------- */

/* Called after every status, with the row that just changed. */
void dispatch(scheduler_t *s, const machine_t *changed);
