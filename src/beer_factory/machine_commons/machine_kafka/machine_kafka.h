#pragma once

/*
 * Thin librdkafka wrapper shared by every machine and the scheduler.
 *
 * - One topic per machine type (e.g. "machine.whirlpooling"). Commands and
 *   statuses go on the same topic.
 * - The key is the machine id, so all messages for one machine stay in order.
 * - Everyone also reads its own messages back: a machine handles only
 *   commands (for its own id), the scheduler handles only statuses.
 * - Producer: idempotent, no batching. Consumer: at-least-once, the offset is
 *   committed after the handler returns.
 */

#include <librdkafka/rdkafka.h>
#include <stddef.h>

/* Buffer sizes include the terminating NUL. */
#define MK_NAME_LEN 32    /* machine_type, error_code, param name */
#define MK_ID_LEN 96      /* message_id */
#define MK_BATCH_LEN 64   /* batch_id */
#define MK_REASON_LEN 128 /* error_reason (free text) */
#define MK_TS_LEN 32      /* ts_utc, e.g. 2026-11-18T12:00:00Z */

/* Parameters carried by a start (name -> number), e.g. duration_s. */
#define MK_MAX_PARAMS 16

#define MK_ERRSTR_LEN 512         /* librdkafka error string buffer */
#define MK_KEY_LEN 16             /* machine_id as decimal text, fits any int */
#define MK_POLL_TIMEOUT_MS 100    /* how long mk_poll waits for a message */
#define MK_FLUSH_TIMEOUT_MS 10000 /* how long close waits for deliveries */

typedef enum {
  /* Commands: scheduler -> machine */
  MK_START,
  MK_STOP,  /* stop the machine (FR-13); may be answered with UNSUPPORTED */
  MK_FLUSH, /* discard the batch in the machine (FR-14); may be UNSUPPORTED */
  /* Statuses: machine -> scheduler */
  MK_IDLE,
  MK_RUNNING,
  MK_ERROR
} mk_type_t;

typedef struct {
  char name[MK_NAME_LEN];
  double value; /* finite */
} mk_param_t;

typedef struct {
  mk_type_t type;
  char machine_type[MK_NAME_LEN];
  int machine_id;
  char batch_id[MK_BATCH_LEN];
  char message_id[MK_ID_LEN];
  char ts_utc[MK_TS_LEN];
  /* start only */
  mk_param_t params[MK_MAX_PARAMS];
  size_t n_params;
  /* error only */
  char error_code[MK_NAME_LEN];
  char error_reason[MK_REASON_LEN];
} mk_message_t;

/* Called by mk_poll for every received message. */
typedef void (*mk_handler_fn)(const mk_message_t *msg, void *opaque);

/* Producer */
rd_kafka_t *mk_producer_new(const char *brokers);
int mk_send(rd_kafka_t *rk, const char *topic, const mk_message_t *msg);
void mk_producer_close(rd_kafka_t *rk); /* flush + destroy */

/* Consumer */
rd_kafka_t *mk_consumer_new(const char *brokers, const char *group_id,
                            const char *topic);
/* Waits up to MK_POLL_TIMEOUT_MS for one message, hands it to handler, then
 * commits it. Returns 1 if a message was handled, 0 if none came. */
int mk_poll(rd_kafka_t *rk, mk_handler_fn handler, void *opaque);
void mk_consumer_close(rd_kafka_t *rk); /* close + destroy */
