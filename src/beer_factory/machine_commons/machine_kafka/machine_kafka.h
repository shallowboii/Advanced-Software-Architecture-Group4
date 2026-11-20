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
#define KAFKA_NAME_LEN 32    /* machine_type, error_code, param name */
#define KAFKA_ID_LEN 96      /* message_id */
#define KAFKA_BATCH_LEN 64   /* batch_id */
#define KAFKA_REASON_LEN 128 /* error_reason (free text) */
#define KAFKA_TS_LEN 32      /* ts_utc, e.g. 2026-11-18T12:00:00Z */

/* Parameters carried by a start (name -> number), e.g. duration_s. */
#define KAFKA_MAX_PARAMS 16

#define KAFKA_ERRSTR_LEN 512         /* librdkafka error string buffer */
#define KAFKA_KEY_LEN 16             /* machine_id as decimal text */
#define KAFKA_POLL_TIMEOUT_MS 100    /* how long kafka_poll waits */
#define KAFKA_FLUSH_TIMEOUT_MS 10000 /* how long close waits for deliveries */

typedef enum {
  /* Commands: scheduler -> machine */
  KAFKA_START,
  KAFKA_STOP,  /* stop the machine (FR-13); may be answered with UNSUPPORTED */
  KAFKA_FLUSH, /* discard the batch in the machine (FR-14); may be
                  UNSUPPORTED */
  /* Statuses: machine -> scheduler */
  KAFKA_IDLE,
  KAFKA_RUNNING,
  KAFKA_ERROR
} kafka_type_t;

typedef struct {
  char name[KAFKA_NAME_LEN];
  double value; /* finite */
} kafka_param_t;

typedef struct {
  kafka_type_t type;
  char machine_type[KAFKA_NAME_LEN];
  int machine_id;
  char batch_id[KAFKA_BATCH_LEN];
  char message_id[KAFKA_ID_LEN];
  char ts_utc[KAFKA_TS_LEN];
  /* start only */
  kafka_param_t params[KAFKA_MAX_PARAMS];
  size_t n_params;
  /* error only */
  char error_code[KAFKA_NAME_LEN];
  char error_reason[KAFKA_REASON_LEN];
} kafka_message_t;

/* Called by kafka_poll for every received message. */
typedef void (*kafka_handler_fn)(const kafka_message_t *msg, void *opaque);

/* Producer */
rd_kafka_t *kafka_producer_new(const char *brokers);
int kafka_send(rd_kafka_t *rk, const char *topic, const kafka_message_t *msg);
void kafka_producer_close(rd_kafka_t *rk); /* flush + destroy */

/* Consumer */
rd_kafka_t *kafka_consumer_new(const char *brokers, const char *group_id,
                               const char *topic);
/* Waits up to KAFKA_POLL_TIMEOUT_MS for one message, hands it to handler, then
 * commits it. Returns 1 if a message was handled, 0 if none came. */
int kafka_poll(rd_kafka_t *rk, kafka_handler_fn handler, void *opaque);
void kafka_consumer_close(rd_kafka_t *rk); /* close + destroy */
