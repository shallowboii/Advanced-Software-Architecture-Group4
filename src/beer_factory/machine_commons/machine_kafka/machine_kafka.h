#pragma once

/*
 * Kafka production bus shared by every machine and the scheduler.
 *
 * - One topic per machine type ("boiling", "whirlpooling", ...). The topic of
 *   a message is its machine_type, and its key is the machine_id, so all
 *   messages for one machine instance stay in order.
 * - Three message types: start (scheduler -> machine), done and error
 *   (machine -> scheduler). Payloads are JSON (see README.md).
 * - Idempotent producer, no batching (linger.ms=0), offsets committed only
 *   after the handler returns (at-least-once). Use mk_dedup_* to ignore
 *   redelivered messages, which makes processing effectively exactly once.
 *
 * librdkafka and cJSON types are kept out of this header.
 */

#include <stddef.h>

#define MK_SCHEMA_VERSION 1

/* Buffer sizes include the terminating NUL. */
#define MK_NAME_LEN 32    /* machine_type, error_code */
#define MK_ID_LEN 96      /* message_id, reply_to */
#define MK_BATCH_LEN 64   /* batch_id */
#define MK_REASON_LEN 128 /* error_reason */
#define MK_TS_LEN 32      /* ts_utc, e.g. 2026-11-18T12:00:00Z */

/* Largest encoded message; mk_encode needs a buffer at least this big to be
 * sure every valid message fits. */
#define MK_MAX_JSON_LEN 1024

typedef enum { MK_START, MK_DONE, MK_ERROR } mk_type_t;

typedef enum {
  MK_OK = 0,
  MK_ERR_ARG = -1,    /* NULL pointer, bad config or string too long */
  MK_ERR_ENCODE = -2, /* message invalid or buffer too small */
  MK_ERR_DECODE = -3, /* not a valid bus message */
  MK_ERR_KAFKA = -4,  /* broker/client error; the call may be retried */
  MK_ERR_FATAL = -5   /* client unusable; mk_close and mk_open again */
} mk_result_t;

/*
 * Empty strings are sent as JSON null and decoded back to empty strings.
 * Required fields:
 *   all:   machine_type, machine_id >= 1, message_id
 *   start: batch_id
 *   done:  reply_to
 *   error: reply_to, error_code
 * mk_send fills in message_id and ts_utc when they are empty.
 */
typedef struct {
  mk_type_t type;
  char machine_type[MK_NAME_LEN];
  int machine_id;
  char batch_id[MK_BATCH_LEN];
  char message_id[MK_ID_LEN];
  char reply_to[MK_ID_LEN];
  char ts_utc[MK_TS_LEN];
  char error_code[MK_NAME_LEN];
  char error_reason[MK_REASON_LEN];
} mk_message_t;

/* ---- Messages (no broker needed) ------------------------------------- */

const char *mk_type_name(mk_type_t type);
const char *mk_result_name(int result);

/* Start for machine_type/machine_id with batch_id. */
mk_result_t mk_make_start(mk_message_t *out, const char *machine_type,
                          int machine_id, const char *batch_id);

/* Done / error reply to a received start (copies machine, batch, reply_to). */
mk_result_t mk_make_done(mk_message_t *out, const mk_message_t *start);
mk_result_t mk_make_error(mk_message_t *out, const mk_message_t *start,
                          const char *code, const char *reason);

/* JSON encoding. Returns the length written (without NUL) or a negative
 * mk_result_t. */
int mk_encode(const mk_message_t *msg, char *buf, size_t buf_len);

/* Parses and validates one message. Unknown extra fields are ignored so that
 * newer senders stay readable. */
mk_result_t mk_decode(const char *json, size_t len, mk_message_t *out);

/* ---- Duplicate detection --------------------------------------------- */

/* Remembers the last MK_DEDUP_SIZE message ids (in memory only). */
#define MK_DEDUP_SIZE 32

typedef struct {
  char ids[MK_DEDUP_SIZE][MK_ID_LEN];
  size_t next;
} mk_dedup_t;

void mk_dedup_init(mk_dedup_t *dedup);

/* Returns 1 if message_id was seen before, otherwise records it and returns
 * 0. */
int mk_dedup_seen(mk_dedup_t *dedup, const char *message_id);

/* ---- Bus (librdkafka) ------------------------------------------------- */

typedef struct mk_bus mk_bus_t;

typedef struct {
  const char *brokers;    /* "localhost:9092" */
  const char *client_id;  /* unique per process, e.g. "boiling-3" */
  const char *group_id;   /* consumer group; NULL = client_id */
  const char *const *topics; /* topics to consume; may be empty */
  size_t n_topics;
  const char *offset_reset; /* "earliest" (NULL) or "latest" */
} mk_config_t;

typedef void (*mk_handler_fn)(const mk_message_t *msg, void *user);

/* Creates the producer and consumer. Returns NULL on error (printed to
 * stderr). Connecting happens in the background, so a broker that is down is
 * not an error here. */
mk_bus_t *mk_open(const mk_config_t *config);

/*
 * Queues msg on topic msg->machine_type with key machine_id. Fills in
 * message_id and ts_utc if they are empty, so the caller can read the id
 * afterwards (e.g. to match replies). Delivery happens in the background and
 * is retried until the broker accepts it.
 */
mk_result_t mk_send(mk_bus_t *bus, mk_message_t *msg);

/*
 * Waits up to timeout_ms for messages and calls handler for each valid one,
 * then commits its offset. Malformed messages are reported and skipped. Also
 * serves producer delivery reports. Returns the number of messages handled or
 * a negative mk_result_t (only MK_ERR_ARG or MK_ERR_FATAL).
 */
int mk_poll(mk_bus_t *bus, int timeout_ms, mk_handler_fn handler, void *user);

/* Messages sent but not yet confirmed by the broker. */
int mk_outstanding(mk_bus_t *bus);

/* Messages the broker permanently rejected (should stay 0). */
unsigned long mk_delivery_failures(mk_bus_t *bus);

/* Waits up to timeout_ms for outstanding messages, then closes everything.
 * Returns the number of messages that were still not delivered. */
int mk_close(mk_bus_t *bus, int timeout_ms);
