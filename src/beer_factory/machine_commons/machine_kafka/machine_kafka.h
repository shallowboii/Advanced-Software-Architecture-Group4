#pragma once

/*
 * Kafka production bus shared by every machine and the scheduler.
 *
 * - One topic per machine type, named after the type: "mashing", "boiling",
 *   ... (MK_MACHINE_TYPES). Commands (scheduler -> machines: start, stop,
 *   flush) and statuses (machines -> scheduler: idle, running, done, error)
 *   share it. The key is the machine_id, so all messages of one machine
 *   instance stay in order.
 * - mk_poll hands a machine only commands and the scheduler only statuses
 *   (mk_config_t.receive); the other direction is skipped.
 * - Payloads are JSON (schema in docs/kafka-design.md).
 * - Idempotent producer, no batching (linger.ms=0), offsets committed only
 *   after the handler returns (at-least-once delivery).
 * - Delivery is at-least-once, NOT exactly-once. What is guaranteed is that a
 *   machine that uses mk_ledger_* never runs the same batch twice, even
 *   across scheduler and machine restarts. Duplicate starts are answered
 *   with the last known status instead.
 *
 * librdkafka and cJSON types are kept out of this header. All calls on one
 * mk_bus_t must come from the same thread.
 */

#include <stddef.h>

#define MK_SCHEMA_VERSION 3

/* Buffer sizes include the terminating NUL. */
#define MK_NAME_LEN 32    /* machine_type (= topic), error_code, param name */
#define MK_ID_LEN 96      /* message_id */
#define MK_BATCH_LEN 64   /* batch_id */
#define MK_REASON_LEN 128 /* error_reason (free text) */
#define MK_TS_LEN 32      /* ts_utc, e.g. 2026-11-18T12:00:00Z */
#define MK_PATH_LEN 256   /* ledger file path */

/* Recipe parameters carried by a start (name -> number). */
#define MK_MAX_PARAMS 16

/* Largest encoded message. Identifiers are restricted to characters that JSON
 * never escapes, so this bound holds for every valid message (checked by
 * tests/test_message.c). */
#define MK_MAX_JSON_LEN 4096

/* Every machine type, which is also its topic name. For the scheduler:
 *   const char *topics[] = {MK_MACHINE_TYPES}; */
#define MK_MACHINE_TYPES                                                       \
  "mashing", "lautering", "boiling", "whirlpooling", "fermentation",           \
      "filtering"

/* Commands older than this (by ts_utc) are dropped by mk_poll. */
#define MK_DEFAULT_COMMAND_MAX_AGE_S 300

typedef enum {
  /* Commands: scheduler -> machine */
  MK_START,
  MK_STOP,  /* stop the machine (FR-13); may be answered with UNSUPPORTED */
  MK_FLUSH, /* discard the batch in the machine (FR-14); may be UNSUPPORTED */
  /* Statuses: machine -> scheduler */
  MK_IDLE,
  MK_RUNNING,
  MK_DONE,
  MK_ERROR
} mk_type_t;

typedef enum {
  MK_OK = 0,
  MK_ERR_ARG = -1,    /* NULL pointer, bad config or string too long */
  MK_ERR_ENCODE = -2, /* message invalid or buffer too small */
  MK_ERR_DECODE = -3, /* not a valid bus message */
  MK_ERR_KAFKA = -4,  /* broker/client error; the call may be retried */
  MK_ERR_FATAL = -5,  /* client unusable; mk_close and mk_open again */
  MK_ERR_IO = -6      /* ledger file could not be read or written */
} mk_result_t;

typedef struct {
  char name[MK_NAME_LEN];
  double value; /* finite */
} mk_param_t;

/*
 * Empty strings are sent as JSON null and decoded back to empty strings.
 *
 * Identifiers (message_id, batch_id, ts_utc, error_code, param names) may
 * only contain [A-Za-z0-9._:-]. machine_type may only contain [a-z0-9_-] and
 * must start with a letter, because it is the topic name. error_reason is
 * free text.
 *
 * Required fields (besides machine_type, machine_id >= 1 and message_id):
 *   start:   batch_id          params optional
 *   stop:    -                 batch_id optional
 *   flush:   batch_id
 *   idle:    -
 *   running: batch_id
 *   done:    batch_id
 *   error:   error_code        batch_id optional
 * mk_send fills in message_id and ts_utc when they are empty.
 */
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

/* ---- Messages (no broker needed) ------------------------------------- */

const char *mk_type_name(mk_type_t type);
const char *mk_result_name(int result);

/* 1 for start/stop/flush, 0 for statuses. */
int mk_is_command(mk_type_t type);

/* Any command. batch_id may be NULL where it is optional. */
mk_result_t mk_make_command(mk_message_t *out, mk_type_t type,
                            const char *machine_type, int machine_id,
                            const char *batch_id);

/* Start of batch_id on machine_type/machine_id. Add recipe parameters with
 * mk_param_set. */
mk_result_t mk_make_start(mk_message_t *out, const char *machine_type,
                          int machine_id, const char *batch_id);

/* Adds or replaces a parameter on a start. MK_ERR_ARG if the name is invalid,
 * the value is not finite or MK_MAX_PARAMS are already set. */
mk_result_t mk_param_set(mk_message_t *msg, const char *name, double value);

/* Returns 1 and sets *value if the parameter exists, otherwise 0. */
int mk_param_get(const mk_message_t *msg, const char *name, double *value);

/* Machine is up and ready for a new batch. */
mk_result_t mk_make_idle(mk_message_t *out, const char *machine_type,
                         int machine_id);

/* Statuses answering a received start (same machine and batch). */
mk_result_t mk_make_running(mk_message_t *out, const mk_message_t *start);
mk_result_t mk_make_done(mk_message_t *out, const mk_message_t *start);

/* Error answering any received command (same machine and batch). */
mk_result_t mk_make_error(mk_message_t *out, const mk_message_t *command,
                          const char *code, const char *reason);

/* JSON encoding. Returns the length written (without NUL) or a negative
 * mk_result_t. */
int mk_encode(const mk_message_t *msg, char *buf, size_t buf_len);

/* Parses and validates one message. Unknown extra fields are ignored so that
 * newer senders stay readable. */
mk_result_t mk_decode(const char *json, size_t len, mk_message_t *out);

/* ---- Ledger: never run the same batch twice -------------------------- */

/*
 * Remembers the last MK_LEDGER_SIZE batches this machine accepted and stores
 * them in a small text file so they survive a restart. For every start
 * addressed to this machine:
 *
 *   mk_ledger_lookup  -> 1: already answered; send *resend instead of running
 *   mk_ledger_refuse  -> start not run (BUSY, bad params): send the error
 *   mk_ledger_accept  -> persisted; only now actuate anything
 *   mk_ledger_record  -> with the done/error of an accepted batch, before
 *                        mk_send
 *
 * Batches that were running when the process stopped are reported as error
 * INTERRUPTED after a restart and are never run again automatically.
 */
#define MK_LEDGER_SIZE 16

typedef struct {
  mk_type_t state; /* MK_RUNNING, MK_DONE or MK_ERROR */
  int refused;     /* start answered with an error without running it */
  int unreported;  /* interrupted by a restart, not yet sent (not stored) */
  char batch_id[MK_BATCH_LEN];
  char command_id[MK_ID_LEN]; /* message_id of the accepted/refused start */
  char error_code[MK_NAME_LEN];
  char error_reason[MK_REASON_LEN];
} mk_ledger_entry_t;

typedef struct {
  char path[MK_PATH_LEN]; /* empty: memory only (tests) */
  char machine_type[MK_NAME_LEN];
  int machine_id;
  mk_ledger_entry_t entries[MK_LEDGER_SIZE]; /* oldest first */
  size_t count;
} mk_ledger_t;

/* Loads path (a missing file is an empty ledger). path NULL or "" keeps the
 * ledger in memory only. MK_ERR_ARG if the file belongs to another machine,
 * MK_ERR_IO if it cannot be read or parsed: refuse to start the machine
 * rather than risk running a batch twice. */
mk_result_t mk_ledger_open(mk_ledger_t *ledger, const char *path,
                           const char *machine_type, int machine_id);

/* For a start addressed to this machine: returns 1 and fills *resend with the
 * last status (running, done or error) if the batch was accepted before or
 * this very message was refused before; 0 if it is new; or a negative
 * mk_result_t (e.g. MK_ERR_ARG for a start addressed to another machine). */
int mk_ledger_lookup(const mk_ledger_t *ledger, const mk_message_t *start,
                     mk_message_t *resend);

/* Records the start as running and writes the file. Only actuate after
 * MK_OK. MK_ERR_ARG if the batch is already in the ledger. */
mk_result_t mk_ledger_accept(mk_ledger_t *ledger, const mk_message_t *start);

/* Answers a new start with an error without running it (e.g. code "BUSY")
 * and fills *status with that error, ready for mk_send. The refusal is
 * remembered by the start's message_id: a redelivery of that same message
 * gets the same error, while a new start for the batch (new message_id) is
 * evaluated again. */
mk_result_t mk_ledger_refuse(mk_ledger_t *ledger, const mk_message_t *start,
                             const char *code, const char *reason,
                             mk_message_t *status);

/* Stores the done or error status of an accepted batch. Other statuses are
 * accepted and ignored. */
mk_result_t mk_ledger_record(mk_ledger_t *ledger, const mk_message_t *status);

/* After mk_ledger_open: returns 1 and fills *status with an error
 * INTERRUPTED for a batch that was running when the process stopped, until
 * none are left (then 0). Send these once the bus is ready. */
int mk_ledger_next_interrupted(mk_ledger_t *ledger, mk_message_t *status);

/* ---- Bus (librdkafka) ------------------------------------------------- */

typedef struct mk_bus mk_bus_t;

/* Which messages mk_poll hands to the handler. */
typedef enum {
  MK_RECEIVE_COMMANDS = 0, /* machines */
  MK_RECEIVE_STATUSES = 1  /* the scheduler */
} mk_receive_t;

/* Called from mk_poll/mk_close for a message the broker permanently rejected.
 * msg is NULL if the payload could not be decoded. */
typedef void (*mk_delivery_fn)(const mk_message_t *msg, const char *reason,
                               void *user);

typedef struct {
  const char *brokers;   /* "localhost:9092" */
  const char *client_id; /* unique per process, e.g. "boiling-3" */
  const char *group_id;  /* consumer group; NULL = client_id */
  /* Topics (= machine types) to consume; may be empty. A machine reads its
   * own type, the scheduler all of them (MK_MACHINE_TYPES). */
  const char *const *topics;
  size_t n_topics;
  mk_receive_t receive;
  /* Where a consumer group without a committed offset starts: "latest" or
   * "earliest". NULL = latest for machines (never replay old commands),
   * earliest for the scheduler (never miss a status). */
  const char *offset_reset;
  /* Received commands older than this are dropped by mk_poll.
   * 0 = MK_DEFAULT_COMMAND_MAX_AGE_S, < 0 = never. */
  int command_max_age_s;
  mk_delivery_fn on_delivery_failure; /* optional */
  void *delivery_user;
} mk_config_t;

typedef void (*mk_handler_fn)(const mk_message_t *msg, void *user);

/* Creates the producer and consumer. Returns NULL on error (printed to
 * stderr). Connecting happens in the background, so a broker that is down is
 * not an error here. */
mk_bus_t *mk_open(const mk_config_t *config);

/* 1 once the consumer owns its partitions and knows where to start reading,
 * so a command sent from now on will be received. A machine sends its first
 * idle only after this. Always 1 for a bus without topics. */
int mk_ready(mk_bus_t *bus);

/*
 * Queues msg on the topic msg->machine_type with key machine_id. Fills in
 * message_id and ts_utc if they are empty, so the caller can read the id
 * afterwards. Delivery happens in the background and is retried until the
 * broker accepts it.
 */
mk_result_t mk_send(mk_bus_t *bus, mk_message_t *msg);

/*
 * Waits up to timeout_ms for messages and calls handler for each valid one
 * of the configured direction, then commits its offset. Malformed messages,
 * messages on the wrong topic and expired commands are reported and skipped;
 * messages of the other direction are skipped silently. Also serves producer
 * delivery reports. Returns the number of messages handled or a negative
 * mk_result_t (only MK_ERR_ARG or MK_ERR_FATAL).
 */
int mk_poll(mk_bus_t *bus, int timeout_ms, mk_handler_fn handler, void *user);

/* Messages sent but not yet confirmed by the broker. */
int mk_outstanding(mk_bus_t *bus);

/* Messages the broker permanently rejected (should stay 0). */
unsigned long mk_delivery_failures(mk_bus_t *bus);

/* Waits up to timeout_ms for outstanding messages, then closes everything.
 * Returns the number of messages that were still not delivered. */
int mk_close(mk_bus_t *bus, int timeout_ms);
