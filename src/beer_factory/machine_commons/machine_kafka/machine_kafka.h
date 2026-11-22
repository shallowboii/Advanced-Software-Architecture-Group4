#pragma once

/*
 * machine_kafka: the message bus between the scheduler and the machines.
 *
 * A thin wrapper around librdkafka. Every machine and the scheduler use it.
 *
 * How it works:
 *  - One topic per machine type, e.g. "machine.whirlpooling".
 *    Commands (scheduler -> machine) and statuses (machine -> scheduler)
 *    go on the same topic.
 *  - The message key is the machine id, so all messages for one machine
 *    stay in order.
 *  - Everyone also reads their own messages back. A machine only handles
 *    commands for its own id, the scheduler only handles statuses.
 *  - A message is marked as done only after it was handled, so after a
 *    crash it is delivered again (at-least-once).
 */

#include <librdkafka/rdkafka.h>
#include <stddef.h>

/* Text sizes, including the closing '\0' */
#define KAFKA_NAME_LEN   32   /* machine_type, error_code, param name */
#define KAFKA_ID_LEN     96   /* message_id */
#define KAFKA_BATCH_LEN  64   /* batch_id */
#define KAFKA_REASON_LEN 128  /* error_reason (free text) */
#define KAFKA_TS_LEN     32   /* ts_utc, e.g. 2026-11-18T12:00:00Z */
#define KAFKA_MAX_PARAMS 16   /* parameters of a start, e.g. duration_s */

#define KAFKA_ERROR_TEXT_LEN   512    /* room for a librdkafka error message */
#define KAFKA_KEY_LEN          16     /* machine id written as text */
#define KAFKA_POLL_TIMEOUT_MS  100    /* how long kafka_poll waits */
#define KAFKA_FLUSH_TIMEOUT_MS 10000  /* how long close waits for sending */

/* What a message is about */
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
  double value;
} kafka_param_t;

/* One message on the bus */
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

/* Your function that kafka_poll calls for every received message.
 * context is whatever you passed to kafka_poll (e.g. your machine). */
typedef void (*kafka_message_handler)(const kafka_message_t *msg,
                                      void *context);

/* ---- Sending ---- */

/* Create a producer that talks to brokers, e.g. "localhost:9092". */
rd_kafka_t *kafka_producer_new(const char *brokers);

/* Send msg on topic. The key is msg->machine_id. */
int kafka_send(rd_kafka_t *producer, const char *topic,
               const kafka_message_t *msg);

/* Wait until everything is sent, then free the producer. */
void kafka_producer_close(rd_kafka_t *producer);

/* ---- Receiving ---- */

/* Create a consumer that reads topic. Every machine needs its own group_id
 * (e.g. "whirlpooling-1"), otherwise machines share the messages. */
rd_kafka_t *kafka_consumer_new(const char *brokers, const char *group_id,
                               const char *topic);

/* Wait up to KAFKA_POLL_TIMEOUT_MS for one message and pass it to handler.
 * Kafka errors and messages of the wrong size are printed to stderr and
 * skipped. Returns 1 if a message was handled, 0 if not. */
int kafka_poll(rd_kafka_t *consumer, kafka_message_handler handler,
               void *context);

/* Leave the group and free the consumer. */
void kafka_consumer_close(rd_kafka_t *consumer);
