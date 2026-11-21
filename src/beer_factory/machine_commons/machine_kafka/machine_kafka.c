/*
 * machine_kafka: sending and receiving machine messages over Kafka.
 * See machine_kafka.h for how the topics and keys are used.
 *
 * Build (together with your program):
 *   gcc -I path/to/machine_kafka my_machine.c machine_kafka.c -lrdkafka
 */
#include "machine_kafka.h"

#include <stdio.h>
#include <string.h>

/* librdkafka calls this once for every sent message, to tell whether it
 * reached Kafka. It runs inside rd_kafka_poll() / rd_kafka_flush(). */
static void on_delivery_report(rd_kafka_t *producer,
                               const rd_kafka_message_t *report,
                               void *context)
{
  (void)producer; /* not used */
  (void)context;  /* not used */

  if (report->err) {
    fprintf(stderr, "Kafka: message could not be delivered: %s\n",
            rd_kafka_err2str(report->err));
    // TODO(marton): what to do when a delivery fails for good
  }
}

/* ---- Sending ---- */

rd_kafka_t *kafka_producer_new(const char *brokers)
{
  char error_text[KAFKA_ERROR_TEXT_LEN];
  rd_kafka_conf_t *config = rd_kafka_conf_new();

  /* 1. Settings */
  // TODO: error handling
  rd_kafka_conf_set(config, "bootstrap.servers", brokers,
                    error_text, sizeof(error_text));
  /* Kafka drops repeated sends, so a retry never duplicates or reorders */
  rd_kafka_conf_set(config, "enable.idempotence", "true",
                    error_text, sizeof(error_text));
  /* Send every message right away, don't wait to group them */
  rd_kafka_conf_set(config, "linger.ms", "0",
                    error_text, sizeof(error_text));
  rd_kafka_conf_set_dr_msg_cb(config, on_delivery_report);

  /* 2. Create the producer (it takes over the settings) */
  rd_kafka_t *producer = rd_kafka_new(RD_KAFKA_PRODUCER, config,
                                      error_text, sizeof(error_text));
  // TODO: error handling
  return producer;
}

int kafka_send(rd_kafka_t *producer, const char *topic,
               const kafka_message_t *msg)
{
  /* 1. Key = machine id, so one machine's messages stay in order */
  char key[KAFKA_KEY_LEN];
  snprintf(key, sizeof(key), "%d", msg->machine_id);

  /* 2. Queue the message; librdkafka sends it in the background.
   *    MSG_F_COPY: it keeps its own copy, so msg can be reused right away. */
  // TODO(marton): payload format (JSON fields); for now the raw struct
  // TODO: error handling
  rd_kafka_producev(producer,
                    RD_KAFKA_V_TOPIC(topic),
                    RD_KAFKA_V_KEY(key, strlen(key)),
                    RD_KAFKA_V_VALUE((void *)msg, sizeof(*msg)),
                    RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                    RD_KAFKA_V_END);

  /* 3. Handle delivery reports of earlier messages (doesn't wait) */
  rd_kafka_poll(producer, 0);
  return 0;
}

void kafka_producer_close(rd_kafka_t *producer)
{
  /* 1. Wait until the last messages are sent */
  rd_kafka_flush(producer, KAFKA_FLUSH_TIMEOUT_MS);
  // TODO: error handling (rd_kafka_outq_len() > 0: messages not delivered)

  /* 2. Free it */
  rd_kafka_destroy(producer);
}

/* ---- Receiving ---- */

rd_kafka_t *kafka_consumer_new(const char *brokers, const char *group_id,
                               const char *topic)
{
  char error_text[KAFKA_ERROR_TEXT_LEN];
  rd_kafka_conf_t *config = rd_kafka_conf_new();

  /* 1. Settings */
  // TODO: error handling
  rd_kafka_conf_set(config, "bootstrap.servers", brokers,
                    error_text, sizeof(error_text));
  rd_kafka_conf_set(config, "group.id", group_id,
                    error_text, sizeof(error_text));
  /* Don't mark messages as done by ourselves: kafka_poll does it after
   * the handler ran (at-least-once) */
  rd_kafka_conf_set(config, "enable.auto.commit", "false",
                    error_text, sizeof(error_text));

  /* 2. Create the consumer (it takes over the settings) */
  rd_kafka_t *consumer = rd_kafka_new(RD_KAFKA_CONSUMER, config,
                                      error_text, sizeof(error_text));
  // TODO: error handling

  /* 3. Read all partitions through one queue (rd_kafka_consumer_poll) */
  rd_kafka_poll_set_consumer(consumer);

  /* 4. Subscribe to the topic */
  rd_kafka_topic_partition_list_t *topics =
      rd_kafka_topic_partition_list_new(1);
  rd_kafka_topic_partition_list_add(topics, topic, RD_KAFKA_PARTITION_UA);
  // TODO: error handling
  rd_kafka_subscribe(consumer, topics);
  rd_kafka_topic_partition_list_destroy(topics);

  return consumer;
}

int kafka_poll(rd_kafka_t *consumer, kafka_message_handler handler,
               void *context)
{
  /* 1. Wait for the next message */
  rd_kafka_message_t *received =
      rd_kafka_consumer_poll(consumer, KAFKA_POLL_TIMEOUT_MS);
  if (received == NULL)
    return 0; /* nothing came in time */
  // TODO: error handling (received->err)

  /* 2. Hand it to the caller */
  // TODO(marton): payload format (JSON fields); for now the raw struct
  kafka_message_t msg;
  memcpy(&msg, received->payload, sizeof(msg));
  handler(&msg, context);

  /* 3. Only now mark it as done (commit). A crash before this line
   *    means the message is delivered again. 0 = wait for the commit. */
  // TODO(marton): duplicates are possible (at-least-once); decide handling
  // TODO: error handling
  rd_kafka_commit_message(consumer, received, 0);

  /* 4. Free it */
  rd_kafka_message_destroy(received);
  return 1;
}

void kafka_consumer_close(rd_kafka_t *consumer)
{
  /* 1. Leave the group */
  rd_kafka_consumer_close(consumer);

  /* 2. Free it */
  rd_kafka_destroy(consumer);
}
