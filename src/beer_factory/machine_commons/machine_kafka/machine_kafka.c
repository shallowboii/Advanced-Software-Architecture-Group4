#include "machine_kafka.h"

#include <stdio.h>
#include <string.h>

/* Called from rd_kafka_poll()/rd_kafka_flush() once per sent message. */
static void dr_msg_cb(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage,
                      void *opaque) {
  (void)rk;
  (void)opaque;
  if (rkmessage->err) {
    fprintf(stderr, "%% Message delivery failed: %s\n",
            rd_kafka_err2str(rkmessage->err));
    // TODO(marton): what to do when a delivery fails for good
  }
}

rd_kafka_t *mk_producer_new(const char *brokers) {
  char errstr[MK_ERRSTR_LEN];
  rd_kafka_conf_t *conf = rd_kafka_conf_new();

  // TODO: error handling
  rd_kafka_conf_set(conf, "bootstrap.servers", brokers, errstr,
                    sizeof(errstr));
  /* Retries can't duplicate or reorder messages. */
  rd_kafka_conf_set(conf, "enable.idempotence", "true", errstr,
                    sizeof(errstr));
  /* No batching: send every message right away. */
  rd_kafka_conf_set(conf, "linger.ms", "0", errstr, sizeof(errstr));
  rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);

  /* rd_kafka_new() takes ownership of conf. */
  rd_kafka_t *rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
  // TODO: error handling
  return rk;
}

int mk_send(rd_kafka_t *rk, const char *topic, const mk_message_t *msg) {
  /* Key = machine id, so one machine's messages stay on one partition. */
  char key[MK_KEY_LEN];
  int key_len = snprintf(key, sizeof(key), "%d", msg->machine_id);

  // TODO(marton): payload format (JSON fields); for now the raw struct
  rd_kafka_resp_err_t err = rd_kafka_producev(
      rk, RD_KAFKA_V_TOPIC(topic), RD_KAFKA_V_KEY(key, (size_t)key_len),
      RD_KAFKA_V_VALUE((void *)msg, sizeof(*msg)),
      RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY), RD_KAFKA_V_END);
  (void)err; // TODO: error handling

  /* Serve delivery reports of earlier messages. */
  rd_kafka_poll(rk, 0);
  return 0;
}

void mk_producer_close(rd_kafka_t *rk) {
  /* Wait for the final messages to be delivered or fail. */
  rd_kafka_flush(rk, MK_FLUSH_TIMEOUT_MS);
  // TODO: error handling (rd_kafka_outq_len() > 0: messages not delivered)
  rd_kafka_destroy(rk);
}

rd_kafka_t *mk_consumer_new(const char *brokers, const char *group_id,
                            const char *topic) {
  char errstr[MK_ERRSTR_LEN];
  rd_kafka_conf_t *conf = rd_kafka_conf_new();

  // TODO: error handling
  rd_kafka_conf_set(conf, "bootstrap.servers", brokers, errstr,
                    sizeof(errstr));
  rd_kafka_conf_set(conf, "group.id", group_id, errstr, sizeof(errstr));
  /* At-least-once: mk_poll commits only after the message was handled. */
  rd_kafka_conf_set(conf, "enable.auto.commit", "false", errstr,
                    sizeof(errstr));

  /* rd_kafka_new() takes ownership of conf. */
  rd_kafka_t *rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
  // TODO: error handling

  /* All partitions feed the main queue, read by rd_kafka_consumer_poll(). */
  rd_kafka_poll_set_consumer(rk);

  rd_kafka_topic_partition_list_t *subscription =
      rd_kafka_topic_partition_list_new(1);
  rd_kafka_topic_partition_list_add(subscription, topic, RD_KAFKA_PARTITION_UA);
  rd_kafka_resp_err_t err = rd_kafka_subscribe(rk, subscription);
  (void)err; // TODO: error handling
  rd_kafka_topic_partition_list_destroy(subscription);
  return rk;
}

int mk_poll(rd_kafka_t *rk, mk_handler_fn handler, void *opaque) {
  rd_kafka_message_t *rkm = rd_kafka_consumer_poll(rk, MK_POLL_TIMEOUT_MS);
  if (!rkm)
    return 0; /* no message within the timeout */
  // TODO: error handling (rkm->err)

  // TODO(marton): payload format (JSON fields); for now the raw struct
  mk_message_t msg;
  memcpy(&msg, rkm->payload, sizeof(msg));
  handler(&msg, opaque);

  /* Commit only after handling: a crash before this redelivers the message. */
  // TODO(marton): duplicates are possible (at-least-once); decide handling
  rd_kafka_resp_err_t err = rd_kafka_commit_message(rk, rkm, 0 /* sync */);
  (void)err; // TODO: error handling
  rd_kafka_message_destroy(rkm);
  return 1;
}

void mk_consumer_close(rd_kafka_t *rk) {
  /* Leave the group, then free everything. */
  rd_kafka_consumer_close(rk);
  rd_kafka_destroy(rk);
}
