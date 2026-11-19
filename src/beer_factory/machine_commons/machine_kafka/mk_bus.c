#define _POSIX_C_SOURCE 200809L /* gmtime_r */

#include "machine_kafka.h"

#include <librdkafka/rdkafka.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* At most this many messages are handled per mk_poll call, so the machine
 * loop keeps running even when a backlog is waiting. */
#define MK_POLL_MAX_MESSAGES 64

struct mk_bus {
  rd_kafka_t *producer;
  rd_kafka_t *consumer; /* NULL when no topics are consumed */
  /* Leaves room for "-" + 20 digits of the sequence number in message_id. */
  char id_prefix[MK_ID_LEN - 22];
  unsigned long long next_seq;
  int fatal;
  unsigned long delivery_failures;
};

static int set(rd_kafka_conf_t *conf, const char *key, const char *value) {
  char errstr[256];
  if (rd_kafka_conf_set(conf, key, value, errstr, sizeof(errstr)) !=
      RD_KAFKA_CONF_OK) {
    fprintf(stderr, "kafka config %s=%s: %s\n", key, value, errstr);
    return 0;
  }
  return 1;
}

/* Broker-down and similar errors are transient: librdkafka reconnects by
 * itself. Only __FATAL makes the client unusable. */
static void error_cb(rd_kafka_t *rk, int err, const char *reason,
                     void *opaque) {
  mk_bus_t *bus = opaque;
  if (err == RD_KAFKA_RESP_ERR__FATAL) {
    char fatal[256];
    rd_kafka_fatal_error(rk, fatal, sizeof(fatal));
    fprintf(stderr, "kafka %s: FATAL: %s\n", rd_kafka_name(rk), fatal);
    bus->fatal = 1;
    return;
  }
  fprintf(stderr, "kafka %s: %s\n", rd_kafka_name(rk), reason);
}

static void delivery_cb(rd_kafka_t *rk, const rd_kafka_message_t *msg,
                        void *opaque) {
  (void)rk;
  mk_bus_t *bus = opaque;
  if (msg->err) {
    ++bus->delivery_failures;
    fprintf(stderr, "kafka delivery to %s failed: %s\n",
            rd_kafka_topic_name(msg->rkt), rd_kafka_err2str(msg->err));
  }
}

static rd_kafka_conf_t *base_conf(mk_bus_t *bus, const mk_config_t *config) {
  rd_kafka_conf_t *conf = rd_kafka_conf_new();
  if (!set(conf, "bootstrap.servers", config->brokers) ||
      !set(conf, "client.id", config->client_id)) {
    rd_kafka_conf_destroy(conf);
    return NULL;
  }
  rd_kafka_conf_set_opaque(conf, bus);
  rd_kafka_conf_set_error_cb(conf, error_cb);
  return conf;
}

static rd_kafka_t *new_client(rd_kafka_type_t type, rd_kafka_conf_t *conf) {
  char errstr[256];
  rd_kafka_t *rk = rd_kafka_new(type, conf, errstr, sizeof(errstr));
  if (rk == NULL) {
    fprintf(stderr, "kafka client: %s\n", errstr);
    rd_kafka_conf_destroy(conf); /* only taken over on success */
  }
  return rk;
}

static rd_kafka_t *open_producer(mk_bus_t *bus, const mk_config_t *config) {
  rd_kafka_conf_t *conf = base_conf(bus, config);
  if (conf == NULL)
    return NULL;
  /* Idempotent producer: retries can't duplicate or reorder messages.
   * Implies acks=all. No batching: send every message immediately.
   * message.timeout.ms=0 keeps retrying until the broker is back, so a done
   * is not lost while Kafka restarts. */
  if (!set(conf, "enable.idempotence", "true") ||
      !set(conf, "acks", "all") || !set(conf, "linger.ms", "0") ||
      !set(conf, "compression.type", "none") ||
      !set(conf, "message.timeout.ms", "0")) {
    rd_kafka_conf_destroy(conf);
    return NULL;
  }
  rd_kafka_conf_set_dr_msg_cb(conf, delivery_cb);
  return new_client(RD_KAFKA_PRODUCER, conf);
}

static rd_kafka_t *open_consumer(mk_bus_t *bus, const mk_config_t *config) {
  rd_kafka_conf_t *conf = base_conf(bus, config);
  if (conf == NULL)
    return NULL;
  const char *group = config->group_id ? config->group_id : config->client_id;
  const char *reset = config->offset_reset ? config->offset_reset : "earliest";
  /* Offsets are committed by mk_poll after the handler ran. */
  if (!set(conf, "group.id", group) ||
      !set(conf, "enable.auto.commit", "false") ||
      !set(conf, "auto.offset.reset", reset)) {
    rd_kafka_conf_destroy(conf);
    return NULL;
  }
  rd_kafka_t *rk = new_client(RD_KAFKA_CONSUMER, conf);
  if (rk == NULL)
    return NULL;
  rd_kafka_poll_set_consumer(rk);

  rd_kafka_topic_partition_list_t *topics =
      rd_kafka_topic_partition_list_new((int)config->n_topics);
  for (size_t i = 0; i < config->n_topics; ++i)
    rd_kafka_topic_partition_list_add(topics, config->topics[i],
                                      RD_KAFKA_PARTITION_UA);
  rd_kafka_resp_err_t err = rd_kafka_subscribe(rk, topics);
  rd_kafka_topic_partition_list_destroy(topics);
  if (err) {
    fprintf(stderr, "kafka subscribe: %s\n", rd_kafka_err2str(err));
    rd_kafka_consumer_close(rk);
    rd_kafka_destroy(rk);
    return NULL;
  }
  return rk;
}

mk_bus_t *mk_open(const mk_config_t *config) {
  if (config == NULL || config->brokers == NULL ||
      config->client_id == NULL || config->client_id[0] == '\0' ||
      (config->n_topics > 0 && config->topics == NULL) ||
      config->n_topics > INT32_MAX) {
    fprintf(stderr, "kafka: invalid config\n");
    return NULL;
  }
  for (size_t i = 0; i < config->n_topics; ++i) {
    if (config->topics[i] == NULL || config->topics[i][0] == '\0') {
      fprintf(stderr, "kafka: invalid topic name\n");
      return NULL;
    }
  }

  mk_bus_t *bus = calloc(1, sizeof(*bus));
  if (bus == NULL)
    return NULL;

  /* message_id = <client_id>-<start time>-<seq>. The start time keeps ids
   * unique across restarts of the same machine. */
  int n = snprintf(bus->id_prefix, sizeof(bus->id_prefix), "%s-%llx",
                   config->client_id, (unsigned long long)time(NULL));
  if (n < 0 || (size_t)n >= sizeof(bus->id_prefix)) {
    fprintf(stderr, "kafka: client_id too long\n");
    free(bus);
    return NULL;
  }

  bus->producer = open_producer(bus, config);
  if (bus->producer == NULL) {
    free(bus);
    return NULL;
  }
  if (config->n_topics > 0) {
    bus->consumer = open_consumer(bus, config);
    if (bus->consumer == NULL) {
      rd_kafka_destroy(bus->producer);
      free(bus);
      return NULL;
    }
  }
  return bus;
}

static void fill_timestamp(char *dst, size_t len) {
  time_t now = time(NULL);
  struct tm utc;
  if (gmtime_r(&now, &utc) == NULL ||
      strftime(dst, len, "%Y-%m-%dT%H:%M:%SZ", &utc) == 0)
    dst[0] = '\0';
}

mk_result_t mk_send(mk_bus_t *bus, mk_message_t *msg) {
  if (bus == NULL || msg == NULL)
    return MK_ERR_ARG;
  if (bus->fatal)
    return MK_ERR_FATAL;

  mk_message_t out = *msg;
  if (out.message_id[0] == '\0')
    snprintf(out.message_id, sizeof(out.message_id), "%s-%llu",
             bus->id_prefix, ++bus->next_seq);
  if (out.ts_utc[0] == '\0')
    fill_timestamp(out.ts_utc, sizeof(out.ts_utc));

  char payload[MK_MAX_JSON_LEN];
  int len = mk_encode(&out, payload, sizeof(payload));
  if (len < 0)
    return (mk_result_t)len;

  char key[16];
  int key_len = snprintf(key, sizeof(key), "%d", out.machine_id);

  rd_kafka_vu_t vus[4];
  memset(vus, 0, sizeof(vus));
  vus[0].vtype = RD_KAFKA_VTYPE_TOPIC;
  vus[0].u.cstr = out.machine_type;
  vus[1].vtype = RD_KAFKA_VTYPE_KEY;
  vus[1].u.mem.ptr = key;
  vus[1].u.mem.size = (size_t)key_len;
  vus[2].vtype = RD_KAFKA_VTYPE_VALUE;
  vus[2].u.mem.ptr = payload;
  vus[2].u.mem.size = (size_t)len;
  vus[3].vtype = RD_KAFKA_VTYPE_MSGFLAGS;
  vus[3].u.i = RD_KAFKA_MSG_F_COPY; /* payload is on our stack */

  rd_kafka_error_t *error = rd_kafka_produceva(bus->producer, vus, 4);
  if (error != NULL &&
      rd_kafka_error_code(error) == RD_KAFKA_RESP_ERR__QUEUE_FULL) {
    /* Let delivery reports drain the local queue, then try once more. */
    rd_kafka_error_destroy(error);
    rd_kafka_poll(bus->producer, 100);
    error = rd_kafka_produceva(bus->producer, vus, 4);
  }
  if (error != NULL) {
    mk_result_t result = rd_kafka_error_is_fatal(error) ? MK_ERR_FATAL
                                                        : MK_ERR_KAFKA;
    fprintf(stderr, "kafka send to %s: %s\n", out.machine_type,
            rd_kafka_error_string(error));
    rd_kafka_error_destroy(error);
    if (result == MK_ERR_FATAL)
      bus->fatal = 1;
    return result;
  }

  rd_kafka_poll(bus->producer, 0); /* serve delivery reports */
  *msg = out;
  return MK_OK;
}

int mk_poll(mk_bus_t *bus, int timeout_ms, mk_handler_fn handler, void *user) {
  if (bus == NULL || (bus->consumer != NULL && handler == NULL))
    return MK_ERR_ARG;
  if (bus->fatal)
    return MK_ERR_FATAL;

  if (bus->consumer == NULL) {
    rd_kafka_poll(bus->producer, timeout_ms);
    return bus->fatal ? MK_ERR_FATAL : 0;
  }

  rd_kafka_poll(bus->producer, 0);
  int handled = 0;
  int wait = timeout_ms;
  for (int i = 0; i < MK_POLL_MAX_MESSAGES && !bus->fatal; ++i) {
    rd_kafka_message_t *rkm = rd_kafka_consumer_poll(bus->consumer, wait);
    wait = 0; /* only block for the first message */
    if (rkm == NULL)
      break;

    if (rkm->err) {
      /* e.g. topic does not exist yet; the consumer keeps going. */
      fprintf(stderr, "kafka consume: %s\n", rd_kafka_message_errstr(rkm));
      rd_kafka_message_destroy(rkm);
      continue;
    }

    mk_message_t msg;
    if (rkm->payload != NULL &&
        mk_decode(rkm->payload, rkm->len, &msg) == MK_OK) {
      handler(&msg, user);
      ++handled;
    } else {
      fprintf(stderr, "kafka: skipping malformed message %s[%d]@%lld\n",
              rd_kafka_topic_name(rkm->rkt), (int)rkm->partition,
              (long long)rkm->offset);
    }

    /* Commit after handling: a crash before this line redelivers the
     * message (at-least-once), which mk_dedup turns into exactly-once. */
    rd_kafka_resp_err_t err = rd_kafka_commit_message(bus->consumer, rkm, 0);
    if (err)
      fprintf(stderr, "kafka commit: %s\n", rd_kafka_err2str(err));
    rd_kafka_message_destroy(rkm);
  }

  rd_kafka_poll(bus->producer, 0); /* replies sent by the handler */
  return bus->fatal ? MK_ERR_FATAL : handled;
}

int mk_outstanding(mk_bus_t *bus) {
  return bus == NULL ? 0 : rd_kafka_outq_len(bus->producer);
}

unsigned long mk_delivery_failures(mk_bus_t *bus) {
  return bus == NULL ? 0 : bus->delivery_failures;
}

int mk_close(mk_bus_t *bus, int timeout_ms) {
  if (bus == NULL)
    return 0;
  if (bus->consumer != NULL) {
    rd_kafka_consumer_close(bus->consumer);
    rd_kafka_destroy(bus->consumer);
  }
  rd_kafka_flush(bus->producer, timeout_ms);
  int left = rd_kafka_outq_len(bus->producer);
  if (left > 0)
    fprintf(stderr, "kafka: %d message(s) not delivered before close\n", left);
  rd_kafka_destroy(bus->producer);
  free(bus);
  return left;
}
