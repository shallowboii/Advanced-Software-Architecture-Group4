#define _POSIX_C_SOURCE 200809L /* gmtime_r */

#include "machine_kafka.h"
#include "trace.h"

#include <librdkafka/rdkafka.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* At most this many messages are handled per mk_poll call, so the machine
 * loop keeps running even when a backlog is waiting. */
#define MK_POLL_MAX_MESSAGES 64

/* Timeout of the blocking offset lookups done while partitions are assigned. */
#define MK_OFFSET_QUERY_MS 10000

struct mk_bus {
  char name[MK_ID_LEN]; /* client_id, for trace output */
  rd_kafka_t *producer;
  rd_kafka_t *consumer; /* NULL when no topics are consumed */
  /* Leaves room for "-" + 20 digits of the sequence number in message_id. */
  char id_prefix[MK_ID_LEN - 22];
  unsigned long long next_seq;
  int fatal;
  int ready;        /* consumer has an assignment with fixed start offsets */
  int reset_latest; /* auto.offset.reset is latest */
  mk_receive_t receive;
  int command_max_age_s;
  unsigned long delivery_failures;
  mk_delivery_fn on_delivery_failure;
  void *delivery_user;
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
  if (mk_trace_enabled()) {
    mk_message_t sent;
    int decoded = msg->payload != NULL &&
                  mk_decode(msg->payload, msg->len, &sent) == MK_OK;
    mk_trace_write(bus->name, msg->err ? "delivery.fail" : "delivery.ok",
                   "topic=%s part=%d off=%lld id=%s%s%s%s",
                   rd_kafka_topic_name(msg->rkt), (int)msg->partition,
                   (long long)msg->offset, decoded ? sent.message_id : "?",
                   msg->err ? " reason=\"" : "",
                   msg->err ? rd_kafka_err2str(msg->err) : "",
                   msg->err ? "\"" : "");
  }
  if (!msg->err)
    return;
  ++bus->delivery_failures;
  fprintf(stderr, "kafka delivery to %s failed: %s\n",
          rd_kafka_topic_name(msg->rkt), rd_kafka_err2str(msg->err));
  if (bus->on_delivery_failure != NULL) {
    mk_message_t failed;
    int decoded = msg->payload != NULL &&
                  mk_decode(msg->payload, msg->len, &failed) == MK_OK;
    bus->on_delivery_failure(decoded ? &failed : NULL,
                             rd_kafka_err2str(msg->err), bus->delivery_user);
  }
}

/*
 * A group without a committed offset starts at auto.offset.reset. librdkafka
 * resolves "latest" lazily, some time after the assignment, so a command sent
 * right after the machine reports idle could fall before that point and be
 * skipped. Resolve the start offset here instead; once mk_ready() is 1 every
 * later message is guaranteed to be read.
 */
static void fix_start_offsets(mk_bus_t *bus, rd_kafka_t *rk,
                              rd_kafka_topic_partition_list_t *parts) {
  rd_kafka_resp_err_t err = rd_kafka_committed(rk, parts, MK_OFFSET_QUERY_MS);
  if (err) {
    fprintf(stderr, "kafka committed offsets: %s\n", rd_kafka_err2str(err));
    return; /* fall back to auto.offset.reset */
  }
  for (int i = 0; i < parts->cnt; ++i) {
    rd_kafka_topic_partition_t *p = &parts->elems[i];
    if (p->offset >= 0) {
      MK_TRACE(bus->name, "assign.offset",
               "topic=%s part=%d start=%lld why=committed", p->topic,
               (int)p->partition, (long long)p->offset);
      continue; /* resume after the last commit */
    }
    if (!bus->reset_latest) {
      p->offset = RD_KAFKA_OFFSET_BEGINNING;
      MK_TRACE(bus->name, "assign.offset",
               "topic=%s part=%d start=beginning why=\"no commit, earliest\"",
               p->topic, (int)p->partition);
      continue;
    }
    int64_t low = 0, high = 0;
    err = rd_kafka_query_watermark_offsets(rk, p->topic, p->partition, &low,
                                           &high, MK_OFFSET_QUERY_MS);
    if (err) {
      fprintf(stderr, "kafka end offset of %s[%d]: %s\n", p->topic,
              (int)p->partition, rd_kafka_err2str(err));
      p->offset = RD_KAFKA_OFFSET_INVALID;
    } else {
      p->offset = high;
    }
    MK_TRACE(bus->name, "assign.offset",
             "topic=%s part=%d start=%lld why=\"no commit, latest = end\"",
             p->topic, (int)p->partition, (long long)p->offset);
  }
}

static void rebalance_cb(rd_kafka_t *rk, rd_kafka_resp_err_t err,
                         rd_kafka_topic_partition_list_t *parts,
                         void *opaque) {
  mk_bus_t *bus = opaque;
  if (err == RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS) {
    MK_TRACE(bus->name, "rebalance.assign", "partitions=%d", parts->cnt);
    fix_start_offsets(bus, rk, parts);
    rd_kafka_assign(rk, parts);
    bus->ready = parts->cnt > 0;
  } else {
    MK_TRACE(bus->name, "rebalance.revoke", "partitions=%d reason=\"%s\"",
             parts->cnt, rd_kafka_err2str(err));
    rd_kafka_assign(rk, NULL);
    bus->ready = 0;
  }
  MK_TRACE(bus->name, "ready", "value=%d", bus->ready);
}

static rd_kafka_conf_t *base_conf(mk_bus_t *bus, const mk_config_t *config) {
  rd_kafka_conf_t *conf = rd_kafka_conf_new();
  if (!set(conf, "bootstrap.servers", config->brokers) ||
      !set(conf, "client.id", config->client_id)) {
    rd_kafka_conf_destroy(conf);
    return NULL;
  }
  /* MK_KAFKA_DEBUG=cgrp,topic,msg (or "all") turns on librdkafka's own
   * debug log, printed to stderr as "%7|...". */
  const char *debug = getenv("MK_KAFKA_DEBUG");
  if (debug != NULL && debug[0] != '\0' && !set(conf, "debug", debug)) {
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
   * is not lost while Kafka restarts; stale commands are dropped by the
   * receiving machine instead (command_max_age_s). murmur2_random is the
   * Java client's partitioner: a key lands on the same partition whether C
   * or Java produced it, so per-machine order holds across languages. */
  if (!set(conf, "enable.idempotence", "true") ||
      !set(conf, "acks", "all") || !set(conf, "linger.ms", "0") ||
      !set(conf, "compression.type", "none") ||
      !set(conf, "message.timeout.ms", "0") ||
      !set(conf, "partitioner", "murmur2_random")) {
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
  /* Offsets are committed by mk_poll after the handler ran. */
  if (!set(conf, "group.id", group) ||
      !set(conf, "enable.auto.commit", "false") ||
      !set(conf, "auto.offset.reset",
           bus->reset_latest ? "latest" : "earliest")) {
    rd_kafka_conf_destroy(conf);
    return NULL;
  }
  rd_kafka_conf_set_rebalance_cb(conf, rebalance_cb);
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

/* client_id ends up in message_id, so it has to be a valid identifier. */
static int valid_client_id(const char *s) {
  if (s == NULL || s[0] == '\0')
    return 0;
  for (const char *c = s; *c != '\0'; ++c) {
    if (!((*c >= 'a' && *c <= 'z') || (*c >= 'A' && *c <= 'Z') ||
          (*c >= '0' && *c <= '9') || *c == '.' || *c == '_' || *c == ':' ||
          *c == '-'))
      return 0;
  }
  return 1;
}

mk_bus_t *mk_open(const mk_config_t *config) {
  if (config == NULL || config->brokers == NULL ||
      !valid_client_id(config->client_id) ||
      (config->n_topics > 0 && config->topics == NULL) ||
      config->n_topics > INT32_MAX ||
      (config->receive != MK_RECEIVE_COMMANDS &&
       config->receive != MK_RECEIVE_STATUSES) ||
      (config->offset_reset != NULL &&
       strcmp(config->offset_reset, "latest") != 0 &&
       strcmp(config->offset_reset, "earliest") != 0)) {
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
  bus->receive = config->receive;
  bus->reset_latest = config->offset_reset == NULL
                          ? config->receive == MK_RECEIVE_COMMANDS
                          : strcmp(config->offset_reset, "latest") == 0;
  bus->command_max_age_s = config->command_max_age_s == 0
                               ? MK_DEFAULT_COMMAND_MAX_AGE_S
                               : config->command_max_age_s;
  bus->on_delivery_failure = config->on_delivery_failure;
  bus->delivery_user = config->delivery_user;
  snprintf(bus->name, sizeof(bus->name), "%s", config->client_id);
  if (mk_trace_enabled()) {
    char topics[512] = "";
    size_t pos = 0;
    for (size_t i = 0; i < config->n_topics && pos < sizeof(topics); ++i) {
      int n = snprintf(topics + pos, sizeof(topics) - pos, "%s%s",
                       i > 0 ? "," : "", config->topics[i]);
      pos += n > 0 ? (size_t)n : 0;
    }
    mk_trace_write(bus->name, "open",
                   "brokers=%s group=%s topics=%s receive=%s reset=%s "
                   "max_age_s=%d delivery_cb=%s",
                   config->brokers,
                   config->group_id ? config->group_id : config->client_id,
                   config->n_topics > 0 ? topics : "(none)",
                   bus->receive == MK_RECEIVE_COMMANDS ? "commands"
                                                       : "statuses",
                   bus->reset_latest ? "latest" : "earliest",
                   bus->command_max_age_s,
                   config->on_delivery_failure ? "yes" : "no");
  }

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
  } else {
    bus->ready = 1;
  }
  return bus;
}

int mk_ready(mk_bus_t *bus) { return bus != NULL && bus->ready; }

static void fill_timestamp(char *dst, size_t len) {
  time_t now = time(NULL);
  struct tm utc;
  if (gmtime_r(&now, &utc) == NULL ||
      strftime(dst, len, "%Y-%m-%dT%H:%M:%SZ", &utc) == 0)
    dst[0] = '\0';
}

/* Seconds since 1970 for "YYYY-MM-DDTHH:MM:SS..." (UTC), without timegm. */
static int parse_timestamp(const char *ts, long long *out) {
  int y, mo, d, h, mi, s;
  if (sscanf(ts, "%4d-%2d-%2dT%2d:%2d:%2d", &y, &mo, &d, &h, &mi, &s) != 6 ||
      mo < 1 || mo > 12 || d < 1 || d > 31 || h > 23 || mi > 59 || s > 60)
    return 0;
  /* days_from_civil (H. Hinnant) */
  y -= mo <= 2;
  long long era = (y >= 0 ? y : y - 399) / 400;
  long long yoe = y - era * 400;
  long long doy = (153 * (mo + (mo > 2 ? -3 : 9)) + 2) / 5 + d - 1;
  long long doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
  long long days = era * 146097 + doe - 719468;
  *out = days * 86400 + h * 3600 + mi * 60 + s;
  return 1;
}

/* A command that waited too long (e.g. queued while the broker was down) is
 * no longer meaningful; the scheduler re-sends it if it still wants it. */
static int expired(const mk_bus_t *bus, const mk_message_t *msg,
                   long long *age) {
  long long sent;
  if (!mk_is_command(msg->type) || bus->command_max_age_s < 0 ||
      !parse_timestamp(msg->ts_utc, &sent))
    return 0;
  *age = (long long)time(NULL) - sent;
  return *age > bus->command_max_age_s;
}

/* A message must be on the topic of its machine_type; anything else (e.g. a
 * mashing start on the boiling topic) is treated as malformed. */
static int on_right_topic(const mk_message_t *msg, const char *topic) {
  return strcmp(msg->machine_type, topic) == 0;
}

/* Commands and statuses share a topic: a machine skips the statuses (its own
 * and those of the other machines), the scheduler skips its own commands. */
static int for_me(const mk_bus_t *bus, const mk_message_t *msg) {
  return mk_is_command(msg->type) == (bus->receive == MK_RECEIVE_COMMANDS);
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
  if (len < 0) {
    MK_TRACE(bus->name, "send.fail", "result=\"%s\" (see encode.fail above)",
             mk_result_name(len));
    return (mk_result_t)len;
  }

  const char *topic = out.machine_type; /* checked by mk_encode */
  char key[16];
  int key_len = snprintf(key, sizeof(key), "%d", out.machine_id);

  rd_kafka_vu_t vus[4];
  memset(vus, 0, sizeof(vus));
  vus[0].vtype = RD_KAFKA_VTYPE_TOPIC;
  vus[0].u.cstr = topic;
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
    fprintf(stderr, "kafka send to %s: %s\n", topic,
            rd_kafka_error_string(error));
    rd_kafka_error_destroy(error);
    if (result == MK_ERR_FATAL)
      bus->fatal = 1;
    return result;
  }

  if (mk_trace_enabled()) {
    char summary[MK_TRACE_MSG_LEN];
    mk_trace_write(bus->name, "send", "topic=%s key=%s bytes=%d %s", topic,
                   key, len, mk_trace_msg(&out, summary, sizeof(summary)));
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

    const char *topic = rd_kafka_topic_name(rkm->rkt);
    MK_TRACE(bus->name, "recv", "topic=%s part=%d off=%lld key=%.*s bytes=%zu",
             topic, (int)rkm->partition, (long long)rkm->offset,
             (int)(rkm->key_len < 32 ? rkm->key_len : 32),
             rkm->key != NULL ? (const char *)rkm->key : "", rkm->len);
    mk_message_t msg;
    long long age = 0;
    if (rkm->payload == NULL ||
        mk_decode(rkm->payload, rkm->len, &msg) != MK_OK) {
      fprintf(stderr, "kafka: skipping malformed message %s[%d]@%lld\n",
              topic, (int)rkm->partition, (long long)rkm->offset);
      MK_TRACE(bus->name, "skip", "off=%lld why=malformed (see decode.fail)",
               (long long)rkm->offset);
    } else if (!on_right_topic(&msg, topic)) {
      fprintf(stderr, "kafka: skipping malformed message %s[%d]@%lld\n",
              topic, (int)rkm->partition, (long long)rkm->offset);
      if (mk_trace_enabled()) {
        char summary[MK_TRACE_MSG_LEN];
        mk_trace_write(bus->name, "skip",
                       "off=%lld why=wrong_topic expected=%s %s",
                       (long long)rkm->offset, msg.machine_type,
                       mk_trace_msg(&msg, summary, sizeof(summary)));
      }
    } else if (!for_me(bus, &msg)) {
      MK_TRACE(bus->name, "skip", "off=%lld why=other_direction type=%s id=%s",
               (long long)rkm->offset, mk_type_name(msg.type), msg.message_id);
    } else if (expired(bus, &msg, &age)) {
      fprintf(stderr, "kafka: skipping expired %s %s (sent %s)\n",
              mk_type_name(msg.type), msg.message_id, msg.ts_utc);
      MK_TRACE(bus->name, "skip",
               "off=%lld why=expired age_s=%lld max_age_s=%d id=%s",
               (long long)rkm->offset, age, bus->command_max_age_s,
               msg.message_id);
    } else {
      struct timespec t0, t1;
      int tracing = mk_trace_enabled();
      if (tracing) {
        char summary[MK_TRACE_MSG_LEN];
        mk_trace_write(bus->name, "handler.call", "off=%lld %s",
                       (long long)rkm->offset,
                       mk_trace_msg(&msg, summary, sizeof(summary)));
        clock_gettime(CLOCK_MONOTONIC, &t0);
      }
      handler(&msg, user);
      ++handled;
      if (tracing) {
        clock_gettime(CLOCK_MONOTONIC, &t1);
        mk_trace_write(bus->name, "handler.return", "off=%lld took_ms=%.3f",
                       (long long)rkm->offset,
                       (double)(t1.tv_sec - t0.tv_sec) * 1e3 +
                           (double)(t1.tv_nsec - t0.tv_nsec) / 1e6);
      }
    }

    /* Commit after handling: a crash before this line redelivers the
     * message (at-least-once); mk_ledger keeps that from running a batch
     * twice. */
    rd_kafka_resp_err_t err = rd_kafka_commit_message(bus->consumer, rkm, 0);
    if (err)
      fprintf(stderr, "kafka commit: %s\n", rd_kafka_err2str(err));
    MK_TRACE(bus->name, "commit", "topic=%s part=%d next_off=%lld result=\"%s\"",
             topic, (int)rkm->partition, (long long)rkm->offset + 1,
             err ? rd_kafka_err2str(err) : "ok");
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
  MK_TRACE(bus->name, "close", "undelivered=%d delivery_failures=%lu", left,
           bus->delivery_failures);
  rd_kafka_destroy(bus->producer);
  free(bus);
  return left;
}
