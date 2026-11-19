#include "machine_kafka.h"

#include <cjson/cJSON.h>
#include <limits.h>
#include <math.h>
#include <string.h>

static const char *const TYPE_NAMES[] = {"start", "done", "error"};

const char *mk_type_name(mk_type_t type) {
  if (type < MK_START || type > MK_ERROR)
    return "unknown";
  return TYPE_NAMES[type];
}

const char *mk_result_name(int result) {
  if (result >= 0)
    return "ok";
  switch (result) {
  case MK_ERR_ARG:
    return "invalid argument";
  case MK_ERR_ENCODE:
    return "cannot encode message";
  case MK_ERR_DECODE:
    return "malformed message";
  case MK_ERR_KAFKA:
    return "kafka error";
  case MK_ERR_FATAL:
    return "fatal kafka error";
  default:
    return "unknown error";
  }
}

/* Copies src into dst; fails instead of truncating. NULL copies as "". */
static int copy_str(char *dst, size_t dst_len, const char *src) {
  if (src == NULL)
    src = "";
  size_t len = strlen(src);
  if (len >= dst_len)
    return 0;
  memcpy(dst, src, len + 1);
  return 1;
}

/* Topic names: Kafka allows [A-Za-z0-9._-]. */
static int valid_topic(const char *name) {
  if (name[0] == '\0')
    return 0;
  for (const char *c = name; *c != '\0'; ++c) {
    if (!((*c >= 'a' && *c <= 'z') || (*c >= 'A' && *c <= 'Z') ||
          (*c >= '0' && *c <= '9') || *c == '.' || *c == '_' || *c == '-'))
      return 0;
  }
  return 1;
}

/* Rules shared by encode and decode; message_id is checked separately because
 * mk_send fills it in before encoding. */
static int valid_body(const mk_message_t *m) {
  if (m->type < MK_START || m->type > MK_ERROR)
    return 0;
  if (!valid_topic(m->machine_type) || m->machine_id < 1)
    return 0;
  switch (m->type) {
  case MK_START:
    return m->batch_id[0] != '\0';
  case MK_DONE:
    return m->reply_to[0] != '\0';
  case MK_ERROR:
    return m->reply_to[0] != '\0' && m->error_code[0] != '\0';
  }
  return 0;
}

mk_result_t mk_make_start(mk_message_t *out, const char *machine_type,
                          int machine_id, const char *batch_id) {
  if (out == NULL)
    return MK_ERR_ARG;
  memset(out, 0, sizeof(*out));
  out->type = MK_START;
  out->machine_id = machine_id;
  if (!copy_str(out->machine_type, sizeof(out->machine_type), machine_type) ||
      !copy_str(out->batch_id, sizeof(out->batch_id), batch_id))
    return MK_ERR_ARG;
  return valid_body(out) ? MK_OK : MK_ERR_ARG;
}

static mk_result_t make_reply(mk_message_t *out, const mk_message_t *start,
                              mk_type_t type) {
  if (out == NULL || start == NULL || start->type != MK_START ||
      start->message_id[0] == '\0')
    return MK_ERR_ARG;
  mk_message_t reply;
  memset(&reply, 0, sizeof(reply));
  reply.type = type;
  reply.machine_id = start->machine_id;
  memcpy(reply.machine_type, start->machine_type, sizeof(reply.machine_type));
  memcpy(reply.batch_id, start->batch_id, sizeof(reply.batch_id));
  memcpy(reply.reply_to, start->message_id, sizeof(reply.reply_to));
  *out = reply; /* out may alias start */
  return MK_OK;
}

mk_result_t mk_make_done(mk_message_t *out, const mk_message_t *start) {
  return make_reply(out, start, MK_DONE);
}

mk_result_t mk_make_error(mk_message_t *out, const mk_message_t *start,
                          const char *code, const char *reason) {
  if (code == NULL || code[0] == '\0')
    return MK_ERR_ARG;
  mk_message_t reply;
  mk_result_t result = make_reply(&reply, start, MK_ERROR);
  if (result != MK_OK)
    return result;
  if (!copy_str(reply.error_code, sizeof(reply.error_code), code) ||
      !copy_str(reply.error_reason, sizeof(reply.error_reason), reason))
    return MK_ERR_ARG;
  *out = reply;
  return MK_OK;
}

/* Adds a string field, or null when it is empty. */
static int add_str(cJSON *obj, const char *key, const char *value) {
  cJSON *item =
      value[0] == '\0' ? cJSON_CreateNull() : cJSON_CreateString(value);
  return item != NULL && cJSON_AddItemToObject(obj, key, item);
}

int mk_encode(const mk_message_t *msg, char *buf, size_t buf_len) {
  if (msg == NULL || buf == NULL || buf_len == 0)
    return MK_ERR_ARG;
  if (!valid_body(msg) || msg->message_id[0] == '\0')
    return MK_ERR_ENCODE;

  cJSON *root = cJSON_CreateObject();
  int ok = root != NULL &&
           cJSON_AddNumberToObject(root, "schema_version", MK_SCHEMA_VERSION) &&
           add_str(root, "message_id", msg->message_id) &&
           add_str(root, "type", mk_type_name(msg->type)) &&
           add_str(root, "machine_type", msg->machine_type) &&
           cJSON_AddNumberToObject(root, "machine_id", msg->machine_id) &&
           add_str(root, "batch_id", msg->batch_id) &&
           add_str(root, "reply_to", msg->reply_to) &&
           add_str(root, "ts_utc", msg->ts_utc);
  if (ok) {
    if (msg->type == MK_ERROR) {
      cJSON *error = cJSON_AddObjectToObject(root, "error");
      ok = error != NULL && add_str(error, "code", msg->error_code) &&
           add_str(error, "reason", msg->error_reason);
    } else {
      ok = cJSON_AddNullToObject(root, "error") != NULL;
    }
  }

  /* PrintPreallocated needs 5 spare bytes; we only accept lengths that leave
   * room for the NUL. */
  int written = MK_ERR_ENCODE;
  if (ok && buf_len <= INT_MAX &&
      cJSON_PrintPreallocated(root, buf, (int)buf_len, 0))
    written = (int)strlen(buf);
  cJSON_Delete(root);
  return written;
}

/* Reads an optional string field (missing or null -> ""). */
static int get_str(const cJSON *obj, const char *key, char *dst,
                   size_t dst_len) {
  const cJSON *item = cJSON_GetObjectItemCaseSensitive(obj, key);
  if (item == NULL || cJSON_IsNull(item)) {
    dst[0] = '\0';
    return 1;
  }
  return cJSON_IsString(item) && copy_str(dst, dst_len, item->valuestring);
}

static int get_int(const cJSON *obj, const char *key, int *out) {
  const cJSON *item = cJSON_GetObjectItemCaseSensitive(obj, key);
  if (!cJSON_IsNumber(item))
    return 0;
  double v = item->valuedouble;
  if (v != floor(v) || v < INT_MIN || v > INT_MAX)
    return 0;
  *out = (int)v;
  return 1;
}

mk_result_t mk_decode(const char *json, size_t len, mk_message_t *out) {
  if (json == NULL || out == NULL)
    return MK_ERR_ARG;

  mk_message_t m;
  memset(&m, 0, sizeof(m));
  cJSON *root = cJSON_ParseWithLength(json, len);
  if (root == NULL || !cJSON_IsObject(root)) {
    cJSON_Delete(root);
    return MK_ERR_DECODE;
  }

  int version = 0;
  char type[MK_NAME_LEN];
  int ok = get_int(root, "schema_version", &version) &&
           version == MK_SCHEMA_VERSION &&
           get_str(root, "type", type, sizeof(type)) &&
           get_str(root, "message_id", m.message_id, sizeof(m.message_id)) &&
           get_str(root, "machine_type", m.machine_type,
                   sizeof(m.machine_type)) &&
           get_int(root, "machine_id", &m.machine_id) &&
           get_str(root, "batch_id", m.batch_id, sizeof(m.batch_id)) &&
           get_str(root, "reply_to", m.reply_to, sizeof(m.reply_to)) &&
           get_str(root, "ts_utc", m.ts_utc, sizeof(m.ts_utc));

  if (ok) {
    if (strcmp(type, "start") == 0)
      m.type = MK_START;
    else if (strcmp(type, "done") == 0)
      m.type = MK_DONE;
    else if (strcmp(type, "error") == 0)
      m.type = MK_ERROR;
    else
      ok = 0;
  }

  if (ok && m.type == MK_ERROR) {
    const cJSON *error = cJSON_GetObjectItemCaseSensitive(root, "error");
    ok = cJSON_IsObject(error) &&
         get_str(error, "code", m.error_code, sizeof(m.error_code)) &&
         get_str(error, "reason", m.error_reason, sizeof(m.error_reason));
  }
  cJSON_Delete(root);

  if (!ok || m.message_id[0] == '\0' || !valid_body(&m))
    return MK_ERR_DECODE;
  *out = m;
  return MK_OK;
}

void mk_dedup_init(mk_dedup_t *dedup) {
  if (dedup != NULL)
    memset(dedup, 0, sizeof(*dedup));
}

int mk_dedup_seen(mk_dedup_t *dedup, const char *message_id) {
  if (dedup == NULL || message_id == NULL || message_id[0] == '\0')
    return 0;
  for (size_t i = 0; i < MK_DEDUP_SIZE; ++i) {
    if (strcmp(dedup->ids[i], message_id) == 0)
      return 1;
  }
  /* Ids longer than a message can carry are never received; truncating keeps
   * the ring consistent anyway. */
  strncpy(dedup->ids[dedup->next], message_id, MK_ID_LEN - 1);
  dedup->ids[dedup->next][MK_ID_LEN - 1] = '\0';
  dedup->next = (dedup->next + 1) % MK_DEDUP_SIZE;
  return 0;
}
