#include "machine_kafka.h"

#include <cjson/cJSON.h>
#include <limits.h>
#include <math.h>
#include <stdio.h>
#include <string.h>

static const char *const TYPE_NAMES[] = {"start", "stop",    "flush", "idle",
                                         "running", "done", "error"};
#define N_TYPES (sizeof(TYPE_NAMES) / sizeof(TYPE_NAMES[0]))

static int valid_type(mk_type_t type) {
  return (int)type >= (int)MK_START && (int)type <= (int)MK_ERROR;
}

const char *mk_type_name(mk_type_t type) {
  return valid_type(type) ? TYPE_NAMES[type] : "unknown";
}

int mk_is_command(mk_type_t type) {
  return type == MK_START || type == MK_STOP || type == MK_FLUSH;
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
  case MK_ERR_IO:
    return "ledger file error";
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

/* machine_type is the topic name: [a-z][a-z0-9_-]*. */
static int valid_machine_type(const char *s) {
  if (!(s[0] >= 'a' && s[0] <= 'z'))
    return 0;
  for (const char *c = s; *c != '\0'; ++c) {
    if (!((*c >= 'a' && *c <= 'z') || (*c >= '0' && *c <= '9') || *c == '_' ||
          *c == '-'))
      return 0;
  }
  return 1;
}

/* Identifiers: [A-Za-z0-9._:-]. None of these is escaped in JSON, which keeps
 * MK_MAX_JSON_LEN a real upper bound. Empty is allowed here. */
static int valid_ident(const char *s) {
  for (const char *c = s; *c != '\0'; ++c) {
    if (!((*c >= 'a' && *c <= 'z') || (*c >= 'A' && *c <= 'Z') ||
          (*c >= '0' && *c <= '9') || *c == '.' || *c == '_' || *c == ':' ||
          *c == '-'))
      return 0;
  }
  return 1;
}

static int find_param(const mk_message_t *m, const char *name) {
  for (size_t i = 0; i < m->n_params; ++i) {
    if (strcmp(m->params[i].name, name) == 0)
      return (int)i;
  }
  return -1;
}

static const char *check_params(const mk_message_t *m) {
  if (m->n_params > MK_MAX_PARAMS)
    return "more than MK_MAX_PARAMS params";
  if (m->n_params > 0 && m->type != MK_START)
    return "only start carries params";
  for (size_t i = 0; i < m->n_params; ++i) {
    const mk_param_t *p = &m->params[i];
    if (p->name[0] == '\0' || !valid_ident(p->name))
      return "param name empty or outside [A-Za-z0-9._:-]";
    if (!isfinite(p->value))
      return "param value not finite";
    if (find_param(m, p->name) != (int)i)
      return "duplicate param name";
  }
  return NULL;
}

/* Rules shared by encode and decode; message_id is checked separately because
 * mk_send fills it in before encoding. Returns NULL if valid, otherwise what
 * is wrong. */
static const char *check_body(const mk_message_t *m) {
  if (!valid_type(m->type))
    return "unknown type";
  if (!valid_machine_type(m->machine_type))
    return "machine_type must match [a-z][a-z0-9_-]*";
  if (m->machine_id < 1)
    return "machine_id must be >= 1";
  if (!valid_ident(m->batch_id))
    return "batch_id has characters outside [A-Za-z0-9._:-]";
  if (!valid_ident(m->message_id))
    return "message_id has characters outside [A-Za-z0-9._:-]";
  if (!valid_ident(m->ts_utc))
    return "ts_utc has characters outside [A-Za-z0-9._:-]";
  if (!valid_ident(m->error_code))
    return "error code has characters outside [A-Za-z0-9._:-]";
  const char *params = check_params(m);
  if (params != NULL)
    return params;
  if (m->type != MK_ERROR &&
      (m->error_code[0] != '\0' || m->error_reason[0] != '\0'))
    return "only error carries an error code/reason";

  switch (m->type) {
  case MK_START:
  case MK_FLUSH:
  case MK_RUNNING:
  case MK_DONE:
    return m->batch_id[0] != '\0' ? NULL : "batch_id is required";
  case MK_ERROR:
    return m->error_code[0] != '\0' ? NULL : "error requires error.code";
  case MK_STOP:
  case MK_IDLE:
    return NULL;
  }
  return "unknown type";
}

static mk_result_t make_base(mk_message_t *out, mk_type_t type,
                             const char *machine_type, int machine_id) {
  memset(out, 0, sizeof(*out));
  out->type = type;
  out->machine_id = machine_id;
  return copy_str(out->machine_type, sizeof(out->machine_type), machine_type)
             ? MK_OK
             : MK_ERR_ARG;
}

mk_result_t mk_make_command(mk_message_t *out, mk_type_t type,
                            const char *machine_type, int machine_id,
                            const char *batch_id) {
  if (out == NULL || !mk_is_command(type))
    return MK_ERR_ARG;
  mk_message_t m;
  if (make_base(&m, type, machine_type, machine_id) != MK_OK ||
      !copy_str(m.batch_id, sizeof(m.batch_id), batch_id))
    return MK_ERR_ARG;
  const char *why = check_body(&m);
  if (why != NULL)
    return MK_ERR_ARG;
  *out = m;
  return MK_OK;
}

mk_result_t mk_make_start(mk_message_t *out, const char *machine_type,
                          int machine_id, const char *batch_id) {
  return mk_make_command(out, MK_START, machine_type, machine_id, batch_id);
}

mk_result_t mk_param_set(mk_message_t *msg, const char *name, double value) {
  if (msg == NULL || name == NULL || msg->type != MK_START ||
      name[0] == '\0' || strlen(name) >= MK_NAME_LEN || !valid_ident(name) ||
      !isfinite(value) || msg->n_params > MK_MAX_PARAMS)
    return MK_ERR_ARG;
  int i = find_param(msg, name);
  if (i < 0) {
    if (msg->n_params == MK_MAX_PARAMS)
      return MK_ERR_ARG;
    i = (int)msg->n_params++;
    memcpy(msg->params[i].name, name, strlen(name) + 1);
  }
  msg->params[i].value = value;
  return MK_OK;
}

int mk_param_get(const mk_message_t *msg, const char *name, double *value) {
  if (msg == NULL || name == NULL || msg->n_params > MK_MAX_PARAMS)
    return 0;
  int i = find_param(msg, name);
  if (i < 0)
    return 0;
  if (value != NULL)
    *value = msg->params[i].value;
  return 1;
}

mk_result_t mk_make_idle(mk_message_t *out, const char *machine_type,
                         int machine_id) {
  if (out == NULL)
    return MK_ERR_ARG;
  mk_message_t m;
  const char *why = make_base(&m, MK_IDLE, machine_type, machine_id) != MK_OK
                        ? "machine_type too long"
                        : check_body(&m);
  if (why != NULL)
    return MK_ERR_ARG;
  *out = m;
  return MK_OK;
}

/* Status answering command: same machine and batch. */
static mk_result_t make_reply(mk_message_t *out, const mk_message_t *command,
                              mk_type_t type) {
  if (out == NULL || command == NULL || !mk_is_command(command->type))
    return MK_ERR_ARG;
  mk_message_t reply;
  memset(&reply, 0, sizeof(reply));
  reply.type = type;
  reply.machine_id = command->machine_id;
  memcpy(reply.machine_type, command->machine_type,
         sizeof(reply.machine_type));
  memcpy(reply.batch_id, command->batch_id, sizeof(reply.batch_id));
  *out = reply; /* out may alias command */
  return MK_OK;
}

mk_result_t mk_make_running(mk_message_t *out, const mk_message_t *start) {
  if (start == NULL || start->type != MK_START)
    return MK_ERR_ARG;
  return make_reply(out, start, MK_RUNNING);
}

mk_result_t mk_make_done(mk_message_t *out, const mk_message_t *start) {
  if (start == NULL || start->type != MK_START)
    return MK_ERR_ARG;
  return make_reply(out, start, MK_DONE);
}

mk_result_t mk_make_error(mk_message_t *out, const mk_message_t *command,
                          const char *code, const char *reason) {
  if (code == NULL || code[0] == '\0')
    return MK_ERR_ARG;
  mk_message_t reply;
  mk_result_t result = make_reply(&reply, command, MK_ERROR);
  if (result != MK_OK)
    return result;
  const char *why =
      !copy_str(reply.error_code, sizeof(reply.error_code), code) ||
              !copy_str(reply.error_reason, sizeof(reply.error_reason), reason)
          ? "error code or reason too long"
          : check_body(&reply);
  if (why != NULL)
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

static int add_params(cJSON *root, const mk_message_t *msg) {
  if (msg->type != MK_START)
    return cJSON_AddNullToObject(root, "params") != NULL;
  cJSON *params = cJSON_AddObjectToObject(root, "params");
  if (params == NULL)
    return 0;
  for (size_t i = 0; i < msg->n_params; ++i) {
    if (cJSON_AddNumberToObject(params, msg->params[i].name,
                                msg->params[i].value) == NULL)
      return 0;
  }
  return 1;
}

int mk_encode(const mk_message_t *msg, char *buf, size_t buf_len) {
  if (msg == NULL || buf == NULL || buf_len == 0)
    return MK_ERR_ARG;
  const char *why = msg->message_id[0] == '\0' ? "message_id is empty"
                                                : check_body(msg);
  if (why != NULL)
    return MK_ERR_ENCODE;

  cJSON *root = cJSON_CreateObject();
  int ok = root != NULL &&
           cJSON_AddNumberToObject(root, "schema_version", MK_SCHEMA_VERSION) &&
           add_str(root, "message_id", msg->message_id) &&
           add_str(root, "type", mk_type_name(msg->type)) &&
           add_str(root, "machine_type", msg->machine_type) &&
           cJSON_AddNumberToObject(root, "machine_id", msg->machine_id) &&
           add_str(root, "batch_id", msg->batch_id) &&
           add_str(root, "ts_utc", msg->ts_utc) && add_params(root, msg);
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

static int get_params(const cJSON *root, mk_message_t *m) {
  const cJSON *params = cJSON_GetObjectItemCaseSensitive(root, "params");
  if (params == NULL || cJSON_IsNull(params))
    return 1;
  if (!cJSON_IsObject(params))
    return 0;
  const cJSON *item;
  cJSON_ArrayForEach(item, params) {
    if (m->n_params == MK_MAX_PARAMS || !cJSON_IsNumber(item) ||
        item->string == NULL ||
        !copy_str(m->params[m->n_params].name, MK_NAME_LEN, item->string))
      return 0;
    m->params[m->n_params++].value = item->valuedouble;
  }
  return 1; /* names, duplicates and finiteness are checked by valid_body */
}

/* Reads the fields shared by all types; returns NULL or what is wrong. */
static const char *decode_common(const cJSON *root, mk_message_t *m,
                                 char *type, size_t type_len) {
  int version = 0;
  if (!get_int(root, "schema_version", &version))
    return "schema_version missing or not an integer";
  if (version != MK_SCHEMA_VERSION)
    return "unsupported schema_version";
  if (!get_str(root, "type", type, type_len))
    return "type not a string or too long";
  if (!get_str(root, "message_id", m->message_id, sizeof(m->message_id)))
    return "message_id not a string or longer than 95";
  if (!get_str(root, "machine_type", m->machine_type,
               sizeof(m->machine_type)))
    return "machine_type not a string or longer than 31";
  if (!get_int(root, "machine_id", &m->machine_id))
    return "machine_id missing or not an integer";
  if (!get_str(root, "batch_id", m->batch_id, sizeof(m->batch_id)))
    return "batch_id not a string or longer than 63";
  if (!get_str(root, "ts_utc", m->ts_utc, sizeof(m->ts_utc)))
    return "ts_utc not a string or longer than 31";
  return NULL;
}

mk_result_t mk_decode(const char *json, size_t len, mk_message_t *out) {
  if (json == NULL || out == NULL)
    return MK_ERR_ARG;

  mk_message_t m;
  memset(&m, 0, sizeof(m));
  cJSON *root = cJSON_ParseWithLength(json, len);
  char type[MK_NAME_LEN] = "";
  const char *why = NULL;
  if (root == NULL || !cJSON_IsObject(root))
    why = "not a JSON object";
  else
    why = decode_common(root, &m, type, sizeof(type));

  if (why == NULL) {
    why = "unknown type";
    for (size_t i = 0; i < N_TYPES; ++i) {
      if (strcmp(type, TYPE_NAMES[i]) == 0) {
        m.type = (mk_type_t)i;
        why = NULL;
      }
    }
  }

  /* Type-specific fields are only read for their type; elsewhere they are
   * ignored like unknown fields. */
  if (why == NULL && m.type == MK_START && !get_params(root, &m))
    why = "params not an object of numbers, or more than 16";
  if (why == NULL && m.type == MK_ERROR) {
    const cJSON *error = cJSON_GetObjectItemCaseSensitive(root, "error");
    if (!cJSON_IsObject(error) ||
        !get_str(error, "code", m.error_code, sizeof(m.error_code)) ||
        !get_str(error, "reason", m.error_reason, sizeof(m.error_reason)))
      why = "error must be {code, reason} strings of max 31/127";
  }
  cJSON_Delete(root);

  if (why == NULL && m.message_id[0] == '\0')
    why = "message_id is empty";
  if (why == NULL)
    why = check_body(&m);
  if (why != NULL)
    return MK_ERR_DECODE;
  *out = m;
  return MK_OK;
}
