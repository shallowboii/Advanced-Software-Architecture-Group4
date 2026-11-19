/* Message encode/decode, status helpers and params. No broker needed. */
#include "machine_kafka.h"

#include <assert.h>
#include <float.h>
#include <math.h>
#include <stdio.h>
#include <string.h>

static mk_message_t make_sent_start(void) {
  mk_message_t start;
  assert(mk_make_start(&start, "boiling", 3, "B-0007") == MK_OK);
  strcpy(start.message_id, "scheduler-1-1");
  strcpy(start.ts_utc, "2026-11-18T12:00:00Z");
  return start;
}

static mk_message_t round_trip(const mk_message_t *in) {
  char buf[MK_MAX_JSON_LEN];
  int len = mk_encode(in, buf, sizeof(buf));
  assert(len > 0 && (size_t)len == strlen(buf));
  mk_message_t out;
  assert(mk_decode(buf, (size_t)len, &out) == MK_OK);
  return out;
}

static void expect_decode_fails(const char *json) {
  mk_message_t out;
  if (mk_decode(json, strlen(json), &out) != MK_ERR_DECODE) {
    fprintf(stderr, "accepted bad input: %s\n", json);
    assert(0);
  }
}

static void test_machine_types(void) {
  /* Every listed type is a valid topic / machine_type. */
  const char *types[] = {MK_MACHINE_TYPES};
  assert(sizeof(types) / sizeof(types[0]) == 6);
  mk_message_t idle;
  for (size_t i = 0; i < 6; ++i)
    assert(mk_make_idle(&idle, types[i], 1) == MK_OK);
  assert(mk_make_idle(&idle, "a.b", 1) == MK_ERR_ARG);
  assert(mk_make_idle(&idle, "Boiling", 1) == MK_ERR_ARG);
  assert(mk_is_command(MK_START) && mk_is_command(MK_FLUSH));
  assert(!mk_is_command(MK_IDLE) && !mk_is_command(MK_ERROR));
}

static void test_start_round_trip(void) {
  mk_message_t start = make_sent_start();
  mk_message_t out = round_trip(&start);
  assert(memcmp(&start, &out, sizeof(out)) == 0);
}

static void test_params(void) {
  mk_message_t start = make_sent_start();
  assert(mk_param_set(&start, "target_temp_c", 100.0) == MK_OK);
  assert(mk_param_set(&start, "duration_s", 3600) == MK_OK);
  assert(mk_param_set(&start, "hops_g", 0.1) == MK_OK);
  assert(mk_param_set(&start, "hops_g", 250.5) == MK_OK); /* replaces */
  assert(start.n_params == 3);

  mk_message_t out = round_trip(&start);
  assert(memcmp(&start, &out, sizeof(out)) == 0); /* exact doubles */
  double v = 0;
  assert(mk_param_get(&out, "hops_g", &v) == 1 && v == 250.5);
  assert(mk_param_get(&out, "yeast_g", &v) == 0);

  char buf[MK_MAX_JSON_LEN];
  assert(mk_encode(&start, buf, sizeof(buf)) > 0);
  assert(strstr(buf, "\"params\":{\"target_temp_c\":100,\"duration_s\":3600,"
                     "\"hops_g\":250.5}") != NULL);

  /* Bad names and values are rejected, too many params too. */
  assert(mk_param_set(&start, "", 1) == MK_ERR_ARG);
  assert(mk_param_set(&start, "with space", 1) == MK_ERR_ARG);
  assert(mk_param_set(&start, "a-very-long-parameter-name-over-32", 1) ==
         MK_ERR_ARG);
  assert(mk_param_set(&start, "x", NAN) == MK_ERR_ARG);
  assert(mk_param_set(&start, "x", INFINITY) == MK_ERR_ARG);
  char name[16];
  for (int i = (int)start.n_params; i < MK_MAX_PARAMS; ++i) {
    snprintf(name, sizeof(name), "p%d", i);
    assert(mk_param_set(&start, name, i) == MK_OK);
  }
  assert(mk_param_set(&start, "one_too_many", 1) == MK_ERR_ARG);
  assert(start.n_params == MK_MAX_PARAMS);
  out = round_trip(&start);
  assert(out.n_params == MK_MAX_PARAMS);

  /* Only starts carry params. */
  mk_message_t done;
  assert(mk_make_done(&done, &start) == MK_OK);
  assert(done.n_params == 0);
  assert(mk_param_set(&done, "x", 1) == MK_ERR_ARG);

  const char *prefix =
      "{\"schema_version\":3,\"message_id\":\"a\",\"type\":\"start\","
      "\"machine_type\":\"boiling\",\"machine_id\":1,\"batch_id\":\"B\","
      "\"params\":";
  const char *bad_params[] = {
      "[1,2]}",                         /* not an object */
      "{\"t\":\"100\"}}",               /* not a number */
      "{\"t\":1e999}}",                 /* not finite */
      "{\"t\":1,\"t\":2}}",             /* duplicate name */
      "{\"bad name\":1}}",              /* invalid name */
      "{\"p0\":0,\"p1\":1,\"p2\":2,\"p3\":3,\"p4\":4,\"p5\":5,\"p6\":6,"
      "\"p7\":7,\"p8\":8,\"p9\":9,\"pa\":10,\"pb\":11,\"pc\":12,\"pd\":13,"
      "\"pe\":14,\"pf\":15,\"pg\":16}}", /* MK_MAX_PARAMS + 1 */
  };
  char json[1024];
  for (size_t i = 0; i < sizeof(bad_params) / sizeof(bad_params[0]); ++i) {
    snprintf(json, sizeof(json), "%s%s", prefix, bad_params[i]);
    expect_decode_fails(json);
  }
  snprintf(json, sizeof(json), "%snull}", prefix);
  mk_message_t m;
  assert(mk_decode(json, strlen(json), &m) == MK_OK && m.n_params == 0);
}

static void test_status_messages(void) {
  mk_message_t start = make_sent_start();

  mk_message_t idle;
  assert(mk_make_idle(&idle, "boiling", 3) == MK_OK);
  strcpy(idle.message_id, "boiling-3-0");
  mk_message_t out = round_trip(&idle);
  assert(memcmp(&idle, &out, sizeof(out)) == 0);
  assert(out.type == MK_IDLE && out.batch_id[0] == '\0');

  mk_message_t running;
  assert(mk_make_running(&running, &start) == MK_OK);
  assert(running.type == MK_RUNNING && strcmp(running.batch_id, "B-0007") == 0);
  strcpy(running.message_id, "boiling-3-1");
  out = round_trip(&running);
  assert(memcmp(&running, &out, sizeof(out)) == 0);

  mk_message_t done;
  assert(mk_make_done(&done, &start) == MK_OK);
  assert(done.type == MK_DONE && done.machine_id == 3);
  assert(strcmp(done.machine_type, "boiling") == 0);
  assert(strcmp(done.batch_id, "B-0007") == 0);
  assert(done.message_id[0] == '\0'); /* filled in by mk_send */
  strcpy(done.message_id, "boiling-3-2");
  out = round_trip(&done);
  assert(memcmp(&done, &out, sizeof(out)) == 0);

  mk_message_t error;
  assert(mk_make_error(&error, &start, "BUSY", "already boiling") == MK_OK);
  strcpy(error.message_id, "boiling-3-3");
  out = round_trip(&error);
  assert(out.type == MK_ERROR && strcmp(out.batch_id, "B-0007") == 0);
  assert(strcmp(out.error_code, "BUSY") == 0);
  assert(strcmp(out.error_reason, "already boiling") == 0);

  /* Statuses answer commands only; an error needs a valid code. */
  assert(mk_make_done(&done, &done) == MK_ERR_ARG); /* not a start */
  assert(mk_make_running(&running, &idle) == MK_ERR_ARG);
  assert(mk_make_error(&error, &start, "", NULL) == MK_ERR_ARG);
  assert(mk_make_error(&error, &start, "BAD CODE", NULL) == MK_ERR_ARG);
}

static void test_stop_and_flush(void) {
  mk_message_t stop, flush;
  assert(mk_make_command(&stop, MK_STOP, "boiling", 3, NULL) == MK_OK);
  strcpy(stop.message_id, "s-1");
  mk_message_t out = round_trip(&stop);
  assert(out.type == MK_STOP && out.batch_id[0] == '\0');

  assert(mk_make_command(&flush, MK_FLUSH, "boiling", 3, NULL) ==
         MK_ERR_ARG); /* flush needs a batch */
  assert(mk_make_command(&flush, MK_FLUSH, "boiling", 3, "B-1") == MK_OK);
  strcpy(flush.message_id, "s-2");
  out = round_trip(&flush);
  assert(out.type == MK_FLUSH && strcmp(out.batch_id, "B-1") == 0);

  /* A machine that does not implement them yet answers with an error. */
  mk_message_t error;
  assert(mk_make_error(&error, &flush, "UNSUPPORTED", "flush") == MK_OK);
  assert(strcmp(error.batch_id, "B-1") == 0);
  assert(mk_make_command(&stop, MK_DONE, "boiling", 3, "B") == MK_ERR_ARG);
}

static void test_json_shape(void) {
  mk_message_t start = make_sent_start();
  char buf[MK_MAX_JSON_LEN];
  assert(mk_encode(&start, buf, sizeof(buf)) > 0);
  assert(strcmp(buf,
                "{\"schema_version\":3,\"message_id\":\"scheduler-1-1\","
                "\"type\":\"start\",\"machine_type\":\"boiling\","
                "\"machine_id\":3,\"batch_id\":\"B-0007\","
                "\"ts_utc\":\"2026-11-18T12:00:00Z\",\"params\":{},"
                "\"error\":null}") == 0);
}

static void test_decode_rejects_bad_input(void) {
  const char *bad[] = {
      "",
      "not json",
      "[]",
      /* old schema version */
      "{\"schema_version\":2,\"message_id\":\"a\",\"type\":\"start\","
      "\"machine_type\":\"boiling\",\"machine_id\":1,\"batch_id\":\"B\","
      "\"step\":\"s\"}",
      /* unknown type */
      "{\"schema_version\":3,\"message_id\":\"a\",\"type\":\"pause\","
      "\"machine_type\":\"boiling\",\"machine_id\":1,\"batch_id\":\"B\"}",
      /* start / done / flush without batch */
      "{\"schema_version\":3,\"message_id\":\"a\",\"type\":\"start\","
      "\"machine_type\":\"boiling\",\"machine_id\":1}",
      "{\"schema_version\":3,\"message_id\":\"a\",\"type\":\"done\","
      "\"machine_type\":\"boiling\",\"machine_id\":1,\"batch_id\":null}",
      "{\"schema_version\":3,\"message_id\":\"a\",\"type\":\"flush\","
      "\"machine_type\":\"boiling\",\"machine_id\":1}",
      /* error without code */
      "{\"schema_version\":3,\"message_id\":\"a\",\"type\":\"error\","
      "\"machine_type\":\"boiling\",\"machine_id\":1,"
      "\"error\":{\"reason\":\"r\"}}",
      /* machine_id not a positive integer */
      "{\"schema_version\":3,\"message_id\":\"a\",\"type\":\"idle\","
      "\"machine_type\":\"boiling\",\"machine_id\":1.5}",
      "{\"schema_version\":3,\"message_id\":\"a\",\"type\":\"idle\","
      "\"machine_type\":\"boiling\",\"machine_id\":0}",
      "{\"schema_version\":3,\"message_id\":\"a\",\"type\":\"idle\","
      "\"machine_type\":\"boiling\",\"machine_id\":\"1\"}",
      /* missing message_id */
      "{\"schema_version\":3,\"type\":\"idle\","
      "\"machine_type\":\"boiling\",\"machine_id\":1}",
      /* machine_type is not usable as a topic name */
      "{\"schema_version\":3,\"message_id\":\"a\",\"type\":\"idle\","
      "\"machine_type\":\"boil ing\",\"machine_id\":1}",
      "{\"schema_version\":3,\"message_id\":\"a\",\"type\":\"idle\","
      "\"machine_type\":\"boil.ing\",\"machine_id\":1}",
      /* identifier with characters outside [A-Za-z0-9._:-] */
      "{\"schema_version\":3,\"message_id\":\"a b\",\"type\":\"idle\","
      "\"machine_type\":\"boiling\",\"machine_id\":1}",
      "{\"schema_version\":3,\"message_id\":\"a\",\"type\":\"start\","
      "\"machine_type\":\"boiling\",\"machine_id\":1,\"batch_id\":\"B\\\"\"}",
      /* batch_id too long for the buffer */
      "{\"schema_version\":3,\"message_id\":\"a\",\"type\":\"start\","
      "\"machine_type\":\"boiling\",\"machine_id\":1,"
      "\"batch_id\":"
      "\"0123456789012345678901234567890123456789012345678901234567890123"
      "456789\"}",
  };
  for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); ++i)
    expect_decode_fails(bad[i]);
}

static void test_decode_ignores_unknown_fields(void) {
  /* Unknown fields, including the removed v2 ones, are ignored. */
  const char *json =
      "{\"schema_version\":3,\"message_id\":\"a\",\"type\":\"start\","
      "\"machine_type\":\"boiling\",\"machine_id\":2,\"batch_id\":\"B\","
      "\"step\":\"boiling\",\"reply_to\":\"x\",\"recipe\":\"ale\"}";
  mk_message_t out;
  assert(mk_decode(json, strlen(json), &out) == MK_OK);
  assert(out.machine_id == 2 && strcmp(out.batch_id, "B") == 0);
  assert(out.ts_utc[0] == '\0');

  /* Type-specific fields on other types are ignored too. */
  const char *idle =
      "{\"schema_version\":3,\"message_id\":\"a\",\"type\":\"idle\","
      "\"machine_type\":\"boiling\",\"machine_id\":2,\"params\":{\"x\":1},"
      "\"progress\":5,\"error\":{\"code\":\"X\"}}";
  assert(mk_decode(idle, strlen(idle), &out) == MK_OK);
  assert(out.n_params == 0 && out.error_code[0] == '\0');
}

static void test_encode_rejects_bad_messages(void) {
  mk_message_t start = make_sent_start();
  char buf[MK_MAX_JSON_LEN];

  mk_message_t no_id = start;
  no_id.message_id[0] = '\0';
  assert(mk_encode(&no_id, buf, sizeof(buf)) == MK_ERR_ENCODE);

  assert(mk_encode(&start, buf, 10) == MK_ERR_ENCODE); /* too small */
  assert(mk_encode(NULL, buf, sizeof(buf)) == MK_ERR_ARG);

  mk_message_t m;
  assert(mk_make_start(&m, "boiling", 0, "B") == MK_ERR_ARG);
  assert(mk_make_start(&m, "boiling", 1, "") == MK_ERR_ARG);
  assert(mk_make_start(&m, "boiling", 1, NULL) == MK_ERR_ARG);
  assert(mk_make_start(&m, "boiling", 1, "B 1") == MK_ERR_ARG);
  assert(mk_make_start(&m, "a/b", 1, "B") == MK_ERR_ARG);
  assert(mk_make_start(&m, "a-very-long-machine-type-name-over-32", 1, "B") ==
         MK_ERR_ARG);
}

static void test_escaping(void) {
  mk_message_t start = make_sent_start();
  mk_message_t error;
  assert(mk_make_error(&error, &start, "FAULT", "quote \" and \\ \n") ==
         MK_OK);
  strcpy(error.message_id, "x");
  mk_message_t out = round_trip(&error);
  assert(strcmp(out.error_reason, "quote \" and \\ \n") == 0);
}

static void fill(char *dst, size_t len, char c) {
  memset(dst, c, len - 1);
  dst[len - 1] = '\0';
}

/* The largest possible messages must fit MK_MAX_JSON_LEN: every string at
 * its maximum length, the reason made only of characters JSON escapes as
 * \u00XX (6 bytes each), all params with the longest number cJSON prints. */
static void test_max_size(void) {
  mk_message_t start;
  assert(mk_make_start(&start, "boiling", 1, "B") == MK_OK);
  fill(start.machine_type, MK_NAME_LEN, 'm');
  start.machine_id = 2147483647;
  fill(start.batch_id, MK_BATCH_LEN, 'b');
  fill(start.message_id, MK_ID_LEN, 'i');
  fill(start.ts_utc, MK_TS_LEN, 't');
  for (int i = 0; i < MK_MAX_PARAMS; ++i) {
    fill(start.params[i].name, MK_NAME_LEN, 'p');
    start.params[i].name[0] = (char)('A' + i);
    start.params[i].value = -DBL_MAX / 3; /* 17 significant digits */
  }
  start.n_params = MK_MAX_PARAMS;

  mk_message_t error = start;
  error.type = MK_ERROR;
  error.n_params = 0;
  memset(error.params, 0, sizeof(error.params));
  fill(error.error_code, MK_NAME_LEN, 'c');
  fill(error.error_reason, MK_REASON_LEN, '\x01');

  const mk_message_t *all[] = {&start, &error};
  char buf[MK_MAX_JSON_LEN];
  for (size_t i = 0; i < 2; ++i) {
    int len = mk_encode(all[i], buf, sizeof(buf));
    assert(len > 0);
    printf("  largest %s: %d bytes (limit %d)\n", mk_type_name(all[i]->type),
           len, MK_MAX_JSON_LEN);
    mk_message_t out;
    assert(mk_decode(buf, (size_t)len, &out) == MK_OK);
    assert(memcmp(all[i], &out, sizeof(out)) == 0);
  }
}

int main(void) {
  test_machine_types();
  test_start_round_trip();
  test_params();
  test_status_messages();
  test_stop_and_flush();
  test_json_shape();
  test_decode_rejects_bad_input();
  test_decode_ignores_unknown_fields();
  test_encode_rejects_bad_messages();
  test_escaping();
  test_max_size();
  puts("machine_kafka: all message tests passed");
  return 0;
}
