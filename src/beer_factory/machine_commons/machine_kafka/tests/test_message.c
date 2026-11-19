/* Message encode/decode, reply helpers and dedup. No broker needed. */
#include "machine_kafka.h"

#include <assert.h>
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

static void test_start_round_trip(void) {
  mk_message_t start = make_sent_start();
  mk_message_t out = round_trip(&start);
  assert(memcmp(&start, &out, sizeof(out)) == 0);
}

static void test_done_and_error_replies(void) {
  mk_message_t start = make_sent_start();

  mk_message_t done;
  assert(mk_make_done(&done, &start) == MK_OK);
  assert(done.type == MK_DONE && done.machine_id == 3);
  assert(strcmp(done.reply_to, "scheduler-1-1") == 0);
  assert(strcmp(done.batch_id, "B-0007") == 0);
  strcpy(done.message_id, "boiling-3-1");
  mk_message_t out = round_trip(&done);
  assert(memcmp(&done, &out, sizeof(out)) == 0);

  mk_message_t error;
  assert(mk_make_error(&error, &start, "BUSY", "already boiling") == MK_OK);
  strcpy(error.message_id, "boiling-3-2");
  out = round_trip(&error);
  assert(out.type == MK_ERROR);
  assert(strcmp(out.error_code, "BUSY") == 0);
  assert(strcmp(out.error_reason, "already boiling") == 0);

  /* A reply needs a start that was actually sent (has an id). */
  mk_message_t unsent;
  assert(mk_make_start(&unsent, "boiling", 3, "B-1") == MK_OK);
  assert(mk_make_done(&done, &unsent) == MK_ERR_ARG);
  assert(mk_make_done(&done, &done) == MK_ERR_ARG); /* not a start */
  assert(mk_make_error(&error, &start, "", NULL) == MK_ERR_ARG);
}

static void test_json_shape(void) {
  mk_message_t start = make_sent_start();
  char buf[MK_MAX_JSON_LEN];
  assert(mk_encode(&start, buf, sizeof(buf)) > 0);
  assert(strstr(buf, "\"type\":\"start\"") != NULL);
  assert(strstr(buf, "\"machine_id\":3") != NULL);
  assert(strstr(buf, "\"reply_to\":null") != NULL);
  assert(strstr(buf, "\"error\":null") != NULL);
  assert(strstr(buf, "\"schema_version\":1") != NULL);
}

static void test_decode_rejects_bad_input(void) {
  const char *bad[] = {
      "",
      "not json",
      "[]",
      /* wrong schema version */
      "{\"schema_version\":2,\"message_id\":\"a\",\"type\":\"start\","
      "\"machine_type\":\"boiling\",\"machine_id\":1,\"batch_id\":\"B\"}",
      /* unknown type */
      "{\"schema_version\":1,\"message_id\":\"a\",\"type\":\"pause\","
      "\"machine_type\":\"boiling\",\"machine_id\":1,\"batch_id\":\"B\"}",
      /* start without batch */
      "{\"schema_version\":1,\"message_id\":\"a\",\"type\":\"start\","
      "\"machine_type\":\"boiling\",\"machine_id\":1}",
      /* done without reply_to */
      "{\"schema_version\":1,\"message_id\":\"a\",\"type\":\"done\","
      "\"machine_type\":\"boiling\",\"machine_id\":1}",
      /* error without code */
      "{\"schema_version\":1,\"message_id\":\"a\",\"type\":\"error\","
      "\"machine_type\":\"boiling\",\"machine_id\":1,\"reply_to\":\"x\","
      "\"error\":{\"reason\":\"r\"}}",
      /* machine_id not a positive integer */
      "{\"schema_version\":1,\"message_id\":\"a\",\"type\":\"start\","
      "\"machine_type\":\"boiling\",\"machine_id\":1.5,\"batch_id\":\"B\"}",
      "{\"schema_version\":1,\"message_id\":\"a\",\"type\":\"start\","
      "\"machine_type\":\"boiling\",\"machine_id\":0,\"batch_id\":\"B\"}",
      "{\"schema_version\":1,\"message_id\":\"a\",\"type\":\"start\","
      "\"machine_type\":\"boiling\",\"machine_id\":\"1\",\"batch_id\":\"B\"}",
      /* missing message_id */
      "{\"schema_version\":1,\"type\":\"start\","
      "\"machine_type\":\"boiling\",\"machine_id\":1,\"batch_id\":\"B\"}",
      /* machine_type is not a valid topic name */
      "{\"schema_version\":1,\"message_id\":\"a\",\"type\":\"start\","
      "\"machine_type\":\"boil ing\",\"machine_id\":1,\"batch_id\":\"B\"}",
      /* batch_id too long for the buffer */
      "{\"schema_version\":1,\"message_id\":\"a\",\"type\":\"start\","
      "\"machine_type\":\"boiling\",\"machine_id\":1,\"batch_id\":"
      "\"0123456789012345678901234567890123456789012345678901234567890123"
      "456789\"}",
  };
  for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); ++i) {
    mk_message_t out;
    if (mk_decode(bad[i], strlen(bad[i]), &out) != MK_ERR_DECODE) {
      fprintf(stderr, "accepted bad input %zu: %s\n", i, bad[i]);
      assert(0);
    }
  }
}

static void test_decode_ignores_unknown_fields(void) {
  const char *json =
      "{\"schema_version\":1,\"message_id\":\"a\",\"type\":\"start\","
      "\"machine_type\":\"boiling\",\"machine_id\":2,\"batch_id\":\"B\","
      "\"recipe\":\"ale\"}";
  mk_message_t out;
  assert(mk_decode(json, strlen(json), &out) == MK_OK);
  assert(out.machine_id == 2 && strcmp(out.batch_id, "B") == 0);
  assert(out.reply_to[0] == '\0' && out.ts_utc[0] == '\0');
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

static void test_dedup(void) {
  mk_dedup_t dedup;
  mk_dedup_init(&dedup);
  assert(mk_dedup_seen(&dedup, "a") == 0);
  assert(mk_dedup_seen(&dedup, "a") == 1);
  assert(mk_dedup_seen(&dedup, "b") == 0);
  assert(mk_dedup_seen(&dedup, "") == 0);
  assert(mk_dedup_seen(&dedup, "") == 0);

  /* Old ids are forgotten after MK_DEDUP_SIZE newer ones. */
  char id[16];
  for (int i = 0; i < MK_DEDUP_SIZE; ++i) {
    snprintf(id, sizeof(id), "n%d", i);
    assert(mk_dedup_seen(&dedup, id) == 0);
  }
  assert(mk_dedup_seen(&dedup, "a") == 0);
  snprintf(id, sizeof(id), "n%d", MK_DEDUP_SIZE - 1);
  assert(mk_dedup_seen(&dedup, id) == 1);
}

int main(void) {
  test_start_round_trip();
  test_done_and_error_replies();
  test_json_shape();
  test_decode_rejects_bad_input();
  test_decode_ignores_unknown_fields();
  test_encode_rejects_bad_messages();
  test_escaping();
  test_dedup();
  puts("machine_kafka: all message tests passed");
  return 0;
}
