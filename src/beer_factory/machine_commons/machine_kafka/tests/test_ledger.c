/* Ledger: duplicate starts and restarts. No broker needed; uses a state file
 * in build/ (or $MK_TEST_DIR). */
#include "machine_kafka.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static char path[MK_PATH_LEN];

/* A start as the machine receives it; every resend gets a new message_id,
 * like a restarted scheduler would produce. */
static mk_message_t start_for(const char *batch, const char *message_id) {
  mk_message_t start;
  assert(mk_make_start(&start, "boiling", 3, batch) == MK_OK);
  strcpy(start.message_id, message_id);
  return start;
}

static void test_duplicate_with_new_message_id(void) {
  mk_ledger_t ledger;
  assert(mk_ledger_open(&ledger, NULL, "boiling", 3) == MK_OK);
  mk_message_t first = start_for("B-1", "sched-a-1");
  mk_message_t again = start_for("B-1", "sched-b-7");
  mk_message_t resend;

  assert(mk_ledger_lookup(&ledger, &first, &resend) == 0);
  assert(mk_ledger_accept(&ledger, &first) == MK_OK);
  assert(mk_ledger_accept(&ledger, &first) == MK_ERR_ARG);

  /* Same batch, different message_id: still running. */
  assert(mk_ledger_lookup(&ledger, &again, &resend) == 1);
  assert(resend.type == MK_RUNNING && strcmp(resend.batch_id, "B-1") == 0);

  /* Finished: duplicates now get done. */
  mk_message_t done;
  assert(mk_make_done(&done, &first) == MK_OK);
  assert(mk_ledger_record(&ledger, &done) == MK_OK);
  assert(mk_ledger_lookup(&ledger, &again, &resend) == 1);
  assert(resend.type == MK_DONE && strcmp(resend.batch_id, "B-1") == 0);

  /* Another batch is new. */
  mk_message_t other = start_for("B-2", "sched-b-9");
  assert(mk_ledger_lookup(&ledger, &other, &resend) == 0);

  /* A refused start (busy) is remembered by its message_id only: a
   * redelivery of that message gets BUSY again, a new attempt is evaluated
   * again and can then be accepted. */
  mk_message_t busy;
  assert(mk_ledger_refuse(&ledger, &other, "BUSY", "B-1", &busy) == MK_OK);
  assert(busy.type == MK_ERROR && strcmp(busy.error_code, "BUSY") == 0);
  assert(strcmp(busy.batch_id, "B-2") == 0);
  assert(mk_ledger_lookup(&ledger, &other, &resend) == 1);
  assert(resend.type == MK_ERROR && strcmp(resend.error_code, "BUSY") == 0);
  mk_message_t retry = start_for("B-2", "sched-b-10");
  assert(mk_ledger_lookup(&ledger, &retry, &resend) == 0);
  assert(mk_ledger_accept(&ledger, &retry) == MK_OK);
  assert(mk_ledger_lookup(&ledger, &other, &resend) == 1);
  assert(resend.type == MK_RUNNING); /* accepted batch wins over refusal */
  /* An accepted batch cannot be refused any more. */
  assert(mk_ledger_refuse(&ledger, &retry, "BUSY", "x", &busy) == MK_ERR_ARG);

  /* Starts for another machine are refused. */
  mk_message_t foreign;
  assert(mk_make_start(&foreign, "boiling", 4, "B-1") == MK_OK);
  strcpy(foreign.message_id, "x");
  assert(mk_ledger_lookup(&ledger, &foreign, &resend) == MK_ERR_ARG);
  assert(mk_ledger_accept(&ledger, &foreign) == MK_ERR_ARG);
  assert(mk_ledger_refuse(&ledger, &foreign, "BUSY", "", &busy) == MK_ERR_ARG);
}

static void test_restart(void) {
  remove(path);
  mk_ledger_t ledger;
  assert(mk_ledger_open(&ledger, path, "boiling", 3) == MK_OK);
  assert(ledger.count == 0);

  /* B-1 finished, B-2 failed, B-3 was running when the process died. */
  mk_message_t s1 = start_for("B-1", "a-1");
  mk_message_t s2 = start_for("B-2", "a-2");
  mk_message_t s3 = start_for("B-3", "a-3");
  mk_message_t status;
  assert(mk_ledger_accept(&ledger, &s1) == MK_OK);
  assert(mk_make_done(&status, &s1) == MK_OK);
  assert(mk_ledger_record(&ledger, &status) == MK_OK);
  assert(mk_ledger_accept(&ledger, &s2) == MK_OK);
  assert(mk_make_error(&status, &s2, "OVERHEAT",
                       "sensor 2\tat 120 C\nstopped") == MK_OK);
  assert(mk_ledger_record(&ledger, &status) == MK_OK);
  assert(mk_ledger_accept(&ledger, &s3) == MK_OK);
  mk_message_t s4 = start_for("B-4", "a-4");
  assert(mk_ledger_refuse(&ledger, &s4, "BUSY", "B-3", &status) == MK_OK);
  /* ...crash: nothing else is called. */

  /* New process: reload the state file. */
  mk_ledger_t reloaded;
  assert(mk_ledger_open(&reloaded, path, "boiling", 3) == MK_OK);
  assert(reloaded.count == 4);

  mk_message_t resend;
  /* The refused start, redelivered after a lost commit: refused again. */
  assert(mk_ledger_lookup(&reloaded, &s4, &resend) == 1);
  assert(resend.type == MK_ERROR && strcmp(resend.error_code, "BUSY") == 0);
  mk_message_t again1 = start_for("B-1", "b-1");
  assert(mk_ledger_lookup(&reloaded, &again1, &resend) == 1);
  assert(resend.type == MK_DONE);

  mk_message_t again2 = start_for("B-2", "b-2");
  assert(mk_ledger_lookup(&reloaded, &again2, &resend) == 1);
  assert(resend.type == MK_ERROR);
  assert(strcmp(resend.error_code, "OVERHEAT") == 0);
  assert(strcmp(resend.error_reason, "sensor 2 at 120 C stopped") == 0);

  /* The interrupted batch is never run again: it is reported once at
   * startup, and later duplicates get the same error. */
  assert(mk_ledger_next_interrupted(&reloaded, &status) == 1);
  assert(status.type == MK_ERROR && strcmp(status.batch_id, "B-3") == 0);
  assert(strcmp(status.error_code, "INTERRUPTED") == 0);
  assert(mk_ledger_next_interrupted(&reloaded, &status) == 0);
  mk_message_t again3 = start_for("B-3", "b-3");
  assert(mk_ledger_lookup(&reloaded, &again3, &resend) == 1);
  assert(resend.type == MK_ERROR &&
         strcmp(resend.error_code, "INTERRUPTED") == 0);

  /* A second restart does not report it again. */
  assert(mk_ledger_open(&reloaded, path, "boiling", 3) == MK_OK);
  assert(mk_ledger_next_interrupted(&reloaded, &status) == 0);

  /* The file belongs to boiling 3 only. */
  assert(mk_ledger_open(&reloaded, path, "boiling", 4) == MK_ERR_ARG);
  assert(mk_ledger_open(&reloaded, path, "mashing", 3) == MK_ERR_ARG);
}

static void test_eviction(void) {
  remove(path);
  mk_ledger_t ledger;
  assert(mk_ledger_open(&ledger, path, "boiling", 3) == MK_OK);
  char batch[16], id[16];
  for (int i = 0; i < MK_LEDGER_SIZE + 2; ++i) {
    snprintf(batch, sizeof(batch), "B-%d", i);
    snprintf(id, sizeof(id), "m-%d", i);
    mk_message_t s = start_for(batch, id), done;
    assert(mk_ledger_accept(&ledger, &s) == MK_OK);
    assert(mk_make_done(&done, &s) == MK_OK);
    assert(mk_ledger_record(&ledger, &done) == MK_OK);
  }
  assert(ledger.count == MK_LEDGER_SIZE);
  mk_ledger_t reloaded;
  assert(mk_ledger_open(&reloaded, path, "boiling", 3) == MK_OK);
  assert(reloaded.count == MK_LEDGER_SIZE);
  mk_message_t resend, oldest = start_for("B-0", "x"),
                       newest = start_for("B-17", "y");
  assert(mk_ledger_lookup(&reloaded, &oldest, &resend) == 0); /* forgotten */
  assert(mk_ledger_lookup(&reloaded, &newest, &resend) == 1);
}

static void write_file(const char *content) {
  FILE *f = fopen(path, "w");
  assert(f != NULL);
  fputs(content, f);
  fclose(f);
}

static void test_corrupt_files(void) {
  mk_ledger_t ledger;
  const char *corrupt[] = {
      "",
      "garbage\n",
      "mk-ledger 1 boiling 3\nD B-1 boiling a-1 - \n", /* old format */
      "mk-ledger 2 boiling 3\nQ B-1 a-1 - \n",
      "mk-ledger 2 boiling 3\nD B-1\n",
      "mk-ledger 2 boiling 3\nD B-1 a-1 - no newline",
  };
  for (size_t i = 0; i < sizeof(corrupt) / sizeof(corrupt[0]); ++i) {
    write_file(corrupt[i]);
    assert(mk_ledger_open(&ledger, path, "boiling", 3) == MK_ERR_IO);
  }
  write_file("mk-ledger 2 boiling 3\nD B-1 a-1 - \n");
  assert(mk_ledger_open(&ledger, path, "boiling", 3) == MK_OK);
  assert(ledger.count == 1 && ledger.entries[0].state == MK_DONE);
  remove(path);
}

int main(void) {
  const char *dir = getenv("MK_TEST_DIR");
  snprintf(path, sizeof(path), "%s/test-ledger.state", dir ? dir : "build");
  test_duplicate_with_new_message_id();
  test_restart();
  test_eviction();
  test_corrupt_files();
  remove(path);
  puts("machine_kafka: all ledger tests passed");
  return 0;
}
