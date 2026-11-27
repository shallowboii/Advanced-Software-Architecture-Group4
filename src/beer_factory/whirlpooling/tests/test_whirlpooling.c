#include "whirlpooling.h"

#include <stdio.h>
#include <stdlib.h>

/* Keep checks enabled even when a release build defines NDEBUG. */
#define CHECK(condition)                                                       \
  do {                                                                         \
    if (!(condition)) {                                                        \
      fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #condition);          \
      exit(EXIT_FAILURE);                                                      \
    }                                                                          \
  } while (0)

static void test_start_guards(void) {
  whirlpooling_batches_t batches = {0, 0};
  whirlpooling_t w;

  CHECK(whirlpooling_init(&w, &batches) == WHIRLPOOLING_OK);
  CHECK(w.state == WHIRLPOOLING_WAIT);

  /* Guard has_hot_wort() fails: nothing changes. */
  CHECK(whirlpooling_start(&w, 10) == WHIRLPOOLING_NO_HOT_WORT);
  CHECK(w.state == WHIRLPOOLING_WAIT);
  CHECK(batches.hot_wort_batches == 0 && batches.clarified_wort_batches == 0);

  batches.hot_wort_batches = 2;
  CHECK(whirlpooling_start(&w, 10) == WHIRLPOOLING_STARTED);
  CHECK(w.state == WHIRLPOOLING_WHIRLPOOL);
  CHECK(batches.hot_wort_batches == 1);

  /* Already whirlpooling: a second start is refused and takes nothing. */
  CHECK(whirlpooling_start(&w, 100) == WHIRLPOOLING_BUSY);
  CHECK(w.started_at_seconds == 10);
  CHECK(batches.hot_wort_batches == 1);
}

static void test_no_output_limit(void) {
  /* The UPPAAL model has no output-full guard: it starts even if
   * clarified wort is already waiting downstream. */
  whirlpooling_batches_t batches = {1, 5};
  whirlpooling_t w;

  CHECK(whirlpooling_init(&w, &batches) == WHIRLPOOLING_OK);
  CHECK(whirlpooling_start(&w, 0) == WHIRLPOOLING_STARTED);
  CHECK(whirlpooling_update(&w, WHIRLPOOLING_DURATION_SECONDS) ==
        WHIRLPOOLING_DONE);
  CHECK(batches.clarified_wort_batches == 6);
}

static void test_deadline_and_single_completion(void) {
  whirlpooling_batches_t batches = {1, 0};
  whirlpooling_t w;

  CHECK(whirlpooling_init(&w, &batches) == WHIRLPOOLING_OK);
  CHECK(whirlpooling_update(&w, 100) == WHIRLPOOLING_NO_CHANGE); /* idle */
  CHECK(whirlpooling_start(&w, 100) == WHIRLPOOLING_STARTED);
  CHECK(whirlpooling_update(&w, 100) == WHIRLPOOLING_NO_CHANGE);
  CHECK(whirlpooling_update(&w, 3699) == WHIRLPOOLING_NO_CHANGE);
  CHECK(w.state == WHIRLPOOLING_WHIRLPOOL);
  CHECK(batches.clarified_wort_batches == 0);

  CHECK(whirlpooling_update(&w, 3700) == WHIRLPOOLING_DONE);
  CHECK(w.state == WHIRLPOOLING_WAIT);
  CHECK(batches.hot_wort_batches == 0 && batches.clarified_wort_batches == 1);

  /* Done is reported exactly once. */
  CHECK(whirlpooling_update(&w, 3700) == WHIRLPOOLING_NO_CHANGE);
  CHECK(whirlpooling_update(&w, 9000) == WHIRLPOOLING_NO_CHANGE);
  CHECK(batches.clarified_wort_batches == 1);
}

static void test_late_update_and_next_batch(void) {
  whirlpooling_batches_t batches = {2, 0};
  whirlpooling_t w;

  CHECK(whirlpooling_init(&w, &batches) == WHIRLPOOLING_OK);
  CHECK(whirlpooling_start(&w, 0) == WHIRLPOOLING_STARTED);
  CHECK(whirlpooling_update(&w, 5000) == WHIRLPOOLING_DONE); /* late */

  CHECK(whirlpooling_start(&w, 5000) == WHIRLPOOLING_STARTED);
  CHECK(whirlpooling_update(&w, 8599) == WHIRLPOOLING_NO_CHANGE);
  CHECK(whirlpooling_update(&w, 8600) == WHIRLPOOLING_DONE);
  CHECK(batches.hot_wort_batches == 0 && batches.clarified_wort_batches == 2);
  CHECK(whirlpooling_start(&w, 8600) == WHIRLPOOLING_NO_HOT_WORT);
}

static void test_invalid_time_does_not_change_state(void) {
  whirlpooling_batches_t batches = {1, 0};
  whirlpooling_t w;

  CHECK(whirlpooling_init(&w, &batches) == WHIRLPOOLING_OK);
  CHECK(whirlpooling_update(&w, 100) == WHIRLPOOLING_NO_CHANGE);
  CHECK(whirlpooling_start(&w, 99) == WHIRLPOOLING_INVALID_TIME);
  CHECK(w.state == WHIRLPOOLING_WAIT && batches.hot_wort_batches == 1);

  CHECK(whirlpooling_start(&w, 100) == WHIRLPOOLING_STARTED);
  CHECK(whirlpooling_update(&w, 200) == WHIRLPOOLING_NO_CHANGE);
  CHECK(whirlpooling_update(&w, 199) == WHIRLPOOLING_INVALID_TIME);
  CHECK(w.last_time_seconds == 200 && w.started_at_seconds == 100);
  CHECK(w.state == WHIRLPOOLING_WHIRLPOOL);
  CHECK(batches.clarified_wort_batches == 0);
  CHECK(whirlpooling_update(&w, 3700) == WHIRLPOOLING_DONE);

  /* Near the top of the time range the subtraction must not overflow. */
  batches.hot_wort_batches = 1;
  CHECK(whirlpooling_start(&w, UINT64_MAX - WHIRLPOOLING_DURATION_SECONDS) ==
        WHIRLPOOLING_STARTED);
  CHECK(whirlpooling_update(&w, UINT64_MAX - 1) == WHIRLPOOLING_NO_CHANGE);
  CHECK(whirlpooling_update(&w, UINT64_MAX) == WHIRLPOOLING_DONE);
}

static void test_invalid_arguments(void) {
  whirlpooling_batches_t batches = {1, 0};
  whirlpooling_t w;

  CHECK(whirlpooling_init(NULL, &batches) == WHIRLPOOLING_INVALID_ARGUMENT);
  CHECK(whirlpooling_init(&w, NULL) == WHIRLPOOLING_INVALID_ARGUMENT);
  CHECK(whirlpooling_start(NULL, 0) == WHIRLPOOLING_INVALID_ARGUMENT);
  CHECK(whirlpooling_update(NULL, 0) == WHIRLPOOLING_INVALID_ARGUMENT);
}

static void test_duration(void) {
  whirlpooling_batches_t batches = {1, 0};
  whirlpooling_t w;

  CHECK(whirlpooling_init(&w, &batches) == WHIRLPOOLING_OK);
  CHECK(whirlpooling_set_duration(&w, 0) == WHIRLPOOLING_INVALID_ARGUMENT);
  CHECK(whirlpooling_set_duration(&w, 1800) == WHIRLPOOLING_OK);
  CHECK(whirlpooling_start(&w, 0) == WHIRLPOOLING_STARTED);
  CHECK(whirlpooling_set_duration(&w, 60) == WHIRLPOOLING_BUSY);
  CHECK(whirlpooling_update(&w, 1799) == WHIRLPOOLING_NO_CHANGE);
  CHECK(whirlpooling_update(&w, 1800) == WHIRLPOOLING_DONE);
  CHECK(whirlpooling_start(&w, UINT64_MAX) == WHIRLPOOLING_INVALID_TIME);
}

static void test_stop_and_flush(void) {
  whirlpooling_batches_t batches = {2, 0};
  whirlpooling_t w;

  CHECK(whirlpooling_init(&w, &batches) == WHIRLPOOLING_OK);
  CHECK(whirlpooling_stop(&w) == WHIRLPOOLING_DONE);
  CHECK(whirlpooling_flush(&w) == WHIRLPOOLING_DONE);

  /* Stop: nothing is produced, the vessel is ready for another batch. */
  CHECK(whirlpooling_start(&w, 0) == WHIRLPOOLING_STARTED);
  CHECK(whirlpooling_stop(&w) == WHIRLPOOLING_DONE);
  CHECK(w.state == WHIRLPOOLING_WAIT);
  CHECK(whirlpooling_stop(&w) == WHIRLPOOLING_DONE);
  CHECK(whirlpooling_update(&w, 9999) == WHIRLPOOLING_NO_CHANGE);
  CHECK(batches.clarified_wort_batches == 0);

  /* Flush: discard a running batch without producing clarified wort. */
  CHECK(whirlpooling_start(&w, 10000) == WHIRLPOOLING_STARTED);
  CHECK(whirlpooling_flush(&w) == WHIRLPOOLING_DONE);
  CHECK(w.state == WHIRLPOOLING_WAIT && batches.hot_wort_batches == 0);
  CHECK(whirlpooling_update(&w, 20000) == WHIRLPOOLING_NO_CHANGE);
  CHECK(batches.clarified_wort_batches == 0);
  CHECK(whirlpooling_flush(&w) == WHIRLPOOLING_DONE);

  batches.hot_wort_batches = 1;
  CHECK(whirlpooling_start(&w, 20000) == WHIRLPOOLING_STARTED);
  CHECK(whirlpooling_update(&w, 20000 + WHIRLPOOLING_DURATION_SECONDS) ==
        WHIRLPOOLING_DONE);
  CHECK(batches.clarified_wort_batches == 1);
}

int main(void) {
  test_start_guards();
  test_no_output_limit();
  test_deadline_and_single_completion();
  test_late_update_and_next_batch();
  test_invalid_time_does_not_change_state();
  test_invalid_arguments();
  test_duration();
  test_stop_and_flush();
  puts("whirlpooling: all core tests passed");
  return EXIT_SUCCESS;
}
