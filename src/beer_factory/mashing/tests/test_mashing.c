#include "mashing.h"

#include <stdio.h>
#include <stdlib.h>

/* Keep checks enabled when NDEBUG is set. */
#define CHECK(condition) do { \
  if (!(condition)) { \
    fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #condition); \
    exit(EXIT_FAILURE); \
  } \
} while (0)

static void test_initialization(void) {
  unsigned int wort_batches = 2;
  mashing_t mashing;

  mashing_init(&mashing, &wort_batches);
  CHECK(mashing.state == MASHING_WAIT);
  CHECK(mashing.wort_batches == &wort_batches);
  CHECK(mashing.started_at_seconds == 0);
  CHECK(mashing.last_time_seconds == 0);
  CHECK(wort_batches == 2);

  CHECK(mashing_update(&mashing, 100) == MASHING_INVALID_ARGUMENT);
  CHECK(mashing.state == MASHING_WAIT && wort_batches == 2);
}

static void test_start_and_busy(void) {
  unsigned int wort_batches = 0;
  mashing_t mashing;

  mashing_init(&mashing, &wort_batches);
  CHECK(mashing_start(&mashing, 100) == MASHING_STARTED);
  CHECK(mashing.state == MASHING_MASH && wort_batches == 0);

  CHECK(mashing_start(&mashing, 101) == MASHING_BUSY);
  CHECK(mashing.started_at_seconds == 100);
  CHECK(mashing.last_time_seconds == 100);
  CHECK(mashing.state == MASHING_MASH && wort_batches == 0);
}

static void test_deadline_and_single_completion(void) {
  unsigned int wort_batches = 0;
  mashing_t mashing;
  const uint64_t start = 100;
  const uint64_t deadline = start + 7200;

  mashing_init(&mashing, &wort_batches);
  CHECK(mashing_start(&mashing, start) == MASHING_STARTED);
  CHECK(mashing_update(&mashing, start) == MASHING_NO_CHANGE);
  CHECK(mashing_update(&mashing, deadline - 1) == MASHING_NO_CHANGE);
  CHECK(mashing.state == MASHING_MASH && wort_batches == 0);

  CHECK(mashing_update(&mashing, deadline) == MASHING_DONE);
  CHECK(mashing.state == MASHING_WAIT && wort_batches == 1);
  CHECK(mashing_update(&mashing, deadline) == MASHING_INVALID_ARGUMENT);
  CHECK(mashing_update(&mashing, deadline + 1) == MASHING_INVALID_ARGUMENT);
  CHECK(mashing.state == MASHING_WAIT && wort_batches == 1);
}

static void test_late_update_and_next_batch(void) {
  unsigned int wort_batches = 0;
  mashing_t mashing;
  const uint64_t second_start = 7305;
  const uint64_t second_deadline = second_start + MASHING_DURATION_SECONDS;

  mashing_init(&mashing, &wort_batches);
  CHECK(mashing_start(&mashing, 100) == MASHING_STARTED);
  CHECK(mashing_update(&mashing, second_start) == MASHING_DONE);
  CHECK(mashing.state == MASHING_WAIT && wort_batches == 1);

  CHECK(mashing_start(&mashing, second_start) == MASHING_STARTED);
  CHECK(mashing_update(&mashing, second_deadline - 1) == MASHING_NO_CHANGE);
  CHECK(mashing.state == MASHING_MASH && wort_batches == 1);
  CHECK(mashing_update(&mashing, second_deadline) == MASHING_DONE);
  CHECK(mashing.state == MASHING_WAIT && wort_batches == 2);
}

static void test_backward_time(void) {
  unsigned int wort_batches = 0;
  mashing_t mashing;

  mashing_init(&mashing, &wort_batches);
  CHECK(mashing_start(&mashing, 100) == MASHING_STARTED);
  CHECK(mashing_update(&mashing, 200) == MASHING_NO_CHANGE);
  CHECK(mashing_update(&mashing, 199) == MASHING_INVALID_TIME);
  CHECK(mashing.started_at_seconds == 100);
  CHECK(mashing.last_time_seconds == 200);
  CHECK(mashing.state == MASHING_MASH && wort_batches == 0);

  CHECK(mashing_update(&mashing, 7300) == MASHING_DONE);
  CHECK(mashing_start(&mashing, 7299) == MASHING_INVALID_TIME);
  CHECK(mashing.last_time_seconds == 7300);
  CHECK(mashing.state == MASHING_WAIT && wort_batches == 1);
}

static void test_large_timestamps(void) {
  unsigned int wort_batches = 0;
  mashing_t mashing;
  const uint64_t start = UINT64_MAX - MASHING_DURATION_SECONDS;

  mashing_init(&mashing, &wort_batches);
  CHECK(mashing_start(&mashing, start) == MASHING_STARTED);
  CHECK(mashing_update(&mashing, UINT64_MAX - 1) == MASHING_NO_CHANGE);
  CHECK(mashing_update(&mashing, UINT64_MAX) == MASHING_DONE);
  CHECK(mashing.state == MASHING_WAIT && wort_batches == 1);

  CHECK(mashing_start(&mashing, UINT64_MAX) == MASHING_STARTED);
  CHECK(mashing_update(&mashing, UINT64_MAX) == MASHING_NO_CHANGE);
  CHECK(mashing.state == MASHING_MASH && wort_batches == 1);
}

static void test_invalid_start_arguments(void) {
  mashing_t mashing = {.state = MASHING_WAIT, .wort_batches = NULL};

  CHECK(mashing_start(NULL, 100) == MASHING_INVALID_ARGUMENT);
  CHECK(mashing_start(&mashing, 100) == MASHING_INVALID_ARGUMENT);
  CHECK(mashing.state == MASHING_WAIT);
}

int main(void) {
  test_initialization();
  test_start_and_busy();
  test_deadline_and_single_completion();
  test_late_update_and_next_batch();
  test_backward_time();
  test_large_timestamps();
  test_invalid_start_arguments();
  puts("All mashing tests passed.");
  return EXIT_SUCCESS;
}
