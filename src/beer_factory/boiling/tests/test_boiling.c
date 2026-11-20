#include "boiling.h"

#include <stdio.h>
#include <stdlib.h>

/* Keep checks enabled even when a release build defines NDEBUG. */
#define CHECK(condition) do { \
    if (!(condition)) { \
        fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #condition); \
        exit(EXIT_FAILURE); \
    } \
} while (0)

static void test_start_guards(void)
{
    boiling_batches_t batches = { 0, 0 };
    boiling_t boiling;

    boiling_init(&boiling, &batches);
    CHECK(boiling.state == BOILING_WAIT);
    CHECK(boiling_start(&boiling, 10) == BOILING_NO_SWEET_WORT);
    CHECK(boiling.state == BOILING_WAIT);
    CHECK(batches.sweet_wort_batches == 0 && batches.hot_wort_batches == 0);

    batches.sweet_wort_batches = 2;
    batches.hot_wort_batches = 1;
    CHECK(boiling_start(&boiling, 10) == BOILING_OUTPUT_FULL);
    CHECK(boiling.state == BOILING_WAIT);
    CHECK(batches.sweet_wort_batches == 2 && batches.hot_wort_batches == 1);

    batches.hot_wort_batches = 0;
    CHECK(boiling_start(&boiling, 10) == BOILING_STARTED);
    CHECK(boiling.state == BOILING_BOIL);
    CHECK(batches.sweet_wort_batches == 1 && batches.hot_wort_batches == 0);
    CHECK(boiling_start(&boiling, 100) == BOILING_BUSY);
    CHECK(boiling.started_at_seconds == 10);
    CHECK(batches.sweet_wort_batches == 1 && batches.hot_wort_batches == 0);
}

static void test_deadline_and_single_completion(void)
{
    boiling_batches_t batches = { 1, 0 };
    boiling_t boiling;

    boiling_init(&boiling, &batches);
    CHECK(boiling_update(&boiling, 100) == BOILING_INVALID_ARGUMENT);
    CHECK(boiling_start(&boiling, 100) == BOILING_STARTED);
    CHECK(boiling_update(&boiling, 100) == BOILING_NO_CHANGE);
    CHECK(boiling_update(&boiling, 3699) == BOILING_NO_CHANGE);
    CHECK(boiling.state == BOILING_BOIL && batches.hot_wort_batches == 0);
    CHECK(boiling_update(&boiling, 3700) == BOILING_DONE);
    CHECK(boiling.state == BOILING_WAIT);
    CHECK(batches.sweet_wort_batches == 0 && batches.hot_wort_batches == 1);
    CHECK(boiling_update(&boiling, 3700) == BOILING_INVALID_ARGUMENT);
    CHECK(boiling_update(&boiling, 9000) == BOILING_INVALID_ARGUMENT);
    CHECK(batches.hot_wort_batches == 1);
}

static void test_late_update_and_next_batch(void)
{
    boiling_batches_t batches = { 2, 0 };
    boiling_t boiling;

    boiling_init(&boiling, &batches);
    CHECK(boiling_start(&boiling, 0) == BOILING_STARTED);
    CHECK(boiling_update(&boiling, 4000) == BOILING_DONE);
    CHECK(boiling_start(&boiling, 4000) == BOILING_OUTPUT_FULL);
    CHECK(batches.sweet_wort_batches == 1 && batches.hot_wort_batches == 1);

    batches.hot_wort_batches--; /* Downstream consumes the first batch. */
    CHECK(boiling_start(&boiling, 4000) == BOILING_STARTED);
    CHECK(boiling_update(&boiling, 7599) == BOILING_NO_CHANGE);
    CHECK(boiling_update(&boiling, 7600) == BOILING_DONE);
    CHECK(batches.sweet_wort_batches == 0 && batches.hot_wort_batches == 1);
}

static void test_invalid_time_does_not_change_state(void)
{
    boiling_batches_t batches = { 1, 0 };
    boiling_t boiling;

    boiling_init(&boiling, &batches);
    CHECK(boiling_start(&boiling, 100) == BOILING_STARTED);
    CHECK(boiling_update(&boiling, 200) == BOILING_NO_CHANGE);
    CHECK(boiling_update(&boiling, 199) == BOILING_INVALID_TIME);
    CHECK(boiling.last_time_seconds == 200 && boiling.started_at_seconds == 100);
    CHECK(boiling.state == BOILING_BOIL && batches.hot_wort_batches == 0);
    CHECK(boiling_update(&boiling, 3700) == BOILING_DONE);

    batches.hot_wort_batches = 0;
    batches.sweet_wort_batches = 1;
    CHECK(boiling_start(&boiling, UINT64_MAX - BOILING_DURATION_SECONDS) ==
          BOILING_STARTED);
    CHECK(boiling_update(&boiling, UINT64_MAX - 1) == BOILING_NO_CHANGE);
    CHECK(boiling_update(&boiling, UINT64_MAX) == BOILING_DONE);
}

static void test_initialization_and_large_timestamps(void)
{
    boiling_batches_t batches = { 1, 0 };
    boiling_t boiling;

    boiling_init(&boiling, &batches);
    CHECK(boiling.state == BOILING_WAIT);
    CHECK(boiling.batches == &batches);
    CHECK(boiling.started_at_seconds == 0 && boiling.last_time_seconds == 0);
    CHECK(boiling_update(&boiling, 0) == BOILING_INVALID_ARGUMENT);
    CHECK(boiling_start(&boiling, UINT64_MAX) == BOILING_STARTED);
    CHECK(boiling_update(&boiling, UINT64_MAX) == BOILING_NO_CHANGE);
    CHECK(batches.hot_wort_batches == 0);
}

int main(void)
{
    test_start_guards();
    test_deadline_and_single_completion();
    test_late_update_and_next_batch();
    test_invalid_time_does_not_change_state();
    test_initialization_and_large_timestamps();
    puts("All boiling tests passed.");
    return EXIT_SUCCESS;
}
