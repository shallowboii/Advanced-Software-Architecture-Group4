#include "lautering.h"

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
    lautering_batches_t batches = { 0, 0 };
    lautering_t lautering;

    lautering_init(&lautering, &batches);
    CHECK(lautering.state == LAUTERING_WAIT);
    CHECK(lautering_start(&lautering, 10) == LAUTERING_NO_WORT);
    CHECK(lautering.state == LAUTERING_WAIT);
    CHECK(batches.wort_batches == 0 && batches.sweet_wort_batches == 0);

    batches.wort_batches = 2;
    CHECK(lautering_start(&lautering, 10) == LAUTERING_STARTED);
    CHECK(lautering.state == LAUTERING_LAUTER);
    CHECK(batches.wort_batches == 1 && batches.sweet_wort_batches == 0);
    CHECK(lautering_start(&lautering, 100) == LAUTERING_BUSY);
    CHECK(lautering.started_at_seconds == 10);
    CHECK(batches.wort_batches == 1 && batches.sweet_wort_batches == 0);
}

static void test_deadline_and_single_completion(void)
{
    lautering_batches_t batches = { 1, 0 };
    lautering_t lautering;

    lautering_init(&lautering, &batches);
    CHECK(lautering_update(&lautering, 100) == LAUTERING_INVALID_ARGUMENT);
    CHECK(lautering_start(&lautering, 100) == LAUTERING_STARTED);
    CHECK(lautering_update(&lautering, 100) == LAUTERING_NO_CHANGE);
    CHECK(lautering_update(&lautering, 7299) == LAUTERING_NO_CHANGE);
    CHECK(lautering.state == LAUTERING_LAUTER && batches.sweet_wort_batches == 0);
    CHECK(lautering_update(&lautering, 7300) == LAUTERING_DONE);
    CHECK(lautering.state == LAUTERING_WAIT);
    CHECK(batches.wort_batches == 0 && batches.sweet_wort_batches == 1);
    CHECK(lautering_update(&lautering, 7300) == LAUTERING_INVALID_ARGUMENT);
    CHECK(lautering_update(&lautering, 20000) == LAUTERING_INVALID_ARGUMENT);
    CHECK(batches.sweet_wort_batches == 1);
}

static void test_late_update_and_next_batch(void)
{
    lautering_batches_t batches = { 2, 0 };
    lautering_t lautering;

    lautering_init(&lautering, &batches);
    CHECK(lautering_start(&lautering, 0) == LAUTERING_STARTED);
    CHECK(lautering_update(&lautering, 8000) == LAUTERING_DONE);
    CHECK(batches.wort_batches == 1 && batches.sweet_wort_batches == 1);

    /* No output cap in the model: the next batch may start at once. */
    CHECK(lautering_start(&lautering, 8000) == LAUTERING_STARTED);
    CHECK(lautering_update(&lautering, 15199) == LAUTERING_NO_CHANGE);
    CHECK(lautering_update(&lautering, 15200) == LAUTERING_DONE);
    CHECK(batches.wort_batches == 0 && batches.sweet_wort_batches == 2);
    CHECK(lautering_start(&lautering, 15200) == LAUTERING_NO_WORT);
}

static void test_invalid_time_does_not_change_state(void)
{
    lautering_batches_t batches = { 1, 0 };
    lautering_t lautering;

    lautering_init(&lautering, &batches);
    CHECK(lautering_start(&lautering, 100) == LAUTERING_STARTED);
    CHECK(lautering_update(&lautering, 200) == LAUTERING_NO_CHANGE);
    CHECK(lautering_update(&lautering, 199) == LAUTERING_INVALID_TIME);
    CHECK(lautering.last_time_seconds == 200 && lautering.started_at_seconds == 100);
    CHECK(lautering.state == LAUTERING_LAUTER && batches.sweet_wort_batches == 0);
    CHECK(lautering_update(&lautering, 7300) == LAUTERING_DONE);
    CHECK(lautering_start(&lautering, 7299) == LAUTERING_INVALID_TIME);

    batches.wort_batches = 1;
    CHECK(lautering_start(&lautering, UINT64_MAX - LAUTERING_DURATION_SECONDS) ==
          LAUTERING_STARTED);
    CHECK(lautering_update(&lautering, UINT64_MAX - 1) == LAUTERING_NO_CHANGE);
    CHECK(lautering_update(&lautering, UINT64_MAX) == LAUTERING_DONE);
}

static void test_initialization_and_large_timestamps(void)
{
    lautering_batches_t batches = { 1, 0 };
    lautering_t lautering;

    lautering_init(&lautering, &batches);
    CHECK(lautering.state == LAUTERING_WAIT);
    CHECK(lautering.batches == &batches);
    CHECK(lautering.started_at_seconds == 0 && lautering.last_time_seconds == 0);
    CHECK(lautering_update(&lautering, 0) == LAUTERING_INVALID_ARGUMENT);
    CHECK(lautering_start(&lautering, UINT64_MAX) == LAUTERING_STARTED);
    CHECK(lautering_update(&lautering, UINT64_MAX) == LAUTERING_NO_CHANGE);
    CHECK(batches.sweet_wort_batches == 0);
}

int main(void)
{
    test_start_guards();
    test_deadline_and_single_completion();
    test_late_update_and_next_batch();
    test_invalid_time_does_not_change_state();
    test_initialization_and_large_timestamps();
    puts("All lautering tests passed.");
    return EXIT_SUCCESS;
}
