#include "tank.h"

#include <stdio.h>
#include <stdlib.h>

#define CHECK(condition)                                                        \
  do {                                                                        \
    if (!(condition)) {                                                       \
      fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #condition);            \
      exit(EXIT_FAILURE);                                                     \
    }                                                                         \
  } while (0)

static void check_shared(const tank_shared_t *shared) {
  unsigned int busy = 0;
  for (unsigned int i = 0; i < TANK_MAX_TANKS; i++) {
    busy += shared->tanks_status[i] ? 1U : 0U;
    CHECK(!shared->almost_done[i] || shared->tanks_status[i]);
  }
  CHECK(shared->free_tank + busy == TANK_MAX_TANKS);
  CHECK(shared->started_batches == shared->finished_tanks + busy);
  CHECK(shared->started_batches <= TANK_MAX_BATCHES);
  CHECK(shared->green_beer_batches == 0);
}

static void setup(tank_shared_t *shared, tank_t *tank, unsigned int index) {
  CHECK(tank_shared_init(shared) == TANK_OK);
  CHECK(shared->chosen_tank == -1);
  CHECK(shared->free_tank == TANK_MAX_TANKS);
  CHECK(shared->clarified_wort_batches == 0 && shared->beer_batches == 0);
  CHECK(shared->started_batches == 0 && shared->finished_tanks == 0);
  CHECK(!tank_any_almost_done(shared));
  CHECK(tank_init(tank, shared, index) == TANK_OK);
  CHECK(tank->state == TANK_WAIT);
  shared->chosen_tank = (int)index;
  check_shared(shared);
}

static unsigned int update(tank_t *tank, uint64_t now_seconds) {
  unsigned int events = ~0U;
  CHECK(tank_update(tank, now_seconds, &events) == TANK_OK);
  check_shared(tank->shared);
  return events;
}

static void test_start_guards(void) {
  tank_shared_t shared;
  tank_t tank;
  setup(&shared, &tank, 0);

  CHECK(tank_start(&tank, 10) == TANK_NO_COLD_WORT);
  CHECK(tank.state == TANK_WAIT && tank.last_time_seconds == 0);
  CHECK(shared.started_batches == 0 && shared.free_tank == TANK_MAX_TANKS);
  shared.chosen_tank = -1;
  CHECK(tank_start(&tank, 10) == TANK_NOT_SELECTED);
  shared.chosen_tank = 1;
  shared.clarified_wort_batches = 2;
  CHECK(tank_start(&tank, 10) == TANK_NOT_SELECTED);
  CHECK(shared.clarified_wort_batches == 2);
  shared.chosen_tank = 0;
  CHECK(tank_start(&tank, 10) == TANK_STARTED);
  CHECK(tank.state == TANK_FERMENTATION && shared.tanks_status[0]);
  CHECK(tank.started_at_seconds == 10);
  CHECK(shared.clarified_wort_batches == 1 && shared.started_batches == 1);
  CHECK(shared.free_tank == TANK_MAX_TANKS - 1);
  CHECK(tank_start(&tank, 100) == TANK_BUSY);
  CHECK(tank.started_at_seconds == 10 && tank.last_time_seconds == 10);
  CHECK(shared.clarified_wort_batches == 1 && shared.started_batches == 1);
  check_shared(&shared);
}

static void test_phase_boundaries(void) {
  tank_shared_t shared;
  tank_t tank;
  setup(&shared, &tank, 23);
  shared.clarified_wort_batches = 1;
  const uint64_t start = 17;

  CHECK(update(&tank, start) == TANK_EVENT_NONE);
  CHECK(tank_start(&tank, start) == TANK_STARTED);
  CHECK(update(&tank, start) == TANK_EVENT_NONE);
  CHECK(update(&tank, start + TANK_FERMENTATION_SECONDS - 1) == TANK_EVENT_NONE);
  CHECK(tank.state == TANK_FERMENTATION && shared.beer_batches == 0);
  CHECK(update(&tank, start + TANK_FERMENTATION_SECONDS) ==
        TANK_EVENT_MATURATION_STARTED);
  CHECK(tank.state == TANK_MATURATION);
  CHECK(tank.started_at_seconds == start);
  CHECK(shared.free_tank == TANK_MAX_TANKS - 1);
  CHECK(shared.green_beer_batches == 0 && shared.beer_batches == 0);
  CHECK(update(&tank, start + TANK_FERMENTATION_SECONDS) == TANK_EVENT_NONE);
  CHECK(update(&tank, start + TANK_ALMOST_DONE_SECONDS - 1) == TANK_EVENT_NONE);
  CHECK(!shared.almost_done[23] && !tank_any_almost_done(&shared));
  CHECK(update(&tank, start + TANK_ALMOST_DONE_SECONDS) == TANK_EVENT_ALMOST_DONE);
  CHECK(tank.state == TANK_MATURATION_END && shared.almost_done[23]);
  CHECK(tank_any_almost_done(&shared));
  CHECK(update(&tank, start + TANK_ALMOST_DONE_SECONDS) == TANK_EVENT_NONE);
  CHECK(update(&tank, start + TANK_TOTAL_SECONDS - 1) == TANK_EVENT_NONE);
  CHECK(shared.beer_batches == 0 && shared.finished_tanks == 0);
  CHECK(update(&tank, start + TANK_TOTAL_SECONDS) == TANK_EVENT_COMPLETED);
  CHECK(tank.state == TANK_WAIT && !shared.tanks_status[23]);
  CHECK(shared.free_tank == TANK_MAX_TANKS);
  CHECK(shared.beer_batches == 1 && shared.finished_tanks == 1);
  CHECK(!shared.almost_done[23] && !tank_any_almost_done(&shared));
  CHECK(update(&tank, start + TANK_TOTAL_SECONDS) == TANK_EVENT_NONE);
  CHECK(update(&tank, start + TANK_TOTAL_SECONDS + 100) == TANK_EVENT_NONE);
  CHECK(shared.beer_batches == 1 && shared.finished_tanks == 1);
}

static void test_broadcast_selection(void) {
  tank_shared_t shared;
  tank_t tanks[TANK_MAX_TANKS];
  CHECK(tank_shared_init(&shared) == TANK_OK);
  for (unsigned int i = 0; i < TANK_MAX_TANKS; i++) {
    CHECK(tank_init(&tanks[i], &shared, i) == TANK_OK);
  }
  shared.clarified_wort_batches = 1;
  shared.chosen_tank = 3;
  for (unsigned int i = 0; i < TANK_MAX_TANKS; i++) {
    CHECK(tank_start(&tanks[i], 0) ==
          (i == 3U ? TANK_STARTED : TANK_NOT_SELECTED));
    CHECK(tanks[i].state == (i == 3U ? TANK_FERMENTATION : TANK_WAIT));
    check_shared(&shared);
  }
  CHECK(shared.started_batches == 1 && shared.clarified_wort_batches == 0);
  CHECK(shared.free_tank == 23);
}

static void test_concurrent_tanks(void) {
  tank_shared_t shared;
  tank_t first, second;
  setup(&shared, &first, 0);
  CHECK(tank_init(&second, &shared, 1) == TANK_OK);
  shared.clarified_wort_batches = 2;
  CHECK(tank_start(&first, 0) == TANK_STARTED);
  const uint64_t offset = UINT64_C(2) * 3600;
  shared.chosen_tank = 1;
  CHECK(tank_start(&second, offset) == TANK_STARTED);
  CHECK(shared.free_tank == 22 && shared.started_batches == 2);

  CHECK(update(&first, TANK_ALMOST_DONE_SECONDS) ==
        (TANK_EVENT_MATURATION_STARTED | TANK_EVENT_ALMOST_DONE));
  CHECK(update(&second, TANK_ALMOST_DONE_SECONDS) == TANK_EVENT_MATURATION_STARTED);
  CHECK(shared.almost_done[0] && !shared.almost_done[1]);
  CHECK(update(&second, TANK_TOTAL_SECONDS) == TANK_EVENT_ALMOST_DONE);
  CHECK(update(&first, TANK_TOTAL_SECONDS) == TANK_EVENT_COMPLETED);
  CHECK(!shared.almost_done[0] && shared.almost_done[1]);
  CHECK(tank_any_almost_done(&shared) && shared.free_tank == 23);
  CHECK(shared.beer_batches == 1 && shared.finished_tanks == 1);
  CHECK(update(&second, offset + TANK_TOTAL_SECONDS) == TANK_EVENT_COMPLETED);
  CHECK(shared.free_tank == 24 && shared.beer_batches == 2);
  CHECK(!tank_any_almost_done(&shared));
}

static void test_late_updates_reuse_and_batch_limit(void) {
  tank_shared_t shared;
  tank_t tank;
  setup(&shared, &tank, 0);
  shared.clarified_wort_batches = TANK_MAX_BATCHES + 1;
  uint64_t now = 0;
  const unsigned int all_events = TANK_EVENT_MATURATION_STARTED |
                                  TANK_EVENT_ALMOST_DONE | TANK_EVENT_COMPLETED;

  for (unsigned int i = 0; i < TANK_MAX_BATCHES; i++) {
    CHECK(tank_start(&tank, now) == TANK_STARTED);
    CHECK(tank.started_at_seconds == now);
    now += TANK_TOTAL_SECONDS + 123;
    CHECK(update(&tank, now) == all_events);
    CHECK(tank.state == TANK_WAIT && shared.free_tank == TANK_MAX_TANKS);
    CHECK(shared.beer_batches == i + 1 && shared.finished_tanks == i + 1);
    CHECK(update(&tank, now) == TANK_EVENT_NONE);
  }
  CHECK(tank_start(&tank, now) == TANK_BATCH_LIMIT_REACHED);
  CHECK(shared.clarified_wort_batches == 1 && shared.started_batches == 50);
  CHECK(shared.beer_batches == 50 && shared.finished_tanks == 50);
  CHECK(shared.free_tank == TANK_MAX_TANKS);
  shared.chosen_tank = 1;
  CHECK(tank_start(&tank, now) == TANK_BATCH_LIMIT_REACHED);
}

static void test_time_errors(void) {
  tank_shared_t shared;
  tank_t tank;
  unsigned int events = ~0U;
  setup(&shared, &tank, 0);
  shared.clarified_wort_batches = 2;
  CHECK(update(&tank, 100) == TANK_EVENT_NONE);
  CHECK(tank_start(&tank, 99) == TANK_INVALID_TIME);
  CHECK(tank_start(&tank, UINT64_MAX) == TANK_INVALID_TIME);
  CHECK(tank.state == TANK_WAIT && shared.clarified_wort_batches == 2);
  CHECK(tank_start(&tank, 100) == TANK_STARTED);
  CHECK(update(&tank, 200) == TANK_EVENT_NONE);
  CHECK(tank_update(&tank, 199, &events) == TANK_INVALID_TIME);
  CHECK(events == TANK_EVENT_NONE);
  CHECK(tank.last_time_seconds == 200 && tank.started_at_seconds == 100);
  CHECK(tank.state == TANK_FERMENTATION && shared.beer_batches == 0);
  CHECK(update(&tank, 100 + TANK_TOTAL_SECONDS) & TANK_EVENT_COMPLETED);

  CHECK(tank_start(&tank, UINT64_MAX - TANK_TOTAL_SECONDS) == TANK_STARTED);
  CHECK(update(&tank, UINT64_MAX - 1) ==
        (TANK_EVENT_MATURATION_STARTED | TANK_EVENT_ALMOST_DONE));
  CHECK(update(&tank, UINT64_MAX) == TANK_EVENT_COMPLETED);
  CHECK(shared.beer_batches == 2);
}

static void test_invalid_arguments(void) {
  tank_shared_t shared;
  tank_t tank;
  unsigned int events = ~0U;
  CHECK(tank_shared_init(NULL) == TANK_INVALID_ARGUMENT);
  CHECK(tank_shared_init(&shared) == TANK_OK);
  CHECK(tank_init(NULL, &shared, 0) == TANK_INVALID_ARGUMENT);
  CHECK(tank_init(&tank, NULL, 0) == TANK_INVALID_ARGUMENT);
  CHECK(tank_init(&tank, &shared, TANK_MAX_TANKS) == TANK_INVALID_ARGUMENT);
  CHECK(tank_start(NULL, 0) == TANK_INVALID_ARGUMENT);
  CHECK(tank_update(NULL, 0, &events) == TANK_INVALID_ARGUMENT);
  CHECK(events == TANK_EVENT_NONE);
  CHECK(!tank_any_almost_done(NULL));
  CHECK(tank_init(&tank, &shared, 0) == TANK_OK);
  CHECK(tank_update(&tank, 10, NULL) == TANK_INVALID_ARGUMENT);
  CHECK(tank.last_time_seconds == 0);
  shared.clarified_wort_batches = 1;
  shared.chosen_tank = 0;
  CHECK(tank_start(&tank, 0) == TANK_STARTED);
  tank_t other;
  CHECK(tank_init(&other, &shared, 0) == TANK_INVALID_ARGUMENT);
  check_shared(&shared);
}

int main(void) {
  test_start_guards();
  test_phase_boundaries();
  test_broadcast_selection();
  test_concurrent_tanks();
  test_late_updates_reuse_and_batch_limit();
  test_time_errors();
  test_invalid_arguments();
  puts("All tank tests passed.");
  return EXIT_SUCCESS;
}
