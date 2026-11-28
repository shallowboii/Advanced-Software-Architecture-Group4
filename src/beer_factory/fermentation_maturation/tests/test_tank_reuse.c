#include "tank.h"

#include <assert.h>
#include <stdio.h>

static void test_stop_and_flush(void) {
  tank_result_t (*commands[])(tank_t *) = {tank_stop, tank_flush};

  for (unsigned int i = 0; i < sizeof(commands) / sizeof(commands[0]); i++) {
    for (int maturation = 0; maturation < 2; maturation++) {
      tank_shared_t shared;
      tank_t tank, other;
      assert(tank_shared_init(&shared) == TANK_OK);
      assert(tank_init(&tank, &shared, 7) == TANK_OK);
      assert(tank_init(&other, &shared, 8) == TANK_OK);
      shared.clarified_wort_batches = 3;
      shared.chosen_tank = 8;
      assert(tank_start(&other, 0) == TANK_STARTED);
      shared.chosen_tank = 7;
      assert(tank_start(&tank, 0) == TANK_STARTED);
      if (maturation) {
        assert(tank_update(&tank, TANK_FERMENTATION_SECONDS) == TANK_OK);
        assert(tank_update(&tank, TANK_ALMOST_DONE_SECONDS) == TANK_OK);
        assert(shared.almost_done[7]);
      }

      assert(commands[i](&tank) == TANK_OK);
      assert(tank.state == TANK_WAIT);
      assert(!shared.tanks_status[7] && !shared.almost_done[7]);
      assert(shared.tanks_status[8] && other.state == TANK_FERMENTATION);
      assert(shared.free_tank == TANK_MAX_TANKS - 1);
      assert(commands[i](&tank) == TANK_OK);
      assert(shared.free_tank == TANK_MAX_TANKS - 1);
      assert(tank_update(&tank, TANK_TOTAL_SECONDS) == TANK_OK);
      assert(shared.beer_batches == 0 && shared.finished_tanks == 0);

      assert(tank_start(&tank, TANK_TOTAL_SECONDS) == TANK_STARTED);
      assert(tank_update(&tank, TANK_TOTAL_SECONDS + TANK_FERMENTATION_SECONDS) ==
             TANK_OK);
      assert(tank_update(&tank, 2 * TANK_TOTAL_SECONDS) == TANK_OK);
      assert(shared.beer_batches == 1 && shared.finished_tanks == 1);
      assert(shared.free_tank == TANK_MAX_TANKS - 1);
    }
  }
}

int main(void) {
  test_stop_and_flush();
  tank_shared_t shared;
  tank_t tank;
  assert(tank_shared_init(&shared) == TANK_OK);
  assert(tank_init(&tank, &shared, 7) == TANK_OK);
  shared.chosen_tank = 7;

  uint64_t now = 0;
  for (int batch = 0; batch < 100; ++batch) {
    shared.clarified_wort_batches = 1;
    assert(tank_start(&tank, now) == TANK_STARTED);
    assert(tank_update(&tank, now + TANK_FERMENTATION_SECONDS) == TANK_OK);
    now += TANK_TOTAL_SECONDS;
    assert(tank_update(&tank, now) == TANK_OK);
    assert(tank.state == TANK_WAIT);
    assert(shared.free_tank == TANK_MAX_TANKS);
    assert(shared.started_batches == batch + 1);
    assert(shared.finished_tanks == batch + 1);
  }

  puts("Tank reuse test passed (100 batches).");
  return 0;
}
