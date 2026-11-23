#include "tank.h"

#include <assert.h>
#include <stdio.h>

int main(void) {
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
