#include "tank.h"

#include <stddef.h>

tank_result_t tank_shared_init(tank_shared_t *shared) {
  *shared = (tank_shared_t){.free_tank = TANK_MAX_TANKS, .chosen_tank = -1};
  return TANK_OK;
}

tank_result_t tank_init(tank_t *tank, tank_shared_t *shared,
                        unsigned int tank_idx) {
  if (tank_idx >= TANK_MAX_TANKS || shared->tanks_status[tank_idx] ||
      shared->almost_done[tank_idx]) {
    return TANK_INVALID_ARGUMENT;
  }

  *tank = (tank_t){.state = TANK_WAIT, .tank_idx = tank_idx, .shared = shared};
  return TANK_OK;
}

tank_result_t tank_start(tank_t *tank, uint64_t now_seconds) {
  if (now_seconds < tank->last_time_seconds) {
    return TANK_INVALID_TIME;
  }

  if (tank->state != TANK_WAIT) {
    return TANK_BUSY;
  }

  tank_shared_t *shared = tank->shared;
  if (shared->started_batches >= TANK_MAX_BATCHES) {
    return TANK_BATCH_LIMIT_REACHED;
  }

  /* Wait -> CheckTankIdx -> Wait for an unselected broadcast recipient. */
  if (tank->tank_idx != shared->chosen_tank) {
    return TANK_NOT_SELECTED;
  }

  if (shared->clarified_wort_batches == 0) {
    return TANK_NO_CLARIFIED_WORT;
  }

  if (shared->free_tank == 0) {
    return TANK_INVALID_ARGUMENT;
  }

  tank->started_at_seconds = now_seconds;
  tank->last_time_seconds = now_seconds;
  shared->tanks_status[tank->tank_idx] = true;
  shared->free_tank--;
  shared->clarified_wort_batches--;
  shared->started_batches++;
  tank->state = TANK_FERMENTATION;

  return TANK_STARTED;
}

tank_result_t tank_update(tank_t *tank, uint64_t now_seconds) {
  if (now_seconds < tank->last_time_seconds) {
    return TANK_INVALID_TIME;
  }

  tank->last_time_seconds = now_seconds;
  if (tank->state == TANK_WAIT) {
    return TANK_OK;
  }

  tank_shared_t *shared = tank->shared;
  const uint64_t elapsed = now_seconds - tank->started_at_seconds;

  // if tank has done fermentating
  if (tank->state == TANK_FERMENTATION &&
      elapsed >= TANK_FERMENTATION_SECONDS) {
    tank->state = TANK_MATURATION;
  // if tank has done maturation
  } else if (tank->state == TANK_MATURATION && elapsed >= TANK_TOTAL_SECONDS) {
    shared->beer_batches++;
    shared->tanks_status[tank->tank_idx] = false;
    shared->free_tank++;
    shared->finished_tanks++;
    shared->almost_done[tank->tank_idx] = false;
    tank->state = TANK_WAIT;
  // if tank has ALMOST done maturation
  } else if (tank->state == TANK_MATURATION &&
             elapsed >= TANK_ALMOST_DONE_SECONDS) {
    shared->almost_done[tank->tank_idx] = true;
  }

  return TANK_OK;
}

bool tank_any_almost_done(const tank_shared_t *shared) {
  for (unsigned int i = 0; i < TANK_MAX_TANKS; i++) {
    if (shared->almost_done[i]) {
      return true;
    }
  }

  return false;
}
