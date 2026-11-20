#pragma once

#include <stdbool.h>
#include <stdint.h>

#define TANK_MAX_TANKS 24
#define TANK_MAX_BATCHES 50
#define TANK_FERMENTATION_SECONDS 120 * 3600
#define TANK_MATURATION_SECONDS 216 * 3600
#define TANK_TIME_LEFT_LIMIT_SECONDS 6 * 3600
#define TANK_TOTAL_SECONDS (TANK_FERMENTATION_SECONDS + TANK_MATURATION_SECONDS)
#define TANK_ALMOST_DONE_SECONDS                                               \
  (TANK_TOTAL_SECONDS - TANK_TIME_LEFT_LIMIT_SECONDS)

typedef enum { TANK_WAIT, TANK_FERMENTATION, TANK_MATURATION } tank_state_t;

/* Shared globals in Beer_Factory.xml; all 24 tanks use the same instance. */
typedef struct {
  int clarified_wort_batches;
  int green_beer_batches;
  int beer_batches;
  int started_batches;
  int finished_tanks;
  int free_tank;
  int chosen_tank;
  bool tanks_status[TANK_MAX_TANKS]; /* false = free, true = busy */
  bool almost_done[TANK_MAX_TANKS];
} tank_shared_t;

typedef struct {
  tank_state_t state;
  int tank_idx;
  tank_shared_t *shared;
  uint64_t started_at_seconds;
  uint64_t last_time_seconds;
} tank_t;

typedef enum {
  TANK_OK,
  TANK_STARTED,
  TANK_NOT_SELECTED,
  TANK_BUSY,
  TANK_BATCH_LIMIT_REACHED,
  TANK_NO_CLARIFIED_WORT,
  TANK_INVALID_ARGUMENT,
  TANK_INVALID_TIME
} tank_result_t;

tank_result_t tank_shared_init(tank_shared_t *shared);
tank_result_t tank_init(tank_t *tank, tank_shared_t *shared, int tank_idx);
tank_result_t tank_start(tank_t *tank, uint64_t now_seconds);
tank_result_t tank_update(tank_t *tank, uint64_t now_seconds);
bool tank_any_almost_done(const tank_shared_t *shared);
