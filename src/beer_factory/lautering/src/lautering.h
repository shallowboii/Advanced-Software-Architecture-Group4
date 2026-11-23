#pragma once

#include <stdint.h>

#ifndef TIME_DIVISOR
#define TIME_DIVISOR 1
#endif

#define LAUTERING_DURATION_SECONDS (7200 / TIME_DIVISOR)

typedef enum { LAUTERING_WAIT, LAUTERING_LAUTER } lautering_state_t;

/* The two shared counters Lautering reads and writes in Beer_Factory.xml. */
typedef struct {
  unsigned int wort_batches;
  unsigned int sweet_wort_batches;
} lautering_batches_t;

typedef enum {
  LAUTERING_NO_CHANGE,
  LAUTERING_STARTED,
  LAUTERING_DONE,
  LAUTERING_BUSY,
  LAUTERING_NO_WORT,
  LAUTERING_INVALID_ARGUMENT,
  LAUTERING_INVALID_TIME
} lautering_result_t;

typedef struct {
  lautering_state_t state;
  lautering_batches_t *batches;
  uint64_t started_at_seconds;
  uint64_t last_time_seconds;
} lautering_t;

void lautering_init(lautering_t *lautering, lautering_batches_t *batches);

/* Wait -> Lauter: needs one wort batch, takes it. */
lautering_result_t lautering_start(lautering_t *lautering, uint64_t now_seconds);

/*
 * Advances time. At/after the two-hour deadline, returns LAUTERING_DONE once,
 * produces one sweet-wort batch and returns to LAUTERING_WAIT.
 */
lautering_result_t lautering_update(lautering_t *lautering, uint64_t now_seconds);
