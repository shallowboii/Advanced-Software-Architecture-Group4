#pragma once

#include <stdint.h>

/* T_MASHING is two hours in Beer_Factory.xml. */
#define MASHING_DURATION_SECONDS 7200

typedef enum { MASHING_WAIT, MASHING_MASH } mashing_state_t;

typedef enum {
  MASHING_NO_CHANGE,
  MASHING_STARTED,
  MASHING_DONE,
  MASHING_BUSY,
  MASHING_INVALID_ARGUMENT,
  MASHING_INVALID_TIME
} mashing_result_t;

typedef struct {
  mashing_state_t state;
  unsigned int *wort_batches;
  uint64_t started_at_seconds;
  uint64_t last_time_seconds; 
} mashing_t;


void mashing_init(mashing_t *mashing, unsigned int *wort_batches);

/* Accepts start_mashing and resets the clock using monotonic seconds. */
mashing_result_t mashing_start(mashing_t *mashing, uint64_t now_seconds);
mashing_result_t mashing_stop(mashing_t *mashing);
mashing_result_t mashing_flush(mashing_t *mashing);

/*
 * Advances time using an absolute monotonic timestamp in seconds.
 * At or after two hours, produces one wort batch, returns to MASHING_WAIT,
 * and returns MASHING_DONE once. While idle, returns MASHING_INVALID_ARGUMENT.
 */
mashing_result_t mashing_update(mashing_t *mashing, uint64_t now_seconds);
