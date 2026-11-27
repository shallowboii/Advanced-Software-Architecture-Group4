#pragma once

#include <stdint.h>

#define FILTERING_DURATION_SECONDS (4 * 3600)

typedef enum { FILTERING_WAIT, FILTERING_FILTER } filtering_state_t;

typedef struct {
  unsigned int beer_batches;
  unsigned int finished_batches;
} filtering_batches_t;

typedef enum {
  FILTERING_NO_CHANGE,
  FILTERING_STARTED,
  FILTERING_DONE,
  FILTERING_BUSY,
  FILTERING_NO_BEER,
  FILTERING_INVALID_ARGUMENT,
  FILTERING_INVALID_TIME
} filtering_result_t;

typedef struct {
  filtering_state_t state;
  filtering_batches_t *batches;
  uint64_t started_at_seconds;
  uint64_t last_time_seconds;
} filtering_t;

void filtering_init(filtering_t *filtering, filtering_batches_t *batches);
filtering_result_t filtering_start(filtering_t *filtering, uint64_t now_seconds);
filtering_result_t filtering_stop(filtering_t *filtering);
filtering_result_t filtering_flush(filtering_t *filtering);

/* Produces one finished batch and returns FILTERING_DONE once at the deadline. */
filtering_result_t filtering_update(filtering_t *filtering, uint64_t now_seconds);
