#pragma once

#include <stdint.h>

#define BOILING_DURATION_SECONDS 3600
#define BOILING_MAX_HOT_WORT_BATCHES 1

typedef enum { BOILING_WAIT, BOILING_BOIL } boiling_state_t;

/* The two shared counters passed to Boiling in Beer_Factory.xml. */
typedef struct {
  unsigned int sweet_wort_batches;
  unsigned int hot_wort_batches;
} boiling_batches_t;

typedef enum {
  BOILING_OK,
  BOILING_NO_CHANGE,
  BOILING_STARTED,
  BOILING_DONE,
  BOILING_BUSY,
  BOILING_NO_SWEET_WORT,
  BOILING_OUTPUT_FULL,
  BOILING_INVALID_ARGUMENT,
  BOILING_INVALID_TIME
} boiling_result_t;

typedef struct {
  boiling_state_t state;
  boiling_batches_t *batches;
  uint64_t started_at_seconds;
  uint64_t last_time_seconds;
} boiling_t;

void boiling_init(boiling_t *boiling, boiling_batches_t *batches);
boiling_result_t boiling_start(boiling_t *boiling, uint64_t now_seconds);
boiling_result_t boiling_stop(boiling_t *boiling);
boiling_result_t boiling_flush(boiling_t *boiling);

/*
 * Advances time. At/after the one-hour deadline, returns BOILING_DONE once,
 * produces one hot-wort batch and returns to BOILING_WAIT.
 */
boiling_result_t boiling_update(boiling_t *boiling, uint64_t now_seconds);
