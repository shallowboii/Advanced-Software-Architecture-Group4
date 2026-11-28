#include "boiling.h"

#include <stddef.h>

void boiling_init(boiling_t *boiling, boiling_batches_t *batches) {
  *boiling = (boiling_t){.state = BOILING_WAIT,
                         .batches = batches,
                         .started_at_seconds = 0,
                         .last_time_seconds = 0};
}

boiling_result_t boiling_start(boiling_t *boiling, uint64_t now_seconds) {
  if (now_seconds < boiling->last_time_seconds) {
    return BOILING_INVALID_TIME;
  }

  if (boiling->state != BOILING_WAIT) {
    return BOILING_BUSY;
  }

  if (boiling->batches->sweet_wort_batches < 1) {
    return BOILING_NO_SWEET_WORT;
  }

  if (boiling->batches->hot_wort_batches >= BOILING_MAX_HOT_WORT_BATCHES) {
    return BOILING_OUTPUT_FULL;
  }

  boiling->started_at_seconds = now_seconds;
  boiling->last_time_seconds = now_seconds;
  boiling->batches->sweet_wort_batches--;
  boiling->state = BOILING_BOIL;

  return BOILING_STARTED;
}

boiling_result_t boiling_stop(boiling_t *boiling) {
  boiling->state = BOILING_WAIT;
  return BOILING_DONE;
}

boiling_result_t boiling_flush(boiling_t *boiling) {
  boiling->state = BOILING_WAIT;
  return BOILING_DONE;
}

boiling_result_t boiling_update(boiling_t *boiling, uint64_t now_seconds) {
  if (boiling->state != BOILING_BOIL) {
    return BOILING_INVALID_ARGUMENT;
  }

  if (now_seconds < boiling->last_time_seconds) {
    return BOILING_INVALID_TIME;
  }

  boiling->last_time_seconds = now_seconds;
  if (now_seconds - boiling->started_at_seconds < BOILING_DURATION_SECONDS) {
    return BOILING_NO_CHANGE;
  }

  boiling->batches->hot_wort_batches++;
  boiling->state = BOILING_WAIT;

  return BOILING_DONE;
}
