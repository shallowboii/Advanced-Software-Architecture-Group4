#include "mashing.h"

#include <stddef.h>

void mashing_init(mashing_t *mashing, unsigned int *wort_batches) {
  *mashing = (mashing_t){.state = MASHING_WAIT,
                         .wort_batches = wort_batches,
                         .started_at_seconds = 0,
                         .last_time_seconds = 0};
}

mashing_result_t mashing_start(mashing_t *mashing, uint64_t now_seconds) {
  if (mashing == NULL || mashing->wort_batches == NULL) {
    return MASHING_INVALID_ARGUMENT;
  }
  if (now_seconds < mashing->last_time_seconds) {
    return MASHING_INVALID_TIME;
  }
  if (mashing->state != MASHING_WAIT) {
    return MASHING_BUSY;
  }

  mashing->started_at_seconds = now_seconds;
  mashing->last_time_seconds = now_seconds;
  mashing->state = MASHING_MASH;
  return MASHING_STARTED;
}

mashing_result_t mashing_stop(mashing_t *mashing) {
  mashing->state = MASHING_WAIT;
  return MASHING_DONE;
}

mashing_result_t mashing_flush(mashing_t *mashing) {
  mashing->state = MASHING_WAIT;
  return MASHING_DONE;
}

mashing_result_t mashing_update(mashing_t *mashing, uint64_t now_seconds) {
  if (mashing->state != MASHING_MASH) {
    return MASHING_INVALID_ARGUMENT;
  }
  if (now_seconds < mashing->last_time_seconds) {
    return MASHING_INVALID_TIME;
  }

  mashing->last_time_seconds = now_seconds;
  if (now_seconds - mashing->started_at_seconds < MASHING_DURATION_SECONDS) {
    return MASHING_NO_CHANGE;
  }

  *mashing->wort_batches += 1;
  mashing->state = MASHING_WAIT;
  return MASHING_DONE;
}
