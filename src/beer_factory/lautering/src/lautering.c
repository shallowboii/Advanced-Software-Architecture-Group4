#include "lautering.h"

void lautering_init(lautering_t *lautering, lautering_batches_t *batches) {
  *lautering = (lautering_t){.state = LAUTERING_WAIT,
                             .batches = batches,
                             .started_at_seconds = 0,
                             .last_time_seconds = 0};
}

lautering_result_t lautering_start(lautering_t *lautering, uint64_t now_seconds) {
  if (now_seconds < lautering->last_time_seconds) {
    return LAUTERING_INVALID_TIME;
  }

  if (lautering->state != LAUTERING_WAIT) {
    return LAUTERING_BUSY;
  }

  if (lautering->batches->wort_batches < 1) {
    return LAUTERING_NO_WORT;
  }

  lautering->started_at_seconds = now_seconds;
  lautering->last_time_seconds = now_seconds;
  lautering->batches->wort_batches--;
  lautering->state = LAUTERING_LAUTER;

  return LAUTERING_STARTED;
}

lautering_result_t lautering_update(lautering_t *lautering, uint64_t now_seconds) {
  if (lautering->state != LAUTERING_LAUTER) {
    return LAUTERING_INVALID_ARGUMENT;
  }

  if (now_seconds < lautering->last_time_seconds) {
    return LAUTERING_INVALID_TIME;
  }

  lautering->last_time_seconds = now_seconds;
  if (now_seconds - lautering->started_at_seconds < LAUTERING_DURATION_SECONDS) {
    return LAUTERING_NO_CHANGE;
  }

  lautering->batches->sweet_wort_batches++;
  lautering->state = LAUTERING_WAIT;

  return LAUTERING_DONE;
}
